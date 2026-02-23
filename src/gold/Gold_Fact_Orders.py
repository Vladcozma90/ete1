import logging
import uuid
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, count, current_date, current_timestamp,
    lit, max as fmax, row_number
)
from pyspark.sql.window import Window
from src.utils.env import load_envs, EnvConfig
from src.utils.logger import setup_log
from src.utils.watermark import get_last_watermark, upsert_watermark

logger = logging.getLogger(__name__)

def _get_spark() -> SparkSession:
    return SparkSession.builder.appName("gold_fact_orders_asof").getOrCreate()

def _build_config(env: EnvConfig) -> dict[str, str]:

    return {
        # OPS
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",

        # Tables
        "silver_orders_table" : f"{env.catalog}.{env.project}_silver.conform_orders",
        "silver_customers_table" : f"{env.catalog}.{env.project}_silver.conform_customers",
        "silver_products_table" : f"{env.catalog}.{env.project}_silver.conform_products",
        "gold_fact_table" : f"{env.catalog}.{env.project}_gold.fact_orders_asof",
        
        # Paths
        "gold_fact_path" : f"{env.curated_base_path}/{env.project}/fact_orders_asof",
    }

def run_gold_fact_orders_asof(spark: SparkSession, env: EnvConfig, backfill_days: int = 0) -> None:
    cfg = _build_config(env)
    
    pipeline_name = "gold_fact_orders_asof"
    dataset = "fact_orders_asof"
    run_id = str(uuid.uuid4())
    target_table = cfg["gold_fact_table"]

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (pipeline_name, dataset, target_table, run_id, started_at, status)
                VALUES ('{pipeline_name}', '{dataset}', '{target_table}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    rows_in = 0
    rows_out = 0
    last_wm = None
    new_wm = None

    try:

        logger.info("Gold fact orders start | run_id=%s | dataset=%s", run_id, dataset)

        spark.sql(f"""
                    Create table if not exists {cfg["gold_fact_table"]} (
                        order_id STRING,
                        customer_id STRING,
                        product_id STRING,
                        customer_sk BIGINT,
                        product_sk BIGINT,
                        order_date DATE,
                        order_ts TIMESTAMP,
                        quantity BIGINT,
                        total_amount DOUBLE,

                        etl_run_id STRING,
                        silver_processed_ts TIMESTAMP,
                        silver_processed_date DATE,

                        gold_loaded_ts TIMESTAMP,
                        gold_loaded_date DATE,
                        source_layer STRING
                    )
                    USING DELTA
                    LOCATION '{cfg["gold_fact_path"]}'
                """)
        
        silver_orders = spark.read.table(cfg["silver_orders_table"])

        last_wm = get_last_watermark(spark, cfg["state_table"], pipeline_name, dataset)

        if backfill_days and backfill_days > 0:
            last_wm = last_wm - timedelta(days=backfill_days)

        incr_orders = silver_orders.filter(col("silver_processed_ts") > lit(last_wm))

        if incr_orders.take(1) == []:
            spark.sql(f"""
                        UPDATE {cfg["run_logs_table"]}
                        SET finished_at = current_timestamp(),
                            status = 'SUCCESS',
                            dq_result = 'SKIPPED_NO_NEW_DATA',
                            rows_in = 0,
                            rows_out = 0,
                            last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                        WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                    """)
            logger.info("Gold fact orders: no new data (watermarks=%s)", last_wm)
            return
        
        new_wm = incr_orders.agg(fmax(col("silver_processed_ts")).alias("mx")).collect()[0]["mx"]

        o = (
            incr_orders
            .withColumn("order_ts", col("order_date").cast("timestamp"))
            .select(
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "order_ts",
                "quantity",
                "total_amount",
                "etl_run_id",
                "silver_processed_ts",
                "silver_processed_date",
            ).alias("o")
        )

        cdim = (
            spark.read.table(cfg["silver_customers_table"])
            .select(
                "customer_id",
                "customer_sk",
                col("silver_effective_start_ts").alias("c_start"),
                col("silver_effective_end_ts").alias("c_end")
            ).alias("c")
        )

        pdim = (
            spark.read.table(cfg["silver_products_table"])
            .select(
                "product_id",
                "product_sk",
                col("silver_effective_start_ts").alias("p_start"),
                col("silver_effective_end_ts").alias("p_end")
            ).alias("p")
        )

        c_end_max = coalesce(col("c.c_end"), lit("9999-12-31 23:59:59").cast("timestamp"))
        p_end_max = coalesce(col("p.p_end"), lit("9999-12-31 23:59:59").cast("timestamp"))

        oc = (
            o.join(cdim, on=(
                (col("o.customer_id") == col("c.customer_id")) &
                (col("o.order_ts") >= col("c.c_start")) &
                (col("o.order_ts") < c_end_max)
            ),
            how="left")
            .withColumn("customer_sk", coalesce(col("customer_sk"), lit(-1).cast("bigint")))
            .select("o.*", "customer_sk", "c_start", "c_end")
            .alias("oc")
        )

        # Handle multiple matches by taking the one with latest effective start date
        w_c = Window.partitionBy("order_id").orderBy(col("c_start").desc_nulls_last())
        oc = oc.withColumn("_rn", row_number().over(w_c)).filter(col("_rn") == 1).drop("_rn")

        ocp = (
            oc.join(pdim, on=(
                (col("oc.product_id") == col("p.product_id")) &
                (col("oc.order_ts") >= col("p.p_start")) &
                (col("oc.order_ts") < p_end_max)
            ),
            how="left")
            .withColumn("product_sk", coalesce(col("product_sk"), lit(-1).cast("bigint")))
            .select("oc.*", "product_sk", "p_start", "p_end")
        )

        # Handle multiple matches by taking the one with latest effective start date
        w_p = Window.partitionBy("order_id").orderBy(col("p_start").desc_nulls_last())
        ocp = ocp.withColumn("_rn", row_number().over(w_p)).filter(col("_rn") == 1).drop("_rn")

        gold_batch = (
            ocp.select(
                "order_id",
                "customer_id",
                "product_id",
                "customer_sk",
                "product_sk",
                "order_date",
                "order_ts",
                col("quantity").cast("bigint").alias("quantity"),
                col("total_amount").cast("double").alias("total_amount"),
                "etl_run_id",
                "silver_processed_ts",
                "silver_processed_date",
                current_timestamp().alias("gold_loaded_ts"),
                current_date().alias("gold_loaded_date"),
                lit("silver").alias("source_layer"),
            )
        )

        gold_dt = DeltaTable.forName(spark, cfg["gold_fact_table"])

        (gold_dt.alias("t")
         .merge(gold_batch.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )
        
        rows_in = incr_orders.count()
        rows_out = gold_batch.count()

        upsert_watermark(spark, cfg["state_table"], pipeline_name, dataset, new_wm, run_id)

        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp(),
                        status = 'SUCCESS',
                        rows_in = {rows_in},
                        rows_out = {rows_out},
                        last_watermark_ts = TIMESTAMP '{new_wm.isoformat(sep=" ")}'
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        logger.info("Gold fact orders completed successfully | run_id=%s | dataset=%s | rows_in=%d | rows_out=%d | wm=%s", run_id, dataset, rows_in, rows_out, new_wm)
        

    except Exception as e:
        msg = str(e).replace("'", "''")
        wm_to_log = last_wm if last_wm is not None else datetime(1900, 1, 1)
        spark.sql(
            f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'FAILED',
                error_msg = '{msg}',
                rows_in = {rows_in},
                rows_out = {rows_out},
                last_watermark_ts = TIMESTAMP '{wm_to_log.isoformat(sep=" ")}'
            WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
            """
        )
        logger.exception("Gold fact orders FAILED")
        raise

if __name__ == "__main__":
    spark = _get_spark()
    env = load_envs()
    setup_log(env.logger_level)

    try:

        backfill_days = env.backfill_days
        run_gold_fact_orders_asof(spark, env, backfill_days)
    
    except Exception:
        logger.exception("Gold fact orders job FAILED.")
        raise

    finally:
        spark.stop()