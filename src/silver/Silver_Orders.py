import logging
import uuid
from datetime import datetime
from typing import Dict
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from src.utils.env import EnvConfig


logger = logging.getLogger(__name__)

DQ_MAX_PK_NULL = 0.0
DQ_MAX_CUSTOMER_ID_NULL = 0.0
DQ_MAX_PRODUCT_ID_NULL = 0.0
DQ_MAX_BAD_MEASURES = 0.01

def _build_config(env: EnvConfig) -> Dict[str, str]:

    return {
        # OPS
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",

        # TABLES
        "stage_table": f"{env.catalog}.{env.project}_silver.stage_orders",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.quarantine_orders",
        "conform_table": f"{env.catalog}.{env.project}_silver.conform_orders",
        "dq_table": f"{env.catalog}.{env.project}_silver.dq_orders",

        # PATHS
        "bronze_path": f"{env.raw_base_path}/{env.project}/orders",
        "stage_path": f"{env.curated_base_path}/{env.project}/orders/stage_orders",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/orders/quarantine/bad_keys",
        "conform_path": f"{env.curated_base_path}/{env.project}/orders/conform_orders",
        "dq_path": f"{env.curated_base_path}/{env.project}/orders/data_quality",
    }


def _ensure_table_props(spark: SparkSession, table_name: str):
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)


def _get_last_watermark(
    spark: SparkSession,
    state_table: str,
    pipeline_name: str,
    dataset: str
) -> datetime:

    rows = spark.sql(f"""
        SELECT last_watermark_ts
        FROM {state_table}
        WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}'
        ORDER BY updated_at DESC
        LIMIT 1
    """).collect()

    if (not rows) or rows[0]["last_watermark_ts"] is None:
        return datetime(1900, 1, 1)

    return rows[0]["last_watermark_ts"]


def _upsert_watermark(
    spark: SparkSession,
    state_table: str,
    pipeline_name: str,
    dataset: str,
    new_wm: datetime,
    run_id: str
):

    wm_str = new_wm.isoformat(sep=" ")

    spark.sql(f"""
        MERGE INTO {state_table} t
        USING (
            SELECT
                '{pipeline_name}' AS pipeline_name,
                '{dataset}' AS dataset,
                TIMESTAMP '{wm_str}' AS last_watermark_ts,
                '{run_id}' AS updated_by_run_id
        ) s
        ON t.pipeline_name = s.pipeline_name AND t.dataset = s.dataset
        WHEN MATCHED THEN UPDATE SET
            t.last_watermark_ts = s.last_watermark_ts,
            t.updated_at = current_timestamp(),
            t.updated_by_run_id = s.updated_by_run_id
        WHEN NOT MATCHED THEN INSERT (
            pipeline_name,
            dataset,
            last_watermark_ts,
            updated_at,
            updated_by_run_id
        )
        VALUES (
            s.pipeline_name,
            s.dataset,
            s.last_watermark_ts,
            current_timestamp(),
            s.updated_by_run_id
        )
    """)


def run_silver_orders(spark: SparkSession, env: EnvConfig, dq_scope: str = None) -> None:


    cfg = _build_config(env)
    pipeline_name = "silver_orders"
    dataset = "orders"
    target_table = cfg["conform_table"]
    run_id = str(uuid.uuid4())
    pk = "order_id"

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (pipeline_name, dataset, target_table, run_id, started_at, status)
                VALUES ('{pipeline_name}', '{dataset}', '{target_table}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    rows_in = 0
    rows_quarantined = 0
    rows_out = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    try:

        logger.info("Silver orders started | run_id=%s", run_id)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_cols = [
            "order_id", "customer_id", "product_id",
            "order_date", "quantity", "total_amount",
            "_ingest_ts", "_ingest_date", "_file_name", "_source"
        ]

        missing = [c for c in required_cols if c not in bronze_df.columns]

        if missing:
            raise ValueError(f"Bronze missing required cols: {missing}")
        
        last_wm = _get_last_watermark(spark, cfg["state_table"])

        incr_df = bronze_df.filter(col("_ingest_ts") > lit(last_wm))

        if incr_df.take(1) == []:
            spark.sql(f"""
                        UPDATE {cfg["run_logs_table"]}
                        SET finished_at = current_timestamp(),
                            status = 'SUCCESS',
                            dq_result = 'SKIPPED_NO_NEW_DATA',
                            rows_in = 0,
                            rows_quarantined = 0,
                            rows_out = 0,
                            last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                        WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                    """)
            logger.info("No new data. Exiting.")
            return
        
        new_wm = incr_df.agg(max(col("_ingest_ts")).alias("mx")).collect()[0]["mx"]


        # -------------------------
        # Stage (overwrite slice)
        # -------------------------

        stage_df = (
            incr_df
            .withColumn(pk, col(pk).cast("string"))
            .withColumn("customer_id", col("customer_id").cast("string"))
            .withColumn("product_id", col("product_id").cast("string"))
            .withColumn("order_date", col("order_date").cast("date"))
            .withColumn("quantity", col("quantity").cast("bigint"))
            .withColumn("total_amount", col("total_amount").cast("double"))
            .withColumn("etl_run_id", lit(run_id))
            .withColumn("silver_processed_ts", current_timestamp())
            .withColumn("silver_processed_date", current_date())
            .withColumn("source_layer", lit("bronze"))
        )

        (stage_df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(cfg["stage_path"])
         )
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["stage_table"]}
                    USING DELTA
                    LOCATION '{cfg['stage_path']}'
                """)
        _ensure_table_props(spark, cfg["stage_table"])

        stage_df = spark.read.table(cfg["stage_table"])


        # -------------------------
        # Quarantine + dedup
        # -------------------------

        bad_conditions = (
        col(pk).isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
        | col("order_date").isNull()
        | col("quantity").isNull() | (col("quantity") <= 0)
        | col("total_amount").isNull() | (col("total_amount") < 0)
    )

        bad_keys = (stage_df
                .filter(bad_conditions)
                .withColumn(
                    "_quarantine_reason",
                    concat_ws("; ",
                    when(col(pk).isNull(), lit(f"{pk} is NULL")),
                    when(col("customer_id").isNull(), lit("customer_id is NULL")),
                    when(col("product_id").isNull(), lit("product_id is NULL")),
                    when(col("order_date").isNull(), lit("order_id is NULL")),
                    when(col("quantity").isNull(), lit("quantity is NULL")),
                    when(col("quantity") <= 0, lit("quantity <= 0")),
                    when(col("total_amount").isNull(), lit("total_amount is NULL")),
                    when(col("total_amount") < 0, lit("total_amount < 0")),
                    )
                )
                .withColumn("_quarantine_ts", current_timestamp())
            )
        
        good_keys = (stage_df.filter(~bad_conditions))

        w = Window.partitionBy(col(pk)).orderBy(col("_ingest_ts").desc(), col("processed_ts").desc())

        dedup = (
            good_keys
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        rows_in = stage_df.count()
        rows_quarantined = bad_keys.count()
        rows_out = dedup.count()


        # -------------------------
        # DQ gate
        # -------------------------

        dq_agg = stage_df.agg(
            count(lit(1)).alias("total_rows"),
            sum(when(col(pk).isNull(), 1).otherwise(0)).alias("pk_null"),
            sum(when(col("customer_id").isNull(), 1).otherwise(0)).alias("customer_id_null"),
            sum(when(col("product_id").isNull(), 1).otherwise(0)).alias("product_id_null"),
            sum(when(col("quantity").isNull() | (col("quantity") <= 0), 1).otherwise(0)).alias("bad_quantity"),
            sum(when(col("total_amount").isNull() | (col("total_amount") < 0, 1)).otherwise(0)).alias("bad_total_amount")
        ).collect()[0]


        dq_source_table = spark.read.table(cfg["state_table"])

        total_rows = int(dq_agg["total_rows"])
        pk_null = int(dq_agg["pk_null"])
        customer_id_null = int(dq_agg["customer_id_null"])
        product_id_null = int(dq_agg["product_id_null"])
        bad_measures = int(dq_agg["bad_quantity"] + dq_agg["bad_total_amount"])

        pk_null_pct = (pk_null / total_rows) if total_rows else 0.0
        customer_id_null_pct = (customer_id_null / total_rows) if total_rows else 0.0
        product_id_null_pct = (product_id_null / total_rows) if total_rows else 0.0
        bad_measures_pct = (bad_measures / total_rows) if total_rows else 0.0


        dq_result = "OK"
        if (pk_null_pct > DQ_MAX_PK_NULL) or (customer_id_null_pct > DQ_MAX_CUSTOMER_ID_NULL) or (product_id_null_pct > DQ_MAX_PRODUCT_ID_NULL) or (bad_measures_pct > DQ_MAX_BAD_MEASURES):
            dq_result = "FAIL"


        dq_df = spark.createDataFrame([(
            dq_source_table,
            run_id,
            datetime.utcnow(),
            total_rows,
            pk_null,
            customer_id_null,
            product_id_null,
            bad_measures,
            float(pk_null_pct),
            float(customer_id_null_pct),
            float(product_id_null_pct),
            float(bad_measures_pct)
        )],
        """
        table_name STRING,
        run_id STRING,
        dq_ts TIMESTAMP,
        total_rows BIGINT,
        pk_null BIGINT,
        customer_id_null BIGINT,
        product_id_null BIGINT,
        bad_measures BIGINT,
        pk_null_pct DOUBLE,
        customer_id_null_pct DOUBLE,
        product_id_null_pct DOUBLE,
        bad_measures_pct DOUBLE,
        dq_result STRING
        """
        )

        (dq_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(cfg["dq_path"])
         )
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["dq_table"]}
                    USING DELTA
                    LOCATION '{cfg["dq_path"]}'
                """)
        _ensure_table_props(spark, cfg["dq_table"])

        if dq_result == "FAIL":
            raise ValueError(
            f"dq FAIL: pk_null_pct={pk_null_pct:.4f}, customer_id_null_pct={customer_id_null_pct:.4f}, "
            f"product_id_null_pct={product_id_null_pct:.4f}, bad_measures_pct={bad_measures_pct:.4f}"
            )
        
        # -------------------------
        # Current snapshot (SCD1)
        # -------------------------

        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["conform_table"]} (
                    order_id STRING,
                    customer_id STRING,
                    product_id STRING,
                    order_date DATE,
                    quantity BIGINT,
                    total_amount DOUBLE,
                    _file_name STRING,
                    _source STRING,
                    silver_processed_ts TIMESTAMP,
                    silver_processed_date DATE,
                    etl_run_id STRING,
                    source_layer STRING
                    )
                    USING DELTA
                    LOCATION '{cfg["conform_path"]}'  
                """)
        _ensure_table_props(spark, cfg["conform_table"])

        conf_dt = DeltaTable.forName(spark, cfg["conform_table"])

        (conf_dt.alias("t")
         .merge(dedup.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )
        
        _upsert_watermark(spark, cfg["state_table"], pipeline_name, dataset, new_wm, run_id)

        

        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp(),
                        status = 'SUCCESS',
                        dq_result = '{dq_result}',
                        rows_in = {rows_in},
                        rows_out = {rows_out},
                        rows_quarantined = {rows_quarantined},
                        last_watermark_ts = TIMESTAMP '{new_wm.isoformat(sep=" ")}'
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                    """)
        
        logger.info("Silver orders SUCCESS | rows_in=%d rows_out=%d quarantined=%d", rows_in, rows_out, rows_quarantined)
    
    except Exception as e:
        msg = str(e).replace("'", "''")
        wm_to_log = last_wm if last_wm is not None else datetime(1900, 1, 1)
        dq_to_log = dq_result if dq_result is not None else "ERROR"
        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp,
                        status = 'FAILED',
                        error_msg = '{msg}',
                        rows_in = {rows_in},
                        rows_quarantined = {rows_quarantined},
                        rows_out = {rows_out},
                        dq_result = '{dq_to_log}',
                        last_watermark_ts = TIMESTAMP {wm_to_log.isoformat(sep=" ")}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        logger.exception("Silver orders FAILED")
        raise