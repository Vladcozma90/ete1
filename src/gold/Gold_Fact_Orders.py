from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import uuid
from src.utils.env import load_envs

spark = SparkSession.builder.getOrCreate()
env = load_envs()


dbutils.widgets.text("backfill_days", "0")
BACKFILL_DAYS = int(dbutils.widgets.get("backfill_days").strip())

CONFIG = {

    "silver_orders_table" : f"{env.catalog}.{env.dataset}_silver.conform_orders",

    "gold_dim_customers_table" : f"{env.catalog}.{env.dataset}_gold.dim_customers_hist",
    "gold_dim_products_table" : f"{env.catalog}.{env.dataset}_gold.dim_products_hist",

    "gold_fact_table" : f"{env.catalog}.{env.dataset}_gold.fact_orders_asof",
    "gold_fact_path" : f"{env.catalog}/{env.curated_base_path}/orders/fact_orders_asof",
}

OPS = {
    "run_logs_table" : f"{env.catalog}.{env.dataset}_ops.run_logs",
    "state_table" : f"{env.catalog}.{env.dataset}_ops.state_pipeline",
}

PIPELINE_NAME = "gold_fact_orders_asof"
DATASET = "fact_orders_asof"
RUN_ID = str(uuid.uuid4())
TARGET_TABLE = CONFIG["gold_fact_table"]

def get_last_watermark(pipeline_name: str, dataset: str) -> datetime:
    rows = spark.sql(f"""
      SELECT last_watermark_ts
      FROM {OPS["state_table"]}
      WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}'
      ORDER BY updated_at DESC
      LIMIT 1
    """).collect()
    if (not rows) or rows[0]["last_watermark_ts"] is None:
        return datetime(1900,1,1)
    return rows[0]["last_watermark_ts"]

def upsert_watermark(pipeline_name: str, dataset: str, new_wm: datetime, run_id: str):
    wm_str = new_wm.isoformat(sep=" ")
    spark.sql(f"""
      MERGE INTO {OPS["state_table"]} t
      USING (
        SELECT '{pipeline_name}' AS pipeline_name,
               '{dataset}' AS dataset,
               TIMESTAMP '{wm_str}' AS last_watermark_ts,
               '{run_id}' AS updated_by_run_id
      ) s
      ON t.pipeline_name = s.pipeline_name AND t.dataset = s.dataset
      WHEN MATCHED THEN UPDATE SET
        t.last_watermark_ts = s.last_watermark_ts,
        t.updated_at = current_timestamp(),
        t.updated_by_run_id = s.updated_by_run_id
      WHEN NOT MATCHED THEN INSERT (pipeline_name, dataset, last_watermark_ts, updated_at, updated_by_run_id)
      VALUES (s.pipeline_name, s.dataset, s.last_watermark_ts, current_timestamp(), s.updated_by_run_id)
    """)

# Run log start
spark.sql(f"""
  INSERT INTO {OPS["run_logs_table"]} (pipeline_name, dataset, target_table, run_id, started_at, status)
  VALUES ('{PIPELINE_NAME}', '{DATASET}', '{TARGET_TABLE}', '{RUN_ID}', current_timestamp(), 'RUNNING')
""")


rows_in = 0
rows_out = 0
last_wm = None
new_wm = None

try:

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {CONFIG["gold_fact_table"]} (
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
                    gold_loaded_ts TIMESTAMP,
                    gold_loaded_date DATE,
                    source_layer STRING 
                )
                USING DELTA
                LOCATION '{CONFIG['gold_fact_path']}'
            """)

    silver_orders = spark.read.table(CONFIG["silver_orders_table"])

    last_wm = get_last_watermark(PIPELINE_NAME, DATASET)

    incr_silver = silver_orders.filter(col("_ingest_ts") > lit(last_wm))

    if incr_silver.limit(1).count() == 0:
        spark.sql(f"""
                  UPDATE {OPS["run_logs_table"]}
                  SET finished_at = current_timestamp(),
                      status = 'SUCCESS',
                      rows_in = 0,
                      rows_out = 0,
                      last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                      WHERE run_id = '{RUN_ID}'
                  """)
        dbutils.notebook.exit("No new data to process. Exiting.")

    new_wm = incr_silver.agg(max(col("_ingest_ts")).alias("mx")).collect()[0]["mx"]

    o = (incr_silver
         .withColumn("order_ts", col("order_date").cast("timestamp"))
         .alias("o")
         )
    
    cdim = (spark.read.table(CONFIG["gold_dim_customers_table"])
            .select("customer_id", "customer_sk", "silver_effective_start_ts", "silver_effective_end_ts")
            .alias("c")
            )
    
    pdim = (spark.read.table(CONFIG["gold_dim_products_table"])
            .select("product_id", "product_sk", "silver_effective_start_ts", "silver_effective_end_ts")
            .alias("p")
            )
    
    c_end_max = coalesce(col("c.silver_effective_end_ts"), lit("9999-12-31 23:59:59").cast("timestamp"))

    joined_oc = (
        o.join(
            cdim,
            on=(
                (col("o.customer_id") == col("c.customer_id")) &
                (col("o.order_ts") >= col("c.silver_effective_start_ts")) &
                (col("o.order_ts") <= c_end_max)
            ),
            how="left"
        )
        .withColumnRenamed("silver_effective_start_ts", "c_start")
        .withColumnRenamed("silver_effective_end_ts", "c_end")
    )

    w_c = Window.partitionBy(col("o.order_id")).orderBy(col("c_start").desc_nulls_last())

    joined_oc = (joined_oc
                 .withColumn("_rn", row_number().over(w_c))
                 .filter(col("_rn") == 1)
                 .drop("_rn")
                 .withColumn("customer_sk", coalesce(col("customer_sk"), lit(-1)).cast("bigint"))
                 )
    
    p_end_max = coalesce(col("p.silver_effective_end_ts"), lit("9999-12-31 23:59:59").cast("timestamp"))

    joined_ocp = (
        joined_oc.alias("oc")
        .join(
            pdim,
            on=(
                (col("oc.product_id") == col("p.product_id")) &
                (col("oc.order_ts") >= col("p.silver_effective_start_ts")) &
                (col("oc.order_ts") <= p_end_max)                
                ),
            how="left"
        )
        .withColumnRenamed("silver_effective_start_ts", "p_start")
        .withColumnRenamed("silver_effective_end_ts", "p_end")
    )

    w_p = Window.partitionBy(col("o.order_id")).orderBy(col("p_start").desc_nulls_last())

    joined_ocp = (joined_ocp
                  .withColumn("_rn", row_number().over(w_p))
                  .filter(col("_rn") == 1)
                  .drop("_rn")
                  .withColumn("product_sk", coalesce(col("product_sk"), lit(-1)).cast("bigint"))
                  )
    
    gold_batch = (joined_ocp
                  .select(
                      col("oc.order_id").alias("order_id"),
                      col("oc.customer_id").alias("customer_id"),
                      col("oc.product_id").alias("product_id"),
                      col("oc.customer_sk").cast("bigint").alias("customer_sk"),
                      col("p.product_sk").cast("bigint").alias("product_sk"),
                      col("oc.order_date").alias("order_date"),
                      col("oc.order_ts").alias("order_ts"),
                      col("oc.quantity").cast("bigint").alias("quantity"),
                      col("oc.total_amount").cast("double").alias("total_amount"),
                      col("oc.etl_run_id").alias("etl_run_id"),
                      col("oc.silver_processed_ts").alias("silver_processed_ts"),
                      col("oc.silver_processed_date").alias("silver_processed_date"),
                      current_timestamp().alias("gold_loaded_ts"),
                      current_date().alias("gold_loaded_date"),
                      lit("silver").alias("source_layer")
                  ))
    
    gold_dt = DeltaTable.forName(spark, CONFIG["gold_fact_table"])

    (gold_dt.alias("t")
     .merge(gold_batch.alias("s"), "t.order_id = s.order_id")
     .whenNotMatchedInsertAll()
     .execute()
     )
    
    metrics = (
        incr_silver
        .agg(count(lit(1)).alias("rows_in"))
        .crossJoin(
            gold_batch.agg(count(lit(1)).alias("rows_out")))
        .collect()[0]
    )

    rows_in = int(metrics["rows_in"])
    rows_out = int(metrics["rows_out"])

    upsert_watermark(PIPELINE_NAME, DATASET, new_wm, RUN_ID)

    spark.sql(f"""
      UPDATE {OPS["run_logs_table"]}
      SET finished_at = current_timestamp(),
          status = 'SUCCESS',
          rows_in = {rows_in},
          rows_out = {rows_out},
          last_watermark_ts = TIMESTAMP '{new_wm.isoformat(sep=" ")}'
      WHERE pipeline_name = '{PIPELINE_NAME}' AND dataset = '{DATASET}' AND run_id = '{RUN_ID}'
    """)

except Exception as e:
    msg = str(e).replace("'", "''")
    wm_to_log = last_wm if last_wm is not None else datetime(1900,1,1)
    spark.sql(f"""
      UPDATE {OPS["run_logs_table"]}
      SET finished_at = current_timestamp(),
          status = 'FAILED',
          error_msg = '{msg}',
          rows_in = {rows_in},
          rows_out = {rows_out},
          last_watermark_ts = TIMESTAMP '{wm_to_log.isoformat(sep=" ")}'
      WHERE pipeline_name = '{PIPELINE_NAME}' AND dataset = '{DATASET}' AND run_id = '{RUN_ID}'
    """)
    raise