from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime
from src.utils.env import load_envs
import uuid

spark = SparkSession.builder.getOrCreate()
env = load_envs()

CONFIG = {
    "silver_scd2_table" : f"{env.catalog}.{env.dataset}_silver.conform_products",

    "gold_dim_table" : f"{env.catalog}.{env.dataset}_gold.dim_products_hist",
    "gold_dim_path" : f"{env.curated_base_path}/{env.dataset}/products/dim_products_hist"
}

OPS = {
    "run_logs_table" : f"{env.catalog}.{env.dataset}_ops.run_logs",
    "state_table" : f"{env.catalog}.{env.dataset}_ops.pipeline_state",
}

PIPELINE_NAME = "gold_dim_products_hist"
DATASET = "dim_products_hist"
TARGET_TABLE = CONFIG["gold_dim_table"]
RUN_ID = str(uuid.uuid4())

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
                CREATE TABLE IF NOT EXISTS {CONFIG["gold_dim_table"]} (
                    product_sk BIGINT,
                    product_id STRING,
                    product_name STRING,
                    category STRING,
                    brand STRING,
                    price DOUBLE,
                    updated_at TIMESTAMP,
                    etl_run_id STRING,
                    silver_effective_start_ts TIMESTAMP,
                    silver_effective_end_ts TIMESTAMP,
                    gold_loaded_ts TIMESTAMP,
                    gold_loaded_date DATE,
                    source_layer STRING
                )
                USING DELTA
                LOCATION '{CONFIG["gold_dim_path"]}'
            """)
    
    silver_df = spark.read.table(CONFIG["silver_scd2_table"])

    last_wm = get_last_watermark(PIPELINE_NAME, DATASET)

    incr_silver = silver_df.filter(col("_updated_at") > lit(last_wm))

    if incr_silver.limit(1).count() == 0:
        spark.sql(f"""
                    UPDATE {OPS['run_logs_table']}
                    SET finished_at = current_timestamp(),
                        status = 'SUCCESS',
                        rows_in = 0,
                        rows_out = 0,
                        dq_result = 'SKIPPED_NO_NEW_DATA',
                        last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                    WHERE pipeline_name = '{PIPELINE_NAME}' AND dataset = '{DATASET}' AND run_id = '{RUN_ID}'
                """)
        dbutils.notebook.exit("No new customer dim history to publish.")
    
    new_wm = incr_silver.agg(max(col("updated_at")).alias("mx")).collect()[0]["mx"]


    gold_batch = (
        incr_silver.select(
            col("product_sk").cast("bigint").alias("product_sk"),
            "product_id",
            "product_name",
            "category",
            "brand",
            "price",
            "updated_at",
            "etl_run_id"
        )
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .withColumn("source_layer", lit("silver"))
    )

    gold_dt = DeltaTable.forName(spark, CONFIG["gold_dim_table"])

    (gold_dt.alias("t")
     .merge(gold_batch.alias("s"), "t.customer_sk = s.customer_sk")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

    metrics = (
        incr_silver.agg(count(lit(1)).alias("rows_in"))
        .crossJoin(gold_batch.agg(count(lit(1)).alias("rows_out")))
    )

    rows_in = int(metrics["rows_in"])
    rows_out = int(metrics["rows_out"])
    
    upsert_watermark(PIPELINE_NAME, DATASET, RUN_ID)

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