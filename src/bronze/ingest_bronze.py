import os
import uuid
import logging
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.utils.env import load_envs, EnvConfig
from src.utils.logger import setup_log

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig, entity: str) -> dict[str, str]:

    allowed = {"customers", "products", "orders"}

    if entity not in allowed:
        raise ValueError(f"Unknown ENTITY: {entity}")
    
    return {
        "table" : f"{env.catalog}.{env.dataset}_bronze.{entity}",
        "src_path" : f"{env.landing_base_path}/{env.dataset}/{entity}",
        "tgt_path" : f"{env.raw_base_path}/{env.dataset}/{entity}",
        "checkpoint_path" : f"{env.checkpoint_base_path}/{env.dataset}/{entity}",
        "schema_loc" : f"{env.checkpoint_base_path}/{env.dataset}/{entity}",
    }

def run_ingestion(spark: SparkSession, env: EnvConfig, entity: str) -> None:

    cfg = _build_config(env, entity)

    pipeline_name = f"bronze_{entity}"
    run_id = str(uuid.uuid4())
    run_logs = f"{env.catalog}.{env.dataset}_ops.run_logs"

    logger.info("Starting bronze ingestion | entity=%s | run_id=%s", entity, run_id)

    spark.sql(f"""
                INSERT INTO {run_logs} (
                pipeline_name, dataset, target_table, run_id, started_at, status)
                Values ('{pipeline_name}', '{entity}', '{cfg["table"]}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    try:

        stream_df = (spark.readStream
                     .format("cloudfiles")
                     .option("cloudfiles.format", "parquet")
                     .option("cloudFiles.schemaLocation", cfg["schema_loc"])
                     .load(cfg["src_path"])
                     .drop("_rescued_data")
                     )
        
        stream_df = (stream_df
                     .withColumn("_ingest_ts", current_timestamp())
                     .withColumn("_ingest_date", current_date())
                     .withColumn("_file_name", input_file_name())
                     .withColumn("_source", lit(cfg["src_path"]))
                     )
        
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["table"]}
                    USING DELTA
                    LOCATION '{cfg["tgt_path"]}'
                """)
        
        query = (stream_df.writeStream
                 .format("delta")
                 .partitionBy("_ingest_date")
                 .option("checkpointLocation", cfg["checkpoint_path"])
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .start()
                 )
        query.awaitTermination()

        rows_in = int(query.lastProgress or {}).get("numInputRows", 0)

        spark.sql(f"""
                    UPDATE {run_logs}
                    SET finished_at = current_timestamp(),
                        status = 'SUCCESS',
                        rows_in = {rows_in},
                        rows_out = {rows_in},
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{entity}' AND run_id = '{run_id}'
                """)
        
        logger.info("Bronze ingestion SUCCESS | entity=%s", entity)
    
    except Exception as e:
        logger.exception("Bronze ingest FAILED | entity=%s", entity)
        msg = str(e).replace("'", "''")
        spark.sql(f"""
                    UPDATE {run_logs}
                    SET finished_at = current_timestamp(),
                        status = 'FAILED',
                        error_msg = '{msg}'
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{entity}', AND run_id = '{run_id}'
                """)
        raise

def main() -> None:

    spark = SparkSession.builder.getOrCreate()

    env = load_envs()
    setup_log(env.logger_level)

    entity = os.getenv("ENTITY") # set in Airflow
    if not entity:
        raise ValueError("ENTITY environmentt veriable not set in Airflow")

    run_ingestion(spark, env, entity.strip().lower())


if __name__ == '__main__':
    main()