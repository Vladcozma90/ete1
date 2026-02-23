import uuid
import logging
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.utils.env import load_envs, EnvConfig
from src.utils.logger import setup_log

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig, dataset: str) -> dict[str, str]:
    
    return {
        "table" : f"{env.catalog}.{env.project}_bronze.{dataset}",
        "src_path" : f"{env.landing_base_path}/{env.project}/{dataset}",
        "tgt_path" : f"{env.raw_base_path}/{env.project}/{dataset}",
        "checkpoint_path" : f"{env.checkpoint_base_path}/{env.project}/{dataset}",
        "schema_loc" : f"{env.checkpoint_base_path}/{env.project}/{dataset}",
        "run_logs_table" : f"{env.catalog}.{env.project}_ops.run_logs"
    }

def run_ingestion(spark: SparkSession, env: EnvConfig, dataset: str) -> None:

    allowed = {"customers", "products", "orders"}

    if dataset not in allowed:
        raise ValueError(f"Unknown dataset: {dataset}. allowed entities: {allowed}")

    cfg = _build_config(env, dataset)

    pipeline_name = f"bronze_{dataset}"
    run_id = str(uuid.uuid4())

    logger.info("Starting bronze ingestion | dataset=%s | run_id=%s", dataset, run_id)

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (
                pipeline_name, dataset, target_table, run_id, started_at, status)
                Values ('{pipeline_name}', '{dataset}', '{cfg["table"]}', '{run_id}', current_timestamp(), 'RUNNING')
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
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        
        logger.info("Bronze ingestion SUCCESS | dataset=%s", dataset)
    
    except Exception as e:
        logger.exception("Bronze ingest FAILED | dataset=%s", dataset)
        msg = str(e).replace("'", "''")
        spark.sql(f"""
                    UPDATE {run_logs}
                    SET finished_at = current_timestamp(),
                        status = 'FAILED',
                        error_msg = '{msg}'
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}', AND run_id = '{run_id}'
                """)
        raise

def main() -> None:

    spark = SparkSession.builder.getOrCreate()
    env = load_envs()
    setup_log(env.logger_level)
    datasets = env.allowed_datasets

    try:

        for dataset in datasets:
            logger.info("Running bronze pipeline %s", dataset)
            run_ingestion(spark, env, dataset)
            logger.info("Finished bronze pipeline: %s", dataset)

        logger.info("BRONZE layer completed successfully.")

    except Exception:
        logger.exception("BRONZE layer FAILED.")
        raise

    finally:
        spark.stop()


if __name__ == '__main__':
    main()