from __future__ import annotations

import uuid
import logging
from typing import Optional, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, input_file_name, lit

from src.utils.env import load_envs, EnvConfig
from src.utils.logger import setup_log

logger = logging.getLogger(__name__)


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_layer_job")
        .getOrCreate()
    )


def _build_config(env: EnvConfig, dataset: str) -> dict[str, str]:
    return {
        "table": f"{env.catalog}.{env.project}_bronze.{dataset}",
        "src_path": f"{env.landing_base_path}/{env.project}/{dataset}",
        "tgt_path": f"{env.raw_base_path}/{env.project}/{dataset}",
        "checkpoint_path": f"{env.checkpoint_base_path}/{env.project}/bronze/{dataset}/checkpoint",
        "schema_loc": f"{env.checkpoint_base_path}/{env.project}/bronze/{dataset}/schema",
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
    }


def run_ingestion(spark: SparkSession, env: EnvConfig, dataset: str) -> None:
    if dataset not in env.allowed_datasets:
        raise ValueError(
            f"Unknown dataset: {dataset}. Allowed: {env.allowed_datasets}"
        )

    cfg = _build_config(env, dataset)

    pipeline_name = f"bronze_{dataset}"
    run_id = str(uuid.uuid4())

    logger.info("Starting bronze ingestion | dataset=%s | run_id=%s", dataset, run_id)

    spark.sql(f"""
        INSERT INTO {cfg["run_logs_table"]}
        (pipeline_name, dataset, target_table, run_id, started_at, status)
        VALUES ('{pipeline_name}', '{dataset}', '{cfg["table"]}', '{run_id}', current_timestamp(), 'RUNNING')
    """)

    rows_in = 0

    try:
        stream_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", cfg["schema_loc"])
            .load(cfg["src_path"])
            .drop("_rescued_data")
        )

        enriched_df = (
            stream_df
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

        query = (
            enriched_df.writeStream
            .format("delta")
            .partitionBy("_ingest_date")
            .option("checkpointLocation", cfg["checkpoint_path"])
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .start(cfg["tgt_path"])
        )

        query.awaitTermination()

        progress: dict[str, Any] = query.lastProgress or {}
        rows_in = int(progress.get("numInputRows", 0))

        spark.sql(f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'SUCCESS',
                rows_in = {rows_in},
                rows_out = {rows_in}
            WHERE pipeline_name = '{pipeline_name}'
              AND dataset = '{dataset}'
              AND run_id = '{run_id}'
        """)

        logger.info("Bronze ingestion SUCCESS | dataset=%s | rows_in=%d", dataset, rows_in)

    except Exception as e:
        logger.exception("Bronze ingestion FAILED | dataset=%s", dataset)
        msg = str(e).replace("'", "''")

        spark.sql(f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'FAILED',
                error_msg = '{msg}',
                rows_in = {rows_in},
                rows_out = 0
            WHERE pipeline_name = '{pipeline_name}'
              AND dataset = '{dataset}'
              AND run_id = '{run_id}'
        """)
        raise


def run_bronze(selected_datasets: Optional[list[str]] = None) -> None:

    spark = _get_spark()
    env = load_envs()

    setup_log(env.logger_level)

    if selected_datasets is None:
        datasets = env.allowed_datasets
    else:
        datasets = selected_datasets

    invalid = [d for d in datasets if d not in env.allowed_datasets]
    if invalid:
        raise ValueError(
            f"Invalid bronze datasets requested: {invalid}. Allowed: {env.allowed_datasets}"
        )

    logger.info("Starting BRONZE layer")
    logger.info("Datasets to run: %s", datasets)

    try:
        for dataset in datasets:
            
            run_ingestion(spark, env, dataset)

        logger.info("BRONZE layer completed successfully.")

    except Exception:
        logger.exception("BRONZE layer FAILED.")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze()