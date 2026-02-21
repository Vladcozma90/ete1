from __future__ import annotations
from typing import Iterable
from pyspark.sql import SparkSession
from src.utils.env import EnvConfig, load_envs
from utils.logger import setup_log
import logging

logger = logging.getLogger(__name__)

def _create_schemas(spark: SparkSession, env: EnvConfig, schemas: Iterable[str]) -> None:
    spark.sql(f"USE CATALOG {env.catalog}")
    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.{s}")
        logger.info("Ensured schemas exists: %s", f"{env.catalog}.{s}")


def bootstrap(spark: SparkSession, env:EnvConfig) -> dict[str, str]:
    schemas = [
        f"{env.project}_bronze",
        f"{env.project}_silver",
        f"{env.project}_gold",
        f"{env.project}_ops"
    ]
    _create_schemas(spark, env, schemas)


    ops_schema = f"{env.catalog}.{env.project}_ops"
    ops = {
        "run_logs_table" : f"{ops_schema}.run_logs",
        "state_table" : f"{ops_schema}.pipeline_state",
        "run_logs_path" : f"{env.ops_base_path}/{env.project}/run_logs",
        "state_path" : f"{env.ops_base_path}/{env.project}_ops/pipeline_state"
    }
    logger.info("Creating/validating OPS tables in schema: %s", ops_schema)    

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ops["run_logs_table"]} (
                pipeline_name STRING,
                dataset STRING,
                target_table STRING,
                run_id STRING,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status STRING,
                rows_in BIGINT,
                rows_quarantined BIGINT,
                rows_out BIGINT,
                error_msg STRING,
                dq_result STRING,
                last_watermark_ts TIMESTAMP
            )
            USING DELTA
            LOCATION '{ops["run_logs_path"]}'
            """)
    logger.info("Ensured table exists: %s", ops["run_logs_table"])

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ops["state_table"]} (
                pipeline_name STRING,
                dataset STRING,
                last_watermark_ts TIMESTAMP,
                updated_at TIMESTAMP,
                updated_by_run_id STRING    
            )
            USING DELTA
            LOCATION '{ops["state_path"]}'
            """)
    logger.info("Ensured table exists: %s", ops["state_table"])

    for t in (ops["run_logs_table"], ops["state_table"]):
        spark.sql(f"""
                    ALTER TABLE {t}
                    SET TBLPROPRIETIES(
                    delta.autoOptimize.optimizeWrite = 'true',
                    delta.autoOptimize.autoCompact = 'true'
                    )
                """)
        logger.info("Applied Delta optimize proprieties: %s", t)

    logger.info("Bootstrap complete.")
    return ops


def main() -> None:
    env = load_envs()
    setup_log(env.logger_level)
    logger.info("Starting bootstrap with ENV=%s, catalog=%s, dataset=%s", env.catalog, env.catalog, env.project)
    spark = SparkSession.builder.getOrCreate()
    
    bootstrap(spark, env)

if __name__ == "__main__":
    main()





