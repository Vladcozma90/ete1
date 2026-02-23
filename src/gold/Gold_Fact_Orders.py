import logging
import uuid
import os
from datetime import datetime
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

logger = logging.basicConfig(__name__)

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
        "silver_products_table" : f"{env.catalog}.{env.project}_silver.customer_products",
        "gold_fact_table" : f"{env.catalog}.{env.project}_gold.fact_orders_asof",
        
        # Paths
        "gold_fact_path" : f"{env.curated_base_path}/{env.project}/fact_orders_asof",
    }

