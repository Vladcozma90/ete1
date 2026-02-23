import logging
from typing import Optional
from pyspark.sql import SparkSession
from src.utils.env import load_envs
from src.utils.logger import setup_log
from src.silver.Silver_Customers import run_silver_customers
from src.silver.Silver_Products import run_silver_products
from src.silver.Silver_Orders import run_silver_orders

logger = logging.getLogger(__name__)

def _get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_layer_job")
        .getOrCreate()
    )

def _execution_map():
    return {
        "customers" : run_silver_customers,
        "products" : run_silver_products,
        "orders" : run_silver_orders
    }

def run_silver(selected_datasets: Optional[list[str]] = None) -> None:

    spark = _get_spark()
    env = load_envs()

    setup_log(env.logger_level)
    dq_scope = env.dq_scope
    execution_map = _execution_map()

    if selected_datasets is None:
        datasets = env.allowed_datasets
    else:
        datasets = selected_datasets

    invalid = [e for e in datasets if e not in execution_map]
    if invalid:
        raise ValueError(f"Invalid silver datasets requested: {invalid}. Expected: {execution_map.keys()}")
    
    logger.info("Starting SILVER layer")
    logger.info("datasets to run %s", datasets)

    try:

        for dataset in datasets:
            execution_map[dataset](spark, env, dq_scope)

        logger.info("SILVER layer completed successfully.")
    
    except Exception:
        logger.exception("SILVER layer FAILED.")
        raise

    finally:
        spark.stop()

if __name__ == '__main__':
    run_silver()