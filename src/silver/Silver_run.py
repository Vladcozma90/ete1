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

def run_silver(selected_entities: Optional[list[str]] = None) -> None:

    spark = _get_spark()
    env = load_envs()

    setup_log(env.logger_level)
    dq_scope = env.dq_scope
    execution_map = _execution_map()

    if selected_entities is None:
        entities = env.allowed_entities
    else:
        entities = selected_entities

    invalid = [e for e in entities if e not in execution_map]
    if invalid:
        raise ValueError(f"Invalid silver entities requested: {invalid}. Expected: {execution_map.keys()}")
    
    logger.info("Starting SILVER layer")
    logger.info("Entities to run %s", entities)

    try:

        for entity in entities:
            logger.info("Running silver pipeline %s", entity)
            execution_map[entity](spark, env, dq_scope)
            logger.info("Finished silver pipeline: %s", entity)

        logger.info("SILVER layer completed successfully.")
    
    except Exception:
        logger.exception("SILVER layer FAILED.")
        raise

    finally:
        spark.stop()

if __name__ == '__main__':
    run_silver()