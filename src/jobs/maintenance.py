from pyspark.sql import SparkSession
from src.utils.env import load_envs

spark = SparkSession.builder.getOrCreate()
env = load_envs()

dbutils.widgets.text("do_optimize", "true")
dbutils.widgets.text("do_vacuum", "true")
dbutils.widgets.text("vacuum_hours", "168")

DO_OPTIMIZE = dbutils.widgets.get("do_optimize").strip().lower() == "true"
DO_VACUUM = dbutils.widgets.get("do_vacuum").strip().lower() == "true"
VACUUM_HOURS = int(dbutils.widgets.get("vacuum_hours").strip())

TABLES = {
    "fact_orders_asof": {
        "name": f"{env.catalog}.{env.dataset}_gold.fact_orders_asof",
        "optimize": True,
        "vacuum": True,
        "strategy": "liquid",
        "zorder_cols": []
    },
    "conform_orders": {
        "name": f"{env.catalog}.{env.dataset}_silver.conform_orders",
        "optimize": True,
        "vacuum": True,
        "strategy": "liquid",
        "zorder_cols": []
    },
}

def _optimize(table: str, strategy: str, zcols: list[str]):
    if strategy == "zorder" and zcols:
        cols = ", ".join(zcols)
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {table}")

def _vacuum(table: str, hours: int):
    spark.sql(f"VACUUM {table} RETAIN {hours} HOURS")

failures = []

for key, cfg in TABLES.items():
    table = cfg["name"]
    optimize_flag = bool(cfg.get("optimize", False))
    vacuum_flag = bool(cfg.get("vacuum", False))
    strategy = cfg.get("strategy", "none")
    zcols = cfg.get("zorder_cols", [])

    try:
        if DO_OPTIMIZE and optimize_flag:
            _optimize(table, strategy, zcols)

        if DO_VACUUM and vacuum_flag:
            _vacuum(table, VACUUM_HOURS)

    except Exception as e:
        failures.append(f"{key} | {table} | {type(e).__name__}: {e}")

if failures:
    raise RuntimeError("Maintenance failures:\n" + "\n".join(failures))