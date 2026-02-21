from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import date
from src.utils.env import load_envs

spark = SparkSession.builder.getOrCreate()
env = load_envs()


CONFIG = {
    
    "gold_dim_table" : f"{env.catalog}.{env.dataset}_gold.dim_date",
    "gold_dim_path" : f"{env.curated_base_path}/{env.dataset}/dim_date",
}

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CONFIG["gold_dim_table"]} (
            date_key INT,
            date DATE,
            year INT,
            quarter INT,
            month INT,
            day INT,
            day_of_week_num INT,
            day_of_week STRING,
            week_of_year INT,
            is_weekend BOOLEAN
            )
            USING DELTA
            LOCATION '{CONFIG["gold_dim_path"]}'
        """)

START_DATE = "2020-01-01"
END_DATE = "2035-12-31"

date_df = (spark.range(1)
           .select(explode(sequence(to_date(lit(START_DATE)), to_date(lit(END_DATE)), expr("interval 1 day"))).alias("date"))
           .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
           .withColumn("year", year(col("date")))
           .withColumn("quarter", quarter(col("date")))
           .withColumn("month", month(col("month")))
           .withColumn("day", dayofmonth(col("date")))
           .withColumn("day_of_week_num", dayofweek(col("date")))
           .withColumn("day_of_week", date_format(col("date"), "E"))
           .withColumn("week_of_year", weekofyear(col("date")))
           .withColumn("is_weekend", col("day_of_week_num").isin([1, 7]))
           .select(
               "date_key", "date", "year", "quarter", "month", "day",
               "day_of_week_num", "day_of_week", "week_of_year", "is_weekend")
           )

dt = DeltaTable.forName(spark, CONFIG["gold_dim_table"])

(dt.alias("t")
 .merge(date_df.alias("s"), "t.date_key = s.date_key")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)