from pyspark.sql.functions import when, trim, lit



def norm_str(c):
    return when(trim(c) == "", lit(None)).otherwise(trim(c))