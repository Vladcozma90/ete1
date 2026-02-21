import logging
import uuid
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from src.utils.env import EnvConfig
from src.utils.watermark import get_last_watermark, upsert_watermark
from src.utils.transform_utils import norm_str

logger = logging.getLogger(__name__)

DQ_MAX_PK_NULL = 0.0
DQ_MAX_EMAIL_NULL = 0.05
DQ_MAX_EMAIL_INVALID = 0.05



def _build_config(env: EnvConfig) -> dict[str, str]:

    return {
        #Tables
        "run_logs_table" : f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table" : f"{env.catalog}.{env.project}_ops.pipeline_state",
        "stage_table" : f"{env.catalog}.{env.project}_silver.stage_customers",
        "current_table" : f"{env.catalog}.{env.project}_silver.current_customers",
        "conform_table" : f"{env.catalog}.{env.project}_silver.conform_customers",
        "quarantine_table" : f"{env.catalog}.{env.project}_silver.quarantine_customers",
        "dq_table" : f"{env.catalog}.{env.project}_silver.dq_customers",

        #Paths
        "bronze_path" : f"{env.raw_base_path}/{env.project}/customers",
        "stage_path" : f"{env.curated_base_path}/{env.project}/customers/stage_customers",
        "current_path" : f"{env.curated_base_path}/{env.project}/customers/current_customers",
        "conform_path" : f"{env.curated_base_path}/{env.project}/customers/conform_customers",
        "quarantine_path" : f"{env.curated_base_path}/{env.project}/customers/quarantine/bad_keys",
        "dq_path" : f"{env.curated_base_path}/{env.project}/customers/data_quality"
    }




def run_silver_customers(spark: SparkSession, env: EnvConfig, dq_scope: str = "batch") -> None:

    dq_scope = (dq_scope or "batch").strip().lower()
    if dq_scope not in ("batch", "current"):
        raise ValueError(f"dq_scope must be 'batch' or 'current'. Got: {dq_scope}")
    
    pipeline_name = "silver_customers"
    dataset = "customers"
    run_id = str(uuid.uuid4())

    pk = "customer_id"
    

    cfg = _build_config(env)

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (
                pipeline_name, dataset, target_table, run_id, started_at, status)
                VALUES ('{pipeline_name}', '{dataset}', '{cfg["conform_table"]}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    rows_in = 0
    rows_quarantined = 0
    rows_out = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    try:

        logger.info("Silver customers start | run_id=%s | dq_scope=%s", run_id, dq_scope)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_cols = [
        "customer_id", "email", "first_name", "last_name", "city", "state",
        "_ingest_ts", "_ingest_date", "_file_name", "_source"
        ]

        missing = [c for c in required_cols if c not in bronze_df.columns]

        if missing:
            raise ValueError(f"Bronze missing required cols: {missing}")
        
        last_wm = get_last_watermark(spark, cfg["state_table"], pipeline_name, dataset)
        
        incr_df = bronze_df.filter(col("_ingest_ts") > lit(last_wm))

        if incr_df.take(1) == []:
            spark.sql(f"""
                        UPDATE {cfg["run_logs_table"]}
                        SET finished_at = current_timestamp(),
                            status = 'SUCCESS',
                            dq_result = 'SKIPPED_NO_NEW_DATA',
                            rows_in = 0,
                            rows_quarantined = 0,
                            rows_out = 0,
                            last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                        WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                    """)
            logger.info("No new data. Exiting.")
            return
        
        new_wm = incr_df.agg(max(col("_ingest_ts")).alias("mx")).collect()[0]["mx"]

        # -------------------------
        # Stage (overwrite slice)
        # -------------------------

        stage_df = (incr_df
                    .withColumn(pk, col(pk).cast("string"))
                    .withColumn("email", lower(norm_str(col("email").cast("string"))))
                    .withColumn("first_name", norm_str(col("first_name").cast("string")))
                    .withColumn("last_name", norm_str(col("last_name").cast("string")))
                    .withColumn("city", norm_str(col("city").cast("string")))
                    .withColumn("state", upper(norm_str(col("state").cast("string"))))
                    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
                    .withColumn("domain", split(col("email"), "@").getItem(1).cast("string"))
                    .withColumn("_valid_email", regexp_like(col("email"), r"^[a-zA-Z0-9_.%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
                    .withColumn("etl_run_id", lit(run_id))
                    .withColumn("processed_ts", current_timestamp())
                    .withColumn("processed_date", current_date())
                    .withColumn("source_layer", lit("bronze"))
                    .drop("first_name", "last_name")
                    )
        
        (stage_df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(cfg["stage_path"])
         )
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["stage_table"]} USING DELTA LOCATION '{cfg["stage_path"]}'
                """)
        stage_df = spark.read.table(cfg["stage_table"])

        # -------------------------
        # Quarantine + dedup
        # -------------------------

        bad_records = (stage_df
                       .filter(col(pk).isNull())
                       .withColumn("_quarantine_reason", lit(f"{pk} is NULL"))
                       .withColumn("_quarantine_ts", current_timestamp())
                       )
        
        good_records = (stage_df.filter(col(pk).isNotNull()))

        (bad_records.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(cfg["quarantine_path"])
         )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg["quarantine_table"]} USING DELTA LOCATION '{cfg["quarantine_path"]}'")

        w = Window.partitionBy(pk).orderBy(col("_ingest_ts").desc(), col("processed_ts").desc())

        dedup = (good_records
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        rows_in = stage_df.count()
        rows_quarantined = bad_records.count()
        rows_out = dedup.count()

        # -------------------------
        # Current snapshot (SCD1)
        # -------------------------

        if not DeltaTable.isDeltaTable(spark, cfg["current_path"]):
            (dedup.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .save(cfg["current_path"])
             )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg["current_table"]} USING DELTA LOCATION '{cfg["current_path"]}'")

        current_dt = DeltaTable.forName(spark, cfg["current_table"])
        (current_dt.alias("t")
         .merge(dedup.alias("s"), f"t.{pk} = s.{pk}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )
        
        current_df = spark.read.table(cfg["current_table"])

        # -------------------------
        # DQ gate
        # -------------------------

        dq_source = stage_df if dq_scope == "batch" else current_df
        dq_source_table = cfg["stage_table"] if dq_scope == "batch" else cfg["current_table"]

        dq_agg = (dq_source.agg(
            count(lit(1)).alias("total_rows"),
            sum(when(col(pk).isNull(), 1).otherwise(0)).alias("pk_null"),
            sum(when(col("email").isNull(), 1).otherwise(0)).alias("email_null"),
            sum(when(col("email").isNotNull() & (~col("_valid_email")), 1).otherwise(0)).alias("invalid_email")
            )).collect()[0]
        
        total_rows = int(dq_agg["total_rows"])
        pk_null = int(dq_agg["pk_null"])
        email_null = int(dq_agg["email_null"])
        invalid_email = int(dq_agg["invalid_email"])

        pk_null_pct = (pk_null / total_rows) if total_rows else 0.0
        email_null_pct = (email_null / total_rows) if total_rows else 0.0
        invalid_email_pct = (invalid_email / total_rows) if total_rows else 0.0

        dq_result = "OK"
        if (pk_null_pct > DQ_MAX_PK_NULL) or (email_null_pct > DQ_MAX_EMAIL_NULL) or (invalid_email_pct > DQ_MAX_EMAIL_INVALID):
            dq_result = "FAIL"
        
        dq_df = spark.createDataFrame([(
            dq_source_table,
            run_id,
            datetime.utcnow(),
            dq_scope,
            total_rows,
            pk_null,
            email_null,
            invalid_email,
            float(pk_null_pct),
            float(email_null_pct),
            float(invalid_email_pct),
            dq_result
        )], 
            """
            table_name STRING,
            run_id STRING,
            dq_ts TIMESTAMP,
            dq_scope STRING,
            total_rows BIGINT,
            pk_null BIGINT,
            email_null BIGINT,
            invalid_email BIGINT,
            pk_null_pct DOUBLE,
            email_null_pct DOUBLE,
            invalid_email_pct DOUBLE,
            dq_result STRING
            """)
        
        (dq_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(cfg["dq_path"])
         )
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["dq_table"]}
                    USING DELTA
                    LOCATION '{cfg["dq_path"]}'
                """)

        if dq_result == "FAIL":
            raise ValueError(
                f"DQ FAIL ({dq_result})"
                f"pk_null_pct={pk_null_pct:.4f}"
                f"email_null_pct={email_null_pct:.4f}"
                f"email_invalid_pct={invalid_email_pct:.4f}"
            )
        
        # -------------------------
        # Conform (SCD2-style)
        # -------------------------

        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["conform_table"]} (
                    customer_sk BIGINT GENERATED BY DEFAULT AS IDENTITY,
                    customer_id STRING,
                    email STRING,
                    city STRING,
                    state STRING,
                    full_name STRING,
                    domain STRING,
                    _file_name STRING,
                    _source STRING,
                    record_hash STRING,
                    silver_effective_start_ts TIMESTAMP,
                    silver_effective_end_ts TIMESTAMP,
                    updated_at TIMESTAMP,
                    etl_run_id STRING,
                    is_current BOOLEAN,
                    source_layer STRING
                    )
                    USING DELTA
                    LOCATION '{cfg["conform_path"]}'
                """)

        conf_dt = DeltaTable.forName(spark, cfg["conform_table"])

        unknown = spark.range(1).select(
            lit(-1).cast("bigint").alias("customer_sk"),
            lit("unknown").alias("customer_id"),
            lit(None).cast("string").alias("email"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state"),
            lit("unknown customer name").cast("string").alias("full_name"),
            lit(None).cast("string").alias("domain"),
            lit(None).cast("string").alias("_file_name"),
            lit("system").cast("string").alias("_source"),
            sha2(lit("UNKNOWN"), 256).alias("record_hash"),
            lit(datetime(1900, 1, 1)).cast("timestamp").alias("silver_effective_start_ts"),
            lit(None).cast("timestamp").alias("silver_effective_end_ts"),
            current_timestamp().alias("updated_at"),
            lit(run_id).cast("string").alias("etl_run_id"),
            lit(True).alias("is_current"),
            lit("system").cast("string").alias("source_layer")            
        )

        (conf_dt.alias("t")
         .merge(unknown.alias("s"), "t.customer_sk = s.customer_sk")
         .whenNotMatchedInsertAll()
         .execute()
         )
        
        scd_cols = ["email", "city", "state", "full_name", "domain"]

        incoming = (
            dedup
            .withColumn("record_hash", sha2(concat_ws("||", *[coalesce(col(c), lit("")) for c in scd_cols]), 256))
            .withColumn("silver_effective_start_ts", col("_ingest_ts").cast("timestamp"))
            .select(
                "customer_id", "email", "city", "state", "full_name", "domain",
                "_file_name", "_source", "record_hash", "silver_effective_start_ts"
            )
        )

        conf_active = (
            conf_dt.toDF()
            .filter(col("is_current") == True)
            .select("customer_id", "record_hash")
        )

        joined = (
            incoming.alias("inc")
            .join(conf_active.alias("con"), on="customer_id", how="left")
        )

        changed = (joined
                   .filter(col("con.record_hash").isNotNull() & (col("con.record_hash") != col("inc.record_hash")))
                   .select("inc.*")
                   )
        
        new_recs = (joined
                    .filter(col("con.record_hash").isNull())
                    .select("inc.*")
                    )
        

        update_changed = changed.withColumn("merge_key", col(pk)).withColumn("scd_action", lit("UPDATE"))
        insert_changed = changed.withColumn("merge_key", lit(None).cast("string")).withColumn("scd_action", lit("INSERT"))
        insert_new_recs = new_recs.withColumn("merge_key", lit(None).cast("string")).withColumn("scd_action", lit("INSERT"))

        staged = (update_changed.unionByName(insert_changed).unionByName(insert_new_recs)
                  .withColumn("silver_effective_end_ts", lit(None).cast("timestamp"))
                  .withColumn("updated_at", current_timestamp())
                  .withColumn("etl_run_id", lit(run_id))
                  .withColumn("is_current", lit(True))
                  .withColumn("source_layer", lit("bronze"))
                 )
        
        (conf_dt.alias("t")
         .merge(staged.alias("s"), f"t.customer_id = s.merge_key AND t.is_current = true")
         .whenMatchedUpdate(
             condition= "t.record_hash <> s.record_hash",
             set={
                 "silver_effective_end_ts" : "s.silver_effective_start_ts",
                 "is_current" : "false",
                 "updated_at" : "current_timestamp()",
                 "etl_run_id" : "s.etl_run_id",
             },
         )
         .whenNotMatchedInsert(
             condition= "s.scd_action = 'INSERT'",
             values={
                    "customer_id": "s.customer_id",
                    "email": "s.email",
                    "city": "s.city",
                    "state": "s.state",
                    "full_name": "s.full_name",
                    "domain": "s.domain",
                    "_file_name": "s._file_name",
                    "_source": "s._source",
                    "record_hash": "s.record_hash",
                    "silver_effective_start_ts": "s.silver_effective_start_ts",
                    "silver_effective_end_ts": "s.silver_effective_end_ts",
                    "updated_at": "s.updated_at",
                    "etl_run_id": "s.etl_run_id",
                    "is_current": "s.is_current",
                    "source_layer": "s.source_layer",
             },
         )
         .execute()
        )

        upsert_watermark(spark, cfg["state_table"], pipeline_name, dataset, new_wm, run_id)
        
        spark.sql(f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'SUCCESS',
                dq_result = '{dq_result}',
                rows_in = {rows_in},
                rows_out = {rows_out},
                rows_quarantined = {rows_quarantined},
                last_watermark_ts = TIMESTAMP '{new_wm.isoformat(sep=" ")}'
            WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
        """)

        logger.info("Silver customers SUCCESS | rows_in=%d rows_out=%d rows_quarantined=%d", rows_in, rows_out, rows_quarantined)
    
    except Exception as e:
        msg = str(e).replace("'", "''")
        wm_to_log = last_wm if last_wm is not None else datetime(1900, 1, 1)
        dq_to_log = dq_result if dq_result is not None else "ERROR"
        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp,
                        status = 'FAILED',
                        error_msg = '{msg}',
                        rows_in = {rows_in},
                        rows_quarantined = {rows_quarantined},
                        rows_out = {rows_out},
                        dq_result = '{dq_to_log}',
                        last_watermark_ts = TIMESTAMP {wm_to_log.isoformat(sep=" ")}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        
        logger.exception("Silver customers FAILED")
        raise