import uuid
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.utils.env import EnvConfig

logger = logging.getLogger(__name__)


DQ_MAX_PK_NULL = 0.0
DQ_MAX_PRICE_NULL = 0.0
DQ_MAX_PRICE_NEG = 0.0

def _build_config(env: EnvConfig) -> dict[str, str]:

    return {
        #Tables
        "run_logs_table" : f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table" : f"{env.catalog}.{env.project}_ops.pipeline_state",
        "stage_table" : f"{env.catalog}.{env.project}_silver.stage_products",
        "current_table" : f"{env.catalog}.{env.project}_silver.current_products",
        "conform_table" : f"{env.catalog}.{env.project}_silver.conform_products",
        "quarantine_table" : f"{env.catalog}.{env.project}_silver.quarantine_products",
        "dq_table" : f"{env.catalog}.{env.project}_silver.dq_products",

        #Paths
        "bronze_path" : f"{env.raw_base_path}/{env.project}/products",
        "stage_path" : f"{env.curated_base_path}/{env.project}/products/stage_products",
        "current_path" : f"{env.curated_base_path}/{env.project}/products/current_products",
        "conform_path" : f"{env.curated_base_path}/{env.project}/products/conform_products",
        "quarantine_path" : f"{env.curated_base_path}/{env.project}/products/quarantine/bad_keys",
        "dq_path" : f"{env.curated_base_path}/{env.project}/products/data_quality"
    }

def _norm_str(c):
    return when(trim(c) == "", lit(None)).otherwise(trim(c))

def _ensure_table_props(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
                )
            """)


def _get_last_watermark(spark: SparkSession, state_table: str, pipeline_name: str, dataset: str) -> datetime:
    rows = spark.sql(f"""
                    SELECT last_watermark_ts
                    FROM {state_table}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """).collect()
    
    if (not rows) or rows[0]["last_watermark_ts"] is None:
        return datetime(1900, 1, 1)
    return rows[0]["last_watermark_ts"]

def _upsert_watermark(
        spark: SparkSession,
        state_table: str,
        pipeline_name: str,
        dataset: str,
        new_wm: datetime,
        run_id: str
) -> None:
    wm_to_log = new_wm.isoformat(sep=" ")
    spark.sql(f"""
                MERGE INTO {state_table} t
                USING (
                    SELECT
                    '{pipeline_name}' AS pipeline_name,
                    '{dataset}' AS dataset,
                    TIMESTAMP '{wm_to_log}' AS last_watermark_ts,
                    '{run_id}' AS updated_by_run_id
                ) s
                ON t.pipeline_name = s.pipeline_name AND t.dataset = s.dataset
                WHEN MATCHED THEN UPDATED SET
                    t.last_watermark_ts = s.last_watermark_ts,
                    t.updated_at = current_timestamp(),
                    t.updated_by_run_id = s.updated_by_run_id
                WHEN NOT MATCHED THEN INSERT (pipeline_name, dataset, last_watermark_ts, updated_at, updated_by_run_id)
                VALUES(s.pipeline_name, s.dataset, s.last_watermark_ts, current_timestamp(), s.updated_by_run_id)
            """)




def run_silver_products(spark: SparkSession, env: EnvConfig, dq_scope: str = "batch") -> None:

    dq_scope = (dq_scope or "batch").strip().lower()

    if dq_scope not in ("batch", "current"):
        raise ValueError(f"dq_scope must be 'batch' or 'current'. Got: {dq_scope}")
    
    cfg = _build_config(env)

    pipeline_name = "silver_products"
    dataset = "products"
    target_table = cfg["conform_table"]
    run_id = str(uuid.uuid4())
    pk = "product_id"
    

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (pipeline_name, dataset, target_table, run_id, started_at, status)
                VALUES ('{pipeline_name}', '{dataset}', '{target_table}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    rows_in = 0
    rows_quarantined = 0
    rows_out = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    try:

        logger.info("Silver product started | run_id=%s | dq_scope=%s", run_id, dq_scope)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_cols = ["product_id", "product_name", "category", "brand", "price",
                       "_ingest_ts", "_ingest_date", "_file_name", "_source"]
        
        missing = [c for c in required_cols if c not in required_cols]

        if missing:
            raise ValueError(f"Bronze missing required cols: {missing}")
        
        last_wm = _get_last_watermark(spark, cfg["state_table"], pipeline_name, dataset)

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

        stage_df = (
            incr_df
            .withColumn(pk, col(pk).cast("string"))
            .withColumn("product_name", _norm_str(col("product_name").cast("string")))
            .withColumn("category", _norm_str(col("category").cast("string")))
            .withColumn("brand", _norm_str(col("brand").cast("string")))
            .withColumn("price", col("price").cast("double"))
            .withColumn("etl_run_id", lit(RUN_ID))
            .withColumn("processed_ts", current_timestamp())
            .withColumn("processed_date", current_date())
            .withColumn("source_layer", lit("bronze"))
            .drop("_rescued_data")
        )

        (stage_df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(cfg["stage_path"])
         )
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["stage_table"]}
                    USING DELTA
                    LOCATION '{cfg['stage_path']}'
                """)
        _ensure_table_props(spark, cfg["stage_table"])

        stage_df = spark.read.table(cfg["stage_table"])
        

        # -------------------------
        # Quarantine + dedup
        # -------------------------

        bad_keys = (stage_df.filter(col(pk).isNull())
                    .withColumn("_quarantine_reason", lit(f"{pk} is NULL"))
                    .withColumn("_quarantine_ts", current_timestamp())
                    )
        
        good_keys = stage_df.filter(col(pk).isNotNull())

        w = Window.partitionBy(col(pk)).orderBy(col("_ingest_ts"), col("processed_ts"))

        dedup = (good_keys
                 .withColumn("_rn", row_number().over(w))
                 .filter(col("_rn") == 1)
                 .drop("_rn")
                 )
        
        rows_in = stage_df.count()
        rows_quarantined = bad_keys.count()
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
        
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["current_table"]}
                    USING DELTA
                    LOCATION '{cfg["current_path"]}'
                """)
        _ensure_table_props(spark, cfg["current_table"])

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
        dq_table = cfg["stage_table"] if dq_scope == "batch" else cfg["current_table"]

        dq_agg = (
            dq_source.agg(
                count(lit(1)).alias("total_rows"),
                sum(when(col(pk).isNull(), 1).otherwise(0)).alias("pk_null"),
                sum(when(col("price").isNull(), 1).otherwise(0)).alias("price_null"),
                sum(when(col("price").isNotNull() & (col("price") < 0), 0).otherwise(1)).alias("price_neg"))
        ).collect()[0]

        total_rows = int(dq_agg["total_rows"])
        pk_null = int(dq_agg["pk_null"])
        price_null = int(dq_agg["price_null"])
        price_neg = int(dq_agg["price_neg"])

        pk_null_pct = (pk_null / total_rows) if total_rows else 0.0
        price_null_pct = (price_null / total_rows) if total_rows else 0.0
        price_neg_pct = (price_neg / total_rows) if total_rows else 0.0

        dq_result = "OK"
        if (pk_null_pct > DQ_MAX_PK_NULL) or (price_null_pct > DQ_MAX_PRICE_NULL) or (price_neg_pct > DQ_MAX_PRICE_NEG):
            dq_result = "FAIL"

        dq_df = spark.createDataFrame([(
            dq_table,
            run_id,
            datetime.utcnow(),
            total_rows,
            pk_null,
            price_null,
            price_neg,
            float(pk_null_pct),
            float(price_null_pct),
            float(price_neg_pct),
            dq_result
        )],
        """
        table_name STRING,
        run_id STRING,
        dq_ts TIMESTAMP,
        total_rows BIGINT,
        pk_null BIGINT,
        price_neg BIGINT,
        pk_null_pct DOUBLE,
        price_null_pct DOUBLE,
        price_neg_pct DOUBLE,
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
        _ensure_table_props(spark, cfg["dq_table"])

        if dq_result == "FAIL":
            raise ValueError(f"dq result ({dq_scope}) failed: "
                             f"pk_null_pct={pk_null_pct:.4f},"
                             f"price_null_pct={price_null_pct:.4f},"
                             f"price_neg_pct={price_neg_pct:.4f}"
                            )
        
        # -------------------------
        # Conform (SCD2-style)
        # -------------------------

        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["conform_table"]} (
                        product_sk BIGINT GENERATED BY DEFAULT AS IDENTITY,
                        product_id STRING,
                        product_name STRING,
                        category STRING,
                        brand STRING,
                        price DOUBLE
                        _file_name STRING,
                        _source STRING,
                        record_hash STRING,
                        silver_processed_start_ts TIMESTAMP,
                        silver_processed_end_ts TIMESTAMP,
                        updated_at TIMESTAMP,
                        etl_run_id STRING,
                        is_current BOOLEAN,
                        source_layer STRING
                    )
                    USING DELTA
                    LOCATION '{cfg["conform_path"]}'
                """)
        _ensure_table_props(spark, cfg["conform_table"])

        conf_dt = DeltaTable.forName(spark, cfg["conform_table"])
        
        unknown = (spark.range(1).select(
            lit(-1).cast("bigint").alias("product_sk"),
            lit("unknown customer").cast("string").alias("product_id"),
            lit(None).cast("string").alias("product_name"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("string").alias("brand"),
            lit(None).cast("double").alias("price"),
            lit(None).cast("string").alias("_file_name"),
            lit("system").cast("string").alias("_source"),
            sha2(lit("UNKNOWN"), 256).alias("record_hash"),
            lit(datetime(1900, 1, 1)).cast("timestamp").alias("silver_processed_start_ts"),
            lit(None).cast("timestamp").alias("silver_processed_end_ts"),
            current_timestamp().alias("updated_at"),
            lit(run_id).alias("etl_run_id"),
            lit(True).alias("is_current"),
            lit("system").cast("string").alias("source_layer"),
        ))

        (conf_dt.alias("t")
         .merge(unknown.alias("s"), "t.product_sk = s.product_sk")
         .whenNotMatchedInsertAll()
         .execute()
         )
        
        conf_active = (conf_dt
                       .toDF()
                       .filter(col("is_current") == True)
                       .select(col("product_id"), col("record_hash"))
                       )
        
        scd2_cols = ["product_name", "category", "brand", "price"]
        
        incoming = (dedup
                    .withColumn("record_hash", concat_ws("||", *[coalesce(col(c), lit("")) for c in scd2_cols]))
                    .withColumn("silver_effective_start_ts", col("_ingest_ts").cast("timestamp"))
                    .select("product_id", "product_name", "category", "brand",
                            "price", "_file_name", "_source", "record_hash",
                            "silver_effective_start_ts")
                    )
        


        joined = incoming.alias("inc").join(conf_active.alias("con"), on="product_id", how="left")

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
         .merge(staged.alias("s"), f"t.{pk} = s.merge_key AND t.record_hash <> s.record_hash")
         .whenMatchedUpdate(
             condition= "s.scd_action = 'UPDATE' AND t.is_current = true",
             set={
                 "silver_effective_end_ts" : "s.silver_effective_start_ts",
                 "updated_at" : "current_timestamp()",
                 "is_current" : "false",
                 "etl_run_id" : "s.etl_run_id"
                 })

         .whenNotMatchedInsert(
             condition= "s.scd_action = 'INSERT'",
             values={
             "product_id": "s.product_id",
             "product_name": "s.product_name",
             "category": "s.category",
             "brand": "s.brand",
             "price": "s.price",
             "_file_name": "s._file_name",
             "_source": "s._source",
             "record_hash": "s.record_hash",
             "silver_effective_start_ts": "s.silver_effective_start_ts",
             "silver_effective_end_ts": "s.silver_effective_end_ts",
             "updated_at": "s.updated_at",
             "etl_run_id": "s.etl_run_id",
             "is_current": "s.is_current",
             "source_layer": "s.source_layer"
            })
        .execute()
         )
        
        _upsert_watermark(spark, cfg["state_table"], pipeline_name, dataset, new_wm, run_id)

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

        logger.info("Silver products SUCCESS | rows_in=%d rows_out=%d rows_quarantined=%d", rows_in, rows_out, rows_quarantined)
        

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
        
        logger.exception("Silver products FAILED")
        raise
