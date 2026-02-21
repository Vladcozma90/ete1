from pyspark.sql import SparkSession
from datetime import datetime

def get_last_watermark(
        spark: SparkSession,
        state_table: str,
        pipeline_name: str,
        dataset: str
        ) -> datetime:
    
    row = spark.sql(f"""
                    SELECT last_watermark_ts
                    FROM {state_table}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}'
                    ORDER BY updated_at DESC
                    LIMIT 1                    
                    """).collect()
    if (not row) or row[0]["last_watermark_ts"] is None:
        return datetime(1900, 1, 1)
    return row[0]["last_watermark_ts"]


def upsert_watermark(
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
                TIMESTAMP '{wm_to_log}' AS watermark_ts,
                '{run_id}' AS updated_by_run_id
            ) s
            ON t.pipeline_name = s.pipeline_name AND t.dataset = s.dataset
            WHEN MATCHED THEN UPDATE SET
                t.watermark_ts = s.watermark_ts,
                t.updated_by_run_id = s.updated_by_run_id,
                t.updated_at = current_timestamp()
            WHEN NOT MATCHED INSERT (pipeline_name, dataset, last_watermark_ts, updated_at, updated_by_run_id)
            VALUES (s.pipeline_name, s.dataset, s.last_watermark_ts, current_timestamp(), s.updated_by_run_id)
            """)

