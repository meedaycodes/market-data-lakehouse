"""
Bronze bar tables: one per vendor, append-only, one row per response.

Bronze is the record of what each vendor said and when we asked. It is never
updated or deduplicated -- if the same day is fetched twice, both deliveries
stay, and the difference between them (a vendor correction) is evidence, not
noise. Silver does the deduplicating.

`record_count` is the one derived column: how many bars the payload holds,
so "did we get anything?" is answerable without parsing JSON.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

from ingestion import delta_tables
from ingestion.bars.fetch import RawResponse, params_json

BRONZE_SCHEMA = StructType(
    [
        StructField("source", StringType(), nullable=False),
        StructField("security_id", StringType(), nullable=False),
        StructField("vendor_symbol", StringType(), nullable=False),
        StructField("request_start", DateType(), nullable=False),
        StructField("request_end", DateType(), nullable=False),
        StructField("request_params", StringType(), nullable=False),
        StructField("page", IntegerType(), nullable=False),
        StructField("payload", StringType(), nullable=False),
        StructField("record_count", IntegerType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
    ]
)


def append_responses(
    spark: SparkSession, path: str, responses: list[RawResponse], ingested_at: datetime, run_id: str
) -> int:
    """Append one bronze row per response. Returns rows written.

    One append per run, written as one file. Spark writes a file per
    partition of the DataFrame, and a DataFrame built from a Python list is
    split across every local core -- so without coalesce(1), 80 rows landed
    as 10 files. At a run a day that's ~2,500 tiny files a year per table,
    and every read pays to open each one. A run's worth of bronze is a few
    hundred KB (the 5-year backfill a few MB), far below the size where one
    file becomes the bottleneck.
    """
    delta_tables.ensure_table(spark, path, BRONZE_SCHEMA)
    if not responses:
        return 0
    rows = [
        (
            r.source,
            r.security_id,
            r.vendor_symbol,
            r.request_start,
            r.request_end,
            params_json(r),
            r.page,
            r.payload,
            r.record_count,
            ingested_at,
            run_id,
        )
        for r in responses
    ]
    spark.createDataFrame(rows, schema=BRONZE_SCHEMA).coalesce(1).write.format("delta").mode("append").save(path)
    return len(rows)
