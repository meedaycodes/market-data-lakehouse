"""
Latest-wins MERGE with revision tracking -- the silver-layer counterpart to
scd2.py, generic so every silver table uses the same rules.

SCD2 keeps every version because the security master's history is the
point. Silver bars keep only the current value: vendor corrections to end-of-
day bars are rare, and bronze already holds every delivery, so any past
state can be rebuilt from there. What silver does keep is the evidence that
something changed -- revision_count, and which delivery the value came from.

Rules, applied per key:

    new key                                    -> insert, revision_count 0
    newer delivery, different values           -> update, revision_count + 1
    newer delivery, same values                -> nothing (no file rewrite)
    older delivery than the one already used   -> nothing
    same input again                           -> nothing

"Newer" means the bronze delivery's ingested_at, never the time this job
runs. Every timestamp written here comes from the data, so rebuilding silver
from the same bronze produces the same table.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from ingestion import delta_tables
from ingestion.scd2 import hash_columns

LINEAGE_FIELDS = [
    StructField("value_hash", StringType(), nullable=False),
    StructField("source_ingested_at", TimestampType(), nullable=False),
    StructField("source_run_id", StringType(), nullable=False),
    StructField("first_seen_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("revision_count", IntegerType(), nullable=False),
]


def with_lineage_fields(data: StructType) -> StructType:
    return StructType(data.fields + LINEAGE_FIELDS)


def latest_per_key(updates: DataFrame, keys: list[str]) -> DataFrame:
    """One row per key: the one from the most recent bronze delivery.

    Ties on ingested_at (two rows for one key in one delivery) are broken by
    run_id so the choice is deterministic, never whichever row Spark saw
    first.
    """
    w = Window.partitionBy(*keys).orderBy(F.desc("source_ingested_at"), F.desc("source_run_id"))
    return updates.withColumn("_rank", F.row_number().over(w)).where("_rank = 1").drop("_rank")


def merge_latest(
    spark: SparkSession, path: str, schema: StructType, updates: DataFrame, keys: list[str], values: list[str]
) -> dict:
    """Merge `updates` (keys + values + source_ingested_at + source_run_id).

    Returns {"inserted": n, "revised": n}.
    """
    delta_tables.ensure_table(spark, path, schema)
    staged = latest_per_key(updates, keys).withColumn("value_hash", hash_columns(values))

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    data_cols = [*keys, *values, "value_hash", "source_ingested_at", "source_run_id"]

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(staged.alias("s"), on)
        .whenMatchedUpdate(
            condition="s.source_ingested_at > t.source_ingested_at AND s.value_hash <> t.value_hash",
            set={
                **{c: f"s.{c}" for c in [*values, "value_hash", "source_ingested_at", "source_run_id"]},
                "updated_at": "s.source_ingested_at",
                "revision_count": "t.revision_count + 1",
            },
        )
        .whenNotMatchedInsert(
            values={
                **{c: f"s.{c}" for c in data_cols},
                "first_seen_at": "s.source_ingested_at",
                "updated_at": "s.source_ingested_at",
                "revision_count": "0",
            }
        )
        .execute()
    )

    metrics = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()[0]
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "revised": int(metrics.get("numTargetRowsUpdated", 0)),
    }
