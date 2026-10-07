"""
Slowly Changing Dimension Type 2 on Delta Lake -- generic, not specific to
the security master, so later phases can reuse it.

Type 1 overwrites a row when it changes: you only ever know the present.
Type 2 closes the old row and opens a new one, so every version survives:

    security_id   exchange_ticker  valid_from   valid_to     is_current
    BBG000BP4MH0  MMC              2026-01-05   2026-10-07   false
    BBG000BP4MH0  MRSH             2026-10-07   NULL         true

Intervals are half-open, [valid_from, valid_to): a row is the truth at time
t when valid_from <= t < valid_to (NULL valid_to = still true). Half-open
means consecutive versions share a boundary without overlapping or leaving
a gap, so an as-of query always finds exactly one row.

Why not just use Delta time travel? Two reasons. It answers "what did the
TABLE look like at commit N", which is when we wrote, not when the fact
changed -- backfill a year of history today and time travel puts all of it
at today. And VACUUM deletes the files old versions need, so it's a
retention window, not a history.

What valid_from means here, honestly: the time this pipeline OBSERVED the
change, not when it happened in the world. If MMC became MRSH on a Monday
and the build first ran Wednesday, history says Wednesday. Exact event dates
need a corporate-actions feed; until then this is a record of what we knew
and when -- which is still the right thing for point-in-time correctness,
because a model backtested on Tuesday couldn't have known either.

Securities missing from a snapshot are left as they are, not closed. A
missing row usually means a failed fetch, not a delisting; closing on
absence would turn every outage into fake history.
"""

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from ingestion import delta_tables

SCD2_FIELDS = [
    StructField("row_hash", StringType(), nullable=False),
    StructField("valid_from", TimestampType(), nullable=False),
    StructField("valid_to", TimestampType(), nullable=True),
    StructField("is_current", BooleanType(), nullable=False),
]


def with_scd2_fields(attributes: StructType) -> StructType:
    return StructType(attributes.fields + SCD2_FIELDS)


def hash_columns(columns: list[str]) -> Column:
    """One hash over the tracked columns, so "did anything change?" is a
    single comparison instead of N null-aware ones.

    Nulls get an explicit marker before joining: concat_ws silently skips
    nulls, so without it ("A", NULL, "B") and ("A", "B", NULL) would hash
    the same and a real change would go unrecorded.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in columns]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def ensure_table(spark: SparkSession, path: str, schema: StructType) -> bool:
    """Create an SCD2 table if it isn't there. True if created.

    The CHECK makes an inverted interval -- valid_to before valid_from, e.g.
    from a run with a wrong clock -- a failed write instead of a silently
    corrupt history.
    """
    return delta_tables.ensure_table(
        spark, path, schema, constraints={"valid_interval": "valid_to IS NULL OR valid_to > valid_from"}
    )


def merge_snapshot(spark: SparkSession, snapshot: DataFrame, path: str, key: str, observed_at: datetime) -> dict:
    """Apply one full snapshot to an SCD2 table in a single atomic MERGE.

    For each key in the snapshot:
      - new key                -> insert an open version
      - tracked columns changed -> close the open version, insert a new one
      - nothing changed        -> no-op (so rerunning a snapshot is safe)

    The trick: one MERGE can't both update a target row and insert a
    replacement for the same key, because a source row either matches or it
    doesn't. So each changed row is staged twice -- once with merge_key set
    (matches the open version, closes it) and once with merge_key NULL
    (matches nothing, gets inserted). Doing it in one MERGE matters: Delta
    commits it atomically, so no reader ever sees a security with zero open
    versions or two.
    """
    tracked = [c for c in snapshot.columns if c != key]
    staged = snapshot.withColumn("row_hash", hash_columns(tracked)).withColumn(
        "valid_from", F.lit(observed_at).cast("timestamp")
    )

    target = DeltaTable.forPath(spark, path)
    open_versions = target.toDF().where("is_current").select(key, F.col("row_hash").alias("_open_hash"))
    changed = staged.join(open_versions, key).where("row_hash <> _open_hash").drop("_open_hash")

    source = staged.withColumn("_merge_key", F.col(key)).unionByName(
        changed.withColumn("_merge_key", F.lit(None).cast(snapshot.schema[key].dataType))
    )

    insert_values = {c: f"s.{c}" for c in [key, *tracked, "row_hash", "valid_from"]}
    insert_values.update({"valid_to": "CAST(NULL AS TIMESTAMP)", "is_current": "true"})

    (
        target.alias("t")
        .merge(source.alias("s"), f"t.{key} = s._merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": "s.valid_from"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    metrics = target.history(1).select("operationMetrics").first()[0]
    closed = int(metrics.get("numTargetRowsUpdated", 0))
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    return {"new": inserted - closed, "changed": closed, "unchanged": snapshot.count() - inserted}


def current(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path).where("is_current")


def as_of(spark: SparkSession, path: str, ts: datetime | str) -> DataFrame:
    """The table as it was known at `ts` (UTC). Empty before the first observation."""
    t = F.lit(ts).cast("timestamp")
    return (
        spark.read.format("delta")
        .load(path)
        .where((F.col("valid_from") <= t) & (F.col("valid_to").isNull() | (t < F.col("valid_to"))))
    )
