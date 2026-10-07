"""
Creating Delta tables from an explicit schema -- shared by every layer.

Tables are created up front with `DeltaTable.createIfNotExists`, never by the
first write. A table created by `df.write.save()` takes whatever schema that
first DataFrame happened to have, and Spark marks every column nullable on
the way; created from a StructType, NOT NULL columns become constraints
Delta enforces on every later write.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def ensure_table(
    spark: SparkSession, path: str, schema: StructType, constraints: dict[str, str] | None = None
) -> bool:
    """Create the table if it isn't there; True if created.

    An existing table must have exactly these columns. A mismatch stops the
    job with instructions instead of letting a write fail halfway, or worse,
    succeed against a table that means something else.
    """
    if DeltaTable.isDeltaTable(spark, path):
        existing = set(spark.read.format("delta").load(path).columns)
        expected = set(schema.fieldNames())
        if existing != expected:
            raise ValueError(
                f"Delta table at {path} has a different schema than this code writes.\n"
                f"  missing: {sorted(expected - existing)}\n  unexpected: {sorted(existing - expected)}\n"
                "If it's an old table with nothing worth keeping, delete the folder and rerun."
            )
        return False

    DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).execute()
    for name, expression in (constraints or {}).items():
        spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {name} CHECK ({expression})")
    return True
