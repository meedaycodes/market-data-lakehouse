"""
The security master's table definition, and reading/writing it.

The schema is written out, not inferred. The first version cast every column
to string because inferring types from a frame full of nulls (failed
lookups) is unreliable -- but all-strings pushed the problem downstream:
`ticker_mismatch == "True"` as a string compare, a CIK that sorts "100" before
"20". An explicit schema is a contract: Delta rejects a write that breaks it,
so a bad type fails here, loudly, instead of in a Phase 5 join.

NOT NULL marks what a row can't exist without. security_id and issuer_id
are the join keys -- a null one means the row can never join to anything,
which is exactly what quarantine exists to stop. Vendor columns stay
nullable: a vendor not knowing a security is a fact worth recording.
"""

from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from ingestion import scd2

KEY = "security_id"

ATTRIBUTE_SCHEMA = StructType(
    [
        StructField("security_id", StringType(), nullable=False),  # composite FIGI
        StructField("issuer_id", StringType(), nullable=False),  # CIK, zero-padded: CIK0001652044
        StructField("cik", LongType(), nullable=False),
        StructField("share_class_figi", StringType()),
        StructField("exchange_ticker", StringType(), nullable=False),  # canonical spelling: BRK.B
        StructField("security_name", StringType()),
        StructField("security_type", StringType()),
        StructField("figi_ticker", StringType()),
        StructField("sec_ticker", StringType()),
        StructField("sec_company_name", StringType()),
        StructField("yfinance_symbol", StringType()),
        StructField("yfinance_name", StringType()),
        StructField("yfinance_exchange", StringType()),
        StructField("yfinance_sector", StringType()),
        StructField("alpaca_asset_id", StringType()),
        StructField("alpaca_symbol", StringType()),
        StructField("alpaca_exchange", StringType()),
        StructField("alpaca_tradable", BooleanType()),
        StructField("ticker_mismatch", BooleanType(), nullable=False),
    ]
)

ATTRIBUTE_COLUMNS = ATTRIBUTE_SCHEMA.fieldNames()
TABLE_SCHEMA = scd2.with_scd2_fields(ATTRIBUTE_SCHEMA)


def read_current(spark: SparkSession, path: str) -> pd.DataFrame | None:
    """Open versions as pandas (80 rows), or None before the first build."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return scd2.current(spark, path).select(*ATTRIBUTE_COLUMNS).toPandas()


def write_security_master(spark: SparkSession, snapshot: pd.DataFrame, path: str, observed_at: datetime) -> dict:
    created = scd2.ensure_table(spark, path, TABLE_SCHEMA)

    # pandas NA/NaN -> None, numpy scalars -> Python ones, so Spark checks
    # the values against ATTRIBUTE_SCHEMA instead of guessing types.
    records = snapshot[ATTRIBUTE_COLUMNS].astype(object).where(snapshot[ATTRIBUTE_COLUMNS].notna(), None)
    df = spark.createDataFrame(records.to_dict("records"), schema=ATTRIBUTE_SCHEMA)

    counts = scd2.merge_snapshot(spark, df, path, KEY, observed_at)
    print(
        f"\n{'Created' if created else 'Updated'} {path}: "
        f"{counts['new']} new, {counts['changed']} changed, {counts['unchanged']} unchanged"
    )
    return counts
