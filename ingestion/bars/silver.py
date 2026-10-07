"""
Silver: parse bronze payloads into typed rows.

    silver/daily_bars         one row per (security_id, trade_date, source)
    silver/corporate_actions  one row per (security_id, ex_date, action_type, source)

Parsing happens in Spark (from_json), not pandas, so the same code handles a
day's 160 payloads and the 5-year backfill's ~200k bars.

Choices that make silver trustworthy downstream:

  - Prices are DECIMAL(18,4), not doubles. Yahoo hands over 504.2600097656
    for a $504.26 close (32-bit floats inside yfinance); doubles can't hold
    most decimal amounts exactly, so sums and equality checks drift. VWAP
    keeps 6 places because Alpaca reports it that precisely.
  - `price_basis` says what the prices are: 'raw' for Alpaca, but
    'split_adjusted' for Yahoo, whose "unadjusted" Close is still divided by
    later splits (docs/phase-2-design.md, decision 6). Stated in the data,
    not left to memory.
  - trade_date is the New York calendar date of the bar's timestamp, never
    the UTC date.
  - Vendors stay side by side (`source` is part of the key). Their
    disagreement is what Phase 4 measures.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from ingestion import latest_wins

PRICE = DecimalType(18, 4)
VWAP = DecimalType(18, 6)

BAR_KEYS = ["security_id", "trade_date", "source"]
BAR_VALUES = ["price_basis", "open", "high", "low", "close", "volume", "vwap", "trade_count"]

DAILY_BARS_SCHEMA = latest_wins.with_lineage_fields(
    StructType(
        [
            StructField("security_id", StringType(), nullable=False),
            StructField("trade_date", DateType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("price_basis", StringType(), nullable=False),
            StructField("open", PRICE, nullable=False),
            StructField("high", PRICE, nullable=False),
            StructField("low", PRICE, nullable=False),
            StructField("close", PRICE, nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("vwap", VWAP),  # Alpaca only
            StructField("trade_count", LongType()),  # Alpaca only
        ]
    )
)

ACTION_KEYS = ["security_id", "ex_date", "action_type", "source"]
ACTION_VALUES = ["value"]

CORPORATE_ACTIONS_SCHEMA = latest_wins.with_lineage_fields(
    StructType(
        [
            StructField("security_id", StringType(), nullable=False),
            StructField("ex_date", DateType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),  # 'dividend' | 'split'
            StructField("source", StringType(), nullable=False),
            # dividend: cash per share; split: ratio (10.0 = 10-for-1)
            StructField("value", DecimalType(18, 6), nullable=False),
        ]
    )
)

_ALPACA_BAR = StructType(
    [
        StructField("t", StringType()),
        StructField("o", DoubleType()),
        StructField("h", DoubleType()),
        StructField("l", DoubleType()),
        StructField("c", DoubleType()),
        StructField("v", DoubleType()),
        StructField("n", LongType()),
        StructField("vw", DoubleType()),
    ]
)
_ALPACA_BODY = StructType([StructField("bars", MapType(StringType(), ArrayType(_ALPACA_BAR)))])

# yfinance's DataFrame.to_json(orient="split")
_YAHOO_BODY = StructType(
    [
        StructField("columns", ArrayType(StringType())),
        StructField("index", ArrayType(StringType())),
        StructField("data", ArrayType(ArrayType(DoubleType()))),
    ]
)


def new_york_date(iso_utc: str) -> F.Column:
    """Calendar date in New York of an ISO-8601 UTC timestamp string."""
    return F.to_date(F.from_utc_timestamp(F.col(iso_utc).cast("timestamp"), "America/New_York"))


def _lineage(df: DataFrame) -> list[F.Column]:
    return [F.col("ingested_at").alias("source_ingested_at"), F.col("run_id").alias("source_run_id")]


def parse_alpaca(bronze: DataFrame) -> DataFrame:
    """Alpaca bronze rows -> one row per bar. Prices are raw (adjustment=raw)."""
    bars = bronze.withColumn("body", F.from_json("payload", _ALPACA_BODY)).select(
        "security_id",
        *_lineage(bronze),
        F.explode(F.element_at("body.bars", F.col("vendor_symbol"))).alias("b"),
    )
    return bars.select(
        "security_id",
        new_york_date("b.t").alias("trade_date"),
        F.lit("alpaca").alias("source"),
        F.lit("raw").alias("price_basis"),
        *(F.col(f"b.{src}").cast(PRICE).alias(dst) for src, dst in [("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")]),
        F.col("b.v").cast(LongType()).alias("volume"),
        F.col("b.vw").cast(VWAP).alias("vwap"),
        F.col("b.n").alias("trade_count"),
        "source_ingested_at",
        "source_run_id",
    )


def _yahoo_rows(bronze: DataFrame) -> DataFrame:
    """One row per (bronze row, index position), with a helper to read a column by name."""
    return bronze.withColumn("body", F.from_json("payload", _YAHOO_BODY)).select(
        "security_id",
        *_lineage(bronze),
        "body",
        F.posexplode("body.index").alias("i", "t"),
    )


def _yahoo_value(column: str) -> F.Column:
    # data[i][position of `column`], both 1-based in element_at. array_position
    # returns BIGINT and element_at wants INT, hence the cast.
    return F.expr(f"element_at(element_at(body.data, i + 1), CAST(array_position(body.columns, '{column}') AS INT))")


def parse_yahoo(bronze: DataFrame) -> DataFrame:
    """Yahoo bronze rows -> one row per bar. Prices are split-adjusted (Yahoo's Close)."""
    rows = _yahoo_rows(bronze)
    return rows.select(
        "security_id",
        new_york_date("t").alias("trade_date"),
        F.lit("yahoo").alias("source"),
        F.lit("split_adjusted").alias("price_basis"),
        *(_yahoo_value(c).cast(PRICE).alias(c.lower()) for c in ["Open", "High", "Low", "Close"]),
        _yahoo_value("Volume").cast(LongType()).alias("volume"),
        F.lit(None).cast(VWAP).alias("vwap"),
        F.lit(None).cast(LongType()).alias("trade_count"),
        "source_ingested_at",
        "source_run_id",
    )


def parse_yahoo_actions(bronze: DataFrame) -> DataFrame:
    """Non-zero Dividends / Stock Splits from Yahoo payloads."""
    rows = _yahoo_rows(bronze)
    actions = rows.select(
        "security_id",
        new_york_date("t").alias("ex_date"),
        "source_ingested_at",
        "source_run_id",
        F.explode(
            F.array(
                F.struct(F.lit("dividend").alias("action_type"), _yahoo_value("Dividends").alias("value")),
                F.struct(F.lit("split").alias("action_type"), _yahoo_value("Stock Splits").alias("value")),
            )
        ).alias("a"),
    )
    return actions.where("a.value IS NOT NULL AND a.value <> 0").select(
        "security_id",
        "ex_date",
        F.col("a.action_type").alias("action_type"),
        F.lit("yahoo").alias("source"),
        F.col("a.value").cast(DecimalType(18, 6)).alias("value"),
        "source_ingested_at",
        "source_run_id",
    )


PARSERS = {"alpaca": parse_alpaca, "yahoo": parse_yahoo}


def merge_bars(spark: SparkSession, path: str, bars: DataFrame) -> dict:
    return latest_wins.merge_latest(spark, path, DAILY_BARS_SCHEMA, bars, BAR_KEYS, BAR_VALUES)


def merge_actions(spark: SparkSession, path: str, actions: DataFrame) -> dict:
    return latest_wins.merge_latest(spark, path, CORPORATE_ACTIONS_SCHEMA, actions, ACTION_KEYS, ACTION_VALUES)
