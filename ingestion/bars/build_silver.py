"""
Build silver from bronze for a range of trading dates.

    python -m ingestion.bars.build_silver --start 2026-10-05 --end 2026-10-05

Reads every bronze delivery whose requested range overlaps [start, end],
parses it (silver.py), keeps bars whose trade_date falls in the range, and
merges them latest-wins (latest_wins.py) into silver/daily_bars. Yahoo
payloads also feed silver/corporate_actions.

Safe to rerun: merging the same bronze again changes nothing. To rebuild a
range from scratch after a parsing fix, just rerun it -- bronze is the
source of truth, silver is derived.
"""

import argparse
import os
import sys
from datetime import date

from pyspark.sql import functions as F

from ingestion import paths
from ingestion.bars import silver


def bronze_for_range(spark, source: str, start: date, end: date):
    path = paths.BRONZE_BARS[source]
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return None
    return (
        spark.read.format("delta")
        .load(path)
        .where((F.col("request_end") >= F.lit(start)) & (F.col("request_start") <= F.lit(end)))
    )


def in_range(df, date_col: str, start: date, end: date):
    return df.where(F.col(date_col).between(F.lit(start), F.lit(end)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("--end is before --start")

    from ingestion.spark import get_spark

    spark = get_spark("silver-bars")
    try:
        for source, parse in silver.PARSERS.items():
            bronze = bronze_for_range(spark, source, args.start, args.end)
            if bronze is None:
                print(f"{source}: no bronze table yet, skipped")
                continue

            bars = in_range(parse(bronze), "trade_date", args.start, args.end)
            counts = silver.merge_bars(spark, paths.SILVER_DAILY_BARS, bars)
            print(f"{source} bars:    {counts['inserted']} inserted, {counts['revised']} revised")

            if source == "yahoo":
                actions = in_range(silver.parse_yahoo_actions(bronze), "ex_date", args.start, args.end)
                counts = silver.merge_actions(spark, paths.SILVER_CORPORATE_ACTIONS, actions)
                print(f"{source} actions: {counts['inserted']} inserted, {counts['revised']} revised")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
