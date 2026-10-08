"""
Is every bar that should be in silver actually there?

    python -m ingestion.bars.check_completeness --start 2026-10-05 --end 2026-10-05

Expected = every current security x every NYSE session in the range x every
source. Anything missing is listed and the run exits 1, so the Airflow DAG
stops here instead of handing gold a quietly incomplete day.

Why check against an expectation rather than "did the load succeed": a load
can succeed and still be short. A vendor can answer 200 OK with an empty
list for one symbol, and nothing upstream would notice.

Known limitation: "current securities" is today's universe. Over a long
backfill, a security that listed partway through will show as missing for
the dates before it existed -- real, but not a pipeline failure. The
backfill step has to account for that.
"""

import argparse
import sys
from datetime import date

import exchange_calendars as xcals
from pyspark.sql import functions as F

from ingestion import paths
from ingestion.bars.silver import PARSERS

SOURCES = sorted(PARSERS)


def find_missing(spark, start: date, end: date):
    """DataFrame of expected (security_id, trade_date, source) absent from silver."""
    from ingestion import scd2

    sessions = [s.date() for s in xcals.get_calendar("XNYS").sessions_in_range(start.isoformat(), end.isoformat())]
    securities = scd2.current(spark, paths.SECURITY_MASTER).select("security_id")
    expected = (
        securities.crossJoin(spark.createDataFrame([(d,) for d in sessions], "trade_date date"))
        .crossJoin(spark.createDataFrame([(s,) for s in SOURCES], "source string"))
    )
    actual = (
        spark.read.format("delta")
        .load(paths.SILVER_DAILY_BARS)
        .where(F.col("trade_date").between(F.lit(start), F.lit(end)))
        .select("security_id", "trade_date", "source")
    )
    return expected.join(actual, ["security_id", "trade_date", "source"], "left_anti"), len(sessions)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    from ingestion.spark import get_spark

    spark = get_spark("check-completeness")
    try:
        missing, n_sessions = find_missing(spark, args.start, args.end)
        if n_sessions == 0:
            print(f"No NYSE sessions between {args.start} and {args.end}; nothing expected.")
            return 0
        n_missing = missing.count()
        print(f"{args.start}..{args.end}: {n_sessions} sessions x {len(SOURCES)} sources -- {n_missing} bars missing")
        if n_missing:
            missing.groupBy("trade_date", "source").count().orderBy("trade_date", "source").show(50, truncate=False)
            missing.orderBy("trade_date", "source", "security_id").show(20, truncate=False)
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
