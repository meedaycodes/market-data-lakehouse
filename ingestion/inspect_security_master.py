"""
Quick terminal check on the Delta table build_security_master.py writes.
Run after the build: `python -m ingestion.inspect_security_master`

For the guided version -- history, as-of queries, why each design choice was
made -- open notebooks/01_security_master_tour.ipynb in Jupyter.
"""

import sys

from ingestion import paths, scd2
from ingestion.spark import get_spark

DELTA_TABLE_PATH = paths.SECURITY_MASTER


def main() -> int:
    spark = get_spark("inspect-security-master")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        everything = spark.read.format("delta").load(DELTA_TABLE_PATH)
        current = scd2.current(spark, DELTA_TABLE_PATH)

        n_current = current.count()
        n_securities = current.select("security_id").distinct().count()
        print(f"Versions (all history): {everything.count()}")
        print(f"Current securities:     {n_current}")
        print(f"Distinct issuers:       {current.select('issuer_id').distinct().count()}")

        print("\nSample current rows:")
        current.select("security_id", "issuer_id", "exchange_ticker", "yfinance_symbol", "alpaca_symbol").orderBy(
            "exchange_ticker"
        ).show(10, truncate=False)

        print("Securities whose sources disagree on the ticker spelling:")
        current.where("ticker_mismatch").select(
            "exchange_ticker", "figi_ticker", "sec_ticker", "yfinance_symbol", "alpaca_symbol"
        ).show(truncate=False)

        print("Securities that have changed since the first build:")
        changed = everything.groupBy("security_id").count().where("count > 1").select("security_id")
        everything.join(changed, "security_id").select(
            "security_id", "exchange_ticker", "valid_from", "valid_to", "is_current"
        ).orderBy("security_id", "valid_from").show(truncate=False)

        # The SCD2 invariant. A MERGE is atomic, so this should never fire;
        # if it does, something wrote to the table outside merge_snapshot.
        if n_current != n_securities:
            print(f"BROKEN: {n_current} open versions for {n_securities} securities", file=sys.stderr)
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
