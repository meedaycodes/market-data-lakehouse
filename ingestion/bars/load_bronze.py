"""
Load one vendor's daily bars for a date range into bronze.

    python -m ingestion.bars.load_bronze --source alpaca --start 2026-10-06 --end 2026-10-06
    python -m ingestion.bars.load_bronze --source yahoo  --start 2021-10-01 --end 2026-10-06   # backfill

The same command serves the daily Airflow task (start = end = the run's
trading date) and the 5-year backfill (a wide range). Airflow passes its
run_id so every bronze row can be traced to the run that wrote it.

Which securities: the open versions in the security master. The fetch is
driven by security_id, so every bronze row is tagged with the stable key at
the moment it's written -- no ticker join needed downstream.

Exit codes: 0 = every security fetched (or the range had no trading
sessions); 1 = some securities failed (the rest were still written).
"""

import argparse
import sys
from datetime import date, datetime, timezone

import exchange_calendars as xcals

from ingestion import paths
from ingestion.bars.bronze import append_responses
from ingestion.bars.fetch import Security, fetch_alpaca, fetch_yahoo
from reference_data.symbology import to_yahoo

FETCHERS = {"alpaca": fetch_alpaca, "yahoo": fetch_yahoo}


def trading_sessions(start: date, end: date) -> int:
    """NYSE sessions in [start, end]. NYSE and Nasdaq share a holiday calendar."""
    return len(xcals.get_calendar("XNYS").sessions_in_range(start.isoformat(), end.isoformat()))


def securities_for(source: str, master) -> list[Security]:
    """Each vendor's spelling of each security's current ticker.

    Alpaca: its own symbol from the security master (falls back to the
    exchange ticker if Alpaca's lookup failed). Yahoo: derived, since Yahoo
    only differs by separator.
    """
    out = []
    for row in master.itertuples():
        if source == "alpaca":
            symbol = row.alpaca_symbol if isinstance(row.alpaca_symbol, str) else row.exchange_ticker
        else:
            symbol = to_yahoo(row.exchange_ticker)
        out.append(Security(row.security_id, symbol))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=sorted(FETCHERS), required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--run-id", default=None, help="defaults to manual__<UTC timestamp>")
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("--end is before --start")

    ingested_at = datetime.now(timezone.utc)
    run_id = args.run_id or f"manual__{ingested_at.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    # A holiday or weekend is a successful run with nothing to fetch, not a
    # failure -- otherwise Airflow would page someone every Thanksgiving.
    sessions = trading_sessions(args.start, args.end)
    if sessions == 0:
        print(f"No NYSE sessions between {args.start} and {args.end}; nothing to fetch.")
        return 0

    from ingestion.security_master_table import read_current
    from ingestion.spark import get_spark

    spark = get_spark(f"bronze-{args.source}")
    try:
        master = read_current(spark, paths.SECURITY_MASTER)
        if master is None or master.empty:
            print("Security master is empty -- run ingestion.build_security_master first.", file=sys.stderr)
            return 1

        securities = securities_for(args.source, master)
        result = FETCHERS[args.source](securities, args.start, args.end)
        written = append_responses(spark, paths.BRONZE_BARS[args.source], result.responses, ingested_at, run_id)
    finally:
        spark.stop()

    bars = sum(r.record_count for r in result.responses)
    print(
        f"{args.source}: {args.start}..{args.end} ({sessions} sessions), run {run_id}\n"
        f"  {len(securities) - len(result.failures)}/{len(securities)} securities fetched, "
        f"{bars} bars in {written} bronze rows"
    )
    if result.failures:
        print("\nFAILED -- not written:", file=sys.stderr)
        for security_id, error in result.failures.items():
            print(f"  {security_id}: {error[:200]}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
