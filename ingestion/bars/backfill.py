"""
Backfill daily bars: bronze for both vendors, then silver, then checks.

    python -m ingestion.bars.backfill                       # 5 years to the last completed session
    python -m ingestion.bars.backfill --start 2024-01-02 --end 2024-12-31

One ranged request per security per vendor (~160 calls for 5 years), not
Airflow's day-by-day backfill (~200k calls for the same data) -- see
docs/phase-2-design.md, decision 2. It runs the same commands the daily DAG
will, in the same order, so a backfill exercises the real pipeline.

The default end is the last NYSE session strictly before today (New York
time): today's bar isn't final until after the close, and the free Alpaca
tier refuses SIP data from the last 15 minutes.

Steps keep going after a failure so one run reports everything; the exit
code is 1 if any step failed. Every step is idempotent, so the fix for a
failed backfill is to rerun it.
"""

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

from ingestion.bars import build_silver, check_completeness, load_bronze, load_reorganizations

DEFAULT_YEARS = 5


def last_completed_session(today: date) -> date:
    """The last NYSE session strictly before `today` = the session on or before yesterday."""
    yesterday = (today - timedelta(days=1)).isoformat()
    return xcals.get_calendar("XNYS").date_to_session(yesterday, direction="previous").date()


def default_range(today: date, years: int = DEFAULT_YEARS) -> tuple[date, date]:
    end = last_completed_session(today)
    try:
        start = end.replace(year=end.year - years)
    except ValueError:  # 29 February
        start = end.replace(year=end.year - years, day=28)
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args(argv)

    today_ny = datetime.now(ZoneInfo("America/New_York")).date()
    default_start, default_end = default_range(today_ny)
    start, end = args.start or default_start, args.end or default_end
    run_id = f"backfill__{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    window = ["--start", start.isoformat(), "--end", end.isoformat()]
    print(f"Backfill {start}..{end}, run {run_id}")

    steps = [
        ("bronze alpaca", lambda: load_bronze.main(["--source", "alpaca", *window, "--run-id", run_id])),
        ("bronze yahoo", lambda: load_bronze.main(["--source", "yahoo", *window, "--run-id", run_id])),
        ("silver", lambda: build_silver.main(window)),
        ("reorganizations", lambda: load_reorganizations.main()),
        ("completeness", lambda: check_completeness.main(window)),
    ]
    failed = []
    for name, step in steps:
        print(f"\n== {name}")
        if step() != 0:
            failed.append(name)

    print(f"\nBackfill {start}..{end}: " + (f"FAILED steps: {', '.join(failed)}" if failed else "all steps succeeded"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
