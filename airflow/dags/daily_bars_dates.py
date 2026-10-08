"""
Which trading date a daily_bars run is responsible for.

Kept free of Airflow imports so the lakehouse test suite can test it
without Airflow installed (tests/test_dag_dates.py).

Three clocks are in play, and only one decides the date:

    Airflow's server   UTC -- how times are stored
    the developer      Europe/London -- only how the UI displays them
    the market         America/New_York -- what a "trading date" means

So the date is always the run time converted to New York, never the UTC or
London date. The DAG runs at 18:30 New York time, which is 22:30 or 23:30
UTC depending on US daylight saving; and the US and UK change clocks on
different Sundays, so New York's offset from London is 4 hours for a few
weeks a year instead of 5. Only a timezone-aware conversion is right on
every one of those days.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")

# Bars aren't final until after the 16:00 close. A run whose time is
# earlier than this (in New York) can't be about that same day's bar.
EARLIEST_HOUR_FOR_SAME_DAY = 17


def trade_date_for(param_date: str | None, logical_date: datetime | None, run_after: datetime) -> str:
    """The trading date (YYYY-MM-DD) a run should load.

    - An explicit `trade_date` param wins: manual reruns and backfill days.
    - Otherwise the run's time in New York. Manual runs in Airflow 3 may
      have no logical_date, so fall back to run_after.
    - A run timed before 17:00 New York without an explicit date is refused:
      it would fetch today's half-finished bar, whose close silver would
      later "revise" -- polluting revision_count with fake corrections.
    """
    if param_date:
        return date.fromisoformat(param_date).isoformat()

    when = (logical_date or run_after).astimezone(NEW_YORK)
    if when.hour < EARLIEST_HOUR_FOR_SAME_DAY:
        raise ValueError(
            f"Run time {when:%Y-%m-%d %H:%M} New York is before the day's bars are final. "
            "Trigger with an explicit trade_date, e.g. {\"trade_date\": \"2026-10-07\"}."
        )
    return when.date().isoformat()
