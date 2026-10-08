"""
The daily_bars DAG's trading-date rule, tested without Airflow installed.

The cases are the ones where a UTC or London date would be wrong.
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "daily_bars_dates", Path(__file__).resolve().parent.parent / "airflow" / "dags" / "daily_bars_dates.py"
)
dates = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dates)


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "run_time, expected",
    [
        (utc(2026, 10, 7, 22, 30), "2026-10-07"),  # summer: 18:30 EDT = 22:30 UTC = 23:30 London
        (utc(2026, 1, 7, 23, 30), "2026-01-07"),  # winter: 18:30 EST = 23:30 UTC = 23:30 London
        (utc(2026, 3, 16, 22, 30), "2026-03-16"),  # US on summer time, UK not yet: 22:30 in London too
        (utc(2026, 10, 27, 22, 30), "2026-10-27"),  # UK back on winter time, US not yet
        (utc(2026, 10, 9, 0, 30), "2026-10-08"),  # 20:30 New York on the 8th -- the UTC date says the 9th
    ],
)
def test_trade_date_is_the_new_york_date_of_the_run(run_time, expected):
    assert dates.trade_date_for(None, run_time, run_time) == expected


def test_manual_run_without_logical_date_uses_run_after():
    assert dates.trade_date_for(None, None, utc(2026, 10, 7, 22, 30)) == "2026-10-07"


def test_explicit_trade_date_wins():
    assert dates.trade_date_for("2026-10-05", utc(2026, 10, 7, 22, 30), utc(2026, 10, 7, 22, 30)) == "2026-10-05"


def test_run_before_the_bars_are_final_is_refused():
    morning = utc(2026, 10, 7, 14, 0)  # 10:00 New York
    with pytest.raises(ValueError, match="explicit trade_date"):
        dates.trade_date_for(None, morning, morning)


def test_malformed_trade_date_is_rejected():
    with pytest.raises(ValueError):
        dates.trade_date_for("07/10/2026", None, utc(2026, 10, 7, 22, 30))
