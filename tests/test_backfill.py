"""Backfill window and the reorganization reference data."""

from datetime import date, datetime, timezone

import pytest

from ingestion.bars.backfill import default_range, last_completed_session
from reference_data.reorganizations import REORGANIZATIONS
from reference_data.universe import UNIVERSE


@pytest.mark.parametrize(
    "today, expected",
    [
        (date(2026, 10, 7), date(2026, 10, 6)),  # Wednesday -> Tuesday
        (date(2026, 10, 5), date(2026, 10, 2)),  # Monday -> previous Friday
        (date(2026, 10, 4), date(2026, 10, 2)),  # Sunday -> Friday
        (date(2026, 11, 27), date(2026, 11, 25)),  # day after Thanksgiving -> Wednesday (26th is a holiday)
    ],
)
def test_last_completed_session_is_strictly_before_today(today, expected):
    assert last_completed_session(today) == expected


def test_default_range_is_five_years_to_last_session():
    assert default_range(date(2026, 10, 7)) == (date(2021, 10, 6), date(2026, 10, 6))


def test_every_reorganization_is_a_universe_security_with_sec_evidence():
    for r in REORGANIZATIONS:
        assert r["security_id"] in UNIVERSE, r
        assert UNIVERSE[r["security_id"]] == r["ticker"]
        assert r["form"] == "8-K12B" and r["accession"].count("-") == 2
        date.fromisoformat(r["effective_date"])


@pytest.mark.spark
def test_reorganizations_load_idempotently_with_filing_lineage(spark, tmp_path):
    from ingestion.bars import silver
    from ingestion.bars.load_reorganizations import reorganization_rows

    path = str(tmp_path / "corporate_actions")
    assert silver.merge_actions(spark, path, reorganization_rows(spark))["inserted"] == len(REORGANIZATIONS)
    assert silver.merge_actions(spark, path, reorganization_rows(spark)) == {"inserted": 0, "revised": 0}

    lin = spark.read.format("delta").load(path).where("security_id = 'BBG01FND0CC1'").first()
    assert (lin.action_type, lin.source, lin.value, lin.source_run_id) == ("reorg", "sec_edgar", None, "0001193125-23-055949")
    assert lin.ex_date == date(2023, 3, 1)
    # Lineage is when the fact became public: the filing's EDGAR acceptance time.
    assert lin.first_seen_at == datetime(2023, 3, 2, 0, 33, 18, tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
