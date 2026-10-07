"""
SCD Type 2 behaviour of the security master, on a real local Delta table.
Each test gets its own table under pytest's tmp_path.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

pytestmark = pytest.mark.spark

MRSH, GOOGL = "BBG000BP4MH0", "BBG009S39JX6"


def at(day):
    return datetime(2026, 10, day, 12, tzinfo=timezone.utc)


@pytest.fixture
def table(tmp_path):
    return str(tmp_path / "security_master")


@pytest.fixture
def write(spark, table):
    from ingestion.security_master_table import write_security_master

    return lambda rows, when: write_security_master(spark, pd.DataFrame(rows), table, when)


def history(spark, table, security_id):
    return (
        spark.read.format("delta").load(table).where(f"security_id = '{security_id}'").orderBy("valid_from").collect()
    )


def test_first_load_inserts_open_versions(spark, table, write, make_row):
    counts = write([make_row(MRSH, "MMC"), make_row(GOOGL, "GOOGL")], at(1))
    assert counts == {"new": 2, "changed": 0, "unchanged": 0}
    assert all(r.is_current and r.valid_to is None for r in history(spark, table, MRSH))


def test_rerunning_the_same_snapshot_is_a_no_op(spark, table, write, make_row):
    rows = [make_row(MRSH, "MMC"), make_row(GOOGL, "GOOGL")]
    write(rows, at(1))
    assert write(rows, at(2)) == {"new": 0, "changed": 0, "unchanged": 2}
    assert spark.read.format("delta").load(table).count() == 2


def test_ticker_change_closes_old_version_and_opens_new(spark, table, write, make_row):
    write([make_row(MRSH, "MMC"), make_row(GOOGL, "GOOGL")], at(1))
    counts = write([make_row(MRSH, "MRSH"), make_row(GOOGL, "GOOGL")], at(3))
    assert counts == {"new": 0, "changed": 1, "unchanged": 1}

    old, new = history(spark, table, MRSH)
    assert (old.exchange_ticker, old.is_current, new.exchange_ticker, new.is_current) == ("MMC", False, "MRSH", True)
    # Half-open intervals: the old version ends exactly where the new one starts.
    assert old.valid_to == new.valid_from


def test_as_of_returns_what_was_known_then(spark, table, write, make_row):
    from ingestion import scd2

    write([make_row(MRSH, "MMC")], at(1))
    write([make_row(MRSH, "MRSH")], at(3))

    def ticker_as_of(ts):
        return [r.exchange_ticker for r in scd2.as_of(spark, table, ts).collect()]

    assert ticker_as_of(at(2)) == ["MMC"]
    assert ticker_as_of(at(3)) == ["MRSH"]  # boundary belongs to the new version
    assert ticker_as_of("2026-09-30") == []  # before the first observation


def test_exactly_one_open_version_per_security(spark, table, write, make_row):
    from ingestion import scd2

    for day, ticker in [(1, "MMC"), (2, "MRSH"), (3, "MRSH2"), (4, "MRSH2")]:
        write([make_row(MRSH, ticker)], at(day))
    assert scd2.current(spark, table).count() == 1
    assert len(history(spark, table, MRSH)) == 3


def test_missing_from_snapshot_is_not_closed(spark, table, write, make_row):
    """Absence usually means a failed fetch, not a delisting."""
    write([make_row(MRSH, "MRSH"), make_row(GOOGL, "GOOGL")], at(1))
    write([make_row(MRSH, "MRSH")], at(2))
    assert history(spark, table, GOOGL)[0].is_current


def test_null_join_key_is_rejected(table, write, make_row):
    with pytest.raises(Exception):
        write([make_row(MRSH, "MRSH", issuer_id=None)], at(1))


def test_inverted_interval_is_rejected_and_nothing_is_written(spark, table, write, make_row):
    write([make_row(MRSH, "MRSH")], at(3))
    with pytest.raises(Exception, match="valid_interval"):
        write([make_row(MRSH, "XYZ")], at(2))  # clock went backwards
    assert [r.exchange_ticker for r in history(spark, table, MRSH)] == ["MRSH"]


def test_old_schema_table_is_refused_with_instructions(spark, table):
    from ingestion import scd2
    from ingestion.security_master_table import TABLE_SCHEMA

    spark.createDataFrame([("CIK0000062709", "MMC")], "canonical_id string, sec_ticker string").write.format(
        "delta"
    ).save(table)
    with pytest.raises(ValueError, match="different schema"):
        scd2.ensure_table(spark, table, TABLE_SCHEMA)
