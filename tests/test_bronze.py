"""Bronze append behaviour on a real local Delta table."""

from datetime import date, datetime, timezone

import pytest

from ingestion.bars.fetch import RawResponse

pytestmark = pytest.mark.spark


def response(payload='{"bars": {}}', security_id="BBG000BBJQV0", count=0):
    return RawResponse(
        source="alpaca",
        security_id=security_id,
        vendor_symbol="NVDA",
        request_start=date(2024, 6, 7),
        request_end=date(2024, 6, 7),
        request_params={"feed": "sip", "adjustment": "raw"},
        payload=payload,
        record_count=count,
    )


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "alpaca_bars")


def test_redelivery_appends_instead_of_overwriting(spark, path):
    from ingestion.bars.bronze import append_responses

    t1, t2 = datetime(2024, 6, 7, 22, tzinfo=timezone.utc), datetime(2024, 6, 8, 22, tzinfo=timezone.utc)
    append_responses(spark, path, [response('{"v": 1}', count=1)], t1, "run_1")
    append_responses(spark, path, [response('{"v": 2}', count=1)], t2, "run_2")

    rows = spark.read.format("delta").load(path).orderBy("ingested_at").collect()
    assert [(r.run_id, r.payload) for r in rows] == [("run_1", '{"v": 1}'), ("run_2", '{"v": 2}')]


def test_request_params_are_stored_as_sorted_json(spark, path):
    from ingestion.bars.bronze import append_responses

    append_responses(spark, path, [response()], datetime.now(timezone.utc), "run_1")
    (row,) = spark.read.format("delta").load(path).collect()
    assert row.request_params == '{"adjustment": "raw", "feed": "sip"}'


def test_null_security_id_is_rejected(spark, path):
    from ingestion.bars.bronze import append_responses

    with pytest.raises(Exception):
        append_responses(spark, path, [response(security_id=None)], datetime.now(timezone.utc), "run_1")


def test_one_run_writes_one_file(spark, path):
    """Regression: 80 rows used to land as 10 files (one per local core)."""
    from ingestion.bars.bronze import append_responses

    responses = [response(security_id=f"BBG{i:09d}") for i in range(80)]
    append_responses(spark, path, responses, datetime.now(timezone.utc), "run_1")
    assert spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().numFiles == 1


def test_empty_run_creates_the_table_and_writes_nothing(spark, path):
    from ingestion.bars.bronze import append_responses

    assert append_responses(spark, path, [], datetime.now(timezone.utc), "run_1") == 0
    assert spark.read.format("delta").load(path).count() == 0
