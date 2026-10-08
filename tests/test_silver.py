"""
Silver parsing and latest-wins merging, on real local Delta tables.

Payloads are copied from real bronze rows (BRK.B on 2026-10-05), including
Yahoo's float noise, so the parsers are tested against what vendors send.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from ingestion.bars.fetch import RawResponse

pytestmark = pytest.mark.spark

BRK = "BBG000DWG505"
NVDA = "BBG000BBJQV0"

ALPACA_BRK = (
    '{"bars":{"BRK.B":[{"c":504.26,"h":505.88,"l":501.2994,"n":97247,"o":502.05,'
    '"t":"2026-10-05T04:00:00Z","v":3585081,"vw":504.353805}]},"next_page_token":null}'
)
YAHOO_BRK = (
    '{"columns":["Open","High","Low","Close","Adj Close","Volume","Dividends","Stock Splits"],'
    '"index":["2026-10-05T04:00:00Z"],'
    '"data":[[502.049987793,505.8800048828,501.299987793,504.2600097656,504.2600097656,3567500,0.0,0.0]]}'
)


def alpaca_payload(symbol, bars):
    return json.dumps({"bars": {symbol: bars}, "next_page_token": None})


def alpaca_bar(t, close, volume=1000):
    return {"t": t, "o": close, "h": close, "l": close, "c": close, "v": volume, "n": 10, "vw": close}


def yahoo_payload(rows):
    """rows: [(iso_t, close, dividends, splits)]"""
    cols = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]
    return json.dumps(
        {"columns": cols, "index": [r[0] for r in rows], "data": [[r[1], r[1], r[1], r[1], r[1], 1000, r[2], r[3]] for r in rows]}
    )


@pytest.fixture
def bronze(spark, tmp_path):
    """Write RawResponses through the real bronze writer; return a loader."""
    from ingestion.bars.bronze import append_responses

    counter = {"n": 0}

    def _load(source, security_id, symbol, payload, ingested_at):
        counter["n"] += 1
        path = str(tmp_path / f"bronze_{source}_{counter['n']}")
        resp = RawResponse(source, security_id, symbol, date(2026, 10, 5), date(2026, 10, 5), {}, payload, 1)
        append_responses(spark, path, [resp], ingested_at, f"run_{counter['n']}")
        return spark.read.format("delta").load(path)

    return _load


def at(day, hour=22):
    return datetime(2026, 10, day, hour, tzinfo=timezone.utc)


def collected(dt):
    """How PySpark hands `dt` back from .collect(): naive, in the MACHINE's local
    time zone -- not the session's UTC. So the same instant compares equal in a
    UTC container and fails on a laptop in London unless converted like this."""
    return dt.astimezone().replace(tzinfo=None)


# --- parsing -----------------------------------------------------------------


def test_alpaca_parses_to_exact_decimals_and_raw_basis(bronze):
    from ingestion.bars.silver import parse_alpaca

    (row,) = parse_alpaca(bronze("alpaca", BRK, "BRK.B", ALPACA_BRK, at(5))).collect()
    assert row.trade_date == date(2026, 10, 5)
    assert (row.open, row.high, row.low, row.close) == (
        Decimal("502.0500"), Decimal("505.8800"), Decimal("501.2994"), Decimal("504.2600"),
    )
    assert (row.volume, row.trade_count, row.vwap) == (3585081, 97247, Decimal("504.353805"))
    assert row.price_basis == "raw"


def test_yahoo_float_noise_becomes_exact_cents(bronze):
    from ingestion.bars.silver import parse_yahoo

    (row,) = parse_yahoo(bronze("yahoo", BRK, "BRK-B", YAHOO_BRK, at(5))).collect()
    assert row.close == Decimal("504.2600")  # payload says 504.2600097656
    assert row.price_basis == "vendor_adjusted"
    assert row.vwap is None and row.trade_count is None


@pytest.mark.parametrize(
    "stamp, expected",
    [
        ("2024-06-07T04:00:00Z", date(2024, 6, 7)),  # summer midnight NY
        ("2024-01-05T05:00:00Z", date(2024, 1, 5)),  # winter midnight NY
        ("2024-06-08T02:00:00Z", date(2024, 6, 7)),  # 22:00 NY on the 7th -- the UTC date would say the 8th
    ],
)
def test_trade_date_is_the_new_york_date(bronze, stamp, expected):
    from ingestion.bars.silver import parse_alpaca

    payload = alpaca_payload("NVDA", [alpaca_bar(stamp, 100.0)])
    (row,) = parse_alpaca(bronze("alpaca", NVDA, "NVDA", payload, at(5))).collect()
    assert row.trade_date == expected


def test_yahoo_actions_keep_only_real_events(bronze):
    from ingestion.bars.silver import parse_yahoo_actions

    payload = yahoo_payload(
        [
            ("2024-06-06T04:00:00Z", 1209.98, 0.0, 0.0),
            ("2024-06-10T04:00:00Z", 121.79, 0.0, 10.0),  # NVDA's 10-for-1
            ("2024-06-11T04:00:00Z", 120.91, 0.01, 0.0),
        ]
    )
    rows = parse_yahoo_actions(bronze("yahoo", NVDA, "NVDA", payload, at(5))).collect()
    assert sorted((r.ex_date, r.action_type, r.value) for r in rows) == [
        (date(2024, 6, 10), "split", Decimal("10.000000")),
        (date(2024, 6, 11), "dividend", Decimal("0.010000")),
    ]


# --- latest wins ---------------------------------------------------------------


@pytest.fixture
def silver_path(tmp_path):
    return str(tmp_path / "silver_daily_bars")


def merge(spark, silver_path, bronze_df):
    from ingestion.bars.silver import merge_bars, parse_alpaca

    return merge_bars(spark, silver_path, parse_alpaca(bronze_df))


def nvda_delivery(bronze, close, ingested_at):
    return bronze("alpaca", NVDA, "NVDA", alpaca_payload("NVDA", [alpaca_bar("2026-10-05T04:00:00Z", close)]), ingested_at)


def only_row(spark, path):
    (row,) = spark.read.format("delta").load(path).collect()
    return row


def test_new_bar_is_inserted_with_lineage(spark, silver_path, bronze):
    assert merge(spark, silver_path, nvda_delivery(bronze, 238.90, at(5))) == {"inserted": 1, "revised": 0}
    row = only_row(spark, silver_path)
    assert (row.revision_count, row.first_seen_at, row.updated_at) == (0, collected(at(5)), collected(at(5)))


def test_identical_refetch_changes_nothing(spark, silver_path, bronze):
    merge(spark, silver_path, nvda_delivery(bronze, 238.90, at(5)))
    assert merge(spark, silver_path, nvda_delivery(bronze, 238.90, at(6))) == {"inserted": 0, "revised": 0}
    assert only_row(spark, silver_path).revision_count == 0


def test_vendor_correction_wins_and_is_counted(spark, silver_path, bronze):
    merge(spark, silver_path, nvda_delivery(bronze, 238.90, at(5)))
    assert merge(spark, silver_path, nvda_delivery(bronze, 238.95, at(6))) == {"inserted": 0, "revised": 1}
    row = only_row(spark, silver_path)
    assert row.close == Decimal("238.9500")
    assert row.revision_count == 1
    assert row.first_seen_at == collected(at(5))  # first sighting is kept
    assert row.updated_at == collected(at(6))


def test_older_delivery_never_overwrites_a_newer_one(spark, silver_path, bronze):
    merge(spark, silver_path, nvda_delivery(bronze, 238.95, at(6)))  # correction processed first
    assert merge(spark, silver_path, nvda_delivery(bronze, 238.90, at(5)))["revised"] == 0
    assert only_row(spark, silver_path).close == Decimal("238.9500")


def test_newest_of_several_deliveries_in_one_batch_wins(spark, silver_path, bronze):
    both = nvda_delivery(bronze, 238.90, at(5)).unionByName(nvda_delivery(bronze, 238.95, at(6)))
    assert merge(spark, silver_path, both) == {"inserted": 1, "revised": 0}
    assert only_row(spark, silver_path).close == Decimal("238.9500")


# --- completeness ----------------------------------------------------------------


def test_completeness_lists_exactly_the_missing_bars(spark, tmp_path, bronze, monkeypatch, make_row):
    import pandas as pd

    from ingestion import paths
    from ingestion.bars import check_completeness
    from ingestion.bars.silver import merge_bars, parse_alpaca, parse_yahoo
    from ingestion.security_master_table import write_security_master

    master, silver_path = str(tmp_path / "master"), str(tmp_path / "silver")
    monkeypatch.setattr(paths, "SECURITY_MASTER", master)
    monkeypatch.setattr(paths, "SILVER_DAILY_BARS", silver_path)
    write_security_master(spark, pd.DataFrame([make_row(BRK, "BRK.B"), make_row(NVDA, "NVDA")]), master, at(5))

    # BRK.B from both vendors; NVDA from Alpaca only.
    merge_bars(spark, silver_path, parse_alpaca(bronze("alpaca", BRK, "BRK.B", ALPACA_BRK, at(5))))
    merge_bars(spark, silver_path, parse_yahoo(bronze("yahoo", BRK, "BRK-B", YAHOO_BRK, at(5))))
    merge_bars(spark, silver_path, parse_alpaca(nvda_delivery(bronze, 238.90, at(5))))

    missing, sessions = check_completeness.find_missing(spark, date(2026, 10, 5), date(2026, 10, 5))
    assert sessions == 1
    assert [tuple(r) for r in missing.collect()] == [(NVDA, date(2026, 10, 5), "yahoo")]
