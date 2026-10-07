"""Bar fetchers, with the vendors faked. Shapes copied from real responses."""

import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from ingestion.bars import fetch
from ingestion.bars.fetch import Security, new_york_day_bounds

NVDA = Security("BBG000BBJQV0", "NVDA")
BRK = Security("BBG000DWG505", "BRK.B")


@pytest.fixture(autouse=True)
def alpaca_env(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)


# --- New York day bounds ---------------------------------------------------


@pytest.mark.parametrize(
    "day, bar_stamp_utc",
    [
        (date(2024, 6, 7), datetime(2024, 6, 7, 4, tzinfo=timezone.utc)),  # summer: EDT, UTC-4
        (date(2024, 1, 5), datetime(2024, 1, 5, 5, tzinfo=timezone.utc)),  # winter: EST, UTC-5
    ],
)
def test_single_day_bounds_contain_the_bar_alpaca_stamps(day, bar_stamp_utc):
    first, last = new_york_day_bounds(day, day)
    assert first <= bar_stamp_utc <= last


def test_bounds_exclude_the_next_days_bar():
    _, last = new_york_day_bounds(date(2024, 6, 7), date(2024, 6, 7))
    assert last < datetime(2024, 6, 8, 4, tzinfo=timezone.utc)


# --- Alpaca ------------------------------------------------------------------


class FakeResponse:
    def __init__(self, body, status=200):
        self.text = json.dumps(body)
        self.status_code = status
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    def get(self, url, params, headers, timeout):
        self.calls.append(params)
        return self.responder(params)


def bars_body(symbol, n, next_token=None):
    bar = {"t": "2024-06-07T04:00:00Z", "o": 1197.7, "h": 1216.9, "l": 1180.2, "c": 1208.88, "v": 41238580}
    return {"bars": {symbol: [bar] * n}, "next_page_token": next_token}


def test_alpaca_always_requests_sip_and_raw():
    session = FakeSession(lambda p: FakeResponse(bars_body(p["symbols"], 1)))
    fetch.fetch_alpaca([NVDA, BRK], date(2024, 6, 7), date(2024, 6, 7), session=session)
    assert all(c["feed"] == "sip" and c["adjustment"] == "raw" for c in session.calls)


def test_alpaca_payload_is_the_response_text_verbatim():
    session = FakeSession(lambda p: FakeResponse(bars_body("NVDA", 3)))
    result = fetch.fetch_alpaca([NVDA], date(2024, 6, 5), date(2024, 6, 7), session=session)
    (resp,) = result.responses
    assert resp.payload == json.dumps(bars_body("NVDA", 3))
    assert (resp.security_id, resp.record_count, resp.source) == (NVDA.security_id, 3, "alpaca")


def test_alpaca_follows_pagination_one_row_per_page():
    def responder(params):
        return FakeResponse(bars_body("NVDA", 2, next_token=None if params.get("page_token") else "tok1"))

    session = FakeSession(responder)
    result = fetch.fetch_alpaca([NVDA], date(2016, 1, 4), date(2024, 6, 7), session=session)
    assert [r.page for r in result.responses] == [0, 1]
    assert session.calls[1]["page_token"] == "tok1"


def test_alpaca_failure_is_reported_and_other_securities_continue():
    def responder(params):
        return FakeResponse({}, status=404) if params["symbols"] == "BRK.B" else FakeResponse(bars_body("NVDA", 1))

    result = fetch.fetch_alpaca([BRK, NVDA], date(2024, 6, 7), date(2024, 6, 7), session=FakeSession(responder))
    assert list(result.failures) == [BRK.security_id]
    assert [r.security_id for r in result.responses] == [NVDA.security_id]


def test_alpaca_failure_on_a_later_page_discards_the_earlier_pages():
    def responder(params):
        if params.get("page_token"):
            return FakeResponse({}, status=500)
        return FakeResponse(bars_body("NVDA", 2, next_token="tok1"))

    result = fetch.fetch_alpaca([NVDA], date(2016, 1, 4), date(2024, 6, 7), session=FakeSession(responder))
    assert result.responses == []  # half a history would look like a short one
    assert NVDA.security_id in result.failures


# --- Yahoo -------------------------------------------------------------------


class FakeTicker:
    calls = []

    def __init__(self, symbol):
        self.symbol = symbol

    def history(self, **kwargs):
        FakeTicker.calls.append((self.symbol, kwargs))
        if self.symbol == "FAIL":
            raise RuntimeError("possibly delisted")
        index = pd.DatetimeIndex(["2024-06-07"], tz="America/New_York", name="Date")
        return pd.DataFrame(
            {"Open": [119.77], "Close": [120.89], "Adj Close": [120.54], "Volume": [412385800],
             "Dividends": [0.0], "Stock Splits": [0.0]},
            index=index,
        )


@pytest.fixture
def fake_yfinance(monkeypatch):
    import yfinance

    FakeTicker.calls = []
    monkeypatch.setattr(yfinance, "Ticker", FakeTicker)


def test_yahoo_requests_unadjusted_with_actions_and_exclusive_end(fake_yfinance):
    fetch.fetch_yahoo([Security(BRK.security_id, "BRK-B")], date(2024, 6, 7), date(2024, 6, 7))
    (symbol, kwargs), = FakeTicker.calls
    assert symbol == "BRK-B"
    assert kwargs["auto_adjust"] is False and kwargs["actions"] is True
    assert (kwargs["start"], kwargs["end"]) == ("2024-06-07", "2024-06-08")  # end is exclusive in yfinance


def test_yahoo_payload_round_trips_every_column_and_records_library_version(fake_yfinance):
    import yfinance

    result = fetch.fetch_yahoo([NVDA], date(2024, 6, 7), date(2024, 6, 7))
    (resp,) = result.responses
    restored = pd.read_json(__import__("io").StringIO(resp.payload), orient="split")
    assert list(restored.columns) == ["Open", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]
    assert restored["Close"].iloc[0] == 120.89
    assert resp.request_params["yfinance_version"] == yfinance.__version__


def test_yahoo_failure_is_reported_not_raised(fake_yfinance):
    result = fetch.fetch_yahoo([Security("BBG_FAIL", "FAIL"), NVDA], date(2024, 6, 7), date(2024, 6, 7))
    assert list(result.failures) == ["BBG_FAIL"]
    assert len(result.responses) == 1
