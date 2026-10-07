"""
Fetch daily bars from each vendor, returning responses as received.

Nothing here parses prices. Each function returns one RawResponse per HTTP
response (Alpaca) or library call (Yahoo), with the body untouched, so bronze
stores what the vendor actually said. Parsing happens later, in silver, where
a bug can be fixed and replayed without calling the vendor again.

One request per security, not one request for all 80. Alpaca's API accepts
many symbols per call, but then a single response holds 80 securities and
can't be tagged with one security_id; at 80 requests a day (and ~80 for the
whole 5-year backfill) the extra calls cost nothing that matters.

Date ranges are trading dates, inclusive at both ends, interpreted in New
York time -- the exchange's calendar, not UTC's.
"""

import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests

NEW_YORK = ZoneInfo("America/New_York")

ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
ALPACA_PAGE_LIMIT = 10_000  # API maximum; ~40 years of daily bars, so one page in practice
ALPACA_REQUEST_INTERVAL = 0.35  # free tier allows 200 requests/minute


@dataclass(frozen=True)
class Security:
    security_id: str
    vendor_symbol: str


@dataclass
class RawResponse:
    source: str
    security_id: str
    vendor_symbol: str
    request_start: date
    request_end: date
    request_params: dict
    payload: str
    record_count: int
    page: int = 0


@dataclass
class FetchResult:
    responses: list[RawResponse] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)  # security_id -> error


def new_york_day_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """[first instant of `start`, last second of `end`] in New York time.

    Alpaca stamps a daily bar at New York midnight (04:00 or 05:00 UTC).
    Bounds built from UTC midnight would cut that off: "end = 2024-06-07" as
    UTC midnight excludes the 2024-06-07 bar stamped 04:00 UTC the same day.
    """
    first = datetime.combine(start, datetime.min.time(), NEW_YORK)
    last = datetime.combine(end + timedelta(days=1), datetime.min.time(), NEW_YORK) - timedelta(seconds=1)
    return first, last


def fetch_alpaca(securities: list[Security], start: date, end: date, session=None) -> FetchResult:
    """Raw Alpaca bars, straight from the REST API.

    Called directly rather than through the alpaca-py SDK because the SDK
    turns the response into Python objects -- a transformation, and bronze
    should hold the bytes Alpaca sent.

    feed=sip and adjustment=raw are set on every request, never defaulted:
    the IEX feed only goes back to 2020-07-27 and covers one exchange, and
    any adjustment rewrites past prices whenever a split happens.
    """
    session = session or requests.Session()
    headers = {
        "APCA-API-KEY-ID": os.environ["ALPACA_API_KEY"],
        "APCA-API-SECRET-KEY": os.environ["ALPACA_SECRET_KEY"],
    }
    first, last = new_york_day_bounds(start, end)
    result = FetchResult()

    for n, sec in enumerate(securities):
        if n:
            time.sleep(ALPACA_REQUEST_INTERVAL)
        params = {
            "symbols": sec.vendor_symbol,
            "timeframe": "1Day",
            "start": first.isoformat(),
            "end": last.isoformat(),
            "feed": "sip",
            "adjustment": "raw",
            "limit": ALPACA_PAGE_LIMIT,
        }
        pages: list[RawResponse] = []
        try:
            page_token = None
            while True:
                query = {**params, **({"page_token": page_token} if page_token else {})}
                resp = session.get(ALPACA_BARS_URL, params=query, headers=headers, timeout=30)
                resp.raise_for_status()
                body = resp.json()
                pages.append(
                    RawResponse(
                        source="alpaca",
                        security_id=sec.security_id,
                        vendor_symbol=sec.vendor_symbol,
                        request_start=start,
                        request_end=end,
                        request_params=query,
                        payload=resp.text,
                        record_count=len((body.get("bars") or {}).get(sec.vendor_symbol) or []),
                        page=len(pages),
                    )
                )
                page_token = body.get("next_page_token")
                if not page_token:
                    break
        except Exception as exc:
            # All pages or none: half a response would look like a short
            # history rather than a failure.
            result.failures[sec.security_id] = f"{type(exc).__name__}: {exc}"
            continue
        result.responses.extend(pages)
    return result


def fetch_yahoo(securities: list[Security], start: date, end: date) -> FetchResult:
    """Yahoo bars via yfinance, serialized without loss.

    Yahoo has no official API, so there are no "raw bytes" we can rely on --
    yfinance's output is the contract. It's stored whole (every column, the
    timezone-aware index) with the library version, so if a yfinance upgrade
    changes what it returns, bronze shows exactly when.

    auto_adjust=False keeps both Close and Adj Close; actions=True adds the
    Dividends and Stock Splits columns, which Phase 5 needs. Note that
    Yahoo's Close is still split-adjusted (see docs/phase-2-design.md).
    """
    import yfinance as yf

    result = FetchResult()
    # yfinance's `end` is exclusive.
    params = {"start": start.isoformat(), "end": (end + timedelta(days=1)).isoformat(), "interval": "1d",
              "auto_adjust": False, "actions": True}
    for n, sec in enumerate(securities):
        if n:
            time.sleep(0.2)
        try:
            df = yf.Ticker(sec.vendor_symbol).history(**params, raise_errors=True)
        except Exception as exc:
            result.failures[sec.security_id] = f"{type(exc).__name__}: {exc}"
            continue
        result.responses.append(
            RawResponse(
                source="yahoo",
                security_id=sec.security_id,
                vendor_symbol=sec.vendor_symbol,
                request_start=start,
                request_end=end,
                request_params={**params, "yfinance_version": yf.__version__},
                payload=df.to_json(orient="split", date_format="iso", date_unit="s"),
                record_count=len(df),
            )
        )
    return result


def params_json(response: RawResponse) -> str:
    """Request parameters as stable JSON (sorted keys), for the bronze row."""
    return json.dumps(response.request_params, sort_keys=True, default=str)
