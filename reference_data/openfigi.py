"""
Minimal client for OpenFIGI's mapping API (https://www.openfigi.com/api).

Shared by the universe helper (`resolve_figis.py`) and the security master
build, so batching, rate limiting and retries are written once.

Which FIGI we use, and why: OpenFIGI hands back three levels for a stock --

    share-class FIGI   one per share class, worldwide (BRK/B everywhere)
    composite FIGI     one per share class per country (BRK/B in the US)
    exchange FIGI      one per listing venue (BRK/B on NYSE, on IEX, ...)

The composite is the right key for a US-only security master: a trade
printed on any US venue is the same security to us, but a London listing of
the same shares would trade in a different currency and needs its own row.
Querying with exchCode "US" returns exactly that composite.
"""

import os
import time

import requests

from reference_data.symbology import to_figi

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# OpenFIGI's published limits: without a key, 25 requests/minute and 10 jobs
# per request; with a free key, 25 requests per 6 seconds and 100 jobs per
# request. Spacing requests evenly is simpler than tracking a window.
_LIMITS = {False: (10, 60 / 25), True: (100, 6 / 25)}
_MAX_RETRIES = 5


def ticker_job(exchange_ticker: str) -> dict:
    """A mapping job for a US-listed equity, by ticker."""
    return {
        "idType": "TICKER",
        "idValue": to_figi(exchange_ticker),
        "exchCode": "US",
        "marketSecDes": "Equity",
    }


def figi_job(figi: str) -> dict:
    """A mapping job that looks a FIGI back up (to get its current ticker)."""
    return {"idType": "ID_BB_GLOBAL", "idValue": figi}


def map_jobs(jobs: list[dict], api_key: str | None = None) -> list[dict]:
    """Run mapping jobs; returns one result per job, in the same order.

    Each result is OpenFIGI's own shape: {"data": [...]} on a match,
    {"warning": "No identifier found."} on no match, {"error": ...} on a bad
    job. Callers decide what zero or several matches mean -- this function
    only moves data.
    """
    api_key = api_key if api_key is not None else os.getenv("OPENFIGI_API_KEY") or None
    batch_size, interval = _LIMITS[bool(api_key)]
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    results: list[dict] = []
    for start in range(0, len(jobs), batch_size):
        if start:
            time.sleep(interval)
        batch = jobs[start : start + batch_size]
        results.extend(_post_with_retry(batch, headers, interval))
    return results


def _post_with_retry(batch: list[dict], headers: dict, interval: float) -> list[dict]:
    backoff = max(interval, 1.0) * 4
    for attempt in range(_MAX_RETRIES):
        resp = requests.post(OPENFIGI_URL, json=batch, headers=headers, timeout=30)
        # 429 = over the rate limit, 5xx = their problem; both are worth waiting
        # out. Any other error means the request itself is wrong, and retrying
        # it would just fail the same way more slowly.
        if resp.status_code == 429 or resp.status_code >= 500:
            time.sleep(backoff * 2**attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    raise RuntimeError(f"OpenFIGI still failing after {_MAX_RETRIES} attempts")
