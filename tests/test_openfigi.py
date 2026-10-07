"""The OpenFIGI client's batching and retry rules, with the HTTP call faked."""

import pytest

from reference_data import openfigi


class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(openfigi.time, "sleep", lambda s: None)


def fake_post(responses, calls):
    def _post(url, json, headers, timeout):
        calls.append(json)
        return responses.pop(0)(json)

    return _post


def ok(batch):
    return FakeResponse(200, [{"data": [{"compositeFIGI": job["idValue"]}]} for job in batch])


def test_batches_of_ten_without_a_key_and_order_preserved(monkeypatch):
    calls = []
    monkeypatch.setattr(openfigi.requests, "post", fake_post([ok, ok, ok], calls))
    jobs = [openfigi.figi_job(f"FIGI{i:02d}") for i in range(25)]

    results = openfigi.map_jobs(jobs, api_key="")
    assert [len(c) for c in calls] == [10, 10, 5]
    assert [r["data"][0]["compositeFIGI"] for r in results] == [j["idValue"] for j in jobs]


def test_retries_rate_limit_then_succeeds(monkeypatch):
    calls = []
    monkeypatch.setattr(openfigi.requests, "post", fake_post([lambda b: FakeResponse(429), ok], calls))
    results = openfigi.map_jobs([openfigi.figi_job("X")], api_key="")
    assert len(calls) == 2
    assert results[0]["data"][0]["compositeFIGI"] == "X"


def test_does_not_retry_a_bad_request(monkeypatch):
    calls = []
    monkeypatch.setattr(openfigi.requests, "post", fake_post([lambda b: FakeResponse(400), ok], calls))
    with pytest.raises(RuntimeError, match="400"):
        openfigi.map_jobs([openfigi.figi_job("X")], api_key="")
    assert len(calls) == 1


def test_ticker_job_uses_figi_spelling():
    assert openfigi.ticker_job("BRK.B")["idValue"] == "BRK/B"
