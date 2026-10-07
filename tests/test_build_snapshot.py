"""
Reconciliation rules, tested on synthetic vendor frames shaped like the real
fetchers' output. No network, no Spark.
"""

import pandas as pd
import pytest

from ingestion.build_security_master import build_snapshot

BRK, GOOGL, GOOG, MRSH = "BBG000DWG505", "BBG009S39JX6", "BBG009S3NB30", "BBG000BP4MH0"


def figi_rows(*pairs):
    return pd.DataFrame(
        [{"security_id": sid, "figi_ticker": t.replace(".", "/"), "exchange_ticker": t} for sid, t in pairs]
    )


# SEC's real spellings and CIKs for these names.
SEC = pd.DataFrame(
    [
        {"match_key": "BRK.B", "issuer_id": "CIK0001067983", "cik": 1067983, "sec_ticker": "BRK-B", "sec_company_name": "BERKSHIRE HATHAWAY INC"},
        {"match_key": "GOOGL", "issuer_id": "CIK0001652044", "cik": 1652044, "sec_ticker": "GOOGL", "sec_company_name": "Alphabet Inc."},
        {"match_key": "GOOG", "issuer_id": "CIK0001652044", "cik": 1652044, "sec_ticker": "GOOG", "sec_company_name": "Alphabet Inc."},
        {"match_key": "MRSH", "issuer_id": "CIK0000062709", "cik": 62709, "sec_ticker": "MRSH", "sec_company_name": "MARSH & MCLENNAN"},
    ]
)
NO_VENDOR = pd.DataFrame(columns=["security_id"])


def yahoo(*pairs):
    return pd.DataFrame([{"security_id": sid, "yfinance_symbol": sym} for sid, sym in pairs])


def test_share_class_finds_its_sec_row():
    """Regression for the original bug: BRK.B vs SEC's BRK-B dropped the row."""
    resolved, quarantined = build_snapshot(figi_rows((BRK, "BRK.B")), SEC, NO_VENDOR, NO_VENDOR)
    assert quarantined.empty
    assert resolved.loc[0, "issuer_id"] == "CIK0001067983"
    assert resolved.loc[0, "sec_ticker"] == "BRK-B"


def test_mismatch_flag_compares_raw_spellings():
    resolved, _ = build_snapshot(
        figi_rows((BRK, "BRK.B"), (MRSH, "MRSH")), SEC, yahoo((BRK, "BRK-B"), (MRSH, "MRSH")), NO_VENDOR
    )
    flags = dict(zip(resolved["exchange_ticker"], resolved["ticker_mismatch"]))
    assert flags == {"BRK.B": True, "MRSH": False}


def test_one_issuer_two_securities():
    """GOOGL and GOOG: same issuer_id, distinct security_id -- why CIK can't be the key."""
    resolved, _ = build_snapshot(figi_rows((GOOGL, "GOOGL"), (GOOG, "GOOG")), SEC, NO_VENDOR, NO_VENDOR)
    assert resolved["issuer_id"].nunique() == 1
    assert resolved["security_id"].nunique() == 2


def test_no_sec_match_is_quarantined_not_written_with_null_key():
    resolved, quarantined = build_snapshot(figi_rows((MRSH, "MMC")), SEC, NO_VENDOR, NO_VENDOR)
    assert resolved.empty
    assert quarantined.loc[0, "quarantine_reason"] == "no SEC match"


def test_openfigi_failure_is_quarantined_with_reason():
    figi = pd.DataFrame([{"security_id": "BBG000NOTREAL", "figi_error": "No identifier found."}])
    resolved, quarantined = build_snapshot(figi, SEC, NO_VENDOR, NO_VENDOR)
    assert resolved.empty
    assert quarantined.loc[0, "quarantine_reason"] == "OpenFIGI: No identifier found."


def test_failed_vendor_read_carries_last_known_values_forward(make_row):
    previous = pd.DataFrame([make_row(MRSH, "MRSH", yfinance_sector="Financial Services")])
    yf_failed = pd.DataFrame([{"security_id": MRSH, "yfinance_error": "timed out"}])

    resolved, _ = build_snapshot(figi_rows((MRSH, "MRSH")), SEC, yf_failed, NO_VENDOR, previous)
    assert resolved.loc[0, "yfinance_sector"] == "Financial Services"

    first_run, _ = build_snapshot(figi_rows((MRSH, "MRSH")), SEC, yf_failed, NO_VENDOR, previous=None)
    assert pd.isna(first_run.loc[0, "yfinance_sector"])


def test_successful_vendor_read_overrides_previous(make_row):
    previous = pd.DataFrame([make_row(MRSH, "MMC")])
    resolved, _ = build_snapshot(figi_rows((MRSH, "MRSH")), SEC, yahoo((MRSH, "MRSH")), NO_VENDOR, previous)
    assert resolved.loc[0, "yfinance_symbol"] == "MRSH"


def test_fan_out_join_fails_loudly():
    sec_dupe = pd.concat([SEC, SEC.iloc[[0]]])
    with pytest.raises(ValueError, match="not unique"):
        build_snapshot(figi_rows((BRK, "BRK.B")), sec_dupe, NO_VENDOR, NO_VENDOR)
