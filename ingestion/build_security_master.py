"""
Phase 1: security master / entity resolution.

Builds a canonical security master by reconciling four independent sources:
  - OpenFIGI   -- composite FIGI -> current ticker and name. The FIGI is the
                  security_id: one per share class, stable across ticker
                  changes (MMC -> MRSH kept BBG000BP4MH0).
  - SEC EDGAR  -- ticker -> CIK, the issuer_id. A CIK identifies a company,
                  not a security: GOOGL and GOOG share CIK 1652044, so the CIK
                  can't be the row key, but it's what joins to fundamentals.
  - yfinance   -- Yahoo's symbol/name/sector
  - Alpaca     -- broker asset_id + symbol

The FIGI is the key from the first line to the last. The universe is a list
of FIGIs; every vendor fetch is driven by a FIGI and returns rows tagged with
it, so vendor data is joined on the FIGI, never on a ticker. The one ticker
join left is the SEC lookup, because SEC's file has no FIGIs -- and it goes
through `symbology.match_key`, since SEC writes BRK-B where OpenFIGI writes
BRK/B. Joining raw tickers there is the bug that silently dropped BRK.B and
BF.B from the first version of this script.

A row that can't be fully identified (no FIGI match, no SEC match) is not
written with a null key. It goes to quarantine and is reported, and the run
exits non-zero so an orchestrator notices. Rows with a null key would load
fine and then quietly vanish from every downstream join.

Run inside the docker-compose environment:
    docker compose exec lakehouse python -m ingestion.build_security_master

Requires SEC_CONTACT_EMAIL in .env (SEC EDGAR's fair-access policy asks for a
real contact in the User-Agent header). ALPACA_API_KEY / ALPACA_SECRET_KEY
and OPENFIGI_API_KEY are optional -- missing Alpaca keys just leave the
Alpaca columns empty and are reported.
"""

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

from ingestion.security_master_table import ATTRIBUTE_COLUMNS
from reference_data.openfigi import figi_job, map_jobs
from reference_data.symbology import from_figi, match_key, to_alpaca, to_yahoo
from reference_data.universe import UNIVERSE

load_dotenv()

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
CONTACT_EMAIL = os.getenv("SEC_CONTACT_EMAIL", "you@example.com")
USER_AGENT = f"market-lakehouse research project ({CONTACT_EMAIL})"

DELTA_TABLE_PATH = os.getenv(
    "SECURITY_MASTER_PATH", "/home/jovyan/work/data/lakehouse/reference/security_master"
)

# Columns each vendor fetch fills. When a fetch fails, these are unknown --
# not empty -- and get carried forward from the last good version.
VENDOR_COLUMNS = {
    "yfinance": ["yfinance_symbol", "yfinance_name", "yfinance_exchange", "yfinance_sector"],
    "alpaca": ["alpaca_asset_id", "alpaca_symbol", "alpaca_exchange", "alpaca_tradable"],
}


def fetch_figi_rows(figis: list[str]) -> pd.DataFrame:
    """Look every FIGI up for its current ticker and name."""
    rows = []
    for figi, result in zip(figis, map_jobs([figi_job(f) for f in figis])):
        matches = result.get("data", [])
        if len(matches) != 1:
            reason = result.get("warning") or result.get("error") or f"{len(matches)} matches"
            rows.append({"security_id": figi, "figi_error": reason})
            continue
        m = matches[0]
        if m.get("compositeFIGI") != figi:
            # An exchange-level or share-class FIGI pasted into UNIVERSE by
            # mistake would resolve fine and then never match anything else.
            rows.append({"security_id": figi, "figi_error": f"not a composite FIGI (composite is {m.get('compositeFIGI')})"})
            continue
        rows.append(
            {
                "security_id": figi,
                "figi_ticker": m["ticker"],
                "exchange_ticker": from_figi(m["ticker"]),
                "security_name": m.get("name"),
                "security_type": m.get("securityType2"),
                "share_class_figi": m.get("shareClassFIGI"),
            }
        )
    return pd.DataFrame(rows)


def fetch_sec_ticker_map() -> pd.DataFrame:
    """SEC EDGAR's current ticker -> CIK map (a snapshot: no history in it)."""
    resp = requests.get(SEC_TICKERS_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    raw = resp.json()  # {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    df = pd.DataFrame(raw.values())
    df["cik"] = df["cik_str"].astype(int)
    df["issuer_id"] = df["cik"].apply(lambda c: f"CIK{c:010d}")
    df["match_key"] = df["ticker"].map(match_key)

    # match_key is only safe if folding separators never makes two different
    # SEC tickers look the same -- otherwise a security could silently pick up
    # the wrong company's CIK. True today; checked every run, not assumed.
    collisions = df[df["match_key"].duplicated(keep=False)]
    if not collisions.empty:
        raise ValueError(f"match_key collides on SEC tickers:\n{collisions.to_string(index=False)}")

    return df.rename(columns={"ticker": "sec_ticker", "title": "sec_company_name"})[
        ["match_key", "issuer_id", "cik", "sec_ticker", "sec_company_name"]
    ]


def fetch_yfinance_row(security_id: str, exchange_ticker: str) -> dict:
    try:
        info = yf.Ticker(to_yahoo(exchange_ticker)).get_info()
        return {
            "security_id": security_id,
            "yfinance_symbol": info.get("symbol"),
            "yfinance_name": info.get("longName") or info.get("shortName"),
            "yfinance_exchange": info.get("exchange"),
            "yfinance_sector": info.get("sector"),
        }
    except Exception as exc:  # yfinance raises all sorts of things on a bad symbol
        return {"security_id": security_id, "yfinance_error": str(exc)}


def fetch_alpaca_rows(securities: list[tuple[str, str]]) -> pd.DataFrame:
    """Alpaca's asset_id (a UUID) is its own stable ID; worth keeping for joins
    against Alpaca data, even though the FIGI is ours."""
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not api_secret:
        return pd.DataFrame(
            [{"security_id": sid, "alpaca_error": "no API key configured"} for sid, _ in securities]
        )

    from alpaca.trading.client import TradingClient

    client = TradingClient(api_key, api_secret, paper=True)
    rows = []
    for security_id, exchange_ticker in securities:
        try:
            asset = client.get_asset(to_alpaca(exchange_ticker))
            rows.append(
                {
                    "security_id": security_id,
                    "alpaca_asset_id": str(asset.id),
                    "alpaca_symbol": asset.symbol,
                    # .value, not str(): str() on alpaca-py's enums gives
                    # "AssetExchange.NASDAQ", the Python name, not the data.
                    "alpaca_exchange": asset.exchange.value,
                    "alpaca_tradable": asset.tradable,
                }
            )
        except Exception as exc:
            rows.append({"security_id": security_id, "alpaca_error": str(exc)})
    return pd.DataFrame(rows)


def tickers_disagree(row: pd.Series) -> bool:
    """The actual reconciliation check: does every source spell this ticker the same way?

    Compares the RAW spellings -- deliberately not `match_key`. Normalizing
    here would collapse BRK.B / BRK-B / BRK/B into "agreement", which defeats
    the entire point of the check.
    """
    variants = {
        row.get("figi_ticker"),
        row.get("sec_ticker"),
        row.get("yfinance_symbol"),
        row.get("alpaca_symbol"),
    }
    return len({v for v in variants if isinstance(v, str) and v}) > 1


def carry_forward(merged: pd.DataFrame, previous: pd.DataFrame | None) -> pd.DataFrame:
    """A failed read is not a change.

    If yfinance times out for AAPL today, AAPL's yfinance columns are
    unknown, not empty. Writing them as nulls would record a fake change in
    the history (and a fake change back on the next good run). So for each
    vendor that failed on a security, keep that vendor's columns from the
    last good version. A missing Alpaca key counts as a failure too: it says
    nothing about whether Alpaca still lists the security.
    """
    if previous is None or previous.empty:
        return merged
    prev = previous.set_index("security_id")
    for vendor, columns in VENDOR_COLUMNS.items():
        error_col = f"{vendor}_error"
        if error_col not in merged:
            continue
        failed = merged[error_col].notna() & merged["security_id"].isin(prev.index)
        for col in columns:
            merged[col] = merged[col].astype(object)
            merged.loc[failed, col] = merged.loc[failed, "security_id"].map(prev[col])
    return merged


def build_snapshot(
    figi_df: pd.DataFrame,
    sec_df: pd.DataFrame,
    yf_df: pd.DataFrame,
    alpaca_df: pd.DataFrame,
    previous: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reconcile the sources into one row per security.

    `previous` is the current version of each security from the last build
    (None on the first), used only to carry forward vendor columns a fetch
    failed to read. Pure function -- no network, no Spark -- so the
    reconciliation rules can be tested offline against synthetic frames.
    Returns (resolved, quarantined).
    """
    merged = figi_df.copy()
    for col in ("exchange_ticker", "figi_error"):
        if col not in merged:
            merged[col] = None

    merged["match_key"] = merged["exchange_ticker"].map(lambda t: match_key(t) if isinstance(t, str) else None)
    merged = (
        merged.merge(sec_df, on="match_key", how="left")
        .merge(yf_df, on="security_id", how="left")
        .merge(alpaca_df, on="security_id", how="left")
    )

    merged["quarantine_reason"] = None
    merged.loc[merged["issuer_id"].isna(), "quarantine_reason"] = "no SEC match"
    merged.loc[merged["figi_error"].notna(), "quarantine_reason"] = "OpenFIGI: " + merged["figi_error"].astype(str)

    if merged["security_id"].duplicated().any():
        raise ValueError("security_id is not unique after reconciliation -- a join fanned out")

    for col in ATTRIBUTE_COLUMNS:
        if col not in merged:
            merged[col] = None
    # Before the mismatch check, so the flag is computed on the values that
    # actually get stored.
    merged = carry_forward(merged, previous)
    merged["ticker_mismatch"] = pd.Series([tickers_disagree(r) for _, r in merged.iterrows()], index=merged.index, dtype=bool)
    merged["cik"] = merged["cik"].astype("Int64")

    is_quarantined = merged["quarantine_reason"].notna()
    resolved = merged.loc[~is_quarantined, ATTRIBUTE_COLUMNS].reset_index(drop=True)
    quarantined = merged.loc[is_quarantined, ["security_id", "exchange_ticker", "quarantine_reason"]]
    return resolved, quarantined.reset_index(drop=True)


def fetch_all() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    figi_df = fetch_figi_rows(list(UNIVERSE))
    sec_df = fetch_sec_ticker_map()

    # Only securities OpenFIGI could resolve have a current ticker to ask about.
    known = figi_df.dropna(subset=["exchange_ticker"]) if "exchange_ticker" in figi_df else figi_df.iloc[0:0]
    securities = list(zip(known["security_id"], known["exchange_ticker"]))

    yf_rows = []
    for security_id, exchange_ticker in securities:
        yf_rows.append(fetch_yfinance_row(security_id, exchange_ticker))
        time.sleep(0.2)  # be polite to free-tier rate limits
    yf_df = pd.DataFrame(yf_rows) if yf_rows else pd.DataFrame(columns=["security_id"])

    alpaca_df = fetch_alpaca_rows(securities)
    if alpaca_df.empty:
        alpaca_df = pd.DataFrame(columns=["security_id"])
    return figi_df, sec_df, yf_df, alpaca_df


def report(resolved: pd.DataFrame, quarantined: pd.DataFrame, yf_df: pd.DataFrame, alpaca_df: pd.DataFrame) -> None:
    print(f"\nResolved {len(resolved)} securities, quarantined {len(quarantined)}.")

    mismatches = resolved[resolved["ticker_mismatch"]]
    if not mismatches.empty:
        print("\nTicker spellings that differ across sources (expected for share classes):")
        cols = ["exchange_ticker", "figi_ticker", "sec_ticker", "yfinance_symbol", "alpaca_symbol"]
        print(mismatches[cols].to_string(index=False))

    shared = resolved[resolved["issuer_id"].duplicated(keep=False)]
    if not shared.empty:
        print("\nIssuers with more than one security (why issuer_id can't be the key):")
        print(shared[["issuer_id", "security_id", "exchange_ticker"]].to_string(index=False))

    # Group vendor failures by message: 80 copies of "no API key configured"
    # is noise, one line with a count is a fact.
    for name, df in (("yfinance", yf_df), ("Alpaca", alpaca_df)):
        col = f"{name.lower()}_error"
        if col in df and df[col].notna().any():
            print(f"\n{name} lookups that failed (columns left empty):")
            print(df[col].value_counts().to_string())

    if not quarantined.empty:
        print("\nQUARANTINED -- not written, needs a human:")
        print(quarantined.to_string(index=False))


def main() -> int:
    from ingestion.security_master_table import read_current, write_security_master
    from ingestion.spark import get_spark

    # One timestamp for the whole run: every version this run writes shares
    # the same valid_from, so a run is one consistent point in history.
    observed_at = datetime.now(timezone.utc)
    figi_df, sec_df, yf_df, alpaca_df = fetch_all()

    spark = get_spark("security-master")
    try:
        previous = read_current(spark, DELTA_TABLE_PATH)
        resolved, quarantined = build_snapshot(figi_df, sec_df, yf_df, alpaca_df, previous)
        report(resolved, quarantined, yf_df, alpaca_df)
        write_security_master(spark, resolved, DELTA_TABLE_PATH, observed_at)
    finally:
        spark.stop()

    # Write what resolved, then fail loudly about what didn't: one bad row
    # shouldn't block 79 good ones, but it also shouldn't pass silently.
    return 1 if not quarantined.empty else 0


if __name__ == "__main__":
    sys.exit(main())
