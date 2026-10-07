"""
Every test here runs offline. Vendor data is synthetic, built to reproduce
the specific real-world cases each fix exists for (BRK.B's spellings,
GOOG/GOOGL sharing a CIK, MMC -> MRSH), so the suite can run in CI with no
keys and no network -- and can't go green just because a vendor happened to
return friendly data that day.
"""

import pandas as pd
import pytest

from ingestion.security_master_table import ATTRIBUTE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole run -- starting one costs ~30s."""
    pytest.importorskip("pyspark")
    from ingestion.spark import get_spark

    session = get_spark("tests")
    session.sparkContext.setLogLevel("FATAL")  # constraint tests fail writes on purpose
    yield session
    session.stop()


@pytest.fixture
def make_row():
    """A valid security master row; override only what a test cares about."""

    def _make(security_id, ticker, **overrides):
        row = {c: None for c in ATTRIBUTE_COLUMNS}
        row.update(
            security_id=security_id,
            issuer_id="CIK0000062709",
            cik=62709,
            exchange_ticker=ticker,
            figi_ticker=ticker,
            sec_ticker=ticker,
            yfinance_symbol=ticker,
            ticker_mismatch=False,
        )
        row.update(overrides)
        return row

    return _make


@pytest.fixture
def frame():
    return lambda rows: pd.DataFrame(rows)
