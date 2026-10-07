"""
Ticker spelling conventions, one function per vendor.

The same security, Berkshire Hathaway Class B, as each source spells it
(checked against the live sources, not assumed):

    exchange / Alpaca   BRK.B
    SEC EDGAR           BRK-B
    Yahoo Finance       BRK-B
    OpenFIGI/Bloomberg  BRK/B

Exchange convention is the project's canonical spelling: it's what appears
on the tape, and it's what UNIVERSE labels use. Every vendor gets a
converter to and from it, so vendor quirks live in this one file instead of
as ad hoc `.replace()` calls scattered through the pipeline.

Two different jobs need two different treatments of the spellings:

  - MATCHING (finding SEC's row for a security) needs them to compare equal,
    so it goes through `match_key`, which folds every separator to one
    character. This is what fixes the old `isin(UNIVERSE)` filter, which
    compared "BRK.B" to SEC's "BRK-B" and silently dropped the row.
  - RECONCILIATION (the `ticker_mismatch` flag) must compare the raw
    spellings and never `match_key`. Normalizing there would make the exact
    disagreement the flag exists to report disappear.

Separators are folded to '.', not deleted: "BRKB" is a different string from
"BRK.B" and could in principle be someone else's ticker.
"""

import re

_SEPARATORS = re.compile(r"[.\-/ ]")


def match_key(ticker: str) -> str:
    """Vendor-neutral form of any vendor's ticker. For joins only."""
    return _SEPARATORS.sub(".", ticker.strip().upper())


def to_figi(exchange_ticker: str) -> str:
    """Exchange -> OpenFIGI/Bloomberg (BRK.B -> BRK/B)."""
    return exchange_ticker.replace(".", "/")


def from_figi(figi_ticker: str) -> str:
    """OpenFIGI/Bloomberg -> exchange (BRK/B -> BRK.B)."""
    return figi_ticker.replace("/", ".")


def to_yahoo(exchange_ticker: str) -> str:
    """Exchange -> Yahoo Finance (BRK.B -> BRK-B)."""
    return exchange_ticker.replace(".", "-")


def to_alpaca(exchange_ticker: str) -> str:
    """Exchange -> Alpaca. Same convention; a function anyway, so that if Alpaca
    ever diverges there's one place to change instead of every call site."""
    return exchange_ticker
