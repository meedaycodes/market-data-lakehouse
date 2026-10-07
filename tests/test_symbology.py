import pytest

from reference_data.symbology import from_figi, match_key, to_alpaca, to_figi, to_yahoo

# Berkshire Class B, as each source really spells it (checked live).
BRK_SPELLINGS = {"exchange": "BRK.B", "sec": "BRK-B", "yahoo": "BRK-B", "figi": "BRK/B"}


def test_vendor_conversions_from_exchange_convention():
    assert to_yahoo("BRK.B") == BRK_SPELLINGS["yahoo"]
    assert to_figi("BRK.B") == BRK_SPELLINGS["figi"]
    assert to_alpaca("BRK.B") == BRK_SPELLINGS["exchange"]
    assert from_figi("BRK/B") == "BRK.B"


@pytest.mark.parametrize("ticker", ["BRK.B", "BF.B", "AAPL"])
def test_figi_round_trip(ticker):
    assert from_figi(to_figi(ticker)) == ticker


def test_match_key_makes_every_spelling_equal():
    assert len({match_key(t) for t in BRK_SPELLINGS.values()}) == 1


def test_match_key_folds_separators_but_does_not_delete_them():
    # BRKB could be a different security's ticker; folding to '.' keeps them apart.
    assert match_key("BRK-B") != match_key("BRKB")


def test_match_key_is_case_and_whitespace_insensitive():
    assert match_key(" brk-b ") == match_key("BRK.B")
