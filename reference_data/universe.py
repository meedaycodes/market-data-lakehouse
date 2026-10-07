"""
The instrument universe for the project.

Deliberately small (~80 names) and deliberately liquid/well-known -- the goal
of this project is to demonstrate correct handling of point-in-time joins,
cross-vendor entity resolution, and streaming ingestion, not to scale to the
full index. Bigger universes are just more of the same problem; they don't
teach you anything new.

Keyed by composite FIGI, not by ticker. The first version of this file was a
list of tickers, and it broke without anyone touching it: Marsh McLennan
moved from MMC to MRSH, so "MMC" stopped resolving at SEC and OpenFIGI. A
universe defined by the thing that changes has to be hand-edited on every
corporate action; a universe defined by a stable ID lets the build discover
the new ticker and record the change as history in the security master.

The ticker next to each FIGI is only a human-readable label from when the
name was added. Nothing reads it -- the build looks up the current ticker
from OpenFIGI on every run.

Names worth knowing about, kept on purpose:
  - BRK.B, BF.B: share-class tickers that four sources spell four ways
    (exchange BRK.B, SEC BRK-B, Yahoo BRK-B, OpenFIGI BRK/B).
  - GOOGL + GOOG: two securities, one issuer (one SEC CIK). Proves the
    security master can't be keyed by CIK.
  - MRSH (was MMC): a real ticker change; the FIGI didn't move.
  - XOM, BLK, LIN: holding-company reorganizations gave these new FIGIs
    (and XOM and BLK new CIKs). No identifier is permanent -- the FIGI
    changes exactly when the security itself is replaced, which is the
    change semantics a security master wants.
"""

UNIVERSE: dict[str, str] = {
    "BBG000B9XRY4": "AAPL",
    "BBG000BPH459": "MSFT",
    "BBG009S39JX6": "GOOGL",
    "BBG009S3NB30": "GOOG",
    "BBG000BVPV84": "AMZN",
    "BBG000BBJQV0": "NVDA",
    "BBG000MM2P62": "META",
    "BBG000N9MNX3": "TSLA",
    "BBG000DWG505": "BRK.B",
    "BBG000DMBXR2": "JPM",
    "BBG000PSKYX7": "V",
    "BBG000CH5208": "UNH",
    "BBG023CY9MM1": "XOM",
    "BBG000BMHYD1": "JNJ",
    "BBG000BR2TH3": "PG",
    "BBG000F1ZSQ2": "MA",
    "BBG000BKZB36": "HD",
    "BBG000K4ND22": "CVX",
    "BBG000BPD168": "MRK",
    "BBG0025Y4RY4": "ABBV",
    "BBG000DH7JK6": "PEP",
    "BBG000BMX289": "KO",
    "BBG00KHY5S69": "AVGO",
    "BBG000F6H8W8": "COST",
    "BBG000BWXBC2": "WMT",
    "BBG000BCTLF6": "BAC",
    "BBG000BNSZP1": "MCD",
    "BBG000BN2DC2": "CRM",
    "BBG000BVDLH9": "TMO",
    "BBG000D9D830": "ACN",
    "BBG01FND0CC1": "LIN",
    "BBG000BB5006": "ADBE",
    "BBG000C3J3C9": "CSCO",
    "BBG000B9ZXB4": "ABT",
    "BBG000BH3JF8": "DHR",
    "BBG000CL9VN6": "NFLX",
    "BBG000BWQFY7": "WFC",
    "BBG000BBQCY0": "AMD",
    "BBG000BVV7G1": "TXN",
    "BBG000J2XL74": "PM",
    "BBG000BJSBJ0": "NEE",
    "BBG000BD2NY8": "BF.B",
    "BBG000BH4R78": "DIS",
    "BBG000C0G1D1": "INTC",
    "BBG000HS77T5": "VZ",
    "BBG000BFT2L4": "CMCSA",
    "BBG000CGC1X8": "QCOM",
    "BBG000H556T9": "HON",
    "BBG000BW3299": "UNP",
    "BBG000BNDN65": "LOW",
    "BBG000BLNNH6": "IBM",
    "BBG000BQLTW7": "ORCL",
    "BBG000BBS2Y0": "AMGN",
    "BBG000BF0K17": "CAT",
    "BBG000BK6MB5": "GE",
    "BBG000BCSST7": "BA",
    "BBG000CTQBF3": "SBUX",
    "BBG000C6CFJ5": "GS",
    "BBG01PSW2WN4": "BLK",
    "BBG000BCG930": "ELV",
    "BBG000B9Z0J8": "PLD",
    "BBG000BP1Q11": "SPGI",
    "BBG000BNWG87": "MDT",
    "BBG000BJPDZ1": "ISRG",
    "BBG000M1R011": "NOW",
    "BBG000BH5DV1": "INTU",
    "BBG000BLBVN4": "BKNG",
    "BBG000BH1NH9": "DE",
    "BBG000BCQZS4": "AXP",
    "BBG000CKGBP2": "GILD",
    "BBG000C1BW00": "LMT",
    "BBG000DN7P92": "SYK",
    "BBG000BP4MH0": "MRSH",
    "BBG000BB6G37": "ADI",
    "BBG000C1S2X2": "VRTX",
    "BBG000BV8DN6": "TJX",
    "BBG000BP6LJ8": "MO",
    "BBG000BR37X2": "PGR",
    "BBG000C734W3": "REGN",
    "BBG000BR14K5": "CB",
}
