"""
Helper for maintaining reference_data/universe.py. Not part of the pipeline.

Add names -- resolve tickers to composite FIGIs, printed as lines ready to
paste into UNIVERSE:

    python -m reference_data.resolve_figis BRK.B GOOG

Audit the universe -- look every FIGI back up and show its current ticker
next to the label in the file, flagging renames (like MMC -> MRSH) and FIGIs
that no longer resolve:

    python -m reference_data.resolve_figis --check

Refuses to guess: a ticker with zero matches, or more than one, is reported
on stderr and left out of the output. Picking "the first match" is how a
universe quietly ends up holding the wrong security. Exits 1 if anything
needs a human, so it can gate a script or CI step.

Set OPENFIGI_API_KEY (free) for higher rate limits; ~80 names work without.
"""

import argparse
import sys

from reference_data.openfigi import figi_job, map_jobs, ticker_job
from reference_data.symbology import from_figi
from reference_data.universe import UNIVERSE

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def resolve(tickers: list[str]) -> int:
    problems = 0
    results = map_jobs([ticker_job(t) for t in tickers])
    for ticker, result in zip(tickers, results):
        matches = result.get("data", [])
        if len(matches) == 1:
            m = matches[0]
            print(f'    "{m["compositeFIGI"]}": "{ticker}",  # {m["name"]}, {m["securityType2"]}')
            continue
        problems += 1
        if not matches:
            print(f"{ticker}: no match ({result.get('warning') or result.get('error')})", file=sys.stderr)
        else:
            print(f"{ticker}: {len(matches)} matches, not picking one:", file=sys.stderr)
            for m in matches:
                print(f"    {m['compositeFIGI']}  {m['name']}  {m['securityType2']}", file=sys.stderr)
    return problems


def check() -> int:
    problems = 0
    figis = list(UNIVERSE)
    results = map_jobs([figi_job(f) for f in figis])
    for figi, result in zip(figis, results):
        label = UNIVERSE[figi]
        matches = result.get("data", [])
        if not matches:
            problems += 1
            print(f"{figi} ({label}): no longer resolves", file=sys.stderr)
            continue
        current = from_figi(matches[0]["ticker"])
        if current != label:
            problems += 1
            print(f"{figi}: label {label}, OpenFIGI now says {current} -- renamed?", file=sys.stderr)
    print(f"{len(figis)} FIGIs checked, {problems} need attention")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tickers", nargs="*", help="exchange-convention tickers, e.g. BRK.B")
    parser.add_argument("--check", action="store_true", help="audit every FIGI in UNIVERSE")
    args = parser.parse_args()
    if args.check == bool(args.tickers):
        parser.error("give tickers to resolve, or --check -- not both, not neither")
    return 1 if (check() if args.check else resolve(args.tickers)) else 0


if __name__ == "__main__":
    sys.exit(main())
