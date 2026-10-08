"""
Holding-company reorganizations inside the backfill window.

In each of these the listed security was replaced by a new one -- new FIGI
(and for XOM and BLK, a new SEC CIK) -- while the ticker stayed the same.
Vendors serve the whole price history under today's ticker, so the backfill
tags pre-reorganization bars with the successor's FIGI. That's a deliberate
shortcut (docs/phase-2-design.md, decision 9); this file makes the boundary
visible in the data instead of hidden.

Every date comes from the successor's Form 8-K12B on SEC EDGAR -- the filing
a successor issuer makes when it takes over its predecessor's registration --
never from memory. `accepted_at` is EDGAR's acceptance time: the moment the
fact became public, which is what point-in-time logic needs.

Not recorded here, and needed for the full fix in Phase 5: the predecessor
FIGIs, and whether the share exchange was one-for-one.
"""

REORGANIZATIONS = [
    {
        "security_id": "BBG01FND0CC1",
        "ticker": "LIN",
        "effective_date": "2023-03-01",
        "sec_cik": 1707925,
        "form": "8-K12B",
        "accession": "0001193125-23-055949",
        "accepted_at": "2023-03-02T00:33:18Z",
    },
    {
        "security_id": "BBG01PSW2WN4",
        "ticker": "BLK",
        "effective_date": "2024-10-01",
        "sec_cik": 2012383,
        "form": "8-K12B",
        "accession": "0001193125-24-229601",
        "accepted_at": "2024-10-01T17:21:52Z",
    },
    {
        "security_id": "BBG023CY9MM1",
        "ticker": "XOM",
        "effective_date": "2026-07-01",
        "sec_cik": 2115436,
        "form": "8-K12B",
        "accession": "0001193125-26-291990",
        "accepted_at": "2026-07-01T16:36:49Z",
    },
]
