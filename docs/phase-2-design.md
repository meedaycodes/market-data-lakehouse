# Phase 2 design: batch EOD ingestion

Status: **accepted** (2026-10-07) · Branch: `phase-2/batch-ingestion`

Daily bars for the ~80 securities in the security master, from Alpaca and
Yahoo Finance, land raw in bronze and clean in silver, orchestrated by
Airflow. A 5-year backfill seeds history.

```
                    Airflow DAG: daily_bars  (weekdays, 18:30 America/New_York)

 refresh_security_master ─┬─► fetch_alpaca_bars ──► bronze.alpaca_bars ──┐
                          │                                              ├─► merge_silver_bars ──► check_completeness
                          └─► fetch_yahoo_bars  ──► bronze.yahoo_bars ───┘        │
                                                                      silver.daily_bars
```

## Decisions

| # | Decision | Chosen | Rejected |
| --- | --- | --- | --- |
| 1 | How tasks run | Each task is a container from the `lakehouse` image (`DockerOperator`) | Spark installed in the Airflow image |
| 2 | Backfill depth | 5 years, one bulk job outside the daily DAG | Airflow day-by-day backfill |
| 3 | Vendor corrections | Silver: latest wins + `revision_count`; bronze keeps every delivery | SCD2 on every price bar |
| 4 | Bronze contents | Raw API responses, append-only, one table per vendor | Parsed rows |
| 5 | Silver key | (`security_id`, `trade_date`, `source`); vendors side by side | One merged "best" price |
| 6 | Price basis | Unadjusted prices + separate corporate actions; adjust as-of in gold | Vendor-adjusted series |
| 7 | Partitioning | None until a partition would reach ~1 GB | Partition by `trade_date` |
| 8 | File sizing | Prevent small files at write time (`coalesce(1)`, one file per run) | Scheduled `OPTIMIZE` / auto compaction |
| 9 | Reorganized securities (LIN, BLK, XOM) | Backfill under the successor FIGI; record each reorg as a `corporate_actions` row with SEC evidence | Map pre-reorg bars to predecessor FIGIs (deferred to Phase 5) |

## Why

**1 — Airflow orchestrates, it doesn't compute.** Running Spark inside
Airflow's processes couples Airflow's dependencies to the jobs' and lets one
heavy job starve the scheduler. Running our image per task keeps the two
apart, and Phase 8 becomes a swap from `DockerOperator` to
`KubernetesPodOperator` with the same image and command.
Cost: Airflow needs the Docker socket locally, which is root-equivalent on
the host. Acceptable for a laptop, never for a shared server. Bind mounts
for task containers must use the **host** path, not the path inside
Airflow's container.
Setup: Airflow 3.x, `LocalExecutor`, Postgres metadata database.

**2 — Bulk backfill.** Daily runs over 5 years would cost
~80 × 2 × 252 × 5 ≈ 200k API calls; one ranged request per security per
vendor costs ~160. The fetch code is shared; only the date range differs.
5 years covers several splits (NVDA 10:1 on 2024-06-10), the 2022 bear
market, and enough history for Project 3's features (~200k rows).

**3 — Latest wins in silver.** EOD bar corrections are rare; full
versioning would double the merge complexity for little gain. Bronze keeps
every delivery, so any past state can be rebuilt. Bitemporal versioning
belongs where restatements are common and material: fundamentals, Phase 5.

**4 — Raw bronze.** A parsing bug (like Phase 1's `AssetExchange.NASDAQ`) is
fixed by re-parsing bronze, without calling the vendor again — which matters
when vendors rate-limit, expire history, or rewrite it. Bronze is never
deduplicated: two deliveries of the same day are the audit trail.

**5 — Vendors side by side.** Their disagreement is what Phase 4's
divergence checks measure. Picking a winner in silver destroys that signal;
gold decides which to trust.

**6 — Unadjusted prices.** Adjusted series are recomputed after every split
and dividend, so past values change. A backtest on today's adjusted series
sees splits that hadn't happened yet: look-ahead bias.
Checked 2026-10-07: Yahoo's "unadjusted" `Close` is still **split-adjusted**
(NVDA 2024-06-07 shows 120.89; the real close was ~1,209) — and, as the
backfill showed, spin-off-adjusted too (see findings below). Alpaca's
`adjustment=raw` is truly raw. So on any split or spin-off, the vendors
differ by that ratio for every earlier date — Phase 4 must expect that
rather than flag it.

**7 — No partitioning.** Silver grows ~40k rows/year. Date partitions would
mean thousands of tiny files, which costs Spark more than scanning a small
table; Delta's file statistics already skip irrelevant files.

**8 — Prevent, don't compact.** A DataFrame built from a Python list is
split across every local core, so the first bronze loads wrote 80 rows as
10 files (~2,500 files/year per table at one run a day). `coalesce(1)` makes
each run one file, and a run is a few hundred KB, so there's nothing left
for `OPTIMIZE` to fix. Delta 3.2's `OPTIMIZE` treats any file under 1 GB as
small and bin-packs toward 1 GB; auto compaction (off by default) triggers at
50 small files and targets 128 MB. Both are deferred to **Phase 3**, where a
streaming query commits a small batch every few seconds and can't control
file count at write time the way one batch run can.

**9 — Reorganizations: visible shortcut now, lineage later.** In a
holding-company reorganization the listed security is replaced (new FIGI;
for XOM and BLK a new CIK too) while the ticker stays. Vendors serve the
whole history under today's ticker, so the backfill tags pre-reorg bars with
the successor's FIGI. Mapping them to predecessor FIGIs needs a security
lineage table (old FIGIs, exchange ratios) the master doesn't have; that's
Phase 5, where point-in-time identity is the subject. Until then the
boundary is data, not folklore: `reference_data/reorganizations.py` lists
each reorg with its SEC Form 8-K12B, loaded as `action_type='reorg'`,
`source='sec_edgar'`, `value=NULL` (no verified ratio), with the filing's
EDGAR acceptance time as `first_seen_at` and its accession number as
`source_run_id`.

| Security | Effective (8-K12B filed) | Accession |
| --- | --- | --- |
| LIN | 2023-03-01 | 0001193125-23-055949 |
| BLK | 2024-10-01 | 0001193125-24-229601 |
| XOM | 2026-07-01 | 0001193125-26-291990 |

## Backfill findings (2021-10-06 .. 2026-10-06)

All 80 securities × 1,255 sessions from both vendors; 0 bars missing.

- **Splits behave as decision 6 predicted.** The Alpaca/Yahoo close ratio
  is exactly the split ratio before a split and 1.00 after (NVDA: 10.00
  through 2024-06-07, 1.00 from 2024-06-10). 89,858 of 100,400
  security-days have ratio 1.00.
- **Yahoo records spin-offs as fractional "splits"** — IBM 1.046
  (2021-11-04), GE 1.281 (2023-01-04) and 1.253 (2024-04-02), DHR 1.128,
  HON 1.061 and 0.9535, CMCSA 1.067, SPGI 1.057 — and adjusts its close for
  them (GE's two compound to the 1.61 ratio seen in the data). So Yahoo's
  `price_basis` is `vendor_adjusted`, not `split_adjusted`, and its `split`
  rows are kept as reported: gold must classify splits vs spin-offs before
  adjusting anything. HON's 0.9535 (a ratio below 1) needs checking first.
- **The 1.01 ratio on 1,021 security-days is HON's two adjustments
  compounding** (1.061 × 0.9535 = 1.0117): all of them are HON, every day
  before 2025-10-30 (found with notebook 02). Not vendor disagreement. Still
  unexplained: why Yahoo's 2026-06-29 HON factor is *below* 1, which raises
  past prices.
- **A parser change needs a rebuild, not a rerun.** Latest-wins revises only
  on a *newer delivery*; relabelling `price_basis` meant deleting
  `silver/daily_bars` and rebuilding it from bronze (~200k bars). That's the
  point of keeping bronze raw.

## Joining to the security master

Bars are tagged with `security_id` at fetch time — the fetch is driven by
the security master, as Phase 1 was driven by FIGIs. A FIGI doesn't change
when a ticker does, so a 2021 MMC bar and a 2026 MRSH bar carry the same
`BBG000BP4MH0`, and the join is plain equality. As-of lookups (`scd2.as_of`)
are only for attributes that change, like the ticker shown on a date, and
the master's history starts 2026-10-07 anyway.

## Reruns and the calendar

- Each run owns one trading date (its Airflow logical date). Rerunning it
  appends a bronze delivery and `MERGE`s silver on its key: no duplicates.
- Exchange calendar (`exchange_calendars`): a holiday run succeeds with
  "no session", not a failure.
- `check_completeness` expects exactly 80 securities × 2 sources per trading
  day and fails otherwise.

## Known interaction with Phase 1

`build_security_master` exits 1 when anything is quarantined, which fails
`refresh_security_master`. That stays — a quarantined security needs a
human — but the fetch tasks use a trigger rule that runs as long as the
security master table exists, so one bad row doesn't block 79 good ones.

## Resolved questions (checked live 2026-10-07)

- [x] **Alpaca history depth.** The free tier serves SIP (full-market)
      daily bars from **2016-01-04** — 5 years is well covered. But the IEX
      feed only starts **2020-07-27** and covers a single exchange's trades,
      so every request must set `feed=sip` explicitly; never rely on the
      default feed.
- [x] **Alpaca `adjustment=raw` is truly raw.** NVDA closed 1,208.88 on
      2024-06-07 and 121.79 on 2024-06-10, with volume jumping ~8× at the
      split — unadjusted prices *and* volumes.
- [x] **Bar timestamps are New York midnight in UTC** (`2024-06-07
      04:00:00+00:00`; 05:00 in winter). `trade_date` must be derived in
      `America/New_York`, never by truncating the UTC timestamp — a habit
      that breaks silently for any feed stamped later in the day.
- [x] **Airflow pin: 3.3.2** (latest, released 2026-09-17; Python ≥ 3.10).
