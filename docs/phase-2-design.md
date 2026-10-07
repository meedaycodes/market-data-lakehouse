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
(NVDA 2024-06-07 shows 120.89; the real close was ~1,209). Alpaca's
`adjustment=raw` is truly raw. So on any split, the vendors differ by the
split ratio for every pre-split date — Phase 4 must expect that rather than
flag it.

**7 — No partitioning.** Silver grows ~40k rows/year. Date partitions would
mean thousands of tiny files, which costs Spark more than scanning a small
table; Delta's file statistics already skip irrelevant files.

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
