# Market Data Lakehouse

The first of three projects: a cross-vendor market data lakehouse built on
Delta Lake, with point-in-time correctness as the design constraint everything
else has to survive.

| Phase | What | Status |
| --- | --- | --- |
| 1 | Security master: FIGI-keyed, SCD2 history | Done |
| 2 | Batch EOD bars: bronze → silver, Airflow | In progress: bronze done; silver, backfill, Airflow next ([design](docs/phase-2-design.md)) |
| 3 | Streaming: Alpaca websocket → Redpanda → Structured Streaming | Planned |
| 4 | Data quality: crossed quotes, gaps, vendor divergence | Planned |
| 5 | Point-in-time gold layer | Planned |
| 6–8 | CI/CD, Terraform, Kubernetes | Planned |

## Phase 1: Security master

The piece everything downstream joins against — a canonical security master
reconciled across four independent sources that don't agree with each other
on what to call the same company.

### Why this phase first

Every later phase (batch/streaming ingestion, data quality monitoring, the
point-in-time gold layer) needs a stable identifier to join on. Get this
wrong and every later join is quietly wrong too.

### The design, and the failure each part prevents

| Decision | What goes wrong without it |
| --- | --- |
| **Row key = composite FIGI** (`security_id`), not a ticker | Tickers change: Marsh McLennan went MMC → MRSH and its FIGI didn't move. Keyed on ticker, the history breaks in two. |
| **CIK is `issuer_id`, a column — not the key** | A CIK identifies a company. GOOGL and GOOG share CIK 1652044; keyed on CIK they collapse into one row. |
| **Universe is a list of FIGIs** | A ticker list breaks on every rename ("MMC" stopped resolving) and has to be hand-fixed. |
| **`match_key` for joins, raw spellings for `ticker_mismatch`** | SEC writes `BRK-B`, OpenFIGI `BRK/B`, the exchange `BRK.B`. Raw joins silently dropped BRK.B and BF.B; normalized *checks* would hide the disagreement. |
| **Quarantine instead of null keys** | A row with a null key loads fine and then vanishes from every downstream join. |
| **SCD Type 2 history** (`valid_from` / `valid_to` / `is_current`) | Overwriting keeps only today. A backtest for April would see today's ticker — look-ahead bias. |
| **Failed vendor reads carry forward** | One yfinance timeout would record a fake change, then a fake change back. |
| **Explicit schema + Delta constraints** | All-string columns (`ticker_mismatch == "True"`) push type bugs downstream; NOT NULL keys and a `valid_to > valid_from` CHECK fail bad writes loudly. |

`valid_from` is when the pipeline *observed* a change, not when it happened in
the world. History starts at your first build. Exact event dates would need a
corporate-actions feed — out of scope for Phase 1.

## Setup

1. **Get an Alpaca paper trading account** (free, no card required) at
   [alpaca.markets](https://alpaca.markets) → Dashboard → API Keys. You'll
   use this key for reference data now and the live websocket feed in a
   later phase.

2. **Copy the env file and fill it in:**
   ```bash
   cp .env.example .env
   # edit .env: set SEC_CONTACT_EMAIL to your real email (SEC EDGAR's fair-
   # access policy wants a real contact in the User-Agent header) and your
   # Alpaca keys. OPENFIGI_API_KEY is optional.
   ```

3. **Build and start the container:**
   ```bash
   docker compose build
   docker compose up -d
   ```
   This gives you a Spark + Delta Lake + Jupyter environment at
   `http://localhost:8888` (no token — local dev only, don't expose this
   port past your own machine).

4. **Run the tests** (offline — no keys or network needed, ~1 minute):
   ```bash
   docker compose exec lakehouse pytest -q
   ```

5. **Run the build:**
   ```bash
   docker compose exec lakehouse python -m ingestion.build_security_master
   ```
   Resolves each FIGI in `reference_data/universe.py` to its current ticker
   via OpenFIGI, reconciles against SEC EDGAR, yfinance and Alpaca, and merges
   the result into the `security_master` Delta table at
   `data/lakehouse/reference/security_master`. It reports share-class
   spelling mismatches (BRK.B, BF.B), issuers with several securities
   (GOOGL/GOOG), vendor failures, and anything quarantined. Exits 1 if
   anything was quarantined.

   Run it again: it should report `0 new, 0 changed` — reruns are safe.

6. **Inspect the result:**
   ```bash
   docker compose exec lakehouse python -m ingestion.inspect_security_master
   ```
   Or open `notebooks/01_security_master_tour.ipynb` at
   `http://localhost:8888` for a guided walkthrough: why the key is a FIGI,
   the four spellings of BRK.B, a ticker change replayed in a sandbox, and
   as-of queries.

## Phase 2: Batch EOD bars (in progress)

Design and the reasoning behind each decision:
[docs/phase-2-design.md](docs/phase-2-design.md).

**Done — bronze.** Raw daily bars from Alpaca and Yahoo, one append-only
Delta table per vendor, one row per vendor response, stored as received:

```bash
# one trading day (what the daily Airflow task will run)
docker compose exec lakehouse python -m ingestion.bars.load_bronze --source alpaca --start 2026-10-05 --end 2026-10-05
docker compose exec lakehouse python -m ingestion.bars.load_bronze --source yahoo  --start 2026-10-05 --end 2026-10-05
```

- Securities come from the security master's current rows, so every bronze
  row is tagged with `security_id` when it's written.
- Alpaca is called through its REST API with `feed=sip` and
  `adjustment=raw` on every request; the payload is the response text,
  untouched. Yahoo is stored as yfinance's full output plus the library
  version.
- A range with no NYSE sessions (weekend, holiday) exits 0 with nothing to
  fetch. Any failed security exits 1; the rest are still written.
- Each run writes one file (`coalesce(1)`), so batch never needs `OPTIMIZE`.

`notebooks/02_lakehouse_tables.ipynb` shows every table in the lakehouse —
row counts, files, schemas, commit history — and the two vendors side by
side.

**Next:** silver (typed, deduplicated, `MERGE` latest-wins), the 5-year
backfill, then the Airflow DAG.

## Running locally (optional)

Docker is the reference environment. For running notebooks in VS Code and
getting editor autocomplete, a local virtualenv mirrors it:

```bash
uv venv --python 3.11 .venv
uv pip install -r requirements.txt -e . ipykernel
```

Then in VS Code pick `.venv` as the notebook kernel. You need Java (17 is
what the container uses; 19 also works) for Spark. Tables are found at
`<repo>/data/lakehouse` in both environments, so the notebooks read the same
data the container wrote.

Two things keep the environments from drifting: `requirements.txt` pins
pandas/numpy/pyarrow to what the Docker image ships, and `ingestion/spark.py`
starts Spark's Python workers with the driver's own interpreter.

## Maintaining the universe

```bash
# add names: prints lines to paste into UNIVERSE
docker compose exec lakehouse python -m reference_data.resolve_figis NEWT1 NEWT2

# audit: flags renamed tickers and FIGIs that no longer resolve
docker compose exec lakehouse python -m reference_data.resolve_figis --check
```

It prints rather than edits: the universe defines what the whole lakehouse
tracks, so changes go through a reviewed diff. A rename needs no action — the
build picks up the new ticker and records it as history.

## What "done" looks like for Phase 1

- A `security_master` Delta table with one current row per security, keyed
  on composite FIGI, with `issuer_id` populated for every row (BRK.B and BF.B
  included) and `ticker_mismatch = true` for the share-class names.
- A second build reports `0 new, 0 changed`.
- `pytest` passes.
- You can explain why the key is a FIGI and not a ticker *or* a CIK.

## A note on validation

Validated on 2026-10-07: the build ran end to end against live OpenFIGI, SEC
EDGAR, yfinance and Alpaca (paper) data — 80 of 80 resolved, 0 quarantined,
every security known and tradable on Alpaca — and a rerun was a no-op.

That run caught one bug: `alpaca_exchange` was stored as
`AssetExchange.NASDAQ` (`str()` of an enum) instead of `NASDAQ`. Since the
table's history was minutes old, the fix was to delete it and rebuild. On a
table with real history you'd correct the column in place instead: SCD2
versions record changes in the world, and a pipeline bug fix isn't one.

Bronze, validated the same day: one trading day (2026-10-05) loaded from
both vendors, 80 of 80 securities each; a weekend range exited cleanly with
nothing to fetch. Closes agreed to the cent across vendors for all 80;
volumes didn't — Alpaca's is consistently 0.5–0.8% higher, a systematic
difference Phase 4 should treat as expected rather than as an error.

## What's next

- **Phase 2 (remaining):** silver, backfill, and orchestration by
  **Apache Airflow** (decided 2026-10-07).
  Airflow over Dagster: it's the most widely deployed orchestrator, it has
  an official Helm chart for Phase 8's Kubernetes deployment, and its
  task-based model makes the dependencies explicit (security master →
  bronze → silver). The cost is that Airflow doesn't know which *tables* a
  task produces the way Dagster's asset model does, so lineage and
  freshness come from Phase 4's quality checks rather than the orchestrator.
- **Phase 3:** a small Alpaca websocket consumer → Redpanda → Spark
  Structured Streaming, landing a live bronze table.
- **Phase 4:** data quality checks (crossed quotes, price gaps, vendor
  divergence) running against both paths.
- **Phase 5 (capstone):** the point-in-time gold layer, with a side-by-side
  demo of a naive query leaking a restated fundamental back in time next to
  the correct as-of query that doesn't.

## Repo layout

```
market-lakehouse/
├── docker-compose.yml          # the whole local dev environment
├── docker/Dockerfile           # Spark + Delta + Jupyter image
├── requirements.txt            # pinned to match the Docker image
├── pyproject.toml              # makes the repo installable (local .venv)
├── pytest.ini
├── .env.example
├── docs/
│   └── phase-2-design.md       # Phase 2 architecture decision record
├── reference_data/
│   ├── universe.py             # the ~80 securities, keyed by FIGI
│   ├── symbology.py            # each vendor's ticker spelling
│   ├── openfigi.py             # OpenFIGI client (batching, rate limits)
│   └── resolve_figis.py        # helper for maintaining the universe
├── ingestion/
│   ├── build_security_master.py
│   ├── inspect_security_master.py
│   ├── security_master_table.py  # schema + read/write
│   ├── scd2.py                 # reusable SCD Type 2 merge + as-of
│   ├── delta_tables.py         # create tables from explicit schemas
│   ├── paths.py                # every table location, one root
│   ├── spark.py                # shared SparkSession (UTC, worker Python)
│   └── bars/
│       ├── fetch.py            # raw vendor responses (Alpaca REST, yfinance)
│       ├── bronze.py           # append-only bronze tables
│       └── load_bronze.py      # CLI: one vendor, one date range
├── tests/                      # offline; synthetic vendor data
├── quality/                    # phase 4
├── notebooks/                  # exploration; commit with outputs cleared
│   ├── 01_security_master_tour.ipynb
│   └── 02_lakehouse_tables.ipynb
└── data/                       # Delta tables land here (gitignored)
```
