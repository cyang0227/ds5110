# Project Directory Structure

```
ds5110/
├── data/
│   ├── raw/
│   │   ├── S&P500.csv
│   │   ├── prices/
│   │   │   └── source=yahoo/year=YYYY/month=MM/*.parquet
│   │   └── fundamentals/
│   │       └── fmp/fundamentals_{annual,quarter}.parquet
│   ├── curated/
│   │   ├── prices/prices_clean.parquet
│   │   ├── fundamentals/fundamentals_clean.parquet
│   │   └── securities_with_gics.csv
│   └── warehouse/data.duckdb
│
├── docs/
│   ├── ERD.drawio
│   ├── schema.md
│   └── …
│
├── src/
│   ├── etl/
│   │   ├── run_etl.py
│   │   ├── database_init.py
│   │   ├── load_securities_to_duckdb.py
│   │   ├── fetch_sp500_prices.py
│   │   ├── transform_prices.py
│   │   ├── load_prices_to_duckdb.py
│   │   ├── fetch_fundamentals.py
│   │   ├── transform_fundamentals.py
│   │   ├── load_fundamentals_to_duckdb.py
│   │   ├── transform_prices.ipynb
│   │   └── S&P500.csv (local copy used by ETL)
│   ├── factors/
│   │   └── momentum/
│   │       ├── momentum.py
│   │       └── smoke_test_momentum.py
│   └── utils/
│       └── factor_db.py
│
├── requirements.txt
├── README.md
├── Iteration*.pdf
├── GICS - Industry Standard-2023-kaggle.csv
└── securities_with_gics.csv
```

---

## Data Lake Layout

- **`data/raw/`** — Immutable ingestion zone.  
  - `S&P500.csv` contains the master list of index constituents used to seed the securities dimension.  
  - `prices/source=yahoo/year=YYYY/month=MM/*.parquet` stores Yahoo! Finance OHLCV snapshots partitioned by year/month (written by `fetch_sp500_prices.py`).  
  - `fundamentals/fmp/` holds the raw Financial Modeling Prep pulls split into annual and quarterly parquet sets.

- **`data/curated/`** — Cleaned, schema-stable datasets produced by the transform scripts.  
  - `prices/prices_clean.parquet` is de-duplicated, validated OHLCV data with enforced symbol/date integrity.  
  - `fundamentals/fundamentals_clean.parquet` is the long-format metrics table generated from the normalized financial statements.  
  - `securities_with_gics.csv` enriches the index list with Yahoo! sector/industry tags.

- **`data/warehouse/`** — DuckDB analytical store (`data.duckdb`). Tables include `securities`, `prices`, `fundamentals`, `corporate_actions`, `factor_definitions`, and `factor_values`. All `load_*.py` scripts target this file using upserts.

---

## ETL Pipeline (`src/etl`)

`run_etl.py` orchestrates the entire workflow via CLI flags (`--only-prices`, `--only-fundamentals`, `--incremental`). Each step invokes a dedicated script so the pipeline can be re-run piecemeal during development.

1. **Environment bootstrap**
   - `database_init.py` builds/updates the DuckDB schema (table creation and sequence setup).
   - `load_securities_to_duckdb.py` cleans `data/raw/S&P500.csv`, enriches rows with Yahoo! sector/industry metadata, writes the curated CSV, and seeds the `securities` table.

2. **Prices ingestion**
   - `fetch_sp500_prices.py` pulls through Yahoo! Finance with resume support, writing per-month parquet partitions under `data/raw/prices/source=yahoo/`. Incremental mode derives the next `trade_date` window directly from existing files.
   - `transform_prices.py` consolidates every parquet file, enforces schema rules (dedupe, non-zero volume, outlier filtering), and publishes `prices_clean.parquet`.
   - `load_prices_to_duckdb.py` maps symbols to `security_id` and merges the curated price history into DuckDB’s `prices` table via a `MERGE` statement.

3. **Fundamentals ingestion**
   - `fetch_fundamentals.py` pulls the required income, balance sheet, cash flow, and enterprise fields from the FMP Stable API for both annual and quarterly periods (multi-threaded with retries) and saves them to `data/raw/fundamentals/fmp/`.
   - `transform_fundamentals.py` filters the raw statements to 2017+, selects the latest filing per symbol/year, reshapes the wide metrics into a tidy long format, and generates` `fundamentals_clean.parquet`.
   - `load_fundamentals_to_duckdb.py` performs symbol lookups, trims to the `fundamentals` schema, deletes overlapping rows once, and inserts new records in chunks to keep DuckDB transactions manageable.

4. **Utilities & notebooks**
   - `transform_prices.ipynb` mirrors the price-cleaning logic in notebook form for ad-hoc validation.
   - Local copies of `S&P500.csv` or temporary parquet files sit beside the scripts for quick smoke tests without touching the global `data/` layout.

This modular layout keeps each ETL phase independently executable (e.g., iterate on `transform_*` without re-fetching raw data) while allowing `run_etl.py` to chain everything for end-to-end refreshes.

---

## Analytical Code & Utilities

- **`src/factors/momentum/`** — Momentum factor implementation plus lightweight testers (`smoke_test_momentum.py`, `test_momentum_to_database.py`) and the `test_nvda_momentum_trace.ipynb` notebook for trace debugging.
- **`src/utils/factor_db.py`** — Shared DuckDB helper for factor read/write operations used by downstream analytics.

---

## Documentation & Artifacts

- **`docs/`** — Architectural references (`ERD.drawio`, `schema.md`, `data_partitioning_rule.md`, etc.) that explain storage conventions and field mappings.
- **Root-level CSV/PDFs** — Reference datasets (`GICS - Industry Standard-2023-kaggle.csv`, `securities_with_gics.csv`) and iteration notes used during project checkpoints.

---
