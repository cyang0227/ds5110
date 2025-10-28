# Project Directory Structure

```
ds5110/
├── data/
│ ├── raw/ # Immutable raw data (Parquet snapshots)
│ │ └── source=yahoo/year=2025/month=10/*.parquet
│ ├── curated/ # Cleaned and normalized datasets
│ └── marts/ # Factor and backtesting layer for analysis / Streamlit
│
├── src/ # 💻 Core source code
│ ├── etl/ # Extract / Transform / Load
│ │ ├── extract_yfinance.py
│ │ ├── extract_fundamentals.py
│ │ ├── extract_corp_actions.py
│ │ ├── transform_prices.py
│ │ ├── load_duckdb.py
│ │ └── validate_schema.py
│ │
│ ├── factors/ # Factor computation logic
│ │ ├── momentum.py
│ │ ├── value.py
│ │ ├── quality.py
│ │ └── zscore_utils.py
│ │
│ ├── backtest/ # Portfolio construction and backtesting
│ │ ├── portfolio.py
│ │ ├── rebalance.py
│ │ ├── performance.py
│ │ └── regression_model.py # optional: linear regression weighting
│ │
│ ├── ui/ # Streamlit web interface
│ │ ├── app.py
│ │ ├── pages/
│ │ │ ├── overview.py
│ │ │ ├── factor_explorer.py
│ │ │ └── backtest_dashboard.py
│ │
│ └── utils/ # Shared utilities
│ ├── db.py
│ ├── logging_utils.py
│ └── config.py
│
├── notebooks/ # Jupyter notebooks for exploration
│ ├── factor_experiments.ipynb
│ └── duckdb_queries.ipynb

---

## 📘 Description

- **`data/`** — Data lake layers following a `raw → curated → marts` hierarchy.  
  Used for reproducibility and incremental updates.

- **`src/etl/`** — Ingestion and transformation scripts for OHLCV, fundamentals, and corporate actions.

- **`src/factors/`** — Implements core factor logic (momentum, value, quality) and z-score normalization.

- **`src/backtest/`** — Portfolio construction, rebalancing logic, and performance evaluation.

- **`src/ui/`** — Streamlit front-end for factor exploration and portfolio backtesting.

- **`src/utils/`** — Configuration, database helpers, and common logging functions.

- **`notebooks/`** — For exploratory data analysis, validation, and prototyping.

---