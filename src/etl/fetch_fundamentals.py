"""
fetch fundamentals from Financial Modeling Prep (FMP)
Premium Plan 750 Calls/Minute, 50GB/Month
https://financialmodelingprep.com/developer/docs/pricing/
----------------------------------------------------------
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from pathlib import Path
from streamlit import empty
from tqdm import tqdm

API_KEY = "kETMJlCxls8W8I8b6OLtnt0qgph8Ek24" # demo key; replace with your own for heavy use, premium key only, I know to remove it later
BASE_URL = "https://financialmodelingprep.com/stable"
DEFAULT_LIMIT = 10
INCREMENTAL_LIMIT = 3

# ============================================================
# Required metrics
# ============================================================

INCOME_FIELDS = [
    "revenue",
    "grossProfit",
    "operatingIncome",
    "netIncome",
    "eps",
]

BALANCE_FIELDS = [
    "totalAssets",
    "totalStockholdersEquity",
    "totalLiabilities",
    "totalDebt",
]

CASHFLOW_FIELDS = [
    "operatingCashFlow",
    "freeCashFlow",
    "capitalExpenditure",
]

ENTERPRISE_FIELDS = [
    "numberOfShares",
    "enterpriseValue",
    "stockPrice",
    "marketCapitalization",
    "minusCashAndCashEquivalents",
    "addTotalDebt",
]

ALL_REQUIRED_FIELDS = (
    INCOME_FIELDS
    + BALANCE_FIELDS
    + CASHFLOW_FIELDS
    + ENTERPRISE_FIELDS
)

# ============================================================
# Paths
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent      # /src/etl
PROJECT_ROOT = SCRIPT_DIR.parent.parent           # /ds5110

SP500_FILE = PROJECT_ROOT / "data/raw/S&P500.csv"
OUTPUT_DIR = PROJECT_ROOT / "data/raw/fundamentals/fmp"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ANNUAL_OUTPUT = OUTPUT_DIR / "fundamentals_annual.parquet"
QUARTER_OUTPUT = OUTPUT_DIR / "fundamentals_quarter.parquet"

# ============================================================
# Incremental helpers
# ============================================================
def detect_latest_date(parquet_path: Path):
    """Return the latest available date stored in the parquet file."""
    if not parquet_path.exists():
        return None
    try:
        df = pd.read_parquet(parquet_path, columns=["date"])
    except Exception as exc:  # pragma: no cover
        tqdm.write(f"[WARN] Unable to read {parquet_path.name}: {exc}")
        return None

    series = pd.to_datetime(df["date"], errors="coerce")
    if series.isna().all():
        return None

    latest = series.max()
    if pd.isna(latest):
        return None

    tqdm.write(f"Detected latest stored date for {parquet_path.name}: {latest.date()}")
    return latest


def filter_newer_than(df: pd.DataFrame, latest_ts):
    """Keep only rows newer than the provided timestamp."""
    if latest_ts is None or df.empty:
        return df
    dt_series = pd.to_datetime(df["date"], errors="coerce")
    return df[dt_series > latest_ts]


def load_existing_data(parquet_path: Path):
    if not parquet_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(parquet_path)
    except Exception as exc:  # pragma: no cover
        tqdm.write(f"[WARN] Unable to load existing data ({parquet_path}): {exc}")
        return pd.DataFrame()


def combine_with_existing(existing: pd.DataFrame, new_rows: pd.DataFrame):
    if existing.empty:
        return new_rows.reset_index(drop=True)
    if new_rows.empty:
        tqdm.write("No new fundamentals detected; keeping existing dataset.")
        return existing
    combined = pd.concat([existing, new_rows], ignore_index=True)
    # Deduplicate using natural keys
    combined = combined.drop_duplicates(subset=["symbol", "date", "period"], keep="last")
    return combined.reset_index(drop=True)

# ============================================================
# Normalizer: ensure FMP response is list-of-dicts
# ============================================================
def normalize_fmp_response(data):
    """Convert FMP response to list-of-dicts safely."""
    if data is None:
        return []
    if isinstance(data, dict):   # error or malformed
        return []
    if isinstance(data, list):
        return data
    return []


# ============================================================
# Convert response to safe DataFrame (guarantees schema)
# ============================================================
def to_df(data, required_fields, symbol):
    """
    Ensure DataFrame ALWAYS contains:
        date, symbol + required_fields
    """
    data = normalize_fmp_response(data)

    df = pd.DataFrame(data)

    # If completely empty → create one empty row with schema
    if df.empty:
        df = pd.DataFrame({
            "date": [None],
            "symbol": [symbol],
            **{f: None for f in required_fields}
        })
        return df

    # Ensure symbol column
    if "symbol" not in df.columns:
        df["symbol"] = symbol

    # Ensure date column
    if "date" not in df.columns:
        df["date"] = None

    # Ensure required fields
    for f in required_fields:
        if f not in df.columns:
            df[f] = None

    return df[["date", "symbol"] + required_fields]


# ============================================================
# FMP API fetchers
# ============================================================
def fetch_income(symbol, period, limit=DEFAULT_LIMIT):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/income-statement?symbol={symbol}&limit={limit}{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_balance(symbol, period, limit=DEFAULT_LIMIT):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/balance-sheet-statement?symbol={symbol}&limit={limit}{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_cashflow(symbol, period, limit=DEFAULT_LIMIT):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/cash-flow-statement?symbol={symbol}&limit={limit}{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_enterprise(symbol, period, limit=DEFAULT_LIMIT):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/enterprise-values?symbol={symbol}&limit={limit}{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


# ============================================================
# Build fundamentals for single symbol
# ============================================================
def fetch_fundamentals_for_symbol(symbol, period, limit):
    tqdm.write(f"Fetching {symbol} fundamentals ({period})...")

    inc = to_df(fetch_income(symbol, period, limit), INCOME_FIELDS, symbol)
    bal = to_df(fetch_balance(symbol, period, limit), BALANCE_FIELDS, symbol)
    cas = to_df(fetch_cashflow(symbol, period, limit), CASHFLOW_FIELDS, symbol)
    ent = to_df(fetch_enterprise(symbol, period, limit), ENTERPRISE_FIELDS, symbol)

    df = inc.merge(bal, on=["date", "symbol"], how="outer")
    df = df.merge(cas, on=["date", "symbol"], how="outer")
    df = df.merge(ent, on=["date", "symbol"], how="outer")

    # Convert numberOfShares → sharesOutstanding
    df["sharesOutstanding"] = df["numberOfShares"]

    # Optional: you may compute marketCap later:
    df["marketCap"] = df["marketCapitalization"]

    df["period"] = "A" if period == "annual" else "Q"
    df["year"] = pd.to_datetime(df["date"], errors="coerce").dt.year

    return df

MAX_RETRY = 3

def fetch_with_retry(symbol, period, limit):
    for i in range(MAX_RETRY):
        df = fetch_fundamentals_for_symbol(symbol, period, limit)
        if not df.empty:
            return df
    tqdm.write(f"[ERROR] Giving up on {symbol} ({period}) after retries.")
    return pd.DataFrame()
    
# ============================================================
# Main
# ============================================================
MAX_WORKERS = 1  # Adjust based on your system and API limits
                 # Better use 1 because of higher rate returns empty data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Fetch only the latest fundamentals and append to existing datasets."
    )
    args = parser.parse_args()

    limit = INCREMENTAL_LIMIT if args.incremental else DEFAULT_LIMIT
    latest_annual = detect_latest_date(ANNUAL_OUTPUT) if args.incremental else None
    latest_quarter = detect_latest_date(QUARTER_OUTPUT) if args.incremental else None
    existing_annual = load_existing_data(ANNUAL_OUTPUT) if args.incremental else pd.DataFrame()
    existing_quarter = load_existing_data(QUARTER_OUTPUT) if args.incremental else pd.DataFrame()

    # 1. Load S&P500 symbols
    sp500 = pd.read_csv(SP500_FILE)

    # Extract Symbol column, drop SPY
    symbols = (
        sp500["Symbol"]
        .dropna()
        .unique()
        .tolist()
    )

    if "SPY" in symbols:
        symbols.remove("SPY")

    print(f"Loaded {len(symbols)} symbols from S&P500 list (excluding SPY).")

    # 2. Fetch ANNUAL fundamentals concurrently
    print("\nFetching annual fundamentals (multi-threaded)...")

    annual_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_with_retry, sym, "annual", limit): sym
                   for sym in symbols}

        pbar = tqdm(total=len(futures), ncols=80)
        for fut in as_completed(futures):
            df = fut.result()
            annual_frames.append(df)
            pbar.update(1)
        pbar.close()

    # 3. Fetch QUARTER fundamentals concurrently
    print("\nFetching quarterly fundamentals (multi-threaded)...")

    quarter_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_with_retry, sym, "quarter", limit): sym
                   for sym in symbols}

        pbar = tqdm(total=len(futures), ncols=80)
        for fut in as_completed(futures):
            df = fut.result()
            quarter_frames.append(df)
            pbar.update(1)
        pbar.close()

    # 4. Combine
    df_annual = pd.concat(annual_frames, ignore_index=True)
    df_quarter = pd.concat(quarter_frames, ignore_index=True)

    # 5. Filter data from 2017 onwards
    df_annual = df_annual[df_annual["year"] >= 2017]
    df_quarter = df_quarter[df_quarter["year"] >= 2017]

    if args.incremental:
        df_annual = filter_newer_than(df_annual, latest_annual)
        df_quarter = filter_newer_than(df_quarter, latest_quarter)

        df_annual = combine_with_existing(existing_annual, df_annual)
        df_quarter = combine_with_existing(existing_quarter, df_quarter)

    # 6. Save to Parquet
    df_annual.to_parquet(ANNUAL_OUTPUT, index=False)
    df_quarter.to_parquet(QUARTER_OUTPUT, index=False)

    print(f"\nSaved {len(df_annual)} annual rows and {len(df_quarter)} quarterly rows.")


if __name__ == "__main__":
    main()
