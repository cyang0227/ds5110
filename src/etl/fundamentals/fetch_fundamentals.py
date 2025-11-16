"""
fetch fundamentals from Financial Modeling Prep (FMP)
Premium Plan 750 Calls/Minute, 50GB/Month
https://financialmodelingprep.com/developer/docs/pricing/
----------------------------------------------------------
"""

import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm

API_KEY = "kETMJlCxls8W8I8b6OLtnt0qgph8Ek24" # demo key; replace with your own for heavy use, premium key only, I know to remove it later
BASE_URL = "https://financialmodelingprep.com/stable"

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
def fetch_income(symbol, period):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/income-statement?symbol={symbol}&limit=10{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_balance(symbol, period):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/balance-sheet-statement?symbol={symbol}&limit=10{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_cashflow(symbol, period):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/cash-flow-statement?symbol={symbol}&limit=10{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


def fetch_enterprise(symbol, period):
    per = "" if period == "annual" else "&period=quarter"
    url = f"{BASE_URL}/enterprise-values?symbol={symbol}&limit=10{per}&apikey={API_KEY}"
    return normalize_fmp_response(requests.get(url).json())


# ============================================================
# Build fundamentals for single symbol
# ============================================================
def fetch_fundamentals_for_symbol(symbol, period):
    print(f"Fetching {symbol} ({period})...")

    inc = to_df(fetch_income(symbol, period), INCOME_FIELDS, symbol)
    bal = to_df(fetch_balance(symbol, period), BALANCE_FIELDS, symbol)
    cas = to_df(fetch_cashflow(symbol, period), CASHFLOW_FIELDS, symbol)
    ent = to_df(fetch_enterprise(symbol, period), ENTERPRISE_FIELDS, symbol)

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


# ============================================================
# Main
# ============================================================
def main():

    # 1. Load S&P500 symbols
    ROOT = Path(__file__).resolve().parent.parent.parent.parent
    SP500_FILE = ROOT / "data/raw/S&P500.csv"
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

    # 2. Output folders
    output_dir = ROOT / "data/raw/fundamentals/fmp"
    output_dir.mkdir(parents=True, exist_ok=True)

    annual_frames = []
    quarter_frames = []

    # 3. Fetch fundamentals
    print("\nFetching annual fundamentals...")
    for sym in tqdm(symbols):
        annual_frames.append(fetch_fundamentals_for_symbol(sym, "annual"))

    print("\nFetching quarterly fundamentals...")
    for sym in tqdm(symbols):
        quarter_frames.append(fetch_fundamentals_for_symbol(sym, "quarter"))

    # 4. Combine
    df_annual = pd.concat(annual_frames, ignore_index=True)
    df_quarter = pd.concat(quarter_frames, ignore_index=True)

    # 5. Filter data from 2017 onwards
    df_annual = df_annual[df_annual["year"] >= 2017]
    df_quarter = df_quarter[df_quarter["year"] >= 2017]

    df_annual.to_parquet(output_dir / "fundamentals_annual.parquet", index=False)
    df_quarter.to_parquet(output_dir / "fundamentals_quarter.parquet", index=False)

    print("\nSamples:")
    print(df_annual.head())
    print(df_quarter.head())

    print(f"\nSaved {len(df_annual)} annual rows and {len(df_quarter)} quarterly rows.")


if __name__ == "__main__":
    main()
