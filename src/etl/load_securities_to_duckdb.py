# ===============================================
# Load Securities with Sector and Industry to DuckDB
# Input: data/raw/S&P500.csv
# Output: data/curated/securities_with_gics.csv
#         data/warehouse/data.duckdb (securities table)
# ===============================================

import duckdb
import pandas as pd
import yfinance as yf
import time
from tqdm import tqdm
from pathlib import Path

# ================================================
# Path Setup (Correct for src/etl)
# ================================================
SCRIPT_DIR = Path(__file__).resolve().parent       # src/etl
PROJECT_ROOT = SCRIPT_DIR.parent.parent            # project root: /ds5110

RAW_SP500 = PROJECT_ROOT / "data/raw/S&P500.csv"
OUTPUT_CSV = PROJECT_ROOT / "data/curated/securities_with_gics.csv"
DB_PATH = PROJECT_ROOT / "data/warehouse/data.duckdb"

print("=== PATH CHECK ===")
print("SCRIPT_DIR:", SCRIPT_DIR)
print("PROJECT_ROOT:", PROJECT_ROOT)
print("RAW_SP500:", RAW_SP500)
print("OUTPUT_CSV:", OUTPUT_CSV)
print("DB_PATH:", DB_PATH)
print("==================")

# ================================================
# Load raw securities csv
# ================================================
sp500 = pd.read_csv(RAW_SP500)

# Clean symbol
sp500.columns = sp500.columns.str.lower()
sp500["symbol"] = sp500["symbol"].str.strip().str.upper()

# ================================================
# Fetch industry data via Yahoo Finance
# ================================================
records = []
for symbol, name in tqdm(zip(sp500["symbol"], sp500["company"]), total=len(sp500)):
    sector, industry = None, None
    try:
        info = yf.Ticker(symbol).info
        sector = info.get("sector")
        industry = info.get("industry")
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

    records.append({
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "industry": industry,
    })

    time.sleep(0.1)  # rate limit

df = pd.DataFrame(records)

# Save to curated
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved curated securities CSV → {OUTPUT_CSV}")

# ================================================
# Insert into DuckDB
# ================================================
con = duckdb.connect(str(DB_PATH))

con.execute("CREATE SEQUENCE IF NOT EXISTS seq_security_id START 1;")

con.execute("""
INSERT INTO securities (security_id, symbol, name, sector, industry)
SELECT 
    nextval('seq_security_id') AS security_id,
    i.symbol,
    i.name,
    i.sector,
    i.industry
FROM read_csv_auto(?) AS i
LEFT JOIN securities s ON s.symbol = i.symbol
WHERE s.symbol IS NULL;
""", [str(OUTPUT_CSV)])

count = con.execute("SELECT COUNT(*) FROM securities;").fetchone()[0]
print(f"Inserted {count} securities into DuckDB.")

con.close()