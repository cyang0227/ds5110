"""
Load fundamentals data into DuckDB.
"""

import duckdb
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import math

# ===============================
# Path Configuration
# ===============================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

PARQUET_PATH = PROJECT_ROOT / "data/curated/fundamentals/fundamentals_clean.parquet"
DB_PATH = PROJECT_ROOT / "data/warehouse/data.duckdb"

# ===============================
# Load Parquet Data
# ===============================
df = pd.read_parquet(PARQUET_PATH)

# Connect to DuckDB and load data
con = duckdb.connect(DB_PATH)
print("connected to DuckDB:", DB_PATH)

# ================================
# Get symbol -> security_id mapping
# ================================

symbols = tuple(df["symbol"].unique())
placeholders = ",".join(["?"] * len(symbols))

symbol_map = con.execute(
    f"""
    SELECT symbol, security_id
    FROM securities
    WHERE symbol IN ({placeholders})
    """,
    symbols,
).fetchdf()

df = df.merge(symbol_map, on="symbol", how="inner")

missing_count = df["security_id"].isna().sum()
if missing_count > 0:
    print(f"Warning: {missing_count} records have missing security_id after merge.")

df_final = df[[
    "security_id",
    "period_end",
    "period_type",
    "metric",
    "value",
]].copy()

# ==========================================
# Remove quarterly Q4 when annual exists
# ==========================================
df_final["period_type"] = df_final["period_type"].str.lower()

# sort so that annual comes last (will be kept by drop_duplicates)
df_final = df_final.sort_values(
    by=["period_type"], 
    key=lambda s: s == "annual"
)

# drop duplicates: keep annual if conflict
df_final = df_final.drop_duplicates(
    subset=["security_id", "period_end", "metric"],
    keep="last"
)

# ================================
# Insert Data into DuckDB
# ================================
con.register("fund_df", df_final)

print("Starting UPSERT with transaction...\n")

# Convert df_final to chunks
CHUNK_SIZE = 5000
n_rows = len(df_final)
n_chunks = math.ceil(n_rows / CHUNK_SIZE)

print(f"Total rows: {n_rows}, Chunk size: {CHUNK_SIZE}, Total chunks: {n_chunks}\n")

con.execute("BEGIN TRANSACTION;")

# 1) Delete overlapping rows once
print("Deleting overlapping rows...")
con.execute("""
    DELETE FROM fundamentals
    WHERE (security_id, period_end, metric) IN (
        SELECT security_id, period_end, metric
        FROM fund_df
    );
""")

# 2) Insert in chunks
print("Inserting new rows...")

for i in tqdm(range(n_chunks), desc="Insert Progress", unit="chunk"):
    chunk = df_final.iloc[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE]

    con.register("chunk_df", chunk)

    con.execute("""
                INSERT OR REPLACE INTO fundamentals
                SELECT security_id, period_end, period_type, metric, value
                FROM chunk_df;
                """)
    
con.execute("COMMIT;")

total = con.execute("SELECT COUNT(*) FROM fundamentals;").fetchone()[0]
print(f"\nUPSERT completed. Total records in fundamentals table: {total}")

con.close()
print("DuckDB connection closed.")