import sys
from pathlib import Path

# --------------------------------------------------------------------
# Add project root to PYTHONPATH
# --------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT))
print(f"Added {ROOT} to sys.path")

import duckdb
from src.factors.value.value import compute_value_factors

# --------------------------------------------------------------------
# Connect to DuckDB
# --------------------------------------------------------------------
db_path = ROOT / "data" / "warehouse" / "data.duckdb"
con = duckdb.connect(str(db_path))
print(f"Connected to {db_path}")

# --------------------------------------------------------------------
# Example: Composite value factor using ALL subfactors
# --------------------------------------------------------------------
df_factor = compute_value_factors(
    con,
    mode="composite",               # "composite" or "single"
    composite_factors="all",        # or ["earnings_yield","book_to_market"]
    save_to_db=True,
    calc_run_id="test_value_001",
    price_col="adj_close",
)

print("=== Composite Value Factor Loaded to DB ===")
print(df_factor.head())
print(df_factor.groupby("security_id")["trade_date"].agg(["min","max","count"]))

# --------------------------------------------------------------------
# Example: also compute SINGLE value factors
# --------------------------------------------------------------------
df_single = compute_value_factors(
    con,
    mode="single",
    factors=[
        "earnings_yield",
        "book_to_market",
        # "free_cash_flow_yield",
        # "sales_to_price",
        # "operating_income_yield",
    ],
    save_to_db=True,
    calc_run_id="test_value_single_001",
    price_col="adj_close",
)

print("=== Single Value Factors Loaded to DB ===")
print(df_single.head())

# --------------------------------------------------------------------
# Close DB
# --------------------------------------------------------------------
con.close()
print("DuckDB connection closed.")