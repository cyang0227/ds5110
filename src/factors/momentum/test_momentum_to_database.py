import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
print(f"Added {ROOT} to sys.path")

import duckdb
from momentum import compute_momentum

db_path = Path(__file__).resolve().parents[3] / "data" / "warehouse" / "data.duckdb"
symbols = ['AAPL', 'MSFT', 'NVDA']
symbol_list = ",".join([f"'{s}'" for s in symbols])

con = duckdb.connect(db_path)

con.execute(f"""
CREATE OR REPLACE VIEW prices_subset AS
SELECT p.*
FROM prices p
JOIN securities s ON p.security_id = s.security_id

""")

df_factor = compute_momentum(
    con,
    lookback_months=3,
    skip_months=1,
    save_to_db=True,
    calc_run_id="test_momentum_001",
    price_col="adj_close",
)

print(df_factor.groupby("security_id")["trade_date"].agg(['min','max','count']))

print(df_factor.head())

df_factor = compute_momentum(
    con,
    lookback_months=6,
    skip_months=1,
    save_to_db=True,
    calc_run_id="test_momentum_001",
    price_col="adj_close",
)

print(df_factor.groupby("security_id")["trade_date"].agg(['min','max','count']))

print(df_factor.head())

df_factor = compute_momentum(
    con,
    lookback_months=9,
    skip_months=1,
    save_to_db=True,
    calc_run_id="test_momentum_001",
    price_col="adj_close",
)

print(df_factor.groupby("security_id")["trade_date"].agg(['min','max','count']))

print(df_factor.head())

df_factor = compute_momentum(
    con,
    lookback_months=12,
    skip_months=1,
    save_to_db=True,
    calc_run_id="test_momentum_001",
    price_col="adj_close",
)

print(df_factor.groupby("security_id")["trade_date"].agg(['min','max','count']))

print(df_factor.head())

con.close()
