#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
from pathlib import Path

# In[3]:


SCRIPT_DIR = Path(__file__).resolve().parent          # /ds5110/src/etl
PROJECT_ROOT = SCRIPT_DIR.parent.parent               # /ds5110

DB_PATH = PROJECT_ROOT / "data/warehouse/data.duckdb"
PARQUET_PATH = PROJECT_ROOT / "data/curated/prices/prices_clean.parquet"

print("DB PATH:", DB_PATH)
print("PARQUET:", PARQUET_PATH)


con = duckdb.connect(DB_PATH)

df = pd.read_parquet(PARQUET_PATH)

print(f"Loaded prices parquet: {len(df):,} rows, {df['symbol'].nunique()} symbols")

df = df[[
    "symbol", "trade_date", "open", "high",
    "low", "close", "adj_close", "volume"
]]

df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

# map symbols to security_id

con = duckdb.connect(str(DB_PATH))
symbol_map = con.execute(
    """
    SELECT symbol, security_id FROM securities
    """
).fetchdf()
df = df.merge(symbol_map, on="symbol", how="left")

df = df[["security_id", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]]
df = df.sort_values(["security_id", "trade_date"])


con.register("df", df)
con.execute("BEGIN TRANSACTION")
con.execute(
    """
    MERGE INTO prices AS t
    USING df AS s
    ON t.security_id = s.security_id
       AND t.trade_date = s.trade_date
    WHEN MATCHED THEN UPDATE SET
        open = s.open,
        high = s.high,
        low = s.low,
        close = s.close,
        adj_close = s.adj_close,
        volume = s.volume
    WHEN NOT MATCHED THEN INSERT (
        security_id, trade_date, open, high, low,
        close, adj_close, volume
    ) VALUES (
        s.security_id, s.trade_date, s.open, s.high, s.low,
        s.close, s.adj_close, s.volume
    );
""")
con.execute("COMMIT")

print(f"Successfully inserted {len(df):,} rows into 'prices'")
con.close()

