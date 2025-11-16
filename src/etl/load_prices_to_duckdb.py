#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd


# In[3]:


DB_PATH = "../../../data/warehouse/data.duckdb"
PARQUET_PATH = "../../../data/curated/price/prices_clean.parquet"

con = duckdb.connect(DB_PATH)


# In[4]:


df = pd.read_parquet(PARQUET_PATH)
df.head()


# In[5]:


print(f"Loaded prices parquet: {len(df):,} rows, {df['symbol'].nunique()} symbols")


# In[14]:


cols_keep = ["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]
df = df[cols_keep].copy()
df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date


# In[15]:


symbol_map = con.execute(
    """
    SELECT symbol, security_id FROM securities
    """
).fetchdf()
df = df.merge(symbol_map, on="symbol", how="left")

missing = df[df["security_id"].isna()]["symbol"].unique()
if len(missing) > 0:
    print(f"Missing symbols not found in securities table: {len(missing)}")
    print(missing[:10])



# In[16]:


df = df[["security_id", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]]
df = df.sort_values(["security_id", "trade_date"])


# In[17]:


con.execute("BEGIN TRANSACTION")
con.execute(
    """
    INSERT OR REPLACE INTO prices
    SELECT * FROM df
    """
)
con.execute("COMMIT")


# In[18]:


print(f"Successfully inserted {len(df):,} rows into 'prices'")
con.close()

