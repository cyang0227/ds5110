#!/usr/bin/env python
# coding: utf-8

# ### Price Data Cleaning & Transformation
# Chen Yang

# In[ ]:


import os
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

RAW_DIR = Path("../../data/raw/price/source=yahoo")
CURATED_DIR = Path("../../data/curated/prices/")
CURATED_DIR.mkdir(parents=True, exist_ok=True)

print("RAW DIR:     ", RAW_DIR.resolve())
print("CURATED DIR: ", CURATED_DIR.resolve())


# In[2]:


all_files = sorted([p for p in RAW_DIR.rglob("*.parquet") if "/tmp/" not in str(p).replace("\\", "/")])
print(f"Found {len(all_files)} parquet files")

# preview first 5 files
all_files[:5]


# In[3]:


dfs = []

for f in tqdm(all_files, desc="Loading parquet files"):
    try:
        df_part = pd.read_parquet(f)
        dfs.append(df_part)
    except Exception as e:
        print(f"[WARN] failed to read {f}: {e}")

if not dfs:
    raise ValueError("No dataframes were loaded. Exiting.")

df = pd.concat(dfs, ignore_index=True)
print(f"Loaded rows: {len(df):,}, symbols: {df['symbol'].nunique() if 'symbol' in df.columns else 'N/A'}")
df.head()


# In[4]:


# Clean the dataframe

#1) check required columns
required = ["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "dividends", "split_ratio"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")

#2) drop duplicates
df = df.drop_duplicates(subset=["symbol", "trade_date"])

#3) drop rows with missing price or illegal values
df = df.dropna(subset=["close", "adj_close", "trade_date", "symbol"])
df = df[df["close"].astype(float) > 0]

#4) normalize symbols to uppercase
df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

# Show sample cleaned data
df


# In[5]:


#5) convert trade_date to date and sort
df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
df = df.sort_values(by=["symbol", "trade_date"]).reset_index(drop=True)

df.info()
df.head(100)


# In[6]:


#6) remove rows where volume is 0 or missing
mask_error = df["volume"].isna() | (df["volume"] == 0)
removed = mask_error.sum()
df = df[~mask_error].copy()
print(f"Removed {removed:,} rows where volume=0 or missing.")


# In[7]:


#7) remove rows with extreme daily returns > 80%
df["_ret"] = df.groupby("symbol")["adj_close"].pct_change()
mask_valid = (
    df["_ret"].between(-0.8, 0.8)
    | df["_ret"].isna()
)
df = df[mask_valid].copy()
df.drop(columns=["_ret"], inplace=True)


# In[8]:


# Visualize a sample symbol
import matplotlib.pyplot as plt

sample_symbol = "NVDA"
s = df[df["symbol"] == sample_symbol]
plt.figure(figsize=(9,4))
plt.plot(s["trade_date"], s["adj_close"], label=sample_symbol, color="green")
plt.title(f"{sample_symbol} Adjusted Close")
plt.xlabel("Date"); plt.ylabel("Price"); plt.grid(True); plt.show()


# In[9]:


#8) save cleaned data
output_path = CURATED_DIR / "prices_clean.parquet"
df.to_parquet(output_path, index=False)
print(f"Saved: {output_path}  | rows={len(df):,} | symbols={df['symbol'].nunique()}")


# In[10]:


summary = (
    df.agg(
        rows=("symbol","size"),
        symbols=("symbol","nunique")
    )
    .T
)
summary["start_date"] = df["trade_date"].min()
summary["end_date"]   = df["trade_date"].max()
summary

