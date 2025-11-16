#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb,os
import pandas as pd
import yfinance as yf
import time
from tqdm import tqdm

# Load raw securities csv
sp500 = pd.read_csv("../../S&P500.csv")
gics = pd.read_csv("../../GICS - Industry Standard-2023-kaggle.csv")


# In[2]:


# Clean symbol
sp500.columns = sp500.columns.str.lower()
sp500["symbol"] = sp500["symbol"].str.strip().str.upper()
sp500.head()


# In[3]:


# Fetch GICS industry info via yahoo
records = []
for symbol, name in tqdm(zip(sp500["symbol"], sp500["company"]), total=len(sp500)):
    try:
        info = yf.Ticker(symbol).info
        sector = info.get("sector", None)
        industry = info.get("industry", None)
        records.append({
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "industry": industry
        })
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        records.append({
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "industry": industry
        })
    time.sleep(0.1)  # Rate limit

df = pd.DataFrame(records)
df.head()


# In[4]:


df.to_csv("../../securities_with_gics.csv", index=False)
print(f"Saved securities with GICS to ../../securities_with_gics.csv | rows={len(df):,}")


# In[5]:


# Confirm address in duckdb
con = duckdb.connect("../../data/warehouse/data.duckdb")
print("Absolute path:", os.path.abspath("../../data/warehouse/data.duckdb"))
print("Tables:", con.execute("SHOW TABLES;").fetchdf())
con.close()


# In[6]:


con = duckdb.connect("/home/clsx6609/ds5110/data/warehouse/data.duckdb")
con.execute("CREATE SEQUENCE IF NOT EXISTS seq_security_id START 1;")
con.execute("""
INSERT INTO securities (security_id, symbol, name, sector, industry)
SELECT 
    nextval('seq_security_id') AS security_id,
    i.symbol,
    i.name,
    i.sector,
    i.industry
FROM read_csv_auto('../../securities_with_gics.csv') AS i
LEFT JOIN securities s ON s.symbol = i.symbol
WHERE s.symbol IS NULL;
""")
print(con.execute("SELECT COUNT(*) FROM securities;").fetchdf())
con.close()

