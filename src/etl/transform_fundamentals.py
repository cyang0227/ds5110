#!/usr/bin/env python
# coding: utf-8

# ### 1. Setup and Import

# In[2]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)


# ### 2. Load Raw Data

# In[3]:


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

annual_path = PROJECT_ROOT / "data/raw/fundamentals/fmp/fundamentals_annual.parquet"
quarter_path = PROJECT_ROOT / "data/raw/fundamentals/fmp/fundamentals_quarter.parquet"

dfA = pd.read_parquet(annual_path)
dfQ = pd.read_parquet(quarter_path)

# ### 3. Explore Raw Data

# In[4]:


print("Annual shape:", dfA.shape)
print("Quarter shape:", dfQ.shape)

print("Annual years:", dfA['year'].unique())
print("Quarter years:", dfQ['year'].unique())

dfA['period'].value_counts(), dfQ['period'].value_counts()


# In[5]:


print("Annual symbol count:", dfA['symbol'].nunique())
print("Quarter symbol count:", dfQ['symbol'].nunique())


# ### 4. Normalize Year Column

# In[6]:


dfA['year'] = pd.to_datetime(dfA['date'], errors='coerce').dt.year
dfQ['year'] = pd.to_datetime(dfQ['date'], errors='coerce').dt.year


# ### 5. Filter Data >= 2000

# In[7]:


dfA = dfA[dfA['year'] >= 2000]
dfQ = dfQ[dfQ['year'] >= 2000]

# ### 6. Combine Annual and Quarterly Data
# In[8]:
dfC = pd.concat([dfA, dfQ], ignore_index=True)

df_clean = dfC.sort_values(["symbol", "date"]).reset_index(drop=True)

# ### 9. Transform wide format to long format

# In[12]:


df_wide = df_clean.copy()
df_wide.columns.tolist()


# In[17]:


metric_map = {
    "revenue" : "revenue",
    "grossProfit" : "gross_profit",
    "operatingIncome" : "operating_income",
    "netIncome" : "net_income",
    "eps" : "eps",
    "totalAssets" : "total_assets",
    "totalStockholdersEquity" : "total_stockholders_equity",
    "totalLiabilities" : "total_liabilities",
    "totalDebt" : "total_debt",
    "operatingCashFlow" : "operating_cash_flow",
    "freeCashFlow" : "free_cash_flow",
    "capitalExpenditure" : "capital_expenditure",
    "numberOfShares" : "number_of_shares",
    "enterpriseValue" : "enterprise_value",
    "marketCapitalization" : "market_capitalization",
    "sharesOutstanding" : "shares_outstanding",
}

value_cols = list(metric_map.keys())

# =============================================
# Period_end at long format
# =============================================

df_long = df_wide.copy()
df_long["period_end"] = pd.to_datetime(df_long["date"]).dt.date

df_long["period_type"] = df_long["period"].map({
    "A" : "annual",
    "Q" : "quarterly"
})

if df_long["period_type"].isna().any():
    raise ValueError("Some rows have invalid period value (not A/Q).")

# =============================================
# Melt to long format
# =============================================

df_long = df_long.melt(
    id_vars = ["symbol", "period_end", "period_type"],
    value_vars = value_cols,
    var_name = "metric_raw",
    value_name = "value"
)

df_long["metric"] = df_long["metric_raw"].map(metric_map)
df_long = df_long.dropna(subset=["value"])

print("Long format rows:", len(df_long))


# In[18]:


# =============================================
# Final tidy dataframe
# =============================================

df_out = df_long[[
    "symbol",
    "period_end",
    "period_type",
    "metric",
    "value"
]]

# =============================================
# Save to parquet
# =============================================

OUTPUT_DIR = PROJECT_ROOT / "data/curated/fundamentals/"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
df_out.to_parquet(OUTPUT_DIR / "fundamentals_clean.parquet", index=False)
print("Saved cleaned fundamentals to:", OUTPUT_DIR / "fundamentals_clean.parquet")

