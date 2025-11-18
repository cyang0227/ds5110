"""
Value factor computation module.

Implements:
- Earnings Yield (eps / price)
- PE Inverse (1 / pe)
- PB Inverse (1 / pb)
- Size Inverse (1 / market_cap)

Author: Chen Yang
"""

from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from utils.factor_db import FactorMeta, register_and_insert_factor
from utils.factor_data import load_fundamentals_daily

# =====================================
# Helper: Save factor to DuckDB
# =====================================
def _save_factor(con, df, name, description, expression, params=None, calc_run_id=None):
    meta = FactorMeta(
        name=name,
        category="value",
        description=description,
        expression=expression,
        source="fundamentals + prices",
        tags="value",
        params=params or {},
    )
    register_and_insert_factor(con, df, meta.to_dict(), calc_run_id)


# ============================================
# 1. Book-to-Market (BM) = equity / market_cap
# ============================================
def compute_value_bm(con, *, save_to_db=False, calc_run_id=None):
    """
    Book-to-Market value (value_bm)
        BM = total_stockholders_equity / market_capitalization
    Equivalent to inverse PB
    """
    df = load_fundamentals_daily(con, metrics=["total_stockholders_equity", "market_capitalization"])
    
    df["value"] = df["value"] = df["total_stockholders_equity"] / df["market_capitalization"]
    df["value"] = df["value"].replace([np.inf, -np.inf], np.nan)
    out = df.loc[df["value"].notna(), ["security_id", "trade_date", "value"]]

    if save_to_db:
        _save_factor(
            con,
            out,
            name="value_bm",
            description="Book-to-Market = total_stockholders_equity / market_capitalization",
            expression= "total_stockholders_equity / market_capitalization",
            params={"metrics": ["total_stockholders_equity", "market_capitalization"]},
            calc_run_id=calc_run_id
        )
    return out

# =========================================
# 2. Earnings Yield = NetIncome / MarketCap
# =========================================

def compute_value_earnings_yield(con, *, save_to_db=False, calc_run_id=None):
    """
    Earnings Yield (inverse PE):
        EY = net_income / market_capitalization
    """
    df = load_fundamentals_daily(
        con,
        metrics=["net_income", "market_capitalization"]
    )

    df["value"] = df["net_income"] / df["market_capitalization"]
    df["value"] = df["value"].replace([np.inf, -np.inf], np.nan)
    out = df.loc[df["value"].notna(), ["security_id", "trade_date", "value"]]

    if save_to_db:
        _save_factor(
            con,
            out,
            name="value_earnings_yield",
            description="Earnings Yield = net_income / market_capitalization",
            expression="net_income / market_capitalization",
            params={"metrics": ["net_income", "market_capitalization"]},
            calc_run_id=calc_run_id,
        )
    return out



# =========================================
# 3. Free Cash Flow Yield = FCF / MarketCap
# =========================================

def compute_value_fcf_yield(con, *, save_to_db=False, calc_run_id=None):
    """
    Free Cash Flow Yield:
        FCFY = free_cash_flow / market_capitalization
    """
    df = load_fundamentals_daily(
        con,
        metrics=["free_cash_flow", "market_capitalization"]
    )

    df["value"] = df["free_cash_flow"] / df["market_capitalization"]
    df["value"] = df["value"].replace([np.inf, -np.inf], np.nan)
    out = df.loc[df["value"].notna(), ["security_id", "trade_date", "value"]]

    if save_to_db:
        _save_factor(
            con,
            out,
            name="value_fcf_yield",
            description="Free Cash Flow Yield = free_cash_flow / market_capitalization",
            expression="free_cash_flow / market_capitalization",
            params={"metrics": ["free_cash_flow", "market_capitalization"]},
            calc_run_id=calc_run_id,
        )
    return out



# ============================================================
# 4. Sales Yield (Sales-to-Price) = revenue / MarketCap
# ============================================================

def compute_value_sales_yield(con, *, save_to_db=False, calc_run_id=None):
    """
    Sales Yield:
        SY = revenue / market_capitalization
    """
    df = load_fundamentals_daily(
        con,
        metrics=["revenue", "market_capitalization"]
    )

    df["value"] = df["revenue"] / df["market_capitalization"]
    out = df.loc[df["value"].notna(), ["security_id", "trade_date", "value"]]

    if save_to_db:
        _save_factor(
            con,
            out,
            name="value_sales_yield",
            description="Sales Yield = revenue / market_capitalization",
            expression="revenue / market_capitalization",
            params={"metrics": ["revenue", "market_capitalization"]},
            calc_run_id=calc_run_id,
        )
    return out