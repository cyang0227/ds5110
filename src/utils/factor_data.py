"""
Shared data-loading helpers for factor calculations.

Current utilities:
    - load_price_history: fetch and sanitize price history from DuckDB.
    - load_fundamentals_wide: fetch fundamentals and pivot.
    - load_prices_with_fundamentals: unified loader.
"""

from __future__ import annotations

from typing import Iterable, List

import duckdb
import pandas as pd


# ===================================================
# Load Prices data to be used in Momentum Calculation
# ===================================================
def load_price_history(
    con: duckdb.DuckDBPyConnection,
    *,
    price_col: str = "adj_close",
    table_name: str = "prices",
    extra_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """
    Fetch price history from DuckDB and apply standard cleaning steps.
    """

    extra_cols: List[str] = list(extra_columns or [])
    required = {"security_id", "trade_date", price_col}
    if set(extra_cols) & required:
        raise ValueError("extra_columns should not repeat required columns.")

    select_cols = ["security_id", "trade_date", price_col] + extra_cols
    select_clause = ", ".join(select_cols)

    df = con.execute(
        f"""
        SELECT {select_clause}
        FROM {table_name}
        WHERE {price_col} IS NOT NULL
        ORDER BY security_id, trade_date
        """
    ).fetchdf()

    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise KeyError(f"Missing required columns from {table_name}: {missing}")

    # [FIX 1] Ensure trade_date is datetime64
    df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
    
    # [FIX 2] Force security_id to int64 to match DuckDB BIGINT
    df["security_id"] = df["security_id"].astype("int64")

    df = (
        df.dropna(subset=[price_col])
        .drop_duplicates(subset=["security_id", "trade_date"])
        .sort_values(["security_id", "trade_date"])
        .reset_index(drop=True)
    )

    if df.empty:
        raise ValueError(f"No price data available in {table_name}.")

    return df

# ===================================================================
# Load Fundamentals and Prices Data to be used in factor calculation
# ===================================================================


# ====================================
# Load Fundamentals -> pivot wide form
# ====================================
def load_fundamentals_wide(
        con: duckdb.DuckDBPyConnection,
        metrics: list[str],
        table_name: str = "fundamentals",
) -> pd.DataFrame:
    """
    Load fundamentals (in long form) and pivot into wide format:
        security_id | period_end | metric1 | metric2 | ...
    """
    placeholders = ", ".join(["?"] * len(metrics))

    df = con.execute(
        f"""
        SELECT security_id, period_end, metric, value
        FROM {table_name}
        WHERE metric IN ({placeholders})
        ORDER BY security_id, period_end
        """,
        metrics,
    ).fetch_df()

    if df.empty:
        raise ValueError(f"No fundamentals for metrics={metrics}")
    
    # [FIX 3] Force security_id to int64 BEFORE pivoting
    df["security_id"] = df["security_id"].astype("int64")

    df_wide = (
        df.pivot_table(
            index=["security_id", "period_end"],
            columns="metric",
            values="value",
            aggfunc="last"
        ).reset_index()
    )
    df_wide.columns.name = None
    
    if "period_end" in df_wide.columns:
        df_wide["period_end"] = pd.to_datetime(df_wide["period_end"])

    return df_wide

# ===============================================
# Merge prices and fundamentals (as-of join)
# ===============================================
def merge_prices_and_fundamentals(
    df_prices: pd.DataFrame,
    df_fund: pd.DataFrame,
) -> pd.DataFrame:
    """
    Perform backward ASOF join per security_id.
    """
    df_prices["trade_date"] = pd.to_datetime(df_prices["trade_date"])
    df_fund["period_end"] = pd.to_datetime(df_fund["period_end"])
    results = []

    # process security by security
    # Grouping by security_id works safely now that both are int64
    for sid, dfp in df_prices.groupby("security_id"):
        dff = df_fund[df_fund["security_id"] == sid]

        if dff.empty:
            results.append(dfp)
            continue
        
        # Sort required by merge_asof
        dfp = dfp.sort_values("trade_date").reset_index(drop=True)
        dff = dff.sort_values("period_end").reset_index(drop=True)

        # Merge logic
        try:
            merged = pd.merge_asof(
                dfp,
                dff,
                left_on="trade_date",
                right_on="period_end",
                direction="backward",
                allow_exact_matches=True,
            )
            results.append(merged)
        except Exception as e:
            # If merge fails (shouldn't happen with type fixes), return prices only
            print(f"Warning: Merge failed for SID {sid}: {e}")
            results.append(dfp)

    return pd.concat(results, ignore_index=True)

# ================================================================
# Main unified loader: price + fundamentals (with forward-fill)
# ================================================================
def load_prices_with_fundamentals(
    con: duckdb.DuckDBPyConnection,
    metrics: list[str],
    *,
    price_col: str = "adj_close",
) -> pd.DataFrame:
    """
    Load daily prices and fundamentals (wide), then merge them:
        security_id | trade_date | price | eps | revenue | operating_income | ...

    Forward-fill fundamentals between reporting periods.
    """

    # 1) load prices (Types fixed inside)
    df_prices = load_price_history(con, price_col=price_col)

    # 2) load fundamentals pivot (Types fixed inside)
    df_fund = load_fundamentals_wide(con, metrics)

    # 3) backward asof merge
    df_merged = merge_prices_and_fundamentals(df_prices, df_fund)

    # 4) Sort and forward-fill fundamentals per stock
    df_merged = df_merged.sort_values(
        ["security_id", "trade_date"]
    )

    # Identify fundamental columns (exclude key columns)
    fund_cols = [
        c for c in df_merged.columns
        if c not in ["security_id", "trade_date", price_col, "period_end"]
    ]

    # Forward-fill within each stock
    if fund_cols:
        df_merged[fund_cols] = df_merged.groupby("security_id")[fund_cols].ffill()

    return df_merged