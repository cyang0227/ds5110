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
    for sid, dfp in df_prices.groupby("security_id"):
        dff = df_fund[df_fund["security_id"] == sid]

        if dff.empty:
            results.append(dfp)
            continue
        
        # Sort required by merge_asof
        dfp = dfp.sort_values("trade_date").reset_index(drop=True)
        dff = dff.sort_values("period_end").reset_index(drop=True)

        # Drop security_id from dff to avoid suffixing (security_id_x, security_id_y)
        dff_clean = dff.drop(columns=["security_id"])

        # Merge logic
        try:
            merged = pd.merge_asof(
                dfp,
                dff_clean,
                left_on="trade_date",
                right_on="period_end",
                direction="backward",
                allow_exact_matches=True,
            )
            results.append(merged)
        except Exception as e:
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


# ===============================================
# VectorBT Helpers: Load data in Wide Format
# ===============================================
def load_ohlcv_wide(
    con: duckdb.DuckDBPyConnection,
    start_date: str = "2017-01-01",
    end_date: str | None = None,
    columns: List[str] | None = None,
    security_ids: List[int] | None = None,
) -> pd.DataFrame:
    """
    Load OHLCV data and pivot to wide format with MultiIndex columns.
    
    Returns:
        pd.DataFrame: 
            Index = trade_date
            Columns = MultiIndex(Variable, security_id)
            
    Example:
        df['adj_close'] returns the wide dataframe of adjusted close prices.
    """
    if columns is None:
        columns = ["open", "high", "low", "close", "adj_close", "volume"]

    select_cols = ", ".join(columns)
    
    where_clauses = [f"trade_date >= '{start_date}'"]
    
    if end_date:
        where_clauses.append(f"trade_date <= '{end_date}'")

    if security_ids:
        sids_str = ", ".join(map(str, security_ids))
        where_clauses.append(f"security_id IN ({sids_str})")
    
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT trade_date, security_id, {select_cols}
        FROM prices
        WHERE {where_sql}
        ORDER BY trade_date, security_id
    """
    
    df = con.execute(query).fetchdf()
    
    if df.empty:
        return pd.DataFrame()
        
    # Ensure types
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["security_id"] = df["security_id"].astype("int64")

    # Pivot to MultiIndex wide format
    # resulting columns: (Variable, SecurityID)
    df_wide = df.pivot_table(
        index="trade_date", 
        columns="security_id", 
        values=columns,
        aggfunc="last"
    )
    
    return df_wide


def load_factor_values_wide(
    con: duckdb.DuckDBPyConnection,
    factor_name: str,
    start_date: str = "2017-01-01",
    end_date: str | None = None,
    security_ids: List[int] | None = None,
    value_col: str = "value",
) -> pd.DataFrame:
    """
    Load factor values and pivot to wide format.
    Index = trade_date, Columns = security_id
    
    Args:
        value_col: The column to load (e.g. 'value', 'zscore_cross', 'rank_cross', 'zscore_cross_sector', 'rank_cross_sector')
    """
    # 1. Get factor_id
    fid_row = con.execute(
        "SELECT factor_id FROM factor_definitions WHERE name = ?", 
        [factor_name]
    ).fetchone()
    
    if not fid_row:
        raise ValueError(f"Factor '{factor_name}' not found in factor_definitions.")
    
    factor_id = fid_row[0]

    # 2. Build Query
    where_clauses = [
        f"factor_id = {factor_id}",
        f"trade_date >= '{start_date}'"
    ]
    
    if end_date:
        where_clauses.append(f"trade_date <= '{end_date}'")

    if security_ids:
        sids_str = ", ".join(map(str, security_ids))
        where_clauses.append(f"security_id IN ({sids_str})")

    where_sql = " AND ".join(where_clauses)

    # Validate value_col to prevent SQL injection
    valid_cols = {"value", "zscore_cross", "rank_cross", "zscore_cross_sector", "rank_cross_sector"}
    if value_col not in valid_cols:
        raise ValueError(f"Invalid value_col: {value_col}. Must be one of {valid_cols}")

    query = f"""
        SELECT trade_date, security_id, {value_col}
        FROM factor_values
        WHERE {where_sql}
        ORDER BY trade_date, security_id
    """

    df = con.execute(query).fetchdf()

    if df.empty:
        return pd.DataFrame()

    # 3. Pivot
    df_wide = df.pivot(index="trade_date", columns="security_id", values=value_col)
    df_wide.index = pd.to_datetime(df_wide.index)

    return df_wide


# ===============================================
# UI Helpers
# ===============================================
def get_all_tickers(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Get all available tickers from the securities table.
    Returns DataFrame with columns: security_id, symbol, name, sector, industry
    """
    return con.execute("""
        SELECT security_id, symbol, name, sector, industry 
        FROM securities 
        ORDER BY symbol
    """).fetchdf()


def get_all_factors(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Get all available factors from factor_definitions.
    Returns DataFrame with columns: factor_id, name, category, description
    """
    return con.execute("""
        SELECT factor_id, name, category, description 
        FROM factor_definitions 
        WHERE is_active = true
        ORDER BY category, name
    """).fetchdf()