"""
Shared data-loading helpers for factor calculations.

Current utilities:
    - load_price_history: fetch and sanitize price history from DuckDB.
"""

from __future__ import annotations

from typing import Iterable, List

import duckdb
import pandas as pd


def load_price_history(
    con: duckdb.DuckDBPyConnection,
    *,
    price_col: str = "adj_close",
    table_name: str = "prices",
    extra_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """
    Fetch price history from DuckDB and apply standard cleaning steps.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    price_col : str, default "adj_close"
        Price/return column to load from the table.
    table_name : str, default "prices"
        Source table name inside DuckDB.
    extra_columns : Iterable[str], optional
        Additional columns to include in the SELECT list.

    Returns
    -------
    DataFrame
        Cleaned frame sorted by security_id, trade_date.
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

    df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
    df = (
        df.dropna(subset=[price_col])
        .drop_duplicates(subset=["security_id", "trade_date"])
        .sort_values(["security_id", "trade_date"])
        .reset_index(drop=True)
    )

    if df.empty:
        raise ValueError(f"No price data available in {table_name}.")

    return df

