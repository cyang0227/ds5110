"""
Momentum factor computation (parameterized).

- Computes total-return momentum over a lookback window while skipping the most recent period.
- Works off DuckDB 'prices' table with columns: security_id, trade_date, adj_close.
- Can optionally persist results to 'factor_values' after auto-registering in 'factor_definitions'.

Author: Chen Yang
"""

from typing import Optional
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime


def _validate_inputs(lookback_months: int, skip_months: int, tpm: int) -> None:
    """Validate user inputs early to fail fast with helpful messages."""
    if not isinstance(lookback_months, int) or lookback_months <= 0:
        raise ValueError("lookback_months must be a positive integer.")
    if not isinstance(skip_months, int) or skip_months < 0:
        raise ValueError("skip_months must be a non-negative integer.")
    if not isinstance(tpm, int) or tpm <= 0:
        raise ValueError("trading_days_per_month must be a positive integer.")


def _momentum_from_prices(arr: np.ndarray, lookback: int, skip: int) -> np.ndarray:
    """
    Vectorized momentum = P[t-skip] / P[t-skip-lookback] - 1
    Returns an array aligned in length with `arr` (NaNs for insufficient history).
    """
    out = np.full_like(arr, np.nan, dtype=float)
    n = arr.shape[0]
    # Only compute when we have enough history: t >= lookback+skip
    # Index math below ensures slices are non-empty before assignment.
    if n > lookback + skip:
        # left side: indices [lookback+skip, n)
        # numerator window: arr[lookback : n - skip]
        # denominator window: arr[: n - lookback - skip]
        num = arr[lookback : n - skip] # P[t - skip]
        den = arr[: n - lookback - skip] # P[t - skip - lookback]
        valid = (den != 0) & np.isfinite(den) & np.isfinite(num)
        tmp = np.full(n - (lookback + skip), np.nan, dtype=float)
        tmp[valid] = num[valid] / den[valid] - 1.0
        out[lookback + skip :] = tmp
    return out


def compute_momentum(
    con: duckdb.DuckDBPyConnection,
    lookback_months: int = 12,
    skip_months: int = 1,
    *,
    save_to_db: bool = False,
    calc_run_id: Optional[str] = None,
    trading_days_per_month: int = 21,
    price_col: str = "adj_close",
) -> pd.DataFrame:
    """
    Compute price momentum factor

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to DuckDB warehouse.
    lookback_months : int, default 12
        Lookback window length in months.
    skip_months : int, default 1
        Months to skip most-recently (e.g., classic momentum_12_1).
    save_to_db : bool, default False
        If True, persist results into factor_values after registering factor_definitions.
    calc_run_id : str, optional
        Run identifier; default uses a timestamp if not provided.
    trading_days_per_month : int, default 21
        Conversion factor from months to trading days.
    price_col : str, default "adj_close"
        Price column to use; must exist in the prices table.

    Returns
    -------
    DataFrame
        Columns: security_id, trade_date, value (momentum), factor_name
        (If save_to_db=True, still returns the in-memory DataFrame used for insertion.)
    """
    _validate_inputs(lookback_months, skip_months, trading_days_per_month)

    # 1) Load and sanitize price data
    df = con.execute(
        f"""
        SELECT security_id, trade_date, {price_col}
        FROM prices
        WHERE {price_col} IS NOT NULL
        ORDER BY security_id, trade_date
        """
    ).fetchdf()

    required_cols = {"security_id", "trade_date", price_col}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise KeyError(f"Missing required columns in prices: {missing}")

    # Ensure correct dtypes and uniqueness
    df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
    df = (
        df.dropna(subset=[price_col])
          .drop_duplicates(subset=["security_id", "trade_date"])
          .sort_values(["security_id", "trade_date"])
          .reset_index(drop=True)
    )
    if df.empty:
        raise ValueError("No price data available after cleaning.")

    # 2) Window parameters in trading days
    lookback = lookback_months * trading_days_per_month
    skip = skip_months * trading_days_per_month

    # 3) Group-wise vectorized momentum
    def _group_calc(x: pd.Series) -> pd.Series:
        arr = x.to_numpy(dtype=float)
        return pd.Series(_momentum_from_prices(arr, lookback, skip), index=x.index)

    # group_keys=False to avoid hierarchical index on return
    df["value"] = df.groupby("security_id", group_keys=False)[price_col].apply(_group_calc)

    # 4) Finalize factor frame
    df_factor = (
        df.loc[~df["value"].isna(), ["security_id", "trade_date", "value"]]
          .reset_index(drop=True)
          .copy()
    )
    factor_name = f"momentum_{lookback_months}m_skip_{skip_months}m"
    df_factor["factor_name"] = factor_name

    # 5) if save_to_db, register and insert
    if save_to_db:
        # Ensure supporting tables exist (will error clearly if missing)
        # Register factor_definitions if needed
        row = con.execute(
            "SELECT factor_id FROM factor_definitions WHERE name = ?",
            [factor_name],
        ).fetchone()

        if row:
            factor_id = row[0]
        else:
            # Generate next available factor_id manually
            factor_id = con.execute("SELECT COALESCE(MAX(factor_id), 0) + 1 FROM factor_definitions").fetchone()[0]

            con.execute(
                """
                INSERT INTO factor_definitions
                (factor_id, name, category, params_json, description, version, expression, source, is_active, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    factor_id,
                    factor_name,
                    "momentum",
                    f'{{"lookback_months": {lookback_months}, "skip_months": {skip_months}}}',
                    f"Price momentum over {lookback_months} months skipping most recent {skip_months} months.",
                    1,
                    "momentum = P[t-skip] / P[t-skip-lookback] - 1",
                    "prices",
                    True,
                    "momentum,price",
                ],
            )
    
        # Prepare DataFrame for insertion
        df_to_insert = df_factor[["security_id", "trade_date", "value"]].copy()
        df_to_insert["factor_id"] = factor_id
        df_to_insert["calc_run_id"] = calc_run_id or f"run_{datetime.now():%Y%m%d_%H%M%S}"
        df_to_insert["updated_at"] = pd.Timestamp.now()

        # Use a transaction for atomicity
        con.begin()
        try:
            con.register("insert_df", df_to_insert)
            con.execute(
                """
                INSERT INTO factor_values (security_id, trade_date, factor_id, value, zscore_cross, rank_cross, calc_run_id, updated_at)
                SELECT security_id, trade_date, factor_id, value, NULL, NULL, calc_run_id, updated_at
                FROM insert_df
                """
            )
            con.commit()
        except Exception:
            con.rollback()
            raise

    return df_factor
