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

from utils.factor_db import FactorMeta, register_and_insert_factor
from utils.factor_data import load_price_history
from utils.factor_postprocess import postprocess_factor


# ---------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------
def _validate_inputs(lookback_months: int, skip_months: int, tpm: int) -> None:
    """Validate user inputs early to fail fast with helpful messages."""
    if not isinstance(lookback_months, int) or lookback_months <= 0:
        raise ValueError("lookback_months must be a positive integer.")
    if not isinstance(skip_months, int) or skip_months < 0:
        raise ValueError("skip_months must be a non-negative integer.")
    if not isinstance(tpm, int) or tpm <= 0:
        raise ValueError("trading_days_per_month must be a positive integer.")


# ---------------------------------------------------------------------
# Core momentum calculation (vectorized)
# ---------------------------------------------------------------------
def _momentum_from_prices(arr: np.ndarray, lookback: int, skip: int) -> np.ndarray:
    """
    Vectorized momentum:
        momentum[t] = P[t-skip] / P[t-skip-lookback] - 1

    The returned array is aligned with `arr`, with NaNs for insufficient history.
    """
    out = np.full_like(arr, np.nan, dtype=float)
    n = arr.shape[0]

    # Only compute when history is sufficient
    if n > lookback + skip:
        # Windows
        num = arr[lookback : n - skip]           # P[t - skip]
        den = arr[: n - lookback - skip]         # P[t - skip - lookback]
        valid = (den != 0) & np.isfinite(num) & np.isfinite(den)

        tmp = np.full(n - (lookback + skip), np.nan, dtype=float)
        tmp[valid] = num[valid] / den[valid] - 1.0

        out[lookback + skip :] = tmp

    return out


# ---------------------------------------------------------------------
# Main computation function
# ---------------------------------------------------------------------
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
    Compute price momentum factor.

    Returns a DataFrame:
        security_id, trade_date, value, zscore_cross, ..., rank_cross_sector, factor_name
    """

    # 1) Validate inputs
    _validate_inputs(lookback_months, skip_months, trading_days_per_month)

    # 2) Load price history
    df = load_price_history(con, price_col=price_col)

    # Convert months → trading days
    lookback = lookback_months * trading_days_per_month
    skip     = skip_months * trading_days_per_month

    # 3) Compute momentum per security
    def _group_calc(x: pd.Series) -> pd.Series:
        arr = x.to_numpy(dtype=float)
        return pd.Series(_momentum_from_prices(arr, lookback, skip), index=x.index)

    df["value"] = df.groupby("security_id", group_keys=False)[price_col].apply(_group_calc)

    # 4) Finalize factor frame
    df_factor = (
        df.loc[~df["value"].isna(), ["security_id", "trade_date", "value"]]
          .reset_index(drop=True)
    )

    factor_name = f"momentum_{lookback_months}m_skip_{skip_months}m"
    df_factor["factor_name"] = factor_name

    # -----------------------------------------------------------------
    # 5) Post-processing: cross-sectional + sector-neutral zscore/rank
    # -----------------------------------------------------------------
    df_factor = postprocess_factor(con, df_factor, enable_sector_neutral=True)

    # -----------------------------------------------------------------
    # 6) Save to DB (UPSERT)
    # -----------------------------------------------------------------
    if save_to_db:
        factor_meta = FactorMeta(
            name=factor_name, 
            category="momentum",
            params={
                "lookback_months": lookback_months,
                "skip_months": skip_months,
                "trading_days_per_month": trading_days_per_month,
                "price_col": price_col,
            },
            description=f"{lookback_months}-month price momentum skipping most recent {skip_months} months.",
            expression=f"(P[t-{skip_months}m] / P[t-{skip_months + lookback_months}m]) - 1",
            source="computed from prices table",
            tags="momentum,price",
        )
        register_and_insert_factor(con, df_factor, factor_meta.to_dict(), calc_run_id)

    return df_factor