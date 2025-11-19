"""
Value factor computation (dynamic composite or single mode).

Supports:
    mode = "composite"  → compute custom composite of selected sub-factors
    mode = "single"     → compute individual sub-factors

Composite is computed as the mean of per-date z-scores of the selected sub-factors.

Author: Chen Yang
"""

from typing import Optional, List, Union
import duckdb
import pandas as pd
import numpy as np

from src.utils.factor_data import load_prices_with_fundamentals
from src.utils.factor_postprocess import postprocess_factor
from src.utils.factor_db import FactorMeta, register_and_insert_factor


# ------------------------------------------------------------
# safe zscore
# ------------------------------------------------------------
def _zscore(x: pd.Series) -> pd.Series:
    mu = x.mean()
    sd = x.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series(np.zeros(len(x)), index=x.index)
    return (x - mu) / sd


# ------------------------------------------------------------
# API
# ------------------------------------------------------------
def compute_value_factors(
    con: duckdb.DuckDBPyConnection,
    *,
    mode: str = "composite",   
    composite_factors: Union[str, List[str]] = "all",  
    factors: Optional[List[str]] = None,                  
    save_to_db: bool = False,
    calc_run_id: Optional[str] = None,
    price_col: str = "adj_close",
) -> pd.DataFrame:
    """
    Compute Value factors.

    Parameters
    ----------
    mode : "composite" | "single"
        composite → combine selected sub-factors. A composite factor mixed with sub-factors to combine to a more stable, predictable factor
        single    → return selected individual sub-factors. Every single sub-factor will have a factor_id

    composite_factors : list[str] or "all"
        If mode == composite:
            - "all"  → use all sub-factors
            - list   → custom list of sub-factors

    factors : list[str]
        If mode == single:
            list of sub-factors to compute

    Returns a DataFrame of processed factors, ready to insert into DB.

    Examples:
    ---------
    compute_value_factors(
    con,
    mode="composite",
    composite_factors=["earnings_yield", "book_to_market"],
    save_to_db=True
    )

    compute_value_factors(
    con,
    mode="single",
    factors=["earnings_yield", "sales_to_price"],
    save_to_db=True)
    """

    # ------------------------------------------------------------
    # 1. unify metrics
    # ------------------------------------------------------------
    sub_factors_all = [
        "earnings_yield",
        "book_to_market",
        "free_cash_flow_yield",
        "sales_to_price",
        "operating_income_yield",
    ]

    # ------------------------------------------------------------
    # 2. load price + fundamentals once
    # ------------------------------------------------------------
    metrics = [
        "eps",
        "total_stockholders_equity",
        "market_capitalization",
        "free_cash_flow",
        "revenue",
        "operating_income",
        "enterprise_value",
    ]

    df_all = load_prices_with_fundamentals(
        con,
        metrics=metrics,
        price_col=price_col,
    )

    # ------------------------------------------------------------
    # Filter out security_id = 504
    # ------------------------------------------------------------
    if not df_all.empty and "security_id" in df_all.columns:
        df_all = df_all[df_all["security_id"] != 504].copy()

    # === DEBUG START ===
    print(f"DEBUG: Loaded df_all shape: {df_all.shape}")
    if df_all.empty:
        print("DEBUG: df_all is empty immediately after loading!")
        return pd.DataFrame()

    print("DEBUG: NaNs per column:")
    print(df_all[metrics].isna().sum())
    
    valid_rows = df_all.dropna(subset=["market_capitalization", "eps"])
    print(f"DEBUG: Rows with valid Market Cap and EPS: {len(valid_rows)}")
    # === DEBUG END ===

    price = df_all[price_col]
    mc = df_all["market_capitalization"]
    ev = df_all["enterprise_value"]

    # ------------------------------------------------------------
    # 3. compute raw sub-factors
    # ------------------------------------------------------------
    df_all["earnings_yield"] = np.where(
        (price > 0) & np.isfinite(price),
        df_all["eps"] / price,
        np.nan,
    )

    df_all["book_to_market"] = np.where(
        (mc > 0) & np.isfinite(mc),
        df_all["total_stockholders_equity"] / mc,
        np.nan,
    )

    df_all["free_cash_flow_yield"] = np.where(
        (mc > 0) & np.isfinite(mc),
        df_all["free_cash_flow"] / mc,
        np.nan,
    )

    df_all["sales_to_price"] = np.where(
        (mc > 0) & np.isfinite(mc),
        df_all["revenue"] / mc,
        np.nan,
    )

    df_all["operating_income_yield"] = np.where(
        (ev > 0) & np.isfinite(ev),
        df_all["operating_income"] / ev,
        np.nan,
    )

    # ------------------------------------------------------------
    # 4. composite mode
    # ------------------------------------------------------------
    if mode == "composite":
        # determine which sub-factors to use
        if composite_factors == "all":
            selected = sub_factors_all
        else:
            unknown = set(composite_factors) - set(sub_factors_all)
            if unknown:
                raise ValueError(f"Unknown composite sub-factors: {unknown}")
            selected = composite_factors

        # compute zscore for selected factors
        zcols = []
        for sf in selected:
            zcol = f"{sf}_z"
            df_all[zcol] = df_all.groupby("trade_date")[sf].transform(_zscore)
            zcols.append(zcol)

        # composite = mean of selected z-scores
        df_all["value_composite_dynamic"] = df_all[zcols].mean(
        axis=1,
        skipna=True,
        )

        df_factor = (
            df_all.loc[
                ~df_all["value_composite_dynamic"].isna(),
                ["security_id", "trade_date", "value_composite_dynamic"],
            ]
            .rename(columns={"value_composite_dynamic": "value"})
            .reset_index(drop=True)
        )

        df_factor = df_factor.dropna(subset=["security_id"]).copy()
        df_factor["security_id"] = df_factor["security_id"].astype("int64")

        # dynamic factor name
        if composite_factors == "all":
            factor_name = "value_composite_all"
        else:
            factor_name = "value_composite_" + "_".join(selected)

        df_factor["factor_name"] = factor_name

        # post-process (zscore_cross + sector-neutral)
        df_factor = postprocess_factor(con, df_factor)

        # write to DB
        if save_to_db:
            meta = FactorMeta(
                name=factor_name,
                category="value",
                params={"components": selected},
                description=f"Composite of sub-factors: {selected}",
                expression="mean(z-subfactors)",
                source="fundamentals + prices",
                tags="value,composite",
            )
            register_and_insert_factor(con, df_factor, meta.to_dict(), calc_run_id)

        return df_factor

    # ------------------------------------------------------------
    # 5. single mode
    # ------------------------------------------------------------
    elif mode == "single":
        if factors is None:
            raise ValueError("Must pass factors=[...] in single mode.")

        unknown = set(factors) - set(sub_factors_all)
        if unknown:
            raise ValueError(f"Unknown single sub-factors: {unknown}")

        outputs = []

        for sf in factors:
            tmp = (
                df_all.loc[
                    ~df_all[sf].isna(),
                    ["security_id", "trade_date", sf],
                ]
                .rename(columns={sf: "value"})
                .reset_index(drop=True)
            )
            tmp["factor_name"] = sf

            tmp = tmp.dropna(subset=["security_id"]).copy()
            tmp["security_id"] = tmp["security_id"].astype("int64")

            tmp = postprocess_factor(con, tmp)

            if save_to_db:
                meta = FactorMeta(
                    name=sf,
                    category="value",
                    params={"metric": sf},
                    description=f"Value single-factor: {sf}",
                    expression=sf,
                    source="fundamentals + prices",
                    tags="value,single",
                )
                register_and_insert_factor(con, tmp, meta.to_dict(), calc_run_id)

            outputs.append(tmp)

        return pd.concat(outputs, ignore_index=True)

    # ------------------------------------------------------------
    # 6. Invalid mode
    # ------------------------------------------------------------
    else:
        raise ValueError("mode must be 'composite' or 'single'")
