import numpy as np
import pandas as pd
import duckdb

def _zscore(x: pd.Series) -> pd.Series:
    """Safe z-score: if std=0, return zeros instead of NaN."""
    mu = x.mean()
    sigma = x.std(ddof=0)
    if sigma == 0 or np.isnan(sigma):
        return pd.Series(np.zeros(len(x)), index=x.index)
    return (x - mu) / sigma

def _winsorize_series(x: pd.Series, limits=(0.01, 0.01)) -> pd.Series:
    lower = x.quantile(limits[0])
    upper = x.quantile(1 - limits[1])
    return x.clip(lower, upper)

def postprocess_factor(
    con: duckdb.DuckDBPyConnection,
    df_factor: pd.DataFrame,
    *,
    enable_sector_neutral: bool = True,
    enable_winsorize: bool = True,
    enable_log: bool = False
) -> pd.DataFrame:
    """
    Unified cross-sectional factor post-processing pipeline.
    
    Required input columns:
        - security_id
        - trade_date
        - value
    
    Outputs:
        - zscore_cross
        - rank_cross
        - zscore_cross_sector (if enabled)
        - rank_cross_sector (if enabled)
    """
    # === Log ===
    if enable_log:
        mask_pos = df_factor["value"] > 0
        df_factor.loc[mask_pos, "value"] = np.log(df_factor.loc[mask_pos, "value"])
        df_factor.loc[~mask_pos, "value"] = np.nan

    # === Winsorize ===
    df_factor["value"] = df_factor.groupby("trade_date")["value"].transform(
        lambda x: _winsorize_series(x)
    ) 

    # === Whole Market Z-score ===
    df_factor["zscore_cross"] = (
        df_factor.groupby("trade_date")["value"].transform(_zscore)
    )

    # === Whole Market rank (%) ===
    df_factor["rank_cross"] = (
        df_factor.groupby("trade_date")["value"]
                            .rank(method="first", pct=True)
    )

    # Join securities' sectors
    if enable_sector_neutral:
        df_sector = con.execute("""
                                SELECT security_id, sector
                                FROM securities
                                """).fetch_df()
        df_factor = df_factor.merge(df_sector, on="security_id", how="left")

        # === Sector Z-score ===
        df_factor["zscore_cross_sector"] = (
            df_factor.groupby(["trade_date", "sector"])["value"]
                     .transform(_zscore)
        )

        # === Sector rank ===
        df_factor["rank_cross_sector"] = (
            df_factor.groupby(["trade_date", "sector"])["value"]
                     .rank(method="first", pct=True)
        )
    
    return df_factor