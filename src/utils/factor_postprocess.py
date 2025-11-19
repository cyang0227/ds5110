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

def postprocess_factor(
    con: duckdb.DuckDBPyConnection,
    df_factor: pd.DataFrame,
    *,
    enable_sector_neutral: bool = True,
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