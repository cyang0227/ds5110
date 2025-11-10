"""
factor_db.py
------------
Utility functions for registering and inserting factor results
into the DuckDB warehouse.

All factor computation scripts (momentum, value, volatility, etc.)
should call `register_and_insert_factor()` after computing
their factor DataFrame.

Schema expectations:
  - factor_definitions(factor_id, name, category, params_json, description, version, expression, source, is_active, tags)
  - factor_values(security_id, trade_date, factor_id, value, zscore_cross, rank_cross, calc_run_id, updated_at)
"""

import duckdb
import pandas as pd
from datetime import datetime
from typing import Dict, Optional

def register_and_insert_factor(
        con: duckdb.DuckDBPyConnection,
        df_factor: pd.DataFrame,
        factor_meta: Dict[str, str],
        calc_run_id: Optional[str] = None,
) -> int:
    """
    Register a factor (if missing) and insert its computed values.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection to the warehouse.
    df_factor : pd.DataFrame
        DataFrame with columns: security_id, trade_date, value.
    factor_meta : Dict[str, str]
        Metadata about the factor:
        - name: str
        - category: str
        - params_json: str
        - description: str
        - version: int
        - expression: str
        - source: str
        - is_active: bool
        - tags: str
    calc_run_id : Optional[str]
        Identifier for this calculation run. If None, a timestamp-based ID is generated.
    
    Returns
    -------
    int
        The factor_id of the registered factor.
    """

    required = {"name", "category", "params_json", "description", "expression", "source", "tags"}
    missing = required - factor_meta.keys()
    if missing:
        raise ValueError(f"Missing required factor_meta keys: {missing}")
    
    # 1) Check if factor already registered
    row = con.execute(
        "SELECT factor_id FROM factor_definitions WHERE name = ?",
        [factor_meta["name"]],
    ).fetchone()

    if row:
        factor_id = row[0]
    else:
        factor_id = con.execute("SELECT COALESCE(MAX(factor_id), 0) + 1 FROM factor_definitions").fetchone()[0]
        con.execute(
            """
            INSERT INTO factor_definitions
            (factor_id, name, category, params_json, description, version, expression, source, is_active, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                factor_id,
                factor_meta["name"],
                factor_meta["category"],
                factor_meta["params_json"],
                factor_meta["description"],
                factor_meta.get("version", 1),
                factor_meta["expression"],
                factor_meta["source"],
                factor_meta.get("is_active", True),
                factor_meta["tags"],
            ],
        )

    # 2) Prepare DataFrame for insertion
    if not {"security_id", "trade_date", "value"}.issubset(df_factor.columns):
        raise ValueError("df_factor must contain columns: security_id, trade_date, value")
    
    df_to_insert = df_factor[["security_id", "trade_date", "value"]].copy()
    df_to_insert["factor_id"] = factor_id
    df_to_insert["calc_run_id"] = calc_run_id or f"run_{datetime.now():%Y%m%d_%H%M%S}"
    df_to_insert["updated_at"] = pd.Timestamp.now()

    # 3) Use a transaction for atomicity
    con.begin()
    try:
        con.register("insert_df", df_to_insert)
        con.execute(
            """
            INSERT INTO factor_values
            (security_id, trade_date, factor_id, value, calc_run_id, updated_at)
            SELECT security_id, trade_date, factor_id, value, calc_run_id, updated_at
            FROM insert_df
            """
        )
        con.commit()
    except Exception as e:
        con.rollback()
        raise RuntimeError(f"Failed to insert factor_values for {factor_meta['name']}: {e}")

    return factor_id