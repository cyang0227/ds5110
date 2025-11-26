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

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

import duckdb
import pandas as pd


@dataclass
class FactorMeta:
    """
    Helper dataclass to standardize factor metadata before registration.

    Parameters mirror columns in factor_definitions plus params (as dict)
    which will be JSON serialized automatically.
    """

    name: str
    category: str
    description: str
    expression: str
    source: str
    tags: str
    params: Dict[str, Any] = field(default_factory=dict)
    version: int = 1
    is_active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "params_json": json.dumps(self.params, sort_keys=True),
            "description": self.description,
            "version": self.version,
            "expression": self.expression,
            "source": self.source,
            "is_active": self.is_active,
            "tags": self.tags,
        }

def register_and_insert_factor(
        con: duckdb.DuckDBPyConnection,
        df_factor: pd.DataFrame,
        factor_meta: Dict[str, Any],
        calc_run_id: Optional[str] = None,
) -> int:
    """
    Register a factor (if missing) and UPSERT its computed values into factor_values.

    UPSERT = if (security_id, trade_date, factor_id) exists → UPDATE
             else → INSERT

    df_factor must contain:
        security_id, trade_date, value

    Optional columns:
        zscore_cross
        rank_cross
        zscore_cross_sector
        rank_cross_sector
    """

    required = {"name", "category", "params_json", "description", "expression", "source", "tags"}
    missing = required - factor_meta.keys()
    if missing:
        raise ValueError(f"Missing required factor_meta keys: {missing}")

    # ------- 1. Register factor -------
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
            (factor_id, name, category, params_json, description, version,
             expression, source, is_active, tags)
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

    # ------- 2. Prepare DataFrame -------
    required_cols = {"security_id", "trade_date", "value"}
    if not required_cols.issubset(df_factor.columns):
        raise ValueError("df_factor must contain security_id, trade_date, value")

    # Add optional columns (NULL allowed)
    opt_cols = [
        "zscore_cross",
        "rank_cross",
        "zscore_cross_sector",
        "rank_cross_sector",
    ]
    for col in opt_cols:
        if col not in df_factor.columns:
            df_factor[col] = None

    df_to_insert = df_factor[
        [
            "security_id",
            "trade_date",
            "value",
            "zscore_cross",
            "rank_cross",
            "zscore_cross_sector",
            "rank_cross_sector",
        ]
    ].copy()

    df_to_insert["factor_id"] = factor_id
    df_to_insert["calc_run_id"] = calc_run_id or f"run_{datetime.now():%Y%m%d_%H%M%S}"
    df_to_insert["updated_at"] = pd.Timestamp.now()

    con.register("insert_df", df_to_insert)

    # ------- 3. MERGE (UPSERT) -------
    con.begin()
    try:
        con.execute(
            """
            MERGE INTO factor_values t
            USING insert_df s
            ON  t.security_id = s.security_id
            AND t.trade_date  = s.trade_date
            AND t.factor_id   = s.factor_id
            WHEN MATCHED THEN UPDATE SET
                value = s.value,
                zscore_cross = s.zscore_cross,
                rank_cross = s.rank_cross,
                zscore_cross_sector = s.zscore_cross_sector,
                rank_cross_sector = s.rank_cross_sector,
                calc_run_id = s.calc_run_id,
                updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
                security_id, trade_date, factor_id,
                value,
                zscore_cross, rank_cross,
                zscore_cross_sector, rank_cross_sector,
                calc_run_id, updated_at
            ) VALUES (
                s.security_id, s.trade_date, s.factor_id,
                s.value,
                s.zscore_cross, s.rank_cross,
                s.zscore_cross_sector, s.rank_cross_sector,
                s.calc_run_id, s.updated_at
            )
            """
        )
        con.commit()
    except Exception as e:
        con.rollback()
        raise RuntimeError(
            f"UPSERT failed for factor_values ({factor_meta['name']}): {e}"
        )

    return factor_id