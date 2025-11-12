"""
Smoke tests for momentum.py

- Creates a tiny in-memory DuckDB with synthetic prices for 2 securities.
- Validates momentum sign and rough magnitude for monotonic series.
- Checks that the function returns non-empty results without exceptions.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from momentum import compute_momentum


def _make_prices(start: str = "2020-01-01", days: int = 420):
    """Build two synthetic price series:
    S1: upward drift  (should yield positive momentum)
    S2: downward drift (should yield negative momentum)
    """
    dates = pd.bdate_range(start=start, periods=days)  # business days
    s1 = 100 * np.cumprod(1 + 0.0008 + 0.01 * np.random.randn(len(dates)) * 0.0)  # deterministic upward
    s2 = 200 * np.cumprod(1 - 0.0008 + 0.01 * np.random.randn(len(dates)) * 0.0)  # deterministic downward

    df1 = pd.DataFrame({"security_id": 1, "trade_date": dates, "adj_close": s1})
    df2 = pd.DataFrame({"security_id": 2, "trade_date": dates, "adj_close": s2})
    return pd.concat([df1, df2], ignore_index=True)


def main():
    # 1) Build in-memory DuckDB and schema
    con = duckdb.connect(":memory:")

    con.execute("""
        CREATE TABLE prices (
            security_id BIGINT,
            trade_date DATE,
            adj_close DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE factor_definitions (
        factor_id BIGINT PRIMARY KEY,
        name TEXT UNIQUE,
        category TEXT,
        params_json TEXT,
        description TEXT,
        version INTEGER,
        expression TEXT,
        source TEXT,
        is_active BOOLEAN,
        tags TEXT
        );
    """)
    con.execute("""
        CREATE TABLE factor_values (
            security_id BIGINT,
            trade_date DATE,
            factor_id BIGINT,
            value DOUBLE,
            zscore_cross DOUBLE,
            rank_cross INTEGER,
            calc_run_id TEXT,
            updated_at TIMESTAMP
        );
    """)

    # 2) Load synthetic prices
    df_prices = _make_prices()
    con.register("prices_df", df_prices)
    con.execute("INSERT INTO prices SELECT * FROM prices_df;")

    # 3) Run compute_momentum (no DB persist)
    out = compute_momentum(con, lookback_months=12, skip_months=1, save_to_db=False)
    assert not out.empty, "Momentum output should not be empty."

    # 4) Quick sanity at latest date
    latest = out["trade_date"].max()
    snap = out[out["trade_date"] == latest]

    # S1 uptrend should have positive momentum, S2 downtrend negative (if enough history)
    val1 = snap.loc[snap["security_id"] == 1, "value"]
    val2 = snap.loc[snap["security_id"] == 2, "value"]

    if not val1.empty:
        assert val1.iloc[0] > 0, "Uptrend series should have positive momentum."
    if not val2.empty:
        assert val2.iloc[0] < 0, "Downtrend series should have negative momentum."

    # 5) Distribution sanity
    desc = out["value"].describe()
    assert np.isfinite(desc["mean"]), "Mean should be finite."
    assert desc["min"] > -0.99, "Min momentum too extreme; check price zeros/divisions."
    print("Smoke test passed ✅")
    print(out.sort_values(["trade_date", "security_id"]).tail(6))


if __name__ == "__main__":
    main()
