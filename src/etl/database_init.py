# ======================================================
# DS5110 - create DuckDB Database Schema
# By Chen Yang
# ======================================================

import duckdb
from pathlib import Path

# Define database path (auto-create if missing)
SCRIPT_DIR = Path(__file__).resolve().parent      # /src/etl
PROJECT_ROOT = SCRIPT_DIR.parent.parent           # /ds5110
DB_DIR = PROJECT_ROOT / "data" / "warehouse"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "data.duckdb"

# Connect to DuckDB (creates file if not exists)
con = duckdb.connect(database=str(DB_PATH), read_only=False)
print(f"Connected to DuckDB: {DB_PATH}")

# ======================================================
# 1. Table: securities
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS securities (
    security_id BIGINT PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL,
    name TEXT,
    sector TEXT,
    industry TEXT
);
""")

# ======================================================
# 2. Table: prices
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS prices (
    security_id BIGINT,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    dividends DOUBLE,
    split_ratio DOUBLE,
    PRIMARY KEY (security_id, trade_date),
    FOREIGN KEY (security_id) REFERENCES securities(security_id)
);
""")

# ======================================================
# 3. Table: corporate_actions
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS corporate_actions (
    security_id BIGINT,
    action_date DATE,
    action_type TEXT,
    split_ratio DOUBLE,
    cash_amount DOUBLE,
    PRIMARY KEY (security_id, action_date, action_type),
    FOREIGN KEY (security_id) REFERENCES securities(security_id)
);
""")

# ======================================================
# 4. Table: fundamentals
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS fundamentals (
    security_id BIGINT,
    period_end DATE,
    period_type TEXT,
    metric TEXT,
    value DOUBLE,
    PRIMARY KEY (security_id, period_end, metric),
    FOREIGN KEY (security_id) REFERENCES securities(security_id)
);
""")

# ======================================================
# 5. Table: factor_definitions
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS factor_definitions (
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

# ======================================================
# 6. Table: factor_values
# ======================================================
con.execute("""
CREATE TABLE IF NOT EXISTS factor_values (
    security_id BIGINT,
    trade_date DATE,
    factor_id BIGINT,
    value DOUBLE,
    zscore_cross DOUBLE,
    rank_cross DOUBLE,
    zscore_cross_sector DOUBLE,
    rank_cross_sector DOUBLE,
    calc_run_id TEXT,
    updated_at TIMESTAMP,
    PRIMARY KEY (security_id, trade_date, factor_id),
    FOREIGN KEY (security_id) REFERENCES securities(security_id),
    FOREIGN KEY (factor_id) REFERENCES factor_definitions(factor_id)
);
""")

# Close connection
con.close()
print("Schema created successfully at:", DB_PATH)
