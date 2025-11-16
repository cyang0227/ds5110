"""
DS5110 - One-click Full ETL Script
This script orchestrates the entire ETL (Extract, Transform, Load) process for the DS

Pipeline Order:
1. Initialize DuckDB schema
2. Transform S&P 500 securities clean file
3. Load securities into DuckDB

4. Fetch S&P500 prices from Yahoo Finance
5. Transform prices data (raw > curated)
6. Load prices into DuckDB

7. Fetch fundamentals data from Financial Modeling Prep
8. Transform fundamentals data (wide > long)
9. Load fundamentals into DuckDB

Usage:
    python run_etl.py
    python run_etl.py --only-prices
    python run_etl.py --only-fundamentals
    python run_etl.py --incremental (prices only)

"""

import subprocess
import argparse
import time
from pathlib import Path

# =============================================
# Helper function to run a script
# =============================================
def run_step(name, cmd, cwd):
    print(f"\n======= {name} =======")
    print("CMD:", " ".join(cmd))
    start = time.time()

    try:
        subprocess.check_call(cmd, cwd=cwd)
        elapsed = time.time() - start
        print(f"[OK] {name} completed in {elapsed:.2f} seconds")

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {name} failed with error: {e}")
        raise e
    
# =============================================
# Detect ETL directory (this file's parent)
# =============================================
SCRIPT_DIR = Path(__file__).parent.resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

print(f"ETL Script Directory: {SCRIPT_DIR}")
print(f"Project Root Directory: {PROJECT_ROOT}")

# =============================================
# Main ETL orchestration logic
# =============================================
def run_pipeline(only_prices=False, only_fundamentals=False, incremental=False):
    # Step 1: Initialize DuckDB schema
    run_step(
        "Initialize DuckDB Schema",
        ["python", "database_init.py"],
        cwd=SCRIPT_DIR,
    )

    # Step 2: Load securities into DuckDB
    run_step(
        "Load Securities into DuckDB",
        ["python", "load_securities_to_duckdb.py"],
        cwd=SCRIPT_DIR,
    )

    # =============================================
    # Prices ETL Steps
    # =============================================
    if not only_fundamentals:
        print("\n===== Prices ETL Steps =====")

        price_cmd = ["python", "fetch_sp500_prices.py"]
        if incremental:
            price_cmd.append("--incremental")
        
        run_step(
            "Fetch S&P500 Prices",
            price_cmd,
            cwd=SCRIPT_DIR,
        )

        run_step(
            "Transform Prices (Raw to Curated)",
            ["python", "transform_prices.py"],
            cwd=SCRIPT_DIR,
        )

        run_step(
            "Load Prices into DuckDB",
            ["python", "load_prices_to_duckdb.py"],
            cwd=SCRIPT_DIR,
        )

    # =============================================
    # Fundamentals ETL Steps
    if not only_prices:
        print("\n===== Fundamentals ETL Steps =====")

        run_step(
            "Fetch Fundamentals Data",
            ["python", "fetch_fundamentals.py"],
            cwd=SCRIPT_DIR,
        )

        run_step(
            "Transform Fundamentals Data (Wide to Long)",
            ["python", "transform_fundamentals.py"],
            cwd=SCRIPT_DIR,
        )

        run_step(
            "Load Fundamentals into DuckDB",
            ["python", "load_fundamentals_to_duckdb.py"],
            cwd=SCRIPT_DIR,
        )

    print("\nETL Pipeline Completed Successfully!")

# =============================================
# Command-line interface
# =============================================
def main():
    parser = argparse.ArgumentParser(description="Run the full ETL pipeline.")
    parser.add_argument(
        "--only-prices",
        action="store_true",
        help="Run only the prices ETL steps.",
    )
    parser.add_argument(
        "--only-fundamentals",
        action="store_true",
        help="Run only the fundamentals ETL steps.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run incremental fetch for prices only.",
    )

    args = parser.parse_args()

    run_pipeline(
        only_prices=args.only_prices,
        only_fundamentals=args.only_fundamentals,
        incremental=args.incremental,
    )

if __name__ == "__main__":
    main()

      