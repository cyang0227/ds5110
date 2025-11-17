"""
DS5110 - One-click Full ETL Script
This script orchestrates the entire ETL (Extract, Transform, Load) process for the DS

Pipeline Order:
1. Initialize DuckDB schema
2. Load securities into DuckDB

3. Fetch S&P500 prices from Yahoo Finance
4. Transform prices data (raw > curated)
5. Load prices into DuckDB

6. Fetch fundamentals data from Financial Modeling Prep
7. Transform fundamentals data (wide > long)
8. Load fundamentals into DuckDB

Usage:
    python run_etl.py (full ETL)
    python run_etl.py --only-prices
    python run_etl.py --only-fundamentals 
    python run_etl.py --incremental (incremental prices + fundamentals)
    python run_etl.py --only-prices --incremental (incremental prices only)
    python run_etl.py --only-fundamentals --incremental (incremental fundamentals only)
    python run_etl.py --sync-s3 --s3-bucket your-bucket [--s3-prefix project] [--aws-profile default]

"""

import subprocess
import argparse
import time
from pathlib import Path
from typing import Optional

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
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CURATED_DATA_DIR = DATA_DIR / "curated"
WAREHOUSE_DATA_DIR = DATA_DIR / "warehouse"

print(f"ETL Script Directory: {SCRIPT_DIR}")
print(f"Project Root Directory: {PROJECT_ROOT}")


def build_s3_uri(bucket: str, prefix: str, suffix: str) -> str:
    safe_prefix = prefix.strip("/")
    key = suffix.strip("/")
    if safe_prefix:
        return f"s3://{bucket}/{safe_prefix}/{key}"
    return f"s3://{bucket}/{key}"


def sync_data_to_s3(bucket: str, prefix: str, profile: Optional[str] = None):
    targets = [
        ("Raw Data", RAW_DATA_DIR, "raw"),
        ("Curated Data", CURATED_DATA_DIR, "curated"),
        ("Warehouse", WAREHOUSE_DATA_DIR, "warehouse"),
    ]

    for label, path, suffix in targets:
        if not path.exists():
            print(f"[WARN] Skipping {label}: {path} does not exist.")
            continue
        dest = build_s3_uri(bucket, prefix, suffix)
        cmd = ["aws", "s3", "sync", str(path), dest]
        if profile:
            cmd.extend(["--profile", profile])
        run_step(
            f"Sync {label} to S3 ({dest})",
            cmd,
            cwd=PROJECT_ROOT,
        )

# =============================================
# Main ETL orchestration logic
# =============================================
def run_pipeline(
    only_prices=False,
    only_fundamentals=False,
    incremental=False,
    sync_s3=False,
    s3_bucket: Optional[str] = None,
    s3_prefix: str = "",
    aws_profile: Optional[str] = None,
):

    # Summarize Mode
    print("\n=============== ETL MODE ===============")
    print(f" only-prices:{only_prices}")
    print(f" only-fundamentals:{only_fundamentals}")
    print(f" incremental:{incremental}")
    print(f" sync-s3:{sync_s3}")
    print("========================================\n")

    # =============================================
    # Full ETL Steps
    # =============================================
    if not (only_prices or only_fundamentals or incremental):
        print("\n===== Full ETL Pipeline =====")

        run_step(
            "Initialize DuckDB Schema",
            ["python", "database_init.py"],
            cwd=SCRIPT_DIR,
        )

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

        fund_cmd = ["python", "fetch_fundamentals.py"]
        if incremental:
            fund_cmd.append("--incremental")

        run_step(
            "Fetch Fundamentals Data",
            fund_cmd,
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

    if sync_s3:
        if not s3_bucket:
            raise ValueError("--s3-bucket is required when --sync-s3 is enabled.")
        print("\n===== S3 Sync =====")
        sync_data_to_s3(s3_bucket, s3_prefix, aws_profile)

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
        help="Run incremental fetch for both prices and fundamentals.",
    )
    parser.add_argument(
        "--sync-s3",
        action="store_true",
        help="After ETL, sync raw/curated/warehouse data folders to S3.",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        help="Destination S3 bucket when --sync-s3 is enabled.",
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="project",
        help="Key prefix inside the bucket for S3 sync (default: project).",
    )
    parser.add_argument(
        "--aws-profile",
        type=str,
        help="AWS CLI profile to use for S3 sync.",
    )

    args = parser.parse_args()

    run_pipeline(
        only_prices=args.only_prices,
        only_fundamentals=args.only_fundamentals,
        incremental=args.incremental,
        sync_s3=args.sync_s3,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        aws_profile=args.aws_profile,
    )

if __name__ == "__main__":
    main()
