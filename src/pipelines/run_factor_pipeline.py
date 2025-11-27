import sys
from pathlib import Path
import duckdb
from datetime import datetime

# Add src and project root to path
current_dir = Path(__file__).resolve().parent
src_root = current_dir.parent
project_root = src_root.parent

if str(src_root) not in sys.path:
    sys.path.append(str(src_root))
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from factors.momentum.momentum import compute_momentum
from factors.value.value import compute_value_factors

def get_db_connection():
    # Path to database
    # src/pipelines -> src -> .. -> data/warehouse/data.duckdb
    db_path = src_root.parent / "data" / "warehouse" / "data.duckdb"
    return duckdb.connect(str(db_path))

def run_pipeline():
    print(f"[{datetime.now()}] Starting Factor Pipeline...")
    con = get_db_connection()
    
    try:
        # 1. Momentum Factors
        # Calculate 1m, 3m, 6m, 12m momentum (skip 1m)
        lookbacks = [1, 3, 6, 9, 12]
        for lb in lookbacks:
            print(f"[{datetime.now()}] Computing Momentum {lb}M...")
            compute_momentum(
                con,
                lookback_months=lb,
                skip_months=1,
                save_to_db=True,
                calc_run_id=f"pipeline_{datetime.now().strftime('%Y%m%d')}",
                price_col="adj_close"
            )
            
        # 2. Value Factors
        # Composite
        print(f"[{datetime.now()}] Computing Value Composite...")
        compute_value_factors(
            con,
            mode="composite",
            composite_factors="all",
            save_to_db=True,
            calc_run_id=f"pipeline_{datetime.now().strftime('%Y%m%d')}"
        )
        
        # Single Factors
        print(f"[{datetime.now()}] Computing Value Single Factors...")
        sub_factors = [
            "earnings_yield",
            "book_to_market",
            "free_cash_flow_yield",
            "sales_to_price",
            "operating_income_yield",
        ]
        compute_value_factors(
            con,
            mode="single",
            factors=sub_factors,
            save_to_db=True,
            calc_run_id=f"pipeline_{datetime.now().strftime('%Y%m%d')}"
        )
        
        print(f"[{datetime.now()}] Factor Pipeline Completed Successfully.")
        
    except Exception as e:
        print(f"[{datetime.now()}] Pipeline Failed: {e}")
        raise e
    finally:
        con.close()

if __name__ == "__main__":
    run_pipeline()
