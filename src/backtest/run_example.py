
import sys
from pathlib import Path

# Add project root to PYTHONPATH
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

import duckdb
import pandas as pd
import vectorbt as vbt
from src.utils.factor_data import load_ohlcv_wide, load_factor_values_wide
from src.backtest.engine import FactorBacktester

def run_example():
    # 1. Connect to DB
    db_path = ROOT / "data" / "warehouse" / "data.duckdb"
    con = duckdb.connect(str(db_path))
    print(f"Connected to {db_path}")

    try:
        # 2. Load Data
        print("Loading OHLCV data...")
        # Load all OHLCV columns
        df_ohlcv = load_ohlcv_wide(
            con, 
            start_date="2017-01-01",
            columns=["open", "high", "low", "close", "adj_close", "volume"]
        )
        print(f"Loaded OHLCV data: {df_ohlcv.shape}")
        if not df_ohlcv.empty:
            print("Columns:", df_ohlcv.columns.get_level_values(0).unique())

        print("Loading Factor data (Composite Value)...")
        # Ensure you have run test_value_to_database.py first to generate this factor!
        # If "value_composite" doesn't exist, try "earnings_yield" or whatever you computed.
        factor_name = "value_composite_all" 
        
        # Check if factor exists first
        exists = con.execute("SELECT 1 FROM factor_definitions WHERE name = ?", [factor_name]).fetchone()
        if not exists:
            print(f"Factor '{factor_name}' not found. Using 'earnings_yield' instead.")
            factor_name = "earnings_yield"

        df_factor = load_factor_values_wide(
            con, 
            factor_name=factor_name,
            start_date="2017-01-01"
        )
        print(f"Loaded Factor data: {df_factor.shape}")

        if df_ohlcv.empty or df_factor.empty:
            print("Data is empty. Cannot run backtest.")
            return

        # 3. Run Backtest
        print("Running Backtest...")
        backtester = FactorBacktester(prices=df_ohlcv, factor_values=df_factor)
        
        pf = backtester.run_top_n_strategy(
            top_n=20, 
            rebalance_freq='M' # Monthly rebalance
        )

        # 4. Analyze Results
        print("\n=== Backtest Results ===")
        print(f"Total Return: {pf.total_return():.2%}")
        print(f"Sharpe Ratio: {pf.sharpe_ratio():.2f}")
        print(f"Max Drawdown: {pf.max_drawdown():.2%}")
        print(f"Alpha: {pf.alpha():.2f}")
        print(f"Beta: {pf.beta():.2f}")
        print(f"Total Profit: {pf.total_profit():.2f}")

        # 5. Trade Records
        print("\n=== Trade Records (Top 10) ===")
        trades_df = pf.trades.records_readable
        print(trades_df.head(10))
        
        output_path = ROOT / "data" / "trade_records.csv"
        trades_df.to_csv(output_path)
        print(f"\nFull trade records saved to: {output_path}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

if __name__ == "__main__":
    run_example()
