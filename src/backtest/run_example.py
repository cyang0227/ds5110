
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
        # Ensure test_value_to_database.py is run first to generate this factor!
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
        
        results = {}

        # 3.1 Top-N Long-Only (Equal Weight)
        print("Running Top-N Long-Only (Equal Weight)...")
        pf_long_equal = backtester.run_top_n_strategy(
            top_n=20, 
            rebalance_freq='M',
            weighting='equal'
        )
        results['Long-Only (Equal)'] = pf_long_equal

        # 3.2 Top-N Long-Only (Factor Weight)
        print("Running Top-N Long-Only (Factor Weight)...")
        pf_long_factor = backtester.run_top_n_strategy(
            top_n=20, 
            rebalance_freq='M',
            weighting='factor'
        )
        results['Long-Only (Factor)'] = pf_long_factor

        # 3.3 Long-Short (Equal Weight)
        print("Running Long-Short (Equal Weight)...")
        pf_ls_equal = backtester.run_long_short_strategy(
            top_n=20, 
            rebalance_freq='M',
            weighting='equal'
        )
        results['Long-Short (Equal)'] = pf_ls_equal

        # 3.4 Long-Short (Factor Weight)
        print("Running Long-Short (Factor Weight)...")
        pf_ls_factor = backtester.run_long_short_strategy(
            top_n=20, 
            rebalance_freq='M',
            weighting='factor'
        )
        results['Long-Short (Factor)'] = pf_ls_factor

        # 3.5 Sector-Neutral Top-N (using rank_cross_sector)
        print("Running Sector-Neutral Top-10%...")
        # Load rank_cross_sector
        df_rank_sector = load_factor_values_wide(
            con, 
            factor_name=factor_name,
            start_date="2017-01-01",
            value_col="rank_cross_sector"
        )
        if not df_rank_sector.empty:
            bt_sector = FactorBacktester(prices=df_ohlcv, factor_values=df_rank_sector)
            pf_sector = bt_sector.run_top_n_strategy(
                top_n=0.1, # Top 10% per sector
                rebalance_freq='M',
                weighting='equal',
                input_is_rank=True
            )
            results['Sector-Neutral Top-10%'] = pf_sector
        else:
            print("Warning: rank_cross_sector is empty.")

        # 3.6 Threshold Strategy (using zscore_cross)
        print("Running Threshold Strategy (Z-Score > 1.5)...")
        # Load zscore_cross
        df_zscore = load_factor_values_wide(
            con, 
            factor_name=factor_name,
            start_date="2017-01-01",
            value_col="zscore_cross"
        )
        if not df_zscore.empty:
            bt_zscore = FactorBacktester(prices=df_ohlcv, factor_values=df_zscore)
            pf_threshold = bt_zscore.run_threshold_strategy(
                upper_threshold=1.5,
                lower_threshold=-1.5,
                rebalance_freq='M'
            )
            results['Threshold (Z>1.5)'] = pf_threshold
        else:
            print("Warning: zscore_cross is empty.")

        # 4. Analyze Results
        print("\n=== Backtest Comparison ===")
        print(f"{'Strategy':<25} | {'Total Return':<15} | {'Sharpe':<10} | {'Max DD':<10} | {'Alpha':<10} | {'Beta':<10}")
        print("-" * 90)
        
        for name, pf in results.items():
            print(f"{name:<25} | {pf.total_return():<15.2%} | {pf.sharpe_ratio():<10.2f} | {pf.max_drawdown():<10.2%} | {pf.alpha():<10.2f} | {pf.beta():<10.2f}")

        # 5. Save Trade Records for the best strategy (e.g. Long-Short Factor)
        best_pf = results['Long-Short (Factor)']
        print(f"\nTotal Trades: {best_pf.trades.count()}")
        print("\n=== Trade Records (Long-Short Factor - Top 10) ===")
        trades_df = best_pf.trades.records_readable
        print(trades_df.head(10))
        
        output_path = ROOT / "data" / "trade_records.csv"
        trades_df.to_csv(output_path)
        print(f"\nFull trade records saved to: {output_path}")

        # 6. Save Position Records (Aggregated Trades)
        print("\n=== Position Records (Top 10) ===")
        positions_df = best_pf.positions.records_readable
        print(positions_df.head(10))
        
        pos_output_path = ROOT / "data" / "position_records.csv"
        positions_df.to_csv(pos_output_path)
        print(f"Position records saved to: {pos_output_path}")

        # 7. Save Daily Holdings (Asset Values)
        # We'll save the value of each asset over time
        print("\nSaving Daily Holdings...")
        # Use group_by=False to get per-asset value instead of total portfolio value
        daily_holdings = best_pf.asset_value(group_by=False)
        
        # Filter to remove columns (assets) that were never held to save space
        if isinstance(daily_holdings, pd.DataFrame):
            daily_holdings = daily_holdings.loc[:, (daily_holdings != 0).any(axis=0)]
        
        holdings_output_path = ROOT / "data" / "daily_holdings.csv"
        daily_holdings.to_csv(holdings_output_path)
        print(f"Daily holdings saved to: {holdings_output_path}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

if __name__ == "__main__":
    run_example()
