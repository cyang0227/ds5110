# Backtest Module Documentation

## Overview
This module (`src/backtest`) provides a robust framework for testing quantitative investment strategies based on financial factors. It integrates data from DuckDB, computes factor signals, and simulates portfolio performance using `vectorbt`.

The primary entry point is `run_example.py`, which demonstrates a complete workflow: loading data, generating a "Composite Value" factor, and running multiple strategy variations to assess performance.

## Workflow
1.  **Data Loading**: Fetches OHLCV (Open, High, Low, Close, Volume) market data and Factor data (e.g., Value, Momentum) from the local DuckDB warehouse.
2.  **Signal Generation**: Converts raw factor values into trading signals (e.g., Ranks, Z-Scores).
3.  **Simulation**: Uses `vectorbt` to simulate daily portfolio rebalancing, accounting for transaction fees and slippage.
4.  **Analysis**: Calculates key performance metrics (Sharpe Ratio, Alpha, Beta) and generates trade logs.

## Strategies Tested
The module automatically runs 6 distinct strategy variations to provide a comprehensive view of the factor's quality:

### 1. Long-Only (Equal Weight)
*   **Logic**: Buy the Top N stocks with the highest factor values.
*   **Weighting**: Equal weight (1/N) for each stock.
*   **Purpose**: Baseline for "picking winners".

### 2. Long-Only (Factor Weight)
*   **Logic**: Buy the Top N stocks.
*   **Weighting**: Proportional to the factor score (higher score = more weight).
*   **Purpose**: Tests if stronger signals deserve more capital.

### 3. Long-Short (Equal Weight)
*   **Logic**: Buy Top N (Long) and Sell Bottom N (Short).
*   **Weighting**: Equal weight on both sides.
*   **Purpose**: Pure factor play. Removes market exposure (Market Neutral) to isolate the factor's Alpha.

### 4. Long-Short (Factor Weight)
*   **Logic**: Same as above, but weighted by factor score.
*   **Purpose**: Aggressive pure factor play.

### 5. Sector-Neutral Top-N
*   **Logic**: Select the Top 10% of stocks **within each sector**.
*   **Purpose**: Removes sector bias. Ensures the strategy doesn't just bet on one sector (e.g., Tech) but picks the best stocks in every industry.

### 6. Threshold Strategy
*   **Logic**: Long if Z-Score > 1.5, Short if Z-Score < -1.5.
*   **Purpose**: Absolute value approach. Only trades when the signal is statistically extreme, rather than always holding N stocks.

## Interpreting Results

### Key Metrics
*   **Total Return**: The absolute percentage gain over the backtest period.
*   **Sharpe Ratio**: Risk-adjusted return. > 1.0 is generally good.
*   **Max Drawdown (Max DD)**: The largest peak-to-trough decline. Measures risk/pain.
*   **Alpha**: Excess return independent of the market. Positive Alpha = Manager skill / Factor edge.
*   **Beta**: Correlation with the market.
    *   Beta ~ 1.0: Moves with the market (Long-Only).
    *   Beta ~ 0.0: Market Neutral (Long-Short).

### Sample Interpretation (Value Factor 2017-2025)
*   **Long-Only**: High Return (~360%), High Beta (0.96).
    *   *Interpretation*: The strategy rode the bull market. Most return came from the market rising, not necessarily the factor.
*   **Long-Short**: Low Return (~6%), Low Beta (-0.13).
    *   *Interpretation*: The "Value" factor itself had weak performance. Buying cheap stocks and selling expensive ones didn't generate much profit after hedging out market risk. This is consistent with the Growth-dominated market of recent years.
*   **Sector-Neutral**: Moderate Return (~237%).
    *   *Interpretation*: Better diversification than pure stock picking, but still constrained by the underlying factor's performance.

## Usage
Run the example script from the project root:
```bash
python src/backtest/run_example.py
```
Results will be printed to the console and saved to `data/trade_records.csv`.
