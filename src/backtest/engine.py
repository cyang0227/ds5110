import pandas as pd
import vectorbt as vbt
from typing import Union, Dict

class FactorBacktester:
    def __init__(
        self, 
        prices: Union[pd.DataFrame, Dict[str, pd.DataFrame]], 
        factor_values: pd.DataFrame
    ):
        """
        Initialize Backtester.
        
        Args:
            prices: 
                Either a DataFrame of close prices (Index=Date, Cols=Security),
                OR a MultiIndex DataFrame (Cols=(Field, Security)),
                OR a Dict of DataFrames (e.g. {'close': df, 'volume': df, 'GDP': df, ...})
            factor_values:
                DataFrame of factor values (Index=Date, Cols=Security)
        """
        self.prices = prices
        self.factor_values = factor_values
        
        # Extract Close prices for alignment and default backtesting
        # If prices is a dict, look for 'adj_close' or 'close'
        if isinstance(prices, dict):
            if 'adj_close' in prices:
                self.close_prices = prices['adj_close']
            elif 'close' in prices:
                self.close_prices = prices['close']
            else:
                raise ValueError("prices dict must contain 'adj_close' or 'close'")
        elif isinstance(prices, pd.DataFrame) and isinstance(prices.columns, pd.MultiIndex):
        # If prices is a MultiIndex DataFrame, look for 'adj_close' or 'close' in the first level
            if 'adj_close' in prices.columns.get_level_values(0):
                self.close_prices = prices['adj_close']
            elif 'close' in prices.columns.get_level_values(0):
                self.close_prices = prices['close']
            else:
                raise ValueError("Could not find 'close' or 'adj_close' in prices MultiIndex.")
        else:
            # Assume it's already a wide close price dataframe
            self.close_prices = prices

        # Align Data (Intersection of Dates and Assets)
        # 1. Calculate common index and columns
        common_dates = self.close_prices.index.intersection(self.factor_values.index)
        common_assets = self.close_prices.columns.intersection(self.factor_values.columns)
        
        # 2. Align close_prices and factor_values
        self.close_prices = self.close_prices.loc[common_dates, common_assets]
        self.factor_values = self.factor_values.loc[common_dates, common_assets]
        
        # 3. Align original prices (handle MultiIndex)
        if isinstance(self.prices, pd.DataFrame):
            if isinstance(self.prices.columns, pd.MultiIndex):
                # MultiIndex: (Field, Security)
                # Slice: All dates, (All fields, Common securities)
                self.prices = self.prices.loc[common_dates, pd.IndexSlice[:, common_assets]]
            else:
                # Single Index
                self.prices = self.prices.loc[common_dates, common_assets]
        elif isinstance(self.prices, dict):
            # If dict, align each dataframe inside
            for k, v in self.prices.items():
                if isinstance(v, pd.DataFrame):
                    # Intersect dates
                    v = v.loc[v.index.intersection(common_dates)]
                    # Intersect columns if they look like assets
                    cols = v.columns.intersection(common_assets)
                    if not cols.empty:
                        self.prices[k] = v[cols]
        
    def run_top_n_strategy(
        self, 
        top_n: int = 20, 
        rebalance_freq: str = 'M',
        fees: float = 0.001,
        slippage: float = 0.001
    ) -> vbt.Portfolio:
        """
        Run a Top-N Long-Only strategy.
        
        Args:
            top_n: Number of stocks to hold.
            rebalance_freq: Rebalancing frequency ('D', 'W', 'M', 'Q').
            fees: Transaction fees (e.g. 0.001 = 10bps).
            slippage: Slippage (e.g. 0.001 = 10bps).
        """
        # 1. Generate Ranks (Higher factor value = Higher rank)
        # We assume Higher Factor Value is Better. If not, invert factor_values before passing.
        ranks = self.factor_values.rank(axis=1, ascending=False)
        
        # 2. Generate Target Weights
        long_entries = ranks <= top_n
        
        # Equal weight for selected stocks
        weights = long_entries.astype(float).div(long_entries.sum(axis=1), axis=0)
        weights = weights.fillna(0.0)
        
        # 3. Resample Weights for Rebalancing
        # Take the last signal of the period and hold it until the next period
        if rebalance_freq:
            # Use 'ME' instead of 'M' if pandas version requires it, but 'M' is safer for older pandas
            # The user saw a warning, so let's try to be safe. 
            # If rebalance_freq is passed as 'M', we keep it.
            weights = weights.resample(rebalance_freq).last()
            # Reindex back to daily to match prices (forward fill the weights)
            weights = weights.reindex(self.close_prices.index).ffill()

        # 4. Run Simulation
        # from_weights is missing in this vbt version, use from_orders with targetpercent
        pf = vbt.Portfolio.from_orders(
            close=self.close_prices,
            size=weights,
            size_type='targetpercent',
            freq='1D',
            fees=fees,
            slippage=slippage,
            group_by=True,      # Group all stocks into one portfolio
            cash_sharing=True   # Share cash across all stocks
        )
        
        return pf
