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
        
    def _calculate_weights(
        self, 
        entries: pd.DataFrame, 
        weighting: str = 'equal'
    ) -> pd.DataFrame:
        """
        Calculate weights based on selected entries and weighting scheme.
        """
        if weighting == 'equal':
            # Equal weight for selected stocks
            weights = entries.astype(float).div(entries.sum(axis=1), axis=0)
        elif weighting == 'factor':
            # Weight proportional to factor value (absolute value)
            # Use the factor values for the selected entries
            selected_factors = self.factor_values.where(entries).abs()
            weights = selected_factors.div(selected_factors.sum(axis=1), axis=0)
        else:
            raise ValueError(f"Unknown weighting scheme: {weighting}")
            
        return weights.fillna(0.0)

    def _prepare_simulation_input(
        self, 
        weights: pd.DataFrame, 
        rebalance_freq: str
    ) -> pd.DataFrame:
        """
        Resample and align weights for simulation.
        """
        if rebalance_freq:
            # Handle 'M' deprecation
            freq = rebalance_freq
            if freq == 'M':
                freq = 'ME'
            
            # 1. Identify Rebalance Dates (Last trading day of the period)
            rebalance_dates = self.close_prices.index.to_series().resample(freq).last()
            
            # 2. Select weights on these dates
            valid_dates = rebalance_dates.dropna()
            
            valid_dates = valid_dates[valid_dates.isin(weights.index)]
            
            weights = weights.loc[valid_dates]
            
            # 3. Reindex back to daily to match prices
            weights = weights.reindex(self.close_prices.index)
            
        return weights

    def run_top_n_strategy(
        self, 
        top_n: int = 20, 
        rebalance_freq: str = 'M',
        weighting: str = 'equal',
        fees: float = 0.001,
        slippage: float = 0.001,
        input_is_rank: bool = False,
        benchmark_prices: pd.Series = None
    ) -> vbt.Portfolio:
        """
        Run a Top-N Long-Only strategy.
        
        Args:
            top_n: Number of stocks to hold.
            rebalance_freq: Rebalancing frequency ('D', 'W', 'M', 'Q').
            weighting: 'equal' or 'factor'.
            fees: Transaction fees (e.g. 0.001 = 10bps).
            slippage: Slippage (e.g. 0.001 = 10bps).
            input_is_rank: If True, treats factor_values as ranks (1=Best).
            benchmark_prices: Optional Series of benchmark prices (e.g. SPY).
        """
        # 1. Generate Ranks
        if input_is_rank:
            # Assume factor_values are already ranks (1 is best)
            ranks = self.factor_values
        else:
            # Higher factor value = Higher rank (1 is best in our logic for selection)
            # Standard rank(ascending=False) gives 1 to largest value.
            ranks = self.factor_values.rank(axis=1, ascending=False)
        
        # 2. Generate Target Weights
        long_entries = ranks <= top_n
        weights = self._calculate_weights(long_entries, weighting=weighting)
        
        # 3. Resample Weights
        weights = self._prepare_simulation_input(weights, rebalance_freq)

        # 4. Run Simulation
        pf = vbt.Portfolio.from_orders(
            close=self.close_prices,
            size=weights,
            size_type='targetpercent',
            freq='1D',
            fees=fees,
            slippage=slippage,
            group_by=True,
            cash_sharing=True,
            init_cash=100000
        )
        
        if benchmark_prices is not None:
            pf._benchmark_close = benchmark_prices
        
        return pf


