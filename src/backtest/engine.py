import pandas as pd
import vectorbt as vbt

class FactorBacktester:

    def __init__(self, prices: pd.DataFrame, ranks: pd.DataFrame):
        self.prices = prices
        self.ranks = ranks

    def build_weights(self, top_n=20):
        weights = pd.DataFrame(0, index=self.ranks.index, columns=self.ranks.columns)

        for date, row in self.ranks.iterrows():
            top = row.nsmallest(top_n).index
            weights.loc[date, top] = 1 / top_n

        return weights

    def run(self, weights):
        pf = vbt.Portfolio.from_weights(
            close=self.prices,
            weights=weights,
            rebalanced=True
        )
        return pf
