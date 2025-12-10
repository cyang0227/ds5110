### Questions
1. How does your platform handle conflicting signals when multiple factors disagree, for example, a stock ranks high on Momentum but low on Value and Profitability?

#### Answer:
My platform handles conflicting signals (e.g., High Momuntum vs. Low Value) using a **Weighted Linear Combination** approach. Before the backtesting process, the platform's **run_factor_pipeline.py** script precomputes the factor scores (cross-sectional z-scores and pct ranks for both whole market and sector-neutral) of all securities of all time and stores them in the data warehouse. The z-scores and pct ranks are used to ensure that all factor scores are on the same scale and same units. The persistence of the factor scores accelerates the backtesting process.
<br><br>
When user select conflicting signals, such as high momentum and low value, the backtesting process will use the **Weighted Linear Combination** approach to combine the factor scores of the selected factors. The weights are determined by the user's input. The backtesting process will retrieve the factor scores from the data warehouse and combine them using the weights. Therefore, the effect of **high momentum** will be canceled out by the effect of **low value**.
<br><br>
Ex. A stock has **High Momentum +2.0** and **Low Value -2.0**. If the user selects **Momentum** with a weight of **0.5** and **Value** with a weight of **0.5**, the combined score will be **0.0**, which is **neutral**.
<br><br>
Right now, users have to choose which factors to do the backtesting. In the future work, I plan to implement AutoML to find the best combination of factors and weights.

2. Since your factors (Value, Size, Momentum, Profitability) are computed from historical
snapshots, what measures prevent the platform from being distorted by stale data or survivorship bias?

#### Answer:
##### Survivorship Bias
The current implementation actually does not prevent survivorship bias yet, which I have talked about it in the report and presentation. Based on the current implementation, I have chosen the S&P500 constituents as of the end of Oct, 2025 (data/raw/S&P500.csv) to defined the universe. This means the platform tracks companies that are currently in the index. Companies that went bankrupt or were removed from the index in the past (e.g., in 2018) are missing from the universe. 

In the future work, I plan to implement the correction to improve the dataset, which will include the companies that were removed from the index. As long as Yahoo Finance and FinancialModelPrep has the data, it is very simple to implement the correction by adding the symbol in the data/raw/S&P500.csv file and run the ETL pipeline again.

##### Stale Data
Good question! The platform implements a quite convient ETL pipeline that supports incremental update to minimize the risk of stale data, which can be run on demand at UI or script. This question reminds me that I should implement a feature to show the last update time of the data warehouse to users.

3.	How do you ensure that your stock rankings are robust to outliers especially extreme P/E
ratios or abnormal returns that may skew standardized factor scores?

#### Answer:
In the current implementation of the postprocess_factor pipeline (src/utils/factor_postprocess.py), there is three-layers techniques to handle the outliers of extreme factor values:

-- Winsorization (clipping): <br>
Before calculating the z-scores, the pipeline applies Winsorization (default 1%/99%) limits. Any value below 1% is set to 1%, and any value above 99% is set to 99%.

-- Log Transformation: <br>
For right-skewed distributions of factor values, users can enable log transformation(enable_log=True) to make the distribution more normal.

-- More Robust Standardization: <br>
In addition to z-scores, the pipeline also calculates the pct ranks of factor values (rank_cross) and save it to the data warehouse. Users can choose which to use when backtesting (zscore_cross or rank_cross, whole market or sector-neutral).

### Case Studies to Test the System
Scenario
- A stock (NVDA, TSLA) experiences a sharp 30–40% run-up in one month, inflating its Momentum score.
- However:
    - P/E ratio becomes extremely high
    - Profitability drops due to a weak earnings quarter
    - Size factor remains stable
- What This Tests
    - Interaction between momentum vs. valuation factors
    - Ranking stability under extreme positive drift
    - Outlier handling in factor normalization (Momentum may dominate unless controlled)
    - Whether the UI clearly shows why the stock ranked where it did
    - Expected System Behavior

#### Answer:
In the current implementation, users will choose which factor/factors to use to rank stocks in the UI. If user only choose Momentum, the stock (NVDA, TSLA) will be ranked high. 
<br>

As explained in the Question 1, when there are conflicting signals (e.g. High Momentum and Low Value), the src/ui/app.py will pass the selected factors and weights to the backtesting engine, which will use the Weighted Linear Combination approach to combine the factor scores of the selected factors. The effect of high momentum will be canceled out by the effect of low value, and the stock (NVDA, TSLA) will be ranked low.
<br>

Under extreme positive drift (ex. the stock price surges 10x in a month), with the protection of the winsorization, the Momentum z-score of the stock will likely be limited to 2.33 (99th percentile). Therefore, it prevents th single outlier from skewing the overall ranking.
<br>

In the current implementation, the UI design is results-driven, which means the UI will not explicitly show the stock ranking process.

There are significant technical challenges to visualizing the full ranking process directly in the UI:
1.  **Visual Scalability**: Displaying hundreds or thousands of stocks simultaneously can be challenging due to frequent changes in the ranking and numorous factors and rebalance dates.
2.  **Performance Overheads**: Rendering hundreds of interactive data points with multiple factor dimensions consumes excessive browser resources. As the system scales to thousands of stocks and factors, this would lead to severe UI latency or crashes.

**Proposed Solution: Data Audit & Export**
Instead of forcing a complex visualization, my planned approach is to implement a **Audit Interface**. Users will be able to export the `factor_values` table (including raw values, Z-scores, and Ranks for every stock) as a CSV file. This allows advanced users to perform their own analysis.

Expected System Behavior
1. User only select Momentum<br>
Result: the stock (NVDA, TSLA) will be ranked high. <br>
Reason: since user explicitly select only Momentum, the system will honors the pure momentum signal and rank the stock high.

2. User select Momentum, Value, Profitability, Size with equal weights<br>
Result: the stock (NVDA, TSLA) will be ranked low. <br>
Reason: <br>
- Momentum z-score: +2.33 (Max) because of the winsorization
- Value z-score: -2.33 (Min) because of the winsorization
- Profitability z-score: -1.0 because of weak earnings quarter
- Size: +1.0 suppose no change
<br>
Calculate the weighted-sum of the z-scores: 
$$ \sum_{i=1}^{n} w_i z_i = 2.33 \times 0.25 + (-2.33) \times 0.25 + (-1.0) \times 0.25 + 1.0 \times 0.25 = -0.5 $$
<br>

The stock (NVDA, TSLA) will be ranked low because the weighted-sum of the z-scores is -0.5, below average. The stock (NVDA, AAPL) will not likely be selected in the portfolio.


