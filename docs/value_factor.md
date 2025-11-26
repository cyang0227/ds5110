| Factor                                  | Expression                                    | Required                               |
| ----------------------------------- | ------------------------------------- | ---------------------------------- |
| **E/P (Earnings Yield)**            | `eps / price`                         | eps, prices                        |
| **Net Income Yield**                | `net_income / market_cap`             | net_income, market_cap             |
| **Operating Income Yield**          | `operating_income / enterprise_value` | operating_income, enterprise_value |
| **Free Cash Flow Yield**            | `free_cash_flow / market_cap`         | free_cash_flow, market_cap         |
| **Operating Cash Flow Yield**       | `operating_cash_flow / market_cap`    | operating_cash_flow, market_cap    |
| **Gross Profitability (Novy-Marx)** | `gross_profit / total_assets`         | gross_profit, total_assets         |

| Factor                             | Expression                                         | Required                               |
| ------------------------------ | ------------------------------------------ | ---------------------------------- |
| **Book-to-Market (B/M)**       | `total_stockholders_equity / market_cap`   | equity, market_cap                 |
| **Book-to-Total-Assets**       | `total_stockholders_equity / total_assets` | equity, assets                     |
| **Enterprise Value Multiples** | `enterprise_value / operating_income`      | enterprise_value, operating_income |
| **Sales-to-Price (S/P)**       | `revenue / market_cap`                     | revenue, market_cap                |

| Factor                     | Expression                          | Required                       |
| ---------------------- | --------------------------- | ------------------------ |
| Size                   | `log(market_cap)`           | market_cap               |
| Size inverse           | `-log(market_cap)`          | market_cap               |