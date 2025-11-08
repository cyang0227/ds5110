#  DS5110 – Factor-Based Stock Tool Schema Summary
*(Universe = Current S&P 500 Constituents (as the end of Oct) · Horizon = Last 8 Years)*

---

## 1. `securities`
| Attribute | Type | Description |
|------------|------|-------------|
| **security_id** | BIGINT (PK) | Unique identifier for each stock. |
| symbol | TEXT (UNIQUE NOT NULL) | Stock ticker symbol (e.g., AAPL). |
| name | TEXT | Company full name. |
| sector | TEXT | GICS sector. |
| industry | TEXT | GICS industry group. |

**Primary Key:** `security_id`  
**Referenced by:** `prices`, `corporate_actions`, `fundamentals`, `factor_values`

---

## 2. `prices`
| Attribute | Type | Description |
|------------|------|-------------|
| **security_id** | BIGINT (FK → securities) | Stock identifier. |
| **trade_date** | DATE | Trading date. |
| open | DOUBLE | Opening price. |
| high | DOUBLE | Highest price of the day. |
| low | DOUBLE | Lowest price of the day. |
| close | DOUBLE | Closing price. |
| adj_close | DOUBLE | Adjusted close (if provided by source). |
| volume | BIGINT | Trading volume. |

**Primary Key:** (`security_id`, `trade_date`)  
**Purpose:** Daily OHLCV time series for each stock.

---

## 3. `corporate_actions`
| Attribute | Type | Description |
|------------|------|-------------|
| **security_id** | BIGINT (FK → securities) | Stock identifier. |
| **action_date** | DATE | Effective date of the corporate action. |
| **action_type** | TEXT | ‘split’ or ‘dividend’. |
| split_ratio | DOUBLE | Split ratio (e.g., 2.0 = 2-for-1). |
| cash_amount | DOUBLE | Cash dividend per share. |

**Primary Key:** (`security_id`, `action_date`, `action_type`)  
**Purpose:** Records splits and dividends used for price adjustment.

---

## 4. `fundamentals`
| Attribute | Type | Description |
|---|---|---|
| **security_id** | BIGINT (FK → securities) | Stock identifier. |
| **period_end** | DATE | Fiscal period end date (quarter or year). |
| **period_type** | TEXT | quarterly or yearly
| **metric** | TEXT | One of: `pe`, `pb`, `eps`, `market cap`, `net_income`, `equity`, `assets`, `revenue`, `gross_profit`, `operating_income`. |
| value | DOUBLE | Metric value. |

**Primary Key:** (`security_id`, `period_end`, `metric`)  
**Purpose:** Stores key financial indicators in tidy (long-form) structure.

---

## 5. `factor_definitions`
| Attribute | Type | Description |
|------------|------|-------------|
| **factor_id** | BIGINT (PK) | Unique identifier for each factor. |
| name | TEXT (UNIQUE) | Factor name (e.g., momentum_12_1). |
| category | TEXT | Factor category (‘momentum’, ‘value’, ‘quality’, ‘vol’). |
| params_json | TEXT | Parameter settings in JSON format. |
| description | TEXT | Text description of the factor. |
| version | INTEGER | Version number of the factor algorithm. |
| expression | TEXT | SQL or DSL expression used for computation. |
| source | TEXT | Data source (‘prices_adj’, ‘fundamentals’, etc.). |
| is_active | BOOLEAN | Whether the factor is currently active. |
| tags | TEXT | Comma-separated keywords for grouping/search. |

**Purpose:** Factor catalog and metadata; new factors are added by inserting new rows only.

---

## 6. `factor_values`
| Attribute | Type | Description |
|------------|------|-------------|
| **security_id** | BIGINT (FK → securities) | Stock identifier. |
| **trade_date** | DATE | Trading date. |
| **factor_id** | BIGINT (FK → factor_definitions) | Factor identifier. |
| value | DOUBLE | Raw factor value. |
| zscore_cross | DOUBLE | Cross-sectional z-score (standardized per date). |
| rank_cross | INTEGER | Cross-sectional rank (per date). |
| calc_run_id | TEXT | Computation run ID (timestamp or UUID). |
| updated_at | TIMESTAMP | Last updated timestamp. |

**Primary Key:** (`security_id`, `trade_date`, `factor_id`)  
**Purpose:** Stores computed factor results in long-form format (one row = one stock × date × factor).

---