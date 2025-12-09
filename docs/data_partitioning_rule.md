## Data Partitioning Rule

## Time Range
- **Coverage:** From **2017-01-01** to **present (2025-10-26)**  
- **Universe:** Current S&P 500 Constituents  
- **Frequency:** Daily (for OHLCV) and Periodic (Quarterly/Annual for fundamentals)  
- **Update Mode:** Incremental append — each run fetches only new data since the last snapshot.

- **Hive-style partitioning (`key=value`)**
- **Three layers:**
  - `raw` → raw ingestion, unmodified data  
  - `curated` → cleaned, standardized data  
  - `marts` → final serving layer (factors, analysis-ready)

### Partitioning Rules
| Data Type | Partition Keys | Example Path |
|------------|----------------|---------------|
| **Daily prices (OHLCV)** | `year + month` | `year=2025/month=10/part-YYYY-MM-DD.parquet` |
| **Factor Values (Cross-sectional)** | `trade_date` | `trade_date=2025-10-25/factor_values.parquet` |

---

## Directory Structure (Example)

```plaintext
data/
  raw/
    prices/
      source=yahoo/year=2025/month=10/2025-10-26.parquet
      source=yahoo/year=2025/month=09/2025-09-30.parquet
    fundamentals/
      source=yahoo/year=2025/month=06/fundamentals.parquet

  curated/
    prices_adj/
      year=2025/month=10/part-0000.parquet
      year=2025/month=09/part-0000.parquet
    fundamentals/
      year=2025/month=06/part-0000.parquet

  warehouse/
    data.duckdb

