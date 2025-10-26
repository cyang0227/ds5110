## 📂 Data Partitioning Summary

- **Hive-style partitioning (`key=value`)**
- **Three layers:**
  - `raw` → raw ingestion, unmodified data
  - `curated` → cleaned, standardized data
  - `marts` → final serving layer (factors, analysis-ready)

**Partitioning rules:**
- **Daily frequency data (prices, OHLCV)** → `symbol + year + month`
- **Low frequency data (fundamentals, corporate actions)** → `symbol + year`
- **Factor data (cross-sectional values)** → `trade_date`

---

## 📂 Directory Structure (Example)

```plaintext
data/
  raw/                          # Raw ingestion (original format)
    prices/
      source=yahoo/symbol=AAPL/year=2025/month=10/2025-10-26.parquet
      source=yahoo/symbol=MSFT/year=2025/month=10/2025-10-26.parquet
    fundamentals/
      source=yahoo/symbol=AAPL/year=2025/fundamentals.parquet

  curated/                      # Cleaned & standardized data
    prices_adj/
      symbol=AAPL/year=2025/month=10/part-0000.parquet
    fundamentals/
      symbol=AAPL/year=2025/part-0000.parquet

  marts/                        # Final serving layer (analysis-ready)
    factors/
      trade_date=2025-10-25/factor_values.parquet
      trade_date=2025-10-26/factor_values.parquet
