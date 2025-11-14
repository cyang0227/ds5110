# Metric → Factor Mapping (Full Version)

This document provides a complete mapping between financial **metrics** 
and the **factor categories** commonly used in quantitative investing:
Value, Quality, Growth, Efficiency, Leverage, and Size.

It covers:
- Raw metrics (from Income Statement, Balance Sheet, Cashflow, Key Metrics)
- Market / Quote data
- Derived factor metrics used in multi-factor models

---

## 📦 1. Income Statement Metrics

| Metric | Factor Category | Description |
|--------|------------------|-------------|
| **revenue** | Quality, Efficiency | Drives margins and turnover ratios |
| **gross_profit** | Quality | Indicates product/business competitiveness |
| **operating_profit** | Quality | Measures operating efficiency |
| **net_income** | Value, Quality | Core profitability measure; used in ROE/ROA |
| **eps** | Value, Growth | Basis for P/E; EPS growth is a key signal |
| **revenue_growth** | Growth | Top-line growth factor |
| **net_income_growth** | Growth | Earnings momentum indicator |
| **eps_growth** | Growth | High-quality growth proxy |

---

## 📦 2. Balance Sheet Metrics

| Metric | Factor Category | Description |
|--------|------------------|-------------|
| **assets** | Quality | Used in ROA and asset turnover |
| **equity** | Quality | Used in ROE |
| **liabilities** | Leverage | Basic leverage measure |
| **debt_total** | Leverage | Financial risk indicator |
| **debt_to_equity** | Leverage | Classic leverage ratio (D/E) |

---

## 📦 3. Cashflow Statement Metrics

| Metric | Factor Category | Description |
|--------|------------------|-------------|
| **operating_cashflow** | Quality | Indicates earnings quality and sustainability |
| **free_cashflow** | Value, Quality | Basis for FCF Yield, a major value factor |
| **capex** | Efficiency, Growth | Capital intensity and growth investment |

---

## 📦 4. Market / Quote Metrics

| Metric | Factor Category | Description |
|--------|------------------|-------------|
| **shares_outstanding** | Misc | Needed for EPS, per-share metrics |
| **market_cap** | Size | Size factor (Fama-French SMB) |

---

## 📦 5. Derived Factor Metrics (Computed)

These factors are derived from raw metrics and typically stored in your factor library.

| Derived Factor | Category | Formula |
|----------------|-----------|---------|
| **ROE** | Quality | net_income / equity |
| **ROA** | Quality | net_income / assets |
| **gross_margin** | Quality | gross_profit / revenue |
| **operating_margin** | Quality | operating_profit / revenue |
| **profit_margin** | Quality | net_income / revenue |
| **asset_turnover** | Efficiency | revenue / assets |
| **capital_intensity** | Efficiency | assets / revenue |
| **fcf_yield** | Value | free_cashflow / market_cap |
| **earnings_yield** | Value | eps / price |
| **accruals_ratio** | Quality | (net_income - operating_cashflow) / assets |

---

## 🧭 6. Category Overview

Value:
- eps, market_cap, free_cashflow, enterprise_value, earnings_yield

Quality:
- net_income, ROE, ROA, gross_margin, operating_margin,
profit_margin, accruals_ratio, operating_cashflow

Growth:
- revenue_growth, eps_growth, net_income_growth

Efficiency:
- revenue, assets, asset_turnover, capital_intensity, capex

Leverage:
- liabilities, debt_total, debt_to_equity

Size:
- market_cap

## 📌 7. Recommended FMP API Endpoints

To collect all metrics above, you will need:

- **Income Statement** (annual + quarterly)  
- **Balance Sheet** (annual + quarterly)  
- **Cashflow Statement** (annual + quarterly)  
- **Key Metrics** (for shares outstanding)  
- **Quote** API (for market cap / price)  

These cover 100% of the metrics required for a professional multi-factor model.

---