# 4️⃣ Minimum Viable Financial Dataset

For this project, the following **nine** financial metrics are required in the `fundamentals` table.  
They are sufficient to calculate all **Momentum**, **Value**, and **Quality** factors in the current DS5110 project.

| Metric | Needed For | Used In Factors |
|---------|-------------|----------------|
| **pe** | Value | `value_pe_inv` |
| **pb** | Value | `value_pb_inv` |
| **eps** | Value / optional | `value_earnings_yield` |
| **net_income** | Quality | `quality_roe`, `quality_roa`, `quality_profit_margin` |
| **equity** | Quality | `quality_roe` |
| **assets** | Quality | `quality_roa` |
| **revenue** | Quality | `quality_profit_margin` |
| **gross_margin** | Quality | `quality_gross_margin` (Gross Profit / Revenue) |
| **operating_margin** | Quality | `quality_operating_margin` (Operating Income / Revenue) |

---

### ✅ Summary
These **nine metrics** fully support the computation of:
- **Momentum factors** → from `prices` table (no financials needed)  
- **Value factors** → require valuation ratios such as `pe`, `pb`, `eps`  
- **Quality factors** → require profitability and efficiency metrics such as `net_income`, `equity`, `assets`, `revenue`, `gross_margin`, `operating_margin`

---
