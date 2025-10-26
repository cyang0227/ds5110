# Minimum Viable Financial Dataset

For this project, the following **ten** financial metrics are required in the `fundamentals` table.  
They are sufficient to calculate all **Momentum**, **Value**, and **Quality** factors in the current DS5110 project.

| Metric | Needed For | Used In Factors |
|---------|-------------|----------------|
| **pe** | Value | `value_pe_inv` |
| **pb** | Value | `value_pb_inv` |
| **market cap** | Value | `value_size_inv`, `value_size` |
| **eps** | Value / optional | `value_earnings_yield` |
| **net_income** | Quality | `quality_roe`, `quality_roa`, `quality_profit_margin` |
| **equity** | Quality | `quality_roe` |
| **assets** | Quality | `quality_roa` |
| **revenue** | Quality | `quality_profit_margin` |
| **gross_profit** | Quality | `quality_gross_margin` (Gross Profit / Revenue) |
| **operating_profit** | Quality | `quality_operating_margin` (Operating Income / Revenue) |
