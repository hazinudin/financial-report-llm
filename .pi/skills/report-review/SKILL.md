---
name: report-review
description: Review or write an IDX equity analysis (investment-bank style) covering income statement, balance sheet, cash flow, business segments, external factors, valuation, SWOT, risks, and a BUY/HOLD/SELL recommendation. Use when asked to review, critique, or produce an analysis for a company in the analysis/ vault.
---

# Equity analysis review

Act as an equity analyst. Base facts on the converted report (`results/`), the database
(`duckdb_query`), and existing vault notes (`notes_search`) — never invent figures. When
producing output, write a new markdown file under `analysis/` and add `[[wikilinks]]` to
related analyses in that folder.

## Framework

1. **Executive summary** — clear thesis and rating; highlight revenue, profit, margins; balanced positives/concerns.
2. **Financial performance**
   - *Income statement*: revenue drivers (volume vs price), gross/operating/net margin trend, cost structure, EPS.
   - *Balance sheet*: liquidity, working capital, leverage (DER, net debt), asset quality; red flags (payables/inventory spikes).
   - *Cash flow*: operating CF vs net profit divergence, free cash flow, capex adequacy, financing (dividends, debt, equity).
   - *Segments*: business lines, per-segment profitability, which line drives revenue/profit.
3. **External factors** — commodity prices/forecasts, regulation/policy, industry headwinds/tailwinds, FX/inflation, company developments (exploration, M&A, ownership).
4. **Valuation** — PER, PBV, P/S, EV/EBITDA; historical and peer comparison; ROE/ROA/ROIC.
5. **SWOT** — strengths, weaknesses, opportunities, threats (operational, financial, market, governance).
6. **Risks** — key risks ranked by severity, quantified where possible; company-specific and industry risks.
7. **Conclusion** — explicit rating (BUY/HOLD/SELL), risk-reward, catalysts, thesis clarity.

## Checks

- Every metric is traceable to a period and a source (report page, DB table, or note).
- Margins and growth are computed consistently (state the period basis).
- Peer/valuation comps use the same period as the subject.
- Note any data gaps or anomalies (e.g. unit/scale inconsistencies) instead of hiding them.
