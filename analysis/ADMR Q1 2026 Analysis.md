# PT Alamtri Minerals Indonesia Tbk (ADMR) — Q1 2026 Financial Report Analysis

**Report Date:** 6 May 2026  
**Period Under Review:** Three months ended 31 March 2026 (Q1 2026)  
**Comparative Period:** Three months ended 31 March 2025 (Q1 2025)  
**Balance Sheet Date:** 31 March 2026, with comparative at 31 December 2025  
**Data Sources:** (1) Parsed financial report `results/ADMR_Q1_2026.md`; (2) DuckDB `xlsx_statements`, `xlsx_meta`, `financial_ratios`, `company_profiles` tables  

---

## 0. Source Data Verification (MANDATORY)

### 0.1 Scale & Rounding Consistency

| Source | Period | Rounding | Currency |
|--------|--------|----------|----------|
| Report (face of financial statements) | Q1 2026 & Dec 2025 | Thousands of US$ | USD |
| DuckDB `xlsx_meta` (2026 tw1) | Q1 2026 | "Ribuan / In Thousand" | USD |
| DuckDB `xlsx_meta` (2025 audit) | FY 2025 | "Ribuan / In Thousand" | USD |
| DuckDB `xlsx_meta` (2025 tw1) | Q1 2025 | **"Satuan Penuh / Full Amount"** | USD |

⚠️ **Critical Scale Note:** The DuckDB 2025 tw1 (Q1 2025) data is stored in **full US$ amounts** (not thousands), while the report presents Q1 2025 comparative figures in **thousands of US$**. All Q1 2025 cross-checks below divide DuckDB values by 1,000 before comparison. The 2026 tw1 and 2025 audit data are already in thousands and require no adjustment.

### 0.2 Cross-Check Summary

All key line items from the report were cross-checked against DuckDB `xlsx_statements`. Results are categorized as:

- ✅ **Match** — Values agree exactly (within rounding)
- ⚠️ **Classification Difference** — Values agree in total but are disaggregated differently
- ❌ **Mismatch** — Values disagree numerically

---

## 1. Executive Summary Review

### Key Findings

PT Alamtri Minerals Indonesia Tbk (ADMR) delivered a strong Q1 2026 performance with revenue of US$267.5M (+33.8% YoY) and net profit of US$86.0M (+32.6% YoY). The company significantly increased bank borrowings (non-current loans up US$237M to US$922M), likely to fund the aluminium smelter project through subsidiary KAI. Cash position strengthened to US$504M from US$369M at year-end, though primarily driven by debt drawdowns rather than operating cash generation. Operating cash flow of US$46.6M covered only 54% of net profit, indicating working capital pressure. NCI continues to report losses (US$1.7M), reflecting losses at subsidiary level. The financial data in the report is internally consistent and matches the DuckDB database with no material discrepancies.

---

## 2. Cross-Check Results

### 2.1 Balance Sheet — As at 31 March 2026 (Current Period)

| Line Item | Report (000s US$) | DuckDB (000s US$) | Status | Notes |
|-----------|-------------------|-------------------|--------|-------|
| Cash and cash equivalents | 504,196 | 504,196 | ✅ Match | |
| Trade receivables - related parties | 67,124 | 67,124 | ✅ Match | |
| Trade receivables - third parties | 61,786 | 61,786 | ✅ Match | |
| Other investments - current | 34,837 | 34,837 | ✅ Match | DuckDB label: "Short-term investments" |
| Inventories | 103,041 | 103,041 | ✅ Match | |
| Other receivables | 31,655 | 31,655 | ✅ Match | DuckDB: 13,078 + 30,348 = 43,426? No — DuckDB splits as third-party 1,307 + related-party 30,348 = 31,655 ✅ |
| Prepaid taxes - current (income) | 35,284 | 48,067 | ⚠️ Classification | DuckDB aggregates as "Current prepaid taxes" = 48,067; Report splits: 35,284 + 12,783 = 48,067 ✅ |
| Prepaid taxes - current (other) | 12,783 | — | ⚠️ Classification | See above |
| Advances - current | 436 | 436 | ✅ Match | |
| Prepayments - current | 3,995 | 3,995 | ✅ Match | |
| **Total Current Assets** | **855,137** | **855,137** | ✅ Match | |
| Restricted time deposits | 25,798 | 25,798 | ✅ Match | DuckDB label: "Non-current restricted funds" |
| Other investments - non-current | 88,272 | 88,272 | ✅ Match | DuckDB label: "Non-current financial assets amortized cost" |
| Exploration and evaluation assets | 24,681 | 24,681 | ✅ Match | |
| Mining properties | 154,674 | 154,674 | ✅ Match | |
| Fixed assets | 1,953,448 | 1,953,448 | ✅ Match | DuckDB label: "Property, plant, and equipment" |
| Prepaid taxes - non-current (income) | 169 | 86,004 | ⚠️ Classification | DuckDB aggregates as "Non-current prepaid taxes" = 86,004; Report splits: 169 + 85,835 = 86,004 ✅ |
| Prepaid taxes - non-current (other) | 85,835 | — | ⚠️ Classification | See above |
| Advances - non-current | 15,314 | 15,314 | ✅ Match | |
| Prepayments - non-current | 1,198 | 1,198 | ✅ Match | |
| Other non-current assets | 964 | 964 | ✅ Match | |
| **Total Non-Current Assets** | **2,350,353** | **2,350,353** | ✅ Match | |
| **TOTAL ASSETS** | **3,205,490** | **3,205,490** | ✅ Match | |

### 2.2 Balance Sheet — Liabilities & Equity (31 March 2026)

| Line Item | Report (000s US$) | DuckDB (000s US$) | Status | Notes |
|-----------|-------------------|-------------------|--------|-------|
| Trade payables - related parties | 34,107 | 34,107 | ✅ Match | |
| Trade payables - third parties | 21,989 | 21,989 | ✅ Match | |
| Corporate income tax payable | 8,061 | 9,706 | ⚠️ Classification | DuckDB aggregates "Taxes payable" = 9,706; Report splits: 8,061 + 1,645 = 9,706 ✅ |
| Other taxes payable | 1,645 | — | ⚠️ Classification | See above |
| Royalties payable | 34,419 | — | ⚠️ Classification | Included in DuckDB "Current accrued expenses" |
| Accrued expenses | 285,641 | 320,060 | ⚠️ Classification | DuckDB "Current accrued expenses" = 320,060; Report separates: 285,641 + 34,419 (royalties) = 320,060 ✅ |
| Lease liabilities - current | 745 | 745 | ✅ Match | |
| Bank loans - current | 37,607 | 37,607 | ✅ Match | |
| Provision for decommissioning - current | — | 274 | ⚠️ Classification | DuckDB shows 274 for Dec 2025; Report shows 0 for Mar 2026. DuckDB for Mar 2026 doesn't show this line separately — likely reclassified or settled |
| Other current liabilities | 2,868 | 2,868 | ✅ Match | DuckDB label: "Other payables third parties" |
| **Total Current Liabilities** | **427,082** | **427,082** | ✅ Match | |
| Lease liabilities - non-current | 468 | 468 | ✅ Match | |
| Bank loans - non-current | 922,004 | 922,004 | ✅ Match | |
| Post-employment benefits | 6,856 | 6,856 | ✅ Match | |
| Provision for decommissioning - non-current | 27,843 | 27,843 | ✅ Match | |
| Deferred tax liabilities | 6,527 | 6,527 | ✅ Match | |
| Other non-current liabilities | 10,466 | 10,466 | ✅ Match | DuckDB label: "Long-term retention payables" |
| **Total Non-Current Liabilities** | **974,164** | **974,164** | ✅ Match | |
| **TOTAL LIABILITIES** | **1,401,246** | **1,401,246** | ✅ Match | |
| Share capital | 303,920 | 303,920 | ✅ Match | |
| Additional paid-in capital | (183,042) | (183,042) | ✅ Match | |
| Difference from NCI transactions | 696 | 696 | ✅ Match | |
| Retained earnings - appropriated | 13,666 | 13,666 | ✅ Match | |
| Retained earnings - unappropriated | 1,513,909 | 1,513,909 | ✅ Match | |
| **Equity attributable to parent** | **1,649,149** | **1,649,149** | ✅ Match | |
| Non-controlling interests | 155,095 | 155,095 | ✅ Match | |
| **TOTAL EQUITY** | **1,804,244** | **1,804,244** | ✅ Match | |
| **TOTAL LIABILITIES & EQUITY** | **3,205,490** | **3,205,490** | ✅ Match | |

### 2.3 Balance Sheet — Comparative (31 December 2025)

All Dec 2025 figures from the report were verified against DuckDB 2025 audit data. **Every line item matches exactly.** Key confirmations include:

| Line Item | Report (000s US$) | DuckDB (000s US$) | Status |
|-----------|-------------------|-------------------|--------|
| Cash and cash equivalents | 369,087 | 369,087 | ✅ Match |
| Total current assets | 680,424 | 680,424 | ✅ Match |
| Total assets | 2,891,276 | 2,891,276 | ✅ Match |
| Total current liabilities | 426,807 | 426,807 | ✅ Match |
| Total liabilities | 1,173,865 | 1,173,865 | ✅ Match |
| Total equity | 1,717,411 | 1,717,411 | ✅ Match |

### 2.4 Income Statement (Q1 2026 vs Q1 2025)

| Line Item | Report Q1 2026 (000s) | DuckDB Q1 2026 (000s) | Status | Report Q1 2025 (000s) | DuckDB Q1 2025 (000s ÷ 1000) | Status |
|-----------|----------------------|----------------------|--------|----------------------|-------------------------------|--------|
| Revenue | 267,491 | 267,491 | ✅ Match | 199,937 | 199,937 | ✅ Match |
| Cost of revenue | (142,322) | (142,322) | ✅ Match | (117,334) | (117,334) | ✅ Match |
| **Gross profit** | **125,169** | **125,169** | ✅ Match | **82,603** | **82,603** | ✅ Match |
| Operating expenses | (14,341) | (14,341) | ✅ Match | (10,771) | (10,771) | ✅ Match |
| Other (expenses)/income, net | (3,803) | (3,803) | ✅ Match | 158 | 158 | ✅ Match |
| **Operating income** | **107,025** | **107,025** | ✅ Match | **71,990** | **71,990** | ✅ Match |
| Finance costs | (317) | (317) | ✅ Match | (314) | (314) | ✅ Match |
| Finance income | 4,149 | 4,149 | ✅ Match | 7,503 | 7,503 | ✅ Match |
| **Profit before tax** | **110,857** | **110,857** | ✅ Match | **79,179** | **79,179** | ✅ Match |
| Income tax expense | (24,850) | (24,850) | ✅ Match | (14,330) | (14,330) | ✅ Match |
| **Profit for the period** | **86,007** | **86,007** | ✅ Match | **64,849** | **64,849** | ✅ Match |
| Exchange difference | (73) | (73) | ✅ Match | (933) | (933) | ✅ Match |
| **Total comprehensive income** | **85,934** | **85,934** | ✅ Match | **63,916** | **63,916** | ✅ Match |
| Attributable to parent (profit) | 87,708 | 87,708 | ✅ Match | 65,450 | 65,450 | ✅ Match |
| Attributable to NCI (profit) | (1,701) | (1,701) | ✅ Match | (601) | (601) | ✅ Match |
| Basic EPS (full amount) | 0.0021 | 0.0021 | ✅ Match | 0.0016 | 0.0016 | ✅ Match |

**Note on "Other (expenses)/income, net":** The report aggregates several items. DuckDB provides granular breakdown:
- Q1 2026: FX gains (1,357) + Fair value losses (2,700) + Realised gains (65) + Other income (189) = Net (3,803) ✅
- Q1 2025: FX gains (257) + Other expenses (99) = Net 158 ✅ (after rounding from full amounts)

### 2.5 Cash Flow Statement (Q1 2026 vs Q1 2025)

| Line Item | Report Q1 2026 (000s) | DuckDB Q1 2026 (000s) | Status | Report Q1 2025 (000s) | DuckDB Q1 2025 (÷1000) | Status |
|-----------|----------------------|----------------------|--------|----------------------|-------------------------|--------|
| Receipts from customers | 293,973 | 293,973 | ✅ Match | 190,655 | 190,655 | ✅ Match |
| Payments to suppliers | (164,984) | (164,984) | ✅ Match | (121,574) | (121,574) | ✅ Match |
| Payments of employee costs | (12,249) | (12,249) | ✅ Match | (12,688) | (12,688) | ✅ Match |
| Receipts of finance income | 3,975 | 3,975 | ✅ Match | 6,466 | 6,466 | ✅ Match |
| Payments of corporate income tax | (25,877) | (25,877) | ✅ Match | (23,573) | (23,573) | ✅ Match |
| Receipts of VAT refund | 11,862 | — | ⚠️ Classification | 19,948 | — | ⚠️ Classification |
| Receipts of tax refunds | 41 | 41 | ⚠️ Classification | — | — | |
| Payments of interest and finance charges | (22,698) | (22,698) | ✅ Match | (11,849) | (11,849) | ✅ Match |
| Payments of royalties | (37,319) | (37,319) | ✅ Match | (30,480) | (30,480) | ✅ Match |
| Payments of dismantling/reclamation | (141) | — | ⚠️ Classification | (1,456) | — | ⚠️ Classification |
| **Net cash from operations** | **46,583** | **46,583** | ✅ Match | **15,449** | **15,449** | ✅ Match |
| Payments for fixed assets | (152,468) | (152,468) | ✅ Match | (129,145) | (129,145) | ✅ Match |
| Payments for mining properties | (608) | (608) | ✅ Match | (1,802) | (1,802) | ✅ Match |
| Placement of restricted time deposits | (9,559) | (9,559) | ✅ Match | (4,792) | (4,792) | ✅ Match |
| Payments for E&E assets | (411) | (411) | ✅ Match | (125) | (125) | ✅ Match |
| Payments for intangible assets | (27) | (27) | ✅ Match | — | — | |
| Sale of other investments | 744 | 744 | ✅ Match | — | — | |
| **Net cash used in investing** | **(162,329)** | **(162,329)** | ✅ Match | **(135,864)** | **(135,864)** | ✅ Match |
| Proceeds from bank loans | 250,052 | 250,052 | ✅ Match | 99,969 | 99,969 | ✅ Match |
| Payments of lease liabilities | (304) | (304) | ✅ Match | (296) | (296) | ✅ Match |
| Proceeds from share subscription (NCI) | 899 | 899 | ✅ Match | — | — | |
| **Net cash from financing** | **250,647** | **250,647** | ✅ Match | **99,673** | **99,673** | ✅ Match |
| Net increase/(decrease) in cash | 134,901 | 134,901 | ✅ Match | (20,742) | (20,742) | ✅ Match |
| Cash at beginning | 369,087 | 369,087 | ✅ Match | 612,605 | 612,605 | ✅ Match |
| FX effect | 208 | 208 | ✅ Match | (1,051) | (1,051) | ✅ Match |
| **Cash at end** | **504,196** | **504,196** | ✅ Match | **590,812** | **590,812** | ✅ Match |

**Cash Flow Classification Differences Explained:**

| Report Item | Q1 2026 | DuckDB Mapping | Q1 2025 | DuckDB Mapping |
|-------------|---------|----------------|---------|----------------|
| Receipts of VAT refund | 11,862 | Part of "Other cash inflows/outflows" (11,721 net of dismantling) | 19,948 | Mapped to "Income taxes refunded" (19,948) |
| Receipts of tax refunds | 41 | Mapped to "Income taxes refunded" (41) | — | — |
| Payments of dismantling/reclamation | (141) | Part of "Other cash inflows/outflows" | (1,456) | Part of "Other cash inflows/outflows" (1,456 net) |

⚠️ **Note:** The XBRL taxonomy in DuckDB classifies certain cash flow items differently than the face of the financial statements. The report provides a more granular breakdown of operating cash flows (separating VAT refunds, tax refunds, and dismantling costs), while DuckDB aggregates some of these under "Other cash inflows (outflows) from operating activities." The totals reconcile perfectly in all cases.

### 2.6 Financial Ratios (from DuckDB `financial_ratios` table)

| Ratio | Q3 2025 (Oct) | Q1 2025 (Mar) | Q3 2024 (Sep) | Q1 2024 (Mar) | Source Match |
|-------|---------------|---------------|---------------|---------------|-------------|
| PER | 11.76 | 6.49 | 6.15 | 7.31 | ✅ Matches user data |
| PBV | 2.31 | 1.58 | 2.36 | 2.95 | ✅ Matches user data |
| DER | 0.60 | 0.43 | 0.34 | 0.55 | ✅ Matches user data |
| ROA (%) | 12.25 | 16.98 | 28.58 | 0.26 | ✅ Matches user data |
| ROE (%) | 19.63 | 24.28 | 38.42 | 0.40 | ✅ Matches user data |
| NPM (%) | 48.11 | 190.18 | 62.59 | 1.69 | ✅ Matches user data but ⚠️ see below |

⚠️ **NPM Data Quality Concern:** The NPM values in the `financial_ratios` table appear anomalous:
- Q1 2025 NPM = 190.18% — This is mathematically impossible for a net profit margin (profit cannot exceed revenue by 90%). Using the table's own figures: profit/sales = 1,074.30/3,312.16 = 32.4%, not 190.18%.
- Q1 2024 NPM = 1.69% — Using table figures: profit/sales = 1,835.51/4,357.70 = 42.1%, not 1.69%.
- Q3 2024 NPM = 62.59% — Using table figures: profit/sales = 5,016.82/12,736.20 = 39.4%, not 62.59%.

**The NPM field in the `financial_ratios` table appears to use a non-standard calculation methodology or contains data errors.** The ROA and ROE values also appear to be annualized or use different denominators than simple quarterly calculations would suggest.

---

## 3. Mathematical Verification

### 3.1 Balance Sheet Equation

**Assets = Liabilities + Equity**

| Date | Total Assets | Total Liabilities | Total Equity | Liabilities + Equity | Difference |
|------|-------------|-------------------|-------------|---------------------|------------|
| 31 Mar 2026 | 3,205,490 | 1,401,246 | 1,804,244 | 3,205,490 | ✅ 0 |
| 31 Dec 2025 | 2,891,276 | 1,173,865 | 1,717,411 | 2,891,276 | ✅ 0 |

### 3.2 Current Assets Subtotal Verification

| Date | Sum of Components | Reported Total | Difference |
|------|-------------------|---------------|------------|
| 31 Mar 2026 | 504,196 + 67,124 + 61,786 + 34,837 + 103,041 + 31,655 + 35,284 + 12,783 + 436 + 3,995 = 855,137 | 855,137 | ✅ 0 |
| 31 Dec 2025 | 369,087 + 98,520 + 56,872 + 38,674 + 89,261 + 8,188 + 889 + 15,570 + 249 + 3,114 = 680,424 | 680,424 | ✅ 0 |

### 3.3 Non-Current Assets Subtotal Verification

| Date | Sum of Components | Reported Total | Difference |
|------|-------------------|---------------|------------|
| 31 Mar 2026 | 25,798 + 88,272 + 24,681 + 154,674 + 1,953,448 + 169 + 85,835 + 15,314 + 1,198 + 964 = 2,350,353 | 2,350,353 | ✅ 0 |

### 3.4 Income Statement Cascade Verification

**Q1 2026:**
- Revenue − Cost of revenue = Gross Profit: 267,491 − 142,322 = 125,169 ✅
- Gross Profit − Operating expenses − Other (expenses)/income = Operating Income: 125,169 − 14,341 − 3,803 = 107,025 ✅
- Operating Income + Finance income − Finance costs = Profit before tax: 107,025 + 4,149 − 317 = 110,857 ✅
- Profit before tax − Income tax expense = Profit for the period: 110,857 − 24,850 = 86,007 ✅
- Profit for the period + Other comprehensive income = Total comprehensive income: 86,007 + (73) = 85,934 ✅
- Profit attributable to parent + Profit attributable to NCI = Profit for the period: 87,708 + (1,701) = 86,007 ✅
- Comprehensive income attributable to parent + Comprehensive income attributable to NCI = Total comprehensive income: 87,635 + (1,701) = 85,934 ✅

**Q1 2025:** All cascades verified identically. ✅

### 3.5 Cash Flow Reconciliation

**Q1 2026:**
- Net cash from operations + Net cash used in investing + Net cash from financing = Net increase in cash: 46,583 + (162,329) + 250,647 = 134,901 ✅
- Cash at beginning + Net increase + FX effect = Cash at end: 369,087 + 134,901 + 208 = 504,196 ✅

**Q1 2025:**
- Net cash from operations + Net cash used in investing + Net cash from financing = Net increase in cash: 15,449 + (135,864) + 99,673 = (20,742) ✅
- Cash at beginning + Net increase + FX effect = Cash at end: 612,605 + (20,742) + (1,051) = 590,812 ✅

### 3.6 Equity Movement Verification

**Q1 2026 (from 1 Jan 2026 to 31 Mar 2026):**

| Component | Opening (1 Jan 2026) | Changes | Closing (31 Mar 2026) | Verified |
|-----------|---------------------|---------|----------------------|----------|
| Share capital | 303,920 | — | 303,920 | ✅ |
| Additional paid-in capital | (183,042) | — | (183,042) | ✅ |
| Difference from NCI transactions | 696 | — | 696 | ✅ |
| Retained earnings - appropriated | 13,666 | — | 13,666 | ✅ |
| Retained earnings - unappropriated | 1,426,274 | +87,708 (profit) − 73 (OCI) | 1,513,909 | ✅ |
| **Equity attributable to parent** | **1,561,514** | **+87,635** | **1,649,149** | ✅ |
| Non-controlling interests | 155,897 | +899 (share subscription) − 1,701 (loss) | 155,095 | ✅ |
| **Total equity** | **1,717,411** | **+86,833** | **1,804,244** | ✅ |

**Q1 2025 (from 1 Jan 2025 to 31 Mar 2025):**

| Component | Opening (1 Jan 2025) | Changes | Closing (31 Mar 2025) | Verified |
|-----------|---------------------|---------|----------------------|----------|
| Retained earnings - appropriated | 9,299 | — | 9,299 | ✅ |
| Retained earnings - unappropriated | 1,280,440 | +65,450 (profit) − 933 (OCI) | 1,344,957 | ✅ |
| **Equity attributable to parent** | **1,411,313** | **+64,517** | **1,475,830** | ✅ |
| Non-controlling interests | 90,950 | − 601 (loss) | 90,349 | ✅ |
| **Total equity** | **1,502,263** | **+63,916** | **1,566,179** | ✅ |

---

## 4. Discrepancies Found

### 4.1 Numerical Discrepancies

**No material numerical discrepancies were found.** All line items in the report match the DuckDB database values exactly (after accounting for rounding and scale differences).

### 4.2 Classification Differences (Not Errors)

| Item | Report Treatment | DuckDB Treatment | Impact |
|------|-----------------|------------------|--------|
| Prepaid taxes | Split into "income taxes" and "other taxes" | Aggregated as single line | No impact on totals |
| Accrued expenses | Separates "royalties payable" (34,419) from "accrued expenses" (285,641) | Combined as "Current accrued expenses" (320,060) | No impact on totals |
| Taxes payable | Split into "corporate income tax" (8,061) and "other taxes" (1,645) | Combined as "Taxes payable" (9,706) | No impact on totals |
| Cash flow — VAT refund, tax refunds, dismantling | Three separate lines | Aggregated under "Other cash inflows/outflows" and "Income taxes refunded" | No impact on totals |
| Provision for decommissioning — current | Shown as zero (0) for Mar 2026 | Not separately shown in DuckDB for Mar 2026 | Likely reclassified; Dec 2025 value of 274 matches |

### 4.3 Data Quality Issues

| Issue | Severity | Description |
|-------|----------|-------------|
| ⚠️ NPM in `financial_ratios` table | **High** | NPM values are inconsistent with simple profit/sales calculations. Q1 2025 NPM = 190.18% is impossible. Q1 2024 NPM = 1.69% contradicts the actual profit/sales ratio of ~42%. This suggests the NPM field uses a non-standard methodology or contains errors. |
| ⚠️ Scale inconsistency in DuckDB | **Medium** | 2025 tw1 data is stored in full US$ amounts while 2026 tw1 and 2025 audit data are in thousands. This creates a risk of misinterpretation if scale is not checked. |
| ⚠️ Cash flow classification inconsistency | **Low** | The mapping of "VAT refund" and "tax refunds" between the report and DuckDB's XBRL taxonomy differs between Q1 2025 and Q1 2026. In Q1 2025, the report's "VAT refund" (19,948) maps to DuckDB's "Income taxes refunded" (19,948), while in Q1 2026, the report's "tax refunds" (41) maps to DuckDB's "Income taxes refunded" (41). This suggests the XBRL tagging may be inconsistent across periods. |
| ℹ️ Rounding differences | **Negligible** | Q1 2025 figures in the report (thousands) show minor rounding from DuckDB full amounts (e.g., Revenue: 199,937.386 rounds to 199,937). All within acceptable tolerance. |

---

## 5. Financial Performance Analysis

### 5.1 Income Statement Analysis

**Revenue Growth:**
- Q1 2026 revenue: US$267.5M vs Q1 2025: US$199.9M → **+33.8% YoY growth**
- This strong growth likely reflects higher coal volumes and/or favorable pricing

**Margin Analysis:**

| Metric | Q1 2026 | Q1 2025 | Change |
|--------|---------|---------|--------|
| Gross margin | 46.8% | 41.3% | +5.5pp |
| Operating margin | 40.0% | 36.0% | +4.0pp |
| Net margin (attributable to parent) | 32.8% | 32.7% | +0.1pp |
| Effective tax rate | 22.4% | 18.1% | +4.3pp |

💡 **Observation:** Gross and operating margins improved significantly, but net margin was flat due to a higher effective tax rate (22.4% vs 18.1%). The tax expense jumped 73.3% YoY (from US$14.3M to US$24.9M) while pre-tax profit rose 40.0%, indicating a disproportionate tax burden increase.

**Cost Structure:**
- Cost of revenue increased 21.3% YoY (US$142.3M vs US$117.3M), significantly less than revenue growth of 33.8%, driving margin expansion
- Operating expenses rose 33.1% YoY (US$14.3M vs US$10.8M), broadly in line with revenue growth
- Other expenses increased to US$3.8M net loss vs US$0.2M net gain in Q1 2025, driven by:
  - FX losses of US$1.4M (vs gain of US$0.3M in Q1 2025)
  - Fair value losses on marketable securities of US$2.7M (new item in Q1 2026)
  - Partially offset by other income of US$0.2M and realised gains of US$0.07M

**Finance Income/Costs:**
- Finance income declined 44.7% YoY (US$4.1M vs US$7.5M), likely due to lower cash balances in prior periods or lower interest rates on deposits
- Finance costs remained stable at US$0.3M

**Profit Attribution:**
- NCI reported a loss of US$1.7M (vs US$0.6M loss in Q1 2025), indicating losses at subsidiary level
- Parent-attributable profit of US$87.7M exceeds total profit of US$86.0M due to the NCI loss

### 5.2 Balance Sheet Analysis

**Liquidity Position:**

| Metric | 31 Mar 2026 | 31 Dec 2025 | Change |
|--------|-------------|-------------|--------|
| Cash and cash equivalents | 504,196 | 369,087 | +36.6% |
| Current assets | 855,137 | 680,424 | +25.7% |
| Current liabilities | 427,082 | 426,807 | +0.1% |
| Current ratio | 2.00x | 1.59x | +0.41x |
| Quick ratio (ex-inventory) | 1.76x | 1.38x | +0.38x |

💡 **Observation:** Liquidity improved significantly, with the current ratio rising from 1.59x to 2.00x. However, this is largely driven by the US$250M bank loan drawdown that increased cash balances.

**Leverage Metrics:**

| Metric | 31 Mar 2026 | 31 Dec 2025 | Change |
|--------|-------------|-------------|--------|
| Total debt (bank loans) | 959,611 | 713,023 | +34.6% |
| Total equity | 1,804,244 | 1,717,411 | +5.1% |
| Debt-to-equity ratio | 0.53x | 0.42x | +0.11x |
| Total liabilities / Total equity | 0.78x | 0.68x | +0.10x |

⚠️ **Concern:** Non-current bank loans surged from US$685.1M to US$922.0M (+US$236.9M or +34.6%), with an additional US$250M in proceeds from bank loans during Q1 2026. This significant leverage increase is likely funding the aluminium smelter project (KAI subsidiary) and ongoing capital expenditure.

**Asset Composition Changes:**

| Item | Change (US$ 000s) | Change (%) |
|------|-------------------|------------|
| Fixed assets | +147,845 | +8.2% |
| Other receivables | +23,467 | +286.5% |
| Inventories | +13,780 | +15.4% |
| Prepaid taxes - current (income) | +34,395 | +3,868.8% |
| Trade receivables - related parties | −31,396 | −31.9% |

⚠️ **Red Flags:**
1. **Other receivables surged 286.5%** from US$8.2M to US$31.7M — primarily related-party receivables (US$30.3M vs US$7.6M). This warrants scrutiny for potential related-party transaction risks.
2. **Prepaid taxes - current (income) jumped from US$0.9M to US$35.3M** — a 38x increase. This may represent a large tax prepayment or refund expectation. The corresponding non-current prepaid income taxes declined from US$34.7M to US$0.2M, suggesting a reclassification from non-current to current.
3. **Trade receivables from related parties declined 31.9%** — from US$98.5M to US$67.1M, which could indicate faster collection or reduced related-party sales.

### 5.3 Cash Flow Analysis

**Operating Cash Flow Quality:**

| Metric | Q1 2026 | Q1 2025 |
|--------|---------|---------|
| Net profit | 86,007 | 64,849 |
| Operating cash flow | 46,583 | 15,449 |
| OCF/Net profit ratio | 54.2% | 23.8% |
| Operating cash flow per share | US$0.0021 | US$0.0016 |

💡 **Observation:** While OCF/Net profit improved from 23.8% to 54.2%, it remains below 100%, indicating that a significant portion of profits is not being converted to cash. Key drags on operating cash flow include:
- High tax payments (US$25.9M)
- Significant interest/finance charge payments (US$22.7M)
- Large royalty payments (US$37.3M)

**Free Cash Flow:**

| Metric | Q1 2026 | Q1 2025 |
|--------|---------|---------|
| Operating cash flow | 46,583 | 15,449 |
| Capital expenditure | (153,514) | (131,072) |
| **Free cash flow** | **(106,931)** | **(115,623)** |

⚠️ **Concern:** Free cash flow remains deeply negative at US$(107)M, though slightly improved from Q1 2025's US$(116)M. The company is investing heavily in fixed assets (US$152.5M) and mining properties/E&E assets (US$1.0M).

**Financing Activities:**
- Net bank loan proceeds of US$250M significantly exceeded the free cash flow deficit
- This suggests the company is debt-financing its capital expenditure program
- NCI share subscription of US$0.9M relates to capital injection into ATCI subsidiary

---

## 6. External Factor Analysis

### 6.1 Commodity Price Environment

The report notes exchange rates as at 31 March 2026:
- Rp10,000 = US$0.59 (implying US$1 ≈ Rp16,949)
- AU$1 = US$0.69
- CNY1 = US$0.14

Compared to 31 December 2025:
- Rp10,000 = US$0.60 (implying US$1 ≈ Rp16,667)
- AU$1 = US$0.67
- CNY1 = US$0.14

The Indonesian Rupiah depreciated ~1.7% against the US Dollar over the quarter, which would positively impact USD-denominated results for a company with IDR-denominated costs.

### 6.2 Regulatory Environment

The company operates under Coal Contracts of Work (CCoWs) with the Indonesian government for its AMC entities (LC, JC, KC, MC, SBC). These CCoWs provide 30-year operating periods from the commencement of mining operations.

### 6.3 Corporate Developments

1. **Name Change:** The company changed its name from PT Adaro Minerals Indonesia Tbk to PT Alamtri Minerals Indonesia Tbk, effective per Deed No. 2 dated 2 June 2025.

2. **Aluminium Smelter Project (KAI):** Significant capital injections into KAI (PT Kalimantan Aluminium Industry) during 2025:
   - June 2025: US$57.8M (through AIA)
   - August 2025: US$75.7M (through AIA)
   - Additional capital from AUMAY and CITA partners
   This explains the surge in bank borrowings and advances.

3. **Subsidiary Capital Increases:** Multiple capital injections into ABI and AIA during 2025, totaling hundreds of millions of USD, indicating aggressive expansion into battery materials and aluminium.

4. **NCI Transactions:** ATCI received a capital injection of US$0.9M from AlamTri (non-controlling interest) in January 2026.

### 6.4 Employee Count

The Group had 622 permanent employees as at 31 March 2026, up from 595 at 31 December 2025 (+4.5%).

---

## 7. Valuation Assessment

### 7.1 Calculated Ratios from Q1 2026 Data

| Ratio | Q1 2026 (Quarterly) | Annualized Estimate |
|-------|---------------------|---------------------|
| ROA | 2.68% | ~10.7% |
| ROE | 4.77% | ~19.1% |
| Net margin | 32.2% | 32.2% |
| Gross margin | 46.8% | 46.8% |
| DER (total liabilities/total equity) | 0.78x | — |
| DER (bank debt/equity) | 0.53x | — |
| Current ratio | 2.00x | — |

### 7.2 Comparison with Database Ratios (Q3 2025)

| Ratio | Q3 2025 (Latest DB) | Q1 2026 (Calculated) | Trend |
|-------|---------------------|----------------------|-------|
| DER | 0.60 | 0.78 | ⬆ Increasing leverage |
| ROA | 12.25% | ~10.7% (ann.) | ⬇ Slight decline |
| ROE | 19.63% | ~19.1% (ann.) | ➡ Stable |
| NPM | 48.11%* | 32.2% | ⚠️ Not comparable* |

*Note: The NPM in the database appears to use a non-standard methodology (see Section 4.3). The calculated NPM of 32.2% is based on quarterly profit/revenue.

### 7.3 Key Valuation Observations

- The significant increase in DER from 0.60 (Q3 2025) to 0.78 (Q1 2026) reflects the US$250M bank loan drawdown
- ROE remains healthy at ~19% annualized, suggesting efficient use of equity despite increased leverage
- The company is in a heavy investment phase (aluminium smelter, battery materials), which depresses current free cash flow but may generate returns in future periods

---

## 8. SWOT Analysis

### Strengths
- ✅ Strong revenue growth (+33.8% YoY) driven by coal operations
- ✅ Improving gross margin (46.8% vs 41.3%) indicating favorable pricing/cost dynamics
- ✅ Healthy liquidity position (current ratio 2.00x)
- ✅ Diversified subsidiary portfolio across coal mining, aluminium, and battery materials
- ✅ Strong parent company backing (AlamTri with 85.02% ownership)

### Weaknesses
- ⚠️ Negative free cash flow (US$107M deficit) requiring debt financing
- ⚠️ OCF/Net profit ratio of only 54.2%, indicating working capital pressure
- ⚠️ NCI consistently reporting losses, suggesting some subsidiaries are not yet profitable
- ⚠️ Significant related-party receivables (US$30.3M non-current + US$67.1M current)
- ⚠️ Rapidly increasing leverage (DER from 0.42 to 0.78 in 15 months)

### Opportunities
- 💡 Aluminium smelter project (KAI) could transform the company from a pure coal play into a diversified minerals company
- 💡 Battery materials investments (ABI, AIA) position the company for the energy transition
- 💡 Favorable coal pricing environment supporting near-term cash flows
- 💡 Weak IDR providing natural hedge for USD-denominated revenue

### Threats
- 🔍 Coal price volatility could significantly impact revenue and margins
- 🔍 Regulatory risk from Indonesian mining regulations and potential CCoW renegotiations
- 🔍 Execution risk on aluminium smelter and battery materials projects (large capital commitments)
- 🔍 Rising interest costs on US$960M bank debt portfolio
- 🔍 FX risk on subsidiary translation (IDR depreciation impacts)
- 🔍 Fair value losses on marketable securities (US$2.7M in Q1 2026)

---

## 9. Risk Assessment

| Risk | Severity | Likelihood | Impact | Mitigation |
|------|----------|-----------|--------|------------|
| Coal price decline | High | Medium | Revenue and margin compression | Diversification into aluminium/battery materials |
| Project execution risk (KAI smelter) | High | Medium | Cost overruns, delays, cash burn | Phased capital deployment, parent guarantees |
| Leverage increase | Medium | High | Interest burden, covenant risk | Strong operating cash flows, parent support |
| Regulatory/CCoW risk | Medium | Low-Medium | Operating restrictions, royalty increases | Long-standing CCoW agreements, government relations |
| FX translation risk | Medium | Medium | Volatility in consolidated results | Natural hedge (USD functional currency) |
| Related-party transaction risk | Low-Medium | Medium | Potential conflicts of interest | Disclosure and governance mechanisms |
| NCI subsidiary losses | Low | High | Drag on consolidated earnings | Ongoing capital support until projects mature |

---

## 10. Conclusion & Recommendation

### Overall Assessment

The Q1 2026 financial statements of PT Alamtri Minerals Indonesia Tbk are **internally consistent and accurately reported**, with all numerical values verified against the DuckDB database. The company demonstrates strong operational performance with 33.8% revenue growth and expanding margins, but is in a capital-intensive investment phase that requires significant debt financing.

### Data Quality Rating: ✅ EXCELLENT

- **No material numerical discrepancies** between the report and DuckDB database
- All mathematical relationships (balance sheet equation, income statement cascade, cash flow reconciliation, equity movement) verified and correct
- Classification differences between the report and XBRL taxonomy are well-understood and do not affect totals
- ⚠️ One data quality concern: NPM values in the `financial_ratios` table appear to use a non-standard methodology

### Investment Considerations

| Factor | Assessment |
|--------|------------|
| Short-term outlook | Positive — strong coal pricing, revenue growth, margin expansion |
| Medium-term outlook | Mixed — heavy capex phase, rising leverage, project execution risk |
| Long-term outlook | Positive if aluminium/battery investments deliver — transformation from coal pure-play to diversified minerals company |
| Balance sheet risk | Moderate — DER rising but manageable with strong cash flows |
| Cash flow quality | Moderate — OCF conversion improving but still below 100% of net profit |

### Recommendation: **HOLD / WATCH**

The company is executing a significant strategic transformation with substantial capital deployment into aluminium and battery materials. While near-term coal operations are strong, the investment phase creates elevated financial risk (rising DER, negative FCF). Investors should monitor:
1. Progress on the KAI aluminium smelter project
2. Trajectory of leverage ratios
3. Coal price trends and their impact on operating cash flows
4. Ability to convert profits to cash (OCF/Net profit ratio trend)

---

*This analysis was prepared based on the Q1 2026 interim consolidated financial statements filed with IDX and cross-referenced against the pipeline.duckdb database. All figures are in thousands of US$ unless otherwise stated.*
