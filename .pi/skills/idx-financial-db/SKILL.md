---
name: idx-financial-db
description: Query the IDX financial DuckDB (idx_data/pipeline.duckdb) for Indonesian stock ratios, balance sheets, income statements, and cash flows. Use when asked about IDX tickers, company financials, valuation multiples (PER/PBV/DER/ROE/ROA/NPM), peer comparisons, or specific XBRL statement line items.
---

# IDX financial database

Source of truth: `idx_data/pipeline.duckdb` (project root). Query it read-only with the
**`duckdb_query`** tool; fall back to the `duckdb` CLI for ad-hoc work.

```
duckdb_query { "sql": "SELECT code, name, roe FROM financial_ratios WHERE code='ADRO' ORDER BY period DESC" }
```

CLI fallback (same DB, never write):

```bash
duckdb -readonly idx_data/pipeline.duckdb -c "SELECT COUNT(*) FROM financial_ratios;"
```

## Tables

**financial_ratios** — company ratios from IDX
`code, name, sector, sub_sector, industry, sub_industry, period, assets, liabilities, equity, sales, ebt, profit, eps, book_value, per, pbv, der, roa, roe, npm`
- `period` is a date string `"2025-03-31"`.
- `assets/liabilities/equity/sales/ebt/profit/book_value` are in **billions of IDR**.
- `roa/roe/npm` are stored as **percent numbers** (e.g. `19.74` = 19.74%). A few older rows store fractions — sanity-check outliers before quoting.
- `per` = P/E, `pbv` = P/B, `der` = Debt/Equity, `eps` = per-share earnings.

**xlsx_statements** — parsed XBRL statement line items (raw IDR)
`ticker, year, period, sheet_code, sheet_title, row_number, label_id, value_current, value_prior, label_en, context_type`
- `sheet_code`: `1210000` balance sheet (current/non-current), `1220000` balance sheet (by liquidity), `1311000` income statement (by function), `1312000` income statement (by nature), `1510000` cash flow (direct), `1520000` cash flow (indirect).
- `value_current`/`value_prior` are **raw IDR** (not billions).
- `period` is lowercase: `audit | tw1 | tw2 | tw3`; `context_type` is `Instant` (balance sheet) or `Duration` (P&L, cash flow).
- `label_id` is Indonesian, `label_en` is English — use `ILIKE` for text search.

**xlsx_meta** — filing metadata: `ticker, year, period, entity_name, entity_code, period_start, period_end, currency, rounding, audit_status, auditor, consolidation_type`

**pipeline_jobs** — run tracking: `job_id, ticker, year, period, stage_download, stage_identify, stage_collect, stage_convert, error_*` (`period` lowercase)

**downloaded_files** — `file_id, job_id, ticker, company_name, year, period, file_name, file_path, file_size, file_type, local_path`

**company_profiles** — `code, name, sector, sub_sector, industry, sub_industry, board, listing_date, website, email, phone, address`

**converted_documents** — PDF→MD results: `convert_id, job_id, source_pdf, output_md, status, error, elapsed_sec`

## Patterns

Revenue (row 6 is always "Penjualan dan pendapatan usaha" / "Sales and revenue"):

```sql
SELECT ticker, value_current, value_prior
FROM xlsx_statements
WHERE sheet_code = '1311000' AND row_number = 6 AND value_current IS NOT NULL
  AND ticker = 'JARR' AND year = 2025 AND period = 'audit';
```

Gross profit: `WHERE (label_id ILIKE '%laba bruto%' OR label_en ILIKE '%gross profit%')`.

Availability checks:

```sql
SELECT DISTINCT ticker, year, period FROM xlsx_statements ORDER BY 1,2,3;
SELECT DISTINCT code, period FROM financial_ratios ORDER BY 1,2;
```

## Missing XLSX data

If `xlsx_statements` has no rows for a requested ticker/year/period, check
`downloaded_files` for the XLSX on disk, then ingest it:

```bash
uv run scripts/report_pipeline/pipeline.py --parse-xlsx --tickers TICKER --years YEAR --periods PERIOD
```

`--periods` is uppercase (`TW1/TW2/TW3/audit`). Do **not** trigger this when the file is
not on disk or the request is only for `financial_ratios` data. If the DB is locked
(`IOException`), remove the WAL: `rm -f idx_data/pipeline.duckdb.wal`, then retry.
