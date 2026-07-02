# IDX Financial Report Pipeline

Downloads Indonesian stock exchange (IDX) financial reports, identifies the main
financial statement, converts PDFs to markdown with docling, and parses XBRL Excel
statements into a queryable DuckDB database.

## Quick Start

```bash
# Install dependencies
uv sync

# View pipeline status
python pipeline.py --status

# Run full pipeline for specific companies (download → identify → collect → convert)
python pipeline.py --tickers JARR,BBCA --years 2025

# Parse XLSX financial statements into DuckDB (no download)
python pipeline.py --parse-xlsx --tickers JARR

# Retry any failed jobs
python pipeline.py --retry-failed
```

## Commands

### Full Pipeline

```bash
python pipeline.py --tickers ESSA,BBCA --years 2024,2025 --periods audit,TW1
```

Stages:
1. **Download** — Fetch PDF reports from IDX API
2. **Identify** — Pick the actual financial report from supplementary documents
3. **Collect** — Copy identified PDFs into `raw/` with normalized names (`TICKER_QX_YEAR.pdf`)
4. **Convert** — Convert PDFs to markdown using docling → `results/*.md`

### Limited Stages

```bash
# Only identify (no conversion)
python pipeline.py --identify-only --tickers ESSA

# Only convert already-downloaded files
python pipeline.py --convert-only --tickers ESSA --years 2025

# Dry run (see what would happen)
python pipeline.py --dry-run --tickers ESSA --years 2025
```

### XLSX Financial Statement Parsing

Parses `FinancialStatement-*.xlsx` files (IDX XBRL format) into DuckDB tables
(`xlsx_meta`, `xlsx_statements`):

```bash
# Parse all XLSX files for a company
python pipeline.py --parse-xlsx --tickers JARR

# Parse specific year/period
python pipeline.py --parse-xlsx --tickers JARR --years 2025 --periods audit

# Parse all XLSX files in reports/
python pipeline.py --parse-xlsx
```

### Monitoring & Retry

```bash
# View job status dashboard
python pipeline.py --status

# Retry all failed jobs (resumes from the failed stage)
python pipeline.py --retry-failed

# Retry only the convert stage
python pipeline.py --retry-failed --stage convert
```

## DuckDB Schema

Database file: `./idx_data/pipeline.duckdb`

| Table | Purpose |
|-------|---------|
| `pipeline_jobs` | Tracks each ticker/year/period with per-stage status (`pending`/`running`/`completed`/`failed`) |
| `downloaded_files` | All downloaded file metadata (replaces `download_metadata.json`) |
| `company_profiles` | Company sector, industry, listing date (replaces `idx_companies.json`) |
| `financial_ratios` | Ratio data from IDX (assets, liabilities, ROA, ROE, etc.) |
| `converted_documents` | PDF-to-markdown conversion results |
| `xlsx_meta` | XLSX filing metadata: entity name, dates, currency, audit status |
| `xlsx_statements` | Financial statement line items parsed from XLSX (balance sheet, P&L, cash flow) |

### Useful DuckDB Queries

```sql
-- Find jobs that failed in any stage
SELECT job_id, ticker, year, period,
       stage_download, stage_identify, stage_collect, stage_convert
FROM pipeline_jobs
WHERE stage_download = 'failed' OR stage_identify = 'failed'
   OR stage_collect = 'failed' OR stage_convert = 'failed';

-- Get revenue for a specific company
SELECT label_id, label_en, value_current, value_prior
FROM xlsx_statements
WHERE ticker = 'JARR' AND year = 2025 AND period = 'audit'
  AND sheet_code = '1311000'
  AND (label_id ILIKE '%penjualan%' OR label_en ILIKE '%sales%' OR label_en ILIKE '%revenue%');

-- Get all balance sheet items for multiple companies
SELECT ticker, label_id, label_en, value_current
FROM xlsx_statements
WHERE sheet_code = '1210000'
  AND year = 2025 AND period = 'audit'
ORDER BY ticker, row_number;

-- Compare a ratio across companies
SELECT code, name, roa, roe, der
FROM financial_ratios
WHERE period >= '2025-01-01'
ORDER BY roe DESC;
```

## Report Identification Strategies

The `--strategy` flag controls how the pipeline selects the main financial report from a
set of files:

| Strategy | Behavior |
|----------|----------|
| `auto` (default) | Score-based: prefers `LK *`/`FinancialStatement-*` files, penalizes supplementary docs |
| `financial_statement` | Only picks `FinancialStatement-*` files |
| `laporan_keuangan` | Prefers files starting with `LK ` or containing "Laporan Keuangan" |
| `largest` | Picks the largest PDF file |

## Directory Structure

```
idx_data/
  pipeline.duckdb       # DuckDB database (all pipeline state & data)
  reports/              # Raw downloaded files organized as reports/TICKER/YEAR_PERIOD/
  ratios/               # Legacy ratio JSON files (migrated to DB on first run)
  idx_companies.json    # Legacy company data (migrated to DB on first run)

raw/                    # Normalized PDF copies (TICKER_QX_YEAR.pdf)
results/                # Converted markdown files (TICKER_QX_YEAR.md)
```

## Error Handling

Each pipeline stage tracks its status independently. When a stage fails:

1. The error is recorded in `pipeline_jobs` (e.g., `error_download`)
2. View failures with `python pipeline.py --status`
3. Retry with `python pipeline.py --retry-failed` (retries from the failed stage)
4. Or retry a specific stage: `python pipeline.py --retry-failed --stage convert`

## XLSX Statement Sheets

The IDX XBRL taxonomy uses standard sheet codes. Parsed sheets:

| Code | Statement |
|------|-----------|
| `1210000` | Balance Sheet (current/non-current) |
| `1220000` | Balance Sheet (order of liquidity) |
| `1311000` | Income Statement (by function) |
| `1312000` | Income Statement (by nature) |
| `1321000` | OCI (before tax) |
| `1322000` | OCI (net of tax) |
| `1410000` | Statement of Changes in Equity |
| `1510000` | Cash Flow (direct method) |
| `1520000` | Cash Flow (indirect method) |

Each row has:
- `label_id` — Indonesian label
- `value_current` — Current period value (IDR)
- `value_prior` — Prior period value (IDR)
- `label_en` — English label
- `context_type` — `Instant` (balance sheet) or `Duration` (P&L, cash flow)

## Syncing the Database

The `sync_db.py` script scans the entire project disk state and populates all DuckDB tables:

```bash
python sync_db.py
```

This registers:
- All files from `reports/` → `downloaded_files`
- All collected PDFs from `raw/` → marks identify/collect stages
- All converted MDs from `results/` → `converted_documents`
- All XLSX financial statements → `xlsx_statements` + `xlsx_meta`
- Pipeline job stages for all ticker/year/period combinations
