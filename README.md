# IDX Financial Report Pipeline

Downloads financial reports from the Indonesia Stock Exchange (IDX), identifies the actual financial report PDF among supplementary documents, converts it to markdown using docling, and parses XLSX structured financial statements into DuckDB.

## Project Structure

```
.
├── scripts/
│   └── report_pipeline/
│       ├── pipeline.py          # Main pipeline orchestrator
│       ├── db.py                # DuckDB database layer
│       ├── idx_downloader.py    # IDX API downloader
│       └── xlsx_parser.py       # XLSX financial statement parser
├── idx_data/
│   ├── reports/                 # Downloaded files from IDX
│   └── pipeline.duckdb          # Pipeline state database
├── raw/                         # Identified report PDFs (ready for conversion)
├── results/                     # Converted markdown files
└── analysis/                    # Analysis outputs
```

## Quick Start

```bash
# Full pipeline: download → identify → collect → convert
uv run scripts/report_pipeline/pipeline.py --tickers BBCA,ESSA --years 2024 --periods audit

# View pipeline job status
uv run scripts/report_pipeline/pipeline.py --status

# Parse XLSX financial statements into DuckDB
uv run scripts/report_pipeline/pipeline.py --parse-xlsx --tickers JARR
```

## Pipeline Stages

1. **Download** - Fetch files from IDX API for specified tickers/years/periods
2. **Identify** - Score and select the actual financial report PDF (filtering out supplementary docs)
3. **Collect** - Copy identified PDFs to `raw/` with standardized naming (`{TICKER}_{Q}_{YEAR}.pdf`)
4. **Convert** - Convert PDFs to markdown using docling, output to `results/`
5. **Parse XLSX** (optional) - Parse `FinancialStatement-*.xlsx` files into DuckDB

## Command Reference

### Full Pipeline

```bash
uv run scripts/report_pipeline/pipeline.py [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--tickers TICKER,...` | Comma-separated ticker symbols (e.g., `ESSA,BBCA`) |
| `--years YEAR,...` | Comma-separated years (e.g., `2024,2025`) |
| `--periods PERIOD,...` | Periods: `TW1`, `TW2`, `TW3`, `audit` |
| `--strategy STRATEGY` | Report identification: `auto` (default), `financial_statement`, `laporan_keuangan`, `largest` |
| `--output DIR` | Output directory (default: `./idx_data`) |
| `--delay SECONDS` | Delay between API requests (default: `0.5`) |

### Mode Flags

| Flag | Description |
|------|-------------|
| `--convert-only` | Skip download; use already-downloaded files |
| `--identify-only` | Only identify which PDF is the financial report |
| `--no-convert` | Download and identify only; skip docling conversion |
| `--parse-xlsx` | Parse XLSX financial statements into DuckDB |
| `--scan-disk` | Scan `reports/` and register files in DuckDB (for manually downloaded files) |
| `--dry-run` | Show what would happen without executing |
| `--force` | Re-convert even if results already exist |

### Job Management

| Flag | Description |
|------|-------------|
| `--status` | Show pipeline job status dashboard |
| `--retry-failed` | Re-run pipeline for jobs with failed stages |
| `--stage STAGE` | Limit `--retry-failed` to: `download`, `identify`, `collect`, `convert` |

## Examples

```bash
# Download and convert annual reports for 2024
uv run scripts/report_pipeline/pipeline.py --tickers BBCA,BRI --years 2024 --periods audit

# Download quarterly reports (TW1 = Q1)
uv run scripts/report_pipeline/pipeline.py --tickers ESSA --years 2025 --periods TW1

# Skip download, just convert already-downloaded files
uv run scripts/report_pipeline/pipeline.py --convert-only --tickers ESSA

# Identify reports without converting
uv run scripts/report_pipeline/pipeline.py --identify-only --tickers ESSA

# Parse XLSX data only
uv run scripts/report_pipeline/pipeline.py --parse-xlsx --tickers JARR

# Retry failed conversions
uv run scripts/report_pipeline/pipeline.py --retry-failed --stage convert

# Dry run to preview actions
uv run scripts/report_pipeline/pipeline.py --dry-run --tickers ESSA --years 2024

# Scan manually downloaded files and register in DB
uv run scripts/report_pipeline/pipeline.py --scan-disk --tickers ESSA
```

## Report Identification Strategies

The pipeline uses scoring to identify the actual financial report PDF among multiple files:

- **auto** (default): Score-based selection using filename patterns
- **financial_statement**: Prefer `FinancialStatement-*.pdf` files
- **laporan_keuangan**: Prefer `LK`/`Laporan Keuangan` prefixed files
- **largest**: Select the largest PDF by file size

## Database

All pipeline state is stored in `idx_data/pipeline.duckdb`. Key tables:

- `pipeline_jobs` - Job status per ticker/year/period
- `downloaded_files` - Metadata for downloaded files
- `companies` - Company profiles
- `financial_ratios` - Financial ratio data
- `xlsx_meta` / `xlsx_statements` - XLSX parsed data
- `conversion_results` - PDF conversion status

Query the database directly using the DuckDB CLI:

```bash
# Interactive mode
duckdb idx_data/pipeline.duckdb

# One-liner queries
duckdb idx_data/pipeline.duckdb "SELECT * FROM pipeline_jobs LIMIT 5;"
duckdb idx_data/pipeline.duckdb "SELECT ticker, year, period FROM pipeline_jobs WHERE ticker='INDR';"
duckdb idx_data/pipeline.duckdb "SELECT COUNT(*) as total FROM downloaded_files;"
duckdb idx_data/pipeline.duckdb "SELECT ticker, COUNT(*) as files FROM downloaded_files GROUP BY ticker ORDER BY files DESC;"
```
