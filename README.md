# IDX Financial Report Pipeline

Downloads financial reports from the Indonesia Stock Exchange (IDX), identifies the actual financial report PDF among supplementary documents, converts it to markdown using docling, and parses XLSX structured financial statements into DuckDB.

## Project Structure

```
.
├── scripts/
│   ├── report_pipeline/
│   │   ├── pipeline.py          # Report download/convert pipeline
│   │   ├── db.py                # DuckDB database layer
│   │   ├── idx_downloader.py    # IDX API downloader
│   │   └── xlsx_parser.py       # XLSX financial statement parser
│   ├── analysis_pipeline/
│   │   ├── pipeline.py          # Analysis orchestrator
│   │   ├── db.py                # Analysis jobs state
│   │   ├── schema.py            # Pydantic fact sheet schema
│   │   ├── opencode_client.py   # OpenCode Go API client
│   │   ├── preprocess.py        # Stage 0 deterministic reducer
│   │   ├── chunker.py           # Adaptive chunk builder
│   │   ├── extract.py           # Stage 1 LLM extraction
│   │   ├── merge.py             # Stage 2a merge + XLSX cross-check
│   │   ├── ratios.py            # Stage 2b deterministic ratios
│   │   ├── context.py           # Stage 3 profile/vault/market
│   │   ├── research.py          # Stage 3b external research (pi agent)
│   │   ├── research_plan.py     # Stage 3b research planning (LLM)
│   │   ├── research_questions.py # Stage 3b deterministic questions
│   │   ├── pi_agent.py          # shared headless pi runner
│   │   ├── write.py             # Stage 4 writer agent
│   │   ├── redflags.py          # Stage 5 contradiction pass
│   │   └── prompts/             # System/user prompt files
│   └── notes_rag/
│       └── rag.py               # Vault search indexer/retriever
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

## Notes RAG Search

Local hybrid search (BM25 + vector embeddings) over the `analysis/` Obsidian vault — used for Q&A and context retrieval in OpenCode sessions.

- **Embeddings**: Ollama `bge-m3` (multilingual EN/ID) — one-time setup: `ollama pull bge-m3`
- **Store**: `idx_data/notes_rag.duckdb` — chunks, metadata, embeddings, and FTS index in one file
- **Retrieval**: BM25 (DuckDB FTS) ⊕ cosine similarity, fused with Reciprocal Rank Fusion
- **Chunking**: markdown header-aware, with ticker/quarter/folder metadata extracted from filenames
- **OpenCode**: exposed as the `notes-search` custom tool (`.opencode/tool/notes-search.ts`)

```bash
# Index the vault (incremental; re-run after editing notes)
uv run scripts/notes_rag/rag.py index          # add --force to rebuild everything

# Search
uv run scripts/notes_rag/rag.py search "which companies benefit from high ammonia prices"
uv run scripts/notes_rag/rag.py search -k 5 --ticker ESSA "gas cost assumption"
uv run scripts/notes_rag/rag.py search --folder forecast --quarter "Q2 2026" "base case"
uv run scripts/notes_rag/rag.py search --json "ammonia"   # machine-readable
```

Results include an `obsidian://` link that opens the note directly in Obsidian.

## Analysis Pipeline

Automates equity-analysis markdown generation from converted reports using the
OpenCode Go subscription model API. Seven-stage pipeline: deterministic
preprocessing → bounded chunk extraction (cheap model) → merge + deterministic
ratios → context assembly → external research (pi agent) → writer pi agent
(strong model) → red-flag pass.

Stage 3b (external research) runs a **headless pi agent** (`pi -p --mode json`)
to explain the external events behind the reported numbers: `exa_search` (from
the `pi-exa-search` extension, using `EXA_API_KEY`) for discovery and
`fetch_content` (from `pi-web-access`) for full-page retrieval. A
source-provenance audit checks every cited URL against the agent's tool trace.
Project pi packages are declared (pinned) in `.pi/settings.json` and loaded with
`--approve` (project trust).

Stage 4 (writer) also runs as a **headless pi agent**, using the canonical
`glm-5.3` writer model with a read-only tool allowlist (`read`,
`duckdb_query`, `notes_search`, `notes_fetch`); external evidence stays owned
by Stage 3b. A pi failure fails the `write` stage.

```bash
# Full run
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1

# Dry-run preview
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --dry-run

# View job status
uv run scripts/analysis_pipeline/pipeline.py --status

# List available model IDs
uv run scripts/analysis_pipeline/pipeline.py --list-models
```

See `scripts/analysis_pipeline/README.md` for the full command reference and
design documentation.

### Prerequisites

Set `OPENCODE_API_KEY` in `.env` (gitignored). Model IDs are configured in
`config.yaml` under `analysis.models`. Run `--list-models` after key setup to
confirm available IDs.

Stages 3b (external research) and 4 (writer) additionally require:

- `pi` CLI ≥ 1.1 on `PATH`
- Node ≥ 22.19

Stage 3b also requires `EXA_API_KEY` in `.env` (gitignored); Stage 4 uses no
new `.env` keys.
