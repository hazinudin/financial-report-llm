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
│   │   ├── research_plan.py     # Stage 3b research planning (pi agent, read-only tools)
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

Stage 3b (external research) runs two **headless pi agents**. The planner
formulates the themed question set first, with read-only tools (`read`,
`duckdb_query`, `notes_search`, `notes_fetch`) and no web access; every planner
run writes a `plan_trace.json`, and a planner failure falls back to
deterministic questions with the reason recorded. The search agent then explains
the external events behind the reported numbers: `exa_search` (from
the `pi-exa-search` extension, using `EXA_API_KEY`) for discovery and
`fetch_content` (from `pi-web-access`) for full-page retrieval. A
source-provenance audit checks every cited URL against the agent's tool trace.
Project pi packages are declared (pinned) in `.pi/settings.json` and loaded with
`--approve` (project trust).

Stage 4 (writer) also runs as a **headless pi agent**, using the canonical
`deepseek/deepseek-v4.1-flash` writer model with a read-only tool allowlist (`read`,
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

The model provider is **Command Code** (`https://api.commandcode.ai/provider/v1`),
configured in `config.yaml`:

- `analysis.llm` — base URL, `api_key_env`, timeout for the HTTP stages
  (extract, red-flags).
- `analysis.models` — one model ID per stage. **These must be Command Code IDs
  (`vendor/model`, e.g. `deepseek/deepseek-v4.1-flash`).** The older opencode-go
  IDs had no prefix; the chat endpoint rejects a bare ID with
  `Model "…" is not supported on this endpoint` even though `--list-models`
  prints it un-prefixed.
- `analysis.write.pi` / `analysis.plan.pi` / `analysis.research.pi` / `chat.pi` —
  `provider: commandcode` + an explicit `model:` for the headless pi agents.

Credentials (see `.env.example`):

```bash
cp .env.example .env   # then fill in
```

- `COMMAND_CODE_API_KEY` — required. Read by the HTTP stages via
  `analysis.llm.api_key_env`, and inherited by the pi subprocesses (the
  `pi-commandcode-provider` extension), so a server needs no interactive
  `pi` `/login`. Locally it may be omitted when a login already exists in
  `~/.pi/agent/auth.json`; the client falls back to it.
- `EXA_API_KEY` — Stage 3b (external research) web discovery only.

Stages 3b (external research) and 4 (writer) additionally require:

- `pi` CLI ≥ 1.1 on `PATH`
- Node ≥ 22.19

Confirm the catalog with `pi --list-models commandcode` (pi stages) and
`uv run scripts/analysis_pipeline/pipeline.py --list-models` (HTTP stages).

### Deploying to a server

No database, queue, or daemon: the pipeline is a CLI over local files plus
DuckDB, so a deploy is a checkout, a virtualenv, `pi`, and `.env`.

```bash
git clone <repo> && cd financial-report-llm
uv sync                                   # .venv from uv.lock
npm i -g @earendil-works/pi-coding-agent  # pi ≥ 1.1 (Node ≥ 22.19)
cp .env.example .env                      # COMMAND_CODE_API_KEY, EXA_API_KEY
pi --list-models commandcode              # must list models (proves the key)
uv run scripts/analysis_pipeline/pipeline.py --dry-run \
  --tickers DKFT --years 2026 --quarters Q1
```

- Pi packages (exa search, web access, the Command Code provider) are pinned in
  `.pi/settings.json` and installed on first run; pipeline runs pass `--approve`
  because project packages only load after project trust.
- `.pi/xdg/` must be writable — `pi_agent.py` points `XDG_CONFIG_HOME` there so
  the research runner picks up `.pi/xdg/pi/web-search.json`.
- Runtime state is local: `idx_data/pipeline.duckdb` (job state + statements)
  and `idx_data/notes_rag.duckdb` (vault index) are single-writer DuckDB files.
  Do not run two pipeline/`chat-app` writers against the same copy — for a shared
  server, keep DuckDB on the host and give each concurrent consumer its own
  process (DuckDB handles multi-process reads, not concurrent writers).
- Secrets live only in `.env`; nothing else in the tree needs credentials.

## Research chat (`chat-app/`)

A local, verified chat over the fact sheets, captured operational documents, and
the notes vault. It is a separate product surface: `chat-app/` reads the
pipeline artifacts and never writes pipeline job state.

```bash
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765
```

Every displayed number is produced by a typed tool and passes a deterministic
number audit; unverifiable answers are refused. Missing operational data returns
`not_yet_published`, `not_disclosed`, `not_captured`, or `unsupported` with
evidence — never "does not exist".

Start here:

- [`chat-app/RUNBOOK.md`](chat-app/RUNBOOK.md) — start the UI, ingest a document,
  extract, enrich, replay a trace, roll back a bad extract.
- [`chat-app/CONTRACTS.md`](chat-app/CONTRACTS.md) — tool, agent, envelope, and
  web API contracts.
- [`chatbot-plan/MVP-PLAN.md`](chatbot-plan/MVP-PLAN.md) — scope, acceptance
  criteria, and the build plan.

Quick checks:

```bash
uv run pytest tests/ chat-app/tests -q
PYTHONPATH=chat-app uv run python -m chat_app.cli --list
PYTHONPATH=chat-app uv run python -m chat_app.eval \
  --questions chat-app/eval/questions.yaml --dry-run
```
