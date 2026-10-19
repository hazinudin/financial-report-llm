# Repository: IDX Financial Report Pipeline (`financial-report-llm`)

This project downloads financial reports from the Indonesia Stock Exchange (IDX),
converts them to markdown (docling), parses XLSX statements into DuckDB, and runs a
multi-stage LLM analysis pipeline that writes equity-research notes into the
`analysis/` Obsidian vault.

## Layout

- `scripts/report_pipeline/` — download → identify → collect → convert + XLSX parser
- `scripts/analysis_pipeline/` — 6-stage LLM analysis pipeline (needs `OPENCODE_API_KEY`)
- `scripts/notes_rag/rag.py` — hybrid BM25 + vector search over `analysis/`
- `idx_data/pipeline.duckdb` — pipeline state + financial data (source of truth)
- `idx_data/notes_rag.duckdb` — vault search index
- `raw/` identified PDFs · `results/` converted markdown · `analysis/` Obsidian vault

## Environment

- Python dependencies live in `.venv`; run scripts as `.venv/bin/python <script>` or
  `uv run <script>`.
- `duckdb` CLI is installed. Open the database read-only:
  `duckdb -readonly idx_data/pipeline.duckdb`.
- Model IDs and timeouts live in `config.yaml`; secrets (`OPENCODE_API_KEY`,
  `EXA_API_KEY`) in `.env` (gitignored — never commit or print).
- `.pi/settings.json` declares the project's pinned pi packages
  (`npm:pi-exa-search@0.1.3`, `npm:pi-web-access@0.41.0`) plus a delta entry
  that disables the global `npm:pi-web-search` extensions in this project.
  Project packages load after project trust (`--approve` for pipeline runs).
- Stage 3b (research) runs on pi in two steps: the planner
  (`research_plan.py`) with the read-only idx-tools and **no** web tools, then
  the search agent with `exa_search` / `fetch_content`. Stage 4 (writer) also
  runs on pi with the read-only idx-tools `duckdb_query`, `notes_search`,
  `notes_fetch` plus `read`.
- `.pi/xdg/pi/web-search.json` configures `pi-web-access` for the Stage 3b
  research runner (fed via `XDG_CONFIG_HOME`); `.agents/skills/` holds dev-time
  Agent Skills (e.g. `build-with-exa`), not loaded by research runs.

## Conventions

- Quote financial values with their units, source table, and period.
  `financial_ratios` values are in billions of IDR (except `eps`);
  `xlsx_statements` values are raw IDR.
- Period formats: `financial_ratios.period` is `"YYYY-MM-DD"`; `xlsx_statements.period`
  and `pipeline_jobs.period` are lowercase `audit | tw1 | tw2 | tw3`.
- Prefer the notes vault (`notes_search`) and DuckDB (`duckdb_query`) over guessing.
  Cite the note path or the table you queried.
- Never commit runtime artifacts: `.env`, `idx_data/analysis/`, `.venv`, `idx_data/*.duckdb`.

## Project skills (`.pi/skills/`)

- `idx-financial-db` — query company ratios, balance sheet, income statement, cash flow
- `vault-search` — search the `analysis/` equity-research notes
- `report-review` — review/write an IDX equity analysis
- `data-pipelines` — run/inspect the report and analysis pipelines
