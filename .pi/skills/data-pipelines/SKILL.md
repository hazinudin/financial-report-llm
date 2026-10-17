---
name: data-pipelines
description: Run or inspect the IDX report pipeline (download, identify, convert PDFs, parse XLSX into DuckDB) and the LLM analysis pipeline (generate equity-research markdown). Use when asked to download IDX reports, convert PDFs, ingest XLSX, run/generate an analysis, or check pipeline status.
---

# Pipelines

Run from the project root. Python deps are in `.venv`; prefer `uv run <script>`.

## Report pipeline — download → identify → collect → convert

```bash
# Full pipeline for tickers/years/periods (periods: TW1,TW2,TW3,audit)
uv run scripts/report_pipeline/pipeline.py --tickers BBCA,ESSA --years 2024 --periods audit

# Modes
uv run scripts/report_pipeline/pipeline.py --convert-only --tickers ESSA      # skip download
uv run scripts/report_pipeline/pipeline.py --identify-only --tickers ESSA     # pick the report PDF
uv run scripts/report_pipeline/pipeline.py --no-convert --tickers ESSA        # download+identify only
uv run scripts/report_pipeline/pipeline.py --parse-xlsx --tickers JARR        # ingest XBRL XLSX → DuckDB
uv run scripts/report_pipeline/pipeline.py --scan-disk --tickers ESSA         # register manually downloaded files
uv run scripts/report_pipeline/pipeline.py --dry-run --tickers ESSA --years 2024

# Jobs
uv run scripts/report_pipeline/pipeline.py --status
uv run scripts/report_pipeline/pipeline.py --retry-failed --stage convert
```

Identification strategies (`--strategy`): `auto` (default), `financial_statement`,
`laporan_keuangan`, `largest`. Output: PDFs in `raw/`, markdown in `results/`.

## Analysis pipeline — LLM equity research (needs `OPENCODE_API_KEY`)

Stages: deterministic preprocess → bounded chunk extraction → merge + deterministic
ratios → context assembly → external research (headless pi agent; needs `EXA_API_KEY`
plus `pi`/Node) → writer (headless pi agent) → red-flag pass. Writes markdown to
`analysis/` (or `idx_data/analysis/`, gitignored).

```bash
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --dry-run
uv run scripts/analysis_pipeline/pipeline.py --status
uv run scripts/analysis_pipeline/pipeline.py --list-models
```

Model IDs and timeouts are configured in `config.yaml` under `analysis`. See
`scripts/analysis_pipeline/README.md` for the full reference.

## Notes

- `OPENCODE_API_KEY` must be set in `.env` (gitignored). Stage 3b external
  research also needs `EXA_API_KEY`, plus `pi` ≥ 1.1 and Node ≥ 22.19 on `PATH`.
- `duckdb_query` reflects pipeline state via `pipeline_jobs`, `downloaded_files`,
  `xlsx_statements`, and `financial_ratios` — check those before re-running work.
- Prefer the narrowest command (e.g. `--parse-xlsx`, `--dry-run`) to avoid long runs.
