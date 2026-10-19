# Analysis Pipeline

Automates equity-analysis markdown generation from docling-converted IDX
reports (`results/{TICKER}_{Q}_{YEAR}.md`) using the OpenCode Go subscription
as the model API.

## Quick Start

```bash
# Set your API key in .env (gitignored)
# OPENCODE_API_KEY=your-key-here
# EXA_API_KEY=your-exa-key-here   # Stage 3b external research

# List available model IDs under your subscription
uv run scripts/analysis_pipeline/pipeline.py --list-models

# Full run for one report
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1

# Dry-run (no model called, shows chunk breakdown)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --dry-run

# View job status
uv run scripts/analysis_pipeline/pipeline.py --status
```

## Pipeline Stages

| Stage | Script | What it does | Model |
|-------|--------|-------------|-------|
| 0 | `preprocess.py` | Strip policy boilerplate, front matter, page banners from docling output | None |
| 1a | `xlsx_seed.py` | Build meta + 3 primary statements deterministically from the IDX XBRL xlsx (pipeline.duckdb) | None |
| 1b | `chunker.py` + `extract.py` | Seeded mode: note chunks only, slim prompt (details + context_notes). Fallback (no xlsx): full statement extraction per chunk | Cheap (glm-5.3-flash, reasoning_effort=low) |
| 2a | `merge.py` | Seed is priority 0; LLM sheets gap-fill null keys + details. Conflicts logged (LLM-vs-seed = hallucination signal) | None |
| 2b | `ratios.py` | Compute all ratios deterministically (no LLM arithmetic) | None |
| 3 | `context.py` | Assemble static profile, outline skeleton, vault hits, market stub | None |
| 3b | `research_plan.py` | Planner: headless pi agent with read-only tools (`read`, `duckdb_query`, `notes_search`, `notes_fetch`), no web → themed question set | Strong (glm-5.3) |
| 3b | `research.py` | External events: headless pi agent (`pi -p --mode json`) with `exa_search` + `fetch_content`, then source-provenance audit | `deepseek-v4.1-flash` |
| 4 | `write.py` | Headless pi agent (`pi -p --mode json`) with read-only tools (`read`, `duckdb_query`, `notes_search`, `notes_fetch`) → analysis markdown | Strong (glm-5.3) |
| 5 | `redflags.py` | Contradiction pass → bullet list for operator review | Cheap (glm-5.3-flash) |

### XLSX seeding (Stage 1a)

Requires the report pipeline to have parsed the filing's
`FinancialStatement-*.xlsx` (`python scripts/report_pipeline/pipeline.py
--parse-xlsx --tickers X`). When coverage exists:

- statement chunks are skipped entirely — the largest, most failure-prone LLM
  calls disappear;
- values, units (`xlsx_meta.rounding`), period dates and audit status come
  straight from the filing — exact by construction, both period columns;
- signs are kept **as stored in the xlsx** (expenses/outflows positive), which
  is what `ratios.py` expects (`fcf = ocf − capex`);
- LLM chunk sheets only fill keys the seed leaves null (e.g.
  `dividends_paid`, `operating_profit`) plus `details`/`context_notes`.

Without coverage the pipeline falls back to the original full-statement LLM
extraction unchanged.

### External research (Stage 3b)

Runs when deterministic triggers fire on the fact sheet. The stage has two pi
agent runs.

**Planner** (`research_plan.py`, `analysis.plan.pi`) — formulates the themed,
prioritized question set before any searching. It uses the **read-only**
idx-tools (`read`, `duckdb_query`, `notes_search`, `notes_fetch`) so it can
verify implied period arithmetic and pull prior-period baselines, and
deliberately has **no web tools**: external evidence belongs to the search
agent. Every run writes `research/{T}_{Q}_{Y}.plan_pi/` (system/user prompts +
`events.jsonl`) and `research/{T}_{Q}_{Y}.plan_trace.json` (pi meta, usage
including reasoning tokens, retries, tool calls, budget warnings) — including
failed runs. A planner failure is **best-effort**: it prints
`[PLAN] FALLBACK: <reason>`, falls back to the deterministic question list, and
the fallback is recorded in the plan artifacts and in `--status` (`Plan` column).

**Search agent** — a second **headless pi agent** (`pi -p --mode json`) whose
tools are allowlisted to:

- `exa_search` — from the `pi-exa-search` extension, authenticated with
  `EXA_API_KEY` from `.env` (batched queries, recency/domain filters, highlights)
- `fetch_content` — from the `pi-web-access` extension (full page/PDF retrieval)

The agent emits a YAML brief; a source-provenance audit then checks every cited
URL against the tool trace and drops/downgrades anything unverified. `--no-search`
still forces the deterministic fallback, which skips both agents entirely.

Requirements: `pi` CLI ≥ 1.1 on `PATH`, Node ≥ 22.19, and `EXA_API_KEY` in
`.env` (gitignored). Project pi packages are declared (pinned) in
`.pi/settings.json` and loaded with `--approve` (project trust).

### Writer (Stage 4)

The writer also runs as a **headless pi agent** (`pi -p --mode json`) through
the shared runner `pi_agent.py`, configured under `analysis.write.pi` in
`config.yaml`. Its model falls back to the canonical writer model
`models.write` (`glm-5.3`) when `analysis.write.pi.model` is unset. Tools are
allowlisted to a **read-only** set:

- `read` — re-read any input file listed in the `# ARTIFACTS ON DISK` block
- `duckdb_query` — read-only SQL for peer comparison and this company's own
  prior periods (never for current-period company figures; the fact sheet wins)
- `notes_search` / `notes_fetch` — pull prior analyses / commodity notes from
  the vault

There are no web tools — external evidence stays owned by Stage 3b. An explicit
`analysis.write.pi.tools: []` downgrades the run to harness-only
(`--no-tools`).

Each run writes `idx_data/analysis/write/{TICKER}_{Q}_{YEAR}.write_pi/`
(`system.md`, `user.md`, `events.jsonl`) and an adjacent
`.write_trace.json` recording the pi meta (including cost), the correlated tool
trace, usage, and any budget warnings (prompt budgets: `max_db_queries: 6`,
`max_notes_search: 4`, `max_notes_fetch: 4`). A pi failure fails the `write`
stage — there is no automatic fallback. The pi session itself is persisted
(`analysis.write.pi.save_session: true`) under `~/.pi/agent/sessions/`, so it can
be browsed/resumed with `pi --resume`; its id is recorded in `write_trace.json`.

## Stage Controls

```bash
# Extract only (stops after fact sheets are produced)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --extract-only

# Write only (requires fact sheets + ratios on disk from a prior extract+merge)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --write-only

# Research only (triggers + plan + pi agent; stops before writing)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --research-only

# Research without web search (deterministic fallback: triggers + internal must-explain only)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --no-search

# Force a specific research mode (pi agent | deterministic_fallback)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --research-tool-mode deterministic_fallback

# Refresh just the research plan (planner agent only; skips the search agent)
uv run scripts/analysis_pipeline/research.py --ticker DKFT --year 2026 --quarter Q1 --plan-only --force

# Red-flag pass (+ optionally re-write to address each flag)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --redflags --inject-redflags

# Retry failed stages
uv run scripts/analysis_pipeline/pipeline.py --retry-failed --stage extract
```

## With Market Data

```bash
# Pass a market-data YAML stub for valuation ratios (price, market_cap, etc.)
cat > idx_data/analysis/market/DKFT.yaml <<EOF
price: 650
market_cap: 3580000000000
shares: 5513485875
pe_ttm: 5.2
beta: 0.59
source: StockAnalysis mid-July 2026
EOF

uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 \
  --market-data idx_data/analysis/market/DKFT.yaml
```

## Configuration

Model IDs, tokens, and worker count are in `config.yaml` under `analysis:`.
Run `--list-models` after setting your API key to confirm exact IDs.

```yaml
# config.yaml (snippet)
analysis:
  opencode:
    base_url: https://opencode.ai/zen/go/v1
    timeout_sec: 300
  models:
    extract: glm-5.3-flash
    write: glm-5.3
    research_plan: glm-5.3
    redflags: glm-5.3-flash
  # Stage 3b planner: headless pi agent, read-only tools, no web tools.
  plan:
    pi:
      provider: opencode-go
      # model omitted → falls back to models.research_plan (glm-5.3)
      thinking: "off"
      timeout_sec: 600
      approve: true
      load_skills: false
      load_context_files: false
      save_session: true
      tools: [read, duckdb_query, notes_search, notes_fetch]  # [] → --no-tools
      max_db_queries: 4
      max_notes_search: 3
      max_notes_fetch: 3
  # Stage 4 writer: headless pi agent with read-only tools (see below).
  write:
    pi:
      provider: opencode-go
      # model omitted → falls back to models.write (glm-5.3)
      thinking: "off"
      timeout_sec: 1800
      approve: true
      load_skills: false
      load_context_files: false
      save_session: true   # persist the pi session (~/.pi/agent/sessions/; resumable)
      tools: [read, duckdb_query, notes_search, notes_fetch]  # [] → --no-tools
      max_db_queries: 6
      max_notes_search: 4
      max_notes_fetch: 4
  max_tokens:
    extract: 16384   # repair retry bumps to 32768 (truncation safety)
    redflags: 4096
  temperature:
    extract: 0.0
    redflags: 0.0
  # Extraction is copy/lookup; reasoning tokens count against max_tokens, so
  # keep effort low (copy task) to stay fast and leave output headroom.
  reasoning_effort:
    extract: low
  extraction_input_token_cap: 16000
  extraction_workers: 4
```

## Runtime Artifacts (gitignored)

```
idx_data/analysis/
├── preprocessed/{TICKER}_{Q}_{YEAR}.md       # Stage 0 output
├── preprocessed/{TICKER}_{Q}_{YEAR}.index.json
├── facts/{TICKER}_{Q}_{YEAR}_chunkNN.yaml     # Stage 1 per-chunk sheets
├── facts/{TICKER}_{Q}_{YEAR}_manifest.json    # chunk -> covers map
├── facts/{TICKER}_{Q}_{YEAR}.yaml             # Stage 2a canonical merged
├── facts/{TICKER}_{Q}_{YEAR}.ratios.yaml      # Stage 2b computed ratios
├── runs/{TICKER}_{YEAR}_{Q}.merge.log         # conflict/warning log
├── runs/{TICKER}_{YEAR}_{Q}.context.json      # Stage 3 package
├── runs/{TICKER}_{YEAR}_{Q}.redflags.md       # Stage 5 output
├── research/{TICKER}_{Q}_{YEAR}.research.yaml        # Stage 3b brief (+ plan yaml/md, raw md)
├── research/{TICKER}_{Q}_{YEAR}.plan_pi/              # planner pi run dir: events.jsonl, system.md, user.md
├── research/{TICKER}_{Q}_{YEAR}.plan_trace.json       # planner pi meta + usage (incl. reasoning) + tool calls + budget warnings
├── research/{TICKER}_{Q}_{YEAR}.research_pi/         # pi run dir: events.jsonl (raw stream), system.md, user.md
├── research/{TICKER}_{Q}_{YEAR}.research_trace.json  # tool trace + source-provenance audit + pi meta
├── write/{TICKER}_{Q}_{YEAR}.write_pi/               # Stage 4 pi run dir: events.jsonl (raw stream), system.md, user.md
├── write/{TICKER}_{Q}_{YEAR}.write_trace.json        # pi meta (incl. cost) + tool trace + usage + budget warnings
└── market/{TICKER}.yaml                       # optional market data stubs
```

## Design Principles

1. **Split reading by document chunk, never by analysis topic.** Cross-cutting
   insight requires one mind seeing P&L, balance sheet, and cash flow together.
2. **One canonical fact sheet** is the single source of truth for all numbers
   in the final document.
3. **No LLM arithmetic.** All ratios/growth rates are computed deterministically
   in `ratios.py`.
4. **No LLM transcription either, when a machine-readable source exists.** The
   IDX XBRL xlsx carries the same primary statements as the PDF — seed from it
   and spend LLM calls only on what it lacks (note disclosures, oddity flags).
5. **Cheap models extract, strong models write.** Extraction is lookup; writing
   is synthesis.
6. **Every LLM call ≤ 32K tokens.** Chunks are built adaptively at note
   boundaries; oversized single notes split at sub-letter headings.
7. **Lenient validation.** Schema before-validators coerce null blocks, "55%"
   strings, YAML dates and type slips instead of failing; a chunk only fails
   when its payload is truly unusable.
