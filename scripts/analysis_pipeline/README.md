# Analysis Pipeline

Automates equity-analysis markdown generation from docling-converted IDX
reports (`results/{TICKER}_{Q}_{YEAR}.md`) using the OpenCode Go subscription
as the model API.

## Quick Start

```bash
# Set your API key in .env (gitignored)
# OPENCODE_API_KEY=your-key-here

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
| 1b | `chunker.py` + `extract.py` | Seeded mode: note chunks only, slim prompt (details + context_notes). Fallback (no xlsx): full statement extraction per chunk | Cheap (deepseek-v4-flash, reasoning_effort=low) |
| 2a | `merge.py` | Seed is priority 0; LLM sheets gap-fill null keys + details. Conflicts logged (LLM-vs-seed = hallucination signal) | None |
| 2b | `ratios.py` | Compute all ratios deterministically (no LLM arithmetic) | None |
| 3 | `context.py` | Assemble static profile, outline skeleton, vault hits, market stub | None |
| 4 | `write.py` | One call to strongest model → analysis markdown | Strong (hy3) |
| 5 | `redflags.py` | Contradiction pass → bullet list for operator review | Cheap (deepseek-v4-flash) |

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

## Stage Controls

```bash
# Extract only (stops after fact sheets are produced)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --extract-only

# Write only (requires fact sheets + ratios on disk from a prior extract+merge)
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --write-only

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
    extract: deepseek-v4-flash
    write: hy3
    redflags: deepseek-v4-flash
  max_tokens:
    extract: 16384   # repair retry bumps to 32768 (truncation safety)
    write: 32768
    redflags: 4096
  temperature:
    extract: 0.0
    write: 0.3
    redflags: 0.0
  # deepseek-v4-flash is a reasoning model; reasoning tokens count against
  # max_tokens. Keep extraction at low effort (copy task) to stay fast.
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
