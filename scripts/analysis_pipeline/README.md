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
| 1 | `chunker.py` + `extract.py` | Build bounded chunks (never mid-table), extract numbers into YAML fact sheets | Cheap (glm-5.1) |
| 2a | `merge.py` | Merge chunk sheets with conflict rules; XLSX cross-validation | None |
| 2b | `ratios.py` | Compute all ratios deterministically (no LLM arithmetic) | None |
| 3 | `context.py` | Assemble static profile, outline skeleton, vault hits, market stub | None |
| 4 | `write.py` | One call to strongest model → analysis markdown | Strong (glm-5.2) |
| 5 | `redflags.py` | Contradiction pass → bullet list for operator review | Cheap (glm-5.1) |

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
    extract: opencode-go/glm-5.1
    write: opencode-go/glm-5.2
    redflags: opencode-go/glm-5.1
  max_tokens:
    extract: 8192
    write: 16384
    redflags: 4096
  temperature:
    extract: 0.0
    write: 0.3
    redflags: 0.0
  extraction_input_token_cap: 16000
  extraction_workers: 3
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
4. **Cheap models extract, strong models write.** Extraction is lookup; writing
   is synthesis.
5. **Every LLM call ≤ 32K tokens.** Chunks are built adaptively at note
   boundaries; oversized single notes split at sub-letter headings.
