# Analysis Pipeline — Implementation Plan

**Status:** Draft for review
**Author:** OpenCode session (post-DKFT Q1 2026 analysis)
**Date:** 2026-07-23
**Scope:** Automate equity-analysis markdown generation (e.g., `analysis/DKFT Q1 2026 Analysis.md`) from converted financial reports (`results/{TICKER}_{Q}_{YEAR}.md`) using the OpenCode Go subscription as the model API, within a small-context, low-cost architecture.

---

## 1. Background and Motivation

The manual DKFT Q1 2026 analysis session consumed a very large context window:

| Activity | Approx. context share |
|---|---|
| Reading `results/DKFT_Q1_2026.md` (4,123 lines, ~250KB) | ~65% |
| Reading the ESSA template (857 lines) | ~15% |
| Vault + web context searches | ~10% |
| Writing + verification | ~10% |

Key insight: **~80% of the source file is noise** for analysis purposes:

- Note 2 ("Material Accounting Policy Information") alone is ~1,100 lines of boilerplate PSAK policy text.
- The docling output is bilingual (Indonesian + English), roughly doubling length.
- TOC, director statements, auditor letters, and repeated page headers add further bulk.

The analytically useful payload (4 primary statements + ~12 key notes) is ~700 lines / ~15–25K tokens. The architecture below is designed so that **no model ever reads the raw converted file**, and the hardest reasoning happens once, in a single writer call, over a compact structured fact sheet.

### Design principles

1. **Split reading by document chunk, never by analysis topic.** Cross-cutting insight (e.g., "revenue +20% but collections only 59.6% → receivables +216%") requires one mind seeing P&L, balance sheet, and cash flow together.
2. **One canonical fact sheet** is the single source of truth for all numbers in the final document.
3. **No LLM arithmetic.** All ratios/growth rates are computed deterministically in Python.
4. **Cheap models extract, strong models write.** Extraction is lookup; writing is synthesis.
5. **Follow existing repo conventions:** `uv run scripts/<pkg>/pipeline.py`, argparse CLI, DuckDB state, `--dry-run` / `--status` / `--force` flags, README updated.

---

## 2. Architecture Overview

```
results/DKFT_Q1_2026.md  (4,123 lines, bilingual, boilerplate)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 0  preprocess.py  (deterministic, no LLM)                 │
│  - strip page headers, TOC, director/auditor statements         │
│  - drop Note 2/3 accounting-policy sections (regex heading map) │
│  - collapse bilingual duplication where safely detectable       │
│  Output: idx_data/analysis/preprocessed/DKFT_Q1_2026.md (~700 ln)│
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1  extract.py  (LLM, cheap model, parallel calls)         │
│  Chunk A: primary statements (BS, P&L, CF, equity changes)      │
│  Chunk B: notes 1 & 4–15 (subsidiaries, IUP, BS notes)          │
│  Chunk C: notes 16–41 (liabilities, sales, COGS, segments…)     │
│  Each call: extraction prompt + chunk → YAML fact sheet (schema)│
│  Output: idx_data/analysis/facts/DKFT_Q1_2026_{A,B,C}.yaml      │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 2  merge.py + ratios.py  (deterministic, no LLM)          │
│  - merge chunk sheets → canonical fact sheet (conflict rules)   │
│  - validate against XLSX data in pipeline.duckdb (if present)   │
│  - compute ALL ratios (margins, growth, ROE, DSO, EV/EBITDA…)   │
│  Output: idx_data/analysis/facts/DKFT_Q1_2026.yaml + ratios.yaml│
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 3  context.py  (deterministic + existing tools)           │
│  - static company profile: analysis/companies/$DKFT.md          │
│  - outline skeleton: prompts/outline_skeleton.md (~50 lines)    │
│  - vault context: subprocess call to scripts/notes_rag/rag.py   │
│  - optional market-data stub (price/PE from pasted snapshot)    │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 4  write.py  (LLM, strongest model, ONE call)             │
│  Input: fact sheet + ratios + skeleton + profile + vault hits   │
│  Output: analysis/DKFT Q1 2026 Analysis.md                      │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 5  redflags.py  (LLM, cheap, short)                       │
│  "Given these facts, what contradicts what?" → bullet list      │
│  Optionally: human review, then write.py --inject-redflags      │
└─────────────────────────────────────────────────────────────────┘
```

All state tracked in `idx_data/pipeline.duckdb` (new `analysis_jobs` table).

---

## 3. Directory Layout

```
scripts/
└── analysis_pipeline/
    ├── IMPLEMENTATION_PLAN.md      # this document
    ├── README.md                   # usage docs (mirrors repo README style)
    ├── pipeline.py                 # orchestrator CLI (conventions match report_pipeline)
    ├── db.py                       # analysis_jobs schema + helpers (extends pipeline.duckdb)
    ├── opencode_client.py          # thin OpenCode Go API client (OpenAI-compatible)
    ├── preprocess.py               # Stage 0: deterministic markdown reducer
    ├── chunker.py                  # Stage 1: build extraction chunks by note-heading map
    ├── extract.py                  # Stage 1: LLM extraction calls (parallel, validated)
    ├── schema.py                   # pydantic models for the fact sheet
    ├── merge.py                    # Stage 2: merge + XLSX cross-validation
    ├── ratios.py                   # Stage 2: deterministic ratio computation
    ├── context.py                  # Stage 3: profile/skeleton/vault/market-data assembly
    ├── write.py                    # Stage 4: writer call
    ├── redflags.py                 # Stage 5: contradiction/red-flag pass
    └── prompts/
        ├── extract_system.md       # shared extraction system prompt
        ├── extract_chunk_A.md      # statements
        ├── extract_chunk_B.md      # notes 1, 4–15
        ├── extract_chunk_C.md      # notes 16–end
        ├── writer_system.md        # writer persona + style rules (mirrors ESSA doc conventions)
        ├── outline_skeleton.md     # the 14-section outline, headers only (~50 lines)
        └── redflags.md             # red-flag synthesis prompt
```

Runtime artifacts (gitignored):

```
idx_data/analysis/
├── preprocessed/{TICKER}_{Q}_{YEAR}.md
├── facts/{TICKER}_{Q}_{YEAR}_{chunk}.yaml
├── facts/{TICKER}_{Q}_{YEAR}.yaml          # merged canonical
├── facts/{TICKER}_{Q}_{YEAR}.ratios.yaml
└── runs/{job_id}.log
```

---

## 4. OpenCode Go API Integration

### Endpoint

- **Base URL:** `https://opencode.ai/zen/go/v1`
- **Auth:** `Authorization: Bearer $OPENCODE_API_KEY` (key from the OpenCode Zen console; store in existing `.env` — do not commit)
- **Surface:** OpenAI-compatible `POST /chat/completions` for most models (GLM, Kimi, DeepSeek, MiniMax, most Qwen). A small number of models are Anthropic-native (`/messages`); **avoid those** so one code path suffices.
- **Model discovery:** `GET /models` lists IDs available under the subscription (e.g., `glm-5.1`, `kimi-*`). Do not hardcode model names; read from `config.yaml`.
- **Not supported:** embeddings (the notes RAG already uses local Ollama `bge-m3` — unaffected).

### Client sketch (`opencode_client.py`)

```python
# thin wrapper — stdlib/httpx only, no SDK dependency required
import os, httpx

BASE_URL = os.getenv("OPENCODE_BASE_URL", "https://opencode.ai/zen/go/v1")
API_KEY  = os.environ["OPENCODE_API_KEY"]          # loaded from .env

def chat(model: str, system: str, user: str,
         temperature: float = 0.0, max_tokens: int = 4096) -> dict:
    r = httpx.post(
        f"{BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json={
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        },
        timeout=300,
    )
    r.raise_for_status()
    data = r.json()
    return {
        "content": data["choices"][0]["message"]["content"],
        "usage": data.get("usage", {}),     # prompt/completion tokens → DB
    }
```

### Model tiering (configure in `config.yaml`)

| Role | Tier | Guidance |
|---|---|---|
| Extraction (Stage 1) | cheapest available | lookup task; `temperature: 0`. Default candidate: the same family already used in `.opencode/agents/financial-report-reviewer.md` |
| Red flags (Stage 5) | cheap | short bounded output |
| Writer (Stage 4) | strongest in subscription | this is the only call where reasoning quality is binding |

```yaml
# config.yaml (extend existing file)
analysis:
  models:
    extract: glm-5.1        # confirm availability via GET /models
    write:   <strongest-go-model>
    redflags: glm-5.1
  max_tokens:
    extract: 8192
    write:   16384
  temperature:
    extract: 0.0
    write:   0.3
```

---

## 5. Stage Specifications

### Stage 0 — `preprocess.py` (deterministic)

**Input:** `results/{TICKER}_{Q}_{YEAR}.md` → **Output:** `idx_data/analysis/preprocessed/…` (~700–900 lines target)

Rules (all regex/heading-based, unit-testable):

1. Build a heading map of `^##\s+(\d+)\.` note headings.
2. **Drop** the note range for accounting policies (`2. Material Accounting Policy Information` / `Informasi Kebijakan Akuntansi`) up to the next numbered note — removes ~1,000+ lines.
3. **Drop** front matter: table of contents, director's statement letter (`SURAT PERNYATAAN DIREKSI` … up to first statement heading), auditor's report pages, `<!-- image -->` artifacts.
4. **Drop** repeated bilingual page-header banners (`PT X Tbk DAN ENTITAS ANAK Catatan atas…` repeated every page).
5. **Keep everything else**, including both language columns of tables (numbers are what matter; de-dup of prose is not worth the risk).
6. Emit a **section index** (JSON sidecar): `{statements: [line ranges], notes: {1: [start,end], 4: …}}` — consumed by the chunker.

Acceptance: DKFT file 4,123 → ≤ 900 lines; all 14-analysis-relevant notes present in the section index.

### Stage 1 — `chunker.py` + `extract.py` (LLM, parallel)

**Chunks** (from the section index; each ≤ ~8K tokens after preprocessing):

| Chunk | Content | Rationale |
|---|---|---|
| A | 4 primary statements | Highest information density; anchors all numbers |
| B | Note 1 (general/subsidiaries/IUP/reserves) + notes 4–15 (asset notes) | Asset-side facts |
| C | Notes 16–end (liabilities, equity, sales, COGS, opex, tax, EPS, related parties, segments, commitments) | Liability/P&L/segment facts |

If a chunk still exceeds the token cap, split at note boundaries (never mid-table).

**Extraction call:** system prompt (`extract_system.md`) + chunk-specific prompt + chunk text. Output must be **YAML conforming to `schema.py`**. Validation loop: pydantic parse → on failure, one automated repair retry with the validation errors appended → then mark job `failed` with the raw output saved for inspection.

**Parallelism:** `concurrent.futures.ThreadPoolExecutor(max_workers=3)` — mind subscription rate limits; make the worker count a CLI flag.

### Stage 2 — `merge.py` + `ratios.py` (deterministic)

**Merge conflict rules:**

1. Statement values (chunk A) win over note values on conflict.
2. Later notes win over earlier notes for the same field.
3. Every conflict is logged to `runs/{job_id}.log` with both values (this would have caught the DKFT note-29 customer-table row misalignment).

**XLSX cross-validation (unique to this repo — leverage it):** if `xlsx_statements` rows exist for the ticker/year/period (from `report_pipeline --parse-xlsx`), compare the fact sheet's primary-statement line items against them; flag deviations > 0.5% as warnings. This is a strong hallucination guard.

**`ratios.py`:** compute the full ratio battery from merged facts (the exact set used in the DKFT/ESSA analyses): growth rates, margin stack, liquidity/solvency/efficiency ratios, ROE/ROA (average balances), DSO/DIO/DPO/CCC, cash-conversion, FCF variants, EV/EBITDA, per-share metrics. Output `ratios.yaml` with formula + inputs recorded per ratio for auditability.

### Stage 3 — `context.py`

Assembles the writer's context package:

1. `analysis/companies/${TICKER}.md` static profile (create if missing; this is also where quarterly runs become *incremental* — static structure, licenses, board, loan history are reused).
2. `prompts/outline_skeleton.md` — the 14-section outline, headers only.
3. Vault context: `uv run scripts/notes_rag/rag.py search --ticker {T} --json "{commodity} outlook {year}"` → top 3 chunk IDs + `notes-fetch` (bounded, ~3 chunks max).
4. Market data stub: accept a `--market-data` flag with a pasted snapshot (price, market cap, P/E, 52-week range) or a small `market_data.yaml`. (Web fetching is intentionally out of scope for the deterministic pipeline; see §9 Open Questions.)

### Stage 4 — `write.py` (one call, strongest model)

Input package (target ≤ 30K tokens total): writer system prompt + skeleton + fact sheet + ratios + profile + ≤ 3 vault chunks + market-data stub. Output written to `analysis/{TICKER} {Q} {YEAR} Analysis.md`. Style rules in `writer_system.md` mirror the existing analyses (frontmatter tags, wikilinks, tables, severity-ranked risks, disclaimer).

### Stage 5 — `redflags.py` (optional, cheap model)

Bounded prompt: "Here is the fact sheet and computed ratios. List up to 10 contradictions, divergences, or anomalies an analyst must address (e.g., profit up but collections down)." Output is shown to the operator and optionally injected into a re-run of Stage 4 via `--inject-redflags` so the writer explicitly addresses them (this is how the receivables-vs-collections insight gets guaranteed, not hoped for).

---

## 6. Fact Sheet Schema (`schema.py`)

Versioned YAML/JSON (`schema_version: 1`). Core shape:

```yaml
meta:        { ticker, quarter, year, currency, units: rupiah|thousand|million, source_file, audited: bool }
income_statement:
  periods: [current, prior_yoy]
  lines:   { revenue, cogs, gross_profit, selling_expense, gna_expense, operating_profit,
             interest_income, interest_expense, other_income_net, pbt, tax, net_profit,
             np_owners, np_nci, eps_basic }
balance_sheet:
  periods: [current, prior_fy_end]
  lines:   { cash, trade_receivables_net, inventories, prepaid_taxes, securities_current,
             current_assets, securities_noncurrent, ppe_net, exploration_assets,
             mining_properties, intangibles, restricted_cash, noncurrent_assets,
             total_assets, trade_payables, taxes_payable, accrued_current,
             other_advances, current_liabilities, loans_current, accrued_noncurrent,
             reclamation_provision, loans_noncurrent, noncurrent_liabilities,
             total_liabilities, equity_owners, nci, total_equity }
cash_flow:
  lines:   { collections_from_customers, interest_received, payments_to_suppliers,
             salaries_paid, ocf, capex_ppe, capex_other, icf, loan_repayments,
             dividends_paid, fcf, net_change, cash_begin, cash_end }
details:   # free-form but schema-typed lists
  customers:        [ { name, amount, pct_of_sales } ]
  loans:            [ { lender, balance, rate, maturity, notes } ]
  subsidiaries:     [ { name, ownership_pct, activity, total_assets } ]
  segments:         [ { name, external_sales, internal_sales, gross_profit, net_profit, assets } ]
  production:       [ { product, volume_mt, period } ]
  reserves:         [ { product, residual_mt, as_of } ]
  related_parties:  [ { party, relationship, item, amount } ]
  commitments:      [ { description, amount, counterparty } ]
  dividends:        [ { period, per_share, total } ]
context_notes: [ "any qualitative fact the extractor judges material, verbatim-ish" ]
```

The `context_notes` escape hatch is deliberate: it lets the cheap extractor flag oddities (e.g., "selling expense dropped to zero vs 8.4B prior") without forcing schema coverage of everything.

---

## 7. State Tracking (`db.py`)

New table in `idx_data/pipeline.duckdb`, mirroring the `pipeline_jobs` pattern:

```sql
CREATE TABLE IF NOT EXISTS analysis_jobs (
  job_id            TEXT PRIMARY KEY,      -- {TICKER}_{YEAR}_{Q}
  ticker            TEXT,
  year              INTEGER,
  quarter           TEXT,
  stage_preprocess  TEXT DEFAULT 'pending',
  stage_extract     TEXT DEFAULT 'pending',
  stage_merge       TEXT DEFAULT 'pending',
  stage_context     TEXT DEFAULT 'pending',
  stage_write       TEXT DEFAULT 'pending',
  stage_redflags    TEXT DEFAULT 'pending',
  model_extract     TEXT,
  model_write       TEXT,
  tokens_prompt     INTEGER,
  tokens_completion INTEGER,
  error             TEXT,
  output_path       TEXT,
  updated_at        TIMESTAMP DEFAULT now()
);
```

Token usage per stage is recorded from API `usage` → cost observability and regression detection.

---

## 8. CLI Design

Mirrors `report_pipeline/pipeline.py` conventions:

```bash
# Full run for one report
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026

# Stage controls
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026 --extract-only
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026 --write-only
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026 --redflags --inject-redflags

# Batch + ops
uv run scripts/analysis_pipeline/pipeline.py --tickers ESSA,DKFT,ELSA --quarters Q1 --years 2026 --workers 3
uv run scripts/analysis_pipeline/pipeline.py --status
uv run scripts/analysis_pipeline/pipeline.py --retry-failed --stage extract
uv run scripts/analysis_pipeline/pipeline.py --dry-run --tickers DKFT --quarters Q1 --years 2026
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026 --force          # re-extract even if fact sheet exists
uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --quarters Q1 --years 2026 --market-data idx_data/analysis/market/DKFT.yaml
```

Behavioral notes:

- Default input resolution: `results/{TICKER}_{Q}_{YEAR}.md` must exist; else job marked `skipped` with a clear message (the report_pipeline owns download/conversion — separation of concerns preserved).
- `--write-only` requires merged fact sheet + ratios on disk (fast iteration on prose without re-spending extraction calls).
- Everything resumable: each stage is idempotent and skips completed work unless `--force`.

---

## 9. Context/Token Budget (per report)

| Component | Naive full-read (this session) | Proposed pipeline |
|---|---|---|
| Source ingestion | ~120K tokens (whole file, multiple passes) | 0 (deterministic) + 3 × ~8K extraction |
| Template | ~25K (full ESSA doc) | ~1K (skeleton) |
| Extraction output | — | 3 × ~2K |
| Writer input | n/a (same session) | ~20–30K total |
| **Total LLM tokens (approx.)** | **~150K+ in one giant window** | **~60K across isolated calls** |

Practical effect: extraction runs comfortably on small-context models; only the writer needs a mid-size window (~32K is ample). The Go subscription is flat-fee, so the win here is **context-window fit and latency/parallelism**, not dollar cost — but token tracking in DuckDB also future-proofs a move to pay-as-you-go Zen.

---

## 10. Testing & QA Plan

1. **Golden test:** run the pipeline on `results/DKFT_Q1_2026.md` and diff against the human-produced `analysis/DKFT Q1 2026 Analysis.md`:
   - All 14 sections present; frontmatter tags correct.
   - Every headline number in the generated doc exists in `ratios.yaml` (grep-based consistency check — no number may appear that is not in the fact sheet or market stub).
   - Numeric deviation vs. golden values: 0% (facts come from the same source deterministically).
2. **Unit tests** (`pytest`): preprocess reducers (fixture: trimmed DKFT markdown), heading-map builder, YAML schema validation, ratio formulas against hand-computed values.
3. **Regression suite:** re-run ESSA Q1 2026 and ELSA Q1 2026 through extract+merge; assert fact sheets match the numbers in their existing human analyses.
4. **XLSX cross-validation** wired as warnings in `runs/{job_id}.log` (not hard failures — XLSX coverage is partial).
5. **Manual review loop** remains: pipeline output is a draft; the `financial-report-reviewer` OpenCode subagent (`.opencode/agents/`) can be pointed at the draft for a final quality pass.

---

## 11. Phased Rollout

| Phase | Deliverable | Est. effort |
|---|---|---|
| **1. MVP (single-shot)** | `preprocess.py`, `schema.py`, one-call extract of the whole preprocessed file (fits 32K), `ratios.py`, `write.py`, `opencode_client.py`, minimal CLI, `analysis_jobs` table. Validate end-to-end on DKFT. | 1–2 days |
| **2. Chunked extraction** | `chunker.py`, parallel extraction, `merge.py` with conflict logging, repair-retry loop, XLSX cross-validation. | 1–2 days |
| **3. Quality & ops** | `redflags.py` (+ `--inject-redflags`), `context.py` with notes_rag integration + static profiles, `--status`/retry polish, token/cost dashboard, README + `scripts/analysis_pipeline/README.md`. | 1 day |
| **4. Batch & automation** | Batch mode across `results/`, reviewer-agent handoff, quarterly incremental mode (reuse static profile, only refresh numbers), optional market-data fetcher. | 1 day |

MVP exit criteria: DKFT draft generated with ≤ 3 LLM calls, all numbers traceable to the fact sheet, human edits limited to tone/focus.

---

## 12. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Docling table misalignment (seen in DKFT note 29 customer table) | Merge-conflict logging + XLSX cross-check; `context_notes` lets extractor flag suspicious rows verbatim |
| Extraction non-determinism / hallucinated numbers | `temperature: 0`, schema validation with repair retry, grep-consistency QA (§10.1), XLSX ground truth where available |
| Go subscription rate limits / endpoint quirks | `--workers` flag, exponential backoff + retry, model names externalized to `config.yaml`, avoid Anthropic-native-only models |
| Writer omits a key red flag | Stage 5 red-flags pass with `--inject-redflags` re-write; reviewer agent as final check |
| Schema drift across tickers (banks vs. miners vs. plantations) | `schema_version` field; per-sector extension keys under `details`; keep core lines generic |
| Static profile staleness | Profile is regenerated if missing; `--refresh-profile` forces rebuild; quarterly runs only touch numbers |
| `.env` key leakage | `.env` already gitignored; client reads env only, never logs headers |

---

## 13. Open Questions

1. **Market data:** keep as manual `--market-data` YAML stub (deterministic, honest), or add a scraper for stockanalysis.com (fragile, ToS-sensitive)? Recommendation: manual stub for v1; the analysis explicitly timestamps market data anyway.
2. **Writer model choice:** benchmark 2–3 Go models on the golden DKFT test and lock the winner in `config.yaml`. The `financial-report-reviewer` agent's current model is the natural baseline.
3. **Bilingual handling:** keep both languages in preprocessed output (safe) vs. attempt English-only reduction (aggressive)? Start safe; measure token counts; revisit.
4. **Anthropic-native Go models:** worth supporting `/messages` for a potentially stronger writer, or stay single-dialect OpenAI-compatible? Defer until benchmark says it matters.
5. **Should Stage 5 red flags auto-inject by default** (extra call per report) or stay opt-in? Default opt-in during the first month of calibration, then decide from yield.

---

## 14. Success Metrics

- **Context fit:** every LLM call ≤ 32K tokens.
- **Coverage:** ≥ 95% of golden-analysis numbers produced automatically (target 100% for statement-derived figures).
- **Cost:** ≤ 6 LLM calls per report end-to-end (3 extract + 1 write + 1 redflags + 1 repair allowance).
- **Latency:** ≤ 10 minutes per report wall-clock (parallel extraction).
- **Quality:** DKFT/ESSA/ELSA regression drafts require < 30 minutes of human editing each.
