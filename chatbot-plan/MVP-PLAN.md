# IDX Research Chat — MVP Implementation Plan

**Status:** implementation plan · **Last updated:** 2026-10-25 · **Owner:** Hannan
**Horizon:** 10 working days (2 weeks, solo) · **Supersedes:** nothing — this is the
build plan for [`README.md`](./README.md). The product thesis there is unchanged.

---

## 0. What v0 ships

A **local web chat** over the fact sheets and operational documents already in this
repo, where:

1. every number is produced by a typed tool and passes a deterministic number audit;
2. every causal claim is labelled **fact** / **management-stated** / **hypothesis**;
3. a missing operational figure always returns one of three states — `not_yet_published`,
   `not_disclosed`, `not_captured` — with evidence, never "does not exist";
4. every displayed number links to `sheet_code:row_number` or `document:page`.

**In scope:** P0 correctness fixes, fact-sheet enrichment (provenance + completeness +
operational), Class B ingest for the four question names, hybrid operational extraction,
typed tools, answer envelope + audit gate, web UI, eval harness.

**Out of scope for v0:** sector screens/peer ranking, the IDX-wide watcher, metered
research/credits, pricing/auth/billing, bank shape completion, market data, forecasts,
BUY/HOLD/SELL language.

---

## 1. Locked decisions

| # | Decision | Choice | Consequence for this plan |
|---|---|---|---|
| 1 | v0 cut line | Chat + fact sheet + operational | No `compare_peers`, no screens, no research in v0 |
| 2 | Delivery surface | Local web chat app (not Telegram) | FastAPI + one static page; CLI/Telegram adapters later |
| 3 | Coverage | Existing repo data, honest about gaps | 37 periods / 24 tickers; operational only where a document exists |
| 4 | Tool architecture | Typed tools only, no raw SQL | Traps A/B/C become structural, not prompt discipline |
| 5 | Fact-sheet access | Typed `company_facts` over YAML sections | Bounded context; YAML stays the single source of truth |
| 6 | Class B extraction | Hybrid: deterministic numbers + LLM verbatim drivers | Marginal cost is small but **not $0**; reconcile gate is mandatory |
| 7 | Acquisition | CLI ingest + cron for 4 tickers | No IDX-wide watcher in v0; measure fetch success first |
| 8 | Answer guarantee | Deterministic number-audit gate | Retry once, then refuse; no unverified number is displayed |
| 9 | Sequencing | P0 hard gate (1–2 days) | No chat code until P0 is merged |
| 10 | Effort | 2 weeks | Cron, streaming, multi-turn are the slack |
| 11 | In-flight work | Review + commit as baseline first | `BANK_IS_MAP` counts as done; clean tree before Day 1 |

### Defaults assumed (change before Day 1 if wrong)

| Topic | Default |
|---|---|
| Chat model | `deepseek-v4.1-flash`, `thinking: off` (measured $0.0028/answer) |
| Answer format | YAML envelope (`answer` + `claims[]` + `missing[]`), rendered by the UI |
| Multi-turn | Stateless server; client sends the last 4 turns |
| Fact sheets | Enriched **in place** (generated artifacts under `idx_data/analysis/facts/`) |
| Traces | JSON files under `idx_data/chat/traces/`, not a new DB table |
| Auth | None; bind `127.0.0.1`, single operator |
| New dependencies | `fastapi`, `uvicorn`, `pytest` (dev) |
| Concurrency | 2 concurrent `pi` runs max, enforced by a semaphore |

---

## 2. Supported dataset (as of 2026-10-25)

Derived from disk, not hand-written. The chat **must** be able to list this set and say
"unsupported" for everything else.

| Layer | Coverage | Where |
|---|---|---|
| XBRL statements | 51 tickers, 211,059 rows | `pipeline.duckdb → xlsx_statements` |
| Fact sheets | **37 periods / 24 tickers** | `idx_data/analysis/facts/{T}_{Q}_{Y}.yaml` |
| Ratios | all 37 periods | `{T}_{Q}_{Y}.ratios.yaml` |
| Notes index | 2,857 chunks / 25 ticker groups | `idx_data/notes_rag.duckdb` |
| **PTBA notes** | **0 chunks** | fixed by P0 #3, then re-index |
| Operational (Class B) | **ITMG Q2 2026 only** | `idx_data/operational/test/` |
| Analysis prose | ~60 files, most covered names | `analysis/*.md` |
| Market data | none | — |

**Fact-sheet periods present:** ADMR Q1/Q2, ADRO Q1/Q2, ARCI Q1/Q2, BBCA Q1/Q2,
BEST Q1/Q2, DKFT Q1, ELSA Q2, ESSA Q2, GZCO Q1, INKP Q1, ISAT Q1/Q2, ITMG Q1/Q2,
KLBF Q1, MAPA Q1/Q2, MEDC Q2, MHKI Q2, MSTI Q1/Q2, PGAS Q1, PTBA Q1/Q2, PTPS Q1/Q2,
SRSN Q1/Q2, TAPG Q1, TINS Q1/Q2, TPMA Q2.

**v0 operational targets:** ITMG Q2 (already captured), then PTBA Q2, PTPS Q2,
ESSA Q2 via ingest. Q1 documents are optional; the chat handles their absence with
`not_captured`.

This table is generated at startup by `list_companies()`; if it drifts, the tool wins
and the UI shows the tool's answer.

---

## 3. Architecture

### 3.1 Runtime flow

```
Browser (static page)
   │ POST /api/ask {question, ticker?, history[]}
   ▼
FastAPI (chat-app/chat_app/web.py)
   │ semaphore(2)
   ▼
Agent runner (chat-app/chat_app/agent.py)  ── pi -p --mode json ──►  model
   │  tools allowlist: company_facts, metric_series, period_derive,
   │  documents_status, operational_lookup, read_document, notes_search, notes_fetch
   ▼
Typed tool CLI (chat-app/chat_app/cli.py)  ── JSON stdout ──►  thin pi extension wrappers
   │ reads: facts/*.yaml, operational/*.json, rag DB (read-only)
   ▼
Answer envelope (YAML) → verify.py → ledger-audited answer → trace JSON → UI
```

### 3.2 Component map

| Path | Purpose | Status |
|---|---|---|
| `chat-app/chat_app/paths.py` | Artifact resolution + supported-set manifest | new |
| `chat-app/chat_app/facts.py` | Load fact sheets; section selection | new |
| `chat-app/chat_app/enrich.py` | Additive provenance/operational/completeness pass | new |
| `chat-app/chat_app/periods.py` | Cumulative→quarter derivation; unit/scale normalization | new |
| `chat-app/chat_app/operational.py` | Load operational entries + drivers | new |
| `chat-app/chat_app/tools.py` | Typed tool implementations (pure functions, JSON) | new |
| `chat-app/chat_app/cli.py` | `python -m chat_app.cli <tool>` for pi | new |
| `chat-app/chat_app/envelope.py` | Pydantic answer schema + YAML repair | new |
| `chat-app/chat_app/verify.py` | Citation audit, number ledger, label/missing-state checks | new |
| `chat-app/chat_app/agent.py` | `pi_agent.run_agent` wrapper + retry/refusal | new |
| `chat-app/chat_app/prompts/chat_system.md` | System prompt | new |
| `chat-app/chat_app/ingest.py` | Fetch CLI + manifests + IR scan | new |
| `chat-app/chat_app/extract_operational.py` | Hybrid extractor + reconcile gate | new |
| `chat-app/chat_app/metrics.yaml` | Deterministic metric grammar (coal v0) | new |
| `chat-app/chat_app/eval.py` | Eval harness | new |
| `chat-app/chat_app/web.py` + `static/` | FastAPI app + single page | new |
| `.pi/extensions/idx-tools.ts` | Register typed tool wrappers | modified |
| `scripts/analysis_pipeline/yaml_io.py` | Repair-and-fail YAML loader | new |
| `scripts/analysis_pipeline/{pipeline,db,merge,redflags,research,xlsx_seed,schema}.py` | P0 + provenance | modified |
| `scripts/notes_rag/rag.py` | Read-only search path | modified |
| `config.yaml` | `chat:` section, pricing table, `read` allowlist | modified |
| `chat-app/tests/test_chat_*.py`, `tests/test_p0_*.py` | Unit + integration | new |

### 3.2.1 Repository boundary

`chat-app/` is a top-level product component, not a pipeline stage. The folder is
`chat-app`; the importable package is `chat_app` (Python identifiers cannot contain a
hyphen). P0 pipeline fixes and their tests stay in `scripts/` and the root `tests/`.

- It **reads** fact sheets, operational JSON, `pipeline.duckdb` and `notes_rag.duckdb`
  read-only, and the `analysis/` vault.
- It **writes** only `idx_data/chat/traces/`, `idx_data/operational/` (ingest), and
  additive keys on fact sheets (`enrich`).
- Pipeline imports are limited to read-only helpers (`schema`, `xlsx_seed` label maps).
  It must never import `merge`, `write`, `research`, or `redflags`.

### 3.3 Why pi + a Python tool CLI

- The repo already runs `pi -p --mode json` in production for research and writing, with
  a working runner (`pi_agent.py`), trace correlation, usage/cost, and `.env` loading.
- Tools stay in **Python**, where the fact sheets, DuckDB, and tests live; the TypeScript
  extension is a thin `execFile` wrapper, matching the existing `notes_search` pattern.
- No raw SQL is exposed. `duckdb_query` remains registered for the analysis agents but is
  **not** in the chat allowlist.

Rejected alternatives: raw SQL (reintroduces Traps A/B/C), an MCP server (new runtime for
no v0 benefit), and a hand-rolled OpenAI function-calling loop (duplicates `pi_agent`).

---

## 4. Contracts

### 4.1 Fact-sheet additions (additive, defaults keep old sheets valid)

```yaml
meta: {ticker, quarter, year, currency, units, audited, period_end, scale}

statements:
  income_statement:
    lines: {...}
    prior_lines: {...}
    provenance:
      revenue: {sheet: "1311000", row: 6, label_en: "Sales and revenue",
                column: value_current, period_type: cumulative}

operational:
  entries:
    - {metric: coal_production, scope: Q2-2026, value: 5.3, unit: Mt,
       prior: null, source_doc: itmg_2026-08-10_news_release, page: 1,
       published_at: 2026-08-10, status: preliminary,
       verification: {method: quoted_in_source, reconciled_to: null}}
  drivers:
    - {driver: higher strip ratio, affects: cost_of_revenue,
       quote: "higher coal mining and transportation costs, resulted from higher strip ratio",
       page: 1, source_doc: itmg_2026-08-10_news_release, type: management_stated}

documents:
  - {id: itmg_2026-08-10_news_release, type: press_release, published: 2026-08-10,
     url: "...", fetched_at: 2026-10-25T10:00:00Z, pages: 8, sha256: "...",
     path: idx_data/operational/ITMG/..., status: captured, error: null}

completeness:
  financials: complete          # complete | partial | missing
  operational: partial          # complete | partial | not_captured
  notes: indexed                # indexed | missing | stale
  analysis: present
  expected_documents: [filing, press_release, presentation]
  as_of: 2026-10-25
  gaps: [{item: presentation, state: not_captured, detail: "fetch 403"}]
```

Enrichment is idempotent: `enrich.py` writes only these new keys, never statement values,
and records `enriched_at` + a content hash. Re-running is a no-op.

### 4.2 Operational entry rules

- **Numbers are deterministic.** The LLM never emits a number as data. It returns quotes,
  page numbers, and `affects`; code extracts values from verified substrings.
- **Two verification classes:**
  - `reconciled`: overlaps an XBRL line and is within `max(0.5%, rounding unit)`.
  - `quoted_in_source`: non-XBRL metrics (production, volumes, ASP) whose value appears
    in the exact sentence at the cited page, with unit and scope.
- **Rejection is loud.** A failed reconciliation marks the entry `rejected`, the document
  `extraction_warnings`, and `completeness.operational = partial`. Nothing is dropped.
- **Revision semantics:** `published_at` + `supersedes`; never overwrite a prior vintage.

### 4.3 Three-state missing data (deterministic, no LLM)

| State | Condition | Answer contains |
|---|---|---|
| `not_yet_published` | No document captured and period end + 45d ≥ today | "No release captured yet; this issuer's releases have landed up to T+41" |
| `not_disclosed` | A captured document exists but does not break out the metric | Document id + page range + "it does not break out tonnage" |
| `not_captured` | A URL was discovered but fetch/convert failed | URL + HTTP status/error + retrieval date |
| `unsupported` | Ticker/period absent from the supported set | The supported periods for that ticker |

`documents_status` is the only tool allowed to answer existence questions.

### 4.4 Answer envelope

```yaml
answer: |
  Markdown. Every number appears in a claim below.
claims:
  - text: "Q2 coal production was 5.3 Mt, +13% QoQ."
    type: fact
    citations:
      - {tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: itmg_2026-08-10_news_release, page: 1}
  - text: "Management attributes the cost increase to a higher strip ratio and fuel costs."
    type: management_stated
    citations:
      - {tool: operational_lookup, ticker: ITMG, driver: higher strip ratio, page: 1}
missing:
  - {item: coal_production, state: not_captured, evidence: {url: "...", error: "403"}}
```

`type` is required for any sentence containing causal language (`because`, `due to`,
`driven by`, `reflecting`, `as a result`, `karena`, `didorong`, `akibat`). The UI renders
each as a badge; the verifier rejects `management_stated` claims without a document+page.

### 4.5 Number-audit rules (the actual moat)

1. Build a **ledger** from every tool result in the trace: raw values, canonical
   `value_idr_bn`, derived values (`period_derive` operands/formula), ratios, and counts.
2. Parse every numeric token in `answer` and `claims[].text`.
3. Whitelist structural numbers: years 2000–2100, `Q1–Q4`, page numbers, citation ids,
   and ratios explicitly emitted by tools.
4. A number passes if it matches a ledger value at display precision (exact for counts;
   ±0.5% for monetary/ratio values), or is an operand/result of an arithmetic expression
   whose operands are in the ledger.
5. Any other number is a violation. Retry **once** with the violation list; if it fails
   again, return `status: refused` with the offending numbers and do not render the answer.
6. Citation audit: every citation must match a tool call in the trace (same ticker, metric,
   doc, page). Unmatched citations are removed and the claim is downgraded or rejected.
7. Missing-state audit: any sentence claiming absence must have a `documents_status` or
   `list_companies` call in the trace **and** a `missing[]` entry. This is the U1 fix at the
   answer layer, independent of the prompt rule.

Every run writes `idx_data/chat/traces/{utc}_{id}.json`:
question, envelope, tool trace, ledger, violations, usage/cost, duration, model, refusal flag.

---

## 5. Workstreams

Effort estimates are focused solo hours; `d` = working day. Each task has an exit test.

### WS0 — Baseline (0.5d)

| # | Task | Exit test |
|---|---|---|
| 0.1 | Review the in-flight diff (`xlsx_seed`, downloader, parser, research prompt) and commit it | Clean tree; `BANK_IS_MAP` goldens pass |
| 0.2 | Branch `chat-mvp`; add `fastapi`, `uvicorn`, `pytest`; create the `chat-app/` skeleton (`chat_app/`, `eval/`, `tests/`) | `uv run python -c "import fastapi"` |
| 0.3 | Freeze the supported-set snapshot in `chat-app/eval/supported-set.json` | Matches §2 |

### WS1 — P0 correctness gate (2d, hard gate)

| # | Task | Files | Exit test |
|---|---|---|---|
| 1.1 | `loads_with_repair()`: strip fences, one conservative repair pass (trailing commas, unbalanced flow collections), then **raise**. Use in research brief, merge, redflags, planner. No more `except: {}` | `yaml_io.py`, `research.py:446`, `merge.py`, `redflags.py`, `research_plan.py` | PTBA Q2 brief parses; a corrupt brief **fails** the research stage |
| 1.2 | Empty-output guards: research brief (empty `findings` when triggers fired), redflags (<40 chars), merge (no statements and no details), context. `completed` requires non-empty output | `research.py`, `redflags.py`, `merge.py`, `context.py` | Injecting an empty response marks the stage `failed` |
| 1.3 | New `index` stage after `redflags`: incremental `rag.py index`; new `stage_index` column; failure marks stage failed without deleting prior chunks | `pipeline.py`, `db.py` | PTBA Q1 run yields >0 chunks; `--status` shows `index` |
| 1.4 | Ban "no results ⇒ doesn't exist": add `read` to `analysis.research.pi.tools`; add the rule to research + chat + writer prompts; typed tools return explicit states | `config.yaml`, prompts | U1 question returns a state or a number, never "note does not exist" |
| 1.5 | `rag.py` split connections: `connect(read_only=True)` for search/fetch, `connect_rw()` for index; `LOAD fts` only | `rag.py` | 1 index + 3 concurrent searches with no "locked" error |
| 1.6 | Cost accounting: add `cost_extract`, `cost_research_llm`, `cost_exa`, `cost_write`, `cost_redflags`, `cost_operational`; `pricing.yaml` for token→USD (labelled `estimated`); add Exa from `pi_meta.exa_cost_usd`; `--status` breakdown | `db.py`, `pipeline.py`, `config.yaml` | PTBA Q1 total within 2% of measured $0.2706 |

### WS2 — Grounding substrate (1.5d)

| # | Task | Files | Exit test |
|---|---|---|---|
| 2.1 | Schema additions: `provenance` on `Statement`; `operational`, `documents`, `completeness` on `FactSheet`; all default-empty | `schema.py` | All 37 existing sheets still validate |
| 2.2 | Provenance at seed: `_fetch_sheet_rows_with_code` also returns `row_number`; `_build_statement` emits `{sheet,row,label,column,period_type}`; pass `is_bank` map through | `xlsx_seed.py` | ITMG revenue → `1311000:6`; equity → parent vs total distinguished |
| 2.3 | `enrich.py`: resolve provenance for existing sheets via `IS_MAP`/`BANK_IS_MAP`/`BS_MAP`/`CF_MAP` + sheet precedence; merge operational JSON; build `documents`/`completeness`; idempotent | `chat-app/chat_app/enrich.py`, `facts.py` | 37 sheets enriched; second run is a no-op; ITMG Q2 shows 5/5 reconciled |
| 2.4 | Canonical units: `periods.py` returns `{raw, currency, scale, value_idr_bn, period_type}`; `period_derive` computes `tw2 − tw1` with operands | `periods.py` | ITMG Q2 revenue `1,000,491 − 497,577 = 502,914` with formula |

### WS3 — Class B ingest + extraction (2.5d)

| # | Task | Files | Exit test |
|---|---|---|---|
| 3.1 | Ingest CLI: `add --ticker --url --type --published`; curl_cffi `impersonate=chrome131`; save PDF + text + per-page map; manifest with `status/error/sha256/pages`; never overwrite a vintage | `ingest.py` | ITMG PDF re-add is a no-op; a 403 is recorded as `not_captured` |
| 3.2 | IR scan for 4 tickers: discover PDF links from configured IR/news pages, filter title/date, download; report **200-rate** | `ingest.py`, `config.yaml` | 4-ticker report printed; if <70% fetch success, **stop and fix the fetcher** |
| 3.3 | Bilingual handling: per-page language score + marker fallback; keep EN pages (or ID-only with ID patterns); retain original page numbers | `extract_operational.py` | ITMG 8-page doc does not double-count; page refs still correct |
| 3.4 | Deterministic metric grammar (`metrics.yaml`): coal production, sales volume, ASP, strip ratio, cash cost, plus financial overlaps; scope from sentence; prior from `from X to Y`/`vs X` | `metrics.yaml`, `extract_operational.py` | ITMG 11 metrics extracted; each retains sentence + page |
| 3.5 | LLM driver pass: one call/doc, cheap model; returns `{quote, page, affects}` only; quote must be an exact normalized substring of the page; no numeric output accepted as data | `extract_operational.py` | ITMG 5 drivers, all quotes verifiable |
| 3.6 | Reconcile-or-reject gate: overlap map → XBRL exact labels; unit/scale from `xlsx_meta.rounding`; cumulative scope for 6M; reject on >0.5% | `extract_operational.py` | ITMG 5/5; a tampered value is rejected, not published |
| 3.7 | Run on PTBA/PTPS/ESSA (whichever fetch); write `operational.json`; `enrich.py` merges | CLI | Each name reports `reconciled N/M`; gaps are explicit |

### WS4 — Typed tools (2d)

| Tool | Signature | Returns |
|---|---|---|
| `list_companies` | `()` | supported tickers/periods + layer completeness |
| `company_facts` | `(ticker, period, sections="core")` | compact sections + provenance; `details` only on request; hard output cap |
| `metric_series` | `(ticker, metric, periods?)` | values with `period_type`, scale, source, reconciled flag |
| `period_derive` | `(ticker, period, metric)` | quarter value + formula + operands |
| `documents_status` | `(ticker, period, item?)` | state enum + evidence (URL/status/page) |
| `operational_lookup` | `(ticker, period, metric?)` | entries + drivers with pages |
| `read_document` | `(ticker, doc_id, page)` | exact page text (≤4k chars) + URL |
| `notes_search` / `notes_fetch` | existing | existing |

Rules: no SQL; no fuzzy labels; sheet pinned per statement; cumulative vs quarter always
labelled; unsupported requests return `unsupported` + the supported list. Output capped at
~12 KB per call. Unit + integration tests for each tool.

### WS5 — Agent + verification (2d)

| # | Task | Exit test |
|---|---|---|
| 5.1 | `config.yaml → chat.pi` (model, timeout 180s, tools allowlist, no session/skills); system prompt with tool rules, label rules, missing-state rules, no forecasts/recommendations | `pi` can call every tool once |
| 5.2 | Envelope schema + repair/retry; fence stripping; YAML failure raises (no silent `{}`) | Malformed envelope → one retry → refusal |
| 5.3 | `verify.py`: citation audit, number ledger, structural whitelist, causal-label check, missing-state check, one retry, refusal | Tampered answer is refused; ITMG answer passes |
| 5.4 | Trace writer + `--replay` for a stored trace | Replay reproduces verdicts offline |
| 5.5 | Prompt/agent tuning on 6 seed questions (D1, B1, U2, U3, ITMG production, PTBA missing) | ≥5/6 pass without manual edits |

### WS6 — Web app (1.5d)

| # | Task | Exit test |
|---|---|---|
| 6.1 | FastAPI: `GET /`, `/api/companies`, `/api/health`, `POST /api/ask`, `GET /api/trace/{id}`; semaphore(2); 127.0.0.1 only | `curl` returns a verified answer |
| 6.2 | UI: ticker/period picker from `list_companies`; question box; label badges; citation chips → side panel with row/page; missing-data banner with evidence; verification + cost/latency footer | Manual click-through on ITMG + PTBA |
| 6.3 | Error states: unsupported, refusal, timeout, tool failure; no blank screen | All five forced states render |
| 6.4 | Design pass (frontend-design skill), accessible contrast, keyboard submit, mobile-safe layout | Light/dark readable; no horizontal scroll |

### WS7 — Eval (1d, overlapping WS5–6)

| # | Task | Exit test |
|---|---|---|
| 7.1 | `chat-app/eval/questions.yaml`: 10 probe + 3 user + 4 operational + 3 traps = 20, with `must_include`, `must_not_claim`, `expect_state`, `expect_label` | Schema validates |
| 7.2 | `eval.py`: run N, deterministic assertions + pass table + JSON artifact; cost/latency summary | One command produces a report |
| 7.3 | Acceptance run + fixes; log every number-audit escape for manual review | Thresholds in §7 met |

### WS8 — Ops + docs (0.5d, slack)

| # | Task |
|---|---|
| 8.1 | `cron` entry: weekly IR scan for 4 tickers → ingest → extract → enrich; log to `idx_data/chat/cron.log` |
| 8.2 | `chat-app/RUNBOOK.md`: start UI, add a document, re-run enrichment, replay a trace, rollback a bad extract |
| 8.3 | README pointer + architecture note; cost report from traces |

---

## 6. Ten-day schedule

| Day | Work | Exit |
|---|---|---|
| 0 (0.5d) | WS0 baseline + deps + supported-set snapshot | Clean tree, branch, deps import |
| 1 | WS1.1–1.2 (YAML fail, empty guards) + tests | Corrupt/empty inputs fail stages |
| 2 | WS1.3–1.6 (index stage, allowlist, read-only RAG, cost) | P0 gate green; PTBA indexed |
| 3 | WS2.1–2.2 (schema, provenance) + WS2.4 (canonical/derive) | ITMG rows trace to `1311000:6` |
| 4 | WS2.3 (`enrich.py` over 37 sheets) + goldens | All sheets enriched, idempotent |
| 5 | WS4 tools + CLI + unit tests; extension wrappers | Every tool returns JSON; pi calls one |
| 6 | WS3.1–3.3 (ingest, scan, bilingual) for PTBA/PTPS/ESSA | Fetch report printed; 200-rate known |
| 7 | WS3.4–3.7 (grammar, drivers, reconcile, run 4 names) | ITMG 5/5 + at least 2 new docs |
| 8 | WS5.1–5.3 (agent, envelope, audit) | 5/6 seed questions verified |
| 9 | WS6 web app + UI | Working chat in browser |
| 10 | WS7 eval + fixes + WS8 cron/runbook + commit | §7 thresholds met |

**Parallelizable:** 6 (ingest) can run while 5 (tools) is in progress; 10 is partly
buffer. **Critical path:** P0 → provenance/enrich → tools → agent/verify → eval.

---

## 7. Acceptance criteria

| # | Criterion | Threshold |
|---|---|---|
| A1 | Displayed numbers that fail the audit | **0** (gate refuses instead) |
| A2 | False "does not exist / not available" claims | **0** |
| A3 | Eval questions correct | **≥17/20**, including all 3 user questions |
| A4 | Causal claims labelled | **100%** |
| A5 | Answers with ≥1 valid citation | **100%** |
| A6 | Median cost per answer | **≤ $0.005** |
| A7 | p95 latency (local, 2 concurrent) | **≤ 60s** |
| A8 | Operational reconciliation on ITMG | **5/5 within 0.5%** |
| A9 | Repeat enrichment | idempotent (no diff) |
| A10 | Unsupported ticker/period | explicit `unsupported` + alternatives |

Seed questions (A3 subset):

1. ITMG Q2 2026 coal production (operational; number + page).
2. ITMG Q2 COGS driver (management-stated + arithmetic).
3. PTBA Q2 coal production (state, not a lie: `not_captured` with URL/error or a number).
4. PTPS Q2 net profit decline (hypothesis/fact separation; tax + other-gains arithmetic).
5. PTPS Q1 COGS premise correction (COGS fell; COGS/revenue rose).
6. ITMG Q2 revenue 502,914 (cumulative→quarter derivation shown).
7. BBCA gross margin (correct refusal: bank has no COGS).
8. ESSA top-2 customer concentration (notes; citation).
9. PTBA related-party sales (notes; PTBA must have chunks after P0 #3).
10. ANJT currency conversion (refuse an unauditable unit conversion).

---

## 8. Cut lines (in order)

If the schedule slips, cut in this order — never the P0 gate, provenance, the audit gate,
or the web chat:

1. Cron/IR scan (WS8.1) → manual ingest.
2. SSE/streaming (never in v0 scope) → spinner + elapsed time.
3. `read_document` + `notes_fetch` tools → `company_facts` + `notes_search` only.
4. Multi-turn history → single-turn.
5. PTPS/ESSA operational ingest → ITMG + PTBA only.
6. UI polish → functional unstyled page.

**Do not cut:** WS1, provenance, `documents_status`, `verify.py`, eval A1–A3.

---

## 9. Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| pi tool wrapper drift (TS vs Python) | medium | Python CLI is the only implementation; one integration test calls each tool **through pi** |
| Envelope non-compliance / YAML failures | medium | Repair once, retry once, then refuse; never `or {}` |
| Number-audit false positives (years, page refs, ratios) | medium | Structural whitelist + ledger tolerance + escape log reviewed on Day 10 |
| Operational extraction brittle on new layouts | high | Reconcile-or-reject; page-verbatim checks; publish `partial`, never guess |
| IR fetch success <70% | medium | Measure on Day 6; per README, fix the fetcher before building more extraction |
| Fact-sheet enrichment corrupts values | low | Only additive keys; hash + idempotency test; goldens on ITMG/PTBA/BBCA |
| Cost creep from large tool outputs | medium | 12 KB cap per tool, max 8 tool calls/answer, cost in every trace |
| DuckDB lock under concurrency | medium | Chat is read-only; RAG search read-only; ingest/index are separate processes |
| LLM invents a driver | medium | Drivers are verbatim quotes verified as substrings with page refs |
| Scope creep toward screens/research | high | §0 out-of-scope list; `compare_peers` is explicitly v1 |

---

## 10. Deferred to v1 (tracked, not lost)

- `compare_peers()` with period pinning and `excluded[{ticker, reason}]` (P2 #15).
- Sector lenses (plantation/extractive/banking) and the 5 palm-oil screens (P3).
- IDX-wide watcher + T+45 scheduler (P1 #8) — v0 is CLI + 4-ticker cron.
- Bank shape completion beyond `BANK_IS_MAP` (P3 #18) — BBCA answers work, ratios do not.
- `checks[]`/`caveats[]` mechanization (P2 #17); v0 has a static caveat list in the tool.
- Top-100 XLSX backfill (P4 #21); the supported set stays honest at 24 tickers.
- Metered research/credits, pricing, auth, OJK review (P5/P6).
- Prose reports: keep the archive; no new writer work.

---

## 11. Definition of done

```bash
# 1. P0 gate
uv run pytest tests/test_p0_*.py -q
uv run scripts/analysis_pipeline/pipeline.py --tickers PTBA --years 2026 --quarters Q1 --force
duckdb -readonly idx_data/notes_rag.duckdb "SELECT COUNT(*) FROM chunks WHERE tickers LIKE '%PTBA%';"  # > 0

# 2. Grounding
PYTHONPATH=chat-app uv run python -m chat_app.enrich --all
PYTHONPATH=chat-app uv run python -m chat_app.cli company_facts --ticker ITMG --period Q2 --year 2026 | head

# 3. Operational
PYTHONPATH=chat-app uv run python -m chat_app.extract_operational --ticker ITMG --period Q2 --year 2026  # 5/5 reconciled

# 4. Chat
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765
# ask the 10 seed questions in the browser; every answer shows citation + label

# 5. Eval
PYTHONPATH=chat-app uv run python -m chat_app.eval --questions chat-app/eval/questions.yaml
# A1=0 A2=0 A3>=17/20 A4=100% A5=100% A6<=0.005 A7<=60s
```

Ship when the eval report is attached to the commit and every failing question has a
written reason (not a retry).
