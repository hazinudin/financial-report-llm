# IDX Research Chat — MVP Implementation Plan

**Status:** implementation plan · **Last updated:** 2026-10-26 · **Owner:** Hannan
**Horizon:** 11 working days (2.5 weeks, solo) · **Supersedes:** nothing — this is the
build plan for [`README.md`](./README.md). The product thesis there is unchanged; the v0
surface is now **two linked workspaces** instead of one chat page.

> **Revision note (2026-10-26):** the existing single-page chat becomes the **desk**
> baseline. WS6 adds the document reader/chat and splits the shell; WS2/WS4/WS5 add the
> document index, tools and mode-aware audit. The P0 gate and the answer invariants are
> unchanged.

---

## 0. What v0 ships

A **local web app with two linked surfaces** over the analysis notes, fact sheets and
operational documents already in this repo:

1. **Document workspace** (`#/doc/{document_id}`, reached from the `#/documents` library)
   — the full analysis note (the analysis document under `analysis/`) is rendered in a
   reader on the left; a chat pinned to that document sits on the right. Users can
   read the whole note, ask about any section, select text and ask about the selection,
   and click a citation to scroll and highlight the cited section. The note is primary
   context for this chat; the model still has the primary tools for verification.
2. **Research desk** (`#/desk`) — a dedicated, full-width chat for wider-scope questions:
   multi-company comparison, sector narrative, cross-document synthesis, methodology and
   missing-data questions. It answers over the supported names only, cites every company
   and document it uses, and states coverage limits instead of implying IDX-wide coverage.

Both surfaces share one answer envelope, one number audit, one set of labels and one trace
format. A document-chat answer can be escalated to the desk with one click; a desk answer
that cites a note can be opened in the document workspace at the cited section.

```
Desktop sketch; below ~960 px the workspace becomes Document / Ask tabs.

Document workspace — #/doc/itmg-q2-2026-analysis
┌──────────────────────────────────┬─────────────────────────────────────┐
│ ITMG Q2 2026 Analysis            │ ASK THIS DOCUMENT                   │
│ Contents  §1 §2 §3 … §14         │ ITMG · Q2 2026 · secondary note     │
│                                  │ Q: What does the note say about     │
│ full markdown + tables           │    the cash-flow swing?             │
│                                  │ A: [NOTE] … §1 [1]                  │
│ select text → "Ask about this"   │    [FACT] … [company_facts]         │
│ citation click → scroll/highlight│ [Ask across companies →]            │
└──────────────────────────────────┴─────────────────────────────────────┘

Research desk — #/desk
┌──────────────────────────────────────────────────────────────────────┐
│ Supported set: 24 issuers · not IDX-wide                             │
│ Compare Q2 coal production across ITMG, PTBA, PTPS and ESSA…         │
│ Q → A   multi-ticker citations · comparison matrix · open note →     │
└──────────────────────────────────────────────────────────────────────┘
```

The product invariants are unchanged:

1. every number is produced by a typed tool (or a cited note passage whose number is in
   the document ledger) and passes a deterministic number audit;
2. every causal claim is labelled **fact** / **management_stated** / **analysis_note** /
   **hypothesis**;
3. a missing operational figure always returns one of four states — `not_yet_published`,
   `not_disclosed`, `not_captured`, `unsupported` — with evidence, never "does not exist";
   document chat adds a fifth, `not_in_document`, for "this note does not cover it";
4. every displayed number links to `sheet_code:row_number`, `document:page`, or
   `analysis_document:section:line`.

**In scope:** P0 correctness fixes, fact-sheet enrichment (provenance + completeness +
operational), Class B ingest for the four question names, hybrid operational extraction,
typed tools, **analysis-document index + reader + document-scoped chat**, **wide-scope
desk chat including a bounded multi-company comparison tool**, answer envelope + audit
gate, eval harness.

**Out of scope for v0:** **deterministic** sector screens and peer ranking, recommendation
language, the IDX-wide watcher, metered research/credits, pricing/auth/billing, bank shape
completion, market data, the model's own forecasts, BUY/HOLD/SELL language. Narrative
sector and comparison answers over the supported set are **in scope**; a complete IDX-wide
sector screen is not. Quoting a note's own published outlook is in scope; issuing a new
forecast is not. The desk never issues a recommendation, but it may report a note's
published view (including a rating) when it is clearly attributed to the note.

---

## 1. Locked decisions

| # | Decision | Choice | Consequence for this plan |
|---|---|---|---|
| 1 | v0 cut line | Document chat + desk chat + fact sheet + operational | No `compare_peers` ranking/screens, no research in v0 |
| 2 | Delivery surface | Local web app, two surfaces: document workspace + research desk | FastAPI + static shell; CLI/Telegram adapters later |
| 3 | Coverage | Existing repo data, honest about gaps | 37 periods / 24 tickers; operational only where a document exists |
| 4 | Tool architecture | Typed tools only, no raw SQL | Traps A/B/C become structural, not prompt discipline |
| 5 | Fact-sheet access | Typed `company_facts` over YAML sections | Bounded context; YAML stays the single source of truth |
| 6 | Class B extraction | Hybrid: deterministic numbers + LLM verbatim drivers | Marginal cost is small but **not $0**; reconcile gate is mandatory |
| 7 | Acquisition | CLI ingest + cron for 4 tickers | No IDX-wide watcher in v0; measure fetch success first |
| 8 | Answer guarantee | Deterministic number-audit gate | Retry once, then refuse; no unverified number is displayed |
| 9 | Sequencing | P0 hard gate (1–2 days) | No chat code until P0 is merged |
| 10 | Effort | 11 working days | Dual-surface UI adds one day; cron, streaming, polish are the slack |
| 11 | In-flight work | Review + commit as baseline first | `BANK_IS_MAP` counts as done; clean tree before Day 1 |
| 12 | Document corpus | Existing `analysis/**/*.md` notes, read-only | `analysis_docs.py` indexes by path, mtime and hash; no new writer stage |
| 13 | Document chat scope | Pinned note is primary context; primary tools allowed for verification | Note-derived claims use `analysis_note`; note numbers may never become `fact` |
| 14 | Wide-scope chat | Narrative comparisons over supported names; no ranking or screens | `compare_supported` returns a cited matrix with explicit exclusions |
| 15 | UI routing | Hash routes `#/documents`, `#/doc/{id}`, `#/desk` | No server-side router; deep links and back/forward work |

### Defaults assumed (change before Day 1 if wrong)

| Topic | Default |
|---|---|
| Chat model | `deepseek-v4.1-flash`, `thinking: off` (measured $0.0028/answer) |
| Answer format | YAML envelope (`answer` + `claims[]` + `missing[]` + `coverage?`), rendered by the UI |
| Multi-turn | Stateless server; client sends the last 4 turns per surface/document |
| Document corpus | `analysis/**/*.md`; 63 top-level notes (mostly company/period) plus company-profile, sector, commodity and article notes |
| Document context | Full note up to 64 KB prefetched; above that, outline + question-relevant sections, with section fetches |
| Comparison cap | 5 tickers × 4 metrics per `compare_supported` call |
| Fact sheets | Enriched **in place** (generated artifacts under `idx_data/analysis/facts/`) |
| Traces | JSON files under `idx_data/chat/traces/`, not a new DB table |
| Auth | None; bind `127.0.0.1`, single operator |
| New dependencies | `fastapi`, `uvicorn`, `pytest` (dev); no JS build step |
| Concurrency | 2 concurrent `pi` runs max, enforced by a semaphore |

---

## 2. Supported dataset (as of 2026-10-25)

Derived from disk, not hand-written. The chat **must** be able to list this set and say
"unsupported" for primary filing data outside it. The document reader is broader: it may
open any note on disk (see below), but a note is not a fact sheet.

| Layer | Coverage | Where |
|---|---|---|
| XBRL statements | 51 tickers, 211,059 rows | `pipeline.duckdb → xlsx_statements` |
| Fact sheets | **37 periods / 24 tickers** | `idx_data/analysis/facts/{T}_{Q}_{Y}.yaml` |
| Ratios | all 37 periods | `{T}_{Q}_{Y}.ratios.yaml` |
| Notes index | 2,857 chunks / 25 ticker groups | `idx_data/notes_rag.duckdb` |
| **PTBA notes** | **0 chunks** | fixed by P0 #3, then re-index |
| Operational (Class B) | **ITMG Q2 2026 only** | `idx_data/operational/test/` |
| Analysis notes | **63 top-level notes** (mostly company/period) + 53 company-profile/sector/commodity/article notes = 116 `.md` files | `analysis/**/*.md` |
| Market data | none | — |

The document reader is the **curated analysis vault** (`analysis/`), not the raw filing
text in `results/*.md`. Raw filings stay behind `read_document` and `documents_status`, so
the reader never confuses a filing with an analyst note.

**Fact-sheet periods present:** ADMR Q1/Q2, ADRO Q1/Q2, ARCI Q1/Q2, BBCA Q1/Q2,
BEST Q1/Q2, DKFT Q1, ELSA Q2, ESSA Q2, GZCO Q1, INKP Q1, ISAT Q1/Q2, ITMG Q1/Q2,
KLBF Q1, MAPA Q1/Q2, MEDC Q2, MHKI Q2, MSTI Q1/Q2, PGAS Q1, PTBA Q1/Q2, PTPS Q1/Q2,
SRSN Q1/Q2, TAPG Q1, TINS Q1/Q2, TPMA Q2.

**v0 operational targets:** ITMG Q2 (already captured), then PTBA Q2, PTPS Q2,
ESSA Q2 via ingest. Q1 documents are optional; the chat handles their absence with
`not_captured`.

**v0 document targets:** every analysis note on disk is readable. The acceptance set
uses ITMG Q2, PTBA Q2, PTPS Q2, BBCA Q2, ESSA Q2, MAPA Q2 and TAPG Q1, because they
exercise a mix of operational, banking, plantation and missing-data behaviour. Notes
without a resolvable ticker/period (sector, commodity, article) still open in the
reader and are searchable from the desk; they are labelled by `kind` rather than
forced into a company-period. Reading is **not** gated by the fact-sheet supported set:
a note that exists opens even when its ticker has no fact sheet. The document chat can
discuss that note, but any primary filing claim still goes through the supported-set
tools and may return `unsupported`.

The coverage table is generated at startup by `list_companies()`; the document library
is generated by `list_documents()` from the `analysis/` vault. If either drifts, the
tool wins and the UI shows the tool's answer. A missing note is an explicit empty state,
never a blank reader.

---

## 3. Architecture

### 3.1 Runtime flow

Two request paths share one agent, one envelope and one verifier. The browser never
decides existence, coverage or a number; it only chooses the surface and the scope.

```
Document workspace (#/doc/{id})
   │ GET /api/documents/{id}  → metadata + full markdown + outline + hash
   │ POST /api/ask {mode:"document", document_id, question, selection?, history[]}
   ▼
FastAPI (chat-app/chat_app/web.py) ── semaphore(2) ──► agent.ask(mode="document")
   │ prefetch the note through analysis_docs (full note ≤64 KB, else outline)
   │ tools: analysis_document, analysis_search, company_facts, metric_series,
   │        period_derive, documents_status, operational_lookup, read_document,
   │        notes_search, notes_fetch
   ▼
Envelope → verify.py (primary ledger + document ledger) → trace JSON → UI
   │ citations: {document, section, line, quote} or {ticker, metric, doc, page}
   ▼
Reader scrolls to the cited section; doc chat renders NOTE / FACT / MGMT / HYP

Research desk (#/desk)
   │ POST /api/ask {mode:"desk", question, ticker?, period?, history[]}
   ▼
FastAPI ── semaphore(2) ──► agent.ask(mode="desk")
   │ tools: list_companies, list_documents, compare_supported, company_facts,
   │        metric_series, period_derive, documents_status, operational_lookup,
   │        analysis_document, analysis_search, analysis_context, notes_search,
   │        notes_fetch, read_document
   ▼
Envelope → verify.py (primary ledger + multi-company citations) → trace JSON → UI
   │ every compared ticker cited; coverage/exclusions stated; no ranking language
   ▼
Desk thread; a cited note opens in the document workspace at that section
```

Both modes use the typed tool CLI, the same YAML envelope repair and the same
retry-then-refuse policy. The mode is recorded in the trace, so a document answer can
never be mistaken for a desk answer during replay.

### 3.2 Component map

| Path | Purpose | Status |
|---|---|---|
| `chat-app/chat_app/paths.py` | Artifact resolution + supported-set manifest | new |
| `chat-app/chat_app/facts.py` | Load fact sheets; section selection | new |
| `chat-app/chat_app/enrich.py` | Additive provenance/operational/completeness pass | new |
| `chat-app/chat_app/periods.py` | Cumulative→quarter derivation; unit/scale normalization | new |
| `chat-app/chat_app/operational.py` | Load operational entries + drivers | new |
| `chat-app/chat_app/analysis_docs.py` | Index `analysis/**/*.md`: doc IDs, metadata, outline, section slices, document ledger | new |
| `chat-app/chat_app/tools.py` | Typed tool implementations (pure functions, JSON) | new |
| `chat-app/chat_app/cli.py` | `python -m chat_app.cli <tool>` for pi | new |
| `chat-app/chat_app/envelope.py` | Pydantic answer schema + YAML repair + new labels/citations | new |
| `chat-app/chat_app/verify.py` | Primary/document ledgers, citation + label + missing-state + no-ranking audit | new |
| `chat-app/chat_app/agent.py` | `pi_agent.run_agent` wrapper, mode routing, prefetch, retry/refusal | new |
| `chat-app/chat_app/prompts/chat_system.md` | Shared system rules | new |
| `chat-app/chat_app/prompts/document_system.md` | Document-scoped rules (note is primary, `analysis_note` label) | new |
| `chat-app/chat_app/prompts/desk_system.md` | Wide-scope rules (comparison/sector coverage, no ranking) | new |
| `chat-app/chat_app/ingest.py` | Fetch CLI + manifests + IR scan | new |
| `chat-app/chat_app/extract_operational.py` | Hybrid extractor + reconcile gate | new |
| `chat-app/chat_app/metrics.yaml` | Deterministic metric grammar (coal v0) | new |
| `chat-app/chat_app/eval.py` | Eval harness (mode-aware assertions) | new |
| `chat-app/chat_app/web.py` | FastAPI app: document API + mode-aware ask + trace | modified |
| `chat-app/chat_app/static/index.html` | App shell + hash routes | modified |
| `chat-app/chat_app/static/app.js` | Shell, routing, theme, coverage feed | modified |
| `chat-app/chat_app/static/shared.js` | Escape-first markdown renderer, citation chips, trace inspector | new |
| `chat-app/chat_app/static/document.js` | Document library + reader + document-scoped chat | new |
| `chat-app/chat_app/static/desk.js` | Wide-scope research desk chat | new |
| `chat-app/CONTRACTS.md` | Tool/API/agent signatures for both modes | modified |
| `.pi/extensions/idx-tools.ts` | Register typed tool wrappers | modified |
| `scripts/analysis_pipeline/yaml_io.py` | Repair-and-fail YAML loader | new |
| `scripts/analysis_pipeline/{pipeline,db,merge,redflags,research,xlsx_seed,schema}.py` | P0 + provenance | modified |
| `scripts/notes_rag/rag.py` | Read-only search path | modified |
| `config.yaml` | `chat:` section, mode-specific prompts/allowlists, pricing table, `read` allowlist | modified |
| `chat-app/tests/test_chat_*.py`, `tests/test_p0_*.py` | Unit + integration | new |

### 3.2.1 Repository boundary

`chat-app/` is a top-level product component, not a pipeline stage. The folder is
`chat-app`; the importable package is `chat_app` (Python identifiers cannot contain a
hyphen). P0 pipeline fixes and their tests stay in `scripts/` and the root `tests/`.

- It **reads** fact sheets, operational JSON, `pipeline.duckdb` and `notes_rag.duckdb`
  read-only, and the `analysis/` vault.
- It **writes** only `idx_data/chat/traces/`, `idx_data/operational/` (ingest), and
  additive keys on fact sheets (`enrich`). The document index is in-memory; the reader
  has no annotations, comments or edit path in v0.
- Document IDs are derived deterministically from the note path (short path-hash suffix
  on collision); section IDs come from heading text; line numbers come from the current
  file. The content hash is freshness metadata, not part of the ID, so deep links survive
  an edit. Re-indexing an unchanged note is a no-op; a changed note re-indexes and the UI
  shows its `updated_at`.
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
  analysis: present            # present | stale | missing
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

### 4.3 Missing-data states (deterministic, no LLM)

| State | Condition | Answer contains |
|---|---|---|
| `not_yet_published` | No document captured and period end + 45d ≥ today | "No release captured yet; this issuer's releases have landed up to T+41" |
| `not_disclosed` | A captured document exists but does not break out the metric | Document id + page range + "it does not break out tonnage" |
| `not_captured` | A URL was discovered but fetch/convert failed | URL + HTTP status/error + retrieval date |
| `unsupported` | Ticker/period absent from the supported set | The supported periods for that ticker |
| `not_in_document` | Document mode only: the pinned note does not discuss the item | Section-search evidence + "this note does not cover X"; offer the desk or filings; never imply the underlying data is missing |

`documents_status` is the only tool allowed to answer existence questions about filings.
`analysis_search` (scoped to the pinned document) is the only tool allowed to answer
"does this note discuss X?"; it returns `not_in_document`, not `not_disclosed`. The two
states must never be conflated: a note can be silent on a metric the filing discloses,
and a filing can omit a metric the note discusses. A silent result is explicit:

```json
{"ok": true, "tool": "analysis_search",
 "data": {"query": "strip ratio", "document_id": "ptba-q2-2026-analysis",
           "matches": [], "state": "not_in_document",
           "evidence": {"sections_searched": 14, "terms": ["strip ratio", "stripping"]}}}
```

An empty `matches` list without `state: not_in_document` is not evidence and must not be
turned into a "does not exist" sentence.

### 4.4 Answer envelope

```yaml
answer: |
  Markdown. Every number appears in a claim below. In document mode, say plainly
  what the note says before adding filing-level verification.
claims:
  - text: "Q2 coal production was 5.3 Mt, +13% QoQ."
    type: fact
    citations:
      - {tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: itmg_2026-08-10_news_release, page: 1}
  - text: "The note argues the cash-flow collapse was a timing artifact, not operating deterioration."
    type: analysis_note
    citations:
      - {tool: analysis_document, document: itmg-q2-2026-analysis,
         section: "1. Executive Summary", line: 18,
         quote: "We treat this as a timing artifact rather than operating deterioration."}
  - text: "Management attributes the cost increase to a higher strip ratio and fuel costs."
    type: management_stated
    citations:
      - {tool: operational_lookup, ticker: ITMG, driver: higher strip ratio, page: 1}
missing:
  - {item: coal_production, state: not_captured, evidence: {url: "...", error: "403"}}
coverage:                      # desk mode only; optional in document mode
  scope: supported_set
  supported_tickers: 24
  compared_tickers: [ITMG, PTBA, PTPS, ESSA]
  caveat: "Supported names only; not an IDX-wide sector screen."
```

`type` is required for any sentence containing causal language (`because`, `due to`,
`driven by`, `reflecting`, `as a result`, `karena`, `didorong`, `akibat`). The UI renders
each as a badge:

| Type | Meaning | Required citation |
|---|---|---|
| `fact` | Reported by a filing, a tool value, or arithmetic over tool values | Primary tool call with ticker/metric plus row/page where available |
| `management_stated` | The company said it (driver, explanation, outlook already published) | `doc` + `page`; never a note only |
| `analysis_note` | Reproduced or paraphrased from an analyst note, including the note's own view, outlook or rating when attributed; secondary, never XBRL-audited | `document` + `section` + `line` + exact `quote` |
| `hypothesis` | The model's interpretation connecting facts; marked "likely"/"consistent with" in the text | Underlying fact citations; must not masquerade as fact or management |

The verifier rejects `management_stated` without a document+page and `analysis_note`
without a document+section+line+quote. A note-only figure must be labelled
`analysis_note`; it can never be promoted to `fact`. `notes_search`/`notes_fetch`
snippets are not `analysis_note` unless they resolve to an analysis-vault document;
otherwise they are discovery evidence and any claim built on them is `hypothesis` unless
a primary tool verifies it.

### 4.5 Number-audit rules (the actual moat)

1. Build a **primary ledger** from every primary tool result in the trace: raw values,
   canonical `value_idr_bn`, derived values (`period_derive` operands/formula), ratios,
   counts and `compare_supported` cells.
2. Build a **document ledger** from `analysis_document` and `analysis_search`: every
   numeric token with `{value, unit, document, section, line, quote}` plus raw/scaled
   variants, so a display rounding ("US$1.0bn") still matches the note's "US$1,000,491k".
   This ledger is evidence for note claims only; it is never merged into the primary
   ledger.
3. Parse every numeric token in `answer` and `claims[].text`.
4. Whitelist structural numbers: years 2000–2100, `Q1–Q4`, page numbers, citation ids,
   section numbers named in a citation, and ratios explicitly emitted by tools.
5. A primary number passes if it matches a primary ledger value at display precision
   (exact for counts; ±0.5% for monetary/ratio values), or is an operand/result of an
   arithmetic expression whose operands are in the primary ledger.
6. A document number passes only when the claim is `analysis_note` (or a `hypothesis`
   whose underlying facts cite note lines), the number matches the document ledger at
   display precision, and the claim's citation quote contains the number verbatim. A
   `fact` claim may not spend a document-ledger value. Arithmetic over two document-ledger
   values is allowed for `analysis_note` only when both operands are cited; otherwise the
   claim is `hypothesis`.
7. Any other number is a violation. Retry **once** with the violation list; if it fails
   again, return `status: refused` with the offending numbers and do not render the answer.
8. Citation audit: primary citations must match a tool call in the trace (same ticker,
   metric, doc, page); document citations must match an `analysis_document`/
   `analysis_search` call and resolve to an existing document, section and line, with the
   quote as a normalized substring of that section. Unmatched citations are removed and
   the claim is downgraded or rejected.
9. Label audit: every causal sentence is in `claims[]`; `management_stated` requires
   doc+page; `analysis_note` requires document+section+line+quote; `fact` may not cite a
   note as its only source.
10. Missing-state audit: any sentence claiming absence must have a `documents_status`
    (filings) or scoped `analysis_search` (document coverage) call in the trace **and** a
    `missing[]` entry. In document mode, note silence is `not_in_document`, never
    `not_disclosed` or "does not exist". This is the U1 fix at the answer layer,
    independent of the prompt rule.
11. No-advice / no-ranking audit: reject or refuse `BUY`/`HOLD`/`SELL`, price targets,
    "best/top pick" language, and any ordering of companies presented as an investment
    ranking **when the desk states or endorses them**. A quoted note view is allowed only
    as `analysis_note`, with document+section and explicit attribution ("the note
    concludes…"); the answer must not adopt it as the desk's recommendation. A comparison
    is a matrix of cited facts, not a league table.
12. Every run writes `idx_data/chat/traces/{utc}_{id}.json`: `mode`, `document_id`,
    selection echo, question, envelope, tool trace, primary/document ledgers, violations,
    usage/cost, duration, model, refusal flag and coverage block.

### 4.6 Analysis-document contract

`analysis_docs.py` indexes the vault without copying it. A document is:

```yaml
id: itmg-q2-2026-analysis          # stable path slug; 6-char path-hash suffix on collision
path: analysis/ITMG Q2 2026 Analysis.md
title: PT Indo Tambangraya Megah Tbk (ITMG) — Q2 2026
kind: company                      # company | sector | commodity | article | other
ticker: ITMG                       # null for sector/article notes
period: Q2                         # null when not a company-period note
year: 2026
updated_at: 2026-10-22T21:26:00Z   # file mtime, UTC
sha256: "..."
bytes: 45120
words: 6200
tags: [ITMG, earnings, Q22026]
aliases: ["ITMG Q2 2026"]
sections:
  - {id: "1-executive-summary", title: "1. Executive Summary", level: 2, line: 12}
  - {id: "3-1-gross-margin-analysis", title: "3.1 Gross Margin Analysis", level: 3, line: 128}
content: |
  ...full markdown, byte-for-byte...
```

Rules:

- **The reader gets the full file.** `GET /api/documents/{id}` never truncates content;
  the note is the product. The 64 KB cap applies only to the **model prefetch**, not to
  the reader or the API.
- **Above the cap, the model gets an outline first.** Above 64 KB, the runner prefetches
  the outline plus the sections most relevant to the question (using scoped
  `analysis_search`); `analysis_document(section=...)` fetches the rest. A document-chat
  answer that ranges over the whole note must call the sections it uses. One
  `analysis_document` call returns at most ~24 KB; the full-note prefetch is a runner-side
  read, not a normal tool result (it is recorded as a synthetic call below).
- **Prefetch is traceable.** The runner records the prefetch as a synthetic
  `analysis_document` call (the same pattern as the existing `analysis_context` prefetch),
  so `verify.py` can match a document citation to the exact document and section that the
  model actually saw.
- **Section IDs are stable.** Derive from heading text (slug + level), with a numeric
  suffix on duplicates. `line` is the 1-based line of the heading; the section runs to the
  next heading of the same or higher level.
- **Unscoped notes are first-class.** Company-profile, sector, commodity and article notes
  get `period/year = null` (and `ticker = null` where there is no issuer), a `kind`, and
  the same reader/chat. The UI shows `kind` instead of a company-period chip; the model
  cites document+section only.
- **Frontmatter is metadata, not prose.** YAML frontmatter is parsed into the document
  header (tags/aliases/dates) and stripped from the rendered body; unknown keys are shown
  read-only. The reader never renders a raw `---` block as an `<hr>`.
- **Wiki links resolve locally.** Obsidian-style `[[Note Name]]` links resolve against the
  document index; a missing target renders as plain text, not a broken link or a network
  request.
- **Tables are evidence too.** A number inside a pipe table becomes a document-ledger
  entry whose `quote` is the full table row and whose `line`/`section` point at the table.
  A note claim may cite that row; it still may not be promoted to `fact`.
- **Idempotent index.** Keyed by path + mtime + sha256. A changed note is re-indexed while
  its ID stays stable; the reader shows `updated_at`. A deleted note becomes an explicit
  missing-document state in the library, not a 500.
- **No writes from the reader.** No annotations, comments or edits in v0. Highlighting is
  client-side and ephemeral.

### 4.7 Ask API contract

```json
{
  "mode": "document",
  "question": "What does the note say about the cash-flow swing?",
  "document_id": "itmg-q2-2026-analysis",
  "selection": {
    "section": "1. Executive Summary",
    "line": 18,
    "quote": "We treat this as a timing artifact rather than operating deterioration."
  },
  "ticker": null,
  "period": null,
  "year": null,
  "history": [{"role": "user", "content": "..."}]
}
```

- `mode` is `document` or `desk`; omitting it preserves the old behaviour as `desk`.
- **Document mode:** `document_id` is required. The client sends only the ID; the server
  loads the note from the index, so document text is never client-supplied context.
  Ticker/period/year are derived from the document; supplied values are ignored and
  echoed back as `document` scope, not used to widen the query. `selection` is optional
  and is injected into the user turn as a quoted passage with its section/line. A
  selection quote that is not an exact substring of the document is dropped and echoed as
  `selection_dropped`, never trusted as document text.
- **Desk mode:** `document_id` and `selection` must be absent. `ticker/period/year` are an
  optional focus, not a hard filter: a comparison may name other supported tickers.
- The response adds `mode`, `document` (metadata or null), `selection_echo`, and the
  existing `trace_id/status/envelope/verification/usage/duration_s/model/tool_calls`.
- `GET /api/documents?ticker=&period=&kind=&q=` returns
  `{ok, documents: [...], count, as_of}` with metadata only.
  `GET /api/documents/{id}` returns metadata + content + outline.
- Every ask, in either mode, is subject to the same 180 s budget and the same 2-run
  semaphore. A document ask that times out leaves the reader usable; only the chat pane
  shows the timeout state.

### 4.8 Wide-scope comparison contract

`compare_supported` is a **bounded matrix**, not `compare_peers` and not a screen.

```json
{
  "tool": "compare_supported",
  "request": {"tickers": ["ITMG", "PTBA", "PTPS", "ESSA"],
               "metrics": ["coal_production", "revenue"],
               "period": "Q2", "year": 2026},
  "data": {
    "period": "Q2", "year": 2026,
    "columns": ["ITMG", "PTBA", "PTPS", "ESSA"],
    "rows": [
      {"metric": "coal_production", "unit": "Mt", "cells": [
        {"ticker": "ITMG", "value": 5.3, "period_type": "quarter",
         "source": "operational:itmg_2026-08-10_news_release:1", "status": "ok"},
        {"ticker": "PTBA", "value": null, "status": "not_captured",
         "evidence": {"url": "...", "error": "403"}}
      ]}
    ],
    "excluded": [
      {"ticker": "BBCA", "metric": "gross_margin", "reason": "bank has no COGS"}
    ],
    "coverage": {"requested": 4, "answered": 3, "scope": "supported_set"}
  },
  "ledger": [
    {"value": 5.3, "unit": "Mt", "kind": "count",
     "source": "operational:itmg_2026-08-10_news_release:1",
     "meta": {"ticker": "ITMG", "metric": "coal_production", "period": "Q2", "year": 2026}}
  ]
}
```

Rules:

- Max **5 tickers × 4 metrics** per call; output capped at ~12 KB. The desk may call it
  twice for a wider set, within the 8-call budget.
- Every populated cell carries a source and a ledger entry; empty cells carry a state and
  evidence, never a zero.
- Derived metrics (growth, margin) are computed from the same period plus the prior
  period, with operands in the ledger. If the prior is missing, the cell is a state, not
  an inferred number.
- Unsupported tickers, unsupported metrics and not-applicable combinations go in
  `excluded` with a reason; they do not silently disappear.
- No score, rank, percentile, recommendation or "best" field. The UI renders a matrix and
  a coverage banner, not a league table.
- The answer must cite every compared ticker (via the matrix call) and state the coverage
  caveat. Coverage is what was actually called: if the desk compared 5 names, it says 5,
  not "the sector". A comparison that names an unsupported company is refused, not
  softened.

### 4.9 Two-surface UI contract

- **Routes:** `#/documents` (library), `#/doc/{id}` (workspace), `#/desk` (wide scope).
  The route is the source of truth; reload, back/forward and copied links restore the same
  surface. No server-side router is required.
- **Document workspace:** reader is the primary column; document chat is the secondary
  column. On desktop they are side by side with independent scroll; below ~960 px they
  become tabs (`Document` / `Ask this document`), not stacked scroll traps.
- **Citation click:** a document citation scrolls the reader to the section and applies a
  temporary highlight; a primary citation opens the evidence inspector with row/page. The
  chat pane keeps its scroll position.
- **Selection → question:** selecting text in the reader offers "Ask about this"; the
  quoted passage is attached to the next document-chat question and echoed in the turn.
- **Verify against filings:** a note claim offers a one-click desk question scoped to the
  document's ticker/period. The answer shows the note statement and the filing value side
  by side; it does not overwrite or hide the note claim.
- **Escalation:** "Ask across companies" opens the desk with the current question and a
  scope chip naming the source document. The desk does not inherit the documented scope
  silently.
- **Desk answer → document:** a note citation opens the document workspace at the cited
  section, carrying the question as a route hint
  (`#/doc/{id}?from=desk&q=...`); the document chat starts a fresh thread unless the user
  explicitly continues.
- **Advice is attributed.** If a note contains a rating, the document chat renders it as
  a quoted NOTE view, not as a desk recommendation; the desk never adopts it. An
  unattributed BUY/HOLD/SELL sentence is refused by the audit.
- **Coverage banner:** the desk always shows "Supported set: N issuers" and, for any
  comparison/sector answer, the envelope's `coverage` block. It never implies IDX-wide
  coverage.
- **No blank states:** missing document, missing section, unsupported note, stale note,
  refusal, timeout and tool failure each have a designed state with a next action.

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

### WS2 — Grounding substrate (2d)

| # | Task | Files | Exit test |
|---|---|---|---|
| 2.1 | Schema additions: `provenance` on `Statement`; `operational`, `documents`, `completeness` on `FactSheet`; all default-empty | `schema.py` | All 37 existing sheets still validate |
| 2.2 | Provenance at seed: `_fetch_sheet_rows_with_code` also returns `row_number`; `_build_statement` emits `{sheet,row,label,column,period_type}`; pass `is_bank` map through | `xlsx_seed.py` | ITMG revenue → `1311000:6`; equity → parent vs total distinguished |
| 2.3 | `enrich.py`: resolve provenance for existing sheets via `IS_MAP`/`BANK_IS_MAP`/`BS_MAP`/`CF_MAP` + sheet precedence; merge operational JSON; build `documents`/`completeness`; idempotent | `chat-app/chat_app/enrich.py`, `facts.py` | 37 sheets enriched; second run is a no-op; ITMG Q2 shows 5/5 reconciled |
| 2.4 | Canonical units: `periods.py` returns `{raw, currency, scale, value_idr_bn, period_type}`; `period_derive` computes `tw2 − tw1` with operands | `periods.py` | ITMG Q2 revenue `1,000,491 − 497,577 = 502,914` with formula |
| 2.5 | `analysis_docs.py`: scan `analysis/**/*.md`; infer `kind`/ticker/period/year from path + frontmatter + H1; build outline, section line map, doc ID, sha256; cache by path+mtime; classify unscoped notes | `analysis_docs.py` | 63 top-level notes + 53 unscoped notes indexed; ITMG Q2 opens with a section line map; re-scan is a no-op; a changed note re-indexes |

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

### WS4 — Typed tools (2.5d)

| Tool | Signature | Returns |
|---|---|---|
| `list_companies` | `()` | supported tickers/periods + layer completeness |
| `list_documents` | `(ticker?, period?, year?, kind?, limit=50)` | analysis-note metadata + coverage; no content |
| `analysis_document` | `(document_id, section?)` | one section or an outline, line map, document ledger; ≤64 KB runner prefetch, ≤24 KB per tool call |
| `analysis_search` | `(query, document_id?, ticker?, limit=8)` | ranked snippets with `{document, section, line, quote}` + document ledger |
| `analysis_context` | `(ticker, period, year, topic?)` | existing cheap prefetch of focused note snippets; keep for desk-mode operational context |
| `compare_supported` | `(tickers, metrics, period, year)` | cited matrix, per-cell status, `excluded[]`, `coverage`; max 5 tickers × 4 metrics |
| `company_facts` | `(ticker, period, sections="core")` | compact sections + provenance; `details` only on request; hard output cap |
| `metric_series` | `(ticker, metric, periods?)` | values with `period_type`, scale, source, reconciled flag |
| `period_derive` | `(ticker, period, metric)` | quarter value + formula + operands |
| `documents_status` | `(ticker, period, item?)` | state enum + evidence (URL/status/page) |
| `operational_lookup` | `(ticker, period, metric?)` | entries + drivers with pages |
| `read_document` | `(ticker, doc_id, page)` | exact page text (≤4k chars) + URL |
| `notes_search` / `notes_fetch` | existing | existing |

Rules:

- No SQL; no fuzzy labels; sheet pinned per statement; cumulative vs quarter always
  labelled; unsupported requests return `unsupported` + the supported list. Output capped
  at ~12 KB per primary tool call and ~24 KB per document tool call; the runner's
  full-note prefetch is capped at 64 KB.
- **Mode-specific allowlists.** Document mode gets `analysis_document`, `analysis_search`
  and the primary tools; desk mode gets `list_documents`, `compare_supported`,
  `analysis_search` and the primary tools. `analysis_document` is available in desk mode
  for "open that note" questions but is not prefetched.
- `analysis_document` returns line numbers and a document ledger; it never rewrites the
  note. A section larger than the cap is returned in line-bounded chunks with a
  `next_line` cursor; the model must fetch the rest before claiming it read the whole
  section. `analysis_search` scoped to a document is the only way to prove note silence
  (`not_in_document`). `analysis_context` stays as the cheap desk-mode snippet prefetch;
  it is not a substitute for reading the note when the user asks about the note itself.
  Its snippets are discovery aids: re-anchor a note claim with `analysis_search` or
  `analysis_document` before citing it.
- `compare_supported` is a matrix, not a ranker. It has no score, rank or recommendation
  field and refuses unsupported tickers/metrics by listing them in `excluded`.
- Unit + integration tests for each tool, including mode allowlists and output caps.

### WS5 — Agent + verification (2.5d)

| # | Task | Exit test |
|---|---|---|
| 5.1 | `config.yaml → chat.pi` with `modes.document` and `modes.desk`: model, timeout 180s, prompt path, tool allowlist, prefetch policy, untrusted-note delimiters; document mode prefetches the note (recorded as a synthetic `analysis_document` call); desk mode prefetches `list_companies`; no session/skills | `pi` can call every tool once in each mode; traces show the mode; an embedded "ignore instructions" note is treated as data |
| 5.2 | Envelope schema + repair/retry; `analysis_note` type; document citation fields `{document, section, line, quote}`; `coverage` block for desk; fence stripping; YAML failure raises (no silent `{}`) | Malformed envelope → one retry → refusal; doc answer validates |
| 5.3 | `verify.py`: primary/document ledger split, citation audit (tool/doc/section/line/quote), structural whitelist, causal-label check, `not_in_document` check, no-ranking check, one retry, refusal | Tampered note number cannot pass as `fact`; ITMG doc answer passes; a ranked comparison is refused |
| 5.4 | Trace writer + `--replay` for a stored trace, including `mode`, `document_id`, selection echo and `coverage` | Replay reproduces verdicts offline for one doc and one desk trace |
| 5.5 | Prompt/agent tuning on 10 seed questions (6 existing + 2 document + 2 wide-scope), including selection handling and "note is silent" | ≥9/10 pass without manual edits |

### WS6 — Web app: document workspace + desk (2.5d)

One app, two workspaces across three routes, one visual language. `#/documents` is the
library, `#/doc/{id}` is the reader + document chat, `#/desk` is the wide-scope chat.
Hash routing keeps FastAPI a
single static app; back/forward and deep links work without a server router. Vanilla JS,
no build step; the existing escape-first renderer is extended with pipe tables and safe
links.

| # | Task | Exit test |
|---|---|---|
| 6.1 | FastAPI document API: `GET /api/documents`, `GET /api/documents/{id}`; extend `POST /api/ask` with `mode`, `document_id`, `selection`; response echoes `mode`/`document`; 404/422 for missing/unreadable notes; keep `GET /`, `/api/companies`, `/api/health`, `/api/trace/{id}`; semaphore(2); 127.0.0.1 only | `curl` lists all 116 notes and returns the ITMG note + outline; a document ask returns `mode:"document"` |
| 6.2 | Document library: filter by ticker/period/kind; show note title, `updated_at`, section count, ticker/period chips, coverage, and a `kind` label for sector/article notes; explicit empty state for a supported period with no note | All existing notes appear; ITMG Q2 opens; a missing note shows the empty state, not a blank page |
| 6.3 | Reader: the vault's markdown (headings, lists, pipe tables, blockquotes, fenced code, safe links, safe images), sticky TOC, scroll-spy, section anchors, reading progress, print/export; escape-first, no raw HTML, `http(s)` link/image allowlist, lazy images with `referrerpolicy="no-referrer"`; no truncation | ITMG Q2 renders every table and section; TOC jumps; external images/links cannot execute; a crafted `<script>` note cannot execute |
| 6.4 | Document chat dock: pinned document header, scope chip, history per document, new-thread control, selection capture ("Ask about this"), suggested questions from the outline, NOTE/FACT/MGMT/HYP badges, missing/`not_in_document` banner, "verify against filings" action, audit footer, "Ask across companies" escalation; citation click scrolls/highlights the reader | Ask 3 questions about ITMG Q2; every note claim carries `analysis_note` + section; citation click scrolls to the right heading; selection quote is echoed; verify action shows note vs filing |
| 6.5 | Desk: full-width wide-scope chat; optional ticker/period focus; starter prompts for comparison and sector narrative; multi-ticker citation chips; comparison matrix rendering from `compare_supported`; coverage/exclusion banner; "open cited note" links into the workspace; no ranking UI | Compare 4 coal names; every name is cited; missing cells show states; coverage banner renders; a note link opens the right section |
| 6.6 | Shared states and accessibility: unsupported, refusal, timeout, tool failure, empty document, stale document, offline, no coverage; keyboard submit, focus management, screen-reader announcements; mobile tabs (`Document` / `Ask this document`) instead of a split | All forced states render; 390 px has no horizontal scroll; keyboard-only flow completes |
| 6.7 | Design pass (frontend-design skill), contrast, typography, print styles; update `CONTRACTS.md` and `RUNBOOK.md` in the same commit as the API changes | Light/dark readable; reader printable; contracts match the shipped signatures |

### WS7 — Eval (1d, overlapping WS5–6)

| # | Task | Exit test |
|---|---|---|
| 7.1 | `chat-app/eval/questions.yaml`: 8 probe + 3 user + 4 operational + 3 traps + 3 document + 3 wide-scope = 24; each carries `mode`, optional `document_id`, `must_include`, `must_not_claim`, `expect_state`, `expect_label`, `expect_citations` | Schema validates; both modes represented |
| 7.2 | `eval.py`: run by mode; document assertions (section citation, `analysis_note`, `not_in_document`); desk assertions (multi-ticker citation coverage, coverage block, no ranking); deterministic pass table + JSON artifact; cost/latency split by mode | One command produces a per-mode report |
| 7.3 | Acceptance run + fixes; log every number-audit escape and every note-number-promoted-to-fact escape for manual review | Thresholds in §7 met |

### WS8 — Ops + docs (0.5d, slack)

| # | Task |
|---|---|
| 8.1 | `cron` entry: weekly IR scan for 4 tickers → ingest → extract → enrich; log to `idx_data/chat/cron.log` |
| 8.2 | `chat-app/RUNBOOK.md`: start UI, open a note, ask about a selection, switch to desk, add a document, re-run enrichment, replay a trace, rollback a bad extract |
| 8.3 | README pointer + architecture note; cost report split by mode (`document` vs `desk`) and by trace |

---

## 6. Build schedule (11 working days)

| Day | Work | Exit |
|---|---|---|
| 0 (0.5d) | WS0 baseline + deps + supported-set snapshot | Clean tree, branch, deps import |
| 1 | WS1.1–1.2 (YAML fail, empty guards) + tests | Corrupt/empty inputs fail stages |
| 2 | WS1.3–1.6 (index stage, allowlist, read-only RAG, cost) | P0 gate green; PTBA indexed |
| 3 | WS2.1–2.2 (schema, provenance) + WS2.4 (canonical/derive) | ITMG rows trace to `1311000:6` |
| 4 | WS2.3 (`enrich.py` over 37 sheets) + WS2.5 (`analysis_docs.py`) | All sheets enriched, idempotent; 116 notes indexed |
| 5 | WS4 tools + CLI + unit tests; extension wrappers | Every tool returns JSON; both mode allowlists callable |
| 6 | WS3.1–3.3 (ingest, scan, bilingual) for PTBA/PTPS/ESSA | Fetch report printed; 200-rate known |
| 7 | WS3.4–3.7 (grammar, drivers, reconcile, run 4 names) | ITMG 5/5 + at least 2 new docs |
| 8 | WS5.1–5.3 (agent modes, envelope, dual audit) | 9/10 seed questions verified |
| 9 | WS6.1–6.4 (document API, library, reader, document chat) | Read the full ITMG Q2 note and ask 3 questions in the browser |
| 10 | WS6.5–6.7 (desk, shared shell, responsive/design pass) | Compare 4 names; note links open at the right section; mobile tabs work |
| 11 | WS7 eval + fixes + WS8 cron/runbook + commit | §7 thresholds met |

**Parallelizable:** the ingest block (6–7) can interleave with tool or UI work when a
fetch is slow; eval questions can be drafted while the agent is tuned (8). The document
index (2.5) and doc tools (WS4) must land before the workspace (9).

**Critical path:** P0 → provenance/enrich/document index → typed tools → agent modes →
document workspace → eval. The desk is a second consumer of the same tools, not a second
backend.

---

## 7. Acceptance criteria

| # | Criterion | Threshold |
|---|---|---|
| A1 | Displayed numbers that fail the audit | **0** (gate refuses instead) |
| A2 | False "does not exist / not available" claims | **0** |
| A3 | Eval questions correct | **≥21/24**, including all 3 user and all 3 document questions |
| A4 | Causal claims labelled | **100%** |
| A5 | Answers with ≥1 valid citation | **100%** |
| A6 | Median cost per answer | **≤ $0.010** desk (wide comparison measured ~$0.0075); **≤ $0.020** document (full-note prefetch measured ~$0.017) |
| A7 | p95 latency (local, 2 concurrent) | **≤ 60s** desk; **≤ 120s** document (full-note prefetch) |
| A8 | Operational reconciliation on ITMG | **5/5 within 0.5%** |
| A9 | Repeat enrichment | idempotent (no diff) |
| A10 | Unsupported ticker/period | explicit `unsupported` + alternatives |
| A11 | Document reader completeness | **100%** of notes in `analysis/**/*.md` open; no truncation; pipe tables render |
| A12 | Document-chat provenance | **100%** of note-derived claims labelled `analysis_note` with document+section+line; **0** note numbers presented as `fact` |
| A13 | Document-silence honesty | **0** false "the note says" claims; note silence returns `not_in_document`, never `not_disclosed` or "does not exist" |
| A14 | Wide-scope citation coverage | **100%** of desk answers cite every compared ticker/metric; **0** unsupported names; coverage block present |
| A15 | No advice/ranking | **0** desk-issued BUY/HOLD/SELL, price targets, or ranked "best/top" comparisons; quoted note ratings only when attributed and labelled `analysis_note` |
| A16 | Surface navigation | doc→desk and desk→doc deep links preserve document/section; back/forward works |
| A17 | Responsive/accessibility | 390 px no horizontal scroll; keyboard-only flow completes; light/dark contrast |

Seed questions (A3 subset):

**Existing question set (10):**

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

**Document-scoped (A12–A13):**

11. ITMG Q2 note: "What does the note say caused the cash-flow collapse, and is that
    management-stated or the analyst's inference?" (NOTE label + section citation).
12. PTBA Q2 note: "Summarise the note's cost argument and cite the section." (NOTE label;
    no filing number promoted to fact).
13. PTPS Q2 note: "Which claims in the note are hypotheses rather than facts?" (label
    separation; `not_in_document` if the note is silent).

**Wide-scope desk (A14–A15):**

14. "Compare Q2 2026 coal production across ITMG, PTBA, PTPS and ESSA; name every missing
    cell and its state."
15. "Compare revenue growth and gross margin for MAPA and TAPG; state the supported-set
    coverage and any not-applicable metric."
16. "What do the analysis notes say about the coal sector in 2H 2026? Keep the answer to
    supported names and cite the notes."

---

## 8. Cut lines (in order)

If the schedule slips, cut in this order — never the P0 gate, provenance, the dual-surface
UI, or the audit gate:

1. Cron/IR scan (WS8.1) → manual ingest.
2. SSE/streaming (never in v0 scope) → spinner + elapsed time.
3. Selection escalation and suggested questions → basic document chat only.
4. Comparison matrix rendering → prose comparison with the same citations and coverage.
5. `read_document` + `notes_fetch` tools → `company_facts` + `analysis_search` +
   `notes_search`.
6. Multi-turn history → single-turn per surface.
7. PTPS/ESSA operational ingest → ITMG + PTBA only.
8. UI polish → functional but unstyled reader and chat.

**Do not cut:** WS1, provenance, `documents_status`, `verify.py`, the document reader +
document chat, the desk chat, mode-aware audit, eval A1–A3 and A11–A15.

---

## 9. Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| pi tool wrapper drift (TS vs Python) | medium | Python CLI is the only implementation; one integration test calls each tool **through pi** |
| Envelope non-compliance / YAML failures | medium | Repair once, retry once, then refuse; never `or {}` |
| Number-audit false positives (years, page refs, ratios) | medium | Structural whitelist + ledger tolerance + escape log reviewed on Day 11 |
| Operational extraction brittle on new layouts | high | Reconcile-or-reject; page-verbatim checks; publish `partial`, never guess |
| IR fetch success <70% | medium | Measure on Day 6; per README, fix the fetcher before building more extraction |
| Fact-sheet enrichment corrupts values | low | Only additive keys; hash + idempotency test; goldens on ITMG/PTBA/BBCA |
| Cost creep from large tool outputs | medium | 12 KB primary / 24 KB document tool caps, 64 KB prefetch, max 8 tool calls/answer, cost in every trace |
| DuckDB lock under concurrency | medium | Chat is read-only; RAG search read-only; ingest/index are separate processes |
| LLM invents a driver | medium | Drivers are verbatim quotes verified as substrings with page refs |
| Analysis notes are secondary/LLM-written; users treat them as filings | high | `analysis_note` label, separate document ledger, "secondary" badge and a "verify against filings" action; note numbers cannot become `fact` |
| Note is stale relative to the latest filing | medium | Show `updated_at` + hash; `completeness.analysis` can be `stale`; the desk states the vintage before comparing |
| Document context too large / cost creep | medium | 64 KB prefetch cap; outline + section fetches above it; 24 KB document-tool cap; cost split by mode in every trace |
| Reader XSS or unsafe links from a crafted note | low | Escape-first renderer, no raw HTML, `http(s)`/relative link allowlist, `rel="noopener noreferrer"` |
| Prompt injection inside a note or captured document | medium | Treat note text as delimited quoted data, instruct the model to ignore embedded instructions, and keep the verifier independent; note text cannot authorize a tool call |
| Two-surface navigation confuses or loses context | medium | Persistent top bar, hash deep links, scope chip, "Ask across companies" preserves the question, desk→doc opens the cited section |
| Comparison drifts into a screen or ranking | high | `compare_supported` is a bounded matrix with no score/rank; the verifier bans ranking language; sector screens stay v1 |
| Document index drift or slug collisions | low | Deterministic IDs with a short hash suffix; rescan by path+mtime+sha256; missing note is an explicit state |

---

## 10. Deferred to v1 (tracked, not lost)

- `compare_peers()` with period pinning, scoring and `excluded[{ticker, reason}]` (P2 #15).
  v0's `compare_supported` returns a cited matrix, not a rank or a screen.
- Sector lenses (plantation/extractive/banking) and the 5 palm-oil screens (P3). v0 desk
  sector answers are narrative over the supported set with a coverage caveat.
- IDX-wide watcher + T+45 scheduler (P1 #8) — v0 is CLI + 4-ticker cron.
- Bank shape completion beyond `BANK_IS_MAP` (P3 #18) — BBCA answers work, ratios do not.
- `checks[]`/`caveats[]` mechanization (P2 #17); v0 has a static caveat list in the tool.
- Top-100 XLSX backfill (P4 #21); the supported set stays honest at 24 tickers.
- Metered research/credits, pricing, auth, OJK review (P5/P6).
- Persistent per-document chat threads, annotations, comments, collaborative editing.
- Semantic/embedding search over the analysis vault; v0 uses keyword + notes DuckDB.
- PDF export of the reader; v0 supports browser print.
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
uv run pytest chat-app/tests -q

# 3. Operational
PYTHONPATH=chat-app uv run python -m chat_app.extract_operational --ticker ITMG --period Q2 --year 2026  # 5/5 reconciled

# 4. Document workspace
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765
curl -s localhost:8765/api/documents | jq '.count'
curl -s localhost:8765/api/documents/itmg-q2-2026-analysis | jq '.title, (.sections|length)'
curl -s -X POST localhost:8765/api/ask -H 'content-type: application/json' \
  -d '{"mode":"document","document_id":"itmg-q2-2026-analysis","question":"What does the note say about the cash-flow swing?"}' \
  | jq '.status, .mode'
# In the browser: open the ITMG Q2 note, read the full document, ask 3 questions,
# confirm NOTE vs FACT labels, click a citation and confirm the reader scrolls.

# 5. Desk
curl -s -X POST localhost:8765/api/ask -H 'content-type: application/json' \
  -d '{"mode":"desk","question":"Compare Q2 2026 coal production across ITMG, PTBA, PTPS and ESSA."}' \
  | jq '.status, .mode, ((.envelope.claims // [])|length)'
# In the browser: confirm the coverage banner, per-ticker citations, missing-cell states,
# and that "open cited note" lands at the right section.

# 6. Eval
PYTHONPATH=chat-app uv run python -m chat_app.eval --questions chat-app/eval/questions.yaml
# A1=0 A2=0 A3>=21/24 A4=100% A5=100% A6 desk<=0.010 / doc<=0.020 A7 desk<=60s / doc<=120s A11-A17 pass
```

Ship when the eval report is attached to the commit, the document workspace and desk
click-throughs are recorded, and every failing question has a written reason (not a
retry).
