# chat-app

The IDX research desk: one audited backend, two product surfaces.

- **Document workspace** — read a full analysis note and ask about it, including
  a selected passage.
- **Desk** — wide-scope questions over the 24-name supported set, with
  multi-ticker citations and an explicit coverage block.

The same agent, tools, envelope and verifier serve both. The surface changes the
scope and the allowed tool set, never the audit.

**Status:** v0 implemented; two-surface acceptance is tracked by the 25-question
eval. The build order, contracts and acceptance criteria live in
[`../chatbot-plan/MVP-PLAN.md`](../chatbot-plan/MVP-PLAN.md); module interfaces
are in [`CONTRACTS.md`](./CONTRACTS.md); operations are in
[`RUNBOOK.md`](./RUNBOOK.md).

## Why a top-level folder

The chat is a product surface, not a pipeline stage. It consumes the artifacts the
pipelines produce and must not grow into the report/analysis code. Keeping it at
`chat-app/` makes that boundary visible in the tree and in code review.

## Boundary

| Owns | Does not own |
|---|---|
| FastAPI server + static UI (library, reader, document chat, desk) | Report download/convert pipeline |
| Typed tool layer (`company_facts`, `analysis_document`, `compare_supported`, ...) | Analysis extraction/merge/writer stages |
| Answer envelope + number-audit gate, primary/document ledger split | Notes RAG indexer (it only queries it) |
| Read-only analysis-vault index (`analysis_docs.py`) | Analysis-note authoring |
| Document ingest + operational extraction | Pipeline job state |
| Chat traces + 25-question eval harness | Fact-sheet generation (it enriches, additively) |

**Reads:** `idx_data/analysis/facts/`, `idx_data/operational/`,
`idx_data/notes_rag.duckdb` (read-only), `idx_data/pipeline.duckdb` (read-only),
`analysis/**/*.md` (read-only), and its own prompts.

**Writes:** `idx_data/chat/traces/`, `idx_data/chat/eval/`,
`idx_data/operational/` (ingest), and the additive keys on fact sheets via
`enrich`. Never writes the analysis vault or pipeline job tables.

## Naming

The folder is `chat-app` (as requested); the importable package inside it is
`chat_app`, because Python identifiers cannot contain a hyphen.

## Surfaces and routes

Hash routing keeps FastAPI a single static app; reload, back/forward and copied
links restore the same surface. No server-side router.

| Route | Surface | Primary API |
|---|---|---|
| `#/documents` | Note library; filter by ticker/period/kind, `updated_at`, section count | `GET /api/documents` |
| `#/doc/{id}` | Reader (primary) + document chat (secondary); mobile tabs below ~960 px | `GET /api/documents/{id}`, `POST /api/ask` (`mode: "document"`) |
| `#/desk` | Wide-scope chat, coverage banner, comparison matrix, cited-note links | `POST /api/ask` (`mode: "desk"`) |

Shared states: missing document/section, stale note, unsupported note,
`not_in_document`, refusal, timeout, tool failure and empty coverage each have a
designed state and a next action.

### Ask API

`POST /api/ask` accepts:

```json
{
  "mode": "document",
  "question": "What does the note say about the cash-flow swing?",
  "document_id": "itmg-q2-2026-analysis",
  "selection": {"section": "1. Executive Summary", "line": 16, "quote": "We treat this as a timing artifact rather than operating deterioration."},
  "ticker": null,
  "period": null,
  "year": null,
  "history": []
}
```

- `mode` is `document` or `desk`; omitting it preserves the old desk behaviour.
- Document mode requires `document_id`; `selection` is optional and must be an
  exact substring of the note. Ticker/period/year are derived from the note, not
  used to widen the query. The note is loaded server-side from the vault.
- Desk mode must not carry `document_id` or `selection`. Ticker/period/year are
  an optional focus, not a hard filter: a comparison may name other supported
  tickers.
- The response adds `mode`, `document`, `selection_echo`, `selection_dropped`
  and `coverage` alongside the existing `trace_id/status/envelope/verification/
  usage/duration_s/model/tool_calls`.

Other endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /` | Static app |
| `GET /api/health` | Liveness + version |
| `GET /api/companies` | Supported set (24 tickers / 37 periods) |
| `GET /api/documents?ticker=&period=&kind=&q=` | Vault metadata only (`{ok, documents, count, as_of}`) |
| `GET /api/documents/{id}` | Metadata + full content + outline; 404/422 for missing/unreadable notes |
| `POST /api/ask` | One audited answer, 180 s budget, 2-run semaphore |
| `GET /api/trace/{trace_id}` | Stored trace (mode, document, selection echo, coverage, ledgers) |

## Layout

```text
chat-app/
├── README.md
├── RUNBOOK.md                  # start UI, open note, selection, desk, ingest, enrich, replay, rollback
├── eval/
│   ├── questions.yaml          # 25 graded questions: 8 probe + 3 user + 4 operational
│   │                           #   + 4 traps + 3 document + 3 wide-scope
│   └── supported-set.json      # frozen coverage snapshot
├── chat_app/
│   ├── __init__.py
│   ├── paths.py                # artifact resolution
│   ├── facts.py                # fact-sheet loading + enrichment
│   ├── periods.py              # cumulative→quarter, units
│   ├── operational.py          # operational entries + drivers
│   ├── analysis_docs.py        # read-only analysis-vault index (ids, sections, lines, ledger)
│   ├── tools.py                # typed tools incl. analysis_document/search, compare_supported
│   ├── cli.py                  # JSON CLI consumed by the pi extension
│   ├── envelope.py             # answer schema, claim types, coverage, YAML repair
│   ├── verify.py               # primary/document ledger, citation/number/label/silence audit
│   ├── agent.py                # mode-aware pi runner + retry/refusal + trace
│   ├── ingest.py               # curl_cffi fetch + manifests + IR scan
│   ├── extract_operational.py  # hybrid extractor + reconcile gate
│   ├── metrics.yaml            # deterministic metric grammar
│   ├── eval.py                 # 25-question, mode-aware eval harness
│   ├── web.py                  # FastAPI app (document API + desk)
│   ├── prompts/                # desk_system.md, document_system.md, shared rules
│   └── static/
│       ├── index.html          # shell
│       ├── shared.js           # hash router, renderer, evidence inspector
│       ├── app.js              # desk surface
│       ├── document.js         # library, reader, document chat
│       └── styles.css
└── tests/
    └── test_*.py
```

P0 pipeline fixes and their tests stay where they are: `scripts/analysis_pipeline/`
and the root `tests/`.

## Entry points

Run from the repo root; `PYTHONPATH` makes the package importable without a second
`pyproject.toml`:

```bash
# Web app
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765

# Document workspace + comparison tools
PYTHONPATH=chat-app uv run python -m chat_app.cli list_documents --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.cli analysis_document --document-id itmg-q2-2026-analysis
PYTHONPATH=chat-app uv run python -m chat_app.cli analysis_search --query "strip ratio" --document-id ptba-q2-2026-analysis
PYTHONPATH=chat-app uv run python -m chat_app.cli compare_supported --tickers ITMG,PTBA,PTPS,ESSA --metrics coal_production,revenue --period Q2 --year 2026

# Facts / status / derivation
PYTHONPATH=chat-app uv run python -m chat_app.cli company_facts --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.cli documents_status --ticker PTBA --period Q2 --year 2026

# One audited ask, either mode
PYTHONPATH=chat-app uv run python -m chat_app.agent \
  "What does the ITMG Q2 2026 note say about the cash-flow swing?" \
  --mode document --document-id itmg-q2-2026-analysis
PYTHONPATH=chat-app uv run python -m chat_app.agent \
  "Compare Q2 2026 coal production across ITMG, PTBA, PTPS and ESSA." --mode desk

# Additive fact-sheet enrichment
PYTHONPATH=chat-app uv run python -m chat_app.enrich --all
```

Pipeline imports (`schema`, `xlsx_seed` label maps) are read-only and explicit; the
chat app must never import `merge`, `write`, `research`, or `redflags`.

## Eval

`chat-app/eval/questions.yaml` is the acceptance contract, not a prompt list. It
contains 25 questions:

| Category | Count | Mode |
|---|---:|---|
| probe | 8 | desk |
| user | 3 | desk |
| operational | 4 | desk |
| trap | 4 | desk |
| document | 3 | document |
| wide_scope | 3 | desk |

Each question carries `mode`, optional `document_id`, `must_include`,
`must_not_claim`, `expect_state`, `expect_label`, `expect_citations` and notes.
Document questions pin real notes on disk (`itmg-q2-2026-analysis`,
`ptba-q2-2026-analysis`, `ptps-q2-2026-analysis`); wide-scope questions require
multi-ticker citation coverage and an envelope `coverage` block.

```bash
# Schema + counts only; no model call
PYTHONPATH=chat-app uv run python -m chat_app.eval --dry-run

# Both modes
PYTHONPATH=chat-app uv run python -m chat_app.eval

# One surface (the report still splits cost/latency by mode)
PYTHONPATH=chat-app uv run python -m chat_app.eval --mode document
PYTHONPATH=chat-app uv run python -m chat_app.eval --mode desk
```

The harness writes `idx_data/chat/eval/{utc}.json` and prints a pass table plus
A1–A7 and the surface metrics (A12 note-as-fact, A13 `not_in_document`, A14
wide-scope citations/coverage, A15 no advice/ranking). If the agent backend
cannot be imported it exits 2 with a clear error instead of faking passes.

## Non-negotiables

1. No raw SQL is exposed to the chat model; `duckdb_query` stays out of both
   mode allowlists.
2. Every displayed number traces to a tool result and passes the number audit.
   Document-ledger numbers are separate and only `analysis_note` may spend them.
3. Missing data returns `not_yet_published` / `not_disclosed` / `not_captured` /
   `unsupported` — never "does not exist". In document mode, note silence is
   `not_in_document`, never `not_disclosed`.
4. Every run writes a trace to `idx_data/chat/traces/`, including `mode`,
   `document_id`, selection echo and coverage.
5. `compare_supported` is a bounded cited matrix, not a screen or a ranking.
   The desk never issues BUY/HOLD/SELL, price targets or "best/top pick"
   language; a quoted note view is attributed and labelled `analysis_note`.
6. The reader gets the full note; caps apply only to model-facing tool results
   and the prefetch.
