# chat-app

The IDX research chat application: web UI, agent runner, typed tools, answer
verification, document ingest/extraction, and the eval harness.

**Status:** planned. No implementation yet. The build order, contracts, and
acceptance tests live in [`../chatbot-plan/MVP-PLAN.md`](../chatbot-plan/MVP-PLAN.md).

## Why a top-level folder

The chat is a product surface, not a pipeline stage. It consumes the artifacts the
pipelines produce and must not grow into the report/analysis code. Keeping it at
`chat-app/` makes that boundary visible in the tree and in code review.

## Boundary

| Owns | Does not own |
|---|---|
| FastAPI server + static UI | Report download/convert pipeline |
| Typed tool layer (`company_facts`, `documents_status`, ...) | Analysis extraction/merge/writer stages |
| Answer envelope + number-audit gate | Notes RAG indexer (it only queries it) |
| Document ingest + operational extraction | Pipeline job state |
| Chat traces + eval harness | Fact-sheet generation (it enriches, additively) |

**Reads:** `idx_data/analysis/facts/`, `idx_data/operational/`,
`idx_data/notes_rag.duckdb` (read-only), `idx_data/pipeline.duckdb` (read-only),
`analysis/`, and its own prompts.

**Writes:** `idx_data/chat/traces/`, `idx_data/operational/` (ingest), and the
additive keys on fact sheets via `enrich`. Never writes pipeline job tables.

## Naming

The folder is `chat-app` (as requested); the importable package inside it is
`chat_app`, because Python identifiers cannot contain a hyphen.

## Planned layout

```text
chat-app/
├── README.md
├── RUNBOOK.md                  # start UI, ingest, enrich, replay, rollback
├── eval/
│   ├── questions.yaml          # 20 graded questions
│   └── supported-set.json      # frozen coverage snapshot
├── chat_app/
│   ├── __init__.py
│   ├── paths.py                # artifact resolution
│   ├── facts.py                # fact-sheet loading + enrichment
│   ├── periods.py              # cumulative→quarter, units
│   ├── operational.py          # operational entries + drivers
│   ├── tools.py                # typed tool implementations
│   ├── cli.py                  # JSON CLI consumed by the pi extension
│   ├── envelope.py             # answer schema + YAML repair
│   ├── verify.py               # citation/number/label audit
│   ├── agent.py                # pi runner + retry/refusal
│   ├── ingest.py               # curl_cffi fetch + manifests + IR scan
│   ├── extract_operational.py  # hybrid extractor + reconcile gate
│   ├── metrics.yaml            # deterministic metric grammar
│   ├── eval.py                 # eval harness
│   ├── web.py                  # FastAPI app
│   ├── prompts/chat_system.md
│   └── static/index.html
└── tests/
    └── test_chat_*.py
```

P0 pipeline fixes and their tests stay where they are: `scripts/analysis_pipeline/`
and the root `tests/`.

## Planned entry points

Run from the repo root; `PYTHONPATH` makes the package importable without a second
`pyproject.toml`:

```bash
PYTHONPATH=chat-app uv run python -m chat_app.cli company_facts --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.enrich --all
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765
```

Pipeline imports (`schema`, `xlsx_seed` label maps) are read-only and explicit; the
chat app must never import `merge`, `write`, `research`, or `redflags`.

## Non-negotiables

1. No raw SQL is exposed to the chat model; `duckdb_query` stays out of its allowlist.
2. Every displayed number traces to a tool result and passes the number audit.
3. Missing data returns `not_yet_published` / `not_disclosed` / `not_captured` /
   `unsupported` — never "does not exist".
4. Every run writes a trace to `idx_data/chat/traces/`.
