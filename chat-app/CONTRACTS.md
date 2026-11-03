# chat-app internal contracts

Every module in `chat_app/` codes against this file. It is the interface
boundary between the substrate (facts, periods, operational, tools), the agent
(envelope, verify), and the web UI. Change it before changing a signature.

All paths resolve through `chat_app.paths`. All tool results are JSON-safe
dicts. All successful tool results carry a `ledger` list (see §3) so the number
audit never has to re-parse prose. Analysis-vault tools carry document-ledger
entries (`kind: "document_number"`) in the same list; the verifier separates
them from the primary ledger before auditing.

---

## 1. Tool surface (`chat_app/tools.py`)

```python
list_companies() -> dict
list_documents(ticker: str | None = None, period: str | None = None,
               year: int | None = None, kind: str | None = None,
               limit: int = 50) -> dict
analysis_document(document_id: str, section: str | None = None,
                  next_line: int | None = None) -> dict
analysis_search(query: str, document_id: str | None = None,
                ticker: str | None = None, limit: int = 8) -> dict
compare_supported(tickers: list[str], metrics: list[str], period: str,
                  year: int) -> dict
company_facts(ticker: str, period: str, year: int, sections: str = "core") -> dict
metric_series(ticker: str, metric: str, periods: list[str] | None = None,
              year: int | None = None) -> dict
period_derive(ticker: str, period: str, year: int, metric: str) -> dict
documents_status(ticker: str, period: str | None = None, year: int | None = None,
                 item: str | None = None) -> dict
operational_lookup(ticker: str, period: str, year: int, metric: str | None = None) -> dict
analysis_context(ticker: str, period: str, year: int, topic: str | None = None,
                  limit: int = 4) -> dict
read_document(ticker: str, doc_id: str, page: int) -> dict
notes_search(query: str, ticker: str | None = None, limit: int = 8) -> dict
notes_fetch(chunk_id: str) -> dict
```

Mode allowlists are enforced by the agent:

- `document`: `analysis_document`, `analysis_search`, `company_facts`,
  `metric_series`, `period_derive`, `documents_status`, `operational_lookup`,
  `read_document`, `notes_search`, `notes_fetch`.
- `desk`: `list_companies`, `list_documents`, `compare_supported`,
  `company_facts`, `metric_series`, `period_derive`, `documents_status`,
  `operational_lookup`, `analysis_document`, `analysis_search`,
  `analysis_context`, `notes_search`, `notes_fetch`, `read_document`.

`duckdb_query` is never in either allowlist.

Errors are raised as `paths.ChatError` subclasses (`UnsupportedRequest`,
`ToolInputError`, `ArtifactMissing`). The CLI turns them into
`{"error": "...", "message": "...", ...}` with exit code 2, never a traceback.

### Common envelope

```json
{
  "ok": true,
  "tool": "company_facts",
  "request": {"ticker": "ITMG", "period": "Q2", "year": 2026},
  "data": { "...tool-specific..." },
  "ledger": [ ...see §3... ],
  "caveats": ["Values are cumulative for the period unless period_type=quarter."]
}
```

`list_companies` returns:

```json
{"ok": true, "tool": "list_companies",
 "data": {"companies": [{"ticker": "ITMG", "periods": ["Q1 2026", "Q2 2026"],
                          "operational_periods": ["Q2 2026"],
                          "financials": "complete", "filings": "captured",
                          "notes": "indexed"}],
           "as_of": "2026-10-25"},
 "ledger": []}
```

`operational_periods` lists periods with an extracted **operational release**
(production, sales volume, drivers). It is not filing coverage: a period may have
a captured `raw/{TICKER}_{PERIOD}_{YEAR}.pdf` filing and an empty
`operational_periods`. Use `documents_status` to establish filing existence,
including for non-canonical topics (geography, segment mix, customer
concentration).

`company_facts` returns compact sections only. `data`:

```json
{"ticker": "ITMG", "period": "Q2", "year": 2026,
 "meta": {"currency": "USD", "units": "thousand", "period_end": "2026-06-30"},
 "sections": {
   "income_statement": {
     "revenue": {"value": 1000491.0, "currency": "USD", "units": "thousand",
                  "value_idr_bn": 81540.0, "period_type": "cumulative",
                  "provenance": {"sheet": "1311000", "row": 6, "label_en": "sales and revenue"}}
   },
   "balance_sheet": {"...": "..."},
   "cash_flow": {"...": "..."},
   "details": {"...": "only when sections='details' or 'all'..."}
 },
 "completeness": {"financials": "complete", "filings": "captured",
                   "operational": "captured", "...": "..."}}
```

`metric_series` returns:

```json
{"ticker": "ITMG", "metric": "revenue",
 "series": [{"period": "Q2", "year": 2026, "value": 1000491.0, "unit": "USD thousand",
              "period_type": "cumulative", "value_idr_bn": 81540.0,
              "source": "1311000:6", "reconciled": null}]}
```

`period_derive` returns:

```json
{"ticker": "ITMG", "metric": "revenue", "period": "Q2", "year": 2026,
 "formula": "tw2 - tw1", "operands": {"tw2": 1000491.0, "tw1": 497577.0},
 "quarter_value": 502914.0, "unit": "USD thousand", "value_idr_bn": 40987.0,
 "source": {"tw2": "1311000:6", "tw1": "1311000:6"}}
```

`documents_status` returns the state and evidence. A captured filing is searched
by extracted PDF text; a hit returns a page hint for `read_document`:

```json
{"ticker": "MAPA", "period": "Q1", "year": 2026,
 "item": "geographic revenue",
 "state": "available",
 "evidence": {"document_id": "mapa_q1_2026_filing", "page": 72, "pages": 91,
              "matched_terms": ["geographic", "revenue"],
              "search": "filing_text"},
 "documents": [{"id": "mapa_q1_2026_filing", "type": "filing", "status": "captured",
                 "pages": 91, "url": null}]}
```

When no document of the requested class was captured:

```json
{"ticker": "PTBA", "period": "Q2", "year": 2026,
 "item": "press_release",
 "state": "not_captured",
 "evidence": {"url": "https://...", "http_status": 403, "retrieved_at": "2026-10-25"},
 "documents": [{"id": "ptba_...", "type": "press_release", "status": "not_captured",
                 "url": "...", "error": "403", "pages": null}]}
```

A miss on a captured filing returns the searched terms and how many pages were
scanned:

```json
{"ticker": "MAPA", "period": "Q2", "year": 2026,
 "item": "store count",
 "state": "not_disclosed",
 "evidence": {"document_id": "mapa_q2_2026_filing", "pages_searched": 96,
              "terms": ["store count", "outlet"], "search": "filing_text"},
 "documents": [{"id": "mapa_q2_2026_filing", "type": "filing", "status": "captured",
                 "pages": 96, "url": null}]}
```

`state` ∈ `available | not_yet_published | not_disclosed | not_captured |
unsupported`. State semantics:

- `available` — a captured document of the requested class contains the item.
  The `page` is a hint; call `read_document` for the exact text before citing.
- `not_disclosed` — a captured filing/release was searched and the item was not
  found in it.
- `not_captured` — no document of the requested period/class was captured. This
  means the source document itself is missing, not merely that no operational
  extract exists.
- `not_yet_published` — no release captured and the period is still inside the
  T+45 publication window.
- `unsupported` — the ticker or period is outside the supported set.

`documents_status` is the only tool allowed to answer existence questions.

`operational_lookup` returns:

```json
{"ticker": "ITMG", "period": "Q2", "year": 2026,
 "entries": [{"metric": "coal_production", "scope": "Q2-2026", "value": 5.3,
               "unit": "Mt", "prior": null, "page": 1,
               "source_doc": "itmg_2026-08-10_news_release",
               "published_at": "2026-08-10", "status": "preliminary",
               "verification": {"method": "quoted_in_source", "reconciled_to": null},
               "quote": "..."}],
 "drivers": [{"driver": "...", "affects": "cost_of_revenue", "quote": "...",
              "page": 1, "source_doc": "...", "type": "management_stated"}],
 "documents": [{"id": "...", "pages": 8, "status": "captured"}],
 "analysis_context": [
   {"id": "…", "path": "analysis/ITMG Q2 2026 Analysis.md",
    "title": "...", "section": "...", "snippet": "…≤600 chars…",
    "secondary": true}
 ]}
```

`analysis_context` returns secondary analyst-note snippets. The desk agent
prefetches it for focused questions and records a synthetic `analysis_context`
call in the trace. Document mode does not use it; the pinned note is the
primary context there:

```json
{"ticker": "ITMG", "period": "Q2", "year": 2026, "topic": null,
 "analysis_context": [
   {"id": "…", "path": "analysis/ITMG Q2 2026 Analysis.md",
    "title": "…", "section": "1. Executive Summary",
    "snippet": "…≤420 chars…", "secondary": true}
 ]}
```

`read_document` returns up to 8000 characters per page (enough for a full
geographic-revenue table):

```json
{"ticker": "MAPA", "doc_id": "mapa_q1_2026_filing", "page": 72,
 "text": "…≤8000 chars…", "url": "…", "pages": 91}
```

### Analysis-vault tools

`list_documents` returns metadata only. `data`:

```json
{"documents": [{"id": "itmg-q2-2026-analysis",
                 "path": "analysis/ITMG Q2 2026 Analysis.md",
                 "title": "PT Indo Tambangraya Megah Tbk (ITMG) — Q2 2026",
                 "kind": "company", "ticker": "ITMG", "period": "Q2", "year": 2026,
                 "updated_at": "2026-10-22T21:26:00Z", "sha256": "…",
                 "bytes": 45120, "words": 6200,
                 "tags": ["ITMG", "earnings", "Q22026"],
                 "aliases": ["ITMG Q2 2026"], "section_count": 14}],
 "count": 116, "as_of": "2026-10-26"}
```

`analysis_document` returns one section or, with no section, an outline. `data`:

```json
{"document": {"id": "itmg-q2-2026-analysis", "...": "metadata"},
 "section": {"id": "1-executive-summary", "title": "1. Executive Summary",
              "level": 2, "line": 12, "end_line": 31},
 "content": "…≤24 KB…", "start_line": 12, "end_line": 31,
 "next_line": null, "truncated": false,
 "outline": [{"id": "1-executive-summary", "title": "1. Executive Summary",
               "level": 2, "line": 12, "end_line": 31}],
 "line_map": {"12": "1-executive-summary"},
 "ledger_total": 42, "ledger_complete": true}
```

`analysis_search` returns line-level evidence. A scoped miss is explicit:

```json
{"query": "strip ratio", "document_id": "ptba-q2-2026-analysis",
 "matches": [{"document": "ptba-q2-2026-analysis",
              "path": "analysis/PTBA Q2 2026 Analysis.md",
              "title": "…", "section": "3.1 Gross Margin",
              "section_id": "3-1-gross-margin", "line": 128,
              "quote": "…exact line…", "snippet": "…"}]}
```

`state` is present only for a scoped miss (`not_in_document`); matches are
implicitly `ok`.

```json
{"query": "strip ratio", "document_id": "ptba-q2-2026-analysis",
 "matches": [], "state": "not_in_document",
 "evidence": {"document": "ptba-q2-2026-analysis",
               "sections_searched": 14, "terms": ["strip ratio", "stripping"]}}
```

An empty `matches` list without `state: "not_in_document"` is **not** note-silence
evidence.

`compare_supported` returns a bounded cited matrix, never a rank:

```json
{"period": "Q2", "year": 2026,
 "columns": ["ITMG", "PTBA", "PTPS", "ESSA"],
 "rows": [{"metric": "coal_production", "unit": "Mt", "cells": [
   {"ticker": "ITMG", "value": 5.3, "period_type": "quarter",
    "source": "operational:itmg_2026-08-10_news_release:1", "status": "ok"},
   {"ticker": "PTBA", "value": null, "status": "not_captured",
    "evidence": {"url": "…", "error": "403"}}]}],
 "excluded": [{"ticker": "BBCA", "metric": "gross_margin",
                "reason": "bank has no COGS"}],
 "coverage": {"requested": 4, "answered": 3, "scope": "supported_set"}}
```

Max 5 tickers × 4 metrics. No score, rank, recommendation or "best" field.
Empty cells carry a status + evidence, never zero. Per-cell units are present
on mixed rows.

### CLI (`chat_app/cli.py`)

`python -m chat_app.cli <tool> [--flag value]` prints one JSON object to stdout.
Flags mirror the signatures (kebab-case). Exit 0 on success, 2 on a typed tool
error, 1 on an unexpected exception. `--list` prints the tool schemas.

---

## 2. Agent surface (`chat_app/agent.py`)

```python
def ask(question: str, *, ticker: str | None = None, period: str | None = None,
        year: int | None = None, history: list[dict] | None = None,
        mode: str = "desk", document_id: str | None = None,
        selection: dict | None = None) -> dict
```

`mode` is `desk` or `document`. Document mode requires `document_id`; desk mode
forbids `document_id`/`selection`. `selection` is
`{section?, line?, quote?}` and is validated against the pinned note; an
inexact quote is dropped and echoed as `selection_dropped`.

Returns exactly:

```json
{"trace_id": "20261025T101500Z_ab12cd34",
 "status": "answered | refused | error",
 "mode": "desk | document",
 "document": {"id": "itmg-q2-2026-analysis", "...": "metadata"} | null,
 "selection_echo": {"status": "used | dropped", "section": "…", "line": 18,
                     "quote": "…"} | null,
 "selection_dropped": false,
 "envelope": {"answer": "...", "claims": [...], "missing": [...],
               "coverage": {"...": "..."} | null} | null,
 "coverage": {"scope": "supported_set", "supported_tickers": 24,
               "compared_tickers": ["ITMG"], "caveat": "…"} | null,
 "verification": {"ok": true, "violations": [],
                   "citations": {"ok": 3, "removed": 0, "document_ok": 1},
                   "ledger": [...], "document_ledger": [...]},
 "usage": {"prompt_tokens": 0, "completion_tokens": 0, "cost_usd": 0.0},
 "duration_s": 12.4, "model": "deepseek-v4.1-flash",
 "error": null,
 "tool_calls": ["company_facts", "..."]}
```

Document mode prefetches the pinned note (full note ≤64 KB, else outline plus
question-relevant sections) and records the prefetch as a synthetic
`analysis_document` tool call. Desk mode prefetches `list_companies` and
`analysis_context` for focused questions. Every trace records `mode`,
`document_id`, `selection_echo`, `selection_dropped`, `coverage`, the primary
ledger and the document ledger.

Retry policy: envelope parse failure → one repair/retry; number/citation audit
failure → one retry with the violation list; second failure → `refused` with
the offending numbers. A trace JSON is written for every run.

---

## 3. Ledger contract

Every successful tool result includes:

```json
"ledger": [
  {"value": 1000491.0, "unit": "USD thousand", "kind": "money",
   "source": "1311000:6",
   "variants": [1000491.0, 1000.491, 815.4],
   "meta": {"ticker": "ITMG", "period": "Q2", "year": 2026,
            "metric": "revenue", "period_type": "cumulative"}}
]
```

- `variants` contains the raw value, the value in millions, and the value in
  billions of IDR. The verifier accepts a displayed number within ±0.5% of any
  variant; counts must match exactly.
- `kind` ∈ `money | ratio | count | page | year | percent`.
- Derived values (`period_derive`) use `kind: "money"` with
  `meta.derived: true` and `meta.formula`.

The verifier also whitelists structural numbers: years 2000–2100, `Q1–Q4`,
page numbers and citation ids. It does **not** whitelist numbers that merely
appear in tool text — only `ledger` entries and arithmetic over them.

### Document ledger

`analysis_document` and `analysis_search` emit a **document ledger**, kept
separate from the primary ledger:

```json
{"value": 1000491.0, "unit": "USD thousand", "kind": "document_number",
 "source": "analysis/ITMG Q2 2026 Analysis.md:14",
 "variants": [1000491.0, 1000.491, 1.000491],
 "meta": {"document": "itmg-q2-2026-analysis",
          "section": "1. Executive Summary",
          "section_id": "1-executive-summary",
          "line": 14, "quote": "…exact line/table row…",
          "numeric_kind": "money"}}
```

Rules:

- A document number passes only for an `analysis_note` claim (or a `hypothesis`
  whose underlying facts cite note lines), when the number is in the document
  ledger and the citation quote contains it verbatim.
- A `fact` claim may **never** spend a document-ledger value.
- Arithmetic over two document-ledger values is allowed for `analysis_note` only
  when both operands are cited; otherwise the claim is `hypothesis`.
- Table rows are ledger entries with the full row as `quote`.

---

## 4. Envelope (`chat_app/envelope.py`)

```python
class Citation(BaseModel):
    tool: str
    ticker: str | None = None
    metric: str | None = None
    driver: str | None = None
    doc: str | None = None
    page: int | None = None
    row: int | None = None
    document: str | None = None      # analysis-vault document id
    section: str | None = None       # section id or heading
    line: int | None = None          # 1-based line in the note
    quote: str | None = None         # exact substring of the section

class Claim(BaseModel):
    text: str
    type: Literal["fact", "management_stated", "analysis_note", "hypothesis"]
    citations: list[Citation] = []

class Missing(BaseModel):
    item: str
    state: Literal["not_yet_published", "not_disclosed", "not_captured",
                   "unsupported", "not_in_document"]
    evidence: dict = {}

class Coverage(BaseModel):
    scope: str = "supported_set"
    supported_tickers: int | None = None
    compared_tickers: list[str] = []
    caveat: str | None = None

class AnswerEnvelope(BaseModel):
    answer: str
    claims: list[Claim] = []
    missing: list[Missing] = []
    coverage: Coverage | None = None
```

Labels:

- `fact` — filing/tool value or arithmetic over primary ledger values.
- `management_stated` — company's own words; requires `doc` + `page`.
- `analysis_note` — reproduced/paraphrased from an analysis note, including the
  note's own view/outlook/rating when attributed; requires
  `document` + `section` + `line` + exact `quote`.
- `hypothesis` — model interpretation; must cite underlying facts and must not
  masquerade as fact or management.

`parse_envelope(text: str) -> AnswerEnvelope` strips ``` fences, attempts one
conservative YAML repair (trailing commas, unbalanced flow collections), then
raises `EnvelopeError`. It must never return an empty object on failure.

---

## 5. Verification (`chat_app/verify.py`)

```python
def verify_envelope(envelope: AnswerEnvelope, tool_trace: list[dict]) -> dict
```

Returns:

```json
{"ok": false,
 "violations": [{"type": "number_not_in_ledger", "token": "12.7",
                  "context": "…"}],
 "citations": {"ok": 2, "removed": 1, "document_ok": 1, "unmatched": [...]},
 "ledger": [...], "document_ledger": [...],
 "numbers": {"checked": 4, "passed": 3, "document_passed": 1},
 "missing_state_ok": true, "labels_ok": true, "advice_ok": true}
```

Violation types include: `number_not_in_ledger`, `causal_claim_unlabelled`,
`management_stated_without_document`, `missing_without_documents_status`,
`citation_unmatched`, `document_citation_unmatched`,
`analysis_note_without_quote`, `fact_cites_note_only`,
`document_number_as_fact`, `document_number_not_in_quote`,
`not_in_document_without_search`, `not_in_document_required`,
`advice_or_ranking`, `coverage_ticker_uncited`.

`build_ledger(tool_trace)` returns the primary ledger only;
`build_document_ledger(tool_trace)` returns document entries only. The verifier
never merges them. `repair_envelope` drops unmatched citations and downgrades
empty claims deterministically; the agent re-verifies after repair.

---

## 6. Web API (`chat_app/web.py`)

| Method | Path | Body / params | Returns |
|---|---|---|---|
| GET | `/` | — | `static/index.html` |
| GET | `/api/health` | — | `{"ok": true, "version": "0.1.0"}` |
| GET | `/api/companies` | — | `{"ok": true, "companies": [...], "as_of": "..."}` |
| GET | `/api/documents` | `?ticker=&period=&year=&kind=&q=` | `{"ok": true, "documents": [...], "count": 116, "as_of": "..."}` (metadata only) |
| GET | `/api/documents/{id}` | — | `{"ok": true, "document": {...}, "content": "...", "outline": [...], "line_map": {...}, "frontmatter": {...}}`; 404/422 |
| POST | `/api/ask` | `{mode, question, document_id?, selection?, ticker?, period?, year?, history?}` | agent.ask result |
| GET | `/api/trace/{id}` | — | stored trace JSON |

`mode` is `desk` (default) or `document`. Document mode requires
`document_id`; desk mode forbids `document_id`/`selection`. The response
includes `mode`, `document`, `selection_echo`, `selection_dropped`, `coverage`
and the existing answer/verification fields.

Server binds `127.0.0.1` only and enforces `Semaphore(2)` around `ask`.
Timeout: 180 s, returns `status: "error"` with `error: "timeout"`.
