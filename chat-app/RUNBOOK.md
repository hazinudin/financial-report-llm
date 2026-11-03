# chat-app runbook

Single-operator, loopback-only research desk with two surfaces over one audited
backend:

- **Document workspace** (`#/documents`, `#/doc/{id}`): read an analysis note and
  ask about it, including a selected passage.
- **Desk** (`#/desk`): wide-scope questions over the supported set, with
  multi-ticker citations and an explicit coverage block.

Every command runs from the repo root; `PYTHONPATH=chat-app` makes the package
importable without a second `pyproject.toml`.

## 0. Current status

- Eval set: **25 questions** — 8 probe + 3 user + 4 operational + 4 traps +
  3 document + 3 wide-scope. 22 run on the desk, 3 pin a real note.
- Two-surface acceptance is run with the command in §9. This runbook does not
  quote a live 25-question result until that run is attached to a commit.
- Historical, pre-two-surface baseline: the 20-question set last scored **19/20**
  (`idx_data/chat/eval/20261025T161354Z.json`), A1 0 audit violations, A2 0
  false-absence claims, A3 19/20 with all 3 user questions, A4/A5 100%, A6
  median $0.0019, A7 p95 37.5 s. That artifact predates document mode and the
  wide-scope questions; do not present it as the current acceptance run.
- The one historical failure was `probe_essa_top2_customers`: the answer added
  an absence sentence without a `missing[]` entry, so the missing-state audit
  refused instead of displaying an unverified absence. That is the intended U1
  behaviour, not a data error.
- ITMG Q2 operational reconciliation: 5/5 within 0.5%. PTBA Q2 has a captured
  1H26 press release (production 19.45 Mt, sales 21.10 Mt, strip ratio 5.26x).
  PTPS/ESSA have captured filings but no extracted operational metrics
  (`operational: partial`).

## 1. Start the web UI

```bash
PYTHONPATH=chat-app uv run uvicorn chat_app.web:app \
  --host 127.0.0.1 --port 8765
```

Open <http://127.0.0.1:8765>. The server binds loopback only and allows two
concurrent answers (`Semaphore(2)`); each ask has a 180 s budget. A non-loopback
client is refused with HTTP 403 unless `IDX_CHAT_ALLOW_REMOTE=1` is set
deliberately.

Health and surface checks:

```bash
curl -s http://127.0.0.1:8765/api/health | jq
curl -s http://127.0.0.1:8765/api/companies | jq '.count'
curl -s http://127.0.0.1:8765/api/documents | jq '{ok, count, as_of}'
```

Routes (hash routing; reload, back/forward and copied links restore the same
surface):

| Route | Surface |
|---|---|
| `#/documents` | Library; filter by ticker/period/kind, shows `updated_at`, sections, coverage |
| `#/doc/{id}` | Reader (primary) + document chat (secondary); mobile tabs below ~960 px |
| `#/desk` | Wide-scope chat, coverage banner, multi-ticker comparison |

Ask a question from the CLI without the browser:

```bash
PYTHONPATH=chat-app uv run python -m chat_app.agent \
  "How much coal did ITMG produce in Q2 2026?" \
  --ticker ITMG --period Q2 --year 2026
```

Requires `pi` on `PATH`, the `pi-commandcode-provider` package (pinned in
`.pi/settings.json`), and `COMMAND_CODE_API_KEY` in `.env` (the runner loads it).
Every run writes `idx_data/chat/traces/{utc}_{id}.json`; attempt prompts and raw
NDJSON live under `idx_data/chat/traces/runs/{id}/attempt_N/`.

## 2. Open a note

In the browser, choose **Documents**, filter to `ITMG` / `Q2 2026`, and open
**PT Indo Tambangraya Megah Tbk (ITMG) — Q2 2026**. The reader is the full file,
never truncated; the sticky TOC scrolls to a section and the chat pane keeps its
own scroll position.

The same note over the API:

```bash
# Library metadata only (no content)
curl -s 'http://127.0.0.1:8765/api/documents?ticker=ITMG&period=Q2&kind=company' \
  | jq '.count, .documents[0] | {id, title, ticker, period, year, updated_at, sections}'

# Reader payload: metadata + full content + outline
curl -s http://127.0.0.1:8765/api/documents/itmg-q2-2026-analysis \
  | jq '{id, title, ticker, period, year, sections: (.sections|length), bytes: (.content|length)}'
```

Document ids are stable path slugs: `ITMG Q2 2026 Analysis.md` →
`itmg-q2-2026-analysis`. A missing id is an explicit empty state, not a 500. A
note that changed on disk keeps its id and shows a new `updated_at`.

## 3. Ask about a selection

1. In the reader, select an exact passage.
2. Choose **Ask about this**. The passage is echoed above the composer and
   attached to the next document-chat question.
3. Ask the question. Claims derived from the note render as **NOTE**
   (`analysis_note`) with document + section + line; filing values render as
   **FACT**, management quotes as **MGMT**, and the model's own interpretation
   as **HYP**.
4. Click a NOTE citation: the reader scrolls to the section and applies a
   temporary highlight. Click a primary citation: the evidence inspector opens
   with row/page.

The equivalent API body:

```bash
curl -s -X POST http://127.0.0.1:8765/api/ask \
  -H 'content-type: application/json' \
  -d '{
    "mode": "document",
    "document_id": "itmg-q2-2026-analysis",
    "question": "Is the selected passage the analyst view or management guidance, and what caused the cash-flow swing?",
    "selection": {
      "section": "1. Executive Summary",
      "line": 16,
      "quote": "We treat this as a timing artifact rather than operating deterioration."
    }
  }' \
  | jq '{status, mode, document: .document.id, selection: .selection_echo,
         labels: [.envelope.claims[].type]}'
```

Rules for a selection:

- The quote must be an **exact substring** of the pinned note. A stale or
  altered quote is dropped and echoed as `selection_dropped`; it is never
  trusted as document text.
- The client sends only the document id and selection; note content is loaded
  server-side from the vault, never from client-supplied context.
- Ticker/period/year are derived from the note; supplied values are ignored and
  echoed as document scope, not used to widen the query.
- Note silence is `not_in_document`, with section-search evidence, and offers
  the desk or filings. It is **never** `not_disclosed` or "does not exist".

## 4. Switch to the desk

From a document answer, choose **Ask across companies**. The desk opens at
`#/desk` with the current question and a scope chip naming the source document;
it does not silently inherit the documented scope. You can also navigate
directly to `#/desk`.

```bash
curl -s -X POST http://127.0.0.1:8765/api/ask \
  -H 'content-type: application/json' \
  -d '{"mode":"desk","question":"Compare Q2 2026 coal production across ITMG, PTBA, PTPS and ESSA. Name every missing cell and its state."}' \
  | jq '{status, mode, coverage,
         cited: [.envelope.claims[].citations[]?.ticker] | unique,
         cells: [.envelope.missing[]? | {item, state}]}'
```

What the desk guarantees:

- The coverage banner always says **Supported set: 24 issuers**. A comparison
  shows the envelope `coverage` block (`supported_tickers`, `compared_tickers`,
  `caveat`) and never implies IDX-wide coverage.
- `compare_supported` is a bounded matrix (max 5 tickers × 4 metrics per call),
  not a screen. Every populated cell cites a tool source; every empty cell is a
  state (`not_captured`, `not_disclosed`, `unsupported`), never a zero.
- Every compared ticker must be cited. A comparison that names an unsupported
  company is refused, not softened.
- No desk-issued `BUY`/`HOLD`/`SELL`, price target, "best/top pick" or ranking.
  A quoted note view is allowed only as an attributed `analysis_note` with
  document + section; the desk does not adopt it.
- **Open cited note** takes you back to `#/doc/{id}` at the cited section,
  carrying the question as a route hint (`?from=desk&q=...`). The document chat
  starts a fresh thread unless you explicitly continue.

## 5. Add a document

There are two document classes; keep them distinct.

### 5a. Add an analysis note (document workspace)

1. Write the note as Markdown under `analysis/`, using the existing naming
   convention `TICKER Qn YYYY Analysis.md` (or a slugged title). Frontmatter is
   metadata; the body is rendered as the reader sees it.
2. Reload the library (`#/documents`) or call `GET /api/documents`. The vault is
   scanned read-only and re-indexed by path + mtime + sha256, so a new note
   appears without restarting the server. Its id is the path slug.
3. Verify the note and outline:

   ```bash
   PYTHONPATH=chat-app uv run python -m chat_app.cli list_documents \
     --ticker ITMG --period Q2 --year 2026
   PYTHONPATH=chat-app uv run python -m chat_app.cli analysis_document \
     --document-id itmg-q2-2026-analysis
   ```

4. Ask one document question and confirm every note-derived claim is
   `analysis_note` with a section/line/quote citation. A note-only number must
   never render as `fact`.

No annotations/comments/edits are written back to the vault in v0. Highlighting
is client-side and ephemeral.

### 5b. Add a captured filing (Class B ingest)

```bash
PYTHONPATH=chat-app uv run python -m chat_app.ingest add \
  --ticker ITMG \
  --url 'https://itmg.co.id/files/media/News%20Release....pdf' \
  --type press_release --published 2026-08-10
```

What it writes:

```text
idx_data/operational/ITMG/
  manifest.json                       # one entry per vintage, status/error/sha256/pages
  <doc_id>.pdf
  <doc_id>.txt                        # === PAGE n === markers, original page numbers
```

Re-adding the same URL/hash is a no-op. A 403 is recorded as `not_captured` with
the HTTP status and does **not** overwrite an existing capture. Never edit a
captured document in place: add a new vintage (revision semantics are
`published_at` + `supersedes`).

Discover links from the configured IR/news pages:

```bash
PYTHONPATH=chat-app uv run python -m chat_app.ingest scan --ticker PTBA
PYTHONPATH=chat-app uv run python -m chat_app.ingest scan --all
```

The report prints discovered / captured / 200-rate. If the 200-rate is below
70%, fix the fetcher before building more extraction (MVP-PLAN §9).

## 6. Extract operational entries

```bash
# Deterministic grammar + XBRL reconciliation + one driver call
PYTHONPATH=chat-app uv run python -m chat_app.extract_operational \
  --ticker ITMG --period Q2 --year 2026

# No model call (deterministic causal-sentence fallback for drivers)
PYTHONPATH=chat-app uv run python -m chat_app.extract_operational \
  --ticker ITMG --period Q2 --year 2026 --offline
```

Output: `idx_data/operational/{TICKER}_{Q}_{YYYY}_operational.json`.
The report prints `reconciled N/M`. A metric that overlaps XBRL but misses the
tolerance is marked `rejected` and the document gets an `extraction_warnings`
entry — it is never silently published. Fix the document or the grammar; do not
hand-edit a rejected value.

## 7. Re-run enrichment

```bash
# Additive keys only: provenance, operational, documents, completeness
PYTHONPATH=chat-app uv run python -m chat_app.enrich --all
PYTHONPATH=chat-app uv run python -m chat_app.enrich --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.enrich --all --dry-run
```

Enrichment never changes statement values. It writes `meta.enriched_at` and
`meta.enriched_hash`; a second run with unchanged inputs is byte-identical.
Run it after every ingest or extraction. It does not re-index the analysis
vault; the vault is scanned separately and does not need enrichment.

## 8. Replay a trace, then roll back a bad extract

### 8a. Replay a trace

```bash
# Latest trace
ls -t idx_data/chat/traces/*.json | head -1

# Re-run verification offline (no pi, no network, no model cost)
PYTHONPATH=chat-app uv run python -m chat_app.agent \
  --replay idx_data/chat/traces/<trace_id>.json
```

Replay rebuilds the primary and document ledgers from the stored tool trace and
prints `answered`/`refused` plus violations. Document traces also carry `mode`,
`document_id`, `selection_echo`, `selection_dropped` and `coverage`; replay
reproduces the document-citation and `not_in_document` verdicts offline. Use it
to answer "why was this refused?" without paying for another model call.

### 8b. Roll back a bad extract

1. The previous canonical artifact is in git history or the ingest manifest
   (one entry per vintage). Re-run the previous vintage:

   ```bash
   PYTHONPATH=chat-app uv run python -m chat_app.extract_operational \
     --ticker ITMG --period Q2 --year 2026 --doc-id <previous_doc_id> --force
   ```

2. Re-run enrichment for the sheet (§7).
3. Replay a recent desk trace and a document trace to confirm the answer
   verdicts are unchanged (§8a).
4. If the extractor itself is wrong, revert `metrics.yaml` or
   `extract_operational.py` and re-run; do not patch the JSON.

Never delete `idx_data/operational/{TICKER}/manifest.json` — it is the capture
audit trail.

## 9. Tools and the eval harness

```bash
# Analysis vault + bounded comparison
PYTHONPATH=chat-app uv run python -m chat_app.cli list_documents \
  --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.cli analysis_document \
  --document-id itmg-q2-2026-analysis --section "1. Executive Summary"
PYTHONPATH=chat-app uv run python -m chat_app.cli analysis_search \
  --query "strip ratio" --document-id ptba-q2-2026-analysis
PYTHONPATH=chat-app uv run python -m chat_app.cli compare_supported \
  --tickers ITMG,PTBA,PTPS,ESSA --metrics coal_production,revenue \
  --period Q2 --year 2026

# Existing typed tools
PYTHONPATH=chat-app uv run python -m chat_app.cli company_facts \
  --ticker ITMG --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.cli documents_status \
  --ticker PTBA --period Q2 --year 2026
PYTHONPATH=chat-app uv run python -m chat_app.cli period_derive \
  --ticker ITMG --period Q2 --year 2026 --metric revenue

# Eval: schema check, then the 25-question acceptance run
PYTHONPATH=chat-app uv run python -m chat_app.eval \
  --questions chat-app/eval/questions.yaml --dry-run
PYTHONPATH=chat-app uv run python -m chat_app.eval \
  --questions chat-app/eval/questions.yaml

# One surface only (cost/latency are always split by mode in the report)
PYTHONPATH=chat-app uv run python -m chat_app.eval --mode document
PYTHONPATH=chat-app uv run python -m chat_app.eval --mode desk
```

`--dry-run` prints 25 questions, the category counts (probe 8, user 3,
operational 4, trap 4, document 3, wide_scope 3) and the mode split (desk 22,
document 3) without importing the agent or calling a model. The live run writes
`idx_data/chat/eval/{utc}.json`; use `--id <question_id>` for one question,
`--limit N` for a smoke run, and `--snapshot` to refresh
`chat-app/eval/supported-set.json` from disk.

The report keeps A1–A7 and adds the surface metrics:

| Metric | What it checks |
|---|---|
| A1 | 0 displayed numbers that fail the audit |
| A2 | 0 false "does not exist / no data" claims |
| A3 | ≥21/24 correct, including all 3 user and all 3 document questions |
| A4 | 100% of causal claims carry a label |
| A5 | 100% of citation-required answers have ≥1 citation |
| A6 | median cost ≤ $0.005 desk / ≤ $0.010 document (split by mode) |
| A7 | p95 latency ≤ 60 s |
| A12 | 0 note-derived numbers presented as `fact` |
| A13 | 100% of note-silence cases return `not_in_document` |
| A14 | 100% of wide-scope answers cite every compared ticker and carry coverage |
| A15 | 0 desk-issued advice/ranking statements |

If the agent backend cannot be imported, the harness exits 2 with
`eval aborted: agent backend unavailable: ...` and writes no artifact. It never
turns a missing backend into a table of green rows.

## 10. Tests

```bash
PYTHONPATH=chat-app uv run pytest chat-app/tests/test_eval.py -q   # eval harness
uv run pytest tests/ chat-app/tests -q                             # full suite
uv run pytest tests/test_p0_*.py -q                                # P0 gate only
uv run pytest chat-app/tests -q                                    # chat app only
```

The eval tests monkeypatch `agent.ask`; they never call a model.

## 11. Weekly cron (v0)

`chat-app/cron.sh` scans the four configured tickers, ingests any new PDFs,
extracts, and enriches. It logs to `idx_data/chat/cron.log`. The analysis vault
is scanned by the app, not by cron.

```cron
# Monday 06:15 local time
15 6 * * 1 cd /Users/hannan/Projects/financial-report-llm && ./chat-app/cron.sh >> idx_data/chat/cron.log 2>&1
```

There is no IDX-wide watcher in v0. Manual ingest is always acceptable; cron is
the first cut line if the schedule slips.

## 12. Known limits

### Tool budget and mode allowlists

The pi extension enforces a per-prompt typed-tool budget (default **6** calls;
override with `IDX_CHAT_TOOL_BUDGET=12` for exploratory manual sessions). It
resets on every `agent_start`, so an interactive pi session does not accumulate
a lifetime budget. A tool result of the form `Tool budget exhausted (6 calls)`
is our guard, not a pi/provider limit: the tool was not executed, but the model
can still answer from the evidence already returned. If you need more calls,
raise the env var or edit `CHAT_TOOL_BUDGET` in `.pi/extensions/idx-tools.ts`.
This is separate from the ~12 KB primary / ~24 KB document per-tool caps.

Document mode is scoped to the pinned note plus `analysis_document` /
`analysis_search`; desk mode adds `list_companies`, `list_documents`,
`compare_supported` and `analysis_context`. `duckdb_query` is never allowed.

- Coverage is 24 tickers / 37 periods. Anything else returns `unsupported`
  with the supported list.
- Operational data exists only where a document was captured. ITMG Q2 2026 and
  PTBA Q2 2026 (1H26 press release) have extracted operational entries; PTPS
  and ESSA Q2 2026 have captured filings but no extracted operational metrics,
  so their `operational` completeness is `partial` and `operational_lookup`
  returns `not_disclosed` for tonnage — never "does not exist".
- TAPG has only Q1 2026 in the supported set; its extracted filing does not
  disclose consolidated revenue/COGS/margin, so a Q2 comparison must state the
  period mismatch and `not_disclosed` rather than infer a margin.
- A document-only figure is `analysis_note`; it can never be promoted to
  `fact`. Note silence is `not_in_document`, not `not_disclosed`.
- The four issuer listing pages are JavaScript-only, 404, or unreachable. New
  documents were discovered with Exa and added via `ingest add`; the weekly
  `scan` will not find them until the listing pages or a discovery source is
  fixed. Manual ingest is the expected path in v0.
- Market data, forecasts, peer screens, rankings and BUY/HOLD/SELL language are
  out of scope. A quoted note rating is allowed only as an attributed
  `analysis_note`.
- The web server cannot kill a pi subprocess when the 180 s HTTP budget expires;
  the worker finishes and releases its semaphore permit.
- Conversation history is client-side; a page reload clears the thread, but
  the hash route and document scope survive.
