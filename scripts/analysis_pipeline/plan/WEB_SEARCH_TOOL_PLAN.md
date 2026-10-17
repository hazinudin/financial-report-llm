# Web Search Tool — Implementation Plan

**Status:** Draft for review  
**Author:** OpenCode session  
**Date:** 2026-08-04  
**Superseded by:** [[PI_RESEARCH_AGENT_PLAN]] (2026-10-15) — the Python `web_search` / `fetch_web_page` tool loop (`search_backend.py`, `tool_registry.py`, `search_extractors.py`) was retired in favor of a headless pi agent (`exa_search` + `fetch_content`). Kept as a historical record.  
**Scope:** Add a local Python web-search and page-fetching tool that can be called by the LLM during the external-research stage. The tool is not an MCP server. It runs inside the analysis pipeline, uses SearXNG for discovery, fetches page content when requested, and returns structured evidence to the model.

**Related plan:** `EXTERNAL_RESEARCH_PLAN.md`  
**Primary consumers:** `research.py`, `opencode_client.py`, `redflags.py`

**Module ownership:** this plan owns `search_backend.py`, `search_extractors.py`, and `tool_registry.py`. `EXTERNAL_RESEARCH_PLAN.md` owns `research.py`, the research prompts, the trigger rules, and the brief schema. `research.py` — the primary consumer — is delivered by that plan, so its Phases 1–2 precede this plan's Phase 4 integration.

---

## 1. Purpose

The external-research stage needs more than a search-result snippet, but fetching every page is expensive and unreliable. This plan introduces a controlled tool boundary:

```text
LLM asks for research
        │
        ▼
Python tool dispatcher
        │
        ├── web_search()       → SearXNG discovery + cached metadata
        └── fetch_web_page()   → cached content extraction for selected URLs
        │
        ▼
Structured evidence returned to the LLM
```

The LLM may request searches and page fetches, but Python remains responsible for:

- Backend access
- Query and fetch budgets
- Cache reads and writes
- Domain restrictions
- Timeouts and retries
- Content-size limits
- Provenance and publication-date preservation
- Failure classification

The LLM must never execute arbitrary shell commands or construct unrestricted HTTP requests through a generic tool.

---

## 2. Design Goals

1. Expose web research through typed, narrow tools rather than an unrestricted browser or shell tool.
2. Support search-result snippets and optional full-page content.
3. Reuse the SearXNG adapter and cache defined in `EXTERNAL_RESEARCH_PLAN.md`.
4. Make every returned claim traceable to a URL, title, and publication date when available.
5. Keep the tool usable without LangGraph or MCP.
6. Preserve best-effort behavior: unavailable search must degrade research, not block report writing.
7. Keep the tool deterministic enough for replay through cached results and persisted research briefs.
8. Make page extraction extensible for HTML, JavaScript-rendered pages, and PDFs without requiring all extractors in the first phase.
9. Prefer bounded HTTP retrieval and escalate to a browser only when the response indicates that browser rendering is necessary.
10. Reuse the repository's Docling-based PDF conversion capability instead of introducing a second PDF parsing stack.

## 3. Non-Goals

- Building a general-purpose autonomous browser agent.
- Replacing the vault RAG search.
- Making external research responsible for extracting financial-statement facts.
- Treating generated findings as permanent memory without source evidence.
- Fetching every search result automatically.
- Adding MCP protocol support to the repository.
- Solving paywalls, CAPTCHA challenges, or authenticated websites.

---

## 4. Relationship to the External Research Stage

`research.py` remains the owner of the research workflow:

```text
Facts + ratios
    │
    ▼
Deterministic triggers
    │
    ▼
Classify internal/external events
    │
    ▼
Generate bounded questions
    │
    ▼
LLM uses web_search and fetch_web_page tools
    │
    ▼
Synthesize research brief
    │
    ▼
context.py injects research_brief into writer input
```

The tools provide evidence. They do not decide whether an event is material, whether it is internal or external, or whether the final explanation is acceptable. Those decisions remain in `research.py` and the synthesis prompt.

The existing plan's vault-first order remains unchanged:

1. Search the notes vault for curated internal and sector context.
2. Use `web_search` for evidence not covered by the vault or requiring recency.
3. Use `fetch_web_page` only for findings whose snippets are insufficient.

---

## 5. Tool API

### 5.1 `web_search`

Purpose: discover relevant sources and return lightweight metadata plus snippets.

Proposed Python signature:

```python
def web_search(
    query: str,
    *,
    topic: Literal["news", "finance", "general"] = "news",
    time_range: Literal["day", "week", "month", "year"] | None = None,
    max_results: int = 5,
    ticker: str | None = None,
    period_start: str | None = None,
    period_end: str | None = None,
) -> SearchResponse:
    ...
```

The LLM-facing arguments must be narrower than the internal function if needed. For example, `period_start` and `period_end` may be supplied by `research.py` rather than chosen freely by the model.

Returned data:

```yaml
status: ok                         # ok | no_results | degraded | error
query: "Indonesia nickel RKAB Q1 2026"
cache_hit: false
results:
  - result_id: "sha256:..."
    title: "..."
    url: "https://example.com/article"
    snippet: "..."
    published_date: "2026-02-12"
    source_domain: "example.com"
    source_type: "news"            # news | filing | government | research | unknown
    fetchable: true
    authoritative: false
errors: []
usage:
  query_count: 1
```

`web_search` must not claim that a snippet is the full article. The response schema should explicitly label it as `snippet`.

### 5.2 `fetch_web_page`

Purpose: retrieve and normalize one selected source.

Proposed Python signature:

```python
def fetch_web_page(
    url: str,
    *,
    max_chars: int = 30000,
    source_title: str | None = None,
) -> PageResponse:
    ...
```

The LLM-facing tool must enforce:

- One URL per call
- Allowed URL schemes: `https` and optionally `http`
- Maximum response bytes
- Maximum extracted characters
- Request timeout
- Redirect limit — and every redirect hop is re-validated against the scheme and private-network rules *before* it is followed, so a 302 cannot bounce the fetcher to `localhost` or a link-local/metadata address
- Domain allow-list for fetches, enforced by the dispatcher (see below)
- No localhost, private IP, file, or shell URLs

The dispatcher also enforces the fetch policy resolved in `EXTERNAL_RESEARCH_PLAN.md` §5 (operator decision 2026-07-31): fetches are for low-confidence findings on authoritative domains, not arbitrary LLM-chosen pages. A `fetch_web_page` call for a domain outside the configured allow-list is rejected with `status: blocked` and does **not** consume fetch budget. The LLM decides *whether* to fetch; Python decides *what may* be fetched.

Returned data:

```yaml
status: ok                         # ok | blocked | unsupported | degraded | error
url: "https://example.com/article"
canonical_url: "https://example.com/article"
title: "..."
content: "Readable page text..."
content_type: "text/html"
extractor: "httpx_readability"
published_date: "2026-02-12"
retrieved_at: "2026-08-04T...Z"
content_hash: "sha256:..."
cache_hit: false
truncated: false
fetch_attempted: true
error: null
```

When extraction fails, return a structured failure rather than fabricated or empty evidence:

```yaml
status: degraded
content: ""
fetch_attempted: true
error: "browser and HTTP extraction failed"
```

---

## 6. Search Backend and Extraction Strategy

### 6.1 Discovery

`web_search` uses the SearXNG JSON API already selected in `EXTERNAL_RESEARCH_PLAN.md`:

```text
GET /search?q={query}&format=json&categories=news&time_range={range}
```

SearXNG is responsible for discovery only. Its result should be normalized into the internal `SearchResult` schema. Engine-specific fields must not leak into prompts or the research brief.

Backend availability follows the companion plan's container contract (`EXTERNAL_RESEARCH_PLAN.md` §5): a `/healthz` probe runs before the search step, the engine list is pinned to a stable low-blocking set in `config/searxng/settings.yml`, and container-down yields `status: degraded` rather than a pipeline failure. The adapter reads base URL, timeout, and engine config from `config.yaml`; it never manages the Docker lifecycle.

### 6.2 Page extraction

The page fetcher should use a layered extractor modeled on the web-search MCP implementation, while preserving this plan's explicit budgets, safeguards, and structured failure semantics:

```text
1. `httpx` GET with bounded timeout and a normal browser-like User-Agent
2. Content-Type and response-size checks before parsing
3. HTML readability/main-content extraction
4. Content-quality checks for empty responses, bot-detection pages, access-denied pages, and JavaScript-only shells
5. Optional Playwright fallback for JavaScript-rendered or HTTP-blocked pages
6. PDF extraction through a bounded Docling adapter
```

Recommended initial behavior:

- HTML: `httpx` plus a lightweight readability/HTML-to-text pass.
- PDF: download the bounded response to a temporary or content-addressed cache file, then pass it to the existing Docling-based conversion path (`DocumentConverter` plus `export_to_markdown()`). Return the exported Markdown as normalized page content.
- The web-search layer should reuse the Docling dependency and conversion behavior from `scripts/report_pipeline/pipeline.py`, but should not call `run_pipeline()` or `convert_reports()` directly because those functions also manage IDX report selection, filesystem output, and DuckDB pipeline state.
- Introduce a narrow adapter such as `search_extractors.extract_pdf_with_docling(path, max_chars)` around `DocumentConverter`; keep it independent of report-pipeline directories and database objects.
- Text-based PDFs should be supported in the first implementation. Scanned or image-only PDFs should return `unsupported` or `degraded` unless OCR support is explicitly configured.
- JavaScript-heavy or HTTP-blocked pages: if browser extraction is enabled, retry with an isolated Playwright context; otherwise return a structured extraction failure and preserve the URL/snippet.
- Browser extraction should block images, fonts, media, and other non-essential resources; enforce a bounded navigation timeout; and close the context in a `finally` block.
- Browser extraction is a fallback, not the default path, and must not be attempted for every result automatically.
- The browser layer should remain optional so deployments without installed Playwright browsers still support HTTP extraction.
- Playwright ships as an optional dependency (`[project.optional-dependencies] browser = ["playwright>=…"]`, enabled via `uv sync --extra browser && playwright install chromium`). The Phase 1 extractor interface is defined regardless; when the extra or config flag is absent, the browser extractor returns `status: unsupported, error: "browser extraction disabled"`.
- Navigation timeouts alone do not bound the browser path — a hung renderer must be killable. Run browser fetches in a subprocess (or an abandonable worker thread) so the outer fetch timeout is enforced even if the browser never returns.
- Use one long-lived browser instance with a fresh isolated context per fetch, and cap browser concurrency at 1–2 to contain memory and avoid zombie Chromium processes on crash.
- **Enablement criterion** (resolves "when to turn it on"): enable the Playwright fallback only if ≥ 2 low-confidence authoritative fetches across the DKFT/ESSA/ELSA regression runs return JavaScript-shell pages under the HTTP-first path.

The tool should not depend on the web-search MCP implementation. The MCP tests in this session demonstrate that different pages can succeed or fail depending on their rendering and hosting behavior; the local tool needs the same explicit failure semantics.

### 6.3 Source classification

Classify results using domain and URL heuristics before the LLM sees them:

- `government`: ministry, regulator, central bank, IDX, official statistics
- `filing`: exchange filing or company disclosure
- `news`: recognized news publisher
- `research`: analyst, industry, or institutional research
- `unknown`: everything else

Classification is advisory. It must not by itself establish factual reliability.

---

## 7. LLM Tool-Calling Protocol

The current `opencode_client.chat()` sends only ordinary messages and returns only text. It must be extended or wrapped to support an OpenAI-compatible tool loop.

### 7.1 Request flow

```text
research.py builds messages and tool schemas
    │
    ▼
opencode_client sends messages + tools
    │
    ├── assistant content only → return final answer
    │
    └── assistant tool_calls
          │
          ▼
        validate arguments
          │
          ▼
        execute registered Python tool
          │
          ▼
        append tool result message
          │
          ▼
        call model again
```

### 7.2 Client changes

Add a separate function rather than changing existing callers unexpectedly:

```python
def chat_with_tools(
    model: str,
    messages: list[dict],
    tools: list[dict],
    tool_executor: Callable[[str, dict], dict],
    *,
    max_tool_rounds: int = 6,
    ...,
) -> ChatResult:
    ...
```

Requirements:

- Preserve the existing `chat()` behavior for extraction, writing, context, and redflags.
- Validate tool names against an explicit registry.
- Validate JSON arguments before execution.
- Enforce a maximum tool-call round count, applied per research session.
- Execute tool calls within a round in bounded parallel (default cap 8 per round) — rounds × per-round cap must cover the configured call budgets (§8); each call is still recorded individually for audit.
- Enforce per-tool and per-report budgets.
- Record prompt, completion, and tool usage.
- Return the final assistant content plus the full tool trace needed for the research artifact.
- Handle providers/models that return no tool calls or reject tool schemas.

### 7.3 Capability fallback

Tool calling must be feature-detected or configured per model. If the configured model does not support tool calls:

1. Keep the deterministic query templates.
2. Execute the bounded search plan in Python.
3. Send the collected evidence to the synthesis call.
4. Mark the mode as `deterministic_fallback` in the research metadata.

The pipeline must not fail solely because a model lacks native tool calling.

---

## 8. Budgets and Guardrails

Budgets apply across the full report, not independently inside every LLM tool-call round:

```yaml
research:
  tool_mode: llm_tools             # llm_tools | deterministic_fallback
  max_tool_rounds: 6               # per research session
  max_parallel_calls_per_round: 8  # 6 rounds × 8 = 48 ≥ 36 searches + 8 fetches
  max_search_calls: 36
  max_fetch_calls: 8
  max_results_per_search: 5
  max_page_chars: 30000
  max_total_evidence_chars: 120000
  search_timeout_s: 15
  fetch_timeout_s: 20
  max_redirects: 5                 # scheme + destination re-validated on every hop
  fetch_allow_domains: []          # the single fetch allow-list, enforced by the dispatcher; empty = fetch_web_page always returns status: blocked
  fetch_requires_low_confidence: true   # dispatcher rejects fetches not tied to a low-confidence finding (EXTERNAL_RESEARCH_PLAN §5 hybrid policy)
```

`fetch_allow_domains` is the one key that owns the fetch allow-list (it supersedes the earlier `allowed_fetch_domains` sketch and the companion plan's `fetch.authoritative_domains` sketch — populate it from the same authoritative list: Reuters, Bloomberg, ministry portals, IDX filings).

The tool dispatcher must reject calls after a budget is exhausted and return:

```yaml
status: degraded
error: "fetch budget exhausted"
```

The LLM should then synthesize from available evidence rather than retrying indefinitely.

Security controls:

- Reject non-HTTP(S) URLs.
- Reject localhost and private-network destinations.
- Re-validate scheme and destination on every redirect hop — redirects must not bypass the private-network rules.
- Do not expose arbitrary headers or request methods to the model.
- Do not allow the model to select filesystem paths.
- Limit response size before parsing.
- Avoid logging query secrets or authorization headers.
- Treat fetched page text as untrusted input and delimit it in prompts.

---

## 9. Caching and Artifacts

### 9.1 Query cache

Cache key should include all inputs that affect discovery:

```text
sha256(
  normalized_query +
  topic +
  time_range +
  max_results +
  backend_name +
  backend_config_version
)
```

Persist raw normalized search responses under:

```text
idx_data/analysis/research/cache/search/{query_hash}.json
```

### 9.2 Page cache

Cache key:

```text
sha256(canonical_url + extractor_config_version + max_chars)
```

Persist fetch metadata and extracted content under:

```text
idx_data/analysis/research/cache/fetch/{url_hash}.json
```

### 9.3 Cache staleness policy

Cached entries never expire on their own: replay fidelity takes priority over freshness, and a re-run must see the same evidence the original run saw. Freshness is operator-driven via `--refresh-research-cache`, which bypasses cache reads while preserving old artifacts for audit. An optional age-out keyed on `time_range` (e.g. `day` queries treated as stale after 24 h) may be added later; it is out of scope for the first milestone.

### 9.4 Research trace

The final research artifact should record:

- Tool mode
- Tool calls and cache hits
- Search and fetch counts
- Failed fetches
- Model and prompt versions
- Evidence references
- Final findings

The existing research brief remains canonical. Tool traces support audit and replay but should not be injected wholesale into the writer prompt.

---

## 10. Integration With the Research Brief

The synthesis prompt must require the LLM to use tool results only as evidence and to return the existing brief schema:

```yaml
meta:
  ticker: DKFT
  quarter: Q1
  year: 2026
  tool_mode: llm_tools
  search_count: 4
  fetch_count: 1
findings:
  - event: "Gross margin increased by 1,344 bps YoY"
    explanation: "..."
    confidence: medium
    period: in-period
    sources:
      - title: "..."
        url: "..."
        published_date: "..."
        evidence_type: "page"
    writer_hint: "..."
```

Rules:

- A snippet-only finding must identify its evidence as a snippet.
- A full-page finding must reference the fetched page URL.
- No source means no external claim.
- Missing publication date lowers confidence or requires an explicit `date_unverified` marker.
- The tool may provide external numbers, but financial figures remain canonical from the fact sheet.
- Internal events remain `internal_must_explain` and must not be sent to web search.

---

## 11. Pipeline and CLI Integration

Add the search-tool mode to `research.py`, not to the existing extraction or writing stages.

Proposed flags:

```text
--research-only
--skip-research
--no-search
--research-tool-mode llm_tools|deterministic_fallback
--refresh-research-cache
```

Expected behavior:

- `--research-only`: run triggers, search, tool calls, and synthesis; stop before writing.
- `--skip-research`: do not run the research stage; writer proceeds without a brief.
- `--no-search`: create triggers and internal must-explain items but prohibit external search calls.
- `--refresh-research-cache`: bypass query/page cache while preserving old artifacts for audit.
- `--write-only`: reuse the persisted research brief and must not call the search tool.

`analysis_jobs` should track at least:

```text
stage_research
research_tool_mode
research_search_count
research_fetch_count
research_cache_hits
research_degraded
research_error
```

If schema expansion is undesirable in the first phase, store these fields in the research artifact and add only `stage_research` to `analysis_jobs` initially.

---

## 12. Testing Strategy

### 12.1 Unit tests

- Query normalization produces stable cache keys.
- Different backend/config versions produce different cache keys.
- SearXNG JSON maps correctly into `SearchResult`.
- Missing publication dates remain missing rather than fabricated.
- URL validation rejects file, localhost, private-network, and unsupported schemes.
- Redirect chains are re-validated per hop (a redirect to a private-network address is rejected).
- Domain classification handles government, filing, news, research, and unknown domains.
- HTML extraction returns readable content and metadata.
- Oversized responses are truncated or rejected safely.
- Failed extraction returns structured `degraded`/`unsupported` results.
- Cache hits avoid network calls.
- Tool registry rejects unknown tool names and malformed arguments.
- Tool budgets stop further calls deterministically.

### 12.2 Protocol tests

- Assistant requests one tool call and receives the tool result.
- Multiple tool calls are handled in sequence or parallel according to configuration.
- Tool-call IDs are preserved in the conversation.
- The loop stops on final assistant content.
- The loop stops at `max_tool_rounds`.
- Existing non-tool `chat()` callers remain unchanged.
- Model tool-call rejection activates deterministic fallback.

### 12.3 Integration tests

Use mocked SearXNG and mocked page responses. Do not rely on live web pages in normal tests.

Test scenarios:

1. Search result is sufficient; no page fetch occurs.
2. Low-confidence authoritative result triggers one fetch.
3. Non-authoritative low-confidence result remains low confidence without automatic fetch.
4. Page fetch fails; synthesis continues with snippet evidence.
5. Search backend is unavailable; research stage is degraded and writer still runs.
6. `--write-only` reuses the brief and makes zero network calls.
7. A DKFT golden run produces the expected external event categories and provenance fields.

### 12.4 Live smoke test

Keep live testing separate from the default test suite. It should require an explicit flag and record:

- Backend URL
- Query count
- Fetch count
- Cache status
- Extraction failures

---

## 13. Phased Rollout

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **1. Search and fetch library** | `search_backend.py` with normalized schemas, SearXNG adapter, HTTP cache, layered HTTP/HTML extraction, Docling PDF adapter (text-based PDFs), and browser-fallback interfaces | Mocked search/fetch tests pass; failures are structured and browser fallback returns `unsupported` by default |
| **2. Tool registry** | `web_search` and `fetch_web_page` registered as typed Python tools with budgets and URL safeguards | Tool calls execute only registered operations and obey limits |
| **3. LLM tool protocol** | Add `chat_with_tools()` without changing existing `chat()` callers | Tool-call loop, multi-round limits, usage accounting, and fallback tests pass |
| **4. Research-stage integration** | Connect tools to `research.py`; produce existing research brief schema | DKFT research-only run produces replayable brief and trace |
| **5. Writer and redflags integration** | Inject brief into context; audit citations, dates, and fetch failures | Writer uses source-backed explanations and redflags detect unsupported claims |
| **6. Optional browser extractor** | Enable the Playwright fallback behind configuration, only after the §6.2 criterion is met | ≥ 2 low-confidence authoritative fetches in the DKFT/ESSA/ELSA regression runs returned JavaScript-shell pages under the HTTP-first path; enabling the fallback lets those pages be retried without changing the HTTP-first path; unsupported pages remain safe and auditable |

The first production milestone should stop after Phase 4. Browser automation and LangGraph orchestration are optional follow-up work.

---

## 14. Recommended File Layout

```text
scripts/analysis_pipeline/
├── search_backend.py              # SearXNG, cache, schemas, HTTP fetch
├── search_extractors.py           # HTML/Docling-PDF/browser extractors as they mature
├── tool_registry.py               # LLM-facing tool schemas and dispatcher
├── opencode_client.py             # existing client + chat_with_tools()
├── research.py                    # trigger/question/tool/synthesis workflow
└── plan/
    ├── EXTERNAL_RESEARCH_PLAN.md
    └── WEB_SEARCH_TOOL_PLAN.md

idx_data/analysis/research/
├── {TICKER}_{Q}_{YEAR}.research.yaml
├── {TICKER}_{Q}_{YEAR}.research.md
└── cache/
    ├── search/{query_hash}.json
    └── fetch/{url_hash}.json
```

Do not put raw fetched content in Git. Do put curated monthly sector snapshots in the vault as specified by `EXTERNAL_RESEARCH_PLAN.md`.

---

## 15. Open Questions

1. Which OpenCode Go models reliably support OpenAI-compatible tool calls? *(Open — the top implementation risk; the §7.3 deterministic fallback exists for this reason. Verify against the configured model before Phase 3.)*
2. Should the first page extractor use only standard-library/httpx-compatible dependencies, or add a readability package?
3. ~~Should the Docling PDF adapter be enabled in the first production milestone, or remain a later optional extractor?~~ **Resolved (2026-08-04):** enabled in Phase 1. The `docling` dependency is already installed for `report_pipeline`, and the adapter is a narrow wrapper around `DocumentConverter`; scope is text-based PDFs only, with scanned/image-only PDFs returning `unsupported`.
4. ~~Should tool calls execute sequentially for easier auditing, or allow bounded parallel search calls?~~ **Resolved (2026-08-04):** bounded parallel within a round (`max_parallel_calls_per_round: 8`). This keeps 6 rounds sufficient for the 44-call report budget while preserving per-call audit records.
5. Should `analysis_jobs` receive separate research usage columns immediately, or should the first version store usage only in the research trace?
6. ~~Should the LLM be allowed to call `fetch_web_page` directly, or should Python automatically fetch only after a low-confidence synthesis result?~~ **Resolved (2026-08-04):** LLM-direct, with the dispatcher enforcing the companion plan's hybrid policy — calls for domains outside `fetch_allow_domains` (or not tied to a low-confidence finding while `fetch_requires_low_confidence` is on) are rejected with `status: blocked` at no budget cost. The LLM decides *whether*; Python decides *what*.

**Recommendation:** direct typed tools with a dispatcher-enforced fetch policy, bounded-parallel rounds, cache-backed artifacts, an HTTP-first layered extractor, and the narrow Docling PDF adapter in Phase 1. Reuse `scripts/report_pipeline/pipeline.py`'s Docling dependency and conversion semantics without coupling web research to its orchestration or database state. Keep Playwright as an optional dependency, disabled by default, and enable it only when the concrete §6.2 criterion is met — then behind strict resource, timeout, concurrency, and subprocess-kill limits.

---

## 16. Success Metrics

- Tool-call loop completes without changing existing non-tool pipeline stages.
- Deterministic-fallback runs produce a complete brief marked `tool_mode: deterministic_fallback` with zero tool calls.
- 100% of fetched evidence has a URL and retrieval timestamp.
- 100% of external brief findings reference at least one search result or fetched page.
- Cache replay produces zero network requests in `--write-only` mode.
- Search and fetch budgets are never exceeded.
- Search backend failure does not block report writing.
- DKFT, ESSA, and ELSA regression runs produce named, dated external explanations rather than generic speculation.
- Redflags can identify unsupported external claims, missing dates, and failed fetches.
