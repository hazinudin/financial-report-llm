# External Research via pi Agent — Implementation Plan

**Status:** Draft for review (planned — **not yet implemented**)
**Author:** OpenCode session (GLM-5.3)
**Date:** 2026-10-15
**Scope:** Replace Stage 3b's hand-rolled OpenAI-compatible tool-calling loop (`opencode_client.chat_with_tools` + `tool_registry` + SearXNG-backed `search_backend`) with a headless **pi agent** run whose web tools come from the **`pi-exa-search`** extension (`exa_search`, authenticated with `EXA_API_KEY` loaded from `.env`) and **`pi-web-access`** (`fetch_content`). The deterministic triggers, planner (`research_plan.py`), question generation, brief schema, YAML artifacts, and the source-provenance audit are all preserved.

**Supersedes:** the search mechanics of [[EXTERNAL_RESEARCH_PLAN]] §5 (SearXNG Docker container) and [[WEB_SEARCH_TOOL_PLAN]] (Python `web_search` / `fetch_web_page` tools) — both retired by this plan. Note: [[EXTERNAL_RESEARCH_PLAN]] §12.1 already anticipated "an Exa/Tavily adapter … behind the same interface"; this plan takes the further step of moving the *whole agent loop* into pi's harness rather than swapping the search backend behind the Python tools.

**Operator decisions (2026-10-15):**

1. **Fetch strategy:** `pi-web-access` `fetch_content`; the Python-side domain allow-list and fetch gates (`fetch_allow_domains`, `fetch_requires_low_confidence`) are **disabled for now**. SSRF/local-IP protection *inside* pi-web-access remains active.
2. **Rollout:** **replace the legacy tool loop outright** — no runtime fallback to `chat_with_tools`. `deterministic_fallback` (the `--no-search` path) is retained unchanged.
3. Persist this plan doc (house convention).
4. **Research agent model (2026-10-15, later same day):** `deepseek-v4.1-flash` **for now** (also the machine's pi default). **Rollback rule:** if the model cannot use the tools — tool calling unavailable or failing for `exa_search` or `fetch_content` — revert `models.research` / `research.pi.model` to `glm-5.3-flash`. Evidence so far: the §2 live `exa_search` spike already ran *on* `deepseek-v4.1-flash` (the probe used pi's default model) and drove the tool correctly; `fetch_content` is exercised by the Phase-0 pre-flight (§8).
5. **Fetch fallback (2026-10-15, Phase 0):** pi-web-access is configured **project-scoped** via `.pi/xdg/pi/web-search.json` (fed to the pi subprocess through `XDG_CONFIG_HOME`): browser User-Agent, `fetchRouting.providers: [http, ollama, jina]` with `allowRemoteHostedProviders: true`, and `ollamaApiKey: "$OLLAMA_API_KEY"` (the key already in `.env`). Verified live: the bot-protected IDX PDF (direct 403; Jina returns a bot-check page) fetches successfully through **Ollama web fetch** — 215,754 chars of the real SMGR Q2 2026 statement.

---

## 1. Background and motivation

Current Stage 3b flow (`research.py`):

```
triggers → questions/plan → chat_with_tools loop
   tools: web_search (SearXNG Docker, engines: duckduckgo/brave/bing)
          fetch_web_page (domain allow-list + cache + budget gates)
→ YAML brief → source-provenance audit vs tool trace → artifacts
```

Pain points:

| Pain | Detail |
|---|---|
| Hand-rolled agent loop | `chat_with_tools` re-implements tool calling: no auto-retry, no abort, no cost accounting, parallel-round bookkeeping done by hand |
| SearXNG fragility | Meta-search over scrapeable engines (rate limits, blocks, engine drift); container is an external Docker dependency that must be up |
| Thin discovery | Snippets from SearXNG are short and undated-ish; most depth must come from the fetch machinery |
| No unified harness | The repo already standardizes on pi for interactive agent work (`.pi/` config, `idx-tools.ts` extension, project skills); Stage 3b is the odd Python re-implementation |

What pi gives us (verified, §2): a real agent loop with auto-retry and usage/cost tracking, a clean JSONL event stream, an extension ecosystem — including `pi-exa-search` (`exa_search`: multi-query, recency/date filters, domain include/exclude, highlights) and `pi-web-access` (`fetch_content`: full page/PDF/GitHub/YouTube extraction with stored results) — and Exa-quality discovery at ~$0.005–0.01/query.

---

## 2. Verified spike (2026-10-15) — evidence this design works

Probes run against pi 1.1.0 (`~/.local/bin/pi`) with `.env` credentials, **no repo or global config changes**:

| Probe | Result |
|---|---|
| `pi -p "…" --mode json` headless | ✅ NDJSON event stream on stdout; provider `opencode-go`, model `deepseek-v4.1-flash`, auth via `OPENCODE_API_KEY` from `.env` (subprocess env inheritance) |
| `pi-exa-search` 0.1.3 under pi 1.1.0 | ✅ Loads via `-e` (pi maps the extension's legacy `@mariozechner/*` imports through its `virtual-modules` loader — compat confirmed) |
| Live `exa_search` call | ✅ Real results (IDX filing PDF, Indopremer PDF, marketscreener news) with URLs, `publishedDate`, highlights; `costDollars: 0.007`; key came from **`EXA_API_KEY` in the environment** (no `~/.pi/exa-search.json` exists). Driven by **`deepseek-v4.1-flash`** (the probe used pi's default model) — i.e. the operator's chosen research model has already demonstrated working `exa_search` tool-calling (§1 decision 4) |
| JSONL trace shape | ✅ `tool_execution_start` carries `args`; `tool_execution_end` carries `result.content[].text` + structured `result.details.queries[].results[]` (`url`, `publishedDate`, `title`, `highlights`, `score`) — everything the provenance audit needs |
| Model availability | ✅ `pi --list-models` shows both `glm-5.3-flash` (current `models.research`, rollback model per §1 decision 4) and `deepseek-v4.1-flash` (chosen research model) on `opencode-go`, 1M context, 384K max-out |
| `skills` CLI | ✅ `npx skills add exa-labs/agent-skills --skill build-with-exa -a pi -y` installs to `.agents/skills/` (pi discovers `.agents/skills/` per Agent Skills spec) |

Spike gotchas baked into the design:

1. **Split the JSONL stream on LF only.** Python `str.splitlines()` also splits on U+2028/U+2029/`\x85`, which can appear *inside* JSON strings — records then fail to parse. (Actually hit this in the spike; pi docs warn about it explicitly.)
2. **`tool_execution_end` has no `args` field** — pair `tool_execution_start.args` with `tool_execution_end.result` via `toolCallId`.
3. The **subprocess cwd must be the project root**: pi discovers `.pi/settings.json` packages, `.pi/extensions/`, and project trust from the working directory.
4. Project-local resources are trust-gated → pass `--approve` per pipeline run.

### Phase 0 execution evidence (2026-10-15)

- `pi install -l --approve npm:pi-exa-search@0.1.3` + `npm:pi-web-access@0.41.0` ✅ (project `.pi/settings.json`, pinned, committed).
- `npx skills add exa-labs/agent-skills --skill build-with-exa -a pi -y --copy` ✅ → `.agents/skills/build-with-exa/`.
- **Extension conflict found & resolved (important):** the personal `npm:pi-web-search` package and the project `pi-web-access` both register a `web_search` tool → pi aborts at startup (`Failed to load extension … tool "web_search" conflicts`). Fix: project **filtering delta** entry `{ "source": "npm:pi-web-search", "autoload": false, "extensions": ["!*"] }`. Gotcha: `"extensions": []` in a delta is a **no-op** — pi's `applyPackageDeltaFilter` early-returns on empty patterns; `!*` is required (first-write-wins resource accumulator keeps the disabled marks). Interactive sessions in this repo are fixed by the same delta.
- **Model pre-flight under `deepseek-v4.1-flash`: PASS** — `exa_search` returned results ✅ and the model **called** `fetch_content` correctly ✅; rollback rule NOT triggered. The IDX PDF fetch returned HTTP 403 (site bot protection — target-site issue, not a model issue).
- **Fetch fallback:** a browser UA alone does not bypass IDX; Jina Reader (keyless) obtains only an IDX bot-check page; **Ollama web fetch returns the full statement** (215,754 chars, SMGR Q2 2026). Config lives in the committed project file §4.5.
- **Argv forms verified:** `--system-prompt <path>` (replaces the default system prompt) and `pi -p @<file>` (first-message file include) both work (PONG harness test, exit 0).

### Phase 3 validation evidence (2026-10-15)

- **ADMR Q2 2026 — PASS (agent path):** mode `pi_agent`, exit 0, settled, 373 s; 10/16 `exa_search` calls, 7/8 `fetch_content` calls (2 tool errors, handled); exa cost **$0.245**, token cost $0.038; **20 findings / 11 internal_must_explain**; **56 verified citations, zero findings left without sources**. Baseline comparison: the SearXNG-era last ADMR run returned "search backend unavailable" for every query and produced 0 sources.
- **Audit behaviour observed:** 4 of 60 cited URLs were model-mutated (en-dash percent-encoding slip, a `dobel`/`dua` filename typo, a missing path segment) — all correctly dropped and affected findings downgraded; the stored brief contains only trace-verified URLs.
- **YAML gotcha fixed:** unquoted `thinking: off` in `config.yaml` parses as boolean `False` (PyYAML); `pi_research` now normalises bools (`False→off`, `True→high`) and the config value is quoted. Without this fix pi silently used the model's default thinking level (`high`).
- **Known issue (pre-existing, not a pi-path defect):** the `.env` `OPENCODE_API_KEY` is rejected by `https://opencode.ai/zen/go/v1` (`Invalid credential` on `/models` and all chat calls), so `research_plan` fell back to deterministic questions (logged as `planner call failed: HTTP 401`). pi is unaffected (its own `api_key` credential for `opencode-go` is valid). Fix: refresh `OPENCODE_API_KEY` in `.env` (e.g. `pi auth print-api-key --provider opencode-go`).
- **Cost note:** ADMR exa spend ($0.245) exceeded the $0.15 target — tuning knob: `research.pi.max_search_calls` and/or narrower queries.
- **BEST Q2 2026 — PASS:** mode `pi_agent`, exit 0, settled, 126 s; 4/16 `exa_search` calls, 2/8 `fetch_content` calls (batched 3-URL fetch), no tool errors; **source audit `passed: true` — 37/37 citations verified, zero dropped**; 12 findings / 10 internal; exa cost **$0.105**, token cost $0.013; `thinking: off` effective after the YAML-boolean fix.
- **ARCI Q2 2026 — PASS:** mode `pi_agent`, exit 0, settled, 208 s; 6/16 `exa_search` calls, 5/8 `fetch_content` calls, no tool errors; **source audit `passed: true` — 52/52 citations verified (94 URLs traced), zero dropped**; 14 findings / 6 internal; exa cost **$0.161**, token cost $0.023. The brief independently flagged a fact-sheet reconciliation issue (dataset's "tax decreased 119%" vs the filed accounts' +118.7% tax *expense*) — the RCA behaviour this stage exists for.

---

## 3. Architecture overview

```
facts/ratios → triggers (unchanged) → plan (unchanged: research_plan.py via opencode_client.chat)
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ ③ PI AGENT RUN  (new: pi_research.py)                        │
│   pi -p --mode json …                                        │
│   tools (allowlisted): exa_search (pi-exa-search, EXA_API_KEY │
│                         from .env via subprocess env)        │
│                        fetch_content (pi-web-access)          │
│   harness: retries, abort, usage/cost, JSONL event stream    │
└──────────────────────────────────────────────────────────────┘
        │  NDJSON → {final text, tool trace, usage, stopReason}
        ▼
brief YAML extraction (unchanged rules) → source-provenance audit
(adapted to pi/exa trace shapes) → artifacts (+ raw JSONL, prompts)
```

### 3.1 pi invocation contract

```sh
pi -p \
  --mode json \
  --no-session \                  # ephemeral; artifacts carry the trace (pi.save_session to flip)
  --no-skills \                   # deterministic; config pi.load_skills
  --no-context-files \            # AGENTS.md/CLAUDE.md off; config pi.load_context_files
  --no-mcp \                      # no MCP servers configured
  --no-prompt-templates \
  --approve \                     # trust project-local .pi/ for this process
  --provider opencode-go \
  --model deepseek-v4.1-flash \   # operator choice "for now" (§1 decision 4); rollback → glm-5.3-flash
  --thinking off \                # maps legacy reasoning_effort: none
  -t exa_search,fetch_content \   # tool allowlist: ONLY these two are declared
  --system-prompt <run_dir>/system.md \
  @<run_dir>/user.md              # first message includes the user-prompt file
```

Notes:

- Packages come from **project `.pi/settings.json`** (§4.5), discovered because cwd = project root and `--approve` is passed. No `-e` flags needed in the pipeline run.
- The `-t` allowlist keeps every other tool undeclared (pi's built-in `read`/`bash`/`edit`/`write`, `idx-tools.ts` `duckdb_query`/`notes_search`/`notes_fetch`, pi-web-access's `web_search`/`get_search_content`/`source_check`). This is the safety replacement for the old "Python decides what may be called" dispatcher — the model cannot even see undeclared tools.
- System prompt replaces pi's coding-assistant default (`--system-prompt <path>` accepts a file); the user prompt rides in via `@file` (avoids argv-size/escaping concerns).
- **Credentials:** the runner ensures `.env` is loaded into `os.environ` (reuse `opencode_client._load_env_file()`), then spawns pi; the subprocess inherits `EXA_API_KEY` (pi-exa-search reads the env var first — verified) and `OPENCODE_API_KEY` (pi's `opencode-go` provider).
- **Credentials & env (runner-injected):** `.env` values (`EXA_API_KEY`, `OPENCODE_API_KEY`, `OLLAMA_API_KEY`, …) via subprocess env inheritance, plus `XDG_CONFIG_HOME=<repo>/.pi/xdg` so the committed project pi-web-access config applies (§4.5). Verified: `--system-prompt <path>` and `@<file>` forms both work.
- **Timeout:** whole-run subprocess timeout (`pi.timeout_sec`, default 900 s) → kill the process group, return an error result; research stays best-effort (a failed research stage never blocks `write`, per EXTERNAL_RESEARCH_PLAN §7.5).

---

## 4. Component changes

### 4.1 NEW `scripts/analysis_pipeline/pi_research.py`

Single responsibility: run the pi agent and translate the JSONL stream into the result contract the rest of `research.py` already consumes.

```python
run_agent(system_prompt, user_prompt, *, cfg) -> {
    "content": str,            # last assistant text block (the YAML brief)
    "tool_rounds": int,        # assistant turns that called tools
    "tool_trace": [            # correlated by toolCallId
        {"tool_call_id", "tool_name", "arguments", "result"}
    ],
    "usage": {"prompt_tokens", "completion_tokens", "cost_usd"},
    "finish_reason": str,      # last assistant stopReason ("stop" | "length" | …)
    "mode": "pi_agent",
    "error": str | None,
    "pi_meta": { provider, model, session_id, exit_code, duration_s,
                 thinking, retries, tool_calls: {exa_search: n, fetch_content: m},
                 exa_cost_usd },   # sum of result.details.queries[].costDollars
    "raw_jsonl": str,          # full event stream (persisted as artifact)
}
```

Internals:

- `_build_argv(cfg)` — flags per §3.1, all values from `config.yaml`.
- `_parse_jsonl(stdout)` — **split on `"\n"` only**, strip trailing `\r`; collect `tool_execution_start/end` pairs, `message_end` assistant messages (final text = last assistant message with a text block; usage/stopReason from it), `auto_retry_*` (count; surface final failure), `agent_settled` (stream completeness marker).
- Prompt files written to a per-run directory under `idx_data/analysis/research/` (gitignored, replayable): `system.md`, `user.md`, `events.jsonl`.
- Failure modes handled: non-zero exit, timeout, empty stdout, no final assistant text, stderr diagnostics → captured into `error` while still returning whatever trace was collected.

### 4.2 `research.py` rewire

| Area | Change |
|---|---|
| Imports | Drop `tool_registry`, `search_backend`; add `pi_research`. Drop dead `RAG_SCRIPT` const. |
| `_run_tool_loop` | Replaced by a thin orchestration calling `pi_research.run_agent` (model/provider/thinking from `research.pi` + `models.research`); keep the surrounding facts/events/plan assembly exactly as-is |
| `_build_research_prompt` | Tool-name updates (§4.8); everything else (identity rules, YAML contract, question-ID discipline, period labels) unchanged |
| `_trace_source_urls` | New pi/exa shapes: `exa_search` → `result.details.queries[].results[].url` (primary) + regex fallback over `result.content[].text`; `fetch_content` → `arguments.url` / `arguments.urls[]` (+ `result.details.url` if present — confirm exact shape in the Phase-3 smoke test) |
| `_validate_brief_sources` | Logic unchanged; `no_tool_calls` = zero `tool_execution_end` events in the trace |
| `meta` | Replace `backend_health` (SearXNG) with the `pi` block from `pi_meta` + post-hoc `search_count`/`fetch_count` from the trace |
| Artifacts | Keep `.research.yaml`, `.research_plan.{yaml,md}`, `.research_plan_raw.md`, `.research_raw.md`, `.research_trace.json` (extended with the `pi` block); **add** `{T}_{Q}_{Y}.research_pi.jsonl` (raw stream) and the run prompt files |
| `run()` setup | Drop `SEARCH_CACHE_DIR`/`FETCH_CACHE_DIR` creation (search_backend is retired); pi-web-access keeps its own result storage |
| Modes | `tool_mode`: `pi_agent` (default) \| `deterministic_fallback`; `--no-search` still forces `deterministic_fallback` (unchanged) |

### 4.3 `opencode_client.py`

- Delete `chat_with_tools` (only `research.py` used it). `chat` stays — the planner, writer, extraction and redflags stages keep using it.
- Update the module docstring (drop the tool-loop bullet).

### 4.4 Deletions (outright replacement — after Phase 3 validation, same change set)

```
scripts/analysis_pipeline/tool_registry.py        # schemas/dispatcher/budget
scripts/analysis_pipeline/search_backend.py       # SearXNG adapter, caches, fetch, validators
scripts/analysis_pipeline/search_extractors.py    # readability/docling extraction
tests/test_web_search_tool.py                     # tests the deleted modules
docker-compose.yml                                 # SearXNG-only service (`docker compose down` first)
config/searxng/                                    # pinned engine settings
opencode_client.chat_with_tools()                  # §4.3
research.py: tool_registry/search_backend imports, RAG_SCRIPT, cache-dir setup
config.yaml: research.searxng, fetch_allow_domains, fetch_requires_low_confidence,
             search_backend, max_tool_rounds, max_parallel_calls_per_round,
             extractor_config_version, search/fetch timeout+redirect+chars keys,
             timeouts.research (replaced by research.pi.timeout_sec),
             max_tokens.research / temperature.research / reasoning_effort.research
             (pi manages its own call params; --thinking replaces reasoning_effort)
```

Kept in `config.yaml`: `models.research` (becomes the pi `--model`), triggers, `max_questions`, `max_queries_per_question`, `brief_token_cap`, `max_planner_analysis_chars`, `sector_pack_max_age_days`, `source_classification` — the domain lists become **advisory prompt guidance** ("prefer these authoritative domains when choosing what to fetch/cite") since enforcement is disabled per the operator decision.

### 4.5 `.pi/settings.json` (project, committed)

```json
{
  "packages": [
    "npm:pi-exa-search@0.1.3",
    "npm:pi-web-access@0.41.0"
  ]
}
```

- Installed with `pi install -l npm:pi-exa-search@0.1.3` / `pi install -l npm:pi-web-access@0.41.0` (project scope, **pinned** — pi treats versioned npm specs as pinned).
- Project packages load only after project trust: pipeline runs pass `--approve`; for interactive pi sessions, approve the trust prompt once (writes `~/.pi/agent/trust.json`).
- Effect: every pi session in this repo (interactive + pipeline) gets `exa_search`/`fetch_content`; the pipeline additionally allowlists them via `-t`.
- Alternative (rejected): per-invocation `-e npm:…` — works (spike used `-e <path>`), but settings.json is the single source of truth for both interactive and pipeline runs.
- **Conflict resolution (required):** the personal `npm:pi-web-search` (from `~/.pi/agent/settings.json`) registers a `web_search` tool that collides with `pi-web-access`'s `web_search`. The project entry

  ```json
  { "source": "npm:pi-web-search", "autoload": false, "extensions": ["!*"] }
  ```

  acts as a **filtering delta** over the personal package (pi ≥ 1.1): its extensions are disabled in this project only, so discovery loads cleanly for both pipeline and interactive sessions. Gotcha: `"extensions": []` is a **no-op** for deltas (`applyPackageDeltaFilter` early-returns on empty patterns) — `!*` is required.
- **pi-web-access configuration (project-scoped, committed):** `.pi/xdg/pi/web-search.json` holds the fetch policy — browser `fetchContent.userAgent`, `fetchRouting.providers: [http, ollama, jina]`, `allowRemoteHostedProviders: true`, `ollamaApiKey: "$OLLAMA_API_KEY"`, `fetch.timeout: 30`. The runner exposes it by setting `XDG_CONFIG_HOME=<repo>/.pi/xdg` for the pi subprocess (pi-web-access resolves its config dir from `XDG_CONFIG_HOME/pi`; pi's own config/auth is unaffected — verified). No user-global config required; interactive sessions would need their own copy if desired.

### 4.6 Skills install (dev-time guidance)

```sh
npx skills add exa-labs/agent-skills --skill build-with-exa -a pi -y
```

→ `.agents/skills/build-with-exa/` (+ skills lock metadata), committed. Purpose: **development-time** canonical Exa API guidance (endpoints, request shapes, pitfalls) for coding agents maintaining this integration — *not* loaded by deterministic research runs (`-ns` default, §3.1). The Exa skill's guidance already matches what `pi-exa-search` implements at runtime (recommended request = query + `contents.highlights`, no over-decoration), so no custom Exa HTTP code is needed anywhere.

### 4.7 `config.yaml` migration sketch

```yaml
analysis:
  models:
    research: deepseek-v4.1-flash    # pi --model; operator choice "for now" — rollback → glm-5.3-flash (§1 decision 4)
  research:
    enabled: true
    agent: pi                        # pi | deterministic_fallback
    pi:
      provider: opencode-go
      model: deepseek-v4.1-flash
      thinking: off                  # maps legacy reasoning_effort: none
      timeout_sec: 900               # whole-run subprocess timeout
      approve: true                  # --approve per run
      load_skills: false             # -ns
      load_context_files: false      # -nc
      save_session: false            # --no-session
      tools: [exa_search, fetch_content]
      max_search_calls: 16           # prompt-enforced exa budget (~$0.11/report @ ~$0.007)
      max_fetch_calls: 8             # prompt-enforced fetch budget
    max_questions: 12
    max_queries_per_question: 3      # now expressed as exa_search `queries[]` batching guidance
    brief_token_cap: 4000
    max_planner_analysis_chars: 12000
    sector_pack_max_age_days: 31
    triggers: { …unchanged… }
    source_classification: { …advisory only… }
```

### 4.8 Prompt changes (`_build_research_prompt`)

| Old rule | New rule |
|---|---|
| "use `web_search` to find relevant news; `fetch_web_page` only for authoritative sources when snippets are insufficient" | "use `exa_search` for discovery — batch 2–4 related `queries` per call, use `numResults`/`recencyFilter`/`domainFilter` to shape results; highlights are short — use `fetch_content` on the strongest URLs for full text (PDFs and filings are handled; any domain is currently allowed)" |
| Budget enforced by the dispatcher (hard `status: degraded` returns) | Budget stated in-prompt: "at most N `exa_search` calls and M `fetch_content` calls; then synthesize" — actual counts audited post-hoc into `meta` |
| "Cite ONLY URLs returned by web_search or fetch_web_page tool results" | Unchanged (now: `exa_search` / `fetch_content` results) |
| Internal events → `internal_must_explain`, not searched | Unchanged |
| Identity rules, question-ID discipline, period labels, YAML-only output contract | Unchanged |

---

## 5. Budgets, cost, and failure posture

- **No hard budget enforcement exists in pi** (the old `BudgetState` dispatcher is gone). Mitigation: (a) prompt-stated budgets, (b) post-hoc counts + `exa_cost_usd` in `meta`, (c) the allowlist means no other tools can burn anything. If overruns show up in soak runs, tighten by forking/wrapping the extension (out of scope for v1).
- **Exa cost** ≈ $0.005–0.01 per query; at ≤ 16 `exa_search` calls (each may batch up to 4 `queries`) the ceiling is ~$0.11–0.6/report — target ≤ $0.15 typical.
- **Best-effort, never blocks write:** pi spawn failure, timeout, empty output, or all-tool-errors → research stage marked failed/degraded and the pipeline continues (existing behavior in `pipeline.py`).
- **Degradation visibility:** tool errors land in the JSONL trace (`isError: true` results) and in `meta.pi`; a brief with findings but zero tool calls keeps the `no_tool_calls` audit flag.

---

## 6. Tests — `tests/test_pi_research.py` (new; replaces `test_web_search_tool.py`)

- **argv construction**: flags, allowlist, pinned packages via settings, `--approve`, `--no-session`, `-ns`/`-nc`, provider/model/thinking, prompt-file paths, cwd = project root.
- **JSONL parsing** with the committed fixture streams (`tests/fixtures/pi_research_preflight.jsonl`: exa success + fetch error + usage/cost; `tests/fixtures/pi_research_fetch_fallback.jsonl`: fetch success shape): LF-only split regression (a record containing `\u2028` inside a JSON string must not break), `toolCallId` correlation, final-text extraction, usage/cost sums, `stopReason`, `agent_settled`.
- **Trace → URL extraction** for `exa_search` (`details.queries[].results[].url` + text fallback) and `fetch_content` (`arguments.url`/`urls`).
- **`_validate_brief_sources`** against pi-shaped traces: invented-URL removal, confidence downgrade, `no_tool_calls` flag.
- **Failure modes**: subprocess timeout (process-group kill), non-zero exit, empty stdout, no assistant text, stderr diagnostics captured.
- **`research.run` mode mapping**: `agent: pi` default; `--no-search` → `deterministic_fallback`; force/skip logic unchanged.
- No network in unit tests; the live smoke test is Phase 3 (manual / separately marked).

---

## 7. Documentation updates

- `scripts/analysis_pipeline/README.md` — Stage 3b section: pi agent, extensions, `EXA_API_KEY`, prerequisites (`pi` on PATH, Node ≥ 22.19), no more SearXNG container.
- Root `README.md` — SearXNG/docker references → pi + Exa; `.env` now needs `EXA_API_KEY` (gitignored).
- `.pi/APPEND_SYSTEM.md` — note the project packages and that `.agents/skills/` holds dev-time skills.
- This plan: status flips to *implemented* after Phase 5; add an addendum note to [[EXTERNAL_RESEARCH_PLAN]] §12.1 pointing here.

---

## 8. Phased rollout

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **0 Setup** *(done 2026-10-15 — see §2; rollback not triggered)* | `pi install -l` both pinned packages; `npx skills add … -a pi -y`; approve project trust once interactively; `pi list`; **model pre-flight**: one `exa_search` + one `fetch_content` call under `deepseek-v4.1-flash` | `exa_search` + `fetch_content` callable from an interactive pi session in the repo, **and** both succeed under `deepseek-v4.1-flash`; if either tool call fails or is unavailable under the model → apply the rollback rule (revert to `glm-5.3-flash`, §1 decision 4) and re-run the pre-flight |
| **1 Runner + tests** *(done 2026-10-15 — 18/18 tests)* | `pi_research.py` + `tests/test_pi_research.py` (mocked, spike fixtures) | ✅ All unit tests green; no network |
| **2 Rewire** *(done 2026-10-15 — py_compile clean; skip/dry-run paths checked; `deterministic_fallback` retained)* | `research.py` → pi runner; prompt updates; `config.yaml` migration; new artifacts | `--research-only --dry-run` OK; `deterministic_fallback` intact |
| **3 Live validation** *(done 2026-10-15 — ADMR ✅, BEST ✅, ARCI ✅)* | `--research-only --force` on 2–3 golden tickers (recent quarters with detected events) | ✅ Stored citations 100% trace-verified across all three (audit dropped only model-mutated URLs); coverage far above the degraded SearXNG baseline (0 sources); cost per report: exa $0.105–0.245 + tokens $0.013–0.038 |
| **4 Dead-code sweep** *(done 2026-10-15 — 22/22 tests green; grep clean)* | Delete the §4.4 list; `docker compose down`; docs (§7) | ✅ grep clean for removed symbols; tests green; docs updated |
| **5 Soak** *(ready 2026-10-15 — operator batch run next)* | Batch pipeline run | §11 metrics hold; no pi crashes (`~/.pi/agent/crashes.json` unchanged by the three validation runs) |

---

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| JSONL parsing fragility | LF-only split + golden fixtures (the bug was hit in the spike, now a regression test) |
| No hard tool budgets in pi | Prompt budgets + post-hoc audit (`tool_calls`, `exa_cost_usd`); tighten via wrapper extension later if needed |
| Exa spend per query | Batched `queries[]`, highlights-first, 16-call ceiling, cost tracked per run |
| `deepseek-v4.1-flash` tool-calling gaps (only `exa_search` exercised in the spike; `fetch_content` untested) | Phase-0 pre-flight exercises both tools under the model; on failure, rollback to `glm-5.3-flash` (spike-verified tool-capable) per §1 decision 4 |
| pi version drift (1.1.0 → future) | Pinned package versions; re-run spike + Phase 3 after `pi update` |
| Unrestricted fetch domains (operator choice, for now) | SSRF/local-IP protection still on inside pi-web-access; `source_classification` lists steer preference in the prompt; revisit after soak |
| Determinism drift (skills/context files, model temperature not CLI-exposed) | `-ns`/`-nc` defaults; one model+thinking level pinned in config; monitor brief variance across `--force` re-runs |
| New runtime deps in a Python pipeline (pi + Node) | Document as prerequisite (already installed on the operator machine); `pi --version` check in Phase-0 exit criteria |
| Extension/tool result shape drift (`fetch_content` details) | Confirm exact URL fields in Phase-3 smoke test; text-regex fallback already covers it |

---

## 10. Open questions

1. Expose `notes_search` (idx-tools) to the research agent so `internal_must_explain` routing can be verified against the vault? (Default: off — allowlist stays `exa_search,fetch_content`; experiment after Phase 5.)
2. Enable the `exa-web-research` skill (shipped by pi-exa-search) in research runs instead of encoding the workflow in the system prompt? (Default: off, `-ns`.)
3. Is the ~$0.11/report exa ceiling acceptable, or tighten `max_search_calls` to 8–12?
4. Keep `timeouts.research: 90` removed (replaced by `pi.timeout_sec: 900`)? (Plan assumes yes.)
5. Post-period window rule unchanged from [[EXTERNAL_RESEARCH_PLAN]] §12.3 (up to run date, always labeled)?

---

## 11. Success metrics

- **Provenance:** 100% of cited URLs trace to tool results; `source_audit.passed: true` on every run; zero `unverified_sources` survivors.
- **Coverage parity/quality:** on golden tickers, ≥ the SearXNG-era baseline of external events explained with `confidence ≥ medium`, with named, dated causes (no "suggests a favorable environment" speculation).
- **Cost:** exa ≤ $0.15/report typical; total token usage ≤ 2× the old loop.
- **Reliability:** no pi crashes or timeouts across the soak batch; a forced exa outage degrades the stage (tool errors in `meta`) without blocking `write`.
- **Determinism:** argv snapshot shows `-ns`/`-nc`/allowlist; repeated `--force` runs produce comparable briefs.
