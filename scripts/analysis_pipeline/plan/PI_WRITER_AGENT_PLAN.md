# Writer via pi Agent — Implementation Plan

**Status:** **Phases 1–4 implemented and live-validated** (2026-10-15, ADMR + ARCI) — soak (§9 Phase 5) pending
**Author:** OpenCode session
**Date:** 2026-10-15
**Scope:** Replace Stage 4's single bare OpenAI-compatible call
(`opencode_client.chat` in `write.py`) with a **headless pi agent** run
(`pi -p --mode json`), reusing the runner shipped for Stage 3b
([[PI_RESEARCH_AGENT_PLAN]]). The agent keeps the existing input package (writer
system prompt + 14-section skeleton + canonical fact sheet + computed ratios +
static profile + research brief + vault hits + market stub) and still emits the
final markdown document as its last assistant message. The only behaviour
change is that the model may spend a bounded number of **read-only** tool calls
(`read`, `duckdb_query`, `notes_search`, `notes_fetch`) to ground peer
comparisons and prior-period trends before writing.

**Supersedes:** the single-call mechanics of Stage 4 in
[[IMPLEMENTATION_PLAN]] (§ "Stage 4"). The writer prompt, the fact-sheet/
ratios authority rules, the 14-section skeleton, the output path, and the
`--inject-redflags` re-write loop are all preserved.

**Relationship to [[PI_RESEARCH_AGENT_PLAN]]:** that plan moved the *whole
agent loop* for research into pi's harness. This plan does the same for the
writer, but with a deliberately **narrower** tool surface: writing must not add
new external evidence, so `exa_search`/`fetch_content` stay out of the
writer's allowlist. pi's harness (retry, usage/cost, JSONL trace, deterministic
argv) plus read-only access to the project's own data is the whole point.

**Operator decisions (2026-10-15):**

1. **Tool surface — CONFIRMED:** read-only set
   `read, duckdb_query, notes_search, notes_fetch`; excluded: `bash`, `edit`,
   `write`, `exa_search`, `fetch_content`. `tools: []` yields the harness-only
   variant (`--no-tools`), selected via config only.
2. **Rollout — CONFIRMED:** replace the bare call **outright** — no
   `direct_api` path, no mode toggle. The `opencode_client.chat` writer call and
   the write-only config keys (`timeouts.write`, `max_tokens.write`,
   `temperature.write`) are deleted in the same change set (research precedent).
3. **Failure posture:** the writer is **not** best-effort. A pi failure raises
   and fails the `write` stage (no silent empty document, no automatic
   fallback, no alternate code path). Re-running is the operator's call.
4. **Model:** `write.pi.model` defaults to `models.write` (`glm-5.3`), verified
   present on `opencode-go` under pi 1.1.0 (1M ctx / 131.1K max-out). Rollback
   rule: if `glm-5.3` cannot drive the tools cleanly, drop to `tools: []`
   (harness-only) rather than change the model — the document quality is bound
   to `glm-5.3`, not to the tools.
5. **Next step:** the live pre-flight is **folded into Phase 3** — it validates
   the finished code path rather than a hand-built spike.

---

## 1. Background and motivation

Current Stage 4 flow (`write.py`):

```
facts/ratios + context package + research brief
        │
        ▼
opencode_client.chat(model=glm-5.3, system=writer_system.md, user=~30K-token package,
                     temperature=0.3, max_tokens=32768, timeout_sec=1200)
        │  (4 in-process retries on 429/5xx)
        ▼
content.strip() → analysis/{TICKER} {Q} {Y} Analysis.md
```

Pain points:

| Pain | Detail |
|---|---|
| No cost visibility | `chat()` returns only `prompt_tokens`/`completion_tokens`; `db.add_tokens` stores those two, nothing else (verified: `db.py` has no cost column). The most expensive stage in the pipeline reports no dollar cost. pi reports `usage.cost.total` per assistant message. |
| Thin comparative context | The writer gets one fact sheet (current + one prior column) and ≤3 vault snippets. Peer multiples (PER/PBV/ROE vs. peers) and multi-year trends are therefore either omitted or filled from the model's parametric memory — exactly the failure mode "numbers are sacred" exists to prevent. `duckdb_query` (`financial_ratios`, `xlsx_statements`) and `notes_search` can close this gap with the company's *own* DB/vault data. |
| No execution trace | Replayability depends on the `context.json` package; there is no record of model turns, retries, or what the model read. pi emits a full JSONL stream (persisted like the research run). |
| The odd one out | Stage 3b already runs on pi (pinned packages in `.pi/settings.json`, `--approve`, JSONL → result contract, timeout kill). Stage 4 is the last LLM-execution stage still on the hand-rolled HTTP path. |

Non-goals: no change to the fact sheet, ratios, research brief, skeleton, or
the output path; no new external web evidence; no change to Stage 5 redflags
(beyond the re-write it already triggers).

---

## 2. Facts verified in the repo (2026-10-15)

| Check | Result |
|---|---|
| pi on PATH | `pi 1.1.0` (`~/.local/bin/pi`) |
| Write model under pi | `pi --list-models` lists `opencode-go glm-5.3` (1M ctx, **131.1K max-out**) — the configured `models.write` is usable; the target document (~10K tokens) has ~10× headroom |
| pi call-parameter flags | No `--max-tokens` / `--temperature`; pi manages call params and `--thinking` replaces `reasoning_effort` (matches the research plan §4.4 treatment) |
| No-tools argv form | `--no-tools`, `-nt` exist ("Disable all tools by default") |
| Tool allowlist | `-t, --tools <comma-list>` (already used: `-t exa_search,fetch_content`) |
| Project tools | `.pi/extensions/idx-tools.ts` registers `duckdb_query`, `notes_search`, `notes_fetch`; `.pi/settings.json` pins `pi-exa-search` + `pi-web-access`; all load with `--approve` + cwd = project root |
| Existing runner is generic | `pi_research.run_agent(system_prompt, user_prompt, *, cfg, run_dir, project_root)` already does argv build → spawn → LF-only NDJSON parse → result contract. Research-specific parts are only the `exa_cost_usd` accumulation (a no-op without exa calls) and `extract_tool_urls` (research-only helper). One gap: empty `tools` currently emits `-t ""`; it must switch to `--no-tools` |
| Stage 4 contract consumed downstream | `pipeline.py`: `res["usage"]["prompt_tokens"/"completion_tokens"]`, `res["model"]`, `res["status"]`, `res["output"]`; the pi result contract already carries all of these (+ `cost_usd`, `pi_meta`, `tool_trace`) |
| Writer tests | `tests/test_writer_context.py` covers only `_load_research_context`; `tests/fixtures/` holds two research JSONL streams |
| Baseline documents | `analysis/ADMR Q2 2026 Analysis.md`, `BEST Q2 2026`, `ARCI Q2 2026` exist (same tickers the research plan validated) → available for A/B in Phase 3 |

**Pre-flight (Phase 3, done 2026-10-15):** ✅ `glm-5.3` drives
`duckdb_query`/`notes_search` correctly, and a pi run emits a complete
14-section document as its final message — see the §9 evidence block.

---

## 3. Architecture overview

```
facts/ratios ─┐
context pkg   ├─► (unchanged) _build_user() prompt package ~30K tokens
research brief┘        │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  STAGE 4 PI AGENT RUN  (write.py → shared pi_agent.run_agent) │
│   pi -p @user.md --mode json                                 │
│   --no-skills --no-context-files --no-mcp                    │
│   --no-prompt-templates --approve                            │
│   --provider opencode-go --model glm-5.3 --thinking off       │
│   -t read,duckdb_query,notes_search,notes_fetch               │
│   --system-prompt <run_dir>/system.md                         │
│   harness: auto-retry, usage/cost, JSONL event stream,        │
│            process-group timeout                              │
└──────────────────────────────────────────────────────────────┘
        │  NDJSON → {final markdown, tool trace, usage+cost, stopReason}
        ▼
outer-fence strip → analysis/{TICKER} {Q} {Y} Analysis.md
                   + write/{...}.write_trace.json (audit artifact)
```

### 3.1 pi invocation contract (writer)

```sh
pi -p \
  --mode json \
  --no-skills \
  --no-context-files \
  --no-mcp \
  --no-prompt-templates \
  --approve \
  --provider opencode-go \
  --model glm-5.3 \
  --thinking off \
  -t read,duckdb_query,notes_search,notes_fetch \
  --system-prompt <run_dir>/system.md \
  @<run_dir>/user.md
```

Notes:

- The `-t` allowlist keeps `bash`/`edit`/`write` and the web tools
  (`exa_search`, `fetch_content`, `web_search`, …) undeclared — the model
  cannot see them. The allowlist is the safety replacement for "the pipeline
  decides what may be called".
- `@user.md` carries the ~30K-token package (avoids argv size/escaping) — same
  form already validated for research.
- `XDG_CONFIG_HOME=<repo>/.pi/xdg` is set by the runner (research parity).
- No `--max-tokens`/`--temperature`: pi owns those. The document target is
  well inside `glm-5.3`'s 131.1K max-out.
- Empty `tools` ⇒ `--no-tools` instead of `-t ""` (see §4.1).
- **Sessions are persisted** (flipped 2026-10-16): `write.pi.save_session: true`
  (and `research.pi.save_session: true`) ⇒ `--no-session` is omitted and pi
  writes the session under `~/.pi/agent/sessions/`, browsable/resumable with
  `pi --resume`. The session id is still recorded in `pi_meta`/the run trace, so
  the run directory and `write_trace.json` remain the canonical audit artifacts.
  Set `save_session: false` to return to ephemeral runs.

---

## 4. Component changes

### 4.1 NEW `scripts/analysis_pipeline/pi_agent.py` (shared runner)

Lift the generic core out of `pi_research.py` unchanged:

```python
run_agent(system_prompt, user_prompt, *, cfg, run_dir, project_root=None)
  → {"content", "tool_rounds", "tool_trace", "usage"(+cost_usd),
     "finish_reason", "mode", "error", "pi_meta", "raw_jsonl"}
```

Moved: `_as_int`, `_as_float`, `_normalise_thinking`, `_clean_url` (used by the
URL helper), `_empty_result`, `_build_argv`, `_split_records`, `_parse_jsonl`,
`_spawn_pi`, `_build_result`, `run_agent`.

One behavioural generalisation (§2 gap):

```python
tools = cfg.get("tools") or []
if tools:
    argv += ["-t", ",".join(str(t) for t in tools)]
else:
    argv.append("--no-tools")
```

`exa_cost_usd` stays in `pi_meta` (0.0 when no `exa_search` calls) so the result
shape is identical for both stages.

### 4.2 Module rename + test alias (as implemented)

`pi_research.py` is **renamed** to `pi_agent.py`. The research defaults
(`_DEFAULT_TOOLS = ["exa_search", "fetch_content"]`) and `extract_tool_urls`
move with it. Two mechanical call-site edits landed:

- `research.py`: `import pi_agent` + `pi_agent.run_agent(...)` /
  `pi_agent.extract_tool_urls(...)` (3 lines) and one stale docstring reference.
- `tests/test_pi_research.py`: the import becomes `import pi_agent as
  pi_research` — **one line**, so every assertion and patch target
  (`pi_research._spawn_pi`, `pi_research.shutil`, `pi_research.os`, ...)
  resolves unchanged.

Rationale: a re-export shim cannot preserve `patch.object(pi_research,
"_spawn_pi")`, because the extracted `run_agent` resolves `_spawn_pi` in its own
module namespace. An `import ... as` alias in the test is the minimal correct
fix and keeps every assertion byte-identical.

### 4.3 `write.py` re-wiring

| Area | Change |
|---|---|
| Config access | New `_write_config()` reading `analysis.write` (mirrors `research._research_config`) |
| Prompt build | `_build_user(...)` unchanged; **append** an `# ARTIFACTS ON DISK` block with absolute paths (fact sheet, ratios, research brief/plan, context package) so `read` has something to read |
| System prompt | `load_writer_prompt()` + `_tools_section(pi_cfg)` (budget numbers resolved from config; §4.6) |
| New `_run_pi_writer(...)` | Writes `run_dir`, calls `pi_agent.run_agent`, raises on `error`, strips an outer code fence, returns `(content, result)` |
| Run dir | `idx_data/analysis/write/{T}_{Q}_{Y}.write_pi/` → `system.md`, `user.md`, `events.jsonl` |
| Audit artifact | `idx_data/analysis/write/{T}_{Q}_{Y}.write_trace.json`: `pi_meta`, `tool_trace`, `usage`, `finish_reason`, budget counts + warnings, output path |
| Return contract | `{"status", "output", "model", "usage", "pi_meta", "tool_trace"}`; `model` from `pi_meta.model or pi_cfg.model` |
| `dry_run` | Does not spawn; reports the model + `user_chars` (as today) |
| Skip logic | Unchanged (`out_path.exists() and not force and not dry_run`) |
| Deleted | The `opencode_client.chat` writer call and `parse_model_settings("write")` usage (outright replacement) |

Sketch:

```python
def run(ticker, year, quarter, market_data_path=None, force=False,
        dry_run=False, inject_redflags=None) -> dict:
    cfg = _write_config()
    pi_cfg = _resolve_pi_cfg(cfg)          # provider/model default ← models.write
    ...                       # facts/ratios/context load — unchanged
    user = _build_user(...) + _artifacts_block(ticker, year, quarter)
    if inject_redflags:
        user += f"\n\n# RED FLAGS TO ADDRESS EXPLICITLY\n{inject_redflags}\n\n..."

    if dry_run:
        return {"status": "dry", "model": pi_cfg["model"], "user_chars": len(user)}

    content, result = _run_pi_writer(ticker, year, quarter, user, pi_cfg,
                                     run_dir=WRITE_DIR / f"{ticker}_{quarter}_{year}.write_pi")
    ...                                     # write file, dump trace, print [WRITE pi_agent]
    return {"status": "success", "output": str(out_path),
            "model": result["pi_meta"].get("model") or pi_cfg["model"],
            "usage": result["usage"], "pi_meta": result["pi_meta"],
            "tool_trace": result["tool_trace"]}
```

Note `run()` keeps its existing positional signature (`__main__` calls it
positionally), so `write_tool_mode` is **not** added.

### 4.4 `pipeline.py`

- No CLI flag and no mode toggle: pi is the only writer path, so nothing new to
  thread through — `run_job`'s two `write.run(...)` calls (stage 4 and the
  `--inject-redflags` re-write) keep their current arguments.
- Token recording unchanged (same key names). Cost is *not* written to the DB
  in v1 — it lands in the trace artifact and the returned dict (§11 Q4).

### 4.5 `config.yaml` migration sketch

```yaml
analysis:
  models:
    write: glm-5.3            # canonical; write.pi.model falls back to this
  write:
    pi:
      provider: opencode-go
      # model: glm-5.3        # optional; defaults to models.write
      thinking: "off"         # mirrors today's non-reasoning call
      timeout_sec: 1800       # whole-run (replaces timeouts.write: 1200)
      approve: true
      load_skills: false
      load_context_files: false
      save_session: true      # persist the pi session (flipped 2026-10-16)
      tools: [read, duckdb_query, notes_search, notes_fetch]   # [] → --no-tools
      max_db_queries: 6       # prompt-enforced budget
      max_notes_search: 4
      max_notes_fetch: 4
```

`models.write` stays and is the pi `--model`; `timeouts.write`,
`max_tokens.write`, and `temperature.write` are **removed** — pi owns call
params and the writer now has no non-pi path. No new `.env` keys — the runner
loads `.env` (reusing `opencode_client._load_env_file`) and inherits
`OPENCODE_API_KEY`; `EXA_API_KEY` is irrelevant to this stage.

### 4.6 Prompt changes

`prompts/writer_system.md` stays as-is; `write.py` appends a
`_tools_section(pi_cfg)` whose budget numbers come from config (the resolved
prompt is captured in `run_dir/system.md`):

```
## Tools (read-only — for context, never for company figures)
- read: re-read any input file listed in the ARTIFACTS ON DISK block.
- duckdb_query: read-only SQL over idx_data/pipeline.duckdb. Use ONLY for
  (a) peer comparison (financial_ratios: per, pbv, roe, roa, der by code+period;
  values in billions IDR except eps) and (b) this company's OWN prior periods.
  NEVER source the current-period company figures in this document from
  duckdb_query — the fact sheet wins, always. If a DB figure contradicts the
  fact sheet for the current period, use the fact sheet and flag the discrepancy.
- notes_search / notes_fetch: vault search for prior analyses / commodity notes;
  prefer these over the 3 snippet hits when you need a prior-quarter number or a
  full cited section.
Budget: at most {max_db_queries} duckdb_query, {max_notes_search} notes_search,
{max_notes_fetch} notes_fetch calls — then write. Label every non-fact-sheet
figure with its source ("per pipeline DB", "[[note]]"). In "Related Analysis",
prefer wikilinks to notes you actually read or that appear in VAULT CONTEXT.
```

`_build_user` additions (always present, cheap):

```
# ARTIFACTS ON DISK (re-read with the read tool to re-check any input)
- Canonical fact sheet: {FACTS_DIR}/{T}_{Q}_{Y}.yaml
- Computed ratios:      {FACTS_DIR}/{T}_{Q}_{Y}.ratios.yaml
- Research brief:       {RESEARCH_DIR}/{T}_{Q}_{Y}.research.yaml
- Research plan:        {RESEARCH_DIR}/{T}_{Q}_{Y}.research_plan.yaml
- Context package:      {RUNS_DIR}/{T}_{Y}_{Q}.context.json
```

Everything else in `writer_system.md` (numbers-are-sacred, provenance,
unresolved items, structure, scoring, recommendation, end matter, output
discipline) is unchanged.

### 4.7 Output handling

- `content = _strip_outer_fence(result["content"]).strip()` — strip ``` fences
  only when the *entire* text is wrapped (same approach as the research YAML
  parse; inner fenced blocks in a markdown document are left alone).
- Empty/whitespace content ⇒ `RuntimeError` (fail the stage, never write an
  empty file).
- Write with a trailing newline (today's behaviour).
- `--inject-redflags` re-write: unchanged entry point (`write.run(..., force=True,
  inject_redflags=bullets)`); the block is appended to `user.md`, so both modes
  get it for free.

---

## 5. Tool policy and guardrails

| Layer | Control |
|---|---|
| Declaration | `-t` allowlist; `bash`/`edit`/`write` and all web tools are undeclared and invisible |
| Filesystem | No `write`/`edit`/`bash` ⇒ the agent cannot modify the repo or the output document; the pipeline owns the file |
| Facts authority | Prompt rule: fact sheet + ratios are absolute for the company's current period; DB/vault figures are comparative context only, always labelled |
| Budget | Prompt-stated caps (§4.5) + post-hoc counts (`tool_calls`) with `budget_warnings` in the trace, exactly like research |
| Audit | `write_trace.json` records every tool call (name, args, result) → an operator can see which numbers were fetched and from where |
| External evidence | Out of scope for Stage 4; `exa_search`/`fetch_content` are not in the allowlist (Stage 3b owns them, with its own provenance audit) |

Residual risk and mitigation: a DB figure can still be mislabelled as a
company fact. Mitigations are (a) the prompt hierarchy above, (b) the Stage 5
red-flag pass as a second reader, (c) the trace artifact for spot audits. A
full number-level provenance checker (parse the document, match every figure
back to the fact sheet) is explicitly out of scope for v1 (§11 Q5).

---

## 6. Budgets, cost, and failure posture

- **Cost visibility (new):** `usage.cost_usd` (sum of `message.usage.cost.total`)
  is returned and stored in the trace. This is the first stage-4 cost number the
  pipeline has ever produced; §12 uses it for the A/B.
- **Latency:** one write + up to `6+4+4` tool rounds. Default whole-run timeout
  1800 s (today's single call is capped at 1200 s); `duration_s` is recorded.
- **Failure posture — the key departure from research:** the writer is *not*
  best-effort. pi missing / timeout / non-zero exit / no assistant text / empty
  document ⇒ `run()` raises ⇒ stage `write` is marked failed and the job stops
  (`pipeline.py` already handles this branch). No automatic fallback and no
  alternate code path — re-running (with `tools: []` if the tools are at fault)
  is the operator's call.
- **Degradation visibility:** tool errors land in the JSONL trace
  (`isError: true`) and in `pi_meta.tool_errors`; a `notes_search` failure (e.g.
  stale index) is non-fatal to the agent, which still has the prompt's ≤3 hits.
- **Retries:** pi's `auto_retry_start` events are counted into `pi_meta.retries`
  (replaces `chat()`'s in-process 4× backoff, which is deleted with the writer
  call).

---

## 7. Tests

`tests/test_pi_agent.py` (new) — the existing research-runner coverage keeps
running against `pi_agent` through the one-line test alias, **plus** a new case:
`tools: []` ⇒ `--no-tools` (and `tools: [...]` ⇒ one `-t` flag).

`tests/test_pi_writer.py` (new, no network, spawn faked — mirrors
`test_pi_research.py`):

- **argv/defaults:** provider/model/thinking from cfg, model falls back to
  `models.write` when `write.pi.model` is unset; allowlist string; `--approve`;
  `--no-session`/`--no-skills`/`--no-context-files`; cwd = project root;
  `XDG_CONFIG_HOME` set.
- **Fixture stream** `tests/fixtures/pi_writer_preflight.jsonl`: `read` +
  `duckdb_query` + `notes_search` turns, then a final `message_end` whose text
  is a miniature document (front matter + headings + tables). Asserts final-text
  extraction, `tool_trace` correlation by `toolCallId`, `usage`/`cost_usd`
  sums, `finish_reason`, `agent_settled`.
- **Output handling:** outer fence stripped; inner fenced block preserved;
  empty/whitespace content ⇒ error.
- **`write.run`** (runner mocked): writes the file and returns
  `usage`/`model`/`pi_meta`; a runner error ⇒ raises; `--inject-redflags` text
  appears in `run_dir/user.md`; `dry_run` spawns nothing; existing skip logic
  intact; `write.pi.model` falls back to `models.write`.
- **Trace artifact:** `write_trace.json` written with counts + `budget_warnings`
  when a budget is exceeded.

Existing `tests/test_writer_context.py` must stay green untouched.

---

## 8. Documentation updates

- `scripts/analysis_pipeline/README.md` — Stage 4 row: "headless pi agent
  (`pi -p --mode json`), read-only tools (`read`/`duckdb_query`/`notes_search`/
  `notes_fetch`), `glm-5.3`"; artifacts tree gains
  `write/{T}_{Q}_{Y}.write_pi/` + `.write_trace.json`.
- Root `README.md` — Stage 4 sentence ("ONE strongest-model call") → pi agent;
  no new prerequisites (pi/Node already documented for Stage 3b).
- `.pi/APPEND_SYSTEM.md` — note that Stage 4 now also runs on pi with the
  read-only idx-tools.
- This plan: status flips to *implemented* after Phase 5.

---

## 9. Phased rollout

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **1 Runner + tests** *(done 2026-10-15 — 25/25 tests)* | `pi_agent.py` extraction (rename; 3-line `research.py` update; 1-line test alias); `test_pi_agent.py` no-tools case | ✅ All 23 prior tests green (assertions unchanged), 25 total; no network |
| **2 Re-wiring** *(done 2026-10-15 — 42/42 tests; dry-run OK)* | `write.py` → pi runner (run dir, trace, budgets, fence strip), `config.yaml` (`write.pi`; write-only keys deleted), `_tools_section`, docs | ✅ 42 tests green; dry-run prints `[DRY] … glm-5.3 … user_chars≈89k`; no `chat()` writer call remains |
| **3 Live validation** *(done 2026-10-15 — ADMR ✅, ARCI ✅)* | `--force` on ADMR Q2 2026 + ARCI Q2 2026 (BEST is unavailable — its analysis was deleted before this work) | ✅ Both: tools driven, zero tool errors, 14-section documents (591 / 538 lines), cost $0.1365 / $0.1287, traces written, no fabricated peer data when the DB returned nothing. See the evidence block below |
| **4 Docs / cleanup** *(docs done 2026-10-15)* | README (stage table + artifacts + Writer section), root README, APPEND_SYSTEM; remove now-dead writer code | ✅ Docs updated and verified against the code (stale `hy3`/`deepseek-v4-flash` IDs corrected); no dead `chat()` writer path remains |
| **5 Soak** | Batch pipeline run | §12 metrics hold; no pi crashes (`~/.pi/agent/crashes.json` unchanged) |

### Phase 3 validation evidence (2026-10-15)

- **Capability probe — PASS.** A one-shot `pi -p` run with `-t duckdb_query`
  under `glm-5.3` (opencode-go) issued a correct query and read back real data
  (`roe 19.6309`, `pbv 2.31`); cost $0.00063. The model **can** drive the
  idx-tools — so a run with no tool calls is a prompting issue, not capability.
- **First ADMR Q2 2026 run (tools available, no nudge) — PASS but tools unused.**
  exit 0, settled, 210.7 s, **0 tool calls**, 474 lines, all 14 sections, cost
  **$0.0998** (30,631 in / 12,927 out). The tools section and `# ARTIFACTS ON
  DISK` block were both present in `system.md`/`user.md`; the model simply did
  not call them (events.jsonl: zero `tool_execution_*`).
- **Nudge.** One directive sentence was added to `_tools_section`, naming §6/§8
  (peer multiples via `duckdb_query`) and §9 (prior-quarter trend via
  `notes_search`/`notes_fetch`) as the places to ground, plus "if a tool returns
  nothing useful, continue".
- **ADMR Q2 2026 re-run (nudged) — PASS.** exit 0, settled, 299.8 s,
  **2 tool calls** — `duckdb_query` (ADMR/ADRO/AADI/PTBA/ITMG multiples) and
  `notes_search` (Q1 analysis / forecast / KAI) — **zero tool errors, zero
  budget warnings**; **591 lines** (14 sections + Related Analysis, front matter,
  disclaimer); cost **$0.1365** (32,496 in / 18,844 out). §8/§9 now carry a
  labelled peer comparison ("external, per pipeline DB, latest period Oct 2025":
  ADMR P/E 11.76x vs ADRO 5.65x / AADI 4.15x) **while still declining to state a
  market-based target price** without a price feed — the fact-sheet-wins
  guardrail held.
- **A/B line count:** pre-pi baseline 545 · pi no-tools 474 · pi + tools 591.
  Only the tool-enabled run has sourced peer multiples. Cost of the nudge:
  +$0.037, +90 s.
- **ARCI Q2 2026 (nudged) — PASS, with an honest-omission case.** exit 0, settled,
  255.3 s; **3 tool calls** (`duckdb_query` ×2, `notes_search` ×1); 538 lines
  (14 sections + Related Analysis); cost **$0.1287** (30,219 in / 15,616 out);
  zero tool **errors** (empty result sets are not errors) and zero budget
  warnings. Both peer queries returned `(no rows)` — the model guessed peer
  codes (`ANTM`/`MDKA`/`BRMS`/`PSAB`) that this pipeline has never ingested
  (`financial_ratios` holds only 27 codes, and none of those four), and the
  document **explicitly reported the empty result and omitted the comp table
  rather than inventing one** — i.e. the "don't fabricate" guardrail held under
  a tool failure. `notes_search` returned usable prior-quarter context (Q1 2026
  analysis, gold seasonality note), which the document cites.
  - Follow-up (implemented, not yet live re-tested): `_tools_section` now tells
    the agent to confirm a peer code exists (`SELECT DISTINCT code, name FROM
    financial_ratios WHERE name ILIKE '%gold%'`) and to omit the comp rather
    than guess. Re-validate in Phase 5 (soak).
- Backups for the A/B live in the session temp dir
  (`…/opencode/baselines/`: baseline + `ADMR_pi_notools.md`).

---

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| `glm-5.3` tool-calling gaps (unverified pairing) | Phase-3 smoke first; fallback to `tools: []` (still pi, harness-only, behaviourally equivalent to the old single call) |
| DB/vault figures leak into the company's own numbers | Prompt authority hierarchy + labels; Stage 5 redflags as second reader; trace artifact for audits |
| Agent spends the whole budget in tool rounds | Prompt caps + `budget_warnings`; 1800 s timeout with process-group kill |
| `notes_search` index missing/stale | Tool error is non-fatal; the agent falls back to the prompt's ≤3 hits; recorded in `pi_meta.tool_errors` |
| Output wrapped in a code fence | Outer-fence strip (inner blocks preserved) |
| No temperature knob under pi | `--thinking off` pinned; monitor variance across `--force` re-runs (research precedent) |
| Long document truncation | `glm-5.3` max-out 131.1K ≫ target; `finish_reason` recorded and asserted in tests |
| pi version drift | Pinned packages; pi ≥ 1.1 prerequisite already documented |
| Hidden regression in `research.py` from the runner move | Pure-move refactor with the research tests unchanged as the guard |

---

## 11. Open questions (operator decision points)

1. **Tool surface:** **confirmed** as `[read, duckdb_query, notes_search,
   notes_fetch]` (2026-10-15) and validated live on ADMR + ARCI. The Phase-3
   smoke did **not** trigger the `tools: []` fallback; that remains available as
   a config-only escape hatch.
2. **`thinking`:** `off` (mirrors today) vs `low` for synthesis quality — A/B in
   Phase 3 if cost allows.
3. **Persist cost to `analysis_jobs`** (small schema change) or keep cost in the
   trace artifact only. v1 assumes artifacts only.
4. **Number-level provenance checker** (parse the document, verify each figure
   against the fact sheet/ratios) — out of scope for v1; revisit after soak.
5. **Give the writer web tools** for a missing market stub — default **no**
   (Stage 3b owns external evidence; strict provenance).
6. **`load_skills: true`** so the `report-review` project skill is available —
   default **no** (`writer_system.md` already encodes the framework; keep the
   run deterministic and comparable).

---

## 12. Success metrics

- **Quality:** parity or better vs. the current documents on the three golden
  tickers (structure checklist: front matter, related-note wikilinks, all 14
  sections, margin tables, severity-ranked risks, X/10 scores, BUY/HOLD/SELL +
  target range, Related Analysis, disclaimer).
- **Traceability:** every tool call recorded; counts within prompt budgets; zero
  tool errors on golden tickers (or recorded + non-fatal).
- **Cost:** a real per-write `cost_usd` is now recorded (baseline: unknown);
  completion tokens ≤ ~2× today's single-call output.
- **Reliability:** no pi crashes; timeout rare (target < 1 in 10); a failed
  write fails the job loudly — never a silent empty file.
- **Determinism:** argv snapshot pins provider/model/thinking/allowlist; repeated
  `--force` runs produce comparable documents.
