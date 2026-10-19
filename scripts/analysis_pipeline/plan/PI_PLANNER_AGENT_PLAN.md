# Research Planner via pi Agent — Implementation Plan

**Status:** **Proposed** — design validated by a live spike (2026-10-19, ITMG Q1 + Q2); implementation not started
**Author:** OpenCode session
**Date:** 2026-10-19
**Scope:** Replace Stage 3b's research-plan step (`opencode_client.chat` inside
`research_plan.formulate_plan`) with a **headless pi agent** run
(`pi -p --mode json`) through the shared runner in
[[PI_RESEARCH_AGENT_PLAN]] / `pi_agent.py`. The planner keeps its prompt, its
YAML contract, its parser, and its deterministic fallback; it gains a
**read-only tool surface** (`read`, `duckdb_query`, `notes_search`,
`notes_fetch`) so it can verify period arithmetic and prior-period baselines
before framing questions. What changes is the transport, the grounding, and what
we can observe about it.

**Supersedes:** the single-call mechanics of the planner only. Stage 3b's search
agent, the brief schema, the source-provenance audit, and Stage 4's writer are
untouched.

**Relationship to [[PI_RESEARCH_AGENT_PLAN]] and [[PI_WRITER_AGENT_PLAN]]:** those
plans moved the research loop and the writer onto pi. This is the last
LLM-execution step in the pipeline still on the hand-rolled HTTP path, and it is
the one whose failures are currently **silent**.

**Operator decisions (2026-10-19) — CONFIRMED:**

1. **Rollout — replace outright.** No `direct_api` toggle, no soak escape hatch.
   The `opencode_client.chat` planner call and the planner-only config keys
   (`timeouts.research_plan`, `max_tokens.research_plan`,
   `temperature.research_plan`, `reasoning_effort.research_plan`) are deleted in
   the same change set, exactly as the research and writer migrations did.
2. **Failure posture — best-effort, but loud.** A planner failure still falls
   back to deterministic questions (the research stage already shipped documents
   on the fallback), but it must write a trace, record the reason in the plan
   artifacts, and be visible in `--status`. No silent degradation.
3. **Tools — read-only set.** `read`, `duckdb_query`, `notes_search`,
   `notes_fetch` (the same allowlist as the Stage 4 writer). External web tools
   (`exa_search`, `fetch_content`) stay excluded: the planner establishes *what*
   to research; Stage 3b owns the external evidence and its provenance audit.
   This makes the planner's grounding stronger than the inline, character-capped
   context it gets today, at the cost of a small prompt-enforced tool budget
   (§5/§6).

---

## 1. Background and motivation

Today's planner path:

```
facts/ratios + events + prior analyses
        │
        ▼
opencode_client.chat(model=glm-5.3, system=..., user=~4K-token context,
                     temperature=0.0, max_tokens=16384, reasoning_effort=None,
                     timeout_sec=120)          # 4 in-process retries on 429/5xx
        │
        ▼
parse_plan(raw) ── failure ──► _fallback_plan(reason=...)  ⇒ deterministic questions
```

Three defects were observed on the 2026-10-19 ITMG run, all of which cost the
themed plan and produced only a console line:

| Defect | Evidence | Root cause |
|---|---|---|
| **Every planner call failed** on the ITMG Q1/Q2 run | `[RESEARCH PLAN] failed: HTTP 400 … GLM-5.3 is a thinking-only model; disabling thinking (reasoning_effort='none') is not supported` | `reasoning_effort.research_plan: none` survived the model switch to `glm-5.3` (commit `c85b97c`). Fixed in `config.yaml` during this investigation. |
| **Silent timeout** on Q2 | `[RESEARCH PLAN] failed: chat failed after 4 retries: The read operation timed out`; `timeouts.research_plan: 120` | A thinking-only model needs minutes. Raised to 600 s during this investigation. |
| **Silent truncation** on Q1 retest | `finish_reason: length`, `completion_tokens: 16384`, **`content: ""`** | `max_tokens.research_plan: 16384` is consumed entirely by reasoning; nothing is left for the YAML. |

The common thread is **not** a prompt problem — it is that a bare
`chat()` call gives us no visibility into reasoning spend, no per-run trace, no
retry semantics we can inspect, and no cost number, so each of these three
failures degraded silently into "planner unavailable". The pipeline still
produced good documents (the ITMG Q1/Q2 analyses shipped), which is exactly why
the defect went unnoticed: **the fallback is good enough to hide the failure.**

Pain points, mirroring the writer plan's table:

| Pain | Detail |
|---|---|
| No cost visibility | `chat()` returns only `prompt_tokens`/`completion_tokens`; `analysis_jobs` has no cost column. The planner's reasoning tokens — the thing that broke it — are invisible. |
| No reasoning visibility | `message_end` from pi carries `usage.reasoning`; `chat()` discards it. We cannot tell "model thought and gave up" from "model produced nothing". |
| No execution trace | The only artifact is `research_plan_raw.md`, and it is written **only on success** — a failed call leaves nothing behind. |
| Opaque retries | 4 in-process retries with exponential backoff, all inside `chat()`, no record of attempts or of which attempt failed how. |
| Silent degradation | `fallback=True` lands in the YAML/markdown artifacts and one `(fallback)` suffix on a console line. Nothing fails, nothing alerts. |
| The odd one out | Stage 3b's search loop and Stage 4's writer both run on pi with traces, cost, and session ids. The planner — which runs *between* them — is the last bare call. |

Non-goals: no change to the plan schema, the parser, the fallback question
generator, the research prompt, the brief, or the writer. No new external
evidence (the planner explicitly must not search the web; that stays true).

---

## 2. Verified spike (2026-10-19) — evidence this design works

Run against the same ITMG Q1/Q2 inputs that failed on the bare call.

### 2.1 Harness-only planner run (transport validation)

`pi_agent.run_agent(system, user, cfg={model: glm-5.3, thinking: off, tools: [], timeout_sec: 600}, run_dir=…)`

| Case | Result |
|---|---|
| **ITMG Q1 2026** | exit 0, `agent_settled`, `finish_reason: stop`, **0 tool calls**, 44.1 s, **10,684 chars** of YAML, cost **$0.0216** (5,751 in / 3,089 out), session id recorded |
| **ITMG Q2 2026** | exit 0, `finish_reason: stop`, **0 tool calls**, 27.6 s, cost **$0.0254** (5,878 in / 3,906 out) |
| Raw `pi -p` equivalent (Q1) | 28.3 s wall, same content, `usage.cost.total` **$0.0208** |

Both runs parse cleanly through the **unchanged** `research_plan.parse_plan`:

```
ITMG Q1: error=None  fallback=False  themes=4  questions=13  hypotheses=5
ITMG Q2: error=None  fallback=False  themes=5  questions=17  hypotheses=8
```

Q1 theme names (for sanity — these are the themed plan the deterministic
fallback could never produce): *Gross Margin Drivers — Price, Mix, and Cost*;
*Tax Decrease — Restitusi, Audits, and One-off Credits*; *Earnings Quality and
Normalized Profitability*; *Forward Outlook and Price Sustainability*.

**Evidence boundary (be explicit):** the spike validates the *transport* — that
pi runs glm-5.3 reliably, returns the YAML, and exposes usage/cost/reasoning.
The spike ran with `tools: []`; the confirmed read-only tool surface is **not**
yet spike-validated for the planner role. The writer migration already proved
that glm-5.3 can drive `duckdb_query`/`notes_search` under pi (ADMR Q2 2026:
2 tool calls, zero errors, cost $0.1365), so the capability exists — but Phase 1
must repeat the one-shot capability probe for the *planner* prompt, because
"model can call tools" and "model calls them when the task is planning" are
different questions (the writer needed a nudge to use them at all, see
[[PI_WRITER_AGENT_PLAN]] §9 Phase 3).

### 2.2 What the trace gives us that `chat()` cannot

From the Q1 stream (`message_end`, assistant):

```json
{"input": 5751, "output": 3089, "cacheRead": 9, "cacheWrite": 0,
 "reasoning": 202, "totalTokens": 8849,
 "cost": {"input": 0.0080514, "output": 0.0135916, "total": 0.0216453}}
```

`usage.reasoning` is the field that would have made the truncation defect
self-diagnosing on day one. `pi_agent._build_result` currently sums
`usage.input`/`usage.output`/`cost.total` but **not** `usage.reasoning` — §4.1
adds it.

### 2.3 Fixtures captured for tests

- `tests/fixtures/pi_planner_preflight.jsonl` (~94 KB) — a real Q1 harness-only
  stream (`tools: []`) with the `thinking` block body trimmed. Replays through
  `pi_agent._build_result` with `error=None`, `rounds=0`, `settled=true`,
  `reasoning_tokens=202`, `cost_usd≈0.0216`, and a 10,684-char YAML plan
  (4 themes / 10 questions).
- `tests/fixtures/pi_planner_tools_preflight.jsonl` (~169 KB) — the Phase 1
  capability probe under the confirmed read-only allowlist: 2 `read` + 2
  `notes_search` calls, zero tool errors, 59.4 s, **$0.0367**, `reasoning_tokens=895`,
  and a 4-theme / 12-question plan.

Plan counts vary run to run (the model is sampled), so tests assert *structure*
(≥ 3 themes, ≥ 8 questions, well-formed IDs) rather than exact question counts;
the usage/cost figures are deterministic from the recorded stream. No network in
tests.

---

## 3. Architecture overview

```
facts/ratios → triggers → events → identity            (unchanged)
        │
        ▼
build_planner_context()  (unchanged prompt + unchanged YAML contract)
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│  PLANNER PI RUN  (new: research_plan.py)                     │
│   pi -p @user.md --mode json …                               │
│   tools: read, duckdb_query, notes_search, notes_fetch       │
│     (NO exa_search / fetch_content — Stage 3b owns evidence) │
│   harness: retries, timeout kill, usage/cost/reasoning,      │
│            JSONL trace, session id, tool budgets             │
└──────────────────────────────────────────────────────────────┘
        │  NDJSON → {content, usage, finish_reason, pi_meta}
        ▼
parse_plan(content)  (unchanged)  ── failure ──► _fallback_plan (unchanged)
        │
        ▼
plan artifacts + NEW planner_trace.json   (unchanged plan yaml/md)
```

### 3.1 pi invocation contract (planner)

```sh
pi -p @<run_dir>/user.md \
  --mode json \
  --no-session \                  # ephemeral by default; pi.save_session to flip
  --no-skills \
  --no-context-files \
  --no-mcp \
  --no-prompt-templates \
  --approve \
  --provider opencode-go \
  --model glm-5.3 \
  --thinking off \                # see §3.2 — "off" is the *thinking level*, not reasoning_effort
  -t read,duckdb_query,notes_search,notes_fetch \   # read-only allowlist (§3.3)
  --system-prompt <run_dir>/system.md
```

This is exactly the argv `pi_agent._build_argv` already emits for a non-empty
`tools` list, so **no changes to `pi_agent.py` argv construction are needed** —
only the `usage.reasoning` accumulation in §4.1.

### 3.2 Why `--thinking off` is correct here and `reasoning_effort: none` was not

These are different knobs and the confusion caused defect #1:

- `reasoning_effort: none` (OpenAI-compatible field) asks the **upstream
  provider** to disable thinking. `glm-5.3` is thinking-only and rejects it with
  HTTP 400.
- `--thinking off` (pi) sets pi's **thinking level**, which for a thinking-only
  model means "don't surface/request extended thinking", and pi handles the
  provider's constraints itself. The spike ran clean with it.

So the pi path structurally removes the class of failure that killed the bare
call: pi owns the provider dialect, and `opencode_client.chat` is no longer in
the planner's path. The `config.yaml` fix applied during this investigation
(removing `reasoning_effort.research_plan`, raising the timeout) remains correct
for the extraction/redflags calls, and `timeouts.research_plan` /
`max_tokens.research_plan` / `temperature.research_plan` are **deleted** with the
bare call (§4.4).

### 3.3 Tool surface — read-only, no web

The confirmed allowlist is the writer's read-only set:

| Tool | Why the planner gets it |
|---|---|
| `read` | Pull the full prior-quarter analysis, fact sheet, or ratios instead of the inline, 12,000-char-capped excerpt from `build_planner_context`. This is the main upgrade: the planner can verify period arithmetic (Q2 = H1 − Q1) against the actual prior document. |
| `duckdb_query` | Read-only SQL over `pipeline.duckdb` for this company's own prior periods (e.g. Q1 vs Q2 reality checks, multi-quarter trends) before framing questions. |
| `notes_search` / `notes_fetch` | Vault lookup for prior analyses / commodity notes that anchor a flagged movement. |

**Excluded: `exa_search`, `fetch_content`, and every mutating tool (`bash`,
`edit`, `write`).** The planner's contract is to formulate questions *before*
searching (`research_plan.py` docstring); external evidence stays owned by
Stage 3b, including its source-provenance audit. Giving the planner web tools
would collapse the plan/search separation and let un-audited URLs enter the
plan.

**Facts authority (prompt rule, mirrors the writer):** the canonical fact sheet
and computed ratios remain authoritative for the company's current period;
figures pulled via `duckdb_query`/`notes_fetch` are comparative context and must
be labelled as such. The planner must not overwrite a fact-sheet number with a
DB number.

**DB lock safety:** `research.run()` executes inside `pipeline._db_lock_released`
(`pipeline.py:257`), so the planner's `duckdb_query` calls do not contend with
the pipeline's own connection. Verified — no change needed.

**Residual risk:** with tools on, the planner can spend its budget exploring
instead of planning, and can mislabel a DB figure as a company fact. Mitigations
are (a) the prompt budget + directive (§4.2), (b) post-hoc `budget_warnings` in
`plan_trace.json`, and (c) the Stage 5 red-flag pass as a second reader. This is
the same trade the writer already made; the planner's failure mode is a weaker
plan, not a wrong number in the published document (the writer re-derives all
figures from the fact sheet).

---

## 4. Component changes

### 4.1 `pi_agent.py` — add reasoning tokens to usage

One-line extension of the existing `message_end` accumulator in
`_build_result`:

```python
completion_tokens += _as_int(usage.get("output"))
reasoning_tokens  += _as_int(usage.get("reasoning"))     # NEW
...
result["usage"] = {
    "prompt_tokens": prompt_tokens,
    "completion_tokens": completion_tokens,
    "reasoning_tokens": reasoning_tokens,                # NEW
    "cost_usd": cost_usd,
}
```

Rationale: this is the single field that makes truncation diagnosable. It is
additive — the research and writer traces gain the key, and
`tests/test_pi_research.py` asserts on specific keys, not an exact `usage` dict,
so it stays green (verify in Phase 1).

No argv, spawn, timeout, or parse changes.

### 4.2 `research_plan.py` — replace the transport

`formulate_plan` keeps its signature and its fallback semantics. The prompt text
is unchanged; the changes are the transport, the grounding sections appended to
the prompt, and the artifacts.

```python
def formulate_plan(ticker, year, quarter, events, *, identity=None,
                   deterministic_fallback=None, max_analysis_chars=None,
                   dry_run=False) -> ResearchPlan:
    ...
    pi_cfg = _resolve_planner_pi_cfg()               # NEW (tools: read-only set)
    user   = build_planner_context(...)              # unchanged
    user  += "\n\n" + _planner_artifacts_block(ticker, year, quarter)   # NEW
    system = _PLANNER_SYSTEM_PROMPT + "\n\n" + _planner_tools_section(pi_cfg)  # NEW

    run_dir = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.plan_pi"
    result  = pi_agent.run_agent(system, user, cfg=pi_cfg, run_dir=run_dir)

    raw = result.get("content") or ""
    _write_planner_trace(ticker, year, quarter, result, pi_cfg)   # NEW
    _write_raw_plan(ticker, year, quarter, raw, result)           # always, not just on success

    plan = parse_plan(raw, ticker, year, quarter, identity)
    if plan.error or not plan.questions:
        reason = plan.error or "planner produced no questions"
        if result.get("finish_reason") in {"length", "max_tokens"}:
            reason = f"planner response truncated at max_tokens; {reason}"
        if result.get("error"):
            reason = f"planner agent failed: {result['error']}"
        return _fallback_plan(..., reason=reason)
    return plan
```

Details:

| Item | Change |
|---|---|
| `_resolve_planner_pi_cfg()` | New, mirrors `write._resolve_pi_cfg`: reads `analysis.plan.pi`; `provider` → `opencode-go`; `model` → falls back to `models.research_plan` (`glm-5.3`); `timeout_sec` → 600; `tools` → the read-only set (explicit `[]` downgrades to `--no-tools`); `thinking` → `"off"`. |
| `_planner_tools_section(pi_cfg)` | New, mirrors `write._tools_section`: a bounded markdown block appended to the system prompt naming the four tools, the **facts-authority rule**, and the prompt budget ("at most N `duckdb_query`, M `notes_search`, K `notes_fetch` calls — then output the plan"). Planner-specific directive: use the tools to *verify period arithmetic and prior-period baselines before framing questions*, not to discover new external facts. |
| `_planner_artifacts_block(ticker, year, quarter)` | New, mirrors `write._artifacts_block`: absolute on-disk paths for the fact sheet, ratios, company profile, and prior analyses so `read` has targets. |
| `_write_planner_trace()` | New. Writes `{T}_{Q}_{Y}.plan_trace.json` with `pi_meta` (session id, retries, duration, `tool_calls`, `tool_errors`, cost), `usage` (incl. reasoning), `finish_reason`, `error`, `budget_warnings`, and the resolved model. Mirrors `write_trace.json`. |
| `_planner_budget_warnings()` | New. Post-hoc comparison of `pi_meta.tool_calls` against `max_db_queries` / `max_notes_search` / `max_notes_fetch`, same shape as `write._budget_warnings`. |
| `_write_raw_plan()` | Replaces the inline block; now writes **even when the call failed** (defect #3 left no artifact). Header records `finish_reason`, `prompt_tokens`, `completion_tokens`, **`reasoning_tokens`**, `cost_usd`, `error`. |
| Fallback reason strings | Now include the agent error and reasoning spend, so `research_plan.yaml`'s `themes[0].rationale` says *why* (e.g. "truncated at max_tokens after 16,384 reasoning tokens") instead of a bare HTTP string. |
| `dry_run` | New kwarg: build the prompt, print `[DRY] would plan with glm-5.3 (user_chars=…)`, spawn nothing. `research.run` already has a `dry_run` path to thread it through. |
| Imports | Add `pi_agent`. Keep `opencode_client` (still used by `load_analysis_config`). |

### 4.3 `research.py` — thread the dry-run flag

`_run_pi_agent` already receives `tool_mode`; pass `dry_run` from `run()` into
`research_plan.formulate_plan(..., dry_run=dry_run)`. In dry-run, `run()` returns
before planning today (`if dry_run: return {"status": "dry", ...}` at
`research.py:500`), so this is only needed for a future planner-only CLI — mark
it optional.

### 4.4 `config.yaml` migration sketch

```yaml
analysis:
  models:
    research_plan: glm-5.3        # becomes the pi --model
  # ── Research planner (Stage 3b, step 1) ─────────────────────────────
  # Headless pi agent with the read-only idx-tools (no web tools — external
  # evidence stays with Stage 3b). See PI_PLANNER_AGENT_PLAN.md.
  plan:
    pi:
      provider: opencode-go
      # model omitted → falls back to models.research_plan (glm-5.3)
      thinking: "off"
      timeout_sec: 600
      approve: true
      load_skills: false
      load_context_files: false
      save_session: true          # persist for `pi --resume` debugging
      tools: [read, duckdb_query, notes_search, notes_fetch]  # [] → --no-tools
      max_db_queries: 4           # prompt-enforced budgets (post-hoc audited)
      max_notes_search: 3
      max_notes_fetch: 3
```

**Deleted with the bare call:** `timeouts.research_plan`, `max_tokens.research_plan`,
`temperature.research_plan`, and the `reasoning_effort.research_plan` comment
block added during this investigation. pi owns call params; there is no non-pi
planner path left to configure.

`models.research_plan` stays (it is the model identity, and the fallback source
for `plan.pi.model`).

`max_planner_analysis_chars` (currently under `research`) stays in place and
still bounds the *inline* prior-analysis excerpt; the `read` tool is what now
lets the planner go beyond it.

### 4.5 `db.py` / `pipeline.py` — cost and planner visibility

The planner currently contributes **nothing** to `analysis_jobs` — its tokens
are not even added (`research.py` returns `token_usage` for the *search* agent
only). Two additive changes:

1. `research.py` returns `plan_usage` and `plan_pi_meta` alongside the existing
   `token_usage`; `run()`'s trace JSON gains a `planner` block.
2. `analysis_jobs` gains `cost_usd DOUBLE` and `model_plan VARCHAR` (via the
   existing `ALTER TABLE … ADD COLUMN IF NOT EXISTS` pattern already used for
   `stage_research`), plus `add_cost()` / `set_models(plan=…)`. `print_status()`
   gains a `$` column.

This is the "budget review" half of the operator request: one dashboard row
showing what the planner cost and whether it fell back.

**Minimal alternative** (if the schema change is unwanted in v1): leave the DB
alone and rely on `plan_trace.json`. Recommend the schema change — it is 3
columns and makes the planner comparable to the other stages.

### 4.6 CLI — planner-only refresh

`research.py`'s `__main__` gains `--plan-only` (stop after the plan, write the
plan artifacts, skip the search agent). Useful for iterating on the planner
prompt without paying for a full research run:

```bash
uv run scripts/analysis_pipeline/research.py --ticker ITMG --year 2026 \
  --quarter Q1 --plan-only --force
```

### 4.7 Prompt changes

The **task prompt** (`build_planner_context`) is unchanged — the spike used it
verbatim and produced a valid, well-themed plan. Two additive blocks, both
generated by `research_plan.py` (never hand-edited into the prompt):

1. `_planner_tools_section(pi_cfg)` appended to the **system** prompt —
   describes the four read-only tools, the facts-authority rule (fact sheet wins
   for the current period), and the tool budget, with a directive to verify
   period arithmetic and prior-period baselines *before* framing questions. It
   is the planner analogue of `write._tools_section`.
2. `_planner_artifacts_block(...)` appended to the **user** prompt — absolute
   on-disk paths for the fact sheet, ratios, company profile, and prior analyses
   so `read` has targets, mirroring `write._artifacts_block`.

Optional tightening (decide in Phase 2): the system prompt's "Do not expose
reasoning or schema examples" line is now redundant under pi, because reasoning
arrives as a separate `thinking` block and never enters `content`. Dropping it
avoids confusing the model about whether to think at all.

---

## 5. Tool policy and guardrails

| Layer | Control |
|---|---|
| Declaration | `-t read,duckdb_query,notes_search,notes_fetch`; `bash`/`edit`/`write` and all web tools are undeclared and invisible |
| Filesystem | No `write`/`edit`/`bash` ⇒ the planner cannot modify the repo, the plan artifacts, or the DB |
| Evidence ownership | `exa_search`/`fetch_content` are not in the allowlist; the plan stays pre-search and un-audited URLs cannot enter it |
| Facts authority | Prompt rule: the fact sheet + ratios win for the company's current period; DB/vault figures are comparative context and must be labelled |
| Budget | Prompt-stated caps (4 `duckdb_query`, 3 `notes_search`, 3 `notes_fetch`) + post-hoc counts in `plan_trace.json` (`budget_warnings`) |
| Audit | `plan_trace.json` records every tool call (name, args, result), plus `pi_meta`, usage/cost/reasoning, retries, and the session id for `pi --resume` |
| Fallback | Deterministic questions remain the safety net, now traced and surfaced in `--status` |

Residual risk and mitigation: with tools on, a planner can mislabel a DB figure
as a company fact, or burn its budget exploring. Mitigations are (a) the prompt
hierarchy and bounded budget above, (b) `budget_warnings` in the trace, and
(c) the fact that the published document's figures are re-derived by the writer
from the fact sheet — a mislabelled planner figure can bias *which questions are
asked*, not the reported numbers.

---

## 6. Budgets, cost, and failure posture

- **Measured cost:** $0.021–$0.025 per plan (ITMG Q1/Q2). For comparison on the
  same reports: Stage 3b's research run cost $0.025–$0.031 in model tokens plus
  $0.147–$0.154 in Exa calls (~$0.17–$0.19 total), and the Stage 4 writer cost
  $0.127–$0.130. So the planner is **~12–14 % of a research run and ~1/6 of a
  writer run** — cheap relative to the stages it feeds; failing silently is what
  was expensive.
- **Latency:** 28–44 s observed **with no tool calls**; budget up to
  4+3+3 tool rounds on top (the writer's tool-enabled run took 300 s including
  2 tool calls, so the planner should land well under 5 minutes). `timeout_sec:
  600` is the hard stop.
- **Tool budget:** 4 `duckdb_query` + 3 `notes_search` + 3 `notes_fetch`,
  prompt-enforced and post-hoc audited into `budget_warnings` (same mechanism as
  the writer). Tune in Phase 4 based on actual usage.
- **Failure posture — confirmed: best-effort, but loud.** The planner is an
  enhancement, not a correctness gate; the ITMG run shipped usable analyses on
  the deterministic fallback. So: on failure, still fall back, but (a) write
  `plan_trace.json` with the error, (b) include the reason and reasoning spend in
  `research_plan.yaml`, (c) mark it in `print_status()`, and (d) print a single
  `[PLAN] FALLBACK: <reason>` line. (A hard failure was considered and rejected:
  it would block writing for a non-correctness issue.)
- **Retries:** pi's `auto_retry_start` events are counted into
  `pi_meta.retries`, replacing `chat()`'s invisible 4× backoff.

---

## 7. Tests

`tests/test_pi_planner.py` (new, no network, spawn faked — mirrors
`test_pi_writer.py`):

- **Fixture replay (harness-only capture):**
  `tests/fixtures/pi_planner_preflight.jsonl` (captured from the `tools: []`
  spike) → `pi_agent._build_result` → `content` non-empty, `error is None`,
  `tool_rounds == 0`, `pi_meta.settled is True`, `usage.cost_usd ≈ 0.0216`,
  `usage.reasoning_tokens == 202`. This fixture validates the **parser and
  usage accounting**, not the tools path.
- **Tools fixture (captured in Phase 1, the capability probe):**
  `tests/fixtures/pi_planner_tools_preflight.jsonl` — a real planner run under
  the confirmed allowlist that makes at least one `read` and one
  `duckdb_query`/`notes_search` call, then emits the YAML plan. Drives the same
  `tool_trace` correlation assertions the writer fixture does.
- **Parse through the unchanged parser:** the fixture's content →
  `research_plan.parse_plan` → `error is None`, `fallback is False`,
  `len(themes) == 4`, `len(questions) == 13`, IDs match `^[A-Z]\d+$`.
- **`formulate_plan` wiring** (runner mocked):
  - success ⇒ `plan.fallback is False`, `plan_trace.json` written with
    `pi_meta`/`usage`/`finish_reason`, `plan_pi/{system,user}.md` exist,
    `research_plan_raw.md` written;
  - runner error ⇒ `plan.fallback is True`, `plan.error` contains the agent
    error, **and the trace + raw artifact are still written** (the defect #3
    regression);
  - `finish_reason: length` ⇒ fallback reason mentions truncation;
  - `tools` reaches the argv as
    `-t read,duckdb_query,notes_search,notes_fetch`; an explicit `tools: []`
    emits `--no-tools` instead;
  - `model` unset in `plan.pi` ⇒ argv carries `models.research_plan`;
  - `dry_run` ⇒ no spawn.
- **Grounding prompt:** `_planner_tools_section` appears in
  `plan_pi/system.md`; `_planner_artifacts_block` appears in
  `plan_pi/user.md`; the facts-authority rule and the budget numbers are present.
- **Budget audit:** a faked `pi_meta.tool_calls` over the configured caps ⇒
  `budget_warnings` non-empty in `plan_trace.json`; within caps ⇒ empty.
- **`pi_agent` usage extension:** `reasoning_tokens` present and summed across
  multiple assistant messages; existing `test_pi_research.py` /
  `test_pi_writer.py` assertions stay green (they assert individual keys).
- **`db` additive schema:** `cost_usd`/`model_plan` added to an existing DB
  without error; `add_cost` accumulates.

`tests/test_writer_context.py` and `test_xlsx_ingestion.py` must stay green
untouched.

---

## 8. Documentation updates

- `scripts/analysis_pipeline/README.md` — Stage 3b section: the plan step is now
  a **headless pi agent** with the read-only idx-tools (`read`, `duckdb_query`,
  `notes_search`, `notes_fetch`), `glm-5.3`, with `plan_trace.json`; the
  artifacts tree gains `research/{T}_{Q}_{Y}.plan_pi/` + `.plan_trace.json`.
- Root `README.md` — Stage 3b description ("research plan (`research_plan.py`)"
  → "research plan via pi agent, read-only tools, no web access").
- `.pi/APPEND_SYSTEM.md` — note that the planner also runs on pi with the
  read-only idx-tools (and no web tools).
- This plan: status flips to *implemented* after Phase 4.

---

## 9. Phased rollout

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **1 Runner + probe** | `pi_agent.py` reasoning-token accumulation; one-shot **capability probe** with the read-only allowlist; capture `pi_planner_tools_preflight.jsonl`; `tests/test_pi_planner.py` fixture-replay cases | Probe: `read`/`duckdb_query`/`notes_search` driven by glm-5.3 under the planner prompt, zero errors, valid YAML. Fixtures replay with `cost_usd≈0.0216`, `reasoning_tokens=202`; all existing tests green |
| **2 Re-wiring** | `research_plan.py` → `pi_agent.run_agent`; `plan.pi` config; `_planner_tools_section` + `_planner_artifacts_block`; trace + always-write raw artifact; delete planner keys from `config.yaml` | Plan for ITMG Q1 via the real path (tools present, ≥3 themes, ≥8 questions); zero `opencode_client.chat` calls remain in `research_plan.py`; fallback path still reachable and now writes artifacts |
| **3 Visibility** | `db.py` cost/model columns; `print_status()` cost column; `plan_usage`/`plan_pi_meta` plumbed through `research.py`; `[PLAN] FALLBACK:` line | Status dashboard shows planner cost + fallback for a forced-failure run |
| **4 Live validation** | `--force` on ITMG Q1 + Q2 2026 (the two tickers that exposed the defects), then one unrelated ticker (e.g. ADRO Q1) | All three: `fallback=False`, themed plan, trace + session id + `tool_calls` present, cost recorded, `budget_warnings` empty; documents regenerate unchanged in structure |
| **5 Docs / cleanup** | READMEs, APPEND_SYSTEM, remove dead planner config keys and any now-unused imports | No stale references to `timeouts.research_plan` / `max_tokens.research_plan`; `--list-models` path untouched |
| **6 Soak** | Batch run over the pending tickers in the status table | Planner fallback rate ≈ 0 on non-outage runs; cost within §6 bounds; tool calls within budget; no pi crashes (`~/.pi/agent/crashes.json` unchanged) |

### Phase 0 evidence (already gathered, 2026-10-19)

- Bare call: **3 distinct failure modes** reproduced (HTTP 400 on
  `reasoning_effort: none`; read timeout at 120 s; empty content at
  `finish_reason: length` with 16,384 completion tokens).
- pi harness-only: **2/2 success**, 28–44 s, $0.021–$0.025, valid YAML, 13/17
  questions, 4/5 themes, 5/8 hypotheses.
- Fixture captured and replayed through the existing parser.

---

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| pi missing / not on PATH | Same posture as research/writer: error result → deterministic fallback with a loud reason (best-effort confirmed) |
| pi session/config drift changes planner behaviour | `--no-skills`, `--no-context-files`, `--no-mcp`, `--no-prompt-templates`, `--approve` — same hermetic contract as the other two stages; prompt files captured per run |
| Thinking-only model burns the timeout again | 600 s whole-run cap + `reasoning_tokens` recorded per run; a repeat failure is now diagnosable from the trace instead of guesswork |
| **Planner over-explores and stalls** | Bounded, prompt-stated tool budget (4/3/3) + post-hoc `budget_warnings`; the writer showed the model uses tools sparingly (2 calls) when given a budget |
| **Planner mislabels a DB/vault figure as a company fact** | Facts-authority prompt rule; the writer re-derives every published figure from the fact sheet, so a mislabel can bias question selection, not the reported numbers; Stage 5 red-flags is a second reader |
| **`duckdb_query` contends with the pipeline's DB lock** | `research.run()` already executes inside `pipeline._db_lock_released` (`pipeline.py:257`); verified, no change needed |
| Plan quality regresses vs the bare call | Phase 4 compares against the ITMG baseline and the deterministic fallback; the parser, fallback, and question-count discipline are unchanged |
| Added `plan.pi` block confuses the config surface | `models.research_plan` remains the single model identity; the pi block only carries harness knobs, exactly like `write.pi` |
| Schema change on a live DB | Additive `ADD COLUMN IF NOT EXISTS`, the pattern already in `init_schema`; no migration script needed |

---

## 11. Decisions and remaining questions

**Resolved 2026-10-19:**

1. **Rollout shape — replace outright.** No `direct_api` toggle; the bare call
   and its config keys are deleted in the same change set.
2. **Failure posture — best-effort, but loud.** Fall back to deterministic
   questions on failure, with trace + artifact + status visibility.
3. **Tools — read-only set.** `read`, `duckdb_query`, `notes_search`,
   `notes_fetch`; no web tools.
4. **Session persistence — `save_session: true`**, matching the writer, so a
   failed plan can be inspected with `pi --resume`.
5. **DB schema — add `cost_usd` / `model_plan`** to `analysis_jobs` and a cost
   column to `print_status()`.

**Still open (decide during implementation):**

1. **Tool budgets.** Start at 4 `duckdb_query` / 3 `notes_search` / 3
   `notes_fetch`. Tune in Phase 4 from actual `tool_calls`.
2. **Fallback hard-stop threshold.** Should a *repeated* planner fallback (e.g.
   N consecutive runs across the batch) raise an operator alert, or is the
   dashboard marker enough? *Default: dashboard marker only for v1.*
3. **Prompt tightening.** The system prompt's "Do not expose reasoning" line is
   now redundant under pi (reasoning is a separate `thinking` block). Decide by
   A/B on one ticker in Phase 2.
4. **`--plan-only` CLI.** Confirm the dedicated planner-only refresh flag is
   wanted (it makes prompt iteration cheap). *Default: yes.*

---

## 12. Success metrics

- **Planner success rate** ≥ 95 % of non-outage runs produce `fallback=False`
  (today: 0 % on the ITMG run).
- **Fallback visibility:** 100 % of fallbacks carry a reason in
  `research_plan.yaml`, a `plan_trace.json`, and a status-dashboard marker.
- **Cost:** ≤ $0.035 per plan with tools (spike, no tools: $0.021–$0.025);
  planner ≤ 15 % of per-report model spend.
- **Latency:** p95 ≤ 180 s with tools (spike, no tools: 28–44 s).
- **Tool discipline:** 0 tool errors; `tool_calls` within the configured budget
  on ≥ 90 % of runs; `budget_warnings` empty.
- **No silent failures:** every planner run leaves `plan_pi/{system,user}.md`,
  `events.jsonl`, `plan_trace.json`, and `research_plan_raw.md` — including
  failures.
- **No regression:** plan schema, parser, fallback generator, research prompt,
  brief, and writer unchanged; ITMG Q1/Q2 and one control ticker regenerate with
  ≥ 3 themes and ≥ 8 questions.
