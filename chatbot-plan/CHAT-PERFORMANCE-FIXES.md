# Chat Performance Fixes — Points 2–7

**Status:** implemented · **Date:** 2026-10-28 · **Owner:** Hannan
**Scope:** `chat-app/` request latency and output cost. Points 2–7 of the
performance review. Point 1 (streaming) and point 8 (tool precision) are
explicitly out of scope.
**Related:** [`MVP-PLAN.md`](./MVP-PLAN.md), [`README.md`](./README.md)

---

## 0. Executive summary

The chat-app was measured at **70–152s** for the same question the plain `pi`
session answered in **27s**. The gap was not the tool layer:

- tools were only **3–8%** of wall time;
- the final answer turn was **39s** of a 70s no-audit run;
- the chat-app produced **5,294 output + 3,013 reasoning tokens** on that turn,
  versus pi’s **1,144 + 259**;
- **36%** of stored runs retried, and retries roughly doubled wall time;
- `list_companies()` cost **1.7–2.3s** per desk request;
- high thinking added **~57s** to a simple lookup with no answer gain.

This work implements six fixes:

1. compact claims (`sentence: N`) to cut envelope duplication;
2. an opt-in two-phase prose → claims path;
3. retry prompts that reuse prior evidence;
4. a fast path for simple single-name questions;
5. cached preflight/completeness data;
6. thinking-level routing.

The current default ITMG question now answers in **32.28s** with **2 tool calls**,
**9,210 prompt / 2,882 completion tokens**, and **$0.00315**, passing the audit
with **67/67 numbers** and **13/13 citations**. A compact-claim variant reached
**23.67s**, but single-pass output now prefers `text:` for reliability.

Two-phase is implemented and measured but remains **OFF** by default: it was
**+31% wall clock** in the live A/B. ADMR still refuses because the excluded
point 8 (`documents_status` item precision / gap handling) is unresolved.

---

## 1. Baseline and diagnosis

### 1.1 Same ITMG question, different runtimes

Question: `Show me the highlight of ITMG operation in Q2 2026`

| Setup | Wall | Model turns | Tool calls | Output tokens | Reasoning | Cost |
|---|---:|---:|---:|---:|---:|---:|
| Plain `pi` session, high thinking | **27.0s** | 4 | 6 | 1,842 | 486 | $0.0069 |
| Chat-app, high thinking, audit bypassed | 70.1s | 5 | 7 | 6,259 | 3,387 | $0.0084 |
| Chat-app, high thinking, audit on | 152.6s | 10 | 14 | 16,541 | 11,057 | $0.0196 |
| Chat-app, current low/off, audit on | 95.8s | 8 | 10 | 7,354 | ~2,901 | $0.0113 |

### 1.2 Where the 70s went

| Phase | Chat-app no audit | Pi | Delta |
|---|---:|---:|---:|
| Preflight / startup | ~2.3s | ~0s | +2.3s |
| Tool-gathering model turns | 26.2s (4 turns) | 15.2s (3 turns) | +11.0s |
| Tool execution | 2.5s | 1.9s | +0.6s |
| **Final answer turn** | **39.0s** | **10.2s** | **+28.9s** |
| Total | **70.1s** | **27.0s** | **+43.1s** |

The final turn was 67% of the gap. The visible answers were similar in length
(2,358 vs 2,754 chars); the chat-app envelope was **7,402 chars**, because
`claims[]` repeated the answer sentence by sentence.

### 1.3 Root causes

1. **Envelope duplication** — every number/causal sentence repeated as a claim.
2. **Retry amplification** — a failed audit reran the entire pi agent.
3. **Over-fetching on simple questions** — note tools and `period_derive` used
   when the report and fact sheet were sufficient.
4. **Preflight cost** — `operational.completeness()` re-parsed YAML repeatedly.
5. **Thinking not routed** — simple lookups paid the high-thinking penalty.

---

## 2. Implemented fixes

### 2.1 Point 2 — Compact envelope claims

**Rationale.** The final answer turn was dominated by `claims[].text` repeating
the answer. A compact claim can reference the answer sentence by index.

**Implementation.**

- `Claim` now accepts either:
  - `text: "..."` (existing form), or
  - `sentence: N` — 1-based index into `split_sentences(answer)`.
- `AnswerEnvelope` resolves `sentence` to the full sentence before any audit, so
  number, citation, label, missing-state and coverage checks are unchanged.
- `split_sentences()` is shared with `verify.py`; audit and index cannot disagree.
- **Fail-safe:** if an index is stale but `text` is present, `text` wins and the
  index is discarded. If no text is present, the claim is dropped and the
  deterministic audit flags any uncovered number or causal sentence. A bad index
  no longer causes `envelope_invalid`.
- Single-pass prompts now prefer `text:`; `sentence:` is used by the two-phase
  claims pass, where the model receives an explicit numbered sentence list.

**Why single-pass prefers `text:`.** The first ADMR run used `sentence: 10` when
the answer had 8 sentences. That produced `envelope_invalid` and a retry. The
fail-safe and prompt preference remove that failure mode at the cost of some
output tokens.

**Config:** no flag; compact support is always available.

**Files:** `chat-app/chat_app/envelope.py`,
`chat-app/chat_app/verify.py`, `chat-app/chat_app/prompts/chat_system.md`,
`desk_system.md`, `document_system.md`.

**Tests:** compact index resolution, stale index with text, stale index without
text (dropped), zero index, claims-only parser, audit of compact claims.

---

### 2.2 Point 3 — Two-phase prose → claims (opt-in)

**Rationale.** Phase 1 writes the answer as prose with tools; phase 2 attaches
claims to that prose without tools. This removes the need for the answer pass to
reason about YAML and citations while writing.

**Implementation.**

- `_ask_two_phase()` in `agent.py`.
- **Phase 1:** normal system/user prompt plus `## Prose-only pass`; tools allowed;
  returns Markdown only. If it errors, returns empty, or returns a full envelope,
  the path falls back to single-pass.
- **Phase 2:** receives the question, phase-1 prose, a numbered sentence list, and
  a bounded tool-evidence block. `tools=[]`, `thinking` from
  `two_phase.claims_thinking` (default `off`). Returns only
  `claims`/`missing`/`coverage`.
- `parse_claims_envelope()` merges the phase-1 prose as `answer` and validates the
  result. The existing audit then runs. Deterministic repair, one phase-2 retry,
  then fallback to single-pass.
- `result["two_phase"]` / `trace["two_phase"]` record
  `{enabled, used, fallback, claims, phase1_chars, phase2_attempts,
  phase2_violations}`.
- Supported for desk and document modes; only desk was measured live.

**Config.**

```yaml
chat:
  two_phase:
    enabled: false          # committed default OFF
    max_evidence_chars: 20000
    claims_thinking: "off"
    claims_model: null       # null => same model as the answer pass
```

**Live A/B (same fast-path ITMG question):**

| Run | Wall | Completion | Cost | Claims | Answer chars |
|---|---:|---:|---:|---:|---:|
| Single-pass | 26.3s | 4,201 | $0.004552 | 13 | 1,515 |
| Two-phase | 34.5s | 3,454 | $0.004449 | 13 | 1,315 |

Two-phase was **+31% wall clock**, +8% tokens, −2% cost. Default remains OFF
until a broader eval shows a quality/audit-pass gain.

**Files:** `chat-app/chat_app/agent.py`, `chat-app/chat_app/envelope.py`,
`config.yaml`, tests.

---

### 2.3 Point 4 — Retry reuse, not retry from scratch

**Rationale.** Retries were full agent runs. The model did not see the prior
envelope or tool results, so it re-fetched everything. Retry cost was the main
tail latency.

**Implementation.**

- `_retry_context()` builds a bounded block:
  - `## Evidence already collected` — non-context, non-synthetic tool entries
    from the most recent attempt; tool name, arguments, result text.
  - `## Previous answer (rejected)` — previous envelope YAML, or raw output for a
    parse failure.
  - `## Correction` — existing repair hint / YAML error.
- Caps: **24,000 chars total**, **4,500 per tool result**, **800 per argument
  list**, **8,000 previous answer/output**.
- Tool-trace entries are tagged with `attempt`; context/synthetic entries are
  skipped. Oversized entries are truncated with an omission note.
- Both parse-failure and verification-failure retries use the new context.
- One-retry limit, refusal behavior, and audit semantics are unchanged.

**Files:** `chat-app/chat_app/agent.py`, `chat-app/tests/test_agent.py`.

**Tests:** prior evidence in prompt, previous envelope present, bounded prompt,
parse-failure retry, attempt tagging, most-recent-attempt selection.

---

### 2.4 Point 5 — Fast path for simple single-name questions

**Rationale.** The chat-app was using note tools and `period_derive` for
questions that needed only a report and a fact sheet. Pi answered the same
question with 2–3 tools.

**Implementation.**

- `_route_request()` in `agent.py` returns:
  - `fast_path`
  - `thinking`
  - `tools`
  - `prompt_suffix`
  - `reason`
- Fast path applies only when **all** hold:
  - desk mode;
  - a supported ticker is named or supplied as focus;
  - at most one supported ticker named;
  - no comparison markers (`compare`, `versus`, `vs`, `across`, `peer`, `sector`,
    `rank`, `best`, `top`, `matrix`, `screen`);
  - no note/analysis markers (`note`, `analysis`, `thesis`, `outlook`, `risk`,
    `why`, `explain`, `analyse`, `analyze`, `scenario`, `catalyst`).
- Fast tools: `operational_lookup`, `company_facts`, `read_document`,
  `documents_status`.
- `period_derive` is added only for explicit single-quarter/derived questions
  (`single quarter`, `qoq`, `quarter-on-quarter`, `derive`, `standalone`).
- Prompt suffix instructs: batch independent calls, max 4 tool calls, no
  `period_derive` unless asked, no note tools unless the question is about the
  note, and answer with available financial highlights + a stated limitation
  rather than refusing.
- `route` is recorded on the result and trace.

**Config.**

```yaml
chat:
  fast_path:
    enabled: true
    max_tool_calls: 4
```

**Files:** `chat-app/chat_app/agent.py`, `config.yaml`,
`chat-app/tests/test_agent.py`.

**Tests:** simple ITMG fast path, derived question includes `period_derive`,
comparison stays full surface + high thinking, two-ticker guard, note question
not fast, document mode not fast, explicit mode thinking override.

---

### 2.5 Point 6 — Cache the preflight

**Rationale.** `list_companies()` measured **1.7–2.3s** warm because it called
`operational.completeness()` for every ticker and repeatedly parsed YAML. It ran
on every desk question.

**Implementation.**

- `paths.file_versions(files)` returns sorted `(path, mtime_ns, size)` tuples.
- `tools.list_companies()` cache:
  - keyed by day + file signature of facts, operational artifacts, analysis
    notes, raw filings, preprocessed files, notes DB, metrics YAML;
  - returns `deepcopy` so callers cannot poison the cache;
  - `clear_companies_cache()` for tests/manual refresh.
- `operational.completeness()` cache:
  - keyed by `(ticker, period, year, day)` + signature of the exact inputs it
    reads (fact sheet, operational JSON, manifest, filing, notes DB, metrics,
    matching analysis notes);
  - deep-copied; `operational_block=...` bypasses the cache for enrichment.
- `analysis_docs._index()` already cached per file by path + mtime_ns + size; a
  regression test confirms warm calls do not re-parse and edits invalidate.
- No TTL: a changed signature forces recomputation, so stale data is not served.

**Measured.**

```text
list_companies cold: 2.27s
list_companies warm: 6.8ms
operational.completeness warm: ~0ms (from ~49ms)
signature cost: ~5.6ms
```

**Files:** `chat-app/chat_app/paths.py`, `tools.py`, `operational.py`,
`chat-app/tests/test_tools.py`, `test_operational.py`, `test_analysis_docs.py`.

---

### 2.6 Point 7 — Route thinking levels

**Rationale.** High thinking added **~57s** to a simple lookup with no observed
answer gain. It should be reserved for comparison/analysis questions.

**Implementation.**

- `_resolve_thinking()` precedence:
  1. explicit `mode.thinking`;
  2. explicit `chat.pi.thinking`;
  3. fast path → `chat.thinking.default` (`off`);
  4. complex markers → `chat.thinking.complex` (`high`);
  5. fallback `off`.
- `chat.pi.thinking` was removed from `config.yaml` so the router can decide;
  an operator can re-add it to force a level.
- `route["thinking"]` and `route["tools"]` are rewritten after `_pi_config` so
  the trace reports the actual level/tool set used.

**Config.**

```yaml
chat:
  thinking:
    default: "off"
    complex: "high"
    complex_markers: [compare, versus, vs, across, sector, peer, why, explain,
                      analyse, analyze, thesis, outlook, risk, scenario, catalyst]
```

**Example decisions:**

| Question | Fast | Thinking |
|---|---|---|
| ITMG Q2 operation highlight | ✅ | off |
| Compare ITMG and PTBA Q2 production | ❌ | high |
| ITMG outlook and key risks | ❌ | high |
| Derive ITMG Q2 QoQ production | ✅ | off (+`period_derive`) |

**Files:** `chat-app/chat_app/agent.py`, `config.yaml`,
`chat-app/tests/test_agent.py`.

---

## 3. Final results

### 3.1 ITMG — default after all fixes

Trace: `idx_data/chat/traces/20261028T081554Z_64614ba0.json`

| Metric | Value |
|---|---:|
| Status | **answered** |
| Wall time | **32.28s** |
| Route | fast path, thinking off |
| Tool calls | **2** (`operational_lookup`, `company_facts`) |
| Prompt tokens | 9,210 |
| Completion tokens | 2,882 |
| Cost | **$0.003154** |
| Claims | 12 |
| Numbers | 67/67 passed |
| Citations | 13/13 matched |
| Violations | none |

**Compact-claim variant** (same question, before single-pass switched to `text:`):

Trace: `idx_data/chat/traces/20261028T081020Z_73a222cd.json`

```text
23.67s · 2 tools · 9,220 prompt · 2,513 completion · $0.002934
11 claims · 64/64 numbers · 17/17 citations · answered
```

It was faster, but the stale-index failure on ADMR showed the compact form is
unsafe as a single-pass default for longer answers. The capability remains for
the two-phase numbered-list pass.

### 3.2 Cache result

```text
Before: list_companies() ~1.7–2.3s per desk request
After:  ~6.8ms warm
```

### 3.3 Two-phase result

Implemented, tested, default OFF. Live A/B: **26.3s single-pass vs 34.5s
two-phase**. The second round trip outweighs the smaller claims output on the
current model. Keep it as an opt-in for evals.

### 3.4 ADMR — still refused (point 8 not in scope)

Trace: `idx_data/chat/traces/20261028T081447Z_bbca4318.json`

```text
Status: refused
Wall:   59.88s
Route:  fast path, thinking off
Tools:  8
Tokens: 33,320 prompt / 6,746 completion
Cost:   $0.009294
Numbers: 59/59 passed
```

Remaining violations:

- `documents_status` citations omitted the `item` field, so they did not match
  the trace call.
- `documents_status` returned `available` for keyword hits on pages 59 and 113
  that were a disclaimer and a coal-pricing regulation, while the model marked
  `coal_production` / `sales_volume` as `not_disclosed` →
  `missing_contradicts_documents_status`.

This is the excluded point 8: `documents_status` item precision and
`company_facts.completeness.gaps` acceptance. The compact-index regression is
fixed; ADMR no longer fails with `envelope_invalid`.

---

## 4. Tests and repository state

Targeted suites:

```text
test_envelope + test_verify + test_agent: 108 passed
```

Full chat-app suite:

```text
298 passed, 3 failed
```

The 3 failures are **data-snapshot failures**, not code regressions:

```text
test_supported_set_snapshot_matches_disk
  disk has 39 periods, frozen manifest has 38

test_build_snapshot_reproduces_the_frozen_manifest
  39 != 38

test_filing_registry_uses_fact_sheet_and_raw_pdf
  MAPA now has Q3 2026 in addition to Q1/Q2
```

MAPA Q3 2026 fact sheets and `raw/MAPA_Q3_2026.pdf` were generated at 15:16 on
2026-10-28, after the earlier 300-pass run. Refreshing
`chat-app/eval/supported-set.json` and the MAPA filing expectation will restore
a green suite. No implementation code caused these failures.

---

## 5. Configuration reference

```yaml
chat:
  fast_path:
    enabled: true
    max_tool_calls: 4

  thinking:
    default: "off"
    complex: "high"
    complex_markers: [compare, versus, vs, across, sector, peer, why, explain,
                      analyse, analyze, thesis, outlook, risk, scenario, catalyst]

  two_phase:
    enabled: false
    max_evidence_chars: 20000
    claims_thinking: "off"
    claims_model: null
```

Operational notes:

- `two_phase.enabled: true` enables the prose → claims path. Expect roughly
  +30% wall clock on simple questions until a prompt/model change removes the
  second-round-trip cost.
- `fast_path.enabled: false` restores the full desk tool surface for every
  question; thinking routing still applies.
- Re-adding `chat.pi.thinking` or `modes.*.thinking` forces that level and
  disables router decisions.

---

## 6. Known gaps and follow-ups

1. **Point 8 — tool precision (ADMR).** Make `documents_status` item-aware:
   return `available` only when the page contains usable data, not a keyword hit;
   accept `company_facts.completeness.gaps` as evidence for `not_captured`
   items; allow `not_disclosed` when the filing is captured but the operational
   item is absent.
2. **Refresh the eval snapshot** for MAPA/ELSA Q3 2026 so the full suite is
   green.
3. **Two-phase quality eval.** Run the 25-question eval with two-phase on/off;
   enable only if audit pass rate or answer quality improves enough to justify
   +31% wall clock.
4. **Compact claims in single-pass.** If prompt tuning can make sentence indices
   reliable for short answers, re-enable them selectively; keep the `text:`
   fallback and the fail-safe.
5. **Streaming (point 1)** remains the largest perceived-latency improvement.
   It was excluded from this scope.
6. **Prompt duplication.** The supported set still appears in both system and
   user prompts; removing one is a small prefill/caching win.

---

## Appendix A — Trace IDs

| Run | Trace |
|---|---|
| ITMG, final default | `idx_data/chat/traces/20261028T081554Z_64614ba0.json` |
| ITMG, compact claims | `idx_data/chat/traces/20261028T081020Z_73a222cd.json` |
| ADMR, after fixes | `idx_data/chat/traces/20261028T081447Z_bbca4318.json` |
| ITMG, pre-fix refused | `idx_data/chat/traces/20261027T125411Z_33ab968d.json` |
| ITMG, high no-audit | `idx_data/chat/traces/20261028T042708Z_63ce19bc.json` |
| Pi session, high | `~/.pi/agent/sessions/--Users-hannanazinuddin-Projects-financial-report-llm--/2026-10-28T06-42-36-190Z_01a1813f-101c-745e-8221-62356fb5f2bc.jsonl` |

## Appendix B — Reproduction commands

```bash
# ITMG default fast path
PYTHONPATH=chat-app .venv/bin/python -m chat_app.agent \
  "Show me the highlight of ITMG operation in Q2 2026" --mode desk

# ADMR (still blocked by point 8)
PYTHONPATH=chat-app .venv/bin/python -m chat_app.agent \
  "Show me the highlight of ADMR operation in Q2 2026" --mode desk

# Targeted suites
PYTHONPATH=chat-app .venv/bin/python -m pytest \
  chat-app/tests/test_envelope.py chat-app/tests/test_verify.py \
  chat-app/tests/test_agent.py -q

# Full suite
PYTHONPATH=chat-app .venv/bin/python -m pytest chat-app/tests -q
```
