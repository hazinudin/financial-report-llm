# IDX Research Chatbot — Product Plan

**Status:** working document · **Last updated:** 2026-10-25 · **Owner:** Hannan
**Scope:** what to build, in what order, and the evidence behind each decision.

This plan supersedes the "generate a 14-section analysis" product. It is grounded in
measurements taken against this repo (see §9), not estimates.

**Implementation plan:** [`MVP-PLAN.md`](./MVP-PLAN.md) — the 2-week build plan for the
v0 chat, with locked decisions, contracts, tasks, and acceptance tests.

---

## 0. TL;DR

Build a **research chat** over any IDX company-period, grounded in three sources — XBRL
financials, the notes, and the operational disclosures (public expose / press release) —
where every number cites a row or a page, every cause is labelled *fact* /
*management-stated* / *hypothesis*, and every missing data point says why it is missing.

| Surface | Role | Measured cost |
|---|---|---|
| **Chat** | the product — "why" questions, premise correction, decomposition | **$0.0028 / answer** |
| **Fact sheet** | grounding layer — makes chat cheap and citeable | ~$0 |
| **Sector screens** | acquisition — free, weekly, shareable | $0.005 / screen |
| **Operational extract** | the missing half of the data model | **$0.00** |
| **Research** | metered add-on (Rp 15k / run) | $0.15–0.38 |
| **Prose reports** | retire; keep `--with-prose` for an archive only | $0.145 |

Order of work: **P0 correctness bugs → P1 operational documents → P2 grounding substrate
→ P3 sector machinery → P4 coverage → P5 product surface → P6 commercial.**

---

## 1. Product definition

> A chat that answers questions about an IDX-listed company's reported period, where
> the model never invents a number, never invents a cause, and never claims a document
> does not exist when it simply has not been indexed.

### 1.1 The three claims we can defend

| Claim | Evidence | Who else does it |
|---|---|---|
| **Row-level provenance** — every figure traceable to `sheet_code:row_number` | agent cites `1311000:6` unprompted | nobody |
| **Stated exclusions** — a company missing from a screen is named *with the reason* | *"BBCA (bank) and ISAT (no COGS line) have no comparable gross margin"* | nobody; screeners show blanks |
| **Restatement awareness** — both vintages shown | SGRO Q1-25: 1,677,323 as filed → 1,633,336 restated (§9.4) | nobody |

These three are the moat. The LLM is not.

### 1.2 Question taxonomy — what the chat should and should not do

| Type | Example | Needs | Surface | Cost |
|---|---|---|---|---|
| **Lookup** | "PTBA coal production Q2 2026" | expose/release + `read` | chat, but sourced from operational extract | ~$0.002 |
| **Reconciliation** | "did COGS rise in PTPS Q1?" | XBRL rows + arithmetic | **chat** ✅ | $0.003 |
| **Attribution** | "why did net profit fall?" | XBRL + notes + release | **chat** ✅✅ | $0.004–0.02 |
| **Screen** | "which palms grew >20%?" | normalized universe | table | $0.005 |
| **Forecast** | "what will Q3 revenue be?" | assumptions | **refuse**, or label as scenario | — |

Chat wins on rows 2–3. Screens win on row 4. Row 1 is a fact-sheet field. Row 5 is a
refusal with an explanation.

---

## 2. Decision log — how the product changed, and what changed it

| # | Position held | What changed it |
|---|---|---|
| 1 | Sell 14-section analyses B2B, 20 seats @ $200/mo | Token cost was never the problem: $0.30–0.85/analysis, ~$3k/yr for all IDX. Real blockers: OJK licensing, distribution, no valuation data |
| 2 | Chat over grounded facts | Coverage: 51/958 tickers; 2 of 16 mega caps. "Grounded" is tiered — XBRL is audit-grade, LLM extraction is not |
| 3 | Typed tools, **not** text-to-SQL | Trap A (by-nature sheet all NULL) and Trap B (cumulative vs quarter) are structural; a model cannot be trusted to remember them per query (§6) |
| 4 | Deep research as a premium artifact, not a chat | Measured Exa at **6.5×** token cost ($0.126 vs $0.0195 per run) — search, not tokens, is the variable cost |
| 5 | Rp 50k/mo cannot fund research; it can fund records | Rp ~41k net after fees/PPN ÷ $0.15 per run ≈ 5 research runs/month at zero margin |
| 6 | Fact sheet replaces the prose report | Writer = **53.7%** of run cost; dropping it also removes `HOLD` → removes the OJK exposure |
| 7 | Cross-company screens are the strongest differentiator | 5 palm-oil screens, $0.023, all directionally correct — including the SGRO restatement catch |
| 8 | Ship screens, chat as an escape hatch | **Wrong.** Overturned at #9 |
| 9 | **Chat is the front door** | User's 3 questions: one corrected a false premise, one found the real P&L driver, at **$0.0028/answer** — 7× cheaper than estimated |
| 10 | **Operational documents are a second data class** | Pipeline downloads only the filing package; `details.production = []` everywhere. The answers were in the expose all along |
| 11 | **Operational extraction costs $0, not $0.15** | Live test: 430 KB release fetched, 11 metrics + 5 drivers extracted, **5/5 reconciled to XBRL**, zero tokens |

---

## 3. Unit economics to hold yourself to

| Artifact | Baseline | Current | Target |
|---|---|---|---|
| Exa per search | $0.0232 | **$0.0108** (clamped) | ≤ $0.012 |
| Chat answer | — | **$0.0028** | ≤ $0.005 |
| Sector screen | — | **$0.0047** | ≤ $0.010 |
| **Operational extraction** | — | **$0.00** | keep at $0 |
| Fact sheet rebuild | — | ~$0 | ≤ $0.01 |
| Research run | $0.15–0.38 | — | **metered, never unlimited** |

### 3.1 Pricing model

| Tier | Price | Content |
|---|---|---|
| Free | Rp 0 | 1 rotating sector pack/week, watermarked — marketing |
| Retail | **Rp 50k/mo** or Rp 500k/yr | all packs + all fact sheets + QoQ diffs + unlimited records-grounded chat (~$0 to serve) |
| Pro / B2B | **$200/mo** | + CSV/API export, filing-day alerts, provenance export, custom universes |
| Credits | **Rp 15k each** | research runs / deep investigations |

**Rule:** unlimited anything that costs ~$0; metered anything that costs real money.
Annual billing is required — the Rp 2,000 flat gateway fee is 4% of a Rp 50k ticket.

### 3.2 Why retail works at Rp 50k

```
Rp 50,000 − gateway (Rp 3,450) − PPN 11% (Rp 5,500) ≈ Rp 41,000 net ≈ $2.50
at $0.0028/answer → ~890 answers/month at zero margin, ~620 at 70% margin
```

Chat is effectively unlimited at this price. **Research is not** — that is the metered line.

---

## 4. Data model

### 4.1 Two document classes

| | **Class A — Filing** | **Class B — Operational disclosure** |
|---|---|---|
| Documents | XBRL/XLSX, FS PDF, submission letter, board statement, checklist | Public expose (keterbukaan informasi), *siaran pers*, analyst presentation, MD&A, transcript |
| Timing | Filing day (T) | **T+0 to T+45** — ITMG's 6M26 release landed **T+41 days** |
| Contents | Statements, notes | Production, sales volume, ASP, cash cost/t, strip ratio, hectares, utilization, capex guidance, contract wins, **management's stated reason for the result** |
| Reliability | Audited/reviewed, structured | Unaudited, prose, often image PDFs, sometimes revised |
| Coverage today | 51 tickers ✅ | **0** ❌ |

**Class B is where the user's questions live.** Every example question so far
(production volume, "why did COGS rise", "why did net profit fall") is a Class B question.

### 4.2 When the release arrives, the answer changes shape

| Question | Without Class B | With Class B |
|---|---|---|
| "ITMG Q2 production?" | "not available" *(+ false claim the note didn't exist)* | "**5.3 Mt in Q2**, 9.9 Mt H1 (−4.8% YoY), per 10 Aug release p1" |
| "Why did cost of revenue rise?" | "hypotheses: strip ratio, fuel…" | "**Management attributes it to a higher strip ratio and higher fuel costs** (p1). Filings confirm COGS +7%" |
| "Why did OCF fall 78%?" | arithmetic only | "**Lower tax refunds ($169M) + higher supplier payments**, per management (p2)" |

Every "why" answer upgrades from *inference* to *cited management statement + arithmetic*.

### 4.3 Target fact-sheet shape

```yaml
meta: {ticker, quarter, year, currency, units, audited}

statements:
  income_statement: {lines: {...}, provenance: {revenue: {sheet: "1311000", row: 6}}}
  balance_sheet:    {lines: {...}, provenance: {...}}
  cash_flow:        {lines: {...}, provenance: {...}}

ratios:
  gross_margin: {value: 0.1563, prior: 0.1049, formula: "gross_profit / revenue", applicable: true}

details:
  customers: [...]        # concentration matters
  segments: [...]
  related_parties: [...]  # retrieve, do not dump
  commitments: [...]

operational:              # NEW — from Class B documents
  entries:
    - {metric: coal_production, scope: Q2-2026, value: 5.3, unit: Mt,
       source_doc: release_2026-08-10, page: 1, status: preliminary}

documents:                # NEW — provenance for the whole sheet
  - {type: filing,        published: 2026-06-30, url: ..., converted: true}
  - {type: press_release, published: 2026-08-10, url: ..., converted: true}
  - {type: presentation,  published: 2026-09-04, url: ..., converted: false, error: "fetch 403"}

completeness:             # NEW — the sheet knows what it is missing
  financials: complete
  operational: partial    # 2 of 4 expected documents captured
  as_of: 2026-08-12

checks:                   # was redflags.md — now structured
  - {id: ocf_np_divergence, severity: high, evidence: [cash_flow.ocf, income_statement.net_profit]}

caveats:                  # was prose in §"basis of preparation"
  - {id: template_no_below_operating_line}
  - {id: etr_sign_convention}
  - {id: eps_base_effect}
  - {id: no_market_data, detail: "no price/P/E as of 2026-10-25"}

research:
  findings: [{event, explanation, confidence, sources: [{url, verified}]}]
  provenance_audit: {verified: 19, removed: 2}

disclosure_notes: [...]   # client-facing
extraction_warnings: [...] # internal only
```

### 4.4 Three states for a missing operational number — never one

| State | What the chat must say |
|---|---|
| `not_yet_published` | "The Q2 expose isn't out yet. Q1's release landed T+41 days; expect this one around X." |
| `not_disclosed` | "Captured the 12 June presentation; it does not break out tonnage." |
| `not_captured` | "Found `ptba.co.id/.../press-release.pdf` but the fetch failed (403)." |

### 4.5 Revision semantics

Operational figures are preliminary then revised. Always carry `published_at` and
`supersedes`; **never overwrite**. Analysts care which vintage they are quoting.

---

## 5. To-do list

### P0 — Correctness bugs (do first; they block everything)

| # | Task | Effort | Why |
|---|---|---|---|
| 1 | YAML repair-and-retry, then **fail the stage** | 1d | Trailing commas silently discarded 12 findings / 21 URLs (PTBA Q2) |
| 2 | Empty-output guard on every writing stage | 0.5d | `redflags.md` was **1 byte** and marked `completed` |
| 3 | Run `rag.py index` after Stage 4 | 2h | PTBA had **0 chunks** → chat claimed a note didn't exist (it was 49 KB on disk) |
| 4 | Ban "no search results ⇒ doesn't exist"; add `read` to the tool allowlist | 2h | Root cause of the U1 false claim |
| 5 | `duckdb.connect(read_only=True)` for the notes index | 0.5d | `rag.py:138` opens read-write → "notes index is locked" under concurrency |
| 6 | Add Exa + extraction + redflags to `add_cost()`; surface in `--status` | 1d | `cost_usd` understated PTBA Q1 by 31% ($0.187 vs $0.271) |

### P1 — Operational document layer (highest value per dollar)

| # | Task | Effort | Why |
|---|---|---|---|
| 7 | Route document fetches through `curl_cffi impersonate="chrome131"` | 2h | Gets HTTP 200 where `fetch_content` failed 3/3 on the same URL |
| 8 | Expose/release **watcher**: IDX announcements + IR sites (bootstrap from `idx_companies.json.website`, 51/51 populated) | 3–4d | Poll **T+0…T+45**; ITMG's release landed T+41 |
| 9 | Operational extraction + **reconcile-or-reject gate** (0.5% vs XBRL) + page refs | 2–3d | Turns unaudited prose into a number you can publish |
| 10 | Bilingual split + **exact-label precedence lists** (no wildcards) | 1d | See §6.3 — wildcards silently returned PBT and parent-only equity |
| 11 | Schema: `documents[]`, `operational.entries[]`, `completeness{}` | 1d | §4.3 |
| 12 | Three-state missing-data contract | 1d | §4.4 |

### P2 — Grounding substrate

| # | Task | Effort | Why |
|---|---|---|---|
| 13 | Canonical units view (`currency`, `scale`, `value_idr_bn`) | 1d | Two answers in the same session labelled SSMS's scale differently |
| 14 | Per-line provenance at merge (`sheet_code`, `row_number`) | 1d | The XLSX cross-check already exists; the link is discarded |
| 15 | `compare_peers()` typed tool: period pinning, basis per row, `excluded[{ticker, reason}]` | 3d | SSMS loans 8,097.5bn vs 7,384bn reproducible — arithmetic not auditable (§9.5) |
| 16 | Split `context_notes` → disclosure / extraction_warning / debug | 1d | 148 strings including chunker debug sent to clients |
| 17 | Mechanise caveats as checks | 2d | Preserves the BBCA "modelling artefact" insight without the writer |

### P3 — Sector machinery: **shape × lens**, not 101 templates

| # | Task | Effort | Why |
|---|---|---|---|
| 18 | `BANK_IS_MAP` | 3–5d | Data already ingested (BBCA 710 rows/period, sheet `4322000`); NII derives correctly (§9.6) |
| 19 | `lenses/plantation.py`, `lenses/extractive.py` with **declared dependencies** | 3d | e.g. `cash_cost_per_tonne` requires `operational.production` |
| 20 | Golden tests: palm GM screen, bank identity | 2d | Only part of the product where exact assertions are possible |

**Shapes (4):** `industrial` (exists), `bank`, `financial/insurance`, `services/no-COGS`.
**Lenses (3 now):** `plantation` (33 covered names), `extractive` (7 coal), `banking`.
A shape changes parsing/validation. A lens adds metrics/presentation. **Neither changes the schema.**

### P4 — Coverage

| # | Task | Effort | Why |
|---|---|---|---|
| 21 | XLSX-only backfill of top ~100 by liquidity (parse, no docling, **$0 tokens**) | 1–2d | 14 of 16 mega caps still missing |

### P5 — Product surface

| # | Task | Effort | Why |
|---|---|---|---|
| 22 | Fact-sheet renderer — deterministic, sections, anchors, tap-a-number-for-its-row | 3d | "Read only part of it" is a rendering problem |
| 23 | QoQ diff | 2d | Free, no LLM, highest-value analytical view prose cannot provide |
| 24 | Chat front door with fact / management-stated / hypothesis labelling | 3d | This labelling **is** the trust product for causal questions |
| 25 | Free weekly sector screen (acquisition) | 2d | The hook; fact sheets are the depth |
| 26 | Metered research + credits | 2d | The only genuinely expensive action |

### P6 — Commercial

| # | Task | Effort |
|---|---|---|
| 27 | OJK legal review before charging for anything recommendation-shaped | external |
| 28 | Pricing rollout per §3.1 | — |
| 29 | **5 B2B conversations before building #25/#26** | — |

---

## 6. Known traps — do not re-learn these

### 6.1 Trap A — the "by nature" sheet exists, is fully labelled, and is entirely NULL

For ITMG, `sheet_code='1312000'` (P&L by nature) is populated with generic template rows
(`Program and broadcasting expenses`, `Toll collection expenses`, `Flight operations
expenses`) and every value is `NULL`. ITMG reports *by function* (`1311000`).

Any query doing `WHERE label_en ILIKE '%revenue%'` without pinning the sheet returns
NULL. `xlsx_seed.py` already encodes the fix (`IS_SHEETS`, *first sheet with rows wins*) —
that knowledge must live in the typed tools, never in a prompt.

### 6.2 Trap B — `tw2` is cumulative, not quarterly

```
ITMG 2026 tw1  revenue =   497,577   (thousand USD)
ITMG 2026 tw2  revenue = 1,000,491   (thousand USD)   ← Jan–Jun
                    Q2 = 1,000,491 − 497,577 = 502,914
```

There is **no cumulative→quarterly derivation in `xlsx_seed.py`**. Tools must return
`period_type: cumulative|quarter` and show the derivation.

### 6.3 Label matching must be exact, with an explicit precedence list

Measured failures from wildcard matching:

| Pattern | Wrong row returned | Correct row |
|---|---|---|
| `%total profit (loss)%` | PBT **158.7** | net profit **109.5** |
| `%total equity%` | parent-attributable **1,900.4** | total equity **1,910.5** |

Both silently plausible. Rule: exact match against a precedence list, and **reject the
extraction if it doesn't reconcile to XBRL within 0.5%**.

### 6.4 Bilingual documents will double-count you

ITMG's 6M26 release is 8 pages: ~7.4k English chars, then the Indonesian duplicate.
Split on the language boundary (`MELAPORKAN HASIL DAN KINERJA`) or you extract every
metric twice.

### 6.5 One peer group can hold three scales and two currencies

Palm-oil group, Q1 2026:

| Scale | Tickers |
|---|---|
| `Full amount` | PGUN, CSRA, JARR, OILS |
| `Thousands` | SSMS |
| `Millions` | AALI, BWPT, DSNG, LSIP, SGRO, SIMP, TAPG |
| **`USD`, full amount** | **ANJT** |

~12 orthogonal normalizations before a single comparison is meaningful.

### 6.6 Never rank on absolute levels across issuers of different size

A screen sorted by absolute OCF put PGUN (Rp53.6bn) next to CSRA (Rp96.1bn) and
OILS (−Rp8.6bn). The sign was right; the ordering was noise. Normalize (ratio, or per
hectare / per tonne).

### 6.7 An audited-looking number can still be wrong

SSMS net debt: agent reported loans **8,097.5bn** (built from row numbers, including
current maturities); reproducible from canonical labels is **7,384.0bn**. Conclusion
(>50% NDE) unchanged, but the arithmetic is not auditable. **Row numbers are not keys.**

### 6.8 Eight seen, one unseen

```
production 9.9 Mt  vs  sales 12.3 Mt   →  arithmetic gap 2.4 Mt
inventory $112.5M  →  $110.5M          →  stock FLAT (−1.8%)
```

The naive read ("sold down 2.4 Mt") is **wrong** — ITMG trades coal, so the gap was
purchased. Neither document says this; the join does. Label it an *arithmetic gap*, not
a physical fact.

---

## 7. Do-not list

**Product**
- ❌ Unlimited research at Rp 50k
- ❌ Prose reports as the product
- ❌ Any paid artifact containing `BUY`/`HOLD`/`SELL` before legal sign-off
- ❌ A general "ask anything about IHSG" bot — competes with free, hides the moat
- ❌ Deep research in v1

**Engineering**
- ❌ Raw SQL exposed to the chat agent
- ❌ Row numbers or fuzzy labels as keys
- ❌ Forking the schema per sector
- ❌ Silently dropping a company from a screen
- ❌ Writing `completed` when output is empty
- ❌ Treating "audit passed" as "grounded" — URL-less external claims sail through today
- ❌ Auto-generating sector templates with an LLM

---

## 8. Stop rules

| Signal | Threshold | Meaning |
|---|---|---|
| Research builds lose paid work | any | P0 #1/#2 not done — stop feature work |
| Narrative requests per name | > 2 | Prose still has demand — keep the flag |
| Provenance click-through | < 20% | Grounding isn't valued; reprice on coverage only |
| Questions clustering on the same 5 fields | > 60% | You need a better rendered section, not a chatbot |
| B2B conversations converting | < 3 of 5 | The thesis is wrong — do not add features to rescue it |
| Third bespoke sector pipeline hand-written | 1 | The shape×lens abstraction is wrong |
| False-negative rate on "does note X exist" | > 0 | You are shipping the U1 lie at scale |

---

## 9. Measured evidence appendix

All figures below were produced in this repo on 2026-10-25. Artifacts are listed in §10.

### 9.1 Exa dominates research cost (18 traces)

| | median | mean | max |
|---|---|---|---|
| Exa search API | **$0.126** | $0.128 | $0.168 |
| LLM tokens | $0.0195 | $0.019 | $0.027 |
| ratio | **6.5×** | 6.7× | — |

Search is ~87% of the marginal cost of the research stage. `exa_cost_usd` was `0.0` in
write traces and `None` in 7 of 18 research traces before this work — the dominant cost
line was unmeasured.

### 9.2 The clamp works, and quality held (same-document A/B, PTBA Q1 2026)

Implemented in `.pi/extensions/exa-budget.ts` — mutates `event.input` in the `tool_call`
hook, so the clamp is enforced and visible in the trace (15/15 searches logged).

| | clamp OFF | clamp ON | Δ |
|---|---|---|---|
| Exa cost | $0.1120 | $0.0840 | −25.0% |
| searches | 6 | 7 | |
| **Exa $/search** | **$0.0187** | **$0.0120** | **−35.8%** |
| research tokens | $0.0147 | $0.0097 | −34% |
| findings | 10 | 11 | +1 |
| unique source URLs | 21 | 21 | 0 |

Against the 11 pre-existing traces (mean $0.0232), clamped runs hit $0.0120 (Q1) and
$0.0095 (Q2) → **−53.7%**. Use **−36%** as the defensible number; the model requested
`numResults: 8` and `6` of its own accord, above the default of 5.

### 9.3 Cost accounting is wrong by construction

| PTBA Q1 2026 | USD |
|---|---|
| plan (glm-5.3) | 0.0317 |
| research tokens | 0.0097 |
| **Exa** | **0.0840** |
| writer (glm-5.3) | 0.1452 |
| **measured total** | **0.2706** |
| **`analysis_jobs.cost_usd`** | **0.1866** |

`add_cost()` is only called for plan + research tokens + writer. Exa, extraction and
redflags are never added. Writer alone is **53.7%** of the measured run.

### 9.4 Cross-company screens work (5 palm-oil screens, $0.0234)

| # | Screen | Result | Verification |
|---|---|---|---|
| P1 | Rank 12 names by Q1-26 gross margin | PGUN 47.44% → OILS 4.85% | exact: 79,011,157,367 / 166,542,924,195 = 47.44% |
| P2 | Highest YoY revenue growth | CSRA +96.6% | + SGRO restatement caught |
| P3 | ANJT vs AALI in IDR bn | AALI Rp7,501.8bn; ANJT **refused** to convert | correct refusal |
| P4 | Negative Q1-26 OCF | only OILS (−Rp8.64bn) | + JARR sign reversal noted |
| P5 | Net debt/equity > 50% | SSMS, OILS, BWPT, JARR | direction right, arithmetic partly unreproducible (§6.7) |

SGRO restatement, verified in the DB:

```
Q1-2025 as originally filed (2025 filing): 1,677,323
Q1-2025 as restated (2026 filing):         1,633,336
growth −23.1% restated vs −25.1% un-restated
```

### 9.5 Grounded Q&A: 10 questions, $0.0288

| # | Verdict |
|---|---|
| A1 PTBA related-party sales | ❌ false negative (stale index) |
| A2 ITMG 1–30d overdue receivables (US$19,382k) | ✅ |
| A3 ESSA top-2 customers (Genesis 89.2%) | ✅ |
| B1 PTBA OCF/NP 2.56× vs 3.06× | ✅ |
| B2 ITMG ETR +31.0% | ✅ |
| C1 GM expansion >300bps across DB | ⚠️ correct set, mislabeled H1 as Q2 |
| C2 Liabilities > equity (12 of 27) | ⚠️ stale source (`financial_ratios`) |
| D1 ITMG Q2 revenue 502,914 | ✅ best answer of the set |
| E1 DKFT Q1 revenue Rp506.0bn | ✅ |
| F1 BBCA NII (H1 42.51tn; Q2 21.40tn derived) | ✅ |

### 9.6 User's own questions: 3 questions, $0.0085

| # | Question | Result |
|---|---|---|
| U1 | PTBA coal production Q2 | ❌ failed — and **falsely claimed the note did not exist** |
| U2 | Why did PTPS COGS rise? | ✅ **corrected the premise** — COGS *fell* 10.7%; COGS/revenue 43.1% → 74.5% |
| U3 | Why did PTPS net profit fall? | ✅ found *"other gains (losses) −1.114bn vs +1.693bn = −2.81bn swing"* + tax anomaly |

U1's root causes: (1) tonnage is not in XBRL, (2) `notes_rag` had **0 PTBA chunks**,
(3) `read` was not in the allowlist, (4) empty search was interpreted as non-existence,
(5) concurrent `notes_search` hit a read-write DuckDB lock.

### 9.7 Operational extraction: $0.00 in tokens, 5/5 reconciled

Source: `itmg.co.id/files/media/News Release on Financial Results and Performance 6M26.pdf`
— **HTTP 200, 430 KB, 8 pages, published 2026-08-10** (T+41 days after period end).

| Method | Result |
|---|---|
| `fetch_content` (Stage 3b, pi-web-access) | ❌ failed (3/3 on this document class) |
| `pubexlive.idx.co.id` | ❌ 403 |
| **`curl_cffi` + `impersonate="chrome131"`** | ✅ **200, 430 KB** |

Extracted (page refs retained):

| Metric | Value | Prior |
|---|---|---|
| Coal production Q2 2026 | 5.3 Mt (+13% QoQ) | — |
| Coal production 6M26 | 9.9 Mt | 10.4 Mt |
| Coal sales volume 6M26 | 12.3 Mt | 11.7 Mt |
| ASP | US$81/t | US$78/t |
| Total assets | $2,417M | — |
| Cash + ST deposits | $738M + $66M (33% of assets) | — |
| Total liabilities | $506M | $498M |
| Total equity | $1,910M | $1,908M |
| Royalties | $111M (+7%) | — |
| Treasury share purchase | $27M | — |

Reconciliation, release vs XBRL — **5/5 within 0.5%**:

| | Release | XBRL |
|---|---|---|
| Revenue | $1,000.0M | $1,000.5M |
| Net profit | $110.0M | $109.5M |
| Operating cash flow | $62.0M | $62.4M |
| Total equity | $1,910.0M | $1,910.5M |
| Total assets | $2,417.0M | $2,416.7M |

Management-stated drivers captured verbatim with pages: higher strip ratio, higher fuel
costs, higher sales volume, lower tax refunds ($169M), lower dividend payment.

### 9.8 BBCA's bank data already exists

`xlsx_statements` holds **710 rows per period** for BBCA under bank taxonomy (sheet
`4322000`). Derived NII for Q1 2026 = 24,592,248 − 3,483,815 = **21,108,433**, which
matches the figure the BBCA note quotes from the company presentation exactly.

**So the bank gap is mapping, not ingestion** — `xlsx_seed.IS_MAP` has no
`interest_income`/`nii`/`provisions` keys.

### 9.9 Current data coverage

| Metric | Value |
|---|---|
| Universe | 958 companies |
| Taxonomy | 11 sectors / 33 sub-sectors / 58 industries / 101 sub-industries |
| XLSX-covered | **51 tickers**, 211,059 statement rows |
| Top-16 by liquidity covered | **2** (ADRO, BBCA) |
| `financial_ratios` | 27 codes, 2023-09-30 → 2025-11-04 (stale) |
| `company_profiles` | 958 (stubs) |
| Notes indexed for PTBA | **0 chunks** |
| Fact sheet size vs prose | 90,562 B (facts) + 20,130 B (research) vs 49,325 B (note) |

---

## 10. Artifacts produced during this investigation

| Path | What |
|---|---|
| `.pi/extensions/exa-budget.ts` | Enforced Exa cost clamp (env-overridable) |
| `.pi/exa-domains.json` | 30-domain allowlist generated from `config.yaml` |
| `scripts/qa_probe.py` | 10-question grounded Q&A probe |
| `scripts/peer_probe.py` | 5-question cross-company screen probe |
| `scripts/user_q_probe.py` | The user's 3 questions |
| `scripts/operational_probe.py` | Fetch + extract + reconcile an issuer release |
| `idx_data/operational/test/ITMG_6M26_news_release.pdf` | The primary source document |
| `idx_data/operational/test/ITMG_Q2_2026_operational.json` | Structured operational extract |
| `idx_data/qa_probe/answers.json` | Q&A probe results + traces |
| `idx_data/peer_probe/answers.json` | Screen probe results + traces |
| `idx_data/analysis/exa_budget.jsonl` | Clamp audit log (15 searches) |
| `analysis/PTBA Q1 2026 Analysis.md`, `PTBA Q2 2026 Analysis.md` | Test output |

---

## 11. Documents still missing operational data (Class B backlog)

| Ticker | Period | Release captured? | Notes |
|---|---|---|---|
| ITMG | Q2 2026 | ✅ (this test) | 430 KB, 5/5 reconciled |
| PTBA | Q1/Q2 2026 | ❌ | `details.production = []` |
| PTPS | Q1/Q2 2026 | ❌ | 36 context notes, no tonnage |
| ESSA | Q1/Q2 2026 | ❌ | customer concentration came from the note, not a release |
| All other 47 covered | — | ❌ | |

**Next action:** run the P1 watcher against the 51 covered tickers and measure what
fraction of releases can be fetched with `curl_cffi`. If that fraction is below ~70%,
fix the fetcher before building the extractor.
