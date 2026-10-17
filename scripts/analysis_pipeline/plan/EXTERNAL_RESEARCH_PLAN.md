# External Research Stage — Implementation Plan

**Status:** Implemented; **§5 search mechanics superseded (2026-10-15)** — the SearXNG Docker container, `search_backend.py`, `tool_registry.py`, and the OpenAI-compatible tool loop were retired. See [[PI_RESEARCH_AGENT_PLAN]].
**Author:** OpenCode session (post-DKFT Q1 2026 report comparison)
**Date:** 2026-07-27
**Scope:** Add an external-research layer to the analysis pipeline so that events found in the financial statements are *explained* (commodity prices, quota/policy, FX, sector news), not just restated — closing the gap between report-only output (e.g., `analysis/DKFT Q1 2026 Analysis - Gemma.md`) and context-enriched output (e.g., `analysis/DKFT Q1 2026 Analysis.md`).

**Companion plan:** [[PI_RESEARCH_AGENT_PLAN]] — replaces the §5 search mechanics (Python `web_search` / `fetch_web_page` over SearXNG) with a headless pi agent (`exa_search` + `fetch_content`). The trigger rules, planner, question generation, brief schema, and provenance audit described here remain current.

---

## 1. Background and Motivation

Comparison of the two DKFT Q1 2026 analyses (same 14-section skeleton, same core numbers) showed the enriched report's advantage decomposes into three input layers:

| Layer | Examples from the enriched DKFT report | Current pipeline support |
|---|---|---|
| **A. Market/valuation data** | Price Rp 650, P/E TTM 5.2x, EV/EBITDA 2.2x, 52-wk range, beta, float, div yield | ⚠️ Manual `--market-data` YAML stub only |
| **B. Industry/macro events** | RKAB quota cut Feb 2026 (~260–270M wet MT), LME path (Q1 ~US$17,000–17,250 → May peak US$19,675 → Jul US$16,220), PP 25/2024 domestic mandate, USD/IDR 16,993 weak-Rupiah tailwind, sulfur shock (HPAL vs NPI), LFP battery shift, royalty risk | ❌ None — `context.py` only runs 3 vault hits via `COMMODITY_HINTS` |
| **C. Note-level forensics (internal)** | Rp 189.28B one-off restricted-cash release inside reported OCF, Eximbank zero-interest + Rp 185.91B deferred-interest waiver, Rp 443.89B mine-closure provision, receivables aging split | ⚠️ Extraction-depth issue — **not** fixable by web research (see §9) |

The report-only version's concrete failures:

1. **Earnings-quality misread:** took reported OCF at face value ("1.39x conversion — high quality"), missing the one-off restricted-cash release; core OCF actually **fell 35.7% YoY**. Opposite conclusion from the same numbers. (Layer C — extraction fix.)
2. **Events restated, not explained:** "revenue increase suggests a favorable price environment or increased throughput" — generic guesswork where the enriched report names the RKAB cut, LME range, and FX tailwind. (Layer B — this plan.)
3. **Miscalibrated thesis:** risks generic ("Commodity Price Volatility (MODERATE)"), no target price, 9/10 health score vs. the enriched 8.5/10 with named deductions. (Consequence of A+B gaps.)

**Design goal:** every material event in the fact sheet gets a *named, dated, sourced* external explanation where one exists — so the writer attaches causes to effects instead of speculating.

### Design decision (from operator discussion)

Two candidate ideas were considered:

- **Idea 1:** agent generates research questions from extracted facts → research agent answers via web search.
- **Idea 2:** agent performs root-cause analysis (RCA) per event.

**Adopted: both, split by phase.** Idea 1 is the correct *trigger mechanism*; Idea 2 is the correct *output format*. Refinements:

- Triggers are **deterministic-first** (rules over `ratios.yaml` deltas), not LLM-first — exhaustive, free, never misses the big moves. LLM adds freeform questions on top.
- Searching is **narrow, dated queries** (not an RCA-prompted agent wandering the web — that meanders and burns budget). RCA happens at **synthesis time** over fetched evidence.
- Every external claim carries **provenance + period label** so `redflags.py` can audit it.

---

## 2. Architecture Overview

New **Stage 3b — `research.py`**, inserted between `context.py` and `write.py`:

```
facts/{T}_{Q}_{Y}.yaml + facts/{T}_{Q}_{Y}.ratios.yaml
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ ① TRIGGERS  (deterministic, no LLM)                             │
│   Rules over ratios/fact deltas → list of "events needing       │
│   explanation", each carrying the exact numbers                 │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ ② CLASSIFY + QUESTIONS  (cheap LLM, glm-5.1)                    │
│   Per event: internal (in report notes → route to writer, NOT   │
│   to web) vs external (→ search).                               │
│   Typed query templates + freeform LLM questions, deduped.      │
│   Cap: ~8–12 questions, 2–3 queries each                        │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ ③ SEARCH  (web backend, cached by query hash)                   │
│   Vault-first: answer from rag.py hits where possible;          │
│   web fills recency gaps. Sector pack reused across tickers.    │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ ④ BRIEF  (cheap LLM synthesizes → RCA-structured YAML + .md)    │
│   event → explanation → confidence → period label → sources     │
│   → writer_hint. Capped ~3–4K tokens.                           │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
context.py injects brief into writer package (research_brief key)
```

Stage tracking: add `stage_research TEXT DEFAULT 'pending'` to `analysis_jobs`. Idempotent, skippable, `--force`-able like all stages. Best-effort: search backend down → pipeline still writes (mirrors `vault_search` failure handling).

---

## 3. Trigger Rules (sub-step ①)

Rules over merged facts + ratios. Each trigger records: `rule_id, event_text, numbers, section_hint`.

| Rule (example thresholds) | DKFT Q1 2026 event it catches |
|---|---|
| Any margin moves > 500 bps YoY | GM +1,344 bps → *why did ore prices/costs move?* |
| Any expense line → 0 or ±100% YoY | Selling expense 8.42B → 0 → *delivery-terms change?* |
| Receivables +>50% AND collections/revenue < 80% | Rp 299B build, 59.6% collections → *sector credit stress?* |
| OCF/net profit < 0.8 or > 1.5 | 1.39x → one-off in OCF (routes **internal**) |
| Any balance-sheet line ±>30% vs prior FY end | Restricted cash -68%, taxes payable +197%, advances +35% |
| Segment sign flip or revenue line ±>3x | Limestone 8.5x YoY |
| Loans -30%+ or capital-structure event | CCB full repayment → *disclosure/covenants news?* |
| Customer mix change: any top-5 customer new/gone YoY | Vantama 26% → 0; Sino +135% → *who are these buyers?* |
| Production vs plan divergence > 10% | 28.8% of annual plan in Q1 → *guidance raised?* |

Thresholds live in `config.yaml` (tunable, per-sector overrides later).

---

## 4. Classify + Question Generation (sub-step ②)

### Internal vs external routing (critical)

- **Internal** — answer is inside the report's own notes (restricted-cash release, deferred-interest waiver, mine-closure provision, related-party items). Route to the **writer prompt as a "must-explain-from-notes" list**, *not* to web search. Research must not become a band-aid for shallow extraction (§9).
- **External** — commodity prices, quota/fiscal policy, FX, customer/sector news, post-period developments. Proceed to search.

### Typed query templates

Each external trigger type maps to dated, geo-anchored templates — this is what prevents generic fluff:

```yaml
# config.yaml sketch
research:
  question_templates:
    margin_spike:
      - "{commodity} price {quarter} {year} trend LME"
      - "Indonesia {commodity} quota RKAB policy {year}"
      - "{commodity} ore premium Indonesia smelter {year}"
    customer_change:
      - "{customer_name} smelter Indonesia nickel"
    capex_or_loan_event:
      - "{ticker} {company_name} news {quarter} {year}"
```

Plus a **catalyst calendar** per commodity so templates know *when* to look:

```yaml
catalyst_calendar:
      nickel: ["RKAB quota decisions ~mid-year", "LME price path", "progressive royalty reviews"]
      coal:   ["DMO policy", "reference price (HBA) monthly"]
    # Monthly snapshots written to vault: analysis/commodities/snapshots/{commodity}_{YYYY-MM}.md
    # Stable primers (analysis/commodities/{Commodity}.md) are hand-curated, never overwritten.
```

Cheap LLM then adds ≤ 3 freeform questions for events templates can't cover; union + dedupe. Budget caps: ~8–12 questions total, 2–3 queries each.

---

## 5. Search Execution (sub-step ③)

Ordering and reuse rules:

1. **Vault-first:** run each question through `scripts/notes_rag/rag.py` (existing pattern from `context.py`); accept hits above a relevance bar. Free, private, already curated ([[Strait_of_Hormuz_Crisis]], [[Rupiah & Ammonia - Meridian Research]], commodity deep-dives). This **includes** the monthly sector snapshots (see §5.2) since they live in the vault — RAG picks them up automatically, no special wiring needed.
2. **Shared sector pack (vault-resident):** commodity/policy context is identical across tickers (nickel: DKFT/INDR/MHKI/ELSA/ARCI). Maintain **two genres, both in the vault under `analysis/commodities/snapshots/`**:
   - **Stable primers** (`analysis/commodities/Coking Coal.md` etc.) — hand-curated, timeless narrative; not overwritten by the pipeline.
   - **Monthly snapshots** (`analysis/commodities/snapshots/{commodity}_{YYYY-MM}.md`) — auto-generated, dated state-of-market (e.g., HBA path this month, DMO moves, recent news), clearly marked machine-produced, regenerated monthly or on `--refresh-sector`. Mirrors [[Coking Coal Market Asia-Pacific 2025-2026]] convention but dated and refreshable.
   
   Snapshots are written into the vault — not the gitignored cache — because RAG already searches the vault, git versions them (auditable, no silent overwrites via cache), and operators can browse them in Obsidian alongside the primers. Quarterly runs only research ticker-specific events + freshness deltas on top of the snapshot. **Biggest cost saver.**
3. **Web search** for the remainder, results cached by query hash in `idx_data/analysis/research/cache/` so `--write-only` and re-runs never re-fetch. (Cache stays gitignored — it holds raw search results, not analysis.)

### Search backend — SearXNG via Docker (resolved, §12)

> **Superseded (2026-10-15):** Stage 3b now runs a headless pi agent with Exa-backed search (`exa_search`) and `fetch_content` fetches — see [[PI_RESEARCH_AGENT_PLAN]]. The mechanics below are kept as a historical record.

**Adopted:** self-hosted **SearXNG** in a Docker container, queried through its
JSON API. Free, keyless, private, no `.env` secret, no pay-per-use — fits the
plan's cost and privacy ethos, and the JSON response maps cleanly onto the
query-hash cache in §5.3 so the "replayable artifacts" rule (§7.3) holds with a
container rather than an API account.

**Container contract:**

- `docker compose up -d` from a repo-root `docker-compose.yml` pinning the
  SearXNG image on `8080:8080`. Pipeline writes stay in `idx_data` (gitignored).
- Query: `GET http://localhost:8080/search?q={query}&format=json&time_range={day|week|month|year}&categories=news`.
- A pinned `settings.yml` overrides the engine list to a **stable, low-blocking
  set** (e.g. `duckduckgo, brave, bing`) for reproducibility + rate-limit hygiene
  — the default meta-of-everything drifts run-to-run.
- **Health gate:** `research.py` pings `/healthz` before sub-step ③; container
  down → skip search, mark stage `degraded`, write proceeds (mirrors
  `vault_search` handling). Container health is *not* a hard pipeline dep.
- **Rate-limit mitigation:** ≤36 queries/report (§7) + query-hash cache + shared
  sector pack (§5.2) keep per-IP load low; the pinned engine set avoids
  Google-scrape blocks.

**Hybrid content depth** (operator choice 2026-07-31): SearXNG returns
URL+title+snippet+`publishedDate`, not article text. To avoid both the thinness
of snippet-only and the cost of always-fetching:

1. First synthesis pass over cached results uses **snippets only** (glm-5.1) →
   provisional finding + `provisional_confidence`.
2. If `provisional_confidence == low` **and** a result URL is on an
   authoritative-domain allow-list (Reuters, Bloomberg, GIA, ministry portals,
   IDX filings — in `config.yaml`), `research.py` fetches **that one** page via
   `httpx` (already a dep, `opencode_client.py:21`) + lightweight readability
   pass, then runs a **second synthesis pass** over snippet+body. Cap: one fetch
   per low-confidence finding, ≤ ~8 fetches/report.
3. Fetch missing/parse-failed → keep `low`, persist `fetch_attempted: true` so
   `redflags.py` can audit.

**Pluggable adapter:** `search_backend_search(query, **kw) -> list[Result]`
behind the cache. SearXNG is the implemented adapter; an Exa/Tavily adapter can
drop in behind the same interface later if precise by-date news filtering ever
justifies the cost (deprioritized, kept open-only). The Python `web_search` /
`fetch_web_page` tools that expose this adapter to the LLM are specified in
[[WEB_SEARCH_TOOL_PLAN]].

**Open caveat — date fidelity (vs Exa):** `time_range` is coarse
(day/week/month/year), so the precise by-date news filtering praised in §2 is
approximated. Period integrity is recovered structurally from `publishedDate`
plus a cheap-LLM date-parsing fallback in synthesis when the field is missing;
`redflags.py` enforces period discipline off whatever date the brief carries, so
the §7.1 constraint holds even with fuzzy sourcing — it just shifts the burden
to the synthesis redflag layer rather than the search layer.

---

## 6. Research Brief Schema (sub-step ④)

Output: `idx_data/analysis/research/{TICKER}_{Q}_{YEAR}.research.yaml` + human-readable `.md`.

```yaml
meta: { ticker, quarter, year, generated_at, sector_pack: "nickel_2026-07", question_count: 9 }
findings:
  - event: "Gross margin +1,344 bps YoY (52.05% → 65.48%)"
    explanation: "RKAB quota cut Feb 2026 to ~260–270M wet MT created domestic
      ore scarcity; LME traded US$17,000–17,250 through Q1"
    confidence: high                    # high | medium | low
    period: in-period                   # in-period | post-period | as-of-today
    sources:
      - { title: "...", url: "...", published_date: "2026-02-.." }
    writer_hint: "Attach to §2.2 revenue drivers and §3.1 gross margin"
  - event: "Post-quarter price reversal"
    explanation: "LME peaked US$19,675 (May 6) then fell to US$16,220 (Jul 2);
      denied rumor of RKAB increase to 360M MT"
    period: post-period
    writer_hint: "Forward outlook §2.4 and risk §11 — never present as a Q1 fact"
internal_must_explain:                  # routed to writer, NOT searched
  - "OCF Rp 330.74B includes one-off restricted-cash release ~Rp 189B — reconcile before earnings-quality claims"
  - "Eximbank loans bear zero cash interest; Rp 185.91B deferred interest waived on full settlement"
```

The `event → explanation → confidence → period → sources → writer_hint` shape **is** the RCA output (operator's Idea 2), produced at synthesis time over fetched evidence.

---

## 7. Non-Negotiable Design Rules

1. **Period discipline.** Every claim labeled `in-period` / `post-period` / `as-of-today`. The enriched DKFT report models this (LME May peak explicitly marked "post-Q1"). Writer must never present a July price as a Q1 fact; `redflags.py` gains a check for period-label violations.
2. **Provenance.** Every external claim carries source + published date. Enables redflags audit of external-vs-financial contradictions.
3. **Replayable artifacts.** Brief + search cache persisted; `--write-only` replays without re-searching. Mirrors `context.json` pattern.
4. **Token budget.** Brief ≤ ~3–4K tokens injected into the writer package (writer call ≤ 32K principle); sector pack referenced, not inlined.
5. **Best-effort failure.** Backend down / no results → stage marked `degraded`, pipeline proceeds to write (same as `vault_search`).
6. **Numbers still canonical.** The brief may carry external *context* numbers (LME prices, quota volumes) but never restate financials; all financial figures remain sourced from the fact sheet only. QA grep-check rule extended: external numbers must trace to brief sources.
7. **Cost caps.** ≤ 12 questions, ≤ 3 queries each, ≤ ~2 synthesis calls per report (+ ≤ 1 follow-up synthesis per low-confidence authoritative fetch, capped at ≤ 8 fetches/report per §5). Record search + fetch + token usage in `analysis_jobs`.
8. **Ops isolation.** The SearXNG container is an *external* dependency, not a pipeline stage: `research.py` health-gates it, treats it as best-effort, and never blocks write. Operators manage `docker compose` lifecycle manually; a `--no-search` flag makes the stage skip the container entirely (triggers + `internal_must_explain` still produce).

---

## 8. Writer + Redflags Prompt Changes

- `writer_system.md`: new section — "External context (research brief): use it to *explain* events; attach explanations to the affected section per `writer_hint`; cite period labels; mark post-period items explicitly; never contradict the fact sheet; list unresolved low-confidence items under risks."
- `redflags.md`: add checks — (a) every external claim in the draft has a source in the brief; (b) no post-period claim presented as in-period; (c) external explanation contradicting financials (e.g., "volume up" when production fell) is flagged.

---

## 9. Companion Fixes NOT Covered by This Plan (Layer C)

Web research does not fix the earnings-quality misread (Finding 1) — those numbers were *inside* the report. Separate, cheap changes:

1. **Extraction schema:** add `cash_flow.note_items` (restricted-cash releases, one-offs, non-cash items) and `balance_sheet.provisions_detail`.
2. **Writer-prompt rule:** "If OCF and net profit diverge > 30%, identify the reconciling items from the notes *before* making any earnings-quality claim."
3. **Trigger routing:** the `OCF/net profit` trigger (§3) always routes **internal**, never to web search.

Track these as a small separate task against `schema.py` / `extract_system.md` / `writer_system.md`.

---

## 10. Directory / Config Additions

```
scripts/analysis_pipeline/
├── research.py                    # Stage 3b: triggers → classify → search → brief
├── search_backend.py              # search_backend_search() SearXNG adapter + cache I/O + fetcher (owned by [[WEB_SEARCH_TOOL_PLAN]])
└── prompts/
    ├── research_questions.md      # classify + freeform-question prompt (cheap model)
    └── research_synthesis.md      # evidence → RCA brief prompt (cheap model)

docker-compose.yml                 # repo root: SearXNG image, 8080:8080, pinned settings.yml
config/searxng/settings.yml        # pinned engine list (duckduckgo/brave/bing), stable + low-blocking

idx_data/analysis/research/        # gitignored
├── {TICKER}_{Q}_{YEAR}.research.yaml
├── {TICKER}_{Q}_{YEAR}.research.md
└── cache/
    ├── search/{query_hash}.json   # raw SearXNG results
    └── fetch/{url_hash}.json      # fetched+extracted page body (for the hybrid pass)

# Sector snapshots live IN the vault (git-tracked, RAG-searchable):
analysis/commodities/
├── Coking Coal.md                # stable primer, hand-curated (existing)
├── snapshots/
│   ├── coal_2026-07.md           # monthly, auto-generated, dated, refreshable
│   └── nickel_2026-07.md
└── ...
```

```yaml
# config.yaml (extend analysis: block)
analysis:
  models:
    research: opencode-go/glm-5.1        # classify + synthesis (cheap)
  max_tokens:
    research: 4096
  research:
    enabled: true
    search_backend: searxng              # searxng (default) | exa | tavily
    searxng:
      base_url: http://localhost:8080     # /search?format=json&categories=news
      health_path: /healthz
      engines: [duckduckgo, brave, bing] # pinned stable, low-blocking set
      timeout_s: 15
    fetch:
      on_low_confidence: true
      authoritative_domains: [reuters.com, bloomberg.com, siaiair.com, gidji.com,
                              kemen-esdm, bappebti, idx.co.id, investing.com]
      max_fetches_per_report: 8
      readability: true                   # httpx + lightweight extractor
    max_questions: 12
    max_queries_per_question: 3
    brief_token_cap: 4000
    sector_pack_max_age_days: 31
    triggers:
      margin_bps: 500
      expense_zero_or_double: true
      receivables_growth_pct: 50
      collections_ratio_floor: 0.80
      ocf_np_divergence: [0.8, 1.5]
      balance_sheet_move_pct: 30
```

New CLI flags: `--skip-research`, `--refresh-sector`, `--research-only` (produce brief, stop before write).

---

## 11. Phased Rollout

| Phase | Deliverable | Est. effort |
|---|---|---|
| **1. Triggers + brief skeleton** | `research.py` with deterministic triggers only, no search — brief contains events + `internal_must_explain`; wire into `context.py` and writer prompt. Validate trigger list on DKFT fact sheet (expect ~9–12 events). | 0.5 day |
| **2. Search backend** | SearXNG Docker container (`docker-compose.yml` + `config/searxng/settings.yml`), `search_backend.py` adapter + query-hash cache, health gate, fetcher for the hybrid low-confidence pass; sector pack builder for nickel. | 1 day |
| **3. Questions + synthesis** | Classifier prompt, templates, RCA synthesis → brief YAML/MD; period-label + provenance enforcement. Golden test vs the enriched DKFT report's external claims (RKAB, LME path, USD/IDR). | 1 day |
| **4. Redflags + Layer C** | Period/provenance checks in `redflags.py`; extraction schema `note_items`; writer divergence rule (§9). | 0.5–1 day |

Phase 1 exit criteria: DKFT trigger list matches the human-identified event set (margin spike, selling-expense zero, receivables build, OCF divergence, restricted cash, taxes payable, advances, customer mix, limestone ramp, CCB repayment).

---

## 12. Open Questions

1. **Search backend** (§5): ~~Exa/Tavily key vs. verifying OpenCode Go tool-calling on glm models.~~ **Resolved (operator discussion 2026-07-31):** self-hosted **SearXNG via Docker**, queried through its JSON API, with a hybrid snippet→conditional-fetch content-depth policy. Free, keyless, replayable via the query-hash cache (§5.3), container-down → `degraded` (§7.5). Exa/Tavily kept as a deprioritized pluggable adapter behind the same `search_backend_search()` cache interface. See §5 for the container contract and the date-fidelity caveat. **Superseded (2026-10-15):** Stage 3b now runs a headless pi agent with Exa-backed search (`exa_search`) and `fetch_content` fetches — see [[PI_RESEARCH_AGENT_PLAN]]; the SearXNG container, `search_backend.py`, `tool_registry.py`, and the OpenAI-compatible tool loop were retired.
2. **Sector-pack authorship:** ~~fully auto-generated monthly, or human-curated with auto-append of dated news items?~~ **Resolved (operator discussion 2026-07-31):** two genres, both vault-resident (§5.2). Stable primers stay hand-curated; monthly snapshots are auto-generated into `analysis/commodities/snapshots/`, git-tracked and RAG-searchable. Open sub-questions remaining: (a) regenerate snapshot in-place vs. diff-append; (b) whether snapshots should carry a `generated_at` header + `confidence` per claim to keep them auditable alongside curated notes.
3. **Post-period window:** how far past quarter-end may "post-period" context extend (report date vs. analysis run date)? Proposal: up to run date, always labeled.
4. **Paywalled sources** (Wood Mackenzie etc.): brief must mark `confidence: low` when the only support is secondary citation; never present as verified.
5. **Market data (Layer A):** keep manual `--market-data` stub (per `IMPLEMENTATION_PLAN.md` §13.1) or fold an auto-fetch into this stage? Recommendation: keep separate concern; revisit after research stage lands.

---

## 13. Success Metrics

- **Trigger recall:** 100% of the DKFT golden event set detected (§11 Phase 1).
- **Explanation coverage:** ≥ 80% of external-routed events get a sourced explanation with `confidence ≥ medium`.
- **Provenance:** 100% of external claims in the draft trace to brief sources (redflags check, zero violations).
- **Period discipline:** zero in-period/post-period mislabels in redflags output.
- **Budget:** ≤ 2 additional cheap-model calls + ≤ 36 searches per report; writer input still ≤ 32K tokens.
- **Quality bar:** generated draft explains margin/revenue moves with named, dated causes (no "suggests a favorable price environment"-style speculation) in the DKFT, ESSA, and ELSA regression runs.
