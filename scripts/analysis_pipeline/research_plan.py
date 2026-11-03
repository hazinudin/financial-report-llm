"""
Stage 3b — research-plane planner (LLM-formulated, themed research plan).

Reads the detected events + financial context (facts/ratios + prior analyses +
company profile) and asks the writer model to produce a themed, prioritized
research plan: analytical framing, themes, prioritized questions, hypotheses.

This deliberately does NOT call web tools; that is the job of the downstream
pi research agent in research.py. The planner formulates *all* questions first,
matching the analyst workflow demonstrated for SRSN Q2 2026.
"""

from __future__ import annotations

import json
import re
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import opencode_client
import pi_agent
import research_questions
import yaml_io

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ANALYSIS_DIR = _PROJECT_ROOT / "analysis"
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RESEARCH_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "research"

_DEFAULT_PLANNER_TOOLS = ["read", "duckdb_query", "notes_search", "notes_fetch"]

_PLANNER_SYSTEM_PROMPT = (
    "You are a senior equity-research analyst. Given financial events and "
    "context, you formulate a THEMED, PRIORITIZED research plan with "
    "specific, falsifiable questions and testable hypotheses. You do NOT "
    "search the web. Do not expose reasoning or schema examples. "
    "Immediately output ONLY valid YAML in the requested structure."
)


@dataclass(frozen=True)
class PlanQuestion:
    id: str
    theme: str
    question: str
    rationale: str = ""
    search_query: str = ""


@dataclass
class ResearchPlan:
    ticker: str
    year: int
    quarter: str
    identity: research_questions.CompanyIdentity
    analytical_framing: str = ""
    themes: list[dict[str, Any]] = field(default_factory=list)
    questions: list[PlanQuestion] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    priority_themes: list[str] = field(default_factory=list)
    fallback: bool = False
    error: str | None = None
    # Planner run provenance (not serialized into the plan YAML/markdown).
    pi_meta: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    plan_trace_path: str | None = None


# ── Context loaders ─────────────────────────────────────────────────────


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _find_prior_analyses(ticker: str, year: int, quarter: str) -> list[Path]:
    """Return existing analysis markdown paths for this ticker (most
    recent first), excluding the current quarter (which may not yet exist
    on a first run; if it does, it is included)."""
    matches = sorted(
        ANALYSIS_DIR.glob(f"{ticker} * Analysis.md"),
        key=lambda p: p.name,
        reverse=True,
    )
    return matches


def _load_company_profile(ticker: str) -> str:
    return _read_text(ANALYSIS_DIR / "companies" / f"{ticker}.md")


def _load_facts_summary(ticker: str, year: int, quarter: str) -> str:
    facts_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    ratios_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"
    parts: list[str] = []
    if facts_path.exists():
        facts = yaml.safe_load(_read_text(facts_path)) or {}
        for stmt_name in ("income_statement", "balance_sheet", "cash_flow"):
            stmt = facts.get(stmt_name) or {}
            lines = stmt.get("lines", {}) or {}
            if lines:
                items = {k: v for k, v in list(lines.items())[:8] if v is not None}
                parts.append(f"{stmt_name}: {yaml.safe_dump(items, sort_keys=False).strip()}")
    if ratios_path.exists():
        ratios = yaml.safe_load(_read_text(ratios_path)) or {}
        flat = {}
        for k, v in ratios.items():
            val = v.get("value") if isinstance(v, dict) else v
            if isinstance(val, (int, float)):
                flat[k] = round(float(val), 4)
        if flat:
            parts.append(f"ratios: {yaml.safe_dump(flat, sort_keys=False).strip()}")
    return "\n".join(parts) if parts else "(no facts/ratios available)"


def build_planner_context(
    ticker: str, year: int, quarter: str, events: str,
    identity: research_questions.CompanyIdentity,
    *,
    max_analysis_chars: int = 12000,
) -> str:
    """Assemble the text fed to the planner LLM."""
    facts_summary = _load_facts_summary(ticker, year, quarter)
    profile = _load_company_profile(ticker)
    prior = _find_prior_analyses(ticker, year, quarter)

    analyses_block: list[str] = []
    used = 0
    for p in prior:
        name = p.name.replace(" Analysis.md", "")
        text = _read_text(p)
        if not text:
            continue
        remaining = max_analysis_chars - used
        if remaining <= 0:
            break
        snippet = text[:remaining]
        analyses_block.append(f"## {name}\n{snippet}")
        used += len(snippet)

    analyses_text = (
        "\n\n".join(analyses_block)
        if analyses_block
        else "(no prior analyses available)"
    )

    return f"""# TASK
You are a financial research analyst planning external research for {identity.legal_name} ({identity.ticker}) — {identity.sector} — for {quarter} {year}.
The deterministic pipeline has flagged these financial events that need external explanation:
{events}

First, derive any implied period arithmetic (e.g. Q2 = H1 − Q1) and state it as an analytical framing.
Then formulate ALL research questions BEFORE searching. Group questions into themes, prioritize them, and propose testable hypotheses.
Do not search the web; another stage does that. Use only the context below.

# COMPANY IDENTITY (authoritative — never confuse with another issuer)
- Legal name: {identity.legal_name}
- Ticker: {identity.ticker}
- Sector: {identity.sector}
- Do not confuse with: {', '.join(identity.forbidden_identities) or '(none)'}

# FINANCIAL CONTEXT (fact sheet + computed ratios)
{facts_summary}

# COMPANY PROFILE
{profile or '(none)'}

# PRIOR ANALYSES (read for segment detail, customer names, projects, seasonality)
{analyses_text}

# OUTPUT RULES
Output ONLY valid YAML. No markdown fences, no prose before or after.
Every multi-line or free-text field MUST use a YAML block scalar (`|`):
  analytical_framing: |
    <text here — colons and quotes are safe inside a block scalar>
NEVER write a colon inside a plain (unquoted) scalar — it breaks the parser.
For short single-line fields, you may use double quotes: question: "...", search_query: "...".
Use exactly these top-level keys: analytical_framing, themes, hypotheses, priority_themes.

Structure:
  analytical_framing: |
    <derived period arithmetic, deltas, and what jumped>
  themes:
    - name: "<Theme A — short label>"
      priority: high|medium|low
      rationale: |
        <why this theme matters for the flagged events>
      questions:
        - id: "A1"
          question: "<specific, falsifiable question>"
          rationale: "<what an answer would explain>"
          search_query: "<ticker legal_name quarter year key terms>"
  hypotheses:
    - "<testable hypothesis, ranked most-likely first>"
  priority_themes:
    - "<Theme name>"
"""


# ── Parsing ────────────────────────────────────────────────────────────


_QUESTION_ID_RE = re.compile(r"^([A-Z]\d+)$")
_TOP_LEVEL_KEYS = ("analytical_framing", "themes", "hypotheses", "priority_themes")


def _extract_yaml_block(raw: str) -> str:
    """Strip fences and any prose preamble/trailing prose, returning the
    YAML substring starting at the first recognised top-level key."""
    text = raw.strip()
    # If the model wrapped the YAML in a fenced block (possibly after prose),
    # extract the content between the first pair of fences.
    fence_start = text.find("```")
    if fence_start >= 0:
        after = text[fence_start:]
        lines = after.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        # drop the closing fence
        for j in range(len(lines) - 1, -1, -1):
            if lines[j].strip() == "```":
                lines = lines[:j]
                break
        text = "\n".join(lines).strip()
    # Use the final candidate: reasoning models often repeat the requested
    # schema before emitting the actual YAML plan.
    matches: list[int] = []
    framing_matches: list[int] = []
    for i, line in enumerate(text.split("\n")):
        for k in _TOP_LEVEL_KEYS:
            if re.match(rf"^{k}\s*:", line):
                matches.append(i)
                if k == "analytical_framing":
                    framing_matches.append(i)
                break
    best = (framing_matches or matches)[-1] if (framing_matches or matches) else -1
    if best > 0:
        text = "\n".join(text.split("\n")[best:])
    return text


def parse_plan(
    raw: str, ticker: str, year: int, quarter: str,
    identity: research_questions.CompanyIdentity,
) -> ResearchPlan:
    """Parse the LLM's YAML response into a ResearchPlan. Tolerant of
    fences, prose preamble, and colon-in-plain-scalar failures."""
    if not raw or not raw.strip():
        return ResearchPlan(ticker, year, quarter, identity, error="empty planner output")

    extracted = _extract_yaml_block(raw)
    if not extracted:
        return ResearchPlan(ticker, year, quarter, identity, error="no YAML block found in planner output")

    data: Any = None
    err: str | None = None
    try:
        data = yaml_io.loads_with_repair(
            extracted, context=f"research plan {ticker} {quarter} {year}",
        )
    except yaml_io.YamlLoadError as e:
        err = f"yaml parse failed: {e.parser_message}"

    if err is not None:
        return ResearchPlan(ticker, year, quarter, identity, error=err)
    if not isinstance(data, dict):
        return ResearchPlan(ticker, year, quarter, identity, error="planner output not a mapping")

    framing = str(data.get("analytical_framing", "") or "").strip()
    themes_raw = data.get("themes", []) or []
    hypotheses = data.get("hypotheses", []) or []
    priority_themes = data.get("priority_themes", []) or []
    if not isinstance(themes_raw, list):
        themes_raw = []
    if not isinstance(hypotheses, list):
        hypotheses = []
    if not isinstance(priority_themes, list):
        priority_themes = []

    themes: list[dict[str, Any]] = []
    questions: list[PlanQuestion] = []
    for theme in themes_raw:
        if not isinstance(theme, dict):
            continue
        tname = str(theme.get("name", "")).strip() or "Untitled theme"
        t_priority = str(theme.get("priority", "medium")).strip().lower() or "medium"
        t_rationale = str(theme.get("rationale", "") or "").strip()
        themes.append({"name": tname, "priority": t_priority, "rationale": t_rationale})
        for q in theme.get("questions", []) or []:
            if not isinstance(q, dict):
                continue
            qtext = str(q.get("question", "")).strip()
            if not qtext:
                continue
            qid = str(q.get("id", "")).strip()
            if not _QUESTION_ID_RE.match(qid):
                # auto-assign: prefer a single-letter token in the theme name
                # (e.g. "Theme A" -> A), else the theme's first letter.
                letter = None
                m = re.search(r"\b([A-Z])\b", tname)
                if m:
                    letter = m.group(1)
                else:
                    letter = tname[0].upper() if tname[:1].isalpha() else "Q"
                qid = f"{letter}{len(questions) + 1}"
            questions.append(PlanQuestion(
                id=qid,
                theme=tname,
                question=qtext,
                rationale=str(q.get("rationale", "") or "").strip(),
                search_query=str(q.get("search_query", "") or "").strip(),
            ))
    hyp_list = [str(h).strip() for h in hypotheses if str(h).strip()]
    pri_list = [str(p).strip() for p in priority_themes if str(p).strip()]

    return ResearchPlan(
        ticker=ticker, year=year, quarter=quarter, identity=identity,
        analytical_framing=framing,
        themes=themes, questions=questions,
        hypotheses=hyp_list, priority_themes=pri_list,
    )


# ── Rendering ───────────────────────────────────────────────────────────


def plan_as_dicts(plan: ResearchPlan) -> dict[str, Any]:
    return {
        "ticker": plan.ticker,
        "year": plan.year,
        "quarter": plan.quarter,
        "identity": {
            "legal_name": plan.identity.legal_name,
            "ticker": plan.identity.ticker,
            "sector": plan.identity.sector,
            "forbidden_identities": list(plan.identity.forbidden_identities),
        },
        "analytical_framing": plan.analytical_framing,
        "themes": plan.themes,
        "questions": [
            {
                "id": q.id, "theme": q.theme, "question": q.question,
                "rationale": q.rationale, "search_query": q.search_query,
            }
            for q in plan.questions
        ],
        "hypotheses": plan.hypotheses,
        "priority_themes": plan.priority_themes,
        "fallback": plan.fallback,
        "error": plan.error,
    }


def plan_as_markdown(plan: ResearchPlan) -> str:
    """Human-readable plan, mirroring the SRSN example structure."""
    if plan.error and not plan.questions:
        return f"# Research Plan — {plan.ticker} {plan.quarter} {plan.year}\n\n_Plan unavailable: {plan.error}_\n"
    lines = [
        f"# Research Plan — {plan.identity.legal_name} ({plan.ticker}) — {plan.quarter} {plan.year}",
        f"Sector: {plan.identity.sector}",
    ]
    forbid = ", ".join(plan.identity.forbidden_identities) or "(none)"
    lines.append(f"Do not confuse with: {forbid}\n")
    if plan.analytical_framing:
        lines.append("## Analytical Framing\n")
        lines.append(plan.analytical_framing.rstrip() + "\n")
    if plan.themes:
        lines.append("## Themes\n")
        for t in plan.themes:
            lines.append(f"### {t['name']} (priority: {t['priority']})")
            if t.get("rationale"):
                lines.append(f"_Rationale:_ {t['rationale']}\n")
            qs = [q for q in plan.questions if q.theme == t["name"]]
            for q in qs:
                lines.append(f"- **{q.id}.** {q.question}")
                if q.rationale:
                    lines.append(f"  - _Rationale:_ {q.rationale}")
                if q.search_query:
                    lines.append(f"  - _Search:_ `{q.search_query}`")
            lines.append("")
    if plan.hypotheses:
        lines.append("## Hypotheses (ranked)\n")
        for i, h in enumerate(plan.hypotheses, 1):
            lines.append(f"{i}. {h}")
        lines.append("")
    if plan.priority_themes:
        lines.append("## Priority Themes\n")
        for p in plan.priority_themes:
            lines.append(f"- {p}")
        lines.append("")
    if plan.fallback:
        lines.append("> _Note: LLM planner unavailable; questions below are from the deterministic fallback._\n")
    return "\n".join(lines).rstrip() + "\n"


def plan_questions_for_prompt(plan: ResearchPlan) -> str:
    """Compact rendering of the plan for the LLM search loop prompt."""
    if not plan.questions:
        return "(no research questions)"
    parts: list[str] = []
    if plan.analytical_framing:
        parts.append(f"Analytical framing:\n{plan.analytical_framing.strip()}\n")
    if plan.priority_themes:
        parts.append("Priorities: " + ", ".join(plan.priority_themes) + "\n")
    grouped: dict[str, list[PlanQuestion]] = {}
    for q in plan.questions:
        grouped.setdefault(q.theme, []).append(q)
    for t in plan.themes:
        qs = grouped.get(t["name"], [])
        if not qs:
            continue
        parts.append(f"## {t['name']} (priority: {t.get('priority', 'medium')})")
        for q in qs:
            parts.append(f"- [{q.id}] {q.question}")
            if q.search_query:
                parts.append(f"  Suggested search: {q.search_query}")
    if plan.hypotheses:
        parts.append("\nHypotheses to test:")
        for i, h in enumerate(plan.hypotheses, 1):
            parts.append(f"{i}. {h}")
    return "\n".join(parts)


# ── Planner pi-agent config and grounding ──────────────────────────────


def _planner_config() -> dict:
    """The ``analysis.plan`` config block."""
    cfg = opencode_client.load_analysis_config()
    return cfg.get("plan", {}) or {}


def _resolve_planner_pi_cfg() -> dict:
    """Resolve the pi runner config for the planner (mirrors ``write._resolve_pi_cfg``).

    ``model`` falls back to ``models.research_plan``; ``tools`` falls back to the
    read-only set, but an explicit ``[]`` survives (harness-only / ``--no-tools``).
    """
    pi_cfg = dict(_planner_config().get("pi", {}) or {})
    if not pi_cfg.get("provider"):
        pi_cfg["provider"] = "commandcode"
    if not pi_cfg.get("model"):
        models_cfg = opencode_client.load_analysis_config().get("models", {}) or {}
        if models_cfg.get("research_plan"):
            pi_cfg["model"] = models_cfg["research_plan"]
    if pi_cfg.get("tools") is None:
        pi_cfg["tools"] = list(_DEFAULT_PLANNER_TOOLS)
    if pi_cfg.get("timeout_sec") is None:
        pi_cfg["timeout_sec"] = 600
    return pi_cfg


def _planner_tools_section(pi_cfg: dict) -> str:
    """Read-only tool description + facts-authority rule + budgets.

    Returns ``""`` when no tools are enabled (``tools: []`` ⇒ harness-only).
    """
    tools = pi_cfg.get("tools")
    if tools is None:
        tools = _DEFAULT_PLANNER_TOOLS
    enabled = [str(t) for t in tools]
    if not enabled:
        return ""

    max_db = int(pi_cfg.get("max_db_queries", 4))
    max_notes_search = int(pi_cfg.get("max_notes_search", 3))
    max_notes_fetch = int(pi_cfg.get("max_notes_fetch", 3))

    bullets: list[str] = []
    if "read" in enabled:
        bullets.append(
            "- read: read any file listed in the ARTIFACTS ON DISK block (the "
            "canonical fact sheet, computed ratios, company profile, and prior "
            "analyses) when the inline excerpt is not enough."
        )
    if "duckdb_query" in enabled:
        bullets.append(
            "- duckdb_query: read-only SQL over idx_data/pipeline.duckdb. Use it "
            "ONLY for this company's OWN prior periods (and, if useful, same-sector "
            "peer context). NEVER source the current period's company figures from "
            "it - the fact sheet wins, always."
        )
    if "notes_search" in enabled or "notes_fetch" in enabled:
        bullets.append(
            "- notes_search / notes_fetch: vault lookup for prior analyses and "
            "commodity notes that anchor a flagged movement."
        )

    budgets: list[str] = []
    if "duckdb_query" in enabled:
        budgets.append(f"{max_db} duckdb_query")
    if "notes_search" in enabled:
        budgets.append(f"{max_notes_search} notes_search")
    if "notes_fetch" in enabled:
        budgets.append(f"{max_notes_fetch} notes_fetch")

    directive = (
        "Before framing questions, ground the plan: verify any implied period "
        "arithmetic (e.g. Q2 = H1 - Q1) and pull the prior-period baseline so each "
        "question is anchored to a real comparison. Do not search the web - a later "
        "stage owns external evidence. If a tool returns nothing useful, continue "
        "planning; do not stall."
    )
    budget_line = (
        "Budget: at most " + ", ".join(budgets) + " calls - then output ONLY the "
        "YAML plan. The canonical fact sheet and computed ratios win for the "
        "company's current-period figures; any figure pulled via duckdb_query or "
        "the vault is comparative context and must be treated as such."
    )
    return "\n".join(
        ["## Tools (read-only - for context, never for company figures)"]
        + bullets + [directive, budget_line]
    )


def _planner_artifacts_block(ticker: str, year: int, quarter: str) -> str:
    """Absolute on-disk paths so the ``read`` tool has something to read."""
    stem = f"{ticker}_{quarter}_{year}"
    lines = [
        "# ARTIFACTS ON DISK (re-read with the read tool to re-check any input)",
        f"- Canonical fact sheet: {FACTS_DIR / (stem + '.yaml')}",
        f"- Computed ratios:      {FACTS_DIR / (stem + '.ratios.yaml')}",
        f"- Company profile:      {ANALYSIS_DIR / 'companies' / (ticker + '.md')}",
    ]
    for path in _find_prior_analyses(ticker, year, quarter)[:3]:
        lines.append(f"- Prior analysis:       {path}")
    return "\n".join(lines)


def _planner_budget_warnings(pi_meta: dict, pi_cfg: dict) -> list[str]:
    """Post-hoc tool-call counts vs the prompt budgets (shape mirrors write)."""
    tool_calls = dict((pi_meta or {}).get("tool_calls", {}) or {})
    max_db = int(pi_cfg.get("max_db_queries", 4))
    max_notes_search = int(pi_cfg.get("max_notes_search", 3))
    max_notes_fetch = int(pi_cfg.get("max_notes_fetch", 3))

    warnings: list[str] = []
    db_calls = int(tool_calls.get("duckdb_query", 0))
    if db_calls > max_db:
        warnings.append(f"duckdb_query calls {db_calls} exceeded budget {max_db}")
    notes_search = int(tool_calls.get("notes_search", 0))
    if notes_search > max_notes_search:
        warnings.append(
            f"notes_search calls {notes_search} exceeded budget {max_notes_search}"
        )
    notes_fetch = int(tool_calls.get("notes_fetch", 0))
    if notes_fetch > max_notes_fetch:
        warnings.append(
            f"notes_fetch calls {notes_fetch} exceeded budget {max_notes_fetch}"
        )
    return warnings


def _write_planner_trace(
    ticker: str, year: int, quarter: str, result: dict, pi_cfg: dict,
    budget_warnings: list[str],
) -> Path:
    """Persist the planner run trace (pi meta + usage + budget audit)."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    pi_meta = dict(result.get("pi_meta", {}) or {})
    path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.plan_trace.json"
    path.write_text(
        json.dumps({
            "ticker": ticker,
            "quarter": quarter,
            "year": year,
            "pi": pi_meta,
            "usage": result.get("usage", {}) or {},
            "finish_reason": result.get("finish_reason"),
            "tool_calls": dict(pi_meta.get("tool_calls", {}) or {}),
            "budget_warnings": budget_warnings,
            "error": result.get("error"),
            "model": pi_meta.get("model") or pi_cfg.get("model"),
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def _write_raw_plan(
    ticker: str, year: int, quarter: str, raw: str, result: dict,
) -> Path:
    """Persist the raw planner response — written even when the run failed."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    usage = result.get("usage", {}) or {}
    path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_plan_raw.md"
    path.write_text(
        f"# Raw planner response — {ticker} {quarter} {year}\n"
        f"# finish_reason: {result.get('finish_reason') or 'unknown'}\n"
        f"# prompt_tokens: {usage.get('prompt_tokens', 0)}\n"
        f"# completion_tokens: {usage.get('completion_tokens', 0)}\n"
        f"# reasoning_tokens: {usage.get('reasoning_tokens', 0)}\n"
        f"# cost_usd: {usage.get('cost_usd', 0.0)}\n"
        f"# error: {result.get('error') or '(none)'}\n\n"
        f"```\n{raw}\n```\n",
        encoding="utf-8",
    )
    return path


# ── Public entry point ──────────────────────────────────────────────────


def formulate_plan(
    ticker: str, year: int, quarter: str, events: str,
    *,
    identity: research_questions.CompanyIdentity | None = None,
    deterministic_fallback: list[research_questions.ResearchQuestion] | None = None,
    max_analysis_chars: int | None = None,
) -> ResearchPlan:
    """Plan via a headless pi agent (read-only tools, no web).

    Falls back to the deterministic question list on missing model, an agent
    failure, or an unparseable/empty plan. Every run writes a plan trace and a
    raw-response artifact — including failures."""
    if identity is None:
        identity = research_questions.resolve_identity(ticker)

    pi_cfg = _resolve_planner_pi_cfg()
    model = pi_cfg.get("model")
    if not model:
        return _fallback_plan(
            ticker, year, quarter, identity, deterministic_fallback,
            reason="no research_plan model configured",
        )

    user = build_planner_context(
        ticker, year, quarter, events, identity,
        max_analysis_chars=max_analysis_chars or 12000,
    )
    user += "\n\n" + _planner_artifacts_block(ticker, year, quarter)
    system = _PLANNER_SYSTEM_PROMPT
    tools_section = _planner_tools_section(pi_cfg)
    if tools_section:
        system += "\n\n" + tools_section

    n_tools = len([t for t in (pi_cfg.get("tools") or [])])
    print(
        f"  [RESEARCH PLAN] pi_agent model={model} tools={n_tools} "
        f"(prompt ~{len(user) // 4} tok, timeout={pi_cfg.get('timeout_sec', 600)}s)",
        flush=True,
    )

    run_dir = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.plan_pi"
    result = pi_agent.run_agent(system, user, cfg=pi_cfg, run_dir=run_dir)

    raw = result.get("content", "") or ""
    pi_meta = dict(result.get("pi_meta", {}) or {})
    budget_warnings = _planner_budget_warnings(pi_meta, pi_cfg)
    trace_path = _write_planner_trace(
        ticker, year, quarter, result, pi_cfg, budget_warnings,
    )
    _write_raw_plan(ticker, year, quarter, raw, result)

    usage = result.get("usage", {}) or {}
    print(
        f"  [RESEARCH PLAN] done: finish={result.get('finish_reason')} "
        f"tool_calls={pi_meta.get('tool_calls', {}) or {}} "
        f"out={len(raw)} chars reasoning={usage.get('reasoning_tokens', 0)} "
        f"cost=${usage.get('cost_usd', 0.0):.4f}",
        flush=True,
    )

    plan = parse_plan(raw, ticker, year, quarter, identity)
    if plan.error or not plan.questions:
        reason = plan.error or "planner produced no questions"
        if result.get("finish_reason") in {"length", "max_tokens"}:
            reason = (
                f"planner response truncated at max_tokens after "
                f"{usage.get('reasoning_tokens', 0)} reasoning tokens; {reason}"
            )
        if result.get("error"):
            reason = f"planner agent failed: {result['error']}"
        print(f"  [PLAN] FALLBACK: {reason}", flush=True)
        plan = _fallback_plan(
            ticker, year, quarter, identity, deterministic_fallback, reason=reason,
        )

    plan.pi_meta = pi_meta
    plan.usage = usage
    plan.plan_trace_path = str(trace_path)
    return plan


def _fallback_plan(
    ticker: str, year: int, quarter: str,
    identity: research_questions.CompanyIdentity,
    deterministic_fallback: list[research_questions.ResearchQuestion] | None,
    *,
    reason: str,
) -> ResearchPlan:
    qs: list[PlanQuestion] = []
    if deterministic_fallback:
        for i, rq in enumerate(deterministic_fallback, 1):
            qs.append(PlanQuestion(
                id=rq.question_id,
                theme="Detected events",
                question=rq.question,
                search_query=rq.search_query,
            ))
    return ResearchPlan(
        ticker=ticker, year=year, quarter=quarter, identity=identity,
        analytical_framing="(LLM planner unavailable; using deterministic questions.)",
        themes=[{"name": "Detected events", "priority": "medium", "rationale": reason}],
        questions=qs,
        hypotheses=[],
        priority_themes=["Detected events"] if qs else [],
        fallback=True,
        error=reason,
    )


def save_plan_artifacts(plan: ResearchPlan, *, yaml_path: Path, md_path: Path) -> None:
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(
        yaml.dump(plan_as_dicts(plan), allow_unicode=True, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    md_path.write_text(plan_as_markdown(plan), encoding="utf-8")
