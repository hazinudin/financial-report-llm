"""
Stage 3b — External Research

Inserted between context.py and write.py. Performs:
  1. Deterministic triggers over facts + ratios → events needing explanation
  2. Research-plan formulation (writer model reads analyses + facts, produces
     a themed, prioritized plan with questions + hypotheses BEFORE searching)
  3. Web search via tool-call loop, guided by the plan
  4. Synthesize research brief YAML + Markdown

research_plan.py formulates the plan (LLM); research_questions.py supplies the
deterministic fallback questions. Web search tools (web_search, fetch_web_page)
are exposed to the LLM through an OpenAI-compatible tool-calling loop. The
dispatcher enforces budgets, domain allow-lists, and URL safety rules.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import yaml

import opencode_client
import tool_registry
import search_backend
import research_questions
import research_plan

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_IDX_DATA = _PROJECT_ROOT / "idx_data"
RESEARCH_DIR = _IDX_DATA / "analysis" / "research"
FACTS_DIR = _IDX_DATA / "analysis" / "facts"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
RAG_SCRIPT = _PROJECT_ROOT / "scripts" / "notes_rag" / "rag.py"


def _research_config() -> dict:
    cfg = opencode_client.load_analysis_config()
    return cfg.get("research", {}) or {}


def _load_research_brief(ticker: str, year: int, quarter: str) -> dict | None:
    path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research.yaml"
    if not path.exists():
        return None
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return None


def _save_research_brief(ticker: str, year: int, quarter: str, brief: dict) -> Path:
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    yaml_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research.yaml"
    yaml_path.write_text(
        yaml.dump(brief, allow_unicode=True, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    return yaml_path


def _build_research_prompt(
    ticker: str, year: int, quarter: str, facts_summary: str, events: str,
    questions: str, identity: research_questions.CompanyIdentity,
) -> list[dict[str, Any]]:
    system = (
        "You are a financial research analyst. You have access to web_search "
        "and fetch_web_page tools to find external context for financial events.\n\n"
        "Rules:\n"
        "1. For each external event, use web_search to find relevant news. "
        "Use fetch_web_page only for authoritative sources when snippets are insufficient.\n"
        "2. If an event is purely internal (answer found in company notes), "
        "do NOT search for it — list it under internal_must_explain.\n"
        "3. Cite ONLY URLs returned by web_search or fetch_web_page tool results. "
        "Never construct, guess, or recall a URL. If no tool result supports a claim, "
        "use sources: [] and confidence: low.\n"
        "4. Company identity is authoritative. Do not substitute another ticker or "
        "company name. Never use sources for a forbidden identity.\n"
        "5. Financial numbers come from the fact sheet, not the web.\n"
        "6. Label each finding as in-period or post-period.\n"
        "7. You are given a THEMED research plan with question IDs (e.g. A1, B2). "
        "Every question has an LLM-issued question_id and theme. Address the plan "
        "questions in priority order; tag each finding with the question ID(s) it "
        "answers under `writer_hint`. Do not introduce free-text questions.\n"
        "8. After you have gathered sufficient evidence, output ONLY valid YAML "
        "in this exact structure (no markdown, no extra text before or after):\n\n"
        "findings:\n"
        "  - event: \"...\"\n"
        "    explanation: \"...\"\n"
        "    confidence: high|medium|low\n"
        "    period: in-period|post-period\n"
        "    sources:\n"
        "      - title: \"...\"\n"
        "        url: \"...\"\n"
        "        published_date: \"...\"\n"
        "    writer_hint: \"...\"\n"
        "internal_must_explain:\n"
        "  - \"...\""
    )
    user = (
        f"Analyze these events for {ticker} {quarter} {year}:\n\n"
        f"Company identity:\n"
        f"- Legal name: {identity.legal_name}\n"
        f"- Ticker: {identity.ticker}\n"
        f"- Sector: {identity.sector}\n"
        f"- Do not confuse with: {', '.join(identity.forbidden_identities) or '(none)'}\n\n"
        f"Financial context:\n{facts_summary[:2000]}\n\n"
        f"Events needing explanation:\n{events[:2000]}\n\n"
        f"Bounded research questions:\n{questions[:4000]}\n\n"
        "Search for external context. Synthesize findings. "
        "Output ONLY valid YAML in the specified structure."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _normalise_source_url(url: str) -> str:
    """Normalize URLs for provenance matching without changing stored citations."""
    try:
        parsed = urlparse(str(url).strip())
        return urlunparse((
            parsed.scheme.lower(), (parsed.netloc or "").lower(),
            parsed.path.rstrip("/") or "/", "", parsed.query, "",
        ))
    except ValueError:
        return ""


def _trace_source_urls(tool_trace: list[dict[str, Any]]) -> set[str]:
    urls: set[str] = set()
    for entry in tool_trace:
        result = entry.get("result") or {}
        if entry.get("tool_name") == "web_search":
            for item in result.get("results", []) or []:
                normalized = _normalise_source_url(item.get("url", ""))
                if normalized:
                    urls.add(normalized)
        elif entry.get("tool_name") == "fetch_web_page":
            if result.get("status") != "ok":
                continue
            for key in ("url", "canonical_url"):
                normalized = _normalise_source_url(result.get(key, ""))
                if normalized:
                    urls.add(normalized)
    return urls


def _validate_brief_sources(
    brief: dict[str, Any], tool_trace: list[dict[str, Any]], *, no_tool_calls: bool,
) -> dict[str, Any]:
    """Remove model-invented citations and return a provenance audit."""
    traced_urls = _trace_source_urls(tool_trace)
    unverified: list[dict[str, str]] = []
    verified_count = 0
    findings = brief.get("findings", [])
    if not isinstance(findings, list):
        findings = []
        brief["findings"] = findings

    for finding in findings:
        if not isinstance(finding, dict):
            continue
        sources = finding.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        valid_sources = []
        for source in sources:
            if not isinstance(source, dict):
                continue
            normalized = _normalise_source_url(source.get("url", ""))
            if normalized and normalized in traced_urls:
                valid_sources.append(source)
                verified_count += 1
            else:
                unverified.append({
                    "event": str(finding.get("event", "")),
                    "url": str(source.get("url", "")),
                })
        finding["sources"] = valid_sources
        if sources and not valid_sources:
            finding["confidence"] = "low"
            finding["source_audit"] = "unverified_sources_removed"
        if no_tool_calls and not valid_sources:
            finding["source_audit"] = "no_tool_calls"

    return {
        "traced_url_count": len(traced_urls),
        "verified_source_count": verified_count,
        "unverified_sources": unverified,
        "no_tool_calls": no_tool_calls,
        "passed": not unverified and not (no_tool_calls and bool(findings)),
    }


def _ratio_value(ratio_entry) -> float | None:
    """Extract numeric value from a ratio entry (dict with .value or raw float)."""
    if isinstance(ratio_entry, dict):
        return ratio_entry.get("value")
    if isinstance(ratio_entry, (int, float)):
        return float(ratio_entry)
    return None


def _build_facts_summary(ticker: str, year: int, quarter: str) -> str:
    facts_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    if not facts_path.exists():
        return "(No fact sheet available)"
    try:
        facts = yaml.safe_load(facts_path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return "(Could not read fact sheet)"

    ratios_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"
    ratios = {}
    if ratios_path.exists():
        try:
            ratios = yaml.safe_load(ratios_path.read_text(encoding="utf-8")) or {}
        except (yaml.YAMLError, OSError):
            pass

    summary_parts = []
    for stmt_name in ("income_statement", "balance_sheet", "cash_flow"):
        stmt = facts.get(stmt_name) or {}
        lines = stmt.get("lines", {}) or {}
        if lines:
            items = [(k, v) for k, v in lines.items() if v is not None][:6]
            if items:
                summary_parts.append(f"{stmt_name}: {json.dumps(dict(items), ensure_ascii=False, default=str)}")

    if ratios:
        flat = {}
        for k, v in ratios.items():
            val = _ratio_value(v)
            if val is not None:
                flat[k] = round(val, 4)
        if flat:
            r_items = list(flat.items())[:10]
            summary_parts.append(f"Ratios: {json.dumps(dict(r_items), ensure_ascii=False)}")

    return "\n".join(summary_parts) if summary_parts else "(No financial data)"


def _detect_events(ticker: str, year: int, quarter: str) -> str:  # noqa: ARG001
    cfg = _research_config()
    triggers_cfg = cfg.get("triggers", {}) or {}
    facts_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    ratios_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"

    facts = {}
    ratios = {}
    try:
        if facts_path.exists():
            facts = yaml.safe_load(facts_path.read_text(encoding="utf-8")) or {}
        if ratios_path.exists():
            ratios = yaml.safe_load(ratios_path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        pass

    events: list[str] = []
    margin_bps = triggers_cfg.get("margin_bps", 500)

    for key, label in [
        ("gross_margin", "Gross Margin"),
        ("net_margin", "Net Margin"),
    ]:
        entry = ratios.get(key)
        val = _ratio_value(entry)
        if val is not None and abs(val) > margin_bps / 10000:
            direction = "increased" if val > 0 else "decreased"
            events.append(
                f"{label} {direction} from prior period — external drivers?"
            )

    bs_move_pct = triggers_cfg.get("balance_sheet_move_pct", 30) / 100.0
    for stmt_name, stmt_label in [
        ("balance_sheet", "Balance-sheet"),
        ("income_statement", "Income-statement"),
    ]:
        stmt = facts.get(stmt_name, {}) or {}
        lines = stmt.get("lines", {}) or {}
        prior = stmt.get("prior_lines", {}) or {}
        for acct, curr_val in lines.items():
            if not isinstance(curr_val, (int, float)):
                continue
            prior_val = prior.get(acct)
            if prior_val and isinstance(prior_val, (int, float)) and prior_val != 0:
                change = (curr_val - prior_val) / abs(prior_val)
                if abs(change) > bs_move_pct:
                    direction = "increased" if change > 0 else "decreased"
                    events.append(
                        f"{stmt_label} line '{acct}' {direction} "
                        f"{abs(change)*100:.0f}% vs prior period"
                    )

    return "\n".join(f"- {e}" for e in events) if events else "(No material events detected)"


def _run_tool_loop(
    ticker: str, year: int, quarter: str, tool_mode: str,
    refresh_cache: bool = False,
) -> dict[str, Any]:
    cfg = _research_config()
    model_cfg = opencode_client.parse_model_settings("research")
    model = model_cfg.get("model")
    if not model:
        return {
            "brief": {"findings": [], "internal_must_explain": []},
            "mode": "deterministic_fallback",
            "search_count": 0,
            "fetch_count": 0,
            "backend_health": search_backend.check_health(),
            "error": "no research model configured",
        }

    budget = tool_registry.BudgetState(
        max_searches=int(cfg.get("max_search_calls", 36)),
        max_fetches=int(cfg.get("max_fetch_calls", 8)),
        force_refresh=refresh_cache,
    )

    facts_summary = _build_facts_summary(ticker, year, quarter)
    events = _detect_events(ticker, year, quarter)
    identity, deterministic_qs = research_questions.build_questions(
        ticker, year, quarter, events,
        max_questions=int(cfg.get("max_questions", 12)),
    )

    if tool_mode == "deterministic_fallback":
        plan = research_plan._fallback_plan(
            ticker, year, quarter, identity, deterministic_qs,
            reason="deterministic_fallback tool mode (search disabled)",
        )
    else:
        plan = research_plan.formulate_plan(
            ticker, year, quarter, events,
            identity=identity, deterministic_fallback=deterministic_qs,
            max_analysis_chars=int(cfg.get("max_planner_analysis_chars", 12000)),
        )

    plan_prompt = research_plan.plan_questions_for_prompt(plan)
    messages = _build_research_prompt(
        ticker, year, quarter, facts_summary, events, plan_prompt, identity,
    )

    if tool_mode == "deterministic_fallback":
        return {
            "brief": {
                "findings": [],
                "internal_must_explain": [],
                "meta": {
                    "ticker": ticker, "quarter": quarter, "year": year,
                    "tool_mode": "deterministic_fallback",
                    "search_count": 0, "fetch_count": 0,
                    "plan_fallback": plan.fallback,
                    "plan_error": plan.error,
                },
            },
            "mode": "deterministic_fallback",
            "search_count": 0,
            "fetch_count": 0,
            "backend_health": search_backend.check_health(),
            "questions": research_questions.questions_as_dicts(deterministic_qs),
            "plan": research_plan.plan_as_dicts(plan),
            "plan_obj": plan,
        }

    def _executor(name: str, args: dict) -> dict:
        return tool_registry.execute_tool(name, args, budget)

    print(
        f"  [RESEARCH] starting synthesis {model} with {len(plan.questions)} "
        f"plan questions (max_tool_rounds={int(cfg.get('max_tool_rounds', 6))})",
        flush=True,
    )
    result = opencode_client.chat_with_tools(
        model=model,
        messages=messages,
        tools=tool_registry.TOOL_SCHEMAS,
        tool_executor=_executor,
        max_tool_rounds=int(cfg.get("max_tool_rounds", 6)),
        max_parallel_calls=int(cfg.get("max_parallel_calls_per_round", 8)),
        temperature=model_cfg.get("temperature", 0.0),
        max_tokens=model_cfg.get("max_tokens", 4096),
        reasoning_effort=model_cfg.get("reasoning_effort"),
        timeout_sec=model_cfg.get("timeout_sec"),
    )

    brief: dict[str, Any]
    raw_content = result.get("content", "")
    try:
        stripped = raw_content.strip()
        if stripped.startswith("```"):
            lines = stripped.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            stripped = "\n".join(lines)
        brief = yaml.safe_load(stripped) or {}
    except (yaml.YAMLError, Exception):
        brief = {}
    if not isinstance(brief, dict):
        brief = {}

    no_tool_calls = not result.get("tool_rounds", 0) and not result.get("tool_trace")
    if no_tool_calls and result.get("mode") == "llm_tools":
        result["mode"] = "llm_tools_no_calls"
    source_audit = _validate_brief_sources(
        brief, result.get("tool_trace", []), no_tool_calls=no_tool_calls,
    )
    brief["meta"] = {
        "ticker": ticker,
        "quarter": quarter,
        "year": year,
        "tool_mode": result.get("mode", "llm_tools"),
        "search_count": budget.search_count,
        "fetch_count": budget.fetch_count,
        "tool_rounds": result.get("tool_rounds", 0),
        "finish_reason": result.get("finish_reason"),
        "tool_error": result.get("error"),
        "backend_health": search_backend.check_health(),
        "source_audit": source_audit,
    }
    if not brief.get("findings"):
        brief["findings"] = []
    if not brief.get("internal_must_explain"):
        brief["internal_must_explain"] = []

    return {
        "brief": brief,
        "mode": result.get("mode", "llm_tools"),
        "search_count": budget.search_count,
        "fetch_count": budget.fetch_count,
        "backend_health": search_backend.check_health(),
        "questions": [
            {
                "id": q.id, "theme": q.theme, "question": q.question,
                "rationale": q.rationale, "search_query": q.search_query,
            }
            for q in plan.questions
        ],
        "plan": research_plan.plan_as_dicts(plan),
        "plan_obj": plan,
        "tool_trace": result.get("tool_trace", []),
        "token_usage": result.get("usage", {}),
        "finish_reason": result.get("finish_reason"),
        "error": result.get("error"),
        "raw_content": raw_content,
    }


def run(
    ticker: str,
    year: int,
    quarter: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    skip_search: bool = False,
    tool_mode: str | None = None,
    refresh_cache: bool = False,
) -> dict[str, Any]:
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    existing = _load_research_brief(ticker, year, quarter)
    if existing and not force:
        return {"status": "skipped", "brief": existing}

    search_backend.SEARCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    search_backend.FETCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cfg = _research_config()
    effective_mode = tool_mode or cfg.get("tool_mode", "llm_tools")

    if skip_search:
        effective_mode = "deterministic_fallback"

    if dry_run:
        return {"status": "dry", "ticker": ticker, "year": year, "quarter": quarter}

    result = _run_tool_loop(
        ticker, year, quarter, effective_mode, refresh_cache=refresh_cache,
    )
    brief = result["brief"]

    _save_research_brief(ticker, year, quarter, brief)

    plan_obj = result.get("plan_obj")
    if plan_obj is not None:
        plan_yaml_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_plan.yaml"
        plan_md_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_plan.md"
        research_plan.save_plan_artifacts(
            plan_obj, yaml_path=plan_yaml_path, md_path=plan_md_path,
        )

    raw_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_raw.md"
    raw_content = result.get("raw_content", "")
    raw_path.write_text(
        f"# Raw LLM response — {ticker} {quarter} {year}\n\n"
        f"```\n{raw_content}\n```\n",
        encoding="utf-8",
    )
    trace_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_trace.json"
    trace_path.write_text(
        json.dumps({
            "ticker": ticker,
            "quarter": quarter,
            "year": year,
            "mode": result.get("mode"),
            "backend_health": result.get("backend_health", search_backend.check_health()),
            "questions": result.get("questions", []),
            "plan": result.get("plan", {}),
            "tool_trace": result.get("tool_trace", []),
            "usage": result.get("token_usage", {}),
            "finish_reason": result.get("finish_reason"),
            "search_count": result.get("search_count", 0),
            "fetch_count": result.get("fetch_count", 0),
            "error": result.get("error"),
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    n_plan_qs = len(plan_obj.questions) if plan_obj else 0
    print(
        f"  [RESEARCH] mode={result['mode']}, "
        f"searches={result['search_count']}, "
        f"fetches={result['fetch_count']}, "
        f"findings={len(brief.get('findings', []))}, "
        f"internal={len(brief.get('internal_must_explain', []))}, "
        f"plan_questions={n_plan_qs}"
        + (" (fallback)" if plan_obj and plan_obj.fallback else "")
    )
    return {
        "status": "success",
        "brief": brief,
        **{k: v for k, v in result.items() if k != "brief"},
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 3b external research")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-search", action="store_true")
    p.add_argument("--research-tool-mode", choices=["llm_tools", "deterministic_fallback"], default=None)
    p.add_argument("--refresh-research-cache", action="store_true")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    print(run(
        args.ticker, args.year, args.quarter,
        dry_run=args.dry_run,
        skip_search=args.no_search,
        tool_mode=args.research_tool_mode,
        refresh_cache=args.refresh_research_cache,
        force=args.force,
    ))
