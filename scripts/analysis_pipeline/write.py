"""
Stage 4 — writer agent (headless pi, strongest model).

Assembles the writer input package (kept <= ~30K tokens): writer system prompt
+ the 14-section skeleton + the canonical fact sheet + computed ratios +
static company profile + research brief + <= 3 vault hits + the market-data
stub + an ``# ARTIFACTS ON DISK`` block. Then runs a headless
``pi -p --mode json`` agent (the shared ``pi_agent.run_agent``) with a
**read-only** tool allowlist (``read``, ``duckdb_query``, ``notes_search``,
``notes_fetch``) so the model can re-check inputs, ground peer comparisons, and
trade the vault for prior-period figures before writing. The final assistant
message is the analysis markdown, written to analysis/; the run directory and a
``write_trace.json`` audit artifact record the tool trace, usage/cost, and
budget counts.

Style rules mirror the existing human analyses (frontmatter tags, wikilinks,
tables, severity-ranked risks, disclaimer) via writer_system.md.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

import opencode_client
import pi_agent
from schema import FactSheet

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RESEARCH_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "research"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
WRITE_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "write"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
ANALYSIS_DIR = _PROJECT_ROOT / "analysis"

_DEFAULT_WRITE_TOOLS = ["read", "duckdb_query", "notes_search", "notes_fetch"]


def load_writer_prompt() -> str:
    p = PROMPTS_DIR / "writer_system.md"
    return p.read_text(encoding="utf-8") if p.exists() else ""


def _write_config() -> dict:
    """The ``analysis.write`` config block (mirrors ``research._research_config``)."""
    cfg = opencode_client.load_analysis_config()
    return cfg.get("write", {}) or {}


def _resolve_pi_cfg(cfg: dict) -> dict:
    """Resolve the pi runner config for the writer.

    ``model`` falls back to ``models.write`` (the canonical writer model, mirrors
    ``research._run_pi_agent``); ``tools`` falls back to the read-only set, but an
    explicit ``[]`` survives (it means ``--no-tools`` / harness-only).
    """
    pi_cfg = dict(cfg.get("pi", {}) or {})
    if not pi_cfg.get("provider"):
        pi_cfg["provider"] = "opencode-go"
    if not pi_cfg.get("model"):
        models_cfg = opencode_client.load_analysis_config().get("models", {}) or {}
        if models_cfg.get("write"):
            pi_cfg["model"] = models_cfg["write"]
    if pi_cfg.get("tools") is None:
        pi_cfg["tools"] = list(_DEFAULT_WRITE_TOOLS)
    if pi_cfg.get("timeout_sec") is None:
        pi_cfg["timeout_sec"] = 1800
    return pi_cfg


def _tools_section(pi_cfg: dict) -> str:
    """A short markdown section describing the enabled read-only tools + budgets.

    Returns ``""`` when no tools are enabled (``tools: []`` ⇒ harness-only run).
    """
    tools = pi_cfg.get("tools")
    if tools is None:
        tools = _DEFAULT_WRITE_TOOLS
    enabled = [str(t) for t in tools]
    if not enabled:
        return ""

    max_db = int(pi_cfg.get("max_db_queries", 6))
    max_notes_search = int(pi_cfg.get("max_notes_search", 4))
    max_notes_fetch = int(pi_cfg.get("max_notes_fetch", 4))

    bullets: list[str] = []
    if "read" in enabled:
        bullets.append(
            "- read: re-read any input file listed in the ARTIFACTS ON DISK block."
        )
    if "duckdb_query" in enabled:
        bullets.append(
            "- duckdb_query: read-only SQL over idx_data/pipeline.duckdb. Use ONLY "
            "for (a) peer comparison (financial_ratios: per, pbv, roe, roa, der by "
            "code+period; values in billions IDR except eps) and (b) this company's "
            "OWN prior periods. NEVER source the current-period company figures in "
            "this document from duckdb_query — the fact sheet wins, always. If a DB "
            "figure contradicts the fact sheet for the current period, use the fact "
            "sheet and flag the discrepancy."
        )
    if "notes_search" in enabled or "notes_fetch" in enabled:
        bullets.append(
            "- notes_search / notes_fetch: vault search for prior analyses / "
            "commodity notes; prefer these over the 3 snippet hits when you need a "
            "prior-quarter number or a full cited section."
        )

    budgets: list[str] = []
    if "duckdb_query" in enabled:
        budgets.append(f"{max_db} duckdb_query")
    if "notes_search" in enabled:
        budgets.append(f"{max_notes_search} notes_search")
    if "notes_fetch" in enabled:
        budgets.append(f"{max_notes_fetch} notes_fetch")
    directive = (
        "Ground the comparison sections before drafting: run one duckdb_query "
        "for peer multiples (same sector/commodity peers, latest available "
        "period) to support §6/§8, and use notes_search/notes_fetch when a "
        "prior-quarter figure would anchor a trend in §9. If a tool returns "
        "nothing useful, continue drafting — do not stall. The DB universe is "
        "small (only tickers this pipeline has ingested): confirm a peer code "
        "exists (e.g. SELECT DISTINCT code, name FROM financial_ratios WHERE "
        "name ILIKE '%gold%') before quoting it, and omit the comp rather than "
        "guessing if it does not."
    )
    budget_line = (
        "Budget: at most " + ", ".join(budgets) + " calls — then write. "
        "The fact sheet and computed ratios win for the company's current-period "
        "figures. Label every non-fact-sheet figure with its source "
        '("per pipeline DB", "[[note]]"). In "Related Analysis", prefer wikilinks '
        "to notes you actually read or that appear in VAULT CONTEXT."
    )
    return "\n".join(
        ["## Tools (read-only — for context, never for company figures)"]
        + bullets + [directive, budget_line]
    )


def _artifacts_block(ticker: str, year: int, quarter: str) -> str:
    """Absolute on-disk paths so the ``read`` tool has something to read."""
    stem = f"{ticker}_{quarter}_{year}"
    facts = FACTS_DIR / f"{stem}.yaml"
    ratios = FACTS_DIR / f"{stem}.ratios.yaml"
    brief = RESEARCH_DIR / f"{stem}.research.yaml"
    plan = RESEARCH_DIR / f"{stem}.research_plan.yaml"
    context = RUNS_DIR / f"{ticker}_{year}_{quarter}.context.json"
    return (
        "# ARTIFACTS ON DISK (re-read with the read tool to re-check any input)\n"
        f"- Canonical fact sheet: {facts}\n"
        f"- Computed ratios:      {ratios}\n"
        f"- Research brief:       {brief}\n"
        f"- Research plan:        {plan}\n"
        f"- Context package:      {context}"
    )


def _strip_outer_fence(text: str) -> str:
    """Strip ``` fences only when the *entire* text is wrapped.

    Same logic as the research runner's inline stripping: an inner fenced block
    inside a markdown document (which does not start with a fence) is preserved.
    """
    stripped = (text or "").strip()
    if not stripped.startswith("```"):
        return text
    lines = stripped.split("\n")
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines)


def _budget_warnings(pi_meta: dict, pi_cfg: dict) -> list[str]:
    """Post-hoc tool-call counts vs the prompt budgets (same shape as research)."""
    tool_calls = dict((pi_meta or {}).get("tool_calls", {}) or {})
    max_db = int(pi_cfg.get("max_db_queries", 6))
    max_notes_search = int(pi_cfg.get("max_notes_search", 4))
    max_notes_fetch = int(pi_cfg.get("max_notes_fetch", 4))

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


def _run_pi_writer(user: str, *, pi_cfg: dict, run_dir: Path) -> tuple[str, dict]:
    """Run the writer pi agent; raise on failure, return ``(content, result)``."""
    system = load_writer_prompt() + "\n\n" + _tools_section(pi_cfg)
    result = pi_agent.run_agent(system, user, cfg=pi_cfg, run_dir=run_dir)
    if result.get("error"):
        raise RuntimeError(f"pi writer failed: {result['error']}")
    content = _strip_outer_fence(result.get("content", "")).strip()
    if not content:
        raise RuntimeError("pi writer produced an empty document")
    return content, result


def _load_yaml_mapping(path: Path) -> dict:
    """Load a research artifact without making writing depend on it."""
    if not path.exists():
        return {}
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return value if isinstance(value, dict) else {}


def _load_research_context(ticker: str, year: int, quarter: str) -> str:
    """Render bounded, provenance-aware research context for the writer.

    The raw planner response and tool trace are intentionally excluded. The
    writer receives the structured plan, findings, confidence/period labels,
    source URLs, and unresolved internal items instead.
    """
    stem = f"{ticker}_{quarter}_{year}"
    plan = _load_yaml_mapping(RESEARCH_DIR / f"{stem}.research_plan.yaml")
    brief = _load_yaml_mapping(RESEARCH_DIR / f"{stem}.research.yaml")
    if not plan and not brief:
        return "(no external research available)"

    plan_context = {
        "analytical_framing": plan.get("analytical_framing", ""),
        "priority_themes": plan.get("priority_themes", []) or [],
        "themes": [
            {
                "name": theme.get("name", ""),
                "priority": theme.get("priority", "medium"),
                "rationale": theme.get("rationale", ""),
            }
            for theme in plan.get("themes", []) or []
            if isinstance(theme, dict)
        ],
        "questions": [
            {
                "id": question.get("id", ""),
                "theme": question.get("theme", ""),
                "question": question.get("question", ""),
            }
            for question in plan.get("questions", []) or []
            if isinstance(question, dict)
        ],
        "hypotheses": plan.get("hypotheses", []) or [],
    }

    findings = []
    for finding in brief.get("findings", []) or []:
        if not isinstance(finding, dict):
            continue
        sources = [
            {
                "title": source.get("title", ""),
                "url": source.get("url", ""),
                "published_date": source.get("published_date"),
            }
            for source in finding.get("sources", []) or []
            if isinstance(source, dict) and source.get("url")
        ]
        findings.append({
            "event": finding.get("event", ""),
            "explanation": finding.get("explanation", ""),
            "confidence": finding.get("confidence", "low"),
            "period": finding.get("period", "in-period"),
            "sources": sources,
            "writer_hint": finding.get("writer_hint", ""),
        })

    research_context = {
        "usage": (
            "Planning and external evidence only. The canonical fact sheet and "
            "computed ratios remain authoritative for company financial figures."
        ),
        "plan": plan_context,
        "findings": findings,
        "internal_must_explain": [
            item for item in brief.get("internal_must_explain", []) or []
            if isinstance(item, str)
        ],
        "source_audit": (brief.get("meta", {}) or {}).get("source_audit", {}),
    }
    return yaml.safe_dump(
        research_context, sort_keys=False, allow_unicode=True,
    )


def _build_user(ticker, year, quarter, sheet: FactSheet, ratios: dict, package: dict) -> str:
    facts_yaml = yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True)
    ratios_yaml = yaml.safe_dump(ratios, sort_keys=False, allow_unicode=True)
    profile = package.get("profile", "")
    skeleton = package.get("skeleton", "")
    md = package.get("market_data", {}) or {}
    hits = package.get("vault_hits", []) or []
    research = package.get("research_context", "(no external research available)")

    md_block = yaml.safe_dump(md, sort_keys=False, allow_unicode=True) if md else "(no market data provided)"
    hits_block = "\n".join(
        f"- [[{h.get('path','').replace('analysis/','').replace('.md','')}]] "
        f"({h.get('section') or ''}): {h.get('snippet','')[:300]}"
        for h in hits
    ) or "(no vault hits)"
    title = f"{ticker} {quarter} {year} Analysis"

    return f"""# TASK
Write a complete equity-analysis markdown document titled "{title}", following
the skeleton below EXACTLY (all sections, in order). Populate every section from
the canonical facts, ratios, and research context provided. You are the analyst;
synthesize, but you may ONLY use company numbers from the fact sheet or ratios,
valuation numbers from the market-data stub, and explicitly cited external
figures from the research context.
Do not invent figures. Where a ratio is null/missing, say so rather than estimating.

# OUTLINE SKELETON (follow this structure)
{skeleton}

# STATIC COMPANY PROFILE
{profile}

# CANONICAL FACT SHEET (single source of truth for all numbers)
{facts_yaml}

# COMPUTED RATIOS (deterministic — use these verbatim)
{ratios_yaml}

# RESEARCH PLAN AND EXTERNAL RESEARCH
{research}

# VAULT CONTEXT (prior analyses / commodity notes — cite as wikilinks)
{hits_block}

# MARKET DATA STUB (use these for valuation figures; cite source/date)
{md_block}

# OUTPUT RULES
- Begin with YAML frontmatter: tags: ["#{ticker}", "#earnings", "#{quarter}{year}"]
- Put related-note wikilinks on the line after the frontmatter and after the H1.
- Use markdown tables for comparisons (Q1 vs prior, current vs prior_fy_end).
- Severity-rank risks (HIGH/MODERATE/LOW).
- End with a "Related Analysis" section (wikilinks from vault hits) and a disclaimer.
- Emit ONLY the markdown document. Target 600-900 lines of dense, analyst-grade prose.
"""


def run(ticker: str, year: int, quarter: str, market_data_path: str | None = None,
        force: bool = False, dry_run: bool = False, inject_redflags: str | None = None) -> dict:
    out_path = ANALYSIS_DIR / f"{ticker} {quarter} {year} Analysis.md"
    if out_path.exists() and not force and not dry_run:
        print(f"  [SKIP] {out_path.name} (already written)")
        return {"status": "skipped", "output": str(out_path)}

    facts_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    ratios_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"
    if not facts_path.exists():
        if dry_run:
            print(f"  [DRY] would write {out_path.name} (canonical fact sheet absent)")
            return {"status": "dry"}
        raise FileNotFoundError(f"Canonical fact sheet missing: {facts_path}")
    sheet = FactSheet.model_validate(yaml.safe_load(facts_path.read_text(encoding="utf-8")))
    ratios = yaml.safe_load(ratios_path.read_text(encoding="utf-8")) if ratios_path.exists() else {}

    # Load context package (replay from disk for --write-only, else build now)
    ctx_path = RUNS_DIR / f"{ticker}_{year}_{quarter}.context.json"
    if ctx_path.exists():
        package = json.loads(ctx_path.read_text(encoding="utf-8"))
    else:
        from context import run as ctx_run  # local import to avoid cycle
        package = ctx_run(ticker, year, quarter, market_data_path)["package"]
    package["research_context"] = _load_research_context(ticker, year, quarter)

    cfg = _write_config()
    pi_cfg = _resolve_pi_cfg(cfg)
    model = pi_cfg.get("model")
    if not model:
        raise RuntimeError("config.yaml: analysis.models.write not set (run --list-models)")

    user = _build_user(ticker, year, quarter, sheet, ratios, package)
    user += "\n\n" + _artifacts_block(ticker, year, quarter)
    if inject_redflags:
        user += f"\n\n# RED FLAGS TO ADDRESS EXPLICITLY\n{inject_redflags}\n\nAddress each of these in the relevant section (do not just append them)."

    if dry_run:
        print(f"  [DRY] would write {out_path.name} with {model} "
              f"(facts={sum(1 for v in sheet.income_statement.lines.values() if v is not None)} IS lines, "
              f"user_chars={len(user)})")
        return {"status": "dry", "model": model, "user_chars": len(user)}

    run_dir = WRITE_DIR / f"{ticker}_{quarter}_{year}.write_pi"
    content, result = _run_pi_writer(user, pi_cfg=pi_cfg, run_dir=run_dir)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")

    pi_meta = dict(result.get("pi_meta", {}) or {})
    tool_trace = result.get("tool_trace", []) or []
    usage = result.get("usage", {}) or {}
    tool_calls = dict(pi_meta.get("tool_calls", {}) or {})
    budget_warnings = _budget_warnings(pi_meta, pi_cfg)
    resolved_model = pi_meta.get("model") or model

    WRITE_DIR.mkdir(parents=True, exist_ok=True)
    trace_path = WRITE_DIR / f"{ticker}_{quarter}_{year}.write_trace.json"
    trace_path.write_text(
        json.dumps({
            "ticker": ticker,
            "quarter": quarter,
            "year": year,
            "pi": pi_meta,
            "tool_trace": tool_trace,
            "usage": usage,
            "finish_reason": result.get("finish_reason"),
            "tool_calls": tool_calls,
            "budget_warnings": budget_warnings,
            "output": str(out_path),
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    n_tool_calls = sum(int(v) for v in tool_calls.values())
    print(f"  [WRITE] pi_agent model={resolved_model} "
          f"tools={n_tool_calls}"
          + (f" warnings={len(budget_warnings)}" if budget_warnings else ""))
    print(f"  [DONE] {out_path} ({len(content.splitlines())} lines)")
    return {
        "status": "success", "output": str(out_path), "model": resolved_model,
        "usage": usage, "pi_meta": pi_meta, "tool_trace": tool_trace,
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 4 writer call")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--market-data", default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--inject-redflags", default=None)
    a = p.parse_args()
    print(run(a.ticker, a.year, a.quarter, a.market_data, a.force, a.dry_run, a.inject_redflags))
