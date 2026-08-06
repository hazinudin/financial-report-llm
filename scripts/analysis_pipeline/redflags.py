"""
Stage 5 — red-flag / contradiction pass (optional, cheap model).

Given the canonical fact sheet and computed ratios, asks a cheap model to list
up to 10 contradictions, divergences, or anomalies an analyst must address
(e.g. "profit up but collections down", "receivables +216% while DSO lengthens").
The output bullets are shown to the operator and written to
idx_data/analysis/runs/{job}.redflags.md. When `--inject-redflags` is passed,
the pipeline feeds them back into a re-run of Stage 4 so the writer explicitly
addresses each point (this is how the receivables-vs-collections insight gets
guaranteed rather than hoped for — §5 Stage 5).
"""

from __future__ import annotations

from pathlib import Path

import yaml

import opencode_client
import research
from schema import FactSheet

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
RESEARCH_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "research"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

PROMPT = """Read the provided fact sheet and computed ratios. Identify up to 10 concrete
contradictions, divergences, or anomalies an analyst MUST address. Output a
markdown bullet list naming the two facts in tension and the section to address.
"""


def load_prompt() -> str:
    p = PROMPTS_DIR / "redflags.md"
    return p.read_text(encoding="utf-8") if p.exists() else PROMPT


def audit_research_sources(ticker: str, year: int, quarter: str) -> list[str]:
    """Audit research citations against the persisted tool trace."""
    brief_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research.yaml"
    trace_path = RESEARCH_DIR / f"{ticker}_{quarter}_{year}.research_trace.json"
    if not brief_path.exists():
        return []
    try:
        brief = yaml.safe_load(brief_path.read_text(encoding="utf-8")) or {}
        trace = yaml.safe_load(trace_path.read_text(encoding="utf-8")) if trace_path.exists() else {}
        trace = trace if isinstance(trace, dict) else {}
        audit = research._validate_brief_sources(
            brief, trace.get("tool_trace", []),
            no_tool_calls=not trace.get("tool_trace"),
        )
    except (OSError, yaml.YAMLError, TypeError):
        return ["- Research citation audit could not read the research artifact or trace."]

    bullets: list[str] = []
    for item in audit.get("unverified_sources", []):
        bullets.append(
            f"- Research citation is not present in the tool trace: "
            f"{item.get('url', '(missing URL)')}"
        )
    if audit.get("no_tool_calls") and brief.get("findings"):
        bullets.append("- Research brief contains findings but the tool trace has zero tool calls.")
    for finding in brief.get("findings", []) or []:
        for source in finding.get("sources", []) or []:
            if not source.get("published_date"):
                bullets.append(
                    f"- Research source has no publication date: {source.get('url', '(missing URL)')}"
                )
    for item in trace.get("tool_trace", []) or []:
        result = item.get("result") or {}
        if item.get("tool_name") == "fetch_web_page" and result.get("status") in {
            "degraded", "unsupported", "blocked", "error",
        }:
            bullets.append(
                f"- Research page fetch failed or was blocked: "
                f"{result.get('url', '(missing URL)')} ({result.get('error', result.get('status'))})"
            )
    return bullets


def run(ticker: str, year: int, quarter: str, dry_run: bool = False) -> dict:
    facts_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    ratios_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"
    if not facts_path.exists():
        if dry_run:
            print(f"  [DRY] would run red-flags pass (canonical fact sheet absent)")
            return {"status": "dry"}
        raise FileNotFoundError(f"Canonical fact sheet missing: {facts_path}")
    sheet = FactSheet.model_validate(yaml.safe_load(facts_path.read_text(encoding="utf-8")))
    ratios = yaml.safe_load(ratios_path.read_text(encoding="utf-8")) if ratios_path.exists() else {}

    settings = opencode_client.parse_model_settings("redflags")
    model = settings["model"]
    if not model:
        raise RuntimeError("config.yaml: analysis.models.redflags not set")

    payload = yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True)
    payload += "\n\n# RATIOS\n" + yaml.safe_dump(ratios, sort_keys=False, allow_unicode=True)

    if dry_run:
        print(f"  [DRY] would run red-flags pass with {model}")
        return {"status": "dry", "model": model}

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  [REDFLAGS] {model}")
    resp = opencode_client.chat(model, load_prompt(), payload, temperature=settings["temperature"],
                                max_tokens=settings["max_tokens"])
    bullets = resp["content"].strip()
    citation_bullets = audit_research_sources(ticker, year, quarter)
    if citation_bullets:
        bullets = (bullets + "\n" if bullets else "") + "\n".join(citation_bullets)
    out_path = RUNS_DIR / f"{ticker}_{year}_{quarter}.redflags.md"
    out_path.write_text(bullets + "\n", encoding="utf-8")
    print(f"  [DONE] {out_path}\n")
    for line in bullets.splitlines():
        if line.strip().startswith(("-", "*", "1.", "##")) or line.strip():
            print("  " + line)
    return {"status": "success", "output": str(out_path), "bullets": bullets, "usage": resp["usage"]}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 5 red-flag pass")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    print(run(a.ticker, a.year, a.quarter, a.dry_run))
