"""
Stage 4 — writer call (ONE call, strongest model).

Assembles the writer input package (kept <= ~30K tokens): writer system prompt
+ the 14-section skeleton + the canonical fact sheet + computed ratios +
static company profile + <= 3 vault hits + the market-data stub. Then calls the
configured writer model and writes the analysis markdown to analysis/.

Style rules mirror the existing human analyses (frontmatter tags, wikilinks,
tables, severity-ranked risks, disclaimer) via writer_system.md.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from pydantic import BaseModel

import opencode_client
from schema import FactSheet

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
ANALYSIS_DIR = _PROJECT_ROOT / "analysis"


def load_writer_prompt() -> str:
    p = PROMPTS_DIR / "writer_system.md"
    return p.read_text(encoding="utf-8") if p.exists() else ""


def _fmt_num(v):
    return "" if v is None else v


def _build_user(ticker, year, quarter, sheet: FactSheet, ratios: dict, package: dict) -> str:
    facts_yaml = yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True)
    ratios_yaml = yaml.safe_dump(ratios, sort_keys=False, allow_unicode=True)
    profile = package.get("profile", "")
    skeleton = package.get("skeleton", "")
    md = package.get("market_data", {}) or {}
    hits = package.get("vault_hits", []) or []

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
the facts and ratios provided. You are the analyst; synthesize, but you may ONLY
use numbers that appear in the fact sheet, the ratios, or the market-data stub.
Do not invent figures. Where a ratio is null/missing, say so rather than estimating.

# OUTLINE SKELETON (follow this structure)
{skeleton}

# STATIC COMPANY PROFILE
{profile}

# CANONICAL FACT SHEET (single source of truth for all numbers)
{facts_yaml}

# COMPUTED RATIOS (deterministic — use these verbatim)
{ratios_yaml}

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

    settings = opencode_client.parse_model_settings("write")
    model = settings["model"]
    if not model:
        raise RuntimeError("config.yaml: analysis.models.write not set (run --list-models)")

    user = _build_user(ticker, year, quarter, sheet, ratios, package)
    if inject_redflags:
        user += f"\n\n# RED FLAGS TO ADDRESS EXPLICITLY\n{inject_redflags}\n\nAddress each of these in the relevant section (do not just append them)."

    if dry_run:
        print(f"  [DRY] would write {out_path.name} with {model} "
              f"(facts={sum(1 for v in sheet.income_statement.lines.values() if v is not None)} IS lines)")
        return {"status": "dry", "model": model, "user_chars": len(user)}

    system = load_writer_prompt()
    print(f"  [WRITE] {model} (user prompt ~{len(user)//4} tok)")
    resp = opencode_client.chat(
        model, system, user, temperature=settings["temperature"], max_tokens=settings["max_tokens"]
    )
    content = resp["content"].strip()
    out_path.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")
    print(f"  [DONE] {out_path} ({len(content.splitlines())} lines)")
    return {
        "status": "success", "output": str(out_path), "model": model,
        "usage": resp["usage"],
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