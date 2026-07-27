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
from schema import FactSheet

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

PROMPT = """Read the provided fact sheet and computed ratios. Identify up to 10 concrete
contradictions, divergences, or anomalies an analyst MUST address. Output a
markdown bullet list naming the two facts in tension and the section to address.
"""


def load_prompt() -> str:
    p = PROMPTS_DIR / "redflags.md"
    return p.read_text(encoding="utf-8") if p.exists() else PROMPT


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