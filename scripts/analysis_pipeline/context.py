"""
Stage 3 — assemble the writer's context package (no LLM).

Pieces (kept small per the context budget, §9):
  1. Static company profile: analysis/companies/$TICKER.md (created if missing).
  2. Outline skeleton: prompts/outline_skeleton.md (the 14-section outline).
  3. Vault context: subprocess to scripts/notes_rag/rag.py search --json, top 3
     chunks (prior analyses / commodity notes). Failures (e.g. Ollama down) are
     non-fatal — vault context is best-effort.
  4. Market-data stub: a pasted --market-data YAML (price, market_cap, shares,
     pe_ttm, 52wk range, beta). Optional.

The assembled package is persisted to idx_data/analysis/runs/{job}.context.json
so `--write-only` can replay the writer without re-running context.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
COMPANIES_DIR = _PROJECT_ROOT / "analysis" / "companies"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
RAG_SCRIPT = _PROJECT_ROOT / "scripts" / "notes_rag" / "rag.py"

# Tiny sector -> commodity hint map to broaden the vault query beyond the ticker.
COMMODITY_HINTS = {
    "ESSA": "ammonia", "DKFT": "nickel", "ARCI": "gold", "INDR": "nickel",
    "ADMR": "coal aluminium", "CITA": "coal", "ITMG": "coal", "PTPS": "coal",
    "BTEK": "gold", "SEAL": "coal", "ELSA": "nickel bauxite",
    "MHKI": "nickel", "MEDC": "oil gas",
}


def get_or_create_profile(ticker: str, dry_run: bool = False) -> tuple[str, bool]:
    """Return (profile_text, created_now)."""
    path = COMPANIES_DIR / f"${ticker}.md"
    if path.exists():
        return path.read_text(encoding="utf-8"), False
    if dry_run:
        return f"(profile {ticker}.md would be created on a real run)", False
    COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
    stub = (
        f"[[{ticker} Q1 2026 Analysis]]\n"
        f"# {ticker} — Company Profile (stub)\n\n"
        f"Auto-created by the analysis pipeline. Edit this file to add static\n"
        f"structure: sector, licenses, board, loan history, ownership. Static\n"
        f"content here is reused across quarterly runs (only numbers refresh).\n"
    )
    path.write_text(stub, encoding="utf-8")
    return stub, True


def load_skeleton() -> str:
    p = PROMPTS_DIR / "outline_skeleton.md"
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


def vault_search(query: str, ticker: str | None, limit: int = 3) -> list[dict]:
    """Call the notes RAG script as a subprocess; return parsed JSON hits."""
    if not RAG_SCRIPT.exists():
        return []
    cmd = [sys.executable, str(RAG_SCRIPT), "search", "-k", str(limit), "--json"]
    if ticker:
        cmd += ["--ticker", ticker]
    cmd += [query]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=str(_PROJECT_ROOT))
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []
    if proc.returncode != 0:
        return []
    # rag prints JSON to stdout (possibly after warnings on stderr)
    out = proc.stdout.strip()
    if not out:
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return []
    return data[:limit] if isinstance(data, list) else []


def run(ticker: str, year: int, quarter: str, market_data_path: str | None = None,
        dry_run: bool = False) -> dict:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    profile_text, created = get_or_create_profile(ticker, dry_run=dry_run)
    skeleton = load_skeleton()

    q1 = f"{quarter} {year}"
    query = f"{ticker} {q1}"
    hits = vault_search(query, ticker=ticker)
    commodity = COMMODITY_HINTS.get(ticker.upper())
    if commodity:
        extra = vault_search(f"{commodity} outlook {year}", ticker=None)
        for h in extra:
            if h.get("id") not in {x.get("id") for x in hits}:
                hits.append(h)
        hits = hits[:3]

    market_data: dict = {}
    if market_data_path:
        mp = Path(market_data_path)
        if mp.exists():
            market_data = yaml.safe_load(mp.read_text(encoding="utf-8")) or {}

    package = {
        "ticker": ticker, "year": year, "quarter": quarter,
        "profile": profile_text, "profile_created": created,
        "skeleton": skeleton,
        "vault_hits": [
            {"path": h.get("path"), "section": h.get("section"),
             "snippet": h.get("snippet"), "obsidian": h.get("obsidian")}
            for h in hits
        ],
        "market_data": market_data,
        "commodity_hint": commodity,
    }

    out_path = RUNS_DIR / f"{ticker}_{year}_{quarter}.context.json"
    if not dry_run:
        out_path.write_text(json.dumps(package, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  [CTX] profile={'created' if created else 'exists'}, "
              f"{len(hits)} vault hit(s), market={'yes' if market_data else 'no'} -> {out_path.name}")
    else:
        print(f"  [DRY] profile={'created' if created else 'exists'}, {len(hits)} vault hit(s)")
    return {"status": "dry" if dry_run else "success", "package": package, "path": str(out_path)}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 3 context assembly")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--market-data", default=None)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    print(run(a.ticker, a.year, a.quarter, a.market_data, a.dry_run))