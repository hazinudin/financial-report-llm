"""
Stage 3 — assemble the writer's context package.

Pieces (kept small per the context budget, §9):
  1. Static company profile: analysis/companies/$TICKER.md (created if missing).
  2. Outline skeleton: prompts/outline_skeleton.md (the 14-section outline).
  3. Vault context: subprocess to scripts/notes_rag/rag.py search --json, top 3
     chunks (prior analyses / commodity notes). Broadened by business-line terms:
       (a) an LLM-deduced main commodity/product (from name + IDX sector +
           extracted fact-sheet details) — non-fatal, skipped on any failure,
       (b) the authoritative IDX sector labels (sub_industry/industry/sub_sector/
           sector) from idx_companies.json, falling back to COMMODITY_HINTS.
     Vault/RAG failures (e.g. Ollama down) are non-fatal — context is best-effort.
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

import opencode_client

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
COMPANIES_DIR = _PROJECT_ROOT / "analysis" / "companies"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
RAG_SCRIPT = _PROJECT_ROOT / "scripts" / "notes_rag" / "rag.py"
IDX_COMPANIES_PATH = _PROJECT_ROOT / "idx_data" / "idx_companies.json"
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"

# Fallback sector -> commodity hint map (used only if a ticker is missing from
# the authoritative IDX company master below). Prefer the master.
COMMODITY_HINTS = {
    "ESSA": "ammonia", "DKFT": "nickel", "ARCI": "gold", "INDR": "nickel",
    "ADMR": "coal aluminium", "CITA": "coal", "ITMG": "coal", "PTPS": "coal",
    "BTEK": "gold", "SEAL": "coal", "ELSA": "nickel bauxite",
    "MHKI": "nickel", "MEDC": "oil gas",
}


def _load_company_index() -> dict[str, dict]:
    """Map ticker -> IDX company master record from idx_companies.json."""
    if not IDX_COMPANIES_PATH.exists():
        return {}
    try:
        data = json.loads(IDX_COMPANIES_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return {c["code"]: c for c in data if isinstance(c, dict) and c.get("code")}


def business_terms(ticker: str) -> list[str]:
    """Authoritative business-line labels for a ticker, most-specific first
    (sub_industry, industry, sub_sector, sector). Used to broaden the vault
    RAG query beyond the ticker itself. Empty if ticker is unknown."""
    rec = _load_company_index().get(ticker.upper())
    if not rec:
        return []
    terms: list[str] = []
    for key in ("sub_industry", "industry", "sub_sector", "sector"):
        v = rec.get(key)
        if v and v not in terms:
            terms.append(v)
    return terms


def _load_facts_details(ticker: str, year: int, quarter: str) -> str:
    """Pull the most commodity-revealing fields from the canonical fact sheet
    (segments, production, reserves, subsidiaries, context_notes) to ground the
    LLM commodity deduction. Returns '' if the sheet is absent/unreadable."""
    path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    if not path.exists():
        return ""
    try:
        sheet = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return ""
    d = sheet.get("details") or {}
    bits: list[str] = []
    for key in ("segments", "production", "reserves", "subsidiaries"):
        items = d.get(key) or []
        if items:
            bits.append(f"{key}: {json.dumps(items, ensure_ascii=False)[:500]}")
    notes = sheet.get("context_notes") or []
    if notes:
        bits.append("context_notes: " + " | ".join(notes)[:500])
    return "\n".join(bits)


def deduce_commodity(ticker: str, year: int, quarter: str) -> str | None:
    """LLM step: infer the company's main product/commodity/business line from
    its name, IDX sector classification, and extracted financial details.

    Returns a short comma-separated phrase (e.g. 'coal' or 'palm oil, edible
    oil'), or None on any failure. Non-fatal — the caller falls back to IDX
    sector labels only.
    """
    rec = _load_company_index().get(ticker.upper(), {})
    name = rec.get("name", ticker)
    sector_bits = " / ".join(
        v for v in (rec.get("sector"), rec.get("sub_sector"),
                    rec.get("industry"), rec.get("sub_industry")) if v
    )
    details = _load_facts_details(ticker, year, quarter)
    prompt = (
        f"Company: {name} ({ticker})\n"
        f"IDX classification: {sector_bits}\n"
        + (f"Extracted disclosures:\n{details}\n" if details else "")
        + "\nWhat is the main product, commodity, or business line this company "
          "sells? Reply with a short comma-separated list of 1-3 keywords a "
          "commodity/equity research analyst would search for (e.g. 'coal', "
          "'palm oil, edible oil', 'gold'). No prose, no explanation."
    )
    try:
        settings = opencode_client.parse_model_settings("extract")
        model = settings["model"]
        if not model:
            return None
        resp = opencode_client.chat(
            model,
            "You map a company to its main commodity/business line. Be concise.",
            prompt, temperature=0.0, max_tokens=64, reasoning_effort="none",
        )
        text = (resp.get("content") or "").strip()
    except Exception:
        return None
    if not text:
        return None
    items = [p.strip().strip(".'\"") for p in text.split(",")]
    items = [p for p in items if p][:3]
    return ", ".join(items) if items else None


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

    # Broaden with business-line context, in priority order:
    #  1) LLM-deduced main commodity/product (data-driven, most specific)
    #  2) authoritative IDX sector labels (fallback: COMMODITY_HINTS)
    deduced = deduce_commodity(ticker, year, quarter)
    biz_terms = business_terms(ticker)
    if not biz_terms:
        commodity = COMMODITY_HINTS.get(ticker.upper())
        if commodity:
            biz_terms = [commodity]
    broaden: list[str] = []
    if deduced:
        broaden.append(deduced)
    broaden.extend(biz_terms)

    for term in broaden:
        if len(hits) >= 3:
            break
        extra = vault_search(f"{term} outlook {year}", ticker=None)
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
        "commodity_hint": ", ".join(broaden) if broaden else None,
    }

    has_content = any(
        (
            (package.get("profile") or "").strip(),
            (package.get("skeleton") or "").strip(),
            bool(package.get("vault_hits")),
            bool(package.get("market_data")),
        )
    )
    if not dry_run and not has_content:
        raise RuntimeError(
            f"context package is empty for {ticker} {quarter} {year} "
            "(no profile, skeleton, vault hits, or market data)"
        )

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