"""
Stage 1 — LLM extraction (one cheap-model call per chunk, run in parallel).

For each chunk built by chunker.py:
  1. Assemble the extraction prompt (system from prompts/extract_system.md +
     a data-driven user prompt carrying the chunk's coverage and the canonical
     line-item keys).
  2. Call the configured extraction model (temperature 0).
  3. Strip code fences, parse YAML, validate against schema.FactSheet.
  4. ON FAILURE: one automated repair retry that appends the pydantic errors
     to the user prompt; then give up, saving the raw output for inspection.
  5. Write {T}_{Q}_{Y}_chunk{NN}.yaml (and .raw.txt on terminal failure).

The extractor NEVER does arithmetic — only lookup/copy. Ratios are computed
deterministically in ratios.py. `context_notes` is the escape hatch for
qualitative oddities the model judges material.
"""

from __future__ import annotations

import json
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

import opencode_client
from chunker import Chunk, build_chunks
from schema import FactSheet, canonical_line_keys

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

_FENCE_RE = re.compile(r"^```(?:ya?ml)?\s*\n(.*?)\n```\s*$", re.S)


def _strip_fences(s: str) -> str:
    s = s.strip()
    m = _FENCE_RE.match(s)
    if m:
        return m.group(1).strip()
    # tolerate leading ```yaml without closing fence
    if s.startswith("```"):
        s = re.sub(r"^```(?:ya?ml)?\s*\n?", "", s)
        s = re.sub(r"\n?```\s*$", "", s)
    return s.strip()


def _safe_key(k: str) -> str:
    return k.strip().lower().replace(" ", "_").replace("-", "_")


def load_system_prompt() -> str:
    p = PROMPTS_DIR / "extract_system.md"
    return p.read_text(encoding="utf-8") if p.exists() else ""


def _user_prompt(chunk: Chunk, line_keys: dict) -> str:
    covers = ", ".join(chunk.covers)
    keys_block = "\n".join(
        f"  {stmt}:\n    " + ", ".join(keys) for stmt, keys in line_keys.items()
    )
    return f"""You are extracting structured financial facts from ONE chunk of a docling-converted
IDX interim report. This chunk covers: {covers}.

Rules:
- Output ONLY a YAML object conforming to the schema. No prose, no explanations.
- Copy numbers EXACTLY as printed (preserve precision). Do NOT round, convert, or compute.
- For the primary statements (income_statement / balance_sheet / cash_flow), the report shows
  TWO columns: the current period and a comparative. Put the CURRENT-column value of each line
  under `lines` (keyed by the snake_case names below), and the COMPARATIVE-column value under
  `prior_lines` using the SAME keys. Set `periods` to name the two columns in order
  (e.g. ["current","prior_yoy"] for P&L/cash flow, ["current","prior_fy_end"] for the balance sheet).
- Use `null` for any line not present in this chunk — do NOT invent numbers.
- Use `details` lists (customers, loans, subsidiaries, segments, production, reserves,
  related_parties, commitments, dividends) when a table is present.
- Use `context_notes` to flag anything odd (a line that vanished YoY, a misaligned table row, a
  unit surprise) — verbatim-ish, one short bullet per item.
- `meta`: include ticker/quarter/year if known from context; otherwise leave for merge.

Canonical line-item keys (snake_case):
{keys_block}

YAML begins on the next line:
"""


def _parse_fact_sheet(raw: str) -> FactSheet:
    text = _strip_fences(raw)
    # Some models prefix with a stray sentence; cut to first top-level key.
    text = re.sub(r"^[^\n]*\n(?=meta:|income_statement:|balance_sheet:)", "", text, count=1, flags=re.M)
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("Top-level YAML is not a mapping")
    # Normalize line-item keys to snake_case within each statement.
    for stmt in ("income_statement", "balance_sheet", "cash_flow"):
        block = data.get(stmt)
        if isinstance(block, dict) and isinstance(block.get("lines"), dict):
            block["lines"] = {_safe_key(k): v for k, v in block["lines"].items()}
    return FactSheet.model_validate(data)


def _extract_one(
    chunk: Chunk, line_keys: dict, model: str, temperature: float, max_tokens: int,
) -> dict[str, Any]:
    system = load_system_prompt()
    user = _user_prompt(chunk, line_keys)
    resp = opencode_client.chat(
        model, system, user + "\n" + chunk.text,
        temperature=temperature, max_tokens=max_tokens,
    )
    usage = resp["usage"]
    try:
        sheet = _parse_fact_sheet(resp["content"])
        return {"chunk": chunk.name, "covers": chunk.covers, "sheet": sheet, "usage": usage, "ok": True}
    except (ValidationError, yaml.YAMLError, ValueError) as e:
        # One repair retry with errors appended.
        repair_user = user + "\n" + chunk.text + (
            f"\n\nYour previous output failed validation:\n{e}\n\n"
            "Re-emit the corrected YAML only."
        )
        resp2 = opencode_client.chat(
            model, system, repair_user, temperature=temperature, max_tokens=max_tokens,
        )
        usage2 = resp2["usage"]
        usage = {
            "prompt_tokens": usage.get("prompt_tokens", 0) + usage2.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0) + usage2.get("completion_tokens", 0),
        }
        try:
            sheet = _parse_fact_sheet(resp2["content"])
            return {"chunk": chunk.name, "covers": chunk.covers, "sheet": sheet, "usage": usage, "ok": True, "repaired": True}
        except Exception as e2:
            return {
                "chunk": chunk.name, "covers": chunk.covers, "ok": False,
                "error": f"{e!s} / {e2!s}", "raw": resp2["content"], "usage": usage,
            }


def _dump_sheet_yaml(sheet: FactSheet) -> str:
    # pydantic -> dict -> yaml, None kept as null
    return yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True)


def run(
    ticker: str, year: int, quarter: str,
    preprocessed_path: str, force: bool = False, dry_run: bool = False,
    workers: int | None = None, cap: int | None = None,
) -> dict:
    """Run extraction for one report. Returns summary + per-chunk usage."""
    FACTS_DIR.mkdir(parents=True, exist_ok=True)
    chunks = build_chunks(preprocessed_path, cap=cap)
    line_keys = canonical_line_keys()
    settings = opencode_client.parse_model_settings("extract")
    model = settings["model"]
    if not model:
        raise RuntimeError("config.yaml: analysis.models.extract not set (run --list-models)")

    if workers is None:
        workers = opencode_client.load_analysis_config().get("extraction_workers", 3)

    existing = sorted(FACTS_DIR.glob(f"{ticker}_{quarter}_{year}_chunk*.yaml"))
    if existing and not force and not dry_run:
        print(f"  [SKIP] {len(existing)} chunk sheet(s) already present (use --force to re-extract)")
        return {"status": "skipped", "chunks": len(existing), "model": model}

    if dry_run:
        print(f"  [DRY] would extract {len(chunks)} chunks with {model}")
        for c in chunks:
            print(f"       - {c.name} covers={c.covers} ~{c.approx_tokens} tok")
        return {"status": "dry", "chunks": len(chunks), "model": model}

    print(f"  [EXTRACT] {len(chunks)} chunks with {model} (workers={workers})")
    results: list[dict] = []
    total_prompt = total_completion = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_extract_one, c, line_keys, model, settings["temperature"], settings["max_tokens"]): c
            for c in chunks
        }
        for fut in as_completed(futs):
            r = fut.result()
            results.append(r)
            u = r.get("usage", {})
            total_prompt += u.get("prompt_tokens", 0)
            total_completion += u.get("completion_tokens", 0)
            tag = "OK" if r.get("ok") else "FAIL"
            extra = " (repaired)" if r.get("repaired") else ""
            print(f"    [{tag}] {r['chunk']:<18} covers={r['covers']}{extra}")

    # Write chunk sheets; collect failures.
    failed: list[dict] = []
    ok_count = 0
    manifest: list[dict] = []
    for i, r in enumerate(sorted(results, key=lambda x: x["chunk"]), start=1):
        name = f"{ticker}_{quarter}_{year}_chunk{i:02d}"
        if r.get("ok"):
            out = FACTS_DIR / f"{name}.yaml"
            out.write_text(_dump_sheet_yaml(r["sheet"]), encoding="utf-8")
            ok_count += 1
            manifest.append({"file": out.name, "covers": r["covers"], "chunk": r["chunk"]})
        else:
            failed.append(r)
            (FACTS_DIR / f"{name}.raw.txt").write_text(
                r.get("raw", "") + f"\n\n# ERROR: {r.get('error')}\n", encoding="utf-8"
            )
    if manifest:
        (FACTS_DIR / f"{ticker}_{quarter}_{year}_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    summary = {
        "status": "success" if not failed else "partial",
        "model": model, "chunks": len(chunks), "ok": ok_count, "failed": len(failed),
        "tokens": {"prompt": total_prompt, "completion": total_completion},
    }
    if failed:
        summary["error"] = f"{len(failed)} chunk(s) failed validation after repair: {[r['chunk'] for r in failed]}"
    return summary


if __name__ == "__main__":
    import argparse, sys
    p = argparse.ArgumentParser(description="Stage 1 LLM extraction")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--preprocessed", required=True, help="path to preprocessed .md")
    p.add_argument("--workers", type=int, default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    try:
        res = run(a.ticker, a.year, a.quarter, a.preprocessed,
                  force=a.force, dry_run=a.dry_run, workers=a.workers)
        print(res)
        if res.get("status") == "partial":
            sys.exit(1)
    except Exception as e:
        traceback.print_exc()
        sys.exit(2)