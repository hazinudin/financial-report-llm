"""
Stage 1 — LLM extraction (one cheap-model call per chunk, run in parallel).

Two modes, chosen automatically per report:

  SEEDED (default when xlsx_statements has coverage for the ticker/period):
    The primary statements are transcribed deterministically by xlsx_seed.py,
    so statement chunks are skipped entirely and note chunks get a slim
    details-only prompt (details + context_notes). Fewer, smaller, faster
    calls — and no LLM transcription of the big tables to fail validation.

  FALLBACK (no XLSX file or failed XLSX parsing): the original full-fact-sheet
  extraction over all chunks (statements + notes).

For each chunk built by chunker.py:
  1. Assemble the extraction prompt (system from prompts/extract_system.md +
     a data-driven user prompt).
  2. Call the configured extraction model (temperature 0).
  3. Strip code fences, parse YAML, validate against schema.FactSheet
     (lenient coercion handles null blocks, "55%" strings, YAML dates, etc.).
  4. ON FAILURE: one automated repair retry that appends the pydantic errors
     to the user prompt; then give up, saving the raw output for inspection.
  5. Write {T}_{Q}_{Y}_chunk{NN}.yaml in DOCUMENT order (and .raw.txt on
     terminal failure). Stale chunk files from previous runs are deleted
     first so merge.py never mixes runs.

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
import xlsx_seed
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


def _user_prompt_notes(chunk: Chunk) -> str:
    """Slim prompt for SEEDED mode: the primary statements come from the xlsx
    seed, so note chunks only carry tabular disclosures + oddity flags."""
    covers = ", ".join(chunk.covers)
    return f"""You are extracting NOTE disclosures from ONE chunk of a docling-converted IDX interim
report. This chunk covers note(s): {covers}.

The primary financial statements (income statement, balance sheet, cash flow) are sourced
elsewhere — do NOT extract statement line items, do NOT emit `meta`, `income_statement`,
`balance_sheet`, or `cash_flow` blocks.

Output ONLY a YAML object with these two keys:

1. `details` — for each tabular disclosure present in this chunk, one entry per row under the
   matching list (omit lists with no table):
     customers:        [ {{ name, amount, pct_of_sales }} ]
     loans:            [ {{ lender, balance, rate, maturity, notes }} ]
     subsidiaries:     [ {{ name, ownership_pct, activity, total_assets }} ]
     segments:         [ {{ name, external_sales, internal_sales, gross_profit, net_profit, assets }} ]
     production:       [ {{ product, volume_mt, period }} ]
     reserves:         [ {{ product, residual_mt, as_of }} ]
     related_parties:  [ {{ party, relationship, item, amount }} ]
     commitments:      [ {{ description, amount, counterparty }} ]
     dividends:        [ {{ period, per_share, total }} ]

2. `context_notes` — short bullets (plain strings) flagging anything an analyst should verify:
   a balance that vanished YoY, a misaligned docling table row, a unit surprise, a material
   one-off described in prose. Also capture, as bullets, key non-tabular facts an equity
   write-up would need (e.g. a new facility, a default, a dispute, a subsequent event).

Rules:
- Copy numbers EXACTLY as printed (strip thousands separators; keep the report's unit scale).
  Do NOT round, convert, or compute.
- Percentages: emit the number only (55% -> 55). Rates stay strings ("4.90% - 6.10%").
- If the chunk has no relevant tables and nothing notable, emit `details: {{}}` and
  `context_notes: []`.

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
    notes_only: bool = False, meta_override: dict | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    system = load_system_prompt()
    user = _user_prompt_notes(chunk) if notes_only else _user_prompt(chunk, line_keys)
    resp = opencode_client.chat(
        model, system, user + "\n" + chunk.text,
        temperature=temperature, max_tokens=max_tokens,
        reasoning_effort=reasoning_effort,
    )
    usage = resp["usage"]

    def _finalize(sheet: FactSheet) -> FactSheet:
        # We already know these — never leave them to the model (observed:
        # quarter emitted as int, ticker misread as GOZCO for GZCO).
        if meta_override:
            sheet.meta.ticker = meta_override["ticker"]
            sheet.meta.quarter = meta_override["quarter"]
            sheet.meta.year = meta_override["year"]
        return sheet

    base = {"chunk": chunk.name, "covers": chunk.covers, "line_start": chunk.line_range[0]}
    try:
        if not (resp["content"] or "").strip():
            raise ValueError("empty model response (reasoning budget exhausted?)")
        sheet = _finalize(_parse_fact_sheet(resp["content"]))
        return {**base, "sheet": sheet, "usage": usage, "ok": True}
    except (ValidationError, yaml.YAMLError, ValueError) as e:
        # One repair retry with errors appended. Give the repair a larger
        # output budget: the common first-attempt failures are truncation at
        # max_tokens and empty content after long hidden reasoning.
        repair_max = min(32768, max(max_tokens * 2, 16384))
        repair_user = user + "\n" + chunk.text + (
            f"\n\nYour previous output failed validation:\n{e}\n\n"
            "Re-emit the corrected YAML only."
        )
        resp2 = opencode_client.chat(
            model, system, repair_user, temperature=temperature, max_tokens=repair_max,
            reasoning_effort=reasoning_effort,
        )
        usage2 = resp2["usage"]
        usage = {
            "prompt_tokens": usage.get("prompt_tokens", 0) + usage2.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0) + usage2.get("completion_tokens", 0),
        }
        try:
            sheet = _finalize(_parse_fact_sheet(resp2["content"]))
            return {**base, "sheet": sheet, "usage": usage, "ok": True, "repaired": True}
        except Exception as e2:
            return {
                **base, "ok": False,
                "error": f"{e!s} / {e2!s}", "raw": resp2["content"], "usage": usage,
            }


def _dump_sheet_yaml(sheet: FactSheet) -> str:
    # pydantic -> dict -> yaml, None kept as null
    return yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True)


def _clear_stale_outputs(ticker: str, quarter: str, year: int) -> int:
    """Delete chunk sheets/raws + manifest from previous runs for this report.
    Without this, a re-run with different chunk boundaries leaves stale files
    that merge.py would glob in (observed: GZCO merging 3 overlapping runs)."""
    removed = 0
    for pattern in (f"{ticker}_{quarter}_{year}_chunk*.yaml",
                    f"{ticker}_{quarter}_{year}_chunk*.raw.txt"):
        for f in FACTS_DIR.glob(pattern):
            f.unlink()
            removed += 1
    manifest = FACTS_DIR / f"{ticker}_{quarter}_{year}_manifest.json"
    if manifest.exists():
        manifest.unlink()
        removed += 1
    return removed


def run(
    ticker: str, year: int, quarter: str,
    preprocessed_path: str, force: bool = False, dry_run: bool = False,
    workers: int | None = None, cap: int | None = None, db_conn=None,
) -> dict:
    """Run extraction for one report. Returns summary + per-chunk usage."""
    FACTS_DIR.mkdir(parents=True, exist_ok=True)
    xlsx_sync = xlsx_seed.ensure_xlsx_coverage(
        ticker, year, quarter, conn=db_conn, dry_run=dry_run,
    )
    seeded = xlsx_sync["seeded"]
    chunks = build_chunks(preprocessed_path, cap=cap, include_statements=not seeded)
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
        return {"status": "skipped", "chunks": len(existing), "model": model, "seeded": seeded}

    mode = "seeded (notes-only)" if seeded else "fallback (full statements)"
    if dry_run:
        print(f"  [DRY] would extract {len(chunks)} chunks with {model} — {mode}")
        for c in chunks:
            print(f"       - {c.name} covers={c.covers} ~{c.approx_tokens} tok")
        return {"status": "dry", "chunks": len(chunks), "model": model, "seeded": seeded}

    removed = _clear_stale_outputs(ticker, quarter, year)
    if removed:
        print(f"  [CLEAN] removed {removed} stale chunk file(s)")

    print(f"  [EXTRACT] {len(chunks)} chunks with {model} (workers={workers}) — {mode}")
    meta_override = {"ticker": ticker, "quarter": quarter, "year": year}
    results: list[dict] = []
    total_prompt = total_completion = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_extract_one, c, line_keys, model, settings["temperature"],
                      settings["max_tokens"], seeded, meta_override,
                      settings.get("reasoning_effort")): c
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

    # Write chunk sheets in DOCUMENT order; collect failures.
    failed: list[dict] = []
    ok_count = 0
    manifest: list[dict] = []
    for i, r in enumerate(sorted(results, key=lambda x: x["line_start"]), start=1):
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
        "seeded": seeded,
        "xlsx": xlsx_sync,
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
