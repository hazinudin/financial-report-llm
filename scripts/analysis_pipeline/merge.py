"""
Stage 2a — merge per-chunk fact sheets into one canonical fact sheet.

Priority layers (first non-null value wins, conflicts logged):
  0. XLSX SEED — deterministic primary statements from xlsx_seed.py, when the
     IDX XBRL filing is parsed in pipeline.duckdb. Exact by construction.
  1. Primary-statement values from LLM chunks covering "statements" — these
     fill keys the seed leaves null (dividends_paid, operating_profit, ...).
  2. Note chunks, later notes winning over earlier notes for the same field.

Every conflict is logged with both values (this catches docling row
misalignments such as the DKFT note-29 customer table, and any LLM value
disagreeing with the xlsx seed = hallucination signal).

When no xlsx seed exists, the legacy XLSX cross-validation runs instead,
comparing merged LLM values against any xlsx rows as warnings-only ground
truth. With a seed it is skipped (the values ARE the xlsx).

Inputs:  idx_data/analysis/facts/{T}_{Q}_{Y}_chunkNN.yaml (+ _manifest.json)
         pipeline.duckdb xlsx_statements/xlsx_meta (optional seed)
Output:  idx_data/analysis/facts/{T}_{Q}_{Y}.yaml          (canonical)
         idx_data/analysis/runs/{job_id}.merge.log          (conflicts/warnings)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

import xlsx_seed
import yaml_io
from schema import FactSheet, Details, Meta, Statement

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"
DEFAULT_DB_PATH = _PROJECT_ROOT / "idx_data" / "pipeline.duckdb"

DETAIL_DEDUP_KEYS = {
    "customers": lambda e: e.name,
    "loans": lambda e: e.lender,
    "subsidiaries": lambda e: e.name,
    "segments": lambda e: e.name,
    "production": lambda e: e.product,
    "reserves": lambda e: e.product,
    "related_parties": lambda e: (e.party, e.item),
    "commitments": lambda e: (e.description, e.counterparty),
    "dividends": lambda e: e.period,
}


def _sheet_priority(covers: list[str]) -> tuple[int, int]:
    if "xlsx" in covers:
        return (-1, 0)  # deterministic seed outranks every LLM sheet
    is_stmt = any("statements" in c for c in covers)
    if is_stmt:
        return (0, 0)
    nums = [int(c) for c in covers if c.isdigit()]
    return (1, max(nums) if nums else 0)


def _merge_statement(merged: Statement, sheets: list[tuple[FactSheet, list[str], int]], log: logging.Logger, name: str):
    ordered = sorted(sheets, key=lambda t: t[2])  # by priority
    for column in ("lines", "prior_lines"):
        out: dict[str, float | None] = {}
        ownership: dict[str, list[str]] = {}  # key -> list of chunk names holding a value
        for sheet, covers, _prio in ordered:
            block = getattr(sheet, name)
            col = getattr(block, column) or {}
            for k, v in col.items():
                if k in ownership:
                    ownership[k].append(_covers_label(covers))
                else:
                    ownership[k] = [_covers_label(covers)]
                if v is None:
                    continue
                if k not in out:
                    out[k] = v
                elif out[k] != v:
                    log.warning(
                        f"CONFLICT {name}.{column}.{k}: {out[k]} vs {v} "
                        f"(keepers={ownership[k]}) -> kept {out[k]}"
                    )
        setattr(merged, column, out)


def _covers_label(covers: list[str]) -> str:
    return "+".join(covers) if covers else "?"


def _merge_details(merged: Details, sheets: list[FactSheet], log: logging.Logger):
    for field, keyfn in DETAIL_DEDUP_KEYS.items():
        seen: dict[Any, BaseModel] = {}
        for sheet in sheets:
            for e in getattr(sheet.details, field):
                k = keyfn(e)
                if k is None:
                    continue
                if k not in seen:
                    seen[k] = e
                else:
                    # merge None fields from the new entry into the existing one
                    prev = seen[k]
                    for fname, val in e.model_dump().items():
                        if getattr(prev, fname) is None and val is not None:
                            setattr(prev, fname, val)
        setattr(merged, field, list(seen.values()))


def _merge_meta(sheets: list[FactSheet], ticker: str, year: int, quarter: str) -> Meta:
    base: Meta | None = None
    for s in sheets:
        if s.meta.ticker or s.meta.currency or s.meta.units:
            base = s.meta
            break
    if base is None:
        base = sheets[0].meta if sheets else Meta(ticker=ticker, quarter=quarter, year=year)
    # fill gaps from any sheet
    for s in sheets:
        m = s.meta
        if base.currency is None and m.currency:
            base.currency = m.currency
        if base.units is None and m.units:
            base.units = m.units
        if base.audited is None and m.audited is not None:
            base.audited = m.audited
        if base.period_end is None and m.period_end:
            base.period_end = m.period_end
        if base.prior_period_end is None and m.prior_period_end:
            base.prior_period_end = m.prior_period_end
    base.ticker, base.year, base.quarter = ticker, year, quarter
    base.schema_version = base.schema_version or 1
    return base


def merge_sheets(sheets_with_covers: list[tuple[FactSheet, list[str]]], log: logging.Logger) -> FactSheet:
    sheets = [s for s, _ in sheets_with_covers]
    enriched = [(s, c, _sheet_priority(c)) for s, c in sheets_with_covers]

    meta = _merge_meta(sheets, sheets[0].meta.ticker or "", int(sheets[0].meta.year) if sheets[0].meta.year else 0, sheets[0].meta.quarter or "")
    income = Statement(periods=["current", "prior_yoy"])
    balance = Statement(periods=["current", "prior_fy_end"])
    cash = Statement(periods=["current", "prior_yoy"])
    _merge_statement(income, enriched, log, "income_statement")
    _merge_statement(balance, enriched, log, "balance_sheet")
    _merge_statement(cash, enriched, log, "cash_flow")

    details = Details()
    _merge_details(details, sheets, log)

    notes: list[str] = []
    seen_notes: set[str] = set()
    for s in sheets:
        for n in s.context_notes:
            if n and n not in seen_notes:
                seen_notes.add(n)
                notes.append(n)

    return FactSheet(meta=meta, income_statement=income, balance_sheet=balance,
                     cash_flow=cash, details=details, context_notes=notes)


# ── XLSX cross-validation ──────────────────────────────────────────────

XLSX_MAP = {
    # canonical line key -> list of label_en substrings to match xlsx rows
    "income_statement": {
        "revenue": ["revenue", "pendapatan", "sales"],
        "gross_profit": ["gross profit", "laba kotor"],
        "operating_profit": ["operating profit", "laba usaha"],
        "pbt": ["profit before tax", "laba sebelum"],
        "tax": ["income tax", "pajak"],
        "net_profit": ["net profit", "laba bersih"],
        "eps_basic": ["earning per share", "laba per saham"],
    },
    "balance_sheet": {
        "cash": ["cash and cash equivalents", "kas dan setara"],
        "total_assets": ["total assets", "total aset"],
        "total_liabilities": ["total liabilities", "total liabilitas"],
        "total_equity": ["total equity", "total ekuitas"],
    },
    "cash_flow": {
        "ocf": ["net cash from operating", "arus kas dari aktivitas operasi"],
        "cash_end": ["cash at end", "kas pada akhir"],
    },
}


def xlsx_cross_validate(sheet: FactSheet, db_path: Path, tolerance: float, log: logging.Logger):
    try:
        conn = xlsx_seed._connect(db_path)
    except Exception as e:  # noqa: BLE001
        log.debug(f"xlsx cross-validation skipped: {e}")
        return
    try:
        # Map quarter -> period code used in xlsx_statements
        qmap = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}
        period = qmap.get(sheet.meta.quarter, sheet.meta.quarter.lower())
        rows = conn.execute(
            "SELECT label_en, value_current, sheet_code FROM xlsx_statements "
            "WHERE ticker=? AND year=? AND period=?",
            (sheet.meta.ticker, sheet.meta.year, period),
        ).fetchall()
        if not rows:
            log.info("xlsx cross-validation: no XLSX data for this ticker/period")
            return
        # label_en (lowercased) -> value_current, ignoring sheet_code
        label_val = {str(r[0] or "").lower(): r[1] for r in rows if r[0]}
        for stmt_name, keymap in XLSX_MAP.items():
            stmt = getattr(sheet, stmt_name)
            for canonical_key, needles in keymap.items():
                cur = stmt.lines.get(canonical_key)
                if cur is None:
                    continue
                matched = None
                for needle in needles:
                    for lbl, val in label_val.items():
                        if needle in lbl and val is not None:
                            matched = val
                            break
                    if matched is not None:
                        break
                if matched is None:
                    continue
                if matched == 0 and cur == 0:
                    continue
                denom = abs(matched) or abs(cur) or 1.0
                dev = abs(cur - matched) / denom
                if dev > tolerance:
                    log.warning(
                        f"XLSX DEVIATION {stmt_name}.{canonical_key}: "
                        f"fact={cur} vs xlsx={matched} (dev={dev:.2%})"
                    )
    finally:
        conn.close()


# ── IO ─────────────────────────────────────────────────────────────────

def load_chunk_sheets(ticker: str, quarter: str, year: int) -> list[tuple[FactSheet, list[str]]]:
    manifest_path = FACTS_DIR / f"{ticker}_{quarter}_{year}_manifest.json"
    covers_by_file: dict[str, list[str]] = {}
    if manifest_path.exists():
        for m in json.loads(manifest_path.read_text(encoding="utf-8")):
            covers_by_file[m["file"]] = m.get("covers", [])
    out: list[tuple[FactSheet, list[str]]] = []
    for f in sorted(FACTS_DIR.glob(f"{ticker}_{quarter}_{year}_chunk*.yaml")):
        data = yaml_io.load_file_with_repair(f, context=f"chunk sheet {f.name}")
        sheet = FactSheet.model_validate(data)
        out.append((sheet, covers_by_file.get(f.name, [])))
    return out


def _has_statement_values(sheet: FactSheet) -> bool:
    for stmt_name in ("income_statement", "balance_sheet", "cash_flow"):
        block = getattr(sheet, stmt_name)
        for column in ("lines", "prior_lines"):
            values = getattr(block, column) or {}
            if any(v is not None for v in values.values()):
                return True
    return False


def _has_details(sheet: FactSheet) -> bool:
    return any(getattr(sheet.details, field) for field in DETAIL_DEDUP_KEYS)


def run(ticker: str, year: int, quarter: str, force: bool = False, dry_run: bool = False) -> dict:
    out_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    if out_path.exists() and not force and not dry_run:
        print(f"  [SKIP] {out_path.name} (already merged)")
        return {"status": "skipped", "output": str(out_path)}

    chunk_count = len(list(FACTS_DIR.glob(f"{ticker}_{quarter}_{year}_chunk*.yaml")))
    seed = xlsx_seed.seed_from_xlsx(ticker, year, quarter, DEFAULT_DB_PATH)
    if dry_run:
        print(f"  [DRY] would merge {chunk_count} chunk sheet(s)"
              f"{' + xlsx seed' if seed else ''} -> {out_path.name}")
        return {"status": "dry", "chunks": chunk_count, "seeded": seed is not None}

    sheets = load_chunk_sheets(ticker, quarter, year)
    if seed is not None:
        sheets.insert(0, (seed, ["xlsx"]))
    if not sheets:
        raise FileNotFoundError(
            f"No chunk fact sheets and no xlsx seed found for {ticker}_{quarter}_{year}. "
            "Run extract first (or parse the xlsx into pipeline.duckdb)."
        )

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUNS_DIR / f"{ticker}_{year}_{quarter}.merge.log"
    logger = logging.getLogger(f"merge.{ticker}.{year}.{quarter}")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)
    logger.propagate = False

    if seed is not None:
        gaps = xlsx_seed.missing_keys(seed)
        logger.info(f"xlsx seed active; keys left for LLM gap-fill: {gaps or 'none'}")

    canonical = merge_sheets(sheets, logger)
    if not _has_statement_values(canonical) and not _has_details(canonical):
        raise RuntimeError(
            f"merge produced no statement values and no details for "
            f"{ticker}_{quarter}_{year}"
        )
    if seed is None:
        xlsx_cross_validate(canonical, DEFAULT_DB_PATH, 0.005, logger)
    else:
        logger.info("xlsx cross-validation skipped (canonical statements seeded from xlsx)")

    out_path.write_text(
        yaml.safe_dump(canonical.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    logger.info(f"merged {len(sheets)} sheet(s) ({'seed + ' if seed else ''}{len(sheets) - (1 if seed else 0)} chunk) -> {out_path.name}")
    n_warns = len([l for l in open(log_path, encoding="utf-8").read().splitlines() if "WARNING" in l])
    print(f"  [MERGE] {len(sheets)} sheets{' (incl. xlsx seed)' if seed else ''} -> {out_path.name} ({n_warns} warning(s) in {log_path.name})")
    return {"status": "success", "output": str(out_path), "chunks": len(sheets),
            "seeded": seed is not None, "warnings": n_warns, "log": str(log_path)}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 2a merge")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    print(run(a.ticker, a.year, a.quarter, force=a.force, dry_run=a.dry_run))