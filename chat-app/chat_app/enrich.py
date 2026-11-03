"""Additive, idempotent enrichment of canonical fact sheets (WS2.3).

For every fact sheet this pass adds four keys and nothing else:

- ``<statement>.provenance`` — ``sheet:row`` for every non-null line, resolved
  from the XLSX seed when the sheet predates provenance.
- ``operational`` — flat entries + management-stated drivers from the
  operational JSON.
- ``documents`` — document records from the operational JSON and any
  ``idx_data/operational/{TICKER}/manifest.json``.
- ``completeness`` — the layer-completeness block from ``operational.py``.

Statement values are never touched and unknown keys are never dropped. The
pass records ``meta.enriched_at`` plus ``meta.enriched_hash``; when the hash and
the additive content are unchanged the file is not rewritten, so a second run
produces byte-identical YAML.

Usage::

    PYTHONPATH=chat-app uv run python -m chat_app.enrich --all
    PYTHONPATH=chat-app uv run python -m chat_app.enrich --ticker ITMG --period Q2 --year 2026
    PYTHONPATH=chat-app uv run python -m chat_app.enrich --all --dry-run
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from . import facts, operational, paths

_FACT_FILE_RE = re.compile(
    r"^(?P<ticker>[A-Z]{4})_(?P<quarter>Q[1-4])_(?P<year>20\d{2})\.yaml$"
)

ENRICH_VERSION = 1


def fact_files(
    *,
    ticker: str | None = None,
    quarter: str | None = None,
    year: int | None = None,
    all_sheets: bool = False,
) -> list[Path]:
    """Select canonical fact sheets (chunks, ratios and manifests excluded)."""
    directory = paths.FACTS_DIR
    if not directory.exists():
        return []
    if not all_sheets and not ticker:
        raise paths.ToolInputError("pass --all or --ticker TICKER")
    tk = paths.normalize_ticker(ticker) if ticker else None
    q = paths.normalize_quarter(quarter) if quarter else None
    y = paths.normalize_year(year) if year is not None else None

    found: list[Path] = []
    for path in sorted(directory.glob("*.yaml")):
        match = _FACT_FILE_RE.match(path.name)
        if not match:
            continue
        if tk and match.group("ticker") != tk:
            continue
        if q and match.group("quarter") != q:
            continue
        if y and int(match.group("year")) != y:
            continue
        found.append(path)
    return found


def _seed_provenance(
    ticker: str, quarter: str, year: int
) -> dict[str, dict[str, dict[str, Any]]]:
    seed = facts._seed(ticker, quarter, year)  # noqa: SLF001 — read-only cache
    if seed is None:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for statement in paths.STATEMENTS:
        block = getattr(seed, statement, None)
        if block is None:
            continue
        table: dict[str, dict[str, Any]] = {}
        for key, provenance in (getattr(block, "provenance", {}) or {}).items():
            if hasattr(provenance, "model_dump"):
                provenance = provenance.model_dump(exclude_none=True)
            if isinstance(provenance, dict) and provenance:
                table[key] = provenance
        out[statement] = table
    return out


def _build_additive(
    ticker: str, quarter: str, year: int, sheet: dict[str, Any]
) -> dict[str, Any]:
    """Compute the additive keys for one sheet without mutating it."""
    seed = _seed_provenance(ticker, quarter, year)
    provenance: dict[str, dict[str, dict[str, Any]]] = {}
    for statement in paths.STATEMENTS:
        block = sheet.get(statement) or {}
        lines = block.get("lines") or {}
        existing = block.get("provenance") or {}
        table: dict[str, dict[str, Any]] = {
            key: value
            for key, value in (existing or {}).items()
            if isinstance(value, dict) and value
        }
        for key in sorted(lines, key=lambda k: _line_order(statement, k)):
            if lines.get(key) is None or key in table:
                continue
            resolved = seed.get(statement, {}).get(key)
            if resolved:
                table[key] = resolved
        if table:
            provenance[statement] = table

    op = operational.load_operational(
        ticker, quarter, year, include_fact_sheet=True
    )
    op_block = {
        "entries": op.get("entries") or [],
        "drivers": op.get("drivers") or [],
    }
    documents = [
        operational._document_record(doc)  # noqa: SLF001 — same package contract
        for doc in op.get("documents") or []
    ]
    existing_docs = {
        str(doc.get("id")): doc
        for doc in sheet.get("documents") or []
        if isinstance(doc, dict) and doc.get("id")
    }
    merged_documents = []
    for doc in documents:
        base = existing_docs.get(str(doc.get("id")), {})
        merged = dict(base)
        for key, value in doc.items():
            if value is not None or key not in merged:
                merged[key] = value
        merged_documents.append(merged)
    documents = merged_documents
    completeness = operational.completeness(
        ticker,
        quarter,
        year,
        operational_block={**op_block, "documents": documents},
    )
    payload = {
        "version": ENRICH_VERSION,
        "provenance": provenance,
        "operational": op_block,
        "documents": documents,
        "completeness": completeness,
    }
    return payload


def _line_order(statement: str, key: str) -> int:
    try:
        keys = facts._schema().canonical_line_keys()[statement]  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return 0
    return keys.index(key) if key in keys else len(keys)


def content_hash(payload: dict[str, Any]) -> str:
    """Stable sha256 over the additive content only (no timestamps)."""
    body = {
        "version": payload.get("version", ENRICH_VERSION),
        "provenance": payload.get("provenance") or {},
        "operational": payload.get("operational") or {},
        "documents": payload.get("documents") or [],
        "completeness": payload.get("completeness") or {},
    }
    text = json.dumps(body, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _merged_provenance(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Final provenance per statement (existing entries preserved verbatim)."""
    merged: dict[str, dict[str, Any]] = {}
    for statement in paths.STATEMENTS:
        table = payload.get("provenance", {}).get(statement) or {}
        if table:
            merged[statement] = table
    return merged


def _apply(sheet: dict[str, Any], payload: dict[str, Any], digest: str, now: str) -> None:
    for statement, table in _merged_provenance(payload).items():
        block = sheet.setdefault(statement, {})
        block["provenance"] = table
    sheet["operational"] = payload["operational"]
    sheet["documents"] = payload["documents"]
    sheet["completeness"] = payload["completeness"]
    meta = sheet.setdefault("meta", {})
    meta["enriched_at"] = now
    meta["enriched_hash"] = digest


def _is_current(sheet: dict[str, Any], payload: dict[str, Any], digest: str) -> bool:
    meta = sheet.get("meta") or {}
    if meta.get("enriched_hash") != digest:
        return False
    merged = _merged_provenance(payload)
    for statement in paths.STATEMENTS:
        current = (sheet.get(statement) or {}).get("provenance") or {}
        if current != merged.get(statement, {}):
            return False
    for key in ("operational", "documents", "completeness"):
        if sheet.get(key) != payload[key]:
            return False
    return True


def enrich_sheet(path: Path, *, dry_run: bool = False) -> dict[str, Any]:
    """Enrich one fact sheet. Returns a report dict; writes unless ``dry_run``."""
    data = paths.load_yaml(path)
    if not isinstance(data, dict):
        raise paths.ChatError(f"fact sheet is not a mapping: {path}")
    meta = data.get("meta") or {}
    match = _FACT_FILE_RE.match(path.name)
    ticker = str(meta.get("ticker") or (match.group("ticker") if match else "")).upper()
    quarter = str(meta.get("quarter") or (match.group("quarter") if match else ""))
    year = meta.get("year") or (int(match.group("year")) if match else None)
    if not ticker or not quarter or year is None:
        raise paths.ChatError(f"cannot determine ticker/period for {path}")
    year = int(year)

    payload = _build_additive(ticker, quarter, year, data)
    digest = content_hash(payload)
    changed = not _is_current(data, payload, digest)
    report = {
        "file": str(path),
        "ticker": ticker,
        "period": quarter,
        "year": year,
        "changed": changed,
        "dry_run": dry_run,
        "enriched_hash": digest,
        "previous_hash": meta.get("enriched_hash"),
        "added_provenance": {
            statement: len(table)
            for statement, table in (payload.get("provenance") or {}).items()
        },
        "operational_entries": len(payload["operational"]["entries"]),
        "operational_drivers": len(payload["operational"]["drivers"]),
        "documents": len(payload["documents"]),
        "completeness": {
            key: payload["completeness"].get(key)
            for key in ("financials", "operational", "notes", "analysis")
        },
    }
    if not changed:
        report["written"] = False
        return report
    if dry_run:
        report["written"] = False
        report["would_write"] = True
        return report

    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()
    _apply(data, payload, digest, now)
    text = yaml.safe_dump(
        data, sort_keys=False, allow_unicode=True, default_flow_style=False
    )
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    report["written"] = True
    report["enriched_at"] = now
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m chat_app.enrich",
        description="Add provenance/operational/documents/completeness to fact sheets",
    )
    parser.add_argument("--all", action="store_true", help="enrich every fact sheet")
    parser.add_argument("--ticker", help="IDX ticker, e.g. ITMG")
    parser.add_argument("--period", help="Q1-Q4 (optional with --ticker)")
    parser.add_argument("--year", type=int, help="year (optional with --ticker)")
    parser.add_argument("--dry-run", action="store_true", help="report without writing")
    args = parser.parse_args(argv)

    try:
        files = fact_files(
            ticker=args.ticker,
            quarter=args.period,
            year=args.year,
            all_sheets=args.all,
        )
    except paths.ChatError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False))
        return 2
    if not files:
        print(json.dumps({
            "error": "invalid_input",
            "message": "no fact sheets matched the request",
        }))
        return 2

    reports = []
    failed = 0
    for path in files:
        try:
            reports.append(enrich_sheet(path, dry_run=args.dry_run))
        except paths.ChatError as exc:
            failed += 1
            reports.append({"file": str(path), **exc.as_dict()})
        except Exception as exc:  # noqa: BLE001 — report, keep the batch going
            failed += 1
            reports.append({
                "file": str(path),
                "error": "internal",
                "message": str(exc),
            })

    print(json.dumps({
        "ok": failed == 0,
        "dry_run": args.dry_run,
        "sheets": len(reports),
        "changed": sum(1 for r in reports if r.get("changed")),
        "written": sum(1 for r in reports if r.get("written")),
        "failed": failed,
        "reports": reports,
    }, ensure_ascii=False, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
