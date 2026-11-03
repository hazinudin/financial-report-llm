"""Typed tool implementations (CONTRACTS.md §1) with the §3 ledger.

Every successful call returns the common envelope::

    {"ok": true, "tool": ..., "request": {...}, "data": {...},
     "ledger": [...], "caveats": [...]}

No raw SQL is exposed. Every number a tool returns also appears in ``ledger`` so
``verify.py`` can audit the answer without parsing prose. Money entries carry
``[raw, millions, value_idr_bn]`` variants; ratios and counts carry their own
kind.

Errors are typed :class:`chat_app.paths.ChatError` subclasses. The CLI renders
them as JSON with exit code 2.
"""

from __future__ import annotations

import copy
import datetime as _dt
import json
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

from . import analysis_docs, facts, operational, paths, periods
from . import periods as periods_lib

RAG_SCRIPT = paths.PROJECT_ROOT / "scripts" / "notes_rag" / "rag.py"
READ_DOCUMENT_CHARS = 4000
READ_PAGE_CHARS = 8000
NOTES_TIMEOUT = 120

_PAGE_MARKER_RE = re.compile(
    r"^\s*(?:=+|-+|#{2,}|\*+)?\s*PAGE\s+(\d+)\s*(?:=+|-+|#{2,}|\*+)?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_MONEY_UNIT_RE = re.compile(
    r"\b(m|mn|million|millions|bn|billion|billions|k|thousand|thousands)\b",
    re.IGNORECASE,
)
_COUNT_UNITS = {"mt", "t", "ton", "tons", "tonne", "tonnes", "juta ton",
                "million tonnes", "million tons", "bbl", "kbbl", "mmboe"}


# ---------------------------------------------------------------------------
# Envelope, ledger, and the 12 KB cap
# ---------------------------------------------------------------------------


def _envelope(
    tool: str,
    request: dict[str, Any],
    data: dict[str, Any],
    *,
    ledger: list[dict[str, Any]] | None = None,
    caveats: list[str] | None = None,
) -> dict[str, Any]:
    return _fit({
        "ok": True,
        "tool": tool,
        "request": request,
        "data": data,
        "ledger": ledger or [],
        "caveats": caveats or [],
    })


def _fit(payload: dict[str, Any], cap: int = paths.OUTPUT_CAP_BYTES) -> dict[str, Any]:
    """Deterministically shrink a payload until it fits the output cap."""
    out = copy.deepcopy(payload)
    if _size(out) <= cap:
        return out

    # Nulls cost a line each when pretty-printed; absent == null for readers.
    out = _strip_nulls(out)
    if _size(out) <= cap:
        out["truncated"] = True
        return out

    data = out.get("data")
    if isinstance(data, dict) and isinstance(data.get("sections"), dict):
        sections = data["sections"]
        if "details" in sections:
            sections["details_omitted"] = True
            sections.pop("details", None)
        for block in sections.values():
            if not isinstance(block, dict):
                continue
            for entry in block.values():
                if not isinstance(entry, dict):
                    continue
                provenance = entry.get("provenance")
                if isinstance(provenance, dict):
                    provenance.pop("label_en", None)
                    provenance.pop("components", None)
                    provenance.pop("column", None)
                    provenance.pop("period_type", None)
        if _size(out) <= cap:
            out["truncated"] = True
            return out

        # Do not drop statement lines here: company_facts prunes data and
        # ledger together so the audit stays consistent.

    if _size(out) > cap and isinstance(out.get("caveats"), list) and len(out["caveats"]) > 2:
        text = out["caveats"][0]
        out["caveats"] = [text, "Additional caveats omitted for output cap."]
        if _size(out) <= cap:
            out["truncated"] = True
            return out

    if _size(out) > cap:
        out["truncated"] = True
    return out


def _size(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(
            payload, ensure_ascii=False, indent=2, default=str
        ).encode("utf-8")
    )


def _strip_nulls(value: Any) -> Any:
    """Recursively drop ``None`` values (JSON-null lines cost bytes)."""
    if isinstance(value, dict):
        return {
            key: _strip_nulls(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_strip_nulls(item) for item in value]
    return value


def _meta(
    ticker: str,
    period: str | None,
    year: int | None,
    metric: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    out: dict[str, Any] = {"ticker": ticker, "period": period, "year": year}
    if metric:
        out["metric"] = metric
    out.update(extra)
    return out


def _money_ledger(
    value: float | None,
    currency: str | None,
    units: str | None,
    *,
    source: str | None,
    meta: dict[str, Any],
    derived: bool = False,
    formula: str | None = None,
) -> dict[str, Any] | None:
    if value is None:
        return None
    full = value * paths.scale_multiplier(units)
    millions = round(full / 1e6, 6)
    idr_bn = paths.to_idr_bn(value, currency, units)
    entry_meta = dict(meta)
    if derived:
        entry_meta["derived"] = True
        entry_meta["formula"] = formula
    unit = periods.unit_label(currency, units)
    return {
        "value": _round(value),
        "unit": unit,
        "kind": "money",
        "source": source,
        "variants": [
            _round(value),
            millions,
            round(idr_bn, 4) if idr_bn is not None else None,
            # The prose may scale to trillions ("Rp16.43 tn"); keep a variant
            # so the citation mapper and the number audit can match it.
            round(full / 1e12, 6),
        ],
        "meta": entry_meta,
    }


def _ratio_ledger(
    value: float | None,
    *,
    unit: str = "ratio",
    source: str | None,
    meta: dict[str, Any],
) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "value": _round(value),
        "unit": unit,
        "kind": "ratio",
        "source": source,
        "variants": [_round(value), _round(float(value) * 100)],
        "meta": meta,
    }


def _count_ledger(
    value: float | None,
    *,
    unit: str,
    source: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "value": _round(value),
        "unit": unit,
        "kind": "count",
        "source": source,
        "variants": [_round(value)],
        "meta": meta or {},
    }


def _page_ledger(page: int, meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "value": page,
        "unit": "page",
        "kind": "page",
        "source": None,
        "variants": [page],
        "meta": meta,
    }


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    out = round(float(value), digits)
    return 0.0 if out == 0 else out


def _statement_ledger(
    blocks: dict[str, dict[str, dict[str, Any]]],
    meta: dict[str, Any],
    *,
    period_type: str | None = None,
) -> list[dict[str, Any]]:
    ledger: list[dict[str, Any]] = []
    for statement, lines in blocks.items():
        for key, entry in lines.items():
            provenance = entry.get("provenance") or {}
            meta_entry = _meta(
                meta.get("ticker"), meta.get("period"), meta.get("year"), key,
                period_type=entry.get("period_type") or period_type,
            )
            ledger.append(_money_ledger(
                entry.get("value"),
                entry.get("currency"),
                entry.get("units"),
                source=_source(provenance),
                meta=meta_entry,
            ))
            prior = entry.get("prior")
            if isinstance(prior, (int, float)):
                ledger.append(_money_ledger(
                    prior,
                    entry.get("currency"),
                    entry.get("units"),
                    source=_source(provenance),
                    meta={**meta_entry, "prior": True,
                          "period_type": entry.get("prior_period_type") or "prior"},
                ))
    return [entry for entry in ledger if entry]


def _source(provenance: dict[str, Any]) -> str | None:
    sheet = provenance.get("sheet")
    row = provenance.get("row")
    if sheet and row is not None:
        return f"{sheet}:{row}"
    return None


# ---------------------------------------------------------------------------
# list_companies
# ---------------------------------------------------------------------------


# Today's payload plus the artifact signature it was built from. A single slot
# is enough: the supported set is small and every artifact change replaces it.
_COMPANIES_CACHE: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {}
_COMPANIES_LOCK = threading.Lock()


def _companies_signature() -> tuple[Any, ...]:
    """Version key for every on-disk input ``list_companies`` reads.

    The globs are deliberately conservative: adding, removing or editing any
    fact sheet, operational artifact, note or filing invalidates the cache, so
    a cached answer can never outlive the files behind it. Stats are cheap
    compared with re-parsing every fact sheet and operational JSON.
    """
    return (
        paths.file_versions(paths.FACTS_DIR.glob("*.yaml")),
        paths.file_versions(paths.OPERATIONAL_DIR.rglob("*")),
        paths.file_versions(paths.ANALYSIS_DIR.rglob("*.md")),
        paths.file_versions(paths.RAW_DIR.glob("*.pdf")),
        paths.file_versions(paths.RESULTS_DIR.glob("*.md")),
        paths.file_versions(paths.PREPROCESSED_DIR.glob("*.md")),
        paths.file_versions([paths.NOTES_DB, paths.PACKAGE_DIR / "metrics.yaml"]),
    )


def clear_companies_cache() -> None:
    """Drop the cached ``list_companies`` payload (tests and manual refresh)."""
    with _COMPANIES_LOCK:
        _COMPANIES_CACHE.clear()


def list_companies() -> dict:
    """Supported tickers/periods plus per-layer completeness."""
    today = _dt.date.today()
    day = today.isoformat()
    signature = _companies_signature()
    with _COMPANIES_LOCK:
        cached = _COMPANIES_CACHE.get(day)
        if cached is not None and cached[0] == signature:
            return copy.deepcopy(cached[1])

    payload = _build_companies(today)

    with _COMPANIES_LOCK:
        _COMPANIES_CACHE.clear()
        _COMPANIES_CACHE[day] = (signature, payload)
    return copy.deepcopy(payload)


def _build_companies(today: _dt.date) -> dict:
    """Uncached ``list_companies`` payload (kept identical to the old shape)."""
    companies: list[dict[str, Any]] = []
    for ticker, entries in sorted(paths.supported_periods().items()):
        latest = entries[-1]
        try:
            comp = operational.completeness(ticker, latest["period"], latest["year"])
            financials = comp.get("financials")
            filings_state = comp.get("filings")
            notes = comp.get("notes")
        except paths.ChatError:
            financials, filings_state, notes = "missing", "missing", "missing"
        companies.append({
            "ticker": ticker,
            "periods": [f"{e['period']} {e['year']}" for e in entries],
            "operational_periods": [
                f"{e['period']} {e['year']}" for e in entries if e["has_operational"]
            ],
            "financials": financials,
            "filings": filings_state,
            "notes": notes,
        })
    summary = {
        "tickers": len(companies),
        "periods": sum(len(company["periods"]) for company in companies),
        "operational_periods": sum(
            len(company["operational_periods"]) for company in companies
        ),
        "as_of": today.isoformat(),
    }
    ledger = [
        _count_ledger(
            summary["tickers"], unit="tickers", meta={"kind": "supported_set"},
        ),
        _count_ledger(
            summary["periods"], unit="periods", meta={"kind": "supported_set"},
        ),
    ]
    return _envelope(
        "list_companies",
        {},
        {"companies": companies, "summary": summary, "as_of": today.isoformat()},
        ledger=[entry for entry in ledger if entry],
        caveats=[
            "Coverage is derived from canonical fact sheets on disk. "
            "operational_periods lists extracted operational releases, not filing "
            "coverage; use documents_status to establish filing existence.",
        ],
    )


# ---------------------------------------------------------------------------
# company_facts
# ---------------------------------------------------------------------------


def company_facts(
    ticker: str, period: str, year: int, sections: str = "core"
) -> dict:
    """Compact fact-sheet sections with provenance and completeness."""
    tk, q, y = paths.require_supported(ticker, period, year)
    sheet = facts.load_facts(tk, q, y)
    comp = operational.completeness(tk, q, y)
    data = facts.compact_sections(sheet, sections, completeness=comp)

    blocks = {
        name: block for name, block in data.get("sections", {}).items()
        if name in paths.STATEMENTS
    }
    ledger = _statement_ledger(blocks, {"ticker": tk, "period": q, "year": y})
    ledger += _details_ledger(
        data.get("sections", {}).get("details") or {},
        {"ticker": tk, "period": q, "year": y},
        sheet,
    )

    caveats = [
        f"{q} {y} income-statement and cash-flow values are cumulative for the "
        "period; balance-sheet values are point-in-time.",
        "Every line carries provenance (sheet:row) from the source XBRL filing.",
    ]
    if not (data["sections"].get("income_statement") or {}).get("cogs"):
        caveats.append(
            "This issuer's statement shape has no cost-of-sales/gross-profit "
            "lines (bank or financial-services taxonomy)."
        )

    request = {"ticker": tk, "period": q, "year": y, "sections": sections}
    envelope = _envelope(
        "company_facts", request, data, ledger=ledger, caveats=caveats
    )
    if _size(envelope) > paths.OUTPUT_CAP_BYTES:
        envelope = _prune_company_facts(envelope, data, request, caveats)
    return envelope


# Lines an answer is most likely to need. Everything else is droppable when the
# 12 KB cap bites; the pruned payload records how many lines were omitted.
_PRIORITY_LINES = (
    ("income_statement", "revenue"),
    ("income_statement", "gross_profit"),
    ("income_statement", "net_profit"),
    ("income_statement", "pbt"),
    ("income_statement", "operating_profit"),
    ("income_statement", "cogs"),
    ("income_statement", "np_owners"),
    ("income_statement", "eps_basic"),
    ("balance_sheet", "total_assets"),
    ("balance_sheet", "total_equity"),
    ("balance_sheet", "total_liabilities"),
    ("balance_sheet", "cash"),
    ("balance_sheet", "current_assets"),
    ("balance_sheet", "current_liabilities"),
    ("cash_flow", "ocf"),
    ("cash_flow", "capex_ppe"),
    ("cash_flow", "fcf"),
    ("cash_flow", "icf"),
    ("cash_flow", "cash_end"),
    ("cash_flow", "net_change"),
)


def _priority(statement: str, key: str) -> int:
    try:
        return _PRIORITY_LINES.index((statement, key))
    except ValueError:
        return 100 + _line_index(statement, key)


def _line_index(statement: str, key: str) -> int:
    try:
        keys = facts._schema().canonical_line_keys()[statement]  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return 999
    return keys.index(key) if key in keys else 999


def _prune_company_facts(
    envelope: dict[str, Any],
    data: dict[str, Any],
    request: dict[str, Any],
    caveats: list[str],
) -> dict[str, Any]:
    """Drop lowest-priority lines (and their ledger entries) to fit the cap."""
    sections = data.get("sections") or {}
    omitted: dict[str, int] = {}

    if "details" in sections:
        sections.pop("details", None)
        data["details_omitted"] = True
        statement_metrics = {
            key
            for statement in paths.STATEMENTS
            for key in (facts._schema().canonical_line_keys()[statement])  # noqa: SLF001
        }
        envelope["ledger"] = [
            entry for entry in envelope.get("ledger") or []
            if (entry.get("meta") or {}).get("metric") in statement_metrics
        ]
        caveats = list(caveats) + [
            "The details block was omitted to respect the 12 KB output cap."
        ]

    data["lines_omitted"] = omitted
    caveats = list(caveats) + [
        "Some lower-priority lines were omitted to respect the 12 KB output "
        "cap; request a specific line with metric_series."
    ]

    while True:
        envelope = _envelope(
            "company_facts",
            request,
            data,
            ledger=envelope.get("ledger") or [],
            caveats=caveats,
        )
        if _size(envelope) <= paths.OUTPUT_CAP_BYTES:
            break
        candidates: list[tuple[int, str, str]] = []
        for statement in paths.STATEMENTS:
            block = sections.get(statement) or {}
            for key in block:
                candidates.append((_priority(statement, key), statement, key))
        if not candidates:
            break
        _score, statement, key = max(candidates)
        sections.get(statement, {}).pop(key, None)
        omitted[statement] = omitted.get(statement, 0) + 1
        envelope["ledger"] = [
            entry for entry in envelope.get("ledger") or []
            if (entry.get("meta") or {}).get("metric") != key
        ]

    if omitted:
        envelope["truncated"] = True
    return envelope


def _details_ledger(
    details: dict[str, Any],
    meta: dict[str, Any],
    sheet: dict[str, Any],
) -> list[dict[str, Any]]:
    """Ledger entries for detail rows that carry money-like values."""
    if not details:
        return []
    sheet_meta = sheet.get("meta") or {}
    currency = sheet_meta.get("currency")
    units = sheet_meta.get("units")
    fields = {
        "customers": ("amount", "pct_of_sales"),
        "loans": ("balance",),
        "subsidiaries": ("total_assets",),
        "segments": ("external_sales", "internal_sales", "gross_profit",
                     "net_profit", "assets"),
        "production": ("volume_mt",),
        "reserves": ("residual_mt",),
        "related_parties": ("amount",),
        "commitments": ("amount",),
        "dividends": ("per_share", "total"),
    }
    ledger: list[dict[str, Any]] = []
    for name, rows in details.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            label = row.get("name") or row.get("product") or row.get("party") \
                or row.get("description") or name
            for field in fields.get(name, ()):
                value = row.get(field)
                if not isinstance(value, (int, float)):
                    continue
                item_meta = _meta(
                    meta.get("ticker"), meta.get("period"), meta.get("year"),
                    f"{name}.{field}",
                    detail=str(label),
                )
                if field == "pct_of_sales":
                    entry = _ratio_ledger(
                        value, unit="ratio", source=None, meta=item_meta
                    )
                elif field in ("volume_mt", "residual_mt"):
                    entry = _count_ledger(
                        value, unit="Mt", source=None, meta=item_meta
                    )
                elif field == "per_share":
                    entry = _ratio_ledger(
                        value, unit=currency or "currency", source=None,
                        meta=item_meta,
                    )
                else:
                    entry = _money_ledger(
                        value, currency, units, source=None, meta=item_meta
                    )
                if entry:
                    ledger.append(entry)
    return ledger


# ---------------------------------------------------------------------------
# metric_series
# ---------------------------------------------------------------------------


def _parse_periods(
    ticker: str,
    period_list: Iterable[str] | None,
    year: int | None,
) -> list[tuple[str, int]]:
    supported = {
        (entry["period"], entry["year"])
        for entry in paths.supported_periods().get(ticker, [])
    }
    available_years = sorted({y for _q, y in supported})
    if isinstance(period_list, str):
        period_list = [
            part.strip() for part in period_list.split(",") if part.strip()
        ]
    if period_list is None:
        wanted = set(supported)
        if year is not None:
            wanted = {item for item in wanted if item[1] == year}
        if not wanted:
            raise paths.UnsupportedRequest(
                f"{ticker} has no supported periods in {year}",
                alternatives=[f"{q} {y}" for q, y in sorted(supported)],
            )
        return sorted(wanted, key=lambda item: (item[1], paths.PERIOD_ORDER[item[0]]))

    out: list[tuple[str, int]] = []
    for raw in period_list:
        text = str(raw).strip().upper()
        if not text:
            continue
        match = re.fullmatch(r"(?:FY\s*)?(20\d{2})\s*[-/ ]?\s*(Q[1-4])", text)
        if match:
            q, y = match.group(2), int(match.group(1))
        else:
            match = re.fullmatch(r"(Q[1-4])\s*[-/ ]?\s*(20\d{2})", text)
            if match:
                q, y = match.group(1), int(match.group(2))
            elif text in paths.QUARTERS:
                q = text
                if year is not None:
                    y = paths.normalize_year(year)
                elif len(available_years) == 1:
                    y = available_years[0]
                else:
                    raise paths.ToolInputError(
                        f"ambiguous period {raw!r}; include the year (e.g. 'Q2 2026')",
                        years=available_years,
                    )
            else:
                raise paths.ToolInputError(
                    f"period must look like Q2 or 'Q2 2026', got {raw!r}"
                )
        if (q, y) not in supported:
            raise paths.UnsupportedRequest(
                f"{ticker} {q} {y} is not in the supported set",
                alternatives=[f"{pq} {py}" for pq, py in sorted(supported)],
            )
        if (q, y) not in out:
            out.append((q, y))
    return sorted(out, key=lambda item: (item[1], paths.PERIOD_ORDER[item[0]]))


def metric_series(
    ticker: str,
    metric: str,
    periods: list[str] | None = None,
    year: int | None = None,
) -> dict:
    """Values for one canonical metric across supported periods."""
    tk = paths.normalize_ticker(ticker)
    if not paths.supported_periods().get(tk):
        raise paths.UnsupportedRequest(
            f"{tk} is not in the supported set",
            supported_tickers=paths.supported_tickers(),
        )
    statement, key = facts.resolve_metric(metric)
    wanted = _parse_periods(tk, periods, year)

    series: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    missing_periods: list[str] = []
    for q, y in wanted:
        lines = facts.statement_lines(tk, q, y, statement)
        entry = lines.get(key)
        if not entry:
            missing_periods.append(f"{q} {y}")
            series.append({
                "period": q, "year": y, "value": None, "unit": None,
                "period_type": periods_lib.period_type(statement),
                "value_idr_bn": None, "source": None, "reconciled": None,
            })
            continue
        provenance = entry.get("provenance") or {}
        source = _source(provenance)
        reconciled = _reconciled_operational(
            tk, q, y, key, entry["value"],
            entry.get("currency"), entry.get("units"),
        )
        series.append({
            "period": q,
            "year": y,
            "value": _round(entry["value"]),
            "unit": periods_lib.unit_label(entry.get("currency"), entry.get("units")),
            "period_type": entry.get("period_type"),
            "value_idr_bn": _round(entry.get("value_idr_bn")),
            "source": source,
            "reconciled": reconciled,
        })
        money = _money_ledger(
            entry["value"], entry.get("currency"), entry.get("units"),
            source=source,
            meta=_meta(tk, q, y, key, statement=statement,
                       period_type=entry.get("period_type")),
        )
        if money:
            ledger.append(money)
        if reconciled and reconciled.get("value") is not None:
            release = _money_ledger(
                reconciled["value"],
                reconciled.get("currency") or "USD",
                reconciled.get("units"),
                source=reconciled.get("source"),
                meta=_meta(tk, q, y, key, statement=statement,
                           period_type="release"),
            )
            if release:
                ledger.append(release)

    caveats = [
        "Income-statement and cash-flow values are cumulative through the "
        "quarter; use period_derive for a single-quarter value.",
        "Units are the filing's units; value_idr_bn is a display conversion.",
    ]
    if missing_periods:
        caveats.append(
            "No value for: " + ", ".join(missing_periods) + "."
        )
    return _envelope(
        "metric_series",
        {"ticker": tk, "metric": key, "periods": periods, "year": year},
        {"ticker": tk, "metric": key, "series": series},
        ledger=ledger,
        caveats=caveats,
    )


def _reconciled_operational(
    ticker: str,
    quarter: str,
    year: int,
    metric: str,
    statement_value: float,
    statement_currency: str | None,
    statement_units: str | None,
) -> dict[str, Any] | None:
    try:
        op = operational.load_operational(ticker, quarter, year)
    except paths.ChatError:
        return None
    found = operational._has_metric(op, metric)  # noqa: SLF001 — shared matcher
    if not found:
        return None
    value = found.get("value")
    currency = _currency_from_unit(found.get("unit"))
    units = _scale_from_unit(found.get("unit"))
    delta = None
    if isinstance(value, (int, float)) and statement_value:
        normalized = periods_lib.convert_value(
            value, currency, units, statement_currency, statement_units
        )
        if normalized is not None:
            delta = round((normalized - statement_value) / statement_value * 100, 4)
    return {
        "value": value,
        "unit": found.get("unit"),
        "currency": currency,
        "units": units,
        "source": found.get("source_doc"),
        "page": found.get("page"),
        "where": found.get("where"),
        "method": (found.get("verification") or {}).get("method")
        if isinstance(found.get("verification"), dict) else None,
        "delta_pct": delta,
    }


def _currency_from_unit(unit: str | None) -> str | None:
    text = (unit or "").lower()
    if "usd" in text or "us$" in text:
        return "USD"
    if "idr" in text or "rp" in text:
        return "IDR"
    return None


def _scale_from_unit(unit: str | None) -> str | None:
    text = (unit or "").lower()
    if re.search(r"\bbn\b|billion", text):
        return "billion"
    if re.search(r"\bm\b|mn\b|million", text):
        return "million"
    if re.search(r"\bk\b|thousand", text):
        return "thousand"
    return None


# ---------------------------------------------------------------------------
# period_derive
# ---------------------------------------------------------------------------


def period_derive(ticker: str, period: str, year: int, metric: str) -> dict:
    """Derive one quarter from cumulative filings, with operands and sources."""
    tk, q, y = paths.require_supported(ticker, period, year)
    derived = periods_lib.derive_quarter(tk, metric, q, y)
    key = derived["metric"]
    ledger: list[dict[str, Any]] = []
    for operand, value in derived["operands"].items():
        money = _money_ledger(
            value, derived.get("currency"), derived.get("units"),
            source=derived["source"].get(operand),
            meta=_meta(tk, q, y, key, operand=operand,
                       period_type="cumulative"),
        )
        if money:
            ledger.append(money)
    result = _money_ledger(
        derived["quarter_value"], derived.get("currency"), derived.get("units"),
        source=derived["source"].get(next(iter(derived["source"]))),
        meta=_meta(tk, q, y, key, period_type="quarter"),
        derived=True,
        formula=derived["formula"],
    )
    if result:
        ledger.append(result)

    if q == "Q1":
        caveat = (
            "Q1 has no prior cumulative period: the quarter value equals the "
            "Q1 cumulative filing value."
        )
    else:
        caveat = (
            f"{q} quarter = {derived['formula']} (cumulative), all values in "
            f"{derived['unit']}."
        )
    return _envelope(
        "period_derive",
        {"ticker": tk, "period": q, "year": y, "metric": key},
        derived,
        ledger=ledger,
        caveats=[
            caveat,
            "Derived values inherit the source filing's units; the IDR value "
            "uses a labelled static FX rate.",
        ],
    )


# ---------------------------------------------------------------------------
# documents_status
# ---------------------------------------------------------------------------


def documents_status(
    ticker: str,
    period: str | None = None,
    year: int | None = None,
    item: str | None = None,
) -> dict:
    """Existence/state answer with evidence (delegates to operational.py)."""
    data = operational.documents_status(ticker, period, year, item)
    ledger: list[dict[str, Any]] = []
    docs = data.get("documents") or []
    count = _count_ledger(
        len(docs), unit="documents", meta=_meta(
            data.get("ticker"), data.get("period"), data.get("year"),
            "documents",
        ),
    )
    if count and docs:
        ledger.append(count)
    pages = (data.get("evidence") or {}).get("pages")
    if isinstance(pages, int):
        page_entry = _count_ledger(
            pages, unit="pages",
            meta=_meta(data.get("ticker"), data.get("period"), data.get("year"),
                       "pages"),
        )
        if page_entry:
            ledger.append(page_entry)
    return _envelope(
        "documents_status",
        {"ticker": ticker, "period": period, "year": year, "item": item},
        data,
        ledger=ledger,
        caveats=[
            "States are deterministic: unsupported | not_yet_published | "
            "not_disclosed | not_captured | available. 'Absent from the "
            "captured set' never means 'does not exist'.",
        ],
    )


# ---------------------------------------------------------------------------
# operational_lookup
# ---------------------------------------------------------------------------


def operational_lookup(
    ticker: str, period: str, year: int, metric: str | None = None
) -> dict:
    """Operational entries, drivers, and documents for one period."""
    tk, q, y = paths.require_supported(ticker, period, year)
    op = operational.load_operational(tk, q, y)

    entries = op.get("entries") or []
    if metric:
        wanted = facts.canonical_metric(metric)
        entries = [
            entry for entry in entries
            if facts.canonical_metric(entry.get("metric") or "") == wanted
        ]
    drivers = op.get("drivers") or []
    documents = [
        operational._document_summary(doc)  # noqa: SLF001 — shared shape
        for doc in op.get("documents") or []
    ]

    state = "available" if entries else None
    evidence: dict[str, Any] = {}
    if not entries:
        status = operational.documents_status(
            tk, q, y, item=metric, include_filings=False,
        )
        state = status["state"]
        evidence = status.get("evidence") or {}

    data = {
        "ticker": tk,
        "period": q,
        "year": y,
        "metric": facts.canonical_metric(metric) if metric else None,
        "state": state or "available",
        "entries": entries,
        "drivers": drivers,
        "documents": documents,
        "evidence": evidence,
    }
    ledger: list[dict[str, Any]] = []
    for entry in entries:
        item = _operational_ledger(entry, tk, q, y)
        if item:
            ledger.append(item)
        prior = entry.get("prior")
        if isinstance(prior, (int, float)):
            prior_item = _operational_ledger(
                {**entry, "value": prior}, tk, q, y,
            )
            if prior_item:
                prior_item["meta"]["prior"] = True
                prior_item["source"] = (
                    f"{prior_item.get('source') or entry.get('source_doc') or ''} (prior)"
                ).strip()
                ledger.append(prior_item)
    for doc in documents:
        pages = doc.get("pages")
        if isinstance(pages, int):
            entry = _count_ledger(
                pages, unit="pages",
                meta=_meta(tk, q, y, "pages", document=doc.get("id")),
            )
            if entry:
                ledger.append(entry)

    caveats = [
        "Operational figures are management-reported from the captured "
        "release and are not XBRL-audited; quotes and pages are evidence.",
    ]
    if not entries:
        caveats.append(
            f"No operational entry matched; state={state or 'not_captured'}."
        )
    request = {"ticker": tk, "period": q, "year": y, "metric": metric}
    envelope = _envelope(
        "operational_lookup", request, data, ledger=ledger, caveats=caveats
    )
    if _size(envelope) > paths.OUTPUT_CAP_BYTES:
        envelope = _trim_operational(envelope, data, ledger, request, caveats)
    return envelope


def _analysis_context_ledger(
    items: list[dict[str, Any]],
    ticker: str,
    quarter: str,
    year: int,
    *,
    exclude: set[float] | None = None,
) -> list[dict[str, Any]]:
    """Ledger entries for numbers quoted from analyst-note snippets.

    ``analysis_context`` is a tool result, so its numbers are auditable evidence
    (secondary, not XBRL). The snippets are short and bounded, so this cannot
    flood the ledger.
    """
    out: list[dict[str, Any]] = []
    seen: set[float] = set()
    skip = exclude or set()
    for item in items:
        body = str(item.get("snippet") or "")
        source = f"notes:{item.get('id')}"
        for match in re.finditer(r"\d[\d,]*(?:\.\d+)?", body):
            try:
                value = float(match.group(0).replace(",", ""))
            except ValueError:
                continue
            key = round(value, 6)
            if value == 0 or key in seen or key in skip:
                continue
            if 2000 <= value <= 2100:  # years are structural, already whitelisted
                continue
            seen.add(key)
            out.append({
                "value": value,
                "unit": "note",
                "kind": "note",
                "source": source,
                "variants": [value],
                "meta": {
                    "document": item.get("path"),
                    "secondary": True,
                },
            })
            if len(out) >= 8:
                return out
    return out


def analysis_context(
    ticker: str,
    period: str,
    year: int,
    topic: str | None = None,
    limit: int = 4,
) -> dict:
    """Secondary analyst-note snippets for one ticker/period.

    This is the guaranteed path to operational context that lives only in
    ``analysis/*.md`` (cash cost, strip-ratio plans, guidance, mine-level
    detail). The agent prefetches it for focused chat questions, and it is also
    callable directly. It never replaces the primary operational document.
    """
    tk, q, y = paths.require_supported(ticker, period, year)
    items = operational.analysis_context(tk, q, y, metric=topic, limit=limit)
    ledger = _analysis_context_ledger(items, tk, q, y)
    data = {
        "ticker": tk,
        "period": q,
        "year": y,
        "topic": topic,
        "analysis_context": items,
    }
    caveats = [
        "Analyst-note snippets are secondary evidence, not XBRL-audited.",
        "Cite the note path; label forward-looking or interpretive statements "
        "as hypothesis. Primary release entries live in operational_lookup.",
    ]
    request = {"ticker": tk, "period": q, "year": y, "topic": topic}
    envelope = _envelope(
        "analysis_context", request, data, ledger=ledger, caveats=caveats
    )
    while _size(envelope) > paths.OUTPUT_CAP_BYTES and len(items) > 1:
        items = items[:-1]
        ledger = _analysis_context_ledger(items, tk, q, y)
        data["analysis_context"] = items
        enclosure = _envelope(
            "analysis_context", request, data, ledger=ledger, caveats=caveats
        )
        envelope = enclosure
        envelope["truncated"] = True
    return envelope


# ---------------------------------------------------------------------------
# Analysis vault: list_documents, analysis_document, analysis_search
# ---------------------------------------------------------------------------


def _document_envelope(
    tool: str,
    request: dict[str, Any],
    data: dict[str, Any],
    *,
    ledger: list[dict[str, Any]] | None = None,
    caveats: list[str] | None = None,
) -> dict[str, Any]:
    """Common envelope with the larger document-tool byte cap (~24 KB)."""
    return _fit({
        "ok": True,
        "tool": tool,
        "request": request,
        "data": data,
        "ledger": ledger or [],
        "caveats": caveats or [],
    }, cap=analysis_docs.DOCUMENT_TOOL_CAP_BYTES)


def list_documents(
    ticker: str | None = None,
    period: str | None = None,
    year: int | None = None,
    kind: str | None = None,
    limit: int = 50,
) -> dict:
    """Analysis-note metadata only (no content, no ledger numbers)."""
    try:
        count = int(limit)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"limit must be an integer, got {limit!r}") from exc
    if count < 1:
        raise paths.ToolInputError(f"limit must be >= 1, got {count}")
    count = min(count, 500)

    rows = analysis_docs.list_documents(
        ticker=ticker, period=period, year=year, kind=kind, limit=count
    )
    total = len(analysis_docs.list_documents(
        ticker=ticker, period=period, year=year, kind=kind, limit=500
    ))
    data: dict[str, Any] = {
        "documents": rows,
        "count": len(rows),
        "total": total,
        "as_of": _dt.date.today().isoformat(),
    }
    ledger = [
        _count_ledger(
            len(rows), unit="documents", meta={"kind": "analysis_vault"},
        )
    ]
    request = {
        "ticker": ticker, "period": period, "year": year, "kind": kind,
        "limit": count,
    }
    caveats = [
        "Metadata only: call analysis_document for content and section lines.",
        "The vault is the curated analysis notes; raw filings are behind "
        "read_document and documents_status.",
    ]
    envelope = _document_envelope(
        "list_documents", request, data,
        ledger=[entry for entry in ledger if entry], caveats=caveats,
    )
    omitted = 0
    while _size(envelope) > analysis_docs.DOCUMENT_TOOL_CAP_BYTES and len(rows) > 1:
        rows.pop()
        omitted += 1
        data["documents"] = rows
        data["count"] = len(rows)
        data["documents_omitted"] = omitted
        ledger = [
            _count_ledger(
                len(rows), unit="documents", meta={"kind": "analysis_vault"},
            )
        ]
        envelope = _document_envelope(
            "list_documents", request, data,
            ledger=[entry for entry in ledger if entry], caveats=caveats,
        )
    if omitted:
        envelope["truncated"] = True
    return envelope


def analysis_document(
    document_id: str, section: str | None = None, next_line: int | None = None
) -> dict:
    """One vault section, or the outline + content when it fits.

    A section larger than ~12 KB is returned in line-bounded chunks with a
    ``next_line`` cursor. The reader never truncates; this cap applies only to
    what the model sees in a tool result.
    """
    document = analysis_docs.get_document(document_id)
    if next_line is not None and not section:
        raise paths.ToolInputError("next_line requires a section")
    meta = analysis_docs.metadata(document)
    outline = list(document.get("sections") or [])
    line_map = {str(item["line"]): item["id"] for item in outline}
    section_obj: dict[str, Any] | None = None
    content: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    cursor: int | None = None
    truncated = False
    content_omitted = False

    if section:
        chunk = analysis_docs.document_section(
            document_id, section, next_line=next_line,
            max_chars=analysis_docs.SECTION_CHUNK_CHARS,
        )
        section_obj = chunk["section"]
        content = chunk["content"]
        start_line = chunk["start_line"]
        end_line = chunk["end_line"]
        cursor = chunk["next_line"]
        truncated = chunk["truncated"]
        ledger = analysis_docs.document_ledger(
            document_id, start_line=start_line, end_line=end_line, limit=80,
        )
    else:
        full = str(document.get("content") or "")
        outline_bytes = len(json.dumps(outline, ensure_ascii=False, indent=2).encode("utf-8"))
        full_bytes = len(full.encode("utf-8"))
        if full_bytes + outline_bytes + 2_500 <= analysis_docs.DOCUMENT_TOOL_CAP_BYTES:
            content = full
            start_line = 1
            end_line = int(document.get("line_count") or 0)
            ledger = analysis_docs.document_ledger(document_id, limit=80)
        else:
            content_omitted = True
            truncated = True
            ledger = []

    ledger_total = len(ledger)
    data: dict[str, Any] = {
        "document": meta,
        "section": section_obj,
        "content": content,
        "start_line": start_line,
        "end_line": end_line,
        "next_line": cursor,
        "truncated": truncated,
        "content_omitted": content_omitted,
        "ledger_total": ledger_total,
        "ledger_complete": True,
    }
    if section is None:
        # The outline is the no-section answer; a section fetch already carries
        # its own line range and should spend its byte budget on evidence.
        data["outline"] = outline
        data["line_map"] = line_map
    request = {"document_id": document_id, "section": section, "next_line": next_line}
    caveats = [
        "Section text is verbatim from the analysis vault; line numbers are "
        "1-based and include YAML frontmatter.",
        "Numbers from this note are document-ledger evidence: label note claims "
        "analysis_note, never fact. A section may need extra fetches when "
        "next_line is set.",
    ]
    envelope = _document_envelope(
        "analysis_document", request, data, ledger=ledger, caveats=caveats,
    )
    omitted = 0
    while (
        _size(envelope) > analysis_docs.DOCUMENT_TOOL_CAP_BYTES and ledger
    ):
        drop = max(1, min(len(ledger), 8))
        for _ in range(drop):
            ledger.pop()
            omitted += 1
        data["ledger_omitted"] = omitted
        envelope = _document_envelope(
            "analysis_document", request, data, ledger=ledger, caveats=caveats,
        )
    data["ledger_complete"] = (not omitted) and len(ledger) == ledger_total
    envelope = _document_envelope(
        "analysis_document", request, data, ledger=ledger, caveats=caveats,
    )
    if omitted:
        envelope["truncated"] = True
    if (
        _size(envelope) > analysis_docs.DOCUMENT_TOOL_CAP_BYTES
        and section is None
        and content is not None
    ):
        # Very large outline plus full content: fall back to the outline answer.
        data["content"] = None
        data["content_omitted"] = True
        data["truncated"] = True
        data["ledger_omitted"] = data.get("ledger_omitted", 0) + len(ledger)
        data["ledger_complete"] = True
        envelope = _document_envelope(
            "analysis_document", request, data, ledger=[], caveats=caveats,
        )
        envelope["truncated"] = True
    return envelope


def _matches_ledger(matches: list[dict[str, Any]], *, limit: int = 24) -> list[dict[str, Any]]:
    by_document: dict[str, set[int]] = {}
    for match in matches:
        document_id = str(match.get("document") or "")
        if not document_id:
            continue
        by_document.setdefault(document_id, set()).add(int(match.get("line") or 0))
    ledger: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for document_id, lines in by_document.items():
        try:
            entries = analysis_docs.document_ledger(
                document_id, lines=lines, limit=limit,
            )
        except paths.ChatError:
            continue
        for entry in entries:
            key = (
                str(entry.get("source") or ""),
                float(entry.get("value") or 0),
                str(entry.get("unit") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            ledger.append(entry)
            if len(ledger) >= limit:
                return ledger
    return ledger


def analysis_search(
    query: str,
    document_id: str | None = None,
    ticker: str | None = None,
    limit: int = 8,
) -> dict:
    """Ranked note snippets; document-scoped emptiness is ``not_in_document``."""
    text = str(query or "").strip()
    if not text:
        raise paths.ToolInputError("query must not be empty")
    try:
        count = int(limit)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"limit must be an integer, got {limit!r}") from exc
    if count < 1:
        raise paths.ToolInputError(f"limit must be >= 1, got {count}")
    count = min(count, 50)
    tk = paths.normalize_ticker(ticker) if ticker else None

    matches = analysis_docs.search_documents(
        text, document_id=document_id, ticker=tk, limit=count,
    )
    data: dict[str, Any] = {
        "query": text,
        "document_id": document_id,
        "ticker": tk,
        "matches": matches,
    }
    if document_id and not matches:
        document = analysis_docs.get_document(document_id)
        data["state"] = "not_in_document"
        data["evidence"] = {
            "document": document.get("id"),
            "sections_searched": len(document.get("sections") or []),
            "terms": analysis_docs.query_terms(text),
        }

    ledger = _matches_ledger(matches) if matches else []
    request = {
        "query": text, "document_id": document_id, "ticker": tk,
        "limit": count,
    }
    caveats = [
        "Scoped search proves note silence only when state=not_in_document; an "
        "empty matches list without that state is not evidence.",
        "Scoped searches require at least two distinctive terms on one line; "
        "phrase matches rank higher than partial matches.",
        "Snippets are note prose, not XBRL; number-bearing claims remain "
        "analysis_note and need document+section+line+quote citations.",
    ]
    envelope = _document_envelope(
        "analysis_search", request, data, ledger=ledger, caveats=caveats,
    )
    if not matches:
        return envelope
    omitted = 0
    while _size(envelope) > analysis_docs.DOCUMENT_TOOL_CAP_BYTES and ledger:
        ledger.pop()
        data["ledger_omitted"] = data.get("ledger_omitted", 0) + 1
        envelope = _document_envelope(
            "analysis_search", request, data, ledger=ledger, caveats=caveats,
        )
    while _size(envelope) > analysis_docs.DOCUMENT_TOOL_CAP_BYTES and len(matches) > 1:
        matches.pop()
        omitted += 1
        data["matches"] = matches
        data["matches_omitted"] = omitted
        ledger = _matches_ledger(matches) if matches else []
        envelope = _document_envelope(
            "analysis_search", request, data,
            ledger=ledger, caveats=caveats,
        )
    if omitted:
        envelope["truncated"] = True
    return envelope


# ---------------------------------------------------------------------------
# compare_supported: a bounded, cited matrix (never a ranking)
# ---------------------------------------------------------------------------

_MARGIN_LINES = {
    "gross_margin": "gross_profit",
    "operating_margin": "operating_profit",
    "net_margin": "net_profit",
    "pbt_margin": "pbt",
    "ocf_margin": "ocf",
}


def _string_list(value: Any, *, flag: str) -> list[str]:
    if isinstance(value, str):
        items = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        items = [str(part).strip() for part in value]
    else:
        raise paths.ToolInputError(f"{flag} must be a list or comma-separated string")
    out: list[str] = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def _metric_mode(key: str) -> tuple[str, str | None]:
    """Return ``(mode, base)`` for a compare metric."""
    if key.endswith("_growth"):
        return "growth", facts.canonical_metric(key[:-len("_growth")])
    if key in _MARGIN_LINES:
        return "margin", _MARGIN_LINES[key]
    if facts.statement_for_metric(key) is not None:
        return "financial", key
    if key in operational._grammar():  # noqa: SLF001 — shared metric grammar
        return "operational", key
    return "unsupported", None


def _operational_source(entry: dict[str, Any]) -> str:
    doc = entry.get("source_doc") or "captured"
    page = entry.get("page")
    return f"operational:{doc}:{page}" if page is not None else f"operational:{doc}"


def _cell_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence") or {}
    keys = (
        "document_id", "page", "page_range", "pages", "url", "error",
        "http_status", "retrieved_at", "detail", "supported_periods",
    )
    compact = {key: evidence.get(key) for key in keys if evidence.get(key) is not None}
    if not compact:
        compact = {"detail": payload.get("detail") or payload.get("state") or "unavailable"}
    return compact


def _compare_financial(
    ticker: str, quarter: str, year: int, key: str
) -> dict[str, Any]:
    statement = facts.statement_for_metric(key)
    if statement is None:
        return {"status": "unsupported"}
    try:
        lines = facts.statement_lines(ticker, quarter, year, statement)
    except paths.ChatError as exc:
        return {"status": "not_disclosed", "evidence": {"detail": str(exc)}}
    entry = lines.get(key)
    if not entry or entry.get("value") is None:
        if statement == "income_statement" and key in ("cogs", "gross_profit") \
                and not lines.get("cogs") and not lines.get("gross_profit"):
            reason = f"{ticker} has no COGS or gross-profit line (bank shape)"
            return {
                "status": "not_applicable",
                "excluded_reason": reason,
                "evidence": {
                    "document_id": f"{ticker}_{quarter}_{year}.yaml",
                    "detail": reason,
                },
            }
        return {
            "status": "not_disclosed",
            "evidence": {
                "document_id": f"{ticker}_{quarter}_{year}.yaml",
                "detail": f"{ticker} does not report {key}",
                "statement": statement,
            },
        }
    provenance = entry.get("provenance") or {}
    source = _source(provenance) or f"fact:{ticker}_{quarter}_{year}.yaml:{key}"
    unit = periods.unit_label(entry.get("currency"), entry.get("units"))
    meta = {
        "ticker": ticker, "period": quarter, "year": year, "metric": key,
    }
    ledger = _money_ledger(
        entry.get("value"), entry.get("currency"), entry.get("units"),
        source=source, meta=meta,
    )
    return {
        "status": "ok",
        "value": _round(entry.get("value")),
        "unit": unit,
        "period_type": entry.get("period_type"),
        "source": source,
        "ledger": [ledger] if ledger else [],
    }


def _compare_operational(
    ticker: str, quarter: str, year: int, key: str
) -> dict[str, Any]:
    op = operational.load_operational(ticker, quarter, year)
    found = operational._has_metric(op, key)  # noqa: SLF001 — shared matcher
    if not found:
        status = operational.documents_status(ticker, quarter, year, item=key)
        return {
            "status": status.get("state") or "not_captured",
            "evidence": _cell_evidence(status),
        }
    value = found.get("value")
    if not isinstance(value, (int, float)):
        return {
            "status": "not_disclosed",
            "evidence": {"detail": f"{key} has no numeric value in the captured release"},
        }
    scope = str(found.get("scope") or "").upper()
    period_type = "cumulative" if scope.startswith(("6M", "9M", "12M", "FY")) else "quarter"
    source = _operational_source(found)
    entry = _operational_ledger(found, ticker, quarter, year)
    if entry:
        entry["source"] = source
        entry["meta"] = {
            "ticker": ticker, "period": quarter, "year": year, "metric": key,
        }
    return {
        "status": "ok",
        "value": _round(value),
        "unit": found.get("unit") or (entry or {}).get("unit"),
        "period_type": period_type,
        "source": source,
        "ledger": [entry] if entry else [],
    }


def _compare_margin(
    ticker: str, quarter: str, year: int, numerator_key: str
) -> dict[str, Any]:
    try:
        income = facts.statement_lines(ticker, quarter, year, "income_statement")
    except paths.ChatError as exc:
        return {"status": "not_disclosed", "evidence": {"detail": str(exc)}}
    numerator = income.get(numerator_key)
    revenue = income.get("revenue")
    if not numerator or numerator.get("value") is None or not revenue or not revenue.get("value"):
        reason = (
            f"{ticker} has no {numerator_key} line (bank/financial statement shape)"
            if not numerator
            else f"{ticker} is missing {numerator_key} or revenue"
        )
        return {
            "status": "not_applicable",
            "excluded_reason": reason,
            "evidence": {
                "document_id": f"{ticker}_{quarter}_{year}.yaml",
                "detail": reason,
            },
        }
    value = float(numerator["value"]) / float(revenue["value"])
    numerator_source = _source(numerator.get("provenance") or {}) or "fact"
    revenue_source = _source(revenue.get("provenance") or {}) or "fact"
    source = f"{numerator_source}+{revenue_source}"
    ledger = _ratio_ledger(
        value, unit="ratio", source=source,
        meta={
            "ticker": ticker, "period": quarter, "year": year,
            "metric": numerator_key, "derived": True,
            "operands": {
                "numerator": _round(numerator.get("value")),
                "denominator": _round(revenue.get("value")),
            },
        },
    )
    return {
        "status": "ok",
        "value": _round(value),
        "unit": "ratio",
        "period_type": "ratio",
        "source": source,
        "ledger": [ledger] if ledger else [],
    }


def _compare_growth(
    ticker: str, quarter: str, year: int, base: str
) -> dict[str, Any]:
    statement = facts.statement_for_metric(base)
    if statement is None:
        return {"status": "unsupported"}
    try:
        lines = facts.statement_lines(ticker, quarter, year, statement)
    except paths.ChatError as exc:
        return {"status": "not_disclosed", "evidence": {"detail": str(exc)}}
    current = lines.get(base)
    if not current or current.get("value") is None:
        return {"status": "not_disclosed", "evidence": {"detail": f"no {base} value"}}
    prior = current.get("prior")
    if not isinstance(prior, (int, float)):
        return {
            "status": "not_disclosed",
            "evidence": {"detail": f"no prior-period {base} value for growth"},
        }
    if float(prior) == 0:
        return {"status": "not_disclosed", "evidence": {"detail": f"prior {base} is zero"}}
    value = (float(current["value"]) - float(prior)) / float(prior) * 100.0
    source = _source(current.get("provenance") or {}) or "fact"
    ledger = {
        "value": _round(value),
        "unit": "%",
        "kind": "ratio",
        "source": source,
        "variants": [_round(value), _round(value / 100.0)],
        "meta": {
            "ticker": ticker, "period": quarter, "year": year,
            "metric": f"{base}_growth", "derived": True,
            "operands": {
                "current": _round(current["value"]),
                "prior": _round(prior),
            },
        },
    }
    return {
        "status": "ok",
        "value": _round(value),
        "unit": "%",
        "period_type": "growth",
        "source": source,
        "ledger": [ledger],
    }


def _compare_cell(
    ticker: str, quarter: str, year: int, key: str, mode: str, base: str | None
) -> dict[str, Any]:
    if mode == "financial":
        result = _compare_financial(ticker, quarter, year, key)
    elif mode == "operational":
        result = _compare_operational(ticker, quarter, year, key)
    elif mode == "margin":
        result = _compare_margin(ticker, quarter, year, str(base))
    elif mode == "growth":
        result = _compare_growth(ticker, quarter, year, str(base))
    else:
        result = {"status": "unsupported"}
    cell: dict[str, Any] = {
        "ticker": ticker,
        "value": result.get("value"),
        "status": result.get("status", "not_disclosed"),
    }
    for name in ("unit", "period_type", "source", "evidence"):
        if result.get(name) is not None:
            cell[name] = result[name]
    return {"cell": cell, "ledger": result.get("ledger") or [],
            "excluded_reason": result.get("excluded_reason")}


def compare_supported(
    tickers: Any,
    metrics: Any,
    period: str,
    year: int,
) -> dict:
    """Bounded cited matrix: max 5 tickers × 4 metrics, no score or rank."""
    wanted_tickers = _string_list(tickers, flag="tickers")
    wanted_metrics = _string_list(metrics, flag="metrics")
    if not wanted_tickers:
        raise paths.ToolInputError("tickers must not be empty")
    if not wanted_metrics:
        raise paths.ToolInputError("metrics must not be empty")
    normalized_tickers: list[str] = []
    for raw_ticker in wanted_tickers:
        ticker = str(raw_ticker).strip().upper()
        if ticker not in normalized_tickers:
            normalized_tickers.append(ticker)
    canonical_metrics: list[str] = []
    for metric in wanted_metrics:
        key = facts.canonical_metric(metric)
        if key not in canonical_metrics:
            canonical_metrics.append(key)
    if len(normalized_tickers) > 5:
        raise paths.ToolInputError(
            f"compare_supported accepts at most 5 tickers, got {len(normalized_tickers)}"
        )
    if len(canonical_metrics) > 4:
        raise paths.ToolInputError(
            f"compare_supported accepts at most 4 metrics, got {len(canonical_metrics)}"
        )
    quarter = paths.normalize_quarter(period)
    fiscal_year = paths.normalize_year(year)
    supported = paths.supported_periods()

    columns: list[str] = []
    excluded: list[dict[str, Any]] = []

    for ticker in normalized_tickers:
        if not re.fullmatch(r"[A-Z]{3,5}", ticker):
            for metric in canonical_metrics:
                excluded.append({
                    "ticker": ticker or "invalid",
                    "metric": metric,
                    "reason": "invalid ticker",
                })
            continue
        if ticker not in supported:
            for metric in canonical_metrics:
                excluded.append({
                    "ticker": ticker,
                    "metric": metric,
                    "reason": (
                        "ticker is not in the supported set "
                        f"({len(supported)} issuers)"
                    ),
                })
            continue
        if ticker not in columns:
            columns.append(ticker)

    rows: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    for metric in canonical_metrics:
        mode, base = _metric_mode(metric)
        cells: list[dict[str, Any]] = []
        units: set[str] = set()
        if mode == "unsupported":
            for ticker in columns:
                excluded.append({
                    "ticker": ticker,
                    "metric": metric,
                    "reason": "unsupported metric; use a canonical statement line, "
                              "an operational metric, _margin or _growth",
                })
            continue
        for ticker in columns:
            _entry, ticker_supported = _period_entry(ticker, quarter, fiscal_year)
            if not ticker_supported:
                cells.append({
                    "ticker": ticker,
                    "value": None,
                    "status": "unsupported",
                    "evidence": {
                        "detail": f"{ticker} {quarter} {fiscal_year} is not in the supported set",
                        "supported_periods": [
                            f"{item['period']} {item['year']}"
                            for item in supported.get(ticker, [])
                        ],
                    },
                })
                excluded.append({
                    "ticker": ticker,
                    "metric": metric,
                    "reason": f"{quarter} {fiscal_year} is not a supported period",
                })
                continue
            built = _compare_cell(ticker, quarter, fiscal_year, metric, mode, base)
            cell = built["cell"]
            cells.append(cell)
            if cell.get("status") == "ok":
                unit = cell.get("unit")
                if unit:
                    units.add(str(unit))
                ledger.extend(built["ledger"])
            elif built.get("excluded_reason"):
                excluded.append({
                    "ticker": ticker,
                    "metric": metric,
                    "reason": built["excluded_reason"],
                })
        row_unit = next(iter(units)) if len(units) == 1 else (
            "mixed" if units else None
        )
        rows.append({
            "metric": metric,
            "unit": row_unit,
            "cells": cells,
        })

    for row in rows:
        if row.get("unit") not in (None, "mixed"):
            for cell in row["cells"]:
                if cell.get("unit") == row["unit"]:
                    cell.pop("unit", None)

    answered = sum(
        1 for ticker in columns
        if any(
            cell.get("status") == "ok"
            for row in rows for cell in row["cells"]
            if cell.get("ticker") == ticker
        )
    )
    data = {
        "period": quarter,
        "year": fiscal_year,
        "columns": columns,
        "rows": rows,
        "excluded": excluded,
        "coverage": {
            "requested": len(normalized_tickers),
            "answered": answered,
            "scope": "supported_set",
        },
    }
    request = {
        "tickers": wanted_tickers, "metrics": wanted_metrics,
        "period": period, "year": year,
    }
    caveats = [
        "Cited matrix, not a league table or sector screen; empty cells are "
        "explicit states, never zero.",
    ]
    envelope = _fit({
        "ok": True,
        "tool": "compare_supported",
        "request": request,
        "data": data,
        "ledger": ledger,
        "caveats": caveats,
    }, cap=paths.OUTPUT_CAP_BYTES)
    omitted = 0
    while _size(envelope) > paths.OUTPUT_CAP_BYTES and ledger:
        ledger.pop()
        omitted += 1
        data["ledger_omitted"] = omitted
        envelope = _fit({
            "ok": True,
            "tool": "compare_supported",
            "request": request,
            "data": data,
            "ledger": ledger,
            "caveats": caveats,
        }, cap=paths.OUTPUT_CAP_BYTES)
    if omitted:
        envelope["truncated"] = True
    return envelope


def _period_entry(
    ticker: str, quarter: str, year: int
) -> tuple[dict[str, Any] | None, bool]:
    entries = paths.supported_periods().get(ticker) or []
    for entry in entries:
        if entry["period"] == quarter and entry["year"] == year:
            return entry, True
    return None, False


def _trim_operational(
    envelope: dict[str, Any],
    data: dict[str, Any],
    ledger: list[dict[str, Any]],
    request: dict[str, Any],
    caveats: list[str],
) -> dict[str, Any]:
    """Keep entries ahead of drivers when the 12 KB cap bites."""
    drivers = data.get("drivers") or []
    entries = data.get("entries") or []
    analysis = data.get("analysis_context") or []
    drivers_omitted = 0
    entries_omitted = 0
    analysis_omitted = 0
    while _size(envelope) > paths.OUTPUT_CAP_BYTES - 350:
        if len(analysis) > 1:
            removed = analysis.pop()
            analysis_omitted += 1
            ledger = [
                item for item in ledger
                if item.get("source") != f"notes:{removed.get('id')}"
            ]
        elif len(drivers) > 3:
            drivers.pop()
            drivers_omitted += 1
        elif len(entries) > 1:
            removed = entries.pop()
            entries_omitted += 1
            ledger = [
                item for item in ledger
                if not (
                    (item.get("meta") or {}).get("metric")
                    == removed.get("metric")
                    and (item.get("meta") or {}).get("page")
                    == removed.get("page")
                    and (item.get("meta") or {}).get("source_doc")
                    == removed.get("source_doc")
                )
            ]
        else:
            break
        if drivers_omitted:
            data["drivers_omitted"] = drivers_omitted
        if entries_omitted:
            data["entries_omitted"] = entries_omitted
        if analysis_omitted:
            data["analysis_context_omitted"] = analysis_omitted
        envelope = _envelope(
            "operational_lookup", request, data,
            ledger=ledger, caveats=caveats,
        )
    if drivers_omitted:
        data["drivers_omitted"] = drivers_omitted
    if entries_omitted:
        data["entries_omitted"] = entries_omitted
    if analysis_omitted:
        data["analysis_context_omitted"] = analysis_omitted
    if drivers_omitted or entries_omitted or analysis_omitted:
        caveats = list(caveats) + [
            "Some context/entries/drivers were omitted to respect the 12 KB output cap."
        ]
        envelope = _envelope(
            "operational_lookup", request, data,
            ledger=ledger, caveats=caveats,
        )
        envelope["truncated"] = True
    return envelope


def _operational_ledger(
    entry: dict[str, Any], ticker: str, quarter: str, year: int
) -> dict[str, Any] | None:
    value = entry.get("value")
    if not isinstance(value, (int, float)):
        return None
    unit = entry.get("unit") or ""
    meta = _meta(
        ticker, quarter, year, entry.get("metric"),
        source_doc=entry.get("source_doc"),
        page=entry.get("page"),
    )
    lowered = unit.lower()
    if "/t" in lowered or "/ton" in lowered or lowered in ("x", "%", "times"):
        return _ratio_ledger(
            value, unit=unit or "ratio", source=entry.get("source_doc"),
            meta=meta,
        )
    if lowered in _COUNT_UNITS or "tonne" in lowered or "ton" in lowered:
        return _count_ledger(
            value, unit=unit or "count", source=entry.get("source_doc"),
            meta=meta,
        )
    if "usd" in lowered or "us$" in lowered or "idr" in lowered or "rp" in lowered:
        currency = _currency_from_unit(unit) or "USD"
        scale = _scale_from_unit(unit)
        units = {"million": "million", "billion": "billion",
                 "thousand": "thousand"}.get(scale or "", "full")
        return _money_ledger(
            value, currency, units, source=entry.get("source_doc"), meta=meta
        )
    return _count_ledger(
        value, unit=unit or "count", source=entry.get("source_doc"), meta=meta
    )


# ---------------------------------------------------------------------------
# read_document
# ---------------------------------------------------------------------------


def _ticker_documents(ticker: str) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for entry in paths.supported_periods().get(ticker, []):
        try:
            docs.extend(operational.documents_for_period(
                ticker, entry["period"], entry["year"],
            ))
        except paths.ChatError:
            continue
    docs.extend(operational._manifest_documents(ticker))  # noqa: SLF001
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for doc in docs:
        doc_id = str(doc.get("id") or "")
        if not doc_id or doc_id in seen:
            continue
        seen.add(doc_id)
        out.append(doc)
    return out


def read_document(ticker: str, doc_id: str, page: int) -> dict:
    """Exact page text (≤8k chars) for a captured document."""
    tk = paths.normalize_ticker(ticker)
    page = _validate_page(page)
    docs = _ticker_documents(tk)
    doc = next(
        (d for d in docs if str(d.get("id") or "").lower() == str(doc_id).lower()),
        None,
    )
    if doc is None:
        raise paths.ArtifactMissing(
            f"document {doc_id!r} is not captured for {tk}",
            available_documents=[d.get("id") for d in docs],
        )

    text, total_pages, used = _page_text(tk, doc, page)
    truncated = len(text) > READ_PAGE_CHARS
    data = {
        "ticker": tk,
        "doc_id": doc.get("id"),
        "page": page,
        "text": text[:READ_PAGE_CHARS],
        "url": doc.get("url"),
        "pages": total_pages or doc.get("pages"),
        "source_file": str(used) if used else None,
    }
    caveats = [
        "Page text is extracted verbatim from the captured document; line "
        "breaks may differ from the PDF layout.",
    ]
    if truncated:
        caveats.append(
            f"Page text truncated to {READ_PAGE_CHARS} characters."
        )
    return _envelope(
        "read_document",
        {"ticker": tk, "doc_id": doc_id, "page": page},
        data,
        ledger=[_page_ledger(page, _meta(tk, None, None, document=doc.get("id")))],
        caveats=caveats,
    )


def _validate_page(page: int) -> int:
    try:
        value = int(page)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"page must be an integer, got {page!r}") from exc
    if value < 1:
        raise paths.ToolInputError(f"page must be >= 1, got {value}")
    return value


def _page_text(
    ticker: str, doc: dict[str, Any], page: int
) -> tuple[str, int | None, Path | None]:
    candidates = _document_candidates(ticker, doc)
    total_pages = doc.get("pages")

    for path in candidates:
        if path.suffix.lower() not in (".txt", ".md"):
            continue
        text = _read_text(path)
        if text is None:
            continue
        pages = _split_pages(text)
        if pages:
            total_pages = max(total_pages or 0, max(pages)) or total_pages
            if page not in pages:
                raise paths.ArtifactMissing(
                    f"{doc.get('id')} has no page {page}",
                    available_pages=sorted(pages),
                )
            return pages[page], total_pages, path

    for path in candidates:
        if path.suffix.lower() != ".pdf":
            continue
        extracted = _pdf_page(path, page)
        if extracted is not None:
            count = _pdf_page_count(path) or total_pages
            return extracted, count, path

    # Last resort: a marker-free text file is treated as page 1 only.
    for path in candidates:
        if path.suffix.lower() in (".txt", ".md"):
            text = _read_text(path)
            if text is None:
                continue
            if page == 1:
                return text, total_pages or 1, path
            raise paths.ArtifactMissing(
                f"{doc.get('id')} text has no page markers; only page 1 is readable",
                pages_available=1,
            )
    raise paths.ArtifactMissing(
        f"no readable text or PDF found for {doc.get('id')}",
        searched=[str(p) for p in candidates],
    )


def _document_candidates(ticker: str, doc: dict[str, Any]) -> list[Path]:
    candidates: list[Path] = []
    for key in ("text_path", "path", "pdf_path"):
        raw = doc.get(key)
        if not raw:
            continue
        path = Path(str(raw))
        if not path.is_absolute():
            path = paths.PROJECT_ROOT / path
        if path.exists():
            candidates.append(path)

    doc_id = str(doc.get("id") or "").lower()
    needles = [doc_id]
    doc_type = str(doc.get("type") or "").lower()
    if "press_release" in doc_type or "news_release" in doc_type:
        needles.append("news_release")
        needles.append("release")
    roots = [paths.OPERATIONAL_DIR / ticker, paths.OPERATIONAL_DIR / "test"]
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.iterdir()):
            if not path.is_file() or path.suffix.lower() not in (".pdf", ".txt"):
                continue
            name = path.name.lower()
            if any(needle and needle in name for needle in needles):
                candidates.append(path)
    # Deduplicate, preferring text with page markers, then PDF.
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique.append(path)
    unique.sort(key=lambda p: (p.suffix.lower() != ".txt", p.name))
    return unique


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None


def _split_pages(text: str) -> dict[int, str]:
    matches = list(_PAGE_MARKER_RE.finditer(text))
    if not matches:
        return {}
    pages: dict[int, str] = {}
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        pages[int(match.group(1))] = text[start:end].strip()
    return pages


def _pdf_page(path: Path, page: int) -> str | None:
    try:
        import fitz  # noqa: PLC0415 — optional PDF backend
    except ImportError:
        return None
    try:
        with fitz.open(path) as doc:
            if page < 1 or page > doc.page_count:
                raise paths.ArtifactMissing(
                    f"{(path.name)} has no page {page}",
                    available_pages=list(range(1, doc.page_count + 1)),
                )
            return doc.load_page(page - 1).get_text("text")
    except paths.ChatError:
        raise
    except Exception:  # noqa: BLE001
        return None


def _pdf_page_count(path: Path) -> int | None:
    try:
        import fitz  # noqa: PLC0415
    except ImportError:
        return None
    try:
        with fitz.open(path) as doc:
            return doc.page_count
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# notes_search / notes_fetch (subprocess: rag.py has script-level imports)
# ---------------------------------------------------------------------------


def _run_rag(args: list[str], action: str) -> Any:
    cmd = [sys.executable, str(RAG_SCRIPT), *args]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(paths.PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=NOTES_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise paths.ChatError(
            f"{action} timed out after {NOTES_TIMEOUT}s",
            detail=str(exc),
        ) from exc
    except OSError as exc:
        raise paths.ChatError(f"{action} could not start: {exc}") from exc
    if proc.returncode != 0:
        raise paths.ChatError(
            f"{action} failed (exit {proc.returncode})",
            stderr=(proc.stderr or "").strip()[:1200],
        )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise paths.ChatError(
            f"{action} returned non-JSON output",
            stdout=(proc.stdout or "").strip()[:1200],
        ) from exc


def notes_search(query: str, ticker: str | None = None, limit: int = 8) -> dict:
    """Hybrid notes search via ``scripts/notes_rag/rag.py search --json``."""
    text = (query or "").strip()
    if not text:
        raise paths.ToolInputError("query must not be empty")
    try:
        count = int(limit)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"limit must be an integer, got {limit!r}") from exc
    if count < 1 or count > 25:
        raise paths.ToolInputError(f"limit must be between 1 and 25, got {count}")
    tk = paths.normalize_ticker(ticker) if ticker else None

    args = ["search", text, "-k", str(count), "--json"]
    if tk:
        args += ["--ticker", tk]
    results = _run_rag(args, "notes_search")
    if not isinstance(results, list):
        raise paths.ChatError("notes_search returned an unexpected payload")
    rows = []
    for item in results[:count]:
        if not isinstance(item, dict):
            continue
        rows.append({
            "id": item.get("id"),
            "rank": item.get("rank"),
            "score": item.get("rrf"),
            "path": item.get("path"),
            "section": item.get("section"),
            "period": item.get("period"),
            "tickers": item.get("tickers") or [],
            "snippet": (item.get("snippet") or "")[:300],
        })
    def build():
        ledger = [
            _count_ledger(
                len(rows), unit="results", meta=_meta(tk or "", None, None, "notes")
            )
        ]
        return _envelope(
            "notes_search", request, data,
            ledger=[entry for entry in ledger if entry], caveats=caveats,
        )

    request = {"query": text, "ticker": tk, "limit": count}
    data = {"query": text, "ticker": tk, "results": rows}
    caveats = [
        "Search results are snippets from the local notes vault; call "
        "notes_fetch with an id for the full chunk.",
    ]
    envelope = build()
    omitted = 0
    while _size(envelope) > paths.OUTPUT_CAP_BYTES and len(rows) > 1:
        rows.pop()
        omitted += 1
        data["results_omitted"] = omitted
        envelope = build()
    if omitted:
        envelope["truncated"] = True
    return envelope


def notes_fetch(chunk_id: str) -> dict:
    """Full chunk text via ``scripts/notes_rag/rag.py fetch <id> --json``."""
    value = (chunk_id or "").strip()
    if not value:
        raise paths.ToolInputError("chunk_id must not be empty")
    result = _run_rag(["fetch", value, "--json"], "notes_fetch")
    if not isinstance(result, dict):
        raise paths.ChatError("notes_fetch returned an unexpected payload")
    text = str(result.get("text") or "")
    truncated = len(text) > READ_DOCUMENT_CHARS
    data = {
        "id": result.get("id"),
        "path": result.get("path"),
        "title": result.get("title"),
        "section": result.get("section"),
        "period": result.get("period"),
        "tickers": result.get("tickers") or [],
        "text": text[:READ_DOCUMENT_CHARS],
    }
    caveats = [
        "Note text is an analyst note, not a filing; verify numbers against "
        "company_facts or period_derive.",
    ]
    if truncated:
        caveats.append(
            f"Text truncated to {READ_DOCUMENT_CHARS} characters."
        )
    return _envelope(
        "notes_fetch",
        {"chunk_id": value},
        data,
        ledger=[],
        caveats=caveats,
    )


# ---------------------------------------------------------------------------
# Schema registry for the CLI's --list
# ---------------------------------------------------------------------------

TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "list_companies": {
        "summary": "Supported tickers/periods plus per-layer completeness.",
        "parameters": {},
    },
    "company_facts": {
        "summary": "Compact fact-sheet sections with provenance.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "period": {"type": "string", "required": True, "enum": list(paths.QUARTERS)},
            "year": {"type": "integer", "required": True},
            "sections": {"type": "string", "default": "core",
                         "enum": ["core", "details", "all"]},
        },
    },
    "metric_series": {
        "summary": "Metric values across supported periods.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "metric": {"type": "string", "required": True},
            "periods": {"type": "array", "items": "string", "required": False},
            "year": {"type": "integer", "required": False},
        },
    },
    "period_derive": {
        "summary": "Single-quarter value derived from cumulative filings.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "period": {"type": "string", "required": True},
            "year": {"type": "integer", "required": True},
            "metric": {"type": "string", "required": True},
        },
    },
    "documents_status": {
        "summary": "Deterministic document existence state with evidence.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "period": {"type": "string", "required": False},
            "year": {"type": "integer", "required": False},
            "item": {"type": "string", "required": False},
        },
    },
    "operational_lookup": {
        "summary": "Operational entries, drivers, and documents.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "period": {"type": "string", "required": True},
            "year": {"type": "integer", "required": True},
            "metric": {"type": "string", "required": False},
        },
    },
    "analysis_context": {
        "summary": "Secondary analyst-note snippets for operational context.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "period": {"type": "string", "required": True},
            "year": {"type": "integer", "required": True},
            "topic": {"type": "string", "required": False},
            "limit": {"type": "integer", "required": False, "default": 4},
        },
    },
    "read_document": {
        "summary": "Exact page text (<=8000 chars) plus URL.",
        "parameters": {
            "ticker": {"type": "string", "required": True},
            "doc_id": {"type": "string", "required": True},
            "page": {"type": "integer", "required": True},
        },
    },
    "notes_search": {
        "summary": "Hybrid search over the local analyst notes vault.",
        "parameters": {
            "query": {"type": "string", "required": True},
            "ticker": {"type": "string", "required": False},
            "limit": {"type": "integer", "default": 8},
        },
    },
    "notes_fetch": {
        "summary": "Full text of one notes chunk.",
        "parameters": {"chunk_id": {"type": "string", "required": True}},
    },
    "list_documents": {
        "summary": "Analysis-note metadata (no content) with filters.",
        "parameters": {
            "ticker": {"type": "string", "required": False},
            "period": {"type": "string", "required": False, "enum": list(paths.QUARTERS)},
            "year": {"type": "integer", "required": False},
            "kind": {"type": "string", "required": False, "enum": list(analysis_docs.KINDS)},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
    },
    "analysis_document": {
        "summary": "One analysis-note section, or outline + content when it fits.",
        "parameters": {
            "document_id": {"type": "string", "required": True},
            "section": {"type": "string", "required": False},
            "next_line": {"type": "integer", "required": False},
        },
    },
    "analysis_search": {
        "summary": "Search analysis-note lines; scoped emptiness is not_in_document.",
        "parameters": {
            "query": {"type": "string", "required": True},
            "document_id": {"type": "string", "required": False},
            "ticker": {"type": "string", "required": False},
            "limit": {"type": "integer", "required": False, "default": 8},
        },
    },
    "compare_supported": {
        "summary": "Bounded cited matrix (max 5 tickers x 4 metrics); not a league table.",
        "parameters": {
            "tickers": {"type": "array", "items": "string", "required": True},
            "metrics": {"type": "array", "items": "string", "required": True},
            "period": {"type": "string", "required": True, "enum": list(paths.QUARTERS)},
            "year": {"type": "integer", "required": True},
        },
    },
}


def schemas() -> dict[str, Any]:
    return {"ok": True, "tool": "list_tools", "tools": TOOL_SCHEMAS}
