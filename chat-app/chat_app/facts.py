"""Fact-sheet loading and section selection (CONTRACTS.md §1).

The canonical YAML under ``idx_data/analysis/facts/`` is the single source of
truth for primary-statement numbers. This module only *reads* it:

- ``load_facts`` resolves and parses one sheet through :mod:`chat_app.paths`.
- ``compact_sections`` shapes it into the bounded ``company_facts`` payload.
- ``statement_lines`` exposes one statement with provenance for ``metric_series``.

Provenance is resolved additively: when a sheet predates the schema change (no
``provenance`` block) the matching XLSX seed is rebuilt read-only from
``pipeline.duckdb`` and its provenance is used. Nothing is written back — that is
``enrich.py``'s job.
"""

from __future__ import annotations

import functools
import sys
from typing import Any, Optional

from . import paths

# scripts/analysis_pipeline is not an installed package. Inserting it on the
# path lets us import ``xlsx_seed`` (which itself imports ``schema`` by bare
# name) without copying label maps or importing merge/write/research.
_PIPELINE_DIR = paths.PROJECT_ROOT / "scripts" / "analysis_pipeline"


def _ensure_pipeline_path() -> None:
    value = str(_PIPELINE_DIR)
    if value not in sys.path:
        sys.path.insert(0, value)


def _schema():
    _ensure_pipeline_path()
    import schema  # noqa: PLC0415 — deliberate lazy import (path is set above)

    return schema


def _xlsx_seed():
    _ensure_pipeline_path()
    import xlsx_seed  # noqa: PLC0415 — deliberate lazy import

    return xlsx_seed


# Metric spellings an analyst is likely to use, mapped to canonical line keys.
METRIC_ALIASES = {
    "sales": "revenue",
    "sales_and_revenue": "revenue",
    "net_sales": "revenue",
    "net_income": "net_profit",
    "profit_for_the_period": "net_profit",
    "profit_attributable_to_parent": "np_owners",
    "net_profit_attributable_to_parent": "np_owners",
    "cost_of_revenue": "cogs",
    "cost_of_sales": "cogs",
    "operating_cash_flow": "ocf",
    "cash_flow_from_operations": "ocf",
    "capital_expenditure": "capex_ppe",
    "total_debt": "loans_current",  # best-effort alias; not a reported total
    "shareholders_equity": "total_equity",
    "equity": "total_equity",
}


def normalize_metric(metric: str) -> str:
    """Lowercase, strip and snake-case a user-supplied metric name."""
    value = (metric or "").strip().lower()
    value = value.replace("-", "_").replace(" ", "_")
    while "__" in value:
        value = value.replace("__", "_")
    return value


def canonical_metric(metric: str) -> str:
    key = normalize_metric(metric)
    return METRIC_ALIASES.get(key, key)


def resolve_metric(metric: str) -> tuple[str, str]:
    """Return ``(statement, canonical_key)`` or raise ``ToolInputError``."""
    key = canonical_metric(metric)
    for statement, keys in _schema().canonical_line_keys().items():
        if key in keys:
            return statement, key
    raise paths.ToolInputError(
        f"unknown statement metric {metric!r}",
        hint="use a canonical line key, e.g. revenue, gross_profit, net_profit, ocf",
    )


def statement_for_metric(metric: str) -> Optional[str]:
    try:
        return resolve_metric(metric)[0]
    except paths.ToolInputError:
        return None


def load_facts(ticker: str, quarter: str, year: int | str) -> dict[str, Any]:
    """Load one canonical fact sheet as a raw dict.

    Existing files win (tests and a just-written enrichment copy may live
    outside the current supported set). When the file is absent the request is
    validated against the supported set so callers get ``UnsupportedRequest``
    with the alternatives instead of a bare missing artifact.
    """
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    path = paths.fact_path(tk, q, y)
    if not path.exists() and not paths.is_supported(tk, q, y):
        entries = paths.supported_periods().get(tk)
        if not entries:
            raise paths.UnsupportedRequest(
                f"{tk} is not in the supported set",
                supported_tickers=paths.supported_tickers(),
                alternatives=paths.supported_summary(),
            )
        raise paths.UnsupportedRequest(
            f"{tk} {q} {y} is not in the supported set",
            supported_periods=[f"{e['period']} {e['year']}" for e in entries],
            alternatives=[f"{e['period']} {e['year']}" for e in entries],
        )
    data = paths.load_yaml(path)
    if not isinstance(data, dict):
        raise paths.ChatError(f"fact sheet is not a mapping: {path}")
    return data


def load_ratios(ticker: str, quarter: str, year: int | str) -> dict[str, Any]:
    """Return the sheet's ``.ratios.yaml`` mapping, or ``{}`` when absent."""
    path = paths.ratios_path(ticker, quarter, year)
    if not path.exists():
        return {}
    data = paths.load_yaml(path)
    return data if isinstance(data, dict) else {}


@functools.lru_cache(maxsize=64)
def _seed(ticker: str, quarter: str, year: int):
    """Cached XLSX seed FactSheet, or ``None`` when coverage is incomplete."""
    try:
        return _xlsx_seed().seed_from_xlsx(
            ticker, year, quarter, db_path=paths.PIPELINE_DB
        )
    except Exception:  # noqa: BLE001 — provenance is additive, never fatal
        return None


def _seed_provenance(ticker: str, quarter: str, year: int) -> dict[str, dict[str, Any]]:
    """``{statement: {line_key: provenance}}`` from the cached XLSX seed."""
    seed = _seed(ticker, quarter, year)
    if seed is None:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for name in paths.STATEMENTS:
        statement = getattr(seed, name, None)
        if statement is None:
            continue
        dump = getattr(statement, "provenance", {}) or {}
        cleaned: dict[str, dict[str, Any]] = {}
        for key, value in dump.items():
            if hasattr(value, "model_dump"):
                value = value.model_dump(exclude_none=True)
            if isinstance(value, dict) and value:
                cleaned[key] = value
        out[name] = cleaned
    return out


def _provenance_for(
    key: str,
    statement: str,
    raw_provenance: dict[str, Any],
    seed_provenance: dict[str, dict[str, Any]],
    period_type: str,
) -> dict[str, Any]:
    existing = raw_provenance.get(key)
    if isinstance(existing, dict) and existing:
        return existing
    seeded = seed_provenance.get(statement, {}).get(key)
    if isinstance(seeded, dict) and seeded:
        return seeded
    return {"resolved": False, "period_type": period_type}


def provenance_table(ticker: str, quarter: str, year: int) -> dict[str, dict[str, dict[str, Any]]]:
    """Provenance per statement, preferring the YAML and falling back to XLSX.

    Used by ``enrich.py`` and by the verifier's ``sheet:row`` citation checks.
    """
    sheet = load_facts(ticker, quarter, year)
    seed = _seed_provenance(ticker, quarter, year)
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for statement in paths.STATEMENTS:
        block = sheet.get(statement) or {}
        raw = block.get("provenance") or {}
        lines = block.get("lines") or {}
        ptype = "instant" if statement == "balance_sheet" else "cumulative"
        table: dict[str, dict[str, Any]] = {}
        for key, value in lines.items():
            if value is None:
                continue
            table[key] = _provenance_for(key, statement, raw, seed, ptype)
        out[statement] = table
    return out


def _source(provenance: dict[str, Any]) -> Optional[str]:
    sheet_code = provenance.get("sheet")
    row = provenance.get("row")
    if sheet_code and row is not None:
        return f"{sheet_code}:{row}"
    return None


_PRIOR_VISIBLE = {
    "income_statement": {"revenue", "cogs", "gross_profit", "operating_profit",
                         "pbt", "net_profit"},
    "balance_sheet": {"total_assets", "total_equity", "total_liabilities", "cash"},
    "cash_flow": {"ocf", "capex_ppe", "fcf"},
}


def _statement_block(
    block: dict[str, Any],
    statement: str,
    meta: dict[str, Any],
    seed_provenance: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    lines = block.get("lines") or {}
    prior_lines = block.get("prior_lines") or {}
    raw_provenance = block.get("provenance") or {}
    currency = meta.get("currency")
    units = meta.get("units")
    period_type = "instant" if statement == "balance_sheet" else "cumulative"
    canonical = _schema().canonical_line_keys()[statement]
    out: dict[str, dict[str, Any]] = {}
    for key in canonical:
        value = lines.get(key)
        if value is None:
            continue
        provenance = _provenance_for(
            key, statement, raw_provenance, seed_provenance, period_type
        )
        entry = {
            "value": value,
            "currency": currency,
            "units": units,
            "value_idr_bn": _round_idr(paths.to_idr_bn(value, currency, units)),
            "period_type": period_type,
            "provenance": provenance,
        }
        # The comparable column is what YoY/QoQ prose needs. Expose it only for
        # the lines answers actually cite; the full statement stays available
        # through metric_series/period_derive.
        prior = prior_lines.get(key)
        if key in _PRIOR_VISIBLE.get(statement, set()) and prior is not None:
            entry["prior"] = prior
            entry["prior_period_type"] = period_type
        out[key] = entry
    # Keep non-canonical extras out of core; they belong in details/metrics.
    return out


def _round_idr(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(value, 4)


def compact_sections(
    sheet: dict[str, Any],
    sections: str = "core",
    *,
    completeness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Shape a raw sheet into the ``company_facts`` ``data`` payload.

    ``sections`` is ``core`` (meta + three statements + completeness),
    ``details`` (core + the details block) or ``all`` (both). Values are only
    trimmed by the tool layer via ``paths.dump_json``.
    """
    mode = (sections or "core").strip().lower()
    if mode not in ("core", "details", "all"):
        raise paths.ToolInputError(
            f"sections must be core|details|all, got {sections!r}"
        )
    meta = sheet.get("meta") or {}
    ticker = meta.get("ticker")
    quarter = meta.get("quarter")
    year = meta.get("year")
    seed = _seed_provenance(ticker, quarter, year) if ticker and quarter and year else {}

    data: dict[str, Any] = {
        "ticker": ticker,
        "period": quarter,
        "year": year,
        "meta": {
            "currency": meta.get("currency"),
            "units": meta.get("units"),
            "period_end": meta.get("period_end"),
            "audited": meta.get("audited"),
            "source_file": meta.get("source_file"),
        },
        "sections": {},
    }
    for statement in paths.STATEMENTS:
        block = sheet.get(statement) or {}
        data["sections"][statement] = _statement_block(block, statement, meta, seed)

    if mode in ("details", "all"):
        details = sheet.get("details") or {}
        data["sections"]["details"] = _trim_details(details)

    if completeness is not None:
        data["completeness"] = completeness
    elif sheet.get("completeness"):
        data["completeness"] = sheet["completeness"]
    else:
        # Lazy import avoids a module cycle at import time; operational.py does
        # not import facts.py.
        from . import operational  # noqa: PLC0415

        try:
            data["completeness"] = operational.completeness(ticker, quarter, year)
        except paths.ChatError:
            data["completeness"] = {}
    return data


def _trim_details(details: dict[str, Any]) -> dict[str, Any]:
    """Drop empty detail lists so a details request stays inside the cap."""
    out: dict[str, Any] = {}
    for key, value in details.items():
        if isinstance(value, list) and not value:
            continue
        if value in (None, "", {}):
            continue
        out[key] = value
    return out


def statement_lines(
    ticker: str,
    quarter: str,
    year: int | str,
    statement: str,
) -> dict[str, dict[str, Any]]:
    """One statement's canonical lines, each with provenance and units."""
    name = (statement or "").strip().lower()
    aliases = {
        "income": "income_statement",
        "is": "income_statement",
        "pnl": "income_statement",
        "profit_and_loss": "income_statement",
        "balance": "balance_sheet",
        "bs": "balance_sheet",
        "cashflow": "cash_flow",
        "cf": "cash_flow",
        "cash_flow_statement": "cash_flow",
    }
    name = aliases.get(name, name)
    if name not in paths.STATEMENTS:
        raise paths.ToolInputError(
            f"statement must be one of {', '.join(paths.STATEMENTS)}, got {statement!r}"
        )
    sheet = load_facts(ticker, quarter, year)
    meta = sheet.get("meta") or {}
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    seed = _seed_provenance(tk, q, y)
    return _statement_block(sheet.get(name) or {}, name, meta, seed)


def source_for(
    ticker: str, quarter: str, year: int | str, metric: str
) -> Optional[str]:
    """``sheet:row`` for a metric, or ``None`` when it cannot be traced."""
    statement, key = resolve_metric(metric)
    entry = statement_lines(ticker, quarter, year, statement).get(key) or {}
    return _source(entry.get("provenance") or {})
