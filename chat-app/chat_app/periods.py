"""Cumulative→quarter derivation and canonical unit normalization.

IDX interim statements are cumulative: a Q2 income statement covers six months
(``tw2``), Q3 nine months (``tw3``) and Q4 the audited full year (``audit``).
Balance-sheet lines are instants and are never differenced.

``derive_quarter`` follows MVP-PLAN §4.1:

===========  ==========================================
Quarter      Quarter value
===========  ==========================================
Q1           ``tw1`` (the cumulative value itself)
Q2           ``tw2 - tw1``
Q3           ``tw3 - tw2``
Q4           ``audit - tw3``
===========  ==========================================

Operands come from the prior quarter's canonical fact sheet when it exists and
from ``xlsx_statements`` (read-only) otherwise. Every operand carries a
``sheet:row`` source so the answer auditor can trace the arithmetic.
"""

from __future__ import annotations

from typing import Any, Optional

from . import facts, paths

QUARTER_PERIOD = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}

# Prior cumulative period used by each quarter's subtraction.
PRIOR_QUARTER = {"Q2": "Q1", "Q3": "Q2", "Q4": "Q3"}

_FORMULAS = {
    "Q1": "tw1",
    "Q2": "tw2 - tw1",
    "Q3": "tw3 - tw2",
    "Q4": "audit - tw3",
}
_OPERAND_KEYS = {
    "Q1": ("tw1",),
    "Q2": ("tw2", "tw1"),
    "Q3": ("tw3", "tw2"),
    "Q4": ("audit", "tw3"),
}


def period_type(statement: str, quarter: str | None = None) -> str:
    """``cumulative`` for P&L/cash flow, ``instant`` for the balance sheet."""
    name = (statement or "").strip().lower()
    if name in ("balance_sheet", "balance", "bs"):
        return "instant"
    if name in ("income_statement", "income", "is", "cash_flow", "cashflow", "cf"):
        return "cumulative"
    raise paths.ToolInputError(f"unknown statement {statement!r}")


def scale(units: str | None) -> float:
    """Multiplier from the filing's units to full currency units."""
    return paths.scale_multiplier(units)


def normalize_value(
    raw: float | None,
    currency: str | None,
    units: str | None,
    period_type: str | None = None,
) -> dict[str, Any]:
    """Canonicalize one raw filing value.

    Returns ``{raw, currency, units, scale, value_idr_bn, period_type}``. The
    raw value and its unit always travel together; ``value_idr_bn`` is
    display-only and uses the explicitly-labelled static FX rate in
    :mod:`chat_app.paths`.
    """
    cur = (currency or "IDR").strip().upper()
    return {
        "raw": raw,
        "currency": cur,
        "units": units,
        "scale": scale(units),
        "value_idr_bn": (
            round(paths.to_idr_bn(raw, cur, units), 4) if raw is not None else None
        ),
        "period_type": period_type,
    }


def canonical_value(
    raw: float | None,
    currency: str | None,
    units: str | None,
    statement: str,
    quarter: str | None = None,
) -> dict[str, Any]:
    """``normalize_value`` plus the statement's ``period_type``."""
    out = normalize_value(raw, currency, units, period_type(statement, quarter))
    return out


def to_idr_bn(
    value: float | None, currency: str | None, units: str | None
) -> Optional[float]:
    """Re-export :func:`chat_app.paths.to_idr_bn` for callers in this module."""
    return paths.to_idr_bn(value, currency, units)


def unit_label(currency: str | None, units: str | None) -> str:
    cur = (currency or "IDR").strip().upper()
    u = (units or "").strip()
    if not u or u.lower() in ("full", "rupiah", "full amount"):
        return cur
    return f"{cur} {u}"


def convert_value(
    value: float | None,
    from_currency: str | None,
    from_units: str | None,
    to_currency: str | None,
    to_units: str | None,
) -> Optional[float]:
    """Convert a value between filing units via the IDR bridge.

    Nearly every issuer keeps one currency/scale across quarters; this exists so
    a scale change between filings cannot silently corrupt a derived quarter.
    """
    if value is None:
        return None
    same_currency = (from_currency or "IDR").upper() == (to_currency or "IDR").upper()
    same_units = (from_units or "") == (to_units or "")
    if same_currency and same_units:
        return value
    idr_bn = paths.to_idr_bn(value, from_currency, from_units)
    if idr_bn is None:
        return None
    target_scale = scale(to_units)
    target_currency = (to_currency or "IDR").upper()
    if target_currency == "USD":
        return idr_bn * 1e9 / (paths.USD_IDR_RATE * target_scale)
    return idr_bn * 1e9 / target_scale


def _clean(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    out = round(float(value), 6)
    return 0.0 if out == 0 else out


def _fact_operand(
    ticker: str,
    quarter: str,
    year: int,
    statement: str,
    key: str,
) -> Optional[dict[str, Any]]:
    """Read one operand from a canonical fact sheet, if it is present."""
    try:
        sheet = facts.load_facts(ticker, quarter, year)
    except paths.ChatError:
        return None
    line = facts.statement_lines(ticker, quarter, year, statement).get(key)
    if not line or line.get("value") is None:
        return None
    meta = sheet.get("meta") or {}
    return {
        "value": line["value"],
        "currency": meta.get("currency"),
        "units": meta.get("units"),
        "source": _source(line.get("provenance") or {}),
        "period_type": line.get("period_type"),
        "from": f"fact:{paths.normalize_ticker(ticker)}_{quarter}_{year}.yaml",
    }


def _source(provenance: dict[str, Any]) -> Optional[str]:
    sheet_code = provenance.get("sheet")
    row = provenance.get("row")
    if sheet_code and row is not None:
        return f"{sheet_code}:{row}"
    return None


def _xlsx_operand(
    ticker: str,
    quarter: str,
    year: int,
    statement: str,
    key: str,
) -> Optional[dict[str, Any]]:
    """Read one operand straight from ``xlsx_statements`` (read-only)."""
    seed = facts._xlsx_seed()  # noqa: SLF001 — shared label maps, read-only use

    period = seed.QUARTER_TO_PERIOD.get(quarter, quarter.lower())
    if statement == "income_statement":
        sheet_codes, mapping = seed.IS_SHEETS, seed.IS_MAP
    elif statement == "cash_flow":
        sheet_codes, mapping = seed.CF_SHEETS, seed.CF_MAP
    elif statement == "balance_sheet":
        sheet_codes, mapping = seed.BS_SHEETS, seed.BS_MAP
    else:
        return None

    try:
        conn = seed._connect(paths.PIPELINE_DB)  # noqa: SLF001 — read-only
    except Exception:  # noqa: BLE001
        return None
    try:
        meta_row = conn.execute(
            "SELECT currency, rounding FROM xlsx_meta WHERE ticker=? AND year=? AND period=?",
            (ticker, year, period),
        ).fetchone()
        sheet_code, rows = seed._fetch_sheet_rows_with_code(  # noqa: SLF001
            conn, ticker, year, period, sheet_codes
        )
        if not rows or not sheet_code:
            return None
        if statement == "income_statement" and sheet_code in seed.BANK_IS_SHEETS:
            mapping = seed.BANK_IS_MAP
        spec = mapping.get(key)
        if spec is None:
            return None
        value, _prior, prov = seed._resolve(seed._label_index(rows), spec)  # noqa: SLF001
        if value is None:
            return None
        currency, rounding = meta_row if meta_row else (None, None)
        units = seed._normalize_units(rounding or "")  # noqa: SLF001
        return {
            "value": value,
            "currency": seed._normalize_currency(currency or ""),  # noqa: SLF001
            "units": units,
            "source": (
                f"{sheet_code}:{prov.get('row')}"
                if prov.get("row") is not None
                else None
            ),
            "period_type": period_type(statement),
            "from": f"xlsx:{period}",
        }
    except Exception:  # noqa: BLE001 — derivation degrades to ArtifactMissing
        return None
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def _operand(
    ticker: str,
    quarter: str,
    year: int,
    statement: str,
    key: str,
) -> Optional[dict[str, Any]]:
    found = _fact_operand(ticker, quarter, year, statement, key)
    if found and found.get("value") is not None:
        return found
    return _xlsx_operand(ticker, quarter, year, statement, key)


def derive_quarter(
    ticker: str,
    metric: str,
    quarter: str,
    year: int | str,
) -> dict[str, Any]:
    """Derive a single quarter's value for a cumulative statement line.

    Returns the CONTRACTS.md §1 ``period_derive`` payload:
    ``{formula, operands, quarter_value, unit, value_idr_bn, source}`` plus the
    request identifiers. Balance-sheet metrics are instants and are rejected.
    """
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    statement, key = facts.resolve_metric(metric)
    if statement == "balance_sheet":
        raise paths.ToolInputError(
            f"{key} is a balance-sheet instant; quarter derivation only applies to "
            "cumulative income-statement and cash-flow lines",
        )
    if q not in QUARTER_PERIOD:
        raise paths.ToolInputError(
            f"period_derive supports Q1-Q4, not {q}; use company_facts for FY totals"
        )

    paths.require_supported(tk, q, y)

    current = _operand(tk, q, y, statement, key)
    if current is None or current.get("value") is None:
        raise paths.ArtifactMissing(
            f"{key} has no value in {tk} {q} {y}",
            ticker=tk, period=q, year=y, metric=key,
        )

    currency = current.get("currency")
    units = current.get("units")
    operands: dict[str, float] = {}
    source: dict[str, Optional[str]] = {}
    label = _OPERAND_KEYS[q]

    if q == "Q1":
        operands["tw1"] = _clean(current["value"])
        source["tw1"] = current.get("source")
        quarter_value = operands["tw1"]
    else:
        prior_q = PRIOR_QUARTER[q]
        prior = _operand(tk, prior_q, y, statement, key)
        if prior is None or prior.get("value") is None:
            raise paths.ArtifactMissing(
                f"{key} has no Q1/Q2/Q3 cumulative value to subtract "
                f"for {tk} {q} {y}",
                ticker=tk, period=q, year=y, metric=key, prior_period=prior_q,
            )
        prior_value = convert_value(
            prior["value"],
            prior.get("currency"),
            prior.get("units"),
            currency,
            units,
        )
        if prior_value is None:
            raise paths.ArtifactMissing(
                f"could not normalize the prior operand for {tk} {key}",
                ticker=tk, period=q, year=y, metric=key,
            )
        operands[label[0]] = _clean(current["value"])
        operands[label[1]] = _clean(prior_value)
        source[label[0]] = current.get("source")
        source[label[1]] = prior.get("source")
        quarter_value = _clean(operands[label[0]] - operands[label[1]])

    return {
        "ticker": tk,
        "metric": key,
        "period": q,
        "year": y,
        "formula": _FORMULAS[q],
        "operands": operands,
        "quarter_value": quarter_value,
        "unit": unit_label(currency, units),
        "value_idr_bn": (
            round(paths.to_idr_bn(quarter_value, currency, units), 4)
            if quarter_value is not None
            else None
        ),
        "source": source,
        "period_type": "quarter",
        "statement": statement,
        "currency": currency,
        "units": units,
    }
