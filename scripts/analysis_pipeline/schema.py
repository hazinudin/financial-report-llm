"""
Pydantic models for the versioned fact sheet (Stage 1 output / Stage 2 merge).

Fields are optional with `None` defaults so per-chunk extraction outputs (which
cover only a subset of the document) validate against the same schema as the
final merged sheet. The merge stage (merge.py) fills the gaps.

All monetary values are raw integers/floats in the reporting currency scaled per
`meta.units`; downstream ratio code (ratios.py) handles unit normalization.
"""

from __future__ import annotations

import datetime
import re
import typing
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

SCHEMA_VERSION = 1


# ── Lenient input coercion ─────────────────────────────────────────────
# LLM chunk outputs fail validation on trivial type issues far more often than
# on actual content (null blocks, "55%" strings, YAML-parsed dates, int quarters).
# These before-validators coerce what is coercible and null out the rest, so a
# chunk sheet only fails when the payload is truly unusable.

_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")


def _coerce_float(v: Any) -> Optional[float]:
    """Best-effort float coercion. Keeps the number AS PRINTED (no % scaling,
    no unit conversion). Unparseable -> None (never raises)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "-", "n/a", "na"):
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):  # accounting parentheses
        neg = True
        s = s[1:-1]
    if s.endswith("%"):
        s = s[:-1]
    s = s.replace(",", "").replace(" ", "")
    if not _NUM_RE.fullmatch(s):
        m = _NUM_RE.search(s)  # tolerate stray tokens: "161322044693 la"
        if not m:
            return None
        s = m.group(0)
    try:
        out = float(s)
    except ValueError:
        return None
    return -out if neg else out


def _stringify(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()[:10]
    if isinstance(v, dict):
        return "; ".join(f"{k}: {_stringify(x)}" for k, x in v.items())
    if isinstance(v, (list, tuple)):
        return "; ".join(_stringify(x) for x in v)
    return str(v)


def _wants(annotation: Any, typ: type) -> bool:
    if annotation is typ:
        return True
    if typing.get_origin(annotation) is typing.Union:
        return any(a is typ for a in typing.get_args(annotation))
    return False


class _CoerceModel(BaseModel):
    """Base model that coerces str->float and any->str per field annotation."""

    @model_validator(mode="before")
    @classmethod
    def _coerce_fields(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        for name, f in cls.model_fields.items():
            v = d.get(name)
            if v is None:
                continue
            if _wants(f.annotation, float) and not isinstance(v, (int, float)):
                d[name] = _coerce_float(v)
            elif _wants(f.annotation, str) and not isinstance(v, str):
                d[name] = _stringify(v)
        return d


class Meta(_CoerceModel):
    schema_version: int = SCHEMA_VERSION
    ticker: Optional[str] = None
    quarter: Optional[str] = None
    year: Optional[int] = None
    currency: Optional[str] = None
    units: Optional[str] = Field(None, description="rupiah | thousand | million | full")
    source_file: Optional[str] = None
    audited: Optional[bool] = None
    period_end: Optional[str] = None
    prior_period_end: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_meta(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return {} if data is None else data
        d = dict(data)
        if d.get("year") is not None and not isinstance(d["year"], int):
            d["year"] = int(_coerce_float(d["year"])) if _coerce_float(d["year"]) is not None else None
        if d.get("audited") is not None and not isinstance(d["audited"], bool):
            s = str(d["audited"]).strip().lower()
            if s in ("true", "yes", "audited", "diaudit"):
                d["audited"] = True
            elif s in ("false", "no", "unaudited", "not audited", "tidak diaudit"):
                d["audited"] = False
            else:
                d["audited"] = None
        return d


class Provenance(_CoerceModel):
    """Where a statement line came from in the source XBRL workbook.

    Additive and optional: sheets produced before provenance existed validate
    unchanged. `components` records the rows behind a `sum` mapping.
    """
    sheet: Optional[str] = None
    row: Optional[int] = None
    label_en: Optional[str] = None
    column: Optional[str] = Field(None, description="value_current | value_prior")
    period_type: Optional[str] = Field(None, description="cumulative | instant")
    components: list[dict[str, Any]] = Field(default_factory=list)


class Statement(BaseModel):
    """A primary statement.

    `periods` names the columns (e.g. ["current", "prior_yoy"] for the P&L/CF,
    ["current", "prior_fy_end"] for the balance sheet). `lines` holds the
    CURRENT-period value per line key; `prior_lines` holds the COMPARATIVE
    column (Q1 prior year for P&L/CF, Dec prior year for the balance sheet).
    Both are needed so ratios.py can compute YoY growth and QoQ moves without
    any LLM arithmetic.
    """
    periods: list[str] = Field(default_factory=list)
    lines: dict[str, Optional[float]] = Field(default_factory=dict)
    prior_lines: dict[str, Optional[float]] = Field(default_factory=dict)
    provenance: dict[str, Provenance] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        if d.get("periods") is None:
            d["periods"] = []
        elif isinstance(d["periods"], str):
            d["periods"] = [d["periods"]]
        for col in ("lines", "prior_lines"):
            if d.get(col) is None:
                d[col] = {}
            elif isinstance(d[col], dict):
                d[col] = {k: _coerce_float(v) for k, v in d[col].items()}
        prov = d.get("provenance")
        if prov is None:
            d["provenance"] = {}
        elif isinstance(prov, dict):
            d["provenance"] = {
                k: (v if isinstance(v, (dict, BaseModel)) else {})
                for k, v in prov.items()
            }
        else:
            d["provenance"] = {}
        return d


class Customer(_CoerceModel):
    name: Optional[str] = None
    amount: Optional[float] = None
    pct_of_sales: Optional[float] = None


class Loan(_CoerceModel):
    lender: Optional[str] = None
    balance: Optional[float] = None
    rate: Optional[str] = None
    maturity: Optional[str] = None
    notes: Optional[str] = None


class Subsidiary(_CoerceModel):
    name: Optional[str] = None
    ownership_pct: Optional[float] = None
    activity: Optional[str] = None
    total_assets: Optional[float] = None


class Segment(_CoerceModel):
    name: Optional[str] = None
    external_sales: Optional[float] = None
    internal_sales: Optional[float] = None
    gross_profit: Optional[float] = None
    net_profit: Optional[float] = None
    assets: Optional[float] = None


class Production(_CoerceModel):
    product: Optional[str] = None
    volume_mt: Optional[float] = None
    period: Optional[str] = None


class Reserve(_CoerceModel):
    product: Optional[str] = None
    residual_mt: Optional[float] = None
    as_of: Optional[str] = None


class RelatedParty(_CoerceModel):
    party: Optional[str] = None
    relationship: Optional[str] = None
    item: Optional[str] = None
    amount: Optional[float] = None


class Commitment(_CoerceModel):
    description: Optional[str] = None
    amount: Optional[float] = None
    counterparty: Optional[str] = None


class Dividend(_CoerceModel):
    period: Optional[str] = None
    per_share: Optional[float] = None
    total: Optional[float] = None


_DETAIL_LIST_FIELDS = (
    "customers", "loans", "subsidiaries", "segments", "production",
    "reserves", "related_parties", "commitments", "dividends",
)


class Details(BaseModel):
    customers: list[Customer] = Field(default_factory=list)
    loans: list[Loan] = Field(default_factory=list)
    subsidiaries: list[Subsidiary] = Field(default_factory=list)
    segments: list[Segment] = Field(default_factory=list)
    production: list[Production] = Field(default_factory=list)
    reserves: list[Reserve] = Field(default_factory=list)
    related_parties: list[RelatedParty] = Field(default_factory=list)
    commitments: list[Commitment] = Field(default_factory=list)
    dividends: list[Dividend] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        for name in _DETAIL_LIST_FIELDS:
            v = d.get(name)
            if v is None:
                d[name] = []
            elif isinstance(v, dict):
                d[name] = [v]  # single entry emitted as a bare mapping
            elif isinstance(v, list):
                d[name] = [e for e in v if isinstance(e, (dict, BaseModel))]
            else:
                d[name] = []
        return d


class Verification(_CoerceModel):
    method: Optional[str] = Field(None, description="reconciled | quoted_in_source")
    reconciled_to: Optional[str] = None
    detail: Optional[str] = None


class OperationalEntry(_CoerceModel):
    metric: Optional[str] = None
    scope: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    prior: Optional[float] = None
    source_doc: Optional[str] = None
    page: Optional[int] = None
    published_at: Optional[str] = None
    status: Optional[str] = Field(None, description="preliminary | final | rejected")
    verification: Verification = Field(default_factory=Verification)
    quote: Optional[str] = None


class OperationalDriver(_CoerceModel):
    driver: Optional[str] = None
    affects: Optional[str] = None
    quote: Optional[str] = None
    page: Optional[int] = None
    source_doc: Optional[str] = None
    type: Optional[str] = Field("management_stated", description="management_stated | fact | hypothesis")


class OperationalBlock(BaseModel):
    entries: list[OperationalEntry] = Field(default_factory=list)
    drivers: list[OperationalDriver] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        for key in ("entries", "drivers"):
            value = d.get(key)
            if value is None:
                d[key] = []
            elif isinstance(value, dict):
                d[key] = [value]
            elif isinstance(value, list):
                d[key] = [e for e in value if isinstance(e, (dict, BaseModel))]
            else:
                d[key] = []
        return d


class DocumentRecord(_CoerceModel):
    id: Optional[str] = None
    type: Optional[str] = None
    published: Optional[str] = None
    url: Optional[str] = None
    fetched_at: Optional[str] = None
    pages: Optional[int] = None
    sha256: Optional[str] = None
    path: Optional[str] = None
    status: Optional[str] = Field(None, description="captured | not_captured | rejected")
    error: Optional[str] = None
    extraction_warnings: list[str] = Field(default_factory=list)


class Gap(_CoerceModel):
    item: Optional[str] = None
    state: Optional[str] = None
    detail: Optional[str] = None


class Completeness(BaseModel):
    financials: Optional[str] = Field(None, description="complete | partial | missing")
    operational: Optional[str] = Field(None, description="complete | partial | not_captured")
    notes: Optional[str] = Field(None, description="indexed | missing | stale")
    analysis: Optional[str] = Field(None, description="present | missing")
    expected_documents: list[str] = Field(default_factory=list)
    as_of: Optional[str] = None
    gaps: list[Gap] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        for key in ("expected_documents", "gaps"):
            value = d.get(key)
            if value is None:
                d[key] = []
            elif isinstance(value, dict):
                d[key] = [value]
            elif not isinstance(value, list):
                d[key] = []
        return d


class FactSheet(BaseModel):
    meta: Meta = Field(default_factory=Meta)
    income_statement: Statement = Field(default_factory=lambda: Statement(periods=["current", "prior_yoy"]))
    balance_sheet: Statement = Field(default_factory=lambda: Statement(periods=["current", "prior_fy_end"]))
    cash_flow: Statement = Field(default_factory=Statement)
    details: Details = Field(default_factory=Details)
    context_notes: list[str] = Field(default_factory=list)
    operational: OperationalBlock = Field(default_factory=OperationalBlock)
    documents: list[DocumentRecord] = Field(default_factory=list)
    completeness: Completeness = Field(default_factory=Completeness)

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        if data is None:
            return {}
        if not isinstance(data, dict):
            return data
        d = dict(data)
        for key in ("meta", "income_statement", "balance_sheet", "cash_flow", "details"):
            if d.get(key) is None:
                d[key] = {}
        cn = d.get("context_notes")
        if cn is None:
            d["context_notes"] = []
        elif isinstance(cn, str):
            d["context_notes"] = [cn]
        elif isinstance(cn, list):
            d["context_notes"] = [_stringify(n) for n in cn if n is not None]
        else:
            d["context_notes"] = [_stringify(cn)]
        docs = d.get("documents")
        if docs is None:
            d["documents"] = []
        elif isinstance(docs, dict):
            d["documents"] = [docs]
        elif not isinstance(docs, list):
            d["documents"] = []
        if d.get("operational") is None:
            d["operational"] = {}
        if d.get("completeness") is None:
            d["completeness"] = {}
        return d


# Canonical line-item keys (used for validation + grep-consistency QA).
INCOME_LINES = (
    "revenue", "cogs", "gross_profit", "selling_expense", "gna_expense",
    "operating_profit", "interest_income", "interest_expense", "other_income_net",
    "pbt", "tax", "net_profit", "np_owners", "np_nci", "eps_basic",
)
BALANCE_LINES = (
    "cash", "trade_receivables_net", "inventories", "prepaid_taxes",
    "securities_current", "current_assets", "securities_noncurrent", "ppe_net",
    "exploration_assets", "mining_properties", "intangibles", "restricted_cash",
    "noncurrent_assets", "total_assets", "trade_payables", "taxes_payable",
    "accrued_current", "other_advances", "current_liabilities", "loans_current",
    "accrued_noncurrent", "reclamation_provision", "loans_noncurrent",
    "noncurrent_liabilities", "total_liabilities", "equity_owners", "nci",
    "total_equity",
)
CASHFLOW_LINES = (
    "collections_from_customers", "interest_received", "payments_to_suppliers",
    "salaries_paid", "ocf", "capex_ppe", "capex_other", "icf", "loan_repayments",
    "interest_paid", "dividends_paid", "fcf", "net_change", "cash_begin", "cash_end",
)


def canonical_line_keys() -> dict[str, tuple[str, ...]]:
    return {
        "income_statement": INCOME_LINES,
        "balance_sheet": BALANCE_LINES,
        "cash_flow": CASHFLOW_LINES,
    }


def consistent_prior_eps(
    eps: Optional[float],
    eps_prior: Optional[float],
    np_owners: Optional[float],
    np_owners_prior: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """Detect a prior-period EPS stored at the wrong decimal scale.

    IDX XBRL xlsx files occasionally carry EPS at the wrong scale (ITMG 2026-II
    stores prior basic EPS as 8e-05 while the PDF reports 0.08). Both periods
    should imply a comparable share count; when they do not, try rescaling the
    prior EPS by powers of ten and adopt the first scale that reconciles.

    Returns ``(eps, eps_prior, correction_note)``; ``eps_prior`` is ``None`` when
    no rescale reconciles the figure (callers then omit the growth rate).
    """
    if eps in (None, 0) or eps_prior in (None, 0) or np_owners is None or np_owners_prior is None:
        return eps, eps_prior, None
    shares_cur = np_owners / eps
    if shares_cur <= 0:
        return eps, eps_prior, None

    def plausible(prior_eps: float) -> bool:
        shares_prior = np_owners_prior / prior_eps
        # Treasury-share buybacks move the count by a few percent, never 10x.
        return shares_prior > 0 and 0.5 <= shares_prior / shares_cur <= 2.0

    if plausible(eps_prior):
        return eps, eps_prior, None

    for exponent in (1, 2, 3, -1, -2, -3):
        candidate = eps_prior * (10 ** exponent)
        if plausible(candidate):
            note = (
                f"prior eps_basic {eps_prior!r} implies an implausible share count; "
                f"rescaled by 1e{exponent:+d} to {candidate:.4f} "
                f"(implied shares {np_owners_prior / candidate:,.0f} vs current {shares_cur:,.0f})"
            )
            return eps, candidate, note

    note = (
        f"prior eps_basic {eps_prior!r} inconsistent with implied share count "
        f"({np_owners_prior / eps_prior:,.0f} vs {shares_cur:,.0f}) and no decimal "
        f"rescale reconciles it"
    )
    return eps, None, note