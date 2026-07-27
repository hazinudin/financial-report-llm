"""
Pydantic models for the versioned fact sheet (Stage 1 output / Stage 2 merge).

Fields are optional with `None` defaults so per-chunk extraction outputs (which
cover only a subset of the document) validate against the same schema as the
final merged sheet. The merge stage (merge.py) fills the gaps.

All monetary values are raw integers/floats in the reporting currency scaled per
`meta.units`; downstream ratio code (ratios.py) handles unit normalization.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

SCHEMA_VERSION = 1


class Meta(BaseModel):
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


class Customer(BaseModel):
    name: Optional[str] = None
    amount: Optional[float] = None
    pct_of_sales: Optional[float] = None


class Loan(BaseModel):
    lender: Optional[str] = None
    balance: Optional[float] = None
    rate: Optional[str] = None
    maturity: Optional[str] = None
    notes: Optional[str] = None


class Subsidiary(BaseModel):
    name: Optional[str] = None
    ownership_pct: Optional[float] = None
    activity: Optional[str] = None
    total_assets: Optional[float] = None


class Segment(BaseModel):
    name: Optional[str] = None
    external_sales: Optional[float] = None
    internal_sales: Optional[float] = None
    gross_profit: Optional[float] = None
    net_profit: Optional[float] = None
    assets: Optional[float] = None


class Production(BaseModel):
    product: Optional[str] = None
    volume_mt: Optional[float] = None
    period: Optional[str] = None


class Reserve(BaseModel):
    product: Optional[str] = None
    residual_mt: Optional[float] = None
    as_of: Optional[str] = None


class RelatedParty(BaseModel):
    party: Optional[str] = None
    relationship: Optional[str] = None
    item: Optional[str] = None
    amount: Optional[float] = None


class Commitment(BaseModel):
    description: Optional[str] = None
    amount: Optional[float] = None
    counterparty: Optional[str] = None


class Dividend(BaseModel):
    period: Optional[str] = None
    per_share: Optional[float] = None
    total: Optional[float] = None


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


class FactSheet(BaseModel):
    meta: Meta
    income_statement: Statement = Field(default_factory=lambda: Statement(periods=["current", "prior_yoy"]))
    balance_sheet: Statement = Field(default_factory=lambda: Statement(periods=["current", "prior_fy_end"]))
    cash_flow: Statement = Field(default_factory=Statement)
    details: Details = Field(default_factory=Details)
    context_notes: list[str] = Field(default_factory=list)


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