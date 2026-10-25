"""
Stage 1a — deterministic primary-statement seed from IDX XBRL Excel filings.

The report_pipeline already parses `FinancialStatement-*.xlsx` into
pipeline.duckdb (xlsx_meta + xlsx_statements). Those rows ARE the primary
statements — same numbers the PDF prints, with both period columns — so the
LLM never needs to transcribe them. This module builds a FactSheet containing
meta + income_statement + balance_sheet + cash_flow directly from DuckDB.

The seed becomes the highest-priority layer in merge.py; LLM chunk sheets only
fill keys the seed leaves null (e.g. dividends_paid, operating_profit) plus
`details` and `context_notes`, which the xlsx does not carry.

Conventions
-----------
- Label matching is EXACT on lowercased `label_en` within a preferred sheet
  (1210000/2210000/3210000 balance sheet, 1311000/2311000/3311000 income
  statement by function, 1510000/2510000/3510000 direct cash flow). The IDX
  taxonomy makes these labels stable across issuers.
- `mode: "sum"` aggregates composed lines (e.g. trade_receivables_net =
  third-party + related-party rows); `mode: "first"` takes the first candidate
  with a value. `minus` labels are subtracted (other_income_net).
- Signs are kept AS STORED in the xlsx: expenses and cash outflows are
  POSITIVE (cogs 174,670,963,271, capex 15,910,169,208). ratios.py relies on
  this (fcf = ocf - capex). Note this differs from old LLM sheets, which
  sometimes copied the PDF's parenthesized negatives.
- Values stay in the FILING'S units (meta.units from xlsx_meta.rounding:
  "million" for "Jutaan / In Million", "full" for "Satuan Penuh / Full
  Amount"). ratios.py normalizes downstream; nothing is rescaled here.
- 1410000 (changes in equity) is NOT used: its column-per-component layout
  does not survive the report_pipeline's 4-column parser.

When no complete xlsx coverage exists, ensure_xlsx_coverage attempts to parse the
local FinancialStatement workbook before the pipeline falls back to full LLM
statement extraction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from schema import FactSheet, Meta, Statement, consistent_prior_eps

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DB_PATH = _PROJECT_ROOT / "idx_data" / "pipeline.duckdb"
REPORTS_DIR = _PROJECT_ROOT / "idx_data" / "reports"
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"

QUARTER_TO_PERIOD = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}

# Preferred sheet per statement (first sheet with rows wins).
BS_SHEETS = ("1210000", "1220000", "2210000", "2220000", "3210000", "3220000",
             "4220000")
IS_SHEETS = ("1311000", "1312000", "1321000", "1322000",
             "2311000", "2312000", "2321000", "2322000",
             "3311000", "3312000", "3321000", "3322000",
             "4322000", "4312000")
CF_SHEETS = ("1510000", "1520000", "2510000", "2520000", "3510000", "3520000",
             "4510000", "4520000")

# Banking/financial-services taxonomy sheets (banks, e.g. BBCA). Filers often
# leave 4312000 (P&L "presented using...") blank and put the numbers in
# 4322000 (P&L + OCI), so both are candidates and the null-filter below picks
# whichever actually carries values.
BANK_IS_SHEETS = ("4312000", "4322000")

REQUIRED_SEED_KEYS = {
    "income_statement": ("revenue", "gross_profit", "pbt", "net_profit"),
    "balance_sheet": ("total_assets", "total_liabilities", "total_equity"),
    "cash_flow": ("ocf", "net_change", "cash_end"),
}

# Banks have no cost of sales / gross profit line, so a bank seed is complete
# once revenue, PBT and net profit are present in both columns.
BANK_REQUIRED_SEED_KEYS = {
    "income_statement": ("revenue", "pbt", "net_profit"),
    "balance_sheet": REQUIRED_SEED_KEYS["balance_sheet"],
    "cash_flow": REQUIRED_SEED_KEYS["cash_flow"],
}

# canonical key -> {"labels": [...], "mode": "first"|"sum", "minus": [...]}
# Labels are matched case-insensitively against label_en.
IS_MAP: dict[str, dict[str, Any]] = {
    "revenue":          {"labels": ["sales and revenue"]},
    "cogs":             {"labels": ["cost of sales and revenue"]},
    "gross_profit":     {"labels": ["total gross profit"]},
    "selling_expense":  {"labels": ["selling expenses"]},
    "gna_expense":      {"labels": ["general and administrative expenses"]},
    "operating_profit": {"labels": ["total operating profit", "total operating profit (loss)",
                                    "operating profit", "operating profit (loss)"]},
    "interest_income":  {"labels": ["finance income", "interest income"]},
    "interest_expense": {"labels": ["interest and finance costs", "finance costs"]},
    "other_income_net": {"labels": ["other income"], "minus": ["other expenses"]},
    "pbt":              {"labels": ["total profit (loss) before tax"]},
    "tax":              {"labels": ["tax benefit (expenses)", "income tax expense"]},
    "net_profit":       {"labels": ["total profit (loss)"]},
    "np_owners":        {"labels": ["profit (loss) attributable to parent entity"]},
    "np_nci":           {"labels": ["profit (loss) attributable to non-controlling interests"]},
    "eps_basic":        {"labels": ["basic earnings (loss) per share",
                                    "basic earnings (loss) per share from continuing operations"]},
}

# Banking P&L map. Banks report "Interest income" rather than "Sales and revenue"
# and have no cost of sales / gross profit line, so the industry IS_MAP above
# matches nothing and the seed would be rejected. Convention used here:
# `revenue` = total operating income = interest income + other operating income
# (the standard IDX bank top line), `operating_profit` = "Total profit from
# operation" (the PPOP-equivalent line).
BANK_IS_MAP: dict[str, dict[str, Any]] = {
    "revenue":          {"labels": ["interest income", "other operating income"], "mode": "sum"},
    "interest_income":  {"labels": ["interest income"]},
    "interest_expense": {"labels": ["interest expenses"]},
    "selling_expense":  {"labels": ["selling expenses"]},
    "gna_expense":      {"labels": ["general and administrative expenses"]},
    "operating_profit": {"labels": ["total profit from operation",
                                    "total operating profit", "operating profit"]},
    "other_income_net": {"labels": ["other operating income"]},
    "pbt":              {"labels": ["total profit (loss) before tax"]},
    "tax":              {"labels": ["tax benefit (expenses)", "income tax expense"]},
    "net_profit":       {"labels": ["total profit (loss)"]},
    "np_owners":        {"labels": ["profit (loss) attributable to parent entity"]},
    "np_nci":           {"labels": ["profit (loss) attributable to non-controlling interests"]},
    "eps_basic":        {"labels": ["basic earnings (loss) per share",
                                    "basic earnings (loss) per share from continuing operations"]},
}

BS_MAP: dict[str, dict[str, Any]] = {
    "cash":                  {"labels": ["cash and cash equivalents"]},
    "trade_receivables_net": {"labels": ["trade receivables third parties",
                                         "trade receivables related parties",
                                         "trade receivables"], "mode": "sum"},
    "inventories":           {"labels": ["current inventories"]},
    "prepaid_taxes":         {"labels": ["current prepaid taxes"]},
    "securities_current":    {"labels": ["short-term investments",
                                         "current financial assets fair value through other comprehensive income",
                                         "current financial assets at fair value through profit or loss"]},
    "current_assets":        {"labels": ["total current assets"]},
    "securities_noncurrent": {"labels": ["long-term investments",
                                         "non-current financial assets fair value through other comprehensive income",
                                         "non-current financial assets at fair value through profit or loss"]},
    "ppe_net":               {"labels": ["property, plant, and equipment"]},
    "exploration_assets":    {"labels": ["exploration and evaluation assets"]},
    "mining_properties":     {"labels": ["mining properties"]},
    "intangibles":           {"labels": ["goodwill", "intangible assets other than goodwill",
                                         "intangible assets"], "mode": "sum"},
    "restricted_cash":       {"labels": ["current restricted funds",
                                         "non-current restricted funds"], "mode": "sum"},
    "noncurrent_assets":     {"labels": ["total non-current assets"]},
    "total_assets":          {"labels": ["total assets"]},
    "trade_payables":        {"labels": ["trade payables third parties",
                                         "trade payables related parties",
                                         "trade payables"], "mode": "sum"},
    "taxes_payable":         {"labels": ["taxes payable"]},
    "accrued_current":       {"labels": ["current accrued expenses"]},
    "other_advances":        {"labels": ["current advances from customers third parties",
                                         "current advances from customers related parties"], "mode": "sum"},
    "current_liabilities":   {"labels": ["total current liabilities"]},
    "loans_current":         {"labels": ["short term bank loans", "short-term bank loans",
                                         "current bank loans",
                                         "current maturities of bank loans",
                                         "current maturities of finance lease liabilities",
                                         "current maturities of other borrowings",
                                         "current maturities of non-bank financial insitutions loan",
                                         "current maturities of non-bank financial institutions loan"],
                              "mode": "sum"},
    "accrued_noncurrent":    {"labels": ["non-current accrued expenses", "long-term accrued expenses"]},
    # Mining issuers carry reclamation under generic "other provisions" rows;
    # dedicated reclamation labels win when present.
    "reclamation_provision": {"labels": ["reclamation provision",
                                         "provision for environmental rehabilitation",
                                         "provision for mine closure",
                                         "other current provisions",
                                         "other non-current provisions"], "mode": "sum"},
    "loans_noncurrent":      {"labels": ["long-term bank loans", "long term bank loans",
                                         "long-term non-bank financial institutions loan",
                                         "long term non-bank financial insitutions loan",
                                         "long-term finance lease liabilities",
                                         "long-term other borrowings",
                                         "long-term bonds payable"], "mode": "sum"},
    "noncurrent_liabilities": {"labels": ["total non-current liabilities"]},
    "total_liabilities":     {"labels": ["total liabilities"]},
    "equity_owners":         {"labels": ["total equity attributable to equity owners of parent entity"]},
    "nci":                   {"labels": ["non-controlling interests"]},
    "total_equity":          {"labels": ["total equity"]},
}

CF_MAP: dict[str, dict[str, Any]] = {
    "collections_from_customers": {"labels": ["receipts from customers"]},
    "interest_received":  {"labels": ["interests received from operating activities",
                                      "interests received from investing activities",
                                      "interest received"]},
    "payments_to_suppliers": {"labels": ["payments to suppliers for goods and services"]},
    "salaries_paid":      {"labels": ["payments for salaries and allowances",
                                      "payments to employees"]},
    "ocf":                {"labels": ["total net cash flows received from (used in) operating activities",
                                      "net cash flows from (used in) operating activities"]},
    "capex_ppe":          {"labels": ["payments for acquisition of property, plant and equipment"]},
    "capex_other":        {"labels": ["payments for acquisition of intangible assets",
                                      "payments for acquisition of plantation assets",
                                      "payments for deferred charges",
                                      "payments for advances for purchase of property, plant and equipment",
                                      "payments for acquisition of exploration and evaluation assets"],
                           "mode": "sum"},
    "icf":                {"labels": ["total net cash flows received from (used in) investing activities",
                                      "net cash flows from (used in) investing activities"]},
    "loan_repayments":    {"labels": ["payments of bank loans",
                                      "payments of non-bank financial institution loan",
                                      "payments of finance lease liabilities",
                                      "payments of bonds payable"], "mode": "sum"},
    "interest_paid":      {"labels": ["interests paid from operating activities",
                                      "interests paid from financing activities",
                                      "interest paid"]},
    "dividends_paid":     {"labels": ["payments of dividends", "dividends paid"]},
    "net_change":         {"labels": ["total net increase (decrease) in cash and cash equivalents"]},
    "cash_begin":         {"labels": ["cash and cash equivalents cash flows, beginning of the period",
                                      "cash and cash equivalents at beginning of the period"]},
    "cash_end":           {"labels": ["cash and cash equivalents cash flows, end of the period",
                                      "cash and cash equivalents at end of the period"]},
}

_ROUNDING_TO_UNITS = (
    ("jutaan", "million"), ("in million", "million"),
    ("ribuan", "thousand"), ("in thousand", "thousand"),
    ("satuan penuh", "full"), ("full amount", "full"), ("full rupiah", "full"),
)


def _normalize_units(rounding: str) -> Optional[str]:
    s = (rounding or "").strip().lower()
    for needle, unit in _ROUNDING_TO_UNITS:
        if needle in s:
            return unit
    return s or None


def _normalize_currency(currency: str) -> Optional[str]:
    s = (currency or "").strip().lower()
    if "idr" in s or "rupiah" in s:
        return "IDR"
    if "usd" in s or "dollar" in s:
        return "USD"
    return currency or None


def _normalize_audited(audit_status: str) -> Optional[bool]:
    s = (audit_status or "").strip().lower()
    if not s:
        return None
    if "tidak diaudit" in s or "unaudit" in s:
        return False
    if "diaudit" in s or "audit" in s:
        return True
    return None


def _label_index(rows: list[tuple]) -> dict[str, tuple[Optional[float], Optional[float]]]:
    """lowercased label_en -> (value_current, value_prior). When a label repeats
    (taxonomy template rows), prefer the row that has a current value."""
    idx: dict[str, tuple[Optional[float], Optional[float]]] = {}
    for lbl, vc, vp in rows:
        if not lbl:
            continue
        key = str(lbl).strip().lower()
        if key not in idx or (idx[key][0] is None and vc is not None):
            idx[key] = (vc, vp)
    return idx


def _resolve(idx: dict[str, tuple], spec: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    mode = spec.get("mode", "first")
    labels = spec["labels"]

    def pick(col: int) -> Optional[float]:
        if mode == "sum":
            vals = [idx[l][col] for l in labels if l in idx and idx[l][col] is not None]
            return float(sum(vals)) if vals else None
        for l in labels:
            if l in idx and idx[l][col] is not None:
                return idx[l][col]
        return None

    cur, pri = pick(0), pick(1)
    minus = spec.get("minus") or []
    if minus:
        mcur = sum(idx[l][0] for l in minus if l in idx and idx[l][0] is not None)
        mpri = sum(idx[l][1] for l in minus if l in idx and idx[l][1] is not None)
        if mcur:
            cur = (cur or 0.0) - mcur
        if mpri:
            pri = (pri or 0.0) - mpri
    return cur, pri


def _build_statement(idx: dict[str, tuple], mapping: dict[str, dict], periods: list[str]) -> Statement:
    lines: dict[str, Optional[float]] = {}
    prior: dict[str, Optional[float]] = {}
    for key, spec in mapping.items():
        cur, pri = _resolve(idx, spec)
        lines[key] = cur
        prior[key] = pri
    return Statement(periods=list(periods), lines=lines, prior_lines=prior)


def _fetch_sheet_rows(conn, ticker: str, year: int, period: str, sheet_codes: tuple[str, ...]) -> list[tuple]:
    return _fetch_sheet_rows_with_code(conn, ticker, year, period, sheet_codes)[1]


def _fetch_sheet_rows_with_code(
    conn, ticker: str, year: int, period: str, sheet_codes: tuple[str, ...],
) -> tuple[Optional[str], list[tuple]]:
    """Return (sheet_code, rows) for the first sheet that carries values."""
    for code in sheet_codes:
        rows = conn.execute(
            "SELECT label_en, value_current, value_prior FROM xlsx_statements "
            "WHERE ticker=? AND year=? AND period=? AND sheet_code=? "
            "  AND (value_current IS NOT NULL OR value_prior IS NOT NULL) "
            "ORDER BY row_number",
            (ticker, year, period, code),
        ).fetchall()
        if rows:
            return code, rows
    return None, []


def _connect(db_path: Path):
    """Open DuckDB read-only; fall back to a plain connection when the caller
    (e.g. pipeline.py) already holds a read-write handle in this process —
    DuckDB forbids mixing read-only with read-write handles on the same file."""
    import duckdb

    try:
        return duckdb.connect(str(db_path), read_only=True)
    except Exception:  # noqa: BLE001
        return duckdb.connect(str(db_path))


def has_coverage(ticker: str, year: int, quarter: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """True when a complete primary-statement XLSX seed can be built."""
    try:
        return seed_from_xlsx(ticker, year, quarter, db_path) is not None
    except Exception:  # noqa: BLE001
        return False


def seed_from_xlsx(
    ticker: str, year: int, quarter: str, db_path: Path = DEFAULT_DB_PATH, conn=None,
) -> Optional[FactSheet]:
    """Build a FactSheet with meta + 3 primary statements from xlsx_statements.
    Returns None when no coverage (caller falls back to LLM extraction)."""
    import sys

    period = QUARTER_TO_PERIOD.get(quarter, quarter.lower())
    own_conn = conn is None
    try:
        if own_conn:
            conn = _connect(db_path)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] xlsx seed unavailable ({e}); falling back to LLM extraction", file=sys.stderr)
        return None
    try:
        meta_row = conn.execute(
            "SELECT currency, rounding, audit_status, period_end, prior_period_end "
            "FROM xlsx_meta WHERE ticker=? AND year=? AND period=?",
            (ticker, year, period),
        ).fetchone()
        bs_rows = _fetch_sheet_rows(conn, ticker, year, period, BS_SHEETS)
        is_code, is_rows = _fetch_sheet_rows_with_code(conn, ticker, year, period, IS_SHEETS)
        cf_rows = _fetch_sheet_rows(conn, ticker, year, period, CF_SHEETS)
    except Exception:
        return None
    finally:
        if own_conn and conn is not None:
            conn.close()

    if not (bs_rows and is_rows and cf_rows):
        return None

    currency, rounding, audit_status, period_end, prior_period_end = (
        meta_row if meta_row else (None, None, None, None, None)
    )
    meta = Meta(
        ticker=ticker, quarter=quarter, year=year,
        currency=_normalize_currency(currency or ""),
        units=_normalize_units(rounding or ""),
        audited=_normalize_audited(audit_status or ""),
        period_end=period_end or None,
        prior_period_end=prior_period_end or None,
        source_file="xlsx",
    )

    is_bank = is_code in BANK_IS_SHEETS

    income = _build_statement(
        _label_index(is_rows), BANK_IS_MAP if is_bank else IS_MAP, ["current", "prior_yoy"],
    )
    balance = _build_statement(_label_index(bs_rows), BS_MAP, ["current", "prior_fy_end"])
    cash = _build_statement(_label_index(cf_rows), CF_MAP, ["current", "prior_yoy"])

    # Some IDX XBRL files store the prior-period EPS at the wrong decimal scale.
    # Correct it here so the canonical fact sheet (not just the ratio layer)
    # carries the figure the PDF actually reports.
    eps, eps_prior, eps_note = consistent_prior_eps(
        income.lines.get("eps_basic"),
        income.prior_lines.get("eps_basic"),
        income.lines.get("np_owners"),
        income.prior_lines.get("np_owners"),
    )
    if eps_note:
        income.prior_lines["eps_basic"] = eps_prior
        print(f"  [SEED] {ticker} {quarter} {year}: {eps_note}", file=sys.stderr)

    seed = FactSheet(meta=meta, income_statement=income, balance_sheet=balance, cash_flow=cash)
    return seed if _is_complete_seed(seed, is_bank=is_bank) else None


def _is_complete_seed(sheet: FactSheet, is_bank: bool = False) -> bool:
    """Require core current and comparative values before using XLSX-only mode."""
    required = BANK_REQUIRED_SEED_KEYS if is_bank else REQUIRED_SEED_KEYS
    for statement_name, keys in required.items():
        statement = getattr(sheet, statement_name)
        for column in ("lines", "prior_lines"):
            values = getattr(statement, column) or {}
            if any(values.get(key) is None for key in keys):
                return False
    return True


def _xlsx_path(ticker: str, year: int, quarter: str) -> Path | None:
    period = QUARTER_TO_PERIOD.get(quarter.upper(), quarter.lower())
    report_dir = REPORTS_DIR / ticker.upper()
    candidates = sorted(
        path for path in report_dir.glob(f"{year}_*/FinancialStatement*.xlsx")
        if path.parent.name.split("_", 1)[-1].lower() == period
    )
    return candidates[0] if candidates else None


def ensure_xlsx_coverage(
    ticker: str,
    year: int,
    quarter: str,
    db_path: Path = DEFAULT_DB_PATH,
    conn=None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ingest a local XLSX when DuckDB lacks a complete seed."""
    if seed_from_xlsx(ticker, year, quarter, db_path, conn=conn) is not None:
        return {"status": "available", "seeded": True, "ingested": False}

    xlsx_path = _xlsx_path(ticker, year, quarter)
    if xlsx_path is None:
        return {"status": "unavailable", "seeded": False, "ingested": False}
    if dry_run:
        return {"status": "would_ingest", "seeded": False, "ingested": False,
                "path": str(xlsx_path)}

    own_conn = conn is None
    if own_conn:
        import duckdb
        conn = duckdb.connect(str(db_path))
    try:
        from scripts.report_pipeline.db import PipelineDB
        from scripts.report_pipeline.xlsx_parser import parse_xlsx

        pipeline_db = PipelineDB(conn)
        pipeline_db.init_schema()
        period = QUARTER_TO_PERIOD.get(quarter.upper(), quarter.lower())
        meta, rows = parse_xlsx(str(xlsx_path), ticker=ticker.upper(), year=year, period=period)
        if not rows:
            return {"status": "parse_failed", "seeded": False, "ingested": False,
                    "path": str(xlsx_path), "error": "no statement rows extracted"}

        conn.execute("DELETE FROM xlsx_statements WHERE ticker=? AND year=? AND period=?",
                     (ticker.upper(), year, period))
        conn.execute("DELETE FROM xlsx_meta WHERE ticker=? AND year=? AND period=?",
                     (ticker.upper(), year, period))
        pipeline_db.upsert_xlsx_meta(meta)
        pipeline_db.upsert_xlsx_statements(rows)
    except Exception as exc:  # noqa: BLE001
        return {"status": "parse_failed", "seeded": False, "ingested": False,
                "path": str(xlsx_path), "error": str(exc)}
    finally:
        if own_conn:
            conn.close()

    seeded = seed_from_xlsx(
        ticker, year, quarter, db_path, conn=None if own_conn else conn,
    ) is not None
    if not seeded:
        return {"status": "incomplete", "seeded": False, "ingested": True,
                "path": str(xlsx_path), "error": "required statement values are missing"}
    return {"status": "ingested", "seeded": True, "ingested": True,
            "path": str(xlsx_path)}


def fact_artifact_needs_seed(ticker: str, year: int, quarter: str) -> bool:
    """Return whether an existing canonical fact file predates XLSX seeding."""
    import yaml

    path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    if not path.exists():
        return False
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return True
    return (data.get("meta") or {}).get("source_file") != "xlsx"


def missing_keys(sheet: FactSheet) -> dict[str, list[str]]:
    """Canonical line keys the seed left null — useful for logging gap-fill."""
    out: dict[str, list[str]] = {}
    for name in ("income_statement", "balance_sheet", "cash_flow"):
        stmt = getattr(sheet, name)
        gaps = [k for k, v in stmt.lines.items() if v is None]
        if gaps:
            out[name] = gaps
    return out


if __name__ == "__main__":
    import argparse
    import yaml

    p = argparse.ArgumentParser(description="Seed a fact sheet from parsed xlsx (no LLM)")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    a = p.parse_args()
    sheet = seed_from_xlsx(a.ticker, a.year, a.quarter)
    if sheet is None:
        print(f"No xlsx coverage for {a.ticker} {a.quarter} {a.year}")
    else:
        print(yaml.safe_dump(sheet.model_dump(exclude_none=False), sort_keys=False, allow_unicode=True))
        print("# missing:", missing_keys(sheet))
