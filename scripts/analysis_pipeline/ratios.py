"""
Stage 2b — deterministic ratio computation (no LLM).

All ratios are derived purely from the canonical fact sheet (and an optional
market-data stub). Every ratio records its formula and the exact line-item
inputs used, emitted to {T}_{Q}_{Y}.ratios.yaml for auditability.

Units: monetary values are taken at the scale recorded in `meta.units`. Ratios
and growth rates are scale-invariant; absolute per-share/EV figures assume
`meta.units` is consistent across columns (the report guarantees this). Quarter
figures are annualized with a factor of 4 where a rate notion requires it, and
the annualization is recorded in the formula string.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from schema import FactSheet, consistent_prior_eps


class Ratio(BaseModel):
    value: float | None = None
    formula: str
    inputs: dict[str, Any] = {}

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FACTS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "facts"


def _val(stmt, key: str | None, col: str = "lines") -> float | None:
    if key is None:
        return None
    block = getattr(stmt, col) or {}
    v = block.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class R:
    def __init__(self, sheet: FactSheet, market: dict | None = None):
        self.sheet = sheet
        self.is_ = sheet.income_statement
        self.bs = sheet.balance_sheet
        self.cf = sheet.cash_flow
        self.md = market or {}
        self.out: dict[str, Ratio] = {}

    def cur(self, stmt, key):
        return _val(stmt, key, "lines")

    def pri(self, stmt, key):
        return _val(stmt, key, "prior_lines")

    def add(self, name, value, formula, inputs):
        self.out[name] = Ratio(value=value, formula=formula, inputs=inputs)
        return value

    @staticmethod
    def _safe(v):
        return None if v is None else round(v, 6)

    def _growth(self, name, cur, pri, label):
        if cur is None or pri is None:
            return self.add(name, None, f"{label}: ({cur}-{pri})/{pri}", {label+"_cur": cur, label+"_prior": pri})
        if pri == 0:
            return self.add(name, None, f"{label}: prior=0 (undefined)", {label+"_cur": cur, label+"_prior": pri})
        return self.add(name, self._safe((cur - pri) / pri), f"({cur}-{pri})/{pri}", {label+"_cur": cur, label+"_prior": pri})

    def _consistent_eps(self, eps, eps_p, npo, npo_p) -> tuple[float | None, float | None, str | None]:
        """Source-scale guard; see ``schema.consistent_prior_eps``."""
        return consistent_prior_eps(eps, eps_p, npo, npo_p)

    def _pct(self, name, num, den, label):
        if num is None or den is None or den == 0:
            return self.add(name, None, f"{label}: {num}/{den}", {label+"_num": num, label+"_den": den})
        return self.add(name, self._safe(num / den), f"{num}/{den}", {label+"_num": num, label+"_den": den})

    def _ratio(self, name, num, den, label):
        return self._pct(name, num, den, label)

    def compute(self) -> dict[str, dict]:
        is_, bs, cf = self.is_, self.bs, self.cf
        rev = self.cur(is_, "revenue")
        rev_p = self.pri(is_, "revenue")
        cogs = self.cur(is_, "cogs")
        gp = self.cur(is_, "gross_profit")
        gp_p = self.pri(is_, "gross_profit")
        sell = self.cur(is_, "selling_expense")
        gna = self.cur(is_, "gna_expense")
        op = self.cur(is_, "operating_profit")
        op_p = self.pri(is_, "operating_profit")
        oi = self.cur(is_, "other_income_net")
        pbt = self.cur(is_, "pbt")
        tax = self.cur(is_, "tax")
        np = self.cur(is_, "net_profit")
        np_p = self.pri(is_, "net_profit")
        npo = self.cur(is_, "np_owners")
        nci = self.cur(is_, "np_nci")
        eps = self.cur(is_, "eps_basic")
        eps_p = self.pri(is_, "eps_basic")
        eps_p_raw = eps_p
        eps, eps_p, eps_note = self._consistent_eps(eps, eps_p, npo, self.pri(is_, "np_owners"))
        iinc = self.cur(is_, "interest_income")
        iexp = self.cur(is_, "interest_expense")

        cash = self.cur(bs, "cash")
        sec_curr = self.cur(bs, "securities_current")
        sec_nc = self.cur(bs, "securities_noncurrent")
        recv = self.cur(bs, "trade_receivables_net")
        inv = self.cur(bs, "inventories")
        ca = self.cur(bs, "current_assets")
        cl = self.cur(bs, "current_liabilities")
        ppe = self.cur(bs, "ppe_net")
        ta = self.cur(bs, "total_assets")
        ta_p = self.pri(bs, "total_assets")
        tp = self.cur(bs, "trade_payables")
        loans_curr = self.cur(bs, "loans_current")
        loans_nc = self.cur(bs, "loans_noncurrent")
        rec_prov = self.cur(bs, "reclamation_provision")
        tl = self.cur(bs, "total_liabilities")
        eq = self.cur(bs, "total_equity")
        eq_p = self.pri(bs, "total_equity")
        eqo = self.cur(bs, "equity_owners")
        eqo_p = self.pri(bs, "equity_owners")

        coll = self.cur(cf, "collections_from_customers")
        ocf = self.cur(cf, "ocf"); ocf_p = self.pri(cf, "ocf")
        capex_ppe = self.cur(cf, "capex_ppe"); capex_other = self.cur(cf, "capex_other")
        loan_rep = self.cur(cf, "loan_repayments")
        div_paid = self.cur(cf, "dividends_paid")
        fcf_line = self.cur(cf, "fcf")
        cash_end = self.cur(cf, "cash_end")

        # ── Profitability ──
        self._pct("gross_margin", gp, rev, "gross_margin")
        self._pct("operating_margin", op, rev, "operating_margin")
        self._pct("pretax_margin", pbt, rev, "pretax_margin")
        self._pct("net_margin", np, rev, "net_margin")
        # The xlsx stores tax expense as a negative charge (186/190 filings
        # observed). Express the rate as expense/pretax so a normal tax take
        # reads positive, and a genuine benefit reads negative.
        eff_tax = -tax if tax is not None else None
        self._ratio("effective_tax_rate", eff_tax, pbt, "eff_tax")

        # ── Growth (YoY vs prior_lines) ──
        self._growth("revenue_growth", rev, rev_p, "revenue")
        self._growth("gross_profit_growth", gp, gp_p, "gross_profit")
        self._growth("operating_profit_growth", op, op_p, "operating_profit")
        self._growth("net_profit_growth", np, np_p, "net_profit")
        self._growth("eps_growth", eps, eps_p, "eps")
        if eps_note:
            self.out["eps_growth"].inputs["source_correction"] = eps_note
            self.out["eps_growth"].inputs["eps_prior_as_reported"] = eps_p_raw
        self._growth("total_assets_growth", ta, ta_p, "total_assets")
        self._growth("equity_growth", eq, eq_p, "total_equity")
        self._growth("ocf_growth", ocf, ocf_p, "ocf")

        # ── Returns (annualized over average balances) ──
        avg_eq = (eq + eq_p) / 2 if eq is not None and eq_p is not None else None
        avg_eqo = (eqo + eqo_p) / 2 if eqo is not None and eqo_p is not None else None
        avg_ta = (ta + ta_p) / 2 if ta is not None and ta_p is not None else None
        if np is not None and avg_eq:
            self.add("roe_total", self._safe(np * 4 / avg_eq), "(np*4)/avg_equity",
                     {"net_profit": np, "avg_equity": avg_eq, "factor": 4})
        else:
            self.add("roe_total", None, "(np*4)/avg_equity", {"net_profit": np, "avg_equity": avg_eq})
        if npo is not None and avg_eqo:
            self.add("roe_owners", self._safe(npo * 4 / avg_eqo), "(np_owners*4)/avg_owners_equity",
                     {"np_owners": npo, "avg_owners_equity": avg_eqo, "factor": 4})
        else:
            self.add("roe_owners", None, "(np_owners*4)/avg_owners_equity", {"np_owners": npo, "avg_owners_equity": avg_eqo})
        if np is not None and avg_ta:
            self.add("roa", self._safe(np * 4 / avg_ta), "(np*4)/avg_assets",
                     {"net_profit": np, "avg_assets": avg_ta, "factor": 4})
        else:
            self.add("roa", None, "(np*4)/avg_assets", {"net_profit": np, "avg_assets": avg_ta})

        # ── Liquidity ──
        self._ratio("current_ratio", ca, cl, "current_ratio")
        quick_num = (ca - inv) if ca is not None and inv is not None else None
        self._ratio("quick_ratio", quick_num, cl, "quick_ratio")
        self._ratio("cash_ratio", cash, cl, "cash_ratio")
        wc = (ca - cl) if ca is not None and cl is not None else None
        self.add("working_capital", self._safe(wc) if wc is not None else None, "current_assets-current_liabilities",
                 {"current_assets": ca, "current_liabilities": cl})

        # ── Solvency ──
        self._ratio("debt_to_equity", tl, eq, "de")
        total_loans = (loans_curr or 0) + (loans_nc or 0) if (loans_curr is not None or loans_nc is not None) else None
        self._ratio("loans_to_equity", total_loans, eq, "loans_de")
        self._pct("equity_ratio", eq, ta, "equity_ratio")
        sec_total = (sec_curr or 0) + (sec_nc or 0) if (sec_curr is not None or sec_nc is not None) else None
        net_debt = None
        if cash is not None and sec_total is not None and total_loans is not None:
            net_debt = total_loans - cash - sec_total
        self.add("net_debt", self._safe(net_debt) if net_debt is not None else None,
                 "loans - cash - securities", {"loans": total_loans, "cash": cash, "securities": sec_total})
        if net_debt is not None and eq:
            self.add("net_debt_to_equity", self._safe(net_debt / eq), "net_debt/equity",
                     {"net_debt": net_debt, "equity": eq})
        else:
            self.add("net_debt_to_equity", None, "net_debt/equity", {"net_debt": net_debt, "equity": eq})
        self._ratio("interest_coverage", op, iexp, "int_cov")

        # ── Efficiency (annualized) ──
        if rev is not None and avg_ta:
            self.add("asset_turnover", self._safe(rev * 4 / avg_ta), "(revenue*4)/avg_assets",
                     {"revenue": rev, "avg_assets": avg_ta, "factor": 4})
        else:
            self.add("asset_turnover", None, "(revenue*4)/avg_assets", {"revenue": rev, "avg_assets": avg_ta})
        self._dso_dio_dpo(rev, recv, inv, tp)
        ocf_for_ccc = ocf
        self._pct("collections_ratio", coll, rev, "coll/rev")

        # ── Cash flow ──
        capex_total = (capex_ppe or 0) + (capex_other or 0) if (capex_ppe is not None or capex_other is not None) else None
        if fcf_line is not None:
            self.add("free_cash_flow", self._safe(fcf_line), "fcf (reported)", {"fcf": fcf_line})
        elif ocf is not None and capex_total is not None:
            self.add("free_cash_flow", self._safe(ocf - capex_total), "ocf - capex",
                     {"ocf": ocf, "capex": capex_total})
        else:
            self.add("free_cash_flow", None, "ocf - capex", {"ocf": ocf, "capex": capex_total})
        if ocf is not None and np is not None and np != 0:
            self.add("cash_conversion", self._safe(ocf / np), "ocf/net_profit", {"ocf": ocf, "net_profit": np})
        else:
            self.add("cash_conversion", None, "ocf/net_profit", {"ocf": ocf, "net_profit": np})
        self.add("capex_total", self._safe(capex_total) if capex_total is not None else None,
                 "capex_ppe + capex_other", {"capex_ppe": capex_ppe, "capex_other": capex_other})
        self.add("loan_repayments", loan_rep, "loan_repayments", {"loan_repayments": loan_rep})
        self.add("dividends_paid", div_paid, "dividends_paid", {"dividends_paid": div_paid})

        # ── Per-share ──
        shares = None
        if eps not in (None, 0) and npo is not None:
            shares = npo / eps
        self.add("shares_outstanding", self._safe(shares) if shares is not None else None,
                 "np_owners / eps_basic", {"np_owners": npo, "eps_basic": eps})
        if shares and eqo:
            self.add("book_value_per_share", self._safe(eqo / shares), "equity_owners/shares",
                     {"equity_owners": eqo, "shares": shares})
        else:
            self.add("book_value_per_share", None, "equity_owners/shares", {"equity_owners": eqo, "shares": shares})

        # ── Valuation (needs market data) ──
        self._valuation(shares, np, op, net_debt, eqo, eps)

        return {k: v.model_dump() for k, v in self.out.items()}

    def _dso_dio_dpo(self, rev, recv, inv, tp):
        if rev and rev != 0:
            daily = rev / 91.25  # one quarter ≈ 91.25 days; DSO = recv/daily (quarter), annualized equivalent via /revenue*365*4 same thing
            # use annualized basis for consistency with the golden analysis: recv/(revenue/365*4)
            denom = rev / 365 * 4 if rev else None
            if denom and recv is not None:
                self.add("dso", self._safe(recv / denom), "receivables / (revenue/365*4)",
                         {"receivables": recv, "revenue": rev, "basis": "annualized"})
            else:
                self.add("dso", None, "receivables / (revenue/365*4)", {"receivables": recv, "revenue": rev})
            if denom and inv is not None:
                self.add("dio", self._safe(inv / denom), "inventories / (revenue/365*4)",
                         {"inventories": inv, "revenue": rev})
            else:
                self.add("dio", None, "inventories / (revenue/365*4)", {"inventories": inv, "revenue": rev})
            if denom and tp is not None:
                self.add("dpo", self._safe(tp / denom), "payables / (revenue/365*4)",
                         {"payables": tp, "revenue": rev})
            else:
                self.add("dpo", None, "payables / (revenue/365*4)", {"payables": tp, "revenue": rev})
            dso = self.out["dso"].value; dio = self.out["dio"].value; dpo = self.out["dpo"].value
            if dso is not None and dio is not None and dpo is not None:
                self.add("ccc", self._safe(dso + dio - dpo), "dso + dio - dpo",
                         {"dso": dso, "dio": dio, "dpo": dpo})
            else:
                self.add("ccc", None, "dso + dio - dpo", {"dso": dso, "dio": dio, "dpo": dpo})
        else:
            for k in ("dso", "dio", "dpo", "ccc"):
                self.add(k, None, "revenue unavailable", {"revenue": rev})

    def _valuation(self, shares, np, op, net_debt, eqo, eps):
        md = self.md
        price = md.get("price")
        market_cap = md.get("market_cap")
        if not price and not market_cap:
            for k in ("pe_annualized", "pb", "ev", "ev_ebitda"):
                self.add(k, None, "market data not provided", {})
            return
        if market_cap is None and shares and price:
            market_cap = shares * price
        ann_eps = (eps * 4) if eps is not None else None
        if price and ann_eps and ann_eps != 0:
            self.add("pe_annualized", self._safe(price / ann_eps), "price / (eps*4)",
                     {"price": price, "annualized_eps": ann_eps})
        else:
            self.add("pe_annualized", None, "price / (eps*4)", {"price": price, "annualized_eps": ann_eps})
        if price and shares:
            bvps = eqo / shares if eqo and shares else None
            if bvps:
                self.add("pb", self._safe(price / bvps), "price / book_value_per_share",
                         {"price": price, "bvps": bvps})
            else:
                self.add("pb", None, "price / bvps", {"price": price, "bvps": bvps})
        else:
            self.add("pb", None, "price / bvps", {"price": price})
        if market_cap is not None and net_debt is not None:
            ev = market_cap - net_debt  # net_debt already cash&sec - loans => subtract net_debt = add net cash
            self.add("ev", self._safe(ev), "market_cap - net_debt", {"market_cap": market_cap, "net_debt": net_debt})
            ebitda = (op * 4) if op is not None else None  # EBIT proxy, annualized
            if ev is not None and ebitda and ebitda != 0:
                self.add("ev_ebitda", self._safe(ev / ebitda), "ev / (operating_profit*4)",
                         {"ev": ev, "ebitda_proxy": ebitda})
            else:
                self.add("ev_ebitda", None, "ev / (operating_profit*4)", {"ev": ev, "ebitda_proxy": ebitda})
        else:
            self.add("ev", None, "market_cap - net_debt", {"market_cap": market_cap, "net_debt": net_debt})
            self.add("ev_ebitda", None, "ev / (operating_profit*4)", {})


def run(ticker: str, year: int, quarter: str, market_data: dict | None = None,
        force: bool = False, dry_run: bool = False) -> dict:
    canonical_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.yaml"
    out_path = FACTS_DIR / f"{ticker}_{quarter}_{year}.ratios.yaml"
    if out_path.exists() and not force and not dry_run:
        print(f"  [SKIP] {out_path.name} (ratios already computed)")
        return {"status": "skipped", "output": str(out_path)}
    if not canonical_path.exists():
        if dry_run:
            print(f"  [DRY] would compute ratios -> {out_path.name} (canonical fact sheet absent)")
            return {"status": "dry"}
        raise FileNotFoundError(f"Canonical fact sheet not found: {canonical_path}. Run merge first.")
    sheet = FactSheet.model_validate(yaml.safe_load(canonical_path.read_text(encoding="utf-8")))

    ratios = R(sheet, market_data or {}).compute()
    if dry_run:
        n = sum(1 for r in ratios.values() if r["value"] is not None)
        print(f"  [DRY] would compute {len(ratios)} ratios ({n} resolved) -> {out_path.name}")
        return {"status": "dry", "ratios": len(ratios), "resolved": n}

    out_path.write_text(yaml.safe_dump(ratios, sort_keys=False, allow_unicode=True), encoding="utf-8")
    n = sum(1 for r in ratios.values() if r["value"] is not None)
    print(f"  [RATIOS] {n}/{len(ratios)} resolved -> {out_path.name}")
    return {"status": "success", "output": str(out_path), "ratios": len(ratios), "resolved": n}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 2b ratio computation")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--market-data", default=None, help="path to market_data.yaml")
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    md = None
    if a.market_data:
        md = yaml.safe_load(Path(a.market_data).read_text(encoding="utf-8"))
    print(run(a.ticker, a.year, a.quarter, market_data=md, force=a.force, dry_run=a.dry_run))