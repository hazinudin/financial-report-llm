"""Unit tests for the deterministic ratio layer (``ratios.py``).

Focused on sign conventions that have caused artifacts in generated analyses:
tax expense is stored as a negative charge in the IDX xlsx, so the effective
tax rate must be expressed as expense/pretax.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import ratios  # noqa: E402
from schema import FactSheet  # noqa: E402


def _sheet(*, tax: float | None, pbt: float, eps: float = 0.1,
           eps_prior: float = 0.08, np_owners: float = 106_000,
           np_owners_prior: float = 91_000) -> FactSheet:
    return FactSheet.model_validate({
        "meta": {"ticker": "TST", "quarter": "Q2", "year": 2026,
                 "currency": "USD", "units": "thousand"},
        "income_statement": {
            "periods": ["current", "prior_yoy"],
            "lines": {"revenue": 1_000.0, "pbt": pbt, "tax": tax,
                      "net_profit": 110.0, "np_owners": np_owners,
                      "eps_basic": eps},
            "prior_lines": {"revenue": 900.0, "np_owners": np_owners_prior,
                            "eps_basic": eps_prior},
        },
        "balance_sheet": {"periods": ["current", "prior_fy_end"], "lines": {}},
        "cash_flow": {"periods": ["current", "prior_yoy"], "lines": {}},
    })


class EffectiveTaxRateTests(unittest.TestCase):
    def test_negative_stored_charge_yields_a_positive_rate(self):
        result = ratios.R(_sheet(tax=-49_197.0, pbt=158_711.0)).compute()
        etr = result["effective_tax_rate"]
        self.assertAlmostEqual(etr["value"], 0.309979, places=6)
        self.assertGreater(etr["value"], 0)

    def test_positive_stored_value_reads_as_a_benefit(self):
        # A genuine tax credit (stored positive) should show a negative rate.
        result = ratios.R(_sheet(tax=5_000.0, pbt=100_000.0)).compute()
        self.assertAlmostEqual(result["effective_tax_rate"]["value"], -0.05)

    def test_null_tax_yields_null_rate(self):
        result = ratios.R(_sheet(tax=None, pbt=100_000.0)).compute()
        self.assertIsNone(result["effective_tax_rate"]["value"])


class PriorEpsGuardTests(unittest.TestCase):
    def test_mis_scaled_prior_eps_is_rescaled_in_the_ratio_layer(self):
        sheet = _sheet(tax=-1.0, pbt=100.0, eps=0.10, eps_prior=8e-05)
        result = ratios.R(sheet).compute()
        growth = result["eps_growth"]
        self.assertAlmostEqual(growth["value"], 0.25, places=6)
        self.assertIn("rescaled", growth["inputs"]["source_correction"])

    def test_consistent_prior_eps_has_no_correction(self):
        result = ratios.R(_sheet(tax=-1.0, pbt=100.0)).compute()
        self.assertNotIn("source_correction", result["eps_growth"]["inputs"])


def _flow_sheet(quarter: str) -> FactSheet:
    """Sheet with the lines needed for annualized flow ratios."""
    return FactSheet.model_validate({
        "meta": {"ticker": "TST", "quarter": quarter, "year": 2026,
                 "currency": "IDR", "units": "million"},
        "income_statement": {
            "periods": ["current", "prior_yoy"],
            "lines": {"revenue": 900.0, "net_profit": 90.0, "np_owners": 90.0,
                      "eps_basic": 9.0},
            "prior_lines": {"revenue": 800.0, "net_profit": 80.0, "np_owners": 80.0,
                            "eps_basic": 8.0},
        },
        "balance_sheet": {
            "periods": ["current", "prior_fy_end"],
            "lines": {"total_assets": 1_800.0, "total_equity": 900.0,
                      "equity_owners": 900.0, "trade_receivables_net": 150.0,
                      "inventories": 50.0, "trade_payables": 100.0},
            "prior_lines": {"total_assets": 1_800.0, "total_equity": 900.0,
                            "equity_owners": 900.0},
        },
        "cash_flow": {"periods": ["current", "prior_yoy"], "lines": {}},
    })


class InterimAnnualizationTests(unittest.TestCase):
    """IDX interims are cumulative: Q2 = H1, Q3 = 9M. A flat ×4 is wrong."""

    def test_period_months_mapping(self):
        self.assertEqual(ratios.period_months("Q1"), 3)
        self.assertEqual(ratios.period_months("q2"), 6)
        self.assertEqual(ratios.period_months("Q3"), 9)
        self.assertEqual(ratios.period_months("Q4"), 12)
        self.assertEqual(ratios.period_months("audit"), 12)
        self.assertEqual(ratios.period_months(None), 12)

    def test_nine_month_flows_use_factor_four_thirds(self):
        result = ratios.R(_flow_sheet("Q3")).compute()
        # 90 * 4/3 / avg_equity(900) = 0.133333
        self.assertAlmostEqual(result["roe_total"]["value"], 0.133333, places=6)
        self.assertAlmostEqual(result["roe_total"]["inputs"]["annualization_factor"], 4 / 3)
        self.assertAlmostEqual(result["roa"]["value"], 90 * (4 / 3) / 1800, places=6)
        self.assertAlmostEqual(result["asset_turnover"]["value"], 900 * (4 / 3) / 1800, places=6)
        self.assertIn("4/3", result["roe_total"]["formula"])
        self.assertEqual(result["roe_total"]["inputs"]["period_months"], 9)

    def test_half_year_flows_use_factor_two(self):
        result = ratios.R(_flow_sheet("Q2")).compute()
        self.assertAlmostEqual(result["roe_total"]["value"], 0.2, places=6)  # 90*2/900

    def test_first_quarter_flows_use_factor_four(self):
        result = ratios.R(_flow_sheet("Q1")).compute()
        self.assertAlmostEqual(result["roe_total"]["value"], 0.4, places=6)  # 90*4/900

    def test_full_year_flows_are_not_scaled(self):
        result = ratios.R(_flow_sheet("Q4")).compute()
        self.assertAlmostEqual(result["roe_total"]["value"], 0.1, places=6)  # 90/900
        self.assertEqual(result["roe_total"]["formula"], "(net_profit)/avg_equity")

    def test_dso_uses_period_matched_annualized_revenue(self):
        result = ratios.R(_flow_sheet("Q3")).compute()
        # 150 / (900 * 4/3 / 365) = 45.625 days
        self.assertAlmostEqual(result["dso"]["value"], 45.625, places=3)
        self.assertAlmostEqual(result["dpo"]["value"], 100 / (900 * (4 / 3) / 365), places=3)

    def test_valuation_multiples_use_period_matched_annualization(self):
        result = ratios.R(_flow_sheet("Q3"), {"price": 120.0}).compute()
        # EPS 9 annualized to 12 for 9M => 12; P/E = 10
        self.assertAlmostEqual(result["pe_annualized"]["value"], 10.0, places=6)


if __name__ == "__main__":
    unittest.main()
