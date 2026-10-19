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


if __name__ == "__main__":
    unittest.main()
