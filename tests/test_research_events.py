"""Regression tests for ``research._detect_events``.

Expense lines are stored negative in the fact sheet (``cogs: -6360781``), so a
signed percentage diff reports a cost *increase* as a decrease. The event lines
feed the planner's research questions, so a flipped sign makes it chase a
nonexistent decline (TINS Q2 2026: "cogs decreased 89%" vs the real +88.5%).
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import research  # noqa: E402


class DetectEventsSignTest(unittest.TestCase):
    """Negative-stored expense lines must report direction by magnitude."""

    def _events(self, facts: dict) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            facts_dir = Path(tmp)
            (facts_dir / "TSTA_Q2_2026.yaml").write_text(
                yaml.safe_dump(facts), encoding="utf-8"
            )
            with patch.object(research, "FACTS_DIR", facts_dir):
                out = research._detect_events("TSTA", 2026, "Q2")
        return [line.lstrip("- ").strip() for line in out.splitlines()]

    def test_expense_increase_reports_increased(self):
        # H1 2026 vs H1 2025: COGS 6,360,781 vs 3,374,088 (both stored negative).
        events = self._events({
            "income_statement": {
                "lines": {"cogs": -6360781.0, "tax": -831719.0},
                "prior_lines": {"cogs": -3374088.0, "tax": -103644.0},
            }
        })
        self.assertIn("Income-statement line 'cogs' increased 89% vs prior period", events)
        self.assertIn("Income-statement line 'tax' increased 702% vs prior period", events)
        self.assertFalse([e for e in events if "decreased" in e])

    def test_asset_and_liability_directions_unchanged(self):
        events = self._events({
            "balance_sheet": {
                "lines": {"cash": 4483933.0, "loans_noncurrent": 539653.0},
                "prior_lines": {"cash": 1711598.0, "loans_noncurrent": 805255.0},
            }
        })
        self.assertIn("Balance-sheet line 'cash' increased 162% vs prior period", events)
        self.assertIn(
            "Balance-sheet line 'loans_noncurrent' decreased 33% vs prior period", events
        )

    def test_expense_decrease_still_reports_decreased(self):
        events = self._events({
            "income_statement": {
                "lines": {"selling_expense": -46125.0},
                "prior_lines": {"selling_expense": -89681.0},
            }
        })
        self.assertIn(
            "Income-statement line 'selling_expense' decreased 49% vs prior period", events
        )

    def test_no_events_when_moves_are_small(self):
        events = self._events({
            "income_statement": {
                "lines": {"cogs": -1030000.0},
                "prior_lines": {"cogs": -1000000.0},
            }
        })
        self.assertEqual(events, ["(No material events detected)"])


if __name__ == "__main__":
    unittest.main()
