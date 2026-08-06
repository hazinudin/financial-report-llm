import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import write  # noqa: E402


class WriterResearchContextTests(unittest.TestCase):
    def test_load_research_context_renders_plan_findings_and_open_items(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "MHKI_Q2_2026.research_plan.yaml").write_text(
                """analytical_framing: Q2 is H1 minus Q1.
priority_themes: [Demand]
themes:
  - name: Demand
    priority: high
    rationale: Explains revenue.
questions:
  - id: A1
    theme: Demand
    question: Did industrial demand contract?
hypotheses:
  - Industrial demand weakened.
""",
                encoding="utf-8",
            )
            (root / "MHKI_Q2_2026.research.yaml").write_text(
                """findings:
  - event: Manufacturing PMI contracted.
    explanation: Lower industrial activity may reduce waste volumes.
    confidence: high
    period: in-period
    sources:
      - title: PMI release
        url: https://example.com/pmi
        published_date: '2026-07-01'
    writer_hint: A1
internal_must_explain:
  - Confirm the customer-volume effect from internal records.
meta:
  source_audit:
    passed: true
""",
                encoding="utf-8",
            )

            with patch.object(write, "RESEARCH_DIR", root):
                rendered = write._load_research_context("MHKI", 2026, "Q2")

        self.assertIn("Q2 is H1 minus Q1", rendered)
        self.assertIn("Did industrial demand contract?", rendered)
        self.assertIn("Manufacturing PMI contracted", rendered)
        self.assertIn("https://example.com/pmi", rendered)
        self.assertIn("Confirm the customer-volume effect", rendered)
        self.assertIn("passed: true", rendered)

    def test_missing_research_is_non_fatal(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            write, "RESEARCH_DIR", Path(directory),
        ):
            rendered = write._load_research_context("MHKI", 2026, "Q2")

        self.assertEqual(rendered, "(no external research available)")


if __name__ == "__main__":
    unittest.main()
