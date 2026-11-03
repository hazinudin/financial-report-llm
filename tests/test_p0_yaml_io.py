"""Tests for the fail-loud YAML loader used by the P0 pipeline stages.

The old code did ``except: {}`` in research, merge, red-flags, and the planner,
so a corrupt artifact became an empty-but-successful stage. These tests pin the
single conservative repair pass and the exception that replaces the fallback.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import yaml_io  # noqa: E402


class LoadsWithRepairTests(unittest.TestCase):
    def test_plain_yaml(self):
        self.assertEqual(
            yaml_io.loads_with_repair("findings:\n  - event: a\n"),
            {"findings": [{"event": "a"}]},
        )

    def test_fenced_yaml(self):
        text = "```yaml\nfindings:\n  - event: a\n```"
        self.assertEqual(
            yaml_io.loads_with_repair(text),
            {"findings": [{"event": "a"}]},
        )

    def test_trailing_commas_are_repaired(self):
        text = "findings: [{event: a,}, {event: b,},]\n"
        self.assertEqual(
            yaml_io.loads_with_repair(text),
            {"findings": [{"event": "a"}, {"event": "b"}]},
        )

    def test_quoted_value_with_a_stray_line_end_comma(self):
        text = 'findings:\n  - event: "a"\n    explanation: "text, with comma",\n'
        self.assertEqual(
            yaml_io.loads_with_repair(text),
            {"findings": [{"event": "a", "explanation": "text, with comma"}]},
        )

    def test_unbalanced_flow_collections_are_closed(self):
        self.assertEqual(
            yaml_io.loads_with_repair("findings: [{event: a}\n"),
            {"findings": [{"event": "a"}]},
        )

    def test_does_not_discard_quoted_brackets(self):
        parsed = yaml_io.loads_with_repair('note: "a ] brace, inside"\n')
        self.assertEqual(parsed, {"note": "a ] brace, inside"})

    def test_corrupt_yaml_raises_with_context_and_parser_message(self):
        with self.assertRaises(yaml_io.YamlLoadError) as ctx:
            yaml_io.loads_with_repair(
                'findings: "unterminated\nother: [1, 2\n',
                context="research brief PTBA Q1 2026",
            )
        error = ctx.exception
        self.assertIn("research brief PTBA Q1 2026", str(error))
        self.assertIn("research brief PTBA Q1 2026", error.context)
        self.assertTrue(error.parser_message)
        self.assertIsInstance(error, ValueError)

    def test_empty_document_is_none_not_an_empty_mapping(self):
        self.assertIsNone(yaml_io.loads_with_repair("   \n"))


class LoadFileTests(unittest.TestCase):
    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "brief.yaml"
            path.write_text("findings: []\n", encoding="utf-8")
            self.assertEqual(yaml_io.load_file_with_repair(path), {"findings": []})

    def test_context_defaults_to_the_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broken.yaml"
            path.write_text("a: [1\nb: \"oops\n", encoding="utf-8")
            with self.assertRaises(yaml_io.YamlLoadError) as ctx:
                yaml_io.load_file_with_repair(path)
            self.assertIn(str(path), str(ctx.exception))


class PtbaBriefTests(unittest.TestCase):
    """The Q2 2026 raw response is the real-world reason this repair exists."""

    def test_ptba_q2_raw_response_now_parses(self):
        raw_path = (
            PROJECT_ROOT / "idx_data" / "analysis" / "research"
            / "PTBA_Q2_2026.research_raw.md"
        )
        if not raw_path.exists():
            self.skipTest("PTBA Q2 raw response is not present")
        text = raw_path.read_text(encoding="utf-8")
        start = text.find("```")
        end = text.rfind("```")
        if start < 0 or end <= start:
            self.skipTest("raw response is not fenced")
        parsed = yaml_io.loads_with_repair(
            text[start + 3:end], context="research brief PTBA Q2 2026",
        )
        self.assertIsInstance(parsed, dict)
        self.assertTrue(parsed.get("findings"))


if __name__ == "__main__":
    unittest.main()
