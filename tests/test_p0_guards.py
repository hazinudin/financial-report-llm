"""P0 empty-output guards: an empty stage must fail, never report ``completed``.

Covers research (triggers fired but no findings / no content), red-flags
(<40 chars), merge (no statements and no details), and context (empty package).
Also asserts the pipeline turns those exceptions into ``stage_* = failed``.
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

import context  # noqa: E402
import merge  # noqa: E402
import pipeline  # noqa: E402
import redflags  # noqa: E402
import research  # noqa: E402
import research_plan  # noqa: E402
import research_questions  # noqa: E402
import yaml_io  # noqa: E402
from db import AnalysisDB, get_db  # noqa: E402


def _identity():
    return research_questions.resolve_identity("ITMG")


def _fake_plan():
    return research_plan._fallback_plan(
        "TST", 2026, "Q1", _identity(), [], reason="test",
    )


def _agent_result(content: str) -> dict:
    return {
        "content": content,
        "tool_rounds": 1,
        "tool_trace": [{"tool_name": "exa_search", "arguments": {}, "result": {}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "cost_usd": 0.001},
        "finish_reason": "stop",
        "mode": "pi_agent",
        "error": None,
        "pi_meta": {
            "tool_calls": {"exa_search": 1},
            "exa_cost_usd": 0.0,
        },
    }


class ResearchEmptyGuardTests(unittest.TestCase):
    _CFG = {"pi": {"model": "deepseek-v4.1-flash"}}

    def _run(self, content: str, events: str = "- margin increased 20%"):
        with patch.object(research, "_research_config", return_value=self._CFG), \
                patch.object(
                    research, "_prepare_plan",
                    return_value=(
                        _fake_plan(), "facts", events, _identity(), [],
                    ),
                ), \
                patch.object(
                    research.pi_agent, "run_agent",
                    return_value=_agent_result(content),
                ):
            return research._run_pi_agent("TST", 2026, "Q1", "pi")

    def test_no_content_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run("   \n")
        self.assertIn("no content", str(ctx.exception))

    def test_corrupt_output_raises_yaml_error(self):
        with self.assertRaises(yaml_io.YamlLoadError):
            self._run('findings: "unterminated\nother: [1, 2\n')

    def test_triggers_fired_with_empty_findings_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run("findings: []\ninternal_must_explain: []\n")
        self.assertIn("triggers fired", str(ctx.exception))

    def test_no_triggers_with_empty_findings_is_allowed(self):
        result = self._run(
            "findings: []\ninternal_must_explain: []\n",
            events="(No material events detected)",
        )
        self.assertEqual(result["brief"]["findings"], [])

    def test_corrupt_existing_brief_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "TST_Q1_2026.research.yaml").write_text(
                'findings: "unterminated\n', encoding="utf-8",
            )
            with patch.object(research, "RESEARCH_DIR", root):
                with self.assertRaises(yaml_io.YamlLoadError):
                    research.run("TST", 2026, "Q1", force=False)


class RedflagsEmptyGuardTests(unittest.TestCase):
    def _run(self, content: str):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            facts = root / "facts"
            runs = root / "runs"
            research_dir = root / "research"
            for d in (facts, runs, research_dir):
                d.mkdir()
            (facts / "TST_Q1_2026.yaml").write_text(
                yaml.safe_dump({
                    "meta": {"ticker": "TST", "quarter": "Q1", "year": 2026},
                    "income_statement": {"lines": {"revenue": 100.0}},
                }),
                encoding="utf-8",
            )
            with patch.object(redflags, "FACTS_DIR", facts), \
                    patch.object(redflags, "RUNS_DIR", runs), \
                    patch.object(redflags, "RESEARCH_DIR", research_dir), \
                    patch.object(
                        redflags.opencode_client, "parse_model_settings",
                        return_value={"model": "test-model", "temperature": 0.0,
                                      "max_tokens": 100,
                                      "reasoning_effort": None},
                    ), \
                    patch.object(
                        redflags.opencode_client, "chat",
                        return_value={"content": content,
                                      "usage": {"prompt_tokens": 1,
                                                "completion_tokens": 1}},
                    ):
                return redflags.run("TST", 2026, "Q1")

    def test_short_output_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run("  ok  ")
        self.assertIn("too-short", str(ctx.exception))

    def test_substantive_output_passes(self):
        out = self._run(
            "- §4.2 receivables rose 216% while §5.1 collections are 59.6% "
            "of revenue.\n"
        )
        self.assertEqual(out["status"], "success")
        self.assertEqual(out["model"], "test-model")


class MergeEmptyGuardTests(unittest.TestCase):
    def _run(self, chunk_yaml: str):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            facts = root / "facts"
            runs = root / "runs"
            facts.mkdir()
            runs.mkdir()
            out = facts / "TST_Q1_2026.yaml"
            (facts / "TST_Q1_2026_chunk01.yaml").write_text(
                chunk_yaml, encoding="utf-8",
            )
            with patch.object(merge, "FACTS_DIR", facts), \
                    patch.object(merge, "RUNS_DIR", runs), \
                    patch.object(merge.xlsx_seed, "seed_from_xlsx", return_value=None), \
                    patch.object(merge, "xlsx_cross_validate", lambda *a, **k: None):
                result = merge.run("TST", 2026, "Q1", force=True)
            return result, out.read_text(encoding="utf-8") if out.exists() else None

    def test_no_statements_and_no_details_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run(
                "meta:\n  ticker: TST\n  quarter: Q1\n  year: 2026\n"
                "income_statement:\n  lines: {}\n"
                "balance_sheet:\n  lines: {}\n"
                "cash_flow:\n  lines: {}\n"
                "details: {}\n"
            )
        self.assertIn("no statement values", str(ctx.exception))

    def test_statement_values_pass(self):
        result, content = self._run(
            "meta:\n  ticker: TST\n  quarter: Q1\n  year: 2026\n"
            "income_statement:\n  lines:\n    revenue: 100.0\n"
        )
        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(content)


class ContextEmptyGuardTests(unittest.TestCase):
    def test_empty_package_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory) / "runs"
            runs.mkdir()
            with patch.object(context, "RUNS_DIR", runs), \
                    patch.object(
                        context, "get_or_create_profile", return_value=("", False),
                    ), \
                    patch.object(context, "load_skeleton", return_value=""), \
                    patch.object(context, "vault_search", return_value=[]), \
                    patch.object(context, "deduce_commodity", return_value=None), \
                    patch.object(context, "business_terms", return_value=[]):
                with self.assertRaises(RuntimeError) as ctx:
                    context.run("TST", 2026, "Q1")
            self.assertIn("context package is empty", str(ctx.exception))


class PipelineStageFailureTests(unittest.TestCase):
    def test_research_yaml_failure_marks_the_stage_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()
            with patch.object(
                research, "run",
                side_effect=yaml_io.YamlLoadError("research brief", "boom"),
            ):
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, stages={"research"},
                )
            self.assertEqual(summary["stages"]["research"], "failed")
            self.assertEqual(db.get_stage("TST_2026_Q1", "research"), "failed")
            db.conn.close()

    def test_context_empty_failure_marks_the_stage_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()
            with patch.object(
                context, "run", side_effect=RuntimeError("context package is empty"),
            ):
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, stages={"context"},
                )
            self.assertEqual(summary["stages"]["context"], "failed")
            self.assertEqual(db.get_stage("TST_2026_Q1", "context"), "failed")
            db.conn.close()


if __name__ == "__main__":
    unittest.main()
