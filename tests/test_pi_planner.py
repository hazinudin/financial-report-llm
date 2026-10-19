"""Unit tests for the Stage 3b research planner running on the pi agent.

No network, no real ``pi`` execution: the recorded planner streams are replayed
through ``pi_agent._build_result`` and ``research_plan.parse_plan``, so the
transport, usage accounting, and YAML contract are validated without spawning a
process.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pi_agent  # noqa: E402
import research_questions  # noqa: E402
import research_plan  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PLANNER_PREFLIGHT = FIXTURES / "pi_planner_preflight.jsonl"
PLANNER_TOOLS_PREFLIGHT = FIXTURES / "pi_planner_tools_preflight.jsonl"

_CFG = {"provider": "opencode-go", "model": "glm-5.3", "thinking": "off"}


def _build(fixture: Path, *, duration_s: float = 0.0) -> dict:
    stdout = fixture.read_text(encoding="utf-8")
    events = pi_agent._parse_jsonl(stdout)
    return pi_agent._build_result(
        events,
        dict(_CFG),
        exit_code=0,
        duration_s=duration_s,
        stderr="",
        timed_out=False,
        timeout_s=600,
    )


def _parse(content: str, quarter: str = "Q1"):
    return research_plan.parse_plan(
        content, "ITMG", 2026, quarter, research_questions.resolve_identity("ITMG"),
    )


class UsageAccountingTests(unittest.TestCase):
    def test_reasoning_tokens_are_summed_across_messages(self):
        usage = {"input": 10, "output": 5, "reasoning": 7, "cost": {"total": 0.001}}
        messages = [
            {"type": "message_end", "message": {"role": "assistant", "usage": usage, "content": []}},
            {"type": "message_end", "message": {"role": "assistant", "usage": usage, "content": []}},
        ]
        result = pi_agent._build_result(
            messages, dict(_CFG), exit_code=0, duration_s=0.0,
            stderr="", timed_out=False, timeout_s=600,
        )
        self.assertEqual(result["usage"]["reasoning_tokens"], 14)
        self.assertEqual(result["usage"]["prompt_tokens"], 20)

    def test_empty_result_carries_reasoning_tokens(self):
        self.assertEqual(pi_agent._empty_result({})["usage"]["reasoning_tokens"], 0)


class HarnessOnlyFixtureTests(unittest.TestCase):
    """The ``tools: []`` capture validates the parser and usage accounting."""

    def test_fixture_replays(self):
        result = _build(PLANNER_PREFLIGHT, duration_s=44.1)
        self.assertIsNone(result["error"])
        self.assertEqual(result["finish_reason"], "stop")
        self.assertEqual(result["tool_rounds"], 0)
        self.assertEqual(result["tool_trace"], [])
        self.assertTrue(result["pi_meta"]["settled"])
        self.assertEqual(result["pi_meta"]["model"], "glm-5.3")
        self.assertTrue(result["pi_meta"]["session_id"])
        self.assertAlmostEqual(result["usage"]["cost_usd"], 0.02164534, places=8)
        self.assertEqual(result["usage"]["reasoning_tokens"], 202)
        self.assertEqual(result["usage"]["prompt_tokens"], 5751)
        self.assertGreater(len(result["content"]), 5000)

    def test_fixture_content_parses_into_a_plan(self):
        plan = _parse(_build(PLANNER_PREFLIGHT)["content"])
        self.assertIsNone(plan.error)
        self.assertFalse(plan.fallback)
        self.assertGreaterEqual(len(plan.themes), 3)
        self.assertGreaterEqual(len(plan.questions), 8)
        self.assertTrue(plan.hypotheses)
        self.assertTrue(plan.analytical_framing)
        for q in plan.questions:
            self.assertRegex(q.id, r"^[A-Z]\d+$")
            self.assertTrue(q.theme)


class ToolsFixtureTests(unittest.TestCase):
    """The read-only-allowlist capture validates tool correlation."""

    def test_fixture_replays_with_tool_trace(self):
        result = _build(PLANNER_TOOLS_PREFLIGHT, duration_s=59.4)
        self.assertIsNone(result["error"])
        self.assertEqual(result["finish_reason"], "stop")
        self.assertEqual(result["tool_rounds"], 2)
        self.assertEqual(
            result["pi_meta"]["tool_calls"], {"read": 2, "notes_search": 2},
        )
        self.assertEqual(result["pi_meta"]["tool_errors"], {})
        self.assertEqual(result["usage"]["reasoning_tokens"], 895)

        names = [entry["tool_name"] for entry in result["tool_trace"]]
        self.assertEqual(names, ["read", "notes_search", "read", "notes_search"])
        for entry in result["tool_trace"]:
            self.assertEqual(
                set(entry), {"tool_call_id", "tool_name", "arguments", "result"},
            )
            self.assertTrue(entry["tool_call_id"])
            self.assertIsInstance(entry["result"], dict)

    def test_no_web_tools_were_called(self):
        result = _build(PLANNER_TOOLS_PREFLIGHT)
        called = set(result["pi_meta"]["tool_calls"])
        self.assertNotIn("exa_search", called)
        self.assertNotIn("fetch_content", called)

    def test_fixture_content_parses_into_a_plan(self):
        plan = _parse(_build(PLANNER_TOOLS_PREFLIGHT)["content"])
        self.assertIsNone(plan.error)
        self.assertFalse(plan.fallback)
        self.assertGreaterEqual(len(plan.themes), 3)
        self.assertGreaterEqual(len(plan.questions), 8)


class PlannerArgvTests(unittest.TestCase):
    """The runner emits the read-only allowlist, not ``--no-tools``."""

    def _argv_for(self, cfg: dict) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            captured: dict = {}

            def fake_spawn(argv, env, cwd, timeout_s):
                captured["argv"] = argv
                return 0, PLANNER_PREFLIGHT.read_text(encoding="utf-8"), "", False

            with patch.object(pi_agent.shutil, "which", return_value="/fake/pi"), \
                    patch.object(pi_agent, "_spawn_pi", side_effect=fake_spawn):
                pi_agent.run_agent("SYS", "USER", cfg=cfg, run_dir=run_dir)
            return captured["argv"]

    def test_read_only_allowlist(self):
        argv = self._argv_for({**_CFG, "tools": [
            "read", "duckdb_query", "notes_search", "notes_fetch",
        ]})
        self.assertIn("-t", argv)
        self.assertEqual(
            argv[argv.index("-t") + 1],
            "read,duckdb_query,notes_search,notes_fetch",
        )
        self.assertNotIn("--no-tools", argv)

    def test_empty_tools_downgrades_to_no_tools(self):
        argv = self._argv_for({**_CFG, "tools": []})
        self.assertIn("--no-tools", argv)
        self.assertNotIn("-t", argv)


if __name__ == "__main__":
    unittest.main()
