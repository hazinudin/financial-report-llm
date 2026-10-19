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


class PlannerPiCfgTests(unittest.TestCase):
    def test_defaults_to_readonly_tools_and_models_fallback(self):
        cfg = research_plan._resolve_planner_pi_cfg()
        self.assertEqual(
            cfg["tools"], ["read", "duckdb_query", "notes_search", "notes_fetch"],
        )
        self.assertEqual(cfg["provider"], "opencode-go")
        self.assertEqual(cfg["model"], "glm-5.3")
        self.assertEqual(cfg["timeout_sec"], 600)
        self.assertEqual(cfg["max_db_queries"], 4)

    def test_explicit_empty_tools_survive(self):
        with patch.object(
            research_plan, "_planner_config", return_value={"pi": {"tools": []}},
        ):
            cfg = research_plan._resolve_planner_pi_cfg()
        self.assertEqual(cfg["tools"], [])

    def test_missing_model_yields_no_model(self):
        with patch.object(
            research_plan, "_planner_config", return_value={"pi": {"provider": "p"}},
        ), patch.object(
            research_plan.opencode_client, "load_analysis_config",
            return_value={"models": {}},
        ):
            cfg = research_plan._resolve_planner_pi_cfg()
        self.assertIsNone(cfg.get("model"))


class PlannerToolsSectionTests(unittest.TestCase):
    def test_empty_when_no_tools(self):
        self.assertEqual(research_plan._planner_tools_section({"tools": []}), "")

    def test_lists_enabled_tools_and_budgets(self):
        section = research_plan._planner_tools_section({
            "tools": ["duckdb_query", "notes_search"],
            "max_db_queries": 2,
            "max_notes_search": 1,
        })
        self.assertIn("duckdb_query", section)
        self.assertIn("notes_search", section)
        self.assertNotIn("- read:", section)
        self.assertIn("at most 2 duckdb_query, 1 notes_search calls", section)
        self.assertIn("canonical fact sheet and computed ratios win", section)


class FormulatePlanTests(unittest.TestCase):
    _PLAN_YAML = (
        "analytical_framing: |\n"
        "  test framing\n"
        "themes:\n"
        '  - name: "Theme A"\n'
        "    priority: high\n"
        "    rationale: |\n"
        "      why it matters\n"
        "    questions:\n"
        '      - id: "A1"\n'
        '        question: "What drove the move?"\n'
        '        rationale: "anchors the driver"\n'
        '        search_query: "ITMG 2026 Q1 test"\n'
        "hypotheses:\n"
        '  - "h1"\n'
        "priority_themes:\n"
        '  - "Theme A"\n'
    )

    def _result(self, content: str, **overrides) -> dict:
        result = {
            "content": content,
            "tool_rounds": 1,
            "tool_trace": [],
            "usage": {
                "prompt_tokens": 100, "completion_tokens": 50,
                "reasoning_tokens": 7, "cost_usd": 0.01,
            },
            "finish_reason": "stop",
            "mode": "pi_agent",
            "error": None,
            "pi_meta": {
                "provider": "opencode-go", "model": "glm-5.3",
                "session_id": "sess-plan", "exit_code": 0, "duration_s": 12.0,
                "thinking": "off", "retries": 0,
                "tool_calls": {"read": 1}, "tool_errors": {},
                "exa_cost_usd": 0.0, "settled": True,
            },
        }
        result.update(overrides)
        return result

    def _run(self, research_dir: Path, result: dict):
        """Patch the runner; the fake mirrors run_agent's prompt-file contract."""
        captured: dict = {}

        def fake_run_agent(system, user, *, cfg, run_dir, project_root=None):
            captured["cfg"] = cfg
            captured["system"] = system
            captured["user"] = user
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "system.md").write_text(system, encoding="utf-8")
            (run_dir / "user.md").write_text(user, encoding="utf-8")
            captured["run_dir"] = run_dir
            return result

        with patch.object(research_plan, "RESEARCH_DIR", research_dir), \
                patch.object(
                    research_plan.pi_agent, "run_agent", side_effect=fake_run_agent,
                ):
            plan = research_plan.formulate_plan(
                "ITMG", 2026, "Q1", "- gross margin increased from prior period",
            )
        return plan, captured

    def _research_dir(self) -> Path:
        return Path(self.enterContext(tempfile.TemporaryDirectory())) / "research"

    def test_success_writes_trace_and_raw_and_attaches_usage(self):
        plan, captured = self._run(
            self._research_dir(), self._result(self._PLAN_YAML),
        )
        self.assertFalse(plan.fallback)
        self.assertIsNone(plan.error)
        self.assertEqual(len(plan.questions), 1)
        self.assertEqual(plan.usage["reasoning_tokens"], 7)
        self.assertEqual(plan.pi_meta["session_id"], "sess-plan")
        self.assertEqual(captured["cfg"]["tools"],
                         ["read", "duckdb_query", "notes_search", "notes_fetch"])
        self.assertIn("ARTIFACTS ON DISK", captured["user"])
        self.assertIn("Tools (read-only", captured["system"])

        trace_path = Path(plan.plan_trace_path)
        self.assertTrue(trace_path.exists())
        trace = json.loads(trace_path.read_text(encoding="utf-8"))
        self.assertEqual(trace["usage"]["reasoning_tokens"], 7)
        self.assertEqual(trace["tool_calls"], {"read": 1})
        self.assertEqual(trace["model"], "glm-5.3")
        self.assertEqual(trace["budget_warnings"], [])

        plan_dir = captured["run_dir"]
        self.assertEqual(plan_dir.name, "ITMG_Q1_2026.plan_pi")
        self.assertTrue((plan_dir / "system.md").exists())
        raw = trace_path.parent / "ITMG_Q1_2026.research_plan_raw.md"
        self.assertTrue(raw.exists())
        self.assertIn("reasoning_tokens: 7", raw.read_text(encoding="utf-8"))

    def test_runner_error_falls_back_but_still_writes_artifacts(self):
        plan, _ = self._run(
            self._research_dir(),
            self._result("", error="pi timed out after 600s", finish_reason=None),
        )
        self.assertTrue(plan.fallback)
        self.assertIn("pi timed out after 600s", plan.error)
        trace_path = Path(plan.plan_trace_path)
        self.assertTrue(trace_path.exists())
        self.assertEqual(
            json.loads(trace_path.read_text(encoding="utf-8"))["error"],
            "pi timed out after 600s",
        )
        raw = trace_path.parent / "ITMG_Q1_2026.research_plan_raw.md"
        self.assertTrue(raw.exists())
        self.assertIn("pi timed out after 600s", raw.read_text(encoding="utf-8"))

    def test_truncation_reason_mentions_reasoning_tokens(self):
        plan, _ = self._run(
            self._research_dir(),
            self._result("", finish_reason="length",
                         usage={"prompt_tokens": 10, "completion_tokens": 4096,
                                "reasoning_tokens": 4096, "cost_usd": 0.02}),
        )
        self.assertTrue(plan.fallback)
        self.assertIn("truncated at max_tokens", plan.error)
        self.assertIn("4096 reasoning tokens", plan.error)

    def test_budget_warning_recorded_in_trace(self):
        result = self._result(self._PLAN_YAML)
        result["pi_meta"]["tool_calls"] = {"duckdb_query": 9}
        plan, _ = self._run(self._research_dir(), result)
        trace = json.loads(Path(plan.plan_trace_path).read_text(encoding="utf-8"))
        self.assertEqual(len(trace["budget_warnings"]), 1)
        self.assertIn("duckdb_query calls 9 exceeded budget 4",
                      trace["budget_warnings"][0])

    def test_unparseable_output_falls_back(self):
        plan, _ = self._run(
            self._research_dir(), self._result("not: [valid: yaml"),
        )
        self.assertTrue(plan.fallback)
        self.assertTrue(plan.error)


if __name__ == "__main__":
    unittest.main()
