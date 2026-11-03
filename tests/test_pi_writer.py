"""Unit tests for the Stage 4 pi writer ``write.py``.

No network, no real ``pi`` execution: the process spawn is faked, the recorded
writer pre-flight stream is replayed through the parser, and ``write.run`` is
exercised against a mocked runner over a temporary on-disk input package.
"""

from __future__ import annotations

import contextlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import opencode_client  # noqa: E402
import pi_agent  # noqa: E402
import write  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
WRITER_PREFLIGHT = FIXTURES / "pi_writer_preflight.jsonl"

_DOC = (
    "---\n"
    'tags: ["#TST", "#earnings", "#Q22026"]\n'
    "ticker: TST\n"
    "---\n\n"
    "# TST — Q2 2026 Equity Analysis\n\n"
    "Body paragraph with the canonical figures.\n"
)


def _fake_result(*, error: str | None = None, content: str = _DOC) -> dict:
    return {
        "content": content,
        "tool_rounds": 1,
        "tool_trace": [],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "cost_usd": 0.002},
        "finish_reason": "stop",
        "mode": "pi_agent",
        "error": error,
        "pi_meta": {
            "provider": "opencode-go",
            "model": "glm-5.3",
            "session_id": "sess-1",
            "exit_code": 0,
            "duration_s": 1.25,
            "thinking": "off",
            "retries": 0,
            "tool_calls": {},
            "tool_errors": {},
            "exa_cost_usd": 0.0,
            "settled": True,
        },
        "raw_jsonl": "",
    }


class ResolvePiCfgTests(unittest.TestCase):
    def test_tools_default_to_readonly_set(self):
        with patch.object(
            opencode_client, "load_analysis_config",
            return_value={"models": {"write": "glm-5.3"}},
        ):
            cfg = write._resolve_pi_cfg({})
        self.assertEqual(
            cfg["tools"], ["read", "duckdb_query", "notes_search", "notes_fetch"],
        )
        self.assertEqual(cfg["provider"], "commandcode")
        self.assertEqual(cfg["model"], "glm-5.3")
        self.assertEqual(cfg["timeout_sec"], 1800)

    def test_explicit_empty_tools_survive(self):
        with patch.object(
            opencode_client, "load_analysis_config",
            return_value={"models": {"write": "glm-5.3"}},
        ):
            cfg = write._resolve_pi_cfg({"pi": {"tools": []}})
        self.assertEqual(cfg["tools"], [])

    def test_model_falls_back_to_models_write(self):
        with patch.object(
            opencode_client, "load_analysis_config",
            return_value={"models": {"write": "writer-model-x"}},
        ):
            cfg = write._resolve_pi_cfg({"pi": {"provider": "my-provider"}})
        self.assertEqual(cfg["model"], "writer-model-x")
        self.assertEqual(cfg["provider"], "my-provider")

    def test_explicit_model_is_preserved(self):
        with patch.object(
            opencode_client, "load_analysis_config",
            return_value={"models": {"write": "writer-model-x"}},
        ):
            cfg = write._resolve_pi_cfg({"pi": {"model": "explicit-model"}})
        self.assertEqual(cfg["model"], "explicit-model")


class StripOuterFenceTests(unittest.TestCase):
    def test_unwraps_a_fully_fenced_document(self):
        text = "```markdown\n# Title\n\nbody line\n```"
        self.assertEqual(
            write._strip_outer_fence(text).strip(), "# Title\n\nbody line",
        )

    def test_preserves_an_inner_fenced_block(self):
        text = "# Title\n\n```python\nx = 1\n```\n\ntail\n"
        self.assertEqual(write._strip_outer_fence(text), text)

    def test_plain_document_is_untouched(self):
        text = "# Title\n\nno fences here\n"
        self.assertEqual(write._strip_outer_fence(text), text)


class ToolsSectionTests(unittest.TestCase):
    def test_empty_section_when_no_tools(self):
        self.assertEqual(write._tools_section({"tools": []}), "")

    def test_lists_enabled_tools_with_configured_budgets(self):
        section = write._tools_section({
            "tools": ["duckdb_query", "notes_search"],
            "max_db_queries": 3,
            "max_notes_search": 2,
        })
        self.assertIn("duckdb_query", section)
        self.assertIn("notes_search", section)
        self.assertNotIn("- read:", section)
        self.assertIn("at most 3 duckdb_query, 2 notes_search calls", section)
        self.assertIn("The fact sheet and computed ratios win", section)


class RunnerRoundTripTests(unittest.TestCase):
    def test_writer_preflight_fixture(self):
        fixture_text = WRITER_PREFLIGHT.read_text(encoding="utf-8")
        captured: dict = {}

        def fake_spawn(argv, env, cwd, timeout_s):
            captured["argv"] = argv
            return 0, fixture_text, "", False

        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            with patch.object(pi_agent.shutil, "which", return_value="/fake/pi"), \
                    patch.object(pi_agent, "_spawn_pi", side_effect=fake_spawn):
                result = pi_agent.run_agent(
                    "SYSTEM PROMPT", "USER PROMPT",
                    cfg={
                        "provider": "opencode-go",
                        "model": "glm-5.3",
                        "tools": ["read", "duckdb_query", "notes_search",
                                  "notes_fetch"],
                    },
                    run_dir=run_dir,
                )

        self.assertIsNone(result["error"])
        self.assertIn("tags:", result["content"])
        self.assertIn("# ADMR — Q2 2026 Equity Analysis", result["content"])
        self.assertIn("## Related Analysis", result["content"])

        trace = result["tool_trace"]
        self.assertEqual(
            [entry["tool_name"] for entry in trace],
            ["read", "duckdb_query", "notes_search"],
        )
        self.assertEqual(
            [entry["tool_call_id"] for entry in trace],
            ["call_01_wr_read_admrq2", "call_02_wr_duckdb_ratios",
             "call_03_wr_notes_margin"],
        )
        self.assertGreaterEqual(result["tool_rounds"], 1)

        self.assertGreater(result["usage"]["prompt_tokens"], 0)
        self.assertGreater(result["usage"]["completion_tokens"], 0)
        self.assertEqual(result["finish_reason"], "stop")
        self.assertTrue(result["pi_meta"]["settled"])
        self.assertEqual(
            result["pi_meta"]["tool_calls"],
            {"read": 1, "duckdb_query": 1, "notes_search": 1},
        )

        # The allowlist reached the argv.
        argv = captured["argv"]
        self.assertEqual(
            argv[argv.index("-t") + 1],
            "read,duckdb_query,notes_search,notes_fetch",
        )


class _WriterRunTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.facts = self.root / "facts"
        self.research = self.root / "research"
        self.runs = self.root / "runs"
        self.analysis = self.root / "analysis"
        self.write_dir = self.root / "write"
        for directory in (self.facts, self.research, self.runs, self.analysis):
            directory.mkdir(parents=True)

    def tearDown(self):
        self._tmp.cleanup()

    @contextlib.contextmanager
    def _dirs(self):
        with contextlib.ExitStack() as stack:
            for name, value in (
                ("FACTS_DIR", self.facts),
                ("RESEARCH_DIR", self.research),
                ("RUNS_DIR", self.runs),
                ("ANALYSIS_DIR", self.analysis),
                ("WRITE_DIR", self.write_dir),
            ):
                stack.enter_context(patch.object(write, name, value))
            yield

    def _write_inputs(self, ticker="TST", year=2026, quarter="Q2"):
        (self.facts / f"{ticker}_{quarter}_{year}.yaml").write_text(
            "meta:\n"
            f"  ticker: {ticker}\n"
            f"  quarter: {quarter}\n"
            f"  year: {year}\n"
            "income_statement:\n"
            "  lines:\n"
            "    revenue: 100.0\n",
            encoding="utf-8",
        )
        (self.facts / f"{ticker}_{quarter}_{year}.ratios.yaml").write_text(
            "gross_margin:\n  value: 0.4\n", encoding="utf-8",
        )
        (self.runs / f"{ticker}_{year}_{quarter}.context.json").write_text(
            json.dumps({
                "ticker": ticker, "year": year, "quarter": quarter,
                "profile": "Test Co", "skeleton": "# Skeleton",
                "vault_hits": [], "market_data": {},
            }),
            encoding="utf-8",
        )

    def _fake_run_agent(self, result, captured):
        def fake(system, user, *, cfg, run_dir, project_root=None):
            run_dir = Path(run_dir)
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "system.md").write_text(system, encoding="utf-8")
            (run_dir / "user.md").write_text(user, encoding="utf-8")
            captured.update(
                system=system, user=user, cfg=cfg, run_dir=run_dir,
            )
            return result
        return fake

    def _out_path(self, ticker="TST", year=2026, quarter="Q2") -> Path:
        return self.analysis / f"{ticker} {quarter} {year} Analysis.md"

    def _trace_path(self, ticker="TST", year=2026, quarter="Q2") -> Path:
        return self.write_dir / f"{ticker}_{quarter}_{year}.write_trace.json"


class WriteRunTests(_WriterRunTestBase):
    def test_success_writes_file_and_returns_contract(self):
        self._write_inputs()
        result = _fake_result()
        captured: dict = {}
        with self._dirs(), patch.object(
            write.pi_agent, "run_agent",
            side_effect=self._fake_run_agent(result, captured),
        ):
            out = write.run("TST", 2026, "Q2")

        self.assertEqual(out["status"], "success")
        self.assertEqual(out["model"], "glm-5.3")
        self.assertEqual(out["usage"], result["usage"])
        self.assertEqual(out["pi_meta"], result["pi_meta"])
        self.assertEqual(out["tool_trace"], [])
        self.assertEqual(out["output"], str(self._out_path()))

        out_file = self._out_path()
        self.assertTrue(out_file.exists())
        self.assertIn("Equity Analysis", out_file.read_text(encoding="utf-8"))

        # Prompt files + artifacts block.
        self.assertIn("ARTIFACTS ON DISK", captured["user"])
        self.assertIn(str(self.facts / "TST_Q2_2026.yaml"), captured["user"])
        self.assertTrue((captured["run_dir"] / "user.md").exists())
        self.assertIn("## Tools (read-only", captured["system"])

    def test_runner_error_raises(self):
        self._write_inputs()
        result = _fake_result(error="pi produced no assistant text", content="")
        with self._dirs(), patch.object(
            write.pi_agent, "run_agent",
            side_effect=self._fake_run_agent(result, {}),
        ), self.assertRaises(RuntimeError) as ctx:
            write.run("TST", 2026, "Q2")
        self.assertIn("pi writer failed", str(ctx.exception))
        self.assertFalse(self._out_path().exists())

    def test_empty_document_raises(self):
        self._write_inputs()
        result = _fake_result(content="   \n  ")
        with self._dirs(), patch.object(
            write.pi_agent, "run_agent",
            side_effect=self._fake_run_agent(result, {}),
        ), self.assertRaises(RuntimeError):
            write.run("TST", 2026, "Q2")

    def test_dry_run_spawns_nothing_and_writes_no_file(self):
        self._write_inputs()
        runner = MagicMock()
        with self._dirs(), patch.object(write.pi_agent, "run_agent", runner):
            out = write.run("TST", 2026, "Q2", None, False, True)

        runner.assert_not_called()
        self.assertEqual(out["status"], "dry")
        self.assertEqual(out["model"], "deepseek/deepseek-v4.1-flash")
        self.assertGreater(out["user_chars"], 0)
        self.assertFalse(self._out_path().exists())
        self.assertFalse(self.write_dir.exists())

    def test_inject_redflags_lands_in_user_prompt(self):
        self._write_inputs()
        captured: dict = {}
        with self._dirs(), patch.object(
            write.pi_agent, "run_agent",
            side_effect=self._fake_run_agent(_fake_result(), captured),
        ):
            write.run("TST", 2026, "Q2", None, True, False, "REDFLAG-MARKER-123")

        user_md = (captured["run_dir"] / "user.md").read_text(encoding="utf-8")
        self.assertIn("REDFLAG-MARKER-123", user_md)
        self.assertIn("# RED FLAGS TO ADDRESS EXPLICITLY", user_md)

    def test_trace_written_with_budget_warnings(self):
        self._write_inputs()
        result = _fake_result()
        result["pi_meta"]["tool_calls"] = {"duckdb_query": 9, "notes_search": 5}
        with self._dirs(), patch.object(
            write.pi_agent, "run_agent",
            side_effect=self._fake_run_agent(result, {}),
        ):
            write.run("TST", 2026, "Q2")

        trace_path = self._trace_path()
        self.assertTrue(trace_path.exists())
        trace = json.loads(trace_path.read_text(encoding="utf-8"))
        self.assertEqual(trace["tool_calls"], {"duckdb_query": 9, "notes_search": 5})
        self.assertTrue(trace["budget_warnings"])
        joined = " | ".join(trace["budget_warnings"])
        self.assertIn("duckdb_query", joined)
        self.assertIn("budget 6", joined)
        self.assertEqual(trace["output"], str(self._out_path()))

    def test_existing_skip_logic_intact(self):
        self._write_inputs()
        self._out_path().write_text("already here\n", encoding="utf-8")
        runner = MagicMock()
        with self._dirs(), patch.object(write.pi_agent, "run_agent", runner):
            out = write.run("TST", 2026, "Q2")
        runner.assert_not_called()
        self.assertEqual(out["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
