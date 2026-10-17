"""Unit tests for the headless pi research runner (``pi_research.py``).

No network, no real ``pi`` execution: the process spawn is faked and the
recorded spike fixtures are replayed through the parser.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pi_agent as pi_research  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PREFLIGHT = FIXTURES / "pi_research_preflight.jsonl"
FETCH_FALLBACK = FIXTURES / "pi_research_fetch_fallback.jsonl"

_IDX_PDF = (
    "https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/"
    "New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan/"
    "Laporan%20Keuangan%20Tahun%202026/TW2/SMGR/FinancialStatement-2026-II-SMGR.pdf"
)
_EXPECTED_KEYS = {
    "content", "tool_rounds", "tool_trace", "usage", "finish_reason",
    "mode", "error", "pi_meta", "raw_jsonl",
}
_EXPECTED_META_KEYS = {
    "provider", "model", "session_id", "exit_code", "duration_s", "thinking",
    "retries", "tool_calls", "tool_errors", "exa_cost_usd", "settled",
}


def _assistant_event(text: str, *, stop: str = "stop") -> str:
    return json.dumps({
        "type": "message_end",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": text}],
            "provider": "opencode-go",
            "model": "deepseek-v4.1-flash",
            "stopReason": stop,
            "usage": {
                "input": 10, "output": 5, "cacheRead": 0, "cacheWrite": 0,
                "totalTokens": 15, "cost": {"total": 0.0001},
            },
        },
    }, ensure_ascii=False)


class _PiResearchTestBase(unittest.TestCase):
    def _run_agent(
        self,
        run_dir: Path,
        stdout: str = "",
        *,
        cfg: dict | None = None,
        which: str | None = "/fake/pi",
        project_root: Path | None = None,
        stderr: str = "",
        exit_code: int | None = 0,
        timed_out: bool = False,
        spawn_exc: Exception | None = None,
    ) -> tuple[dict, dict]:
        captured: dict = {}

        def fake_spawn(argv, env, cwd, timeout_s):
            captured.update(argv=argv, env=env, cwd=cwd, timeout_s=timeout_s)
            if spawn_exc is not None:
                raise spawn_exc
            return exit_code, stdout, stderr, timed_out

        with patch.object(pi_research.shutil, "which", return_value=which), \
                patch.object(pi_research, "_spawn_pi", side_effect=fake_spawn):
            result = pi_research.run_agent(
                "SYSTEM PROMPT", "USER PROMPT",
                cfg=cfg or {}, run_dir=run_dir, project_root=project_root,
            )
        return result, captured


class ArgvConstructionTests(_PiResearchTestBase):
    def test_default_argv_and_prompt_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_dir = root / "run"
            result, captured = self._run_agent(
                run_dir, PREFLIGHT.read_text(encoding="utf-8"),
                project_root=root,
            )

            argv = captured["argv"]
            self.assertEqual(argv[0], "pi")
            self.assertEqual(argv[1], "-p")
            self.assertTrue(argv[2].startswith("@"))
            self.assertTrue(argv[2].endswith("user.md"))
            self.assertEqual(argv[argv.index("--mode") + 1], "json")
            self.assertIn("--no-session", argv)
            self.assertIn("--no-skills", argv)
            self.assertIn("--no-context-files", argv)
            self.assertIn("--no-mcp", argv)
            self.assertIn("--no-prompt-templates", argv)
            self.assertIn("--approve", argv)
            self.assertEqual(argv[argv.index("--provider") + 1], "opencode-go")
            self.assertEqual(argv[argv.index("--model") + 1], "deepseek-v4.1-flash")
            self.assertEqual(argv[argv.index("--thinking") + 1], "off")
            self.assertEqual(argv[argv.index("-t") + 1], "exa_search,fetch_content")
            self.assertTrue(
                argv[argv.index("--system-prompt") + 1].endswith("system.md")
            )
            self.assertEqual(captured["cwd"], root)

            # Prompt files written before spawn.
            self.assertEqual(
                (run_dir / "system.md").read_text(encoding="utf-8"), "SYSTEM PROMPT",
            )
            self.assertEqual(
                (run_dir / "user.md").read_text(encoding="utf-8"), "USER PROMPT",
            )
            self.assertEqual(result["mode"], "pi_agent")

    def test_env_has_xdg_config_home_and_credential_passthrough(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, {"EXA_API_KEY": "test-key-123"}):
                _, captured = self._run_agent(
                    root / "run", PREFLIGHT.read_text(encoding="utf-8"),
                    project_root=root,
                )
            env = captured["env"]
            self.assertTrue(
                env["XDG_CONFIG_HOME"].endswith(os.path.join(".pi", "xdg"))
            )
            self.assertEqual(env["XDG_CONFIG_HOME"], str(root / ".pi" / "xdg"))
            self.assertEqual(env["EXA_API_KEY"], "test-key-123")

    def test_argv_variants(self):
        cfg = {
            "save_session": True,
            "load_skills": True,
            "load_context_files": True,
            "approve": False,
            "tools": ["exa_search"],
            "provider": "glm",
            "model": "glm-5.3-flash",
            "thinking": "low",
        }
        argv = pi_research._build_argv(cfg, Path("/tmp/u.md"), Path("/tmp/s.md"))
        self.assertNotIn("--no-session", argv)
        self.assertNotIn("--no-skills", argv)
        self.assertNotIn("--no-context-files", argv)
        self.assertNotIn("--approve", argv)
        self.assertEqual(argv[argv.index("-t") + 1], "exa_search")
        self.assertEqual(argv[argv.index("--provider") + 1], "glm")
        self.assertEqual(argv[argv.index("--model") + 1], "glm-5.3-flash")
        self.assertEqual(argv[argv.index("--thinking") + 1], "low")

    def test_yaml_boolean_thinking_is_normalised(self):
        # PyYAML parses an unquoted `off` in config.yaml as boolean False.
        argv = pi_research._build_argv(
            {"thinking": False}, Path("/tmp/u.md"), Path("/tmp/s.md"),
        )
        self.assertEqual(argv[argv.index("--thinking") + 1], "off")
        argv = pi_research._build_argv(
            {"thinking": True}, Path("/tmp/u.md"), Path("/tmp/s.md"),
        )
        self.assertEqual(argv[argv.index("--thinking") + 1], "high")
        result = pi_research._empty_result({"thinking": False})
        self.assertEqual(result["pi_meta"]["thinking"], "off")

    def test_cfg_flag_variants_flow_through_run_agent(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            _, captured = self._run_agent(
                run_dir, PREFLIGHT.read_text(encoding="utf-8"),
                cfg={"save_session": True, "load_skills": True, "approve": False},
            )
            argv = captured["argv"]
            self.assertNotIn("--no-session", argv)
            self.assertNotIn("--no-skills", argv)
            self.assertNotIn("--approve", argv)
            self.assertIn("--no-context-files", argv)


class FixtureParsingTests(_PiResearchTestBase):
    def test_preflight_stream(self):
        fixture_text = PREFLIGHT.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            result, _ = self._run_agent(run_dir, fixture_text)

            self.assertEqual(set(result), _EXPECTED_KEYS)
            self.assertEqual(set(result["pi_meta"]), _EXPECTED_META_KEYS)
            self.assertEqual(result["mode"], "pi_agent")
            self.assertIsNone(result["error"])
            self.assertEqual(result["tool_rounds"], 2)
            self.assertIn("search_ok: true", result["content"])
            self.assertIn("fetch_ok: false", result["content"])
            self.assertEqual(result["finish_reason"], "stop")

            trace = result["tool_trace"]
            self.assertEqual(len(trace), 2)
            self.assertEqual([e["tool_name"] for e in trace],
                             ["exa_search", "fetch_content"])
            self.assertEqual(
                trace[0]["arguments"],
                {
                    "query": "PT Semen Indonesia SMGR laporan keuangan Q2 2026",
                    "numResults": 3,
                },
            )
            self.assertEqual(trace[1]["arguments"]["url"], _IDX_PDF)
            for entry in trace:
                self.assertEqual(set(entry),
                                 {"tool_call_id", "tool_name", "arguments", "result"})
                self.assertTrue(entry["tool_call_id"])
                self.assertIsInstance(entry["result"], dict)

            usage = result["usage"]
            self.assertTrue(usage["prompt_tokens"] > 0)
            self.assertTrue(usage["completion_tokens"] > 0)
            self.assertTrue(usage["cost_usd"] > 0)

            meta = result["pi_meta"]
            self.assertEqual(meta["provider"], "opencode-go")
            self.assertEqual(meta["model"], "deepseek-v4.1-flash")
            self.assertEqual(meta["tool_calls"],
                             {"exa_search": 1, "fetch_content": 1})
            self.assertEqual(meta["tool_errors"], {"fetch_content": 1})
            self.assertAlmostEqual(meta["exa_cost_usd"], 0.007, places=6)
            self.assertTrue(meta["settled"])
            self.assertTrue(meta["session_id"])
            self.assertEqual(meta["exit_code"], 0)
            self.assertEqual(meta["thinking"], "off")
            self.assertGreaterEqual(meta["duration_s"], 0)

    def test_fetch_fallback_stream(self):
        fixture_text = FETCH_FALLBACK.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(Path(directory) / "run", fixture_text)

            self.assertIsNone(result["error"])
            self.assertEqual(result["tool_rounds"], 1)
            self.assertEqual(result["finish_reason"], "stop")
            self.assertIn("fetch_ok: false", result["content"])
            self.assertEqual(len(result["tool_trace"]), 1)
            self.assertEqual(result["tool_trace"][0]["tool_name"], "fetch_content")
            self.assertEqual(result["pi_meta"]["tool_calls"], {"fetch_content": 1})
            self.assertEqual(result["pi_meta"]["tool_errors"], {})
            self.assertEqual(result["pi_meta"]["exa_cost_usd"], 0.0)
            self.assertTrue(result["pi_meta"]["settled"])

    def test_unicode_line_separator_does_not_split_record(self):
        text = "line-a\u2028line-b"
        record = _assistant_event(text)
        # Regression: the naive splitter fragments this record.
        self.assertGreater(len(record.splitlines()), 1)

        stdout = json.dumps({"type": "session", "id": "sess-1"}) + "\n" + record + "\n"
        events = pi_research._parse_jsonl(stdout)
        self.assertEqual(len(events), 2)

        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(Path(directory) / "run", stdout)
        self.assertEqual(result["content"], text)
        self.assertIsNone(result["error"])
        self.assertEqual(result["pi_meta"]["session_id"], "sess-1")


class ExtractToolUrlsTests(_PiResearchTestBase):
    def _trace_for(self, fixture: Path) -> list[dict]:
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(
                Path(directory) / "run", fixture.read_text(encoding="utf-8"),
            )
        return result["tool_trace"]

    def test_exa_and_fetch_urls_from_preflight(self):
        urls = pi_research.extract_tool_urls(self._trace_for(PREFLIGHT))
        self.assertIn(_IDX_PDF, urls)
        self.assertIn(
            "https://indopremier.com/xdir/news/LAPORAN%20KEUANGAN/2026/q2/"
            "SMGR_Q2_2026.pdf",
            urls,
        )
        self.assertIn("https://sig.id/informasi-keuangan", urls)

    def test_fetch_urls_from_fallback(self):
        urls = pi_research.extract_tool_urls(self._trace_for(FETCH_FALLBACK))
        self.assertIn(_IDX_PDF, urls)

    def test_synthetic_trace_shapes(self):
        trace = [
            {
                "tool_name": "exa_search",
                "arguments": {},
                "result": {
                    "details": {"queries": [
                        {"results": [{"url": "https://a.example/1"}]},
                    ]},
                    "content": [{"type": "text", "text": "see https://b.example/x, and "
                                                          "https://c.example/y)."}],
                },
            },
            {
                "tool_name": "fetch_content",
                "arguments": {"url": "https://d.example/arg",
                              "urls": ["https://e.example/args"]},
                "result": {
                    "details": {"urls": ["https://f.example/details"]},
                    "structuredContent": {"urls": [{"url": "https://g.example/sc"}]},
                },
            },
        ]
        urls = pi_research.extract_tool_urls(trace)
        for expected in (
            "https://a.example/1",
            "https://b.example/x",
            "https://c.example/y",
            "https://d.example/arg",
            "https://e.example/args",
            "https://f.example/details",
            "https://g.example/sc",
        ):
            self.assertIn(expected, urls)
        self.assertEqual(len(urls), len(set(urls)))


class FailureModeTests(_PiResearchTestBase):
    def test_pi_missing_from_path(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            result, captured = self._run_agent(run_dir, which=None)
            self.assertEqual(result["error"], "pi binary not found in PATH")
            self.assertEqual(result["tool_trace"], [])
            self.assertIsNone(result["pi_meta"]["exit_code"])
            self.assertEqual(captured, {})
            self.assertTrue((run_dir / "events.jsonl").exists())

    def test_file_not_found_error(self):
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(
                Path(directory) / "run", which="/fake/pi",
                spawn_exc=FileNotFoundError("no pi"),
            )
            self.assertIn("not found", result["error"].lower())
            self.assertIsNone(result["pi_meta"]["exit_code"])

    def test_timeout_error(self):
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(
                Path(directory) / "run", stdout="", stderr="",
                exit_code=-9, timed_out=True,
            )
            self.assertIn("timed out", result["error"].lower())
            self.assertEqual(result["pi_meta"]["exit_code"], -9)

    def test_spawn_pi_kills_process_group_on_timeout(self):
        class _FakeProc:
            def __init__(self):
                self.pid = 4321
                self.returncode = -9
                self.communicate_calls = 0

            def communicate(self, timeout=None):
                self.communicate_calls += 1
                if self.communicate_calls == 1:
                    raise subprocess.TimeoutExpired(cmd="pi", timeout=timeout)
                return "partial-out", ""

        fake = _FakeProc()
        with patch.object(subprocess, "Popen", return_value=fake), \
                patch.object(pi_research.os, "getpgid", return_value=4321), \
                patch.object(pi_research.os, "killpg") as killpg:
            exit_code, stdout, stderr, timed_out = pi_research._spawn_pi(
                ["pi"], {}, Path("/tmp"), 1,
            )
        self.assertTrue(timed_out)
        self.assertEqual(exit_code, -9)
        self.assertEqual(stdout, "partial-out")
        killpg.assert_called_once()

    def test_nonzero_exit_with_stderr(self):
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(
                Path(directory) / "run", stdout="", stderr="kaboom-stderr",
                exit_code=2,
            )
            self.assertIn("2", result["error"])
            self.assertIn("kaboom-stderr", result["error"])
            self.assertEqual(result["pi_meta"]["exit_code"], 2)

    def test_stream_with_no_assistant_text(self):
        event = json.dumps({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "just thinking"}],
                "provider": "opencode-go",
                "model": "deepseek-v4.1-flash",
                "stopReason": "stop",
                "usage": {"input": 1, "output": 1, "cost": {"total": 0.0}},
            },
        })
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(Path(directory) / "run", event + "\n")
            self.assertEqual(result["content"], "")
            self.assertIsNotNone(result["error"])
            self.assertIn("no assistant text", result["error"].lower())

    def test_stream_with_no_tool_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self._run_agent(
                Path(directory) / "run", _assistant_event("just a brief") + "\n",
            )
            self.assertEqual(result["tool_trace"], [])
            self.assertEqual(result["pi_meta"]["tool_calls"], {})
            self.assertEqual(result["tool_rounds"], 0)
            self.assertEqual(result["content"], "just a brief")
            self.assertIsNone(result["error"])


class EventsArtifactTests(_PiResearchTestBase):
    def test_events_jsonl_holds_raw_stdout(self):
        fixture_text = PREFLIGHT.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run"
            result, _ = self._run_agent(run_dir, fixture_text)
            self.assertEqual(
                (run_dir / "events.jsonl").read_text(encoding="utf-8"),
                fixture_text,
            )
            self.assertEqual(result["raw_jsonl"], fixture_text)


if __name__ == "__main__":
    unittest.main()
