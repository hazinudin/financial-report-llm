"""Unit tests for the shared headless pi runner (``pi_agent.py``).

Covers only the new argv tools branch: an explicit empty ``tools`` list must
emit ``--no-tools``, while a non-empty list keeps the ``-t`` behaviour. No
network, no real ``pi`` execution.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

from pi_agent import _build_argv  # noqa: E402


class BuildArgvToolsTests(unittest.TestCase):
    def test_empty_tools_uses_no_tools_flag(self):
        argv = _build_argv({"tools": []}, Path("/tmp/u.md"), Path("/tmp/s.md"))
        self.assertIn("--no-tools", argv)
        self.assertNotIn("-t", argv)

    def test_non_empty_tools_uses_t_flag(self):
        cfg = {"tools": ["read", "duckdb_query"]}
        argv = _build_argv(cfg, Path("/tmp/u.md"), Path("/tmp/s.md"))
        self.assertNotIn("--no-tools", argv)
        self.assertEqual(argv[argv.index("-t") + 1], "read,duckdb_query")


if __name__ == "__main__":
    unittest.main()
