"""Regression test: the pipeline releases its DuckDB write lock around pi stages.

Stage 4's ``duckdb_query`` tool opens ``pipeline.duckdb`` read-only from a
spawned process. DuckDB refuses that while this process holds the read-write
connection ("Conflicting lock is held"), so ``pipeline._db_lock_released`` closes
the connection for the duration of the stage and reopens it afterwards.

Observed live on the 2026-10-16 ADMR pipeline run: the writer's peer query
failed with the lock error and the analysis silently lost its peer comparison.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pipeline  # noqa: E402
from db import AnalysisDB, get_db  # noqa: E402

_PROBE = "import duckdb, sys; duckdb.connect(sys.argv[1], read_only=True).close()"


def _read_only_probe(db_path: Path) -> subprocess.CompletedProcess:
    """Open ``db_path`` read-only in a *separate process* (like the tool does)."""
    return subprocess.run(
        [sys.executable, "-c", _PROBE, str(db_path)],
        capture_output=True, text=True, timeout=60,
    )


class DbLockReleaseTests(unittest.TestCase):
    def test_another_process_can_open_read_only_only_while_released(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "test.duckdb"
            db = AnalysisDB(get_db(db_path))
            db.init_schema()
            original = db.conn

            # While the write lock is held, the tool's read-only open fails.
            # (Pins DuckDB's current lock semantics; revisit if DuckDB ever
            # allows concurrent read-only opens.)
            held = _read_only_probe(db_path)
            self.assertNotEqual(held.returncode, 0)

            with pipeline._db_lock_released(db, db_path):
                released = _read_only_probe(db_path)
                self.assertEqual(released.returncode, 0, released.stderr)

            # The connection is a fresh, usable one afterwards.
            self.assertIsNot(db.conn, original)
            self.assertEqual(
                db.conn.execute("SELECT COUNT(*) FROM analysis_jobs").fetchone()[0], 0,
            )
            db.conn.close()

    def test_connection_is_reopened_even_when_the_stage_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "test.duckdb"
            db = AnalysisDB(get_db(db_path))
            db.init_schema()
            original = db.conn

            with self.assertRaises(RuntimeError):
                with pipeline._db_lock_released(db, db_path):
                    raise RuntimeError("stage blew up")

            self.assertIsNot(db.conn, original)
            self.assertEqual(
                db.conn.execute("SELECT COUNT(*) FROM analysis_jobs").fetchone()[0], 0,
            )
            db.conn.close()

    def test_helper_reopens_the_connection_it_was_given(self):
        # No explicit path: the target is discovered from the connection itself,
        # so a temp/non-default database is reopened correctly.
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "other.duckdb"
            db = AnalysisDB(get_db(db_path))
            db.init_schema()

            with pipeline._db_lock_released(db):
                self.assertEqual(_read_only_probe(db_path).returncode, 0)

            self.assertEqual(
                Path(db.conn.execute("PRAGMA database_list").fetchall()[0][2]).resolve(),
                db_path.resolve(),
            )
            db.conn.close()


class PipelineWriteStageLockTests(unittest.TestCase):
    """The real ``run_job`` write stage must release the lock (integration)."""

    def test_write_stage_releases_the_lock_for_the_agent(self):
        import write as write_mod  # noqa: PLC0415

        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "test.duckdb"
            db = AnalysisDB(get_db(db_path))
            db.init_schema()
            observed: dict = {}

            def fake_write_run(*_args, **_kwargs):
                # Mirrors the duckdb_query tool: a separate process opening the
                # same file read-only while the pipeline drives the stage.
                observed["probe"] = _read_only_probe(db_path)
                return {
                    "status": "success",
                    "output": str(Path(directory) / "out.md"),
                    "model": "test-model",
                    "usage": {"prompt_tokens": 3, "completion_tokens": 4},
                }

            with patch.object(write_mod, "run", side_effect=fake_write_run):
                summary = pipeline.run_job(
                    "TEST", 2026, "Q2", db, force=True, stages={"write"},
                )

            self.assertEqual(summary["stages"]["write"], "success")
            self.assertIn("probe", observed, "write.run was not called")
            self.assertEqual(observed["probe"].returncode, 0, observed["probe"].stderr)
            db.conn.close()


if __name__ == "__main__":
    unittest.main()
