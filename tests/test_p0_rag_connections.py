"""WS1.5: split read-only search connections from the read-write indexer.

Search/fetch must never create tables or INSTALL extensions, and must survive a
concurrent ``rag.py index`` writer. DuckDB permits either one writer or many
readers, so the read path retries briefly while the indexer owns the file.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "notes_rag"))

import rag  # noqa: E402

_WRITER = (
    "import duckdb, sys, time; "
    "c = duckdb.connect(sys.argv[1]); "
    "print('ready', flush=True); "
    "time.sleep(float(sys.argv[2])); "
    "c.close()"
)


class RagConnectionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "notes.duckdb"
        self._path_patch = patch.object(rag, "DB_PATH", self.db_path)
        self._path_patch.start()
        con = rag.connect_rw()
        con.execute(
            "INSERT INTO chunks VALUES ('c1', 'a.md', 'A', 'root', '', "
            "'body', 'TST', NULL, NULL)"
        )
        con.close()

    def tearDown(self):
        self._path_patch.stop()
        self._tmp.cleanup()

    def test_index_connection_is_read_write(self):
        con = rag.connect_rw()
        con.execute("CREATE TABLE IF NOT EXISTS scratch (i INTEGER)")
        con.close()

    def test_read_only_connection_can_select_but_not_create(self):
        con = rag.connect(read_only=True)
        try:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 1,
            )
            with self.assertRaises(Exception):
                con.execute("CREATE TABLE should_fail (i INTEGER)")
        finally:
            con.close()

    def test_read_only_connect_waits_out_a_writer_process(self):
        writer = subprocess.Popen(
            [sys.executable, "-c", _WRITER, str(self.db_path), "0.7"],
            stdout=subprocess.PIPE, text=True,
        )
        try:
            self.assertEqual(writer.stdout.readline().strip(), "ready")
            started = time.monotonic()
            con = rag.connect(read_only=True, timeout_s=10)
            elapsed = time.monotonic() - started
            try:
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 1,
                )
            finally:
                con.close()
            self.assertGreaterEqual(elapsed, 0.4)
        finally:
            writer.wait(timeout=20)

    def test_three_concurrent_searches_have_no_locked_error(self):
        writer = subprocess.Popen(
            [sys.executable, "-c", _WRITER, str(self.db_path), "0.8"],
            stdout=subprocess.PIPE, text=True,
        )
        errors: list[Exception] = []
        counts: list[int] = []

        def search():
            try:
                con = rag.connect_readonly(timeout_s=10)
                try:
                    counts.append(
                        con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
                    )
                finally:
                    con.close()
            except Exception as e:  # noqa: BLE001 — surfaced by the assertion
                errors.append(e)

        try:
            self.assertEqual(writer.stdout.readline().strip(), "ready")
            threads = [threading.Thread(target=search) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=20)
        finally:
            writer.wait(timeout=20)

        self.assertEqual(errors, [])
        self.assertEqual(counts, [1, 1, 1])

    def test_read_only_open_reports_a_missing_index(self):
        missing = Path(self._tmp.name) / "missing.duckdb"
        with patch.object(rag, "DB_PATH", missing):
            with self.assertRaises(SystemExit):
                rag.connect_readonly()


if __name__ == "__main__":
    unittest.main()
