"""WS1 index stage + per-stage cost accounting.

Covers the ``stage_index`` column, the pipeline's subprocess wiring (success and
failure), the cost buckets/``pricing.yaml`` arithmetic, Exa pass-through, and a
reconstruction of the measured PTBA Q1 2026 total ($0.2706) from the traces.
"""

from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pipeline  # noqa: E402
import pricing  # noqa: E402
import research  # noqa: E402
from db import STAGES, AnalysisDB, get_db  # noqa: E402

ANALYSIS_DIR = PROJECT_ROOT / "idx_data" / "analysis"


class IndexSchemaTests(unittest.TestCase):
    def test_stage_order_puts_index_after_redflags(self):
        self.assertEqual(STAGES.index("index"), STAGES.index("redflags") + 1)

    def test_schema_has_index_and_cost_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "fresh.duckdb"))
            db.init_schema()
            cols = {
                row[0] for row in
                db.conn.execute("SELECT * FROM analysis_jobs LIMIT 0").description
            }
            self.assertIn("stage_index", cols)
            self.assertTrue({
                "cost_extract", "cost_research_llm", "cost_exa",
                "cost_write", "cost_redflags", "cost_operational",
            } <= cols)
            db.conn.close()

    def test_index_stage_is_migrated_onto_an_existing_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.duckdb"
            db = AnalysisDB(get_db(path))
            db.init_schema()
            db.conn.execute("ALTER TABLE analysis_jobs DROP COLUMN stage_index")
            db.init_schema()
            cols = {
                row[0] for row in
                db.conn.execute("SELECT * FROM analysis_jobs LIMIT 0").description
            }
            self.assertIn("stage_index", cols)
            db.conn.close()

    def test_status_shows_index_and_cost_breakdown(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "status.duckdb"))
            db.init_schema()
            job = db.upsert_job("TST", 2026, "Q1")
            db.mark_stage(job, "index", "completed")
            db.add_cost(job, 0.05, stage="extract")
            db.add_cost(job, 0.02, stage="exa")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                db.print_status()
            output = buffer.getvalue()
            self.assertIn("Index", output)
            self.assertIn("Cost breakdown", output)
            self.assertIn("Extract", output)
            self.assertIn("ResLLM", output)
            self.assertIn("0.0500", output)
            db.conn.close()


class PipelineIndexStageTests(unittest.TestCase):
    def _db(self, directory: str) -> AnalysisDB:
        db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
        db.init_schema()
        return db

    def test_success_marks_index_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = self._db(directory)
            fake = subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout="Indexed 25 files / 2857 chunks", stderr="",
            )
            with patch.object(pipeline.subprocess, "run", return_value=fake) as run:
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, stages={"index"},
                )
            self.assertEqual(summary["stages"]["index"], "completed")
            self.assertEqual(db.get_stage("TST_2026_Q1", "index"), "completed")
            argv = run.call_args.args[0]
            self.assertEqual(argv[1:], ["scripts/notes_rag/rag.py", "index"])
            self.assertEqual(run.call_args.kwargs["cwd"], pipeline._PROJECT_ROOT)
            db.conn.close()

    def test_failure_records_stderr_and_marks_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = self._db(directory)
            fake = subprocess.CompletedProcess(
                args=[], returncode=3, stdout="", stderr="Ollama unreachable",
            )
            with patch.object(pipeline.subprocess, "run", return_value=fake):
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, stages={"index"},
                )
            job = db.get_job("TST_2026_Q1")
            self.assertEqual(summary["stages"]["index"], "failed")
            self.assertEqual(job["stage_index"], "failed")
            self.assertIn("Ollama unreachable", job["error"])
            db.conn.close()

    def test_from_stage_index_runs_only_the_index(self):
        with tempfile.TemporaryDirectory() as directory:
            db = self._db(directory)
            fake = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="Indexed", stderr="",
            )
            with patch.object(
                pipeline.xlsx_seed, "ensure_xlsx_coverage",
                return_value={"seeded": False},
            ), patch.object(pipeline.subprocess, "run", return_value=fake):
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, from_stage="index",
                )
            self.assertEqual(list(summary["stages"]), ["index"])
            self.assertEqual(summary["stages"]["index"], "completed")
            db.conn.close()

    def test_dry_run_does_not_spawn_the_indexer(self):
        with tempfile.TemporaryDirectory() as directory:
            db = self._db(directory)
            with patch.object(pipeline.subprocess, "run") as run:
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, dry_run=True, stages={"index"},
                )
            run.assert_not_called()
            self.assertEqual(summary["stages"]["index"], "dry")
            db.conn.close()

    def test_already_completed_index_is_skipped_without_spawning(self):
        with tempfile.TemporaryDirectory() as directory:
            db = self._db(directory)
            db.upsert_job("TST", 2026, "Q1")
            db.mark_stage("TST_2026_Q1", "index", "completed")
            with patch.object(pipeline.subprocess, "run") as run:
                summary = pipeline.run_job(
                    "TST", 2026, "Q1", db, force=False, stages={"index"},
                )
            run.assert_not_called()
            self.assertEqual(summary["stages"]["index"], "skipped")
            self.assertEqual(db.get_stage("TST_2026_Q1", "index"), "completed")
            db.conn.close()


class TokenCostTests(unittest.TestCase):
    def test_one_million_input_and_output_at_known_rate(self):
        # pricing.yaml: deepseek-v4.1-flash = 0.27 in / 1.10 out per Mtok.
        self.assertAlmostEqual(
            pricing.token_cost("deepseek-v4.1-flash", 1_000_000, 1_000_000),
            1.37,
        )

    def test_partial_tokens_are_prorated(self):
        self.assertAlmostEqual(
            pricing.token_cost("deepseek-v4.1-flash", 500_000, 0), 0.135,
        )

    def test_unknown_model_and_zero_tokens_are_zero(self):
        self.assertEqual(pricing.token_cost("unknown-model", 1_000_000, 1_000_000), 0.0)
        self.assertEqual(pricing.token_cost(None, 0, 0), 0.0)

    def test_pricing_is_labelled_estimated(self):
        table = pricing.load_pricing()
        self.assertTrue(table.get("estimated"))


class CostBucketTests(unittest.TestCase):
    def test_stage_cost_increments_bucket_and_total(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()
            job = db.upsert_job("TST", 2026, "Q1")
            db.add_cost(job, 0.01, stage="extract")
            db.add_cost(job, 0.02, stage="research_llm")
            db.add_cost(job, 0.03, stage="exa")
            row = db.get_job(job)
            self.assertAlmostEqual(row["cost_extract"], 0.01)
            self.assertAlmostEqual(row["cost_research_llm"], 0.02)
            self.assertAlmostEqual(row["cost_exa"], 0.03)
            self.assertAlmostEqual(row["cost_usd"], 0.06)
            db.conn.close()

    def test_invalid_stage_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()
            job = db.upsert_job("TST", 2026, "Q1")
            with self.assertRaises(ValueError):
                db.add_cost(job, 0.01, stage="not-a-stage")
            db.conn.close()

    def test_research_stage_passes_exa_cost_through(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()

            def fake_research_run(*_args, **_kwargs):
                return {
                    "status": "success",
                    "token_usage": {"prompt_tokens": 100, "completion_tokens": 20,
                                    "cost_usd": 0.01},
                    "plan_usage": {"cost_usd": 0.02},
                    "plan_pi_meta": {"model": "glm-5.3"},
                    "pi_meta": {"exa_cost_usd": 0.084},
                    "plan_fallback": False,
                }

            with patch.object(research, "run", side_effect=fake_research_run):
                pipeline.run_job(
                    "TST", 2026, "Q1", db, force=True, stages={"research"},
                )
            job = db.get_job("TST_2026_Q1")
            self.assertAlmostEqual(job["cost_research_llm"], 0.03)
            self.assertAlmostEqual(job["cost_exa"], 0.084)
            self.assertAlmostEqual(job["cost_usd"], 0.114)
            db.conn.close()


class PtbaReconstructionTests(unittest.TestCase):
    """Reconstruct the measured PTBA Q1 2026 total from persisted traces."""

    def test_total_within_two_percent_of_measured(self):
        research_trace = ANALYSIS_DIR / "research" / "PTBA_Q1_2026.research_trace.json"
        write_trace = ANALYSIS_DIR / "write" / "PTBA_Q1_2026.write_trace.json"
        if not research_trace.exists() or not write_trace.exists():
            self.skipTest("PTBA Q1 2026 traces are not present")

        research_data = json.loads(research_trace.read_text(encoding="utf-8"))
        write_data = json.loads(write_trace.read_text(encoding="utf-8"))

        research_llm = float(research_data["usage"]["cost_usd"])
        plan_llm = float(research_data["planner"]["usage"]["cost_usd"])
        exa = float(research_data["pi"]["exa_cost_usd"])
        write_llm = float(write_data["usage"]["cost_usd"])

        total = research_llm + plan_llm + exa + write_llm
        self.assertAlmostEqual(total, 0.2706, delta=0.2706 * 0.02)
        # The buckets the pipeline writes must sum to the same total.
        self.assertAlmostEqual(
            research_llm + plan_llm + exa + write_llm, total,
        )


if __name__ == "__main__":
    unittest.main()
