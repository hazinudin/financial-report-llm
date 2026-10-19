"""Tests for the analysis-job cost/planner visibility columns.

The planner runs on the pi agent now, so its tokens and cost are recorded
alongside the research stage and surfaced in ``--status``. These tests pin the
additive schema migration and the pipeline wiring, with no network and no pi
spawn.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pipeline  # noqa: E402
import research  # noqa: E402
from db import AnalysisDB, get_db  # noqa: E402


class SchemaMigrationTests(unittest.TestCase):
    def test_columns_exist_on_a_fresh_database(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "fresh.duckdb"))
            db.init_schema()
            cols = {
                row[0] for row in
                db.conn.execute("SELECT * FROM analysis_jobs LIMIT 0").description
            }
            self.assertIn("model_plan", cols)
            self.assertIn("cost_usd", cols)
            self.assertIn("plan_fallback", cols)
            db.conn.close()

    def test_init_schema_is_idempotent_on_an_existing_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.duckdb"
            db = AnalysisDB(get_db(path))
            # Simulate a pre-migration table: create, then drop the new columns.
            db.init_schema()
            for col in ("model_plan", "cost_usd", "plan_fallback"):
                db.conn.execute(f"ALTER TABLE analysis_jobs DROP COLUMN {col}")
            db.init_schema()  # must re-add them without error
            cols = {
                row[0] for row in
                db.conn.execute("SELECT * FROM analysis_jobs LIMIT 0").description
            }
            self.assertTrue({"model_plan", "cost_usd", "plan_fallback"} <= cols)
            db.conn.close()


class CostAndPlanColumnTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = AnalysisDB(get_db(Path(self._tmp.name) / "test.duckdb"))
        self.db.init_schema()
        self.job_id = self.db.upsert_job("TEST", 2026, "Q1")

    def tearDown(self):
        self.db.conn.close()
        self._tmp.cleanup()

    def test_add_cost_accumulates(self):
        self.db.add_cost(self.job_id, 0.02)
        self.db.add_cost(self.job_id, 0.015)
        self.assertAlmostEqual(self.db.get_job(self.job_id)["cost_usd"], 0.035)

    def test_set_models_records_the_plan_model(self):
        self.db.set_models(self.job_id, plan="glm-5.3")
        job = self.db.get_job(self.job_id)
        self.assertEqual(job["model_plan"], "glm-5.3")
        self.assertIsNone(job["model_write"])

    def test_plan_fallback_flag(self):
        self.assertFalse(self.db.get_job(self.job_id)["plan_fallback"])
        self.db.set_plan_fallback(self.job_id, True)
        self.assertTrue(self.db.get_job(self.job_id)["plan_fallback"])

    def test_status_prints_cost_and_plan_columns(self):
        self.db.add_cost(self.job_id, 0.0329)
        self.db.set_plan_fallback(self.job_id, True)
        import contextlib
        import io
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.db.print_status()
        output = buffer.getvalue()
        self.assertIn("0.0329", output)
        self.assertIn("FB", output)


class ResearchStageWiringTests(unittest.TestCase):
    """``run_job`` must persist planner tokens, cost, model, and fallback."""

    def test_research_stage_records_planner_cost(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()

            def fake_research_run(*_args, **_kwargs):
                return {
                    "status": "success",
                    "mode": "pi_agent",
                    "token_usage": {"prompt_tokens": 100, "completion_tokens": 20,
                                    "cost_usd": 0.03},
                    "plan_usage": {"prompt_tokens": 10, "completion_tokens": 5,
                                   "reasoning_tokens": 7, "cost_usd": 0.02},
                    "plan_pi_meta": {"model": "glm-5.3", "tool_calls": {"read": 1}},
                    "plan_fallback": False,
                }

            with patch.object(research, "run", side_effect=fake_research_run):
                summary = pipeline.run_job(
                    "TEST", 2026, "Q1", db, force=True, stages={"research"},
                )

            self.assertEqual(summary["stages"]["research"], "success")
            job = db.get_job("TEST_2026_Q1")
            self.assertEqual(job["tokens_prompt"], 110)
            self.assertEqual(job["tokens_completion"], 25)
            self.assertAlmostEqual(job["cost_usd"], 0.05)
            self.assertEqual(job["model_plan"], "glm-5.3")
            self.assertFalse(job["plan_fallback"])
            db.conn.close()

    def test_research_stage_marks_a_planner_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            db = AnalysisDB(get_db(Path(directory) / "test.duckdb"))
            db.init_schema()

            def fake_research_run(*_args, **_kwargs):
                return {
                    "status": "success",
                    "token_usage": {"prompt_tokens": 1, "completion_tokens": 1,
                                    "cost_usd": 0.01},
                    "plan_usage": {"cost_usd": 0.0},
                    "plan_pi_meta": {},
                    "plan_fallback": True,
                }

            with patch.object(research, "run", side_effect=fake_research_run):
                pipeline.run_job(
                    "TEST", 2026, "Q1", db, force=True, stages={"research"},
                )
            self.assertTrue(db.get_job("TEST_2026_Q1")["plan_fallback"])
            db.conn.close()


if __name__ == "__main__":
    unittest.main()
