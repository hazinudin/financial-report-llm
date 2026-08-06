"""
DuckDB state layer for the Analysis Pipeline.

Extends the shared idx_data/pipeline.duckdb with an `analysis_jobs` table that
tracks the per-report multi-stage pipeline (preprocess -> extract -> merge ->
context -> write -> redflags) plus token accounting. Mirrors the
PipelineDB pattern in scripts/report_pipeline/db.py but stays self-contained so
the analysis package does not depend on the download pipeline's internals.

Usage:
    from db import get_db, AnalysisDB
    with get_db() as conn:
        AnalysisDB(conn).init_schema().upsert_job("DKFT", 2026, "Q1")
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
IDX_DATA_DIR = _PROJECT_ROOT / "idx_data"
DEFAULT_DB_PATH = IDX_DATA_DIR / "pipeline.duckdb"

STAGES = ("preprocess", "extract", "merge", "context", "research", "write", "redflags")


def get_db(db_path: str | Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=read_only)
    if not read_only:
        conn.execute("PRAGMA wal_autocheckpoint='32MB'")
    return conn


class AnalysisDB:
    """State store for analysis jobs, layered on top of pipeline.duckdb."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def init_schema(self) -> "AnalysisDB":
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_jobs (
                job_id            VARCHAR PRIMARY KEY,
                ticker            VARCHAR NOT NULL,
                year              INTEGER NOT NULL,
                quarter           VARCHAR NOT NULL,
                stage_preprocess  VARCHAR DEFAULT 'pending',
                stage_extract     VARCHAR DEFAULT 'pending',
                stage_merge       VARCHAR DEFAULT 'pending',
                stage_context     VARCHAR DEFAULT 'pending',
                stage_research    VARCHAR DEFAULT 'pending',
                stage_write       VARCHAR DEFAULT 'pending',
                stage_redflags    VARCHAR DEFAULT 'pending',
                model_extract     VARCHAR,
                model_write       VARCHAR,
                tokens_prompt     BIGINT,
                tokens_completion BIGINT,
                error             VARCHAR,
                output_path       VARCHAR,
                updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            "ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS stage_research VARCHAR DEFAULT 'pending'"
        )
        return self

    # ── Jobs ───────────────────────────────────────────────────────────

    @staticmethod
    def job_id(ticker: str, year: int, quarter: str) -> str:
        return f"{ticker}_{year}_{quarter}"

    def upsert_job(self, ticker: str, year: int, quarter: str) -> str:
        jid = self.job_id(ticker, year, quarter)
        self.conn.execute(
            """
            INSERT INTO analysis_jobs (job_id, ticker, year, quarter)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (job_id) DO UPDATE SET updated_at = now()
            """,
            (jid, ticker, year, quarter),
        )
        return jid

    def mark_stage(self, job_id: str, stage: str, status: str, error: str | None = None):
        if stage not in STAGES:
            raise ValueError(f"Invalid stage: {stage}. Must be one of {STAGES}")
        self.conn.execute(
            f"UPDATE analysis_jobs SET stage_{stage}=?, error=?, updated_at=now() WHERE job_id=?",
            (status, error, job_id),
        )

    def get_stage(self, job_id: str, stage: str) -> str | None:
        if stage not in STAGES:
            raise ValueError(f"Invalid stage: {stage}")
        row = self.conn.execute(
            f"SELECT stage_{stage} FROM analysis_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return row[0] if row else None

    def set_models(self, job_id: str, extract: str | None = None, write: str | None = None):
        if extract:
            self.conn.execute(
                "UPDATE analysis_jobs SET model_extract=?, updated_at=now() WHERE job_id=?",
                (extract, job_id),
            )
        if write:
            self.conn.execute(
                "UPDATE analysis_jobs SET model_write=?, updated_at=now() WHERE job_id=?",
                (write, job_id),
            )

    def add_tokens(self, job_id: str, prompt: int = 0, completion: int = 0):
        self.conn.execute(
            """
            UPDATE analysis_jobs
               SET tokens_prompt = COALESCE(tokens_prompt, 0) + ?,
                   tokens_completion = COALESCE(tokens_completion, 0) + ?,
                   updated_at = now()
             WHERE job_id = ?
            """,
            (prompt, completion, job_id),
        )

    def set_output(self, job_id: str, path: str):
        self.conn.execute(
            "UPDATE analysis_jobs SET output_path=?, updated_at=now() WHERE job_id=?",
            (path, job_id),
        )

    def get_job(self, job_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if not row:
            return None
        cols = [d[0] for d in self.conn.execute("SELECT * FROM analysis_jobs LIMIT 0").description]
        return dict(zip(cols, row))

    def get_jobs(self) -> list[dict]:
        res = self.conn.execute("SELECT * FROM analysis_jobs ORDER BY ticker, year, quarter")
        cols = [d[0] for d in res.description]
        return [dict(zip(cols, r)) for r in res.fetchall()]

    def get_failed_jobs(self, stage: str | None = None) -> list[dict]:
        if stage:
            if stage not in STAGES:
                raise ValueError(f"Invalid stage: {stage}")
            res = self.conn.execute(
                f"SELECT * FROM analysis_jobs WHERE stage_{stage}='failed' ORDER BY ticker, year, quarter"
            )
        else:
            clause = " OR ".join(f"stage_{s}='failed'" for s in STAGES)
            res = self.conn.execute(
                f"SELECT * FROM analysis_jobs WHERE {clause} ORDER BY ticker, year, quarter"
            )
        cols = [d[0] for d in res.description]
        return [dict(zip(cols, r)) for r in res.fetchall()]

    # ── Status ─────────────────────────────────────────────────────────

    def print_status(self):
        res = self.conn.execute("SELECT * FROM analysis_jobs ORDER BY ticker, year, quarter")
        rows = res.fetchall()
        if not rows:
            print("No analysis jobs found.")
            return
        cols = [d[0] for d in res.description]
        print(
            f"\n{'Ticker':<8} {'Year':<6} {'Qtr':<5} "
            f"{'Pre':<11} {'Extract':<11} {'Merge':<11} {'Context':<11} {'Research':<11} {'Write':<11} {'Redflags':<11} "
            f"{'Tok(p/c)':<14} {'Output/Error'}"
        )
        print("-" * 136)
        for r in rows:
            d = dict(zip(cols, r))
            tp = d.get("tokens_prompt") or 0
            tc = d.get("tokens_completion") or 0
            tok = f"{tp}/{tc}" if (tp or tc) else "-"
            out = d.get("output_path") or d.get("error") or ""
            print(
                f"{d.get('ticker',''):<8} {d.get('year',''):<6} {d.get('quarter',''):<5} "
                f"{d.get('stage_preprocess','?'):<11} {d.get('stage_extract','?'):<11} "
                f"{d.get('stage_merge','?'):<11} {d.get('stage_context','?'):<11} "
                f"{d.get('stage_research','?'):<11} "
                f"{d.get('stage_write','?'):<11} {d.get('stage_redflags','?'):<11} "
                f"{tok:<14} {str(out)[:34]}"
            )
        total = len(rows)
        done = sum(
            1 for r in rows
            if all(dict(zip(cols, r)).get(f"stage_{s}") in ("completed", "skipped") for s in STAGES)
        )
        failed = sum(
            1 for r in rows if any(dict(zip(cols, r)).get(f"stage_{s}") == "failed" for s in STAGES)
        )
        print(f"\nTotal: {total} | Completed: {done} | Failed: {failed} | Pending: {total - done - failed}")