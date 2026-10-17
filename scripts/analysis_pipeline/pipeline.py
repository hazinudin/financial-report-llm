"""
IDX Equity Analysis Pipeline
============================

Automates equity-analysis markdown generation from docling-converted IDX reports
(`results/{TICKER}_{Q}_{YEAR}.md`) using the OpenCode Go subscription as the
model API, within a small-context, low-call architecture:

  Stage 0 preprocess   deterministic markdown reducer (drop policy boilerplate)
  Stage 1 extract      cheap-model extraction per bounded chunk -> YAML facts
  Stage 2 merge+ratios  deterministic merge (conflict rules + XLSX cross-check)
                       + all ratios (no LLM arithmetic)
  Stage 3 context      static profile + outline skeleton + vault hits + market stub
  Stage 3b research    headless pi agent (exa_search + fetch_content) -> brief
  Stage 4 write        headless pi writer agent -> analysis/{TICKER} {Q} {Y}.md
  Stage 5 redflags     cheap-model contradiction pass (+ optional re-write)

All state is tracked in idx_data/pipeline.duckdb (analysis_jobs table).

Usage:
    # Full run for one report
    uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1

    # Stage controls
    uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --extract-only
    uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --write-only
    uv run scripts/analysis_pipeline/pipeline.py --tickers DKFT --years 2026 --quarters Q1 --redflags --inject-redflags

    # Batch + ops
    uv run scripts/analysis_pipeline/pipeline.py --tickers ESSA,DKFT --years 2026 --quarters Q1 --workers 3
    uv run scripts/analysis_pipeline/pipeline.py --status
    uv run scripts/analysis_pipeline/pipeline.py --retry-failed --stage extract
    uv run scripts/analysis_pipeline/pipeline.py --list-models
    uv run scripts/analysis_pipeline/pipeline.py --dry-run --tickers DKFT --years 2026 --quarters Q1
"""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from pathlib import Path

import yaml

from db import AnalysisDB, STAGES, get_db

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = _PROJECT_ROOT / "idx_data" / "pipeline.duckdb"
RUNS_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "runs"

import opencode_client  # noqa: E402
import xlsx_seed  # noqa: E402


@contextmanager
def _db_lock_released(db: AnalysisDB, db_path: str | Path | None = None):
    """Release the pipeline's DuckDB write lock for the duration of a stage.

    Stages that spawn a headless pi agent can call ``duckdb_query``, whose tool
    opens ``pipeline.duckdb`` read-only in a separate process. DuckDB refuses
    that while this process holds the read-write connection ("Conflicting lock
    is held"), so the connection is closed before the stage runs and reopened
    afterwards. The stage's own DB writes still go through ``db`` — they just
    happen after the lock is retaken. State written before the release is
    checkpointed by ``close()`` and remains visible on reopen.
    """
    previous = db.conn
    target = Path(db_path) if db_path is not None else _connection_path(previous)
    try:
        previous.close()
    except Exception:  # noqa: BLE001 — an already-closed connection is fine
        pass
    try:
        yield
    finally:
        db.conn = get_db(target)


def _connection_path(conn) -> Path:
    """The on-disk file backing ``conn`` (falls back to the default DB path)."""
    try:
        for _seq, _name, file in conn.execute("PRAGMA database_list").fetchall():
            if file:
                return Path(file)
    except Exception:  # noqa: BLE001 — never block a stage on this lookup
        pass
    return DB_PATH


def _load_market_data(path: str | None) -> str | None:
    return path


def _stage_done(db: AnalysisDB, job_id: str, stage: str) -> bool:
    return db.get_stage(job_id, stage) == "completed"


def run_job(
    ticker: str, year: int, quarter: str, db: AnalysisDB,
    force: bool = False, dry_run: bool = False,
    stages: set[str] | None = None, from_stage: str | None = None,
    market_data_path: str | None = None, workers: int | None = None,
    inject_redflags: bool = False,
    skip_search: bool = False,
    research_tool_mode: str | None = None,
) -> dict:
    import preprocess, extract, merge, ratios, context, write, redflags, research  # noqa: F401

    job_id = db.upsert_job(ticker, year, quarter)
    # XLSX discovery is only needed by the financial-data stages. Avoid doing
    # it for a research-only refresh, which should consume existing facts.
    if stages is None or {"preprocess", "extract", "merge"} & stages:
        xlsx_sync = xlsx_seed.ensure_xlsx_coverage(
            ticker, year, quarter, conn=db.conn, dry_run=dry_run,
        )
        if xlsx_sync.get("seeded") and xlsx_seed.fact_artifact_needs_seed(ticker, year, quarter):
            # Existing fallback artifacts may have been generated before the XLSX
            # appeared. Rebuild from extraction onward so XLSX becomes authoritative.
            force = True
            print("  [XLSX] existing fact artifact predates XLSX seed; rebuilding")
        elif xlsx_sync.get("ingested"):
            # Existing fallback artifacts may have been generated before the XLSX
            # appeared. Rebuild from extraction onward so XLSX becomes authoritative.
            force = True
            print(f"  [XLSX] {xlsx_sync['status']}: {xlsx_sync.get('path', '')}")

    def mark(stage, status, error=None):
        if not dry_run:
            db.mark_stage(job_id, stage, status, error)

    market_data = None
    if market_data_path:
        mp = Path(market_data_path)
        if mp.exists():
            market_data = yaml.safe_load(mp.read_text(encoding="utf-8")) or {}

    def want(stage: str) -> bool:
        if stages is not None:
            return stage in stages
        if from_stage:
            order = STAGES
            return order.index(stage) >= order.index(from_stage)
        return True

    def should_skip(stage: str) -> bool:
        if force or dry_run:
            return False
        if _stage_done(db, job_id, stage):
            return True
        if from_stage and STAGES.index(stage) < STAGES.index(from_stage):
            return True
        return False

    summary = {"job_id": job_id, "ticker": ticker, "year": year, "quarter": quarter, "stages": {}}
    err: str | None = None

    # ── Stage 0: preprocess ──────────────────────────────────────────
    pre_path = None
    if want("preprocess"):
        mark( "preprocess", "running")
        if should_skip("preprocess"):
            pre_path = str(_PROJECT_ROOT / "idx_data" / "analysis" / "preprocessed" / f"{ticker}_{quarter}_{year}.md")
            summary["stages"]["preprocess"] = "skipped"
        else:
            try:
                res = preprocess.run(ticker, year, quarter, force=force, dry_run=dry_run)
                pre_path = res.get("output")
                mark( "preprocess", "completed" if not res.get("status") == "skipped" else "completed")
                summary["stages"]["preprocess"] = res["status"]
            except FileNotFoundError as e:
                mark( "preprocess", "skipped", error=str(e))
                summary["stages"]["preprocess"] = "skipped"
                print(f"\n[{job_id}] SKIP: {e}")
                return summary
            except Exception as e:  # noqa: BLE001
                mark( "preprocess", "failed", error=str(e))
                summary["stages"]["preprocess"] = "failed"
                print(f"\n[{job_id}] preprocess FAILED: {e}")
                return summary

    if pre_path is None:
        pre_path = str(_PROJECT_ROOT / "idx_data" / "analysis" / "preprocessed" / f"{ticker}_{quarter}_{year}.md")

    # ── Stage 1: extract ─────────────────────────────────────────────
    if want("extract"):
        mark( "extract", "running")
        if should_skip("extract"):
            summary["stages"]["extract"] = "skipped"
        elif not Path(pre_path).exists():
            mark( "extract", "skipped", error="preprocessed file missing")
            summary["stages"]["extract"] = "skipped"
        else:
            try:
                res = extract.run(ticker, year, quarter, pre_path, force=force, dry_run=dry_run,
                                  workers=workers, db_conn=db.conn)
                if not dry_run:
                    db.set_models(job_id, extract=res.get("model"))
                tok = res.get("tokens", {})
                if tok and not dry_run:
                    db.add_tokens(job_id, tok.get("prompt", 0), tok.get("completion", 0))
                if res["status"] == "partial":
                    err = res.get("error")
                    mark( "extract", "failed", error=err)
                    summary["stages"]["extract"] = "failed"
                    return summary
                mark( "extract", "completed")
                summary["stages"]["extract"] = res["status"]
            except Exception as e:  # noqa: BLE001
                mark( "extract", "failed", error=str(e))
                summary["stages"]["extract"] = "failed"
                print(f"\n[{job_id}] extract FAILED: {e}")
                return summary

    # ── Stage 2: merge + ratios ──────────────────────────────────────
    if want("merge"):
        mark( "merge", "running")
        if should_skip("merge"):
            summary["stages"]["merge"] = "skipped"
        else:
            try:
                mres = merge.run(ticker, year, quarter, force=force, dry_run=dry_run)
                rres = ratios.run(ticker, year, quarter, market_data=market_data, force=force, dry_run=dry_run)
                mark( "merge", "completed")
                summary["stages"]["merge"] = mres["status"]
                summary["stages"]["ratios"] = rres["status"]
            except Exception as e:  # noqa: BLE001
                mark( "merge", "failed", error=str(e))
                summary["stages"]["merge"] = "failed"
                print(f"\n[{job_id}] merge FAILED: {e}")
                return summary

    # ── Stage 3: context ─────────────────────────────────────────────
    if want("context"):
        mark( "context", "running")
        if should_skip("context"):
            summary["stages"]["context"] = "skipped"
        else:
            try:
                res = context.run(ticker, year, quarter, market_data_path=market_data_path, dry_run=dry_run)
                mark( "context", "completed")
                summary["stages"]["context"] = res["status"]
            except Exception as e:  # noqa: BLE001
                mark( "context", "failed", error=str(e))
                summary["stages"]["context"] = "failed"
                print(f"\n[{job_id}] context FAILED: {e}")
                return summary

    # ── Stage 3b: research ────────────────────────────────────────────
    if want("research"):
        mark( "research", "running")
        if should_skip("research"):
            summary["stages"]["research"] = "skipped"
        else:
            try:
                with _db_lock_released(db):
                    res = research.run(ticker, year, quarter, force=force, dry_run=dry_run,
                                        skip_search=skip_search,
                                        tool_mode=research_tool_mode)
                if not dry_run:
                    tok = res.get("token_usage", {})
                    if tok:
                        db.add_tokens(job_id, tok.get("prompt_tokens", 0), tok.get("completion_tokens", 0))
                status = res.get("status", "success")
                mark( "research", "completed" if status != "failed" else "failed",
                     error=res.get("error"))
                summary["stages"]["research"] = status
            except Exception as e:  # noqa: BLE001
                mark( "research", "failed", error=str(e))
                summary["stages"]["research"] = "failed"
                print(f"\n[{job_id}] research FAILED: {e}")

    # ── Stage 4: write ───────────────────────────────────────────────
    out_path = None
    if want("write"):
        mark( "write", "running")
        if should_skip("write") and not inject_redflags:
            out_path = str(_PROJECT_ROOT / "analysis" / f"{ticker} {quarter} {year} Analysis.md")
            summary["stages"]["write"] = "skipped"
        else:
            try:
                with _db_lock_released(db):
                    res = write.run(ticker, year, quarter, market_data_path=market_data_path,
                                    force=force or inject_redflags, dry_run=dry_run)
                tok = res.get("usage", {})
                if tok and not dry_run:
                    db.add_tokens(job_id, tok.get("prompt_tokens", 0), tok.get("completion_tokens", 0))
                if not dry_run:
                    db.set_models(job_id, write=res.get("model"))
                    db.set_output(job_id, str(_PROJECT_ROOT / "analysis" / f"{ticker} {quarter} {year} Analysis.md"))
                mark( "write", "completed")
                summary["stages"]["write"] = res["status"]
                out_path = res.get("output")
            except Exception as e:  # noqa: BLE001
                mark( "write", "failed", error=str(e))
                summary["stages"]["write"] = "failed"
                print(f"\n[{job_id}] write FAILED: {e}")
                return summary

    # ── Stage 5: redflags (+ optional inject re-write) ──────────────
    if want("redflags"):
        mark( "redflags", "running")
        try:
            rres = redflags.run(ticker, year, quarter, dry_run=dry_run)
            tok = rres.get("usage", {})
            if tok and not dry_run:
                db.add_tokens(job_id, tok.get("prompt_tokens", 0), tok.get("completion_tokens", 0))
            bullets = rres.get("bullets", "")
            if inject_redflags and bullets and not dry_run:
                print(f"\n[{job_id}] re-running write with injected red flags...")
                with _db_lock_released(db):
                    wres = write.run(ticker, year, quarter, market_data_path=market_data_path,
                                     force=True, inject_redflags=bullets)
                wt = wres.get("usage", {})
                if wt:
                    db.add_tokens(job_id, wt.get("prompt_tokens", 0), wt.get("completion_tokens", 0))
                db.set_output(job_id, wres.get("output", ""))
            mark( "redflags", "completed")
            summary["stages"]["redflags"] = rres["status"]
        except Exception as e:  # noqa: BLE001
            mark( "redflags", "failed", error=str(e))
            summary["stages"]["redflags"] = "failed"
            print(f"\n[{job_id}] redflags FAILED: {e}")

    summary["output"] = out_path
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="IDX Equity Analysis Pipeline: preprocess -> extract -> merge -> ratios -> write",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers (e.g. ESSA,DKFT).")
    parser.add_argument("--years", type=str, default=None, help="Comma-separated years (e.g. 2026).")
    parser.add_argument("--quarters", type=str, default=None, help="Comma-separated quarters (e.g. Q1,Q4).")
    parser.add_argument("--market-data", type=str, default=None, help="Path to market_data.yaml stub.")
    parser.add_argument("--workers", type=int, default=None, help="Parallel extraction workers.")
    parser.add_argument("--force", action="store_true", help="Re-run stages even if outputs exist.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without calling the model.")
    parser.add_argument("--status", action="store_true", help="Print the analysis-jobs dashboard and exit.")
    parser.add_argument("--list-models", action="store_true", help="List model IDs available under the Go subscription and exit.")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run jobs with failed stages.")
    parser.add_argument("--stage", type=str, default=None, choices=list(STAGES),
                        help="Limit --retry-failed to a stage, or start from a stage.")
    parser.add_argument("--extract-only", action="store_true", help="Run only preprocess + extract.")
    parser.add_argument("--research-only", action="store_true", help="Run through research and stop before writing.")
    parser.add_argument("--write-only", action="store_true", help="Run only context + write (facts/ratios must exist).")
    parser.add_argument("--skip-research", action="store_true", help="Skip the research stage.")
    parser.add_argument("--no-search", action="store_true", help="Run research triggers + internal analysis without web search.")
    parser.add_argument("--research-tool-mode", type=str, default=None,
                        choices=["pi", "deterministic_fallback"],
                        help="Override the research mode (pi agent or deterministic fallback).")
    parser.add_argument("--redflags", action="store_true", help="Run the Stage 5 red-flag pass after writing.")
    parser.add_argument("--inject-redflags", action="store_true", help="With --redflags, re-run the writer to address each flag.")
    args = parser.parse_args()

    # ── list models ───────────────────────────────────────────────────
    if args.list_models:
        try:
            models = opencode_client.list_models()
            for m in models:
                print(m)
            print(f"\n({len(models)} models)")
        except Exception as e:  # noqa: BLE001
            print(f"[!] could not list models: {e}", file=sys.stderr)
            sys.exit(2)
        return

    conn = get_db(DB_PATH)
    db = AnalysisDB(conn)
    db.init_schema()

    # ── status ────────────────────────────────────────────────────────
    if args.status:
        try:
            db.print_status()
        finally:
            db.conn.close()
        return

    # ── retry-failed ──────────────────────────────────────────────────
    if args.retry_failed:
        failed = db.get_failed_jobs(stage=args.stage)
        print(f"\n[*] {len(failed)} failed analysis job(s)")
        for job in failed:
            t, y, q = job["ticker"], job["year"], job["quarter"]
            print(f"\n[*] Retrying {t} {q} {y} ...")
            stage = args.stage or next((s for s in STAGES if job.get(f"stage_{s}") == "failed"), "preprocess")
            run_job(t, y, q, db, from_stage=stage, force=True,
                    market_data_path=args.market_data, workers=args.workers,
                    inject_redflags=args.inject_redflags,
                    skip_search=args.no_search,
                    research_tool_mode=args.research_tool_mode)
        db.conn.close()
        return

    # ── normal run ───────────────────────────────────────────────────
    if not args.tickers or not args.years or not args.quarters:
        parser.error("--tickers, --years, and --quarters are required (or use --status/--list-models).")

    tickers = [t.strip().upper() for t in args.tickers.split(",")]
    years = [int(y.strip()) for y in args.years.split(",")]
    quarters = [q.strip().upper() for q in args.quarters.split(",")]

    stages: set[str] | None = None
    if args.extract_only:
        stages = {"preprocess", "extract"}
    elif args.research_only:
        stages = {"preprocess", "extract", "merge", "context", "research"}
    elif args.write_only:
        stages = {"context", "research", "write"}
    elif args.skip_research:
        stages = {"preprocess", "extract", "merge", "context", "write", "redflags"}

    combos = [(t, y, q) for t in tickers for y in years for q in quarters]
    print("=" * 60)
    print(f"[*] Analysis pipeline: {len(combos)} job(s)")
    print("=" * 60)
    try:
        for t, y, q in combos:
            print(f"\n==== {t} {q} {y} ====")
            run_job(t, y, q, db, force=args.force, dry_run=args.dry_run, stages=stages,
                    market_data_path=args.market_data, workers=args.workers,
                    inject_redflags=args.redflags and args.inject_redflags,
                    skip_search=args.no_search,
                    research_tool_mode=args.research_tool_mode)
    finally:
        db.conn.close()


if __name__ == "__main__":
    main()
