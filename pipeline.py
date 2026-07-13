"""
IDX Financial Report Pipeline
===============================

Downloads financial reports from IDX, identifies the actual financial report PDF
among several supplementary documents, converts it to markdown using docling, and
parses XLSX structured financial statements into DuckDB.

All pipeline state is persisted in a local DuckDB database (idx_data/pipeline.duckdb),
enabling monitoring, retry of failed stages, and structured queries across companies.

Usage:
    # Full pipeline: download -> identify -> collect -> convert
    python pipeline.py --tickers ESSA,BBCA --years 2024,2025

    # Convert only (skip download, use already downloaded files)
    python pipeline.py --convert-only --tickers ESSA

    # Identify reports without converting
    python pipeline.py --identify-only --tickers ESSA

    # Parse XLSX financial statements into DuckDB
    python pipeline.py --parse-xlsx --tickers JARR

    # View pipeline job status
    python pipeline.py --status

    # Retry failed jobs
    python pipeline.py --retry-failed

    # Retry only failed conversion stage
    python pipeline.py --retry-failed --stage convert

    # Dry run
    python pipeline.py --dry-run --tickers ESSA
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

from db import get_db, PipelineDB
from idx_downloader import IDXClient, sanitize_filename, VALID_PERIODS

BASE_DIR = Path("./idx_data")
REPORTS_DIR = BASE_DIR / "reports"
RAW_DIR = Path("./raw")
RESULTS_DIR = Path("./results")
DB_PATH = BASE_DIR / "pipeline.duckdb"

SUPPLEMENTARY_PATTERNS = [
    re.compile(r"(?i)\bSPD\b"),
    re.compile(r"(?i)surat\s+pernyataan\s+direksi"),
    re.compile(r"(?i)directors?\s+statement"),
    re.compile(r"(?i)perubahan\s+liabilitas"),
    re.compile(r"(?i)penjelasan\s+perubahan"),
    re.compile(r"(?i)penjelasan\s+atas\s+peningkatan"),
    re.compile(r"(?i)peningkatan\s+aset"),
    re.compile(r"(?i)pernyataan\s+total\s+aset"),
    re.compile(r"(?i)kenaikan\s+aset"),
    re.compile(r"(?i)checklist?\s+pengungkapan"),
    re.compile(r"(?i)chec?klist"),             # checklist or cheklist anywhere
    re.compile(r"(?i)OJK\s+checklist"),
    re.compile(r"(?i)cover\s+letter"),
    re.compile(r"(?i)\bESG\b"),
    re.compile(r"(?i)press\s+release"),
    re.compile(r"(?i)surat\s+penyampaian"),
    re.compile(r"(?i)surat\s+pengantar"),      # surat pengantar (transmittal letter)
    re.compile(r"(?i)pengantar"),              # standalone pengantar (cover letter)
    re.compile(r"(?i)penyampaian"),
    re.compile(r"(?i)penyampaian\s+lapkeu"),
    re.compile(r"(?i)surat\s+perubahan"),
    re.compile(r"(?i)surat\s+ojk"),
    re.compile(r"(?i)laporan\s+keuangan\s+per\s+\d"),
    re.compile(r"(?i)\batt[12]\.pdf$"),
    re.compile(r"(?i)annual\s*report.*att\d"),
    re.compile(r"(?i)inlineXBRL"),
    re.compile(r"(?i)^instance\."),
    re.compile(r"(?i)surat\s+keluar"),         # outgoing letter (cover letter)
    re.compile(r"(?i)lk\s+interim"),           # interim report cover letter
    re.compile(r"(?i)no\.\s*\d+.*surat"),      # numbered outgoing letters
]

FULL_REPORT_PATTERNS = [
    re.compile(r"(?i)^LK\s"),
    re.compile(r"(?i)\bLK\s+(Tahunan|TW\s+[IVX]+|Semester)"),  # LK followed by period type
    re.compile(r"(?i)^LAP\s"),            # "LAP" = Laporan (Indonesian report abbreviation)
    re.compile(r"(?i)Laporan\s+Keuangan\s+Konsolidasian"),
    re.compile(r"(?i)_Laporan\s+Keuangan\s+\d"),
    re.compile(r"(?i)Consolidated\s+Financial\s+Statements?"),
    re.compile(r"(?i)^FS\s"),             # "FS" = Financial Statement (common prefix)
    re.compile(r"(?i)\bFinancial\s+Statement\b"),
]

# XLSX-derived PDFs — useful structured data but NOT the full financial report.
# Keep separate from FULL_REPORT_PATTERNS; only used as fallback when no real report exists.
XLSX_DATA_PATTERNS = [
    re.compile(r"(?i)^FinancialStatement-"),
    re.compile(r"(?i)^FS\s\d"),
]


def is_supplementary(filename: str) -> bool:
    for pattern in SUPPLEMENTARY_PATTERNS:
        if pattern.search(filename):
            return True
    return False


def report_score(filename: str) -> int:
    score = 0
    if is_supplementary(filename):
        return -50
    for pattern in FULL_REPORT_PATTERNS:
        if pattern.search(filename):
            score += 20
            break
    if filename.lower().startswith("lk "):
        score += 15
    if filename.lower().startswith("fs "):
        score += 15
    for pattern in XLSX_DATA_PATTERNS:
        if pattern.search(filename):
            score += 0  # XLSX data sheets are fallback only — no score advantage
            break
    return score


def identify_financial_report(
    pdf_files: list[dict],
    strategy: str = "auto",
) -> dict | None:
    """
    Identify the financial report PDF from a list of candidate files.
    """
    if not pdf_files:
        return None

    if strategy == "financial_statement":
        candidates = [
            f for f in pdf_files
            if f["file_name"].lower().startswith("financialstatement-")
        ]
        if candidates:
            return max(candidates, key=lambda f: f.get("file_size", 0))
        print("[!] No 'FinancialStatement-' PDF found, falling back to 'auto'")
        strategy = "auto"

    if strategy == "laporan_keuangan":
        candidates = [
            f for f in pdf_files
            if (f["file_name"].lower().startswith("lk ")
                or "laporan keuangan" in f["file_name"].lower()
                or re.search(r"\bLK\s+(Tahunan|TW\s+[IVX]+|Semester)", f["file_name"])
                or f["file_name"].lower().startswith("fs "))
            and not is_supplementary(f["file_name"])
        ]
        if candidates:
            return max(candidates, key=lambda f: f.get("file_size", 0))
        print("[!] No 'LK'/'Laporan Keuangan' PDF found, falling back to 'auto'")
        strategy = "auto"

    if strategy == "largest":
        return max(pdf_files, key=lambda f: f.get("file_size", 0))

    scored = [(report_score(f["file_name"]), f.get("file_size", 0), f) for f in pdf_files]
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)  # score desc, then size desc

    if scored[0][0] > 0:
        return scored[0][2]

    non_supp = [
        (report_score(f["file_name"]), f)
        for f in pdf_files
        if not is_supplementary(f["file_name"])
    ]
    if non_supp:
        return max(non_supp, key=lambda x: x[1].get("file_size", 0))[1]

    return max(pdf_files, key=lambda f: f.get("file_size", 0))


def get_reports_from_db(
    db: PipelineDB,
    tickers: list[str] | None = None,
    years: list[int] | None = None,
    periods: list[str] | None = None,
) -> dict[str, list[dict]]:
    """
    Read downloaded file records from DuckDB and group by
    '{ticker}/{year}_{period}' key.
    """
    groups: dict[str, list[dict]] = {}

    # Fetch all downloaded files, optionally filtered
    # We query all, then filter in memory for flexibility
    all_files = db.get_downloaded_files()

    for entry in all_files:
        ticker = entry.get("ticker", "")
        if tickers and ticker not in tickers:
            continue

        year_val = entry.get("year", 0)
        if years and year_val not in years:
            continue

        period_val = entry.get("period", "").lower()
        if periods and period_val not in [p.lower() for p in periods]:
            continue

        key = f"{ticker}/{year_val}_{period_val}"
        groups.setdefault(key, []).append(entry)

    return groups


def collect_and_link_reports(
    identified: dict[str, str],
    raw_dir: Path = RAW_DIR,
    dry_run: bool = False,
) -> list[dict]:
    """Copy (or link) identified report PDFs into the raw/ directory."""
    raw_dir.mkdir(parents=True, exist_ok=True)

    PERIOD_TO_QUARTER = {
        "tw1": "Q1", "tw2": "Q2", "tw3": "Q3", "audit": "Q4",
    }

    pipeline_meta = []
    for key, src_path in sorted(identified.items()):
        src = Path(src_path)
        if not src.exists():
            print(f"[!] Source file not found: {src_path}")
            continue

        ticker, year_period = key.split("/", 1)
        year_str, period = year_period.split("_", 1)
        quarter = PERIOD_TO_QUARTER.get(period.lower(), period.upper())
        dest_name = f"{ticker}_{quarter}_{year_str}.pdf"
        dest = raw_dir / dest_name

        if dry_run:
            print(f"  [DRY] Would link: {src} -> {dest}")
            pipeline_meta.append({
                "key": key, "source": str(src), "destination": str(dest), "action": "link",
            })
            continue

        shutil.copy2(src, dest)
        print(f"  [LINK] {src.name} -> {dest}")
        pipeline_meta.append({
            "key": key, "source": str(src), "destination": str(dest), "action": "copy",
        })

    return pipeline_meta


def convert_reports(
    db: PipelineDB | None = None,
    raw_dir: Path = RAW_DIR,
    results_dir: Path = RESULTS_DIR,
    force: bool = False,
) -> list[dict]:
    """Convert all PDFs in raw/ to markdown using docling."""
    from docling.document_converter import DocumentConverter

    raw_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    converter = DocumentConverter()
    pdf_files = sorted(raw_dir.glob("*.pdf"))
    results = []

    if not pdf_files:
        print("[!] No PDF files found in raw/")
        return results

    PERIOD_REVERSE = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}

    for pdf_path in pdf_files:
        output_file = results_dir / (pdf_path.stem + ".md")

        if output_file.exists() and not force:
            print(f"  [SKIP] {pdf_path.name} (already converted)")
            results.append({
                "source": str(pdf_path), "output": str(output_file), "status": "skipped",
            })
            continue

        print(f"  [CONVERT] {pdf_path.name}" + (" (forced)" if force and output_file.exists() else ""))
        try:
            start = time.time()
            result = converter.convert(str(pdf_path))
            elapsed = time.time() - start
            markdown_text = result.document.export_to_markdown()
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown_text)
            print(f"  [DONE] {pdf_path.name} in {elapsed:.1f}s -> {output_file.name}")
            results.append({
                "source": str(pdf_path), "output": str(output_file),
                "status": "success", "elapsed": elapsed,
            })

            # Write conversion record to DB
            if db:
                # Extract ticker/year/period from filename (e.g. JARR_Q4_2025.pdf)
                stem = pdf_path.stem
                parts = stem.rsplit("_", 2)
                if len(parts) == 3:
                    ticker, quarter, year_str = parts
                    period = PERIOD_REVERSE.get(quarter, quarter.lower())
                    job_id = f"{ticker}_{year_str}_{period}"
                    db.upsert_conversion(
                        job_id=job_id,
                        source_pdf=str(pdf_path),
                        output_md=str(output_file),
                        status="success",
                        elapsed_sec=elapsed,
                    )

        except Exception as e:
            print(f"  [ERROR] {pdf_path.name}: {e}")
            results.append({
                "source": str(pdf_path), "output": str(output_file),
                "status": "error", "error": str(e),
            })
            if db:
                db.upsert_conversion(
                    job_id="",
                    source_pdf=str(pdf_path),
                    output_md=str(output_file),
                    status="error",
                    error=str(e),
                )

    return results


# ── XLSX Parsing ───────────────────────────────────────────────────────

PERIOD_MAP_FILENAME = {
    "Tahunan": "audit", "I": "TW1", "II": "TW2", "III": "TW3", "IV": "TW4",
}


def scan_disk_and_register(
    db: PipelineDB,
    reports_dir: Path = REPORTS_DIR,
    tickers: list[str] | None = None,
    dry_run: bool = False,
):
    """
    Scan the reports/ directory for PDF and XLSX files and register them
    in the DuckDB downloaded_files table. Use this when files were manually
    downloaded from the IDX website instead of using the API downloader.

    Directory structure expected: reports/TICKER/YEAR_PERIOD/filename
    """
    if not reports_dir.exists():
        print("[!] Reports directory not found:", reports_dir)
        return

    tickers_upper = {t.upper() for t in tickers} if tickers else None
    registered = 0

    for ticker_dir in sorted(reports_dir.iterdir()):
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        if tickers_upper and ticker not in tickers_upper:
            continue

        for period_dir in sorted(ticker_dir.iterdir()):
            if not period_dir.is_dir():
                continue
            period_dir_name = period_dir.name  # e.g. "2026_TW1"
            if "_" not in period_dir_name:
                continue
            year_str, period_str = period_dir_name.split("_", 1)
            try:
                year_val = int(year_str)
            except ValueError:
                continue
            period_val = period_str.lower()

            job_id = f"{ticker}_{year_val}_{period_val}"

            # Ensure pipeline_job exists
            db.conn.execute(
                "INSERT OR IGNORE INTO pipeline_jobs (job_id, ticker, year, period) VALUES ($1, $2, $3, $4)",
                (job_id, ticker, year_val, period_val),
            )

            for f in sorted(period_dir.iterdir()):
                if f.is_dir():
                    continue
                suffix = f.suffix.lower()
                if suffix not in (".pdf", ".xlsx", ".xls", ".zip"):
                    continue

                file_type = suffix
                file_size = f.stat().st_size
                file_name = f.name
                local_path = str(f)

                meta = {
                    "ticker": ticker,
                    "company_name": "",
                    "year": year_val,
                    "period": period_val,
                    "file_name": file_name,
                    "file_path": "",
                    "file_size": file_size,
                    "file_type": file_type,
                    "local_path": local_path,
                }

                if dry_run:
                    print(f"  [DRY] Would register: {local_path}")
                    registered += 1
                    continue

                db.upsert_downloaded_file(meta, job_id=job_id)
                registered += 1

    if not dry_run:
        print(f"[*] Registered {registered} files in DuckDB")
    else:
        print(f"[*] Would register {registered} files (dry run)")


def parse_xlsx_stage(
    db: PipelineDB,
    tickers: list[str] | None = None,
    years: list[int] | None = None,
    periods: list[str] | None = None,
    dry_run: bool = False,
):
    """
    Scan reports/ directory for FinancialStatement-*.xlsx files,
    parse them, and insert into DuckDB (xlsx_meta + xlsx_statements).
    """
    from xlsx_parser import parse_xlsx

    reports_dir = REPORTS_DIR
    if not reports_dir.exists():
        print("[!] Reports directory not found:", reports_dir)
        return

    xlsx_files = sorted(reports_dir.rglob("*.xlsx"))
    xlsx_files = [f for f in xlsx_files if "FinancialStatement" in f.name]

    if tickers:
        tickers_upper = {t.upper() for t in tickers}
        xlsx_files = [f for f in xlsx_files if f.parent.parent.name in tickers_upper]

    print(f"\n[*] Found {len(xlsx_files)} XLSX financial statement files")
    total_rows = 0
    skipped = 0

    for xf in xlsx_files:
        # Extract ticker/year/period from file path: reports/TICKER/YEAR_PERIOD/FinancialStatement-*.xlsx
        try:
            period_dir = xf.parent
            year_period = period_dir.name  # e.g., "2025_Audit" or "2025_TW1"
            ticker_dir = period_dir.parent
            ticker_val = ticker_dir.name

            if "_" in year_period:
                year_str, period_str = year_period.split("_", 1)
                year_val = int(year_str)
                period_val = period_str.lower()
            else:
                print(f"  [SKIP] Cannot parse year/period from path: {xf}")
                continue

            # Normalize period
            if period_val not in ("tw1", "tw2", "tw3", "tw4", "audit"):
                period_val = period_val.lower()
        except Exception:
            print(f"  [SKIP] Cannot parse path structure: {xf}")
            continue

        if years and year_val not in years:
            continue
        if periods and period_val not in [p.lower() for p in periods]:
            continue

        # Check if already parsed
        if db.has_xlsx_data(ticker_val, year_val, period_val):
            print(f"  [SKIP] {ticker_val} {year_val} {period_val} (already in DB)")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY] Would parse: {xf.name}")
            continue

        print(f"  [PARSE] {xf.name}")
        try:
            meta, statements = parse_xlsx(
                str(xf), ticker=ticker_val, year=year_val, period=period_val,
            )

            if meta:
                db.upsert_xlsx_meta(meta)

            if statements:
                db.upsert_xlsx_statements(statements)
                total_rows += len(statements)
                print(f"    -> {len(statements)} rows ({ticker_val} {year_val} {period_val})")
            else:
                print(f"    -> No statement rows extracted")

            # Mark pipeline job stage
            job_id = db.job_id(ticker_val, year_val, period_val)
            db.upsert_job(ticker_val, year_val, period_val)
            db.mark_stage(job_id, "identify", "completed")

        except Exception as e:
            print(f"  [ERROR] {xf.name}: {e}")

    print(f"\n[*] XLSX parsing complete. {total_rows} rows inserted, {skipped} skipped.")


# ── Pipeline Orchestration ──────────────────────────────────────────────

def run_pipeline(
    tickers: list[str] | None = None,
    years: list[int] | None = None,
    periods: list[str] | None = None,
    strategy: str = "auto",
    output_dir: str = "./idx_data",
    delay: float = 0.5,
    download: bool = True,
    convert: bool = True,
    identify_only: bool = False,
    dry_run: bool = False,
    parse_xlsx: bool = False,
    retry_failed: bool = False,
    retry_stage: str | None = None,
    force: bool = False,
    conn: "duckdb.DuckDBPyConnection | None" = None,  # type: ignore
):
    """
    Run the full pipeline:
      1. Download reports using IDXClient
      2. Identify the financial report PDF in each company/period
      3. Copy identified PDFs to raw/
      4. Convert to markdown using docling
    All state is tracked in DuckDB.
    """
    import duckdb as _duckdb

    close_conn = False
    if conn is None:
        conn = get_db(DB_PATH)
        close_conn = True

    db = PipelineDB(conn)
    db.init_schema()

    try:
        from idx_downloader import run as idx_run

        # ── Handle retry ──────────────────────────────────────────
        if retry_failed:
            if retry_stage:
                failed_jobs = db.get_failed_jobs(stage=retry_stage)
                print(f"\n[*] Found {len(failed_jobs)} job(s) with failed {retry_stage} stage")
            else:
                failed_jobs = db.get_failed_jobs()
                print(f"\n[*] Found {len(failed_jobs)} job(s) with any failed stage")

            if not failed_jobs:
                print("[*] No failed jobs to retry.")
                return

            for job in failed_jobs:
                jid = job["job_id"]
                ticker_j = job["ticker"]
                year_j = job["year"]
                period_j = job["period"]
                print(f"\n[*] Retrying {jid}...")

                # Determine which stage to retry
                stage_order = ["download", "identify", "collect", "convert"]
                retry_from = retry_stage
                if not retry_from:
                    for s in stage_order:
                        if job.get(f"stage_{s}") == "failed":
                            retry_from = s
                            break

                if not retry_from:
                    print(f"  Job {jid} has no failed stage (may have been fixed)")
                    continue

                # Reset the failed stage and re-run from there
                db.mark_stage(jid, retry_from, "pending")
                _run_stages(
                    db=db,
                    tickers=[ticker_j],
                    years=[year_j],
                    periods=[period_j],
                    strategy=strategy,
                    output_dir=output_dir,
                    delay=delay,
                    download=download,
                    convert=convert,
                    dry_run=dry_run,
                    start_from=retry_from,
                    force=force,
                )

            return

        # ── Step 1: Download ──────────────────────────────────────
        if download:
            print("=" * 60)
            print("[STEP 1] Downloading financial reports from IDX")
            print("=" * 60)

            # Create pipeline_jobs for each ticker/year/period being processed
            if tickers:
                _compute = [(t, y, p) for t in tickers for y in (years or []) for p in (periods or [])]
                for t, y, p in _compute:
                    db.upsert_job(t, y, p)
                    db.mark_stage(db.job_id(t, y, p), "download", "running")

            idx_run(
                tickers=tickers,
                years=years,
                periods=periods,
                pdf_only=True,
                dry_run=dry_run,
                output_dir=output_dir,
                delay=delay,
                db_conn=conn,
            )

            # After download, mark jobs as completed (idx_run writes to DB)
            if not dry_run and tickers and years and periods:
                for t in tickers:
                    for y in years:
                        for p in periods:
                            jid = db.job_id(t, y, p)
                            if db.get_stage_status(jid, "download") == "running":
                                db.mark_stage(jid, "download", "completed")

        # ── Step 2: Identify ──────────────────────────────────────
        print("\n" + "=" * 60)
        print("[STEP 2] Identifying financial report PDFs")
        print("=" * 60)

        reports_by_key = get_reports_from_db(db, tickers=tickers, years=years, periods=periods)

        if tickers:
            for t in tickers:
                for y in (years or []):
                    for p in (periods or []):
                        jid = db.job_id(t, y, p)
                        db.mark_stage(jid, "identify", "running")

        identified: dict[str, str] = {}
        for key in sorted(reports_by_key):
            entries = reports_by_key[key]
            ticker_k, rest = key.split("/", 1)
            year_k_str, period_k = rest.split("_", 1)
            year_k = int(year_k_str)
            period_k = period_k.lower()

            pdf_entries = [
                e for e in entries
                if e.get("file_type", "").lower() == ".pdf"
                or e.get("file_name", "").lower().endswith(".pdf")
            ]
            if not pdf_entries:
                print(f"  [{key}] No PDFs found, skipping")
                db.mark_stage(db.job_id(ticker_k, year_k, period_k), "identify", "skipped")
                continue

            winner = identify_financial_report(pdf_entries, strategy=strategy)
            if winner:
                src = winner.get("local_path", "")
                if src and Path(src).exists():
                    identified[key] = src
                    tag = ""
                    if winner["file_name"].lower().startswith("financialstatement-"):
                        tag = " (FinancialStatement)"
                    elif winner["file_name"].lower().startswith("fs "):
                        tag = " (FS – Financial Statement)"
                    elif winner["file_name"].lower().startswith("lk "):
                        tag = " (LK – Laporan Keuangan)"
                    elif "laporan keuangan" in winner["file_name"].lower():
                        tag = " (Laporan Keuangan)"
                    print(f"  [{key}] -> {winner['file_name']}{tag}")
                    db.mark_stage(db.job_id(ticker_k, year_k, period_k), "identify", "completed")
                elif winner.get("file_path", "").startswith("http"):
                    identified[key] = winner["file_path"]
                    print(f"  [{key}] -> {winner['file_name']} (URL only)")
                    db.mark_stage(db.job_id(ticker_k, year_k, period_k), "identify", "completed")
                else:
                    print(f"  [{key}] -> {winner['file_name']} (file missing on disk: {src})")
                    db.mark_stage(db.job_id(ticker_k, year_k, period_k), "identify", "failed",
                                  error=f"File missing on disk: {src}")
            else:
                print(f"  [{key}] Could not identify financial report")
                db.mark_stage(db.job_id(ticker_k, year_k, period_k), "identify", "failed",
                              error="Could not identify financial report")

        print(f"\n[*] Identified {len(identified)} financial report(s)")

        if identify_only:
            print("\n[*] Identify-only mode, stopping here.")
            return identified

        # ── Step 3: Collect ─────────────────────────────────────
        print("\n" + "=" * 60)
        print("[STEP 3] Collecting identified reports into raw/")
        print("=" * 60)

        # Mark collect stages
        for key in sorted(identified):
            ticker_k, rest = key.split("/", 1)
            year_k_str, period_k = rest.split("_", 1)
            db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k.lower()), "collect", "running")

        pipeline_meta = collect_and_link_reports(
            identified, raw_dir=RAW_DIR, dry_run=dry_run
        )

        for meta in pipeline_meta:
            key = meta["key"]
            ticker_k, rest = key.split("/", 1)
            year_k_str, period_k = rest.split("_", 1)
            db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k.lower()), "collect", "completed")

        # ── Step 4: Convert ──────────────────────────────────────
        if convert and not dry_run:
            print("\n" + "=" * 60)
            print("[STEP 4] Converting PDFs to markdown (docling)")
            print("=" * 60)

            for key in sorted(identified):
                ticker_k, rest = key.split("/", 1)
                year_k_str, period_k = rest.split("_", 1)
                db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k.lower()), "convert", "running")

            conversion_results = convert_reports(db, RAW_DIR, RESULTS_DIR, force=force)

            # Update job stages based on conversion results
            PERIOD_REVERSE = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}
            for r in conversion_results:
                src = Path(r.get("source", ""))
                stem = src.stem
                parts = stem.rsplit("_", 2)
                if len(parts) == 3:
                    ticker_k, quarter, year_str = parts
                    period_val = PERIOD_REVERSE.get(quarter, quarter.lower())
                    jid = db.job_id(ticker_k, int(year_str), period_val)
                    status = r.get("status", "unknown")
                    if status == "success":
                        db.mark_stage(jid, "convert", "completed")
                    elif status == "error":
                        db.mark_stage(jid, "convert", "failed", error=r.get("error", ""))
                    elif status == "skipped":
                        db.mark_stage(jid, "convert", "completed")

            print("\n" + "=" * 60)
            print("[*] Pipeline complete!")
            print(f"    Reports identified: {len(identified)}")
            for r in conversion_results:
                status = r.get("status", "unknown")
                name = Path(r.get("source", "")).name
                print(f"      {name}: {status}")

        elif dry_run:
            print("\n[*] Dry run -- no conversion performed.")

        # ── Parse XLSX ────────────────────────────────────────────
        if parse_xlsx:
            print("\n" + "=" * 60)
            print("[STEP 5] Parsing XLSX financial statements into DuckDB")
            print("=" * 60)
            parse_xlsx_stage(db, tickers=tickers, years=years, periods=periods, dry_run=dry_run)

    finally:
        if close_conn:
            conn.close()

    return identified


def _run_stages(
    db: PipelineDB,
    tickers: list[str],
    years: list[int],
    periods: list[str],
    strategy: str,
    output_dir: str,
    delay: float,
    download: bool,
    convert: bool,
    dry_run: bool,
    start_from: str,
    force: bool = False,
):
    """Run stages from a specific starting point (for retry)."""
    from idx_downloader import run as idx_run

    stage_order = ["download", "identify", "collect", "convert"]
    stage_idx = stage_order.index(start_from)

    if stage_idx <= 0 and download:
        print(f"  Re-running download for {tickers}...")
        idx_run(
            tickers=tickers,
            years=years,
            periods=periods,
            pdf_only=True,
            dry_run=dry_run,
            output_dir=output_dir,
            delay=delay,
            db_conn=db.conn,
        )
        for t in tickers:
            for y in years:
                for p in periods:
                    jid = db.job_id(t, y, p)
                    db.mark_stage(jid, "download", "completed")

    if stage_idx <= 1:
        print(f"  Re-running identify for {tickers}...")
        reports_by_key = get_reports_from_db(db, tickers=tickers, years=years, periods=periods)
        for t in tickers:
            for y in years:
                for p in periods:
                    jid = db.job_id(t, y, p)
                    db.mark_stage(jid, "identify", "running")
        identified = {}
        for key in sorted(reports_by_key):
            entries = reports_by_key[key]
            ticker_k, rest = key.split("/", 1)
            year_k_str, period_k = rest.split("_", 1)
            pdf_entries = [
                e for e in entries
                if e.get("file_type", "").lower() == ".pdf"
                or e.get("file_name", "").lower().endswith(".pdf")
            ]
            if not pdf_entries:
                continue
            winner = identify_financial_report(pdf_entries, strategy=strategy)
            if winner and winner.get("local_path") and Path(winner["local_path"]).exists():
                identified[key] = winner["local_path"]
                db.mark_stage(db.job_id(ticker_k, int(year_k_str), p), "identify", "completed")
            else:
                db.mark_stage(db.job_id(ticker_k, int(year_k_str), p), "identify", "failed")

        if identified:
            print(f"  Re-collecting {len(identified)} reports...")
            for key in sorted(identified):
                ticker_k, rest = key.split("/", 1)
                year_k_str, period_k = rest.split("_", 1)
                db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k), "collect", "running")
            collect_and_link_reports(identified, RAW_DIR, dry_run=dry_run)
            for key in sorted(identified):
                ticker_k, rest = key.split("/", 1)
                year_k_str, period_k = rest.split("_", 1)
                db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k), "collect", "completed")

            if stage_idx <= 3 and convert and not dry_run:
                for key in sorted(identified):
                    ticker_k, rest = key.split("/", 1)
                    year_k_str, period_k = rest.split("_", 1)
                    db.mark_stage(db.job_id(ticker_k, int(year_k_str), period_k), "convert", "running")
                convert_reports(db, RAW_DIR, RESULTS_DIR, force=force)


def main():
    parser = argparse.ArgumentParser(
        description="IDX Financial Report Pipeline: download -> identify -> convert",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated list of ticker symbols (e.g., ESSA,BBCA).",
    )
    parser.add_argument(
        "--years", type=str, default=None,
        help="Comma-separated years (e.g., 2024,2025).",
    )
    parser.add_argument(
        "--periods", type=str, default=None,
        help="Comma-separated periods (TW1,TW2,TW3,audit).",
    )
    parser.add_argument(
        "--output", type=str, default="./idx_data",
        help="IDX download directory. Default: ./idx_data",
    )
    parser.add_argument(
        "--delay", type=float, default=0.5,
        help="Delay between API requests in seconds (default: 0.5).",
    )
    parser.add_argument(
        "--strategy", type=str, default="auto",
        choices=["auto", "financial_statement", "largest", "laporan_keuangan"],
        help="Strategy to identify the financial report PDF.",
    )
    parser.add_argument(
        "--convert-only", action="store_true",
        help="Skip download; only identify and convert already-downloaded files.",
    )
    parser.add_argument(
        "--identify-only", action="store_true",
        help="Only identify which PDF is the financial report; do not convert.",
    )
    parser.add_argument(
        "--no-convert", action="store_true",
        help="Download and identify only; skip docling conversion.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would happen without actually downloading or converting.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-convert even if results already exist (skip the existence check).",
    )
    parser.add_argument(
        "--parse-xlsx", action="store_true",
        help="Scan for FinancialStatement-*.xlsx files and parse them into DuckDB.",
    )
    parser.add_argument(
        "--scan-disk", action="store_true",
        help="Scan reports/ directory and register all PDF/XLSX files in DuckDB "
        "(for manually downloaded files, before --convert-only).",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show pipeline job status dashboard and exit.",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Re-run pipeline for any jobs with failed stages.",
    )
    parser.add_argument(
        "--stage", type=str, default=None,
        choices=["download", "identify", "collect", "convert"],
        help="Limit --retry-failed to a specific stage.",
    )

    args = parser.parse_args()

    tickers = None
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    years = None
    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]

    periods = None
    if args.periods:
        periods = [p.strip() for p in args.periods.split(",")]

    # ── Status mode ──────────────────────────────────────────────
    if args.status:
        conn = get_db(DB_PATH)
        try:
            db = PipelineDB(conn)
            db.init_schema()
            db.print_status()
        finally:
            conn.close()
        return

    # ── Parse XLSX only mode ─────────────────────────────────────
    if args.parse_xlsx and not args.convert_only and \
       not args.identify_only and not args.retry_failed:
        conn = get_db(DB_PATH)
        try:
            db = PipelineDB(conn)
            db.init_schema()
            parse_xlsx_stage(db, tickers=tickers, years=years, periods=periods,
                            dry_run=args.dry_run)
        finally:
            conn.close()
        return

    # ── Scan-disk mode ───────────────────────────────────────────
    if args.scan_disk:
        conn = get_db(DB_PATH)
        try:
            db = PipelineDB(conn)
            db.init_schema()
            scan_disk_and_register(db, REPORTS_DIR, tickers=tickers,
                                  dry_run=args.dry_run)
        finally:
            conn.close()
        return

    # ── Normal pipeline run ──────────────────────────────────────
    run_pipeline(
        tickers=tickers,
        years=years,
        periods=periods,
        strategy=args.strategy,
        output_dir=args.output,
        delay=args.delay,
        download=not args.convert_only,
        convert=not args.no_convert and not args.parse_xlsx,
        identify_only=args.identify_only,
        dry_run=args.dry_run,
        parse_xlsx=args.parse_xlsx,
        retry_failed=args.retry_failed,
        retry_stage=args.stage,
        force=args.force,
    )


if __name__ == "__main__":
    main()
