"""
Sync script: populate DuckDB with the current state of the project.

Scans reports/, raw/, results/ directories and registers all files,
pipeline stages, conversions, and XLSX statement data.
"""

import os
from pathlib import Path

from db import init_database, PipelineDB, get_db

DB_PATH = Path("./idx_data/pipeline.duckdb")
REPORTS_DIR = Path("./idx_data/reports")
RAW_DIR = Path("./raw")
RESULTS_DIR = Path("./results")

PERIOD_REVERSE = {"Q1": "tw1", "Q2": "tw2", "Q3": "tw3", "Q4": "audit"}


def sync_all():
    # 1. Init DB with migration from JSON files
    print("=" * 60)
    print("[1] Initializing database + migrating from JSON")
    conn = init_database(DB_PATH, migrate=True)
    db = PipelineDB(conn)

    # 2. Scan reports/ and register all files
    print("\n" + "=" * 60)
    print("[2] Scanning reports/ and registering files")
    registered = 0
    for ticker_dir in sorted(REPORTS_DIR.iterdir()):
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        for period_dir in sorted(ticker_dir.iterdir()):
            if not period_dir.is_dir():
                continue
            dir_name = period_dir.name  # e.g. "2026_TW1" or "2025_Audit"
            if "_" not in dir_name:
                continue
            year_str, period_str = dir_name.split("_", 1)
            try:
                year_val = int(year_str)
            except ValueError:
                continue
            period_val = period_str.lower()
            job_id = f"{ticker}_{year_val}_{period_val}"

            # Ensure pipeline job exists
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
                meta = {
                    "ticker": ticker,
                    "company_name": "",
                    "year": year_val,
                    "period": period_val,
                    "file_name": f.name,
                    "file_path": "",
                    "file_size": f.stat().st_size,
                    "file_type": suffix,
                    "local_path": str(f),
                }
                db.upsert_downloaded_file(meta, job_id=job_id)
                registered += 1
    print(f"  Registered {registered} files")

    # 3. Mark download stage as completed for all jobs with files
    print("\n[3] Marking download stage as completed")
    result = db.conn.execute(
        "SELECT DISTINCT job_id FROM downloaded_files"
    ).fetchall()
    for (jid,) in result:
        if db.get_stage_status(jid, "download") != "completed":
            db.mark_stage(jid, "download", "completed")
    print(f"  Updated {len(result)} jobs")

    # 4. Scan raw/ and mark identify + collect stages
    print("\n[4] Scanning raw/ for collected PDFs")
    if RAW_DIR.exists():
        identified_count = 0
        for pdf_path in sorted(RAW_DIR.glob("*.pdf")):
            stem = pdf_path.stem  # e.g. "JARR_Q4_2025"
            parts = stem.rsplit("_", 2)
            if len(parts) != 3:
                continue
            ticker, quarter, year_str = parts
            period = PERIOD_REVERSE.get(quarter, quarter.lower())
            year_val = int(year_str)
            jid = f"{ticker}_{year_val}_{period}"

            # Ensure job exists
            db.conn.execute(
                "INSERT OR IGNORE INTO pipeline_jobs (job_id, ticker, year, period) VALUES ($1, $2, $3, $4)",
                (jid, ticker, year_val, period),
            )

            # Mark identify and collect as completed
            if db.get_stage_status(jid, "identify") != "completed":
                db.mark_stage(jid, "identify", "completed")
            if db.get_stage_status(jid, "collect") != "completed":
                db.mark_stage(jid, "collect", "completed")
            identified_count += 1
        print(f"  Processed {identified_count} collected PDFs")

    # 5. Scan results/ and register converted documents
    print("\n[5] Scanning results/ for converted markdown files")
    if RESULTS_DIR.exists():
        converted_count = 0
        for md_path in sorted(RESULTS_DIR.glob("*.md")):
            stem = md_path.stem  # e.g. "ELSA_Q1_2026"
            parts = stem.rsplit("_", 2)
            if len(parts) != 3:
                continue
            ticker, quarter, year_str = parts
            period = PERIOD_REVERSE.get(quarter, quarter.lower())
            year_val = int(year_str)
            jid = f"{ticker}_{year_val}_{period}"

            # Ensure job exists
            db.conn.execute(
                "INSERT OR IGNORE INTO pipeline_jobs (job_id, ticker, year, period) VALUES ($1, $2, $3, $4)",
                (jid, ticker, year_val, period),
            )

            # Find the source PDF in raw/
            source_pdf = RAW_DIR / f"{stem}.pdf"
            source_str = str(source_pdf) if source_pdf.exists() else ""
            file_size = md_path.stat().st_size

            # Register conversion
            db.upsert_conversion(
                job_id=jid,
                source_pdf=source_str,
                output_md=str(md_path),
                status="success",
                elapsed_sec=0,
            )

            # Mark convert stage as completed
            if db.get_stage_status(jid, "convert") != "completed":
                db.mark_stage(jid, "convert", "completed")
            converted_count += 1
        print(f"  Registered {converted_count} converted documents")

    # 6. Parse XLSX files into xlsx_statements
    print("\n[6] Parsing XLSX financial statements")
    from xlsx_parser import parse_xlsx
    xlsx_files = list(REPORTS_DIR.rglob("FinancialStatement-*.xlsx"))
    total_rows = 0
    for xf in sorted(xlsx_files):
        try:
            period_dir = xf.parent
            year_period = period_dir.name
            ticker_dir = period_dir.parent
            ticker_val = ticker_dir.name
            if "_" not in year_period:
                continue
            year_str, period_str = year_period.split("_", 1)
            year_val = int(year_str)
            period_val = period_str.lower()

            if db.has_xlsx_data(ticker_val, year_val, period_val):
                continue

            meta, statements = parse_xlsx(
                str(xf), ticker=ticker_val, year=year_val, period=period_val,
            )
            if meta:
                db.upsert_xlsx_meta(meta)
            if statements:
                db.upsert_xlsx_statements(statements)
                total_rows += len(statements)
                print(f"  {ticker_val} {year_val} {period_val}: {len(statements)} rows")
        except Exception as e:
            ticker_str = xf.parent.parent.name if xf.parent.parent.name else "?"
            print(f"  [SKIP] {ticker_str} {xf.name}: {e}")
    print(f"  Total XLSX rows inserted: {total_rows}")

    # 7. Print summary
    print("\n" + "=" * 60)
    print("[*] Sync complete!")

    # Connection counts
    companies = db.get_all_companies()
    files = db.get_downloaded_files()
    ratios_count = conn.execute("SELECT COUNT(*) FROM financial_ratios").fetchone()[0]
    xlsx_rows = conn.execute("SELECT COUNT(*) FROM xlsx_statements").fetchone()[0]
    conversions = conn.execute("SELECT COUNT(*) FROM converted_documents").fetchone()[0]
    jobs = conn.execute("SELECT COUNT(*) FROM pipeline_jobs").fetchone()[0]

    print(f"  Companies:        {len(companies)}")
    print(f"  Downloaded files: {len(files)}")
    print(f"  Financial ratios: {ratios_count}")
    print(f"  XLSX statements:  {xlsx_rows} rows")
    print(f"  Conversions:      {conversions}")
    print(f"  Pipeline jobs:    {jobs}")

    conn.close()


if __name__ == "__main__":
    sync_all()
