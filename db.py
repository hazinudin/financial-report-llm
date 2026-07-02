"""
DuckDB persistent layer for the IDX Financial Report Pipeline.

Stores pipeline job state, downloaded file metadata, company profiles,
financial ratios, XLSX statement data, and conversion results.

Usage:
    from db import get_db, PipelineDB

    with get_db("./idx_data/pipeline.duckdb") as conn:
        PipelineDB(conn).upsert_job(...)
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path("./idx_data/pipeline.duckdb")
IDX_DATA_DIR = Path("./idx_data")


def get_db(db_path: str | Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection and initialize schema if needed."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=read_only)
    if not read_only:
        conn.execute("PRAGMA wal_autocheckpoint='32MB'")
    return conn


class PipelineDB:
    """DuckDB-backed pipeline state and data store."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def _rows_to_dicts(self, result) -> list[dict]:
        """Convert a DuckDB Result to a list of dicts keyed by column name."""
        cols = [d[0] for d in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    # ── Schema ────────────────────────────────────────────────────────

    def init_schema(self):
        """Create all tables and indexes if they don't exist."""
        # Sequences must exist before tables that reference them
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_downloaded_files")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_converted")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_jobs (
                job_id       VARCHAR PRIMARY KEY,
                ticker       VARCHAR NOT NULL,
                year         INTEGER NOT NULL,
                period       VARCHAR NOT NULL,
                stage_download  VARCHAR DEFAULT 'pending',
                stage_identify  VARCHAR DEFAULT 'pending',
                stage_collect   VARCHAR DEFAULT 'pending',
                stage_convert   VARCHAR DEFAULT 'pending',
                error_download  VARCHAR,
                error_identify  VARCHAR,
                error_collect   VARCHAR,
                error_convert   VARCHAR,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_files (
                file_id       BIGINT PRIMARY KEY DEFAULT nextval('seq_downloaded_files'),
                job_id        VARCHAR,
                ticker        VARCHAR NOT NULL,
                company_name  VARCHAR,
                year          INTEGER NOT NULL,
                period        VARCHAR NOT NULL,
                file_name     VARCHAR NOT NULL,
                file_path     VARCHAR,
                file_size     BIGINT,
                file_type     VARCHAR,
                local_path    VARCHAR,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (ticker, year, period, file_name)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS company_profiles (
                code          VARCHAR PRIMARY KEY,
                name          VARCHAR,
                sector        VARCHAR,
                sub_sector    VARCHAR,
                industry      VARCHAR,
                sub_industry  VARCHAR,
                board         VARCHAR,
                listing_date  VARCHAR,
                website       VARCHAR,
                email         VARCHAR,
                phone         VARCHAR,
                address       VARCHAR,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_ratios (
                code          VARCHAR NOT NULL,
                name          VARCHAR,
                sector        VARCHAR,
                sub_sector    VARCHAR,
                industry      VARCHAR,
                sub_industry  VARCHAR,
                period        VARCHAR NOT NULL,
                assets        DOUBLE,
                liabilities   DOUBLE,
                equity        DOUBLE,
                sales         DOUBLE,
                ebt           DOUBLE,
                profit        DOUBLE,
                eps           DOUBLE,
                book_value    DOUBLE,
                per           DOUBLE,
                pbv           DOUBLE,
                der           DOUBLE,
                roa           DOUBLE,
                roe           DOUBLE,
                npm           DOUBLE,
                fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (code, period)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS converted_documents (
                convert_id    BIGINT PRIMARY KEY DEFAULT nextval('seq_converted'),
                job_id        VARCHAR,
                source_pdf    VARCHAR NOT NULL,
                output_md     VARCHAR NOT NULL,
                status        VARCHAR DEFAULT 'pending',
                error         VARCHAR,
                elapsed_sec   DOUBLE,
                converted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS xlsx_meta (
                ticker               VARCHAR NOT NULL,
                year                 INTEGER NOT NULL,
                period               VARCHAR NOT NULL,
                entity_name          VARCHAR,
                entity_code          VARCHAR,
                period_start         VARCHAR,
                period_end           VARCHAR,
                prior_period_start   VARCHAR,
                prior_period_end     VARCHAR,
                currency             VARCHAR,
                rounding             VARCHAR,
                audit_status         VARCHAR,
                auditor              VARCHAR,
                consolidation_type   VARCHAR,
                PRIMARY KEY (ticker, year, period)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS xlsx_statements (
                ticker         VARCHAR NOT NULL,
                year           INTEGER NOT NULL,
                period         VARCHAR NOT NULL,
                sheet_code     VARCHAR NOT NULL,
                sheet_title    VARCHAR,
                row_number     INTEGER NOT NULL,
                label_id       VARCHAR,
                value_current  DOUBLE,
                value_prior    DOUBLE,
                label_en       VARCHAR,
                context_type   VARCHAR,
                PRIMARY KEY (ticker, year, period, sheet_code, row_number)
            )
        """)

    # ── Migration from JSON files ─────────────────────────────────────

    def migrate_from_json(self):
        """Migrate data from legacy JSON/CSV files into DuckDB tables."""
        self._migrate_companies()
        self._migrate_downloads()
        self._migrate_ratios()

    def _migrate_companies(self):
        json_path = IDX_DATA_DIR / "idx_companies.json"
        csv_path = IDX_DATA_DIR / "idx_companies.csv"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                companies = json.load(f)
            rows = [
                (
                    c.get("code", ""),
                    c.get("name", ""),
                    c.get("sector", ""),
                    c.get("sub_sector", ""),
                    c.get("industry", ""),
                    c.get("sub_industry", ""),
                    c.get("board", ""),
                    c.get("listing_date", ""),
                    c.get("website", ""),
                    c.get("email", ""),
                    c.get("phone", ""),
                    c.get("address", ""),
                )
                for c in companies
            ]
            self.conn.executemany(
                """INSERT OR REPLACE INTO company_profiles
                   (code, name, sector, sub_sector, industry, sub_industry,
                    board, listing_date, website, email, phone, address)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            print(f"[db] Migrated {len(rows)} company profiles from idx_companies.json")
        elif csv_path.exists():
            self.conn.execute(f"""
                INSERT OR REPLACE INTO company_profiles
                SELECT regexp_replace(column0, '^"|"$', '', 'g') AS code,
                       regexp_replace(column1, '^"|"$', '', 'g') AS name,
                       regexp_replace(column2, '^"|"$', '', 'g') AS sector,
                       regexp_replace(column3, '^"|"$', '', 'g') AS sub_sector,
                       regexp_replace(column4, '^"|"$', '', 'g') AS industry,
                       regexp_replace(column5, '^"|"$', '', 'g') AS sub_industry,
                       regexp_replace(column6, '^"|"$', '', 'g') AS board,
                       regexp_replace(column7, '^"|"$', '', 'g') AS listing_date,
                       regexp_replace(column8, '^"|"$', '', 'g') AS website,
                       regexp_replace(column9, '^"|"$', '', 'g') AS email
                FROM read_csv_auto('{csv_path}', header=True)
            """)
            print(f"[db] Migrated company profiles from idx_companies.csv")

    def _migrate_downloads(self):
        json_path = IDX_DATA_DIR / "download_metadata.json"
        if not json_path.exists():
            return
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        reports = data.get("reports", [])
        if not reports:
            return

        # First, create pipeline_jobs for every (ticker, year, period) combo
        jobs_seen = set()
        job_rows = []
        for r in reports:
            ticker = r.get("ticker", "")
            year_val = r.get("year", 0)
            period_val = r.get("period", "")
            jid = f"{ticker}_{year_val}_{period_val}"
            if jid not in jobs_seen:
                jobs_seen.add(jid)
                job_rows.append((jid, ticker, year_val, period_val))
        if job_rows:
            self.conn.executemany(
                """INSERT OR IGNORE INTO pipeline_jobs (job_id, ticker, year, period)
                   VALUES (?, ?, ?, ?)""",
                job_rows,
            )

        rows = []
        for r in reports:
            ticker = r.get("ticker", "")
            year_val = r.get("year", 0)
            period_val = r.get("period", "")
            job_id = f"{ticker}_{year_val}_{period_val}"
            rows.append((
                job_id, ticker, r.get("company_name", ""), year_val,
                period_val, r.get("file_name", ""), r.get("file_path", ""),
                r.get("file_size", 0), r.get("file_type", ""),
                r.get("local_path", ""),
            ))
        self.conn.executemany(
            """INSERT OR IGNORE INTO downloaded_files
               (job_id, ticker, company_name, year, period,
                file_name, file_path, file_size, file_type, local_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        print(f"[db] Migrated {len(rows)} downloaded files from download_metadata.json")

    def _migrate_ratios(self):
        ratios_dir = IDX_DATA_DIR / "ratios"
        if not ratios_dir.exists():
            return
        total = 0
        for rfile in sorted(ratios_dir.glob("ratios_*.json")):
            with open(rfile, "r", encoding="utf-8") as f:
                ratio_data = json.load(f)
            rows = [
                (
                    r.get("code", ""),
                    r.get("name", ""),
                    r.get("sector", ""),
                    r.get("sub_sector", ""),
                    r.get("industry", ""),
                    r.get("sub_industry", ""),
                    r.get("period", ""),
                    r.get("assets", 0),
                    r.get("liabilities", 0),
                    r.get("equity", 0),
                    r.get("sales", 0),
                    r.get("ebt", 0),
                    r.get("profit", 0),
                    r.get("eps", 0),
                    r.get("book_value", 0),
                    r.get("per", 0),
                    r.get("pbv", 0),
                    r.get("der", 0),
                    r.get("roa", 0),
                    r.get("roe", 0),
                    r.get("npm", 0),
                )
                for r in ratio_data
            ]
            self.conn.executemany(
                """INSERT OR REPLACE INTO financial_ratios
                   (code, name, sector, sub_sector, industry, sub_industry,
                    period, assets, liabilities, equity, sales, ebt, profit,
                    eps, book_value, per, pbv, der, roa, roe, npm)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            total += len(rows)
        print(f"[db] Migrated {total} financial ratios from ratios/")

    # ── Pipeline Jobs ──────────────────────────────────────────────────

    def job_id(self, ticker: str, year: int, period: str) -> str:
        return f"{ticker}_{year}_{period}"

    def upsert_job(
        self,
        ticker: str,
        year: int,
        period: str,
        **stage_updates,
    ):
        """Create or update a pipeline job entry."""
        jid = self.job_id(ticker, year, period)
        self.conn.execute(
            """INSERT INTO pipeline_jobs (job_id, ticker, year, period)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (job_id) DO UPDATE SET updated_at=now()""",
            (jid, ticker, year, period),
        )
        valid_columns = {
            "stage_download", "stage_identify", "stage_collect", "stage_convert",
            "error_download", "error_identify", "error_collect", "error_convert",
        }
        for col_name, value in stage_updates.items():
            if col_name not in valid_columns:
                raise ValueError(f"Unknown column: {col_name}. Valid: {valid_columns}")
            self.conn.execute(
                f"UPDATE pipeline_jobs SET {col_name}=$1, updated_at=now() WHERE job_id=$2",
                (value, jid),
            )

    def mark_stage(self, job_id: str, stage: str, status: str, error: str | None = None):
        """Update a specific stage column for a job."""
        valid_stages = {"download", "identify", "collect", "convert"}
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage: {stage}. Must be one of {valid_stages}")
        col = f"stage_{stage}"
        err_col = f"error_{stage}"
        self.conn.execute(
            f"UPDATE pipeline_jobs SET {col}=?, {err_col}=?, updated_at=now() WHERE job_id=?",
            (status, error, job_id),
        )

    def get_jobs(
        self,
        ticker: str | None = None,
        year: int | None = None,
        period: str | None = None,
        stage_filter: str | None = None,
        status_filter: str | None = None,
    ) -> list[dict]:
        """Query pipeline jobs with optional filters."""
        where = []
        params = []
        if ticker:
            where.append("ticker=?")
            params.append(ticker)
        if year is not None:
            where.append("year=?")
            params.append(year)
        if period:
            where.append("period=?")
            params.append(period)
        if stage_filter and status_filter:
            valid_stages = {"download", "identify", "collect", "convert"}
            if stage_filter not in valid_stages:
                raise ValueError(f"Invalid stage: {stage_filter}")
            where.append(f"stage_{stage_filter}=?")
            params.append(status_filter)
        clause = "WHERE " + " AND ".join(where) if where else ""
        result = self.conn.execute(
            f"SELECT * FROM pipeline_jobs {clause} ORDER BY ticker, year, period",
            params,
        )
        return self._rows_to_dicts(result)

    def get_failed_jobs(self, stage: str | None = None) -> list[dict]:
        """Return jobs with any failed stage, optionally filtered by stage."""
        if stage:
            valid_stages = {"download", "identify", "collect", "convert"}
            if stage not in valid_stages:
                raise ValueError(f"Invalid stage: {stage}")
            result = self.conn.execute(
                f"SELECT * FROM pipeline_jobs WHERE stage_{stage} = 'failed' ORDER BY ticker, year, period"
            )
        else:
            result = self.conn.execute(
                """SELECT * FROM pipeline_jobs
                   WHERE stage_download = 'failed'
                      OR stage_identify = 'failed'
                      OR stage_collect  = 'failed'
                      OR stage_convert  = 'failed'
                   ORDER BY ticker, year, period"""
            )
        return self._rows_to_dicts(result)

    def get_stage_status(self, job_id: str, stage: str) -> str | None:
        """Get the current status of a specific stage for a job."""
        valid_stages = {"download", "identify", "collect", "convert"}
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage: {stage}")
        col = f"stage_{stage}"
        result = self.conn.execute(
            f"SELECT {col} FROM pipeline_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return result[0] if result else None

    # ── Downloaded Files ───────────────────────────────────────────────

    def upsert_downloaded_file(self, meta: dict, job_id: str | None = None):
        """Insert or update a downloaded file record."""
        ticker = meta.get("ticker", "")
        year_val = meta.get("year", 0)
        period_val = meta.get("period", "")
        if not job_id:
            job_id = self.job_id(ticker, year_val, period_val)
        self.conn.execute(
            """INSERT OR IGNORE INTO downloaded_files
               (job_id, ticker, company_name, year, period,
                file_name, file_path, file_size, file_type, local_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                ticker,
                meta.get("company_name", ""),
                year_val,
                period_val,
                meta.get("file_name", ""),
                meta.get("file_path", ""),
                meta.get("file_size", 0),
                meta.get("file_type", ""),
                meta.get("local_path", ""),
            ),
        )

    def get_downloaded_files(
        self,
        ticker: str | None = None,
        year: int | None = None,
        period: str | None = None,
    ) -> list[dict]:
        """Retrieve downloaded file records."""
        where = []
        params = []
        if ticker:
            where.append("ticker=?")
            params.append(ticker)
        if year is not None:
            where.append("year=?")
            params.append(year)
        if period:
            where.append("period=?")
            params.append(period)
        clause = "WHERE " + " AND ".join(where) if where else ""
        result = self.conn.execute(
            f"SELECT * FROM downloaded_files {clause} ORDER BY ticker, year, period, file_name",
            params,
        )
        return self._rows_to_dicts(result)

    def get_files_by_job(self, ticker: str, year: int, period: str) -> list[dict]:
        """Get all downloaded files for a specific job (ticker/year/period)."""
        return self.get_downloaded_files(ticker=ticker, year=year, period=period)

    # ── Company Profiles ───────────────────────────────────────────────

    def upsert_company(self, profile: dict):
        """Insert or update a company profile."""
        self.conn.execute(
            """INSERT OR REPLACE INTO company_profiles
               (code, name, sector, sub_sector, industry, sub_industry,
                board, listing_date, website, email, phone, address, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (
                profile.get("code", ""),
                profile.get("name", ""),
                profile.get("sector", ""),
                profile.get("sub_sector", ""),
                profile.get("industry", ""),
                profile.get("sub_industry", ""),
                profile.get("board", ""),
                profile.get("listing_date", ""),
                profile.get("website", ""),
                profile.get("email", ""),
                profile.get("phone", ""),
                profile.get("address", ""),
            ),
        )

    def get_company(self, code: str) -> dict | None:
        result = self.conn.execute(
            "SELECT * FROM company_profiles WHERE code=?", (code,)
        ).fetchone()
        if not result:
            return None
        return dict(zip(
            ["code", "name", "sector", "sub_sector", "industry", "sub_industry",
             "board", "listing_date", "website", "email", "phone", "address", "updated_at"],
            result,
        ))

    def get_all_companies(self) -> list[dict]:
        result = self.conn.execute(
            "SELECT * FROM company_profiles ORDER BY code"
        )
        return self._rows_to_dicts(result)

    # ── Financial Ratios ───────────────────────────────────────────────

    def upsert_ratios(self, ratios: list[dict]):
        """Bulk insert or update financial ratio records."""
        rows = [
            (
                r.get("code", ""),
                r.get("name", ""),
                r.get("sector", ""),
                r.get("sub_sector", ""),
                r.get("industry", ""),
                r.get("sub_industry", ""),
                r.get("period", ""),
                r.get("assets", 0),
                r.get("liabilities", 0),
                r.get("equity", 0),
                r.get("sales", 0),
                r.get("ebt", 0),
                r.get("profit", 0),
                r.get("eps", 0),
                r.get("book_value", 0),
                r.get("per", 0),
                r.get("pbv", 0),
                r.get("der", 0),
                r.get("roa", 0),
                r.get("roe", 0),
                r.get("npm", 0),
            )
            for r in ratios
        ]
        if not rows:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO financial_ratios
               (code, name, sector, sub_sector, industry, sub_industry,
                period, assets, liabilities, equity, sales, ebt, profit,
                eps, book_value, per, pbv, der, roa, roe, npm)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )

    # ── Converted Documents ────────────────────────────────────────────

    def upsert_conversion(
        self,
        job_id: str,
        source_pdf: str,
        output_md: str,
        status: str,
        error: str | None = None,
        elapsed_sec: float | None = None,
    ):
        self.conn.execute(
            """INSERT INTO converted_documents
               (job_id, source_pdf, output_md, status, error, elapsed_sec)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (job_id, source_pdf, output_md, status, error, elapsed_sec),
        )

    def get_conversions(self, job_id: str | None = None) -> list[dict]:
        if job_id:
            result = self.conn.execute(
                "SELECT * FROM converted_documents WHERE job_id=? ORDER BY converted_at DESC",
                (job_id,),
            )
        else:
            result = self.conn.execute(
                "SELECT * FROM converted_documents ORDER BY converted_at DESC"
            )
        return self._rows_to_dicts(result)

    # ── XLSX Statements ────────────────────────────────────────────────

    def upsert_xlsx_meta(self, meta: dict):
        self.conn.execute(
            """INSERT OR REPLACE INTO xlsx_meta
               (ticker, year, period, entity_name, entity_code,
                period_start, period_end, prior_period_start, prior_period_end,
                currency, rounding, audit_status, auditor, consolidation_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                meta.get("ticker", ""),
                meta.get("year", 0),
                meta.get("period", ""),
                meta.get("entity_name", ""),
                meta.get("entity_code", ""),
                meta.get("period_start", ""),
                meta.get("period_end", ""),
                meta.get("prior_period_start", ""),
                meta.get("prior_period_end", ""),
                meta.get("currency", ""),
                meta.get("rounding", ""),
                meta.get("audit_status", ""),
                meta.get("auditor", ""),
                meta.get("consolidation_type", ""),
            ),
        )

    def upsert_xlsx_statements(self, rows: list[dict]):
        if not rows:
            return
        for r in rows:
            self.conn.execute(
                """INSERT OR REPLACE INTO xlsx_statements
                   (ticker, year, period, sheet_code, sheet_title,
                    row_number, label_id, value_current, value_prior,
                    label_en, context_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["ticker"],
                    r["year"],
                    r["period"],
                    r["sheet_code"],
                    r.get("sheet_title", ""),
                    r["row_number"],
                    r["label_id"],
                    r.get("value_current"),
                    r.get("value_prior"),
                    r["label_en"],
                    r.get("context_type"),
                ),
            )

    def has_xlsx_data(self, ticker: str, year: int, period: str) -> bool:
        result = self.conn.execute(
            "SELECT COUNT(*) FROM xlsx_statements WHERE ticker=? AND year=? AND period=?",
            (ticker, year, period),
        ).fetchone()
        return result[0] > 0 if result else False

    def get_xlsx_statements(
        self,
        ticker: str | None = None,
        year: int | None = None,
        period: str | None = None,
        sheet_code: str | None = None,
    ) -> list[dict]:
        where = []
        params = []
        if ticker:
            where.append("ticker=?")
            params.append(ticker)
        if year is not None:
            where.append("year=?")
            params.append(year)
        if period:
            where.append("period=?")
            params.append(period)
        if sheet_code:
            where.append("sheet_code=?")
            params.append(sheet_code)
        clause = "WHERE " + " AND ".join(where) if where else ""
        result = self.conn.execute(
            f"SELECT * FROM xlsx_statements {clause} ORDER BY ticker, year, period, sheet_code, row_number",
            params,
        )
        return self._rows_to_dicts(result)

    # ── Status Display ─────────────────────────────────────────────────

    def print_status(self):
        """Print a pipeline job dashboard to stdout."""
        result = self.conn.execute(
            "SELECT * FROM pipeline_jobs ORDER BY ticker, year, period"
        )
        jobs = result.fetchall()
        desc = [d[0] for d in result.description]
        if not jobs:
            print("No pipeline jobs found.")
            return
        print(f"\n{'Ticker':<8} {'Year':<6} {'Period':<8} "
              f"{'Download':<12} {'Identify':<12} {'Collect':<12} {'Convert':<12} {'Error'}")
        print("-" * 100)
        stage_cols = ("download", "identify", "collect", "convert")
        for row in jobs:
            d = dict(zip(desc, row))
            stages = [d.get(f"stage_{s}", "?") for s in stage_cols]
            errors = [d.get(f"error_{s}") for s in stage_cols if d.get(f"error_{s}")]
            error_str = "; ".join(errors)[:60] if errors else ""
            status_icons = {
                "pending":   "pending",
                "running":   "running",
                "completed": "done",
                "failed":    "FAILED",
                "skipped":   "skipped",
            }
            print(
                f"{d.get('ticker', ''):<8} "
                f"{d.get('year', ''):<6} "
                f"{d.get('period', ''):<8} "
                f"{status_icons.get(stages[0], stages[0]):<12} "
                f"{status_icons.get(stages[1], stages[1]):<12} "
                f"{status_icons.get(stages[2], stages[2]):<12} "
                f"{status_icons.get(stages[3], stages[3]):<12} "
                f"{error_str}"
            )

        # Summary counts
        total = len(jobs)
        completed = 0
        failed = 0
        for row in jobs:
            d = dict(zip(desc, row))
            if all(d.get(f"stage_{s}") in ("completed", "skipped") for s in stage_cols):
                completed += 1
            if any(d.get(f"stage_{s}") == "failed" for s in stage_cols):
                failed += 1
        print(f"\nTotal: {total} | Completed: {completed} | Failed: {failed} | "
              f"Pending/In-Progress: {total - completed - failed}")


def init_database(db_path: str | Path = DEFAULT_DB_PATH, migrate: bool = True) -> duckdb.DuckDBPyConnection:
    """Convenience: open, init schema, and optional migration."""
    conn = get_db(db_path)
    db = PipelineDB(conn)
    db.init_schema()
    if migrate:
        db.migrate_from_json()
    return conn
