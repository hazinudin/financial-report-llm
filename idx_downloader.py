"""
IDX Financial Report Downloader

Downloads financial reports (PDF) and structured data for all companies
listed on the Indonesia Stock Exchange (IDX) using the official IDX API.

Based on the NeaByteLab/IDX-API TypeScript project:
https://github.com/NeaByteLab/IDX-API

Usage:
    # Download everything (all companies, all periods, recent years)
    python idx_downloader.py

    # Download for specific tickers only
    python idx_downloader.py --tickers MHKI,ESSA,BBCA

    # Download only annual (audit) reports
    python idx_downloader.py --periods audit

    # Download specific years
    python idx_downloader.py --years 2023,2024

    # Download financial ratio data only (no PDFs)
    python idx_downloader.py --ratios-only

    # Dry run - see what would be downloaded
    python idx_downloader.py --dry-run

    # Combine options
    python idx_downloader.py --tickers MHKI,ESSA --years 2024 --periods audit,TW3
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

from curl_cffi import requests as cffi_requests

BASE_URL = "https://www.idx.co.id"
BROWSER_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Referer": "https://www.idx.co.id/",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
}
VALID_PERIODS = ("TW1", "TW2", "TW3", "audit")
DEFAULT_YEARS = list(range(2020, 2026))
DEFAULT_PERIODS = ["audit"]
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1
DEFAULT_DELAY = 1.5
JITTER = 0.5


class IDXClient:
    """Client for the Indonesia Stock Exchange (IDX) public API."""

    def __init__(self, output_dir: str = "./idx_data", delay: float = DEFAULT_DELAY):
        self.session = cffi_requests.Session(impersonate="chrome131")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._initialized = False
        self._delay = delay

    def _throttle(self, label: str = ""):
        """Sleep with random jitter to avoid detection as automated traffic."""
        jitter = random.uniform(0, JITTER)
        wait = self._delay + jitter
        if label:
            print(f"    [wait {wait:.1f}s] {label}")
        time.sleep(wait)

    def _ensure_session(self):
        """Initialize session by fetching the main page to get cookies."""
        if self._initialized:
            return
        print("[*] Initializing IDX session...")
        try:
            resp = self.session.get(
                f"{BASE_URL}/id",
                headers=BROWSER_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            time.sleep(1)
            resp2 = self.session.get(
                f"{BASE_URL}/primary/home/GetIndexList",
                headers={**BROWSER_HEADERS, "X-Requested-With": "XMLHttpRequest"},
                timeout=30,
            )
            resp2.raise_for_status()
            self._initialized = True
            print("[*] Session initialized successfully.")
        except Exception as e:
            print(f"[!] Failed to initialize session: {e}")
            raise

    def _fetch(self, url: str, max_attempts: int = MAX_RETRIES) -> dict | None:
        """Fetch JSON from URL with retry logic."""
        for attempt in range(1, max_attempts + 1):
            try:
                headers = {
                    **BROWSER_HEADERS,
                    "X-Requested-With": "XMLHttpRequest",
                }
                resp = self.session.get(url, headers=headers, timeout=30)
                if resp.status_code >= 500:
                    print(
                        f"[!] Server error {resp.status_code}, retry {attempt}/{max_attempts}..."
                    )
                    delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), 15)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if attempt >= max_attempts:
                    print(f"[!] Max retries reached for {url}: {e}")
                    return None
                delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), 15)
                print(
                    f"[!] Retry {attempt}/{max_attempts} for {url} "
                    f"(wait {delay:.1f}s): {e}"
                )
                time.sleep(delay)
        return None

    def get_all_tickers(self) -> list[dict]:
        """Fetch all listed company tickers from IDX."""
        self._ensure_session()
        print("[*] Fetching all listed companies...")
        data = self._fetch(
            f"{BASE_URL}/primary/ListedCompany/GetCompanyProfiles?start=0&length=9999"
        )
        if not data or "data" not in data:
            print("[!] Failed to fetch company profiles.")
            return []
        companies = []
        for item in data["data"]:
            companies.append(
                {
                    "code": item.get("KodeEmiten", "").strip(),
                    "name": item.get("NamaEmiten", "").strip(),
                    "listing_date": item.get("TanggalPencatatan", ""),
                }
            )
        print(f"[*] Found {len(companies)} listed companies.")
        return companies

    def get_financial_reports(
        self,
        ticker: str,
        year: int,
        period: str = "audit",
        page_size: int = 100,
    ) -> list[dict]:
        """
        Fetch financial report metadata (including PDF download links).

        Args:
            ticker: Company ticker (e.g. 'MHKI')
            year: Fiscal year (e.g. 2024)
            period: Report period - 'TW1' (Q1), 'TW2' (Q2), 'TW3' (Q3), 'audit' (annual)
            page_size: Number of results per page
        """
        self._ensure_session()
        data = self._fetch(
            f"{BASE_URL}/primary/ListedCompany/GetFinancialReport?"
            f"periode={period}&year={year}&indexFrom=0"
            f"&pageSize={page_size}&reportType=rdf"
            f"&kodeEmiten={ticker}"
        )
        if not data or "Results" not in data:
            return []
        reports = []
        for item in data["Results"]:
            attachments = []
            for att in item.get("Attachments", []):
                attachments.append(
                    {
                        "id": att.get("File_ID", ""),
                        "name": att.get("File_Name", ""),
                        "path": att.get("File_Path", ""),
                        "size": att.get("File_Size", 0),
                        "type": att.get("File_Type", ""),
                        "modified": att.get("File_Modified", ""),
                    }
                )
            reports.append(
                {
                    "code": item.get("KodeEmiten", "").strip(),
                    "name": item.get("NamaEmiten", "").strip(),
                    "year": int(item.get("Report_Year", 0)),
                    "period": item.get("Report_Period", ""),
                    "attachments": attachments,
                }
            )
        return reports

    def get_financial_ratios(self, year: int, month: int = 6) -> list[dict]:
        """
        Fetch financial ratios for all companies for a given period.

        Args:
            year: Target year
            month: Target month (1-12)
        """
        self._ensure_session()
        data = self._fetch(
            f"{BASE_URL}/primary/DigitalStatistic/GetApiDataPaginated?"
            f"urlName=LINK_FINANCIAL_DATA_RATIO"
            f"&periodYear={year}&periodMonth={month}"
            f"&periodType=monthly&isPrint=False&cumulative=False"
        )
        if not data or "data" not in data:
            return []
        ratios = []
        for item in data["data"]:
            ratios.append(
                {
                    "code": item.get("code", ""),
                    "name": item.get("stockName", ""),
                    "sector": item.get("sector", ""),
                    "sub_sector": item.get("subSector", ""),
                    "industry": item.get("industry", ""),
                    "sub_industry": item.get("subIndustry", ""),
                    "period": item.get("fsDate", ""),
                    "assets": item.get("assets", 0),
                    "liabilities": item.get("liabilities", 0),
                    "equity": item.get("equity", 0),
                    "sales": item.get("sales", 0),
                    "ebt": item.get("ebt", 0),
                    "profit": item.get("profitPeriod", 0),
                    "eps": item.get("eps", 0),
                    "book_value": item.get("bookValue", 0),
                    "per": item.get("per", 0),
                    "pbv": item.get("priceBV", 0),
                    "der": item.get("deRatio", 0),
                    "roa": item.get("roa", 0),
                    "roe": item.get("roe", 0),
                    "npm": item.get("npm", 0),
                }
            )
        return ratios

    def get_company_detail(self, ticker: str) -> dict | None:
        """Fetch detailed profile for a specific company."""
        self._ensure_session()
        data = self._fetch(
            f"{BASE_URL}/primary/ListedCompany/GetCompanyProfilesDetail?"
            f"KodeEmiten={ticker}&language=id-id"
        )
        if not data or not data.get("Profiles"):
            return None
        profile = data["Profiles"][0]
        return {
            "code": profile.get("KodeEmiten", ""),
            "name": profile.get("NamaEmiten", ""),
            "sector": profile.get("Sektor", ""),
            "sub_sector": profile.get("SubSektor", ""),
            "industry": profile.get("Industri", ""),
            "sub_industry": profile.get("SubIndustri", ""),
            "board": profile.get("PapanPencatatan", ""),
            "listing_date": profile.get("TanggalPencatatan", ""),
            "website": profile.get("Website", ""),
            "email": profile.get("Email", ""),
            "phone": profile.get("Telepon", ""),
            "address": profile.get("Alamat", ""),
        }

    def download_pdf(self, url: str, filepath: Path) -> bool:
        """Download a PDF file from a URL."""
        try:
            headers = {**BROWSER_HEADERS}
            resp = self.session.get(url, headers=headers, timeout=60, stream=True)
            if resp.status_code == 200:
                filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                return True
            else:
                print(f"[!] HTTP {resp.status_code} downloading {url}")
                return False
        except Exception as e:
            print(f"[!] Error downloading {url}: {e}")
            return False


def list_companies(
    output_dir: str = "./idx_data", delay: float = DEFAULT_DELAY, export: str = "csv"
):
    """Fetch and list all listed companies with their sector/industry classification."""
    client = IDXClient(output_dir=output_dir, delay=delay)
    companies = client.get_all_tickers()

    if not companies:
        print("[!] No companies found.")
        return

    print(f"\n[*] Fetching details for {len(companies)} companies...\n")

    all_data = []
    for idx, company in enumerate(companies):
        code = company["code"]
        detail = client.get_company_detail(code)
        if detail:
            all_data.append(detail)
            client._throttle(f"fetched {code}")

        if (idx + 1) % 50 == 0:
            print(f"[*] Progress: {idx + 1}/{len(companies)} companies")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_file = output_path / "idx_companies.csv"
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write(
            "code,name,sector,sub_sector,industry,sub_industry,board,listing_date,website,email\n"
        )
        for c in all_data:
            f.write(
                f'"{c["code"]}","{c["name"]}","{c["sector"]}","{c["sub_sector"]}","{c["industry"]}","{c["sub_industry"]}","{c["board"]}","{c["listing_date"]}","{c["website"]}","{c["email"]}"\n'
            )

    json_file = output_path / "idx_companies.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n[*] Exported {len(all_data)} companies to:")
    print(f"    CSV:  {csv_file}")
    print(f"    JSON: {json_file}")

    print(f"\n{'Code':<8} {'Name':<45} {'Sector':<25} {'Sub-Sector':<20}")
    print("=" * 100)
    for c in all_data[:20]:
        print(f"{c['code']:<8} {c['name']:<45} {c['sector']:<25} {c['sub_sector']:<20}")
    if len(all_data) > 20:
        print(f"... and {len(all_data) - 20} more (see exported files)")


def sanitize_filename(name: str) -> str:
    """Remove or replace characters that are invalid in filenames."""
    for ch in ["\\", "/", ":", "*", "?", '"', "<", ">", "|"]:
        name = name.replace(ch, "_")
    return name.strip()


def run(
    tickers: list[str] | None = None,
    years: list[int] | None = None,
    periods: list[str] | None = None,
    ratios_only: bool = False,
    pdf_only: bool = False,
    dry_run: bool = False,
    output_dir: str = "./idx_data",
    delay: float = 0.5,
):
    """Main download orchestration."""
    client = IDXClient(output_dir=output_dir, delay=delay)

    if years is None:
        years = DEFAULT_YEARS
    if periods is None:
        periods = list(DEFAULT_PERIODS)

    for p in periods:
        if p not in VALID_PERIODS:
            print(f"[!] Invalid period '{p}'. Must be one of: {VALID_PERIODS}")
            sys.exit(1)

    if tickers:
        companies = [
            {"code": t.strip().upper(), "name": t.strip().upper()} for t in tickers
        ]
    else:
        companies = client.get_all_tickers()
        if not companies:
            print("[!] No companies found. Exiting.")
            sys.exit(1)

    print(f"\n[*] Will process {len(companies)} companies")
    print(f"[*] Years: {years}")
    print(f"[*] Periods: {periods}")
    print(f"[*] Output directory: {output_dir}")
    if dry_run:
        print("[*] DRY RUN - no files will be downloaded\n")

    all_metadata = []
    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for idx, company in enumerate(companies):
        code = company["code"]
        name = sanitize_filename(company.get("name", code))
        print(f"\n[{idx + 1}/{len(companies)}] Processing {code} ({name})")

        if idx > 0:
            client._throttle(f"next company {code}")

        for year in years:
            for period in periods:
                reports = client.get_financial_reports(code, year, period)
                client._throttle(f"{code} {year} {period}")

                if not reports:
                    continue

                for report in reports:
                    report_year = report.get("year", year)
                    report_period = report.get("period", period)
                    attachments = report.get("attachments", [])

                    if not attachments:
                        continue

                    for att in attachments:
                        file_path = att.get("path", "")
                        file_name = sanitize_filename(att.get("name", ""))

                        if not file_path:
                            continue

                        period_dir = f"{report_year}_{report_period}"
                        dest_dir = Path(output_dir) / "reports" / code / period_dir
                        dest_file = dest_dir / file_name if file_name else None

                        meta_entry = {
                            "ticker": code,
                            "company_name": report.get("name", ""),
                            "year": report_year,
                            "period": report_period,
                            "file_name": file_name,
                            "file_path": file_path,
                            "file_size": att.get("size", 0),
                            "file_type": att.get("type", ""),
                            "local_path": str(dest_file) if dest_file else "",
                        }
                        all_metadata.append(meta_entry)

                        if ratios_only:
                            continue

                        if dest_file and dest_file.exists():
                            total_skipped += 1
                            print(f"  [SKIP] {file_name} (already exists)")
                            continue

                        if dry_run:
                            print(f"  [DRY] Would download: {file_name}")
                            total_skipped += 1
                            continue

                        if not file_path.startswith("http"):
                            file_url = (
                                f"{BASE_URL}{file_path}"
                                if file_path.startswith("/")
                                else f"{BASE_URL}/{file_path}"
                            )
                        else:
                            file_url = file_path

                        print(f"  [DL] {file_name} -> {dest_dir}/")
                        success = client.download_pdf(file_url, dest_file)
                        if success:
                            total_downloaded += 1
                        else:
                            total_failed += 1

                        client._throttle(f"downloaded {file_name}")

    if not ratios_only:
        print("\n" + "=" * 60)
        print(f"[*] Download complete.")
        print(f"    Downloaded: {total_downloaded}")
        print(f"    Skipped (exists): {total_skipped}")
        print(f"    Failed: {total_failed}")

    if not pdf_only:
        print("\n[*] Downloading financial ratio data...")
        ratio_dir = Path(output_dir) / "ratios"
        ratio_dir.mkdir(parents=True, exist_ok=True)

        for year in years:
            for month in [6, 12]:
                print(f"  Fetching ratios for {year}-{month:02d}...")
                ratios = client.get_financial_ratios(year, month)
                if ratios:
                    ratio_file = ratio_dir / f"ratios_{year}_{month:02d}.json"
                    with open(ratio_file, "w", encoding="utf-8") as f:
                        json.dump(ratios, f, ensure_ascii=False, indent=2)
                    print(f"    Saved {len(ratios)} records to {ratio_file}")
                else:
                    print(f"    No data for {year}-{month:02d}")
                client._throttle(f"ratios {year}-{month:02d}")

    metadata_file = Path(output_dir) / "download_metadata.json"
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "companies_processed": len(companies),
                "years": years,
                "periods": periods,
                "total_reports_found": len(all_metadata),
                "reports": all_metadata,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"\n[*] Metadata saved to {metadata_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Download financial reports from IDX (Indonesia Stock Exchange)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated list of ticker symbols (e.g., MHKI,ESSA,BBCA). "
        "If omitted, downloads for all listed companies.",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years (e.g., 2023,2024). Default: 2020-2025",
    )
    parser.add_argument(
        "--periods",
        type=str,
        default=None,
        help="Comma-separated periods (TW1,TW2,TW3,audit). Default: audit",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./idx_data",
        help="Output directory. Default: ./idx_data",
    )
    parser.add_argument(
        "--list-companies",
        action="store_true",
        help="List all listed companies with sector/industry classification and exit",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY}). "
        f"A random jitter of 0-{JITTER}s is added automatically.",
    )
    parser.add_argument(
        "--ratios-only",
        action="store_true",
        help="Only download financial ratio data (skip PDF downloads)",
    )
    parser.add_argument(
        "--pdf-only",
        action="store_true",
        help="Only download PDF reports (skip ratio data)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading",
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

    if args.list_companies:
        list_companies(output_dir=args.output, delay=args.delay)
        sys.exit(0)

    run(
        tickers=tickers,
        years=years,
        periods=periods,
        ratios_only=args.ratios_only,
        pdf_only=args.pdf_only,
        dry_run=args.dry_run,
        output_dir=args.output,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
