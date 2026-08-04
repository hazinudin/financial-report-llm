import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import xlsx_seed  # noqa: E402
from scripts.report_pipeline.xlsx_parser import parse_xlsx  # noqa: E402


def _make_workbook(path: Path) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    general = wb.create_sheet("1000000")
    general["A6"] = "Nama entitas"
    general["B6"] = "Test issuer"
    general["A7"] = "Mata uang pelaporan"
    general["B7"] = "Rupiah / IDR"
    general["A8"] = "Pembulatan"
    general["B8"] = "Satuan Penuh / Full Amount"

    rows = {
        "1311000": [
            ("1", 1000, 900, "Sales and revenue"),
            ("2", 600, 550, "Cost of sales and revenue"),
            ("3", 400, 350, "Total gross profit"),
            ("4", 300, 250, "Total profit (loss) before tax"),
            ("5", 250, 200, "Total profit (loss)"),
        ],
        "1210000": [
            ("1", 5000, 4500, "Total assets"),
            ("2", 2000, 1800, "Total liabilities"),
            ("3", 3000, 2700, "Total equity"),
        ],
        "1510000": [
            ("1", 200, 180, "Total net cash flows received from (used in) operating activities"),
            ("2", 100, 90, "Total net increase (decrease) in cash and cash equivalents"),
            ("3", 1100, 1000, "Cash and cash equivalents cash flows, end of the period"),
        ],
    }
    for code, values in rows.items():
        sheet = wb.create_sheet(code)
        sheet["B4"] = "CurrentYearDuration"
        sheet["C4"] = "PriorYearDuration"
        for row_number, (label_id, current, prior, label) in enumerate(values, start=6):
            sheet.cell(row_number, 1, label_id)
            sheet.cell(row_number, 2, current)
            sheet.cell(row_number, 3, prior)
            sheet.cell(row_number, 4, label)
    wb.save(path)


class XlsxIngestionTests(unittest.TestCase):
    def test_parser_autodetects_filename_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "FinancialStatement-2026-I-TEST.xlsx"
            _make_workbook(path)
            meta, rows = parse_xlsx(str(path))

            self.assertEqual(meta["ticker"], "TEST")
            self.assertEqual(meta["year"], 2026)
            self.assertEqual(meta["period"], "TW1")
            self.assertEqual(rows[0]["ticker"], "TEST")

    def test_missing_seed_is_ingested_once_and_becomes_available(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports" / "TEST" / "2026_TW1"
            reports.mkdir(parents=True)
            _make_workbook(reports / "FinancialStatement-2026-I-TEST.xlsx")

            old_reports_dir = xlsx_seed.REPORTS_DIR
            xlsx_seed.REPORTS_DIR = root / "reports"
            try:
                result = xlsx_seed.ensure_xlsx_coverage(
                    "TEST", 2026, "Q1", db_path=root / "pipeline.duckdb",
                )
                self.assertEqual(result["status"], "ingested")
                self.assertTrue(result["seeded"])
                self.assertTrue(xlsx_seed.has_coverage("TEST", 2026, "Q1", root / "pipeline.duckdb"))
            finally:
                xlsx_seed.REPORTS_DIR = old_reports_dir


if __name__ == "__main__":
    unittest.main()
