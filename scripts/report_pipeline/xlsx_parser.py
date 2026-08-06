"""
Parser for IDX XBRL Excel financial statements.

Parses FinancialStatement-*.xlsx files downloaded from IDX, extracting:
  - Entity metadata from the '1000000' (General information) sheet
  - Financial statement line items from statement sheets (balance sheet,
    income statement, cash flow, etc.)

Each XLSX follows the IDX XBRL taxonomy with standard sheet codes:
  1210000 / 1220000 — Balance Sheet (legacy taxonomy)
  1311000 / 1312000 — Income Statement (legacy taxonomy)
  1321000 / 1322000 — Other Comprehensive Income (legacy taxonomy)
  1410000           — Statement of Changes in Equity (legacy taxonomy)
  1510000 / 1520000 — Cash Flow (legacy taxonomy)
  2210000 / 2220000 — Balance Sheet (industry taxonomy)
  2311000 / 2312000 — Income Statement (industry taxonomy)
  2321000 / 2322000 — Other Comprehensive Income (industry taxonomy)
  2410000           — Statement of Changes in Equity (industry taxonomy)
  2510000 / 2520000 — Cash Flow (industry taxonomy)
  Plus several notes/disclosure sheets (161xxxx, 162xxxx, etc.)

Usage:
    from xlsx_parser import parse_xlsx

    meta, statements = parse_xlsx("path/to/FinancialStatement-2025-Tahunan-JARR.xlsx",
                                   ticker="JARR", year=2025, period="audit")
"""

from typing import Any


def _cell(sheet, col_letter: str, row: int) -> Any:
    """Read a cell value, returning None for empty cells."""
    val = sheet[f"{col_letter}{row}"].value
    return val


def _parse_meta(sheet) -> dict:
    """Parse the '1000000' General Information sheet into a metadata dict."""
    meta: dict[str, Any] = {}

    # Read entity metadata — key-value pairs in columns A-B
    for row in range(6, min(sheet.max_row + 1, 50)):
        label_col_a = _cell(sheet, "A", row)
        value_col_b = _cell(sheet, "B", row)

        if label_col_a is None:
            continue

        label = str(label_col_a).strip()

        if "tanggal awal periode berjalan" in label.lower():
            meta["period_start"] = str(value_col_b) if value_col_b else ""
        elif "tanggal akhir periode berjalan" in label.lower():
            meta["period_end"] = str(value_col_b) if value_col_b else ""
        elif "tanggal akhir tahun sebelumnya" in label.lower():
            meta["prior_period_end"] = str(value_col_b) if value_col_b else ""
        elif "tanggal awal periode sebelumnya" in label.lower():
            meta["prior_period_start"] = str(value_col_b) if value_col_b else ""
        elif "nama entitas" in label.lower() and "perubahan" not in label.lower():
            meta["entity_name"] = str(value_col_b) if value_col_b else ""
        elif "kode entitas" in label.lower():
            meta["entity_code"] = str(value_col_b) if value_col_b else ""
        elif "mata uang pelaporan" in label.lower():
            meta["currency"] = str(value_col_b) if value_col_b else ""
        elif "pembulatan" in label.lower():
            meta["rounding"] = str(value_col_b) if value_col_b else ""
        elif "jenis laporan atas laporan keuangan" in label.lower():
            meta["audit_status"] = str(value_col_b) if value_col_b else ""
        elif "auditor tahun berjalan" in label.lower():
            meta["auditor"] = str(value_col_b) if value_col_b else ""
        elif "entitas tunggal" in label.lower() or "entitas induk" in label.lower():
            meta["consolidation_type"] = str(value_col_b) if value_col_b else ""

    return meta


def _parse_statement_sheet(sheet, sheet_code: str) -> list[dict]:
    """
    Parse a financial statement sheet (4-column format).
    Returns a list of row dicts with label_id, value_current, value_prior, label_en, context_type.
    """
    rows: list[dict] = []

    # Row 1: sheet title (e.g. "[1210000] Statement of financial position...")
    title_cell = _cell(sheet, "A", 1)
    sheet_title = str(title_cell) if title_cell else ""

    # Row 4: context types (e.g. "CurrentYearInstant" / "PriorEndYearInstant")
    context_b = _cell(sheet, "B", 4)
    context_c = _cell(sheet, "C", 4)
    context_b_str = str(context_b).strip() if context_b else ""
    context_c_str = str(context_c).strip() if context_c else ""

    # Detect context type for the current column (B)
    # Possible values: CurrentYearInstant, CurrentYearDuration, CurrentYearStartInstant, etc.
    if "Instant" in context_b_str:
        context_type = "Instant"
    elif "Duration" in context_b_str:
        context_type = "Duration"
    else:
        context_type = "Unknown"

    # Data rows start at row 5 (header) or 6 (first data row). We scan from row 5.
    # Row 5 is typically the bilingual header - skip it.
    for row_num in range(6, sheet.max_row + 1):
        label_id = _cell(sheet, "A", row_num)
        value_current = _cell(sheet, "B", row_num)
        value_prior = _cell(sheet, "C", row_num)
        label_en = _cell(sheet, "D", row_num)

        # Skip empty rows or rows with no label
        if label_id is None or str(label_id).strip() == "":
            continue

        label_id_str = str(label_id).strip()

        # Convert values to float if numeric
        val_current = _to_float(value_current)
        val_prior = _to_float(value_prior)

        label_en_str = str(label_en).strip() if label_en else ""

        rows.append({
            "sheet_code": sheet_code,
            "sheet_title": sheet_title,
            "row_number": row_num,
            "label_id": label_id_str,
            "value_current": val_current,
            "value_prior": val_prior,
            "label_en": label_en_str,
            "context_type": context_type,
        })

    return rows


def _to_float(val: Any) -> float | None:
    """Convert a cell value to float, returning None for non-numeric."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val = val.strip().replace(",", "").replace(" ", "")
        if not val:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    return None


# Standard statement sheets in the IDX XBRL taxonomy
STATEMENT_SHEET_CODES = [
    # Legacy taxonomy.
    "1210000", "1220000", "1311000", "1312000",
    "1321000", "1322000", "1410000", "1510000", "1520000",
    # Industry taxonomy used by newer IDX workbooks.
    "2210000", "2220000", "2311000", "2312000",
    "2321000", "2322000", "2410000", "2510000", "2520000",
]


def parse_xlsx(
    xlsx_path: str,
    ticker: str | None = None,
    year: int | None = None,
    period: str | None = None,
) -> tuple[dict, list[dict]]:
    """
    Parse an IDX XBRL Excel financial statement.

    Args:
        xlsx_path: Path to the .xlsx file
        ticker: Override ticker (auto-detected from '1000000' sheet if None)
        year: Override year (auto-detected from filename if None)
        period: Override period (auto-detected from filename if None)

    Returns:
        Tuple of (meta_dict, list_of_statement_row_dicts)
    """
    from openpyxl import load_workbook

    wb = load_workbook(xlsx_path, data_only=True)

    # Auto-detect ticker/year/period from filename if not provided
    if ticker is None or year is None or period is None:
        import re
        stem = xlsx_path.rsplit("/", 1)[-1]
        stem = stem.replace(".xlsx", "").replace(".xls", "")
        # E.g. "FinancialStatement-2025-Tahunan-JARR" or "FinancialStatement-2025-III-JARR"
        ticker_match = re.search(r"-(?P<ticker>[A-Z]{3,5})$", stem)
        year_match = re.search(r"(?P<year>\d{4})", stem)
        period_map = {
            "Tahunan": "audit", "I": "TW1", "II": "TW2", "III": "TW3", "IV": "TW4",
        }
        if ticker is None and ticker_match:
            ticker = ticker_match.group("ticker")
        if year is None and year_match:
            year = int(year_match.group("year"))
        for key, val in period_map.items():
            if key in stem and period is None:
                period = val

    # Parse entity metadata from '1000000' sheet
    meta: dict[str, Any] = {
        "ticker": ticker or "",
        "year": year or 0,
        "period": period or "",
        "entity_name": "",
        "entity_code": "",
        "period_start": "",
        "period_end": "",
        "prior_period_start": "",
        "prior_period_end": "",
        "currency": "",
        "rounding": "",
        "audit_status": "",
        "auditor": "",
        "consolidation_type": "",
    }

    if "1000000" in wb.sheetnames:
        gen_info = wb["1000000"]
        meta.update(_parse_meta(gen_info))

    # Parse financial statement sheets
    all_statements: list[dict] = []
    for sheet_code in STATEMENT_SHEET_CODES:
        if sheet_code in wb.sheetnames:
            ws = wb[sheet_code]
            sheet_rows = _parse_statement_sheet(ws, sheet_code)
            for r in sheet_rows:
                r["ticker"] = ticker or ""
                r["year"] = year or 0
                r["period"] = period or ""
            all_statements.extend(sheet_rows)

    wb.close()
    return meta, all_statements
