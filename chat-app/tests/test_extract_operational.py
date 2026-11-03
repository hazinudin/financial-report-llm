"""WS3.3-3.6 extraction tests against the ITMG Q2 2026 oracle document."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chat_app import extract_operational as ex
from chat_app import ingest

FIXTURE_PDF = (
    Path(__file__).resolve().parents[2]
    / "idx_data" / "operational" / "test" / "ITMG_6M26_news_release.pdf"
)
DOC_ID = "itmg_2026-08-10_news_release"
URL = (
    "https://itmg.co.id/files/media/"
    "News%20Release%20on%20Financial%20Results%20and%20Performance%206M26.pdf"
)


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    operational = tmp_path / "idx_data" / "operational"
    monkeypatch.setattr(ingest, "OPERATIONAL_DIR", operational)
    monkeypatch.setattr(ingest, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(ex, "OPERATIONAL_DIR", operational)
    monkeypatch.setattr(ex, "PROJECT_ROOT", tmp_path)
    ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id=DOC_ID,
                        local_file=FIXTURE_PDF)
    return tmp_path


def run_itmg(tmp_path: Path, **kwargs) -> tuple[dict, dict]:
    out = tmp_path / "ITMG_Q2_2026_operational.json"
    artifact, report = ex.run("ITMG", "Q2", 2026, offline=True, out=out, **kwargs)
    return artifact, report


def entries_by(artifact: dict, metric: str, scope: str | None = None) -> list[dict]:
    return [
        entry for entry in artifact["operational"]["entries"]
        if entry["metric"] == metric and (scope is None or entry["scope"] == scope)
    ]


def test_itmg_q2_operational_facts(workspace):
    artifact, report = run_itmg(workspace)

    production_q2 = entries_by(artifact, "coal_production", "Q2-2026")
    assert len(production_q2) == 1
    assert production_q2[0]["value"] == 5.3
    assert production_q2[0]["unit"] == "Mt"
    assert production_q2[0]["page"] == 1

    production_6m = entries_by(artifact, "coal_production", "6M-2026")
    assert len(production_6m) == 1
    assert production_6m[0]["value"] == 9.9
    assert production_6m[0]["prior"] == 10.4
    assert production_6m[0]["page"] == 1

    sales = entries_by(artifact, "coal_sales_volume", "6M-2026")
    assert [(entry["value"], entry["prior"], entry["page"]) for entry in sales] == [(12.3, 11.7, 1)]

    asp = entries_by(artifact, "asp", "6M-2026")
    assert [(entry["value"], entry["prior"], entry["page"]) for entry in asp] == [(81.0, 78.0, 1)]

    balance = {entry["metric"]: entry for entry in artifact["operational"]["balance_sheet"]}
    assert balance["total_assets"]["value"] == 2417.0
    assert balance["cash_and_equivalents"]["value"] == 738.0
    assert balance["short_term_deposits"]["value"] == 66.0
    assert balance["total_liabilities"]["value"] == 506.0
    assert balance["total_equity"]["value"] == 1910.0
    assert {entry["page"] for entry in artifact["operational"]["balance_sheet"]} == {2}

    other = {entry["metric"]: entry for entry in artifact["operational"]["other"]}
    assert other["royalties_to_government"]["value"] == 111.0
    assert other["treasury_share_purchase"]["value"] == 27.0

    assert artifact["derived"] == {
        "inventory_drawdown_mt": 2.4,
        "implied_revenue_usd_m": 996.3,
        "production_yoy_pct": -4.8,
        "sales_yoy_pct": 5.1,
        "asp_yoy_pct": 3.8,
    }
    assert report["entries"] == 14
    assert report["driver_pass"] == "deterministic"


def test_itmg_reconciles_five_of_five(workspace):
    artifact, report = run_itmg(workspace)
    reconciliation = artifact["reconciliation"]

    assert set(reconciliation) == {"revenue", "net_profit", "operating_cf",
                                   "total_equity", "total_assets"}
    assert reconciliation["revenue"] == {"release": 1000.0, "xbrl": 1000.491}
    assert reconciliation["net_profit"] == {"release": 110.0, "xbrl": 109.514}
    assert reconciliation["operating_cf"] == {"release": 62.0, "xbrl": 62.376}
    assert reconciliation["total_equity"] == {"release": 1910.0, "xbrl": 1910.488}
    assert reconciliation["total_assets"] == {"release": 2417.0, "xbrl": 2416.716}

    assert report["rejected"] == []
    assert len(report["reconciled"]) == 5
    assert all(item["ok"] for item in report["reconciled"])
    assert artifact["completeness"]["operational"] == "complete"

    revenue = entries_by(artifact, "revenue", "6M-2026")[0]
    assert revenue["status"] == "preliminary"
    assert revenue["verification"]["method"] == "reconciled"
    assert revenue["verification"]["reconciled_to"] == "income_statement.revenue (1311000:6)"

    quoted = entries_by(artifact, "coal_production", "Q2-2026")[0]
    assert quoted["verification"]["method"] == "quoted_in_source"
    assert quoted["quote"].startswith("HIGHLIGHTS")


def test_drivers_are_verbatim_quotes_with_pages(workspace):
    artifact, _ = run_itmg(workspace)
    drivers = artifact["operational"]["drivers"]
    assert drivers
    pages = ingest.parse_pages_text(
        (workspace / "idx_data" / "operational" / "ITMG" / f"{DOC_ID}.txt").read_text(
            encoding="utf-8"
        )
    )
    for driver in drivers:
        assert driver["page"] in range(1, len(pages) + 1)
        assert ex._normalize(driver["quote"]) in ex._normalize(pages[driver["page"] - 1])
        assert driver["type"] == "management_stated"
        assert driver["source_doc"] == DOC_ID

    cost_driver = next(d for d in drivers if "strip ratio" in d["quote"])
    assert cost_driver["page"] == 1
    assert cost_driver["affects"] == "cost_of_revenue"


def test_tampered_reconciliation_value_is_rejected_not_published(workspace):
    text_path = workspace / "idx_data" / "operational" / "ITMG" / f"{DOC_ID}.txt"
    text = text_path.read_text(encoding="utf-8")
    assert "$1,000 million" in text
    text_path.write_text(text.replace("$1,000 million", "$1,200 million", 1), encoding="utf-8")

    artifact, report = run_itmg(workspace)
    revenue = entries_by(artifact, "revenue", "6M-2026")[0]

    # Nothing is dropped: the entry is present, but explicitly rejected.
    assert revenue["value"] == 1200.0
    assert revenue["status"] == "rejected"
    assert revenue["verification"]["method"] == "rejected"

    assert report["rejected"], "tampered value must appear in the rejected list"
    assert artifact["completeness"]["operational"] == "partial"
    assert any("revenue" in warning for warning in artifact["document"]["extraction_warnings"])

    # And it is not presented as a verified operational entry.
    assert all(entry["metric"] != "revenue" or entry["status"] != "preliminary"
               for entry in artifact["operational"]["entries"])


def test_indonesian_comma_decimals_parse():
    assert ex.parse_number("5,3") == 5.3
    assert ex.parse_number("12,3") == 12.3
    assert ex.parse_number("1.000") == 1000.0
    assert ex.parse_number("2.417") == 2417.0
    assert ex.parse_number("1.910") == 1910.0
    assert ex.parse_number("1,000.5") == 1000.5
    assert ex.parse_number("1.000,5") == 1000.5


def test_indonesian_pages_are_used_with_original_page_numbers(workspace, monkeypatch):
    full_pages = ingest.pdf_page_texts(FIXTURE_PDF.read_bytes())
    assert [ex.page_language(page) for page in full_pages] == [
        "en", "en", "en", "id", "id", "id", "en", "en",
    ]
    # Simulate an ID-only document: blank the English pages, keep the numbering.
    id_only = [""] * 8
    for index in (3, 4, 5):
        id_only[index] = full_pages[index]
    monkeypatch.setattr(ex, "load_document_pages", lambda doc, directory=None: id_only)

    artifact, report = run_itmg(workspace)
    assert report["language"] == "id"

    production_q2 = entries_by(artifact, "coal_production", "Q2-2026")
    assert len(production_q2) == 1
    assert production_q2[0]["value"] == 5.3           # "5,3 juta ton"
    assert production_q2[0]["page"] == 4              # original page number
    assert "5,3" in production_q2[0]["quote"]

    sales = entries_by(artifact, "coal_sales_volume", "6M-2026")[0]
    assert (sales["value"], sales["prior"], sales["page"]) == (12.3, 11.7, 4)

    assets = entries_by(artifact, "total_assets", "6M-2026")[0]
    assert (assets["value"], assets["page"]) == (2417.0, 5)   # "$2.417 juta"


def test_same_metric_is_not_double_counted_across_language_halves(workspace):
    artifact, report = run_itmg(workspace)
    assert report["language"] == "en+id"
    assert report["selected_pages"]["en"] and report["selected_pages"]["id"]

    for metric, scope in (("coal_production", "Q2-2026"), ("coal_production", "6M-2026"),
                          ("coal_sales_volume", "6M-2026"), ("asp", "6M-2026"),
                          ("net_profit", "6M-2026"), ("revenue", "6M-2026")):
        assert len(entries_by(artifact, metric, scope)) == 1, (metric, scope)

    # The English value won: 5.3 (not the Indonesian 5,3 as a second entry).
    assert entries_by(artifact, "coal_production", "Q2-2026")[0]["page"] == 1
    assert entries_by(artifact, "net_profit", "6M-2026")[0]["page"] == 1


def test_offline_mode_never_calls_the_llm(workspace, monkeypatch):
    def explode(*args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("offline mode must not call the LLM")

    monkeypatch.setattr(ex, "_llm_call", explode)
    artifact, report = run_itmg(workspace)
    assert report["driver_pass"] == "deterministic"
    assert artifact["operational"]["drivers"]


def test_unverifiable_llm_quote_is_dropped(workspace, monkeypatch):
    monkeypatch.setattr(ex, "_llm_call", lambda *args, **kwargs: json.dumps({
        "drivers": [
            {"quote": "higher coal mining and transportation costs, resulted from higher strip ratio",
             "page": 1, "affects": "cost_of_revenue", "driver": "higher strip ratio"},
            {"quote": "this sentence is not in the document", "page": 1,
             "affects": "cost_of_revenue", "driver": "invented"},
        ]
    }))
    out = workspace / "llm.json"
    artifact, report = ex.run("ITMG", "Q2", 2026, offline=False, out=out)
    drivers = artifact["operational"]["drivers"]
    assert any(driver["source"] == "llm" for driver in drivers)
    assert all("this sentence is not in the document" not in driver["quote"]
               for driver in drivers)
    assert any("not verbatim" in warning for warning in report["warnings"])


def test_malformed_llm_json_is_recovered_by_pattern():
    content = (
        '{"drivers": [{"quote": "higher "strip" ratio", "page": 1, '
        '"affects": "cost_of_revenue", "driver": "strip ratio"}]}'
    )
    parsed, warnings = ex._parse_drivers_payload(content)
    assert len(parsed) == 1
    assert parsed[0]["page"] == 1
    assert parsed[0]["affects"] == "cost_of_revenue"
    assert warnings and "recovered" in warnings[0]


def test_missing_document_degrades_to_not_captured(workspace):
    out = workspace / "missing.json"
    artifact, report = ex.run("PTBA", "Q2", 2026, offline=True, out=out)
    assert artifact["completeness"]["operational"] == "not_captured"
    assert artifact["document"]["status"] == "not_captured"
    assert artifact["operational"]["entries"] == []
    assert report["warnings"]
