"""Operational loading and the deterministic missing-data states."""

from __future__ import annotations

import datetime as dt
import json

from chat_app import filings, operational, paths, tools


def test_load_operational_itmg_q2():
    op = operational.load_operational("ITMG", "Q2", 2026)
    assert op["available"] is True
    assert op["ticker"] == "ITMG"
    assert op["period"] == "Q2"

    metrics = {entry["metric"] for entry in op["entries"]}
    assert "coal_production" in metrics
    assert "coal_sales_volume" in metrics
    assert "asp" in metrics
    assert len(op["entries"]) >= 5
    assert len(op["drivers"]) >= 5

    entry = next(e for e in op["entries"] if e["metric"] == "coal_production")
    assert entry["unit"] == "Mt"
    assert entry["page"] == 1
    assert entry["source_doc"] == "itmg_2026-08-10_news_release"
    assert entry["published_at"] == "2026-08-10"
    assert entry["status"] == "preliminary"

    driver = op["drivers"][0]
    assert driver["type"] == "management_stated"
    assert driver["source_doc"] == "itmg_2026-08-10_news_release"

    captured = [d for d in op["documents"] if d["status"] == "captured"]
    assert captured and captured[0]["id"] == "itmg_2026-08-10_news_release"
    assert captured[0]["pages"] == 8


def test_operational_verification_classes():
    op = operational.load_operational("ITMG", "Q2", 2026)
    by_metric = {entry["metric"]: entry for entry in op["entries"]}
    assert by_metric["coal_production"]["verification"]["method"] == "quoted_in_source"
    assert by_metric["total_assets"]["verification"]["method"] == "reconciled"
    assert "1210000" in (by_metric["total_assets"]["verification"]["reconciled_to"] or "")


def test_missing_operational_is_empty_not_error():
    # ADMR has no captured operational document for Q2 2026.
    op = operational.load_operational("ADMR", "Q2", 2026)
    assert op["available"] is False
    assert op["entries"] == []
    assert op["drivers"] == []


def test_filing_registry_uses_fact_sheet_and_raw_pdf():
    doc = filings.filing_document("MAPA", "Q1", 2026)
    assert doc is not None
    assert doc["id"] == "mapa_q1_2026_filing"
    assert doc["type"] == "filing"
    assert doc["status"] == "captured"
    assert doc["period"] == "Q1"
    assert doc["year"] == 2026
    assert doc["path"] == "raw/MAPA_Q1_2026.pdf"
    assert doc["text_path"] == "results/MAPA_Q1_2026.md"
    assert doc["pages"] == 91
    assert doc["url"] is None
    assert doc["published"] is None

    # Both supported MAPA periods are captured.
    docs = filings.filing_documents("MAPA")
    assert {d["period"] for d in docs} == {"Q1", "Q2"}
    one = filings.filing_documents("MAPA", "Q2", 2026)
    assert len(one) == 1 and one[0]["id"] == "mapa_q2_2026_filing"


def test_filing_search_points_at_mapa_geographic_revenue_pages():
    expected = {"Q1": 72, "Q2": 75}
    for quarter, page in expected.items():
        doc = filings.filing_document("MAPA", quarter, 2026)
        hits = filings.search_documents([doc], "geographic revenue")
        assert hits
        assert hits[0]["document_id"] == f"mapa_{quarter.lower()}_2026_filing"
        assert hits[0]["page"] == page
        assert set(hits[0]["matched_terms"]) == {"geographic", "revenue"}


def test_documents_status_mapa_filing_and_topic_search():
    found = operational.documents_status(
        "MAPA", "Q1", 2026, item="geographic revenue"
    )
    assert found["state"] == "available"
    assert found["evidence"]["document_id"] == "mapa_q1_2026_filing"
    assert found["evidence"]["page"] == 72
    assert found["evidence"]["search"] == "filing_text"

    q2 = operational.documents_status("MAPA", "Q2", 2026, item="geographic revenue")
    assert q2["state"] == "available"
    assert q2["evidence"]["page"] == 75

    # Filings spell the market "Filipina"/"Phillipines"; normalize the common
    # English spelling too.
    philippines = operational.documents_status(
        "MAPA", "Q1", 2026, item="Philippines revenue"
    )
    assert philippines["state"] == "available"
    assert philippines["evidence"]["page"] == 72

    # No item still reports the filing as available, never not_captured.
    plain = operational.documents_status("MAPA", "Q1", 2026)
    assert plain["state"] == "available"
    assert any(d["type"] == "filing" for d in plain["documents"])
    assert plain["state"] != "not_captured"

    absent = operational.documents_status(
        "MAPA", "Q1", 2026, item="lithium carbonate"
    )
    assert absent["state"] == "not_disclosed"
    assert absent["evidence"]["document_id"] == "mapa_q1_2026_filing"
    assert absent["evidence"]["pages_searched"] == 91
    assert absent["evidence"]["search"] == "filing_text"
    assert "not found" in absent["evidence"]["detail"]
    assert "No document was discovered" not in absent["evidence"]["detail"]


def test_documents_status_available():
    out = operational.documents_status("ITMG", "Q2", 2026)
    assert out["state"] == "available"
    assert out["evidence"]["document_id"] == "itmg_2026-08-10_news_release"
    assert out["documents"]


def test_documents_status_item_states():
    metric = operational.documents_status("ITMG", "Q2", 2026, item="coal_production")
    assert metric["state"] == "available"
    assert metric["evidence"]["page"] == 1

    # "strip ratio" is not an extracted entry, but the captured release text
    # contains both terms, so the filing search returns a page hint.
    text_match = operational.documents_status(
        "ITMG", "Q2", 2026, item="strip_ratio"
    )
    assert text_match["state"] == "available"
    assert text_match["evidence"]["document_id"]
    assert text_match["evidence"]["search"] == "filing_text"
    assert text_match["evidence"]["page"] in {1, 8}

    undisclosed = operational.documents_status(
        "ITMG", "Q2", 2026, item="lithium carbonate"
    )
    assert undisclosed["state"] == "not_disclosed"
    assert undisclosed["evidence"]["document_id"]
    assert undisclosed["evidence"]["page_range"] in {"1-8", "1-124"}
    assert undisclosed["evidence"]["pages_searched"] >= 8
    assert undisclosed["evidence"]["search"] == "filing_text"

    doc_type = operational.documents_status(
        "ITMG", "Q2", 2026, item="presentation"
    )
    assert doc_type["state"] == "not_captured"


def test_documents_status_captured_ptba_and_admr_filing():
    # PTBA's 1H26 press release was captured; ADMR has a raw filing even
    # though no operational extract exists.
    out = operational.documents_status("PTBA", "Q2", 2026)
    assert out["state"] == "available"
    assert any(d["status"] == "captured" for d in out["documents"])

    admr = operational.documents_status("ADMR", "Q2", 2026)
    assert admr["state"] == "available"
    assert any(d["id"] == "admr_q2_2026_filing" for d in admr["documents"])
    assert "detail" in admr["evidence"]
    assert "does not exist" not in admr["evidence"]["detail"].lower()

    # Operational availability is unchanged: the filing is not an extract.
    op = operational.load_operational("ADMR", "Q2", 2026)
    assert op["available"] is False
    assert op["entries"] == []
    lookup = tools.operational_lookup("ADMR", "Q2", 2026)["data"]
    assert lookup["state"] in {
        "not_captured", "not_disclosed", "not_yet_published"
    }
    assert lookup["state"] != "available"


def test_documents_status_not_yet_published_within_window(monkeypatch):
    # BBCA Q2 period end is 2026-06-30; 2026-08-01 is T+32 with no document.
    # The real BBCA period now has a filing, so hide the registry for this test.
    monkeypatch.setattr(filings, "filing_document", lambda *a, **k: None)
    monkeypatch.setattr(filings, "filing_documents", lambda *a, **k: [])
    out = operational.documents_status(
        "BBCA", "Q2", 2026, today=dt.date(2026, 8, 1)
    )
    assert out["state"] == "not_yet_published"
    assert out["evidence"]["days_after_period_end"] == 32


def test_documents_status_unsupported():
    out = operational.documents_status("ZZZZ")
    assert out["state"] == "unsupported"
    assert "ITMG" in out["evidence"]["supported_tickers"]

    period = operational.documents_status("ITMG", "Q3", 2026)
    assert period["state"] == "unsupported"
    assert "Q2 2026" in period["evidence"]["supported_periods"]


def test_documents_status_implies_latest_period():
    out = operational.documents_status("ITMG")
    assert out["state"] == "available"
    assert out["period"] == "Q2"
    assert out["year"] == 2026


def test_completeness_itmg():
    comp = operational.completeness("ITMG", "Q2", 2026)
    assert comp["financials"] == "complete"
    assert comp["operational"] == "complete"
    assert comp["analysis"] == "present"
    assert comp["notes"] in {"indexed", "stale", "missing"}
    assert comp["expected_documents"] == ["filing", "press_release", "presentation"]
    assert comp["as_of"]


def test_completeness_ptba_and_bank():
    comp = operational.completeness("PTBA", "Q2", 2026)
    assert comp["financials"] == "complete"
    assert comp["operational"] == "complete"

    # A filing without extracted operational entries is partial, not complete.
    ptps = operational.completeness("PTPS", "Q2", 2026)
    assert ptps["operational"] == "partial"

    bank = operational.completeness("BBCA", "Q2", 2026)
    # Banks have no cogs/gross_profit line; that must not make the sheet partial.
    assert bank["financials"] == "complete"


def test_completeness_has_all_layer_states():
    comp = operational.completeness("ITMG", "Q2", 2026)
    for key in ("financials", "operational", "filings", "notes", "analysis",
                "as_of", "expected_documents", "gaps"):
        assert key in comp


def test_completeness_filings_do_not_mask_operational_gaps():
    itmg = operational.completeness("ITMG", "Q2", 2026)
    assert itmg["filings"] == "captured"
    assert {"item": "filing", "state": "not_captured",
            "detail": "no captured document of this type"} not in itmg["gaps"]

    ptba = operational.completeness("PTBA", "Q2", 2026)
    assert ptba["filings"] == "captured"
    assert "filing" not in {gap["item"] for gap in ptba["gaps"]}

    mapa = operational.completeness("MAPA", "Q1", 2026)
    assert mapa["filings"] == "captured"
    assert mapa["operational"] == "not_captured"
    gap_items = {gap["item"] for gap in mapa["gaps"]}
    assert "filing" not in gap_items
    # The raw filing exists; other document classes are still legitimately absent.
    assert {"press_release", "presentation"} <= gap_items


def test_financial_metric_status_uses_fact_sheet_not_document_set():
    """A captured filing with no such line is not_disclosed, never not_captured."""
    itmg = operational.documents_status("ITMG", "Q2", 2026, "revenue")
    assert itmg["state"] == "available"
    assert itmg["evidence"]["source"] == "fact_sheet"
    assert itmg["evidence"]["sheet_row"] == "1311000:6"

    bank = operational.documents_status("BBCA", "Q2", 2026, "cogs")
    assert bank["state"] == "not_disclosed"
    assert bank["evidence"]["statement"] == "income_statement"
    # The bank question must never read as "we failed to capture a filing".
    assert bank["state"] != "not_captured"


def test_operational_metric_still_uses_document_state():
    out = operational.documents_status("PTBA", "Q2", 2026, "coal_production")
    assert out["state"] == "available"
    assert out["evidence"]["document_id"] == "ptba_2026-09-01_press_release"

    # ADMR has no operational entries, but its captured filing mentions both
    # terms, so the answer is a page hint rather than a missing state.
    admr = operational.documents_status("ADMR", "Q2", 2026, "coal_production")
    assert admr["state"] == "available"
    assert admr["evidence"]["search"] == "filing_text"
    assert admr["evidence"]["document_id"] == "admr_q2_2026_filing"
    assert admr["evidence"]["page"] > 0


def test_analysis_context_is_company_specific_and_operational():
    ctx = operational.analysis_context("ITMG", "Q2", 2026, limit=4)
    assert ctx
    assert all("ITMG" in item["path"] for item in ctx)
    text = " ".join(item["snippet"] for item in ctx).lower()
    # The note-only operational context the release does not carry.
    assert "strip ratio" in text or "cash cost" in text
    assert "14.0x" in text or "cash cost" in text
    assert all(item["secondary"] is True for item in ctx)


# ---------------------------------------------------------------------------
# Completeness cache: repeated preflights must not re-parse artifacts
# ---------------------------------------------------------------------------


_FACT_SHEET = """\
meta:
  period_end: "2026-06-30"
income_statement:
  lines:
    revenue: 100
    pbt: 20
    net_profit: 15
    gross_profit: 40
balance_sheet:
  lines:
    total_assets: 500
    total_liabilities: 200
    total_equity: 300
cash_flow:
  lines:
    ocf: 50
    net_change: 5
    cash_end: 60
"""


def _isolated_artifacts(tmp_path, monkeypatch):
    """Point every artifact directory at a throwaway tree with one ticker."""
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(paths, "FACTS_DIR", tmp_path / "facts")
    monkeypatch.setattr(paths, "OPERATIONAL_DIR", tmp_path / "operational")
    monkeypatch.setattr(paths, "ANALYSIS_DIR", tmp_path / "analysis")
    monkeypatch.setattr(paths, "RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(paths, "RESULTS_DIR", tmp_path / "results")
    monkeypatch.setattr(paths, "PREPROCESSED_DIR", tmp_path / "preprocessed")
    monkeypatch.setattr(paths, "NOTES_DB", tmp_path / "notes.duckdb")
    (tmp_path / "facts").mkdir()
    operational.clear_completeness_cache()
    return tmp_path / "facts" / "AAAA_Q2_2026.yaml"


def test_completeness_cache_hit_skips_yaml_parsing(tmp_path, monkeypatch):
    sheet_path = _isolated_artifacts(tmp_path, monkeypatch)
    sheet_path.write_text(_FACT_SHEET, encoding="utf-8")

    loads = {"count": 0}
    real_load_yaml = paths.load_yaml

    def counting_load_yaml(path):
        loads["count"] += 1
        return real_load_yaml(path)

    monkeypatch.setattr(paths, "load_yaml", counting_load_yaml)

    first = operational.completeness("AAAA", "Q2", 2026)
    assert first["financials"] == "complete"
    assert loads["count"] >= 1
    after_first = loads["count"]

    second = operational.completeness("AAAA", "Q2", 2026)
    assert second == first
    assert loads["count"] == after_first

    sheet_path.write_text(
        "income_statement:\n  lines:\n    revenue: 1\n", encoding="utf-8"
    )
    third = operational.completeness("AAAA", "Q2", 2026)
    assert third["financials"] == "missing"
    assert loads["count"] > after_first


def test_completeness_returns_copies(tmp_path, monkeypatch):
    sheet_path = _isolated_artifacts(tmp_path, monkeypatch)
    sheet_path.write_text(_FACT_SHEET, encoding="utf-8")

    first = operational.completeness("AAAA", "Q2", 2026)
    first["financials"] = "missing"
    first["gaps"].append({"item": "poison", "state": "missing", "detail": None})

    second = operational.completeness("AAAA", "Q2", 2026)
    assert second["financials"] == "complete"
    assert "poison" not in {gap["item"] for gap in second["gaps"]}


def test_completeness_cache_invalidates_on_operational_release(
    tmp_path, monkeypatch
):
    sheet_path = _isolated_artifacts(tmp_path, monkeypatch)
    sheet_path.write_text(_FACT_SHEET, encoding="utf-8")
    before = operational.completeness("AAAA", "Q2", 2026)
    assert before["operational"] == "not_captured"

    op_path = tmp_path / "operational" / "AAAA_Q2_2026_operational.json"
    op_path.parent.mkdir(parents=True, exist_ok=True)
    op_path.write_text(json.dumps({
        "document": {
            "id": "aaaa_2026-08-10_news_release",
            "type": "news_release",
            "published": "2026-08-10",
            "pages": 2,
            "status": "captured",
            "path": "raw/AAAA_Q2_2026.pdf",
        },
        "operational": {"entries": [{
            "metric": "coal_production",
            "value": 1.2,
            "unit": "Mt",
            "page": 1,
            "quote": "production was 1.2 Mt",
        }]},
    }), encoding="utf-8")

    after = operational.completeness("AAAA", "Q2", 2026)
    assert after["operational"] == "complete"
