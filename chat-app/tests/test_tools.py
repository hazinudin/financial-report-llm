"""Every typed tool, on real ITMG/PTBA/BBCA artifacts."""

from __future__ import annotations

import json

import pytest

from chat_app import operational, paths, tools


def _size(payload: dict) -> int:
    return len(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def _assert_envelope(result: dict, tool: str) -> None:
    assert result["ok"] is True
    assert result["tool"] == tool
    assert "request" in result
    assert isinstance(result["ledger"], list)
    assert isinstance(result["caveats"], list)
    assert _size(result) <= paths.OUTPUT_CAP_BYTES


def test_list_companies():
    result = tools.list_companies()
    _assert_envelope(result, "list_companies")
    companies = {c["ticker"]: c for c in result["data"]["companies"]}
    assert "ITMG" in companies
    assert "Q2 2026" in companies["ITMG"]["periods"]
    assert companies["MAPA"]["filings"] == "captured"
    assert companies["MAPA"]["operational_periods"] == []
    assert result["data"]["as_of"]


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


def _isolated_world(tmp_path, monkeypatch):
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
    tools.clear_companies_cache()
    operational.clear_completeness_cache()
    return tmp_path / "facts" / "AAAA_Q2_2026.yaml"


def test_list_companies_cache_hit_skips_completeness(tmp_path, monkeypatch):
    fact = _isolated_world(tmp_path, monkeypatch)
    fact.write_text(_FACT_SHEET, encoding="utf-8")

    calls = {"count": 0}
    real = operational.completeness

    def counting(*args, **kwargs):
        calls["count"] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(operational, "completeness", counting)

    first = tools.list_companies()
    assert calls["count"] == 1
    second = tools.list_companies()
    assert calls["count"] == 1
    assert second["data"] == first["data"]
    assert second["ledger"] == first["ledger"]


def test_list_companies_cache_invalidates_on_file_changes(tmp_path, monkeypatch):
    fact = _isolated_world(tmp_path, monkeypatch)
    fact.write_text(_FACT_SHEET, encoding="utf-8")
    first = tools.list_companies()
    company = first["data"]["companies"][0]
    assert company["financials"] == "complete"
    assert company["operational_periods"] == []

    # A fact-sheet rewrite that drops required lines must be visible at once.
    fact.write_text(
        "income_statement:\n  lines:\n    revenue: 100\n", encoding="utf-8"
    )
    second = tools.list_companies()
    assert second["data"]["companies"][0]["financials"] == "missing"

    # A new operational release changes coverage and must be visible too.
    fact.write_text(_FACT_SHEET, encoding="utf-8")
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
    third = tools.list_companies()
    assert third["data"]["companies"][0]["operational_periods"] == ["Q2 2026"]
    assert third["data"]["summary"]["operational_periods"] == 1


def test_list_companies_results_are_copies(tmp_path, monkeypatch):
    fact = _isolated_world(tmp_path, monkeypatch)
    fact.write_text(_FACT_SHEET, encoding="utf-8")
    first = tools.list_companies()
    first["data"]["companies"][0]["ticker"] = "ZZZZ"
    first["data"]["summary"]["tickers"] = 99
    first["ledger"].clear()

    second = tools.list_companies()
    assert second["data"]["companies"][0]["ticker"] == "AAAA"
    assert second["data"]["summary"]["tickers"] == 1
    assert second["ledger"]


def test_company_facts_itmg():
    result = tools.company_facts("ITMG", "Q2", 2026)
    _assert_envelope(result, "company_facts")
    revenue = result["data"]["sections"]["income_statement"]["revenue"]
    assert revenue["value"] == 1_000_491.0
    assert revenue["provenance"]["sheet"] == "1311000"
    assert revenue["provenance"]["row"] == 6
    assert result["data"]["completeness"]["financials"] == "complete"

    ledger = [e for e in result["ledger"] if e["meta"].get("metric") == "revenue"]
    assert ledger and ledger[0]["source"] == "1311000:6"
    assert ledger[0]["kind"] == "money"
    assert len(ledger[0]["variants"]) == 3


def test_company_facts_bank_without_cogs():
    result = tools.company_facts("BBCA", "Q2", 2026)
    _assert_envelope(result, "company_facts")
    income = result["data"]["sections"]["income_statement"]
    assert "revenue" in income
    assert "cogs" not in income
    assert "gross_profit" not in income


def test_company_facts_details_section():
    result = tools.company_facts("ITMG", "Q2", 2026, sections="details")
    _assert_envelope(result, "company_facts")
    # The cap may omit the details block; if present it must be a mapping.
    details = result["data"]["sections"].get("details")
    assert details is None or isinstance(details, dict)


def test_company_facts_unsupported():
    with pytest.raises(paths.UnsupportedRequest) as excinfo:
        tools.company_facts("ZZZZ", "Q2", 2026)
    assert excinfo.value.payload["supported_tickers"]


def test_metric_series_revenue():
    result = tools.metric_series("ITMG", "revenue")
    _assert_envelope(result, "metric_series")
    series = result["data"]["series"]
    values = {(row["period"], row["year"]): row for row in series}
    assert values[("Q1", 2026)]["value"] == 497_577.0
    assert values[("Q2", 2026)]["value"] == 1_000_491.0
    assert values[("Q2", 2026)]["period_type"] == "cumulative"
    assert values[("Q2", 2026)]["source"] == "1311000:6"
    assert values[("Q2", 2026)]["reconciled"]["value"] == 1000.0
    assert abs(values[("Q2", 2026)]["reconciled"]["delta_pct"]) < 0.5
    assert values[("Q1", 2026)]["reconciled"] is None


def test_metric_series_explicit_periods():
    result = tools.metric_series(
        "ITMG", "revenue", periods=["Q1 2026", "Q2 2026"]
    )
    assert [row["period"] for row in result["data"]["series"]] == ["Q1", "Q2"]

    with pytest.raises(paths.UnsupportedRequest):
        tools.metric_series("ITMG", "revenue", periods=["Q3 2026"])


def test_period_derive_exit_arithmetic():
    result = tools.period_derive("ITMG", "Q2", 2026, "revenue")
    _assert_envelope(result, "period_derive")
    data = result["data"]
    assert data["formula"] == "tw2 - tw1"
    assert data["operands"] == {"tw2": 1_000_491.0, "tw1": 497_577.0}
    assert data["quarter_value"] == 502_914.0
    assert data["source"] == {"tw2": "1311000:6", "tw1": "1311000:6"}
    assert data["unit"] == "USD thousand"

    derived = [
        e for e in result["ledger"]
        if (e.get("meta") or {}).get("derived")
    ]
    assert derived
    assert derived[0]["meta"]["formula"] == "tw2 - tw1"
    assert derived[0]["value"] == 502_914.0


def test_period_derive_q1_and_rejections():
    q1 = tools.period_derive("ITMG", "Q1", 2026, "revenue")
    assert q1["data"]["formula"] == "tw1"
    assert q1["data"]["quarter_value"] == 497_577.0

    with pytest.raises(paths.ToolInputError):
        tools.period_derive("ITMG", "Q2", 2026, "total_assets")
    with pytest.raises(paths.UnsupportedRequest):
        tools.period_derive("ITMG", "Q3", 2026, "revenue")


def test_documents_status_itmg_available():
    result = tools.documents_status("ITMG", "Q2", 2026)
    _assert_envelope(result, "documents_status")
    data = result["data"]
    assert data["state"] == "available"
    assert data["evidence"]["document_id"] == "itmg_2026-08-10_news_release"
    assert data["documents"]

    metric = tools.documents_status("ITMG", "Q2", 2026, item="coal_production")
    assert metric["data"]["state"] == "available"


def test_documents_status_ptba_never_says_missing():
    result = tools.documents_status("PTBA", "Q2", 2026)
    _assert_envelope(result, "documents_status")
    data = result["data"]
    assert data["state"] in {
        "not_captured", "not_disclosed", "not_yet_published", "available"
    }
    assert data["evidence"]
    assert "does not exist" not in json.dumps(data).lower()


def test_documents_status_unsupported_is_a_state():
    result = tools.documents_status("ZZZZ")
    _assert_envelope(result, "documents_status")
    assert result["data"]["state"] == "unsupported"
    assert result["data"]["evidence"]["supported_tickers"]


def test_operational_lookup_itmg():
    result = tools.operational_lookup("ITMG", "Q2", 2026)
    _assert_envelope(result, "operational_lookup")
    data = result["data"]
    assert data["state"] == "available"
    metrics = {entry["metric"] for entry in data["entries"]}
    assert "coal_production" in metrics
    assert data["drivers"]
    assert any(doc["status"] == "captured" for doc in data["documents"])
    production = next(
        e for e in data["entries"] if e["metric"] == "coal_production"
    )
    assert production["source_doc"] == "itmg_2026-08-10_news_release"
    assert production["page"] == 1


def test_operational_lookup_filter():
    result = tools.operational_lookup("ITMG", "Q2", 2026, metric="coal_production")
    assert {e["metric"] for e in result["data"]["entries"]} == {"coal_production"}


def test_operational_lookup_ptba_explicit_state():
    result = tools.operational_lookup("PTBA", "Q2", 2026)
    _assert_envelope(result, "operational_lookup")
    assert result["data"]["state"] == "available"
    production = [
        entry for entry in result["data"]["entries"]
        if entry.get("metric") == "coal_production"
    ]
    assert production and production[0]["value"] == 19.45
    assert production[0]["scope"] == "6M-2026"

    missing = tools.operational_lookup("ADMR", "Q2", 2026)
    assert missing["data"]["entries"] == []
    assert missing["data"]["state"] in {
        "not_captured", "not_disclosed", "not_yet_published"
    }


def test_read_document_page_text():
    result = tools.read_document("ITMG", "itmg_2026-08-10_news_release", 1)
    _assert_envelope(result, "read_document")
    data = result["data"]
    assert data["page"] == 1
    assert data["pages"] == 8
    assert "ITMG" in data["text"]
    assert len(data["text"]) <= tools.READ_PAGE_CHARS
    assert data["url"].startswith("https://")
    assert result["ledger"][0]["kind"] == "page"


def test_read_document_mapa_q1_page_72():
    result = tools.read_document("MAPA", "mapa_q1_2026_filing", 72)
    _assert_envelope(result, "read_document")
    data = result["data"]
    assert data["pages"] == 91
    for needle in (
        "Net revenues by geographical market",
        "3.775.872",
        "4.954.727",
        "Indonesia",
    ):
        assert needle in data["text"]


def test_read_document_mapa_q2_page_75():
    result = tools.read_document("MAPA", "mapa_q2_2026_filing", 75)
    _assert_envelope(result, "read_document")
    data = result["data"]
    assert data["pages"] == 96
    for needle in ("7.711.678", "10.122.756", "Kamboja", "26.192"):
        assert needle in data["text"]


def test_read_document_unknown_doc():
    with pytest.raises(paths.ArtifactMissing):
        tools.read_document("ITMG", "not_a_real_document", 1)


def test_notes_search_and_fetch(monkeypatch):
    def fake_run(args, action):
        if action == "notes_search":
            return [{
                "id": "chunk-1",
                "rank": 1,
                "rrf": 0.0312,
                "path": "analysis/ITMG Q2 2026 Analysis.md",
                "section": "Costs",
                "period": "Q2 2026",
                "tickers": ["ITMG"],
                "snippet": "Strip ratio rose in Q2.",
            }]
        return {
            "id": "chunk-1",
            "path": "analysis/ITMG Q2 2026 Analysis.md",
            "title": "ITMG Q2 2026 Analysis",
            "section": "Costs",
            "period": "Q2 2026",
            "tickers": ["ITMG"],
            "text": "x" * 5000,
        }

    monkeypatch.setattr(tools, "_run_rag", fake_run)

    search = tools.notes_search("strip ratio", ticker="ITMG", limit=5)
    _assert_envelope(search, "notes_search")
    assert search["data"]["results"][0]["id"] == "chunk-1"

    fetch = tools.notes_fetch("chunk-1")
    _assert_envelope(fetch, "notes_fetch")
    assert len(fetch["data"]["text"]) == 4000


def test_notes_search_input_validation(monkeypatch):
    monkeypatch.setattr(tools, "_run_rag", lambda args, action: [])
    with pytest.raises(paths.ToolInputError):
        tools.notes_search("")
    with pytest.raises(paths.ToolInputError):
        tools.notes_search("coal", limit=0)


def test_notes_search_respects_output_cap(monkeypatch):
    def fake_run(args, action):
        return [
            {
                "id": f"chunk-{index}",
                "rank": index,
                "rrf": 0.5,
                "path": "analysis/" + ("x" * 80) + ".md",
                "section": "S" * 50,
                "period": "Q2 2026",
                "tickers": ["ITMG"],
                "snippet": "y" * 400,
            }
            for index in range(1, 26)
        ]

    monkeypatch.setattr(tools, "_run_rag", fake_run)
    result = tools.notes_search("coal", limit=25)
    _assert_envelope(result, "notes_search")
    assert result["data"]["results_omitted"] > 0
    assert result["ledger"][0]["value"] == len(result["data"]["results"])


def test_all_tool_results_fit_the_output_cap():
    calls = [
        ("list_companies", lambda: tools.list_companies()),
        ("company_facts", lambda: tools.company_facts("ITMG", "Q2", 2026)),
        ("company_facts_all", lambda: tools.company_facts("PTBA", "Q2", 2026, "all")),
        ("metric_series", lambda: tools.metric_series("PTBA", "revenue")),
        ("period_derive", lambda: tools.period_derive("PTBA", "Q2", 2026, "revenue")),
        ("documents_status", lambda: tools.documents_status("ITMG", "Q2", 2026)),
        ("operational_lookup", lambda: tools.operational_lookup("ITMG", "Q2", 2026)),
    ]
    for name, call in calls:
        result = call()
        assert _size(result) <= paths.OUTPUT_CAP_BYTES, name
        assert result["ok"] is True


def test_schemas_registry_covers_every_tool():
    names = set(tools.schemas()["tools"])
    assert names == {
        "list_companies", "company_facts", "metric_series", "period_derive",
        "documents_status", "operational_lookup", "analysis_context",
        "read_document", "notes_search", "notes_fetch",
        "list_documents", "analysis_document", "analysis_search",
        "compare_supported",
    }


def test_analysis_context_tool_returns_bounded_secondary_snippets():
    result = tools.analysis_context("ITMG", "Q2", 2026)
    _assert_envelope(result, "analysis_context")
    data = result["data"]
    assert data["ticker"] == "ITMG"
    assert data["analysis_context"]
    assert all(item["path"].startswith("analysis/") for item in data["analysis_context"])
    assert all(item["secondary"] for item in data["analysis_context"])
    assert any(entry["kind"] == "note" for entry in result["ledger"])
    assert len(json.dumps(result)) <= paths.OUTPUT_CAP_BYTES


# ---------------------------------------------------------------------------
# Analysis-vault tools
# ---------------------------------------------------------------------------


def _assert_document_envelope(result: dict, tool: str) -> None:
    assert result["ok"] is True
    assert result["tool"] == tool
    assert "request" in result
    assert isinstance(result["ledger"], list)
    assert isinstance(result["caveats"], list)
    assert _size(result) <= 24_000


def test_list_documents_metadata_only():
    result = tools.list_documents(ticker="ITMG", limit=500)
    _assert_document_envelope(result, "list_documents")
    data = result["data"]
    assert data["count"] == len(data["documents"])
    assert data["total"] >= data["count"] > 0
    assert data["as_of"]
    document = next(row for row in data["documents"] if row["id"] == "itmg-q2-2026-analysis")
    assert document["title"]
    assert document["kind"] == "company"
    assert document["ticker"] == "ITMG"
    assert document["period"] == "Q2"
    assert document["year"] == 2026
    assert document["sha256"] and document["bytes"] > 0
    assert "content" not in document
    assert "sections" not in document
    assert result["ledger"][0]["unit"] == "documents"


def test_analysis_document_outline_then_section():
    outline = tools.analysis_document("itmg-q2-2026-analysis")
    _assert_document_envelope(outline, "analysis_document")
    data = outline["data"]
    assert data["document"]["id"] == "itmg-q2-2026-analysis"
    assert data["outline"]
    assert data["line_map"]["12"] == "1-executive-summary"
    assert data["truncated"] is True  # 44 KB note > the tool's content threshold
    assert data["content_omitted"] is True
    assert data["content"] is None

    section = tools.analysis_document(
        "itmg-q2-2026-analysis", section="1. Executive Summary"
    )
    _assert_document_envelope(section, "analysis_document")
    body = section["data"]
    assert body["section"]["id"] == "1-executive-summary"
    assert body["content"].startswith("## 1. Executive Summary")
    assert body["start_line"] == 12
    assert body["truncated"] is False
    assert body["next_line"] is None
    assert "outline" not in body
    assert body.get("ledger_omitted", 0) >= 0
    assert section["ledger"]
    for entry in section["ledger"]:
        assert entry["kind"] == "document_number"
        assert entry["source"].startswith("analysis/ITMG Q2 2026 Analysis.md:")
        meta = entry["meta"]
        assert meta["document"] == "itmg-q2-2026-analysis"
        assert meta["section"] == "1. Executive Summary"
        assert meta["section_id"] == "1-executive-summary"
        assert meta["line"] >= 12
        assert meta["quote"]
        assert entry["variants"]

    with pytest.raises(paths.ToolInputError):
        tools.analysis_document("itmg-q2-2026-analysis", section="no such section")
    with pytest.raises(paths.ArtifactMissing):
        tools.analysis_document("does-not-exist")


def test_analysis_document_uses_next_line_cursor_for_large_sections():
    document = tools.analysis_document("itmg-q2-2026-analysis")
    root = document["data"]["outline"][0]
    first = tools.analysis_document(
        "itmg-q2-2026-analysis", section=root["id"]
    )
    _assert_document_envelope(first, "analysis_document")
    assert first["data"]["truncated"] is True
    assert first["data"]["next_line"] > first["data"]["start_line"]

    second = tools.analysis_document(
        "itmg-q2-2026-analysis",
        section=root["id"],
        next_line=first["data"]["next_line"],
    )
    _assert_document_envelope(second, "analysis_document")
    assert second["data"]["start_line"] == first["data"]["next_line"]
    assert second["data"]["content"]

    with pytest.raises(paths.ToolInputError):
        tools.analysis_document(
            "itmg-q2-2026-analysis", next_line=10
        )


def test_analysis_document_small_note_returns_full_content():
    result = tools.analysis_document("companies-itmg")
    _assert_document_envelope(result, "analysis_document")
    data = result["data"]
    assert data["document"]["kind"] == "company"
    assert data["document"]["period"] is None
    assert data["content"].startswith("[[ITMG Q1 2026 Analysis]]")
    assert data["content_omitted"] is False


def test_analysis_search_matches_and_not_in_document():
    found = tools.analysis_search(
        "strip ratio", document_id="itmg-q2-2026-analysis", limit=3
    )
    _assert_document_envelope(found, "analysis_search")
    data = found["data"]
    assert data["matches"]
    assert "state" not in data
    match = data["matches"][0]
    for key in ("document", "path", "title", "section", "line", "quote", "snippet"):
        assert key in match
    assert match["document"] == "itmg-q2-2026-analysis"
    assert found["ledger"]
    assert all(entry["kind"] == "document_number" for entry in found["ledger"])

    missing = tools.analysis_search(
        "quantum blockchain zzz", document_id="itmg-q2-2026-analysis"
    )
    _assert_document_envelope(missing, "analysis_search")
    assert missing["data"]["matches"] == []
    assert missing["data"]["state"] == "not_in_document"
    evidence = missing["data"]["evidence"]
    assert evidence["sections_searched"] > 0
    assert "quantum" in evidence["terms"]
    assert missing["ledger"] == []

    corpus = tools.analysis_search("coal production", ticker="ITMG", limit=5)
    assert corpus["data"]["matches"]
    assert len(corpus["data"]["matches"]) <= 5

    with pytest.raises(paths.ToolInputError):
        tools.analysis_search("")
    with pytest.raises(paths.ToolInputError):
        tools.analysis_search("coal", limit=0)


def test_compare_supported_matrix_is_cited_and_unranked():
    result = tools.compare_supported(
        ["ITMG", "PTBA", "PTPS", "ESSA"],
        ["coal_production", "revenue"],
        "Q2",
        2026,
    )
    assert result["ok"] is True
    assert result["tool"] == "compare_supported"
    assert _size(result) <= paths.OUTPUT_CAP_BYTES + 2000

    data = result["data"]
    assert data["period"] == "Q2"
    assert data["year"] == 2026
    assert data["columns"] == ["ITMG", "PTBA", "PTPS", "ESSA"]
    assert data["coverage"] == {
        "requested": 4,
        "answered": 4,
        "scope": "supported_set",
    }
    assert data["excluded"] == []

    production = next(row for row in data["rows"] if row["metric"] == "coal_production")
    assert production["unit"] == "Mt"
    itmg = next(cell for cell in production["cells"] if cell["ticker"] == "ITMG")
    assert itmg["status"] == "ok"
    assert itmg["value"] == 5.3
    assert itmg["source"] == "operational:itmg_2026-08-10_news_release:1"

    revenue = next(row for row in data["rows"] if row["metric"] == "revenue")
    assert revenue["unit"] == "mixed"
    assert all(cell["unit"] for cell in revenue["cells"] if cell["status"] == "ok")

    populated = sum(
        1 for row in data["rows"] for cell in row["cells"] if cell["status"] == "ok"
    )
    assert len(result["ledger"]) == populated
    assert all(entry["kind"] in {"money", "count", "ratio"} for entry in result["ledger"])
    assert all(entry["source"] for entry in result["ledger"])

    serialized = json.dumps(data).lower()
    for forbidden in ("score", "rank", "recommendation", "best_pick"):
        assert forbidden not in serialized


def test_compare_supported_derived_metrics_have_one_ledger_entry_per_cell():
    result = tools.compare_supported(
        ["ITMG", "BBCA"], ["gross_margin", "revenue_growth"], "Q2", 2026
    )
    data = result["data"]
    populated = sum(
        1 for row in data["rows"] for cell in row["cells"] if cell["status"] == "ok"
    )
    assert populated >= 2
    assert len(result["ledger"]) == populated
    assert any(
        (entry.get("meta") or {}).get("derived") for entry in result["ledger"]
    )


def test_compare_supported_excludes_unsupported_and_not_applicable():
    result = tools.compare_supported(
        ["ITMG", "BBCA", "ZZZZ"],
        ["gross_margin", "not_a_metric"],
        "Q2",
        2026,
    )
    data = result["data"]
    assert data["columns"] == ["ITMG", "BBCA"]
    assert data["coverage"] == {
        "requested": 3,
        "answered": 1,
        "scope": "supported_set",
    }
    excluded = {(entry["ticker"], entry["metric"]): entry for entry in data["excluded"]}
    assert "ZZZZ" in {ticker for ticker, _metric in excluded}
    assert ("BBCA", "gross_margin") in excluded
    assert any("gross_profit" in entry["reason"] for entry in data["excluded"])
    assert ("ITMG", "not_a_metric") in excluded

    margin = next(row for row in data["rows"] if row["metric"] == "gross_margin")
    bca = next(cell for cell in margin["cells"] if cell["ticker"] == "BBCA")
    assert bca["status"] == "not_applicable"
    assert bca["value"] is None


def test_compare_supported_limits_and_period_states():
    with pytest.raises(paths.ToolInputError):
        tools.compare_supported(
            ["ITMG", "PTBA", "PTPS", "ESSA", "ADRO", "ADMR"],
            ["revenue"],
            "Q2",
            2026,
        )
    with pytest.raises(paths.ToolInputError):
        tools.compare_supported(
            ["ITMG"],
            ["revenue", "net_profit", "coal_production", "gross_margin", "cash_cost"],
            "Q2",
            2026,
        )

    unsupported_period = tools.compare_supported(["ITMG"], ["revenue"], "Q3", 2026)
    data = unsupported_period["data"]
    assert data["coverage"]["answered"] == 0
    assert data["rows"][0]["cells"][0]["status"] == "unsupported"
    assert data["excluded"]


def test_compare_supported_max_shape_fits_the_cap():
    result = tools.compare_supported(
        ["ITMG", "PTBA", "PTPS", "ESSA", "ADRO"],
        ["coal_production", "revenue", "net_profit", "gross_margin"],
        "Q2",
        2026,
    )
    assert _size(result) <= paths.OUTPUT_CAP_BYTES
    populated = sum(
        1 for row in result["data"]["rows"]
        for cell in row["cells"] if cell["status"] == "ok"
    )
    assert len(result["ledger"]) + (result["data"].get("ledger_omitted") or 0) >= populated


