"""Fact-sheet loading, section shaping, and provenance resolution."""

from __future__ import annotations

import pytest

from chat_app import facts, paths


def test_load_facts_itmg_q2():
    sheet = facts.load_facts("ITMG", "Q2", 2026)
    assert sheet["meta"]["ticker"] == "ITMG"
    assert sheet["meta"]["currency"] == "USD"
    assert sheet["meta"]["units"] == "thousand"
    assert sheet["income_statement"]["lines"]["revenue"] == 1_000_491.0


def test_compact_core_shape_and_provenance():
    sheet = facts.load_facts("ITMG", "Q2", 2026)
    data = facts.compact_sections(sheet, "core")

    assert data["ticker"] == "ITMG"
    assert data["period"] == "Q2"
    assert data["year"] == 2026
    assert data["meta"]["period_end"] == "2026-06-30"

    revenue = data["sections"]["income_statement"]["revenue"]
    assert revenue["value"] == 1_000_491.0
    assert revenue["currency"] == "USD"
    assert revenue["units"] == "thousand"
    assert revenue["period_type"] == "cumulative"
    assert revenue["provenance"]["sheet"] == "1311000"
    assert revenue["provenance"]["row"] == 6
    assert revenue["value_idr_bn"] > 0

    assert data["sections"]["balance_sheet"]["total_assets"]["period_type"] == "instant"
    assert data["sections"]["cash_flow"]["ocf"]["period_type"] == "cumulative"
    assert data["completeness"]["financials"] == "complete"


def test_compact_sections_only_emits_present_lines():
    sheet = facts.load_facts("BBCA", "Q2", 2026)
    data = facts.compact_sections(sheet, "core")
    income = data["sections"]["income_statement"]
    assert "revenue" in income
    assert "cogs" not in income
    assert "gross_profit" not in income


def test_compact_details_only_on_request():
    sheet = facts.load_facts("ITMG", "Q2", 2026)
    core = facts.compact_sections(sheet, "core")
    assert "details" not in core["sections"]

    details = facts.compact_sections(sheet, "details")
    assert "details" in details["sections"]


def test_invalid_sections_rejected():
    sheet = facts.load_facts("ITMG", "Q2", 2026)
    with pytest.raises(paths.ToolInputError):
        facts.compact_sections(sheet, "everything")


def test_statement_lines():
    lines = facts.statement_lines("ITMG", "Q2", 2026, "income_statement")
    assert lines["revenue"]["value"] == 1_000_491.0
    assert lines["revenue"]["provenance"]["sheet"] == "1311000"
    assert lines["gross_profit"]["provenance"]["row"] == 8

    with pytest.raises(paths.ToolInputError):
        facts.statement_lines("ITMG", "Q2", 2026, "equity_statement")


def test_load_ratios():
    ratios = facts.load_ratios("ITMG", "Q2", 2026)
    assert "gross_margin" in ratios
    assert ratios["gross_margin"]["value"] == pytest.approx(0.256351)
    assert facts.load_ratios("ITMG", "Q4", 2026) == {}


def test_resolve_metric_aliases():
    assert facts.resolve_metric("revenue") == ("income_statement", "revenue")
    assert facts.resolve_metric("operating_cash_flow") == ("cash_flow", "ocf")
    assert facts.resolve_metric("net income") == ("income_statement", "net_profit")
    with pytest.raises(paths.ToolInputError):
        facts.resolve_metric("ebitda_magic")


def test_unsupported_ticker_raises_with_alternatives():
    with pytest.raises(paths.UnsupportedRequest) as excinfo:
        facts.load_facts("ZZZZ", "Q2", 2026)
    assert "ITMG" in excinfo.value.payload["supported_tickers"]


def test_unsupported_period_raises():
    with pytest.raises(paths.UnsupportedRequest) as excinfo:
        facts.load_facts("ITMG", "Q3", 2026)
    assert "Q2 2026" in excinfo.value.payload["supported_periods"]


def test_missing_artifact_for_supported_period(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "FACTS_DIR", tmp_path)
    with pytest.raises(paths.UnsupportedRequest):
        facts.load_facts("ITMG", "Q2", 2026)
