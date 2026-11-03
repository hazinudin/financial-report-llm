"""Canonical units and cumulative→quarter derivation."""

from __future__ import annotations

import pytest

from chat_app import paths, periods


def test_period_type():
    assert periods.period_type("income_statement") == "cumulative"
    assert periods.period_type("cash_flow") == "cumulative"
    assert periods.period_type("balance_sheet") == "instant"
    with pytest.raises(paths.ToolInputError):
        periods.period_type("equity_statement")


def test_normalize_value_keeps_raw_and_unit():
    out = periods.normalize_value(1_000_491.0, "USD", "thousand")
    assert out["raw"] == 1_000_491.0
    assert out["currency"] == "USD"
    assert out["scale"] == 1_000.0
    # 1,000,491 thousand USD * 16,300 / 1e9
    assert out["value_idr_bn"] == pytest.approx(16_308.0033, rel=1e-6)


def test_normalize_value_none():
    out = periods.normalize_value(None, "IDR", "million")
    assert out["raw"] is None
    assert out["value_idr_bn"] is None


def test_unit_label():
    assert periods.unit_label("USD", "thousand") == "USD thousand"
    assert periods.unit_label("IDR", "million") == "IDR million"
    assert periods.unit_label("IDR", "full") == "IDR"


def test_derive_q1_is_cumulative_value():
    out = periods.derive_quarter("ITMG", "revenue", "Q1", 2026)
    assert out["formula"] == "tw1"
    assert out["operands"] == {"tw1": 497_577.0}
    assert out["quarter_value"] == 497_577.0
    assert out["source"] == {"tw1": "1311000:6"}
    assert out["unit"] == "USD thousand"


def test_derive_q2_revenue_exit_test():
    out = periods.derive_quarter("ITMG", "revenue", "Q2", 2026)
    assert out["formula"] == "tw2 - tw1"
    assert out["operands"] == {"tw2": 1_000_491.0, "tw1": 497_577.0}
    assert out["quarter_value"] == 502_914.0
    assert out["source"] == {"tw2": "1311000:6", "tw1": "1311000:6"}
    assert out["unit"] == "USD thousand"
    assert out["value_idr_bn"] == pytest.approx(8_197.4982, rel=1e-6)


def test_derive_q2_gross_profit():
    out = periods.derive_quarter("ITMG", "gross_profit", "Q2", 2026)
    assert out["operands"]["tw2"] == 256_477.0
    assert out["operands"]["tw1"] == 132_753.0
    assert out["quarter_value"] == 123_724.0
    assert out["source"]["tw2"] == "1311000:8"


def test_derive_q2_ptba_idr_units():
    out = periods.derive_quarter("PTBA", "revenue", "Q2", 2026)
    assert out["quarter_value"] == 12_098_179.0
    assert out["unit"] == "IDR million"
    assert out["source"] == {"tw2": "1311000:6", "tw1": "1311000:6"}


def test_derive_balance_sheet_rejected():
    with pytest.raises(paths.ToolInputError):
        periods.derive_quarter("ITMG", "total_assets", "Q2", 2026)


def test_derive_unknown_metric_rejected():
    with pytest.raises(paths.ToolInputError):
        periods.derive_quarter("ITMG", "ebitda_magic", "Q2", 2026)


def test_derive_unsupported_period():
    with pytest.raises(paths.UnsupportedRequest):
        periods.derive_quarter("ITMG", "revenue", "Q3", 2026)


def test_convert_value_between_scales():
    # 1,000 million IDR == 1,000,000 thousand IDR
    converted = periods.convert_value(1_000.0, "IDR", "million", "IDR", "thousand")
    assert converted == pytest.approx(1_000_000.0)
    same = periods.convert_value(5.0, "USD", "thousand", "USD", "thousand")
    assert same == 5.0
