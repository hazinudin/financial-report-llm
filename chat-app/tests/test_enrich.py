"""Additive, idempotent fact-sheet enrichment."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from chat_app import enrich, paths

SHEETS = ("ITMG", "PTBA", "BBCA")


@pytest.fixture()
def facts_copy(monkeypatch):
    """Copy three real sheets into an in-repo temp dir (paths uses relative_to).

    Additive keys are stripped so the fixture exercises a first enrichment even
    when the developer has already run ``enrich --all`` against the live data.
    """
    root = Path(tempfile.mkdtemp(dir=str(paths.PROJECT_ROOT), prefix=".facts-test-"))
    directory = root / "facts"
    directory.mkdir()
    source = paths.PROJECT_ROOT / "idx_data" / "analysis" / "facts"
    for ticker in SHEETS:
        target = directory / f"{ticker}_Q2_2026.yaml"
        shutil.copy(source / target.name, target)
        data = _load(target)
        _strip_additive(data)
        target.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8",
        )
    monkeypatch.setattr(paths, "FACTS_DIR", directory)
    try:
        yield directory, source
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _strip_additive(data: dict) -> None:
    data.pop("operational", None)
    data.pop("documents", None)
    data.pop("completeness", None)
    meta = data.get("meta") or {}
    meta.pop("enriched_at", None)
    meta.pop("enriched_hash", None)
    for name in ("income_statement", "balance_sheet", "cash_flow"):
        (data.get(name) or {}).pop("provenance", None)


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_enrich_adds_only_additive_keys(facts_copy):
    directory, source = facts_copy
    for ticker in SHEETS:
        path = directory / f"{ticker}_Q2_2026.yaml"
        original = _load(source / f"{ticker}_Q2_2026.yaml")

        report = enrich.enrich_sheet(path)
        assert report["changed"] is True
        assert report["written"] is True

        enriched = _load(path)
        assert enriched["income_statement"]["lines"] == original["income_statement"]["lines"]
        assert enriched["balance_sheet"]["lines"] == original["balance_sheet"]["lines"]
        assert enriched["cash_flow"]["lines"] == original["cash_flow"]["lines"]
        assert enriched["meta"]["enriched_hash"] == report["enriched_hash"]
        assert enriched["meta"]["enriched_at"]
        assert "operational" in enriched
        assert "documents" in enriched
        assert "completeness" in enriched


def test_enrich_resolves_itmg_provenance(facts_copy):
    directory, _source = facts_copy
    path = directory / "ITMG_Q2_2026.yaml"
    enrich.enrich_sheet(path)
    enriched = _load(path)

    revenue = enriched["income_statement"]["provenance"]["revenue"]
    assert revenue["sheet"] == "1311000"
    assert revenue["row"] == 6
    assert enriched["balance_sheet"]["provenance"]["total_assets"]["sheet"] == "1210000"


def test_enrich_merges_operational_and_completeness(facts_copy):
    directory, _source = facts_copy
    path = directory / "ITMG_Q2_2026.yaml"
    enrich.enrich_sheet(path)
    enriched = _load(path)

    metrics = {entry["metric"] for entry in enriched["operational"]["entries"]}
    assert "coal_production" in metrics
    assert enriched["operational"]["drivers"]
    assert any(doc["status"] == "captured" for doc in enriched["documents"])
    assert enriched["completeness"]["financials"] == "complete"
    assert enriched["completeness"]["operational"] == "complete"
    assert enriched["completeness"]["as_of"]


def test_enrich_is_idempotent(facts_copy):
    directory, _source = facts_copy
    for ticker in SHEETS:
        path = directory / f"{ticker}_Q2_2026.yaml"
        enrich.enrich_sheet(path)
        first = path.read_bytes()

        second = enrich.enrich_sheet(path)
        assert second["changed"] is False
        assert second["written"] is False
        assert path.read_bytes() == first
        assert _load(path)["meta"]["enriched_hash"] == second["enriched_hash"]


def test_enrich_dry_run_does_not_write(facts_copy):
    directory, _source = facts_copy
    path = directory / "PTBA_Q2_2026.yaml"
    before = path.read_bytes()

    report = enrich.enrich_sheet(path, dry_run=True)
    assert report["changed"] is True
    assert report["written"] is False
    assert report["would_write"] is True
    assert path.read_bytes() == before


def test_fact_files_selection(facts_copy):
    directory, _source = facts_copy
    assert {p.name for p in enrich.fact_files(all_sheets=True)} == {
        f"{ticker}_Q2_2026.yaml" for ticker in SHEETS
    }
    assert [p.name for p in enrich.fact_files(ticker="ITMG")] == ["ITMG_Q2_2026.yaml"]
    assert enrich.fact_files(ticker="ITMG", quarter="Q1", year=2026) == []


def test_content_hash_ignores_timestamp(facts_copy):
    directory, _source = facts_copy
    path = directory / "ITMG_Q2_2026.yaml"
    payload = enrich._build_additive("ITMG", "Q2", 2026, _load(path))
    hash_a = enrich.content_hash(payload)
    payload["enriched_at"] = "2030-01-01T00:00:00"
    assert enrich.content_hash(payload) == hash_a
