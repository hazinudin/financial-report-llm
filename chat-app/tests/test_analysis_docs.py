"""The analysis vault index: IDs, metadata, sections, ledger and search."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from chat_app import analysis_docs, paths


ITMG_ID = "itmg-q2-2026-analysis"


@pytest.fixture()
def vault(tmp_path, monkeypatch):
    """Point the index at a throwaway vault and drop the cache afterwards."""
    monkeypatch.setattr(paths, "ANALYSIS_DIR", tmp_path)
    analysis_docs.clear_cache()
    yield tmp_path
    analysis_docs.clear_cache()


def _write(path: Path, text: str, *, newline: str = "\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.replace("\n", newline), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Real vault: metadata and stable IDs
# ---------------------------------------------------------------------------


def test_itmg_document_metadata_and_sections():
    rows = analysis_docs.list_documents(ticker="ITMG", limit=500)
    by_id = {row["id"]: row for row in rows}
    assert ITMG_ID in by_id
    row = by_id[ITMG_ID]
    assert row["path"] == "analysis/ITMG Q2 2026 Analysis.md"
    assert row["kind"] == "company"
    assert row["ticker"] == "ITMG"
    assert row["period"] == "Q2"
    assert row["year"] == 2026
    assert row["sha256"] and len(row["sha256"]) == 64
    assert row["bytes"] > 0 and row["words"] > 0
    assert row["section_count"] > 10
    assert "ITMG" in row["title"]
    assert row["updated_at"].endswith("Z")

    document = analysis_docs.get_document(ITMG_ID)
    first = document["sections"][0]
    assert first["level"] == 1
    assert first["line"] >= 1
    executive = next(
        section for section in document["sections"]
        if section["title"] == "1. Executive Summary"
    )
    assert executive["id"] == "1-executive-summary"
    assert executive["level"] == 2
    assert executive["line"] == 12
    assert executive["end_line"] >= executive["line"]
    assert document["content"].count("# PT Indo Tambangraya") == 1


def test_unscoped_notes_are_first_class():
    rows = analysis_docs.list_documents(limit=500)
    by_id = {row["id"]: row for row in rows}

    profile = by_id["companies-itmg"]
    assert profile["kind"] == "company"
    assert profile["ticker"] == "ITMG"
    assert profile["period"] is None
    assert profile["year"] is None

    commodity = by_id["commodities-gold"]
    assert commodity["kind"] == "commodity"
    assert commodity["ticker"] is None
    assert commodity["period"] is None

    article = next(row for row in rows if row["path"].startswith("analysis/articles/"))
    assert article["kind"] == "article"

    sector = by_id["indonesia-fertilizer-market"]
    assert sector["kind"] == "sector"


def test_filters_and_limit():
    rows = analysis_docs.list_documents(ticker="ITMG", period="Q2", year=2026, kind="company")
    assert [row["id"] for row in rows] == [ITMG_ID]

    assert analysis_docs.list_documents(kind="commodity", limit=3) == \
        analysis_docs.list_documents(kind="commodity", limit=3)
    assert len(analysis_docs.list_documents(limit=5)) == 5
    with pytest.raises(paths.ToolInputError):
        analysis_docs.list_documents(kind="not-a-kind")
    with pytest.raises(paths.ToolInputError):
        analysis_docs.list_documents(limit=0)


def test_document_ledger_has_line_quotes_and_prices():
    ledger = analysis_docs.document_ledger(ITMG_ID)
    assert ledger
    revenue = next(
        entry for entry in ledger
        if entry["value"] == 1_000_491.0
        and (entry.get("meta") or {}).get("line") == 14
    )
    meta = revenue["meta"]
    assert revenue["kind"] == "document_number"
    assert revenue["source"].startswith("analysis/ITMG Q2 2026 Analysis.md:")
    assert meta["document"] == ITMG_ID
    assert meta["section"] == "1. Executive Summary"
    assert meta["section_id"] == "1-executive-summary"
    assert meta["line"] == 14
    assert "1,000,491" in meta["quote"]
    # raw/scaled variants let a display rounding match
    assert 1.000491 in revenue["variants"]

    table_entry = next(
        entry for entry in ledger
        if entry["value"] == 919_416.0 and entry["meta"].get("table_row")
    )
    quote = table_entry["meta"]["quote"]
    assert quote.startswith("|") and quote.endswith("|")
    assert "919,416" in quote


def test_section_slicing_and_chunk_cursor():
    chunk = analysis_docs.document_section(
        ITMG_ID, "1. Executive Summary", max_chars=600
    )
    assert chunk["section"]["id"] == "1-executive-summary"
    assert chunk["content"].startswith("## 1. Executive Summary")
    assert chunk["truncated"] is True
    assert chunk["next_line"] == chunk["end_line"] + 1

    rest = analysis_docs.document_section(
        ITMG_ID, "1. Executive Summary", next_line=chunk["next_line"], max_chars=600
    )
    assert rest["start_line"] == chunk["next_line"]
    assert rest["content"]

    with pytest.raises(paths.ToolInputError):
        analysis_docs.document_section(ITMG_ID, "not a real section")
    with pytest.raises(paths.ToolInputError):
        analysis_docs.document_section(
            ITMG_ID, "1. Executive Summary", next_line=9999
        )


def test_search_scoped_and_corpus():
    matches = analysis_docs.search_documents(
        "strip ratio", document_id=ITMG_ID, limit=5
    )
    assert matches
    for match in matches:
        assert match["document"] == ITMG_ID
        assert match["path"].startswith("analysis/")
        assert match["section"] and match["section_id"]
        assert match["line"] >= 1
        assert "strip" in match["quote"].lower()
        assert match["snippet"]

    assert analysis_docs.search_documents(
        "quantum blockchain zzz", document_id=ITMG_ID
    ) == []

    corpus = analysis_docs.search_documents("coal production", limit=10)
    assert corpus
    assert len({match["document"] for match in corpus}) >= 1

    with pytest.raises(paths.ToolInputError):
        analysis_docs.search_documents("   ")


def test_search_scoped_natural_language_query():
    matches = analysis_docs.search_documents(
        "geographic revenue by country Indonesia Vietnam quarterly split",
        document_id="mapa-q2-2026-analysis",
        limit=5,
    )
    assert matches
    assert any(
        "Indonesia accounted" in match["quote"]
        or "7,711,678" in match["quote"]
        or "7.711.678" in match["quote"]
        for match in matches
    )


def test_search_scoped_two_absent_terms_is_silent():
    assert analysis_docs.search_documents(
        "quantum blockchain", document_id="mapa-q2-2026-analysis"
    ) == []


def test_unknown_document_is_explicit():
    with pytest.raises(paths.ArtifactMissing):
        analysis_docs.get_document("does-not-exist")
    with pytest.raises(paths.ArtifactMissing):
        analysis_docs.document_content("does-not-exist")


# ---------------------------------------------------------------------------
# Synthetic vault: frontmatter, collisions, idempotence, unreadable files
# ---------------------------------------------------------------------------


def test_frontmatter_title_tags_aliases_and_heading_lines(vault):
    _write(vault / "Note.md", """---
title: "Custom Title"
tags: ["#TEST", "#earnings"]
aliases: ["Alias One", "Alias Two"]
ticker: TEST
period: Q3
year: 2027
---

# Heading One

Paragraph.

## 1. First Section

Text.

### 1.1 Child Section

More text.
""")
    rows = analysis_docs.list_documents(ticker="TEST", limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Custom Title"
    assert row["tags"] == ["TEST", "earnings"]
    assert row["aliases"] == ["Alias One", "Alias Two", "Note"]
    assert row["kind"] == "company"
    assert row["period"] == "Q3"
    assert row["year"] == 2027

    document = analysis_docs.get_document(row["id"])
    # Frontmatter is 8 lines; the blank line is 9 and # Heading One is line 10.
    assert document["content"].startswith("---\ntitle:")
    assert document["body_start"] == 8
    assert document["sections"][0]["line"] == 10
    assert document["sections"][1]["id"] == "1-first-section"
    assert document["sections"][1]["line"] == 14
    assert document["sections"][2]["id"] == "1-1-child-section"
    assert document["sections"][2]["line"] == 18


def test_table_rows_are_ledger_quotes(vault):
    _write(vault / "Table.md", """# Table Note

## 1. Summary

| Item | H1 2026 (US$'000) |
|---|---|
| Net revenue | 1,000,491 |

Revenue was US$1,000,491k (+8.8%).
""")
    document = analysis_docs.list_documents(limit=10)[0]
    ledger = analysis_docs.document_ledger(document["id"])
    table = next(
        entry for entry in ledger
        if entry["value"] == 1_000_491.0
        and entry["meta"].get("table_row")
    )
    assert table["meta"]["quote"] == "| Net revenue | 1,000,491 |"
    assert table["meta"]["line"] == 7
    assert table["meta"]["section"] == "1. Summary"
    assert table["meta"]["section_id"] == "1-summary"
    assert table["source"].endswith(":7")
    assert table["kind"] == "document_number"

    percent = next(entry for entry in ledger if entry["unit"] == "%")
    assert percent["value"] == 8.8
    assert 0.088 in percent["variants"]


def test_path_collision_gets_a_short_hash_suffix(vault):
    _write(vault / "a" / "Note.md", "# A\n")
    _write(vault / "a-note.md", "# B\n")
    rows = analysis_docs.list_documents(limit=10)
    assert len(rows) == 2
    ids = {row["id"] for row in rows}
    assert len(ids) == 2
    for document_id in ids:
        suffix = document_id.rsplit("-", 1)[-1]
        assert len(suffix) == 6
        int(suffix, 16)


def test_id_survives_an_edit_and_hash_changes(vault):
    path = _write(vault / "ITMG Q2 2026 Analysis.md", "# Title\n\n## 1. Summary\n\nOld.\n")
    first = analysis_docs.list_documents(limit=10)[0]
    first_hash = first["sha256"]

    path.write_text("# Title\n\n## 1. Summary\n\nNew text.\n", encoding="utf-8")
    stamp = path.stat().st_mtime + 10
    os.utime(path, (stamp, stamp))

    second = analysis_docs.list_documents(limit=10)[0]
    assert second["id"] == first["id"] == ITMG_ID
    assert second["sha256"] != first_hash


def test_index_reuses_unchanged_notes_and_invalidates_on_edit(vault, monkeypatch):
    _write(vault / "One.md", "# One\n\n## 1. Summary\n\nOld text.\n")

    parses = {"count": 0}
    real_parse = analysis_docs._parse

    def counting_parse(*args, **kwargs):
        parses["count"] += 1
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(analysis_docs, "_parse", counting_parse)

    first = analysis_docs.list_documents(limit=10)
    assert parses["count"] == 1
    second = analysis_docs.list_documents(limit=10)
    assert parses["count"] == 1
    assert second == first

    path = vault / "One.md"
    path.write_text("# One\n\n## 1. Summary\n\nNew text.\n", encoding="utf-8")
    stamp = path.stat().st_mtime + 10
    os.utime(path, (stamp, stamp))

    third = analysis_docs.list_documents(limit=10)
    assert parses["count"] == 2
    assert third[0]["sha256"] != first[0]["sha256"]


def test_unreadable_document_is_422_ready(vault):
    path = vault / "Broken.md"
    path.write_bytes(b"---\ntags: [\xff]\n---\n# Broken\n")
    rows = analysis_docs.list_documents(limit=10)
    assert len(rows) == 1
    assert rows[0]["unreadable"]

    with pytest.raises(paths.ChatError) as excinfo:
        analysis_docs.get_document(rows[0]["id"])
    assert not isinstance(excinfo.value, paths.ArtifactMissing)


def test_content_is_byte_for_byte(vault):
    text = "---\ntags: [X]\n---\n\n# Title\n\nBody.\n"
    path = _write(vault / "Byte.md", text)
    document = analysis_docs.get_document(analysis_docs.list_documents()[0]["id"])
    assert document["content"] == path.read_text(encoding="utf-8")
    assert document["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
