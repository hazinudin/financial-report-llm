"""
Stage 0 — deterministic markdown reducer (no LLM).

Strips the ~80% of a docling-converted IDX report that is noise for analysis:
front matter (cover/TOC/auditor/director letter), the Material Accounting Policy
boilerplate note, repeated bilingual page banners, and `<!-- image -->` tags.

Input:  results/{TICKER}_{Q}_{YEAR}.md  (bilingual, ~4,000+ lines)
Output: idx_data/analysis/preprocessed/{TICKER}_{Q}_{YEAR}.md  (~700-900 lines)
        idx_data/analysis/preprocessed/{TICKER}_{Q}_{YEAR}.index.json  (section map)

The section index records line ranges (1-indexed, into the OUTPUT file) for the
primary-statements block and each surviving numbered note — consumed by the
chunker to build extraction chunks by note boundary (never mid-table).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_DIR = _PROJECT_ROOT / "results"
PREPROCESSED_DIR = _PROJECT_ROOT / "idx_data" / "analysis" / "preprocessed"

# Anchors for the four primary financial statements (standalone title lines,
# NOT the TOC table rows which begin with "|").
BS_ANCHOR = re.compile(r"Laporan Posisi Keuangan", re.I)
PL_ANCHOR = re.compile(r"Laporan Laba Rugi", re.I)
EQ_ANCHOR = re.compile(r"Laporan Perubahan Ekuitas", re.I)
CF_ANCHOR = re.compile(r"Laporan (?:Arus Kas|Posisi Keuangan.*Arus)", re.I)
CF_ANCHOR2 = re.compile(r"Laporan Arus Kas", re.I)

NOTE_HEADING = re.compile(r"^##\s+(\d+)\.\s+(.*)$")
POLICY_TITLE = re.compile(r"Material Accounting Policy Information|Informasi Kebijakan Akuntansi Material", re.I)

# Page-header banners repeated on every page (bilingual, single line).
PAGE_BANNER = re.compile(r"^PT .{2,80} ENTITAS ANAK Catatan atas", re.I)
PAGE_BANNER2 = re.compile(r"^Catatan atas Laporan Keuangan Interim Konsolidasian \d", re.I)
IMAGE_TAG = re.compile(r"<!-- image -->")
DIRECTOR_BANNER = re.compile(r"SURAT PERNYATAAN DIREKSI", re.I)


def _find_notes_start(lines: list[str]) -> int | None:
    for i, ln in enumerate(lines):
        if NOTE_HEADING.match(ln):
            return i
    return None


def _statement_block_range(lines: list[str]) -> tuple[int, int] | None:
    start = None
    for i, ln in enumerate(lines):
        if BS_ANCHOR.search(ln):
            start = i
            break
    if start is None:
        return None
    notes_start = _find_notes_start(lines)
    end = notes_start if notes_start is not None else len(lines)
    return start, end


def preprocess(text: str) -> tuple[str, dict]:
    """Return (cleaned_markdown, section_index). Pure function — unit-testable."""
    raw_lines = text.splitlines()

    stmt_range = _statement_block_range(raw_lines)
    notes_start = _find_notes_start(raw_lines)
    if stmt_range is None or notes_start is None:
        # Best effort: keep everything that survives the line filters.
        cleaned_lines = [ln for ln in raw_lines if not _is_noise_line(ln)]
        return "\n".join(cleaned_lines) + "\n", {"statements": None, "notes": {}}

    bs_start, _stmt_end = stmt_range  # stmt_end == notes_start

    # ── Build the kept-segment list from the original line indices ──
    # 1) primary statements: [bs_start, notes_start)
    # 2) notes: [notes_start, end) with the policy boilerplate removed
    keep: list[tuple[int, int]] = [(bs_start, notes_start)]

    # Locate the policy heading within the notes region.
    policy_idx = None
    for i in range(notes_start, len(raw_lines)):
        m = NOTE_HEADING.match(raw_lines[i])
        if m and POLICY_TITLE.search(m.group(2)):
            policy_idx = i
            break

    if policy_idx is not None:
        # Drop until the next numbered note heading with number >= 4 (the policy
        # note re-uses note numbers 2/3 for its sub-sections).
        drop_until = len(raw_lines)
        for i in range(policy_idx + 1, len(raw_lines)):
            m = NOTE_HEADING.match(raw_lines[i])
            if m and int(m.group(1)) >= 4:
                drop_until = i
                break
        # notes before policy
        if policy_idx > notes_start:
            keep.append((notes_start, policy_idx))
        # notes from the resumption point onward
        if drop_until < len(raw_lines):
            keep.append((drop_until, len(raw_lines)))
        else:
            # No note >=4 found after policy — keep everything after policy anyway
            keep.append((policy_idx, len(raw_lines)))
    else:
        keep.append((notes_start, len(raw_lines)))

    # Materialize kept lines, dropping per-line noise within them.
    cleaned: list[str] = []
    offset_map: list[int] = []  # cleaned_index -> original line number (1-indexed)
    for a, b in keep:
        for i in range(a, b):
            ln = raw_lines[i]
            if _is_noise_line(ln):
                continue
            cleaned.append(ln)
            offset_map.append(i + 1)

    # ── Build section index against the cleaned line numbers (1-indexed) ──
    index = {"statements": None, "notes": {}, "anchors": {}}

    # statements block: from the first cleaned line back to the line before the
    # first note heading in cleaned output.
    cleaned_notes_idx = None
    for ci, ln in enumerate(cleaned):
        if NOTE_HEADING.match(ln):
            cleaned_notes_idx = ci
            break
    if cleaned_notes_idx is not None:
        index["statements"] = [1, cleaned_notes_idx]  # exclusive end
    else:
        index["statements"] = [1, len(cleaned)]

    # note ranges: group consecutive numbered note headings (IDR + EN paired).
    # A note number N opens at the first ## N. line and closes at the next
    # distinct note number's opening. Sub-letter headings (## a.) are ignored.
    note_starts: list[tuple[int, int]] = []  # (number, cleaned_line_index 0-based)
    seen_nums: set[int] = set()
    for ci, ln in enumerate(cleaned):
        m = NOTE_HEADING.match(ln)
        if not m:
            continue
        num = int(m.group(1))
        if num in seen_nums:
            continue
        seen_nums.add(num)
        note_starts.append((num, ci))

    for idx, (num, ci) in enumerate(note_starts):
        end = note_starts[idx + 1][1] if idx + 1 < len(note_starts) else len(cleaned)
        index["notes"][str(num)] = [ci + 1, end]  # 1-indexed exclusive

    # statement sub-anchors (balance sheet, P&L, equity, cash flow) in cleaned space
    anchors = {}
    for ci, ln in enumerate(cleaned):
        if BS_ANCHOR.match(ln) and "balance_sheet" not in anchors:
            anchors["balance_sheet"] = ci + 1
        elif PL_ANCHOR.match(ln) and "income_statement" not in anchors:
            anchors["income_statement"] = ci + 1
        elif EQ_ANCHOR.match(ln) and "equity_changes" not in anchors:
            anchors["equity_changes"] = ci + 1
        elif CF_ANCHOR2.match(ln) and "cash_flow" not in anchors:
            anchors["cash_flow"] = ci + 1
    index["anchors"] = anchors
    index["original_line_count"] = len(raw_lines)
    index["cleaned_line_count"] = len(cleaned)

    return "\n".join(cleaned) + "\n", index


def _is_noise_line(ln: str) -> bool:
    if IMAGE_TAG.search(ln):
        return True
    if PAGE_BANNER.search(ln) or PAGE_BANNER2.search(ln):
        return True
    # standalone page-number banner "Halaman/ Page"
    if re.match(r"^Halaman\s*/?\s*Page\s*$", ln, re.I):
        return True
    return False


def run(ticker: str, year: int, quarter: str, force: bool = False, dry_run: bool = False) -> dict:
    """Preprocess one report; write outputs to idx_data/analysis/preprocessed/."""
    src = RESULTS_DIR / f"{ticker}_{quarter}_{year}.md"
    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src} (run report_pipeline download/convert first)")

    out_dir = PREPROCESSED_DIR
    out_md = out_dir / f"{ticker}_{quarter}_{year}.md"
    out_idx = out_dir / f"{ticker}_{quarter}_{year}.index.json"

    if out_md.exists() and not force and not dry_run:
        print(f"  [SKIP] {out_md.name} (already preprocessed)")
        return {"output": str(out_md), "status": "skipped"}

    text = src.read_text(encoding="utf-8")
    cleaned, index = preprocess(text)

    if dry_run:
        print(f"  [DRY] {src.name}: {len(text.splitlines())} -> {len(cleaned.splitlines())} lines")
        print(f"       notes present: {sorted(index['notes'].keys(), key=int)}")
        return {"status": "dry", "cleaned_lines": len(cleaned.splitlines()), "index": index}

    out_dir.mkdir(parents=True, exist_ok=True)
    out_md.write_text(cleaned, encoding="utf-8")
    out_idx.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
    reduction = 100 * (1 - len(cleaned.splitlines()) / max(1, len(text.splitlines())))
    print(f"  [PREP] {src.name}: {len(text.splitlines())} -> {len(cleaned.splitlines())} lines (-{reduction:.0f}%)")
    print(f"         statements: {index['statements']}  notes: {sorted(index['notes'].keys(), key=int)}")
    return {"output": str(out_md), "index": str(out_idx), "status": "success",
            "cleaned_lines": len(cleaned.splitlines())}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Stage 0 deterministic preprocessor")
    p.add_argument("--ticker", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", required=True)
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    run(a.ticker, a.year, a.quarter, force=a.force, dry_run=a.dry_run)