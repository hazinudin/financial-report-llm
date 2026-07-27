"""
Stage 1 (part 1) — build LLM extraction chunks from the preprocessed report.

The preprocessed file is split into bounded chunks that fit the extraction
model's input budget. Chunks ALWAYS break at note boundaries (never mid-table);
a single oversized note is split at its sub-letter headings (`## a.`, `## b.` …).
The primary-statements block forms its own chunk (or is split by the four
statement anchors).

Output is a list of `Chunk` objects; extract.py runs each through the LLM.
Chunk names carry the note numbers they cover (e.g. "statements", "note01",
"notes16-19") so the merge stage can attribute provenance.

Token estimates use chars//4 (configurable). With ~99K tokens of bilingual
payload this yields ~6-8 chunks for a large report — more than the plan's
illustrative A/B/C, but the merge step consumes any number of chunk sheets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

NOTE_HEADING = re.compile(r"^##\s+(\d+)\.\s+(.*)$")
SUB_HEADING = re.compile(r"^##\s+([a-z])\.\s+(.*)$")
TOKEN_CHARS = 4  # ~4 chars/token heuristic


def _est_tokens(text: str) -> int:
    return max(1, len(text) // TOKEN_CHARS)


@dataclass
class Chunk:
    name: str
    covers: list[str]            # ["statements"] or ["1","4","5"] etc.
    text: str
    approx_tokens: int
    line_range: tuple[int, int]  # 1-indexed [start, end) into preprocessed file

    def payload(self, header: str) -> str:
        return f"### CHUNK: {self.name} ({', '.join(self.covers)})\n{header}\n\n{self.text}"


@dataclass
class Unit:
    covers: str                  # "statements" | note number string
    text: str
    tokens: int
    line_start: int


def load_config() -> dict:
    from pathlib import Path
    import yaml
    cfg_path = Path(__file__).resolve().parent.parent.parent / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("analysis", {}) or {}
    return {}


def _statement_units(lines: list[str], index: dict) -> list[Unit]:
    stmt = index.get("statements")
    if not stmt:
        return []
    a, b = stmt
    text = "\n".join(lines[a - 1: b - 1])
    if not text.strip():
        return []
    # Optionally split the statement block by the four anchors for finer chunks.
    anchors = index.get("anchors", {}) or {}
    cuts = sorted({v for v in anchors.values() if a <= v < b})
    if len(cuts) <= 1:
        return [Unit("statements", text + "\n", _est_tokens(text + "\n"), a)]
    units: list[Unit] = []
    bounds = [a] + cuts[1:] + [b]
    labels = ["balance_sheet", "income_statement", "equity_changes", "cash_flow"]
    for i in range(len(bounds) - 1):
        s, e = bounds[i], bounds[i + 1]
        seg = "\n".join(lines[s - 1: e - 1])
        label = labels[i] if i < len(labels) else f"stmt{i}"
        units.append(Unit("statements::" + label, seg + "\n", _est_tokens(seg + "\n"), s))
    # If any single statement unit is still huge, keep it whole (no mid-table split).
    return units or [Unit("statements", text + "\n", _est_tokens(text + "\n"), a)]


def _note_units(lines: list[str], index: dict) -> list[Unit]:
    units: list[Unit] = []
    notes = index.get("notes", {}) or {}
    for num, (a, b) in sorted(notes.items(), key=lambda kv: int(kv[0])):
        body = "\n".join(lines[a - 1: b - 1])
        # Find sub-letter heading offsets within this note.
        sub_offsets = []
        for i, ln in enumerate(lines[a - 1: b - 1], start=a):
            if SUB_HEADING.match(ln):
                sub_offsets.append(i)
        if not sub_offsets or sub_offsets[0] == a:
            units.append(Unit(num, body + "\n", _est_tokens(body + "\n"), a))
            continue
        # Split at sub-letter headings; preamble before first sub is its own unit.
        if sub_offsets[0] > a:
            pre = "\n".join(lines[a - 1: sub_offsets[0] - 1])
            units.append(Unit(num + "::preamble", pre + "\n", _est_tokens(pre + "\n"), a))
        bounds = sub_offsets + [b]
        for i in range(len(bounds) - 1):
            s, e = bounds[i], bounds[i + 1]
            seg = "\n".join(lines[s - 1: e - 1])
            units.append(Unit(num + f"::sub{i}", seg + "\n", _est_tokens(seg + "\n"), s))
    return units


def _pack(units: list[Unit], cap: int) -> list[Chunk]:
    """Greedily pack units into chunks under `cap` tokens. A unit larger than
    cap becomes its own chunk (oversized notes were already sub-split; a
    still-oversized unit is kept whole rather than risk a mid-table break)."""
    chunks: list[Chunk] = []
    cur: list[Unit] = []
    cur_tokens = 0
    cur_start = units[0].line_start if units else 0
    covers: list[str] = []

    def flush():
        nonlocal cur, cur_tokens, covers
        if not cur:
            return
        text = "".join(u.text for u in cur)
        names = sorted({u.covers.split("::")[0] for u in cur}, key=lambda x: (x != "statements", x))
        cname = "statements" if names == ["statements"] else _range_name(names)
        chunks.append(Chunk(cname, names, text, _est_tokens(text),
                            (cur_start, cur[-1].line_start + cur[-1].text.count("\n"))))
        cur, cur_tokens, covers = [], 0, []

    for u in units:
        if cur and cur_tokens + u.tokens > cap:
            flush()
        if not cur:
            cur_start = u.line_start
        cur.append(u)
        cur_tokens += u.tokens
    flush()
    return chunks


def _range_name(names: list[str]) -> str:
    if not names:
        return "chunk"
    if len(names) == 1:
        return f"note{int(names[0]):02d}" if names[0].isdigit() else names[0]
    # contiguous numeric run?
    nums = [int(n) for n in names if n.isdigit()]
    if len(nums) == len(names) and nums == list(range(nums[0], nums[0] + len(nums))):
        return f"notes{nums[0]}-{nums[-1]}"
    return "notes" + "-".join(str(int(n)) for n in nums)


def build_chunks(preprocessed_path, cap: int | None = None, include_statements: bool = True) -> list[Chunk]:
    from pathlib import Path
    prep = Path(preprocessed_path)
    text = prep.read_text(encoding="utf-8")
    import json
    index = json.loads(prep.with_suffix(".index.json").read_text(encoding="utf-8"))
    lines = text.splitlines()
    if cap is None:
        cfg = load_config()
        cap = int(cfg.get("extraction_input_token_cap", 16000))
        cap -= int(cfg.get("extraction_prompt_token_overhead", 1500))
        cap = max(2000, cap)
    # When the xlsx seed covers the primary statements, only note chunks go to
    # the LLM (details + context_notes) — the statement tables are transcribed
    # deterministically by xlsx_seed.py instead.
    units = (_statement_units(lines, index) if include_statements else []) + _note_units(lines, index)
    chunks = _pack(units, cap)
    return chunks


if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser(description="Inspect extraction chunks for a report")
    p.add_argument("--preprocessed", required=True, help="path to preprocessed .md")
    p.add_argument("--cap", type=int, default=None)
    a = p.parse_args()
    chunks = build_chunks(a.preprocessed, cap=a.cap)
    total = 0
    for c in chunks:
        print(f"  {c.name:<18} covers={c.covers}  ~{c.approx_tokens} tok  lines {c.line_range}")
        total += c.approx_tokens
    print(f"chunks={len(chunks)}  total~{total} tok")