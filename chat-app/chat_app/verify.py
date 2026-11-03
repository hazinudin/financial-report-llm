"""Deterministic verification of an answer envelope (CONTRACTS.md §5).

The audits are mechanical and mode-agnostic; the trace tells the verifier which
evidence exists:

1. **Numbers** — every numeric token in ``answer`` and ``claims[].text`` must be
   a *primary* ledger value (±0.5% for money/ratios, exact for counts), a
   structural number (year, quarter, page, citation id), or an operand/result of
   simple arithmetic over primary ledger values. A number that exists only in
   the **document ledger** (the analysis vault) is accepted only for an
   ``analysis_note`` claim (or a ``hypothesis`` built on note lines) whose
   citation quote contains that number verbatim. A ``fact`` may never spend a
   document-ledger value (MVP-PLAN §4.5 rules 1–7).
2. **Citations** — primary citations must match a primary tool call on tool name
   plus identifying fields (ticker/metric/driver/doc/page/row). Document
   citations must match an ``analysis_document``/``analysis_search`` call and
   resolve to a document + section + line whose text contains the quote.
3. **Causal labels** — a causal sentence in ``answer`` must be covered by a
   claim. ``management_stated`` needs doc+page; ``analysis_note`` needs
   document+section+line+quote; a ``fact`` may not cite a note as its only
   source.
4. **Missing state** — filing absence needs a ``documents_status`` call and a
   ``missing[]`` entry. Note silence needs a *scoped* ``analysis_search`` call
   and ``state: not_in_document``; note silence is never ``not_disclosed`` and
   never "does not exist". Filing states include ``available`` in addition to
   the four absence states.
5. **No advice / no ranking** — desk-issued BUY/HOLD/SELL, price targets,
   "best/top pick" language and league-table ordering are refused. A note's own
   published view is allowed only as ``analysis_note`` with document+section and
   explicit attribution; the desk must not adopt it.

``verify_envelope`` never mutates its input. :func:`repair_envelope` applies the
deterministic fixes (drop unmatched citations, downgrade/reject claims), after
which the agent re-verifies (CONTRACTS.md §5).
"""

from __future__ import annotations

import bisect
import json
import math
import re
from typing import Any, Iterable

import yaml

from . import paths
from .envelope import (
    MISSING_STATES,
    AnswerEnvelope,
    Citation,
    Claim,
    Coverage,
    Missing,
    split_sentences,
)

# Fallback markers if metrics.yaml is unreadable; the file is authoritative.
_CAUSAL_FALLBACK = [
    "because", "due to", "driven by", "reflecting", "as a result",
    "resulting from", "on the back of", "attributable to",
    "karena", "didorong", "akibat", "sebagai akibat", "seiring",
]

# Sentences that assert absence. Each needs documents_status + a missing entry,
# unless the sentence is specifically about note silence (handled separately).
_ABSENCE_RE = re.compile(
    r"\b(?:does\s+not\s+exist|doesn't\s+exist|do\s+not\s+exist|"
    r"not\s+available|isn't\s+available|is\s+not\s+available|"
    r"no\s+data|not\s+disclosed|not\s+captured|not\s+published|"
    r"not\s+yet\s+(?:published|disclosed|captured)|"
    r"does\s+not\s+(?:cover|discuss|mention|address|break\s+out|include)|"
    r"doesn't\s+(?:cover|discuss|mention|address)|"
    r"is\s+silent|not\s+(?:covered|discussed|mentioned)\s+in\s+the\s+note|"
    r"not\s+in\s+the\s+note|no\s+mention|"
    r"belum\s+tersedia|tidak\s+tersedia|tidak\s+ada|tidak\s+diungkapkan)\b",
    re.IGNORECASE,
)

# Note silence explicitly. This must map to `not_in_document`, never to a filing
# state and never to "does not exist" (MVP-PLAN §4.3/§4.5 rule 10).
_NOTE_SILENCE_RE = re.compile(
    r"(?:\b(?:the|this|pinned|analysis)\s+note\b[^.]{0,80}"
    r"\b(?:does\s+not|doesn't|is\s+silent|has\s+no|contains\s+no|makes\s+no|"
    r"provides\s+no|omits|lacks)\b"
    r"[^.]{0,60}\b(?:cover|discuss|mention|address|detail|include|break\s+out|"
    r"say|note|data|information|disclose|provide|report|state)\b|"
    r"\bnot\s+(?:covered|discussed|mentioned|addressed)\s+in\s+(?:the|this)\s+note\b|"
    r"\bnot\s+(?:disclosed|available|reported|provided)\s+in\s+(?:the|this)\s+note\b|"
    r"\bnot\s+in\s+(?:the|this)\s+note\b|"
    r"\bthe\s+note\s+is\s+silent\b|"
    r"\bthe\s+note\s+omits\b)",
    re.IGNORECASE,
)

# In document mode, "the note states that X is not disclosed" is a note
# statement (analysis_note), not a filing-absence claim. It does not need a
# documents_status call; the note citation is the evidence. This regex is only
# used after _NOTE_SILENCE_RE, so it cannot excuse "the note does not cover X".
_NOTE_STATEMENT_ABSENCE_RE = re.compile(
    r"(?i)\b(?:the|this|pinned)\s+note\b[^.]{0,120}"
    r"\b(?:states?|says?|reports?|notes?|flags?|concedes?|acknowledges?|"
    r"describes?|mentions?)\b[^.]{0,240}"
    r"\b(?:not\s+disclosed|not\s+available|not\s+reported|not\s+provided)\b"
)

_DOCS_TOOLS = ("documents_status", "list_companies")
_DOCUMENT_TOOLS = ("analysis_document", "analysis_search")

# Causal wording used to explain a failed capture is evidence about the tool
# run, not a business driver. It is still covered by the missing-state audit.
_CAPTURE_TERMS = (
    "captur", "fetch", "download", "listing", "pdf", "http", "vault",
    "index", "retriev", "url",
)

# Structural spans are blanked (same length) before numeric tokenization so the
# offsets still point at the original text for context snippets.
_MASK_PATTERNS = (
    re.compile(r"```.*?```", re.DOTALL),
    re.compile(
        r"[A-Za-z][A-Za-z0-9]*(?:_\d{4}[-_]\d{2}[-_]\d{2}[A-Za-z0-9_-]*)"
    ),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(
        r"(?i)\b\d{1,2}\s+"
        r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+\d{4}\b"
    ),
    re.compile(
        r"(?i)\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
        r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|"
        r"Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}\b"
    ),
    re.compile(r"\b\d{4,}:\d+\b"),
    re.compile(
        r"(?i)\b(?:pages?|pp?\.|hal(?:aman)?\.?)\s*\d+(?:\s*[-–—]\s*\d+)?"
    ),
    re.compile(r"(?i)\b(?:rows?|baris|sheets?)\s*\d+"),
    re.compile(r"(?i)(?:sections?|§|bagian)\s*\d+(?:\.\d+)*"),
    re.compile(r"(?i)\b(?:sheet|lembar)\s*`?\d{4,}`?"),
    re.compile(r"\[\d+\]"),
    # Markdown list/heading ordinals ("1. …", "2) …", "**1)** …",
    # "## 3.1 …", "Section 14", "6. Key Financial Ratios") are structure,
    # not quantities. Mask the ordinal only; keep the prose.
    re.compile(r"(?m)^\s*#{1,6}\s+\d{1,3}(?:\.\d+)*[.)]?\s+"),
    re.compile(r"(?m)^\s*(?:\*\*|__)?\d{1,3}[.)]\s+"),
    # A quoted section heading such as "6. Key Financial Ratios" is a
    # reference, not a quantity.
    re.compile(r"[\"“]\s*\d{1,3}[.)]\s+[A-Z][^\"”\n]{1,100}[\"”]"),
    re.compile(r"(?i)\bQ[1-4]\b"),
    re.compile(r"(?i)\b[1-9]M\s*\d{2,4}\b"),
    re.compile(r"(?i)(?<!\$)\b[1-9]M\b"),
    re.compile(r"(?i)\bFY\s?\d{4}\b"),
    re.compile(r"\b(?:19|20)\d{2}\b"),
    re.compile(r"\bT\s?[+\-]\s?\d+\b"),
)

_NUMBER_RE = re.compile(
    r"(?<![\w.,\-–—−])"
    r"(?P<sign>[-–—−])?"
    r"(?:(?:(?:US)?\$|Rp\.?|IDR)\s?)?"
    r"(?P<number>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"(?P<percent>\s?%)?"
    r"(?P<multiple>\s?[x×](?![A-Za-z]))?",
    re.IGNORECASE,
)

_UNIT_WORDS = {
    "m", "mt", "bn", "mn", "k", "rb", "thousand", "million", "billion",
    "trillion", "trn", "tn",
    "juta", "miliar", "triliun", "t", "ton", "tons", "tonne", "tonnes",
    "x", "d", "xr", "hari", "bulan", "days", "day", "months", "month",
}

_TOLERANCE = 0.005
_MAX_DOCUMENT_LEDGER_ENTRIES = 1500

# A 1-31 day followed by a month name is a date component, not a quantity.
_DATE_DAY_RE = re.compile(
    r"\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?|januari|februari|maret|april|mei|juni|"
    r"juli|agustus|september|oktober|november|desember)\b",
    re.IGNORECASE,
)

# No-advice / no-ranking vocabulary (MVP-PLAN §4.5 rule 11, A15).
_RATING_RE = re.compile(r"\b(?:BUY|SELL|HOLD)\b")
_ADVICE_RE = re.compile(
    r"(?i)\b(?:"
    r"price\s+target|target\s+price|12[-\s]?month\s+target|fair\s+value|"
    r"overweight|underweight|outperform(?:s|ed|ing)?|underperform(?:s|ed|ing)?|"
    r"accumulate|we\s+recommend|our\s+recommendation|"
    r"recommend(?:s|ed)?\s+(?:buying|selling|a\s+buy|a\s+sell|"
    r"the\s+(?:stock|shares))|recommendation\s+(?:is|to)\b|"
    r"we\s+rate|our\s+rating|"
    r"we\s+prefer|we\s+are\s+(?:buyers|sellers)|"
    r"buy\s+rating|sell\s+rating|hold\s+rating|"
    r"(?:should|must)\s+(?:buy|sell|hold)|"
    r"(?:buy|sell)\s+(?:the\s+)?(?:stock|shares|rating)|"
    r"hold\s+(?:the\s+)?(?:stock|shares)"
    r")\b"
)
_RANKING_RE = re.compile(
    r"(?i)(?:"
    r"league\s+table|"
    r"(?:top|best)\s+(?:pick|choice|idea|stock|name|performer|performing|"
    r"company|companies)|"
    r"best[-\s]in[-\s]class|"
    r"#1\s+(?:pick|choice|stock|name)|"
    r"no\.?\s*1\s+(?:pick|choice|stock|name)|"
    r"number\s+one\s+(?:pick|choice|stock|name)|"
    r"rank(?:ed|ing)?\s+(?:first|1st|top|best|last)|"
    r"investment\s+rank|"
    r"sector\s+screen|screen(?:ed|ing)?\s+(?:companies|stocks|names)|"
    r"prefer\s+[A-Z]{4}\s+(?:over|to)\s+[A-Z]{4}"
    r")"
)
_ATTRIBUTION_RE = re.compile(
    r"(?i)\b(?:the|this)\s+(?:note|report|analyst|research|analysis)\b|"
    r"\baccording\s+to\b|\banalysts?\b|\bthe\s+author\b"
)
_ADOPTION_RE = re.compile(
    r"(?i)\b(?:we\s+(?:rate|recommend|prefer|have\s+a|are\s+buyers|"
    r"are\s+sellers)|our\s+(?:rating|view|target|recommendation|call))\b"
)


# ---------------------------------------------------------------------------
# Tool trace → calls + ledgers
# ---------------------------------------------------------------------------


def _payload_from_result(result: Any) -> tuple[dict[str, Any], bool]:
    """Return ``(tool envelope, errored)`` for one pi tool result."""
    if result is None:
        return {}, False
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
        except (json.JSONDecodeError, TypeError):
            return {"text": result}, False
        if isinstance(parsed, dict):
            return parsed, bool(parsed.get("error"))
        return {}, False
    if not isinstance(result, dict):
        return {}, False

    errored = bool(result.get("isError"))
    if "ledger" in result or ("ok" in result and "tool" in result):
        if result.get("error"):
            errored = True
        return result, errored

    structured = result.get("structuredContent")
    if isinstance(structured, dict) and (
        "ledger" in structured or ("ok" in structured and "tool" in structured)
    ):
        return structured, errored or bool(structured.get("error"))

    content = result.get("content")
    chunks: list[str] = []
    if isinstance(content, str):
        chunks.append(content)
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and isinstance(block.get("text"), str):
                chunks.append(block["text"])
            elif isinstance(block.get("json"), dict):
                candidate = block["json"]
                if isinstance(candidate, dict):
                    return candidate, errored or bool(candidate.get("error"))

    for chunk in chunks:
        try:
            candidate = json.loads(chunk)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(candidate, dict):
            return candidate, errored or bool(candidate.get("error"))
        if isinstance(candidate, list):
            return {"results": candidate}, errored

    if chunks:
        text = "\n".join(chunks)
        return {"text": text}, errored
    return {}, errored


_NOTE_NUMBER_RE = re.compile(r"\d(?:[\d.,]*\d)?")


def _note_number_values(token: str) -> list[float]:
    """Numeric readings of one token in Indonesian or English formatting.

    Filings mix ``3.775.872`` (dot thousands), ``3,775,872`` (comma thousands),
    ``76,2`` (comma decimal), and ``1.234,56``. A single reading loses the
    table values, so emit every plausible scale-consistent reading and let the
    audit match the one the answer used.
    """
    text = str(token or "").strip().rstrip(".,")
    if not text:
        return []
    values: list[float] = []

    def add(value: float) -> None:
        if value != 0 and math.isfinite(value) and value not in values:
            values.append(value)

    has_comma = "," in text
    has_dot = "." in text
    if has_comma and has_dot:
        last_sep = max(text.rfind(","), text.rfind("."))
        decimals = len(text) - last_sep - 1
        if decimals in (1, 2):
            try:
                if text.rfind(".") > text.rfind(","):
                    add(float(text.replace(",", "")))  # 1,234.56
                else:
                    add(float(text.replace(".", "").replace(",", ".")))  # 1.234,56
            except ValueError:
                pass
        stripped = text.replace(",", "").replace(".", "")
        if stripped.isdigit():
            add(float(stripped))
    elif has_comma:
        parts = text.split(",")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            add(float("".join(parts)))  # 3,775,872
        try:
            add(float(text.replace(",", ".")))  # 76,2
        except ValueError:
            pass
    elif has_dot:
        parts = text.split(".")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            add(float("".join(parts)))  # 3.775.872 / 526.384
        try:
            add(float(text))  # 526.384 / 7.711
        except ValueError:
            pass
    else:
        try:
            add(float(text))
        except ValueError:
            pass
    return values


def _note_ledger(call: dict[str, Any]) -> list[dict[str, Any]]:
    """Numbers quoted from a notes/read_document result are tool evidence.

    The notes tools return prose rather than a structured ledger, so every
    numeric token in the fetched text becomes a ledger entry. This is what lets
    a cited note figure ("Rp3,452,392mn") pass the same audit as an XBRL line.
    """
    tool = call["tool"]
    if tool not in ("notes_search", "notes_fetch", "read_document"):
        return []
    if call["ledger"] and tool != "read_document":
        return []
    payload = call["payload"]
    data = call["data"]
    texts: list[tuple[str, str | None]] = []

    for item in _as_items(data.get("results")):
        if isinstance(item, dict):
            body = item.get("snippet") or item.get("text")
            if isinstance(body, str):
                texts.append((body, str(item.get("id") or item.get("path") or "")))
    body = data.get("text")
    if isinstance(body, str):
        texts.append((body, str(data.get("id") or call["args"].get("doc_id") or "page")))
    if not texts:
        fallback = payload.get("text")
        if isinstance(fallback, str):
            texts.append((fallback, "text"))

    out: list[dict[str, Any]] = []
    seen: set[float] = set()
    for body, source in texts:
        for match in _NOTE_NUMBER_RE.finditer(body):
            for value in _note_number_values(match.group(0)):
                if value == 0 or value in seen or not math.isfinite(value):
                    continue
                seen.add(value)
                out.append({
                    "value": value,
                    "unit": "note",
                    "kind": "note",
                    "source": f"notes:{source}" if source else "notes",
                    "variants": [value, value / 1e3, value / 1e6, value / 1e9],
                    "meta": {
                        "ticker": data.get("ticker") or call["args"].get("ticker"),
                        "document": data.get("path") or source,
                    },
                })
                if len(out) >= 120:
                    return out
    return out


def _is_document_entry(entry: dict[str, Any]) -> bool:
    """True for a document-ledger entry (never merged into the primary ledger)."""
    kind = str(entry.get("kind") or "").strip().lower()
    if kind == "document_number":
        return True
    source = str(entry.get("source") or "").strip()
    return source.startswith("analysis/")


def _build_calls(tool_trace: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    for raw in tool_trace or []:
        if not isinstance(raw, dict):
            continue
        name = str(
            raw.get("tool_name") or raw.get("name") or raw.get("tool") or ""
        ).strip()
        if not name:
            continue
        arguments = raw.get("arguments")
        if arguments is None:
            arguments = raw.get("args")
        if not isinstance(arguments, dict):
            arguments = {}
        payload, errored = _payload_from_result(raw.get("result"))
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        if not data and (
            "results" in payload or payload.get("path") or payload.get("text")
        ):
            data = payload
        request = payload.get("request")
        if not isinstance(request, dict):
            request = {}
        ledger = payload.get("ledger")
        raw_ledger = [e for e in ledger if isinstance(e, dict)] if isinstance(
            ledger, list
        ) else []
        call = {
            "tool": name,
            "args": arguments,
            "request": request,
            "data": data,
            "payload": payload,
            "ledger": raw_ledger,
            "errored": errored,
            "context": bool(raw.get("context")),
        }
        call["ledger"].extend(_note_ledger(call))

        # Document tools only ever contribute to the document ledger. Other
        # tools are split by the documented marker so the two ledgers can never
        # be spent interchangeably.
        if name in _DOCUMENT_TOOLS:
            call["document_ledger"] = list(call["ledger"])
            call["ledger"] = []
        else:
            call["document_ledger"] = [
                entry for entry in call["ledger"] if _is_document_entry(entry)
            ]
            call["ledger"] = [
                entry for entry in call["ledger"]
                if not _is_document_entry(entry)
            ]
        call["document_call"] = (
            name in _DOCUMENT_TOOLS or bool(call["document_ledger"])
        )
        calls.append(call)
    return calls


def build_ledger(tool_trace: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Flatten every successful **primary** tool result's ledger (CONTRACTS.md §3)."""
    ledger: list[dict[str, Any]] = []
    for call in _build_calls(tool_trace):
        if call["errored"]:
            continue
        ledger.extend(call["ledger"])
    return ledger


def build_document_ledger(
    tool_trace: Iterable[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Flatten the **document** ledger: analysis-vault numbers, kept separate."""
    calls = _build_calls(tool_trace)
    documents = _document_index(calls)
    return _document_ledger(calls, documents)


# ---------------------------------------------------------------------------
# Document index (for citation resolution + document ledger)
# ---------------------------------------------------------------------------


def _norm_id(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).casefold()


def _norm_metric(value: Any) -> str:
    text = str(value or "").strip().casefold()
    return re.sub(r"[\s./-]+", "_", text)


def _norm_text(value: Any) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"[^a-z0-9%\s.,-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_section(value: Any) -> str:
    """Section id/title normal form: ``1. Executive Summary`` → ``1 executive summary``."""
    text = str(value or "").casefold().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_quote(value: Any) -> str:
    """Normalize a quote for substring checks (case/space/markdown/quotes)."""
    text = str(value or "").casefold()
    text = (
        text.replace("\u2019", "'").replace("\u2018", "'")
        .replace("\u201c", '"').replace("\u201d", '"')
    )
    text = re.sub(r"[\u2013\u2014\u2212]", "-", text)
    text = re.sub(r"[*_`#>]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _norm_quote_loose(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _norm_quote(value))


def _as_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _line_map_from_content(content: str) -> dict[int, str]:
    if not content:
        return {}
    return {
        number: line for number, line in enumerate(content.splitlines(), 1)
    }


def _section_records(data: dict[str, Any], default_title: str) -> list[dict[str, Any]]:
    """Extract ``{title, line, level, text}`` from a document tool payload."""
    raw = data.get("sections")
    if raw is None:
        raw = data.get("outline")
    if isinstance(raw, dict):
        raw = list(raw.values())
    records: list[dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                records.append({"title": item, "line": None, "level": None, "text": None})
                continue
            if not isinstance(item, dict):
                continue
            title = (
                item.get("title") or item.get("name") or item.get("heading")
                or item.get("id")
            )
            if not title:
                continue
            records.append({
                "title": str(title),
                "line": _coerce_line(item.get("line") or item.get("start_line")),
                "level": item.get("level"),
                "text": item.get("text") or item.get("content") or item.get("body"),
            })

    single = data.get("section")
    if single:
        if isinstance(single, dict):
            title = (
                single.get("title") or single.get("name") or single.get("id")
                or default_title
            )
            line = _coerce_line(
                single.get("line") or single.get("start_line")
                or data.get("start_line")
            )
        else:
            title, line = str(single), _coerce_line(data.get("start_line"))
        body = data.get("text") or data.get("content")
        if isinstance(body, str) and not any(
            str(record.get("title")) == str(title) for record in records
        ):
            records.append({
                "title": str(title), "line": line, "level": None, "text": body,
            })

    if not records:
        body = data.get("content")
        if isinstance(body, str):
            records.append({
                "title": default_title, "line": 1, "level": None, "text": body,
            })
    return records


def _coerce_line(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        match = re.search(r"(\d+)\s*$", str(value))
        return int(match.group(1)) if match else None


def _merge_section(
    doc: dict[str, Any], title: str | None, line: int | None, text: str | None,
    level: Any = None,
) -> None:
    key = _norm_section(title or "")
    if not key:
        return
    existing = doc["section_map"].get(key)
    if existing is None:
        existing = {
            "title": title, "line": line, "level": level, "text": text or "",
        }
        doc["section_map"][key] = existing
        doc["sections"].append(existing)
    else:
        if existing.get("line") is None and line is not None:
            existing["line"] = line
        if existing.get("level") is None and level is not None:
            existing["level"] = level
        if text:
            if not existing.get("text"):
                existing["text"] = text
            elif _norm_quote(text) not in _norm_quote(existing["text"]):
                existing["text"] += "\n" + text
    if line is not None:
        doc["lines"].setdefault(line, text or "")


def _headings_from_content(
    content: str, default_title: str,
) -> list[dict[str, Any]]:
    """Parse markdown headings when a tool returned content but no outline."""
    records: list[dict[str, Any]] = []
    for number, line in enumerate((content or "").splitlines(), 1):
        match = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        title = match.group(2).strip()
        if title:
            records.append({
                "title": title,
                "line": number,
                "level": len(match.group(1)),
                "text": None,
            })
    if not records and content:
        records.append({
            "title": default_title, "line": 1, "level": None, "text": content,
        })
    return records


def _document_record(
    call: dict[str, Any], default_id: str | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Normalize one document tool call into a document record."""
    data = call["data"]
    args = call["args"]
    request = call["request"]
    meta = data.get("document") if isinstance(data.get("document"), dict) else {}
    doc_id = (
        data.get("document_id") or data.get("id") or data.get("doc_id")
        or meta.get("id")
        or args.get("document_id") or args.get("doc") or request.get("document_id")
        or default_id
    )
    if not doc_id:
        return None, []

    title = data.get("title") or meta.get("title") or data.get("name") or str(doc_id)
    path = data.get("path") or meta.get("path") or data.get("file") or ""
    aliases: set[str] = set()
    for value in (
        doc_id, path, title, data.get("slug"), meta.get("slug"),
        *(data.get("aliases") or []), *(meta.get("aliases") or []),
    ):
        if value:
            aliases.add(_norm_id(value))
            section_key = _norm_section(value)
            if section_key:
                aliases.add(section_key)

    doc: dict[str, Any] = {
        "id": str(doc_id),
        "title": str(title),
        "path": str(path or ""),
        "aliases": aliases,
        "sections": [],
        "section_map": {},
        "lines": {},
        "content": "",
    }

    content = data.get("content") or data.get("markdown")
    if not isinstance(content, str):
        content = ""
    if not content and isinstance(data.get("text"), str) and data.get("section") is None:
        content = data["text"]
    doc["content"] = content
    content_start = _coerce_line(data.get("start_line")) or 1
    lines = {
        number + content_start - 1: line
        for number, line in _line_map_from_content(content).items()
    }
    doc["lines"].update(lines)

    line_map = data.get("line_map")
    if isinstance(line_map, dict):
        for key, value in line_map.items():
            line = _coerce_line(key)
            if line is not None and isinstance(value, str):
                doc["lines"].setdefault(line, value)

    for item in _as_items(data.get("lines")):
        if isinstance(item, dict):
            line = _coerce_line(item.get("line") or item.get("number"))
            text = item.get("text")
            if line is not None and isinstance(text, str):
                doc["lines"].setdefault(line, text)
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            line = _coerce_line(item[0])
            if line is not None:
                doc["lines"].setdefault(line, str(item[1]))
        elif isinstance(item, str):
            line = len(doc["lines"]) + 1
            doc["lines"].setdefault(line, item)

    records = _section_records(data, str(title))
    if not records and content:
        records = _headings_from_content(content, str(title))
    for record in records:
        _merge_section(
            doc, record.get("title"), record.get("line"), record.get("text"),
            record.get("level"),
        )

    # Search hits: each match is a small, exact section excerpt.
    for match in _as_items(data.get("matches")):
        if not isinstance(match, dict):
            continue
        section = match.get("section") or match.get("section_title")
        line = _coerce_line(match.get("line"))
        quote = match.get("quote") or match.get("snippet") or match.get("text")
        if section or line is not None or quote:
            _merge_section(
                doc,
                section or str(title),
                line,
                str(quote) if quote else None,
            )

    # Fill section text from the full content when only an outline was returned.
    if content and doc["sections"]:
        content_lines = content.splitlines()
        ordered = sorted(
            (record for record in doc["sections"] if record.get("line")),
            key=lambda record: int(record["line"]),
        )
        for index, record in enumerate(ordered):
            if record.get("text"):
                continue
            level = record.get("level")
            end = len(content_lines)
            for later in ordered[index + 1:]:
                later_level = later.get("level")
                if level is None or later_level is None:
                    end = int(later["line"]) - 1
                    break
                if int(later_level) <= int(level):
                    end = int(later["line"]) - 1
                    break
            start = int(record["line"]) - 1
            start = max(0, min(start, len(content_lines)))
            end = max(start, min(end, len(content_lines)))
            record["text"] = "\n".join(content_lines[start:end]).strip()
            for line_no in range(start + 1, end + 1):
                if 1 <= line_no <= len(content_lines):
                    doc["lines"].setdefault(line_no, content_lines[line_no - 1])

    # A document with no headings still has one citable section.
    if not doc["sections"]:
        _merge_section(doc, str(title), 1, content or None)

    # Ledger entries can carry section/line/quote metadata even when the tool
    # payload is terse; fold them into the index so citations resolve.
    for entry in call.get("document_ledger") or []:
        meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else {}
        section = entry.get("section") or meta.get("section")
        line = _coerce_line(entry.get("line") or meta.get("line"))
        quote = entry.get("quote") or meta.get("quote")
        if section or line is not None or quote:
            _merge_section(
                doc,
                section or str(title),
                line,
                str(quote) if quote else None,
            )

    return doc, records


def _new_document_record(doc_id: str, title: str = "", path: str = "") -> dict[str, Any]:
    aliases: set[str] = set()
    for value in (doc_id, title, path):
        if value:
            aliases.add(_norm_id(value))
            key = _norm_section(value)
            if key:
                aliases.add(key)
    return {
        "id": str(doc_id),
        "title": str(title or doc_id),
        "path": str(path or ""),
        "aliases": aliases,
        "sections": [],
        "section_map": {},
        "lines": {},
        "content": "",
    }


def _search_document_records(call: dict[str, Any]) -> list[dict[str, Any]]:
    """Group one ``analysis_search`` call's matches by document."""
    data = call["data"]
    args = call["args"]
    out: dict[str, dict[str, Any]] = {}
    for match in _as_items(data.get("matches")):
        if not isinstance(match, dict):
            continue
        doc_id = (
            match.get("document") or match.get("document_id")
            or data.get("document_id") or args.get("document_id")
        )
        if not doc_id:
            continue
        key = _norm_id(doc_id)
        doc = out.get(key)
        if doc is None:
            doc = _new_document_record(
                str(doc_id), str(match.get("title") or ""),
                str(match.get("path") or ""),
            )
            out[key] = doc
        section = match.get("section") or match.get("section_title")
        line = _coerce_line(match.get("line"))
        quote = match.get("quote") or match.get("snippet") or match.get("text")
        _merge_section(
            doc, section or str(doc["title"]), line,
            str(quote) if quote else None,
        )
    return list(out.values())


def _merge_document_record(
    documents: dict[str, dict[str, Any]], doc: dict[str, Any],
) -> dict[str, Any]:
    key = _norm_id(doc["id"])
    existing = documents.get(key)
    if existing is None:
        documents[key] = doc
        return doc
    existing["aliases"] |= doc["aliases"]
    existing["lines"].update(doc["lines"])
    if not existing.get("content"):
        existing["content"] = doc["content"]
    if not existing.get("title") or existing.get("title") == existing.get("id"):
        existing["title"] = doc.get("title") or existing.get("title")
    if not existing.get("path"):
        existing["path"] = doc.get("path") or ""
    for record in doc["sections"]:
        _merge_section(
            existing, record.get("title"), record.get("line"),
            record.get("text"), record.get("level"),
        )
    return existing


def _document_index(calls: list[dict[str, Any]]) -> dict[str, Any]:
    """Build ``{aliases, documents, by_id}`` from document tool calls.

    The synthetic prefetch call recorded by the agent is an ``analysis_document``
    call with ``context: True``; it is indexed exactly like a normal call, which
    is what lets a citation resolve to the note the model actually saw.
    """
    registry: dict[str, dict[str, Any]] = {}
    documents: dict[str, dict[str, Any]] = {}

    for call in calls:
        if call["errored"]:
            continue
        if call["tool"] not in _DOCUMENT_TOOLS:
            continue
        records: list[dict[str, Any]] = []
        if call["tool"] == "analysis_search":
            records = _search_document_records(call)
        else:
            doc, _ = _document_record(call)
            if doc:
                records = [doc]
        for doc in records:
            _merge_document_record(documents, doc)

    # Re-register after merges so every alias points at the merged record.
    for doc in documents.values():
        for alias in doc["aliases"]:
            registry.setdefault(alias, doc)
        registry.setdefault(_norm_id(doc["id"]), doc)
        registry.setdefault(_norm_section(doc["title"]), doc)

    return {"registry": registry, "documents": documents}


def _resolve_document(index: dict[str, Any], value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    registry = index.get("registry") or {}
    for key in (_norm_id(value), _norm_section(value)):
        if key and key in registry:
            return registry[key]
    return None


def _find_section(doc: dict[str, Any], value: Any) -> dict[str, Any] | None:
    key = _norm_section(value)
    if not key:
        return None
    section = doc.get("section_map", {}).get(key)
    if section:
        return section
    # Allow a shorter citation ("Executive Summary") to resolve to the heading
    # ("1. Executive Summary") and vice versa.
    for candidate_key, candidate in doc.get("section_map", {}).items():
        if key in candidate_key or candidate_key in key:
            return candidate
    return None


def _section_for_line(doc: dict[str, Any], line: int) -> dict[str, Any] | None:
    ordered = sorted(
        (record for record in doc.get("sections", []) if record.get("line")),
        key=lambda record: int(record["line"]),
    )
    found = None
    for record in ordered:
        if int(record["line"]) <= line:
            found = record
        else:
            break
    return found


def _document_quote_ok(text: str | None, quote: str | None) -> bool:
    if not quote:
        return True
    if text is None:
        return False
    needle = _norm_quote(quote)
    haystack = _norm_quote(text)
    if needle and needle in haystack:
        return True
    loose_needle = _norm_quote_loose(quote)
    loose_haystack = _norm_quote_loose(text)
    return bool(loose_needle) and loose_needle in loose_haystack


def _document_citation_matches(
    citation: Citation, index: dict[str, Any]
) -> bool:
    """A document citation resolves to a document + section + line + quote."""
    if citation.tool not in _DOCUMENT_TOOLS:
        return False
    if not (citation.document or citation.doc):
        return False
    if not citation.section or citation.line is None or not citation.quote:
        return False
    doc_id = citation.document or citation.doc
    doc = _resolve_document(index, doc_id)
    if doc is None:
        return False

    section = _find_section(doc, citation.section)
    if section is None:
        return False
    if citation.line is not None:
        line = int(citation.line)
        max_line = max(doc.get("lines") or {0: ""})
        if max_line and line > max_line:
            return False
        line_section = _section_for_line(doc, line)
        if line not in doc.get("lines", {}) and section is None:
            if line_section is None:
                return False
            section = line_section
        elif section is None:
            section = line_section
        elif section.get("line") and line < int(section["line"]):
            return False

    if citation.quote:
        if section is not None and section.get("text"):
            text = section["text"]
        elif citation.line is not None and citation.line in doc.get("lines", {}):
            text = doc["lines"][int(citation.line)]
        else:
            text = doc.get("content") or "\n".join(
                str(record.get("text") or "")
                for record in doc.get("sections", [])
            )
        if not _document_quote_ok(text, citation.quote):
            return False
    return True


def _document_ledger(
    calls: list[dict[str, Any]], index: dict[str, Any]
) -> list[dict[str, Any]]:
    """Document-ledger entries: ``{value, document, section, line, quote}``."""
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, int | None, str, float]] = set()

    def add(
        value: Any,
        *,
        document: str,
        section: str | None,
        line: int | None,
        quote: str | None,
        percent: bool = False,
        unit: str = "note",
    ) -> None:
        numbers = _floats([value])
        if not numbers:
            return
        number = numbers[0]
        key = (
            _norm_id(document), line, "p" if percent else "n", round(number, 6),
        )
        if key in seen:
            return
        seen.add(key)
        variants = [number]
        if not percent:
            variants += [number / 1e3, number / 1e6, number / 1e9]
        entries.append({
            "value": number,
            "unit": "percent" if percent else unit,
            "kind": "document_number",
            "source": f"analysis/{document}",
            "document": document,
            "section": section,
            "line": line,
            "quote": quote or "",
            "percent": percent,
            "variants": variants,
            "meta": {
                "document": document,
                "section": section,
                "line": line,
                "quote": quote or "",
                "secondary": True,
            },
        })

    # 1. Explicit ledger entries returned by the document tools.
    for call in calls:
        if call["errored"]:
            continue
        for entry in call.get("document_ledger") or []:
            meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else {}
            document = (
                entry.get("document") or meta.get("document")
                or str(entry.get("source") or "").removeprefix("analysis/")
                or "unknown"
            )
            section = entry.get("section") or meta.get("section")
            line = _coerce_line(entry.get("line") or meta.get("line"))
            quote = entry.get("quote") or meta.get("quote")
            add(
                entry.get("value"),
                document=str(document),
                section=str(section) if section else None,
                line=line,
                quote=str(quote) if quote else None,
                percent=bool(entry.get("percent")),
                unit=str(entry.get("unit") or "note"),
            )

    # 2. Every numeric token in the indexed note text. Table rows and prose
    #    lines both become entries whose quote is the full line.
    for doc in (index.get("documents") or {}).values():
        document = str(doc.get("id"))
        for line_no, text in sorted(doc.get("lines", {}).items()):
            if not isinstance(text, str) or not text.strip():
                continue
            section = _section_for_line(doc, line_no)
            section_title = section.get("title") if section else doc.get("title")
            for token in _tokens(text):
                add(
                    token["value"],
                    document=document,
                    section=str(section_title) if section_title else None,
                    line=line_no,
                    quote=text,
                    percent=bool(token.get("percent")),
                )
                if len(entries) >= _MAX_DOCUMENT_LEDGER_ENTRIES:
                    return entries
    return entries


# ---------------------------------------------------------------------------
# Citation audit
# ---------------------------------------------------------------------------


def _call_fields(call: dict[str, Any]) -> dict[str, set]:
    fields: dict[str, set] = {
        "ticker": set(), "metric": set(), "driver": set(),
        "doc": set(), "page": set(), "row": set(),
    }

    def add(kind: str, value: Any) -> None:
        if value is None:
            return
        if kind in ("page", "row"):
            try:
                fields[kind].add(int(value))
            except (TypeError, ValueError):
                pass
            return
        if kind == "ticker" or kind == "doc":
            text = _norm_id(value)
        elif kind == "metric":
            text = _norm_metric(value)
        else:
            text = _norm_text(value)
        if text:
            fields[kind].add(text)

    for source in (call["args"], call["request"]):
        add("ticker", source.get("ticker"))
        add("metric", source.get("metric"))
        add("doc", source.get("doc_id"))
        add("doc", source.get("doc"))
        add("page", source.get("page"))
        add("row", source.get("row"))

    data = call["data"]
    add("ticker", data.get("ticker"))
    add("metric", data.get("metric"))
    add("metric", data.get("item"))
    add("doc", data.get("doc_id"))
    add("doc", data.get("id"))
    add("doc", data.get("path"))
    add("doc", data.get("title"))
    add("page", data.get("page"))
    for ticker in _as_items(data.get("tickers")):
        add("ticker", ticker)

    for entry in list(call["ledger"]) + list(call.get("document_ledger") or []):
        meta = entry.get("meta")
        if isinstance(meta, dict):
            add("ticker", meta.get("ticker"))
            add("metric", meta.get("metric"))
            add("doc", meta.get("source_doc"))
            add("doc", meta.get("document"))
            add("page", meta.get("page"))
            add("row", meta.get("row"))
        source = entry.get("source")
        if isinstance(source, str) and ":" in source:
            tail = source.rsplit(":", 1)[1].strip()
            if tail.isdigit():
                add("row", int(tail))

    for item in _as_items(data.get("entries")):
        if isinstance(item, dict):
            add("metric", item.get("metric"))
            add("page", item.get("page"))
            add("doc", item.get("source_doc"))
            add("doc", item.get("document"))
    for item in _as_items(data.get("drivers")):
        if isinstance(item, dict):
            add("driver", item.get("driver"))
            add("page", item.get("page"))
            add("doc", item.get("source_doc"))
    for item in _as_items(data.get("documents")):
        if isinstance(item, dict):
            add("doc", item.get("id"))
    for item in _as_items(data.get("analysis_context")):
        if isinstance(item, dict):
            add("doc", item.get("path"))
            add("doc", item.get("id"))
            add("doc", item.get("title"))
    for item in _as_items(data.get("results")):
        if isinstance(item, dict):
            for ticker in _as_items(item.get("tickers")):
                add("ticker", ticker)
            add("doc", item.get("path"))
            add("doc", item.get("id"))
            add("doc", item.get("title"))
            add("page", item.get("page"))
    for item in _as_items(data.get("rows")):
        if isinstance(item, dict):
            for cell in _as_items(item.get("cells")):
                if isinstance(cell, dict):
                    add("ticker", cell.get("ticker"))
    if call["data"].get("metric"):
        add("metric", call["data"].get("metric"))

    return fields


def _citation_matches(citation: Citation, call: dict[str, Any]) -> bool:
    """Primary citation match (document calls are excluded by the caller)."""
    if call["errored"]:
        return False
    if _norm_id(citation.tool) != _norm_id(call["tool"]):
        return False
    fields = _call_fields(call)
    hard: list[bool] = []
    matrix = _norm_id(citation.tool) == "compare_supported"

    if citation.ticker:
        hard.append(_norm_id(citation.ticker) in fields["ticker"])
    # A matrix citation identifies a cell by ticker + metric, not by a page or
    # row. Ignore page/row there; the matrix result carries the cell source.
    if citation.doc and not matrix:
        hard.append(_norm_id(citation.doc) in fields["doc"])
    if citation.id and not matrix:
        hard.append(_norm_id(citation.id) in fields["doc"])
    if citation.page is not None and not matrix:
        hard.append(int(citation.page) in fields["page"])
    if citation.row is not None and not matrix:
        hard.append(int(citation.row) in fields["row"])

    # Ticker/doc/page/row are hard evidence. Metric/driver are descriptive:
    # models paraphrase ("cost_of_revenue", "higher strip ratio and fuel costs")
    # and a paraphrase must not invalidate an otherwise exact document+page.
    return all(hard)


def _is_document_citation(citation: Citation) -> bool:
    return bool(
        citation.document or citation.section or citation.line is not None
        or citation.quote or citation.tool in _DOCUMENT_TOOLS
    )


def _claim_document_quotes(claim: Claim) -> list[str]:
    return [
        citation.quote for citation in claim.citations
        if citation.quote and _is_document_citation(citation)
    ]


def _claim_is_note_claim(claim: Claim) -> bool:
    if claim.type == "analysis_note":
        return True
    # A hypothesis may build on note lines, but only with document citations.
    return claim.type == "hypothesis" and any(
        _is_document_citation(citation) for citation in claim.citations
    )


def _kept_citations(
    claim: Claim, index: int, unmatched: list[dict[str, Any]]
) -> list[Citation]:
    dropped = {
        item["citation_index"] for item in unmatched
        if item.get("claim") == index
    }
    return [
        citation for position, citation in enumerate(claim.citations)
        if position not in dropped
    ]


def _citation_audit(
    envelope: AnswerEnvelope,
    calls: list[dict[str, Any]],
    documents: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    unmatched: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    ok_count = 0
    document_ok = 0
    primary_calls = [call for call in calls if not call.get("document_call")]

    for index, claim in enumerate(envelope.claims):
        for position, citation in enumerate(claim.citations):
            if _is_document_citation(citation):
                matched = _document_citation_matches(citation, documents)
                reason = "no matching analysis_document/analysis_search passage"
            else:
                matched = any(
                    _citation_matches(citation, call) for call in primary_calls
                )
                reason = "no matching tool call in the trace"
            if matched:
                ok_count += 1
                if _is_document_citation(citation):
                    document_ok += 1
                continue
            unmatched.append({
                "claim": index,
                "citation_index": position,
                "citation": citation.model_dump(exclude_none=True),
                "reason": reason,
            })
            violations.append({
                "type": (
                    "document_citation_unmatched"
                    if _is_document_citation(citation)
                    else "citation_unmatched"
                ),
                "token": citation.document or citation.doc or citation.metric
                or citation.driver or citation.ticker or citation.tool,
                "context": claim.text[:200],
                "claim": index,
            })

        kept = _kept_citations(claim, index, unmatched)
        if claim.type != "hypothesis" and not kept:
            violations.append({
                "type": "citation_unmatched",
                "token": None,
                "context": claim.text[:200],
                "claim": index,
                "reason": "no valid citation remains for a non-hypothesis claim",
            })

        violations.extend(_label_violations(claim, index, kept))

    result = {
        "ok": ok_count,
        "removed": len(unmatched),
        "unmatched": unmatched,
        "document_ok": document_ok,
    }
    return result, violations


def _label_violations(
    claim: Claim, index: int, kept: list[Citation]
) -> list[dict[str, Any]]:
    """Label rules for the citations that survived the citation audit."""
    violations: list[dict[str, Any]] = []
    context = claim.text[:200]

    if claim.type == "management_stated":
        has_doc_page = any(
            citation.doc and citation.page is not None for citation in kept
        )
        if not has_doc_page:
            violations.append({
                "type": "management_stated_without_document",
                "token": None,
                "context": context,
                "claim": index,
                "reason": "management_stated claims need a citation with doc+page",
            })

    if claim.type == "analysis_note":
        complete = any(
            citation.document
            and citation.section
            and citation.line is not None
            and citation.quote
            for citation in kept
        )
        if not complete:
            violations.append({
                "type": "analysis_note_without_quote",
                "token": None,
                "context": context,
                "claim": index,
                "reason": (
                    "analysis_note claims need document+section+line+quote"
                ),
            })

    if claim.type == "fact" and kept:
        if all(_is_document_citation(citation) for citation in kept):
            violations.append({
                "type": "fact_cites_note_only",
                "token": None,
                "context": context,
                "claim": index,
                "reason": (
                    "a fact may not cite an analysis note as its only source; "
                    "use analysis_note or verify against a primary tool"
                ),
            })
    return violations


# ---------------------------------------------------------------------------
# Number audit
# ---------------------------------------------------------------------------


def _mask(text: str) -> str:
    chars = list(text)
    for pattern in _MASK_PATTERNS:
        for match in pattern.finditer(text):
            for position in range(match.start(), match.end()):
                if chars[position] != "\n":
                    chars[position] = " "
    return "".join(chars)


def _parse_number(number: str) -> float:
    return float(number.replace(",", ""))


def _tokens(text: str) -> list[dict[str, Any]]:
    masked = _mask(text)
    out: list[dict[str, Any]] = []
    for match in _NUMBER_RE.finditer(masked):
        number = match.group("number")
        if not number:
            continue
        percent = bool(match.group("percent"))
        multiple = bool(match.group("multiple"))
        end = match.end()

        # "6M26", "Q2", "3rd" are identifiers/labels, not quantities. Allow a
        # unit suffix only when no digit follows it ("5.3Mt" ok, "6M26" not).
        if not percent and not multiple and end < len(text) and text[end].isalpha():
            word_match = re.match(r"[A-Za-z]+", text[end:])
            word = word_match.group(0).lower() if word_match else ""
            after = text[end + len(word):end + len(word) + 1]
            if word not in _UNIT_WORDS or after.isdigit():
                continue

        value = _parse_number(number)
        if match.group("sign"):
            value = -value

        # "30 June 2026" is a date component, not a quantity. A 1-31 day
        # immediately followed by a month name is structural.
        if (
            not percent and not multiple and 1 <= value <= 31
            and _DATE_DAY_RE.match(text[end:])
        ):
            continue

        out.append({
            "value": value,
            "percent": percent,
            "multiple": multiple,
            "decimals": len(number.split(".")[1]) if "." in number else 0,
            "start": match.start(),
            "end": match.end(),
        })
    return out


def _exact(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)


def _within(value: float, candidate: float) -> bool:
    return abs(value - candidate) <= max(_TOLERANCE * abs(candidate), 5e-5)


def _floats(values: Iterable[Any]) -> list[float]:
    out: list[float] = []
    for item in values:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            continue
        number = float(item)
        if math.isfinite(number):
            out.append(number)
    return out


def _percent_close(value: float, candidate: float, decimals: int) -> bool:
    """Percentages are displayed rounded ("roughly 4%"); honour the precision."""
    half_unit = 0.5 * (10 ** -decimals)
    return abs(value - candidate) <= half_unit + 1e-9


def _entry_matches(
    value: float, percent: bool, entry: dict[str, Any], decimals: int = 0,
) -> bool:
    kind = str(entry.get("kind") or "")
    variants = _floats(entry.get("variants") or [])
    if not variants:
        variants = _floats([entry.get("value")])
    if not variants:
        return False

    if percent:
        if kind in ("page", "year"):
            return False
        if any(
            _within(value, variant)
            or _within(value, variant * 100)
            or _percent_close(value, variant, decimals)
            or _percent_close(value, variant * 100, decimals)
            for variant in variants
        ):
            return True
        return any(_within(value / 100.0, variant) for variant in variants)

    if kind == "count":
        return any(_exact(value, variant) for variant in variants)
    if kind in ("page", "year"):
        return any(_exact(value, variant) for variant in variants)
    return any(_within(value, variant) for variant in variants)


def _ledger_values(ledger: list[dict[str, Any]]) -> list[float]:
    values: list[float] = []
    seen: set[float] = set()
    for entry in ledger:
        if str(entry.get("kind") or "") in ("page", "year"):
            continue
        for candidate in [entry.get("value"), *(entry.get("variants") or [])]:
            for number in _floats([candidate]):
                key = round(number, 6)
                if key == 0 or key in seen:
                    continue
                seen.add(key)
                values.append(number)
    return values


def _unit_scale(unit: str) -> str:
    text = (unit or "").casefold()
    if "thousand" in text or re.search(r"\bk\b", text):
        return "thousand"
    if "million" in text or re.search(r"\bmn\b", text):
        return "million"
    if "billion" in text or re.search(r"\bbn\b", text):
        return "billion"
    return "full"


def _currency(unit: str) -> str:
    text = (unit or "").casefold()
    if "usd" in text or "us$" in text or "dollar" in text:
        return "USD"
    return "IDR"


def _ledger_groups(ledger: list[dict[str, Any]]) -> dict[str, list[float]]:
    """Scale-consistent numeric groups used for arithmetic.

    Only values in the same unit family are combined. This is what stops a
    fabricated 6.9 Mt from matching an arbitrary ratio of rupiah figures while
    still allowing "9.9 − 5.3 = 4.6" and "0.7 / 4.6 ≈ 15%".
    """
    groups: dict[str, list[float]] = {}
    seen: dict[str, set[float]] = {}

    def add(tag: str, value: Any) -> None:
        numbers = _floats([value])
        if not numbers:
            return
        number = numbers[0]
        if number == 0:
            return
        bucket = seen.setdefault(tag, set())
        key = round(number, 6)
        if key in bucket:
            return
        bucket.add(key)
        groups.setdefault(tag, []).append(number)

    for entry in ledger:
        kind = str(entry.get("kind") or "")
        unit = str(entry.get("unit") or "")
        variants = entry.get("variants")
        if not isinstance(variants, list) or not variants:
            variants = [entry.get("value")]
        if kind == "money":
            ccy = _currency(unit)
            for index, value in enumerate(variants):
                if index == 1:
                    add(f"{ccy}:million", value)
                elif index == 2:
                    add("IDR:billion", value)
                else:
                    add(f"{ccy}:{_unit_scale(unit)}", value)
        elif kind == "note":
            # Note prose for IDX names quotes rupiah in millions; variants are
            # [raw, /1e3, /1e6, /1e9]. Direct values still match individually.
            for index, value in enumerate(variants[:3]):
                add(("IDR:million", "IDR:billion", "IDR:trillion")[index], value)
        elif kind == "count":
            add(f"count:{unit or 'count'}", entry.get("value"))
        elif kind == "ratio":
            add(f"ratio:{unit or 'ratio'}", entry.get("value"))
        elif kind == "document_number":
            add("document", entry.get("value"))
    # A read_document page can carry 100+ table numbers; keep enough of each
    # scale group that the cross-page differences remain auditable.
    return {tag: values[:600] for tag, values in groups.items()}


def _sum_diff(values: list[float]) -> list[float]:
    out: list[float] = []
    for index, left in enumerate(values):
        for right in values[index + 1:]:
            out.append(left + right)
            out.append(left - right)
            out.append(right - left)
    return out


def _ratio_candidates(values: list[float]) -> list[float]:
    out: list[float] = []
    for index, left in enumerate(values):
        for right in values[index + 1:]:
            if right:
                out.append(left / right)
                out.append(left / right * 100.0)
                out.append((left - right) / right * 100.0)
            if left:
                out.append(right / left)
                out.append(right / left * 100.0)
                out.append((right - left) / left * 100.0)
    return out


def _context(text: str, start: int, end: int, width: int = 45) -> str:
    snippet = text[max(0, start - width):end + width]
    return re.sub(r"\s+", " ", snippet).strip()


def _evidence_values(missing: Iterable[Any]) -> list[float]:
    """Numbers that appear in ``missing[].evidence`` (structured tool output).

    MVP-PLAN §4.3 requires the answer to carry the URL and HTTP status for
    ``not_captured``; those values are deterministic tool evidence, not prose
    the model invented, so they are accepted alongside the ledger.
    """
    values: list[float] = []

    def walk(value: Any) -> None:
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)):
            values.append(float(value))
        elif isinstance(value, str):
            for match in re.finditer(r"\d[\d,]*(?:\.\d+)?", value):
                try:
                    values.append(float(match.group(0).replace(",", "")))
                except ValueError:
                    continue
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    for item in missing:
        evidence = getattr(item, "evidence", None)
        walk(evidence)
    return values


def _structured_operand_values(calls: list[dict[str, Any]]) -> list[float]:
    """Numeric values a tool returned in structured fields, for arithmetic only.

    ``operational_lookup`` carries a ``prior`` value that is not always a ledger
    entry; a QoQ percentage derived from current/prior is still tool-sourced
    arithmetic. Only *derived* results are accepted from these operands — a
    directly displayed number still has to match the ledger.
    """
    values: list[float] = []

    def collect(value: Any) -> None:
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)):
            values.append(float(value))
        elif isinstance(value, dict):
            for item in value.values():
                collect(item)

    for call in calls:
        if call["errored"]:
            continue
        data = call["data"]
        collect(data.get("operands"))
        for key in ("quarter_value",):
            collect(data.get(key))
        for item in _as_items(data.get("entries")):
            if isinstance(item, dict):
                collect(item.get("prior"))
                collect(item.get("value"))
        for item in _as_items(data.get("series")):
            if isinstance(item, dict):
                collect(item.get("value"))
        for row in _as_items(data.get("rows")):
            if isinstance(row, dict):
                for cell in _as_items(row.get("cells")):
                    if isinstance(cell, dict):
                        collect(cell.get("value"))
    return values


class _Arithmetic:
    """Scale-consistent arithmetic index over a ledger.

    ``value_match`` allows only sums/differences (one extra level) so a bare
    quantity cannot be justified by an unrelated ratio. ``percent_match``
    allows ratios and percent changes, which is how growth rates are derived.
    """

    def __init__(self, groups: dict[str, list[float]], operands: list[float]):
        self.groups = dict(groups)
        if operands:
            self.groups["operand"] = [
                value for value in _floats(operands) if value != 0
            ][:40]
        self.sumdiff = {
            tag: _sum_diff(values) for tag, values in self.groups.items()
        }
        self.sorted_values = {
            tag: sorted(values) for tag, values in self.groups.items()
        }
        self.sorted_sumdiff = {
            tag: sorted(set(self.sumdiff[tag]))
            for tag in self.groups
        }

    @staticmethod
    def _nearest(candidates: list[float], target: float) -> float | None:
        """Closest candidate to a target, or ``None`` for an empty/invalid set."""
        if not candidates or not math.isfinite(target):
            return None
        index = bisect.bisect_left(candidates, target)
        best: float | None = None
        best_distance = math.inf
        for offset in (index - 1, index):
            if 0 <= offset < len(candidates):
                candidate = candidates[offset]
                distance = abs(target - candidate)
                if distance < best_distance:
                    best = candidate
                    best_distance = distance
        return best

    @classmethod
    def _near(
        cls, candidates: list[float], target: float, *, exact: bool = False,
    ) -> bool:
        """Bisect a sorted candidate list, with exact or tolerance matching."""
        candidate = cls._nearest(candidates, target)
        if candidate is None:
            return False
        return _exact(target, candidate) if exact else _within(target, candidate)

    def _value_near(self, tag: str, target: float) -> bool:
        return self._near(self.sorted_values.get(tag) or [], target)

    def _sumdiff_near(
        self, tag: str, target: float, *, exact: bool = False,
    ) -> bool:
        return self._near(
            self.sorted_sumdiff.get(tag) or [], target, exact=exact,
        )

    def value_match(self, value: float) -> bool:
        """Direct values, first differences, and one second-level difference.

        The second-level check is targeted (``value = s - d`` / ``s + d`` /
        ``d - s``) so a note-heavy page does not require an O(n²) table of
        second differences.
        """
        for tag, values in self.groups.items():
            if self._value_near(tag, value):
                return True
            if self._sumdiff_near(tag, value):
                return True
            for direct in values:
                # Second-level arithmetic must be exact: a 0.5% tolerance
                # here would let an unrelated number pass via a nearby sum.
                if (
                    self._sumdiff_near(tag, value + direct, exact=True)
                    or self._sumdiff_near(tag, direct - value, exact=True)
                    or self._sumdiff_near(tag, value - direct, exact=True)
                ):
                    return True
        return False

    def percent_match(self, value: float, decimals: int) -> bool:
        """Direct ratios and ratios whose numerator is a first difference."""
        ratio = value / 100.0
        direct_values = self.sorted_values
        sumdiff_values = self.sorted_sumdiff
        for tag, values in self.groups.items():
            direct_sorted = direct_values.get(tag) or []
            sumdiff_sorted = sumdiff_values.get(tag) or []
            for direct in values:
                if not direct:
                    continue
                # ratio = other / direct
                candidate = self._nearest(sumdiff_sorted, ratio * direct)
                if candidate is not None and _percent_close(
                    value, candidate / direct * 100.0, decimals,
                ):
                    return True
                candidate = self._nearest(direct_sorted, ratio * direct)
                if candidate is not None and _percent_close(
                    value, candidate / direct * 100.0, decimals,
                ):
                    return True
                # percent change = (other - direct) / direct
                candidate = self._nearest(
                    sumdiff_sorted, direct * (1.0 + ratio),
                )
                if candidate is not None and _percent_close(
                    value, (candidate / direct - 1.0) * 100.0, decimals,
                ):
                    return True
                if 1.0 + ratio:
                    candidate = self._nearest(
                        sumdiff_sorted, direct / (1.0 + ratio),
                    )
                    if candidate is not None and _percent_close(
                        value, (direct / candidate - 1.0) * 100.0, decimals,
                    ):
                        return True
        return False


def _document_entry_matches(
    value: float, percent: bool, entry: dict[str, Any], decimals: int = 0,
) -> bool:
    """Match a note value, tolerating a sign used only for direction.

    Notes write "collapsed −77.8%" while an answer may write "collapsed 77.8%";
    the magnitude is the evidence. Non-percent values use the same rule.
    """
    if _entry_matches(value, percent, entry, decimals):
        return True
    variants = _floats(entry.get("variants") or [])
    if not variants:
        variants = _floats([entry.get("value")])
    if not variants:
        return False
    magnitude = dict(entry)
    magnitude["value"] = abs(variants[0])
    magnitude["variants"] = [abs(variant) for variant in variants]
    return _entry_matches(abs(value), percent, magnitude, decimals)


def _document_number_in_quote(
    quote: str | None, value: float, percent: bool, decimals: int
) -> bool:
    if not quote:
        return False
    for token in _tokens(quote):
        if bool(token.get("percent")) != percent:
            continue
        if percent:
            if (
                _within(abs(value), abs(token["value"]))
                or _percent_close(abs(value), abs(token["value"]), decimals)
            ):
                return True
        else:
            candidate = abs(token["value"])
            if any(
                _within(abs(value), candidate / scale)
                for scale in (1.0, 1e3, 1e6, 1e9)
            ):
                return True
    return False


def _document_number_allowed(
    claim: Claim,
    value: float,
    percent: bool,
    decimals: int,
    document_ledger: list[dict[str, Any]],
    documents: dict[str, Any],
) -> bool:
    """A note number is allowed only when this claim quotes it verbatim."""
    if not _claim_is_note_claim(claim):
        return False
    quotes = [quote for quote in _claim_document_quotes(claim) if quote]
    if not quotes:
        return False
    doc_ids: set[str] = set()
    for citation in claim.citations:
        if not _is_document_citation(citation):
            continue
        resolved = _resolve_document(documents, citation.document or citation.doc)
        doc_ids.add(_norm_id(resolved["id"] if resolved else (
            citation.document or citation.doc
        )))
    for quote in quotes:
        if not _document_number_in_quote(quote, value, percent, decimals):
            continue
        normalized_quote = _norm_quote(quote)
        for entry in document_ledger:
            if not _document_entry_matches(value, percent, entry, decimals):
                continue
            entry_doc = _norm_id(
                entry.get("document")
                or (entry.get("meta") or {}).get("document")
            )
            resolved_entry = _resolve_document(documents, entry_doc)
            if resolved_entry:
                entry_doc = _norm_id(resolved_entry["id"])
            if doc_ids and entry_doc and entry_doc not in doc_ids:
                continue
            entry_quote = _norm_quote(
                entry.get("quote") or (entry.get("meta") or {}).get("quote")
            )
            if not entry_quote:
                continue
            if entry_quote in normalized_quote or normalized_quote in entry_quote:
                return True
    return False


def _answer_document_number_allowed(
    envelope: AnswerEnvelope,
    value: float,
    percent: bool,
    decimals: int,
    document_ledger: list[dict[str, Any]],
    documents: dict[str, Any],
) -> bool:
    """Answer prose may spend a note number only via a note claim that quotes it."""
    for claim in envelope.claims:
        if not _claim_is_note_claim(claim):
            continue
        if not _document_number_in_quote(claim.text, value, percent, decimals):
            continue
        if _document_number_allowed(
            claim, value, percent, decimals, document_ledger, documents
        ):
            return True
    return False


def _document_cited_entries(
    claim: Claim, document_ledger: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    quotes = [
        _norm_quote(quote) for quote in _claim_document_quotes(claim) if quote
    ]
    if not quotes:
        return []
    entries: list[dict[str, Any]] = []
    for entry in document_ledger:
        entry_quote = _norm_quote(
            entry.get("quote") or (entry.get("meta") or {}).get("quote")
        )
        if not entry_quote:
            continue
        if any(entry_quote in quote or quote in entry_quote for quote in quotes):
            entries.append(entry)
    return entries


def _document_arithmetic_allowed(
    claim: Claim,
    value: float,
    percent: bool,
    decimals: int,
    document_ledger: list[dict[str, Any]],
) -> bool:
    """Arithmetic over two *cited* note values, for analysis_note only.

    This is deliberately narrower than the primary arithmetic: only entries
    whose quote is part of this claim's citation quote participate.
    """
    if not _claim_is_note_claim(claim):
        return False
    entries = _document_cited_entries(claim, document_ledger)
    if len(entries) < 2:
        return False
    arithmetic = _Arithmetic(_ledger_groups(entries), [])
    if percent:
        return arithmetic.percent_match(value, decimals)
    return arithmetic.value_match(value)


def _answer_document_arithmetic_allowed(
    envelope: AnswerEnvelope,
    value: float,
    percent: bool,
    decimals: int,
    document_ledger: list[dict[str, Any]],
) -> bool:
    for claim in envelope.claims:
        if not _claim_is_note_claim(claim):
            continue
        if not _document_number_in_quote(claim.text, value, percent, decimals):
            continue
        if _document_arithmetic_allowed(
            claim, value, percent, decimals, document_ledger
        ):
            return True
    return False


def _audit_numbers(
    envelope: AnswerEnvelope,
    ledger: list[dict[str, Any]],
    evidence_values: list[float] | None = None,
    operand_values: list[float] | None = None,
    document_ledger: list[dict[str, Any]] | None = None,
    documents: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    violations: list[dict[str, Any]] = []
    checked = 0
    passed = 0
    document_passed = 0

    arithmetic: _Arithmetic | None = None
    evidence = evidence_values or []
    operands = operand_values or []
    document_ledger = document_ledger or []
    documents = documents or {}
    texts: list[tuple[str, str, int | None, Claim | None]] = [
        ("answer", envelope.answer, None, None),
    ]
    texts += [
        ("claim", claim.text, index, claim)
        for index, claim in enumerate(envelope.claims)
    ]

    for where, text, claim_index, claim in texts:
        for token in _tokens(text):
            value = token["value"]
            percent = token["percent"]
            decimals = token["decimals"]
            checked += 1

            if not percent and 2000 <= value <= 2100:
                passed += 1
                continue
            if any(_entry_matches(value, percent, entry, decimals) for entry in ledger):
                passed += 1
                continue
            if any(
                _within(value, candidate) if percent else _exact(value, candidate)
                for candidate in evidence
            ):
                passed += 1
                continue

            if arithmetic is None:
                arithmetic = _Arithmetic(_ledger_groups(ledger), operands)
            if percent:
                if arithmetic.percent_match(value, decimals):
                    passed += 1
                    continue
            elif token.get("multiple"):
                # "0.86x" is a ratio of two ledger values, exactly like "86%".
                if arithmetic.percent_match(value * 100.0, decimals):
                    passed += 1
                    continue
            elif arithmetic.value_match(value):
                passed += 1
                continue

            # Document-ledger numbers are a separate, stricter path.
            document_exists = any(
                _document_entry_matches(value, percent, entry, decimals)
                for entry in document_ledger
            )
            if claim is not None:
                note_allowed = _document_number_allowed(
                    claim, value, percent, decimals, document_ledger, documents
                )
            else:
                note_allowed = _answer_document_number_allowed(
                    envelope, value, percent, decimals, document_ledger, documents
                )
            if note_allowed:
                passed += 1
                document_passed += 1
                continue
            if claim is not None:
                if _document_arithmetic_allowed(
                    claim, value, percent, decimals, document_ledger
                ):
                    passed += 1
                    document_passed += 1
                    continue
            elif _answer_document_arithmetic_allowed(
                envelope, value, percent, decimals, document_ledger
            ):
                passed += 1
                document_passed += 1
                continue

            if document_exists and claim is not None and claim.type != "analysis_note":
                violation_type = "document_number_as_fact"
                reason = (
                    "this number exists only in the analysis-note ledger; a "
                    "fact/management_stated claim may not spend it. Label the "
                    "claim analysis_note with an exact quote, or verify against "
                    "a primary tool."
                )
            elif document_exists:
                note_claim_exists = any(
                    _claim_is_note_claim(other)
                    and _document_number_in_quote(
                        other.text, value, percent, decimals
                    )
                    for other in envelope.claims
                )
                violation_type = (
                    "document_number_not_in_quote" if note_claim_exists
                    else "document_number_as_fact"
                )
                reason = (
                    "the note number is not present verbatim in this claim's "
                    "citation quote."
                    if note_claim_exists else
                    "this number exists only in the analysis-note ledger; a "
                    "fact/management_stated claim may not spend it. Label the "
                    "claim analysis_note with an exact quote, or verify against "
                    "a primary tool."
                )
            else:
                violation_type = "number_not_in_ledger"
                reason = (
                    "not in any primary or document ledger value."
                )

            violation = {
                "type": violation_type,
                "token": ("-" if value < 0 else "") + f"{abs(value):g}"
                + ("%" if percent else ""),
                "context": _context(text, token["start"], token["end"]),
                "where": where,
                "reason": reason,
            }
            if claim_index is not None:
                violation["claim"] = claim_index
            violations.append(violation)

    return violations, {
        "checked": checked,
        "passed": passed,
        "document_passed": document_passed,
    }


# ---------------------------------------------------------------------------
# Causal-label audit
# ---------------------------------------------------------------------------


def _causal_markers() -> list[str]:
    path = paths.CHAT_APP_DIR / "metrics.yaml"
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return list(_CAUSAL_FALLBACK)
    block = (data or {}).get("causal_markers") or {}
    markers: list[str] = []
    if isinstance(block, dict):
        for language in ("en", "id"):
            for marker in block.get(language) or []:
                text = str(marker).strip()
                if text:
                    markers.append(text)
    return markers or list(_CAUSAL_FALLBACK)


def _sentences(text: str) -> list[str]:
    """Thin alias for :func:`chat_app.envelope.split_sentences`.

    ``sentence: N`` claim indices use the same split, so the verifier and the
    envelope schema must never compute sentence boundaries differently.
    """
    return split_sentences(text)


def _sentence_key(text: str) -> str:
    lowered = (text or "").casefold()
    lowered = re.sub(r"[^a-z0-9%$\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


_STOPWORDS = {
    "the", "and", "for", "that", "this", "with", "from", "was", "were", "are",
    "has", "have", "had", "its", "into", "than", "then", "also", "but", "not",
    "which", "while", "when", "where", "what", "says", "said", "per", "due",
}


def _content_tokens(text: str) -> set[str]:
    return {
        token for token in _sentence_key(text).split()
        if len(token) > 2 and token not in _STOPWORDS
    }


def _sentence_covered(sentence: str, claim: Claim) -> bool:
    key = _sentence_key(sentence)
    claim_key = _sentence_key(claim.text)
    if key and claim_key and (key in claim_key or claim_key in key):
        return True
    tokens = _content_tokens(sentence)
    candidate = _content_tokens(claim.text)
    return bool(tokens and candidate) and len(tokens & candidate) / len(tokens) >= 0.6


def _causal_gaps(envelope: AnswerEnvelope) -> list[str]:
    """Causal sentences in the prose that no claim covers."""
    markers = _causal_markers()
    if not markers:
        return []
    pattern = re.compile(
        "|".join(r"\b" + re.escape(marker) + r"\b" for marker in markers),
        re.IGNORECASE,
    )
    claim_keys = [
        _sentence_key(claim.text) for claim in envelope.claims if claim.text
    ]
    claim_tokens = [
        _content_tokens(claim.text) for claim in envelope.claims if claim.text
    ]
    driver_terms = [
        citation.driver.casefold()
        for claim in envelope.claims
        for citation in (claim.citations or [])
        if citation.driver and claim.type == "management_stated"
    ]
    gaps: list[str] = []
    for sentence in _sentences(envelope.answer):
        if not pattern.search(sentence):
            continue
        key = _sentence_key(sentence)
        covered = any(key and key in claim_key for claim_key in claim_keys)
        if not covered:
            tokens = _content_tokens(sentence)
            if tokens:
                covered = any(
                    len(tokens & candidate) / len(tokens) >= 0.6
                    for candidate in claim_tokens if candidate
                )
        if not covered:
            lowered = sentence.casefold()
            covered = any(term in lowered for term in driver_terms)
        if covered:
            continue
        # Capture-failure explanations are operational evidence, not a business
        # driver; the missing-state audit still covers them.
        lowered = sentence.casefold()
        if any(term in lowered for term in _CAPTURE_TERMS):
            continue
        gaps.append(sentence)
    return gaps


def _causal_audit(envelope: AnswerEnvelope) -> list[dict[str, Any]]:
    markers = _causal_markers()
    if not markers:
        return []
    pattern = re.compile(
        "|".join(r"\b" + re.escape(marker) + r"\b" for marker in markers),
        re.IGNORECASE,
    )
    violations: list[dict[str, Any]] = []
    for sentence in _causal_gaps(envelope):
        match = pattern.search(sentence)
        violations.append({
            "type": "causal_claim_unlabelled",
            "token": match.group(0) if match else None,
            "context": sentence[:240],
            "reason": "add this sentence to claims[] with a type",
        })
    return violations


# ---------------------------------------------------------------------------
# Missing-state audit
# ---------------------------------------------------------------------------


def _scoped_analysis_search(calls: list[dict[str, Any]]) -> bool:
    for call in calls:
        if call["errored"] or call["tool"] != "analysis_search":
            continue
        document = (
            call["args"].get("document_id") or call["args"].get("document")
            or call["data"].get("document_id") or call["data"].get("document")
        )
        if document and str(call["data"].get("state") or "") == "not_in_document":
            return True
    return False


def _norm_period(value: Any) -> str:
    """``"Q2 2026"``/``"q2"``/``"H1"`` → ``"q2"``/``"h1"`` (empty if unknown)."""
    text = str(value or "").strip().casefold()
    if not text:
        return ""
    match = re.search(r"q\s*([1-4])", text)
    if match:
        return f"q{match.group(1)}"
    match = re.search(r"h\s*([12])", text)
    if match:
        return f"h{match.group(1)}"
    if re.search(r"\bfy\b|full[\s_-]*year|annual", text):
        return "fy"
    return re.sub(r"\s+", "", text)


def _norm_year(value: Any) -> str:
    match = re.search(r"(?:19|20)\d{2}", str(value or ""))
    return match.group(0) if match else ""


def _call_period_year(
    args: dict[str, Any], data: dict[str, Any]
) -> tuple[str, str]:
    period = data.get("period") or args.get("period")
    year = data.get("year") or args.get("year")
    if not year:
        year = _norm_year(period)
    return _norm_period(period), _norm_year(year)


def _missing_key(item: Missing) -> tuple[str, str, str, str]:
    """Identity of a missing[] entry: ticker, period, year, item."""
    evidence = item.evidence if isinstance(item.evidence, dict) else {}
    period, year = _call_period_year({}, evidence)
    return (
        _norm_id(evidence.get("ticker")),
        period,
        year,
        _norm_metric(item.item),
    )


def _documents_status_states(
    calls: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], str]:
    """Non-context documents_status calls keyed by (ticker, period, year, item)."""
    states: dict[tuple[str, str, str, str], str] = {}
    for call in calls:
        if call["errored"] or call["tool"] != "documents_status":
            continue
        if call.get("context"):
            continue
        args, data = call["args"], call["data"]
        ticker = _norm_id(data.get("ticker") or args.get("ticker"))
        item = _norm_metric(data.get("item") or args.get("item"))
        state = str(data.get("state") or "").strip().casefold()
        if not (ticker and item and state):
            continue
        period, year = _call_period_year(args, data)
        states[(ticker, period, year, item)] = state
    return states


def _completeness_gap_states(
    calls: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], str]:
    """``company_facts.completeness.gaps`` keyed by (ticker, period, year, item).

    A complete period can still lack one document class. The gap list is the
    tool's own item-level statement about what is missing, so it is evidence
    for a ``not_captured`` missing[] entry in the same way an item-level
    ``documents_status`` call is.
    """
    states: dict[tuple[str, str, str, str], str] = {}
    for call in calls:
        if call["errored"] or call["tool"] != "company_facts":
            continue
        if call.get("context"):
            continue
        args, data = call["args"], call["data"]
        completeness = data.get("completeness")
        if not isinstance(completeness, dict):
            continue
        gaps = completeness.get("gaps")
        if not isinstance(gaps, list):
            continue
        ticker = _norm_id(data.get("ticker") or args.get("ticker"))
        if not ticker:
            continue
        period, year = _call_period_year(args, data)
        for gap in gaps:
            if not isinstance(gap, dict):
                continue
            item = _norm_metric(gap.get("item"))
            state = str(gap.get("state") or "").strip().casefold()
            if item and state:
                states[(ticker, period, year, item)] = state
    return states


def _complete_fact_periods(
    calls: list[dict[str, Any]],
) -> dict[tuple[str, str, str], str]:
    """company_facts periods whose financials/filings are complete/captured."""
    complete: dict[tuple[str, str, str], str] = {}
    for call in calls:
        if call["errored"] or call["tool"] != "company_facts":
            continue
        if call.get("context"):
            continue
        args, data = call["args"], call["data"]
        completeness = data.get("completeness")
        if not isinstance(completeness, dict):
            continue
        markers: list[str] = []
        financials = str(completeness.get("financials") or "").casefold()
        filings = str(completeness.get("filings") or "").casefold()
        if financials.strip() == "complete":
            markers.append("financials are complete")
        if filings.strip() == "captured":
            markers.append("filing is captured")
        if not markers:
            continue
        ticker = _norm_id(data.get("ticker") or args.get("ticker"))
        if not ticker:
            continue
        period, year = _call_period_year(args, data)
        complete[(ticker, period, year)] = " / ".join(markers)
    return complete


def _missing_audit(
    envelope: AnswerEnvelope,
    calls: list[dict[str, Any]],
    *,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    violations: list[dict[str, Any]] = []
    gap_states = _completeness_gap_states(calls)
    has_status_call = bool(gap_states) or any(
        call["tool"] in _DOCS_TOOLS
        and not call["errored"]
        and not call.get("context")
        for call in calls
    )
    scoped_search = _scoped_analysis_search(calls)
    not_in_document = [
        item for item in envelope.missing if item.state == "not_in_document"
    ]
    status_states = _documents_status_states(calls)
    complete_periods = _complete_fact_periods(calls)

    states_ok = True
    for item in envelope.missing:
        if item.state not in MISSING_STATES:
            states_ok = False
            violations.append({
                "type": "missing_without_documents_status",
                "token": str(item.state),
                "context": item.item[:200],
                "reason": f"state must be one of {', '.join(MISSING_STATES)}",
            })
        if item.state == "not_in_document" and not scoped_search:
            states_ok = False
            violations.append({
                "type": "not_in_document_without_search",
                "token": item.item[:120],
                "context": "missing[] claims note silence",
                "reason": (
                    "call analysis_search with document_id and add the "
                    "missing[] entry before saying the note is silent"
                ),
            })
        if item.state == "not_in_document" and mode == "desk":
            states_ok = False
            violations.append({
                "type": "not_in_document_in_desk",
                "token": item.item[:120],
                "context": "missing[] uses not_in_document in desk mode",
                "reason": (
                    "not_in_document is document-mode language; use a filing "
                    "state or available"
                ),
            })
        key = _missing_key(item)
        status_state = status_states.get(key)
        if (
            status_state == "available"
            and item.state in ("not_captured", "not_disclosed")
        ):
            states_ok = False
            violations.append({
                "type": "missing_contradicts_documents_status",
                "token": item.item[:120],
                "context": (
                    f"{key[0] or '?'} {key[1] or '?'} {key[2] or '?'}"
                ).strip(),
                "reason": (
                    f"documents_status returned available for this item; "
                    f"missing[] state {item.state} contradicts it"
                ),
            })
        complete_reason = complete_periods.get(key[:3])
        # A period with complete financials/filings can still lack a specific
        # document class (presentation, listing page) or item. If
        # documents_status answered for this exact item, its state is the
        # authority; period-level completeness must not override it.
        item_status = status_states.get(key)
        gap_status = gap_states.get(key)
        if (
            item.state == "not_captured"
            and complete_reason
            and item_status != "not_captured"
            and not (gap_status and gap_status == item.state)
        ):
            states_ok = False
            violations.append({
                "type": "false_not_captured",
                "token": item.item[:120],
                "context": (
                    f"{key[0] or '?'} {key[1] or '?'} {key[2] or '?'}"
                ).strip(),
                "reason": (
                    f"company_facts says {complete_reason}; do not mark the "
                    "period not_captured"
                ),
            })

    has_missing = bool(envelope.missing)
    sentences: list[str] = _sentences(envelope.answer)
    for claim in envelope.claims:
        sentences.extend(_sentences(claim.text))

    for sentence in sentences:
        if _NOTE_SILENCE_RE.search(sentence):
            if not scoped_search:
                states_ok = False
                violations.append({
                    "type": "not_in_document_without_search",
                    "token": None,
                    "context": sentence[:240],
                    "reason": (
                        "note silence needs a scoped analysis_search call and "
                        "missing[] state not_in_document"
                    ),
                })
            if not not_in_document:
                states_ok = False
                violations.append({
                    "type": "not_in_document_required",
                    "token": None,
                    "context": sentence[:240],
                    "reason": (
                        "use missing[] state not_in_document; note silence is "
                        "never not_disclosed and never 'does not exist'"
                    ),
                })
            if re.search(r"(?i)does\s+not\s+exist|doesn't\s+exist", sentence):
                states_ok = False
                violations.append({
                    "type": "not_in_document_required",
                    "token": "does not exist",
                    "context": sentence[:240],
                    "reason": (
                        "a silent note is not evidence that the underlying "
                        "data does not exist; say the note does not cover it"
                    ),
                })
            continue
        if mode == "document" and _NOTE_STATEMENT_ABSENCE_RE.search(sentence):
            # The note is reporting a company's non-disclosure; the note
            # citation is the evidence, not a filing-existence tool call.
            continue
        if not _ABSENCE_RE.search(sentence):
            continue
        if has_status_call and has_missing:
            continue
        violations.append({
            "type": "missing_without_documents_status",
            "token": None,
            "context": sentence[:240],
            "reason": (
                "call documents_status and add a missing[] entry"
                if not has_status_call
                else "add a missing[] entry with one of the states"
            ),
        })

    return violations, states_ok and not violations


# ---------------------------------------------------------------------------
# Coverage audit (desk comparisons must cite every name they claim to compare)
# ---------------------------------------------------------------------------


def _coverage_audit(
    envelope: AnswerEnvelope, calls: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    coverage = envelope.coverage
    if coverage is None or not coverage.compared_tickers:
        return []
    cited: set[str] = set()
    for claim in envelope.claims:
        for citation in claim.citations:
            if citation.ticker:
                cited.add(_norm_id(citation.ticker))
    for call in calls:
        if call["errored"] or call["tool"] != "compare_supported":
            continue
        for ticker in _as_items(call["args"].get("tickers")):
            cited.add(_norm_id(ticker))
        for item in _as_items(call["data"].get("columns")):
            cited.add(_norm_id(item))
    violations: list[dict[str, Any]] = []
    for ticker in coverage.compared_tickers:
        if _norm_id(ticker) in cited:
            continue
        violations.append({
            "type": "coverage_ticker_uncited",
            "token": ticker,
            "context": "coverage.compared_tickers",
            "reason": (
                f"{ticker} is in coverage.compared_tickers but no claim cites it "
                "and no compare_supported call included it"
            ),
        })
    return violations


# ---------------------------------------------------------------------------
# No-advice / no-ranking audit
# ---------------------------------------------------------------------------

_NEGATED_SCOPE_RE = re.compile(
    r"(?i)\b(?:not|never|no)\b[^.]{0,60}"
    r"\b(?:sector\s+screen|league\s+table|ranking)\b"
)


def _advice_markers(sentence: str) -> bool:
    # The required coverage caveat says "not an IDX-wide sector screen".
    # Remove that negated phrase before looking for an actual ranking claim.
    text = _NEGATED_SCOPE_RE.sub(" ", sentence)
    return bool(
        _RATING_RE.search(text)
        or _ADVICE_RE.search(text)
        or _RANKING_RE.search(text)
    )


def _attributed_note_view(claim: Claim) -> bool:
    """A quoted note rating is allowed only with attribution and no adoption."""
    if claim.type != "analysis_note":
        return False
    if not _ATTRIBUTION_RE.search(claim.text):
        return False
    if _ADOPTION_RE.search(claim.text):
        return False
    return any(
        citation.document and citation.section
        for citation in claim.citations
    )


def _advice_audit(envelope: AnswerEnvelope) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []

    def check(sentence: str, claim: Claim | None) -> None:
        if not _advice_markers(sentence):
            return
        if (
            claim is not None
            and _attributed_note_view(claim)
            and _ATTRIBUTION_RE.search(sentence)
            and not _ADOPTION_RE.search(sentence)
        ):
            return
        marker = _RATING_RE.search(sentence) or _ADVICE_RE.search(sentence) \
            or _RANKING_RE.search(sentence)
        violations.append({
            "type": "advice_or_ranking",
            "token": marker.group(0) if marker else None,
            "context": sentence[:240],
            "reason": (
                "the desk must not issue advice, a price target, or a ranking. "
                "A note's published view may be quoted only as analysis_note "
                "with document+section and explicit attribution."
            ),
        })

    for claim in envelope.claims:
        check(claim.text, claim)
    for sentence in _sentences(envelope.answer):
        covering = next(
            (claim for claim in envelope.claims if _sentence_covered(sentence, claim)),
            None,
        )
        check(sentence, covering)
    return violations


# ---------------------------------------------------------------------------
# Repair hint
# ---------------------------------------------------------------------------


def _repair_hint(
    violations: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    document_ledger: list[dict[str, Any]] | None = None,
) -> str:
    if not violations:
        return ""
    lines = [
        "Your previous answer failed the deterministic audit. Return the full "
        "YAML envelope again with every item below fixed:",
    ]
    for violation in violations[:12]:
        kind = violation.get("type")
        token = violation.get("token")
        context = str(violation.get("context") or "")[:160]
        if kind == "number_not_in_ledger":
            lines.append(
                f"- number {token!r} near \"{context}\" is not in any tool "
                "result. Remove it, call the tool again for the exact value, "
                "or derive it arithmetically from two returned values."
            )
        elif kind in ("document_number_as_fact", "document_number_not_in_quote"):
            lines.append(
                f"- number {token!r} near \"{context}\" comes from the analysis "
                "note. It may only appear in an analysis_note (or hypothesis) "
                "claim whose document citation quote contains it verbatim; a "
                "fact claim may not spend a note number."
            )
        elif kind in ("citation_unmatched", "document_citation_unmatched"):
            lines.append(
                f"- a citation on \"{context}\" does not match any tool call. "
                "Cite the exact tool, ticker, metric/doc, page/row — or for a "
                "note, document+section+line+quote from analysis_document/"
                "analysis_search."
            )
        elif kind == "management_stated_without_document":
            lines.append(
                f"- \"{context}\" is management_stated but has no document+page "
                "citation. Add the doc id and page, or label it hypothesis."
            )
        elif kind == "analysis_note_without_quote":
            lines.append(
                f"- \"{context}\" is analysis_note but lacks a document citation "
                "with document+section+line+quote."
            )
        elif kind == "fact_cites_note_only":
            lines.append(
                f"- \"{context}\" is a fact that cites only an analysis note. "
                "Verify it with a primary tool or label it analysis_note."
            )
        elif kind == "causal_claim_unlabelled":
            lines.append(
                f"- causal sentence \"{context}\" is not in claims[]. Add it "
                "as a claim with type fact, management_stated, analysis_note, "
                "or hypothesis."
            )
        elif kind in (
            "missing_without_documents_status", "not_in_document_without_search",
            "not_in_document_required",
        ):
            lines.append(
                f"- \"{context}\" asserts absence without evidence. Call "
                "documents_status (filings) or a document-scoped "
                "analysis_search (note silence) and add the matching missing[] "
                "entry. Note silence is not_in_document, never not_disclosed."
            )
        elif kind in (
            "not_in_document_in_desk",
            "missing_contradicts_documents_status",
            "false_not_captured",
        ):
            reason = str(violation.get("reason") or "").strip()
            lines.append(
                f"- missing[] entry \"{context}\" is wrong: "
                f"{reason or kind}. Fix the state (or remove the entry)."
            )
        elif kind == "advice_or_ranking":
            lines.append(
                f"- \"{context}\" issues advice or a ranking. Remove BUY/HOLD/"
                "SELL, price targets and best/top language. A note's published "
                "view may be quoted only as analysis_note with document+section "
                "and explicit attribution."
            )
        elif kind == "coverage_ticker_uncited":
            lines.append(
                f"- coverage lists {token!r} but no claim cites it. Cite every "
                "compared ticker (matrix call or claim citation) or remove it "
                "from coverage.compared_tickers."
            )
        else:
            lines.append(f"- {kind}: {context}")
    values = _ledger_values(ledger)[:12]
    if values:
        shown = ", ".join(f"{value:g}" for value in values)
        lines.append(f"Values returned by primary tools (first 12): {shown}.")
    note_values = _ledger_values(document_ledger or [])[:8]
    if note_values:
        shown = ", ".join(f"{value:g}" for value in note_values)
        lines.append(
            f"Note-ledger values (analysis_note only, first 8): {shown}."
        )
    lines.append(
        "Output only the YAML envelope with keys answer, claims[], missing[] "
        "(and coverage in desk mode)."
    )
    return "\n".join(lines)[:4000]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _dedupe_violations(
    violations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Drop byte-identical duplicates (answer and claim report the same text)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for violation in violations:
        key = json.dumps(violation, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        out.append(violation)
    return out


def verify_envelope(
    envelope: AnswerEnvelope,
    tool_trace: Iterable[dict[str, Any]] | None,
    *,
    mode: str | None = None,
) -> dict[str, Any]:
    """Audit an envelope against the tool trace. Never mutates the envelope."""
    calls = _build_calls(tool_trace)
    ledger = [
        entry for call in calls if not call["errored"] for entry in call["ledger"]
    ]
    documents = _document_index(calls)
    document_ledger = _document_ledger(calls, documents)

    numbers, number_stats = _audit_numbers(
        envelope,
        ledger,
        _evidence_values(envelope.missing),
        _structured_operand_values(calls),
        document_ledger,
        documents,
    )
    citations, citation_violations = _citation_audit(envelope, calls, documents)
    labels = _causal_audit(envelope)
    missing, missing_state_ok = _missing_audit(envelope, calls, mode=mode)
    advice = _advice_audit(envelope)
    coverage = _coverage_audit(envelope, calls)

    violations = numbers + citation_violations + labels + missing + advice + coverage
    violations = _dedupe_violations(violations)

    ok = not violations
    return {
        "ok": ok,
        "violations": violations,
        "citations": citations,
        "ledger": ledger,
        "document_ledger": document_ledger,
        "missing_state_ok": missing_state_ok,
        "labels_ok": not (labels or citation_violations),
        "advice_ok": not advice,
        "numbers": number_stats,
        "repair_hint": _repair_hint(violations, ledger, document_ledger)
        if not ok else "",
    }


def repair_envelope(
    envelope: AnswerEnvelope, verification: dict[str, Any]
) -> AnswerEnvelope:
    """Apply deterministic fixes: drop unmatched citations, downgrade claims.

    Used by the agent after a verification failure, followed by a re-verify
    (CONTRACTS.md §5). Number violations and advice/ranking are not repairable
    here: they must be fixed by the model on the retry, or the answer is
    refused.
    """
    drop: dict[int, set[int]] = {}
    for item in (verification.get("citations") or {}).get("unmatched") or []:
        claim_index = item.get("claim")
        citation_index = item.get("citation_index")
        if claim_index is None or citation_index is None:
            continue
        drop.setdefault(int(claim_index), set()).add(int(citation_index))

    claims: list[Claim] = []
    for index, claim in enumerate(envelope.claims):
        removed = drop.get(index, set())
        citations = [
            citation for position, citation in enumerate(claim.citations)
            if position not in removed
        ]
        claim_type = claim.type

        if claim_type == "analysis_note":
            complete = any(
                citation.document and citation.section
                and citation.line is not None and citation.quote
                for citation in citations
            )
            if not complete:
                # A note claim without a resolvable quoted passage is only an
                # interpretation. It never becomes a fact.
                claim_type = "hypothesis"
        elif claim_type == "management_stated":
            if not any(
                citation.doc and citation.page is not None
                for citation in citations
            ):
                claim_type = "hypothesis"
        elif claim_type == "fact":
            if citations and all(
                _is_document_citation(citation) for citation in citations
            ):
                # A fact may not be propped up by a note alone.
                citations = []
                claim_type = "hypothesis"
        if not citations and claim_type != "hypothesis":
            claim_type = "hypothesis"

        claims.append(Claim(
            text=claim.text,
            type=claim_type,  # type: ignore[arg-type]
            citations=citations,
        ))
    # A deterministic safety net for the label rule: any causal prose the model
    # left out of claims[] is labelled as a hypothesis (the weakest claim,
    # requiring no citation). This never invents a fact; it only stops an
    # unlabelled interpretation from being displayed as neutral prose.
    existing = {_sentence_key(claim.text) for claim in claims}
    for sentence in _causal_gaps(envelope)[:3]:
        key = _sentence_key(sentence)
        if key and key not in existing:
            claims.append(Claim(text=sentence.strip(), type="hypothesis", citations=[]))
            existing.add(key)
    return AnswerEnvelope(
        answer=envelope.answer,
        claims=claims,
        missing=list(envelope.missing),
        coverage=_copy_coverage(envelope.coverage),
    )


def _copy_coverage(coverage: Coverage | None) -> Coverage | None:
    if coverage is None:
        return None
    return coverage.model_copy(deep=True)
