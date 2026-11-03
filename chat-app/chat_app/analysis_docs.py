"""Read-only index of the ``analysis/**/*.md`` vault (MVP-PLAN §4.6).

The vault is the product, so this module never writes to it. It builds a
deterministic in-memory index keyed by relative path + mtime + sha256:

- document IDs come from the path slug (``ITMG Q2 2026 Analysis.md`` →
  ``itmg-q2-2026-analysis``); a short path hash is appended only on collision.
- section IDs come from heading text; heading line numbers are 1-based and
  include YAML frontmatter, so citations survive a reader scroll.
- every numeric token becomes a *document ledger* entry with the line as its
  quote. Table rows are just lines that begin/end with ``|``, so a row quote is
  the whole row.

The reader gets the full file; the model-facing tools apply their own byte caps
(``tools.analysis_document`` / ``tools.analysis_search``). Frontmatter is parsed
into metadata and kept in the document so the reader can strip it; ``content``
is byte-for-byte the file on disk.
"""

from __future__ import annotations

import bisect
import datetime as _dt
import hashlib
import re
import threading
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

from . import paths

# Per-tool document caps (MVP-PLAN §4.6 / WS4). The runner's full-note prefetch
# is a separate 64 KB read and never goes through these functions.
DOCUMENT_TOOL_CAP_BYTES = 24_000
DOCUMENT_PREFETCH_BYTES = 64_000
SECTION_CHUNK_CHARS = 12_000
MAX_QUOTE_CHARS = 500
MAX_LEDGER_QUOTE_CHARS = 240
MAX_SNIPPET_CHARS = 320

KINDS = ("company", "sector", "commodity", "article", "other")

# Paths (lower-case relative directory names) map directly to a kind.
_PATH_KINDS = {
    "companies": "company",
    "commodities": "commodity",
    "articles": "article",
    "geopolitics": "article",
    "forecast": "company",
}

_COMMODITY_WORDS = (
    "coal", "coking coal", "cpo", "palm oil", "crude palm", "gold", "aluminium",
    "aluminum", "ammonia", "tin", "nickel", "copper", "methanol", "oil", "gas",
    "silver", "commodity", "commodities",
)
_SECTOR_WORDS = (
    "companies", "company list", "market", "industry", "sector", "plantation",
    "policy", "geopolitics", "outlook",
)

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_FENCE_RE = re.compile(r"^\s*(```|~~~)")
_TICKER_AT_START_RE = re.compile(r"^\$?([A-Z]{3,5})(?=$|[\s_\-:])")
_PERIOD_RE = re.compile(r"\bQ([1-4])\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")

# Currency prefix + number + unit suffix. The lookbehind keeps ticker/H1/Q2
# digits ("H1", "Q2", "1Q26") out of the ledger.
_NUMBER_RE = re.compile(
    r"(?<![\w$])"
    r"(?P<sign>[-\u2212])?\s*"
    r"(?P<prefix>US\$|US\s?\$|USD|Rp\.?|IDR|\$|€|£)?\s*"
    r"(?P<num>\d[\d,]*(?:\.\d+)?)"
    r"(?P<suffix>"
    r"\s*(?:%|bps|bn|billion|trillion|tn|mn|million|m|k|thousand|x|times|"
    r"Mt|mt|tonnes?|tons?|oz|kg|ha|kbbl|bbl|t)(?:\s*/\s*[A-Za-z]+)?"
    r"|/\s*[A-Za-z]+"
    r")?"
    r"(?![\w])",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4})\b")

_SCALE = {
    "k": ("thousand", 1e3),
    "thousand": ("thousand", 1e3),
    "m": ("million", 1e6),
    "mn": ("million", 1e6),
    "million": ("million", 1e6),
    "bn": ("billion", 1e9),
    "billion": ("billion", 1e9),
    "tn": ("trillion", 1e12),
    "trillion": ("trillion", 1e12),
}
_COUNT_UNITS = {
    "mt": "Mt", "t": "t", "ton": "t", "tons": "t", "tonne": "t",
    "tonnes": "t", "oz": "oz", "kg": "kg", "ha": "ha", "bbl": "bbl",
    "kbbl": "kbbl",
}
_PERCENT_UNITS = {"%"}
_RATIO_UNITS = {"x", "times", "bps"}

_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "if", "then", "than", "so", "such",
    "is", "are", "was", "were", "be", "been", "being", "am", "do", "does",
    "did", "has", "have", "had", "what", "which", "who", "whom", "whose",
    "how", "why", "when", "where", "about", "say", "says", "said", "discuss",
    "discusses", "cover", "covers", "note", "notes", "this", "that", "these",
    "those", "of", "on", "in", "for", "to", "from", "by", "as", "at", "it",
    "its", "there", "their", "we", "i", "you", "they", "he", "she", "will",
    "would", "can", "could", "should", "may", "might", "not", "no", "any",
    "with", "per", "vs", "into", "over", "under", "between", "during",
}

_CACHE: dict[str, dict[str, dict[str, Any]]] = {}
_CACHE_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _slug(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _stem(rel_path: str) -> str:
    name = rel_path.rsplit("/", 1)[-1]
    return name[:-3] if name.lower().endswith(".md") else name


def _path_slug(rel_path: str) -> str:
    stem = rel_path[:-3] if rel_path.lower().endswith(".md") else rel_path
    if stem.startswith("analysis/"):
        stem = stem[len("analysis/"):]
    parts = [part for part in stem.split("/") if part]
    slug = "-".join(filter(None, (_slug(part) for part in parts)))
    return slug or "document"


def _short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:6]


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, (dict, list)):
                continue
            out.append(str(item))
        return out
    return [str(value)]


def _clean_tag(value: str) -> str:
    return str(value).strip().lstrip("#").strip()


def _json_safe(value: Any) -> Any:
    """Frontmatter may contain YAML dates; the API must stay JSON-safe."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _frontmatter(text: str) -> tuple[dict[str, Any], int]:
    """Return ``(mapping, number_of_frontmatter_lines)``.

    A missing or unterminated block is not frontmatter. Invalid YAML is kept as
    an empty mapping so one malformed note cannot take down the index.
    """
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, 0
    for index in range(1, len(lines)):
        if lines[index].strip() in ("---", "..."):
            raw = "\n".join(lines[1:index])
            try:
                data = yaml.safe_load(raw) or {}
            except yaml.YAMLError:
                data = {}
            if not isinstance(data, dict):
                data = {}
            return data, index + 1
    return {}, 0


def _headings(text: str, body_start: int) -> list[dict[str, Any]]:
    """Headings outside frontmatter and fenced code, in document order."""
    lines = text.splitlines()
    found: list[dict[str, Any]] = []
    in_fence = False
    for number, line in enumerate(lines, start=1):
        if number <= body_start:
            continue
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = _HEADING_RE.match(line)
        if not match:
            continue
        level = len(match.group(1))
        title = match.group(2).strip()
        if not title:
            continue
        found.append({"title": title, "level": level, "line": number})
    return found


def _sections(text: str, body_start: int) -> list[dict[str, Any]]:
    headings = _headings(text, body_start)
    total_lines = len(text.splitlines())
    counts: dict[str, int] = {}
    for heading in headings:
        base = _slug(heading["title"]) or "section"
        count = counts.get(base, 0) + 1
        counts[base] = count
        heading["id"] = base if count == 1 else f"{base}-{count}"
    for index, heading in enumerate(headings):
        end = total_lines
        for later in headings[index + 1:]:
            if later["level"] <= heading["level"]:
                end = later["line"] - 1
                break
        heading["end_line"] = end
    return [
        {
            "id": heading["id"],
            "title": heading["title"],
            "level": heading["level"],
            "line": heading["line"],
            "end_line": heading["end_line"],
        }
        for heading in headings
    ]


def _section_at(sections: list[dict[str, Any]], line: int) -> Optional[dict[str, Any]]:
    if not sections:
        return None
    starts = [section["line"] for section in sections]
    index = bisect.bisect_right(starts, line) - 1
    if index < 0:
        return None
    section = sections[index]
    return section if line <= section["end_line"] else None


def _window(line: str, start: int, end: int, max_chars: int) -> str:
    """An exact substring of ``line`` around ``start``–``end``, word-aligned."""
    text = line.strip()
    if len(text) <= max_chars:
        return text
    lead = len(line) - len(line.lstrip())
    start = min(max(0, start - lead), len(text))
    end = min(max(start, end - lead), len(text))
    span = end - start
    if span >= max_chars:
        left, right = start, start + max_chars
    else:
        context = max_chars - span
        left = max(0, start - context // 2)
        right = min(len(text), left + max_chars)
        if right == len(text):
            left = max(0, right - max_chars)
    if left > 0:
        space = text.find(" ", left)
        if 0 <= space < start:
            left = space + 1
    if right < len(text):
        space = text.rfind(" ", left, right)
        if space > end:
            right = space
    return text[left:right].strip()


def _number_parts(match: re.Match[str]) -> tuple[float, str, str, float]:
    """Return ``(raw_value, numeric_kind, unit, scale_multiplier)``."""
    raw = float(match.group("num").replace(",", ""))
    if match.group("sign") in ("-", "\u2212"):
        raw = -raw
    prefix = (match.group("prefix") or "").strip().lower()
    raw_suffix = (match.group("suffix") or "").strip()
    per: Optional[str] = None
    if "/" in raw_suffix:
        suffix, per = [part.strip() for part in raw_suffix.split("/", 1)]
        per = per.lower() or None
        if per in ("us", "us$", "usd", "dollar"):
            per = "USD"
        elif per in ("rp", "rp.", "idr"):
            per = "IDR"
    else:
        suffix = raw_suffix
    scale_l = suffix.lower()
    currency = None
    if prefix in ("us$", "us $", "usd", "$"):
        currency = "USD"
    elif prefix in ("rp", "rp.", "idr"):
        currency = "IDR"

    if scale_l in _PERCENT_UNITS:
        return raw, "percent", "%", 1.0
    if scale_l in _RATIO_UNITS:
        return raw, "ratio", "x" if scale_l in ("x", "times") else "bps", 1.0
    if scale_l in _SCALE:
        scale_name, multiplier = _SCALE[scale_l]
        unit = scale_name if not currency else f"{currency} {scale_name}"
    elif scale_l in _COUNT_UNITS:
        unit = _COUNT_UNITS[scale_l]
        multiplier = 1.0
        if per:
            unit = f"{unit}/{per}"
        return raw, "count", unit, 1.0
    elif scale_l:
        unit = suffix
        multiplier = 1.0
    else:
        unit = ""
        multiplier = 1.0

    if per:
        if unit:
            unit = f"{unit}/{per}"
        elif currency:
            unit = f"{currency}/{per}"
        else:
            unit = f"/{per}"
    if currency:
        return raw, "money", unit or currency, multiplier
    if multiplier != 1.0:
        return raw, "number", unit, multiplier
    return raw, "number", unit, 1.0


def _variants(raw: float, multiplier: float, numeric_kind: str) -> list[float]:
    full = raw * multiplier
    candidates = [
        raw, raw / 1e3, raw / 1e6, raw / 1e9,
        full, full / 1e3, full / 1e6, full / 1e9,
    ]
    if numeric_kind == "percent":
        candidates.extend([raw / 100.0, full / 100.0])
    out: list[float] = []
    seen: set[float] = set()
    for value in candidates:
        if value == 0:
            continue
        rounded = round(value, 6)
        if rounded == 0 or rounded in seen:
            continue
        seen.add(rounded)
        out.append(rounded)
    return out


def _line_ledger(
    document_id: str,
    rel_path: str,
    line: str,
    line_number: int,
    section: Optional[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not _NUMBER_RE.search(line):
        return entries
    dates = [match.span() for match in _DATE_RE.finditer(line)]
    is_table = line.strip().startswith("|") and line.strip().endswith("|")
    for match in _NUMBER_RE.finditer(line):
        start, end = match.span()
        if any(date_start <= start and end <= date_end for date_start, date_end in dates):
            continue
        raw, numeric_kind, unit, multiplier = _number_parts(match)
        if raw == 0:
            continue
        # A bare 1–3 digit integer is almost always a list marker, date day or
        # section number; plain years are structural and already whitelisted.
        num_text = match.group("num")
        if (
            not is_table
            and "." not in num_text
            and "," not in num_text
            and abs(raw) < 1000
            and numeric_kind == "number"
        ):
            continue
        if (
            not is_table
            and 2000 <= raw <= 2100
            and "." not in num_text
            and "," not in num_text
        ):
            continue
        quote = line.strip() if is_table else _window(
            line, start, end, MAX_LEDGER_QUOTE_CHARS
        )
        if is_table and len(quote) > 1_000:
            quote = quote[:1_000]
        if not quote:
            continue
        entries.append({
            "value": round(raw, 6),
            "unit": unit or None,
            "kind": "document_number",
            "source": f"{rel_path}:{line_number}",
            "variants": _variants(raw, multiplier, numeric_kind),
            "meta": {
                "document": document_id,
                "section": section["title"] if section else None,
                "section_id": section["id"] if section else None,
                "line": line_number,
                "quote": quote,
                "numeric_kind": numeric_kind,
                **({"table_row": True} if is_table else {}),
            },
        })
    return entries


def _ledger(
    document_id: str,
    rel_path: str,
    text: str,
    body_start: int,
    sections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if number <= body_start:
            continue
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        entries.extend(_line_ledger(
            document_id, rel_path, line, number, _section_at(sections, number)
        ))
    return entries


# ---------------------------------------------------------------------------
# Metadata inference
# ---------------------------------------------------------------------------


def _infer_ticker(rel_path: str, front: dict[str, Any]) -> Optional[str]:
    explicit = front.get("ticker")
    if isinstance(explicit, str):
        value = explicit.strip().lstrip("$").upper()
        if re.fullmatch(r"[A-Z]{3,5}", value):
            return value
    parts = rel_path.split("/")
    if "companies" in parts[:-1]:
        name = _stem(rel_path).lstrip("$").strip()
        if re.fullmatch(r"[A-Z]{3,5}", name):
            return name
    # A commodity/article name like "CPO.md" is not an issuer code.
    if any(part in ("commodities", "geopolitics", "sectors") for part in parts[:-1]):
        return None
    stem = _stem(rel_path).replace("_", " ")
    match = _TICKER_AT_START_RE.match(stem)
    if match:
        return match.group(1)
    return None


def _infer_period(rel_path: str, front: dict[str, Any]) -> Optional[str]:
    for key in ("period", "quarter"):
        value = front.get(key)
        if isinstance(value, str):
            match = re.search(r"Q([1-4])", value.upper())
            if match:
                return f"Q{match.group(1)}"
    stem = _stem(rel_path).replace("_", " ").replace("-", " ")
    match = _PERIOD_RE.search(stem)
    return f"Q{match.group(1)}" if match else None


def _infer_year(rel_path: str, front: dict[str, Any]) -> Optional[int]:
    value = front.get("year")
    if isinstance(value, (int, str)):
        match = re.search(r"(20\d{2})", str(value))
        if match:
            return int(match.group(1))
    stem = _stem(rel_path).replace("_", " ").replace("-", " ")
    near = re.search(r"Q[1-4][\s]*(20\d{2})", stem, re.IGNORECASE)
    if near:
        return int(near.group(1))
    years = [int(match.group(1)) for match in _YEAR_RE.finditer(stem)]
    return max(years) if years else None


def _infer_kind(rel_path: str, ticker: Optional[str], period: Optional[str]) -> str:
    parts = [part.lower() for part in rel_path.split("/")[:-1]]
    for part in parts:
        kind = _PATH_KINDS.get(part)
        if kind:
            if kind == "company" and not ticker:
                continue
            return kind
    stem = _stem(rel_path).replace("_", " ").lower()
    titleish = stem
    if ticker:
        if period or re.search(r"\b(analysis|forecast|strategy|seasonality)\b", titleish):
            return "company"
        if titleish.startswith(ticker.lower()):
            return "company"
    if "compan" in titleish:
        return "sector"
    if any(word in titleish for word in _COMMODITY_WORDS):
        return "commodity"
    if any(word in titleish for word in _SECTOR_WORDS):
        return "sector"
    return "other"


def _title(front: dict[str, Any], text: str, body_start: int, rel_path: str) -> str:
    value = front.get("title")
    if isinstance(value, str) and value.strip():
        return value.strip()
    for heading in _headings(text, body_start):
        if heading["level"] == 1:
            return heading["title"]
    return _stem(rel_path).replace("_", " ").strip()


def _parse(path: Path, rel_path: str) -> dict[str, Any]:
    stat = path.stat()
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return {
            "path": rel_path,
            "title": _stem(rel_path).replace("_", " "),
            "kind": "other",
            "ticker": None,
            "period": None,
            "year": None,
            "updated_at": _utc(stat.st_mtime),
            "sha256": None,
            "bytes": stat.st_size,
            "words": 0,
            "tags": [],
            "aliases": [],
            "sections": [],
            "content": "",
            "frontmatter": {},
            "body_start": 0,
            "line_count": 0,
            "section_count": 0,
            "unreadable": str(exc) or type(exc).__name__,
            "ledger": [],
            "mtime": stat.st_mtime,
            "mtime_ns": stat.st_mtime_ns,
        }

    front, body_start = _frontmatter(text)
    front = _json_safe(front)
    sections = _sections(text, body_start)
    ticker = _infer_ticker(rel_path, front)
    period = _infer_period(rel_path, front)
    year = _infer_year(rel_path, front)
    kind = _infer_kind(rel_path, ticker, period)
    explicit_kind = str(front.get("kind") or "").strip().lower()
    if explicit_kind in KINDS:
        kind = explicit_kind
    body = "\n".join(text.splitlines()[body_start:])
    document = {
        "path": rel_path,
        "title": _title(front, text, body_start, rel_path),
        "kind": kind,
        "ticker": ticker,
        "period": period,
        "year": year,
        "updated_at": _utc(stat.st_mtime),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "words": len(re.findall(r"[\w'-]+", body)),
        "tags": list(dict.fromkeys(filter(None, (_clean_tag(tag) for tag in _as_list(front.get("tags")))))),
        "aliases": list(dict.fromkeys(filter(None, [
            *_as_list(front.get("aliases") or front.get("alias")),
            _stem(rel_path).replace("_", " "),
        ]))),
        "sections": sections,
        "content": text,
        "frontmatter": front,
        "body_start": body_start,
        "line_count": len(text.splitlines()),
        "section_count": len(sections),
        "ledger": [],
        "mtime": stat.st_mtime,
        "mtime_ns": stat.st_mtime_ns,
    }
    document["ledger"] = _ledger(
        document.get("id") or "", rel_path, text, body_start, sections
    )
    return document


def _utc(timestamp: float) -> str:
    value = _dt.datetime.fromtimestamp(timestamp, tz=_dt.timezone.utc)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _index() -> dict[str, dict[str, Any]]:
    root = Path(paths.ANALYSIS_DIR).resolve()
    key = str(root)
    with _CACHE_LOCK:
        cache = _CACHE.setdefault(key, {})
        if not root.exists():
            cache.clear()
            return cache
        files = sorted(
            path for path in root.rglob("*.md")
            if not any(part.startswith(".") for part in path.relative_to(root).parts)
        )
        rels: list[str] = []
        for path in files:
            rel = _relative(path, root)
            rels.append(rel)
        bases = {rel: _path_slug(rel) for rel in rels}
        counts: dict[str, int] = {}
        for base in bases.values():
            counts[base] = counts.get(base, 0) + 1
        fresh: dict[str, dict[str, Any]] = {}
        for path, rel in zip(files, rels):
            base = bases[rel]
            document_id = base if counts[base] == 1 else f"{base}-{_short_hash(rel)}"
            cached = cache.get(rel)
            if (
                cached
                and cached.get("mtime_ns") == path.stat().st_mtime_ns
                and cached.get("bytes") == path.stat().st_size
            ):
                document = cached
            else:
                document = _parse(path, rel)
            document["id"] = document_id
            for entry in document.get("ledger") or []:
                entry["meta"]["document"] = document_id
            fresh[rel] = document
        _CACHE[key] = fresh
        return fresh


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(paths.PROJECT_ROOT).as_posix()
    except ValueError:
        return f"analysis/{path.relative_to(root).as_posix()}"


def clear_cache() -> None:
    """Drop the in-memory index (tests and manual re-indexing)."""
    with _CACHE_LOCK:
        _CACHE.clear()


# ---------------------------------------------------------------------------
# Public index API
# ---------------------------------------------------------------------------


def _meta(document: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id", "path", "title", "kind", "ticker", "period", "year",
        "updated_at", "sha256", "bytes", "words", "tags", "aliases",
        "section_count",
    )
    meta = {key: document.get(key) for key in keys}
    if document.get("unreadable"):
        meta["unreadable"] = document.get("unreadable")
    return meta


def metadata(document: dict[str, Any]) -> dict[str, Any]:
    """Public metadata-only view of an indexed document (no content/ledger)."""
    return _meta(document)


def _ordered(documents: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    kind_order = {kind: index for index, kind in enumerate(KINDS)}
    return sorted(
        documents,
        key=lambda doc: (
            kind_order.get(str(doc.get("kind")), 99),
            str(doc.get("ticker") or "~"),
            -(doc.get("year") or 0),
            -paths.PERIOD_ORDER.get(str(doc.get("period") or ""), 0),
            str(doc.get("title") or "").lower(),
            str(doc.get("id") or ""),
        ),
    )


def _matches_ticker(document: dict[str, Any], ticker: str) -> bool:
    wanted = str(ticker).strip().upper()
    if str(document.get("ticker") or "").upper() == wanted:
        return True
    return any(str(tag).strip().upper() == wanted for tag in document.get("tags") or [])


def list_documents(
    ticker: str | None = None,
    period: str | None = None,
    year: int | None = None,
    kind: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Metadata for notes matching the filters, newest/most specific first."""
    try:
        count = int(limit)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"limit must be an integer, got {limit!r}") from exc
    if count < 1:
        raise paths.ToolInputError(f"limit must be >= 1, got {count}")
    count = min(count, 500)

    wanted_period = None
    if period:
        wanted_period = paths.normalize_quarter(period)
    wanted_year = paths.normalize_year(year) if year is not None else None
    wanted_kind = str(kind).strip().lower() if kind else None
    if wanted_kind and wanted_kind not in KINDS:
        raise paths.ToolInputError(
            f"kind must be one of {', '.join(KINDS)}, got {kind!r}"
        )
    wanted_ticker = str(ticker).strip().upper() if ticker else None

    rows = []
    for document in _ordered(_index().values()):
        if wanted_ticker and not _matches_ticker(document, wanted_ticker):
            continue
        if wanted_period and str(document.get("period") or "").upper() != wanted_period:
            continue
        if wanted_year is not None and document.get("year") != wanted_year:
            continue
        if wanted_kind and document.get("kind") != wanted_kind:
            continue
        rows.append(_meta(document))
        if len(rows) >= count:
            break
    return rows


def _find(document_id: str) -> dict[str, Any]:
    wanted = str(document_id or "").strip()
    if not wanted:
        raise paths.ToolInputError("document_id must not be empty")
    index = _index()
    for document in index.values():
        if str(document.get("id")) == wanted:
            return document
    lowered = wanted.lower()
    for document in index.values():
        if str(document.get("id")).lower() == lowered:
            return document
    raise paths.ArtifactMissing(
        f"analysis document {document_id!r} was not found",
        document_id=document_id,
        document_count=len(index),
    )


def get_document(document_id: str) -> dict[str, Any]:
    """Full indexed document: metadata + headings + content + document ledger."""
    document = _find(document_id)
    if document.get("unreadable"):
        raise paths.ChatError(
            f"analysis document {document_id!r} is unreadable",
            document_id=document_id,
            detail=document.get("unreadable"),
        )
    return document


def document_content(document_id: str) -> str:
    """The note's markdown, byte-for-byte (frontmatter included)."""
    return str(get_document(document_id).get("content") or "")


def _resolve_section(
    document: dict[str, Any], section: str
) -> dict[str, Any]:
    key = str(section or "").strip().lstrip("#").strip()
    if not key:
        raise paths.ToolInputError("section must not be empty")
    sections = document.get("sections") or []
    lowered = key.lower()
    slug = _slug(key)
    for candidate in sections:
        if str(candidate["id"]) == lowered or str(candidate["id"]) == key:
            return candidate
    for candidate in sections:
        if str(candidate["title"]).strip().lower() == lowered:
            return candidate
    for candidate in sections:
        title = str(candidate["title"]).strip().lower()
        if title.startswith(lowered + " ") or title.startswith(lowered + ".") \
                or title == lowered:
            return candidate
    if slug:
        for candidate in sections:
            candidate_id = str(candidate["id"])
            if candidate_id == slug or candidate_id.endswith(f"-{slug}"):
                return candidate
    if key.isdigit():
        line = int(key)
        for candidate in sections:
            if int(candidate["line"]) == line:
                return candidate
    available = [
        {"id": item["id"], "title": item["title"], "line": item["line"]}
        for item in sections[:40]
    ]
    raise paths.ToolInputError(
        f"section {section!r} was not found in {document.get('id')}",
        document=document.get("id"),
        available_sections=available,
    )


def document_section(
    document_id: str,
    section: str,
    next_line: int | None = None,
    *,
    max_chars: int = SECTION_CHUNK_CHARS,
) -> dict[str, Any]:
    """One section (or a line-bounded chunk of it) with a ``next_line`` cursor."""
    document = get_document(document_id)
    target = _resolve_section(document, section)
    start = int(target["line"])
    end = int(target["end_line"])
    if next_line is not None:
        try:
            start = int(next_line)
        except (TypeError, ValueError) as exc:
            raise paths.ToolInputError(
                f"next_line must be an integer, got {next_line!r}"
            ) from exc
        if start < int(target["line"]) or start > end + 1:
            raise paths.ToolInputError(
                f"next_line {start} is outside section lines "
                f"{target['line']}-{end}",
                section=target["id"],
            )
    lines = str(document.get("content") or "").splitlines()
    limit = max(200, int(max_chars))
    cursor = start
    length = 0
    selected: list[str] = []
    while cursor <= end and cursor <= len(lines):
        line = lines[cursor - 1]
        addition = len(line) + 1
        if selected and length + addition > limit:
            break
        selected.append(line)
        length += addition
        cursor += 1
    next_cursor = cursor if cursor <= end else None
    return {
        "document": document.get("id"),
        "path": document.get("path"),
        "title": document.get("title"),
        "section": target,
        "start_line": start,
        "end_line": min(cursor - 1, end),
        "next_line": next_cursor,
        "truncated": next_cursor is not None,
        "content": "\n".join(selected),
    }


def document_ledger(
    document_id: str,
    *,
    section: str | None = None,
    start_line: int | None = None,
    end_line: int | None = None,
    lines: Iterable[int] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Document-ledger entries, optionally narrowed to a section or line range."""
    document = get_document(document_id)
    entries = list(document.get("ledger") or [])
    if section:
        target = _resolve_section(document, section)
        start_line = int(target["line"])
        end_line = int(target["end_line"])
    wanted_lines = {int(line) for line in lines} if lines is not None else None
    out: list[dict[str, Any]] = []
    for entry in entries:
        line = int((entry.get("meta") or {}).get("line") or 0)
        if start_line is not None and line < start_line:
            continue
        if end_line is not None and line > end_line:
            continue
        if wanted_lines is not None and line not in wanted_lines:
            continue
        out.append(entry)
        if limit is not None and len(out) >= limit:
            break
    return out


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def query_terms(query: str) -> list[str]:
    """Content terms used for ranking and for ``not_in_document`` evidence."""
    raw = re.findall(r"[A-Za-z0-9][A-Za-z0-9'._-]*", str(query or "").lower())
    terms = [term for term in raw if term not in _STOPWORDS]
    if not terms:
        terms = raw
    terms = list(dict.fromkeys(terms))
    if len(terms) > 1:
        terms.insert(0, " ".join(terms))
    return list(dict.fromkeys(terms))


def search_documents(
    query: str,
    document_id: str | None = None,
    ticker: str | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Ranked line matches with a stable document/section/line/quote identity."""
    text = str(query or "").strip()
    if not text:
        raise paths.ToolInputError("query must not be empty")
    try:
        count = int(limit)
    except (TypeError, ValueError) as exc:
        raise paths.ToolInputError(f"limit must be an integer, got {limit!r}") from exc
    if count < 1:
        raise paths.ToolInputError(f"limit must be >= 1, got {count}")
    count = min(count, 200)
    terms = query_terms(text)
    phrase = terms[0] if terms and " " in terms[0] else ""
    individual = [term for term in terms if " " not in term]
    wanted_ticker = str(ticker).strip().upper() if ticker else None

    def _passes_scoped_gate(line_lowered: str) -> bool:
        """Two-distinctive-term evidence gate for scoped searches.

        Requiring two terms on one line keeps "strip ratio" from matching a
        generic "Key Financial Ratios" heading, while still finding
        natural-language queries whose exact phrase is absent (e.g.
        "geographic revenue by country" against "Geographically, Indonesia
        accounted for ..."). Phrase matches still rank higher in scoring.
        """
        if len(individual) >= 2:
            return sum(
                1 for term in individual if term in line_lowered
            ) >= 2
        if len(individual) == 1:
            return individual[0] in line_lowered
        return bool(phrase) and phrase in line_lowered

    if document_id:
        documents = [get_document(document_id)]
    else:
        documents = _ordered(_index().values())
        if wanted_ticker:
            documents = [
                document for document in documents
                if _matches_ticker(document, wanted_ticker)
            ]

    scored: list[tuple[int, int, str, int, dict[str, Any]]] = []
    for doc_index, document in enumerate(documents):
        if document.get("unreadable"):
            continue
        sections = document.get("sections") or []
        body_start = int(document.get("body_start") or 0)
        for number, line in enumerate(
            str(document.get("content") or "").splitlines(), start=1
        ):
            if number <= body_start or not line.strip():
                continue
            lowered = line.lower()
            # Scoped searches are evidence for note silence, so require at
            # least two distinctive terms on one line (see the gate above).
            # Unscoped/ticker searches are discovery and keep the broader
            # any-term ranking.
            if document_id and not _passes_scoped_gate(lowered):
                continue
            hits = [term for term in terms if term in lowered]
            if not hits:
                continue
            score = len(hits) * 2
            phrase_pos = lowered.find(phrase) if phrase else -1
            if phrase_pos >= 0:
                score += 6
            if len(hits) == len(terms):
                score += 3
            if line.lstrip().startswith("#"):
                score += 1
            section = _section_at(sections, number)
            if section and any(term in str(section["title"]).lower() for term in terms):
                score += 2
            if phrase_pos >= 0:
                focus = phrase_pos
                focus_end = focus + len(phrase)
            else:
                focus = min(lowered.find(term) for term in hits)
                focus_end = focus + max(len(term) for term in hits)
            scored.append((score, doc_index, str(document.get("id")), number, {
                "document": document.get("id"),
                "path": document.get("path"),
                "title": document.get("title"),
                "section": section["title"] if section else None,
                "section_id": section["id"] if section else None,
                "line": number,
                "quote": _window(line, focus, focus_end, MAX_QUOTE_CHARS),
                "snippet": _window(line, focus, focus_end, MAX_SNIPPET_CHARS),
                "_score": score,
            }))
    scored.sort(key=lambda item: (-item[0], item[1], item[3], item[2]))
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for _score, _doc_index, doc_id, line, match in scored:
        key = (doc_id, line)
        if key in seen:
            continue
        seen.add(key)
        match.pop("_score", None)
        matches.append(match)
        if len(matches) >= count:
            break
    return matches
