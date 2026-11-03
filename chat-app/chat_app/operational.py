"""Operational (Class B) data and the deterministic missing-data states.

Operational artifacts come from the document extractor as JSON:

- ``idx_data/operational/{TICKER}_{Q}_{Y}_operational.json`` (canonical), or
- ``idx_data/operational/test/{TICKER}_{Q}_{Y}_operational.json`` (legacy/test
  oracle used for ITMG Q2 2026 before the ingest stage writes the canonical
  path), or
- the ``operational`` block already merged into a fact sheet by ``enrich.py``.

Two independent shapes are accepted: the current extractor shape
(``operational.entries`` / ``operational.drivers``) and the ITMG oracle shape
(``operational.production`` / ``sales`` / ``price`` / ``balance_sheet`` /
``other`` plus ``management_stated_drivers``). Both normalize to the same flat
``entries`` and ``drivers`` lists from CONTRACTS.md §1.

This module is the only place allowed to answer existence questions
(``documents_status``); everything else returns data or an explicit state.
"""

from __future__ import annotations

import copy
import datetime as _dt
import functools
import re
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

from . import facts, filings, paths

EXPECTED_DOCUMENTS = ("filing", "press_release", "presentation")

_SEARCH_CAVEAT = (
    "A match means the terms appear on that page of the extracted PDF text; "
    "call read_document for the exact text. Image-only pages are not searched."
)

# Groups in the ITMG oracle shape → flat entries.
_ORACLE_GROUPS = ("production", "sales", "price", "balance_sheet", "other")

_DOCUMENT_TYPES = {
    "filing",
    "press_release",
    "news_release",
    "presentation",
    "results_presentation",
    "annual_report",
    "financial_statements",
    "earnings_release",
}

_STATE_ALIASES = {"captured": "captured", "ok": "captured", "success": "captured"}


# ---------------------------------------------------------------------------
# Metric grammar (only used to label verification, never to emit numbers)
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _grammar() -> dict[str, dict[str, Any]]:
    path = paths.PACKAGE_DIR / "metrics.yaml"
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    metrics = data.get("metrics") or {}
    return metrics if isinstance(metrics, dict) else {}


def _overlap(metric: str | None) -> Optional[str]:
    if not metric:
        return None
    spec = _grammar().get(facts.normalize_metric(metric)) or {}
    value = spec.get("xbrl_overlap")
    return value if isinstance(value, str) else None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _num(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    number = _num(value)
    return int(number) if number is not None else None


def _slug(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _document_id(ticker: str, doc: dict[str, Any]) -> str:
    existing = doc.get("id")
    if existing:
        return str(existing)
    published = str(doc.get("published") or doc.get("date") or "undated")[:10]
    doc_type = _slug(str(doc.get("type") or "document"))
    if doc_type == "press_release":
        doc_type = "news_release"
    return f"{ticker.lower()}_{published}_{doc_type}"


def _normalize_document(
    ticker: str, raw: dict[str, Any], *, default_status: str | None = None
) -> dict[str, Any]:
    status = raw.get("status") or default_status
    status = _STATE_ALIASES.get(_slug(str(status)), status)
    if status not in ("captured", "not_captured", "rejected"):
        status = "captured" if raw.get("path") or raw.get("pdf_path") else status
    period = raw.get("period") or raw.get("quarter")
    return {
        "id": _document_id(ticker, raw),
        "type": raw.get("type"),
        "period": str(period).upper() if period else None,
        "year": _int(raw.get("year")),
        "published": str(raw.get("published"))[:10] if raw.get("published") else None,
        "url": raw.get("url"),
        "fetched_at": raw.get("fetched_at"),
        "pages": _int(raw.get("pages")),
        "sha256": raw.get("sha256"),
        "path": raw.get("path") or raw.get("pdf_path"),
        "text_path": raw.get("text_path") or raw.get("text"),
        "status": status,
        "error": raw.get("error"),
        "http_status": _int(raw.get("http_status")),
        "extraction_warnings": list(raw.get("extraction_warnings") or []),
    }


def _document_record(doc: dict[str, Any]) -> dict[str, Any]:
    """Filter a normalized doc to the fact-sheet ``DocumentRecord`` fields."""
    keys = (
        "id", "type", "published", "url", "fetched_at", "pages", "sha256",
        "path", "status", "error", "extraction_warnings",
    )
    return {key: doc.get(key) for key in keys}


def _document_summary(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": doc.get("id"),
        "type": doc.get("type"),
        "published": doc.get("published"),
        "status": doc.get("status"),
        "url": doc.get("url"),
        "error": doc.get("error"),
        "http_status": doc.get("http_status"),
        "pages": doc.get("pages"),
    }


def _normalize_entry(
    raw: dict[str, Any],
    *,
    source_doc: str | None,
    published_at: str | None,
    ticker: str,
    quarter: str,
    year: int,
) -> dict[str, Any]:
    metric = raw.get("metric")
    overlap = _overlap(metric)
    verification = raw.get("verification")
    if not isinstance(verification, dict) or not verification:
        verification = {
            "method": "reconciled" if overlap else "quoted_in_source",
            "reconciled_to": _reconciled_source(ticker, quarter, year, overlap),
        }
    return {
        "metric": facts.normalize_metric(metric) if metric else None,
        "scope": raw.get("scope"),
        "value": _num(raw.get("value")),
        "unit": raw.get("unit"),
        "prior": _num(raw.get("prior")),
        "page": _int(raw.get("page")),
        "source_doc": raw.get("source_doc") or source_doc,
        "published_at": raw.get("published_at") or published_at,
        "status": raw.get("status") or "preliminary",
        "verification": verification,
        "quote": raw.get("quote"),
    }


def _reconciled_source(
    ticker: str, quarter: str, year: int, overlap: str | None
) -> str | None:
    if not overlap:
        return None
    try:
        statement, key = facts.resolve_metric(overlap)
    except paths.ToolInputError:
        return None
    try:
        return facts.source_for(ticker, quarter, year, key)
    except paths.ChatError:
        return None


def _normalize_driver(
    raw: dict[str, Any], *, source_doc: str | None
) -> dict[str, Any]:
    return {
        "driver": raw.get("driver"),
        "affects": raw.get("affects"),
        "quote": raw.get("quote"),
        "page": _int(raw.get("page")),
        "source_doc": raw.get("source_doc") or source_doc,
        "type": raw.get("type") or "management_stated",
    }


def _merge_documents(docs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge duplicate document records by id, filling null fields."""
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for doc in docs:
        marker = str(doc.get("id") or "")
        if not marker:
            continue
        if marker not in merged:
            merged[marker] = dict(doc)
            order.append(marker)
            continue
        current = merged[marker]
        for key, value in doc.items():
            if current.get(key) in (None, "", []) and value not in (None, "", []):
                current[key] = value
    return [merged[marker] for marker in order]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _legacy_path(ticker: str, quarter: str, year: int) -> Path:
    return (
        paths.OPERATIONAL_DIR
        / "test"
        / f"{ticker}_{quarter}_{year}_operational.json"
    )


def _manifest_path(ticker: str) -> Path:
    return paths.OPERATIONAL_DIR / ticker / "manifest.json"


def _manifest_documents(ticker: str) -> list[dict[str, Any]]:
    path = _manifest_path(ticker)
    if not path.exists():
        return []
    try:
        data = paths.load_json(path)
    except paths.ChatError:
        return []
    if isinstance(data, dict):
        raw_docs = data.get("documents") or []
    elif isinstance(data, list):
        raw_docs = data
    else:
        raw_docs = []
    docs = []
    for raw in raw_docs:
        if isinstance(raw, dict):
            docs.append(_normalize_document(ticker, raw))
    return docs


def _empty(ticker: str, quarter: str, year: int) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "period": quarter,
        "year": year,
        "entries": [],
        "drivers": [],
        "documents": [],
        "reconciliation": {},
        "available": False,
        "source": None,
    }


def _from_oracle(
    ticker: str, quarter: str, year: int, data: dict[str, Any]
) -> dict[str, Any]:
    operational = data.get("operational") or {}
    doc_raw = data.get("document") or {}
    document = _normalize_document(ticker, doc_raw) if doc_raw else None
    doc_id = document.get("id") if document else None
    published = document.get("published") if document else None

    entries: list[dict[str, Any]] = []
    # Current extractor shape: operational.entries.
    for raw in operational.get("entries") or []:
        if isinstance(raw, dict):
            entries.append(_normalize_entry(
                raw, source_doc=doc_id, published_at=published,
                ticker=ticker, quarter=quarter, year=year,
            ))
    # Oracle shape: one list per group.
    if not entries:
        for group in _ORACLE_GROUPS:
            for raw in operational.get(group) or []:
                if isinstance(raw, dict):
                    entries.append(_normalize_entry(
                        raw, source_doc=doc_id, published_at=published,
                        ticker=ticker, quarter=quarter, year=year,
                    ))
        for group, items in operational.items():
            if group in _ORACLE_GROUPS or group in ("entries", "drivers",
                                                    "management_stated_drivers"):
                continue
            if isinstance(items, list):
                for raw in items:
                    if isinstance(raw, dict) and raw.get("metric"):
                        entries.append(_normalize_entry(
                            raw, source_doc=doc_id, published_at=published,
                            ticker=ticker, quarter=quarter, year=year,
                        ))

    driver_raw = list(operational.get("drivers") or [])
    driver_raw += list(operational.get("management_stated_drivers") or [])
    drivers = []
    _seen_drivers: set[tuple[str, str, int | None]] = set()
    for raw in driver_raw:
        if not isinstance(raw, dict):
            continue
        normalized = _normalize_driver(raw, source_doc=doc_id)
        key = (
            (normalized.get("quote") or "").strip().lower(),
            (normalized.get("driver") or "").strip().lower(),
            normalized.get("page"),
        )
        if key in _seen_drivers:
            continue
        _seen_drivers.add(key)
        drivers.append(normalized)

    documents = [document] if document else []
    for raw in data.get("documents") or []:
        if isinstance(raw, dict):
            documents.append(_normalize_document(ticker, raw))

    return {
        "ticker": ticker,
        "period": quarter,
        "year": year,
        "entries": entries,
        "drivers": drivers,
        "documents": _merge_documents(documents),
        "reconciliation": {
            facts.normalize_metric(key): value
            for key, value in (data.get("reconciliation") or {}).items()
            if isinstance(value, dict)
        },
        "available": bool(
            entries or drivers
            or any(doc.get("status") == "captured" for doc in documents)
        ),
        "source": None,
    }


def _from_block(
    ticker: str, quarter: str, year: int, block: dict[str, Any]
) -> dict[str, Any]:
    entries = [
        _normalize_entry(
            {**raw, "source_doc": raw.get("source_doc")},
            source_doc=None,
            published_at=raw.get("published_at"),
            ticker=ticker, quarter=quarter, year=year,
        )
        for raw in block.get("entries") or []
        if isinstance(raw, dict)
    ]
    drivers = [
        _normalize_driver(raw, source_doc=None)
        for raw in block.get("drivers") or []
        if isinstance(raw, dict)
    ]
    return {
        "ticker": ticker,
        "period": quarter,
        "year": year,
        "entries": entries,
        "drivers": drivers,
        "documents": [],
        "reconciliation": {},
        "available": bool(entries or drivers),
        "source": None,
    }


def _doc_matches_period(doc: dict[str, Any], quarter: str, year: int) -> bool:
    """Does a manifest document belong to the requested reporting period?

    Manifests are ticker-level, so a Q2 release would otherwise make a Q1
    request look captured. Prefer explicit period fields, then infer from the
    document id's publication date; listing records are period-agnostic
    evidence and never make a period available on their own.
    """
    if str(doc.get("type") or "").lower() == "listing":
        return True
    explicit_q = str(doc.get("period") or doc.get("quarter") or "").upper()
    explicit_y = doc.get("year")
    if explicit_q:
        return explicit_q == quarter and (
            explicit_y is None or int(explicit_y) == year
        )
    match = re.search(r"(20\d{2})-(\d{2})-(\d{2})", str(doc.get("id") or ""))
    if not match:
        return False
    doc_year, month = int(match.group(1)), int(match.group(2))
    if doc_year != year:
        return False
    if month in (4, 5):
        inferred = "Q1"
    elif month in (6, 7, 8):
        inferred = "Q2"
    elif month in (9, 10, 11):
        inferred = "Q3"
    else:
        inferred = "FY"
    return inferred == quarter


def analysis_context(
    ticker: str,
    quarter: str,
    year: int | str,
    *,
    metric: str | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Relevant analyst-note snippets for one ticker/period.

    ``operational_lookup`` returns these alongside the primary release entries
    so operational context that lives only in ``analysis/*.md`` (cash cost,
    strip-ratio plans, guidance, mine-level detail) is not missed. They are
    explicitly secondary evidence: the caller must cite the note path and must
    not treat note prose as XBRL-audited.

    This is a read-only DuckDB query with keyword ranking — no embedding call,
    so it costs milliseconds rather than the ~2s of ``notes_search``.
    """
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    if not paths.NOTES_DB.exists():
        return []
    period_label = f"{q} {y}"
    try:
        import duckdb  # noqa: PLC0415 — optional at import time

        conn = duckdb.connect(str(paths.NOTES_DB), read_only=True)
        try:
            rows = conn.execute(
                "SELECT id, path, title, breadcrumb, text FROM chunks "
                "WHERE (',' || tickers || ',') ILIKE ? "
                "  AND (period = ? OR period IS NULL OR period = '')",
                [f"%,{tk},%", period_label],
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    "SELECT id, path, title, breadcrumb, text FROM chunks "
                    "WHERE (',' || tickers || ',') ILIKE ?",
                    [f"%,{tk},%"],
                ).fetchall()
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 — context is additive, never fatal
        return []
    if not rows:
        return []
    # A chunk's `tickers` field includes every ticker it mentions, so a PTBA
    # note that references ITMG can match an ITMG query. Prefer company-specific
    # paths when they exist.
    company_rows = [row for row in rows if tk in str(row[1] or "").upper()]
    if company_rows:
        rows = company_rows

    weights = {
        "cash cost": 5, "strip ratio": 5, "stripping": 4, "strip": 3,
        "cash conversion": 4, "unit cost": 3, "overburden": 4,
        "production plan": 4, "guidance": 3, "target": 2, "rkab": 3,
        "production": 2, "sales": 2, "asp": 2, "average selling": 2,
        "volume": 1, "inventory": 1, "cost": 1, "margin": 1,
        "outlook": 2, "risk": 2, "capex": 1, "logistics": 1,
        "dmo": 2, "royalty": 1,
    }
    terms = [
        "production", "sales", "volume", "strip ratio", "stripping", "strip",
        "cash cost", "cash conversion", "unit cost", "overburden",
        "production plan", "asp", "average selling", "guidance", "target",
        "rkab", "inventory", "cost", "margin", "outlook", "risk",
        "capex", "logistics", "dmo", "royalty",
    ]
    if metric:
        metric_text = facts.canonical_metric(metric).replace("_", " ")
        terms = [metric_text, *metric_text.split(), *terms]
        weights[metric_text] = 5
        for part in metric_text.split():
            weights[part] = max(weights.get(part, 1), 3)
    terms = list(dict.fromkeys(term.casefold() for term in terms if term))

    scored: list[tuple[int, str, str, str, str, str]] = []
    for cid, path, title, crumb, text in rows:
        body = str(text or "")
        lowered = body.casefold()
        score = sum(
            weight * lowered.count(term)
            for term, weight in weights.items()
        )
        if period_label.casefold() in str(path or "").casefold():
            score += 8
        if f"Q1 {y}" in str(path or ""):
            score -= 4
        section_text = f"{title} {crumb}".casefold()
        if "concern" in section_text or "risk" in section_text:
            score += 12
        elif any(term in section_text for term in ("outlook", "guidance")):
            score += 8
        elif any(term in section_text for term in ("operation", "cost", "production")):
            score += 6
        if score <= 0:
            continue
        scored.append((score, str(cid), str(path), str(title or ""), str(crumb or ""), body))

    scored.sort(key=lambda item: (-item[0], item[1]))
    out: list[dict[str, Any]] = []
    for score, cid, path, title, crumb, body in scored[: max(1, limit)]:
        snippet = _best_snippet(body, terms, weights, max_chars=520)
        out.append({
            "id": cid,
            "path": f"analysis/{path}" if not path.startswith("analysis/") else path,
            "title": title,
            "section": crumb,
            "snippet": snippet,
            "secondary": True,
            "score": score,
        })
    return out


def _best_snippet(
    text: str,
    terms: list[str],
    weights: dict[str, int] | None = None,
    max_chars: int = 420,
) -> str:
    """Pick the sentence cluster with the strongest operational term density."""
    body = re.sub(r"\s+", " ", text).strip()
    if len(body) <= max_chars:
        return body
    sentences = re.split(r"(?<=[.!?])\s+", body)
    if not sentences:
        return body[:max_chars].rsplit(" ", 1)[0] + "…"
    weights = weights or {}
    best_index = 0
    best_score = -1
    for index, sentence in enumerate(sentences):
        lowered = sentence.casefold()
        score = sum(
            weights.get(term, 1) * lowered.count(term) for term in terms
        )
        if score > best_score:
            best_score = score
            best_index = index
    start = max(0, best_index - 1)
    end = min(len(sentences), best_index + 2)
    snippet = " ".join(sentences[start:end]).strip()
    while len(snippet) > max_chars and end > best_index + 1:
        end -= 1
        snippet = " ".join(sentences[start:end]).strip()
    if len(snippet) > max_chars:
        snippet = snippet[:max_chars].rsplit(" ", 1)[0] + "…"
    return snippet


def load_operational(
    ticker: str,
    quarter: str,
    year: int | str,
    *,
    include_fact_sheet: bool = True,
) -> dict[str, Any]:
    """Load and normalize operational entries/drivers/documents for one period.

    Never raises for an absent artifact: an empty structure with
    ``available: false`` is returned so callers can ask ``documents_status``
    for the explicit missing state.
    """
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    result = _empty(tk, q, y)

    candidate = paths.operational_path(tk, q, y)
    source = None
    if candidate.exists():
        source = candidate
    else:
        legacy = _legacy_path(tk, q, y)
        if legacy.exists():
            source = legacy

    if source is not None:
        try:
            data = paths.load_json(source)
        except paths.ChatError:
            data = None
        if isinstance(data, dict):
            result = _from_oracle(tk, q, y, data)
            result["source"] = str(source.relative_to(paths.PROJECT_ROOT))

    if not result["available"] and include_fact_sheet:
        try:
            sheet = paths.load_yaml(paths.fact_path(tk, q, y))
        except paths.ChatError:
            sheet = None
        if isinstance(sheet, dict):
            block = sheet.get("operational") or {}
            embedded = _from_block(tk, q, y, block)
            docs = [
                _normalize_document(tk, raw)
                for raw in sheet.get("documents") or []
                if isinstance(raw, dict) and _doc_matches_period(raw, q, y)
            ]
            if embedded["available"] or docs:
                embedded["documents"] = docs
                embedded["available"] = bool(
                    embedded["entries"]
                    or embedded["drivers"]
                    or any(doc.get("status") == "captured" for doc in docs)
                )
                embedded["source"] = f"fact:{paths.fact_path(tk, q, y).name}"
                result = embedded

    manifest_docs = _manifest_documents(tk)
    if manifest_docs or result["documents"]:
        primary_id = next(
            (doc.get("id") for doc in result["documents"] if doc.get("id")),
            None,
        )
        relevant = [
            doc for doc in manifest_docs
            if (primary_id and doc.get("id") == primary_id)
            or _doc_matches_period(doc, q, y)
        ]
        result["documents"] = _merge_documents(
            list(result["documents"]) + relevant
        )
    result["available"] = bool(
        result["entries"]
        or result["drivers"]
        or any(doc.get("status") == "captured" for doc in result["documents"])
    )
    return result


def documents_for_period(
    ticker: str,
    quarter: str,
    year: int | str,
    *,
    include_filings: bool = True,
) -> list[dict[str, Any]]:
    """Operational documents plus the raw filing registry for one period.

    ``load_operational`` keeps its existing availability semantics: it reports
    what the operational extractor captured. This helper adds the broader
    fact-sheet + raw-PDF existence signal for document questions.
    """
    op = load_operational(ticker, quarter, year)
    docs = list(op.get("documents") or [])
    if include_filings:
        docs.extend(filings.filing_documents(ticker, quarter, year))
    return _merge_documents(docs)


# ---------------------------------------------------------------------------
# Existence: the only three-state (plus unsupported) answer in the app
# ---------------------------------------------------------------------------


def _period_entry(
    ticker: str,
    quarter: str | None,
    year: int | None,
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    """Resolve an explicit or implied (ticker, period, year) supported entry."""
    entries = paths.supported_periods().get(ticker) or []
    if not entries:
        return None, []
    if quarter is None and year is None:
        return entries[-1], entries
    if quarter is not None and year is not None:
        q = paths.normalize_quarter(quarter)
        y = paths.normalize_year(year)
        for entry in entries:
            if entry["period"] == q and entry["year"] == y:
                return entry, entries
        return None, entries
    if quarter is not None:
        q = paths.normalize_quarter(quarter)
        candidates = [e for e in entries if e["period"] == q]
        return (candidates[-1], entries) if candidates else (None, entries)
    y = paths.normalize_year(year)
    candidates = [e for e in entries if e["year"] == y]
    return (candidates[-1], entries) if candidates else (None, entries)


def _parse_date(value: Any) -> Optional[_dt.date]:
    if not value:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _meta_period_end(ticker: str, quarter: str, year: int) -> Optional[_dt.date]:
    try:
        sheet = paths.load_yaml(paths.fact_path(ticker, quarter, year))
    except paths.ChatError:
        return None
    if not isinstance(sheet, dict):
        return None
    return _parse_date((sheet.get("meta") or {}).get("period_end"))


def _latest_captured_release(ticker: str) -> Optional[dict[str, Any]]:
    """Latest captured document anywhere for the issuer (for not_yet_published)."""
    candidates: list[dict[str, Any]] = []
    pattern = f"{ticker}_*_operational.json"
    for path in paths.OPERATIONAL_DIR.rglob(pattern):
        try:
            data = paths.load_json(path)
        except paths.ChatError:
            continue
        if not isinstance(data, dict):
            continue
        doc_raw = data.get("document")
        if isinstance(doc_raw, dict):
            doc = _normalize_document(ticker, doc_raw)
            if doc.get("status") == "captured":
                candidates.append(doc)
    candidates += [d for d in _manifest_documents(ticker) if d.get("status") == "captured"]
    dated = [d for d in candidates if d.get("published")]
    if not dated:
        return None
    return max(dated, key=lambda d: str(d["published"]))


def _doc_type_matches(item: str, doc: dict[str, Any]) -> bool:
    wanted = _slug(item)
    actual = _slug(str(doc.get("type") or ""))
    if wanted == actual:
        return True
    # press_release / news_release are the same document class across issuers.
    return {wanted, actual} == {"press_release", "news_release"}


def _has_metric(op: dict[str, Any], metric: str) -> Optional[dict[str, Any]]:
    wanted = facts.canonical_metric(metric)
    for entry in op.get("entries") or []:
        if facts.canonical_metric(entry.get("metric") or "") == wanted:
            return {"where": "entries", **entry}
    for key, value in (op.get("reconciliation") or {}).items():
        if facts.canonical_metric(key) == wanted:
            spec = _grammar().get(facts.normalize_metric(key)) or {}
            return {
                "where": "reconciliation",
                "metric": key,
                "value": _num(value.get("release")),
                "unit": spec.get("unit"),
                "xbrl": _num(value.get("xbrl")),
            }
    return None


def _page_range(docs: list[dict[str, Any]]) -> Optional[str]:
    pages = [d.get("pages") for d in docs if isinstance(d.get("pages"), int)]
    if not pages:
        return None
    return f"1-{max(pages)}"


def _fact_sheet_metric(
    ticker: str, quarter: str, year: int, item: str,
) -> tuple[str, str, dict[str, Any]] | None:
    """Answer a canonical financial line from the captured fact sheet.

    Returns ``None`` when the item is not a primary-statement line, so
    operational metrics keep the document-based logic below.
    """
    statement = facts.statement_for_metric(item)
    if statement is None:
        return None
    try:
        sheet = facts.load_facts(ticker, quarter, year)
    except paths.ChatError:
        return None
    metric = facts.canonical_metric(item)
    block = sheet.get(statement) or {}
    lines = block.get("lines") or {}
    provenance = (block.get("provenance") or {}).get(metric) or {}
    source = None
    if provenance:
        source = ":".join(
            str(part) for part in (provenance.get("sheet"), provenance.get("row"))
            if part is not None
        ) or None
    if metric in lines and lines[metric] is not None:
        extra: dict[str, Any] = {
            "source": "fact_sheet",
            "document_id": f"{ticker}_{quarter}_{year}.yaml",
            "statement": statement,
            "metric": metric,
            "value": lines[metric],
        }
        if source:
            extra["sheet_row"] = source
        return (
            "available",
            f"{metric} is reported in the {quarter} {year} fact sheet"
            + (f" at {source}" if source else ""),
            extra,
        )
    # The filing is captured; this issuer simply does not report the line
    # (e.g. a bank has no cost of sales / gross profit).
    return (
        "not_disclosed",
        f"the captured {quarter} {year} filing does not report {metric} "
        f"(primary statements of this issuer have no such line)",
        {
            "source": "fact_sheet",
            "document_id": f"{ticker}_{quarter}_{year}.yaml",
            "statement": statement,
            "metric": metric,
        },
    )


def documents_status(
    ticker: str,
    period: str | None = None,
    year: int | str | None = None,
    item: str | None = None,
    *,
    today: _dt.date | None = None,
    include_filings: bool = True,
) -> dict[str, Any]:
    """Deterministic availability answer with evidence (MVP-PLAN §4.3)."""
    today = today or _dt.date.today()
    tk = paths.normalize_ticker(ticker)
    entry, entries = _period_entry(tk, period, year)

    if entry is None:
        alternatives = (
            [f"{e['period']} {e['year']}" for e in entries]
            if entries
            else paths.supported_summary()
        )
        return {
            "ticker": tk,
            "period": period,
            "year": year,
            "item": item,
            "state": "unsupported",
            "evidence": {
                "as_of": today.isoformat(),
                "supported_tickers": paths.supported_tickers(),
                "supported_periods": alternatives,
                "detail": (
                    f"{tk} {period or ''} {year or ''}".strip()
                    + " is not in the supported set"
                ),
            },
            "documents": [],
        }

    q = entry["period"]
    y = entry["year"]
    op = load_operational(tk, q, y)
    docs = documents_for_period(tk, q, y, include_filings=include_filings)
    captured = [d for d in docs if d.get("status") == "captured"]
    failed = [d for d in docs if d.get("status") == "not_captured"]
    period_end = _meta_period_end(tk, q, y)
    latest = _latest_captured_release(tk)

    evidence: dict[str, Any] = {
        "as_of": today.isoformat(),
        "period_end": period_end.isoformat() if period_end else None,
        "expected_documents": list(EXPECTED_DOCUMENTS),
    }

    state: str
    detail: str

    if item:
        wanted = _slug(item)
        # A canonical financial line is answered from the fact sheet, not from the
        # operational document set. This is what distinguishes "the bank does not
        # report COGS" (not_disclosed) from "we never captured a release"
        # (not_captured): the filing IS captured, the line is absent by design.
        fact_state = _fact_sheet_metric(tk, q, y, item)
        if fact_state is not None:
            state, detail, extra = fact_state
            evidence.update(extra)
            evidence["detail"] = detail
            return {
                "ticker": tk,
                "period": q,
                "year": y,
                "item": item,
                "state": state,
                "evidence": evidence,
                "documents": [_document_summary(d) for d in docs],
            }
        matching_captured = [d for d in captured if _doc_type_matches(item, d)]
        if matching_captured:
            doc = matching_captured[0]
            state = "available"
            detail = f"{item} captured as {doc.get('id')}"
            evidence.update({
                "document_id": doc.get("id"),
                "pages": doc.get("pages"),
                "url": doc.get("url"),
            })
        elif wanted in _DOCUMENT_TYPES or any(
            _doc_type_matches(item, d) for d in docs
        ):
            matching_failed = [d for d in failed if _doc_type_matches(item, d)]
            if matching_failed:
                doc = matching_failed[0]
                state = "not_captured"
                detail = (
                    f"{item} was discovered but could not be captured"
                    + (f" ({doc.get('error')})" if doc.get("error") else "")
                )
                evidence.update({
                    "document_id": doc.get("id"),
                    "url": doc.get("url"),
                    "http_status": doc.get("http_status"),
                    "error": doc.get("error"),
                    "retrieved_at": doc.get("fetched_at") or today.isoformat(),
                })
            elif captured:
                state = "not_captured"
                detail = (
                    f"no {item} captured for {tk} {q} {y}; captured: "
                    + ", ".join(str(d.get("type")) for d in captured)
                )
                evidence.update({
                    "document_id": captured[0].get("id"),
                    "page_range": _page_range(captured),
                })
            else:
                state, detail, extra = _no_document_state(
                    latest, period_end, today
                )
                evidence.update(extra)
        else:
            found = _has_metric(op, item)
            if found:
                state = "available"
                doc_id = found.get("source_doc") or (
                    captured[0].get("id") if captured else None
                )
                detail = f"{item} is disclosed in {doc_id or 'the captured release'}"
                evidence.update({
                    "document_id": doc_id,
                    "page": found.get("page"),
                    "where": found.get("where"),
                })
            elif captured:
                matches = filings.search_documents(captured, item)
                if matches:
                    hit = matches[0]
                    state = "available"
                    detail = (
                        f"{item} found on page {hit.get('page')} of "
                        f"{hit.get('document_id')}; call read_document for exact text"
                    )
                    evidence.update({
                        "document_id": hit.get("document_id"),
                        "page": hit.get("page"),
                        "pages": hit.get("pages"),
                        "matched_terms": hit.get("matched_terms"),
                        "search": "filing_text",
                        "search_caveat": _SEARCH_CAVEAT,
                    })
                else:
                    doc = next(
                        (d for d in captured if str(d.get("type")).lower() == "filing"),
                        captured[0],
                    )
                    pages_searched = sum(
                        d["pages"] for d in captured if isinstance(d.get("pages"), int)
                    ) or None
                    state = "not_disclosed"
                    detail = (
                        f"the captured filing {doc.get('id')} was searched and "
                        f"{item} was not found in its extracted text"
                    )
                    evidence.update({
                        "document_id": doc.get("id"),
                        "page_range": _page_range(captured),
                        "pages": doc.get("pages"),
                        "pages_searched": pages_searched,
                        "terms": item,
                        "query": item,
                        "search": "filing_text",
                        "search_caveat": _SEARCH_CAVEAT,
                    })
            elif failed:
                doc = failed[0]
                state = "not_captured"
                detail = (
                    f"{item} could not be checked: the release was not captured"
                    + (f" ({doc.get('error')})" if doc.get("error") else "")
                )
                evidence.update({
                    "document_id": doc.get("id"),
                    "url": doc.get("url"),
                    "error": doc.get("error"),
                    "retrieved_at": doc.get("fetched_at") or today.isoformat(),
                })
            else:
                state, detail, extra = _no_document_state(
                    latest, period_end, today
                )
                evidence.update(extra)
    elif captured:
        doc = captured[0]
        state = "available"
        detail = f"captured {doc.get('type') or 'document'} {doc.get('id')}"
        evidence.update({
            "document_id": doc.get("id"),
            "pages": doc.get("pages"),
            "url": doc.get("url"),
            "published": doc.get("published"),
        })
    elif failed:
        doc = failed[0]
        state = "not_captured"
        detail = (
            "the document was discovered but could not be captured"
            + (f" ({doc.get('error')})" if doc.get("error") else "")
        )
        evidence.update({
            "document_id": doc.get("id"),
            "url": doc.get("url"),
            "http_status": doc.get("http_status"),
            "error": doc.get("error"),
            "retrieved_at": doc.get("fetched_at") or today.isoformat(),
        })
    else:
        state, detail, extra = _no_document_state(latest, period_end, today)
        evidence.update(extra)

    evidence["detail"] = detail
    return {
        "ticker": tk,
        "period": q,
        "year": y,
        "item": item,
        "state": state,
        "evidence": evidence,
        "documents": [_document_summary(d) for d in docs],
    }


def _no_document_state(
    latest: dict[str, Any] | None,
    period_end: _dt.date | None,
    today: _dt.date,
) -> tuple[str, str, dict[str, Any]]:
    """``not_yet_published`` inside the T+45 window, else ``not_captured``."""
    extra: dict[str, Any] = {}
    if latest:
        extra["latest_captured_release"] = latest.get("published")
        extra["latest_captured_document"] = latest.get("id")
    if period_end is not None:
        age = (today - period_end).days
        extra["days_after_period_end"] = age
        if age <= 45:
            detail = (
                "No release captured yet; this issuer's releases have landed up to "
                f"T+{age if latest else 0}"
            )
            if latest and latest.get("published"):
                detail = (
                    f"No {period_end.year} release captured yet; this issuer's latest "
                    f"captured release is {latest.get('published')}"
                )
            return "not_yet_published", detail, extra
    return (
        "not_captured",
        "No document was discovered or captured for this period",
        extra,
    )


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


def _financials_state(sheet: dict[str, Any]) -> tuple[str, list[str]]:
    income = (sheet.get("income_statement") or {}).get("lines") or {}
    balance = (sheet.get("balance_sheet") or {}).get("lines") or {}
    cash = (sheet.get("cash_flow") or {}).get("lines") or {}
    is_bank = income.get("cogs") is None and income.get("gross_profit") is None

    required = ["revenue", "pbt", "net_profit"]
    if not is_bank:
        required += ["gross_profit"]
    required += ["total_assets", "total_liabilities", "total_equity"]
    required += ["ocf", "net_change", "cash_end"]
    sources = {"revenue": income, "pbt": income, "net_profit": income,
               "gross_profit": income, "total_assets": balance,
               "total_liabilities": balance, "total_equity": balance,
               "ocf": cash, "net_change": cash, "cash_end": cash}
    missing = [key for key in required if sources[key].get(key) is None]
    if not missing:
        return "complete", []
    if len(missing) <= 2:
        return "partial", missing
    return "missing", missing


def _operational_state(op: dict[str, Any]) -> str:
    docs = op.get("documents") or []
    captured = [d for d in docs if d.get("status") == "captured"]
    if not captured:
        return "not_captured"
    entries = op.get("entries") or []
    if not entries:
        return "partial"
    if any(e.get("status") == "rejected" for e in entries):
        return "partial"
    return "complete"


def _notes_state(ticker: str) -> str:
    db = paths.NOTES_DB
    if not db.exists():
        return "missing"
    try:
        import duckdb  # noqa: PLC0415 — optional at import time

        conn = duckdb.connect(str(db), read_only=True)
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE (',' || tickers || ',') ILIKE ?",
                [f"%,{ticker},%"],
            ).fetchone()[0]
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 — missing/locked DB is a gap, not a crash
        return "missing"
    if not count:
        return "missing"
    latest_note = max(
        (
            path.stat().st_mtime
            for path in paths.ANALYSIS_DIR.rglob("*.md")
            if ticker in path.name.upper()
        ),
        default=0.0,
    )
    if latest_note and db.stat().st_mtime < latest_note:
        return "stale"
    return "indexed"


def _analysis_state(ticker: str) -> str:
    try:
        present = any(
            ticker in path.name.upper()
            for path in paths.ANALYSIS_DIR.rglob("*.md")
        )
    except OSError:
        present = False
    return "present" if present else "missing"


# One slot per (ticker, period, year, day); an artifact change replaces it.
_COMPLETENESS_CACHE: dict[
    tuple[str, str, int, str], tuple[tuple[Any, ...], dict[str, Any]]
] = {}
_COMPLETENESS_LOCK = threading.Lock()


def _completeness_signature(
    ticker: str, quarter: str, year: int
) -> tuple[Any, ...]:
    """Version key for the files ``completeness`` actually reads.

    Fact sheet, operational release/legacy/manifest, the filing PDF for this
    period, the notes index and the metric grammar all feed the layer states.
    Only notes naming this ticker affect the notes/analysis layers.
    """
    files: list[Path] = [
        paths.fact_path(ticker, quarter, year),
        paths.operational_path(ticker, quarter, year),
        _legacy_path(ticker, quarter, year),
        _manifest_path(ticker),
        paths.RAW_DIR / f"{ticker}_{quarter}_{year}.pdf",
        paths.NOTES_DB,
        paths.PACKAGE_DIR / "metrics.yaml",
    ]
    try:
        files.extend(
            path for path in paths.ANALYSIS_DIR.rglob("*.md")
            if ticker in path.name.upper()
        )
    except OSError:
        pass
    return paths.file_versions(files)


def clear_completeness_cache() -> None:
    """Drop cached completeness results (tests and manual re-indexing)."""
    with _COMPLETENESS_LOCK:
        _COMPLETENESS_CACHE.clear()


def completeness(
    ticker: str,
    quarter: str,
    year: int | str,
    *,
    today: _dt.date | None = None,
    operational_block: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Layer completeness for one period (MVP-PLAN §4.1).

    Results are cached by ``(ticker, period, year, today)`` plus a signature of
    the exact artifacts the answer reads, so repeated preflights (``list_companies``
    scores every ticker) do not re-parse YAML. ``operational_block`` requests are
    never cached: ``enrich.py`` scores the block it is about to write.
    """
    today = today or _dt.date.today()
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)

    if operational_block is not None:
        return _compute_completeness(tk, q, y, today, operational_block)

    key = (tk, q, y, today.isoformat())
    signature = _completeness_signature(tk, q, y)
    with _COMPLETENESS_LOCK:
        cached = _COMPLETENESS_CACHE.get(key)
        if cached is not None and cached[0] == signature:
            return copy.deepcopy(cached[1])

    result = _compute_completeness(tk, q, y, today, None)

    with _COMPLETENESS_LOCK:
        _COMPLETENESS_CACHE[key] = (signature, result)
    return copy.deepcopy(result)


def _compute_completeness(
    tk: str,
    q: str,
    y: int,
    today: _dt.date,
    operational_block: dict[str, Any] | None,
) -> dict[str, Any]:
    """Uncached completeness computation for already-normalized arguments."""
    try:
        sheet = paths.load_yaml(paths.fact_path(tk, q, y))
    except paths.ChatError:
        sheet = {}
    if not isinstance(sheet, dict):
        sheet = {}

    financials, missing = _financials_state(sheet)
    if operational_block is not None:
        op = {
            "entries": operational_block.get("entries") or [],
            "drivers": operational_block.get("drivers") or [],
            "documents": operational_block.get("documents") or [],
        }
    else:
        op = load_operational(tk, q, y)
    operational = _operational_state(op)
    notes = _notes_state(tk)
    analysis = _analysis_state(tk)

    filing_docs = filings.filing_documents(tk, q, y)
    all_docs = _merge_documents(list(op.get("documents") or []) + filing_docs)
    filings_state = "captured" if filing_docs else "not_captured"
    capt = {_slug(str(d.get("type") or "")) for d in all_docs
            if d.get("status") == "captured"}
    gaps: list[dict[str, Any]] = []
    if financials != "complete":
        gaps.append({
            "item": "financials",
            "state": financials,
            "detail": "missing: " + ", ".join(missing) if missing else None,
        })
    if operational != "complete":
        gaps.append({
            "item": "operational",
            "state": operational if operational == "partial" else "not_captured",
            "detail": (
                "captured document has no extracted entries"
                if operational == "partial"
                else "no operational document captured for this period"
            ),
        })
    if notes != "indexed":
        gaps.append({"item": "notes", "state": notes, "detail": None})
    if analysis != "present":
        gaps.append({"item": "analysis", "state": analysis, "detail": None})
    if all_docs:
        for doc_type in EXPECTED_DOCUMENTS:
            wanted = _slug(doc_type)
            if wanted in capt or (wanted == "press_release" and "news_release" in capt):
                continue
            gaps.append({
                "item": doc_type,
                "state": "not_captured",
                "detail": "no captured document of this type",
            })

    return {
        "financials": financials,
        "operational": operational,
        "filings": filings_state,
        "notes": notes,
        "analysis": analysis,
        "expected_documents": list(EXPECTED_DOCUMENTS),
        "as_of": today.isoformat(),
        "gaps": gaps,
    }

