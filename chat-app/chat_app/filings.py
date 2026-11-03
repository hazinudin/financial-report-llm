"""Captured filing registry and page-level text search.

The operational extractor only knows about documents it processed. The raw
filing PDFs under ``raw/`` are a broader, more reliable existence signal: when a
fact sheet and its PDF both exist, the filing was captured even if no operational
extract was written. This module exposes that registry and a small, deterministic
page search so ``documents_status`` can point the model at the right PDF page.

Search is deliberately simple (lower-cased terms with a few hand-written
synonyms) and cached per file version. It is a *pointer*, not an answer: the
model must call ``read_document`` for exact text.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from . import paths

# Hand-written synonym groups. The first item is the canonical term name.
_SYNONYM_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "geographic",
        (
            "geographic", "geographical", "geography", "country", "countries",
            "market", "markets", "region", "regions", "regional",
        ),
    ),
    ("revenue", ("revenue", "revenues", "sales")),
    ("segment", ("segment", "segments", "division", "divisions")),
    ("production", ("production", "output", "outputs", "produce", "produced")),
    ("cost", ("cost", "costs", "expense", "expenses")),
    # Country spellings used across the Indonesian/English filing tables.
    ("indonesia", ("indonesia", "indonesian")),
    ("philippines", ("philippines", "phillipines", "filipina")),
    ("thailand", ("thailand",)),
    ("malaysia", ("malaysia",)),
    ("singapore", ("singapore", "singapura")),
    ("vietnam", ("vietnam",)),
    ("cambodia", ("cambodia", "kamboja")),
)

_VARIANT_TO_TERM: dict[str, str] = {
    variant: term
    for term, variants in _SYNONYM_GROUPS
    for variant in variants
}
_TERM_VARIANTS: dict[str, tuple[str, ...]] = dict(_SYNONYM_GROUPS)

# Lexical forms rank above broad contextual synonyms: "geographical market"
# should outrank an incidental "marketing" or "future market" mention.
_PRIMARY_VARIANTS: dict[str, tuple[str, ...]] = {
    "geographic": ("geographic", "geographical", "geography"),
    "revenue": ("revenue", "revenues"),
    "segment": ("segment", "segments"),
    "production": ("production", "outputs", "output"),
    "cost": ("cost", "costs"),
}

_STOPWORDS = {
    "by", "of", "the", "and", "or", "in", "on", "to", "for", "from", "with",
    "vs", "versus", "q1", "q2", "q3", "q4", "year", "years", "change",
    "changes", "breakdown", "detail", "details", "data", "split", "compare",
    "comparison",
}
_YEAR_RE = re.compile(r"^20\d{2}$")
_TOKEN_RE = re.compile(r"[a-z0-9]+")

# (path, mtime_ns, size) → page text tuple. The version key makes edits and
# replacements invalidate naturally.
_PAGE_TEXT_CACHE: dict[tuple[str, int, int], tuple[str, ...]] = {}
_PAGE_COUNT_CACHE: dict[tuple[str, int, int], int | None] = {}


def _version(path: Path) -> tuple[str, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), stat.st_mtime_ns, stat.st_size)


def page_count(path: str | Path) -> int | None:
    """PDF page count, cached by path version. ``None`` when unreadable."""
    candidate = Path(path)
    key = _version(candidate)
    if key is None:
        return None
    if key in _PAGE_COUNT_CACHE:
        return _PAGE_COUNT_CACHE[key]
    try:
        import fitz  # noqa: PLC0415 — optional PDF backend
    except ImportError:
        _PAGE_COUNT_CACHE[key] = None
        return None
    try:
        with fitz.open(candidate) as doc:
            count = int(doc.page_count)
    except Exception:  # noqa: BLE001 — a broken PDF is a miss, not a crash
        count = None
    _PAGE_COUNT_CACHE[key] = count
    return count


def _page_texts(path: Path) -> tuple[str, ...]:
    key = _version(path)
    if key is None:
        return ()
    cached = _PAGE_TEXT_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        import fitz  # noqa: PLC0415 — optional PDF backend
    except ImportError:
        return ()
    try:
        with fitz.open(path) as doc:
            texts = tuple(
                doc.load_page(index).get_text("text")
                for index in range(doc.page_count)
            )
    except Exception:  # noqa: BLE001
        return ()
    _PAGE_TEXT_CACHE[key] = texts
    return texts


def _resolve_pdf(path_value: Any) -> Path | None:
    if not path_value:
        return None
    candidate = Path(str(path_value))
    if not candidate.is_absolute():
        candidate = paths.PROJECT_ROOT / candidate
    if candidate.suffix.lower() != ".pdf" or not candidate.exists():
        return None
    return candidate


def _relative_or_none(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(paths.PROJECT_ROOT))
    except ValueError:
        return str(path)


def filing_document(ticker: str, quarter: str, year: int | str) -> dict[str, Any] | None:
    """The captured filing for one period, or ``None`` when it is not there.

    A filing counts as captured only when both the canonical fact sheet and the
    raw PDF exist. The markdown text is optional provenance, not a requirement.
    """
    tk = paths.normalize_ticker(ticker)
    q = paths.normalize_quarter(quarter)
    y = paths.normalize_year(year)
    if not paths.fact_path(tk, q, y).exists():
        return None
    pdf = paths.RAW_DIR / f"{tk}_{q}_{y}.pdf"
    if not pdf.exists():
        return None
    results_text = paths.RESULTS_DIR / f"{tk}_{q}_{y}.md"
    preprocessed_text = paths.PREPROCESSED_DIR / f"{tk}_{q}_{y}.md"
    text_path = results_text if results_text.exists() else (
        preprocessed_text if preprocessed_text.exists() else None
    )
    return {
        "id": f"{tk.lower()}_{q.lower()}_{y}_filing",
        "type": "filing",
        "status": "captured",
        "period": q,
        "year": y,
        "path": _relative_or_none(pdf),
        "text_path": _relative_or_none(text_path),
        "pages": page_count(pdf),
        "url": None,
        "published": None,
    }


def filing_documents(
    ticker: str,
    quarter: str | None = None,
    year: int | str | None = None,
) -> list[dict[str, Any]]:
    """Captured filings for one supported period or every supported period."""
    tk = paths.normalize_ticker(ticker)
    if quarter is not None and year is not None:
        doc = filing_document(tk, quarter, year)
        return [doc] if doc else []

    out: list[dict[str, Any]] = []
    for entry in paths.supported_periods().get(tk, []):
        if quarter is not None and entry["period"] != paths.normalize_quarter(quarter):
            continue
        if year is not None and entry["year"] != paths.normalize_year(year):
            continue
        doc = filing_document(tk, entry["period"], entry["year"])
        if doc:
            out.append(doc)
    return out


def _terms(query: str) -> tuple[list[str], list[str]]:
    """Return ``(canonical_terms, original_distinctive_terms)`` for a query."""
    canonical: list[str] = []
    original: list[str] = []
    for token in _TOKEN_RE.findall((query or "").lower()):
        if token in _STOPWORDS or _YEAR_RE.match(token):
            continue
        mapped = _VARIANT_TO_TERM.get(token)
        if mapped is None and len(token) > 4 and token.endswith("s"):
            mapped = _VARIANT_TO_TERM.get(token[:-1])
        term = mapped or token
        if term not in canonical:
            canonical.append(term)
        if token not in original:
            original.append(token)
    return canonical, original


def _contains(haystack: str, term: str) -> bool:
    return re.search(rf"\b{re.escape(term)}\b", haystack) is not None


def _matched(haystack: str, term: str) -> bool:
    variants = _TERM_VARIANTS.get(term, (term,))
    return any(_contains(haystack, variant) for variant in variants)


def _variant_hits(haystack: str, variants: tuple[str, ...]) -> int:
    return sum(
        len(re.findall(rf"\b{re.escape(variant)}\b", haystack))
        for variant in variants
    )


def search_documents(
    docs: Iterable[dict[str, Any]],
    query: str,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Find PDF pages whose text matches the query terms.

    Returns page hints only (1-based PDF pages), ranked by matched-term count,
    literal phrase/term bonus, filing preference, then page order.
    """
    canonical, original = _terms(query)
    if not canonical or limit <= 0:
        return []
    required = len(canonical) if len(canonical) == 1 else 2
    normalized_query = re.sub(r"\s+", " ", (query or "").strip().lower())

    hits: list[dict[str, Any]] = []
    for doc in docs:
        pdf = _resolve_pdf(doc.get("path") or doc.get("pdf_path"))
        if pdf is None:
            continue
        pages = _page_texts(pdf)
        if not pages:
            continue
        total = len(pages)
        doc_id = doc.get("id")
        is_filing = str(doc.get("type") or "").lower() == "filing"
        for index, text in enumerate(pages, start=1):
            haystack = (text or "").lower()
            if not haystack:
                continue
            matched_terms = [term for term in canonical if _matched(haystack, term)]
            if len(matched_terms) < required:
                continue
            literal_hits = sum(1 for term in original if _contains(haystack, term))
            if normalized_query and normalized_query in haystack:
                phrase_bonus = 3
            elif original and literal_hits == len(original):
                phrase_bonus = 2
            elif literal_hits:
                phrase_bonus = 1
            else:
                phrase_bonus = 0
            phrase_bonus += sum(
                _variant_hits(haystack, _PRIMARY_VARIANTS.get(term, (term,)))
                for term in canonical
            )
            score = len(matched_terms) * 10 + phrase_bonus + (2 if is_filing else 0)
            hits.append({
                "document_id": doc_id,
                "page": index,
                "matched_terms": matched_terms,
                "pages": total if total is not None else doc.get("pages"),
                "score": score,
                "_phrase_bonus": phrase_bonus,
                "_is_filing": is_filing,
            })

    hits.sort(key=lambda hit: (
        -len(hit["matched_terms"]),
        -hit["_phrase_bonus"],
        0 if hit["_is_filing"] else 1,
        hit["page"],
        str(hit["document_id"] or ""),
    ))
    out: list[dict[str, Any]] = []
    for hit in hits[: max(0, int(limit))]:
        clean = dict(hit)
        clean.pop("_phrase_bonus", None)
        clean.pop("_is_filing", None)
        out.append(clean)
    return out


__all__ = [
    "filing_document",
    "filing_documents",
    "page_count",
    "search_documents",
]
