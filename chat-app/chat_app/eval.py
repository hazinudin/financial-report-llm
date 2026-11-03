"""WS7 eval harness: deterministic assertions over the 25-question acceptance set.

The harness has two modes:

* ``--dry-run`` validates ``questions.yaml`` and prints the category/mode counts
  without importing the agent or calling a model.
* live mode calls ``chat_app.agent.ask`` once per question (lazily imported, so
  this module stays importable without ``pi``/the agent stack) and applies the
  deterministic assertions below.

A question carries the answer contract: required substrings/regexes, forbidden
absence claims, an optional missing state, an optional claim label, and the
citation obligations for its surface.  Desk questions may require multi-ticker
citation coverage and an envelope ``coverage`` block; document questions assert
the pinned note's citations, ``analysis_note`` labels and ``not_in_document``
silence state.  A refused/error run only counts as correct when the question
explicitly expects a refusal.

Output: a pass table, per-category and per-mode summaries, the A1-A7 metrics
plus A12-A15 surface metrics, and a JSON artifact under ``idx_data/chat/eval/``
(never inside the eval fixtures).

``--snapshot`` regenerates ``chat-app/eval/supported-set.json`` from disk so
the frozen supported set cannot drift from ``paths.supported_periods()``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as _dt
import json
import math
import re
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml

from . import paths

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

CATEGORIES = ("probe", "user", "operational", "trap", "document", "wide_scope")
CATEGORY_TARGETS = {
    "probe": 8,
    "user": 3,
    "operational": 4,
    "trap": 4,
    "document": 3,
    "wide_scope": 3,
}
MODES = ("desk", "document")
TOTAL_QUESTIONS = 25
MISSING_STATES = (
    "not_yet_published",
    "not_disclosed",
    "not_captured",
    "unsupported",
    "not_in_document",
)
CLAIM_TYPES = ("fact", "management_stated", "analysis_note", "hypothesis")

REQUIRED_KEYS = (
    "id",
    "category",
    "mode",
    "question",
    "ticker",
    "period",
    "year",
    "must_include",
    "must_not_claim",
    "expect_state",
    "expect_label",
    "expect_citations",
    "notes",
)
OPTIONAL_KEYS = (
    "document_id",
    "selection",
    "expect_refusal",
    "expect_coverage",
)
CITATION_KEYS = ("required", "document", "tickers")

SUPPORTED_SET_PATH = paths.EVAL_DIR / "supported-set.json"
QUESTIONS_PATH = paths.EVAL_DIR / "questions.yaml"
DEFAULT_ARTIFACT_DIR = paths.IDX_DATA / "chat" / "eval"

# False-absence detector used for A2.  Deliberately narrow: it fires on the
# sentences the plan calls out (U1), not on a legitimate missing-state answer.
ABSENCE_RE = re.compile(
    r"\b(?:does not exist|doesn't exist|do not exist|don't exist|"
    r"no data available|no data exists)\b",
    re.IGNORECASE,
)
CAUSAL_RE = re.compile(
    r"\b(?:because|due to|driven by|reflecting|as a result|"
    r"karena|didorong|akibat)\b",
    re.IGNORECASE,
)
# A15: desk-issued advice/ranking.  Uppercase rating words are deliberate: a
# lowercase "hold" in "cash holdings" must not trip the audit.  A quoted note
# view is allowed only on an attributed analysis_note claim.
RATING_RE = re.compile(r"\b(?:BUY|HOLD|SELL)\b")
RANKING_RE = re.compile(
    r"\b(?:price target|top pick|best (?:stock|pick|idea|name)|"
    r"we recommend|investment recommendation|league table|"
    r"ranked (?:first|last|#?\s*1)|outperform(?:s|ed)? the sector)\b",
    re.IGNORECASE,
)
ATTRIBUTION_RE = re.compile(
    r"\b(?:note|analyst|analysts|research|report)\b", re.IGNORECASE
)
# A pattern is treated as a regex only when it carries regex-only syntax.
# Plain values such as "5.3" or "1,910" stay literal substrings.
_REGEX_HINT_RE = re.compile(r"[\\^$*+?{}\[\]|()]")

_THRESHOLDS = {
    "A6_median_cost_usd": {"desk": 0.010, "document": 0.020},
    "A7_p95_latency_s": 120.0,
    "A3_min_correct": 21,
}


class QuestionSchemaError(ValueError):
    """Raised when ``questions.yaml`` violates the eval contract."""


class BackendUnavailable(RuntimeError):
    """The live agent cannot be imported; abort instead of faking results."""


# ---------------------------------------------------------------------------
# Question loading and validation
# ---------------------------------------------------------------------------


def _resolve_path(raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    if path.exists():
        return path
    candidate = paths.PROJECT_ROOT / path
    return candidate if candidate.exists() else path


# ---------------------------------------------------------------------------
# Analysis-vault documents (the pinned-note questions must be real notes)
# ---------------------------------------------------------------------------

_DOC_META_RE = re.compile(
    r"^(?P<ticker>[A-Z]{4})\s+(?P<period>Q[1-4])\s+(?P<year>20\d{2})\b"
)


def document_slug(stem: str) -> str:
    """Slug rule used by the document index (MVP-PLAN §4.6)."""
    return re.sub(r"[^a-z0-9]+", "-", str(stem).lower()).strip("-")


def document_manifest(analysis_dir: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Map stable document id → metadata for every markdown note on disk.

    This is intentionally a local, read-only scan, not a dependency on the
    document index module: the eval must validate the fixture even when the
    agent stack is mid-edit.  IDs follow the same path-slug rule (directory
    parts included, short path hash only on collision) as ``analysis_docs``.
    """
    root = Path(analysis_dir) if analysis_dir else paths.ANALYSIS_DIR
    found: dict[str, dict[str, Any]] = {}
    if not root.exists():
        return found

    files = [
        path for path in sorted(root.rglob("*.md"))
        if not any(part.startswith(".") for part in path.relative_to(root).parts)
    ]
    rels = [path.relative_to(root).as_posix() for path in files]
    bases = {
        rel: "-".join(
            document_slug(part)
            for part in rel[:-3].split("/")
            if document_slug(part)
        )
        for rel in rels
    }
    counts = Counter(bases.values())
    for path, rel in zip(files, rels):
        base = bases[rel]
        slug = (
            base
            if counts[base] == 1
            else f"{base}-{_short_hash(rel)}"
        )
        match = _DOC_META_RE.match(path.stem)
        found[slug] = {
            "id": slug,
            "path": str(path.relative_to(paths.PROJECT_ROOT))
            if root == paths.ANALYSIS_DIR else str(path),
            "title": path.stem,
            "ticker": match.group("ticker") if match else None,
            "period": match.group("period") if match else None,
            "year": int(match.group("year")) if match else None,
        }
    return found


def _short_hash(value: str) -> str:
    import hashlib  # noqa: PLC0415 — tiny, local helper

    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:6]


def load_questions(path: str | Path = QUESTIONS_PATH) -> list[dict[str, Any]]:
    """Load and validate the question set (raises ``QuestionSchemaError``)."""
    resolved = _resolve_path(path)
    try:
        data = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise QuestionSchemaError(f"question file not found: {resolved}") from exc
    except yaml.YAMLError as exc:
        raise QuestionSchemaError(f"question file is not valid YAML: {exc}") from exc
    if not isinstance(data, list):
        raise QuestionSchemaError("questions.yaml must contain a list of questions")
    normalised = [_normalise_question(item, index) for index, item in enumerate(data, 1)]
    validate_questions(normalised)
    return normalised


def _normalise_question(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise QuestionSchemaError(f"question #{index} must be a mapping")
    item = dict(raw)
    item["year"] = _as_int(item.get("year"), f"question #{index} year")
    item["mode"] = str(item.get("mode") or "desk").strip().lower()
    item["must_include"] = _as_list(item.get("must_include"), f"question #{index} must_include")
    item["must_not_claim"] = _as_list(
        item.get("must_not_claim"), f"question #{index} must_not_claim"
    )
    item["expect_citations"] = _normalise_citations(
        item.get("expect_citations"), f"question #{index} expect_citations"
    )
    item["expect_coverage"] = bool(item.get("expect_coverage", False))
    item["expect_refusal"] = bool(item.get("expect_refusal", False))
    return item


def _normalise_citations(value: Any, label: str) -> dict[str, Any]:
    """Accept the documented mapping, plus bool/list shorthand for hand edits."""
    if value is None:
        return {"required": False, "document": False, "tickers": []}
    if isinstance(value, bool):
        return {"required": value, "document": False, "tickers": []}
    if isinstance(value, list):
        return {
            "required": True,
            "document": False,
            "tickers": [str(item).strip().upper() for item in value],
        }
    if not isinstance(value, dict):
        raise QuestionSchemaError(f"{label} must be a mapping, boolean, or list")
    unknown = sorted(set(value) - set(CITATION_KEYS))
    if unknown:
        raise QuestionSchemaError(f"{label}: unknown keys {unknown}")
    tickers = value.get("tickers") or []
    if isinstance(tickers, str):
        tickers = [tickers]
    if not isinstance(tickers, list):
        raise QuestionSchemaError(f"{label}.tickers must be a list")
    return {
        "required": bool(value.get("required", True)),
        "document": bool(value.get("document", False)),
        "tickers": [str(item).strip().upper() for item in tickers if str(item).strip()],
    }


def _as_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        raise QuestionSchemaError(f"{label} must be a list of strings")
    return [str(item) for item in value]


def _as_int(value: Any, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise QuestionSchemaError(f"{label} must be an integer, not a boolean")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise QuestionSchemaError(f"{label} must be an integer, got {value!r}") from exc


def _validate_selection(
    selection: Any, document: dict[str, Any], label: str
) -> list[str]:
    """A selection echo must be structurally valid and an exact quote."""
    errors: list[str] = []
    if not isinstance(selection, dict):
        return [f"{label}: selection must be a mapping"]
    section = selection.get("section")
    line = selection.get("line")
    quote = selection.get("quote")
    if not isinstance(section, str) or not section.strip():
        errors.append(f"{label}: selection.section must be a non-empty string")
    if isinstance(line, bool) or not isinstance(line, int) or line < 1:
        errors.append(f"{label}: selection.line must be a positive integer")
    if not isinstance(quote, str) or not quote.strip():
        errors.append(f"{label}: selection.quote must be a non-empty string")
    if isinstance(quote, str) and quote.strip():
        path = paths.PROJECT_ROOT / str(document.get("path") or "")
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"{label}: cannot read {document.get('path')}: {exc}")
        else:
            if quote not in content:
                errors.append(
                    f"{label}: selection.quote is not an exact substring of "
                    f"{document.get('path')}"
                )
    return errors


def validate_questions(
    questions: list[dict[str, Any]], *, strict: bool = True
) -> list[str]:
    """Validate the contract.  Returns errors, or raises when ``strict``."""
    errors: list[str] = []
    supported = paths.supported_periods()
    supported_tickers = set(supported)
    documents = document_manifest()
    seen_ids: set[str] = set()
    counts: Counter[str] = Counter()

    if strict and len(questions) != TOTAL_QUESTIONS:
        errors.append(
            f"expected {TOTAL_QUESTIONS} questions, found {len(questions)}"
        )

    for index, item in enumerate(questions, 1):
        label = str(item.get("id") or f"question #{index}")
        missing_keys = [key for key in REQUIRED_KEYS if key not in item]
        if missing_keys:
            errors.append(f"{label}: missing keys {missing_keys}")
            continue

        unknown_keys = sorted(set(item) - set(REQUIRED_KEYS) - set(OPTIONAL_KEYS))
        if unknown_keys:
            errors.append(f"{label}: unknown keys {unknown_keys}")

        question_id = item.get("id")
        if not isinstance(question_id, str) or not re.fullmatch(
            r"[a-z0-9_]+", question_id
        ):
            errors.append(f"{label}: id must be a lowercase slug")
        elif question_id in seen_ids:
            errors.append(f"{label}: duplicate id")
        else:
            seen_ids.add(question_id)

        category = item.get("category")
        if category not in CATEGORIES:
            errors.append(f"{label}: category must be one of {list(CATEGORIES)}")
        else:
            counts[str(category)] += 1

        mode = item.get("mode")
        if mode not in MODES:
            errors.append(f"{label}: mode must be one of {list(MODES)}")

        question = item.get("question")
        if not isinstance(question, str) or not question.strip():
            errors.append(f"{label}: question must be a non-empty string")

        ticker = item.get("ticker")
        if ticker is not None and not re.fullmatch(r"[A-Z]{4}", str(ticker)):
            errors.append(f"{label}: ticker must be a 4-letter uppercase code")

        period = item.get("period")
        if period is not None and period not in ("Q1", "Q2", "Q3", "Q4", "FY"):
            errors.append(f"{label}: period must be Q1-Q4/FY or null")

        year = item.get("year")
        if year is not None and not (2000 <= int(year) <= 2100):
            errors.append(f"{label}: year must be between 2000 and 2100")

        document_id = item.get("document_id")
        selection = item.get("selection")
        if mode == "desk":
            if document_id:
                errors.append(f"{label}: desk questions must not set document_id")
            if selection:
                errors.append(f"{label}: desk questions must not set selection")
        elif mode == "document":
            if not isinstance(document_id, str) or not re.fullmatch(
                r"[a-z0-9]+(?:-[a-z0-9]+)*", document_id
            ):
                errors.append(
                    f"{label}: document mode requires a slug document_id"
                )
            else:
                document = documents.get(document_id)
                if document is None:
                    errors.append(
                        f"{label}: document_id {document_id!r} is not a note on disk"
                    )
                else:
                    if ticker is not None and str(ticker) != document.get("ticker"):
                        errors.append(
                            f"{label}: ticker {ticker} does not match "
                            f"{document_id} ({document.get('ticker')})"
                        )
                    if period is not None and period != document.get("period"):
                        errors.append(
                            f"{label}: period {period} does not match "
                            f"{document_id} ({document.get('period')})"
                        )
                    if year is not None and int(year) != document.get("year"):
                        errors.append(
                            f"{label}: year {year} does not match "
                            f"{document_id} ({document.get('year')})"
                        )
                    if selection is not None:
                        errors.extend(
                            _validate_selection(
                                selection, document, label
                            )
                        )

        for field in ("must_include", "must_not_claim"):
            values = item.get(field) or []
            if not isinstance(values, list):
                errors.append(f"{label}: {field} must be a list")
                continue
            for value in values:
                if not isinstance(value, str) or not value.strip():
                    errors.append(f"{label}: {field} entries must be non-empty strings")
                    continue
                if _REGEX_HINT_RE.search(value):
                    try:
                        re.compile(value)
                    except re.error as exc:
                        errors.append(f"{label}: invalid regex {value!r}: {exc}")

        expect_state = item.get("expect_state")
        if expect_state is not None and expect_state not in MISSING_STATES:
            errors.append(
                f"{label}: expect_state must be null or one of {list(MISSING_STATES)}"
            )
        if mode == "document" and expect_state not in (None, "not_in_document"):
            errors.append(
                f"{label}: document mode may only expect_state not_in_document "
                "(note silence is never not_disclosed)"
            )
        if mode == "desk" and expect_state == "not_in_document":
            errors.append(
                f"{label}: not_in_document is a document-mode state"
            )

        expect_label = item.get("expect_label")
        if expect_label is not None and expect_label not in CLAIM_TYPES:
            errors.append(
                f"{label}: expect_label must be null or one of {list(CLAIM_TYPES)}"
            )

        citations = item.get("expect_citations")
        if not isinstance(citations, dict):
            errors.append(f"{label}: expect_citations must be a mapping")
        else:
            unknown = sorted(set(citations) - set(CITATION_KEYS))
            if unknown:
                errors.append(f"{label}: expect_citations unknown keys {unknown}")
            if not isinstance(citations.get("required"), bool):
                errors.append(f"{label}: expect_citations.required must be a boolean")
            if not isinstance(citations.get("document"), bool):
                errors.append(f"{label}: expect_citations.document must be a boolean")
            tickers = citations.get("tickers")
            if not isinstance(tickers, list) or not all(
                isinstance(item, str) for item in tickers or []
            ):
                errors.append(f"{label}: expect_citations.tickers must be a list")
            else:
                for cited_ticker in tickers:
                    if not re.fullmatch(r"[A-Z]{4}", cited_ticker):
                        errors.append(
                            f"{label}: expect_citations ticker {cited_ticker!r} is invalid"
                        )
                    elif cited_ticker not in supported_tickers:
                        errors.append(
                            f"{label}: expect_citations ticker {cited_ticker} is not supported"
                        )
                if mode == "desk" and len(
                    [cited_ticker for cited_ticker in tickers if cited_ticker in supported_tickers]
                ) >= 2 and not citations.get("required"):
                    errors.append(
                        f"{label}: multi-ticker citations require required: true"
                    )
            if mode == "document":
                if citations.get("required") is not True:
                    errors.append(
                        f"{label}: document questions must require citations"
                    )
                if citations.get("document") is not True:
                    errors.append(
                        f"{label}: document questions must require a document citation"
                    )
                if citations.get("tickers"):
                    errors.append(
                        f"{label}: document questions cite the pinned note, not tickers"
                    )
            elif citations.get("document"):
                errors.append(
                    f"{label}: expect_citations.document is document-mode only"
                )

        expect_coverage = item.get("expect_coverage")
        if expect_coverage is not None and not isinstance(expect_coverage, bool):
            errors.append(f"{label}: expect_coverage must be a boolean")
        if expect_coverage and mode != "desk":
            errors.append(f"{label}: coverage blocks are desk-mode only")
        if category == "wide_scope":
            if mode != "desk":
                errors.append(f"{label}: wide-scope questions run in desk mode")
            if not expect_coverage:
                errors.append(f"{label}: wide-scope questions require expect_coverage")
            if citations and len(citations.get("tickers") or []) < 2:
                errors.append(
                    f"{label}: wide-scope questions require at least two cited tickers"
                )

        if "expect_refusal" in item and not isinstance(item["expect_refusal"], bool):
            errors.append(f"{label}: expect_refusal must be a boolean")

        if not isinstance(item.get("notes"), str):
            errors.append(f"{label}: notes must be a string")

        # Supported-set consistency.  Unsupported traps are the explicit
        # exception and must declare both the state and the refusal.
        if ticker is not None:
            ticker = str(ticker)
            if ticker not in supported_tickers:
                if not (
                    item.get("expect_state") == "unsupported"
                    and item.get("expect_refusal") is True
                ):
                    errors.append(
                        f"{label}: {ticker} is not in the supported set; "
                        "mark expect_state: unsupported and expect_refusal: true"
                    )
            elif period is not None and year is not None:
                if not paths.is_supported(ticker, period, year):
                    errors.append(
                        f"{label}: {ticker} {period} {year} is not a supported period"
                    )

    if strict:
        for category, target in CATEGORY_TARGETS.items():
            if counts.get(category, 0) != target:
                errors.append(
                    f"category {category}: expected {target}, found {counts.get(category, 0)}"
                )

    if errors and strict:
        raise QuestionSchemaError("; ".join(errors))
    return errors


def category_counts(questions: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(item.get("category")) for item in questions)
    return {category: counts.get(category, 0) for category in CATEGORIES}


def mode_counts(questions: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(item.get("mode")) for item in questions)
    return {mode: counts.get(mode, 0) for mode in MODES}


# ---------------------------------------------------------------------------
# Supported-set snapshot (derived from disk, never hand-written)
# ---------------------------------------------------------------------------


def build_snapshot() -> dict[str, Any]:
    """Build the frozen supported-set manifest from artifacts on disk."""
    supported = paths.supported_periods()
    tickers = sorted(supported)
    periods: list[dict[str, Any]] = []
    for ticker in tickers:
        for entry in supported[ticker]:
            periods.append(
                {
                    "ticker": ticker,
                    "period": entry["period"],
                    "year": entry["year"],
                    "fact": entry["fact_path"],
                    "has_ratios": bool(entry.get("has_ratios")),
                    "has_operational": bool(entry.get("has_operational")),
                }
            )

    ratio_periods = [item for item in periods if item["has_ratios"]]
    operational_periods = [item for item in periods if item["has_operational"]]
    notes = _notes_coverage(tickers)

    counts = {
        "tickers": len(tickers),
        "periods": len(periods),
        "facts": len(periods),
        "ratios": len(ratio_periods),
        "operational_periods": len(operational_periods),
        "notes_chunks": notes.get("chunks"),
        "notes_tickers": notes.get("tickers"),
        "supported_notes_tickers": notes.get("supported_tickers"),
    }

    return {
        "as_of": _dt.date.today().isoformat(),
        "source": {
            "facts": str(paths.FACTS_DIR.relative_to(paths.PROJECT_ROOT)),
            "notes": str(paths.NOTES_DB.relative_to(paths.PROJECT_ROOT)),
            "generated_by": "python -m chat_app.eval --snapshot",
        },
        "tickers": tickers,
        "counts": counts,
        "layer_coverage": {
            "facts": {
                "periods": len(periods),
                "tickers": len({item["ticker"] for item in periods}),
            },
            "ratios": {
                "periods": len(ratio_periods),
                "tickers": len({item["ticker"] for item in ratio_periods}),
            },
            "operational": {
                "periods": len(operational_periods),
                "tickers": len({item["ticker"] for item in operational_periods}),
                "items": [
                    f"{item['ticker']} {item['period']} {item['year']}"
                    for item in operational_periods
                ],
            },
            "notes": notes,
        },
        "periods": periods,
    }


def _notes_coverage(tickers: list[str]) -> dict[str, Any]:
    """Read-only notes coverage; a missing/locked DB is a gap, not a crash."""
    if not paths.NOTES_DB.exists():
        return {"available": False, "error": "notes_rag.duckdb not found"}
    try:
        import duckdb  # noqa: PLC0415 — optional at import time

        connection = duckdb.connect(str(paths.NOTES_DB), read_only=True)
        try:
            rows = connection.execute("SELECT tickers FROM chunks").fetchall()
        finally:
            connection.close()
    except Exception as exc:  # noqa: BLE001 — snapshot must still be writable
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}

    supported = set(tickers)
    by_ticker: Counter[str] = Counter()
    supported_chunks = 0
    for (label,) in rows:
        members = [part.strip() for part in str(label or "").split(",") if part.strip()]
        for member in members:
            by_ticker[member] += 1
        if any(member in supported for member in members):
            supported_chunks += 1

    present_supported = {ticker: by_ticker[ticker] for ticker in sorted(supported)}
    return {
        "available": True,
        "chunks": len(rows),
        "tickers": len(by_ticker),
        "supported_tickers": sum(1 for count in present_supported.values() if count),
        "supported_chunks": supported_chunks,
        "missing_supported_tickers": [
            ticker for ticker, count in present_supported.items() if not count
        ],
        "by_ticker": dict(sorted(by_ticker.items())),
    }


def write_snapshot(path: str | Path | None = None) -> Path:
    """Write the supported-set snapshot (default: ``eval/supported-set.json``)."""
    target = Path(path) if path else SUPPORTED_SET_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(build_snapshot(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return target


# ---------------------------------------------------------------------------
# Deterministic assertions
# ---------------------------------------------------------------------------


def _is_regex(pattern: str) -> bool:
    return bool(_REGEX_HINT_RE.search(pattern))


def _match(pattern: str, text: str) -> bool:
    if _is_regex(pattern):
        try:
            return re.search(pattern, text, re.IGNORECASE) is not None
        except re.error:
            pass
    return pattern.casefold() in text.casefold()


def _answer_text(result: dict[str, Any]) -> str:
    envelope = result.get("envelope") or {}
    parts = [str(envelope.get("answer") or "")]
    for claim in envelope.get("claims") or []:
        if isinstance(claim, dict):
            parts.append(str(claim.get("text") or ""))
    return "\n".join(parts)


def _refusal_text(result: dict[str, Any]) -> str:
    envelope = result.get("envelope") or {}
    payload = {
        "status": result.get("status"),
        "error": result.get("error"),
        "missing": envelope.get("missing") or [],
        "verification": result.get("verification") or {},
    }
    return json.dumps(payload, default=str, ensure_ascii=False)


def _claim_list(result: dict[str, Any]) -> list[dict[str, Any]]:
    envelope = result.get("envelope") or {}
    return [
        claim for claim in envelope.get("claims") or [] if isinstance(claim, dict)
    ]


def _citations(claim: dict[str, Any]) -> list[Any]:
    citations = claim.get("citations")
    return citations if isinstance(citations, list) else []


def _citation_tickers(citation: dict[str, Any]) -> set[str]:
    """Tickers named by a citation (primary or matrix)."""
    candidates: list[Any] = [citation.get("ticker")]
    for key in ("tickers", "document", "doc"):
        value = citation.get(key)
        if isinstance(value, list):
            candidates.extend(value)
        elif value is not None:
            candidates.append(value)
    found: set[str] = set()
    for value in candidates:
        if not value:
            continue
        for part in re.split(r"[^A-Za-z0-9]+", str(value)):
            if re.fullmatch(r"[A-Za-z]{4}", part):
                found.add(part.upper())
    return found


def _trace_tickers(trace_id: str | None) -> set[str]:
    """Tickers covered by a compare_supported call in the stored trace.

    A bounded matrix call is a citation even when the prose reports the cell
    under a separate claim. The verifier's coverage audit counts these tickers;
    the eval must count them too.
    """
    if not trace_id:
        return set()
    try:
        trace = json.loads(paths.trace_path(str(trace_id)).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()

    found: set[str] = set()

    def add(values: Any) -> None:
        for value in values if isinstance(values, list) else []:
            text = str(value or "").strip().upper()
            if re.fullmatch(r"[A-Z]{4}", text):
                found.add(text)

    for entry in trace.get("tool_trace") or []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("tool_name") or entry.get("name") or entry.get("tool")
        if str(name or "") != "compare_supported":
            continue
        arguments = entry.get("arguments") or entry.get("args") or {}
        if isinstance(arguments, dict):
            add(arguments.get("tickers"))
        payload: Any = entry.get("result")
        if isinstance(payload, dict):
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            add(data.get("columns"))
            for block in payload.get("content") or []:
                if not isinstance(block, dict) or block.get("type") != "text":
                    continue
                try:
                    parsed = json.loads(block.get("text") or "")
                except (json.JSONDecodeError, TypeError):
                    continue
                data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
                add(data.get("columns"))
    return found


def _document_citation_fields(citation: dict[str, Any]) -> dict[str, Any] | None:
    """Return the complete document-citation payload, or None if incomplete."""
    document = citation.get("document") or citation.get("doc")
    section = citation.get("section")
    line = citation.get("line")
    quote = citation.get("quote")
    if not isinstance(document, str) or not document.strip():
        return None
    if not isinstance(section, str) or not section.strip():
        return None
    if isinstance(line, bool) or not isinstance(line, int) or line < 1:
        return None
    if not isinstance(quote, str) or not quote.strip():
        return None
    return {
        "document": document,
        "section": section,
        "line": line,
        "quote": quote,
    }


def _document_citations(claim: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for citation in _citations(claim):
        if not isinstance(citation, dict):
            continue
        fields = _document_citation_fields(citation)
        if fields:
            found.append(fields)
    return found


def _state_present(result: dict[str, Any], state: str) -> bool:
    envelope = result.get("envelope") or {}
    missing_entries = [
        missing for missing in envelope.get("missing") or [] if isinstance(missing, dict)
    ]
    if any(str(missing.get("state") or "") == state for missing in missing_entries):
        return True
    # Note silence is a structured state.  Prose such as "the note does not
    # cover X" is not evidence without the missing[] entry.
    if state == "not_in_document":
        return False
    text = _answer_text(result).casefold()
    if state.casefold() in text or state.replace("_", " ") in text:
        return True
    if state == "unsupported":
        blob = json.dumps(result, default=str, ensure_ascii=False).casefold()
        return any(
            marker in blob
            for marker in (
                "unsupported",
                "not in the supported set",
                "not supported",
                "supported_tickers",
            )
        )
    if str(result.get("status") or "") == state:
        return True
    return state in json.dumps(missing_entries, default=str).casefold()


def _conflicting_silence(result: dict[str, Any]) -> str | None:
    """Document mode must never turn note silence into a filing-style state."""
    envelope = result.get("envelope") or {}
    for missing in envelope.get("missing") or []:
        if not isinstance(missing, dict):
            continue
        if str(missing.get("state") or "") in ("not_disclosed", "not_captured"):
            return str(missing.get("state"))
    return None


def _coverage_failures(
    result: dict[str, Any], expected_tickers: list[str]
) -> list[str]:
    failures: list[str] = []
    envelope = result.get("envelope") or {}
    coverage = envelope.get("coverage")
    if not isinstance(coverage, dict) or not coverage:
        # The agent also exposes a normalized response-level coverage block
        # (fallback when the envelope omits it); either is UI-visible evidence.
        coverage = result.get("coverage")
    if not isinstance(coverage, dict) or not coverage:
        return ["coverage:block missing"]
    supported = coverage.get("supported_tickers")
    if isinstance(supported, bool) or not isinstance(supported, int) or supported <= 0:
        failures.append("coverage:supported_tickers missing")
    if not (
        isinstance(coverage.get("caveat"), str) and coverage.get("caveat").strip()
    ) and not (isinstance(coverage.get("scope"), str) and coverage.get("scope").strip()):
        failures.append("coverage:scope/caveat missing")
    compared = coverage.get("compared_tickers") or []
    if not isinstance(compared, list):
        failures.append("coverage:compared_tickers must be a list")
    else:
        compared_set = {str(item).strip().upper() for item in compared}
        missing = [ticker for ticker in expected_tickers if ticker not in compared_set]
        if missing:
            failures.append(f"coverage:compared_tickers missing {missing}")
    return failures


def _advice_violations(result: dict[str, Any]) -> list[str]:
    """A15: no desk-issued rating/ranking; attributed note views are allowed."""
    violations: list[str] = []
    claims = _claim_list(result)
    answer_text = _answer_text(result)
    allowed_text: list[str] = []

    for index, claim in enumerate(claims, 1):
        text = str(claim.get("text") or "")
        if not (RATING_RE.search(text) or RANKING_RE.search(text)):
            continue
        claim_type = str(claim.get("type") or "")
        attributed = bool(ATTRIBUTION_RE.search(text))
        has_note_citation = bool(_document_citations(claim))
        if claim_type == "analysis_note" and attributed and has_note_citation:
            allowed_text.append(text)
            continue
        violations.append(f"advice:claim {index} ({claim_type or 'unlabelled'})")

    for sentence in re.split(r"(?<=[.!?])\s+", answer_text):
        if not (RATING_RE.search(sentence) or RANKING_RE.search(sentence)):
            continue
        if any(sentence.strip() in allowed for allowed in allowed_text):
            continue
        if any(sentence.strip() in str(claim.get("text") or "") and
               str(claim.get("type") or "") == "analysis_note" and
               ATTRIBUTION_RE.search(sentence) and _document_citations(claim)
               for claim in claims):
            continue
        if _attributed_note_rating(claims, sentence):
            continue
        violations.append(f"advice:answer {_truncate(sentence, 48)}")
    return violations


def _attributed_note_rating(
    claims: list[dict[str, Any]], sentence: str
) -> bool:
    """Allow a rating sentence when a cited analysis_note carries the same view.

    Answers often paraphrase a note's rating in prose while the exact
    ``analysis_note`` claim is shorter. The verifier already checks attribution
    and the document citation; this keeps the eval from double-refusing a
    correctly attributed note view.
    """
    for claim in claims:
        if str(claim.get("type") or "") != "analysis_note":
            continue
        if not _document_citations(claim):
            continue
        text = str(claim.get("text") or "")
        quotes = " ".join(
            str(citation.get("quote") or "")
            for citation in (claim.get("citations") or [])
            if isinstance(citation, dict)
        )
        if not (
            RATING_RE.search(text) or RATING_RE.search(quotes)
            or RANKING_RE.search(text) or RANKING_RE.search(quotes)
        ):
            continue
        if ATTRIBUTION_RE.search(sentence) or ATTRIBUTION_RE.search(text):
            return True
    return False


def check_result(item: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Apply every deterministic assertion to one agent result."""
    failures: list[str] = []
    status = str(result.get("status") or "error")
    verification = result.get("verification") or {}
    verification_ok = bool(verification.get("ok"))
    expect_refusal = bool(item.get("expect_refusal"))
    expect_state = item.get("expect_state")
    mode = str(item.get("mode") or "desk")
    document_id = item.get("document_id")
    claims = _claim_list(result)
    answer_text = _answer_text(result)
    citations = item.get("expect_citations") or {}
    # A refused/error run has no usable prose answer; the structured refusal
    # payload is the evidence the assertions must read.
    if status in ("refused", "error") or not answer_text.strip():
        searchable = "\n".join([answer_text, _refusal_text(result)])
    else:
        searchable = answer_text

    if status == "answered":
        if not verification_ok:
            failures.append("verification.ok is false")
        if str(result.get("mode") or "") != mode:
            failures.append(
                f"mode:{result.get('mode')!r} != {mode!r}"
            )
        if mode == "document":
            document = result.get("document")
            if not isinstance(document, dict) or str(
                document.get("id") or ""
            ) != str(document_id):
                failures.append("document:metadata not echoed")
        elif result.get("document"):
            failures.append("document:desk answer echoes a document")
    elif status in ("refused", "error"):
        if not expect_refusal:
            failures.append(f"status={status} but no refusal expected")
        elif status == "error" and expect_state != "unsupported":
            failures.append("status=error")
    else:
        failures.append(f"unexpected status={status}")

    for pattern in item.get("must_include") or []:
        if not _match(str(pattern), searchable):
            failures.append(f"missing:{pattern}")

    for pattern in item.get("must_not_claim") or []:
        if _match(str(pattern), answer_text):
            failures.append(f"forbidden:{pattern}")

    if expect_state and not _state_present(result, str(expect_state)):
        failures.append(f"state:{expect_state} not found")

    if expect_state == "not_in_document":
        conflicting = _conflicting_silence(result)
        if conflicting:
            failures.append(f"state:not_in_document conflated with {conflicting}")

    if item.get("expect_label"):
        label = str(item["expect_label"])
        matching = [
            claim
            for claim in claims
            if str(claim.get("type") or "") == label and _citations(claim)
        ]
        if label == "analysis_note":
            matching = [
                claim for claim in matching if _document_citations(claim)
            ]
        if not matching:
            failures.append(f"label:{label} missing or uncited")

    if citations.get("required"):
        if not claims:
            failures.append("citation:no claims")
        else:
            for index, claim in enumerate(claims, 1):
                claim_type = str(claim.get("type") or "")
                if claim_type != "hypothesis" and not _citations(claim):
                    failures.append(f"citation:claim {index} has none")
            if not any(_citations(claim) for claim in claims):
                failures.append("citation:envelope has none")

    cited_tickers: set[str] = set()
    note_claims = 0
    for index, claim in enumerate(claims, 1):
        claim_type = str(claim.get("type") or "")
        document_citations = _document_citations(claim)
        if document_citations:
            note_claims += 1
            if claim_type not in ("analysis_note", "hypothesis"):
                failures.append(
                    f"note-label:claim {index} is {claim_type or 'unlabelled'}"
                )
            if document_id:
                for fields in document_citations:
                    if fields["document"] != document_id:
                        failures.append(
                            f"document:claim {index} cites {fields['document']} "
                            f"not {document_id}"
                        )
        if claim_type == "analysis_note" and not document_citations:
            failures.append(
                f"note-citation:claim {index} analysis_note lacks document/section/line/quote"
            )
        for citation in _citations(claim):
            if isinstance(citation, dict):
                cited_tickers.update(_citation_tickers(citation))

    cited_tickers.update(_trace_tickers(result.get("trace_id")))

    if citations.get("document"):
        if not note_claims and status == "answered":
            failures.append("citation:no document citation")
        if not any(
            str(claim.get("type") or "") == "analysis_note"
            and _document_citations(claim)
            for claim in claims
        ) and status == "answered":
            failures.append("citation:no analysis_note document claim")

    if citations.get("tickers"):
        missing = [
            ticker
            for ticker in citations["tickers"]
            if ticker not in cited_tickers
        ]
        if missing:
            failures.append(f"citation:tickers not cited {missing}")

    if item.get("expect_coverage"):
        failures.extend(_coverage_failures(result, list(citations.get("tickers") or [])))

    advice = _advice_violations(result)
    failures.extend(advice)

    return {
        "passed": not failures,
        "reasons": failures,
        "status": status,
        "verification_ok": verification_ok,
        "claims": claims,
        "answer_text": answer_text,
        "note_claims": note_claims,
        "cited_tickers": sorted(cited_tickers),
        "advice_violations": advice,
        "coverage": (result.get("envelope") or {}).get("coverage")
        or result.get("coverage"),
    }


# ---------------------------------------------------------------------------
# Live execution
# ---------------------------------------------------------------------------


def _ensure_backend() -> None:
    """Fail clearly when the live agent cannot be imported.

    A missing backend must abort the run.  Emitting a table of synthetic
    ``error`` rows would look like a real acceptance run and could hide a
    broken environment.
    """
    try:
        from .agent import ask  # noqa: PLC0415 — lazy seam
    except Exception as exc:  # noqa: BLE001 — any import failure is fatal preflight
        raise BackendUnavailable(
            "agent backend unavailable: "
            f"{type(exc).__name__}: {exc}. Run from the repo root with "
            "`PYTHONPATH=chat-app` and install the chat-app dependencies; "
            "no eval results were recorded."
        ) from exc
    if not callable(ask):
        raise BackendUnavailable("agent backend unavailable: ask is not callable")


def _call_agent(
    question: str,
    *,
    ticker: str | None = None,
    period: str | None = None,
    year: int | None = None,
    history: list[dict] | None = None,
    mode: str = "desk",
    document_id: str | None = None,
    selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Lazy seam: imported only in live mode, and monkeypatchable in tests."""
    try:
        from .agent import ask  # noqa: PLC0415 — keeps eval importable without pi
    except Exception as exc:  # noqa: BLE001
        raise BackendUnavailable(
            f"agent backend unavailable: {type(exc).__name__}: {exc}"
        ) from exc

    return ask(
        question,
        ticker=ticker,
        period=period,
        year=year,
        history=history,
        mode=mode,
        document_id=document_id,
        selection=selection,
    )


def _run_one(
    item: dict[str, Any],
    agent_fn: Callable[..., dict[str, Any]] = _call_agent,
) -> dict[str, Any]:
    started = time.perf_counter()
    mode = str(item.get("mode") or "desk")
    try:
        result = agent_fn(
            item["question"],
            ticker=item.get("ticker"),
            period=item.get("period"),
            year=item.get("year"),
            mode=mode,
            document_id=item.get("document_id") if mode == "document" else None,
            selection=item.get("selection") if mode == "document" else None,
        )
        if not isinstance(result, dict):
            raise TypeError(f"agent.ask returned {type(result).__name__}, expected dict")
    except BackendUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 — one bad run must not abort the suite
        elapsed = time.perf_counter() - started
        result = {
            "status": "error",
            "envelope": None,
            "verification": {"ok": False, "violations": []},
            "usage": {"cost_usd": 0.0},
            "duration_s": elapsed,
            "error": f"{type(exc).__name__}: {exc}",
            "mode": mode,
        }
    elapsed = time.perf_counter() - started
    check = check_result(item, result)
    usage = result.get("usage") or {}
    try:
        cost = float(usage.get("cost_usd") or 0.0)
    except (TypeError, ValueError):
        cost = 0.0
    try:
        duration = float(result.get("duration_s") or elapsed)
    except (TypeError, ValueError):
        duration = elapsed

    envelope = result.get("envelope") or {}
    missing = envelope.get("missing") or []
    forbidden_hits = [
        str(pattern)
        for pattern in item.get("must_not_claim") or []
        if _match(str(pattern), check["answer_text"])
    ]
    return {
        "id": item["id"],
        "category": item["category"],
        "mode": mode,
        "document_id": item.get("document_id"),
        "question": item["question"],
        "ticker": item.get("ticker"),
        "period": item.get("period"),
        "year": item.get("year"),
        "status": result.get("status"),
        "passed": bool(check["passed"]),
        "reasons": check["reasons"],
        "forbidden_hits": forbidden_hits,
        "cost_usd": cost,
        "duration_s": round(duration, 3),
        "trace_id": result.get("trace_id"),
        "verification": result.get("verification"),
        "envelope": result.get("envelope"),
        "missing": missing,
        "coverage": check.get("coverage"),
        "note_claims": check.get("note_claims", 0),
        "cited_tickers": check.get("cited_tickers", []),
        "advice_violations": check.get("advice_violations", []),
        "tool_calls": result.get("tool_calls"),
        "error": result.get("error"),
        "agent_result": result,
    }


def select_questions(
    questions: list[dict[str, Any]],
    *,
    limit: int | None = None,
    ids: list[str] | None = None,
    mode: str | None = None,
) -> list[dict[str, Any]]:
    selected = list(questions)
    if mode:
        if mode not in MODES:
            raise QuestionSchemaError(f"unknown mode {mode!r}; expected {list(MODES)}")
        selected = [item for item in selected if item.get("mode") == mode]
    if ids:
        wanted = {part.strip() for raw in ids for part in str(raw).split(",") if part.strip()}
        selected = [item for item in selected if item["id"] in wanted]
        missing = wanted - {item["id"] for item in selected}
        if missing:
            raise QuestionSchemaError(f"unknown question id(s): {sorted(missing)}")
    if limit is not None:
        if limit <= 0:
            raise QuestionSchemaError("--limit must be >= 1")
        selected = selected[:limit]
    if not selected:
        raise QuestionSchemaError("no questions selected")
    return selected


def run_eval(
    questions: list[dict[str, Any]],
    *,
    limit: int | None = None,
    ids: list[str] | None = None,
    mode: str | None = None,
    concurrency: int = 1,
    agent_fn: Callable[..., dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run the selected questions and return (results, metrics)."""
    selected = select_questions(questions, limit=limit, ids=ids, mode=mode)
    if agent_fn is None:
        _ensure_backend()
        worker = _call_agent
    else:
        worker = agent_fn
    workers = max(1, min(int(concurrency or 1), 2))

    if workers == 1:
        results = [_run_one(item, worker) for item in selected]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda item: _run_one(item, worker), selected))

    metrics = compute_metrics(results, selected)
    return results, metrics


# ---------------------------------------------------------------------------
# Metrics and thresholds
# ---------------------------------------------------------------------------


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _mode_stats(results: list[dict[str, Any]], mode: str) -> dict[str, Any]:
    subset = [result for result in results if result.get("mode") == mode]
    costs = [float(result.get("cost_usd") or 0.0) for result in subset]
    latencies = [float(result.get("duration_s") or 0.0) for result in subset]
    return {
        "total": len(subset),
        "passed": sum(1 for result in subset if result["passed"]),
        "answered": sum(1 for result in subset if result.get("status") == "answered"),
        "median_cost_usd": round(statistics.median(costs), 6) if costs else 0.0,
        "total_cost_usd": round(sum(costs), 6),
        "p95_latency_s": round(_percentile(latencies, 0.95), 3),
        "median_latency_s": round(statistics.median(latencies), 3) if latencies else 0.0,
    }


def compute_metrics(
    results: list[dict[str, Any]], questions: list[dict[str, Any]]
) -> dict[str, Any]:
    total = len(results)
    by_id = {item["id"]: item for item in questions}
    correct = sum(1 for result in results if result["passed"])
    user_results = [result for result in results if result["category"] == "user"]
    user_passed = sum(1 for result in user_results if result["passed"])
    document_results = [
        result for result in results if result["category"] == "document"
    ]
    document_passed = sum(1 for result in document_results if result["passed"])
    wide_results = [
        result for result in results if result["category"] == "wide_scope"
    ]
    wide_passed = sum(1 for result in wide_results if result["passed"])

    audit_violations = 0
    false_absence_claims = 0
    citation_expected = 0
    citation_met = 0
    causal_claims = 0
    causal_labelled = 0
    note_mislabels = 0
    silence_expected = 0
    silence_ok = 0
    advice_violations = 0

    for result in results:
        item = by_id[result["id"]]
        status = str(result.get("status") or "")
        verification = result.get("verification") or {}
        if status == "answered":
            violations = verification.get("violations") or []
            audit_violations += len(violations)

        answer_text = str(result.get("answer_text") or "")
        if not answer_text:
            answer_text = _answer_text(result.get("agent_result") or {})
        forbidden = result.get("forbidden_hits") or []
        absence = bool(ABSENCE_RE.search(answer_text))
        has_state = any(
            isinstance(missing, dict)
            and missing.get("state") in MISSING_STATES
            for missing in result.get("missing") or []
        )
        if forbidden:
            false_absence_claims += 1
        elif (
            absence
            and not has_state
            and not item.get("expect_refusal")
            and not item.get("expect_state")
        ):
            false_absence_claims += 1

        if (item.get("expect_citations") or {}).get("required") and status == "answered":
            citation_expected += 1
            claims = result.get("claims") or _claim_list(result.get("agent_result") or {})
            if any(_citations(claim) for claim in claims):
                citation_met += 1

        claims = result.get("claims") or _claim_list(result.get("agent_result") or {})
        for claim in claims:
            text = str(claim.get("text") or "")
            if CAUSAL_RE.search(text):
                causal_claims += 1
                if claim.get("type") in CLAIM_TYPES:
                    causal_labelled += 1
            if _document_citations(claim) and str(claim.get("type") or "") in (
                "fact",
                "management_stated",
            ):
                note_mislabels += 1

        if item.get("expect_state") == "not_in_document":
            silence_expected += 1
            if result["passed"]:
                silence_ok += 1

        advice_violations += len(result.get("advice_violations") or [])

    required_correct = (
        _THRESHOLDS["A3_min_correct"]
        if total >= TOTAL_QUESTIONS
        else max(1, math.ceil(0.85 * total))
    )
    all_user_passed = user_passed == len(user_results) if user_results else True
    all_document_passed = (
        document_passed == len(document_results) if document_results else True
    )
    by_mode = {mode: _mode_stats(results, mode) for mode in MODES}

    median_cost = (
        statistics.median(
            [float(result.get("cost_usd") or 0.0) for result in results]
        )
        if results
        else 0.0
    )
    p95_latency = _percentile(
        [float(result.get("duration_s") or 0.0) for result in results], 0.95
    )
    causal_pct = (
        100.0 * causal_labelled / causal_claims if causal_claims else 100.0
    )
    citation_pct = (
        100.0 * citation_met / citation_expected
        if citation_expected
        else 100.0
    )
    wide_pct = 100.0 * wide_passed / len(wide_results) if wide_results else 100.0
    silence_pct = (
        100.0 * silence_ok / silence_expected if silence_expected else 100.0
    )

    cost_thresholds = _THRESHOLDS["A6_median_cost_usd"]
    cost_pass = {
        mode: (
            by_mode[mode]["median_cost_usd"] <= threshold
            if by_mode[mode]["total"]
            else True
        )
        for mode, threshold in cost_thresholds.items()
    }

    thresholds = {
        "A1_audit_violations_zero": {
            "value": audit_violations,
            "threshold": 0,
            "passed": audit_violations == 0,
        },
        "A2_false_absence_claims_zero": {
            "value": false_absence_claims,
            "threshold": 0,
            "passed": false_absence_claims == 0,
        },
        "A3_correct": {
            "value": correct,
            "total": total,
            "threshold": required_correct,
            "user_passed": user_passed,
            "user_total": len(user_results),
            "document_passed": document_passed,
            "document_total": len(document_results),
            "passed": (
                correct >= required_correct
                and all_user_passed
                and all_document_passed
            ),
        },
        "A4_causal_labels": {
            "value_pct": round(causal_pct, 2),
            "threshold_pct": 100.0,
            "passed": causal_pct >= 100.0,
        },
        "A5_citation_coverage": {
            "value_pct": round(citation_pct, 2),
            "threshold_pct": 100.0,
            "passed": citation_pct >= 100.0,
        },
        "A6_median_cost_usd": {
            "value": round(median_cost, 6),
            "by_mode": {
                mode: {
                    "value": by_mode[mode]["median_cost_usd"],
                    "threshold": threshold,
                    "passed": cost_pass[mode],
                }
                for mode, threshold in cost_thresholds.items()
            },
            "threshold": cost_thresholds["desk"],
            "passed": all(cost_pass.values()),
        },
        "A7_p95_latency_s": {
            "value": round(p95_latency, 3),
            "by_mode": {
                mode: round(by_mode[mode]["p95_latency_s"], 3) for mode in MODES
            },
            "threshold": _THRESHOLDS["A7_p95_latency_s"],
            "passed": p95_latency <= _THRESHOLDS["A7_p95_latency_s"],
        },
        "A12_note_number_as_fact_zero": {
            "value": note_mislabels,
            "threshold": 0,
            "passed": note_mislabels == 0,
        },
        "A13_not_in_document_honesty": {
            "value_pct": round(silence_pct, 2),
            "threshold_pct": 100.0,
            "expected": silence_expected,
            "passed": silence_pct >= 100.0,
        },
        "A14_wide_scope_citations": {
            "value_pct": round(wide_pct, 2),
            "threshold_pct": 100.0,
            "total": len(wide_results),
            "passed": wide_pct >= 100.0,
        },
        "A15_no_advice_zero": {
            "value": advice_violations,
            "threshold": 0,
            "passed": advice_violations == 0,
        },
    }
    return {
        "total": total,
        "correct": correct,
        "by_category": {
            category: {
                "total": sum(1 for r in results if r["category"] == category),
                "passed": sum(
                    1 for r in results if r["category"] == category and r["passed"]
                ),
            }
            for category in CATEGORIES
        },
        "by_mode": by_mode,
        "user_passed": user_passed,
        "user_total": len(user_results),
        "document_passed": document_passed,
        "document_total": len(document_results),
        "wide_passed": wide_passed,
        "wide_total": len(wide_results),
        "audit_violations": audit_violations,
        "false_absence_claims": false_absence_claims,
        "causal_claims": causal_claims,
        "causal_labelled": causal_labelled,
        "citation_expected": citation_expected,
        "citation_met": citation_met,
        "advice_violations": advice_violations,
        "median_cost_usd": median_cost,
        "p95_latency_s": p95_latency,
        "thresholds": thresholds,
        "passed": all(item["passed"] for item in thresholds.values()),
    }


# ---------------------------------------------------------------------------
# Reporting / artifact
# ---------------------------------------------------------------------------


def _truncate(text: str, width: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= width else text[: width - 1] + "…"


def _print_results(results: list[dict[str, Any]]) -> None:
    print("")
    print(
        f"{'id':<31} {'category':<11} {'mode':<9} {'status':<6} "
        f"{'reason':<48} {'cost':>9} {'latency':>9}"
    )
    print("-" * 132)
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        reason = "ok" if result["passed"] else "; ".join(result["reasons"])
        if result["status"] != "answered" and not result["passed"]:
            reason = f"{result['status']}: {reason}"
        print(
            f"{result['id']:<31} {result['category']:<11} {result['mode']:<9} "
            f"{status:<6} {_truncate(reason, 48):<48} "
            f"{'$' + format(result['cost_usd'], '.4f'):>9} "
            f"{format(result['duration_s'], '.1f') + 's':>9}"
        )


def _print_summary(metrics: dict[str, Any]) -> None:
    print("")
    print("By category:")
    for category in CATEGORIES:
        stats = metrics["by_category"][category]
        print(
            f"  {category:<12} {stats['passed']:>2}/{stats['total']:<2} "
            f"{'pass' if stats['passed'] == stats['total'] else 'FAIL'}"
        )

    print("")
    print("By mode (cost/latency split):")
    for mode in MODES:
        stats = metrics["by_mode"][mode]
        print(
            f"  {mode:<9} {stats['passed']:>2}/{stats['total']:<2} "
            f"median ${stats['median_cost_usd']:.4f} "
            f"p95 {stats['p95_latency_s']:.1f}s "
            f"total ${stats['total_cost_usd']:.4f}"
        )

    print("")
    print("A1-A7 + surface metrics:")
    for name, entry in metrics["thresholds"].items():
        if "value_pct" in entry:
            value = f"{entry['value_pct']:.1f}%"
            target = f">= {entry['threshold_pct']:.0f}%"
        elif name.startswith("A6"):
            mode_bits = " ".join(
                f"{mode}=${item['value']:.4f}"
                for mode, item in entry.get("by_mode", {}).items()
            )
            value = mode_bits or f"${entry['value']:.4f}"
            target = " ".join(
                f"{mode}<=${item['threshold']:.4f}"
                for mode, item in entry.get("by_mode", {}).items()
            ) or f"<= ${entry['threshold']:.4f}"
        elif name.startswith("A7"):
            value = f"{entry['value']:.1f}s"
            per_mode = entry.get("by_mode") or {}
            if per_mode:
                value += " (" + " ".join(
                    f"{mode}={latency:.1f}s" for mode, latency in per_mode.items()
                ) + ")"
            target = f"<= {entry['threshold']:.0f}s"
        elif name.startswith("A3"):
            value = f"{entry['value']}/{entry.get('total', metrics['total'])}"
            target = (
                f">= {entry['threshold']}, all user "
                f"({entry.get('user_passed')}/{entry.get('user_total')}) and doc "
                f"({entry.get('document_passed')}/{entry.get('document_total')})"
            )
        else:
            value = str(entry["value"])
            target = f"= {entry['threshold']}"
        verdict = "PASS" if entry["passed"] else "FAIL"
        print(f"  {name:<34} {value:>18}  {target:<42} {verdict}")

    print("")
    print(f"Overall: {'PASS' if metrics['passed'] else 'FAIL'}")


def write_artifact(
    results: list[dict[str, Any]],
    metrics: dict[str, Any],
    *,
    output: str | Path | None = None,
    questions_path: str | Path | None = None,
) -> Path:
    target = Path(output) if output else (
        DEFAULT_ARTIFACT_DIR / f"{_utc_stamp()}.json"
    )
    if target.exists() and target.is_dir():
        target = target / f"{_utc_stamp()}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "schema": 2,
        "questions_path": str(questions_path or QUESTIONS_PATH),
        "total": len(results),
        "passed": metrics["passed"],
        "counts": {
            "by_category": metrics["by_category"],
            "by_mode": metrics["by_mode"],
        },
        "metrics": metrics,
        "results": results,
    }
    target.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )
    return target


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m chat_app.eval",
        description="Run the deterministic chat acceptance eval (WS7).",
    )
    parser.add_argument(
        "--questions",
        default=str(QUESTIONS_PATH),
        help="path to questions.yaml (default: chat-app/eval/questions.yaml)",
    )
    parser.add_argument("--limit", type=int, default=None, help="run the first N questions")
    parser.add_argument(
        "--mode",
        choices=MODES,
        default=None,
        help="run only one surface (desk or document); default: both",
    )
    parser.add_argument(
        "--id",
        dest="ids",
        action="append",
        default=None,
        help="run only this question id (repeatable or comma-separated)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="result JSON path (default: idx_data/chat/eval/{utc}.json); "
        "with --snapshot, the snapshot path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate the question schema and print counts; do not call the model",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="worker count, bounded at 2 (default: 1)",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="regenerate chat-app/eval/supported-set.json from disk",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.snapshot:
        target = write_snapshot(args.output)
        print(f"wrote supported-set snapshot: {target}")
        return 0

    try:
        questions = load_questions(args.questions)
    except QuestionSchemaError as exc:
        print(f"invalid question set: {exc}", file=sys.stderr)
        return 2

    counts = category_counts(questions)
    modes = mode_counts(questions)
    if args.dry_run:
        print(f"questions: {len(questions)}")
        for category in CATEGORIES:
            print(f"{category}: {counts[category]}")
        for mode in MODES:
            print(f"mode.{mode}: {modes[mode]}")
        print(f"validated: {_resolve_path(args.questions)}")
        return 0

    workers = max(1, min(int(args.concurrency or 1), 2))
    if args.concurrency and args.concurrency > 2:
        print("concurrency capped at 2", file=sys.stderr)

    try:
        results, metrics = run_eval(
            questions,
            limit=args.limit,
            ids=args.ids,
            mode=args.mode,
            concurrency=workers,
        )
    except QuestionSchemaError as exc:
        print(f"invalid selection: {exc}", file=sys.stderr)
        return 2
    except BackendUnavailable as exc:
        print(f"eval aborted: {exc}", file=sys.stderr)
        return 2

    _print_results(results)
    _print_summary(metrics)
    artifact = write_artifact(
        results,
        metrics,
        output=args.output,
        questions_path=args.questions,
    )
    print("")
    print(f"artifact: {artifact}")
    return 0 if metrics["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
