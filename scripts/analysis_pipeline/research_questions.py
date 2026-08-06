"""Deterministic research-question generation for external research.

This module deliberately does not call an LLM or the web. It converts the
detected financial events into bounded, identity-aware questions that can be
shown to the research model and tested independently.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
IDX_COMPANIES_PATH = _PROJECT_ROOT / "idx_data" / "idx_companies.json"


@dataclass(frozen=True)
class CompanyIdentity:
    ticker: str
    legal_name: str
    sector: str
    forbidden_identities: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResearchQuestion:
    question_id: str
    event: str
    question: str
    search_query: str


# These aliases are disambiguation safeguards, not the source of company
# metadata. Canonical names and sectors come from idx_companies.json.
FORBIDDEN_IDENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "MHKI": ("PT Mahaka Media Tbk", "ABBA", "Mahaka Media"),
    "ABBA": ("PT Multi Hanna Kreasi Indonesia Tbk", "MHKI"),
}


def _load_company_index() -> dict[str, dict]:
    """Map ticker to the authoritative IDX company master record."""
    if not IDX_COMPANIES_PATH.exists():
        return {}
    try:
        data = json.loads(IDX_COMPANIES_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return {
        str(company["code"]).strip().upper(): company
        for company in data
        if isinstance(company, dict) and company.get("code")
    }


def resolve_identity(ticker: str) -> CompanyIdentity:
    """Return canonical identity metadata, with a safe unknown-ticker fallback."""
    normalized = ticker.strip().upper()
    record = _load_company_index().get(normalized)
    if not record:
        return CompanyIdentity(
            ticker=normalized,
            legal_name=normalized,
            sector="unknown sector",
        )
    return CompanyIdentity(
        ticker=normalized,
        legal_name=str(record.get("name", normalized)).strip(),
        sector=str(record.get("sector", "unknown sector")).strip(),
        forbidden_identities=FORBIDDEN_IDENTITY_ALIASES.get(normalized, ()),
    )


def _event_key(event: str) -> str:
    match = re.search(r"'([^']+)'", event)
    if match:
        return match.group(1).lower()
    words = re.findall(r"[a-z0-9]+", event.lower())
    return "_".join(words[:3]) or "event"


def _question_for_event(event: str, identity: CompanyIdentity, period: str) -> str:
    lowered = event.lower()
    subject = f"{identity.legal_name} ({identity.ticker})"
    if "receivable" in lowered or "collection" in lowered:
        focus = "trade receivables, customer payment timing, and collections"
    elif "tax" in lowered:
        focus = "tax expense, prepaid taxes, taxes payable, and deferred-tax movements"
    elif "interest" in lowered or "loan" in lowered or "debt" in lowered:
        focus = "interest costs, borrowing, financing facilities, and interest rates"
    elif "other_income" in lowered or "other income" in lowered:
        focus = "other income, investment gains, foreign exchange, and asset disposals"
    elif "revenue" in lowered or "margin" in lowered or "profit" in lowered:
        focus = "revenue drivers, pricing, volume, costs, and industry conditions"
    else:
        focus = "company disclosures and sector conditions relevant to this movement"
    return (
        f"What external evidence explains {subject}'s {focus} for {period}, "
        f"without confusing it with another issuer?"
    )


def build_questions(
    ticker: str,
    year: int,
    quarter: str,
    events: str,
    *,
    max_questions: int = 12,
) -> tuple[CompanyIdentity, list[ResearchQuestion]]:
    """Build stable, bounded questions from newline-delimited detected events."""
    identity = resolve_identity(ticker)
    period = f"{quarter} {year}"
    questions: list[ResearchQuestion] = []
    if max_questions <= 0:
        return identity, questions
    seen_keys: set[str] = set()
    for raw_event in events.splitlines():
        event = raw_event.strip().lstrip("- ").strip()
        if not event:
            continue
        key = _event_key(event)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        questions.append(ResearchQuestion(
            question_id=f"{identity.ticker.lower()}-{len(questions) + 1:02d}",
            event=event,
            question=_question_for_event(event, identity, period),
            search_query=(
                f"{identity.ticker} {identity.legal_name} {quarter} {year} "
                f"{key.replace('_', ' ')}"
            ),
        ))
        if len(questions) >= max_questions:
            break
    return identity, questions


def questions_as_dicts(questions: list[ResearchQuestion]) -> list[dict[str, str]]:
    return [asdict(question) for question in questions]


def format_questions(questions: list[ResearchQuestion]) -> str:
    if not questions:
        return "(No external research questions generated)"
    return "\n".join(
        f"- [{q.question_id}] {q.question}\n  Suggested search: {q.search_query}"
        for q in questions
    )
