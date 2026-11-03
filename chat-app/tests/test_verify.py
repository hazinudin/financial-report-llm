"""The deterministic audit gate on a synthetic ITMG Q2 2026 trace."""

from __future__ import annotations

import json

from chat_app.envelope import AnswerEnvelope, parse_envelope
from chat_app.verify import repair_envelope, verify_envelope

DOC = "itmg_2026-08-10_news_release"


def _tool(name: str, args: dict, payload: dict) -> dict:
    """A pi-shaped tool trace entry (result content blocks carry JSON text)."""
    return {
        "tool_call_id": f"call_{name}",
        "tool_name": name,
        "arguments": args,
        "result": {
            "content": [{"type": "text", "text": json.dumps(payload)}],
        },
    }


def _entry(value, kind, variants, meta, source=None) -> dict:
    return {
        "value": value,
        "kind": kind,
        "variants": variants,
        "source": source,
        "meta": meta,
    }


def _operational_payload() -> dict:
    return {
        "ok": True,
        "tool": "operational_lookup",
        "request": {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": None},
        "data": {
            "ticker": "ITMG",
            "period": "Q2",
            "year": 2026,
            "state": "available",
            "entries": [{
                "metric": "coal_production",
                "scope": "Q2-2026",
                "value": 5.3,
                "unit": "Mt",
                "prior": 4.69,
                "page": 1,
                "source_doc": DOC,
                "status": "preliminary",
            }],
            "drivers": [{
                "driver": "higher strip ratio and fuel costs",
                "affects": "cost_of_revenue",
                "quote": "costs rose because of a higher strip ratio",
                "page": 1,
                "source_doc": DOC,
                "type": "management_stated",
            }],
            "documents": [{"id": DOC, "pages": 8, "status": "captured"}],
            "evidence": {},
        },
        "ledger": [
            _entry(
                5.3,
                "count",
                [5.3],
                {
                    "ticker": "ITMG", "period": "Q2", "year": 2026,
                    "metric": "coal_production", "source_doc": DOC, "page": 1,
                },
                source=DOC,
            ),
            _entry(
                4.69,
                "count",
                [4.69],
                {
                    "ticker": "ITMG", "period": "Q2", "year": 2026,
                    "metric": "coal_production", "period_type": "prior",
                    "source_doc": DOC, "page": 1,
                },
                source=DOC,
            ),
        ],
        "caveats": [],
    }


def _derive_payload() -> dict:
    meta = {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": "revenue"}
    return {
        "ok": True,
        "tool": "period_derive",
        "request": {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": "revenue"},
        "data": {
            "ticker": "ITMG",
            "metric": "revenue",
            "period": "Q2",
            "year": 2026,
            "formula": "tw2 - tw1",
            "operands": {"tw2": 1000491.0, "tw1": 497577.0},
            "quarter_value": 502914.0,
            "unit": "USD thousand",
            "value_idr_bn": 40987.0,
            "source": {"tw2": "1311000:6", "tw1": "1311000:6"},
        },
        "ledger": [
            _entry(
                1000491.0, "money", [1000491.0, 1000.491, 81540.0],
                {**meta, "operand": "tw2", "period_type": "cumulative"},
                source="1311000:6",
            ),
            _entry(
                497577.0, "money", [497577.0, 497.577, 40552.5],
                {**meta, "operand": "tw1", "period_type": "cumulative"},
                source="1311000:6",
            ),
            _entry(
                502914.0, "money", [502914.0, 502.914, 40986.0],
                {**meta, "derived": True, "formula": "tw2 - tw1",
                 "period_type": "quarter"},
                source="1311000:6",
            ),
        ],
        "caveats": [],
    }


def _trace() -> list[dict]:
    return [
        _tool(
            "operational_lookup",
            {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": None},
            _operational_payload(),
        ),
        _tool(
            "period_derive",
            {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": "revenue"},
            _derive_payload(),
        ),
    ]


PASSING = f"""answer: |
  ITMG produced 5.3 Mt of coal in Q2 2026 (page 1 of the release).
  Q2 revenue was 502,914 on a quarterly basis: 1,000,491 cumulative less 497,577 in Q1 (sheet 1311000, row 6).
  Production rose 13% QoQ.
  Management attributes the cost increase to a higher strip ratio.
claims:
  - text: "ITMG produced 5.3 Mt of coal in Q2 2026."
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}
  - text: "Q2 revenue was 502,914 on a quarterly basis: 1,000,491 cumulative less 497,577 in Q1."
    type: fact
    citations:
      - {{tool: period_derive, ticker: ITMG, metric: revenue}}
  - text: "Production rose 13% QoQ."
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production}}
  - text: "Management attributes the cost increase to a higher strip ratio."
    type: management_stated
    citations:
      - {{tool: operational_lookup, ticker: ITMG, driver: higher strip ratio,
         doc: {DOC}, page: 1}}
missing: []
"""


def _violation_types(verification: dict) -> list[str]:
    return [violation["type"] for violation in verification["violations"]]


def test_itmg_envelope_passes():
    verification = verify_envelope(parse_envelope(PASSING), _trace())
    assert verification["ok"] is True, verification["violations"]
    assert verification["violations"] == []
    assert verification["citations"]["ok"] == 4
    assert verification["citations"]["removed"] == 0
    assert verification["labels_ok"] is True
    assert verification["missing_state_ok"] is True
    assert verification["numbers"]["checked"] >= 5


def test_tampered_number_is_a_violation():
    tampered = PASSING.replace("5.3 Mt", "6.9 Mt").replace(
        "produced 5.3 Mt", "produced 6.9 Mt"
    )
    verification = verify_envelope(parse_envelope(tampered), _trace())
    assert verification["ok"] is False
    tokens = [
        violation["token"]
        for violation in verification["violations"]
        if violation["type"] == "number_not_in_ledger"
    ]
    assert "6.9" in tokens
    assert "number_not_in_ledger" in _violation_types(verification)


def test_causal_sentence_must_be_covered_by_a_claim():
    envelope = parse_envelope(PASSING)
    # Causal wording appears in the prose only; no claim covers this sentence.
    envelope.answer += "\nCosts also rose because fuel prices increased."
    verification = verify_envelope(envelope, _trace())
    assert verification["ok"] is False
    assert "causal_claim_unlabelled" in _violation_types(verification)
    assert verification["labels_ok"] is False


def test_management_stated_requires_document_and_page():
    text = PASSING.replace(
        f"""      - {{tool: operational_lookup, ticker: ITMG, driver: higher strip ratio,
         doc: {DOC}, page: 1}}""",
        "      - {tool: operational_lookup, ticker: ITMG, driver: higher strip ratio}",
    )
    envelope = parse_envelope(text)
    verification = verify_envelope(envelope, _trace())
    assert "management_stated_without_document" in _violation_types(verification)
    assert verification["ok"] is False


def test_absence_needs_documents_status_and_missing_entry():
    envelope = AnswerEnvelope.model_validate({
        "answer": "PTBA Q2 coal production does not exist in the covered set.",
        "claims": [{
            "text": "PTBA Q2 coal production does not exist in the covered set.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, _trace())
    assert "missing_without_documents_status" in _violation_types(verification)
    assert verification["missing_state_ok"] is False


def test_absence_with_documents_status_and_missing_entry_passes():
    status = _tool(
        "documents_status",
        {"ticker": "PTBA", "period": "Q2", "year": 2026, "item": "coal_production"},
        {
            "ok": True,
            "tool": "documents_status",
            "request": {"ticker": "PTBA", "period": "Q2", "year": 2026,
                        "item": "coal_production"},
            "data": {
                "ticker": "PTBA", "period": "Q2", "year": 2026,
                "item": "coal_production", "state": "not_captured",
                "evidence": {"url": "https://ptba.co.id/x.pdf", "error": "403"},
                "documents": [],
            },
            "ledger": [],
            "caveats": [],
        },
    )
    envelope = AnswerEnvelope.model_validate({
        "answer": "PTBA Q2 coal production is not captured; the fetch returned 403.",
        "claims": [{
            "text": "PTBA Q2 coal production is not captured; the fetch returned 403.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [{
            "item": "coal_production",
            "state": "not_captured",
            "evidence": {"url": "https://ptba.co.id/x.pdf", "error": "403"},
        }],
    })
    verification = verify_envelope(envelope, _trace() + [status])
    assert verification["ok"] is True, verification["violations"]
    assert verification["missing_state_ok"] is True


def test_percent_change_uses_structured_tool_prior():
    """`operational_lookup` carries `prior` in data even if not in the ledger."""
    payload = _operational_payload()
    payload["ledger"] = [payload["ledger"][0]]  # drop the prior ledger entry
    trace = [
        _tool(
            "operational_lookup",
            {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": None},
            payload,
        ),
        _tool(
            "period_derive",
            {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": "revenue"},
            _derive_payload(),
        ),
    ]
    verification = verify_envelope(parse_envelope(PASSING), trace)
    assert verification["ok"] is True, verification["violations"]


def test_unmatched_citation_is_removed_and_claim_downgraded():
    envelope = AnswerEnvelope.model_validate({
        "answer": "PTBA revenue grew.",
        "claims": [{
            "text": "PTBA revenue grew.",
            "type": "fact",
            "citations": [{
                "tool": "company_facts", "ticker": "PTBA", "metric": "revenue",
            }],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, _trace())
    assert verification["ok"] is False
    assert verification["citations"]["removed"] == 1
    assert "citation_unmatched" in _violation_types(verification)

    repaired = repair_envelope(envelope, verification)
    assert repaired.claims[0].type == "hypothesis"
    assert repaired.claims[0].citations == []

    reverified = verify_envelope(repaired, _trace())
    assert reverified["ok"] is True, reverified["violations"]


def test_tool_errors_are_ignored_for_the_ledger():
    broken = {
        "tool_call_id": "call_broken",
        "tool_name": "company_facts",
        "arguments": {"ticker": "ITMG", "period": "Q2", "year": 2026},
        "result": {
            "content": [{"type": "text", "text": "boom"}],
            "isError": True,
        },
    }
    verification = verify_envelope(parse_envelope(PASSING), _trace() + [broken])
    assert verification["ok"] is True
    assert [entry["meta"]["metric"] for entry in verification["ledger"]] .count(
        "revenue"
    ) == 3


def test_already_parsed_tool_payload_is_accepted():
    trace = [
        {
            "tool_name": "operational_lookup",
            "arguments": {"ticker": "ITMG", "period": "Q2", "year": 2026},
            "result": _operational_payload(),
        }
    ]
    envelope = parse_envelope(
        f"""answer: production was 5.3 Mt
claims:
  - text: production was 5.3 Mt
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production, page: 1}}
missing: []
"""
    )
    verification = verify_envelope(envelope, trace)
    assert verification["ok"] is True, verification["violations"]


# ---------------------------------------------------------------------------
# Two-surface verification: primary/document ledgers + note citations
# ---------------------------------------------------------------------------

from chat_app.verify import build_document_ledger, build_ledger  # noqa: E402

NOTE_ID = "itmg-q2-2026-analysis"
NOTE_CONTENT = (
    "## 1. Executive Summary\n"
    "ITMG delivered net revenue of US$1,000,491k (+8.8% YoY).\n"
    "We treat this as a timing artifact rather than operating deterioration.\n"
    "## 5. Cash Flow Analysis\n"
    "Operating cash flow collapsed −77.8% to US$62,376k.\n"
)
NOTE_QUOTE = (
    "ITMG delivered net revenue of US$1,000,491k (+8.8% YoY).\n"
    "We treat this as a timing artifact rather than operating deterioration."
)
CASH_QUOTE = "Operating cash flow collapsed −77.8% to US$62,376k."


def _note_prefetch(ledger: list[dict] | None = None) -> dict:
    """A synthetic analysis_document prefetch call (agent shape)."""
    return {
        "tool_call_id": "context:analysis_document",
        "tool_name": "analysis_document",
        "arguments": {"document_id": NOTE_ID, "section": None},
        "result": {
            "content": [{"type": "text", "text": json.dumps({
                "ok": True,
                "tool": "analysis_document",
                "request": {"document_id": NOTE_ID, "section": None},
                "data": {
                    "document_id": NOTE_ID,
                    "title": "ITMG Q2 2026 Analysis",
                    "path": "analysis/ITMG Q2 2026 Analysis.md",
                    "kind": "company",
                    "ticker": "ITMG",
                    "period": "Q2",
                    "year": 2026,
                    "sections": [
                        {"title": "1. Executive Summary", "level": 2, "line": 1},
                        {"title": "5. Cash Flow Analysis", "level": 2, "line": 4},
                    ],
                    "content": NOTE_CONTENT,
                },
                "ledger": ledger or [],
                "caveats": [],
            })}],
        },
        "context": True,
    }


def _note_search(
    *, document_id: str | None = NOTE_ID, state: str | None = "not_in_document",
    matches: list[dict] | None = None,
) -> dict:
    data: dict = {
        "query": "strip ratio",
        "document_id": document_id,
        "matches": matches or [],
    }
    if state:
        data["state"] = state
        data["evidence"] = {"document": document_id, "sections_searched": 2}
    return _tool(
        "analysis_search",
        {"query": "strip ratio", "document_id": document_id},
        {"ok": True, "tool": "analysis_search", "request": {},
         "data": data, "ledger": [], "caveats": []},
    )


def _note_trace(ledger: list[dict] | None = None) -> list[dict]:
    return [_note_prefetch(ledger), *_trace()]


def _note_claim(quote: str = CASH_QUOTE, *, line: int = 5) -> dict:
    return {
        "text": "The note reports operating cash flow of US$62,376k and calls the cash-flow drop a timing artifact.",
        "type": "analysis_note",
        "citations": [{
            "tool": "analysis_document",
            "document": NOTE_ID,
            "section": "5. Cash Flow Analysis",
            "line": line,
            "quote": quote,
        }],
    }


def test_document_ledger_is_isolated_from_the_primary_ledger():
    doc_entry = _entry(
        1000491.0, "document_number", [1000491.0, 1000.491, 1.000491],
        {"document": NOTE_ID, "section": "1. Executive Summary", "line": 2,
         "quote": "net revenue of US$1,000,491k"},
        source=f"analysis/{NOTE_ID}",
    )
    primary = _tool(
        "company_facts",
        {"ticker": "ITMG", "period": "Q2", "year": 2026},
        {
            "ok": True, "tool": "company_facts",
            "request": {"ticker": "ITMG"}, "data": {"ticker": "ITMG"},
            "ledger": [_entry(
                5.3, "count", [5.3],
                {"ticker": "ITMG", "metric": "coal_production"},
                source="operational:x:1",
            )],
            "caveats": [],
        },
    )
    trace = [_note_prefetch([doc_entry]), primary]
    assert [entry["kind"] for entry in build_ledger(trace)] == ["count"]
    document_ledger = build_document_ledger(trace)
    assert any(entry["kind"] == "document_number" for entry in document_ledger)
    assert all(
        entry.get("kind") == "document_number" for entry in document_ledger
    )
    verification = verify_envelope(
        parse_envelope(f"""answer: ITMG produced 5.3 Mt.
claims:
  - text: "ITMG produced 5.3 Mt."
    type: fact
    citations:
      - {{tool: company_facts, ticker: ITMG, metric: coal_production}}
missing: []
"""),
        trace,
    )
    assert verification["ok"] is True, verification["violations"]
    # The note number never leaks into the primary ledger.
    assert all(
        entry.get("kind") != "document_number"
        for entry in verification["ledger"]
    )


def test_analysis_note_with_quoted_note_number_passes():
    payload = {
        "answer": "The note reports operating cash flow of US$62,376k and calls the cash-flow drop a timing artifact.",
        "claims": [_note_claim()],
        "missing": [],
    }
    verification = verify_envelope(AnswerEnvelope.model_validate(payload), _note_trace())
    assert verification["ok"] is True, verification["violations"]
    assert verification["numbers"]["document_passed"] >= 1
    assert verification["citations"]["document_ok"] == 1


def test_fact_using_a_note_number_is_rejected():
    payload = {
        "answer": "Operating cash flow was 62,376k.",
        "claims": [{
            "text": "Operating cash flow was 62,376k.",
            "type": "fact",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "5. Cash Flow Analysis",
                "line": 5,
                "quote": "Operating cash flow collapsed −77.8% to US$62,376k.",
            }],
        }],
        "missing": [],
    }
    verification = verify_envelope(AnswerEnvelope.model_validate(payload), _note_trace())
    assert verification["ok"] is False
    types = _violation_types(verification)
    assert "document_number_as_fact" in types
    assert "fact_cites_note_only" in types


def test_analysis_note_without_a_quote_is_rejected():
    payload = {
        "answer": "The note calls it a timing artifact.",
        "claims": [{
            "text": "The note calls it a timing artifact.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 3,
            }],
        }],
        "missing": [],
    }
    verification = verify_envelope(AnswerEnvelope.model_validate(payload), _note_trace())
    assert verification["ok"] is False
    assert "analysis_note_without_quote" in _violation_types(verification)


def test_document_citation_quote_must_be_a_substring_of_the_section():
    payload = {
        "answer": "The note says cash flow collapsed because of tax refunds.",
        "claims": [_note_claim("This sentence is not anywhere in the note.")],
        "missing": [],
    }
    verification = verify_envelope(AnswerEnvelope.model_validate(payload), _note_trace())
    assert verification["ok"] is False
    assert "document_citation_unmatched" in _violation_types(verification)


def test_document_citation_must_name_a_document_tool():
    payload = {
        "answer": "The note calls it a timing artifact.",
        "claims": [{
            "text": "The note calls it a timing artifact.",
            "type": "analysis_note",
            "citations": [{
                "tool": "company_facts",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "timing artifact",
            }],
        }],
        "missing": [],
    }
    verification = verify_envelope(AnswerEnvelope.model_validate(payload), _note_trace())
    assert "document_citation_unmatched" in _violation_types(verification)


def test_repair_drops_an_unmatched_document_citation_and_downgrades():
    payload = {
        "answer": "The note calls it a timing artifact.",
        "claims": [{
            "text": "The note calls it a timing artifact.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "Not in the note at all.",
            }],
        }],
        "missing": [],
    }
    envelope = AnswerEnvelope.model_validate(payload)
    verification = verify_envelope(envelope, _note_trace())
    assert verification["ok"] is False

    repaired = repair_envelope(envelope, verification)
    assert repaired.claims[0].type == "hypothesis"
    assert repaired.claims[0].citations == []
    reverified = verify_envelope(repaired, _note_trace())
    assert reverified["ok"] is True, reverified["violations"]


def test_not_in_document_requires_a_scoped_search_and_a_missing_entry():
    envelope = AnswerEnvelope.model_validate({
        "answer": "This note does not cover strip ratio.",
        "claims": [{
            "text": "This note does not cover strip ratio.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [{
            "item": "strip_ratio",
            "state": "not_in_document",
            "evidence": {"sections_searched": 2},
        }],
    })
    passing = verify_envelope(envelope, _note_trace() + [_note_search()])
    assert passing["ok"] is True, passing["violations"]

    missing_search = verify_envelope(envelope, _note_trace())
    assert "not_in_document_without_search" in _violation_types(missing_search)

    unscoped = verify_envelope(envelope, _note_trace() + [_note_search(document_id=None)])
    assert "not_in_document_without_search" in _violation_types(unscoped)

    no_entry = AnswerEnvelope.model_validate({
        "answer": "This note does not cover strip ratio.",
        "claims": [{
            "text": "This note does not cover strip ratio.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [],
    })
    verdict = verify_envelope(no_entry, _note_trace() + [_note_search()])
    assert "not_in_document_required" in _violation_types(verdict)


def test_note_silence_is_never_not_disclosed_or_does_not_exist():
    envelope = AnswerEnvelope.model_validate({
        "answer": "This note does not cover strip ratio; the data does not exist.",
        "claims": [{
            "text": "This note does not cover strip ratio; the data does not exist.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [{
            "item": "strip_ratio",
            "state": "not_disclosed",
            "evidence": {},
        }],
    })
    verification = verify_envelope(envelope, _note_trace() + [_note_search()])
    types = _violation_types(verification)
    assert "not_in_document_required" in types
    assert verification["ok"] is False


def test_attributed_note_rating_is_allowed_but_unattributed_is_refused():
    attributed = AnswerEnvelope.model_validate({
        "answer": "The note assesses this as a HOLD on policy clarity.",
        "claims": [{
            "text": "The note assesses this as a HOLD on policy clarity.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "We treat this as a timing artifact rather than operating deterioration.",
            }],
        }],
        "missing": [],
    })
    verdict = verify_envelope(attributed, _note_trace())
    assert verdict["ok"] is True, verdict["violations"]
    assert verdict["advice_ok"] is True

    unattributed = AnswerEnvelope.model_validate({
        "answer": "ITMG is a HOLD.",
        "claims": [{
            "text": "ITMG is a HOLD.",
            "type": "hypothesis",
            "citations": [],
        }],
        "missing": [],
    })
    verdict = verify_envelope(unattributed, _note_trace())
    assert verdict["ok"] is False
    assert "advice_or_ranking" in _violation_types(verdict)


def test_attributed_note_rating_must_stay_in_the_answer_prose():
    """A note view adopted in the prose without attribution is still advice."""
    envelope = AnswerEnvelope.model_validate({
        "answer": "ITMG is a HOLD.",
        "claims": [{
            "text": "The note assesses ITMG as a HOLD.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "We treat this as a timing artifact rather than operating deterioration.",
            }],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, _note_trace())
    assert "advice_or_ranking" in _violation_types(verification)


def test_ranking_and_price_target_language_is_refused():
    for text in (
        "ITMG is the best pick among the coal names.",
        "Our price target for ITMG is 30,000.",
        "ITMG is ranked first in the sector.",
    ):
        envelope = AnswerEnvelope.model_validate({
            "answer": text,
            "claims": [{
                "text": text,
                "type": "hypothesis",
                "citations": [],
            }],
            "missing": [],
        })
        verification = verify_envelope(envelope, _trace())
        assert verification["ok"] is False, text
        assert "advice_or_ranking" in _violation_types(verification), text


def test_top_two_customer_concentration_is_not_a_company_ranking():
    """Seed question 8: 'top-2 customer concentration' must not be refused."""
    envelope = AnswerEnvelope.model_validate({
        "answer": "The note discusses ESSA's top-2 customer concentration.",
        "claims": [{
            "text": "The note discusses ESSA's top-2 customer concentration.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "We treat this",
            }],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, _note_trace())
    assert "advice_or_ranking" not in _violation_types(verification)


def test_document_citations_resolve_from_an_unscoped_analysis_search():
    match = {
        "document": NOTE_ID,
        "path": "analysis/ITMG Q2 2026 Analysis.md",
        "title": "ITMG Q2 2026 Analysis",
        "section": "1. Executive Summary",
        "line": 2,
        "quote": "We treat this as a timing artifact",
    }
    trace = [_note_search(document_id=None, state=None, matches=[match])]
    envelope = AnswerEnvelope.model_validate({
        "answer": "The note calls it a timing artifact.",
        "claims": [{
            "text": "The note calls it a timing artifact.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_search",
                "document": NOTE_ID,
                "section": "1. Executive Summary",
                "line": 2,
                "quote": "We treat this as a timing artifact",
            }],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, trace)
    assert verification["ok"] is True, verification["violations"]
    assert verification["citations"]["document_ok"] == 1


def test_coverage_compared_tickers_must_be_cited_or_compared():
    envelope = AnswerEnvelope.model_validate({
        "answer": "ITMG and PTBA were compared.",
        "claims": [{
            "text": "ITMG produced 5.3 Mt.",
            "type": "fact",
            "citations": [{
                "tool": "operational_lookup", "ticker": "ITMG",
                "metric": "coal_production", "page": 1,
            }],
        }],
        "missing": [],
        "coverage": {
            "scope": "supported_set",
            "supported_tickers": 24,
            "compared_tickers": ["ITMG", "PTBA"],
            "caveat": "Supported names only",
        },
    })
    verification = verify_envelope(envelope, _trace())
    assert "coverage_ticker_uncited" in _violation_types(verification)
    assert verification["ok"] is False


def test_list_and_heading_ordinals_are_not_quantities():
    envelope = AnswerEnvelope.model_validate({
        "answer": (
            "1. Cost moved first.\n"
            "2. *The analyst's conclusion.*\n"
            "## 3.1 Gross Margin\n"
            "Revenue was 1,000,491."
        ),
        "claims": [{
            "text": "Revenue was 1,000,491.",
            "type": "fact",
            "citations": [{"tool": "period_derive", "ticker": "ITMG",
                           "metric": "revenue"}],
        }],
        "missing": [],
    })
    verification = verify_envelope(envelope, _trace())
    assert "number_not_in_ledger" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def _compare_payload() -> dict:
    return {
        "ok": True,
        "tool": "compare_supported",
        "request": {"tickers": ["ITMG", "PTBA"], "metrics": ["coal_production"],
                    "period": "Q2", "year": 2026},
        "data": {
            "period": "Q2", "year": 2026,
            "columns": ["ITMG", "PTBA"],
            "rows": [{"metric": "coal_production", "unit": "Mt", "cells": [
                {"ticker": "ITMG", "value": 5.3, "status": "ok",
                 "source": "operational:itmg_2026-08-10_news_release:1"},
            ]}],
            "excluded": [],
            "coverage": {"requested": 2, "answered": 1, "scope": "supported_set"},
        },
        "ledger": [_entry(5.3, "count", [5.3],
                          {"ticker": "ITMG", "metric": "coal_production",
                           "period": "Q2", "year": 2026})],
    }


def test_matrix_citation_ignores_page_on_a_cell():
    trace = [_tool("compare_supported",
                   {"tickers": ["ITMG", "PTBA"], "metrics": ["coal_production"],
                    "period": "Q2", "year": 2026},
                   _compare_payload())]
    envelope = AnswerEnvelope.model_validate({
        "answer": "ITMG produced 5.3 Mt in Q2 2026.",
        "claims": [{
            "text": "ITMG produced 5.3 Mt in Q2 2026.",
            "type": "fact",
            "citations": [{"tool": "compare_supported", "ticker": "ITMG",
                           "metric": "coal_production", "page": 1}],
        }],
        "missing": [],
        "coverage": {"scope": "supported_set", "supported_tickers": 24,
                     "compared_tickers": ["ITMG", "PTBA"],
                     "caveat": "Supported names only"},
    })
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "citation_unmatched" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def test_negated_sector_screen_caveat_is_not_advice():
    trace = [_tool("compare_supported",
                   {"tickers": ["ITMG", "PTBA"], "metrics": ["coal_production"],
                    "period": "Q2", "year": 2026},
                   _compare_payload())]
    envelope = AnswerEnvelope.model_validate({
        "answer": "The comparison covers two supported names; it is not an IDX-wide sector screen.",
        "claims": [{
            "text": "ITMG produced 5.3 Mt in Q2 2026.",
            "type": "fact",
            "citations": [{"tool": "compare_supported", "ticker": "ITMG",
                           "metric": "coal_production"}],
        }],
        "missing": [],
        "coverage": {"scope": "supported_set", "supported_tickers": 24,
                     "compared_tickers": ["ITMG", "PTBA"],
                     "caveat": "Supported names only"},
    })
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "advice_or_ranking" not in _violation_types(verification)


# ---------------------------------------------------------------------------
# Missing-state audit: desk language vs filing states
# ---------------------------------------------------------------------------

def _documents_status(
    *, ticker: str, period: str, year: int, item: str, state: str,
) -> dict:
    return _tool(
        "documents_status",
        {"ticker": ticker, "period": period, "year": year, "item": item},
        {
            "ok": True,
            "tool": "documents_status",
            "request": {"ticker": ticker, "period": period, "year": year,
                        "item": item},
            "data": {
                "ticker": ticker, "period": period, "year": year, "item": item,
                "state": state, "evidence": {}, "documents": [],
            },
            "ledger": [],
            "caveats": [],
        },
    )


def _company_facts_completeness(
    *, ticker: str, period: str, year: int, **completeness: str,
) -> dict:
    return _tool(
        "company_facts",
        {"ticker": ticker, "period": period, "year": year, "sections": "all"},
        {
            "ok": True,
            "tool": "company_facts",
            "request": {"ticker": ticker, "period": period, "year": year},
            "data": {
                "ticker": ticker, "period": period, "year": year,
                "completeness": completeness,
            },
            "ledger": [],
            "caveats": [],
        },
    )


def test_desk_not_in_document_is_rejected():
    envelope = AnswerEnvelope.model_validate({
        "answer": "Coverage is limited to the pinned note.",
        "claims": [],
        "missing": [{
            "item": "strip_ratio",
            "state": "not_in_document",
            "evidence": {"document": NOTE_ID},
        }],
    })
    trace = [_note_search()]
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "not_in_document_in_desk" in _violation_types(verification)
    assert verification["missing_state_ok"] is False
    assert verification["ok"] is False


def test_missing_contradicts_documents_status():
    trace = [
        _documents_status(
            ticker="MAPA", period="Q2", year=2026,
            item="geographic revenue", state="available",
        ),
    ]
    envelope = AnswerEnvelope.model_validate({
        "answer": "Geographic revenue was reported for the period.",
        "claims": [],
        "missing": [{
            "item": "geographic_revenue",
            "state": "not_disclosed",
            "evidence": {"ticker": "MAPA", "period": "Q2 2026"},
        }],
    })
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "missing_contradicts_documents_status" in _violation_types(verification)
    assert verification["missing_state_ok"] is False
    assert verification["ok"] is False


def test_false_not_captured_with_complete_financials():
    envelope = AnswerEnvelope.model_validate({
        "answer": "The quarter's financial statements are in the captured set.",
        "claims": [],
        "missing": [{
            "item": "geographic_revenue",
            "state": "not_captured",
            "evidence": {"ticker": "MAPA", "period": "Q2 2026"},
        }],
    })
    financials = verify_envelope(
        envelope,
        [_company_facts_completeness(
            ticker="MAPA", period="Q2", year=2026, financials="complete",
        )],
        mode="desk",
    )
    assert "false_not_captured" in _violation_types(financials)
    assert financials["missing_state_ok"] is False
    assert financials["ok"] is False

    filings = verify_envelope(
        envelope,
        [_company_facts_completeness(
            ticker="MAPA", period="Q2", year=2026, filings="captured",
        )],
        mode="desk",
    )
    assert "false_not_captured" in _violation_types(filings)
    assert filings["ok"] is False


def test_false_not_captured_allows_item_level_not_captured():
    """A complete period can still lack one document class (e.g. presentation)."""
    envelope = AnswerEnvelope.model_validate({
        "answer": (
            "The filing is captured, but the Q2 presentation was not captured."
        ),
        "claims": [],
        "missing": [{
            "item": "presentation",
            "state": "not_captured",
            "evidence": {"ticker": "MAPA", "period": "Q2 2026"},
        }],
    })
    trace = [
        _company_facts_completeness(
            ticker="MAPA", period="Q2", year=2026,
            financials="complete", filings="captured",
        ),
        _documents_status(
            ticker="MAPA", period="Q2", year=2026,
            item="presentation", state="not_captured",
        ),
    ]
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "false_not_captured" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def test_false_not_captured_accepts_completeness_gap():
    """``completeness.gaps`` is item-level evidence, not a period-wide verdict."""
    envelope = AnswerEnvelope.model_validate({
        "answer": "The filing is captured, but no presentation was captured.",
        "claims": [],
        "missing": [{
            "item": "presentation",
            "state": "not_captured",
            "evidence": {"ticker": "MAPA", "period": "Q3 2026"},
        }],
    })
    trace = [
        _company_facts_completeness(
            ticker="MAPA", period="Q3", year=2026,
            financials="complete", filings="captured",
            gaps=[{"item": "presentation", "state": "not_captured"}],
        ),
    ]
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "false_not_captured" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def test_completeness_gap_does_not_excuse_an_unrelated_item():
    envelope = AnswerEnvelope.model_validate({
        "answer": "Revenue was not captured.",
        "claims": [],
        "missing": [{
            "item": "revenue",
            "state": "not_captured",
            "evidence": {"ticker": "MAPA", "period": "Q3 2026"},
        }],
    })
    trace = [
        _company_facts_completeness(
            ticker="MAPA", period="Q3", year=2026,
            financials="complete", filings="captured",
            gaps=[{"item": "presentation", "state": "not_captured"}],
        ),
    ]
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "false_not_captured" in _violation_types(verification)


def test_false_not_captured_still_rejects_unrelated_item():
    """Period completeness still catches a different item with no status call."""
    envelope = AnswerEnvelope.model_validate({
        "answer": (
            "The Q2 presentation was not captured; revenue was not captured "
            "either."
        ),
        "claims": [],
        "missing": [{
            "item": "revenue",
            "state": "not_captured",
            "evidence": {"ticker": "MAPA", "period": "Q2 2026"},
        }],
    })
    trace = [
        _company_facts_completeness(
            ticker="MAPA", period="Q2", year=2026,
            financials="complete", filings="captured",
        ),
        _documents_status(
            ticker="MAPA", period="Q2", year=2026,
            item="presentation", state="not_captured",
        ),
    ]
    verification = verify_envelope(envelope, trace, mode="desk")
    assert "false_not_captured" in _violation_types(verification)
    assert verification["missing_state_ok"] is False
    assert verification["ok"] is False


def test_read_document_indonesian_thousands_feed_the_ledger():
    """Dot-thousand table values must be auditable, not split into decimals."""
    q1 = _tool(
        "read_document",
        {"ticker": "MAPA", "doc_id": "mapa_q1_2026_filing", "page": 72},
        {
            "ok": True,
            "tool": "read_document",
            "request": {"ticker": "MAPA", "doc_id": "mapa_q1_2026_filing",
                        "page": 72},
            "data": {
                "ticker": "MAPA", "doc_id": "mapa_q1_2026_filing",
                "page": 72, "pages": 91,
                "text": "Net revenues by geographical market\n"
                        "Indonesia\n3.775.872\n3.228.957\n"
                        "Jumlah\n4.954.727\n4.319.434\n",
            },
            "ledger": [{
                "value": 72, "unit": "page", "kind": "page", "source": None,
                "variants": [72],
                "meta": {"ticker": "MAPA", "document": "mapa_q1_2026_filing"},
            }],
            "caveats": [],
        },
    )
    q2 = _tool(
        "read_document",
        {"ticker": "MAPA", "doc_id": "mapa_q2_2026_filing", "page": 75},
        {
            "ok": True,
            "tool": "read_document",
            "request": {"ticker": "MAPA", "doc_id": "mapa_q2_2026_filing",
                        "page": 75},
            "data": {
                "ticker": "MAPA", "doc_id": "mapa_q2_2026_filing",
                "page": 75, "pages": 96,
                "text": "Net revenues by geographical market\n"
                        "Indonesia\n7.711.678\n6.591.698\n"
                        "Jumlah\n10.122.756\n8.794.636\n",
            },
            "ledger": [{
                "value": 75, "unit": "page", "kind": "page", "source": None,
                "variants": [75],
                "meta": {"ticker": "MAPA", "document": "mapa_q2_2026_filing"},
            }],
            "caveats": [],
        },
    )
    envelope = AnswerEnvelope.model_validate({
        "answer": (
            "Q1 Indonesia revenue was 3,775,872 and H1 Indonesia revenue was "
            "7,711,678, so Q2 Indonesia revenue was 3,935,806 "
            "(7,711,678 less 3,775,872)."
        ),
        "claims": [
            {
                "text": "Q1 Indonesia revenue was 3,775,872.",
                "type": "fact",
                "citations": [{
                    "tool": "read_document", "ticker": "MAPA",
                    "doc": "mapa_q1_2026_filing", "page": 72,
                }],
            },
            {
                "text": "H1 Indonesia revenue was 7,711,678.",
                "type": "fact",
                "citations": [{
                    "tool": "read_document", "ticker": "MAPA",
                    "doc": "mapa_q2_2026_filing", "page": 75,
                }],
            },
            {
                "text": (
                    "Q2 Indonesia revenue was 3,935,806 "
                    "(7,711,678 less 3,775,872)."
                ),
                "type": "fact",
                "citations": [
                    {"tool": "read_document", "ticker": "MAPA",
                     "doc": "mapa_q1_2026_filing", "page": 72},
                    {"tool": "read_document", "ticker": "MAPA",
                     "doc": "mapa_q2_2026_filing", "page": 75},
                ],
            },
        ],
        "missing": [],
    })
    verification = verify_envelope(envelope, [q1, q2])
    assert verification["ok"], verification["violations"]


# ---------------------------------------------------------------------------
# Compact claims (`sentence: N`) audit exactly like repeated-`text` claims
# ---------------------------------------------------------------------------


COMPACT = f"""answer: |
  ITMG produced 5.3 Mt of coal in Q2 2026 (page 1 of the release).
  Production rose 13% QoQ.
claims:
  - sentence: 1
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}
  - sentence: 2
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production}}
missing: []
"""


def test_compact_claim_passes_and_resolves_numbers():
    envelope = parse_envelope(COMPACT)
    assert envelope.claims[0].text.startswith("ITMG produced 5.3 Mt")
    verification = verify_envelope(envelope, _trace())
    assert verification["ok"] is True, verification["violations"]
    assert verification["citations"]["ok"] == 2
    # The resolved claim text is audited: every number in it was checked.
    assert verification["numbers"]["checked"] == verification["numbers"]["passed"]
    assert verification["numbers"]["checked"] >= 4


def test_compact_claim_covers_a_causal_sentence():
    envelope = parse_envelope(
        "answer: |\n"
        "  Costs rose because fuel prices increased.\n"
        "claims:\n"
        "  - sentence: 1\n"
        "    type: hypothesis\n"
        "missing: []\n"
    )
    verification = verify_envelope(envelope, _trace())
    assert "causal_claim_unlabelled" not in _violation_types(verification)
    assert verification["labels_ok"] is True


def test_compact_claim_does_not_hide_a_tampered_number():
    tampered = COMPACT.replace("5.3 Mt", "6.9 Mt")
    verification = verify_envelope(parse_envelope(tampered), _trace())
    assert verification["ok"] is False
    tokens = [
        violation["token"]
        for violation in verification["violations"]
        if violation["type"] == "number_not_in_ledger"
    ]
    assert "6.9" in tokens


# ---------------------------------------------------------------------------
# Ratio multiples ("0.86x") are derived values, like percentages
# ---------------------------------------------------------------------------


def _balance_trace() -> list[dict]:
    meta = {"ticker": "MAPA", "period": "Q3", "year": 2026}
    payload = {
        "ok": True,
        "tool": "company_facts",
        "request": {"ticker": "MAPA", "period": "Q3", "year": 2026},
        "data": {
            "ticker": "MAPA", "period": "Q3", "year": 2026,
            "sections": {"balance_sheet": {
                "total_liabilities": {"value": 8818754.0},
                "total_equity": {"value": 10254275.0},
            }},
        },
        "ledger": [
            _entry(
                8818754.0, "money", [8818754.0, 8818.754],
                {**meta, "metric": "total_liabilities"}, source="1210000:247",
            ),
            _entry(
                10254275.0, "money", [10254275.0, 10254.275],
                {**meta, "metric": "total_equity"}, source="1210000:272",
            ),
        ],
        "caveats": [],
    }
    return [_tool(
        "company_facts",
        {"ticker": "MAPA", "period": "Q3", "year": 2026},
        payload,
    )]


def _ratio_envelope(value: str) -> AnswerEnvelope:
    text = f"Liabilities-to-equity was {value}x at period end."
    return AnswerEnvelope.model_validate({
        "answer": text,
        "claims": [{
            "text": text,
            "type": "fact",
            "citations": [
                {"tool": "company_facts", "ticker": "MAPA",
                 "metric": "total_liabilities", "row": 247},
                {"tool": "company_facts", "ticker": "MAPA",
                 "metric": "total_equity", "row": 272},
            ],
        }],
        "missing": [],
    })


def test_ratio_multiple_from_two_ledger_values_is_accepted():
    verification = verify_envelope(_ratio_envelope("0.86"), _balance_trace(), mode="desk")
    assert "number_not_in_ledger" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def test_unrelated_ratio_multiple_is_rejected():
    verification = verify_envelope(_ratio_envelope("1.42"), _balance_trace(), mode="desk")
    assert "number_not_in_ledger" in _violation_types(verification)
    assert verification["ok"] is False


def test_date_day_is_not_a_quantity():
    from chat_app.verify import _tokens

    values = [token["value"] for token in _tokens("Period ended 30 June 2026.")]
    assert 30.0 not in values
    values = [token["value"] for token in _tokens("30 September 2026")]
    assert 30.0 not in values


def test_day_like_quantity_before_a_month_word_is_still_a_quantity():
    from chat_app.verify import _tokens

    assert [token["value"] for token in _tokens("stock cover of 30 months")] == [30.0]


# ---------------------------------------------------------------------------
# Currency prefixes and trillion scaling
# ---------------------------------------------------------------------------


def test_currency_prefix_and_trillion_scale_are_tokenised():
    from chat_app.verify import _tokens

    assert [token["value"] for token in _tokens("Revenue was Rp16.43 trillion.")] == [16.43]
    assert [token["value"] for token in _tokens("Revenue was Rp16.43 tn.")] == [16.43]
    assert [token["value"] for token in _tokens("Revenue was IDR 16,433.45 billion.")] == [16433.45]


def _revenue_trillion_trace() -> list[dict]:
    meta = {"ticker": "MAPA", "period": "Q3", "year": 2026, "metric": "revenue"}
    payload = {
        "ok": True,
        "tool": "company_facts",
        "request": {"ticker": "MAPA", "period": "Q3", "year": 2026},
        "data": {"ticker": "MAPA", "period": "Q3", "year": 2026},
        "ledger": [_entry(
            16433452.0, "money",
            [16433452.0, 16433452.0, 16433.452, 16.433452],
            meta, source="1311000:6",
        )],
        "caveats": [],
    }
    return [_tool(
        "company_facts",
        {"ticker": "MAPA", "period": "Q3", "year": 2026},
        payload,
    )]


def _trillion_envelope(value: str) -> AnswerEnvelope:
    text = f"Revenue was Rp{value} trillion."
    return AnswerEnvelope.model_validate({
        "answer": text,
        "claims": [{
            "text": text,
            "type": "fact",
            "citations": [{
                "tool": "company_facts", "ticker": "MAPA",
                "metric": "revenue", "row": 6,
            }],
        }],
        "missing": [],
    })


def test_trillion_scaled_value_matches_ledger_variant():
    verification = verify_envelope(
        _trillion_envelope("16.43"), _revenue_trillion_trace(), mode="desk",
    )
    assert "number_not_in_ledger" not in _violation_types(verification)
    assert verification["ok"] is True, verification["violations"]


def test_unrelated_trillion_scaled_value_is_rejected():
    verification = verify_envelope(
        _trillion_envelope("19.99"), _revenue_trillion_trace(), mode="desk",
    )
    assert "number_not_in_ledger" in _violation_types(verification)
