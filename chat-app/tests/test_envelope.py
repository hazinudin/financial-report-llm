"""Envelope parsing: fences, one repair pass, and loud failures."""

from __future__ import annotations

import pytest

from chat_app.envelope import (
    AnswerEnvelope,
    EnvelopeError,
    parse_claims_envelope,
    parse_envelope,
    render_envelope_yaml,
    split_sentences,
)


def test_fenced_yaml_parses():
    text = """Here is the envelope:

```yaml
answer: |
  Coal production was 5.3 Mt.
claims:
  - text: "Coal production was 5.3 Mt."
    type: fact
    citations:
      - {tool: operational_lookup, ticker: ITMG, metric: coal_production, page: 1}
missing: []
```

That is all.
"""
    envelope = parse_envelope(text)
    assert "5.3 Mt" in envelope.answer
    assert envelope.claims[0].type == "fact"
    assert envelope.claims[0].citations[0].metric == "coal_production"


def test_trailing_comma_repair():
    """The common `type: fact,` mistake is repaired once, then parses."""
    text = (
        "answer: coal production was 5.3 Mt\n"
        "claims:\n"
        "  - text: Coal production was 5.3 Mt.\n"
        "    type: fact,\n"
        "    citations:\n"
        "      - {tool: operational_lookup, ticker: ITMG, metric: coal_production}\n"
        "missing: []\n"
    )
    envelope = parse_envelope(text)
    assert envelope.claims[0].type == "fact"


def test_trailing_comma_repair_preserves_block_scalar_prose():
    text = (
        "answer: |\n"
        "  Revenue rose, then [briefly] fell,\n"
        "claims:\n"
        "  - text: A claim.\n"
        "    type: fact,\n"
        "missing: []\n"
    )
    envelope = parse_envelope(text)
    assert envelope.answer == "Revenue rose, then [briefly] fell,"
    assert envelope.claims[0].type == "fact"


def test_repair_handles_block_scalar_inside_claims():
    text = (
        "answer: hi\n"
        "claims:\n"
        "  - text: |\n"
        "      Costs fell, because fuel,\n"
        "    type: fact,\n"
        "    citations: []\n"
        "missing: []\n"
    )
    envelope = parse_envelope(text)
    assert envelope.claims[0].text == "Costs fell, because fuel,"
    assert envelope.claims[0].type == "fact"


def test_unbalanced_flow_collection_repair():
    text = (
        "answer: hi\n"
        'claims: [{text: "A causal sentence.", type: hypothesis, '
        "citations: [{tool: company_facts}"
    )
    envelope = parse_envelope(text)
    assert envelope.claims[0].type == "hypothesis"
    assert envelope.claims[0].citations[0].tool == "company_facts"


def test_missing_claim_type_is_rejected():
    text = (
        "answer: hi\n"
        "claims:\n"
        "  - text: A claim with no label.\n"
        "    citations: []\n"
    )
    with pytest.raises(EnvelopeError) as excinfo:
        parse_envelope(text)
    assert "schema" in str(excinfo.value)


def test_completely_malformed_raises_and_never_returns_empty():
    with pytest.raises(EnvelopeError):
        parse_envelope('answer: "unterminated')
    with pytest.raises(EnvelopeError):
        parse_envelope("")
    with pytest.raises(EnvelopeError):
        parse_envelope("- just\n- a\n- list\n")


def test_missing_answer_key_is_rejected():
    with pytest.raises(EnvelopeError):
        parse_envelope("claims: []\nmissing: []\n")


def test_claims_null_is_tolerated():
    envelope = parse_envelope("answer: hi\nclaims: null\nmissing: null\n")
    assert envelope.claims == []
    assert envelope.missing == []


def test_unknown_missing_state_is_rejected():
    text = (
        "answer: hi\nclaims: []\nmissing:\n"
        "  - {item: coal_production, state: maybe_later, evidence: {}}\n"
    )
    with pytest.raises(EnvelopeError):
        parse_envelope(text)


def test_render_round_trip():
    envelope = AnswerEnvelope.model_validate({
        "answer": "Coal production was 5.3 Mt.",
        "claims": [{
            "text": "Coal production was 5.3 Mt.",
            "type": "fact",
            "citations": [{
                "tool": "operational_lookup",
                "ticker": "ITMG",
                "metric": "coal_production",
                "doc": "itmg_2026-08-10_news_release",
                "page": 1,
            }],
        }],
        "missing": [],
    })
    rendered = render_envelope_yaml(envelope)
    assert "answer:" in rendered
    assert parse_envelope(rendered) == envelope


# ---------------------------------------------------------------------------
# Two-surface envelope: analysis_note, document citations, coverage
# ---------------------------------------------------------------------------


def test_analysis_note_and_document_citation_parse():
    text = """answer: |
  The note treats the cash-flow collapse as a timing artifact.
claims:
  - text: "The note treats the cash-flow collapse as a timing artifact."
    type: analysis_note
    citations:
      - {tool: analysis_document, document: itmg-q2-2026-analysis,
         section: "1. Executive Summary", line: 18,
         quote: "We treat this as a timing artifact rather than operating deterioration."}
missing: []
"""
    envelope = parse_envelope(text)
    claim = envelope.claims[0]
    assert claim.type == "analysis_note"
    citation = claim.citations[0]
    assert citation.document == "itmg-q2-2026-analysis"
    assert citation.section == "1. Executive Summary"
    assert citation.line == 18
    assert citation.quote.startswith("We treat this")


def test_citation_document_fields_are_stripped_and_line_coerced():
    envelope = AnswerEnvelope.model_validate({
        "answer": "The note says so.",
        "claims": [{
            "text": "The note says so.",
            "type": "analysis_note",
            "citations": [{
                "tool": " analysis_search ",
                "document": "  itmg-q2-2026-analysis  ",
                "section": "  1. Executive Summary ",
                "line": "line 18",
                "quote": "  We treat this as a timing artifact.  ",
            }],
        }],
        "missing": [],
    })
    citation = envelope.claims[0].citations[0]
    assert citation.tool == "analysis_search"
    assert citation.document == "itmg-q2-2026-analysis"
    assert citation.section == "1. Executive Summary"
    assert citation.line == 18
    assert citation.quote == "We treat this as a timing artifact."


def test_not_in_document_missing_state_parses():
    text = (
        "answer: This note does not cover strip ratio.\n"
        "claims: []\n"
        "missing:\n"
        "  - {item: strip_ratio, state: not_in_document,\n"
        "     evidence: {sections_searched: 14}}\n"
    )
    envelope = parse_envelope(text)
    assert envelope.missing[0].state == "not_in_document"


def test_coverage_block_parses_and_normalizes_tickers():
    text = """answer: ITMG and PTBA were compared.
claims: []
missing: []
coverage:
  scope: supported_set
  supported_tickers: "24"
  compared_tickers: [itmg, " PTBA "]
  caveat: Supported names only; not an IDX-wide sector screen.
"""
    envelope = parse_envelope(text)
    assert envelope.coverage is not None
    assert envelope.coverage.supported_tickers == 24
    assert envelope.coverage.compared_tickers == ["ITMG", "PTBA"]
    assert "not an IDX-wide" in envelope.coverage.caveat


def test_coverage_null_is_tolerated():
    envelope = parse_envelope("answer: hi\nclaims: []\nmissing: []\ncoverage: null\n")
    assert envelope.coverage is None


def test_render_round_trip_preserves_document_claims_and_coverage():
    envelope = AnswerEnvelope.model_validate({
        "answer": "The note says revenue was 1,000,491k.",
        "claims": [{
            "text": "The note says revenue was 1,000,491k.",
            "type": "analysis_note",
            "citations": [{
                "tool": "analysis_document",
                "document": "itmg-q2-2026-analysis",
                "section": "1. Executive Summary",
                "line": 14,
                "quote": "net revenue of US$1,000,491k",
            }],
        }],
        "missing": [{
            "item": "strip_ratio",
            "state": "not_in_document",
            "evidence": {"sections_searched": 14},
        }],
        "coverage": {
            "scope": "document",
            "supported_tickers": 24,
            "compared_tickers": ["ITMG"],
            "caveat": "secondary note",
        },
    })
    rendered = render_envelope_yaml(envelope)
    parsed = parse_envelope(rendered)
    assert parsed == envelope
    assert parsed.claims[0].citations[0].quote == "net revenue of US$1,000,491k"
    assert parsed.coverage is not None


# ---------------------------------------------------------------------------
# Compact claims: `sentence: N` instead of repeating the sentence text
# ---------------------------------------------------------------------------


COMPACT = """answer: |
  Coal production was 5.3 Mt. Production rose 13% QoQ.
claims:
  - sentence: 2
    type: fact
    citations:
      - {tool: operational_lookup, ticker: ITMG, metric: coal_production}
missing: []
"""


def test_compact_claim_resolves_sentence_index():
    envelope = parse_envelope(COMPACT)
    claim = envelope.claims[0]
    assert claim.sentence == 2
    assert claim.text == "Production rose 13% QoQ."
    # The dump stays usable for the trace/UI: every claim carries resolved text.
    dumped = envelope.model_dump()
    assert dumped["claims"][0]["text"] == "Production rose 13% QoQ."


def test_compact_claim_drops_out_of_range_index():
    """A stale index is a coverage problem for the audit, not a parse failure."""
    text = (
        "answer: One sentence only.\n"
        "claims:\n"
        "  - sentence: 4\n"
        "    type: hypothesis\n"
    )
    envelope = parse_envelope(text)
    assert envelope.claims == []


def test_compact_claim_drops_zero_index():
    text = (
        "answer: One sentence only.\n"
        "claims:\n"
        "  - sentence: 0\n"
        "    type: hypothesis\n"
    )
    envelope = parse_envelope(text)
    assert envelope.claims == []


def test_compact_claim_with_text_ignores_stale_index():
    text = (
        "answer: One sentence only.\n"
        "claims:\n"
        "  - sentence: 99\n"
        "    text: One sentence only.\n"
        "    type: hypothesis\n"
    )
    claim = parse_envelope(text).claims[0]
    assert claim.text == "One sentence only."
    assert claim.sentence is None


def test_claim_with_neither_text_nor_sentence_is_rejected():
    text = "answer: hi\nclaims:\n  - type: hypothesis\n"
    with pytest.raises(EnvelopeError) as excinfo:
        parse_envelope(text)
    assert "sentence" in str(excinfo.value)


def test_claim_with_both_text_and_sentence_keeps_text():
    text = (
        "answer: One. Two.\n"
        "claims:\n"
        "  - sentence: 2\n"
        "    text: Two (sub-sentence quote).\n"
        "    type: hypothesis\n"
    )
    claim = parse_envelope(text).claims[0]
    assert claim.text == "Two (sub-sentence quote)."
    assert claim.sentence == 2


def test_claim_with_both_text_and_bad_sentence_keeps_text():
    text = (
        "answer: One.\n"
        "claims:\n"
        "  - sentence: 9\n"
        "    text: One.\n"
        "    type: hypothesis\n"
    )
    claim = parse_envelope(text).claims[0]
    assert claim.text == "One."
    assert claim.sentence is None


def test_split_sentences_shared_with_verify_normalization():
    assert split_sentences("**One.** Two") == ["One.", "Two"]
    assert split_sentences("# Q2 summary\nRevenue rose.") == [
        "Q2 summary", "Revenue rose.",
    ]
    assert split_sentences("") == []
    # Markdown markers are blanked (not deleted), exactly as verify always did.
    assert split_sentences("| coal | 5.3 Mt |")[0].split() == ["coal", "5.3", "Mt"]


def test_compact_claim_round_trips_through_render():
    envelope = parse_envelope(COMPACT)
    reparsed = parse_envelope(render_envelope_yaml(envelope))
    assert reparsed == envelope
    assert reparsed.claims[0].text == "Production rose 13% QoQ."



# ---------------------------------------------------------------------------
# Claims-only replies (two-phase phase 2): merge with already-written prose
# ---------------------------------------------------------------------------


def test_claims_only_reply_merges_with_the_phase_one_prose():
    answer = "Coal production was 5.3 Mt. Production rose 13% QoQ."
    text = """claims:
  - sentence: 2
    type: fact
    citations:
      - {tool: operational_lookup, ticker: ITMG, metric: coal_production}
missing: []
"""
    envelope = parse_claims_envelope(text, answer)

    assert envelope.answer == answer
    assert envelope.claims[0].sentence == 2
    assert envelope.claims[0].text == "Production rose 13% QoQ."


def test_claims_only_reply_ignores_a_returned_answer():
    text = """answer: |
  A different answer the model tried to rewrite.
claims:
  - text: "Coal production was 5.3 Mt."
    type: fact
missing: []
"""
    envelope = parse_claims_envelope(text, "Coal production was 5.3 Mt.")

    assert envelope.answer == "Coal production was 5.3 Mt."
    assert envelope.claims[0].text == "Coal production was 5.3 Mt."


def test_claims_only_reply_strips_a_fence_and_a_preamble():
    text = """Here are the claims:
```yaml
claims:
  - sentence: 1
    type: hypothesis
missing: []
```
"""
    envelope = parse_claims_envelope(text, "One sentence only.")
    assert envelope.claims[0].text == "One sentence only."


def test_claims_only_reply_without_any_claims_keys_is_rejected():
    with pytest.raises(EnvelopeError) as excinfo:
        parse_claims_envelope("answer: hi\n", "prose")
    assert "claims" in str(excinfo.value)


def test_claims_only_reply_with_empty_prose_is_rejected():
    with pytest.raises(EnvelopeError) as excinfo:
        parse_claims_envelope("claims: []\nmissing: []\n", "   ")
    assert "empty answer" in str(excinfo.value)


def test_claims_only_reply_drops_an_out_of_range_sentence():
    envelope = parse_claims_envelope(
        "claims:\n  - sentence: 9\n    type: fact\n", "One. Two.",
    )
    assert envelope.claims == []
