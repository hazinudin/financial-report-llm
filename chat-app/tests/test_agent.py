"""Agent orchestration: retry, refusal, error, and trace writing.

``pi_agent.run_agent`` is monkeypatched; no pi binary, network, or LLM call is
made. Traces go to a temporary directory via ``paths.TRACES_DIR``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from chat_app import agent, paths

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import pi_agent  # noqa: E402

DOC = "itmg_2026-08-10_news_release"

OP_PAYLOAD = {
    "ok": True,
    "tool": "operational_lookup",
    "request": {"ticker": "ITMG", "period": "Q2", "year": 2026, "metric": None},
    "data": {
        "ticker": "ITMG",
        "period": "Q2",
        "year": 2026,
        "metric": None,
        "state": "available",
        "entries": [{
            "metric": "coal_production",
            "scope": "Q2-2026",
            "value": 5.3,
            "unit": "Mt",
            "page": 1,
            "source_doc": DOC,
            "status": "preliminary",
        }],
        "drivers": [],
        "documents": [{"id": DOC, "pages": 8, "status": "captured"}],
        "evidence": {},
    },
    "ledger": [{
        "value": 5.3,
        "kind": "count",
        "variants": [5.3],
        "source": DOC,
        "meta": {
            "ticker": "ITMG", "period": "Q2", "year": 2026,
            "metric": "coal_production", "source_doc": DOC, "page": 1,
        },
    }],
    "caveats": [],
}

TOOL_TRACE = [{
    "tool_call_id": "call_1",
    "tool_name": "operational_lookup",
    "arguments": {"ticker": "ITMG", "period": "Q2", "year": 2026},
    "result": {
        "content": [{"type": "text", "text": json.dumps(OP_PAYLOAD)}],
    },
}]

VALID = f"""answer: |
  ITMG produced 5.3 Mt of coal in Q2 2026.
claims:
  - text: "ITMG produced 5.3 Mt of coal in Q2 2026."
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}
missing: []
"""

TAMPERED = VALID.replace("5.3 Mt", "6.9 Mt")


def _result(content: str, *, error: str | None = None) -> dict:
    return {
        "content": content,
        "tool_trace": TOOL_TRACE if not error else [],
        "usage": {"prompt_tokens": 1200, "completion_tokens": 300, "cost_usd": 0.0},
        "error": error,
        "pi_meta": {"model": "deepseek-v4.1-flash", "tool_calls": {}},
    }


def _install(monkeypatch, responses: list[dict]) -> list[dict]:
    calls: list[dict] = []

    def fake(system_prompt, user_prompt, *, cfg, run_dir, project_root=None):
        calls.append({
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "cfg": cfg,
            "run_dir": str(run_dir),
        })
        index = min(len(calls) - 1, len(responses) - 1)
        return responses[index]

    monkeypatch.setattr(pi_agent, "run_agent", fake)
    return calls


@pytest.fixture
def traces(tmp_path, monkeypatch):
    directory = tmp_path / "traces"
    directory.mkdir()
    monkeypatch.setattr(paths, "TRACES_DIR", directory)
    return directory


def test_valid_answer_is_answered_and_traced(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask(
        "How much coal did ITMG produce in Q2 2026?",
        ticker="ITMG", period="Q2", year=2026,
    )

    assert result["status"] == "answered"
    assert result["error"] is None
    assert result["trace_id"].endswith(tuple("0123456789abcdef"))
    assert result["envelope"]["answer"].startswith("ITMG produced")
    assert result["verification"]["ok"] is True
    assert result["tool_calls"] == ["operational_lookup"]
    assert result["usage"] == {
        "prompt_tokens": 1200,
        "completion_tokens": 300,
        # config.yaml chat.pricing: 0.30 in / 1.20 out per Mtok.
        "cost_usd": pytest.approx(0.00072, rel=1e-3),
    }
    assert result["model"] == "deepseek/deepseek-v4.1-flash"
    assert result["duration_s"] >= 0

    assert len(calls) == 1
    assert calls[0]["cfg"]["thinking"] == "off"
    assert calls[0]["cfg"]["timeout_sec"] == 180.0
    assert "duckdb_query" not in calls[0]["cfg"]["tools"]
    assert "company_facts" in calls[0]["cfg"]["tools"]
    assert "How much coal" in calls[0]["user_prompt"]
    assert "## Supported set" in calls[0]["system_prompt"]
    assert "envelope" in calls[0]["user_prompt"]

    files = list(traces.glob("*.json"))
    assert len(files) == 1
    trace = json.loads(files[0].read_text(encoding="utf-8"))
    assert files[0].stem == result["trace_id"]
    assert trace["trace_id"] == result["trace_id"]
    assert trace["status"] == "answered"
    assert trace["refused"] is False
    assert trace["retries"] == 0
    assert trace["envelope"] is not None
    assert trace["tool_trace"]
    assert trace["ledger"]
    assert trace["estimated_cost_usd"] > 0
    assert trace["cost_source"] == "estimated"
    assert trace["duration_s"] == result["duration_s"]


def test_malformed_envelope_twice_is_refused(monkeypatch, traces):
    calls = _install(monkeypatch, [
        _result("```yaml\nanswer: [unterminated\n```"),
        _result("```yaml\nanswer: [still unterminated\n```"),
    ])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "refused"
    assert result["envelope"] is None
    assert len(calls) == 2
    assert calls[0]["user_prompt"] != calls[1]["user_prompt"]
    assert "not valid YAML" in calls[1]["user_prompt"]
    assert result["verification"]["violations"][0]["type"] == "envelope_invalid"

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["refused"] is True
    assert trace["status"] == "refused"
    assert trace["retries"] == 1
    assert trace["parse_errors"]


def test_tampered_number_twice_is_refused(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(TAMPERED), _result(TAMPERED)])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "refused"
    assert result["envelope"] is None
    assert len(calls) == 2
    assert "6.9" in calls[1]["user_prompt"]
    assert "not in any tool result" in calls[1]["user_prompt"]
    types = [item["type"] for item in result["verification"]["violations"]]
    assert "number_not_in_ledger" in types

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["refused"] is True
    assert trace["violations"]


def test_retry_carries_evidence_and_the_rejected_answer(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(TAMPERED), _result(TAMPERED)])

    result = agent.ask(
        "How much coal did ITMG produce in Q2 2026?",
    )

    assert result["status"] == "refused"
    assert len(calls) == 2
    retry = calls[1]["user_prompt"]
    assert "## Evidence already collected" in retry
    assert "operational_lookup" in retry
    assert "5.3" in retry
    assert "## Previous answer (rejected)" in retry
    assert "6.9" in retry
    # The correction stays last; the retry context goes after the fast path.
    assert retry.index("## Fast path") < retry.index("## Evidence already collected")
    assert retry.rindex("## Correction") > retry.rindex("## Previous answer (rejected)")


def test_retry_prompt_is_bounded_with_large_tool_results(monkeypatch, traces):
    synthetic_body = "SYNTHETIC-" + "S" * 40_000
    real_body = "REAL-" + "R" * 40_000
    tool_trace = [
        {
            "tool_call_id": "context:list_companies",
            "tool_name": "list_companies",
            "arguments": {},
            "result": {"content": [{"type": "text", "text": synthetic_body}]},
            "context": True,
        },
        {
            "tool_call_id": "context:analysis_document",
            "tool_name": "analysis_document",
            "arguments": {"document_id": DOC},
            "result": {"content": [{"type": "text", "text": synthetic_body}]},
            "context": True,
            "synthetic": True,
        },
        {
            **TOOL_TRACE[0],
            "result": {"content": [{"type": "text", "text": real_body}]},
        },
    ]
    first = {**_result(TAMPERED), "tool_trace": tool_trace}
    calls = _install(monkeypatch, [first, _result(TAMPERED)])

    result = agent.ask(
        "How much coal did ITMG produce in Q2 2026?",
    )

    assert result["status"] == "refused"
    retry = calls[1]["user_prompt"]
    assert len(retry) < 30_000
    # Prefetched context and synthetic entries are not replayed as evidence.
    assert "SYNTHETIC-" not in retry
    # A single oversized tool result is truncated per entry.
    assert "REAL-" in retry
    assert "R" * 5_000 not in retry
    assert "operational_lookup" in retry


def test_parse_failure_retry_includes_yaml_error_and_evidence(monkeypatch, traces):
    calls = _install(monkeypatch, [
        _result("```yaml\nanswer: [unterminated\n```"),
        _result("```yaml\nanswer: [still unterminated\n```"),
    ])

    result = agent.ask(
        "How much coal did ITMG produce in Q2 2026?",
    )

    assert result["status"] == "refused"
    retry = calls[1]["user_prompt"]
    assert "## Evidence already collected" in retry
    assert "operational_lookup" in retry
    assert "5.3" in retry
    assert "## Previous answer (rejected)" in retry
    assert "unterminated" in retry
    assert "not valid YAML" in retry


def test_tool_trace_entries_are_tagged_with_the_attempt(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(TAMPERED), _result(TAMPERED)])

    agent.ask("How much coal did ITMG produce?")

    assert len(calls) == 2
    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    attempts = [
        entry.get("attempt") for entry in trace["tool_trace"]
        if not entry.get("context")
    ]
    assert attempts == [1, 2]
    assert all(
        entry.get("tool_name") == "operational_lookup"
        for entry in trace["tool_trace"]
        if not entry.get("context")
    )


def test_retry_context_prefers_the_most_recent_attempt_and_skips_context():
    state = {"tool_trace": [
        {
            "tool_name": "operational_lookup",
            "arguments": {"ticker": "OLD"},
            "result": {"content": [{"type": "text", "text": "old-result"}]},
            "attempt": 1,
        },
        {
            "tool_name": "operational_lookup",
            "arguments": {"ticker": "NEW"},
            "result": {"content": [{"type": "text", "text": "new-result"}]},
            "attempt": 2,
        },
        {
            "tool_name": "list_companies",
            "arguments": {},
            "result": {"content": [{"type": "text", "text": "context-body"}]},
            "context": True,
        },
        {
            "tool_name": "analysis_document",
            "arguments": {},
            "result": {"content": [{"type": "text", "text": "synthetic-body"}]},
            "context": True,
            "synthetic": True,
        },
    ]}

    context = agent._retry_context(state, previous_content="raw output")

    assert "## Evidence already collected" in context
    assert "new-result" in context
    assert "old-result" not in context
    assert "context-body" not in context
    assert "synthetic-body" not in context
    assert "## Previous answer (rejected)" in context
    assert "raw output" in context
    assert len(context) <= agent.RETRY_CONTEXT_MAX_CHARS


def test_unmatched_citation_is_repaired_without_a_retry(monkeypatch, traces):
    """A deterministically repairable citation failure is fixed, not re-asked."""
    extra = VALID.replace(
        f"""      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}""",
        f"""      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}
      - {{tool: company_facts, ticker: PTBA, metric: revenue}}""",
    )
    calls = _install(monkeypatch, [_result(extra)])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "answered"
    assert len(calls) == 1
    assert result["verification"]["ok"] is True
    assert result["verification"].get("repaired") is True
    assert len(result["envelope"]["claims"][0]["citations"]) == 1


def test_pi_error_is_status_error(monkeypatch, traces):
    calls = _install(monkeypatch, [_result("", error="pi timed out after 180s")])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "error"
    assert "timed out" in result["error"]
    assert result["envelope"] is None
    assert len(calls) == 1

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["status"] == "error"
    assert "timed out" in trace["error"]


def test_replay_reproduces_the_verdict(monkeypatch, traces, capsys):
    _install(monkeypatch, [_result(VALID)])
    result = agent.ask("How much coal did ITMG produce?")

    stored = next(traces.glob("*.json"))
    code = agent._replay(str(stored))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "answered"
    assert payload["citations"]["ok"] == 1
    assert result["status"] == "answered"


def test_analysis_context_is_prefetched_and_injected(monkeypatch, traces):
    payload = {
        "ok": True,
        "tool": "analysis_context",
        "data": {
            "ticker": "ITMG",
            "period": "Q2",
            "year": 2026,
            "analysis_context": [{
                "id": "chunk1",
                "path": "analysis/ITMG Q2 2026 Analysis.md",
                "title": "ITMG Q2 2026 Analysis",
                "section": "11.1 Near-Term Risks",
                "snippet": "3Q26 strip ratio of 14.0x and a 5.9 Mt production plan.",
                "secondary": True,
            }],
        },
        "ledger": [],
        "caveats": [],
    }
    monkeypatch.setattr(agent.tools, "analysis_context", lambda *a, **k: payload)
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask(
        "What operational risks does ITMG flag?",
        ticker="ITMG", period="Q2", year=2026,
    )

    assert result["status"] == "answered"
    assert "Analyst context (prefetched, secondary)" in calls[0]["user_prompt"]
    assert "3Q26 strip ratio of 14.0x" in calls[0]["user_prompt"]
    assert "analysis/ITMG Q2 2026 Analysis.md" in calls[0]["user_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text())
    assert any(
        entry.get("tool_name") == "analysis_context" and entry.get("context")
        for entry in trace["tool_trace"]
    )
    # Prefetch is runner context, not a model tool call in the public summary.
    assert "analysis_context" not in result["tool_calls"]


# ---------------------------------------------------------------------------
# Mode routing: document workspace vs research desk
# ---------------------------------------------------------------------------

DOC_ID = "itmg-q2-2026-analysis"
DOC_CONTENT = (
    "## 1. Executive Summary\n"
    "We treat this as a timing artifact rather than operating deterioration.\n"
    "## 5. Cash Flow Analysis\n"
    "Operating cash flow collapsed −77.8% to US$62,376k.\n"
)


def _document_payload(
    *, document_id: str = DOC_ID, content: str = DOC_CONTENT,
) -> dict:
    return {
        "ok": True,
        "tool": "analysis_document",
        "request": {"document_id": document_id, "section": None},
        "data": {
            "document_id": document_id,
            "title": "ITMG Q2 2026 Analysis",
            "path": "analysis/ITMG Q2 2026 Analysis.md",
            "kind": "company",
            "ticker": "ITMG",
            "period": "Q2",
            "year": 2026,
            "updated_at": "2026-10-22T21:26:00Z",
            "sha256": "abc123",
            "bytes": len(content.encode("utf-8")),
            "words": len(content.split()),
            "tags": ["ITMG"],
            "aliases": [],
            "sections": [
                {"id": "1-executive-summary", "title": "1. Executive Summary",
                 "level": 2, "line": 1},
                {"id": "5-cash-flow-analysis", "title": "5. Cash Flow Analysis",
                 "level": 2, "line": 3},
            ],
            "content": content,
        },
        "ledger": [],
        "caveats": [],
    }


DOC_VALID = """answer: |
  The note reports operating cash flow of US$62,376k and calls the drop a timing artifact.
claims:
  - text: "The note reports operating cash flow of US$62,376k and calls the drop a timing artifact."
    type: analysis_note
    citations:
      - {tool: analysis_document, document: itmg-q2-2026-analysis,
         section: "5. Cash Flow Analysis", line: 4,
         quote: "Operating cash flow collapsed −77.8% to US$62,376k."}
missing: []
"""


def test_invalid_mode_is_refused_before_the_runner(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("Compare the coal names.", mode="sector")

    assert result["status"] == "refused"
    assert result["mode"] == "sector"
    assert result["error"] and "mode" in result["error"]
    assert result["verification"]["violations"][0]["type"] == "invalid_mode"
    assert calls == []


def test_document_mode_requires_a_document_id(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("What does the note say?", mode="document")

    assert result["status"] == "refused"
    assert "document_id" in result["error"]
    assert result["document"] is None
    assert calls == []


def test_desk_mode_forbids_document_id_and_selection(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    for kwargs in (
        {"document_id": DOC_ID},
        {"selection": {"section": "1", "line": 1, "quote": "x"}},
    ):
        result = agent.ask("What does the note say?", mode="desk", **kwargs)
        assert result["status"] == "refused"
        assert "desk mode" in result["error"]
    assert calls == []


def test_document_mode_routes_prefetches_and_echoes_selection(
    monkeypatch, traces,
):
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    calls = _install(monkeypatch, [_result(DOC_VALID)])

    result = agent.ask(
        "What does the note say about the cash-flow swing?",
        mode="document",
        document_id=DOC_ID,
        ticker="PTBA",
        period="Q1",
        year=2025,
        selection={
            "section": "1. Executive Summary",
            "line": 2,
            "quote": "We treat this as a timing artifact",
        },
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["mode"] == "document"
    assert result["document"]["id"] == DOC_ID
    assert result["document"]["ticker"] == "ITMG"
    assert "content" not in result["document"]
    assert result["selection_echo"]["status"] == "used"
    assert result["selection_dropped"] is False
    assert result["coverage"] is None
    assert "analysis_document" not in result["tool_calls"]
    assert "analysis_context" not in result["tool_calls"]
    assert result["verification"]["citations"]["document_ok"] == 1

    assert len(calls) == 1
    cfg = calls[0]["cfg"]
    assert cfg["thinking"] == "off"
    assert cfg["timeout_sec"] == 180.0
    assert "analysis_document" in cfg["tools"]
    assert "analysis_search" in cfg["tools"]
    assert "company_facts" in cfg["tools"]
    assert "list_companies" not in cfg["tools"]
    assert "duckdb_query" not in cfg["tools"]

    assert "Document mode" in calls[0]["system_prompt"]
    prompt = calls[0]["user_prompt"]
    assert "UNTRUSTED DOCUMENT" in prompt
    assert "We treat this as a timing artifact" in prompt
    assert "Operating cash flow collapsed" in prompt
    assert "## Selected passage" in prompt

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["mode"] == "document"
    assert trace["document_id"] == DOC_ID
    assert trace["selection_echo"]["status"] == "used"
    assert trace["document"]["id"] == DOC_ID
    # The client-supplied scope is ignored; the document sets it.
    assert trace["focus"] == {"ticker": "ITMG", "period": "Q2", "year": 2026}
    synthetic = [
        entry for entry in trace["tool_trace"]
        if entry.get("tool_name") == "analysis_document" and entry.get("synthetic")
    ]
    assert len(synthetic) == 1
    assert synthetic[0]["arguments"]["document_id"] == DOC_ID
    assert trace["document_ledger"]


def test_document_selection_that_is_not_in_the_note_is_dropped(
    monkeypatch, traces,
):
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    calls = _install(monkeypatch, [_result(DOC_VALID)])

    result = agent.ask(
        "What does this mean?",
        mode="document",
        document_id=DOC_ID,
        selection={
            "section": "1. Executive Summary",
            "line": 99,
            "quote": "This passage is not in the note.",
        },
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["selection_dropped"] is True
    assert result["selection_echo"]["status"] == "dropped"
    assert "not an exact substring" in result["selection_echo"]["reason"]
    assert "This passage is NOT in the note" in calls[0]["user_prompt"]
    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["selection_dropped"] is True


def test_document_not_found_is_a_designed_refusal(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_document", lambda document_id: None)
    calls = _install(monkeypatch, [_result(DOC_VALID)])

    result = agent.ask("What does the note say?", mode="document",
                       document_id="missing-note")

    assert result["status"] == "refused"
    assert result["mode"] == "document"
    assert result["document"] is None
    assert "document not found" in result["error"]
    assert result["verification"]["violations"][0]["type"] == "document_not_found"
    assert calls == []


def test_desk_mode_uses_desk_prompt_allowlist_and_coverage(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask(
        "Why did ITMG's cash flow move in Q2 2026?",
        ticker="ITMG", period="Q2", year=2026,
    )

    assert result["status"] == "answered"
    assert result["mode"] == "desk"
    assert result["document"] is None
    assert result["selection_echo"] is None
    assert result["coverage"]["scope"] == "supported_set"
    assert result["coverage"]["supported_tickers"] >= 20
    assert result["coverage"]["caveat"]
    # "why" is a complex marker: full desk surface, not the fast path.
    assert result["route"]["fast_path"] is False
    assert result["route"]["thinking"] == "high"

    cfg = calls[0]["cfg"]
    assert "list_companies" in cfg["tools"]
    assert "compare_supported" in cfg["tools"]
    assert "analysis_document" in cfg["tools"]
    assert "Desk mode" in calls[0]["system_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["mode"] == "desk"
    assert trace["document_id"] is None
    assert trace["coverage"]["scope"] == "supported_set"


def test_large_document_prefetch_is_outline_plus_relevant_sections():
    filler = "cost detail line with no key terms.\n" * 2400  # > 64 KB
    content = (
        "## 1. Executive Summary\n"
        "The cash-flow swing was a timing artifact.\n"
        "## 2. Cost Detail\n"
        + filler
        + "## 3. Revenue\n"
        "Revenue was stable.\n"
    )
    document = {
        "id": DOC_ID,
        "title": "Big note",
        "content": content,
        "sections": agent._sections_with_text(
            content,
            [
                {"id": "1-executive-summary", "title": "1. Executive Summary",
                 "level": 2, "line": 1},
                {"id": "2-cost-detail", "title": "2. Cost Detail",
                 "level": 2, "line": 3},
                {"id": "3-revenue", "title": "3. Revenue", "level": 2,
                 "line": 3 + filler.count("\n") + 1},
            ],
        ),
    }

    payload, block = agent._document_prefetch(
        document, "What does the note say about cash flow?", None,
    )

    data = payload["data"]
    assert data["prefetch"] == "outline+relevant"
    assert data["content"] == ""
    assert data["selected_sections"]
    assert "larger than the prefetch cap" in block
    assert "UNTRUSTED DOCUMENT" in block
    assert len(json.dumps(data).encode("utf-8")) < 64 * 1024


CMP_PAYLOAD = {
    "ok": True,
    "tool": "compare_supported",
    "request": {"tickers": ["ITMG", "PTBA"], "metrics": ["coal_production"],
                "period": "Q2", "year": 2026},
    "data": {
        "period": "Q2", "year": 2026, "columns": ["ITMG", "PTBA"],
        "rows": [{
            "metric": "coal_production", "unit": "Mt",
            "cells": [
                {"ticker": "ITMG", "value": 5.3, "status": "ok", "unit": "Mt",
                 "period_type": "quarter",
                 "source": "operational:itmg_2026-08-10_news_release:1"},
                {"ticker": "PTBA", "value": 19.45, "status": "ok", "unit": "Mt",
                 "period_type": "cumulative",
                 "source": "operational:ptba_2026-09-01_press_release:3"},
            ],
        }],
        "excluded": [],
        "coverage": {"requested": 2, "answered": 2, "scope": "supported_set"},
    },
    "ledger": [
        {"value": 5.3, "unit": "Mt", "kind": "count", "variants": [5.3],
         "source": "operational:itmg_2026-08-10_news_release:1",
         "meta": {"ticker": "ITMG", "metric": "coal_production",
                  "period": "Q2", "year": 2026,
                  "source_doc": "itmg_2026-08-10_news_release", "page": 1}},
        {"value": 19.45, "unit": "Mt", "kind": "count", "variants": [19.45],
         "source": "operational:ptba_2026-09-01_press_release:3",
         "meta": {"ticker": "PTBA", "metric": "coal_production",
                  "period": "Q2", "year": 2026,
                  "source_doc": "ptba_2026-09-01_press_release", "page": 3}},
    ],
    "caveats": [],
}

CMP_VALID = """answer: |
  Q2 2026 coal production was reported for both names: ITMG 5.3 Mt and PTBA 19.45 Mt.
claims:
  - text: "Q2 2026 coal production was reported for both names: ITMG 5.3 Mt and PTBA 19.45 Mt."
    type: fact
    citations:
      - {tool: compare_supported, ticker: ITMG, metric: coal_production, page: 1}
      - {tool: compare_supported, ticker: PTBA, metric: coal_production, page: 3}
missing: []
coverage:
  scope: supported_set
  supported_tickers: 24
  compared_tickers: [ITMG, PTBA]
  caveat: "Supported names only; not an IDX-wide sector screen."
"""


def test_desk_comparison_cites_every_ticker_and_returns_coverage(
    monkeypatch, traces,
):
    tool_trace = [{
        "tool_call_id": "call_compare",
        "tool_name": "compare_supported",
        "arguments": {"tickers": ["ITMG", "PTBA"],
                      "metrics": ["coal_production"],
                      "period": "Q2", "year": 2026},
        "result": {"content": [{"type": "text", "text": json.dumps(CMP_PAYLOAD)}]},
    }]
    calls = _install(monkeypatch, [{
        "content": CMP_VALID,
        "tool_trace": tool_trace,
        "usage": {"prompt_tokens": 900, "completion_tokens": 200,
                  "cost_usd": 0.0},
        "error": None,
    }])

    result = agent.ask(
        "Compare Q2 2026 coal production across ITMG and PTBA.", mode="desk",
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["coverage"]["compared_tickers"] == ["ITMG", "PTBA"]
    assert result["coverage"]["supported_tickers"] == 24
    assert "not an IDX-wide" in result["coverage"]["caveat"]
    assert result["verification"]["citations"]["ok"] == 2
    assert result["verification"]["advice_ok"] is True
    assert "Desk mode" in calls[0]["system_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["coverage"]["compared_tickers"] == ["ITMG", "PTBA"]
    assert trace["mode"] == "desk"


# ---------------------------------------------------------------------------
# Request routing: fast single-name path and thinking level
# ---------------------------------------------------------------------------


def test_simple_single_name_question_takes_the_fast_path(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("Show me the highlight of ITMG operation in Q2 2026")

    assert result["status"] == "answered", result["verification"]["violations"]
    route = result["route"]
    assert route["fast_path"] is True
    assert route["thinking"] == "off"
    for tool in ("operational_lookup", "company_facts", "read_document"):
        assert tool in route["tools"]
    # No derived/single-quarter ask: no period_derive, no note tools.
    assert "period_derive" not in route["tools"]
    assert "analysis_context" not in route["tools"]
    assert "analysis_search" not in route["tools"]

    cfg = calls[0]["cfg"]
    assert cfg["thinking"] == "off"
    assert cfg["tools"] == route["tools"]
    assert "## Fast path" in calls[0]["user_prompt"]
    assert "at most 4 tool calls" in calls[0]["user_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["route"]["fast_path"] is True
    assert trace["route"]["thinking"] == "off"
    assert trace["route"]["tools"] == route["tools"]


def test_derived_single_quarter_question_keeps_period_derive(monkeypatch, traces):
    _install(monkeypatch, [_result(VALID)])

    result = agent.ask("Derive the QoQ change in ITMG coal production for Q2 2026")

    assert result["route"]["fast_path"] is True
    assert "period_derive" in result["route"]["tools"]


def test_comparison_question_uses_high_thinking_and_the_full_desk_surface(
    monkeypatch, traces,
):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("Compare Q2 2026 coal production across ITMG and PTBA.")

    route = result["route"]
    assert route["fast_path"] is False
    assert route["thinking"] == "high"
    assert set(route["tools"]) == set(agent.DESK_TOOLS)
    assert calls[0]["cfg"]["thinking"] == "high"
    assert "## Fast path" not in calls[0]["user_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["route"]["fast_path"] is False
    assert trace["route"]["thinking"] == "high"


def test_two_named_tickers_without_a_marker_are_not_fast(monkeypatch, traces):
    _install(monkeypatch, [_result(VALID)])

    result = agent.ask("ITMG and PTBA Q2 2026 coal production")

    assert result["route"]["fast_path"] is False
    assert result["route"]["thinking"] == "off"


def test_note_analysis_question_stays_off_the_fast_path(monkeypatch, traces):
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("What is the outlook and key risk for ITMG in Q2 2026?")

    route = result["route"]
    assert route["fast_path"] is False
    assert route["thinking"] == "high"
    assert "analysis_search" in route["tools"]
    assert "## Fast path" not in calls[0]["user_prompt"]


def test_document_mode_never_takes_the_fast_path(monkeypatch, traces):
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    calls = _install(monkeypatch, [_result(DOC_VALID)])

    result = agent.ask(
        "What does the note say about the cash-flow swing?",
        mode="document",
        document_id=DOC_ID,
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["route"]["fast_path"] is False
    assert "analysis_document" in result["route"]["tools"]
    assert "## Fast path" not in calls[0]["user_prompt"]


def test_explicit_mode_thinking_override_wins(monkeypatch, traces):
    chat_cfg = {
        "pi": {"thinking": "off"},
        "modes": {"desk": {"thinking": "medium"}},
        "thinking": {"default": "off", "complex": "high"},
        "fast_path": {"enabled": True, "max_tool_calls": 4},
    }
    monkeypatch.setattr(agent, "_load_chat_config", lambda: chat_cfg)
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("Show me the highlight of ITMG operation in Q2 2026")

    assert result["route"]["fast_path"] is True
    assert result["route"]["thinking"] == "medium"
    assert calls[0]["cfg"]["thinking"] == "medium"


def test_pi_config_keeps_the_backward_compatible_signature():
    chat_cfg = {
        "pi": {"thinking": "off"},
        "thinking": {"default": "off", "complex": "high"},
    }
    route = agent._route_request(
        "Show me ITMG Q2 2026 highlights", "desk", None, None, None, chat_cfg,
    )

    # An explicit chat.pi.thinking still wins over the router.
    assert agent._pi_config(chat_cfg, "desk")["thinking"] == "off"
    assert agent._pi_config(chat_cfg, "desk", route=route)["thinking"] == "off"

    unset = {"pi": {}, "thinking": {"default": "off", "complex": "high"}}
    assert agent._pi_config(unset, "desk", route=route)["thinking"] == "off"
    complex_route = agent._route_request(
        "Compare ITMG and PTBA production", "desk", None, None, None, unset,
    )
    assert agent._pi_config(unset, "desk", route=complex_route)["thinking"] == "high"


# ---------------------------------------------------------------------------
# Two-phase path: prose first, claims second (chat.two_phase, default off)
# ---------------------------------------------------------------------------

FAST_QUESTION = "Show me the highlight of ITMG operation in Q2 2026"

PHASE1_PROSE = "ITMG produced 5.3 Mt of coal in Q2 2026.\n"

CLAIMS_ONLY = f"""claims:
  - sentence: 1
    type: fact
    citations:
      - {{tool: operational_lookup, ticker: ITMG, metric: coal_production,
         doc: {DOC}, page: 1}}
missing: []
"""


def _two_phase_chat_cfg(**overrides) -> dict:
    cfg = {
        "pi": {"thinking": "off"},
        "thinking": {"default": "off", "complex": "high"},
        "fast_path": {"enabled": True, "max_tool_calls": 4},
        "max_answer_chars": 6000,
        "pricing": {
            "deepseek-v4.1-flash": {
                "input_per_mtok": 0.27, "output_per_mtok": 1.10,
            },
        },
        "two_phase": {
            "enabled": True,
            "max_evidence_chars": 20000,
            "claims_thinking": "off",
            "claims_model": None,
        },
    }
    cfg["two_phase"].update(overrides)
    return cfg


def _claims_result(content: str, *, error: str | None = None) -> dict:
    """A phase-2 runner result: no tools, no tool trace."""
    return {
        "content": content,
        "tool_trace": [],
        "usage": {"prompt_tokens": 800, "completion_tokens": 120, "cost_usd": 0.0},
        "error": error,
    }


def test_two_phase_disabled_keeps_the_single_pass(monkeypatch, traces):
    monkeypatch.setattr(
        agent, "_load_chat_config",
        lambda: _two_phase_chat_cfg(enabled=False),
    )
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered"
    assert len(calls) == 1
    assert "## Prose-only pass" not in calls[0]["user_prompt"]
    assert result["two_phase"] == {
        "enabled": False, "used": False, "fallback": None,
    }
    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["two_phase"]["enabled"] is False
    assert agent._two_phase_enabled(agent._load_chat_config()) is False
    assert agent._two_phase_enabled({"two_phase": {"enabled": True}}) is True
    assert agent._two_phase_enabled({}) is False


def test_two_phase_merges_prose_with_claims_and_audits(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    calls = _install(monkeypatch, [
        _result(PHASE1_PROSE),
        _claims_result(CLAIMS_ONLY),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["verification"]["ok"] is True
    # The prose is authoritative; phase 2 only supplied claims/missing.
    assert result["envelope"]["answer"] == PHASE1_PROSE.strip()
    assert len(result["envelope"]["claims"]) == 1
    assert result["envelope"]["claims"][0]["text"] == PHASE1_PROSE.strip()
    assert result["two_phase"]["used"] is True
    assert result["two_phase"]["fallback"] is None
    assert result["two_phase"]["claims"] == 1
    assert len(calls) == 2
    # Phase 1 is the normal envelope-capable pass plus the prose-only suffix.
    assert "## Prose-only pass" in calls[0]["user_prompt"]
    assert calls[0]["cfg"]["tools"]
    assert calls[1]["cfg"]["tools"] == []
    assert calls[1]["cfg"]["thinking"] == "off"

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["two_phase"]["used"] is True
    phases = {
        entry.get("phase") for entry in trace["tool_trace"]
        if entry.get("tool_name") == "operational_lookup"
    }
    assert phases == {"prose"}
    assert trace["attempts"] == [1, 2]


def test_two_phase_phase2_prompt_has_prose_evidence_and_sentence_contract(
    monkeypatch, traces,
):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    calls = _install(monkeypatch, [
        _result(PHASE1_PROSE),
        _claims_result(CLAIMS_ONLY),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered"
    prompt = calls[1]["user_prompt"]
    # The prose under audit, numbered for `sentence: N`.
    assert PHASE1_PROSE.strip() in prompt
    assert "1. ITMG produced 5.3 Mt of coal in Q2 2026." in prompt
    # Compact tool evidence from phase 1 (tool name + arguments + result).
    assert "## Evidence already collected" in prompt
    assert "### operational_lookup" in prompt
    assert "arguments:" in prompt
    assert "coal_production" in prompt
    # The claims-only contract: no answer key, compact sentence form.
    assert "sentence:" in prompt
    assert "Never include `answer`" in prompt
    assert "claims-only YAML" in prompt


def test_two_phase_phase2_audit_failure_falls_back_to_single_pass(
    monkeypatch, traces,
):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    bad_prose = "ITMG produced 6.9 Mt of coal in Q2 2026.\n"
    calls = _install(monkeypatch, [
        {**_result(bad_prose)},
        _claims_result(CLAIMS_ONLY),
        _claims_result(CLAIMS_ONLY),
        _result(VALID),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered", result["verification"]["violations"]
    assert len(calls) == 4
    # The failed claims pass retried once with a correction hint.
    assert calls[2]["user_prompt"] != calls[1]["user_prompt"]
    assert "## Correction" in calls[2]["user_prompt"]
    # Then the normal single-pass path ran and answered.
    assert calls[3]["cfg"]["tools"]
    assert result["two_phase"]["used"] is False
    assert result["two_phase"]["fallback"] == "phase2_audit_failed"
    assert "number_not_in_ledger" in result["two_phase"]["phase2_violations"]
    assert result["envelope"]["answer"].startswith("ITMG produced 5.3 Mt")

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["two_phase"]["fallback"] == "phase2_audit_failed"
    assert trace["attempts"] == [1]


def test_two_phase_phase2_invalid_yaml_falls_back(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    calls = _install(monkeypatch, [
        _result(PHASE1_PROSE),
        _claims_result("```yaml\nclaims: [unterminated\n```"),
        _claims_result("```yaml\nclaims: [still unterminated\n```"),
        _result(VALID),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered", result["verification"]["violations"]
    assert len(calls) == 4
    assert "not a valid claims-only YAML" in calls[2]["user_prompt"]
    assert result["two_phase"]["fallback"] == "phase2_parse_error"


def test_two_phase_phase1_error_falls_back(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    calls = _install(monkeypatch, [
        _result("", error="pi timed out after 180s"),
        _result(VALID),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered", result["verification"]["violations"]
    assert len(calls) == 2
    assert result["two_phase"]["used"] is False
    assert result["two_phase"]["fallback"].startswith("phase1_error")


def test_two_phase_phase1_empty_falls_back(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    calls = _install(monkeypatch, [_result(""), _result(VALID)])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered", result["verification"]["violations"]
    assert len(calls) == 2
    assert result["two_phase"]["fallback"] == "phase1_empty"


DOC_PHASE2_CLAIMS = """claims:
  - sentence: 1
    type: analysis_note
    citations:
      - {tool: analysis_document, document: itmg-q2-2026-analysis,
         section: "5. Cash Flow Analysis", line: 4,
         quote: "Operating cash flow collapsed −77.8% to US$62,376k."}
missing: []
"""


def test_two_phase_document_mode_passes_the_pinned_note_to_phase2(
    monkeypatch, traces,
):
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _two_phase_chat_cfg())
    prose = (
        "The note reports operating cash flow of US$62,376k and calls the drop "
        "a timing artifact.\n"
    )
    calls = _install(monkeypatch, [
        _result(prose),
        _claims_result(DOC_PHASE2_CLAIMS),
    ])

    result = agent.ask(
        "What does the note say about the cash-flow swing?",
        mode="document",
        document_id=DOC_ID,
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["two_phase"]["used"] is True
    assert result["envelope"]["answer"] == prose.strip()
    assert len(calls) == 2
    # Phase 2 gets the same pinned-note text so its quote is verbatim.
    prompt = calls[1]["user_prompt"]
    assert "UNTRUSTED DOCUMENT" in prompt
    assert "Operating cash flow collapsed" in prompt
    assert calls[1]["cfg"]["tools"] == []


def test_two_phase_claims_model_override_only_affects_phase2(
    monkeypatch, traces,
):
    monkeypatch.setattr(
        agent, "_load_chat_config",
        lambda: _two_phase_chat_cfg(claims_model="glm-5.3"),
    )
    calls = _install(monkeypatch, [
        _result(PHASE1_PROSE),
        _claims_result(CLAIMS_ONLY),
    ])

    result = agent.ask(FAST_QUESTION)

    assert result["status"] == "answered"
    assert calls[0]["cfg"]["model"] == "deepseek/deepseek-v4.1-flash"
    assert calls[1]["cfg"]["model"] == "glm-5.3"
    # The public result still reports the answer model.
    assert result["model"] == "deepseek/deepseek-v4.1-flash"


def test_tool_evidence_block_is_shared_by_retry_and_phase_two():
    state = {"tool_trace": [
        {
            "tool_name": "operational_lookup",
            "arguments": {"ticker": "ITMG"},
            "result": {"content": [{"type": "text", "text": "latest-body"}]},
            "attempt": 2,
            "phase": "prose",
        },
        {
            "tool_name": "company_facts",
            "arguments": {},
            "result": {"content": [{"type": "text", "text": "old-body"}]},
            "attempt": 1,
        },
        {
            "tool_name": "list_companies",
            "arguments": {},
            "result": {"content": [{"type": "text", "text": "ctx-body"}]},
            "context": True,
        },
    ]}

    latest = agent._tool_evidence_block(state, max_chars=5000)
    assert "latest-body" in latest
    assert "old-body" not in latest
    assert "ctx-body" not in latest
    assert latest.startswith("## Evidence already collected")

    first = agent._tool_evidence_block(
        state, max_chars=5000, attempt=1, instruction="HEADER ONLY",
    )
    assert "old-body" in first
    assert "latest-body" not in first
    assert first.startswith("HEADER ONLY")

    tiny = agent._tool_evidence_block(state, max_chars=1200)
    assert len(tiny) <= 1200




# ---------------------------------------------------------------------------
# Trace-audited path: prose only, claims derived from the tool trace
# ---------------------------------------------------------------------------

TRACE_PROSE = "ITMG produced 5.3 Mt of coal in Q2 2026."
TRACE_PROSE_TAMPERED = "ITMG produced 6.9 Mt of coal in Q2 2026."


def _trace_audit_chat_cfg(**overrides) -> dict:
    cfg = {
        "pi": {"thinking": "off"},
        "thinking": {"default": "off", "complex": "high"},
        "fast_path": {"enabled": True, "max_tool_calls": 4},
        "max_answer_chars": 6000,
        "pricing": {
            "deepseek-v4.1-flash": {
                "input_per_mtok": 0.27, "output_per_mtok": 1.10,
            },
        },
        "trace_audit": {"enabled": True, "fallback": True},
    }
    cfg["trace_audit"].update(overrides)
    return cfg


def test_trace_audit_config_is_opt_in_and_normalized():
    assert agent._trace_audit_enabled({}) is False
    assert agent._trace_audit_enabled({"trace_audit": {"enabled": True}}) is True
    cfg = agent._trace_audit_config({"trace_audit": {"enabled": True, "fallback": False}})
    assert cfg == {"enabled": True, "fallback": False}


def test_answer_only_prompts_remove_claims_and_envelope_contract():
    system = (
        "## 1. Tools\nkeep\n"
        "## 2. Labels: every causal sentence is a claim\ndrop\n"
        "## 6. Output: one YAML envelope\ndrop\n"
        "## 5. Output\ndrop\n"
    )
    user = "## Question\nq\n## Output contract (mandatory): one YAML envelope\nanswer: |\n"
    prose_system, prose_user = agent._answer_only_prompts(system, user)
    assert "## 1. Tools" in prose_system
    assert "## 2." not in prose_system
    assert "## 6." not in prose_system
    assert "## 5. Output" not in prose_system
    assert "## Output contract" not in prose_user
    assert "answer: |" not in prose_user
    assert "Answer in Markdown" in prose_user


def test_trace_audit_answers_from_prose_and_records_stats(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    calls = _install(monkeypatch, [_result(TRACE_PROSE)])

    result = agent.ask(
        "How much coal did ITMG produce in Q2 2026?",
        ticker="ITMG", period="Q2", year=2026,
    )

    assert result["status"] == "answered"
    assert result["envelope"]["answer"] == TRACE_PROSE
    assert result["envelope"]["claims"]
    assert result["verification"]["ok"] is True
    assert result["trace_audit"]["used"] is True
    assert result["trace_audit"]["fallback"] is None
    assert result["trace_audit"]["claims"] == 1
    assert result["trace_audit"]["numbers"] >= 1
    assert len(calls) == 1
    assert "## Output contract" not in calls[0]["user_prompt"]
    assert "## 6. Output" not in calls[0]["system_prompt"]

    trace = json.loads(next(traces.glob("*.json")).read_text(encoding="utf-8"))
    assert trace["trace_audit"]["used"] is True
    assert trace["trace_audit"]["fallback"] is None


def test_trace_audit_disabled_uses_the_single_pass(monkeypatch, traces):
    monkeypatch.setattr(
        agent, "_load_chat_config",
        lambda: _trace_audit_chat_cfg(enabled=False),
    )
    calls = _install(monkeypatch, [_result(VALID)])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "answered"
    assert result["trace_audit"] == {
        "enabled": False, "used": False, "fallback": None,
    }
    assert len(calls) == 1
    assert "## Output contract" in calls[0]["user_prompt"]


def test_trace_audit_audit_failure_falls_back_to_single_pass(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    calls = _install(monkeypatch, [
        _result(TRACE_PROSE_TAMPERED),
        _result(VALID),
    ])

    result = agent.ask("How much coal did ITMG produce in Q2 2026?")

    assert result["status"] == "answered"
    assert result["trace_audit"]["used"] is False
    assert result["trace_audit"]["fallback"] == "audit_failed"
    assert "number_not_in_ledger" in [
        item["type"] for item in result["trace_audit"]["violations"]
    ]
    assert len(calls) == 2
    assert "## Output contract" in calls[1]["user_prompt"]


def test_trace_audit_prose_error_falls_back(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    calls = _install(monkeypatch, [
        _result("", error="runner exploded"),
        _result(VALID),
    ])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "answered"
    assert result["trace_audit"]["used"] is False
    assert result["trace_audit"]["fallback"].startswith("prose_error")
    assert len(calls) == 2


def test_trace_audit_empty_prose_falls_back(monkeypatch, traces):
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    calls = _install(monkeypatch, [_result("   "), _result(VALID)])

    result = agent.ask("How much coal did ITMG produce?")

    assert result["status"] == "answered"
    assert result["trace_audit"]["fallback"] == "prose_empty"
    assert len(calls) == 2


def test_document_mode_trace_audit_derives_note_citations(
    monkeypatch, traces,
):
    """Document mode answers in prose; the note citation comes from the ledger."""
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    prose = (
        "The note reports operating cash flow of US$62,376k and calls the drop "
        "a timing artifact."
    )
    calls = _install(monkeypatch, [{
        "content": prose,
        "tool_trace": [],
        "usage": {
            "prompt_tokens": 900, "completion_tokens": 120, "cost_usd": 0.0,
        },
        "error": None,
        "pi_meta": {"model": "deepseek-v4.1-flash", "tool_calls": {}},
    }])

    result = agent.ask(
        "What does the note say about the cash-flow swing?",
        mode="document",
        document_id=DOC_ID,
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["trace_audit"]["used"] is True
    assert result["trace_audit"]["fallback"] is None
    claim = result["envelope"]["claims"][0]
    assert claim["type"] == "analysis_note"
    citation = claim["citations"][0]
    assert citation["document"] == DOC_ID
    assert citation["section"] == "5. Cash Flow Analysis"
    assert citation["line"] == 4
    assert "62,376" in citation["quote"]
    assert result["verification"]["citations"]["document_ok"] == 1
    # The prose contract replaces the YAML contract for the model call.
    assert len(calls) == 1
    assert "## Output contract" not in calls[0]["user_prompt"]
    assert "## 6. Output" not in calls[0]["system_prompt"]
    assert "Answer in Markdown" in calls[0]["user_prompt"]
    assert "UNTRUSTED DOCUMENT" in calls[0]["user_prompt"]


def test_document_mode_trace_audit_derives_not_in_document(
    monkeypatch, traces,
):
    """A scoped empty search becomes missing[state=not_in_document]."""
    monkeypatch.setattr(agent, "_load_chat_config", lambda: _trace_audit_chat_cfg())
    monkeypatch.setattr(
        agent.tools, "analysis_document",
        lambda document_id, section=None: _document_payload(document_id=document_id),
    )
    search = {
        "ok": True,
        "tool": "analysis_search",
        "request": {"query": "strip ratio", "document_id": DOC_ID},
        "data": {
            "query": "strip ratio",
            "document_id": DOC_ID,
            "ticker": None,
            "matches": [],
            "state": "not_in_document",
            "evidence": {
                "document": DOC_ID,
                "sections_searched": 2,
                "terms": ["strip", "ratio"],
            },
        },
        "ledger": [],
        "caveats": [],
    }
    calls = _install(monkeypatch, [{
        "content": "The note does not cover the strip ratio.",
        "tool_trace": [{
            "tool_call_id": "call_2",
            "tool_name": "analysis_search",
            "arguments": {"query": "strip ratio", "document_id": DOC_ID},
            "result": {
                "content": [{"type": "text", "text": json.dumps(search)}],
            },
        }],
        "usage": {
            "prompt_tokens": 900, "completion_tokens": 120, "cost_usd": 0.0,
        },
        "error": None,
        "pi_meta": {"model": "deepseek-v4.1-flash", "tool_calls": {}},
    }])

    result = agent.ask(
        "Does the note discuss the strip ratio?",
        mode="document",
        document_id=DOC_ID,
    )

    assert result["status"] == "answered", result["verification"]["violations"]
    assert result["trace_audit"]["used"] is True
    missing = result["envelope"]["missing"]
    assert [item["state"] for item in missing] == ["not_in_document"]
    assert missing[0]["evidence"]["document"] == DOC_ID
    assert len(calls) == 1
