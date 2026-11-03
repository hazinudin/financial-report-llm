"""The web surface: routes, payload passthrough, timeout, and trace reads.

The agent is monkeypatched (never called) so these tests are hermetic and fast:
the point is the HTTP contract in CONTRACTS.md §6, not the model.
"""

from __future__ import annotations

import copy
import json
import sys
import time
import types

import pytest
from fastapi.testclient import TestClient

from chat_app import analysis_docs, paths
from chat_app.web import app

PRODUCT = "IDX Research Desk"

ANSWERED = {
    "trace_id": "20261025T101500Z_ab12cd34",
    "status": "answered",
    "envelope": {
        "answer": "Q2 revenue was **502,914** USD thousand, derived from the cumulative filing.",
        "claims": [
            {
                "text": "Q2 revenue was 502,914 USD thousand.",
                "type": "fact",
                "citations": [
                    {
                        "tool": "period_derive",
                        "ticker": "ITMG",
                        "metric": "revenue",
                        "row": 6,
                    }
                ],
            }
        ],
        "missing": [],
    },
    "verification": {
        "ok": True,
        "violations": [],
        "citations": {"ok": 1, "removed": 0},
    },
    "usage": {"prompt_tokens": 120, "completion_tokens": 250, "cost_usd": 0.0028},
    "duration_s": 12.4,
    "model": "deepseek-v4.1-flash",
    "error": None,
    "tool_calls": ["company_facts", "period_derive"],
}

REFUSED = {
    "trace_id": "20261025T101600Z_deadbeef",
    "status": "refused",
    "envelope": None,
    "verification": {
        "ok": False,
        "violations": [
            {
                "type": "number_not_in_ledger",
                "token": "12.7",
                "context": "margin expanded to 12.7%",
            }
        ],
        "citations": {"ok": 0, "removed": 0},
    },
    "usage": {"prompt_tokens": 100, "completion_tokens": 40, "cost_usd": 0.0002},
    "duration_s": 3.1,
    "model": "deepseek-v4.1-flash",
    "error": None,
    "tool_calls": ["documents_status"],
}

ERROR = {
    "trace_id": "20261025T101700Z_00000000",
    "status": "error",
    "envelope": None,
    "verification": {"ok": False, "violations": [], "citations": {"ok": 0, "removed": 0}},
    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "cost_usd": 0.0},
    "duration_s": 180.0,
    "model": "deepseek-v4.1-flash",
    "error": "timeout",
    "tool_calls": [],
}


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as instance:
        yield instance


@pytest.fixture()
def agent_state(monkeypatch):
    """Install a fake ``chat_app.agent`` module and expose its call log."""
    import chat_app

    state = {
        "result": copy.deepcopy(ANSWERED),
        "calls": [],
        "delay": 0.0,
        "raises": None,
    }

    module = types.ModuleType("chat_app.agent")

    def fake_ask(
        question,
        *,
        ticker=None,
        period=None,
        year=None,
        history=None,
        mode="desk",
        document_id=None,
        selection=None,
    ):
        state["calls"].append({
            "question": question,
            "ticker": ticker,
            "period": period,
            "year": year,
            "history": history,
            "mode": mode,
            "document_id": document_id,
            "selection": selection,
        })
        if state["delay"]:
            time.sleep(state["delay"])
        if state["raises"]:
            raise state["raises"]
        return copy.deepcopy(state["result"])

    module.ask = fake_ask  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "chat_app.agent", module)
    monkeypatch.setattr(chat_app, "agent", module, raising=False)
    return state


# ---------------------------------------------------------------------------
# Static shell and health
# ---------------------------------------------------------------------------


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "version": "0.1.0"}


def test_index_serves_the_desk(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert PRODUCT in response.text
    assert "/static/styles.css" in response.text
    assert "/static/app.js" in response.text


@pytest.mark.parametrize(
    ("path", "needle"),
    [
        ("/static/styles.css", "--paper"),
        ("/static/app.js", "IDX Research Desk"),
    ],
)
def test_static_assets(client, path, needle):
    response = client.get(path)
    assert response.status_code == 200
    assert needle in response.text


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


def test_companies_returns_real_supported_set(client):
    response = client.get("/api/companies")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["as_of"]
    assert payload["count"] == len(payload["companies"]) == 24

    by_ticker = {company["ticker"]: company for company in payload["companies"]}
    assert "ITMG" in by_ticker
    assert by_ticker["ITMG"]["periods"] == ["Q1 2026", "Q2 2026"]
    assert "Q2 2026" in by_ticker["ITMG"]["operational_periods"]
    assert all(company["periods"] for company in payload["companies"])


# ---------------------------------------------------------------------------
# Ask: payload passthrough and the three result shapes
# ---------------------------------------------------------------------------


def test_ask_answered_unchanged_and_forwards_scope(client, agent_state):
    history = [
        {"role": "user", "content": "What about Q1?"},
        {"role": "assistant", "content": "Q1 revenue was 497,577."},
    ]
    response = client.post("/api/ask", json={
        "question": "  What was Q2 revenue?  ",
        "ticker": "ITMG",
        "period": "Q2",
        "year": 2026,
        "history": history,
    })
    assert response.status_code == 200
    assert response.json() == ANSWERED

    call = agent_state["calls"][-1]
    assert call["question"] == "What was Q2 revenue?"
    assert call["ticker"] == "ITMG"
    assert call["period"] == "Q2"
    assert call["year"] == 2026
    assert call["history"] == history


def test_ask_refused_unchanged(client, agent_state):
    agent_state["result"] = copy.deepcopy(REFUSED)
    response = client.post("/api/ask", json={"question": "What was the margin?"})
    assert response.status_code == 200
    assert response.json() == REFUSED


def test_ask_error_result_unchanged(client, agent_state):
    agent_state["result"] = copy.deepcopy(ERROR)
    response = client.post("/api/ask", json={"question": "Anything?"})
    assert response.status_code == 200
    assert response.json() == ERROR


def test_ask_exception_becomes_a_designed_error(client, agent_state):
    agent_state["raises"] = RuntimeError("pi binary not found")
    response = client.post("/api/ask", json={"question": "Anything?"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "error"
    assert "pi binary not found" in payload["error"]


def test_ask_timeout_is_an_error_not_a_hang(client, agent_state, monkeypatch):
    from chat_app import web

    monkeypatch.setattr(web, "ASK_TIMEOUT_SECONDS", 0.05)
    agent_state["delay"] = 0.35
    response = client.post("/api/ask", json={"question": "Slow question"})
    assert response.status_code == 200
    assert response.json() == {"status": "error", "error": "timeout"}


@pytest.mark.parametrize("question", ["", "   ", "\n\t "])
def test_empty_question_is_rejected(client, agent_state, question):
    response = client.post("/api/ask", json={"question": question})
    assert response.status_code == 422
    assert not agent_state["calls"]


def test_overlong_question_is_rejected(client, agent_state):
    response = client.post("/api/ask", json={"question": "x" * 2001})
    assert response.status_code == 422
    assert not agent_state["calls"]


# ---------------------------------------------------------------------------
# Semaphore: three sequential asks must not deadlock or leak permits
# ---------------------------------------------------------------------------


def test_three_sequential_asks_do_not_deadlock(client, agent_state):
    for index in range(3):
        response = client.post("/api/ask", json={"question": f"Question {index}"})
        assert response.status_code == 200
        assert response.json()["status"] == "answered"
    assert len(agent_state["calls"]) == 3


# ---------------------------------------------------------------------------
# Analysis document library + reader
# ---------------------------------------------------------------------------


def test_documents_library_lists_the_vault(client):
    response = client.get("/api/documents")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["as_of"]
    assert payload["count"] == len(payload["documents"])
    assert payload["count"] >= 100  # 116 notes on the current disk

    by_id = {document["id"]: document for document in payload["documents"]}
    assert "itmg-q2-2026-analysis" in by_id
    document = by_id["itmg-q2-2026-analysis"]
    assert document["title"]
    assert document["kind"] == "company"
    assert document["ticker"] == "ITMG"
    assert document["period"] == "Q2"
    assert document["year"] == 2026
    assert document["sha256"] and document["bytes"] > 0
    assert "content" not in document
    assert "sections" not in document


def test_documents_library_filters_and_search(client):
    filtered = client.get("/api/documents", params={
        "ticker": "ITMG", "period": "Q2", "kind": "company",
    }).json()
    assert [row["id"] for row in filtered["documents"]] == ["itmg-q2-2026-analysis"]

    searched = client.get("/api/documents", params={"q": "strip ratio"}).json()
    assert searched["ok"] is True
    assert searched["count"] == len(searched["documents"]) >= 1
    assert any(
        row["id"] == "itmg-q2-2026-analysis" for row in searched["documents"]
    )
    assert all("content" not in row for row in searched["documents"])

    bad = client.get("/api/documents", params={"period": "nonsense"})
    assert bad.status_code == 422


def test_document_detail_returns_full_content_and_outline(client):
    response = client.get("/api/documents/itmg-q2-2026-analysis")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["document"]["id"] == "itmg-q2-2026-analysis"
    assert payload["document"]["ticker"] == "ITMG"
    assert payload["content"].startswith("---")
    assert "# PT Indo Tambangraya Megah Tbk (ITMG)" in payload["content"]
    # The reader is never truncated.
    assert len(payload["content"].encode("utf-8")) == payload["document"]["bytes"]

    outline = payload["outline"]
    assert outline
    executive = next(
        section for section in outline
        if section["title"] == "1. Executive Summary"
    )
    assert executive["id"] == "1-executive-summary"
    assert executive["line"] == 12
    assert payload["line_map"]["12"] == "1-executive-summary"
    assert isinstance(payload["frontmatter"], dict)


def test_document_detail_frontmatter_is_json_safe(client):
    response = client.get("/api/documents/commodities-aluminium")
    assert response.status_code == 200
    frontmatter = response.json()["frontmatter"]
    assert frontmatter["title"] == "Aluminium Market Research Briefing"
    assert isinstance(frontmatter["created"], str)
    assert isinstance(frontmatter["updated"], str)


def test_document_detail_404_and_422(client, monkeypatch):
    missing = client.get("/api/documents/does-not-exist")
    assert missing.status_code == 404

    def unreadable(document_id):
        raise paths.ChatError("document is unreadable", document_id=document_id)

    monkeypatch.setattr(analysis_docs, "get_document", unreadable)
    broken = client.get("/api/documents/itmg-q2-2026-analysis")
    assert broken.status_code == 422
    assert "unreadable" in broken.json()["detail"]


# ---------------------------------------------------------------------------
# Ask modes: desk is the default, document pins a note
# ---------------------------------------------------------------------------


def test_document_mode_requirement_and_scope_validation(client, agent_state):
    missing_id = client.post("/api/ask", json={
        "question": "What does the note say?",
        "mode": "document",
    })
    assert missing_id.status_code == 422
    assert not agent_state["calls"]

    desk_with_document = client.post("/api/ask", json={
        "question": "Compare coal names",
        "mode": "desk",
        "document_id": "itmg-q2-2026-analysis",
    })
    assert desk_with_document.status_code == 422

    desk_with_selection = client.post("/api/ask", json={
        "question": "Compare coal names",
        "mode": "desk",
        "selection": {"quote": "x"},
    })
    assert desk_with_selection.status_code == 422
    assert not agent_state["calls"]


def test_desk_mode_is_the_default_and_forwards_mode(client, agent_state):
    response = client.post("/api/ask", json={"question": "What was Q2 revenue?"})
    assert response.status_code == 200
    assert response.json() == ANSWERED
    call = agent_state["calls"][-1]
    assert call["mode"] == "desk"
    assert call["document_id"] is None
    assert call["selection"] is None


def test_document_mode_forwards_scope_and_passes_response_fields(client, agent_state):
    selection = {
        "section": "1. Executive Summary",
        "line": 18,
        "quote": "We treat this as a timing artifact",
    }
    agent_state["result"] = {
        **copy.deepcopy(ANSWERED),
        "mode": "document",
        "document": {"id": "itmg-q2-2026-analysis", "title": "ITMG Q2 2026"},
        "selection_echo": selection,
    }
    response = client.post("/api/ask", json={
        "question": "What does the note say about the cash-flow swing?",
        "mode": "document",
        "document_id": "itmg-q2-2026-analysis",
        "selection": selection,
    })
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "document"
    assert payload["document"]["id"] == "itmg-q2-2026-analysis"
    assert payload["selection_echo"] == selection

    call = agent_state["calls"][-1]
    assert call["mode"] == "document"
    assert call["document_id"] == "itmg-q2-2026-analysis"
    assert call["selection"] == selection


# ---------------------------------------------------------------------------
# Traces
# ---------------------------------------------------------------------------


def test_trace_missing_is_404(client):
    response = client.get("/api/trace/does-not-exist.json")
    assert response.status_code == 404


def test_trace_round_trip(client, monkeypatch, tmp_path):
    monkeypatch.setattr(paths, "TRACES_DIR", tmp_path)
    trace = {"trace_id": "abc123", "status": "answered", "verification": {"ok": True}}
    (tmp_path / "abc123.json").write_text(json.dumps(trace), encoding="utf-8")

    response = client.get("/api/trace/abc123")
    assert response.status_code == 200
    assert response.json() == trace
