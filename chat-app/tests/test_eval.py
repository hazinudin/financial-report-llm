"""WS7 eval-harness tests.  No live model calls; ``agent.ask`` is monkeypatched."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from chat_app import eval as eval_lib
from chat_app import paths

CHAT_APP = Path(__file__).resolve().parents[1]
QUESTIONS = CHAT_APP / "eval" / "questions.yaml"
SNAPSHOT = CHAT_APP / "eval" / "supported-set.json"


def _question(**overrides):
    item = {
        "id": "synthetic",
        "category": "probe",
        "mode": "desk",
        "question": "How much coal did ITMG produce in Q2 2026?",
        "ticker": "ITMG",
        "period": "Q2",
        "year": 2026,
        "must_include": ["5.3"],
        "must_not_claim": ["does not exist"],
        "expect_state": None,
        "expect_label": "fact",
        "expect_citations": {"required": True, "document": False, "tickers": []},
        "expect_coverage": False,
        "expect_refusal": False,
        "notes": "",
    }
    item.update(overrides)
    return item


def _doc_question(**overrides):
    item = _question(
        id="doc_synthetic",
        category="document",
        mode="document",
        document_id="itmg-q2-2026-analysis",
        question="What does the note say about the cash-flow swing?",
        must_include=["timing artifact"],
        expect_label="analysis_note",
        expect_citations={"required": True, "document": True, "tickers": []},
    )
    item.update(overrides)
    return item


def _result(
    answer: str,
    *,
    claims=None,
    status: str = "answered",
    verification_ok: bool = True,
    missing=None,
    error=None,
    mode: str = "desk",
    document=None,
    coverage=None,
    cost: float = 0.001,
    duration: float = 1.5,
):
    envelope = {
        "answer": answer,
        "claims": claims or [],
        "missing": missing or [],
    }
    if coverage is not None:
        envelope["coverage"] = coverage
    return {
        "status": status,
        "mode": mode,
        "document": document,
        "envelope": envelope,
        "verification": {
            "ok": verification_ok,
            "violations": [],
            "citations": {"ok": 1, "removed": 0},
        },
        "usage": {"cost_usd": cost},
        "duration_s": duration,
        "error": error,
    }


def _claim(text: str, *, claim_type: str = "fact", citations=None):
    return {
        "text": text,
        "type": claim_type,
        "citations": citations
        if citations is not None
        else [{"tool": "operational_lookup", "ticker": "ITMG", "page": 1}],
    }


def _doc_citation(document: str = "itmg-q2-2026-analysis"):
    return {
        "tool": "analysis_document",
        "document": document,
        "section": "1. Executive Summary",
        "line": 16,
        "quote": "We treat this as a timing artifact rather than operating deterioration.",
    }


def _result_row(
    question_id: str,
    *,
    mode: str,
    category: str = "probe",
    passed: bool = True,
    cost: float = 0.001,
    duration: float = 1.0,
):
    claims = [
        {
            "text": "A cited claim.",
            "type": "fact",
            "citations": [{"tool": "company_facts", "ticker": "ITMG"}],
        }
    ]
    return {
        "id": question_id,
        "category": category,
        "mode": mode,
        "passed": passed,
        "status": "answered",
        "cost_usd": cost,
        "duration_s": duration,
        "verification": {"ok": True, "violations": []},
        "claims": claims,
        "missing": [],
        "forbidden_hits": [],
        "advice_violations": [],
        "agent_result": {"envelope": {"answer": "A cited claim.", "claims": claims}},
    }


# ---------------------------------------------------------------------------
# Question set
# ---------------------------------------------------------------------------


def test_questions_load_and_have_expected_shape():
    questions = eval_lib.load_questions(QUESTIONS)
    assert len(questions) == 25

    counts = Counter(item["category"] for item in questions)
    assert dict(counts) == {
        "probe": 8,
        "user": 3,
        "operational": 4,
        "trap": 4,
        "document": 3,
        "wide_scope": 3,
    }
    assert len({item["id"] for item in questions}) == 25
    assert Counter(item["mode"] for item in questions) == {
        "desk": 22,
        "document": 3,
    }

    for item in questions:
        for key in eval_lib.REQUIRED_KEYS:
            assert key in item, f"{item['id']} missing {key}"
        assert isinstance(item["must_include"], list)
        assert isinstance(item["must_not_claim"], list)
        assert isinstance(item["expect_citations"], dict)
        assert isinstance(item["expect_citations"]["required"], bool)
        assert isinstance(item["expect_citations"]["document"], bool)
        assert item["notes"]


def test_document_questions_pin_real_notes_and_selection_is_exact():
    questions = eval_lib.load_questions(QUESTIONS)
    documents = eval_lib.document_manifest()
    docs = [item for item in questions if item["mode"] == "document"]

    assert {item["document_id"] for item in docs} == {
        "itmg-q2-2026-analysis",
        "ptba-q2-2026-analysis",
        "ptps-q2-2026-analysis",
    }
    for item in docs:
        assert item["document_id"] in documents
        assert item["expect_citations"]["document"] is True
        assert item["expect_label"] in eval_lib.CLAIM_TYPES
        if item.get("selection"):
            content = (paths.PROJECT_ROOT / documents[item["document_id"]]["path"]).read_text(
                encoding="utf-8"
            )
            assert item["selection"]["quote"] in content


def test_wide_scope_questions_require_coverage_and_two_tickers():
    questions = eval_lib.load_questions(QUESTIONS)
    wide = [item for item in questions if item["category"] == "wide_scope"]

    assert len(wide) == 3
    for item in wide:
        assert item["mode"] == "desk"
        assert item["expect_coverage"] is True
        assert len(item["expect_citations"]["tickers"]) >= 2
        assert item["expect_citations"]["required"] is True
        assert "coverage" in " ".join(item["must_include"])


def test_question_tickers_and_periods_are_supported():
    supported = paths.supported_periods()
    questions = eval_lib.load_questions(QUESTIONS)

    for item in questions:
        ticker = item["ticker"]
        if ticker is None:
            continue
        if item.get("expect_state") == "unsupported":
            # The unsupported traps deliberately sit outside the set.
            assert item.get("expect_refusal") is True
            continue
        assert ticker in supported, f"{item['id']}: {ticker} not supported"
        assert paths.is_supported(
            ticker, item["period"], item["year"]
        ), f"{item['id']}: {ticker} {item['period']} {item['year']} not supported"


def test_question_patterns_are_valid():
    questions = eval_lib.load_questions(QUESTIONS)
    for item in questions:
        for pattern in item["must_include"] + item["must_not_claim"]:
            assert pattern.strip(), f"{item['id']} has an empty pattern"
            if eval_lib._is_regex(pattern):
                assert eval_lib.re.compile(pattern)


def test_supported_set_snapshot_matches_disk():
    snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    supported = paths.supported_periods()

    assert snapshot["tickers"] == sorted(supported)
    assert snapshot["counts"]["tickers"] == len(supported)
    assert snapshot["counts"]["periods"] == sum(len(v) for v in supported.values())
    assert snapshot["counts"]["tickers"] == 24
    assert snapshot["counts"]["periods"] == 38

    on_disk = {
        (ticker, entry["period"], entry["year"])
        for ticker, entries in supported.items()
        for entry in entries
    }
    in_snapshot = {
        (item["ticker"], item["period"], item["year"])
        for item in snapshot["periods"]
    }
    assert in_snapshot == on_disk

    coverage = snapshot["layer_coverage"]
    assert coverage["facts"]["periods"] == 38
    assert coverage["ratios"]["periods"] == 38
    expected_operational = sorted(
        f"{ticker} {entry['period']} {entry['year']}"
        for ticker, entries in supported.items()
        for entry in entries
        if entry["has_operational"]
    )
    assert coverage["operational"]["items"] == expected_operational
    assert "ITMG Q2 2026" in coverage["operational"]["items"]
    assert coverage["notes"]["by_ticker"]["PTBA"] > 0


def test_build_snapshot_reproduces_the_frozen_manifest():
    rebuilt = eval_lib.build_snapshot()
    frozen = json.loads(SNAPSHOT.read_text(encoding="utf-8"))

    assert rebuilt["tickers"] == frozen["tickers"]
    assert rebuilt["counts"]["tickers"] == frozen["counts"]["tickers"]
    assert rebuilt["counts"]["periods"] == frozen["counts"]["periods"]
    assert rebuilt["periods"] == frozen["periods"]


# ---------------------------------------------------------------------------
# Assertion engine: shared contract
# ---------------------------------------------------------------------------


def test_answered_envelope_passes():
    item = _question()
    result = _result(
        "ITMG produced 5.3 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 5.3 Mt of coal in Q2 2026.")],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True
    assert check["reasons"] == []


def test_missing_required_number_fails():
    item = _question()
    result = _result(
        "ITMG produced 6.0 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 6.0 Mt of coal in Q2 2026.")],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("missing:") for reason in check["reasons"])


def test_must_not_claim_fails():
    item = _question()
    result = _result(
        "ITMG produced 5.3 Mt. The document does not exist.",
        claims=[_claim("ITMG produced 5.3 Mt. The document does not exist.")],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("forbidden:") for reason in check["reasons"])


def test_expect_label_fails_without_that_label():
    item = _question(expect_label="fact")
    result = _result(
        "ITMG produced 5.3 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 5.3 Mt of coal in Q2 2026.", claim_type="hypothesis")],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("label:") for reason in check["reasons"])


def test_expect_citation_fails_for_uncited_fact():
    item = _question()
    result = _result(
        "ITMG produced 5.3 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 5.3 Mt of coal in Q2 2026.", citations=[])],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("citation:") for reason in check["reasons"])


def test_expect_state_accepts_missing_entry():
    item = _question(
        must_include=["not captured"],
        expect_label=None,
        expect_citations={"required": False, "document": False, "tickers": []},
        expect_state="not_captured",
    )
    result = _result(
        "PTBA Q2 2026 coal production was not captured.",
        missing=[{"item": "coal_production", "state": "not_captured", "evidence": {}}],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True


def test_expect_refusal_accepts_refused_run():
    item = _question(
        must_include=["unsupported"],
        expect_label=None,
        expect_citations={"required": False, "document": False, "tickers": []},
        expect_state="unsupported",
        expect_refusal=True,
    )
    result = _result(
        "ANJT is not in the supported set.",
        status="refused",
        verification_ok=False,
        error="unsupported: ANJT is not in the supported set",
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True


def test_refusal_fails_when_not_expected():
    item = _question(expect_refusal=False)
    result = _result(
        "ANJT is not in the supported set.",
        status="refused",
        verification_ok=False,
        error="unsupported",
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("refusal" in reason for reason in check["reasons"])


def test_expect_refusal_accepts_answered_bbcA_style_refusal():
    item = _question(
        must_include=["gross margin is not applicable"],
        expect_label=None,
        expect_citations={"required": False, "document": False, "tickers": []},
        expect_refusal=True,
    )
    result = _result(
        "BBCA reports no cost of sales, so a gross margin is not applicable.",
        claims=[],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True


def test_unanswered_verified_run_fails():
    item = _question()
    result = _result(
        "ITMG produced 5.3 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 5.3 Mt of coal in Q2 2026.")],
        verification_ok=False,
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("verification" in reason for reason in check["reasons"])


def test_mode_echo_must_match_the_question():
    item = _question()
    result = _result(
        "ITMG produced 5.3 Mt of coal in Q2 2026.",
        claims=[_claim("ITMG produced 5.3 Mt of coal in Q2 2026.")],
        mode="document",
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("mode:") for reason in check["reasons"])


# ---------------------------------------------------------------------------
# Assertion engine: document surface
# ---------------------------------------------------------------------------


def test_document_answer_with_analysis_note_passes():
    item = _doc_question()
    result = _result(
        "The note treats the cash-flow collapse as a timing artifact.",
        claims=[
            _claim(
                "The note treats the cash-flow collapse as a timing artifact.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            )
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis", "title": "ITMG Q2 2026"},
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True
    assert check["note_claims"] == 1


def test_document_note_number_as_fact_fails():
    item = _doc_question()
    result = _result(
        "The note treats the cash-flow collapse as a timing artifact.",
        claims=[
            _claim(
                "The note treats the cash-flow collapse as a timing artifact.",
                claim_type="fact",
                citations=[_doc_citation()],
            )
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("note-label:") for reason in check["reasons"])


def test_document_citation_requires_section_line_and_quote():
    item = _doc_question()
    citation = _doc_citation()
    del citation["quote"]
    result = _result(
        "The note treats the cash-flow collapse as a timing artifact.",
        claims=[
            _claim(
                "The note treats the cash-flow collapse as a timing artifact.",
                claim_type="analysis_note",
                citations=[citation],
            )
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("note-citation" in reason for reason in check["reasons"])


def test_document_citation_must_match_the_pinned_document():
    item = _doc_question()
    result = _result(
        "The note treats the cash-flow collapse as a timing artifact.",
        claims=[
            _claim(
                "The note treats the cash-flow collapse as a timing artifact.",
                claim_type="analysis_note",
                citations=[_doc_citation("ptba-q2-2026-analysis")],
            )
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("document:") for reason in check["reasons"])


def test_document_metadata_must_be_echoed():
    item = _doc_question()
    result = _result(
        "The note treats the cash-flow collapse as a timing artifact.",
        claims=[
            _claim(
                "The note treats the cash-flow collapse as a timing artifact.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            )
        ],
        mode="document",
        document=None,
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("document:metadata") for reason in check["reasons"])


def test_not_in_document_accepts_structured_missing_entry():
    item = _doc_question(
        must_include=["strip ratio", "not cover"],
        expect_label="hypothesis",
        expect_state="not_in_document",
    )
    result = _result(
        "The note does not cover strip ratio. This is a palm-oil producer, so the H2 CPO reaction is a hypothesis.",
        claims=[
            _claim(
                "The note does not cover strip ratio.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            ),
            _claim(
                "The H2 CPO reaction is a hypothesis.",
                claim_type="hypothesis",
                citations=[{"tool": "company_facts", "ticker": "PTPS"}],
            ),
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
        missing=[
            {
                "item": "strip_ratio",
                "state": "not_in_document",
                "evidence": {"sections_searched": 14},
            }
        ],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True


def test_not_in_document_rejects_not_disclosed_conflation():
    item = _doc_question(
        must_include=["strip ratio", "not cover"],
        expect_label="hypothesis",
        expect_state="not_in_document",
    )
    result = _result(
        "The note does not cover strip ratio. This is a palm-oil producer, so the H2 CPO reaction is a hypothesis.",
        claims=[
            _claim(
                "The note does not cover strip ratio.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            ),
            _claim(
                "The H2 CPO reaction is a hypothesis.",
                claim_type="hypothesis",
                citations=[{"tool": "company_facts", "ticker": "PTPS"}],
            ),
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
        missing=[{"item": "strip_ratio", "state": "not_disclosed"}],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("state:not_in_document" in reason for reason in check["reasons"])


def test_not_in_document_prose_without_missing_entry_fails():
    item = _doc_question(
        must_include=["strip ratio", "not cover"],
        expect_label="hypothesis",
        expect_state="not_in_document",
    )
    result = _result(
        "The note does not cover strip ratio. This is a palm-oil producer, so the H2 CPO reaction is a hypothesis.",
        claims=[
            _claim(
                "The note does not cover strip ratio.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            ),
            _claim(
                "The H2 CPO reaction is a hypothesis.",
                claim_type="hypothesis",
                citations=[{"tool": "company_facts", "ticker": "PTPS"}],
            ),
        ],
        mode="document",
        document={"id": "itmg-q2-2026-analysis"},
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("state:not_in_document" in reason for reason in check["reasons"])


# ---------------------------------------------------------------------------
# Assertion engine: desk surface
# ---------------------------------------------------------------------------


def test_desk_multi_ticker_citation_coverage():
    item = _question(
        id="wide_synthetic",
        category="wide_scope",
        expect_label=None,
        expect_citations={
            "required": True,
            "document": False,
            "tickers": ["ITMG", "PTBA"],
        },
        expect_coverage=True,
        must_include=["5.3", "19.45", "supported"],
    )
    coverage = {
        "scope": "supported_set",
        "supported_tickers": 24,
        "compared_tickers": ["ITMG", "PTBA"],
        "caveat": "Supported names only; not an IDX-wide sector screen.",
    }
    result = _result(
        "ITMG produced 5.3 Mt; PTBA reported 19.45 Mt for 1H26. Coverage is the supported set.",
        claims=[
            _claim(
                "ITMG produced 5.3 Mt.",
                citations=[{"tool": "operational_lookup", "ticker": "ITMG"}],
            ),
            _claim(
                "PTBA reported 19.45 Mt for 1H26.",
                citations=[{"tool": "operational_lookup", "ticker": "PTBA"}],
            ),
        ],
        coverage=coverage,
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True
    assert check["cited_tickers"] == ["ITMG", "PTBA"]


def test_desk_multi_ticker_coverage_fails_when_one_is_missing():
    item = _question(
        id="wide_synthetic",
        category="wide_scope",
        expect_label=None,
        expect_citations={
            "required": True,
            "document": False,
            "tickers": ["ITMG", "PTBA"],
        },
        expect_coverage=True,
        must_include=["5.3", "19.45", "supported"],
    )
    result = _result(
        "ITMG produced 5.3 Mt; PTBA reported 19.45 Mt for 1H26. Coverage is the supported set.",
        claims=[
            _claim(
                "ITMG produced 5.3 Mt.",
                citations=[{"tool": "operational_lookup", "ticker": "ITMG"}],
            ),
        ],
        coverage={
            "supported_tickers": 24,
            "compared_tickers": ["ITMG", "PTBA"],
            "caveat": "Supported names only.",
        },
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any("citation:tickers" in reason for reason in check["reasons"])


def test_desk_coverage_block_is_required_for_wide_scope():
    item = _question(
        id="wide_synthetic",
        category="wide_scope",
        expect_label=None,
        expect_citations={
            "required": True,
            "document": False,
            "tickers": ["ITMG", "PTBA"],
        },
        expect_coverage=True,
        must_include=["5.3", "19.45", "supported"],
    )
    result = _result(
        "ITMG produced 5.3 Mt; PTBA reported 19.45 Mt for 1H26. Coverage is the supported set.",
        claims=[
            _claim(
                "ITMG produced 5.3 Mt.",
                citations=[{"tool": "operational_lookup", "ticker": "ITMG"}],
            ),
            _claim(
                "PTBA reported 19.45 Mt for 1H26.",
                citations=[{"tool": "operational_lookup", "ticker": "PTBA"}],
            ),
        ],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("coverage:") for reason in check["reasons"])


def test_no_advice_rejects_unattributed_rating():
    item = _question(
        must_include=["5.3"],
        expect_label=None,
    )
    result = _result(
        "ITMG produced 5.3 Mt. We rate the shares BUY.",
        claims=[
            _claim("ITMG produced 5.3 Mt."),
            _claim("We rate the shares BUY.", claim_type="hypothesis"),
        ],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is False
    assert any(reason.startswith("advice:") for reason in check["reasons"])


def test_attributed_note_rating_is_allowed_on_desk():
    item = _question(
        must_include=["5.3"],
        expect_label="analysis_note",
    )
    result = _result(
        "ITMG produced 5.3 Mt. The note concludes HOLD (Qualified) pending price discovery.",
        claims=[
            _claim("ITMG produced 5.3 Mt."),
            _claim(
                "The note concludes HOLD (Qualified) pending price discovery.",
                claim_type="analysis_note",
                citations=[_doc_citation()],
            ),
        ],
    )
    check = eval_lib.check_result(item, result)
    assert check["passed"] is True


# ---------------------------------------------------------------------------
# Mode routing / backend
# ---------------------------------------------------------------------------


def test_mode_routing_passes_mode_document_id_and_selection(monkeypatch):
    import sys
    import types

    calls: list[dict] = []

    def fake_ask(question, **kwargs):
        calls.append({"question": question, **kwargs})
        if kwargs.get("mode") == "document":
            return _result(
                "The note treats this as a timing artifact; management points to the refund and 169.",
                claims=[
                    _claim(
                        "The note treats this as a timing artifact; management points to the refund and 169.",
                        claim_type="analysis_note",
                        citations=[_doc_citation()],
                    )
                ],
                mode="document",
                document={"id": kwargs.get("document_id")},
            )
        return _result(
            "ITMG produced 5.3 Mt in Q2 2026, up 13% QoQ.",
            claims=[_claim("ITMG produced 5.3 Mt in Q2 2026, up 13% QoQ.")],
        )

    # The eval must route to agent.ask; a fake module keeps this test free of the
    # live agent stack (and of a model call) even while that package is mid-edit.
    fake_agent = types.ModuleType("chat_app.agent")
    fake_agent.ask = fake_ask
    monkeypatch.setitem(sys.modules, "chat_app.agent", fake_agent)

    questions = eval_lib.load_questions(QUESTIONS)
    results, _ = eval_lib.run_eval(
        questions,
        ids=["probe_itmg_q2_production", "doc_itmg_q2_cash_flow"],
    )

    assert [result["id"] for result in results] == [
        "probe_itmg_q2_production",
        "doc_itmg_q2_cash_flow",
    ]
    desk_call = next(call for call in calls if call["mode"] == "desk")
    assert desk_call["question"].startswith("How much coal did ITMG produce")
    assert desk_call["document_id"] is None
    assert desk_call["selection"] is None

    doc_call = next(call for call in calls if call["mode"] == "document")
    assert doc_call["document_id"] == "itmg-q2-2026-analysis"
    assert doc_call["selection"]["line"] == 16
    assert doc_call["selection"]["quote"].startswith("We treat this as a timing artifact")
    assert doc_call["ticker"] == "ITMG"


def test_missing_backend_aborts_without_fake_results(monkeypatch, capsys):
    def unavailable():
        raise eval_lib.BackendUnavailable("agent backend unavailable: No module named 'pi'")

    monkeypatch.setattr(eval_lib, "_ensure_backend", unavailable)
    code = eval_lib.main(["--questions", str(QUESTIONS), "--limit", "1"])
    assert code == 2
    err = capsys.readouterr().err
    assert "eval aborted" in err
    assert "agent backend unavailable" in err


def test_per_question_agent_exception_is_an_error_row_not_a_pass():
    item = _question()

    def boom(*args, **kwargs):
        raise RuntimeError("pi binary missing")

    result = eval_lib._run_one(item, boom)
    assert result["passed"] is False
    assert result["status"] == "error"
    assert "pi binary missing" in result["error"]


# ---------------------------------------------------------------------------
# Metrics: mode-split cost/latency
# ---------------------------------------------------------------------------


def test_metrics_split_cost_and_latency_by_mode():
    questions = [
        _question(id="desk_one", category="probe", mode="desk"),
        _question(
            id="doc_one",
            category="document",
            mode="document",
            document_id="itmg-q2-2026-analysis",
            expect_citations={"required": True, "document": True, "tickers": []},
        ),
    ]
    results = [
        _result_row("desk_one", mode="desk", cost=0.004, duration=10.0),
        _result_row(
            "doc_one", mode="document", category="document", cost=0.008, duration=30.0
        ),
    ]
    metrics = eval_lib.compute_metrics(results, questions)

    assert metrics["by_mode"]["desk"]["median_cost_usd"] == 0.004
    assert metrics["by_mode"]["document"]["median_cost_usd"] == 0.008
    assert metrics["by_mode"]["desk"]["p95_latency_s"] == 10.0
    assert metrics["by_mode"]["document"]["p95_latency_s"] == 30.0
    assert metrics["thresholds"]["A6_median_cost_usd"]["by_mode"]["desk"]["passed"]
    assert metrics["thresholds"]["A6_median_cost_usd"]["by_mode"]["document"]["passed"]


def test_document_cost_threshold_is_looser_than_desk():
    questions = [
        _question(id="desk_one", category="probe", mode="desk"),
        _question(
            id="doc_one",
            category="document",
            mode="document",
            document_id="itmg-q2-2026-analysis",
            expect_citations={"required": True, "document": True, "tickers": []},
        ),
    ]
    results = [
        _result_row("desk_one", mode="desk", cost=0.004, duration=10.0),
        _result_row(
            "doc_one", mode="document", category="document", cost=0.03, duration=30.0
        ),
    ]
    metrics = eval_lib.compute_metrics(results, questions)

    assert metrics["thresholds"]["A6_median_cost_usd"]["by_mode"]["desk"]["passed"]
    assert not metrics["thresholds"]["A6_median_cost_usd"]["by_mode"]["document"]["passed"]
    assert not metrics["thresholds"]["A6_median_cost_usd"]["passed"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_dry_run_via_main_does_not_call_model(monkeypatch, capsys):
    def boom(*args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("model called during --dry-run")

    monkeypatch.setattr(eval_lib, "_call_agent", boom)

    code = eval_lib.main(["--questions", str(QUESTIONS), "--dry-run"])
    assert code == 0

    output = capsys.readouterr().out
    assert "questions: 25" in output
    assert "probe: 8" in output
    assert "user: 3" in output
    assert "operational: 4" in output
    assert "trap: 4" in output
    assert "document: 3" in output
    assert "wide_scope: 3" in output
    assert "mode.desk: 22" in output
    assert "mode.document: 3" in output


def test_invalid_question_set_returns_nonzero(tmp_path, capsys):
    broken = tmp_path / "questions.yaml"
    broken.write_text("- id: only_one\n  category: probe\n", encoding="utf-8")

    code = eval_lib.main(["--questions", str(broken), "--dry-run"])
    assert code == 2
    assert "invalid question set" in capsys.readouterr().err


def test_selection_by_id_limit_and_mode():
    questions = eval_lib.load_questions(QUESTIONS)
    selected = eval_lib.select_questions(
        questions, ids=["probe_itmg_q2_production"]
    )
    assert [item["id"] for item in selected] == ["probe_itmg_q2_production"]

    limited = eval_lib.select_questions(questions, limit=3)
    assert len(limited) == 3
    assert [item["id"] for item in limited] == [
        item["id"] for item in questions[:3]
    ]

    documents = eval_lib.select_questions(questions, mode="document")
    assert len(documents) == 3
    assert {item["mode"] for item in documents} == {"document"}

    with pytest.raises(eval_lib.QuestionSchemaError):
        eval_lib.select_questions(questions, ids=["not_a_question"])
