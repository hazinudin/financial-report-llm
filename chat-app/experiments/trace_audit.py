"""Prototype: audit an answer from the tool trace, with no model-written envelope.

The model writes Markdown only. This module maps each sentence's numbers to the
ledger produced by the tool trace, synthesises claims/citations/missing/coverage,
and runs the existing ``verify_envelope``. The audit itself is unchanged.

Usage
-----
    # 1. Replay an existing audited trace's prose against its tool trace
    PYTHONPATH=chat-app .venv/bin/python chat-app/experiments/trace_audit.py \
        --from-trace idx_data/chat/traces/<trace>.json

    # 2. Generate answer-only prose, then audit it from the trace
    PYTHONPATH=chat-app .venv/bin/python chat-app/experiments/trace_audit.py \
        --question "Show me highlight of MAPA Q3 2026 performance"

    # 3. Same, N samples, JSON report
    PYTHONPATH=chat-app .venv/bin/python chat-app/experiments/trace_audit.py \
        --question "..." --runs 3 --json out.json

This is a measurement harness, not production code. It deliberately imports
private verifier helpers so the mapper and the audit agree on tokenisation and
value matching.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

CHAT_APP = Path(__file__).resolve().parents[1]
if str(CHAT_APP) not in sys.path:
    sys.path.insert(0, str(CHAT_APP))

from chat_app import agent, paths, verify  # noqa: E402
from chat_app.envelope import (  # noqa: E402
    AnswerEnvelope,
    Citation,
    Coverage,
    Missing,
    split_sentences,
)

OUTPUT_RULE = """## Output
Answer in Markdown. No YAML, no envelope, no claims list, no citations block.
Lead with the direct answer. State the period scope explicitly (cumulative vs
point-in-time). Keep every number as returned by a tool. Do not put sheet codes,
ledger row numbers, page references, or tool field names in the prose; describe
the source in words and leave row/page detail to the citation layer.
"""


# ---------------------------------------------------------------------------
# Answer-only generation (prompt with the claims/envelope contract removed)
# ---------------------------------------------------------------------------


def _strip_sections(text: str, drop_prefixes: tuple[str, ...]) -> str:
    parts = re.split(r"(?m)^(?=## )", text)
    kept = [
        part for part in parts
        if not any(
            part.startswith("## ") and part.splitlines()[0].startswith(prefix)
            for prefix in drop_prefixes
        )
    ]
    return "".join(kept).strip()


def answer_only_prompt(question: str, mode: str = "desk") -> tuple[str, str, dict]:
    """Return ``(system_prompt, user_prompt, route)`` without claims/YAML."""
    chat_cfg = agent._load_chat_config()
    route = agent._route_request(question, mode, None, None, None, chat_cfg)
    pi_cfg = agent._pi_config(chat_cfg, mode, route=route)
    route = {**route, "thinking": pi_cfg["thinking"], "tools": list(pi_cfg["tools"])}

    supported = agent._supported_summary(agent._supported_payload())
    shared = _strip_sections(
        (paths.PROMPTS_DIR / "chat_system.md").read_text(encoding="utf-8"),
        ("## 2.", "## 6."),
    )
    mode_rules = _strip_sections(
        agent._resolve_prompt_path(mode, chat_cfg).read_text(encoding="utf-8"),
        ("## 5.",),
    )
    system_prompt = shared + "\n\n" + mode_rules + "\n\n" + OUTPUT_RULE

    user_prompt = agent._user_prompt(
        question,
        ticker=None, period=None, year=None,
        supported=supported,
        history_block=agent._history_block(
            None, chat_cfg.get("max_history_turns", agent.DEFAULT_MAX_HISTORY_TURNS)
        ),
        answer_cap=agent._answer_cap(chat_cfg),
        analyst_context="",
    )
    user_prompt = re.split(
        r"(?m)^## (?:Output contract|Prose-only pass)", user_prompt,
    )[0].rstrip()
    user_prompt += "\n\n" + OUTPUT_RULE
    if route.get("prompt_suffix"):
        user_prompt = user_prompt.rstrip() + "\n\n" + route["prompt_suffix"]
    return system_prompt, user_prompt, route


def run_answer_only(question: str, mode: str = "desk") -> dict[str, Any]:
    system_prompt, user_prompt, route = answer_only_prompt(question, mode)
    chat_cfg = agent._load_chat_config()
    pi_cfg = agent._pi_config(chat_cfg, mode, route=route)

    started = time.perf_counter()
    runner = agent._pipeline_runner()
    raw = runner(
        system_prompt,
        user_prompt,
        cfg=pi_cfg,
        run_dir=paths.TRACES_DIR / "runs" / f"{agent._trace_id()}_answer_only",
        project_root=paths.PROJECT_ROOT,
    )
    wall = time.perf_counter() - started
    raw = raw if isinstance(raw, dict) else {}
    trace = list(raw.get("tool_trace") or [])
    # The real agent seeds list_companies as a context entry; mirror it so the
    # supported-set counts ("24 tickers") are in the ledger, as in production.
    supported_payload = agent._supported_payload()
    if supported_payload:
        trace.insert(0, {
            "tool_call_id": "context:list_companies",
            "tool_name": "list_companies",
            "arguments": {},
            "result": {"content": [{
                "type": "text",
                "text": json.dumps(supported_payload, ensure_ascii=False),
            }]},
            "context": True,
        })
    return {
        "wall_s": round(wall, 3),
        "route": route,
        "usage": raw.get("usage") or {},
        "tool_trace": trace,
        "answer": str(raw.get("content") or "").strip(),
        "error": raw.get("error"),
    }


# ---------------------------------------------------------------------------
# Trace -> envelope
# ---------------------------------------------------------------------------


def _entries_with_calls(tool_trace: list[dict]) -> list[dict[str, Any]]:
    """Every primary ledger entry, annotated with its producing call."""
    out: list[dict[str, Any]] = []
    for call in verify._build_calls(tool_trace):
        if call["errored"] or call["context"]:
            continue
        for entry in call["ledger"]:
            if not isinstance(entry, dict):
                continue
            out.append({"entry": entry, "call": call})
    return out


def _citation_for(item: dict[str, Any]) -> dict[str, Any]:
    entry, call = item["entry"], item["call"]
    meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else {}
    fields = verify._call_fields(call)
    citation: dict[str, Any] = {"tool": call["tool"]}

    ticker = (
        meta.get("ticker") or call["args"].get("ticker") or call["data"].get("ticker")
    )
    if ticker and verify._norm_id(ticker) in fields["ticker"]:
        citation["ticker"] = ticker

    metric = meta.get("metric") or call["args"].get("metric")
    if metric:
        citation["metric"] = metric

    row = meta.get("row")
    if row is None and isinstance(entry.get("source"), str) and ":" in entry["source"]:
        tail = entry["source"].rsplit(":", 1)[1].strip()
        if tail.isdigit():
            row = int(tail)
    if row is not None and int(row) in fields["row"]:
        citation["row"] = int(row)

    page = meta.get("page")
    if page is not None and int(page) in fields["page"]:
        citation["page"] = int(page)

    doc = (
        meta.get("source_doc") or meta.get("document")
        or call["args"].get("doc_id") or call["args"].get("doc")
    )
    if doc and verify._norm_id(doc) in fields["doc"]:
        citation["doc"] = doc

    driver = meta.get("driver")
    if driver and verify._norm_text(driver) in fields["driver"]:
        citation["driver"] = driver
    return citation


def _citation_key(citation: dict[str, Any]) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in citation.items()))


def _metric_named_in(metric: Any, sentence: str) -> bool:
    """True when every word of a metric name appears in the sentence."""
    words = [
        word for word in re.split(r"[^a-z]+", str(metric or "").casefold())
        if len(word) > 1
    ]
    if not words:
        return False
    text = " " + re.sub(r"[^a-z]+", " ", sentence.casefold()) + " "
    return all(f" {word} " in text for word in words)


def map_sentence(
    sentence: str, entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Match a sentence's numbers to ledger entries.

    Returns the chosen entries plus counters describing how clean the map was.
    """
    tokens = [
        token for token in verify._tokens(sentence)
        # Years are structural, not quantities (same rule as the number audit).
        if not (not token["percent"] and 2000 <= token["value"] <= 2100)
    ]
    chosen: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    used_keys: set[tuple] = set()

    def key(item: dict[str, Any]) -> tuple:
        return _citation_key(_citation_for(item))

    for token in tokens:
        candidates = [
            item for item in entries
            if verify._entry_matches(
                token["value"], token["percent"], item["entry"], token["decimals"],
            )
        ]
        unique = {key(item): item for item in candidates}
        candidates = list(unique.values())
        if len(candidates) == 1:
            item = candidates[0]
            if key(item) not in used_keys:
                used_keys.add(key(item))
                chosen.append(item)
            continue
        if not candidates:
            unmatched.append(token)
            continue
        # 1) Prefer a candidate whose metric name appears in the sentence
        # ("Net profit ..." → net_profit, not np_owners).
        metric_hits = [
            item for item in candidates
            if _metric_named_in(
                (item["entry"].get("meta") or {}).get("metric"), sentence
            )
        ]
        # 2) Otherwise prefer the metric/source already anchored by a unique
        # token in this sentence.
        anchors = {
            (item["entry"].get("source"), (item["entry"].get("meta") or {}).get("metric"))
            for item in chosen
        }
        anchored = [
            item for item in candidates
            if (
                item["entry"].get("source"),
                (item["entry"].get("meta") or {}).get("metric"),
            ) in anchors
        ]
        preferred = metric_hits if len(metric_hits) == 1 else anchored
        item = (preferred or candidates)[0]
        ambiguous.append({
            "token": token["value"],
            "percent": token["percent"],
            "candidates": [
                {
                    "tool": candidate["call"]["tool"],
                    "source": candidate["entry"].get("source"),
                    "meta": candidate["entry"].get("meta"),
                }
                for candidate in candidates[:6]
            ],
            "resolved": bool(preferred),
        })
        if key(item) not in used_keys:
            used_keys.add(key(item))
            chosen.append(item)

    return {
        "tokens": tokens,
        "entries": chosen,
        "ambiguous": ambiguous,
        "unmatched": unmatched,
    }


def _causal(sentence: str) -> bool:
    markers = verify._causal_markers()
    if not markers:
        return False
    pattern = re.compile(
        "|".join(r"\b" + re.escape(marker) + r"\b" for marker in markers),
        re.IGNORECASE,
    )
    return bool(pattern.search(sentence))


def _absence(sentence: str) -> bool:
    return bool(verify._ABSENCE_RE.search(sentence))


def _claim_type(chosen: list[dict[str, Any]], sentence: str) -> str:
    if not chosen:
        return "hypothesis"
    tools = {item["call"]["tool"] for item in chosen}
    if tools <= set(verify._DOCUMENT_TOOLS):
        return "analysis_note"
    if "operational_lookup" in tools and _causal(sentence):
        has_page = any(
            _citation_for(item).get("page") is not None for item in chosen
        )
        if has_page:
            return "management_stated"
    return "fact"


def _derive_missing(tool_trace: list[dict]) -> list[dict[str, Any]]:
    """Item-level missing states for the answer's focus period, from the tools."""
    states: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    calls = [
        call for call in verify._build_calls(tool_trace)
        if not call["errored"] and not call["context"]
    ]

    periods: list[tuple[str, int]] = []
    for call in calls:
        data = call["data"]
        period = str(data.get("period") or call["args"].get("period") or "").upper()
        year = str(data.get("year") or call["args"].get("year") or "")
        if period and year.isdigit():
            periods.append((period, int(year)))
    focus = (
        max(periods, key=lambda item: (item[1], paths.PERIOD_ORDER.get(item[0], 9)))
        if periods else None
    )

    def add(item: str, state: str, data: dict, call: dict, detail: str | None):
        period = str(data.get("period") or call["args"].get("period") or "").upper()
        year = str(data.get("year") or call["args"].get("year") or "")
        if focus:
            if not period:
                period, year = focus[0], str(focus[1])
            elif period != focus[0] or year != str(focus[1]):
                # A comparative-period gap is not what the answer is about.
                return
        ticker = verify._norm_id(data.get("ticker") or call["args"].get("ticker"))
        key = (ticker, period, year, verify._norm_metric(item))
        # Prefer a more specific state over the generic completeness gap.
        if key in states and states[key]["state"] != "not_captured":
            return
        tool_evidence = data.get("evidence")
        evidence = dict(tool_evidence) if isinstance(tool_evidence, dict) else {}
        evidence.update({
            "tool": call["tool"],
            "ticker": ticker.upper() or None,
            "period": period or None,
            "year": int(year) if year.isdigit() else year or None,
            "detail": evidence.get("detail") or detail,
        })
        states[key] = {"item": item, "state": state, "evidence": evidence}

    for call in calls:
        data = call["data"]
        if call["tool"] == "documents_status":
            state = str(data.get("state") or "").strip().lower()
            item = data.get("item")
            if item and state in ("not_captured", "not_disclosed"):
                add(str(item), state, data, call, "documents_status")
        elif call["tool"] == "operational_lookup":
            state = str(data.get("state") or "").strip().lower()
            if state in ("not_captured", "not_yet_published"):
                add("operational", state, data, call, "operational_lookup")
        elif call["tool"] == "company_facts":
            completeness = data.get("completeness")
            if isinstance(completeness, dict):
                for gap in completeness.get("gaps") or []:
                    if not isinstance(gap, dict):
                        continue
                    item = str(gap.get("item") or "")
                    state = str(gap.get("state") or "").strip().lower()
                    if item and state in ("not_captured", "not_disclosed"):
                        add(item, state, data, call, "completeness.gaps")
    return list(states.values())


def _citation_from_call(call: dict[str, Any]) -> dict[str, Any]:
    """A citation for a call with no ledger number (absence / driver)."""
    fields = verify._call_fields(call)
    citation: dict[str, Any] = {"tool": call["tool"]}
    ticker = call["data"].get("ticker") or call["args"].get("ticker")
    if ticker and verify._norm_id(ticker) in fields["ticker"]:
        citation["ticker"] = ticker
    item = (
        call["args"].get("item") or call["data"].get("item")
        or call["args"].get("metric") or call["data"].get("metric")
    )
    if item:
        citation["metric"] = item
    page = call["args"].get("page") or call["data"].get("page")
    if page is not None and int(page) in fields["page"]:
        citation["page"] = int(page)
    doc = call["args"].get("doc_id") or call["data"].get("id")
    if doc and verify._norm_id(doc) in fields["doc"]:
        citation["doc"] = doc
    return citation


def _absence_citation(sentence: str, calls: list[dict[str, Any]]) -> dict[str, Any] | None:
    lowered = sentence.casefold()
    candidates = [
        call for call in calls
        if call["tool"] in ("documents_status", "operational_lookup")
        and str(call["data"].get("state") or "").casefold()
        in ("not_captured", "not_yet_published", "not_disclosed")
    ]
    if not candidates:
        # completeness.gaps is item-level absence evidence too.
        candidates = [
            call for call in calls
            if call["tool"] == "company_facts"
            and isinstance(call["data"].get("completeness"), dict)
            and (call["data"]["completeness"].get("gaps") or [])
        ]
    if not candidates:
        return None
    # Prefer the call whose item or detail the sentence actually mentions.
    for call in candidates:
        item = str(call["args"].get("item") or call["data"].get("item") or "")
        detail = str((call["data"].get("evidence") or {}).get("detail") or "")
        if (item and item.replace("_", " ").casefold() in lowered) or (
            detail and detail.casefold()[:40] in lowered
        ):
            return _citation_from_call(call)
    return _citation_from_call(candidates[0])


def _driver_citation(
    sentence: str, calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    lowered = sentence.casefold()
    for call in calls:
        if call["tool"] != "operational_lookup":
            continue
        for driver in verify._as_items(call["data"].get("drivers")):
            if not isinstance(driver, dict):
                continue
            text = str(driver.get("driver") or "")
            phrase = text.casefold()
            if not phrase:
                continue
            # Match on a few content words, not the whole sentence.
            words = [w for w in re.split(r"[^a-z]+", phrase) if len(w) > 3]
            if words and sum(word in lowered for word in words) / len(words) >= 0.6:
                citation: dict[str, Any] = {"tool": "operational_lookup"}
                ticker = call["data"].get("ticker") or call["args"].get("ticker")
                if ticker:
                    citation["ticker"] = ticker
                if text:
                    citation["driver"] = text
                if driver.get("page") is not None:
                    citation["page"] = int(driver["page"])
                doc = driver.get("source_doc")
                if doc:
                    citation["doc"] = doc
                return citation
    return None


def derive_envelope(answer: str, tool_trace: list[dict]) -> tuple[AnswerEnvelope, dict]:
    """Build a verifiable envelope from prose + tool trace."""
    started = time.perf_counter()
    entries = _entries_with_calls(tool_trace)
    calls = [
        call for call in verify._build_calls(tool_trace)
        if not call["errored"] and not call["context"]
    ]
    sentences = split_sentences(answer)

    claims: list[dict[str, Any]] = []
    stats = {
        "sentences": len(sentences),
        "numeric_sentences": 0,
        "numbers": 0,
        "matched": 0,
        "ambiguous": 0,
        "unmatched": 0,
        "causal_sentences": 0,
        "absence_sentences": 0,
        "claims": 0,
        "unmatched_examples": [],
        "ambiguous_examples": [],
    }

    for sentence in sentences:
        mapping = map_sentence(sentence, entries)
        numbers = mapping["tokens"]
        causal = _causal(sentence)
        absence = _absence(sentence)
        if numbers:
            stats["numeric_sentences"] += 1
            stats["numbers"] += len(numbers)
            stats["matched"] += len(numbers) - len(mapping["unmatched"]) - len(mapping["ambiguous"])
            stats["ambiguous"] += len(mapping["ambiguous"])
            stats["unmatched"] += len(mapping["unmatched"])
        if causal:
            stats["causal_sentences"] += 1
        if absence:
            stats["absence_sentences"] += 1

        for token in mapping["unmatched"][:3]:
            stats["unmatched_examples"].append(
                {"sentence": sentence[:160], "value": token["value"], "percent": token["percent"]}
            )
        for item in mapping["ambiguous"][:3]:
            stats["ambiguous_examples"].append(
                {"sentence": sentence[:160], **item}
            )

        # A claim is needed when the sentence carries a number, a causal claim,
        # or an absence assertion. Otherwise it is prose and needs no label.
        if not (numbers or causal or absence):
            continue

        citations: list[dict[str, Any]] = []
        seen: set[tuple] = set()
        for item in mapping["entries"]:
            citation = _citation_for(item)
            key = _citation_key(citation)
            if key not in seen:
                seen.add(key)
                citations.append(citation)

        claim_type = _claim_type(mapping["entries"], sentence)

        # An absence sentence has no ledger number to anchor it. Cite the tool
        # call that returned the state, exactly as the answer pass saw it.
        if absence and not citations:
            absence_citation = _absence_citation(sentence, calls)
            if absence_citation:
                citations.append(absence_citation)
                seen.add(_citation_key(absence_citation))
                # An absence state is a tool fact, not an interpretation.
                claim_type = "fact"

        # A causal sentence with no number may be a management driver.
        if causal and not citations:
            driver_citation = _driver_citation(sentence, calls)
            if driver_citation:
                citations.append(driver_citation)
                claim_type = "management_stated"
        # management_stated is only legal with doc+page; downgrade otherwise.
        if claim_type == "management_stated" and not any(
            c.get("doc") and c.get("page") is not None for c in citations
        ):
            claim_type = "fact"
        if claim_type == "analysis_note" and not any(
            c.get("document") and c.get("quote") for c in citations
        ):
            # Note numbers need an exact quote the mapper cannot invent.
            claim_type = "hypothesis"

        claims.append({
            "text": sentence,
            "type": claim_type,
            "citations": citations,
        })
        stats["claims"] += 1

    missing = _derive_missing(tool_trace)

    tickers = {
        str(citation.get("ticker")).upper()
        for claim in claims for citation in claim["citations"]
        if citation.get("ticker")
    }
    # A comparison names tickers in three places: claims it cites, the
    # compare_supported call, and missing[] entries ("PTPS not disclosed").
    for call in calls:
        if call["tool"] != "compare_supported":
            continue
        for value in verify._as_items(call["args"].get("tickers")):
            tickers.add(str(value).upper())
        for value in verify._as_items(call["data"].get("columns")):
            tickers.add(str(value).upper())
    for item in missing:
        ticker = (item.get("evidence") or {}).get("ticker")
        if ticker:
            tickers.add(str(ticker).upper())
    tickers.discard("")
    coverage = None
    if len(tickers) > 1:
        coverage = Coverage(
            supported_tickers=len(paths.supported_periods()),
            compared_tickers=sorted(tickers),
            caveat="Supported names only; not an IDX-wide sector screen.",
        ).model_dump(exclude_none=True)

    envelope = AnswerEnvelope.model_validate({
        "answer": answer,
        "claims": claims,
        "missing": missing,
        "coverage": coverage,
    })
    stats["mapping_ms"] = round((time.perf_counter() - started) * 1000, 2)
    stats["claims_without_citations"] = sum(
        1 for claim in claims if not claim["citations"]
    )
    stats["missing_derived"] = len(missing)
    stats["compared_tickers"] = sorted(tickers)
    return envelope, stats


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def audit(answer: str, tool_trace: list[dict], mode: str = "desk") -> dict[str, Any]:
    envelope, stats = derive_envelope(answer, tool_trace)
    started = time.perf_counter()
    verification = verify.verify_envelope(envelope, tool_trace, mode=mode)
    stats["verify_ms"] = round((time.perf_counter() - started) * 1000, 2)
    return {
        "stats": stats,
        "verification": {
            "ok": verification["ok"],
            "violations": verification.get("violations") or [],
            "numbers": verification.get("numbers"),
            "citations": verification.get("citations"),
        },
        "envelope": envelope.model_dump(exclude_none=True),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--question")
    parser.add_argument("--from-trace", type=Path)
    parser.add_argument("--mode", default="desk", choices=["desk", "document"])
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    reports: list[dict[str, Any]] = []

    if args.from_trace:
        trace = json.loads(args.from_trace.read_text(encoding="utf-8"))
        answer = str((trace.get("envelope") or {}).get("answer") or "")
        report = audit(answer, trace.get("tool_trace") or [], args.mode)
        report["source"] = {"kind": "existing_trace", "path": str(args.from_trace)}
        reports.append(report)
    else:
        if not args.question:
            parser.error("--question or --from-trace is required")
        for index in range(max(1, args.runs)):
            run = run_answer_only(args.question, args.mode)
            report = audit(run["answer"], run["tool_trace"], args.mode)
            report["source"] = {
                "kind": "answer_only_run",
                "run": index + 1,
                "wall_s": run["wall_s"],
                "usage": run["usage"],
                "route": run["route"],
                "answer_chars": len(run["answer"]),
                "answer": run["answer"],
            }
            reports.append(report)
            s = report["stats"]
            print(
                f"run {index + 1}: wall {run['wall_s']}s | "
                f"sentences {s['sentences']} | numbers {s['numbers']} "
                f"(matched {s['matched']}, ambig {s['ambiguous']}, "
                f"unmatched {s['unmatched']}) | claims {s['claims']} | "
                f"audit {'OK' if report['verification']['ok'] else 'FAIL'} "
                f"({len(report['verification']['violations'])} violations) | "
                f"map {s['mapping_ms']}ms + verify {s['verify_ms']}ms",
                flush=True,
            )

    for report in reports:
        v = report["verification"]
        s = report["stats"]
        print("\n=== report ===")
        print(json.dumps({
            "source": {k: v for k, v in report.get("source", {}).items() if k != "answer"},
            "stats": s,
            "audit_ok": v["ok"],
            "violations": v["violations"],
            "numbers": v["numbers"],
            "citations": v["citations"],
        }, indent=2, default=str)[:6000])

    if args.json:
        args.json.write_text(
            json.dumps(reports, indent=2, default=str), encoding="utf-8"
        )
        print(f"\n[saved] {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
