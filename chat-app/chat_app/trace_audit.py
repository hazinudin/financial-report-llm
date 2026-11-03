"""Derive an auditable envelope from prose + the tool trace.

The model writes Markdown only; this module maps each sentence's numbers to the
ledger produced by the tool trace, synthesises claims/citations/missing/coverage,
and leaves the actual gate to :func:`chat_app.verify.verify_envelope`.

This is the production form of ``chat-app/experiments/trace_audit.py``. It
imports a few private verifier helpers on purpose: the mapper and the audit must
tokenise and match values identically.
"""
from __future__ import annotations

import re
import time
from typing import Any

from . import paths, verify
from .envelope import AnswerEnvelope, Coverage, split_sentences


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


def _document_entries(tool_trace: list[dict]) -> list[dict[str, Any]]:
    """Note-ledger entries with the full ``document/section/line/quote`` anchor.

    The verifier resolves a note citation against the note the model actually
    saw, so an entry without all four fields cannot be cited and is skipped.
    """
    calls = verify._build_calls(tool_trace)
    index = verify._document_index(calls)
    out: list[dict[str, Any]] = []
    for entry in verify._document_ledger(calls, index):
        citation = _document_citation({"entry": entry, "document": True})
        if not (
            citation.get("document")
            and citation.get("section")
            and citation.get("line") is not None
            and citation.get("quote")
        ):
            continue
        out.append({"entry": entry, "document": True})
    return out


_NOTE_QUOTE_CHARS = 280


def _note_quote(quote: Any, entry: dict[str, Any]) -> str:
    """Trim a note line to a readable window around the matched number.

    The verifier accepts any quote that contains the number and is itself
    contained in the line-level document-ledger quote, so a window keeps the
    stored claim compact without weakening the audit.
    """
    text = str(quote or "").strip()
    if len(text) <= _NOTE_QUOTE_CHARS:
        return text
    tokens = verify._tokens(text)
    target: float | None = None
    try:
        target = float(entry.get("value"))
    except (TypeError, ValueError):
        target = None

    def window(token: dict[str, Any]) -> str:
        start = max(0, int(token["start"]) - _NOTE_QUOTE_CHARS // 2)
        end = min(len(text), start + _NOTE_QUOTE_CHARS)
        return text[start:end].strip()

    if target is not None:
        # Prefer the token that spells the ledger value exactly; scaled
        # variants ("1.0" for 1,000) match weaker entries first.
        for token in tokens:
            if verify._within(token["value"], target):
                return window(token)
    for token in tokens:
        if verify._entry_matches(
            token["value"], token["percent"], entry, token["decimals"],
        ):
            return window(token)
    return text[:_NOTE_QUOTE_CHARS]


def _document_citation(item: dict[str, Any]) -> dict[str, Any]:
    entry = item["entry"]
    meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else {}
    document = entry.get("document") or meta.get("document")
    section = entry.get("section") or meta.get("section")
    line = entry.get("line")
    if line is None:
        line = meta.get("line")
    quote = _note_quote(entry.get("quote") or meta.get("quote"), entry)
    citation: dict[str, Any] = {"tool": "analysis_document"}
    if document:
        citation["document"] = document
    if section:
        citation["section"] = section
    if line is not None:
        try:
            citation["line"] = int(line)
        except (TypeError, ValueError):
            pass
    if quote:
        citation["quote"] = quote
    return citation


def _citation_for(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("document"):
        return _document_citation(item)
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


def _citation_key(citation: dict[str, Any]) -> tuple:
    return tuple(sorted((key, str(value)) for key, value in citation.items()))


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


def map_sentence(sentence: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Match a sentence's numbers to ledger entries."""
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

    def matches(token: dict[str, Any], item: dict[str, Any]) -> bool:
        if item.get("document"):
            return verify._document_entry_matches(
                token["value"], token["percent"], item["entry"],
                token["decimals"],
            )
        return verify._entry_matches(
            token["value"], token["percent"], item["entry"], token["decimals"],
        )

    for token in tokens:
        candidates = [
            item for item in entries
            if matches(token, item)
        ]
        # In document mode the pinned note is primary: when a number appears in
        # both the note ledger and a filing, cite the note.
        note_candidates = [
            item for item in candidates if item.get("document")
        ]
        if note_candidates:
            candidates = note_candidates
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
        # ("Net profit ..." -> net_profit, not np_owners).
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
                    "tool": (
                        candidate["call"]["tool"]
                        if candidate.get("call") else "analysis_document"
                    ),
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
    # Any note-backed citation makes the sentence a note claim. A mixed claim
    # (note value beside a filing value) stays analysis_note so the note number
    # is never spent as a fact.
    if any(item.get("document") for item in chosen):
        return "analysis_note"
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


def _absence_citation(
    sentence: str, calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
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
            words = [word for word in re.split(r"[^a-z]+", phrase) if len(word) > 3]
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


def _derive_missing(tool_trace: list[dict], *, mode: str = "desk") -> list[dict[str, Any]]:
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
                return
        ticker = verify._norm_id(data.get("ticker") or call["args"].get("ticker"))
        key = (ticker, period, year, verify._norm_metric(item))
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
        elif call["tool"] == "analysis_search":
            # Note silence (`not_in_document`) is document-mode language; desk
            # answers must use a filing state instead.
            if mode != "document":
                continue
            if str(data.get("state") or "").strip().lower() != "not_in_document":
                continue
            query = str(data.get("query") or "").strip()
            add(query or "note item", "not_in_document", data, call,
                "analysis_search")
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


def derive_envelope(
    answer: str, tool_trace: list[dict], *, mode: str = "desk",
) -> tuple[AnswerEnvelope, dict[str, Any]]:
    """Build a verifiable envelope from prose + tool trace."""
    started = time.perf_counter()
    entries = _entries_with_calls(tool_trace)
    if mode == "document":
        # Note entries first so a sentence that spends a note number is anchored
        # to the note even when the same value also exists in a filing.
        entries = _document_entries(tool_trace) + entries
    calls = [
        call for call in verify._build_calls(tool_trace)
        if not call["errored"] and not call["context"]
    ]
    sentences = split_sentences(answer)

    claims: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "sentences": len(sentences),
        "numeric_sentences": 0,
        "numbers": 0,
        "matched": 0,
        "ambiguous": 0,
        "unmatched": 0,
        "causal_sentences": 0,
        "absence_sentences": 0,
        "claims": 0,
        "claims_without_citations": 0,
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
            stats["matched"] += (
                len(numbers) - len(mapping["unmatched"]) - len(mapping["ambiguous"])
            )
            stats["ambiguous"] += len(mapping["ambiguous"])
            stats["unmatched"] += len(mapping["unmatched"])
        if causal:
            stats["causal_sentences"] += 1
        if absence:
            stats["absence_sentences"] += 1

        for token in mapping["unmatched"][:3]:
            stats["unmatched_examples"].append(
                {"sentence": sentence[:160], "value": token["value"],
                 "percent": token["percent"]}
            )
        for item in mapping["ambiguous"][:3]:
            stats["ambiguous_examples"].append({"sentence": sentence[:160], **item})

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

        if absence and not citations:
            absence_citation = _absence_citation(sentence, calls)
            if absence_citation:
                citations.append(absence_citation)
                seen.add(_citation_key(absence_citation))
                # An absence state is a tool fact, not an interpretation.
                claim_type = "fact"

        if causal and not citations:
            driver_citation = _driver_citation(sentence, calls)
            if driver_citation:
                citations.append(driver_citation)
                claim_type = "management_stated"

        if claim_type == "management_stated" and not any(
            citation.get("doc") and citation.get("page") is not None
            for citation in citations
        ):
            claim_type = "fact"
        if claim_type == "analysis_note" and not any(
            citation.get("document") and citation.get("quote")
            for citation in citations
        ):
            claim_type = "hypothesis"

        claims.append({
            "text": sentence,
            "type": claim_type,
            "citations": citations,
        })
        stats["claims"] += 1

    missing = _derive_missing(tool_trace, mode=mode)

    tickers = {
        str(citation.get("ticker")).upper()
        for claim in claims for citation in claim["citations"]
        if citation.get("ticker")
    }
    # A comparison names tickers in claims, the compare call, and missing[].
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


def audit(
    answer: str, tool_trace: list[dict], *, mode: str = "desk",
) -> tuple[AnswerEnvelope, dict[str, Any], dict[str, Any]]:
    """Derive an envelope and run the unchanged audit."""
    envelope, stats = derive_envelope(answer, tool_trace, mode=mode)
    started = time.perf_counter()
    verification = verify.verify_envelope(envelope, tool_trace, mode=mode)
    stats["verify_ms"] = round((time.perf_counter() - started) * 1000, 2)
    return envelope, stats, verification
