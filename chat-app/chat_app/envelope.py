"""The answer envelope: schema, YAML parsing, and one conservative repair pass.

The model must answer with a single YAML document (CONTRACTS.md §4)::

    answer: |
      Markdown answer. Every number also appears in a claim.
    claims:
      - text: "Q2 coal production was 5.3 Mt, +13% QoQ."
        type: fact
        citations:
          - {tool: operational_lookup, ticker: ITMG, metric: coal_production,
             doc: itmg_2026-08-10_news_release, page: 1}
    missing:
      - {item: coal_production, state: not_captured, evidence: {url: "...", error: "403"}}

A claim may cite the answer sentence by index instead of repeating it::

    claims:
      - sentence: 3          # 1-based index into split_sentences(answer)
        type: fact
        citations: [...]

The envelope resolves the index to the full sentence during validation, so
every audit downstream still sees complete claim text.

Failure is loud: :func:`parse_envelope` raises :class:`EnvelopeError` after one
repair attempt. It never returns an empty envelope, because an empty envelope
would silently turn a malformed model response into a plausible-looking answer.
"""

from __future__ import annotations

import re
from typing import Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

MISSING_STATES = (
    "not_yet_published",
    "not_disclosed",
    "not_captured",
    "unsupported",
    "not_in_document",
)
CLAIM_TYPES = ("fact", "management_stated", "analysis_note", "hypothesis")


class EnvelopeError(Exception):
    """Raised when the model output cannot be turned into an envelope."""

    def __init__(self, message: str, *, detail: str | None = None):
        super().__init__(message)
        self.message = message
        self.detail = detail or ""

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        if self.detail and self.detail not in self.message:
            return f"{self.message}: {self.detail}"
        return self.message


class Citation(BaseModel):
    """Where one claim came from (CONTRACTS.md §4).

    ``tool`` is mandatory. Primary citations use ``ticker``/``metric``/
    ``driver``/``doc``/``page``/``row``. Document citations (the analysis vault)
    add ``document``/``section``/``line``/``quote``; the quote must resolve to a
    normalized substring of the cited section (MVP-PLAN §4.5 rule 8).
    """

    model_config = ConfigDict(extra="ignore")

    tool: str
    ticker: str | None = None
    metric: str | None = None
    driver: str | None = None
    doc: str | None = None
    id: str | None = None
    page: int | None = None
    row: int | None = None
    # Analysis-vault document citation (document mode / desk note citations).
    document: str | None = None
    section: str | None = None
    line: int | None = None
    quote: str | None = None

    @field_validator(
        "tool", "ticker", "metric", "driver", "doc", "id", "document",
        "section", "quote",
    )
    @classmethod
    def _strip(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("page", "row", "line", mode="before")
    @classmethod
    def _coerce_ref(cls, value: Any) -> int | None:
        """Accept ``row: "1311000:6"`` and ``page: "1"`` from model output."""
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        match = re.search(r"(\d+)\s*$", str(value))
        return int(match.group(1)) if match else None


_MARKDOWN_MARKERS_RE = re.compile(r"[*_`#>|]+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")


def split_sentences(text: str) -> list[str]:
    """Split prose into sentences (the one the audit and ``sentence:`` use).

    Markdown markers are stripped, then the text splits after sentence-ending
    punctuation followed by whitespace, and on newlines. A hard line break
    therefore ends a sentence, so ``sentence: N`` indices count wrapped lines
    as separate sentences. Empty fragments are dropped.
    """
    cleaned = _MARKDOWN_MARKERS_RE.sub(" ", text or "")
    parts = _SENTENCE_SPLIT_RE.split(cleaned)
    return [part.strip() for part in parts if part.strip()]


class Claim(BaseModel):
    """One labelled sentence (or sentence group) of the answer.

    The model may send ``sentence: N`` (1-based index into
    :func:`split_sentences` of ``answer``) instead of repeating a sentence in
    ``text``. :class:`AnswerEnvelope` resolves the index to the full sentence
    before any audit runs, so every claim downstream always has ``text``.
    """

    model_config = ConfigDict(extra="ignore")

    text: str | None = None
    sentence: int | None = None
    type: Literal["fact", "management_stated", "analysis_note", "hypothesis"]
    citations: list[Citation] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_text_or_sentence(self) -> "Claim":
        if not self.text and self.sentence is None:
            raise ValueError(
                "claim needs `text` or `sentence` (1-based sentence index in "
                "answer)"
            )
        return self

    @field_validator("type", mode="before")
    @classmethod
    def _normalize_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("text")
    @classmethod
    def _clean_text(cls, value: str | None) -> str | None:
        """An empty ``text`` becomes absent; the claim must then index a sentence."""
        if value is None:
            return None
        return str(value).strip() or None

    @field_validator("sentence", mode="before")
    @classmethod
    def _coerce_sentence(cls, value: Any) -> int | None:
        """Accept ``sentence: "2"`` / ``sentence: 2.0`` from model output."""
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError("claim sentence must be a 1-based integer index")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        match = re.search(r"\d+", str(value))
        if not match:
            raise ValueError(
                f"claim sentence {value!r} is not a 1-based sentence index"
            )
        return int(match.group(0))

    @field_validator("citations", mode="before")
    @classmethod
    def _null_citations(cls, value: Any) -> Any:
        return [] if value is None else value


class Missing(BaseModel):
    """A deterministic missing-data state plus its evidence (MVP-PLAN §4.3)."""

    model_config = ConfigDict(extra="ignore")

    item: str
    state: Literal[
        "not_yet_published",
        "not_disclosed",
        "not_captured",
        "unsupported",
        "not_in_document",
    ]
    evidence: dict = Field(default_factory=dict)

    @field_validator("state", mode="before")
    @classmethod
    def _normalize_state(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("evidence", mode="before")
    @classmethod
    def _null_evidence(cls, value: Any) -> Any:
        return {} if value is None else value


class Coverage(BaseModel):
    """Desk-mode coverage block (MVP-PLAN §4.4).

    ``supported_tickers`` is what ``list_companies`` returned, not a sector
    estimate. ``compared_tickers`` is exactly what the answer compared.
    """

    model_config = ConfigDict(extra="ignore")

    scope: str = "supported_set"
    supported_tickers: int | None = None
    compared_tickers: list[str] = Field(default_factory=list)
    caveat: str | None = None

    @field_validator("scope", "caveat")
    @classmethod
    def _strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("supported_tickers", mode="before")
    @classmethod
    def _coerce_count(cls, value: Any) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @field_validator("compared_tickers", mode="before")
    @classmethod
    def _tickers(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        out: list[str] = []
        for item in value:
            text = str(item).strip().upper()
            if text and text not in out:
                out.append(text)
        return out


class AnswerEnvelope(BaseModel):
    """The complete model answer (CONTRACTS.md §4)."""

    model_config = ConfigDict(extra="ignore")

    answer: str
    claims: list[Claim] = Field(default_factory=list)
    missing: list[Missing] = Field(default_factory=list)
    coverage: Coverage | None = None

    @field_validator("answer")
    @classmethod
    def _non_empty_answer(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("answer must not be empty")
        return text

    @field_validator("claims", "missing", mode="before")
    @classmethod
    def _null_lists(cls, value: Any) -> Any:
        return [] if value is None else value

    @field_validator("coverage", mode="before")
    @classmethod
    def _null_coverage(cls, value: Any) -> Any:
        return None if value is None else value

    @model_validator(mode="after")
    def _resolve_sentence_claims(self) -> "AnswerEnvelope":
        """Resolve ``claims[].sentence`` (1-based) into full sentence ``text``.

        A compact claim repeats no prose, so the envelope fills the text here —
        before parsing returns — and every audit keeps seeing resolved text.

        A stale index is not a schema failure. If the claim also carries
        ``text``, the text is authoritative and the index is discarded. If it
        has no text, the claim is dropped: the deterministic audit still sees
        the answer and flags any now-uncovered number or causal sentence. This
        keeps one miscounted index from turning an otherwise usable answer into
        an ``envelope_invalid`` retry loop.
        """
        if not any(claim.sentence is not None for claim in self.claims):
            return self
        sentences = split_sentences(self.answer)
        kept: list[Claim] = []
        for claim in self.claims:
            if claim.sentence is None:
                kept.append(claim)
                continue
            index = claim.sentence
            if index < 1 or index > len(sentences):
                if claim.text:
                    claim.sentence = None
                    kept.append(claim)
                continue
            if not claim.text:
                claim.text = sentences[index - 1]
            kept.append(claim)
        self.claims = kept
        return self


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"```(?:ya?ml)?[ \t]*\r?\n(.*?)```", re.IGNORECASE | re.DOTALL)
_ANSWER_KEY_RE = re.compile(r"(?m)^[ \t]{0,3}answer[ \t]*:")


def _strip_fences(text: str) -> str:
    """Return the YAML body of a fenced block, or slice from the first key."""
    raw = (text or "").strip()
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    # Unfenced output with a chatty preamble: start at the first top-level key.
    match = _ANSWER_KEY_RE.search(raw)
    if match and match.start() > 0:
        return raw[match.start():].strip()
    return raw


def _format_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors()[:6]:
        location = ".".join(str(item) for item in error.get("loc") or ())
        parts.append(f"{location or '<root>'}: {error.get('msg', 'invalid')}")
    return "; ".join(parts)


def _load(text: str) -> AnswerEnvelope:
    """Parse one YAML candidate; raises EnvelopeError with parser detail."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise EnvelopeError("output is not valid YAML", detail=str(exc)) from exc

    if data is None:
        raise EnvelopeError("output is empty (no YAML document)")
    if not isinstance(data, dict):
        raise EnvelopeError(
            "output must be one YAML mapping with answer/claims/missing",
            detail=f"parsed {type(data).__name__}",
        )

    # A model that emits only claims is still a broken envelope.
    if "answer" not in data:
        raise EnvelopeError(
            "output is missing the required `answer` key",
            detail=f"keys: {sorted(str(key) for key in data)}",
        )

    try:
        return AnswerEnvelope.model_validate(data)
    except ValidationError as exc:
        raise EnvelopeError(
            "output does not match the answer-envelope schema",
            detail=_format_validation_error(exc),
        ) from exc


def _flow_depth(line: str, depth: int) -> int:
    """Track []/{} nesting outside quoted scalars for one line."""
    quote: str | None = None
    escaped = False
    for char in line:
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth = max(0, depth - 1)
    return depth


_BLOCK_SCALAR_RE = re.compile(r"(?:^|\s)[|>][+-]?\s*(?:#.*)?$")


def _block_content_lines(lines: list[str]) -> list[bool]:
    """Flag the lines that belong to a block scalar (``answer: |`` …)."""
    flags: list[bool] = []
    block_indent: int | None = None
    pending_indent: int | None = None
    for line in lines:
        stripped = line.rstrip()
        indent = len(line) - len(line.lstrip(" "))
        if pending_indent is not None:
            if not stripped:
                flags.append(True)
                continue
            if indent > pending_indent:
                block_indent = indent
                pending_indent = None
                flags.append(True)
                continue
            # No indented content follows: the header had an empty body.
            pending_indent = None
        elif block_indent is not None:
            if not stripped or indent >= block_indent:
                flags.append(True)
                continue
            block_indent = None
        flags.append(False)
        if _BLOCK_SCALAR_RE.search(stripped):
            pending_indent = indent
    return flags


def _strip_trailing_commas(text: str) -> str:
    """Drop a comma at end of line only at block level (depth 0).

    Flow collections legitimately separate their items with commas, so touching
    them would risk making a valid document invalid. Block-scalar prose
    (``answer: |``) is left byte-for-byte intact. The common model mistake is
    ``type: fact,`` at block level; that comma is always removable.
    """
    raw_lines = text.splitlines(keepends=True)
    in_block = _block_content_lines([line.rstrip("\r\n") for line in raw_lines])
    out: list[str] = []
    depth = 0
    for index, line in enumerate(raw_lines):
        body = line.rstrip("\r\n")
        ending = line[len(body):]
        if in_block[index]:
            out.append(line)
            continue
        stripped = body.rstrip()
        if depth == 0 and stripped.endswith(",") and not stripped.lstrip().startswith("#"):
            body = stripped[:-1]
            ending = ending or "\n"
        depth = _flow_depth(body, depth)
        out.append(body + ending)
    return "".join(out)


def _balance_flow_collections(text: str) -> str:
    """Append the closers for any flow collection left open at EOF.

    Block-scalar prose is ignored: a bracketed phrase in the answer is text, not
    YAML structure.
    """
    raw_lines = text.splitlines(keepends=True)
    in_block = _block_content_lines([line.rstrip("\r\n") for line in raw_lines])
    stack: list[str] = []
    quote: str | None = None
    escaped = False
    for index, line in enumerate(raw_lines):
        if in_block[index]:
            continue
        for char in line:
            if escaped:
                escaped = False
                continue
            if char == "\\" and quote == '"':
                escaped = True
                continue
            if quote:
                if char == quote:
                    quote = None
                continue
            if char in ("'", '"'):
                quote = char
            elif char in "[{":
                stack.append("]" if char == "[" else "}")
            elif char in "]}":
                if stack and stack[-1] == char:
                    stack.pop()
    if not stack:
        return text
    return text.rstrip() + "".join(reversed(stack)) + "\n"


def _repair(text: str) -> str:
    """One conservative repair pass (CONTRACTS.md §4)."""
    repaired = _strip_trailing_commas(text)
    repaired = _balance_flow_collections(repaired)
    return repaired


def parse_envelope(text: str) -> AnswerEnvelope:
    """Parse model output into an :class:`AnswerEnvelope`.

    Strips ``` fences, tries once, applies one conservative repair (trailing
    commas, unbalanced flow collections) and tries again. Always raises
    :class:`EnvelopeError` on failure — never returns an empty envelope.
    """
    body = _strip_fences(text)
    if not body:
        raise EnvelopeError("output is empty")

    try:
        return _load(body)
    except EnvelopeError as first:
        repaired = _repair(body)
        if repaired == body:
            raise
        try:
            return _load(repaired)
        except EnvelopeError as second:
            raise EnvelopeError(
                "output is not a valid answer envelope after one repair pass",
                detail=f"{first}; after repair: {second}",
            ) from second


_CLAIMS_KEY_RE = re.compile(r"(?m)^[ \t]{0,3}(?:answer|claims)[ \t]*:")


def _strip_claims_fences(text: str) -> str:
    """Return the YAML body of a claims-only reply (fences or a preamble)."""
    raw = (text or "").strip()
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    match = _CLAIMS_KEY_RE.search(raw)
    if match and match.start() > 0:
        return raw[match.start():].strip()
    return raw


def _load_claims(text: str, answer: str) -> AnswerEnvelope:
    """Parse a claims-only mapping and merge it with already-written prose.

    The prose ``answer`` is authoritative: any ``answer`` key in the model
    output is ignored (a model that returns a full envelope still contributes
    only its ``claims``/``missing``/``coverage``). ``claims[].sentence`` is
    resolved against the prose, so the compact form works here too.
    """
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise EnvelopeError("output is not valid YAML", detail=str(exc)) from exc

    if data is None:
        raise EnvelopeError("output is empty (no YAML document)")
    if not isinstance(data, dict):
        raise EnvelopeError(
            "output must be one YAML mapping with claims/missing/coverage",
            detail=f"parsed {type(data).__name__}",
        )
    if not any(key in data for key in ("claims", "missing", "coverage")):
        raise EnvelopeError(
            "output has no claims, missing, or coverage key",
            detail=f"keys: {sorted(str(key) for key in data)}",
        )

    merged = {
        "answer": answer,
        "claims": data.get("claims") or [],
        "missing": data.get("missing") or [],
        "coverage": data.get("coverage"),
    }
    try:
        return AnswerEnvelope.model_validate(merged)
    except ValidationError as exc:
        raise EnvelopeError(
            "claims-only output does not match the answer-envelope schema",
            detail=_format_validation_error(exc),
        ) from exc


def parse_claims_envelope(text: str, answer: str) -> AnswerEnvelope:
    """Merge a claims-only reply with ``answer`` and build the full envelope.

    Phase 2 of the two-phase agent path returns only ``claims``/``missing``/
    ``coverage``; the prose was written by phase 1 and is never re-generated.
    Applies the same one conservative repair as :func:`parse_envelope` and
    always raises :class:`EnvelopeError` on failure.
    """
    prose = str(answer or "").strip()
    if not prose:
        raise EnvelopeError("cannot attach claims to an empty answer")

    body = _strip_claims_fences(text)
    if not body:
        raise EnvelopeError("output is empty")

    try:
        return _load_claims(body, prose)
    except EnvelopeError as first:
        repaired = _repair(body)
        if repaired == body:
            raise
        try:
            return _load_claims(repaired, prose)
        except EnvelopeError as second:
            raise EnvelopeError(
                "output is not a valid claims-only envelope after one repair pass",
                detail=f"{first}; after repair: {second}",
            ) from second


def render_envelope_yaml(envelope: AnswerEnvelope) -> str:
    """Serialize an envelope as YAML (tests, traces, retry prompts)."""
    data = envelope.model_dump(exclude_none=True)
    return yaml.safe_dump(
        data,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=1000,
    )
