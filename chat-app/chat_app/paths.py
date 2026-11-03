"""Artifact resolution, the supported-set manifest, and shared IO helpers.

Every path the chat app reads or writes is declared here. Tools never guess a
filename; they resolve it through this module so the supported set stays a
single source of truth (MVP-PLAN §2).
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
IDX_DATA = PROJECT_ROOT / "idx_data"
FACTS_DIR = IDX_DATA / "analysis" / "facts"
PREPROCESSED_DIR = IDX_DATA / "analysis" / "preprocessed"
RESEARCH_DIR = IDX_DATA / "analysis" / "research"
WRITE_DIR = IDX_DATA / "analysis" / "write"
OPERATIONAL_DIR = IDX_DATA / "operational"
TRACES_DIR = IDX_DATA / "chat" / "traces"
LOG_DIR = IDX_DATA / "chat"
NOTES_DB = IDX_DATA / "notes_rag.duckdb"
PIPELINE_DB = IDX_DATA / "pipeline.duckdb"
ANALYSIS_DIR = PROJECT_ROOT / "analysis"
REPORTS_DIR = IDX_DATA / "reports"
RAW_DIR = PROJECT_ROOT / "raw"
RESULTS_DIR = PROJECT_ROOT / "results"

PACKAGE_DIR = Path(__file__).resolve().parent
STATIC_DIR = PACKAGE_DIR / "static"
PROMPTS_DIR = PACKAGE_DIR / "prompts"
CHAT_APP_DIR = PROJECT_ROOT / "chat-app"
EVAL_DIR = CHAT_APP_DIR / "eval"

QUARTERS = ("Q1", "Q2", "Q3", "Q4")
PERIOD_ORDER = {q: i for i, q in enumerate(QUARTERS)}

# Hard cap on what a single tool call may return to the model (MVP-PLAN §5 WS4).
OUTPUT_CAP_BYTES = 12_000

# Canonical statement line keys, mirrored from scripts/analysis_pipeline/schema.py.
STATEMENTS = ("income_statement", "balance_sheet", "cash_flow")

_TICKER_RE = re.compile(r"^[A-Z]{4}$")
_PERIOD_RE = re.compile(r"^(Q[1-4]|FY)$", re.IGNORECASE)
_FACT_FILE_RE = re.compile(r"^(?P<ticker>[A-Z]{4})_(?P<quarter>Q[1-4])_(?P<year>20\d{2})\.yaml$")


class ChatError(Exception):
    """Base class for errors a tool surfaces to the agent as JSON."""

    code = "error"

    def __init__(self, message: str, **payload: Any):
        super().__init__(message)
        self.message = message
        self.payload = payload

    def as_dict(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message, **self.payload}


class UnsupportedRequest(ChatError):
    """Ticker/period outside the supported set. Carries the alternatives."""

    code = "unsupported"


class ToolInputError(ChatError):
    code = "invalid_input"


class ArtifactMissing(ChatError):
    code = "artifact_missing"


def normalize_ticker(ticker: str) -> str:
    value = (ticker or "").strip().upper()
    if not _TICKER_RE.match(value):
        raise ToolInputError(
            f"ticker must be a 4-letter IDX code, got {ticker!r}",
        )
    return value


def normalize_quarter(quarter: str) -> str:
    value = (quarter or "").strip().upper()
    if not _PERIOD_RE.match(value):
        raise ToolInputError(
            f"period must be Q1-Q4 or FY, got {quarter!r}",
        )
    return value


def normalize_year(year: int | str) -> int:
    try:
        value = int(str(year).strip())
    except (TypeError, ValueError) as exc:
        raise ToolInputError(f"year must be an integer, got {year!r}") from exc
    if not 2000 <= value <= 2100:
        raise ToolInputError(f"year must be between 2000 and 2100, got {value}")
    return value


def fact_path(ticker: str, quarter: str, year: int | str) -> Path:
    return FACTS_DIR / f"{normalize_ticker(ticker)}_{normalize_quarter(quarter)}_{normalize_year(year)}.yaml"


def ratios_path(ticker: str, quarter: str, year: int | str) -> Path:
    return FACTS_DIR / f"{normalize_ticker(ticker)}_{normalize_quarter(quarter)}_{normalize_year(year)}.ratios.yaml"


def operational_path(ticker: str, quarter: str, year: int | str) -> Path:
    return OPERATIONAL_DIR / f"{normalize_ticker(ticker)}_{normalize_quarter(quarter)}_{normalize_year(year)}_operational.json"


def trace_path(trace_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", trace_id)
    return TRACES_DIR / f"{safe}.json"


def load_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ArtifactMissing(f"artifact not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ChatError(f"artifact is not valid YAML: {path}", detail=str(exc)) from exc


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ArtifactMissing(f"artifact not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ChatError(f"artifact is not valid JSON: {path}", detail=str(exc)) from exc


def file_versions(files: Iterable[Path | str]) -> tuple[tuple[str, int, int], ...]:
    """Cheap, sorted ``(path, mtime_ns, size)`` key for cache invalidation.

    Callers pass the exact files (or a directory glob) whose contents a cached
    result depends on. Missing files are omitted, so creating or deleting an
    artifact changes the key; rewriting or touching one changes ``mtime_ns`` /
    ``size``. The path is part of the tuple so a rename invalidates too.
    """
    out: list[tuple[str, int, int]] = []
    for item in files:
        path = Path(item)
        try:
            stat = path.stat()
        except OSError:
            continue
        out.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(out))


def dump_json(data: Any, *, cap: int = OUTPUT_CAP_BYTES) -> str:
    """Serialize a tool result, truncating oversized payloads deterministically."""
    text = json.dumps(data, ensure_ascii=False, indent=2, default=_json_default)
    if len(text.encode("utf-8")) <= cap:
        return text
    trimmed = _trim_payload(data, cap)
    trimmed.setdefault("truncated", True) if isinstance(trimmed, dict) else None
    text = json.dumps(trimmed, ensure_ascii=False, indent=2, default=_json_default)
    if len(text.encode("utf-8")) > cap:
        text = text.encode("utf-8")[:cap].decode("utf-8", errors="ignore")
    return text


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _trim_payload(data: Any, cap: int) -> Any:
    """Drop large optional lists before truncating the serialized text."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    # Keep the tail useful: shrink long lists to their first 25 entries.
    for key, value in list(out.items()):
        if isinstance(value, list) and len(value) > 25:
            out[key] = value[:25]
            out[f"{key}_omitted"] = len(value) - 25
        elif isinstance(value, str) and len(value) > 4000:
            out[key] = value[:4000] + "…"
    if isinstance(out.get("sections"), dict):
        out["sections"] = _trim_payload(out["sections"], cap)
    return out


def supported_periods() -> dict[str, list[dict[str, Any]]]:
    """Scan canonical fact sheets. Returns {ticker: [{period, year, ...}]}."""
    found: dict[str, list[dict[str, Any]]] = {}
    if not FACTS_DIR.exists():
        return found
    for path in sorted(FACTS_DIR.glob("*.yaml")):
        match = _FACT_FILE_RE.match(path.name)
        if not match:
            continue
        ticker = match.group("ticker")
        quarter = match.group("quarter")
        year = int(match.group("year"))
        found.setdefault(ticker, []).append({
            "period": quarter,
            "year": year,
            "fact_path": str(path.relative_to(PROJECT_ROOT)),
            "has_ratios": ratios_path(ticker, quarter, year).exists(),
            "has_operational": operational_path(ticker, quarter, year).exists(),
        })
    for entries in found.values():
        entries.sort(key=lambda e: (e["year"], PERIOD_ORDER.get(e["period"], 9)))
    return found


def supported_tickers() -> list[str]:
    return sorted(supported_periods())


def is_supported(ticker: str, quarter: str | None = None, year: int | str | None = None) -> bool:
    ticker = (ticker or "").strip().upper()
    entries = supported_periods().get(ticker)
    if not entries:
        return False
    if quarter is None and year is None:
        return True
    q = normalize_quarter(quarter) if quarter else None
    y = normalize_year(year) if year is not None else None
    return any((q is None or e["period"] == q) and (y is None or e["year"] == y) for e in entries)


def supported_summary() -> list[dict[str, Any]]:
    """Compact per-ticker coverage, used by list_companies and the UI picker."""
    periods = supported_periods()
    out: list[dict[str, Any]] = []
    for ticker, entries in periods.items():
        out.append({
            "ticker": ticker,
            "periods": [f"{e['period']} {e['year']}" for e in entries],
            "operational_periods": [
                f"{e['period']} {e['year']}" for e in entries if e["has_operational"]
            ],
        })
    return out


def require_supported(ticker: str, quarter: str | None = None, year: int | str | None = None) -> tuple[str, str | None, int | None]:
    """Validate a request against the supported set, raising a helpful error."""
    tk = normalize_ticker(ticker)
    entries = supported_periods().get(tk)
    if not entries:
        raise UnsupportedRequest(
            f"{tk} is not in the supported set",
            supported_tickers=supported_tickers(),
        )
    if quarter is None and year is None:
        return tk, None, None
    q = normalize_quarter(quarter) if quarter else None
    y = normalize_year(year) if year is not None else None
    if not any((q is None or e["period"] == q) and (y is None or e["year"] == y) for e in entries):
        raise UnsupportedRequest(
            f"{tk} {q or ''} {y or ''}".strip() + " is not in the supported set",
            supported_periods=[f"{e['period']} {e['year']}" for e in entries],
        )
    return tk, q, y


def scale_multiplier(units: str | None) -> float:
    """Value in the sheet's units → full currency units."""
    u = (units or "").strip().lower()
    if u in ("million", "jutaan", "in million", "millions"):
        return 1_000_000.0
    if u in ("thousand", "ribuan", "in thousand", "thousands"):
        return 1_000.0
    return 1.0


def to_idr_bn(value: float | None, currency: str | None, units: str | None) -> Optional[float]:
    """Canonical display unit: billions of IDR.

    USD figures are converted with a clearly-labelled static rate; the conversion
    is emitted in the tool payload so the auditor can see it.
    """
    if value is None or not math.isfinite(value):
        return None
    full = value * scale_multiplier(units)
    cur = (currency or "IDR").upper()
    if cur == "IDR":
        return full / 1e9
    if cur == "USD":
        return full * USD_IDR_RATE / 1e9
    return full / 1e9


# Static, explicitly-labelled conversion used only for display comparability.
# The raw filing value always travels alongside it.
USD_IDR_RATE = 16_300.0


def dedupe_preserve(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out
