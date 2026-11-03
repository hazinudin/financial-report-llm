"""Class B operational extraction for the IDX research chat (MVP-PLAN §4.2, WS3.3-3.6).

Design rules, in order of importance:

1. **Numbers are deterministic.** ``metrics.yaml`` supplies the grammar; every
   value comes from a numeric group in a sentence on a specific page. The LLM
   pass only ever contributes verbatim quotes + an ``affects`` label for
   management-stated drivers — never a number as data.
2. **Bilingual, no double counting.** Each page is scored for EN vs ID. English
   pages win when present (ID-only documents fall back to the Indonesian
   patterns). One metric + one scope yields at most one entry, whichever
   language half it came from; the original page number is preserved.
3. **Reconcile-or-reject.** Metrics that overlap an XBRL line are compared
   against ``xlsx_seed`` (read-only) with tolerance ``max(0.5%, rounding unit)``.
   A miss marks the entry ``rejected``, adds an ``extraction_warnings`` item and
   sets ``completeness.operational = partial``. Nothing is silently dropped.

The artifact shape mirrors ``idx_data/operational/test/ITMG_Q2_2026_operational.json``
(the ITMG oracle) plus the flat ``entries`` / ``drivers`` lists that
``chat-app/CONTRACTS.md`` §1 defines for ``operational_lookup``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional, Sequence

import yaml

try:  # normal import (tests, `python -m chat_app.extract_operational`)
    from chat_app import ingest, paths
except ImportError:  # executed as a file: chat-app/ is the import root
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from chat_app import ingest, paths


METRICS_PATH = paths.PACKAGE_DIR / "metrics.yaml"
ANALYSIS_PIPELINE_DIR = paths.PROJECT_ROOT / "scripts" / "analysis_pipeline"

# Module-level so tests can point the extractor at a temporary tree / XBRL seed.
OPERATIONAL_DIR: Path = paths.OPERATIONAL_DIR
PROJECT_ROOT: Path = paths.PROJECT_ROOT


# --------------------------------------------------------------------------- #
# metric registry
# --------------------------------------------------------------------------- #

# Category decides where an entry is listed inside `operational` (the oracle's
# production / sales / price / balance_sheet / other blocks). "financials"
# metrics are XBRL overlaps: they live in `entries` and `reconciliation`.
CATEGORY = {
    "coal_production": "production",
    "coal_sales_volume": "sales",
    "asp": "price",
    "total_assets": "balance_sheet",
    "cash_and_equivalents": "balance_sheet",
    "short_term_deposits": "balance_sheet",
    "total_liabilities": "balance_sheet",
    "total_equity": "balance_sheet",
    "royalties_to_government": "other",
    "treasury_share_purchase": "other",
    "strip_ratio": "other",
    "cash_cost": "other",
    "revenue": "financials",
    "net_profit": "financials",
    "operating_cash_flow": "financials",
}

CATEGORY_ORDER = ("production", "sales", "price", "balance_sheet", "other")
METRIC_ORDER = {key: index for index, key in enumerate(CATEGORY)}
SCOPE_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "6M": 5, "9M": 6, "FY": 7}

# Keywords used to locate a metric in a sentence. The regexes in metrics.yaml
# encode the number grammar; these encode "which sentence is about this metric".
KEYWORDS: dict[str, list[str]] = {
    "coal_production": ["coal production", "production volume", "production volumes",
                        "produksi batu bara", "produksi batubara", "volume produksi"],
    "coal_sales_volume": ["coal sales volume", "coal sales", "sales volume",
                          "sales volumes", "volume penjualan", "penjualan batu bara",
                          "penjualan batubara", "penjualan"],
    "asp": ["average selling price", "selling price", "asp", "harga jual rata-rata"],
    "strip_ratio": ["strip ratio", "stripping ratio", "nisbah kupas", "nisbah pengupasan"],
    "cash_cost": ["cash cost", "cash costs", "biaya tunai"],
    "revenue": ["revenue", "net sales", "total revenue", "pendapatan"],
    "net_profit": ["net profit", "net income", "profit for the period", "laba bersih",
                   "laba tahun berjalan", "laba periode berjalan"],
    "operating_cash_flow": ["operating cash flow", "cash flow from operations",
                            "cash generated from operations",
                            "cash inflow from operating activities",
                            "net cash generated from operating activities",
                            "arus kas dari aktivitas operasi", "aktivitas operasi",
                            "arus kas bersih diperoleh dari aktivitas operasi"],
    "total_assets": ["total assets", "total aset", "jumlah aset"],
    "total_liabilities": ["total liabilities", "total liabilitas", "jumlah liabilitas"],
    "total_equity": ["total equity", "total ekuitas", "jumlah ekuitas"],
    "cash_and_equivalents": ["cash and cash equivalents", "kas dan setara kas"],
    "short_term_deposits": ["short-term deposits", "short term deposits",
                            "deposito jangka pendek"],
    "royalties_to_government": ["royalties to the government", "royalty to the government",
                                "royalti kepada pemerintah", "royalti"],
    "treasury_share_purchase": ["treasury share purchase", "treasury shares purchase",
                                "pembelian saham treasuri", "saham treasuri"],
}

# Metrics that exist in the canonical artifact but not in metrics.yaml (which is
# shared with the statement pipeline and intentionally small). Kept here so the
# grammar file stays the single owner of the overlapping statement metrics.
EXTRA_METRICS: dict[str, dict[str, Any]] = {
    "cash_and_equivalents": {
        "label": "Cash and cash equivalents", "unit": "USD m",
        "unit_aliases": ["us$ million", "usd million", "$ million", "million", "juta"],
    },
    "short_term_deposits": {
        "label": "Short-term deposits", "unit": "USD m",
        "unit_aliases": ["us$ million", "usd million", "$ million", "million", "juta"],
    },
    "royalties_to_government": {
        "label": "Royalties to the government", "unit": "USD m",
        "unit_aliases": ["us$ million", "usd million", "$ million", "million", "juta"],
    },
    "treasury_share_purchase": {
        "label": "Treasury share purchase", "unit": "USD m",
        "unit_aliases": ["us$ million", "usd million", "$ million", "million", "juta"],
    },
}

# Unit family -> token regex. A value is only accepted when a number is glued to
# a unit of the metric's family (``5.3 Mt``, ``$1,910 million``, ``$81/ton``).
UNIT_TOKENS: dict[str, str] = {
    "mt": r"(?P<value>\d[\d.,]*)\s*(?P<unit>million\s+(?:metric\s+)?(?:tonnes|tons)|"
          r"mln\s+tons|juta\s+ton|mt)\b",
    "usd m": r"(?:us\$|usd|\$)\s*(?P<value>\d[\d.,]*)\s*(?P<unit>million|juta|m)\b",
    "usd/t": r"(?:us\$|usd|\$)\s*(?P<value>\d[\d.,]*)\s*(?:/|per)\s*(?P<unit>t|ton|tonne)\b",
    "x": r"(?P<value>\d[\d.,]*)\s*(?P<unit>x|times)\b",
}

# Statement overlap -> (statement name, line key, reconciliation key).
XBRL_OVERLAP: dict[str, tuple[str, str, str]] = {
    "revenue": ("income_statement", "revenue", "revenue"),
    "net_profit": ("income_statement", "net_profit", "net_profit"),
    "operating_cash_flow": ("cash_flow", "ocf", "operating_cf"),
    "total_equity": ("balance_sheet", "total_equity", "total_equity"),
    "total_assets": ("balance_sheet", "total_assets", "total_assets"),
    "total_liabilities": ("balance_sheet", "total_liabilities", "total_liabilities"),
}

# The five anchors the plan's exit test names (A8: "5/5 within 0.5%"). Other
# overlaps are still reconciled, but the `reconciliation` block mirrors the
# oracle and only carries these five.
RECONCILE_ANCHORS = ("revenue", "net_profit", "operating_cf", "total_equity", "total_assets")

# Extra causal markers beyond metrics.yaml. Both are management attribution
# language ("in line with", "offset by") and are used only by the offline
# deterministic driver fallback.
EXTRA_CAUSAL_MARKERS = ("in line with", "offset by")

AFFECTS_ORDER = (
    "cost_of_revenue", "selling_expenses", "gna_expenses", "operating_expenses",
    "royalties", "revenue", "net_profit", "total_equity", "total_assets",
    "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "other",
)

AFFECTS_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("strip ratio", "fuel", "mining and transportation", "cost of revenue", "beban pokok"), "cost_of_revenue"),
    (("selling expense", "beban penjualan"), "selling_expenses"),
    (("general and administrative", "beban umum"), "gna_expenses"),
    (("tax refund", "operating activit", "supplier", "aktivitas operasi"), "operating_cash_flow"),
    (("dividend", "financing activit", "treasury share", "bank loan", "lease", "pendanaan"), "financing_cash_flow"),
    (("deposit", "investing activit", "investasi"), "investing_cash_flow"),
    (("royalt",), "royalties"),
    (("sales volume", "volume penjualan"), "selling_expenses"),
    (("asp", "selling price", "benchmark price", "harga jual"), "revenue"),
    (("retained earnings", "laba ditahan"), "total_equity"),
)

# Scope grammar. Order does not matter; the candidate nearest the value wins.
SCOPE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b6M\s*\d{2}\b|\b(?:first|1st)\s+half\b|\bsix[\s-]month\b|"
                r"\b6\s*months?\b|\bh1\b|\bsemester\s+(?:i|pertama)\b", re.I), "6M"),
    (re.compile(r"\b9M\s*\d{2}\b|\bnine\s+months?\b|\b9\s*months?\b", re.I), "9M"),
    (re.compile(r"\bFY\s*\d{2,4}\b|\bfull[\s-]year\b|\btahun\s+penuh\b", re.I), "FY"),
    (re.compile(r"\bQ1\b|\b(?:first|1st)\s+quarter\b|\bkuartal\s+(?:pertama|i)\b", re.I), "Q1"),
    (re.compile(r"\bQ2\b|\b(?:second|2nd)\s+quarter\b|\bkuartal\s+(?:kedua|ii)\b", re.I), "Q2"),
    (re.compile(r"\bQ3\b|\b(?:third|3rd)\s+quarter\b|\bkuartal\s+(?:ketiga|iii)\b", re.I), "Q3"),
    (re.compile(r"\bQ4\b|\b(?:fourth|4th)\s+quarter\b|\bkuartal\s+(?:keempat|iv)\b", re.I), "Q4"),
)

EN_WORDS = frozenset("""
the of and in to with from for was were is are at on by as that this which increased
decreased company million during period its has have had rose fell reached compared
""".split())
ID_WORDS = frozenset("""
dan yang pada untuk dari dengan sebesar juta persen karena menjadi periode perusahaan
meningkat menurun ini itu akan telah dalam terhadap kepada oleh juga sebagai seiring
dikarenakan disebabkan mencapai tercatat lebih rendah lebih tinggi
""".split())

DRIVER_AFFECTS_ALLOWED = set(AFFECTS_ORDER)


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #

def _normalize(text: str) -> str:
    """NFKC + typographic quotes/dashes + collapsed whitespace."""
    text = unicodedata.normalize("NFKC", text or "")
    text = (text.replace("\u2018", "'").replace("\u2019", "'")
            .replace("\u201c", '"').replace("\u201d", '"')
            .replace("\u2013", "-").replace("\u2014", "-").replace("\u00a0", " "))
    return re.sub(r"\s+", " ", text).strip()


def parse_number(raw: str) -> Optional[float]:
    """Parse a number token, handling Indonesian decimal commas and thousands dots.

    - ``5,3`` → 5.3 (decimal comma, one or two trailing digits)
    - ``2,417`` / ``1.000`` → 2417 / 1000 (thousands separator, three digits)
    - ``1.000,5`` / ``1,000.5`` → 1000.5 (the last separator is the decimal)
    """
    text = (raw or "").strip().replace(" ", "")
    if not text:
        return None
    try:
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            head, _, tail = text.rpartition(",")
            if len(tail) == 3 and head and head != "0":
                text = text.replace(",", "")
            else:
                text = text.replace(",", ".")
        elif "." in text:
            head, _, tail = text.rpartition(".")
            if len(tail) == 3 and head and head != "0" and text.count(".") == 1:
                text = text.replace(".", "")
        return float(text)
    except ValueError:
        return None


def language_score(text: str) -> int:
    """Positive → English, negative → Indonesian, 0 → unknown/bilingual table."""
    words = re.findall(r"[a-zA-Z]+", (text or "").lower())
    if not words:
        return 0
    en = sum(1 for word in words if word in EN_WORDS)
    id_ = sum(1 for word in words if word in ID_WORDS)
    return en - id_


def page_language(text: str) -> str:
    score = language_score(text)
    if score > 0:
        return "en"
    if score < 0:
        return "id"
    return "unknown"


def split_sentences(text: str) -> list[str]:
    """Whitespace-normalized, sentence-split text. Never splits ``5.3 Mt``."""
    flat = _normalize(text)
    if not flat:
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9(\"'\u2022])", flat)
    return [part.strip(" \u2022") for part in parts if part.strip(" \u2022")]


def _looks_like_heading(line: str, next_line: str) -> bool:
    """Short, digit-less, unterminated line followed by a capitalised line.

    Section headings ("Total Liabilities and Equity") otherwise glue onto the
    first sentence of the section and make keyword→number linking ambiguous.
    """
    if not line or not next_line:
        return False
    if len(line.split()) > 10:
        return False
    if re.search(r"[.!?:;,]$", line):
        return False
    if re.search(r"\d", line):
        return False
    if not line[:1].isupper() or not next_line[:1].isupper():
        return False
    return True


def drop_headings(text: str) -> str:
    lines = text.split("\n")
    kept: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        next_line = next((candidate.strip() for candidate in lines[index + 1:] if candidate.strip()), "")
        if _looks_like_heading(stripped, next_line):
            kept.append("")
            continue
        kept.append(line)
    return "\n".join(kept)


def parse_pages_text(text: str) -> list[str]:
    return ingest.parse_pages_text(text)


# --------------------------------------------------------------------------- #
# scope / language
# --------------------------------------------------------------------------- #

def default_scope(published: str | None, fallback: str = "6M") -> str:
    """Reporting scope implied by a release date (IDX quarterly cadence)."""
    try:
        month = int(str(published)[5:7])
    except (TypeError, ValueError):
        return fallback
    if month in (1, 2, 3):
        return "FY"
    if month in (4, 5):
        return "Q1"
    if month in (6, 7, 8):
        return "6M"
    if month in (9, 10, 11):
        return "9M"
    return "FY"


def document_scope(pages: Sequence[tuple[int, str]], fallback: str) -> str:
    """Scope stated by the document itself, not just its publication month.

    A 1H release published in September would otherwise be labelled 9M by
    ``default_scope``. The title/body is authoritative: count explicit scope
    phrases and pick the most frequent; ties keep the 6M-first order.
    """
    counts: dict[str, int] = {}
    for _page, text in pages:
        for pattern, scope in SCOPE_PATTERNS:
            count = len(pattern.findall(text))
            if count:
                counts[scope] = counts.get(scope, 0) + count
    if not counts:
        return fallback
    order = {scope: index for index, (_p, scope) in enumerate(SCOPE_PATTERNS)}
    return sorted(
        counts, key=lambda scope: (-counts[scope], order.get(scope, 99))
    )[0]


def detect_scope(sentence: str, default: str, anchor: int | None = None) -> tuple[str, bool]:
    """Scope nearest to ``anchor`` in the sentence; (scope, explicit?)."""
    candidates: list[tuple[int, str]] = []
    for pattern, scope in SCOPE_PATTERNS:
        for match in pattern.finditer(sentence):
            candidates.append((match.start(), scope))
    if not candidates:
        return default, False
    if anchor is None:
        return candidates[0][1], True
    position, scope = min(candidates, key=lambda item: abs(item[0] - anchor))
    return scope, True


def select_pages(pages: Sequence[str]) -> tuple[list[tuple[int, str]], str, dict[str, list[int]]]:
    """Keep EN pages when they exist; otherwise ID pages. Page numbers preserved."""
    languages = [page_language(page) for page in pages]
    en_pages = [i + 1 for i, lang in enumerate(languages) if lang == "en"]
    id_pages = [i + 1 for i, lang in enumerate(languages) if lang == "id"]
    if en_pages:
        selected = en_pages
        language = "en+id" if id_pages else "en"
    elif id_pages:
        selected = id_pages
        language = "id"
    else:
        selected = list(range(1, len(pages) + 1))
        language = "unknown"
    return [(number, pages[number - 1]) for number in selected], language, {
        "en": en_pages, "id": id_pages,
    }


# --------------------------------------------------------------------------- #
# metric grammar
# --------------------------------------------------------------------------- #

@dataclass
class MetricSpec:
    key: str
    label: str
    unit: str
    unit_aliases: list[str]
    patterns: list[re.Pattern[str]] = field(default_factory=list)
    prior_patterns: list[re.Pattern[str]] = field(default_factory=list)
    unit_family: str = "usd m"
    xbrl_overlap: Optional[str] = None
    keywords: list[str] = field(default_factory=list)
    keyword_re: re.Pattern[str] = field(default_factory=lambda: re.compile(r"(?!)"))
    token_re: re.Pattern[str] = field(default_factory=lambda: re.compile(r"(?!)"))


@dataclass
class Candidate:
    metric: str
    value: float
    unit: str
    prior: Optional[float]
    page: int
    sentence: str
    scope: str
    method: str            # "pattern" | "proximity"
    distance: int
    language: str
    raw: str
    explicit_scope: bool = False


def load_metrics(path: Path = METRICS_PATH) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data


def build_specs(metrics: dict[str, Any]) -> dict[str, MetricSpec]:
    specs: dict[str, MetricSpec] = {}
    for key, spec in (metrics.get("metrics") or {}).items():
        unit = str(spec.get("unit") or "USD m")
        family = _unit_family(unit)
        aliases = [str(alias) for alias in spec.get("unit_aliases") or []]
        specs[key] = MetricSpec(
            key=key,
            label=str(spec.get("label") or key),
            unit=unit,
            unit_aliases=aliases or [unit.lower()],
            patterns=[re.compile(p, re.I) for p in spec.get("patterns") or []],
            prior_patterns=[re.compile(p, re.I) for p in spec.get("prior_patterns") or []],
            unit_family=family,
            xbrl_overlap=spec.get("xbrl_overlap"),
            keywords=list(KEYWORDS.get(key) or [key.replace("_", " ")]),
        )
    for key, spec in EXTRA_METRICS.items():
        aliases = [str(alias) for alias in spec.get("unit_aliases") or []]
        specs[key] = MetricSpec(
            key=key,
            label=str(spec.get("label") or key),
            unit=str(spec.get("unit") or "USD m"),
            unit_aliases=aliases or [str(spec.get("unit") or "usd m").lower()],
            unit_family=_unit_family(str(spec.get("unit") or "USD m")),
            keywords=list(KEYWORDS.get(key) or [key.replace("_", " ")]),
        )
    for spec in specs.values():
        alternatives = sorted({keyword.lower() for keyword in spec.keywords}, key=len, reverse=True)
        spec.keyword_re = re.compile(
            "|".join(r"\b" + re.escape(keyword) + r"\b" for keyword in alternatives), re.I
        )
        spec.token_re = re.compile(UNIT_TOKENS[spec.unit_family], re.I)
    return specs


def _unit_family(unit: str) -> str:
    lowered = (unit or "").strip().lower()
    if lowered in ("mt", "million tonnes", "million tons"):
        return "mt"
    if lowered in ("usd/t", "us$/t", "usd/ton"):
        return "usd/t"
    if lowered == "x":
        return "x"
    return "usd m"


def _token_matches(sentence: str, spec: MetricSpec) -> list[dict[str, Any]]:
    tokens: list[dict[str, Any]] = []
    for match in spec.token_re.finditer(sentence):
        value = parse_number(match.group("value"))
        if value is None:
            continue
        tokens.append({
            "value": value,
            "raw": match.group("value"),
            "unit_raw": match.groupdict().get("unit") or "",
            "start": match.start(),
            "end": match.end(),
        })
    return tokens


def _keyword_spans(sentence: str, spec: MetricSpec) -> list[tuple[int, int]]:
    return [(match.start(), match.end()) for match in spec.keyword_re.finditer(sentence)]


def _paired_tokens(sentence: str, tokens: list[dict[str, Any]]) -> Optional[tuple[dict[str, Any], dict[str, Any]]]:
    """``from X to Y`` / ``to Y from X`` → (value_token, prior_token)."""
    for i, first in enumerate(tokens):
        for second in tokens[i + 1:]:
            between = sentence[first["end"]:second["start"]]
            before = sentence[max(0, first["start"] - 48):first["start"]]
            if re.search(r"\bto\b|\bmenjadi\b", between, re.I) and \
                    re.search(r"\bfrom\b|\bdari\b", before, re.I):
                return second, first
            if re.search(r"\bfrom\b|\bdari\b", between, re.I) and \
                    re.search(r"\bto\b|\bmenjadi\b", before, re.I):
                return first, second
    return None


def _prior_by_marker(sentence: str, tokens: list[dict[str, Any]],
                     exclude: dict[str, Any] | None = None,
                     max_distance: int = 90) -> Optional[dict[str, Any]]:
    """A comparison value introduced by than/vs/compared to/from/dari.

    Only tokens near the value token qualify; a stray ``dari $1.478 juta`` 120
    characters earlier must not become the prior of a treasury share purchase.
    """
    marker = re.compile(
        r"\b(?:than|vs\.?|versus|compared\s+(?:to|with)|dibandingkan(?:\s+dengan)?|"
        r"from|dari|sebelumnya)\s*$", re.I,
    )
    candidates: list[dict[str, Any]] = []
    comparative = re.compile(
        r"\b(?:to|menjadi|naik|turun|meningkat|menurun|mencapai|reached|rose|increased|"
        r"decreased|rendah|tinggi|lower|higher|more|less)\b", re.I,
    )
    value_context = sentence[max(0, exclude["start"] - 40):exclude["start"]] if exclude else ""
    for token in tokens:
        if exclude is not None and token is exclude:
            continue
        prefix = sentence[max(0, token["start"] - 40):token["start"]]
        match = marker.search(prefix.rstrip())
        if not match:
            continue
        marker_word = match.group(0).strip().lower()
        if marker_word in ("from", "dari", "sebelumnya"):
            # "from X" is only a prior when the value itself sits in a
            # comparison ("... to $27m from ...", "... mencapai 9,9 ...
            # rendah dari 10,4 ..."). Otherwise it belongs to a neighbouring
            # clause (retained earnings "from $1,478m" is not the prior of a
            # treasury share purchase).
            gap_start = min(token["end"], exclude["start"]) if exclude else token["end"]
            gap_end = max(token["start"], exclude["end"]) if exclude else token["start"]
            gap = sentence[gap_start:gap_end]
            if not (comparative.search(value_context) or comparative.search(gap)):
                continue
        candidates.append(token)
    if not candidates:
        return None
    if exclude is None:
        return candidates[0]
    nearest = min(candidates, key=lambda token: abs(token["start"] - exclude["start"]))
    if abs(nearest["start"] - exclude["start"]) > max_distance:
        return None
    return nearest


def _nearest_after(sentence: str, tokens: list[dict[str, Any]],
                   spans: list[tuple[int, int]], exclude: dict[str, Any] | None = None,
                   window: int = 90) -> Optional[tuple[dict[str, Any], int]]:
    for start, end in reversed(spans):
        after = [token for token in tokens if token["start"] >= end
                 and (exclude is None or token is not exclude)]
        if after:
            token = min(after, key=lambda item: item["start"])
            distance = token["start"] - end
            if distance <= window:
                return token, distance
    return None


def _nearest_before(sentence: str, tokens: list[dict[str, Any]],
                    spans: list[tuple[int, int]], exclude: dict[str, Any] | None = None,
                    window: int = 30) -> Optional[tuple[dict[str, Any], int]]:
    for start, end in reversed(spans):
        before = [token for token in tokens if token["end"] <= start
                  and (exclude is None or token is not exclude)]
        if before:
            token = max(before, key=lambda item: item["end"])
            distance = start - token["end"]
            if distance <= window:
                return token, distance
    return None


def _unit_matches(raw_unit: str, spec: MetricSpec) -> bool:
    if not raw_unit:
        return True
    alias = raw_unit.strip().lower()
    return alias in {a.lower() for a in spec.unit_aliases} or spec.unit_family in ("mt", "x") \
        or alias in ("million", "juta", "m", "t", "ton", "tonne")


def _explicit_matches(sentence: str, spec: MetricSpec) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for pattern in spec.patterns:
        for match in pattern.finditer(sentence):
            groups = match.groupdict()
            value = parse_number(groups.get("value") or "")
            if value is None:
                continue
            if not _unit_matches(groups.get("unit") or "", spec):
                continue
            prior = None
            for prior_pattern in spec.prior_patterns + spec.patterns:
                for prior_match in prior_pattern.finditer(sentence):
                    prior_raw = prior_match.groupdict().get("prior")
                    if prior_raw:
                        parsed = parse_number(prior_raw)
                        if parsed is not None and parsed != value:
                            prior = parsed
                            break
                if prior is not None:
                    break
            matches.append({
                "value": value,
                "raw": groups.get("value") or "",
                "prior": prior,
                "start": match.start("value"),
            })
    return matches


def _near_keyword(token: dict[str, Any], spans: list[tuple[int, int]],
                  window: int = 90) -> bool:
    """True when the token is the kind of number this keyword governs."""
    for start, end in spans:
        if token["start"] >= end and token["start"] - end <= window:
            return True
        if token["end"] <= start and start - token["end"] <= 30:
            return True
    return False


def _scope_label(scope: str, year: str) -> str:
    return f"{scope}-{year}" if year else scope


def sentence_candidate(sentence: str, spec: MetricSpec, page: int, default: str,
                       language: str, year: str = "") -> Optional[Candidate]:
    """Best deterministic candidate for one metric in one sentence, or None."""
    spans = _keyword_spans(sentence, spec)
    if not spans:
        return None
    tokens = _token_matches(sentence, spec)
    if not tokens:
        return None

    # 1. Explicit grammar from metrics.yaml (highest confidence).
    explicit = _explicit_matches(sentence, spec)
    if explicit:
        best = min(explicit, key=lambda item: abs(item["start"] - spans[-1][0]))
        value_tok = min(tokens, key=lambda token: abs(token["start"] - best["start"]))
        prior = best.get("prior")
        if prior is None:
            prior_tok = _prior_by_marker(sentence, tokens, exclude=value_tok)
            prior = prior_tok["value"] if prior_tok else None
        scope, explicit_scope = detect_scope(sentence, default, best["start"])
        return Candidate(
            metric=spec.key, value=best["value"], unit=spec.unit, prior=prior, page=page,
            sentence=sentence, scope=_scope_label(scope, year), method="pattern",
            distance=abs(best["start"] - spans[-1][0]), language=language,
            raw=best["raw"], explicit_scope=explicit_scope,
        )

    # 2. Structural value/prior, then keyword proximity. A from/to pair only
    # counts when one of its tokens is actually governed by the keyword (so
    # "$1,910m from $1,908m" in an equity sentence does not become the treasury
    # share purchase).
    paired = _paired_tokens(sentence, tokens)
    prior_tok: Optional[dict[str, Any]] = None
    if paired is not None and _near_keyword(paired[0], spans):
        value_tok, prior_tok = paired
        distance = 0
    else:
        located = _nearest_after(sentence, tokens, spans)
        if located is not None:
            value_tok, distance = located
            prior_tok = _prior_by_marker(sentence, tokens, exclude=value_tok)
        else:
            located = _nearest_before(sentence, tokens, spans)
            if located is None:
                return None
            value_tok, distance = located
            prior_tok = _prior_by_marker(sentence, tokens, exclude=value_tok)

    scope, explicit_scope = detect_scope(sentence, default, value_tok["start"])
    return Candidate(
        metric=spec.key, value=value_tok["value"], unit=spec.unit,
        prior=prior_tok["value"] if prior_tok is not None else None,
        page=page, sentence=sentence, scope=_scope_label(scope, year), method="proximity",
        distance=distance, language=language, raw=value_tok["raw"],
        explicit_scope=explicit_scope,
    )


def extract_metrics(pages: Sequence[tuple[int, str]], specs: dict[str, MetricSpec],
                    default: str, language: str, year: str = "") -> list[Candidate]:
    """Scan selected pages; keep one candidate per (metric, scope)."""
    best: dict[tuple[str, str], Candidate] = {}
    for page_number, text in pages:
        for sentence in split_sentences(drop_headings(text)):
            for spec in specs.values():
                candidate = sentence_candidate(sentence, spec, page_number, default,
                                               language, year)
                if candidate is None:
                    continue
                key = (candidate.metric, candidate.scope)
                if key not in best or _rank(candidate) < _rank(best[key]):
                    best[key] = candidate
    return list(best.values())


def _rank(candidate: Candidate) -> tuple:
    return (0 if candidate.method == "pattern" else 1,
            0 if candidate.language == "en" else 1,
            candidate.distance,
            candidate.page)


# --------------------------------------------------------------------------- #
# drivers
# --------------------------------------------------------------------------- #

def infer_affects(text: str) -> str:
    lowered = text.lower()
    for needles, affects in AFFECTS_RULES:
        if any(needle in lowered for needle in needles):
            return affects
    return "other"


def causal_markers(metrics: dict[str, Any]) -> list[str]:
    markers: list[str] = []
    for language_markers in (metrics.get("causal_markers") or {}).values():
        markers.extend(str(marker).lower() for marker in language_markers)
    markers.extend(EXTRA_CAUSAL_MARKERS)
    return sorted(set(markers), key=len, reverse=True)


def _driver_label(sentence: str, marker: str) -> str:
    lowered = sentence.lower()
    position = lowered.find(marker)
    if position < 0:
        return sentence[:90]
    tail = sentence[position + len(marker):]
    tail = re.split(r",|;|\band\b|\bwhich\b|\bsehingga\b", tail, maxsplit=1)[0]
    label = tail.strip(" .:-")
    return (label or sentence)[:120]


BOILERPLATE_MARKERS = (
    "disclaimer", "forward-looking", "forward looking", "does not undertake",
    "no event shall", "sangkalan", "berwawasan ke depan", "tidak bertanggung jawab",
    "does not constitute an offer", "not independently verified",
)


def deterministic_drivers(pages: Sequence[tuple[int, str]], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Verbatim causal sentences with a page number; no LLM, no numbers as data."""
    markers = causal_markers(metrics)
    drivers: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page_number, text in pages:
        for sentence in split_sentences(drop_headings(text)):
            lowered = sentence.lower()
            if any(marker in lowered for marker in BOILERPLATE_MARKERS):
                continue
            marker = next((m for m in markers if re.search(r"\b" + re.escape(m) + r"\b", lowered)), None)
            if marker is None:
                continue
            affects = infer_affects(sentence)
            if affects == "other":
                # "attributable to the owners of the parent" etc. is not a driver.
                continue
            key = _normalize(sentence).lower()
            if key in seen:
                continue
            seen.add(key)
            quote = _clause_around(sentence, marker)
            drivers.append({
                "driver": _driver_label(sentence, marker),
                "affects": affects,
                "quote": quote,
                "page": page_number,
                "type": "management_stated",
                "source": "deterministic",
            })
    return drivers


def _clause_around(sentence: str, marker: str) -> str:
    """Verbatim clause from sentence start through the marker's complement."""
    lowered = sentence.lower()
    position = lowered.find(marker)
    if position < 0:
        return sentence
    after = position + len(marker)
    boundary = re.search(r",|;", sentence[after:])
    if boundary and boundary.start() > 12:
        return sentence[:after + boundary.start()].strip()
    return sentence.strip()


DRIVER_SYSTEM = """You extract management-stated causal drivers from an Indonesian \
listed company's results release. Rules:
1. A driver is a causal statement the document itself makes (cost up because strip \
ratio rose, cash flow down due to lower tax refunds).
2. `quote` MUST be copied verbatim from the supplied page text: no paraphrase, no \
added words, no ellipsis, no translation.
3. `page` MUST be the page number the quote appears on.
4. `affects` MUST be one of: {affects}.
5. `driver` is a short label (max 12 words) for the driver.
6. Never output a number as a standalone data field. Numbers may appear inside \
quotes only.
Return JSON only: {{"drivers": [{{"quote": "...", "page": 1, \
"affects": "cost_of_revenue", "driver": "higher strip ratio"}}]}}"""

DRIVER_USER = """Ticker: {ticker}
Document: {doc_id} ({published})

Page text follows. Extract at most {limit} drivers.

{pages}"""


def _llm_call(model: str, system: str, user: str) -> str:
    """One chat() call via the pipeline client. Raises on failure (caller catches)."""
    if str(ANALYSIS_PIPELINE_DIR) not in sys.path:
        sys.path.insert(0, str(ANALYSIS_PIPELINE_DIR))
    import opencode_client  # noqa: PLC0415 - optional heavyweight path

    result = opencode_client.chat(
        model=model, system=system, user=user, temperature=0.0,
        max_tokens=2500, reasoning_effort="low", timeout_sec=60,
    )
    return result.get("content") or ""


_DRIVER_OBJECT_RE = re.compile(
    r'\{\s*"quote"\s*:\s*"(?P<quote>.*?)"\s*,\s*"page"\s*:\s*(?P<page>\d+)\s*,\s*'
    r'"affects"\s*:\s*"(?P<affects>[^"]*)"\s*,\s*"driver"\s*:\s*"(?P<driver>.*?)"\s*\}',
    re.DOTALL,
)


def _unescape_json_string(raw: str) -> str:
    try:
        return json.loads(f'"{raw}"', strict=False)
    except json.JSONDecodeError:
        return raw.replace('\\"', '"').replace("\\n", " ")


def _parse_drivers_payload(content: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Tolerant JSON reader: models occasionally return a raw quote unescaped."""
    warnings: list[str] = []
    text = (content or "").strip()
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        blob = text[start:end + 1]
        try:
            payload = json.loads(blob, strict=False)
            drivers = payload.get("drivers") if isinstance(payload, dict) else payload
            if isinstance(drivers, list):
                return [item for item in drivers if isinstance(item, dict)], warnings
        except json.JSONDecodeError:
            pass
    recovered: list[dict[str, Any]] = []
    for match in _DRIVER_OBJECT_RE.finditer(text):
        recovered.append({
            "quote": _unescape_json_string(match.group("quote")),
            "page": int(match.group("page")),
            "affects": match.group("affects"),
            "driver": _unescape_json_string(match.group("driver")),
        })
    if recovered:
        warnings.append(
            f"llm driver JSON was malformed; recovered {len(recovered)} object(s) by pattern"
        )
        return recovered, warnings
    raise ValueError("no parsable driver objects in response")


def llm_drivers(selected: Sequence[tuple[int, str]], *, ticker: str, doc: dict[str, Any],
                model: str, limit: int = 8) -> tuple[list[dict[str, Any]], list[str]]:
    """One call per document. Quotes are verified; anything unverifiable is dropped."""
    pages_text = "\n\n".join(
        f"=== PAGE {number} ===\n{_normalize(text)[:4000]}" for number, text in selected
    )[:24000]
    system = DRIVER_SYSTEM.format(affects=", ".join(AFFECTS_ORDER))
    user = DRIVER_USER.format(ticker=ticker, doc_id=doc.get("id"), published=doc.get("published"),
                              limit=limit, pages=pages_text)
    raw = _llm_call(model, system, user)
    warnings: list[str] = []
    page_text = {number: _normalize(text) for number, text in selected}
    drivers: list[dict[str, Any]] = []
    parsed, parse_warnings = _parse_drivers_payload(raw)
    warnings.extend(parse_warnings)
    for item in parsed:
        quote = item.get("quote")
        page = item.get("page")
        if not isinstance(quote, str) or not quote.strip():
            warnings.append("llm driver dropped: empty quote")
            continue
        try:
            page_number = int(page)
        except (TypeError, ValueError):
            warnings.append(f"llm driver dropped: non-numeric page for {quote[:40]!r}")
            continue
        if page_number not in page_text:
            warnings.append(f"llm driver dropped: page {page_number} not in document")
            continue
        normalized_quote = _normalize(quote)
        if normalized_quote not in page_text[page_number]:
            warnings.append(f"llm driver dropped: quote not verbatim on page {page_number}: {quote[:60]!r}")
            continue
        affects = str(item.get("affects") or "").strip()
        if affects not in DRIVER_AFFECTS_ALLOWED:
            inferred = infer_affects(normalized_quote)
            warnings.append(f"llm driver affects {affects!r} not allowed; used {inferred}")
            affects = inferred
        drivers.append({
            "driver": str(item.get("driver") or _driver_label(normalized_quote, "due to"))[:120],
            "affects": affects,
            "quote": quote.strip(),
            "page": page_number,
            "type": "management_stated",
            "source": "llm",
        })
    return drivers, warnings


# --------------------------------------------------------------------------- #
# reconciliation
# --------------------------------------------------------------------------- #

def load_xbrl_seed(ticker: str, period: str, year: int) -> Any:
    """Read-only FactSheet from xlsx_statements. Returns None when unavailable."""
    if period not in ("Q1", "Q2", "Q3", "Q4"):
        return None
    if str(ANALYSIS_PIPELINE_DIR) not in sys.path:
        sys.path.insert(0, str(ANALYSIS_PIPELINE_DIR))
    import xlsx_seed  # noqa: PLC0415 - duckdb + schema import path

    return xlsx_seed.seed_from_xlsx(ticker, int(year), period)


def _release_scale(unit: str) -> float:
    lowered = (unit or "").strip().lower()
    if lowered in ("usd m", "us$ m", "usd million", "us$ million", "million"):
        return 1_000_000.0
    return 1.0


def reconcile(candidate: Candidate, seed: Any) -> dict[str, Any]:
    """Return ``{ok, method, reconciled_to, release, xbrl, tolerance, reason}``."""
    overlap = XBRL_OVERLAP.get(candidate.metric)
    if overlap is None or seed is None:
        return {"ok": True, "method": "quoted_in_source", "reconciled_to": None,
                "release": candidate.value, "xbrl": None, "tolerance": None, "reason": None}
    statement_name, line_key, recon_key = overlap
    statement = getattr(seed, statement_name, None)
    xbrl_value = (statement.lines or {}).get(line_key) if statement is not None else None
    if xbrl_value is None:
        return {"ok": False, "method": "reconciled", "reconciled_to": None,
                "release": candidate.value, "xbrl": None, "tolerance": None,
                "reason": f"XBRL has no {statement_name}.{line_key}", "recon_key": recon_key}
    meta_units = getattr(getattr(seed, "meta", None), "units", None)
    xbrl_scale = paths.scale_multiplier(meta_units)
    release_scale = _release_scale(candidate.unit)
    xbrl_full = float(xbrl_value) * xbrl_scale
    release_full = candidate.value * release_scale
    # max(0.5% of the XBRL value, one rounding unit): the release rounds to its
    # display unit (USD m), the XBRL sheet rounds to `meta.units` (e.g. thousand).
    tolerance = max(0.005 * abs(xbrl_full), 1.0 * release_scale, 1.0 * xbrl_scale)
    provenance = (statement.provenance or {}).get(line_key) or {}
    if hasattr(provenance, "model_dump"):  # pydantic Provenance from schema.py
        provenance = provenance.model_dump()
    elif not isinstance(provenance, dict):
        provenance = dict(provenance)
    sheet, row = provenance.get("sheet"), provenance.get("row")
    reconciled_to = f"{statement_name}.{line_key} ({sheet}:{row})" if sheet is not None else \
        f"{statement_name}.{line_key}"
    ok = abs(release_full - xbrl_full) <= tolerance
    return {
        "ok": ok,
        "method": "reconciled" if ok else "rejected",
        "reconciled_to": reconciled_to if ok else None,
        "release": candidate.value,
        "xbrl": round(xbrl_full / release_scale, 4),
        "tolerance": round(tolerance / release_scale, 4),
        "reason": None if ok else (
            f"release {candidate.value:g} vs XBRL {xbrl_full / release_scale:.3f} "
            f"exceeds tolerance {tolerance / release_scale:.3f}"
        ),
        "recon_key": recon_key,
        "statement": statement_name,
        "line_key": line_key,
    }


def verify_quoted(candidate: Candidate, spec: MetricSpec) -> tuple[bool, str]:
    """Non-overlap metrics: value token + unit must be literally in the sentence."""
    if candidate.raw and candidate.raw not in candidate.sentence:
        return False, "value token not verbatim in sentence"
    lowered = candidate.sentence.lower()
    aliases = [alias.lower() for alias in spec.unit_aliases] + [spec.unit.lower()]
    if spec.unit_family == "usd m":
        aliases += ["million", "juta"]
    if spec.unit_family == "usd/t":
        aliases += ["/ton", "per ton", "us$"]
    if spec.unit_family == "mt":
        aliases += ["mt", "juta ton", "million"]
    if not any(alias and alias in lowered for alias in aliases):
        return False, "unit not present in sentence"
    if not candidate.explicit_scope:
        # A defaulted scope is acceptable only when the rate/volume is a period
        # figure the document itself labels; keep it, but make it explicit.
        return True, f"scope {candidate.scope} from document default"
    return True, "value, unit and scope quoted verbatim"


# --------------------------------------------------------------------------- #
# document loading
# --------------------------------------------------------------------------- #

def load_document_pages(doc: dict[str, Any], operational_dir: Path | None = None) -> list[str]:
    """Prefer the captured ``.txt`` (page markers), fall back to the PDF."""
    roots = [Path(operational_dir) if operational_dir else OPERATIONAL_DIR, PROJECT_ROOT]
    candidates: list[Path] = []
    for key in ("text", "path"):
        raw = doc.get(key)
        if not raw:
            continue
        path = Path(raw)
        if path.is_absolute():
            candidates.append(path)
        else:
            candidates.extend(root / path for root in roots)
    # Conventional location next to the manifest.
    directory = Path(operational_dir) if operational_dir else OPERATIONAL_DIR
    doc_id = str(doc.get("id") or "")
    if doc_id:
        candidates.append(directory / str(doc.get("ticker") or "").upper() / f"{doc_id}.txt")
        candidates.append(directory / str(doc.get("ticker") or "").upper() / f"{doc_id}.pdf")
    ordered: list[Path] = []
    for path in candidates:
        ordered.append(path.with_suffix(".txt") if path.suffix == ".pdf" else path)
    ordered.extend(path for path in candidates if path.suffix == ".pdf")
    for path in ordered:
        if not path.exists():
            continue
        if path.suffix == ".txt":
            pages = parse_pages_text(path.read_text(encoding="utf-8"))
            if pages:
                return pages
        elif path.suffix == ".pdf":
            try:
                return ingest.pdf_page_texts(path.read_bytes())
            except ValueError:
                continue
    return []


def select_documents(ticker: str, year: int, doc_ids: Sequence[str] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    """Captured docs for the period window; newest vintage per type."""
    manifest = ingest.load_manifest(ticker)
    warnings: list[str] = []
    captured = [doc for doc in manifest["documents"] if doc.get("status") == "captured"]
    if doc_ids:
        chosen = [doc for doc in captured if doc.get("id") in set(doc_ids)]
        missing = sorted(set(doc_ids) - {doc.get("id") for doc in chosen})
        for doc_id in missing:
            warnings.append(f"document {doc_id} is not captured in the manifest")
        return chosen, warnings
    window = [doc for doc in captured
              if str(doc.get("published") or "")[:4] in (str(year), str(year + 1))]
    pool = window or captured
    if not window:
        warnings.append(f"no captured document published in {year}; using every captured document")
    by_type: dict[str, dict[str, Any]] = {}
    for doc in sorted(pool, key=lambda item: (item.get("published") or "", item.get("fetched_at") or "")):
        by_type[doc.get("type") or "press_release"] = doc
    # Primary document for the artifact: the press release first, then the rest.
    type_priority = {"press_release": 0, "presentation": 1, "filing": 2}
    return sorted(by_type.values(),
                  key=lambda doc: (type_priority.get(doc.get("type"), 9),
                                   doc.get("published") or "")), warnings


# --------------------------------------------------------------------------- #
# artifact assembly
# --------------------------------------------------------------------------- #

@dataclass
class Entry:
    candidate: Candidate
    spec: MetricSpec
    doc: dict[str, Any]
    status: str
    verification: dict[str, Any]
    reason: Optional[str] = None

    def as_flat(self) -> dict[str, Any]:
        payload = {
            "metric": self.candidate.metric,
            "scope": self.candidate.scope,
            "value": _round_value(self.candidate.value),
            "unit": self.candidate.unit,
            "prior": _round_value(self.candidate.prior) if self.candidate.prior is not None else None,
            "page": self.candidate.page,
            "source_doc": self.doc.get("id"),
            "published_at": self.doc.get("published"),
            "status": self.status,
            "verification": dict(self.verification),
            "quote": self.candidate.sentence,
        }
        if self.reason:
            payload["reason"] = self.reason
        return payload

    def as_listed(self) -> dict[str, Any]:
        """The oracle's compact shape (no verification fields)."""
        payload: dict[str, Any] = {
            "metric": self.candidate.metric,
            "value": _round_value(self.candidate.value),
            "unit": self.candidate.unit,
        }
        if CATEGORY.get(self.candidate.metric) in ("production", "sales", "price"):
            payload["scope"] = self.candidate.scope
        if self.candidate.prior is not None and CATEGORY.get(self.candidate.metric) in (
            "production", "sales", "price",
        ):
            payload["prior"] = _round_value(self.candidate.prior)
        payload["page"] = self.candidate.page
        return {key: payload[key] for key in ("metric", "scope", "value", "unit", "prior", "page")
                if key in payload}


def _round_value(value: float | None) -> Optional[float]:
    if value is None:
        return None
    rounded = round(float(value), 4)
    return float(f"{rounded:g}") if rounded is not None else None


def _scope_rank(scope: str) -> int:
    return SCOPE_ORDER.get(str(scope).split("-")[0].upper(), 9)


def _entry_order(entry: Entry) -> tuple:
    return (METRIC_ORDER.get(entry.candidate.metric, 99),
            _scope_rank(entry.candidate.scope), entry.candidate.page)


def _derived(entries: list[Entry]) -> dict[str, float]:
    by_key = {(entry.candidate.metric, entry.candidate.scope): entry.candidate
              for entry in entries if entry.status != "rejected"}

    def value(metric: str, scope: str) -> Optional[float]:
        candidate = by_key.get((metric, scope))
        return candidate.value if candidate else None

    def prior(metric: str, scope: str) -> Optional[float]:
        candidate = by_key.get((metric, scope))
        return candidate.prior if candidate else None

    derived: dict[str, float] = {}
    for metric, label in (("coal_production", "production_yoy_pct"),
                          ("coal_sales_volume", "sales_yoy_pct"),
                          ("asp", "asp_yoy_pct")):
        current, earlier = value(metric, "6M-2026"), prior(metric, "6M-2026")
        if current is None:
            current, earlier = value(metric, "9M-2026"), prior(metric, "9M-2026")
        if current is not None and earlier:
            derived[label] = round((current - earlier) / earlier * 100, 1)

    production = value("coal_production", "6M-2026")
    sales = value("coal_sales_volume", "6M-2026")
    price = value("asp", "6M-2026")
    if production is not None and sales is not None:
        derived["inventory_drawdown_mt"] = round(sales - production, 1)
    if sales is not None and price is not None:
        derived["implied_revenue_usd_m"] = round(sales * price, 1)
    return {key: derived[key] for key in
            ("inventory_drawdown_mt", "implied_revenue_usd_m",
             "production_yoy_pct", "sales_yoy_pct", "asp_yoy_pct") if key in derived}


def build_artifact(
    *, ticker: str, period: str, year: int, docs: list[dict[str, Any]], entries: list[Entry],
    drivers: list[dict[str, Any]], reconciliation: dict[str, Any], warnings: list[str],
    language: str, driver_pass: str, selected_pages: dict[str, list[int]],
) -> dict[str, Any]:
    primary = docs[0]
    operational: dict[str, Any] = {category: [] for category in CATEGORY_ORDER}
    for entry in sorted(entries, key=_entry_order):
        if entry.status == "rejected":
            continue
        category = CATEGORY.get(entry.candidate.metric, "other")
        if category in operational:
            operational[category].append(entry.as_listed())
    operational["management_stated_drivers"] = [
        {key: driver[key] for key in ("driver", "affects", "quote", "page")}
        for driver in drivers
    ]
    operational["entries"] = [entry.as_flat() for entry in sorted(entries, key=_entry_order)]
    operational["drivers"] = [
        {
            "driver": driver["driver"], "affects": driver["affects"],
            "quote": driver["quote"], "page": driver["page"],
            "source_doc": primary.get("id"), "type": "management_stated",
            "source": driver.get("source"),
        }
        for driver in drivers
    ]

    rejected = [entry for entry in entries if entry.status == "rejected"]
    documented_warnings = list(warnings) + [
        f"{entry.candidate.metric} {entry.candidate.scope} page {entry.candidate.page}: "
        f"{entry.reason}" for entry in rejected if entry.reason
    ]
    completeness = {
        "operational": "partial" if rejected else "complete",
        "as_of": date.today().isoformat(),
        "documents_captured": len(docs),
        "driver_pass": driver_pass,
        "gaps": [{"item": entry.candidate.metric, "state": "rejected",
                  "detail": entry.reason} for entry in rejected],
    }

    document = {
        "id": primary.get("id"),
        "type": primary.get("type"),
        "published": primary.get("published"),
        "url": primary.get("url"),
        "pages": primary.get("pages"),
        "bytes": primary.get("bytes"),
        "language": language,
        "fetched_with": primary.get("via") or "unknown",
        "status": primary.get("status"),
        "sha256": primary.get("sha256"),
        "path": primary.get("path"),
        "selected_pages": {
            "en": selected_pages.get("en", []), "id": selected_pages.get("id", []),
        },
        "extraction_warnings": documented_warnings,
    }

    return {
        "ticker": ticker,
        "period": period,
        "year": str(year),
        "document": document,
        "documents": [
            {key: doc.get(key) for key in ("id", "type", "published", "url", "pages", "status")}
            for doc in docs
        ],
        "operational": operational,
        "reconciliation": reconciliation,
        "derived": _derived(entries),
        "completeness": completeness,
    }


# --------------------------------------------------------------------------- #
# run
# --------------------------------------------------------------------------- #

def run(
    ticker: str,
    period: str,
    year: int,
    *,
    doc_ids: Sequence[str] | None = None,
    offline: bool = False,
    force: bool = False,
    model: str | None = None,
    out: Path | str | None = None,
    operational_dir: Path | str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract one period. Returns ``(artifact, report)``. Never raises for data gaps."""
    ticker = ticker.strip().upper()
    period = period.strip().upper()
    year = int(year)
    directory = Path(operational_dir) if operational_dir else OPERATIONAL_DIR
    metrics = load_metrics()
    specs = build_specs(metrics)

    docs, warnings = select_documents(ticker, year, doc_ids)
    report: dict[str, Any] = {
        "ticker": ticker, "period": period, "year": year, "documents": [],
        "entries": 0, "drivers": 0, "reconciled": [], "anchors": [],
        "rejected": [], "warnings": list(warnings), "driver_pass": "none",
        "language": "none", "selected_pages": {"en": [], "id": []},
    }
    if not docs:
        warnings.append(f"no captured document for {ticker} {period} {year}")
        artifact = {
            "ticker": ticker, "period": period, "year": str(year),
            "document": {"status": "not_captured"},
            "documents": [],
            "operational": {**{category: [] for category in CATEGORY_ORDER},
                            "management_stated_drivers": [], "entries": [], "drivers": []},
            "reconciliation": {}, "derived": {},
            "completeness": {"operational": "not_captured", "as_of": date.today().isoformat(),
                             "documents_captured": 0, "gaps": [
                                 {"item": "press_release", "state": "not_captured",
                                  "detail": "no captured document in the manifest"}]},
        }
        report["warnings"] = warnings
        return artifact, report

    seed = None
    try:
        seed = load_xbrl_seed(ticker, period, year)
    except Exception as exc:  # noqa: BLE001 - XBRL is optional evidence
        warnings.append(f"XBRL seed unavailable: {type(exc).__name__}: {exc}")
    if seed is None:
        warnings.append("XBRL seed unavailable: overlap metrics fall back to quoted_in_source")

    all_entries: dict[tuple[str, str], Entry] = {}
    drivers: list[dict[str, Any]] = []
    seen_driver_quotes: set[str] = set()
    language = "unknown"
    selected_pages: dict[str, list[int]] = {"en": [], "id": []}
    driver_pass = "deterministic" if offline else "llm"

    for doc in docs:
        pages = load_document_pages(doc, directory)
        report["documents"].append({"id": doc.get("id"), "pages": len(pages)})
        if not pages:
            warnings.append(f"no readable text for {doc.get('id')}")
            continue
        selected, doc_language, page_map = select_pages(pages)
        language = doc_language if language in ("unknown", doc_language) else "en+id"
        selected_pages = {
            "en": sorted(set(selected_pages["en"]) | set(page_map.get("en", []))),
            "id": sorted(set(selected_pages["id"]) | set(page_map.get("id", []))),
        }
        default = document_scope(selected, default_scope(doc.get("published")))
        candidates = extract_metrics(selected, specs, default, doc_language, str(year))

        for candidate in sorted(candidates, key=_rank):
            key = (candidate.metric, candidate.scope)
            spec = specs[candidate.metric]
            existing = all_entries.get(key)
            if existing is not None:
                if existing.candidate.value != candidate.value:
                    warnings.append(
                        f"{candidate.metric} {candidate.scope}: kept "
                        f"{existing.candidate.value:g} from {existing.doc.get('id')} page "
                        f"{existing.candidate.page}; ignored {candidate.value:g} from "
                        f"{doc.get('id')} page {candidate.page}"
                    )
                continue
            verdict = reconcile(candidate, seed)
            if XBRL_OVERLAP.get(candidate.metric) is not None and seed is not None:
                if verdict["ok"]:
                    status = "preliminary"
                    verification = {"method": "reconciled",
                                    "reconciled_to": verdict["reconciled_to"]}
                else:
                    status = "rejected"
                    verification = {"method": "rejected", "reconciled_to": None}
            else:
                quoted, detail = verify_quoted(candidate, spec)
                if quoted:
                    status = "preliminary"
                    verification = {"method": "quoted_in_source", "reconciled_to": None}
                else:
                    status = "rejected"
                    verification = {"method": "rejected", "reconciled_to": None}
                    verdict = dict(verdict)
                    verdict["reason"] = detail
            all_entries[key] = Entry(candidate=candidate, spec=spec, doc=doc, status=status,
                                     verification=verification, reason=verdict.get("reason"))

        # Drivers: one LLM call per document, or the deterministic fallback.
        doc_drivers: list[dict[str, Any]] = []
        if not offline:
            chosen_model = model or _default_driver_model()
            try:
                doc_drivers, driver_warnings = llm_drivers(
                    selected, ticker=ticker, doc=doc, model=chosen_model
                )
                warnings.extend(driver_warnings)
            except Exception as exc:  # noqa: BLE001 - degrade, never crash
                warnings.append(f"llm driver pass failed for {doc.get('id')}: "
                                f"{type(exc).__name__}: {exc}")
                driver_pass = "deterministic_fallback"
                doc_drivers = deterministic_drivers(selected, metrics)
            if not doc_drivers and driver_pass != "deterministic_fallback":
                driver_pass = "deterministic_fallback"
                doc_drivers = deterministic_drivers(selected, metrics)
        else:
            doc_drivers = deterministic_drivers(selected, metrics)

        for driver in doc_drivers:
            page_text = _normalize(dict(selected).get(driver["page"], ""))
            if _normalize(driver["quote"]) not in page_text:
                warnings.append(
                    f"driver dropped: quote not verbatim on page {driver['page']}: "
                    f"{driver['quote'][:60]!r}"
                )
                continue
            normalized = _normalize(driver["quote"]).lower()
            if normalized in seen_driver_quotes:
                continue
            seen_driver_quotes.add(normalized)
            drivers.append(driver)

    entries = list(all_entries.values())
    reconciliation: dict[str, Any] = {}
    for anchor in RECONCILE_ANCHORS:
        for entry in entries:
            if XBRL_OVERLAP.get(entry.candidate.metric) is None:
                continue
            if XBRL_OVERLAP[entry.candidate.metric][2] != anchor:
                continue
            verdict = reconcile(entry.candidate, seed)
            if verdict["xbrl"] is not None:
                reconciliation[anchor] = {
                    "release": _round_value(verdict["release"]),
                    "xbrl": round(verdict["xbrl"], 3),
                }
            break

    anchors = [key for key in RECONCILE_ANCHORS if key in reconciliation]
    report["reconciled"] = [
        {"key": key, "release": reconciliation[key]["release"], "xbrl": reconciliation[key]["xbrl"],
         "ok": any(entry.candidate.metric in XBRL_OVERLAP
                   and XBRL_OVERLAP[entry.candidate.metric][2] == key
                   and entry.status != "rejected" for entry in entries)}
        for key in anchors
    ]
    report["anchors"] = anchors
    report["rejected"] = [
        {"metric": entry.candidate.metric, "scope": entry.candidate.scope,
         "value": entry.candidate.value, "page": entry.candidate.page,
         "reason": entry.reason}
        for entry in entries if entry.status == "rejected"
    ]
    report["entries"] = len(entries)
    report["drivers"] = len(drivers)
    report["driver_pass"] = driver_pass
    report["language"] = language
    report["selected_pages"] = selected_pages
    report["warnings"] = warnings

    artifact = build_artifact(
        ticker=ticker, period=period, year=year, docs=docs, entries=entries,
        drivers=drivers, reconciliation=reconciliation, warnings=warnings,
        language=language, driver_pass=driver_pass, selected_pages=selected_pages,
    )

    target = Path(out) if out else paths.operational_path(ticker, period, year)
    if target.exists() and not force:
        report["written"] = False
        report["path"] = str(target)
        report["warnings"] = warnings + [f"artifact exists at {target}; re-run with --force to overwrite"]
        return artifact, report
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report["written"] = True
    report["path"] = str(target)
    return artifact, report


def _default_driver_model() -> str:
    try:
        config = yaml.safe_load((PROJECT_ROOT / "config.yaml").read_text(encoding="utf-8")) or {}
        model = ((config.get("analysis") or {}).get("models") or {}).get("extract")
        if model:
            return str(model)
    except (OSError, yaml.YAMLError):
        pass
    return "deepseek-v4.1-flash"


def print_report(report: dict[str, Any]) -> None:
    print(f"\n=== extract {report['ticker']} {report['period']} {report['year']} ===")
    for doc in report.get("documents", []):
        print(f"  document {doc['id']}: {doc['pages']} page(s)")
    print(f"  language {report['language']}; EN pages {report['selected_pages'].get('en')}; "
          f"ID pages {report['selected_pages'].get('id')}")
    print(f"  entries {report['entries']}  drivers {report['drivers']} "
          f"({report['driver_pass']})")
    anchors = report.get("anchors") or []
    passed = sum(1 for item in report["reconciled"] if item["ok"])
    print(f"  reconciled {passed}/{len(anchors)} anchors"
          + (f": {', '.join(anchors)}" if anchors else ""))
    for item in report["reconciled"]:
        verdict = "ok" if item["ok"] else "REJECTED"
        print(f"    {item['key']:<15} release {item['release']} vs xbrl {item['xbrl']}  [{verdict}]")
    if report["rejected"]:
        print(f"  rejected {len(report['rejected'])}:")
        for item in report["rejected"]:
            print(f"    {item['metric']} {item['scope']} p{item['page']}: {item['reason']}")
    else:
        print("  rejected 0")
    for warning in report.get("warnings", []):
        print(f"  warn: {warning}")
    if report.get("written"):
        print(f"  wrote {report['path']}")
    elif report.get("path"):
        print(f"  not written (exists): {report['path']} (use --force)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Class B operational extraction (WS3)")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--period", required=True, choices=("Q1", "Q2", "Q3", "Q4", "FY"))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--doc-id", action="append", default=None)
    parser.add_argument("--offline", action="store_true",
                        help="skip the LLM driver pass (deterministic causal sentences)")
    parser.add_argument("--force", action="store_true", help="overwrite an existing artifact")
    parser.add_argument("--model", default=None)
    parser.add_argument("--out", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _, report = run(
        args.ticker, args.period, args.year, doc_ids=args.doc_id, offline=args.offline,
        force=args.force, model=args.model, out=args.out,
    )
    print_report(report)
    # A missing document is an explicit not_captured state, not a crash.
    return 0


if __name__ == "__main__":
    sys.exit(main())
