"""Class B document ingest for the IDX research chat (MVP-PLAN §4.2, WS3.1-3.2).

One CLI, two entry points:

    ingest.py add  --ticker ITMG --url URL --type press_release --published 2026-08-10
    ingest.py scan --ticker PTBA [--limit N] [--dry-run] | --all
    ingest.py show --ticker ITMG --period Q2 --year 2026

Contract (deliberately boring so the extractor and the typed tools can trust it):

- A document is a PDF plus a per-page text file with explicit ``=== PAGE n ===``
  markers, so page numbers never move when text is reflowed.
- ``manifest.json`` records every attempt, including failures. A fetch that
  returns 403 or times out is *data*, not an exception: it becomes a
  ``not_captured`` record with the HTTP status / error and the command exits 0.
- Adding the same URL (or the same bytes) twice is a no-op. A captured vintage
  is never overwritten: if the bytes changed, a new ``_v2`` document is added
  next to it and the old one is kept.
- ``scan`` discovers PDF links on the configured IR/news pages, filters them by
  title/date, downloads through the same path, and reports the 200-rate. A
  JavaScript-only listing page yields "0 discovered", never a crash.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse

import yaml

try:  # normal import (tests, `python -m chat_app.ingest`)
    from chat_app import paths
except ImportError:  # executed as a file: chat-app/ is the import root
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from chat_app import paths


# Module-level so tests can point ingest at a temporary operational tree.
PROJECT_ROOT: Path = paths.PROJECT_ROOT
OPERATIONAL_DIR: Path = paths.OPERATIONAL_DIR

PAGE_MARKER_TEMPLATE = "=== PAGE {n} ==="
PAGE_MARKER_RE = re.compile(r"^=== PAGE (\d+) ===$", re.MULTILINE)

DOC_STATUSES = ("captured", "not_captured", "rejected")
DOC_TYPES = ("press_release", "presentation", "filing", "annual_report", "listing", "other")

# Titles that are documents but never a results release. Skipped by `scan`.
_NON_RELEASE_TITLES = (
    "annual report",
    "prospectus",
    "charter",
    "code of conduct",
    "whistleblow",
    "guideline",
    "manual",
    "policy",
    "sop",
    "company profile",
    "macro",
)
# A link must look like a results/press release to be attempted.
_RELEASE_HINTS = (
    "release",
    "press",
    "financial",
    "kinerja",
    "laporan",
    "result",
    "investor",
    "quarter",
    "triwulan",
    "semester",
    "earning",
    "6m",
    "9m",
    "3m",
)

_MONTHS = {
    "januari": 1, "january": 1, "februari": 2, "february": 2, "maret": 3, "march": 3,
    "april": 4, "mei": 5, "may": 5, "juni": 6, "june": 6, "juli": 7, "july": 7,
    "agustus": 8, "august": 8, "september": 9, "oktober": 10, "october": 10,
    "november": 11, "desember": 12, "december": 12,
}


# --------------------------------------------------------------------------- #
# config / paths
# --------------------------------------------------------------------------- #

def load_config() -> dict[str, Any]:
    """Read ``config.yaml`` (read-only). Missing file → empty config."""
    path = PROJECT_ROOT / "config.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - config is committed valid YAML
        print(f"warn: config.yaml is not valid YAML ({exc}); using defaults", file=sys.stderr)
        return {}


def ingest_config() -> dict[str, Any]:
    cfg = (load_config().get("chat") or {}).get("ingest") or {}
    return {
        "impersonate": cfg.get("impersonate", "chrome131"),
        "timeout_sec": float(cfg.get("timeout_sec", 60)),
        "user_agent": cfg.get(
            "user_agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ),
        "targets": cfg.get("targets") or {},
    }


def doc_dir(ticker: str) -> Path:
    return OPERATIONAL_DIR / ticker.upper()


def manifest_path(ticker: str) -> Path:
    return doc_dir(ticker) / "manifest.json"


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_date(value: str | date | None) -> Optional[str]:
    """Accept ISO dates, ``YYYYMMDD``, ``10 August 2026``, ``August 2026``."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    match = re.search(r"(20\d{2})[-_/]?(\d{2})[-_/]?(\d{2})", text)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
        except ValueError:
            return None
    match = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})", text)
    if match and match.group(2).lower() in _MONTHS:
        try:
            return date(int(match.group(3)), _MONTHS[match.group(2).lower()],
                        int(match.group(1))).isoformat()
        except ValueError:
            return None
    match = re.search(r"([A-Za-z]+)\s+(20\d{2})", text)
    if match and match.group(1).lower() in _MONTHS:
        return date(int(match.group(2)), _MONTHS[match.group(1).lower()], 1).isoformat()
    return None


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def default_doc_id(ticker: str, published: str, doc_type: str) -> str:
    return f"{ticker.lower()}_{published}_{slug(doc_type)}"


# --------------------------------------------------------------------------- #
# manifest
# --------------------------------------------------------------------------- #

def load_manifest(ticker: str) -> dict[str, Any]:
    """Return ``{ticker, updated_at, documents[], scans[]}``.

    Tolerates a bare list of documents (the shape described in the plan's
    narrative) so a hand-written manifest still works.
    """
    path = manifest_path(ticker)
    data: Any = None
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"warn: manifest for {ticker} is unreadable ({exc}); starting a new one",
                  file=sys.stderr)
            data = None
    if isinstance(data, list):
        data = {"ticker": ticker.upper(), "documents": data}
    if not isinstance(data, dict):
        data = {}
    data.setdefault("ticker", ticker.upper())
    data.setdefault("documents", [])
    data.setdefault("scans", [])
    return data


def save_manifest(ticker: str, manifest: dict[str, Any]) -> Path:
    path = manifest_path(ticker)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["ticker"] = ticker.upper()
    manifest["updated_at"] = _now()
    manifest["documents"] = sorted(
        manifest.get("documents", []), key=lambda d: (d.get("published") or "", d.get("id") or "")
    )
    manifest["scans"] = (manifest.get("scans") or [])[-50:]
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def find_document(ticker: str, doc_id: str | None = None, *, url: str | None = None,
                  sha256: str | None = None) -> Optional[dict[str, Any]]:
    for doc in load_manifest(ticker)["documents"]:
        if doc.get("status") != "captured":
            continue
        if doc_id and doc.get("id") == doc_id:
            return doc
        if url and doc.get("url") == url:
            return doc
        if sha256 and doc.get("sha256") == sha256:
            return doc
    return None


# --------------------------------------------------------------------------- #
# fetch / text extraction
# --------------------------------------------------------------------------- #

@dataclass
class FetchResult:
    status_code: Optional[int]
    content: bytes = b""
    content_type: str = ""
    error: Optional[str] = None
    via: str = ""

    @property
    def ok(self) -> bool:
        return self.status_code == 200 and bool(self.content)


def fetch(url: str, *, timeout: float = 60.0, impersonate: str = "chrome131",
          user_agent: str | None = None) -> FetchResult:
    """GET ``url`` with curl_cffi, falling back to requests. Never raises."""
    headers = {"User-Agent": user_agent} if user_agent else None
    try:
        from curl_cffi import requests as curl_requests

        response = curl_requests.get(url, impersonate=impersonate, timeout=timeout,
                                     headers=headers, allow_redirects=True)
        return FetchResult(
            status_code=response.status_code,
            content=response.content or b"",
            content_type=response.headers.get("content-type", "") or "",
            error=None if response.status_code == 200 else f"HTTP {response.status_code}",
            via=f"curl_cffi impersonate={impersonate}",
        )
    except Exception as exc:  # noqa: BLE001 - network failures are recorded, not raised
        first_error = f"{type(exc).__name__}: {exc}".strip()
    try:
        import requests

        response = requests.get(url, headers=headers or {}, timeout=timeout,
                                allow_redirects=True)
        return FetchResult(
            status_code=response.status_code,
            content=response.content or b"",
            content_type=response.headers.get("content-type", "") or "",
            error=None if response.status_code == 200 else f"HTTP {response.status_code}",
            via="requests",
        )
    except Exception as exc:  # noqa: BLE001
        return FetchResult(status_code=None, error=f"{first_error} | requests: "
                                                   f"{type(exc).__name__}: {exc}".strip(),
                           via="unavailable")


def is_pdf(content: bytes, content_type: str = "") -> bool:
    if content[:5] == b"%PDF-":
        return True
    return "pdf" in (content_type or "").lower() and bool(content)


def pdf_page_texts(content: bytes) -> list[str]:
    """Per-page text via PyMuPDF. Raises ValueError when the bytes are not a PDF."""
    try:
        import fitz  # pymupdf
    except ImportError as exc:  # pragma: no cover - dependency is installed
        raise ValueError(f"pymupdf is unavailable: {exc}") from exc
    try:
        with fitz.open(stream=content, filetype="pdf") as document:
            return [page.get_text("text") or "" for page in document]
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"could not read PDF: {exc}") from exc


def render_pages_text(pages: Iterable[str]) -> str:
    """``=== PAGE 1 ===`` + text per page. Page numbers are the PDF's own."""
    blocks: list[str] = []
    for number, text in enumerate(pages, start=1):
        blocks.append(f"{PAGE_MARKER_TEMPLATE.format(n=number)}\n\n{text.strip()}")
    return "\n\n".join(blocks).strip() + "\n"


def parse_pages_text(text: str) -> list[str]:
    """Inverse of :func:`render_pages_text`. Index i is page i+1 (0 if missing)."""
    matches = list(PAGE_MARKER_RE.finditer(text))
    if not matches:
        return [text] if text.strip() else []
    pages: list[str] = []
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        pages.append(text[start:end].strip())
    return pages


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


# --------------------------------------------------------------------------- #
# add
# --------------------------------------------------------------------------- #

def _record(doc_id: str, doc_type: str, published: str, url: str, fetched_at: str,
            pages: Optional[int], sha: Optional[str], path: Optional[str],
            status: str, error: Optional[str], **extra: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": doc_id,
        "type": doc_type,
        "published": published,
        "url": url,
        "fetched_at": fetched_at,
        "pages": pages,
        "sha256": sha,
        "path": path,
        "status": status,
        "error": error,
    }
    record.update({k: v for k, v in extra.items() if v is not None})
    return record


def add_document(
    ticker: str,
    url: str,
    doc_type: str = "press_release",
    published: str | date | None = None,
    *,
    doc_id: str | None = None,
    force: bool = False,
    local_file: Path | str | None = None,
    cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch (or import) one document, update the manifest, return an action record.

    Action is one of ``created``, ``updated``, ``noop``, ``new_vintage``,
    ``not_captured``, ``rejected``. Never raises for network/HTTP problems.
    """
    ticker = ticker.strip().upper()
    cfg = cfg or ingest_config()
    published_iso = parse_date(published) or date.today().isoformat()
    doc_id = doc_id or default_doc_id(ticker, published_iso, doc_type)
    manifest = load_manifest(ticker)
    documents: list[dict[str, Any]] = manifest["documents"]

    existing = next((d for d in documents if d.get("id") == doc_id), None)
    captured_same_url = next(
        (d for d in documents if d.get("status") == "captured" and d.get("url") == url), None
    )

    # Idempotent re-add: the URL is already a captured vintage.
    if local_file is None and captured_same_url is not None and not force:
        return {"id": captured_same_url["id"], "action": "noop", "status": "captured",
                "error": None, "path": captured_same_url.get("path"),
                "detail": "url already captured"}

    if local_file is not None:
        source = Path(local_file)
        try:
            content = source.read_bytes()
        except OSError as exc:
            result = FetchResult(status_code=None, error=f"local_file: {exc}",
                                 via="local_import")
        else:
            result = FetchResult(status_code=200, content=content,
                                 content_type="application/pdf" if is_pdf(content) else "",
                                 via="local_import")
    else:
        result = fetch(url, timeout=cfg["timeout_sec"], impersonate=cfg["impersonate"],
                       user_agent=cfg["user_agent"])

    now = _now()

    def _upsert(record: dict[str, Any]) -> dict[str, Any]:
        for index, doc in enumerate(documents):
            if doc.get("id") == record["id"]:
                documents[index] = record
                break
        else:
            documents.append(record)
        save_manifest(ticker, manifest)
        return record

    if not result.ok:
        error = result.error or (f"HTTP {result.status_code}" if result.status_code else "fetch failed")
        record = _record(doc_id, doc_type, published_iso, url, now, None, None, None,
                         "not_captured", error, via=result.via)
        # Do not downgrade a captured vintage when a re-fetch fails.
        if existing is not None and existing.get("status") == "captured":
            return {"id": doc_id, "action": "noop", "status": "captured", "error": error,
                    "path": existing.get("path"),
                    "detail": "existing captured vintage kept; refetch failed"}
        _upsert(record)
        return {"id": doc_id, "action": "not_captured", "status": "not_captured",
                "error": error, "path": None}

    sha = sha256_bytes(result.content)
    captured_same_sha = next(
        (d for d in documents if d.get("status") == "captured" and d.get("sha256") == sha), None
    )
    if captured_same_sha is not None:
        return {"id": captured_same_sha["id"], "action": "noop", "status": "captured",
                "error": None, "path": captured_same_sha.get("path"),
                "detail": "identical bytes already captured"}

    if not is_pdf(result.content, result.content_type):
        error = f"not a PDF (content-type={result.content_type or 'unknown'})"
        record = _record(doc_id, doc_type, published_iso, url, now, None, sha, None,
                         "rejected", error, via=result.via)
        _upsert(record)
        return {"id": doc_id, "action": "rejected", "status": "rejected",
                "error": error, "path": None}

    try:
        pages = pdf_page_texts(result.content)
    except ValueError as exc:
        record = _record(doc_id, doc_type, published_iso, url, now, None, sha, None,
                         "rejected", f"pdf parse failed: {exc}", via=result.via)
        _upsert(record)
        return {"id": doc_id, "action": "rejected", "status": "rejected",
                "error": record["error"], "path": None}

    if existing is not None and existing.get("status") == "captured":
        # Bytes differ from an already-captured vintage: keep both.
        index = 2
        while any(d.get("id") == f"{doc_id}_v{index}" for d in documents):
            index += 1
        doc_id = f"{doc_id}_v{index}"
        supersedes = existing.get("id")
    else:
        supersedes = None

    directory = doc_dir(ticker)
    directory.mkdir(parents=True, exist_ok=True)
    pdf_path = directory / f"{doc_id}.pdf"
    txt_path = directory / f"{doc_id}.txt"
    pdf_path.write_bytes(result.content)
    txt_path.write_text(render_pages_text(pages), encoding="utf-8")

    record = _record(
        doc_id, doc_type, published_iso, url, now, len(pages), sha, _rel(pdf_path),
        "captured", None, bytes=len(result.content), via=result.via,
        text=_rel(txt_path), supersedes=supersedes,
    )
    _upsert(record)
    action = "new_vintage" if supersedes else ("updated" if existing else "created")
    return {"id": doc_id, "action": action, "status": "captured", "error": None,
            "path": _rel(pdf_path), "pages": len(pages), "sha256": sha}


# --------------------------------------------------------------------------- #
# scan
# --------------------------------------------------------------------------- #

@dataclass
class ScanReport:
    ticker: str
    listings: list[dict[str, Any]] = field(default_factory=list)

    @property
    def discovered(self) -> int:
        return sum(item["discovered"] for item in self.listings)

    @property
    def attempted(self) -> int:
        return sum(item["attempted"] for item in self.listings)

    @property
    def captured(self) -> int:
        return sum(item["captured"] for item in self.listings)

    @property
    def failed(self) -> int:
        return sum(item["not_captured"] + item["rejected"] for item in self.listings)

    @property
    def success_rate(self) -> Optional[float]:
        return (self.captured / self.attempted) if self.attempted else None

    @property
    def listings_ok(self) -> int:
        return sum(1 for item in self.listings if item.get("http_status") == 200)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "listings": self.listings,
            "discovered": self.discovered,
            "attempted": self.attempted,
            "captured": self.captured,
            "failed": self.failed,
            "success_rate": self.success_rate,
            "listings_ok": self.listings_ok,
            "listings_total": len(self.listings),
        }


def discover_pdf_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Return ``(absolute_url, label)`` for PDF links found in ``html``.

    Handles anchors and inline JS/JSON string blobs (some IR sites render the
    list client-side); a page with neither simply yields an empty list.
    """
    found: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(href: str, label: str) -> None:
        absolute = urljoin(base_url, href.strip()).replace(" ", "%20")
        if absolute in seen:
            return
        seen.add(absolute)
        found.append((absolute, label.strip() or Path(urlparse(absolute).path).name))

    try:
        from bs4 import BeautifulSoup
    except ImportError:  # pragma: no cover - bs4 is installed
        BeautifulSoup = None  # type: ignore[assignment]
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not _looks_like_pdf(href):
                continue
            _add(href, anchor.get_text(" ", strip=True))
    for match in re.finditer(
        r"""["'(\s=](https?://[^"'<>\s]+?\.pdf|/[^"'<>]+?\.pdf)""", html, re.IGNORECASE
    ):
        _add(match.group(1), "")
    return found


def _looks_like_pdf(href: str) -> bool:
    return ".pdf" in (href or "").lower().split("?")[0]


def _filter_candidates(links: list[tuple[str, str]], reference_year: int,
                       existing_urls: set[str]) -> list[dict[str, Any]]:
    """Title/date heuristics. Returns ordered candidates, skipping captured URLs."""
    candidates: list[dict[str, Any]] = []
    for url, label in links:
        if url in existing_urls:
            continue
        haystack = f"{label} {url}".lower()
        if any(bad in haystack for bad in _NON_RELEASE_TITLES):
            continue
        if not any(hint in haystack for hint in _RELEASE_HINTS):
            continue
        published = parse_date(f"{label} {Path(urlparse(url).path).name}")
        years = [int(y) for y in re.findall(r"(20\d{2})", haystack)]
        if published is None and years:
            published = f"{max(years)}-01-01"
        if years and max(years) < reference_year - 1:
            continue
        candidates.append({
            "url": url,
            "label": label,
            "published": published or date.today().isoformat(),
            "published_inferred": published is None,
        })
    # Newest first; undated links keep their listing order.
    candidates.sort(key=lambda c: (c["published"], c["url"]), reverse=True)
    return candidates


def scan_ticker(ticker: str, *, limit: int = 5, dry_run: bool = False,
                cfg: dict[str, Any] | None = None, reference_year: int | None = None) -> ScanReport:
    """Discover + download one ticker's IR PDFs. Records every attempt."""
    ticker = ticker.strip().upper()
    cfg = cfg or ingest_config()
    reference_year = reference_year or date.today().year
    target = (cfg.get("targets") or {}).get(ticker) or {}
    listing_urls = target.get("listing_urls") or []
    report = ScanReport(ticker=ticker)

    existing_urls = {
        d.get("url") for d in load_manifest(ticker)["documents"] if d.get("status") == "captured"
    }

    if not listing_urls:
        report.listings.append({
            "listing_url": None, "http_status": None, "discovered": 0,
            "attempted": 0, "captured": 0, "not_captured": 0, "rejected": 0,
            "error": "no listing_urls configured for this ticker", "documents": [],
        })
        return report

    for listing_url in listing_urls:
        item: dict[str, Any] = {
            "listing_url": listing_url, "http_status": None, "discovered": 0,
            "attempted": 0, "captured": 0, "not_captured": 0, "rejected": 0,
            "error": None, "documents": [], "page_bytes": 0,
        }
        result = fetch(listing_url, timeout=cfg["timeout_sec"],
                       impersonate=cfg["impersonate"], user_agent=cfg["user_agent"])
        item["http_status"] = result.status_code
        item["page_bytes"] = len(result.content or b"")
        if not result.ok:
            item["error"] = result.error or f"HTTP {result.status_code}"
            report.listings.append(item)
            if not dry_run:
                _record_listing_state(ticker, listing_url, item)
                _record_scan(ticker, item)
            continue

        html = result.content.decode("utf-8", errors="ignore")
        links = discover_pdf_links(html, listing_url)
        candidates = _filter_candidates(links, reference_year, existing_urls)
        item["discovered"] = len(candidates)
        if not candidates:
            item["error"] = (
                "no PDF links discovered "
                + ("(JavaScript-only or empty listing page)" if len(html) < 60_000 else "")
            )
        for candidate in candidates[: limit if limit and limit > 0 else None]:
            item["attempted"] += 1
            if dry_run:
                item["documents"].append({"url": candidate["url"], "action": "would_add",
                                          "published": candidate["published"], "status": None})
                continue
            outcome = add_document(
                ticker, candidate["url"], "press_release", candidate["published"],
                cfg=cfg,
            )
            existing_urls.add(candidate["url"])
            item["documents"].append({
                "id": outcome["id"], "url": candidate["url"], "action": outcome["action"],
                "status": outcome["status"], "error": outcome["error"],
            })
            if outcome["status"] == "captured":
                item["captured"] += 1
            elif outcome["status"] == "not_captured":
                item["not_captured"] += 1
            else:
                item["rejected"] += 1
        if not dry_run:
            _record_listing_state(ticker, listing_url, item)
            _record_scan(ticker, item)
        report.listings.append(item)
    return report


def _listing_doc_id(ticker: str, listing_url: str) -> str:
    return f"{ticker.lower()}_listing_{slug(urlparse(listing_url).netloc)}"


def _record_listing_state(ticker: str, listing_url: str, item: dict[str, Any]) -> None:
    """Keep one explicit evidence record for a listing we could not read.

    It is a *listing* document, not a release: ``documents_status`` can show
    "we tried the IR page and it 404'd / is JavaScript-only" without pretending
    a press release exists.
    """
    manifest = load_manifest(ticker)
    doc_id = _listing_doc_id(ticker, listing_url)
    documents = manifest["documents"]
    ok = item.get("http_status") == 200 and item.get("discovered", 0) > 0
    if ok:
        # Discovery works again: drop the stale failure record.
        manifest["documents"] = [doc for doc in documents if doc.get("id") != doc_id]
        save_manifest(ticker, manifest)
        return
    error = item.get("error") or (
        f"HTTP {item.get('http_status')}" if item.get("http_status") not in (200, None)
        else "no PDF links discovered"
    )
    record = _record(doc_id, "listing", date.today().isoformat(), listing_url, _now(),
                     None, None, None, "not_captured", error,
                     http_status=item.get("http_status"))
    for index, doc in enumerate(documents):
        if doc.get("id") == doc_id:
            documents[index] = record
            break
    else:
        documents.append(record)
    save_manifest(ticker, manifest)


def _record_scan(ticker: str, item: dict[str, Any]) -> None:
    manifest = load_manifest(ticker)
    manifest["scans"].append({
        "at": _now(),
        "listing_url": item.get("listing_url"),
        "http_status": item.get("http_status"),
        "discovered": item.get("discovered", 0),
        "attempted": item.get("attempted", 0),
        "captured": item.get("captured", 0),
        "not_captured": item.get("not_captured", 0),
        "rejected": item.get("rejected", 0),
        "error": item.get("error"),
    })
    save_manifest(ticker, manifest)


def print_scan_report(report: ScanReport) -> None:
    print(f"\n=== scan {report.ticker} ===")
    for item in report.listings:
        status = item.get("http_status")
        status_text = f"HTTP {status}" if status is not None else "no response"
        print(f"  listing {item.get('listing_url') or '-'} -> {status_text}, "
              f"{item['discovered']} candidate(s)")
        if item.get("error"):
            print(f"    note: {item['error']}")
        for doc in item.get("documents", []):
            print(f"    {doc.get('action'):<10} {doc.get('status') or '-':<12} "
                  f"{doc.get('id') or doc.get('url')}"
                  + (f"  [{doc.get('error')}]" if doc.get("error") else ""))
    rate = report.success_rate
    rate_text = f"{rate:.0%}" if rate is not None else "n/a (0 attempted)"
    not_captured = sum(item["not_captured"] for item in report.listings)
    rejected = sum(item["rejected"] for item in report.listings)
    print(f"  discovered {report.discovered}  attempted {report.attempted}  "
          f"captured {report.captured}  not_captured {not_captured}  "
          f"rejected {rejected}  200-rate {rate_text}")
    print(f"  listing pages readable {report.listings_ok}/{len(report.listings)}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def cmd_add(args: argparse.Namespace) -> int:
    outcome = add_document(
        args.ticker, args.url, args.type, args.published, doc_id=args.doc_id,
        force=args.force, local_file=args.file,
    )
    print(json.dumps(outcome, ensure_ascii=False, indent=2))
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    cfg = ingest_config()
    if args.all:
        tickers = list((cfg.get("targets") or {}).keys())
        if not tickers:
            print("no chat.ingest.targets configured in config.yaml")
            return 0
    else:
        if not args.ticker:
            print("error: pass --ticker or --all", file=sys.stderr)
            return 2
        tickers = [args.ticker.strip().upper()]
    reports = [
        scan_ticker(ticker, limit=args.limit, dry_run=args.dry_run, cfg=cfg)
        for ticker in tickers
    ]
    for report in reports:
        print_scan_report(report)
    attempted = sum(r.attempted for r in reports)
    captured = sum(r.captured for r in reports)
    listings_ok = sum(r.listings_ok for r in reports)
    listings_total = sum(len(r.listings) for r in reports)
    if attempted:
        print(f"\nALL: listing pages readable {listings_ok}/{listings_total}  "
              f"attempted {attempted}  captured {captured}  "
              f"200-rate {captured / attempted:.0%}")
    else:
        print(f"\nALL: listing pages readable {listings_ok}/{listings_total}  "
              "attempted 0  captured 0  200-rate n/a (nothing discovered)")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    path = paths.operational_path(args.ticker, args.period, args.year)
    if not path.exists():
        print(json.dumps({
            "error": "artifact_missing",
            "message": f"no operational artifact at {_rel(path)}",
            "hint": "run extract_operational.py first",
        }, indent=2))
        return 2
    print(path.read_text(encoding="utf-8").rstrip())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Class B document ingest (WS3)")
    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("add", help="fetch one document (PDF) and record it")
    add.add_argument("--ticker", required=True)
    add.add_argument("--url", required=True)
    add.add_argument("--type", default="press_release", choices=DOC_TYPES)
    add.add_argument("--published", required=True, help="ISO date, e.g. 2026-08-10")
    add.add_argument("--doc-id", default=None)
    add.add_argument("--force", action="store_true",
                     help="retry a not_captured record (captured vintages are never overwritten)")
    add.add_argument("--file", default=None,
                     help="import this local PDF instead of fetching (offline backfill)")
    add.set_defaults(func=cmd_add)

    scan = sub.add_parser("scan", help="discover + fetch PDFs from configured IR pages")
    scan.add_argument("--ticker", default=None)
    scan.add_argument("--all", action="store_true")
    scan.add_argument("--limit", type=int, default=5, help="max documents per listing")
    scan.add_argument("--dry-run", action="store_true")
    scan.set_defaults(func=cmd_scan)

    show = sub.add_parser("show", help="print the canonical operational JSON")
    show.add_argument("--ticker", required=True)
    show.add_argument("--period", required=True)
    show.add_argument("--year", required=True, type=int)
    show.set_defaults(func=cmd_show)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
