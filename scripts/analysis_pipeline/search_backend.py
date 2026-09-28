"""
Web search backend for the external-research stage.

Provides:
- Schemas: SearchResult, SearchResponse, PageResponse
- SearXNG adapter for discovery (JSON API)
- Query and page-fetch caches (sha256-keyed JSON)
- URL validation with redirect-hop re-validation
- HTTP page fetching with bounded responses
- Source domain classification from config rules
- Health check for search-backend availability

Uses httpx (already a dep) and stdlib only for hashing/caching.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import httpx
import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"
_IDX_DATA = _PROJECT_ROOT / "idx_data"
CACHE_DIR = _IDX_DATA / "analysis" / "research" / "cache"
SEARCH_CACHE_DIR = CACHE_DIR / "search"
FETCH_CACHE_DIR = CACHE_DIR / "fetch"

# ── Schemas ────────────────────────────────────────────────────────────

@dataclass
class SearchResult:
    result_id: str
    title: str
    url: str
    snippet: str
    published_date: str | None = None
    source_domain: str = ""
    source_type: str = "unknown"
    fetchable: bool = True
    authoritative: bool = False


@dataclass
class SearchResponse:
    status: str = "ok"
    query: str = ""
    cache_hit: bool = False
    results: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    usage: dict = field(default_factory=lambda: {"query_count": 1})


@dataclass
class PageResponse:
    status: str = "ok"
    url: str = ""
    canonical_url: str = ""
    title: str = ""
    content: str = ""
    content_type: str = ""
    extractor: str = ""
    published_date: str | None = None
    retrieved_at: str = ""
    content_hash: str = ""
    cache_hit: bool = False
    truncated: bool = False
    fetch_attempted: bool = False
    error: str | None = None


# ── Config loading ─────────────────────────────────────────────────────

def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def _research_config() -> dict:
    return (_load_config().get("analysis", {}) or {}).get("research", {}) or {}


# ── Source classification ──────────────────────────────────────────────

def classify_source(domain: str) -> str:
    cfg = _research_config()
    classification = (cfg.get("source_classification", {}) or {})
    for source_type in ("government", "filing", "news", "research"):
        spec = classification.get(source_type)
        if not isinstance(spec, dict):
            continue
        domains = spec.get("domains", []) or []
        for d in domains:
            if domain == d or domain.endswith("." + d):
                return source_type
    return "unknown"


# ── URL validation ─────────────────────────────────────────────────────

_PRIVATE_NETS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

def _host_is_private(host: str) -> bool:
    if host in ("localhost", "0.0.0.0"):
        return True
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except socket.gaierror:
            return False
        for _family, _type, _proto, _canon, sockaddr in infos:
            try:
                addr = ipaddress.ip_address(sockaddr[0])
            except ValueError:
                continue
            if addr.version == 6 and addr.ipv4_mapped:
                addr = addr.ipv4_mapped
            if any(addr in net for net in _PRIVATE_NETS):
                return True
        return False
    if addr.version == 6 and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
    return any(addr in net for net in _PRIVATE_NETS)


def validate_url(url: str) -> tuple[bool, str]:
    if not url:
        return False, "empty URL"
    parsed = urlparse(url)
    if parsed.scheme not in ("https", "http"):
        return False, "unsupported scheme"
    if not parsed.hostname:
        return False, "no hostname in URL"
    if _host_is_private(parsed.hostname):
        return False, "private or loopback host"
    return True, ""


# ── Cache utilities ────────────────────────────────────────────────────

def _make_cache_key(*components: str) -> str:
    return hashlib.sha256("|".join(components).encode()).hexdigest()


def _search_cache_path(cache_key: str) -> Path:
    SEARCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return SEARCH_CACHE_DIR / f"{cache_key}.json"


def _fetch_cache_path(cache_key: str) -> Path:
    FETCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return FETCH_CACHE_DIR / f"{cache_key}.json"


def _read_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ── SearXNG adapter ────────────────────────────────────────────────────

def _normalize_query(query: str) -> str:
    return " ".join(query.strip().lower().split())


def check_health() -> bool:
    cfg = _research_config()
    searxng = cfg.get("searxng", {}) or {}
    base = (searxng.get("base_url") or "http://localhost:8080").rstrip("/")
    path = searxng.get("health_path", "/healthz")
    timeout = float(searxng.get("timeout_s") or 5)
    try:
        r = httpx.get(f"{base}{path}", timeout=timeout)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


_VALID_TOPICS = ("news", "general")


def _coerce_topic(topic: str | None) -> str:
    """Normalize the LLM-supplied topic to a SearXNG-supported category.

    SearXNG has no built-in ``finance`` category; unknown values (including
    list-typed args parsed from malformed tool calls) fall back to ``general``
    so all configured engines (duckduckgo, brave, bing) are queried at full
    strength instead of being restricted to a sparse ``news`` engine subset.
    """
    if isinstance(topic, str):
        t = topic.strip().lower()
        if t in _VALID_TOPICS:
            return t
    return "general"


def _searxng_search(
    query: str,
    topic: str = "general",
    time_range: str | None = None,
    max_results: int = 5,
) -> dict:
    cfg = _research_config()
    searxng = cfg.get("searxng", {}) or {}
    base = (searxng.get("base_url") or "http://localhost:8080").rstrip("/")
    timeout = float(searxng.get("timeout_s") or 15)

    params: dict[str, str | int] = {
        "q": query,
        "format": "json",
        "categories": _coerce_topic(topic),
    }
    if time_range:
        params["time_range"] = time_range

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(f"{base}/search", params=params)
        r.raise_for_status()
        data = r.json()

    items = data.get("results", [])[:max_results]
    results = []
    for item in items:
        url = item.get("url", "")
        domain = urlparse(url).hostname or ""
        ok, _ = validate_url(url)
        results.append({
            "result_id": hashlib.sha256(url.encode()).hexdigest(),
            "title": item.get("title", ""),
            "url": url,
            "snippet": item.get("content") or item.get("snippet", ""),
            "published_date": item.get("publishedDate"),
            "source_domain": domain,
            "source_type": classify_source(domain),
            "fetchable": ok,
            "authoritative": False,
        })
    return {
        "status": "ok" if results else "no_results",
        "query": query,
        "cache_hit": False,
        "results": results,
        "errors": [],
        "usage": {"query_count": 1, "raw_total": data.get("number_of_results", 0)},
    }


# ── Public API: web_search ─────────────────────────────────────────────

def web_search(
    query: str,
    *,
    topic: Literal["news", "general"] = "general",
    time_range: Literal["day", "week", "month", "year"] | None = None,
    max_results: int = 5,
    ticker: str | None = None,  # noqa: ARG001 — available to tool registry
    period_start: str | None = None,  # noqa: ARG001
    period_end: str | None = None,  # noqa: ARG001
    force_refresh: bool = False,
) -> SearchResponse:
    topic = _coerce_topic(topic)  # type: ignore[assignment]
    if not check_health():
        return SearchResponse(
            status="degraded",
            query=query,
            errors=["search backend unavailable"],
        )
    cfg = _research_config()
    version = cfg.get("extractor_config_version", "1")
    cache_key = _make_cache_key(
        _normalize_query(query),
        topic,
        time_range or "",
        str(max_results),
        "searxng",
        version,
    )
    if not force_refresh:
        cached = _read_cache(_search_cache_path(cache_key))
        if cached:
            cached["cache_hit"] = True
            resp = SearchResponse(
                status=cached.get("status", "ok"),
                query=cached.get("query", query),
                cache_hit=True,
                results=cached.get("results", []),
                errors=cached.get("errors", []),
                usage=cached.get("usage", {}),
            )
            return resp
    try:
        raw = _searxng_search(
            query, topic=topic, time_range=time_range, max_results=max_results,
        )
    except Exception as exc:
        return SearchResponse(
            status="degraded",
            query=query,
            errors=[str(exc)],
        )
    raw["cache_hit"] = False
    _write_cache(_search_cache_path(cache_key), raw)
    return SearchResponse(
        status=raw.get("status", "ok"),
        query=raw.get("query", query),
        cache_hit=False,
        results=raw.get("results", []),
        errors=raw.get("errors", []),
        usage=raw.get("usage", {}),
    )


# ── Public API: fetch_web_page ─────────────────────────────────────────

def fetch_web_page(
    url: str,
    *,
    max_chars: int = 30000,
    source_title: str | None = None,  # noqa: ARG001
    force_refresh: bool = False,
) -> PageResponse:
    ok, err_msg = validate_url(url)
    if not ok:
        return PageResponse(
            status="blocked",
            url=url,
            error=err_msg,
        )
    cfg = _research_config()
    version = cfg.get("extractor_config_version", "1")
    cache_key = _make_cache_key(
        url,
        version,
        str(max_chars),
    )
    if not force_refresh:
        cached = _read_cache(_fetch_cache_path(cache_key))
        if cached:
            cached["cache_hit"] = True
            cached["fetch_attempted"] = True
            return PageResponse(**cached)

    fetch_timeout = float(cfg.get("fetch_timeout_s") or 20)
    max_redirects = int(cfg.get("max_redirects") or 5)

    try:
        from search_extractors import extract_html, extract_pdf_via_docling

        with httpx.Client(
            timeout=fetch_timeout,
            follow_redirects=False,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            },
        ) as client:
            current_url = url
            for _hop in range(max_redirects + 1):
                r = client.get(current_url)
                if r.status_code in (301, 302, 303, 307, 308):
                    loc = r.headers.get("location", "")
                    if not loc:
                        break
                    from urllib.parse import urljoin
                    current_url = urljoin(current_url, loc)
                    rok, rerr = validate_url(current_url)
                    if not rok:
                        return PageResponse(
                            status="blocked",
                            url=url,
                            canonical_url=current_url,
                            error=f"redirect to unsafe URL: {rerr}",
                        )
                    continue
                break
            else:
                return PageResponse(
                    status="degraded",
                    url=url,
                    error="too many redirects",
                )

            if r.status_code != 200:
                return PageResponse(
                    status="degraded",
                    url=url,
                    canonical_url=str(r.url),
                    error=f"HTTP {r.status_code}",
                )

        content_type = r.headers.get("content-type", "").lower()
        content = ""
        extractor = ""
        page_title = ""

        if "application/pdf" in content_type:
            if "docling" not in content_type:
                extractor = "docling"
                try:
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp.write(r.content)
                        tmp_path = tmp.name
                    try:
                        result = extract_pdf_via_docling(
                            tmp_path, max_chars=max_chars,
                        )
                        content = result.get("content", "")
                        extractor = result.get("extractor", "docling")
                        page_title = result.get("title", "")
                    finally:
                        Path(tmp_path).unlink(missing_ok=True)
                except Exception as exc:
                    return PageResponse(
                        status="degraded",
                        url=url,
                        canonical_url=str(r.url),
                        error=f"PDF extraction failed: {exc}",
                        fetch_attempted=True,
                    )
        elif "text/html" in content_type:
            extractor = "httpx"
            result = extract_html(
                r.text, url=str(r.url), max_chars=max_chars,
            )
            content = result.get("content", "")
            extractor = result.get("extractor", "httpx")
            page_title = result.get("title", "")
        else:
            return PageResponse(
                status="unsupported",
                url=url,
                canonical_url=str(r.url),
                content_type=content_type,
                error=f"unsupported content type: {content_type}",
                fetch_attempted=True,
            )

        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        truncated = len(content) >= max_chars

        resp = {
            "status": "ok" if content else "degraded",
            "url": url,
            "canonical_url": str(r.url),
            "title": page_title,
            "content": content[:max_chars],
            "content_type": content_type,
            "extractor": extractor,
            "published_date": None,
            "retrieved_at": retrieved_at,
            "content_hash": content_hash,
            "cache_hit": False,
            "truncated": truncated,
            "fetch_attempted": True,
            "error": None if content else "no extractable content",
        }
        _write_cache(_fetch_cache_path(cache_key), resp)
        return PageResponse(**resp)

    except httpx.HTTPError as exc:
        return PageResponse(
            status="degraded",
            url=url,
            error=str(exc),
            fetch_attempted=True,
        )
    except Exception as exc:
        return PageResponse(
            status="degraded",
            url=url,
            error=str(exc),
            fetch_attempted=True,
        )
