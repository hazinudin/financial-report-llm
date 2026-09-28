"""
LLM-facing tool registry and dispatcher for the external-research stage.

Registers `web_search` and `fetch_web_page` as typed Python tools with:
- OpenAI-compatible function-calling schemas
- Budget tracking (search/fetch call counts per report)
- Domain allow-list enforcement (fetch calls blocked for non-authoritative domains)
- Argument validation before execution
- Structured error returns for exhausted budgets or blocked calls

The dispatcher enforces the fetch policy: fetches for domains outside
`fetch_allow_domains` are rejected with `status: blocked` without consuming
fetch budget. The LLM decides *whether* to fetch; Python decides *what* may
be fetched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

import search_backend

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"


# ── Config ─────────────────────────────────────────────────────────────

def _research_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return (cfg.get("analysis", {}) or {}).get("research", {}) or {}
    return {}


# ── Tool schemas (OpenAI function-calling format) ──────────────────────

WEB_SEARCH_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the web for news, financial, or general information. "
            "Returns source metadata and snippets. Use this to discover "
            "relevant articles before fetching full page content."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query string",
                },
                "topic": {
                    "type": "string",
                    "enum": ["news", "general"],
                    "default": "general",
                    "description": (
                        "Search category: 'general' queries all configured "
                        "engines at full strength (recommended default); "
                        "'news' restricts to news-tagged engines."
                    ),
                },
                "time_range": {
                    "type": "string",
                    "enum": ["day", "week", "month", "year"],
                    "description": "Time range filter for recency",
                },
                "max_results": {
                    "type": "integer",
                    "default": 5,
                    "description": "Maximum results to return (1-10)",
                },
            },
            "required": ["query"],
        },
    },
}

FETCH_WEB_PAGE_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "fetch_web_page",
        "description": (
            "Fetch and extract the full text content of a single web page. "
            "Use this only when search snippets are insufficient to answer "
            "the research question. Only one URL per call. Domains outside "
            "the configured allow-list are rejected."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "Full URL of the page to fetch (https preferred)",
                },
                "max_chars": {
                    "type": "integer",
                    "default": 30000,
                    "description": "Maximum characters to return from extracted content",
                },
            },
            "required": ["url"],
        },
    },
}


# ── Tool dispatcher ────────────────────────────────────────────────────

@dataclass
class BudgetState:
    search_count: int = 0
    fetch_count: int = 0
    max_searches: int = 36
    max_fetches: int = 8
    search_errors: list[str] = field(default_factory=list)
    fetch_errors: list[str] = field(default_factory=list)
    force_refresh: bool = False
    # Hosts and URLs surfaced by prior web_search calls in this run. Used by
    # the fetch_requires_low_confidence gate to reject blind fetches the LLM
    # invents without first discovering them via search.
    seen_search_hosts: set[str] = field(default_factory=set)
    seen_search_urls: set[str] = field(default_factory=set)


_VALID_TOPICS = ("news", "general")


def _coerce_topic(topic: Any) -> str:
    """Defensive coercion of the LLM-supplied topic.

    Resilient to list-typed args (e.g. ``"['news']"`` repr strings or actual
    lists) that have been observed from some tool-calling clients. Unknown
    values fall back to 'general' so SearXNG queries all configured engines.
    """
    if isinstance(topic, (list, tuple)):
        topic = topic[0] if topic else "general"
    if isinstance(topic, str):
        t = topic.strip().lower().strip("\"'[] ")
        if t in _VALID_TOPICS:
            return t
    return "general"


def _host_matches_set(hostname: str, allowed: set[str]) -> bool:
    hostname = (hostname or "").lower()
    if not hostname:
        return False
    for d in allowed:
        if hostname == d or hostname.endswith("." + d) or d.endswith("." + hostname):
            return True
    return False


def _is_domain_allowed(url: str) -> bool:
    cfg = _research_config()
    allowed = cfg.get("fetch_allow_domains", []) or []
    if not allowed:
        return False
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    for d in allowed:
        if hostname == d or hostname.endswith("." + d):
            return True
    return False


def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    budget: BudgetState,
) -> dict[str, Any]:
    cfg = _research_config()
    if tool_name == "web_search":
        if budget.search_count >= budget.max_searches:
            return {
                "status": "degraded",
                "error": "search budget exhausted",
            }
        budget.search_count += 1
        topic = _coerce_topic(arguments.get("topic"))
        try:
            resp = search_backend.web_search(
                query=str(arguments.get("query", "")),
                topic=topic,
                time_range=arguments.get("time_range"),
                max_results=int(arguments.get("max_results", 5)),
                force_refresh=budget.force_refresh,
            )
        except Exception as exc:
            budget.search_errors.append(str(exc))
            return {"status": "degraded", "error": str(exc)}
        budget.search_errors.extend(resp.errors)
        for r in resp.results or []:
            url = str(r.get("url", "")) if isinstance(r, dict) else ""
            host = str(r.get("source_domain", "")) if isinstance(r, dict) else ""
            if url:
                budget.seen_search_urls.add(url)
            if host:
                budget.seen_search_hosts.add(host)
        return {
            "status": resp.status,
            "query": resp.query,
            "cache_hit": resp.cache_hit,
            "results": resp.results,
            "errors": resp.errors,
            "usage": resp.usage,
        }

    if tool_name == "fetch_web_page":
        url = str(arguments.get("url", ""))
        if not _is_domain_allowed(url):
            return {
                "status": "blocked",
                "url": url,
                "error": "domain not in fetch allow-list",
            }
        if cfg.get("fetch_requires_low_confidence", False):
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            if (
                url not in budget.seen_search_urls
                and not _host_matches_set(host, budget.seen_search_hosts)
            ):
                return {
                    "status": "blocked",
                    "url": url,
                    "error": (
                        "fetch_requires_low_confidence: URL not present in any "
                        "prior web_search result; run web_search first"
                    ),
                }
        if budget.fetch_count >= budget.max_fetches:
            return {
                "status": "degraded",
                "url": url,
                "error": "fetch budget exhausted",
            }
        budget.fetch_count += 1
        try:
            resp = search_backend.fetch_web_page(
                url,
                max_chars=int(arguments.get("max_chars", 30000)),
                force_refresh=budget.force_refresh,
            )
        except Exception as exc:
            budget.fetch_errors.append(str(exc))
            return {"status": "degraded", "url": url, "error": str(exc)}
        if resp.error:
            budget.fetch_errors.append(resp.error)
        return {
            "status": resp.status,
            "url": resp.url,
            "canonical_url": resp.canonical_url,
            "title": resp.title,
            "content": resp.content,
            "content_type": resp.content_type,
            "extractor": resp.extractor,
            "published_date": resp.published_date,
            "retrieved_at": resp.retrieved_at,
            "content_hash": resp.content_hash,
            "cache_hit": resp.cache_hit,
            "truncated": resp.truncated,
            "fetch_attempted": resp.fetch_attempted,
            "error": resp.error,
        }

    return {"status": "error", "error": f"unknown tool: {tool_name}"}


TOOL_SCHEMAS: list[dict[str, Any]] = [WEB_SEARCH_SCHEMA, FETCH_WEB_PAGE_SCHEMA]
TOOL_NAMES: set[str] = {s["function"]["name"] for s in TOOL_SCHEMAS}
