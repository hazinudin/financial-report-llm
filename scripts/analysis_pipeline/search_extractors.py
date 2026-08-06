"""
Content extractors for the web-search tool.

Layered strategy (HTTP-first, then optional browser fallback):
  1. `extract_html()`      — lightweight HTML-to-text via stdlib html.parser
  2. `extract_pdf_via_docling()` — Docling-based PDF → Markdown conversion
  3. `extract_browser()`   — Playwright fallback (disabled by default, returns
                              `unsupported` when browser extra is absent)

Phase 1 implements (1) and (2). (3) ships as a disabled interface.
"""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
from typing import Any
import re


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._text: list[str] = []
        self._skip = False
        self._title: str = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style", "noscript", "iframe", "svg"):
            self._skip = True
        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "noscript", "iframe", "svg"):
            self._skip = False
        if tag == "title":
            self._in_title = False
        if tag in ("p", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "div", "tr", "article", "section", "header"):
            self._text.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            if self._in_title:
                self._title += data
            else:
                self._text.append(data)

    def get_text(self) -> str:
        raw = "".join(self._text)
        return _clean_whitespace(raw)

    def get_title(self) -> str:
        return self._title.strip()


def _clean_whitespace(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text


def _is_js_shell(html_raw: str) -> bool:
    """Detect pages that are JavaScript-only shells with no usable content."""
    lower = html_raw.lower()
    js_markers = [
        "enable javascript",
        "please enable javascript",
        "you need to enable javascript",
        "noscript",
    ]
    marker_found = any(m in lower for m in js_markers)
    if not marker_found:
        return False
    stripped = _clean_whitespace(html_raw)
    return len(stripped) < 1000


def extract_html(html_text: str, *, url: str = "", max_chars: int = 30000) -> dict[str, Any]:
    parser = _HTMLTextExtractor()
    try:
        parser.feed(html_text)
    except Exception:
        pass
    text = parser.get_text()
    title = parser.get_title()

    if _is_js_shell(html_text):
        return {
            "content": "",
            "extractor": "httpx",
            "title": title,
            "status": "degraded",
            "error": "JavaScript-shell page, browser extraction disabled",
        }

    return {
        "content": text[:max_chars],
        "extractor": "httpx",
        "title": title,
        "status": "ok",
    }


def extract_pdf_via_docling(pdf_path: str, *, max_chars: int = 30000) -> dict[str, Any]:
    try:
        from docling.document_converter import DocumentConverter
    except ImportError:
        return {
            "content": "",
            "extractor": "docling",
            "title": "",
            "status": "unsupported",
            "error": "docling not installed",
        }

    pdf = Path(pdf_path)
    if not pdf.exists():
        return {
            "content": "",
            "extractor": "docling",
            "title": "",
            "status": "degraded",
            "error": f"PDF file not found: {pdf_path}",
        }
    try:
        converter = DocumentConverter()
        result = converter.convert(str(pdf))
        md = result.document.export_to_markdown()
    except Exception as exc:
        return {
            "content": "",
            "extractor": "docling",
            "title": "",
            "status": "degraded",
            "error": f"Docling conversion failed: {exc}",
        }
    title = pdf.stem
    text = md if isinstance(md, str) else str(md)
    return {
        "content": text[:max_chars],
        "extractor": "docling",
        "title": title,
        "status": "ok" if text.strip() else "degraded",
    }


def extract_browser(url: str, *, max_chars: int = 30000) -> dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {
            "content": "",
            "extractor": "browser",
            "title": "",
            "status": "unsupported",
            "error": "browser extraction disabled (playwright not installed)",
        }

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            try:
                context = browser.new_context(
                    java_script_enabled=True,
                )
                page = context.new_page()
                page.route(
                    "**/*",
                    lambda route: (
                        route.abort()
                        if route.request.resource_type in ("image", "font", "media")
                        else route.continue_()
                    ),
                )
                page.goto(url, timeout=20000, wait_until="domcontentloaded")
                title = page.title()
                text = page.inner_text("body")
                content = _clean_whitespace(text)
                return {
                    "content": content[:max_chars],
                    "extractor": "browser",
                    "title": title,
                    "status": "ok" if content.strip() else "degraded",
                }
            finally:
                browser.close()
    except Exception as exc:
        return {
            "content": "",
            "extractor": "browser",
            "title": "",
            "status": "degraded",
            "error": f"browser extraction failed: {exc}",
        }
