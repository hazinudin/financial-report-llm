"""WS3.1-3.2 ingest tests: idempotency, not_captured on 403, page markers, sha256."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from chat_app import ingest

FIXTURE_PDF = (
    Path(__file__).resolve().parents[2]
    / "idx_data" / "operational" / "test" / "ITMG_6M26_news_release.pdf"
)
URL = "https://itmg.co.id/files/media/ITMG%206M26%20release.pdf"


def make_pdf(pages: list[str]) -> bytes:
    import fitz

    document = fitz.open()
    for text in pages:
        page = document.new_page()
        page.insert_textbox(fitz.Rect(72, 72, 520, 760), text, fontsize=11)
    data = document.tobytes()
    document.close()
    return data


@pytest.fixture()
def tree(tmp_path, monkeypatch):
    operational = tmp_path / "idx_data" / "operational"
    monkeypatch.setattr(ingest, "OPERATIONAL_DIR", operational)
    monkeypatch.setattr(ingest, "PROJECT_ROOT", tmp_path)
    return operational


def manifest(tree: Path, ticker: str = "ITMG") -> dict:
    return json.loads((tree / ticker / "manifest.json").read_text(encoding="utf-8"))


def test_add_captures_pdf_text_and_manifest(tree, monkeypatch):
    pdf = make_pdf(["PAGE ONE production 9.9 Mt", "PAGE TWO sales 12.3 Mt"])
    calls: list[str] = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        return ingest.FetchResult(200, pdf, "application/pdf", via="test")

    monkeypatch.setattr(ingest, "fetch", fake_fetch)
    outcome = ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1")

    assert outcome["action"] == "created"
    assert outcome["status"] == "captured"
    assert calls == [URL]

    documents = manifest(tree)["documents"]
    assert len(documents) == 1
    record = documents[0]
    assert record["id"] == "doc1"
    assert record["type"] == "press_release"
    assert record["published"] == "2026-08-10"
    assert record["url"] == URL
    assert record["pages"] == 2
    assert record["status"] == "captured"
    assert record["error"] is None
    assert record["sha256"] == hashlib.sha256(pdf).hexdigest()
    assert record["path"].endswith("ITMG/doc1.pdf")

    pdf_path = tree / "ITMG" / "doc1.pdf"
    text_path = tree / "ITMG" / "doc1.txt"
    assert pdf_path.read_bytes() == pdf
    text = text_path.read_text(encoding="utf-8")
    assert "=== PAGE 1 ===" in text and "=== PAGE 2 ===" in text
    assert text.index("=== PAGE 1 ===") < text.index("PAGE ONE") < text.index("=== PAGE 2 ===")
    assert ingest.parse_pages_text(text) == ["PAGE ONE production 9.9 Mt",
                                             "PAGE TWO sales 12.3 Mt"]


def test_readd_same_url_is_noop_and_does_not_refetch(tree, monkeypatch):
    pdf = make_pdf(["one", "two"])
    monkeypatch.setattr(ingest, "fetch",
                        lambda url, **kwargs: ingest.FetchResult(200, pdf, "application/pdf"))
    first = ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1")
    assert first["action"] == "created"

    def explode(url, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("re-adding the same URL must not fetch again")

    monkeypatch.setattr(ingest, "fetch", explode)
    second = ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1")
    assert second["action"] == "noop"
    assert second["status"] == "captured"
    assert len(manifest(tree)["documents"]) == 1


def test_same_bytes_under_another_url_is_noop(tree, monkeypatch):
    pdf = make_pdf(["one"])
    monkeypatch.setattr(ingest, "fetch",
                        lambda url, **kwargs: ingest.FetchResult(200, pdf, "application/pdf"))
    ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1")
    outcome = ingest.add_document("ITMG", URL + "?mirror", "press_release", "2026-08-10",
                                  doc_id="doc2")
    assert outcome["action"] == "noop"
    assert outcome["id"] == "doc1"
    assert len(manifest(tree)["documents"]) == 1


def test_403_is_recorded_as_not_captured_and_command_exits_zero(tree, monkeypatch):
    monkeypatch.setattr(
        ingest, "fetch",
        lambda url, **kwargs: ingest.FetchResult(403, b"forbidden", "text/html",
                                                 error="HTTP 403", via="test"),
    )
    code = ingest.main([
        "add", "--ticker", "PTBA", "--url", URL, "--type", "press_release",
        "--published", "2026-08-10", "--doc-id", "ptba_403",
    ])
    assert code == 0  # a failed fetch is data, not a crash

    record = manifest(tree, "PTBA")["documents"][0]
    assert record["status"] == "not_captured"
    assert record["error"] == "HTTP 403"
    assert record["pages"] is None
    assert record["sha256"] is None
    assert not (tree / "PTBA" / "ptba_403.pdf").exists()


def test_failed_refetch_does_not_downgrade_a_captured_vintage(tree, monkeypatch):
    pdf = make_pdf(["one"])
    monkeypatch.setattr(ingest, "fetch",
                        lambda url, **kwargs: ingest.FetchResult(200, pdf, "application/pdf"))
    ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1")
    monkeypatch.setattr(
        ingest, "fetch",
        lambda url, **kwargs: ingest.FetchResult(403, b"", "text/html", error="HTTP 403"),
    )
    outcome = ingest.add_document("ITMG", URL, "press_release", "2026-08-10", doc_id="doc1",
                                  force=True)
    assert outcome["action"] == "noop"
    assert manifest(tree)["documents"][0]["status"] == "captured"


def test_page_markers_preserve_original_page_numbers(tree):
    pages = [f"page {index} body" for index in range(1, 6)]
    text = ingest.render_pages_text(pages)
    for index in range(1, 6):
        assert f"\n\n=== PAGE {index} ===\n\n" in "\n\n" + text
    parsed = ingest.parse_pages_text(text)
    assert parsed == pages
    # The separator is exactly the documented marker, not a re-numbering.
    assert text.startswith("=== PAGE 1 ===")
    assert "=== PAGE 6 ===" not in text


def test_sha256_matches_saved_bytes(tree, monkeypatch):
    pdf = FIXTURE_PDF.read_bytes()
    monkeypatch.setattr(ingest, "fetch",
                        lambda url, **kwargs: ingest.FetchResult(200, pdf, "application/pdf"))
    outcome = ingest.add_document("ITMG", URL, "press_release", "2026-08-10",
                                  doc_id="itmg_fixture")
    assert outcome["pages"] == 8
    saved = (tree / "ITMG" / "itmg_fixture.pdf").read_bytes()
    assert hashlib.sha256(saved).hexdigest() == outcome["sha256"]
    assert outcome["sha256"] == hashlib.sha256(pdf).hexdigest()


def test_non_pdf_payload_is_rejected(tree, monkeypatch):
    monkeypatch.setattr(
        ingest, "fetch",
        lambda url, **kwargs: ingest.FetchResult(200, b"<html>not a pdf</html>",
                                                 "text/html"),
    )
    outcome = ingest.add_document("ESSA", URL, "press_release", "2026-08-10", doc_id="essa")
    assert outcome["status"] == "rejected"
    assert "not a PDF" in outcome["error"]
    assert manifest(tree, "ESSA")["documents"][0]["status"] == "rejected"


def test_discover_pdf_links_handles_anchors_and_js_blobs():
    html = """
    <html><body>
      <a href="/files/media/News Release 6M26.pdf">News Release 6M26</a>
      <a href="/files/media/annual-report-2025.pdf">Annual Report</a>
      <script>var next = "/files/media/Press Release Q3 2026.pdf";</script>
    </body></html>
    """
    links = ingest.discover_pdf_links(html, "https://itmg.co.id/en/news")
    urls = [url for url, _ in links]
    assert "https://itmg.co.id/files/media/News%20Release%206M26.pdf" in urls
    assert any("Press%20Release%20Q3%202026.pdf" in url for url in urls)


def test_scan_js_only_listing_reports_zero_discovered(tree, monkeypatch):
    def fake_fetch(url, **kwargs):
        if url.endswith("/news"):
            return ingest.FetchResult(200, b"<html><body><div id='app'></div></body></html>",
                                      "text/html")
        raise AssertionError(f"unexpected fetch {url}")

    monkeypatch.setattr(ingest, "fetch", fake_fetch)
    report = ingest.scan_ticker(
        "PTBA",
        cfg={"targets": {"PTBA": {"listing_urls": ["https://www.ptba.co.id/en/news"]}},
             "timeout_sec": 1, "impersonate": "chrome131", "user_agent": "test"},
    )
    assert report.discovered == 0
    assert report.attempted == 0
    assert report.success_rate is None
    scans = manifest(tree, "PTBA")["scans"]
    assert scans and scans[-1]["http_status"] == 200
    assert "no PDF links" in scans[-1]["error"]


def test_scan_listing_failure_is_recorded(tree, monkeypatch):
    monkeypatch.setattr(
        ingest, "fetch",
        lambda url, **kwargs: ingest.FetchResult(404, b"", "text/html", error="HTTP 404"),
    )
    report = ingest.scan_ticker(
        "ESSA",
        cfg={"targets": {"ESSA": {"listing_urls": ["https://www.essa.co.id/en/news"]}},
             "timeout_sec": 1, "impersonate": "chrome131", "user_agent": "test"},
    )
    assert report.discovered == 0
    scans = manifest(tree, "ESSA")["scans"]
    assert scans[-1]["http_status"] == 404
    assert scans[-1]["error"] == "HTTP 404"


def test_scan_downloads_a_discovered_release(tree, monkeypatch):
    pdf = make_pdf(["Q3 2026 press release"])

    def fake_fetch(url, **kwargs):
        if url.endswith("/news"):
            return ingest.FetchResult(
                200,
                b'<html><body><a href="/files/Press Release Q3 2026.pdf">'
                b"Press Release Q3 2026</a></body></html>",
                "text/html",
            )
        if url.endswith(".pdf"):
            return ingest.FetchResult(200, pdf, "application/pdf")
        raise AssertionError(url)

    monkeypatch.setattr(ingest, "fetch", fake_fetch)
    report = ingest.scan_ticker(
        "PTPS",
        cfg={"targets": {"PTPS": {"listing_urls": ["https://www.ptps.co.id/en/news"]}},
             "timeout_sec": 1, "impersonate": "chrome131", "user_agent": "test"},
    )
    assert report.discovered == 1
    assert report.attempted == 1
    assert report.captured == 1
    assert report.success_rate == 1.0
    documents = manifest(tree, "PTPS")["documents"]
    assert documents[0]["status"] == "captured"
    assert documents[0]["type"] == "press_release"
