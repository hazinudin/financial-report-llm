"""FastAPI surface for the IDX Research Desk (CONTRACTS.md §6).

Start the desk from the repository root::

    PYTHONPATH=chat-app uv run uvicorn chat_app.web:app --host 127.0.0.1 --port 8765

The server binds loopback only. The browser is a single static page; every
answer is produced by :func:`chat_app.agent.ask`, which is imported lazily so
this module can boot while the agent package is still being edited.

Concurrency: at most two ``pi`` runs execute at once (``Semaphore(2)``,
MVP-PLAN §1). Each request has a hard 180 s budget; a timeout is rendered by the
UI as a first-class error state, never as a blank page.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import threading
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

from . import __version__, analysis_docs, paths

APP_VERSION = __version__
ASK_TIMEOUT_SECONDS = 180.0

app = FastAPI(
    title="IDX Research Desk",
    version=APP_VERSION,
    description="Verified filings chat over the local IDX fact sheets.",
)
app.mount(
    "/static", StaticFiles(directory=str(paths.STATIC_DIR)), name="static"
)

# Two concurrent pi runs at most. The semaphore is held inside the worker
# thread, so a client-side timeout can never leave a permit unreleased.
_ASK_SEMAPHORE = threading.Semaphore(2)
_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost", "testclient"}


@app.middleware("http")
async def _loopback_only(request, call_next):
    """Refuse non-loopback clients even if uvicorn is bound to 0.0.0.0.

    v0 has no auth (MVP-PLAN §1); loopback is the security boundary. Set
    ``IDX_CHAT_ALLOW_REMOTE=1`` only for a deliberate, trusted deployment.
    """
    import os

    if os.environ.get("IDX_CHAT_ALLOW_REMOTE") != "1":
        host = request.client.host if request.client else ""
        if host not in _LOOPBACK_HOSTS:
            return JSONResponse(
                {"status": "error", "error": "loopback only"}, status_code=403
            )
    return await call_next(request)


class AskRequest(BaseModel):
    """Body for ``POST /api/ask`` (CONTRACTS.md §6, MVP-PLAN §4.7)."""

    question: str = Field(..., min_length=1, max_length=2000)
    ticker: str | None = None
    period: str | None = None
    year: int | None = None
    history: list[dict[str, Any]] | None = None
    mode: Literal["desk", "document"] = "desk"
    document_id: str | None = None
    selection: dict[str, Any] | None = None

    @field_validator("question")
    @classmethod
    def _question_must_have_content(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("question must not be empty")
        return text

    @model_validator(mode="after")
    def _scope_is_exclusive(self) -> "AskRequest":
        if self.mode == "document":
            if not str(self.document_id or "").strip():
                raise ValueError("document mode requires document_id")
        else:
            if str(self.document_id or "").strip():
                raise ValueError("desk mode must not carry document_id")
            if self.selection is not None:
                raise ValueError("desk mode must not carry selection")
        return self


class Health(BaseModel):
    ok: bool
    version: str


def _error(message: str) -> JSONResponse:
    """A run error is HTTP 200 so the UI can render it as a designed state."""
    return JSONResponse({"status": "error", "error": message})


def _serialize(payload: Any) -> JSONResponse:
    try:
        return JSONResponse(payload)
    except (TypeError, ValueError) as exc:  # non-JSON payload from the agent
        return _error(f"unserialisable result: {exc}")


def _call_agent(body: AskRequest) -> dict[str, Any]:
    """Run ``agent.ask`` while holding one of the two pi permits."""
    from . import agent  # imported lazily: another module may be mid-edit

    with _ASK_SEMAPHORE:
        return agent.ask(
            body.question,
            ticker=body.ticker or None,
            period=body.period or None,
            year=body.year,
            history=body.history or None,
            mode=body.mode,
            document_id=(body.document_id or "").strip() or None,
            selection=body.selection,
        )


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    index_path = paths.STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="static/index.html is missing")
    return FileResponse(index_path, media_type="text/html")


@app.get("/api/health", response_model=Health)
def health() -> dict[str, Any]:
    return {"ok": True, "version": APP_VERSION}


@app.get("/api/companies")
def companies() -> JSONResponse:
    """The supported set, unwrapped from ``tools.list_companies()``."""
    try:
        from . import tools  # lazy so the app boots during a mid-edit

        result = tools.list_companies()
    except Exception as exc:  # noqa: BLE001 - degraded coverage is a UI state
        return JSONResponse({
            "ok": False,
            "companies": [],
            "as_of": None,
            "error": str(exc) or type(exc).__name__,
        })

    data = result.get("data") or {}
    rows = data.get("companies") or []
    return JSONResponse({
        "ok": True,
        "companies": rows,
        "as_of": data.get("as_of"),
        "count": len(rows),
    })


def _with_document_coverage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach a compact completeness label to document metadata.

    ``completeness`` lives on the fact sheet, not on the analysis note. The
    library should still show whether the note has a supported financial
    period and an operational release, so derive that overlay from the
    authoritative ``list_companies`` result instead of guessing per file.
    """
    try:
        from . import tools  # lazy: the vault must still list if facts fail

        companies = (tools.list_companies().get("data") or {}).get("companies") or []
    except Exception:  # noqa: BLE001 - coverage is optional UI metadata
        return rows

    by_ticker = {
        str(company.get("ticker") or "").upper(): company
        for company in companies
        if isinstance(company, dict) and company.get("ticker")
    }
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        ticker = str(item.get("ticker") or "").upper()
        company = by_ticker.get(ticker)
        if company:
            period = item.get("period")
            year = item.get("year")
            label = f"{period} {year}" if period and year else ""
            operational = company.get("operational_periods") or []
            item["coverage"] = {
                "financials": (
                    company.get("financials") or "unknown"
                    if label and label in (company.get("periods") or [])
                    else ("period not in supported set" if label else "company profile")
                ),
                "notes": company.get("notes") or "unknown",
                "operational": (
                    "captured" if label and label in operational else "not_captured"
                ),
                "analysis": "present",
            }
        else:
            item["coverage"] = {"analysis": "present"}
        out.append(item)
    return out


@app.get("/api/documents")
def documents(
    ticker: str | None = None,
    period: str | None = None,
    year: int | None = None,
    kind: str | None = None,
    q: str | None = None,
) -> JSONResponse:
    """The analysis-note library: metadata only, optionally text-filtered."""
    try:
        rows = analysis_docs.list_documents(
            ticker=ticker or None,
            period=period or None,
            year=year,
            kind=kind or None,
            limit=500,
        )
    except paths.ChatError as exc:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"vault is unreadable: {exc}") from exc

    if q and q.strip():
        try:
            matches = analysis_docs.search_documents(
                q, ticker=ticker or None, limit=200
            )
        except paths.ChatError as exc:
            raise HTTPException(status_code=422, detail=exc.message) from exc
        order: list[str] = []
        for match in matches:
            document_id = str(match.get("document") or "")
            if document_id and document_id not in order:
                order.append(document_id)
        by_id = {row["id"]: row for row in rows}
        rows = [by_id[document_id] for document_id in order if document_id in by_id]

    rows = _with_document_coverage(rows)
    return JSONResponse({
        "ok": True,
        "documents": rows,
        "count": len(rows),
        "as_of": _dt.date.today().isoformat(),
    })


@app.get("/api/documents/{document_id}")
def document(document_id: str) -> JSONResponse:
    """One note: metadata + full markdown + outline. Never truncated."""
    try:
        payload = analysis_docs.get_document(document_id)
    except paths.ArtifactMissing as exc:
        raise HTTPException(status_code=404, detail=exc.message) from exc
    except paths.ChatError as exc:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"document is unreadable: {exc}") from exc

    outline = payload.get("sections") or []
    metadata = _with_document_coverage([analysis_docs.metadata(payload)])[0]
    return JSONResponse({
        "ok": True,
        "document": metadata,
        "content": payload.get("content") or "",
        "outline": outline,
        "line_map": {str(item["line"]): item["id"] for item in outline},
        "frontmatter": payload.get("frontmatter") or {},
    })


@app.post("/api/ask")
async def ask(body: AskRequest) -> JSONResponse:
    """Run one question, capped at 180 s, in a worker thread."""
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(_call_agent, body),
            timeout=ASK_TIMEOUT_SECONDS,
        )
    except (asyncio.TimeoutError, TimeoutError):
        return _error("timeout")
    except Exception as exc:  # noqa: BLE001 - every failure is a UI state
        return _error(str(exc) or type(exc).__name__)

    if not isinstance(result, dict):
        return _error("agent returned a non-object result")
    return _serialize(result)


@app.get("/api/trace/{trace_id}")
def trace(trace_id: str) -> JSONResponse:
    path = paths.trace_path(trace_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="trace not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500, detail=f"trace is unreadable: {exc}"
        ) from exc
    return JSONResponse(payload)
