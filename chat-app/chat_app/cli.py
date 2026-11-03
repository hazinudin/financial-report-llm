"""JSON CLI over the typed tools (CONTRACTS.md §1).

Every subcommand maps 1:1 to a function in :mod:`chat_app.tools` and prints one
JSON object to stdout. Flags are the signature parameters in kebab-case.
Exit codes: ``0`` success, ``2`` typed tool error (``ChatError``), ``1``
unexpected failure. ``--list`` prints the tool registry and schemas.

Usage::

    PYTHONPATH=chat-app uv run python -m chat_app.cli company_facts \
        --ticker ITMG --period Q2 --year 2026
    PYTHONPATH=chat-app uv run python -m chat_app.cli period_derive \
        --ticker ITMG --period Q2 --year 2026 --metric revenue
    PYTHONPATH=chat-app uv run python -m chat_app.cli --list
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from . import paths, tools


def _add_common(sub: argparse.ArgumentParser) -> None:
    sub.add_argument("--ticker", help="IDX ticker, e.g. ITMG")
    sub.add_argument("--period", help="Q1-Q4")
    sub.add_argument("--year", type=int, help="fiscal year")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m chat_app.cli",
        description="Typed tools for the IDX research chat (JSON stdout)",
    )
    parser.add_argument(
        "--list", dest="list_tools", action="store_true",
        help="print tool names and schemas",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser(
        "list_companies", aliases=["list-companies"],
        help="supported tickers/periods",
    )

    facts = sub.add_parser(
        "company_facts", aliases=["company-facts"],
        help="compact fact-sheet sections",
    )
    _add_common(facts)
    facts.add_argument("--sections", default="core", choices=["core", "details", "all"])

    series = sub.add_parser(
        "metric_series", aliases=["metric-series"],
        help="metric values across periods",
    )
    series.add_argument("--ticker", required=True)
    series.add_argument("--metric", required=True)
    series.add_argument(
        "--periods", help="comma-separated periods, e.g. 'Q1 2026,Q2 2026'"
    )
    series.add_argument(
        "--period", action="append", default=[],
        help="repeatable period (alternative to --periods)",
    )
    series.add_argument("--year", type=int)

    derive = sub.add_parser(
        "period_derive", aliases=["period-derive"],
        help="derive a single quarter from cumulative filings",
    )
    _add_common(derive)
    derive.add_argument("--metric", required=True)

    status = sub.add_parser(
        "documents_status", aliases=["documents-status"],
        help="document existence state with evidence",
    )
    _add_common(status)
    status.add_argument("--item", help="document type or metric to check")

    lookup = sub.add_parser(
        "operational_lookup", aliases=["operational-lookup"],
        help="operational entries, drivers, and documents",
    )
    _add_common(lookup)
    lookup.add_argument("--metric")

    context = sub.add_parser(
        "analysis_context", aliases=["analysis-context"],
        help="secondary analyst-note snippets for operational context",
    )
    _add_common(context)
    context.add_argument("--topic")
    context.add_argument("--limit", type=int, default=4)

    document = sub.add_parser(
        "read_document", aliases=["read-document"],
        help="exact page text for a captured document",
    )
    document.add_argument("--ticker", required=True)
    document.add_argument("--doc-id", "--doc_id", dest="doc_id", required=True)
    document.add_argument("--page", type=int, required=True)

    search = sub.add_parser("notes_search", aliases=["notes-search"],
                            help="search the analyst notes vault")
    search.add_argument("--query", required=True)
    search.add_argument("--ticker")
    search.add_argument("--limit", type=int, default=8)

    fetch = sub.add_parser("notes_fetch", aliases=["notes-fetch"],
                           help="fetch one notes chunk")
    fetch.add_argument("--id", "--chunk-id", "--chunk_id", dest="chunk_id",
                       required=True)

    documents = sub.add_parser(
        "list_documents", aliases=["list-documents"],
        help="analysis-note metadata (no content)",
    )
    _add_common(documents)
    documents.add_argument("--kind")
    documents.add_argument("--limit", type=int, default=50)

    reader = sub.add_parser(
        "analysis_document", aliases=["analysis-document"],
        help="read one analysis-note section or its outline",
    )
    reader.add_argument("--document-id", "--document_id", dest="document_id",
                        required=True)
    reader.add_argument("--section")
    reader.add_argument("--next-line", "--next_line", dest="next_line", type=int)

    analysis_search = sub.add_parser(
        "analysis_search", aliases=["analysis-search"],
        help="search analysis-note lines with section/line evidence",
    )
    analysis_search.add_argument("--query", required=True)
    analysis_search.add_argument("--document-id", "--document_id",
                                 dest="document_id")
    analysis_search.add_argument("--ticker")
    analysis_search.add_argument("--limit", type=int, default=8)

    compare = sub.add_parser(
        "compare_supported", aliases=["compare-supported"],
        help="bounded cited comparison matrix (no ranking)",
    )
    compare.add_argument("--tickers", required=True, action="append",
                         help="comma-separated tickers, repeatable, e.g. ITMG,PTBA")
    compare.add_argument("--metrics", required=True, action="append",
                         help="comma-separated metrics, repeatable")
    compare.add_argument("--period", required=True)
    compare.add_argument("--year", type=int, required=True)

    return parser


def _require(value: Any, flag: str) -> Any:
    if value is None:
        raise paths.ToolInputError(f"{flag} is required")
    return value


def _list_arg(values: list[str] | None) -> list[str]:
    out: list[str] = []
    for value in values or []:
        out.extend(part.strip() for part in value.split(",") if part.strip())
    return out


def dispatch(args: argparse.Namespace) -> dict[str, Any]:
    command = (args.command or "").replace("-", "_")
    if command == "list_companies":
        return tools.list_companies()
    if command == "company_facts":
        return tools.company_facts(
            _require(args.ticker, "--ticker"),
            _require(args.period, "--period"),
            _require(args.year, "--year"),
            sections=args.sections,
        )
    if command == "metric_series":
        period_list: list[str] | None = None
        values: list[str] = list(args.period or [])
        if args.periods:
            values += [part.strip() for part in args.periods.split(",") if part.strip()]
        if values:
            period_list = values
        return tools.metric_series(
            _require(args.ticker, "--ticker"),
            _require(args.metric, "--metric"),
            periods=period_list,
            year=args.year,
        )
    if command == "period_derive":
        return tools.period_derive(
            _require(args.ticker, "--ticker"),
            _require(args.period, "--period"),
            _require(args.year, "--year"),
            _require(args.metric, "--metric"),
        )
    if command == "documents_status":
        return tools.documents_status(
            _require(args.ticker, "--ticker"),
            period=args.period,
            year=args.year,
            item=args.item,
        )
    if command == "operational_lookup":
        return tools.operational_lookup(
            _require(args.ticker, "--ticker"),
            _require(args.period, "--period"),
            _require(args.year, "--year"),
            metric=args.metric,
        )
    if command == "analysis_context":
        return tools.analysis_context(
            _require(args.ticker, "--ticker"),
            _require(args.period, "--period"),
            _require(args.year, "--year"),
            topic=args.topic,
            limit=args.limit,
        )
    if command == "read_document":
        return tools.read_document(
            _require(args.ticker, "--ticker"),
            _require(args.doc_id, "--doc-id"),
            _require(args.page, "--page"),
        )
    if command == "notes_search":
        return tools.notes_search(
            _require(args.query, "--query"), ticker=args.ticker, limit=args.limit
        )
    if command == "notes_fetch":
        return tools.notes_fetch(_require(args.chunk_id, "--id"))
    if command == "list_documents":
        return tools.list_documents(
            ticker=args.ticker,
            period=args.period,
            year=args.year,
            kind=args.kind,
            limit=args.limit,
        )
    if command == "analysis_document":
        return tools.analysis_document(
            _require(args.document_id, "--document-id"),
            section=args.section,
            next_line=args.next_line,
        )
    if command == "analysis_search":
        return tools.analysis_search(
            _require(args.query, "--query"),
            document_id=args.document_id,
            ticker=args.ticker,
            limit=args.limit,
        )
    if command == "compare_supported":
        return tools.compare_supported(
            _list_arg(_require(args.tickers, "--tickers")),
            _list_arg(_require(args.metrics, "--metrics")),
            _require(args.period, "--period"),
            _require(args.year, "--year"),
        )
    raise paths.ToolInputError(f"unknown command {command!r}")


def _print(payload: dict[str, Any]) -> None:
    # Tool results already enforce their own caps (12 KB primary / 24 KB
    # document). Serialize above those so truncation never cuts valid JSON.
    print(paths.dump_json(payload, cap=64_000))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list_tools:
        _print(tools.schemas())
        return 0
    if not args.command:
        parser.print_help(sys.stderr)
        return 2

    try:
        _print(dispatch(args))
        return 0
    except paths.ChatError as exc:
        _print(exc.as_dict())
        return 2
    except Exception as exc:  # noqa: BLE001 — CLI reports, never dumps a traceback
        _print({
            "error": "internal",
            "message": f"{type(exc).__name__}: {exc}",
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
