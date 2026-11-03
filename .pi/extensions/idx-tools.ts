import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { delimiter, join } from "node:path";

const execFileAsync = promisify(execFile);

const DB = "idx_data/pipeline.duckdb";

const MODE_FLAGS: Record<string, string> = {
  markdown: "-markdown",
  table: "-table",
  json: "-json",
  csv: "-csv",
  box: "-box",
  column: "-column",
  line: "-line",
};

function venvPython(ctx: ExtensionContext): string {
  return join(ctx.cwd, ".venv", "bin", "python");
}

function errorText(prefix: string, err: any): string {
  const detail = String(err?.stderr || err?.stdout || err?.message || err).trim();
  return `${prefix}\n${detail}`;
}

const duckdbQuery = defineTool({
  name: "duckdb_query",
  label: "DuckDB Query",
  description:
    "Run a read-only SQL query against the IDX pipeline database (idx_data/pipeline.duckdb). " +
    "Use for Indonesian stock financials, ratios, valuation multiples, peer comparisons, and " +
    "XBRL statement line items. Key tables: financial_ratios (code, name, sector, period, assets, " +
    "liabilities, equity, sales, ebt, profit, eps, book_value, per, pbv, der, roa, roe, npm; values in " +
    "billions of IDR except eps; period='YYYY-MM-DD'), xlsx_statements (ticker, year, period, sheet_code, " +
    "row_number, label_id, label_en, value_current, value_prior; raw IDR; period lowercase audit|tw1|tw2|tw3), " +
    "company_profiles, pipeline_jobs, downloaded_files, xlsx_meta, converted_documents. " +
    "The database is opened read-only, so writes will fail by design.",
  parameters: Type.Object({
    sql: Type.String({
      description: "A single read-only SQL statement, e.g. SELECT code, roe FROM financial_ratios WHERE code='ADRO' ORDER BY period DESC LIMIT 5;",
    }),
    mode: Type.Optional(
      Type.Union(
        [
          Type.Literal("markdown"),
          Type.Literal("table"),
          Type.Literal("json"),
          Type.Literal("csv"),
          Type.Literal("box"),
          Type.Literal("column"),
          Type.Literal("line"),
        ],
        { description: "Output format (default: markdown)" },
      ),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const flag = MODE_FLAGS[params.mode ?? "markdown"] ?? "-markdown";
    try {
      const { stdout } = await execFileAsync("duckdb", ["-readonly", flag, DB, params.sql], {
        cwd: ctx.cwd,
        maxBuffer: 16 * 1024 * 1024,
        timeout: 120_000,
      });
      return {
        content: [{ type: "text", text: stdout.trim() || "(no rows)" }],
        details: { mode: params.mode ?? "markdown" },
      };
    } catch (err: any) {
      return {
        content: [{ type: "text", text: errorText("duckdb_query failed:", err) }],
        details: { error: true },
        isError: true,
      };
    }
  },
});

const notesSearch = defineTool({
  name: "notes_search",
  label: "Notes Search",
  description:
    "Hybrid semantic + keyword (BM25) search over the analysis/ Obsidian vault — equity research notes on " +
    "IDX stocks (analyses by ticker/quarter, commodity deep-dives, earnings forecasts, geopolitics, saved " +
    "articles; English and Indonesian). Returns ranked excerpts with file path, section breadcrumb, an " +
    "obsidian:// URI, and a chunk id. Use it FIRST for any question about companies, commodities, forecasts, " +
    "or prior research before falling back to general knowledge. Read a full result with notes_fetch.",
  parameters: Type.Object({
    query: Type.String({
      description: "Natural-language query (EN or ID), tickers, quarters, or numbers, e.g. 'ammonia price sensitivity' or 'ELSA Q1 2026 margin'",
    }),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 20, description: "Max results (default 8)" })),
    folder: Type.Optional(
      Type.Union(
        [
          Type.Literal("articles"),
          Type.Literal("commodities"),
          Type.Literal("companies"),
          Type.Literal("forecast"),
          Type.Literal("geopolitics"),
          Type.Literal("root"),
        ],
        { description: "Restrict to a vault subfolder" },
      ),
    ),
    ticker: Type.Optional(Type.String({ description: "Restrict to chunks mentioning an IDX ticker, e.g. ESSA" })),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const blocked = budgetGuard("notes_search");
    if (blocked) return blocked;
    const args = ["scripts/notes_rag/rag.py", "search", "--json", "-k", String(params.limit ?? 8)];
    if (params.folder) args.push("--folder", params.folder);
    if (params.ticker) args.push("--ticker", params.ticker);
    args.push(params.query);
    try {
      const { stdout } = await execFileAsync(venvPython(ctx), args, {
        cwd: ctx.cwd,
        maxBuffer: 16 * 1024 * 1024,
        timeout: 120_000,
      });
      return {
        content: [{ type: "text", text: stdout.trim() || "No results." }],
        details: { query: params.query },
      };
    } catch (err: any) {
      return {
        content: [
          {
            type: "text",
            text: errorText(
              "notes_search failed. The index may be missing — build it with: .venv/bin/python scripts/notes_rag/rag.py index",
              err,
            ),
          },
        ],
        details: { error: true },
        isError: true,
      };
    }
  },
});

const notesFetch = defineTool({
  name: "notes_fetch",
  label: "Notes Fetch",
  description:
    "Fetch the full text of a single vault chunk by id (from a notes_search result). Prefer this over reading " +
    "the whole markdown file when you only need one section.",
  parameters: Type.Object({
    id: Type.String({ description: "Chunk id from a notes_search hit (the JSON `id` field)" }),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const blocked = budgetGuard("notes_fetch");
    if (blocked) return blocked;
    try {
      const { stdout } = await execFileAsync(
        venvPython(ctx),
        ["scripts/notes_rag/rag.py", "fetch", params.id, "--json"],
        { cwd: ctx.cwd, maxBuffer: 16 * 1024 * 1024, timeout: 120_000 },
      );
      return {
        content: [{ type: "text", text: stdout.trim() || "Chunk not found." }],
        details: { id: params.id },
      };
    } catch (err: any) {
      return {
        content: [{ type: "text", text: errorText("notes_fetch failed. Run notes_search first for valid ids.", err) }],
        details: { error: true },
        isError: true,
      };
    }
  },
});

// ---------------------------------------------------------------------------
// Typed chat tools (chat-app/chat_app/cli.py → JSON stdout)
//
// The Python CLI is the only implementation; these wrappers are thin execFile
// shims (same pattern as notes_search). duckdb_query stays registered for the
// analysis agents but is NOT part of the chat allowlist.
// ---------------------------------------------------------------------------

const CHAT_APP_DIR = "chat-app";
const CHAT_TOOL_TIMEOUT_MS = 60_000;
// MVP-PLAN §9: at most 8 typed-tool calls per chat answer. The counter is
// reset on every agent_start (one user prompt), so interactive pi sessions do
// not accumulate a lifetime budget. Override for exploratory manual sessions
// with IDX_CHAT_TOOL_BUDGET=12. The chat app uses the default.
const CHAT_TOOL_BUDGET = (() => {
  const raw = Number(process.env.IDX_CHAT_TOOL_BUDGET);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 8;
})();
let chatMode = false;
let chatToolCalls = 0;

function budgetGuard(toolName: string): ChatToolResult | null {
  if (!chatMode) return null;
  chatToolCalls += 1;
  if (chatToolCalls <= CHAT_TOOL_BUDGET) return null;
  return {
    content: [
      {
        type: "text",
        text:
          `Tool budget exhausted (${CHAT_TOOL_BUDGET} calls). Answer now with the evidence ` +
          `already returned; do not call ${toolName}.`,
      },
    ],
    details: { error: true, budget: true, tool: toolName },
    isError: true,
  };
}

type ChatToolResult = {
  content: Array<{ type: "text"; text: string }>;
  details: Record<string, unknown>;
  isError?: boolean;
};

async function runChatTool(
  ctx: ExtensionContext,
  toolName: string,
  args: string[],
): Promise<ChatToolResult> {
  chatMode = true;
  const blocked = budgetGuard(toolName);
  if (blocked) return blocked;
  const pythonPath = [join(ctx.cwd, CHAT_APP_DIR), process.env.PYTHONPATH]
    .filter((value): value is string => Boolean(value))
    .join(delimiter);
  try {
    const { stdout } = await execFileAsync(
      venvPython(ctx),
      ["-m", "chat_app.cli", toolName, ...args],
      {
        cwd: ctx.cwd,
        env: { ...process.env, PYTHONPATH: pythonPath },
        maxBuffer: 16 * 1024 * 1024,
        timeout: CHAT_TOOL_TIMEOUT_MS,
      },
    );
    return {
      content: [{ type: "text", text: stdout.trim() || "(no output)" }],
      details: { tool: toolName },
    };
  } catch (err: any) {
    return {
      content: [
        {
          type: "text",
          text: errorText(`chat_app.cli ${toolName} failed:`, err),
        },
      ],
      details: { error: true, tool: toolName },
      isError: true,
    };
  }
}

const companyFacts = defineTool({
  name: "company_facts",
  label: "Company Facts",
  description:
    "Compact fact-sheet sections (income statement, balance sheet, cash flow) for one supported IDX ticker/period, " +
    "with provenance (sheet_code:row_number) and completeness. Values in income statement and cash flow are " +
    "CUMULATIVE for the interim period (Q2 = six months); balance-sheet values are point-in-time. Use " +
    "period_derive for a single-quarter value. Every returned number is also in `ledger`.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    period: Type.String({ description: "Quarter: Q1, Q2, Q3, or Q4" }),
    year: Type.Integer({ description: "Fiscal year, e.g. 2026" }),
    sections: Type.Optional(
      Type.Union(
        [Type.Literal("core"), Type.Literal("details"), Type.Literal("all")],
        { description: "Which sections to return (default: core)" },
      ),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = [
      "--ticker", params.ticker,
      "--period", params.period,
      "--year", String(params.year),
    ];
    if (params.sections) args.push("--sections", params.sections);
    return runChatTool(ctx, "company_facts", args);
  },
});

const metricSeries = defineTool({
  name: "metric_series",
  label: "Metric Series",
  description:
    "Values for one canonical statement metric across supported periods, each with period_type " +
    "(cumulative|instant), units, value_idr_bn, sheet:row source, and an operational reconciliation flag. " +
    "Use it for trends; use period_derive when the question needs a single quarter.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    metric: Type.String({
      description: "Canonical metric key or label, e.g. revenue, net_profit, cogs, total_assets, ocf",
    }),
    periods: Type.Optional(
      Type.Array(Type.String(), {
        description: "Periods to include, e.g. ['Q1 2026', 'Q2 2026'] (default: all supported)",
      }),
    ),
    year: Type.Optional(Type.Integer({ description: "Restrict to one fiscal year" })),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = ["--ticker", params.ticker, "--metric", params.metric];
    if (params.periods && params.periods.length > 0) {
      args.push("--periods", params.periods.join(","));
    }
    if (params.year !== undefined) args.push("--year", String(params.year));
    return runChatTool(ctx, "metric_series", args);
  },
});

const periodDerive = defineTool({
  name: "period_derive",
  label: "Period Derive",
  description:
    "Derive ONE quarter from cumulative interim filings (Q2 = tw2 - tw1, Q3 = tw3 - tw2, Q4 = audit - tw3). " +
    "Returns the formula, both operands, the quarter value, and sheet:row sources. Use this whenever the " +
    "question asks for a quarterly (not cumulative) figure.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    period: Type.String({ description: "Quarter to derive: Q1-Q4" }),
    year: Type.Integer({ description: "Fiscal year, e.g. 2026" }),
    metric: Type.String({ description: "Canonical metric key, e.g. revenue or cogs" }),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    return runChatTool(ctx, "period_derive", [
      "--ticker", params.ticker,
      "--period", params.period,
      "--year", String(params.year),
      "--metric", params.metric,
    ]);
  },
});

const documentsStatus = defineTool({
  name: "documents_status",
  label: "Documents Status",
  description:
    "The ONLY tool allowed to answer existence questions. Returns a deterministic state — available, " +
    "not_yet_published, not_disclosed, not_captured, or unsupported — plus evidence (URL, HTTP status, " +
    "page range, captured documents). Never infer absence from a search miss; call this first.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. PTBA" }),
    period: Type.Optional(Type.String({ description: "Quarter: Q1-Q4" })),
    year: Type.Optional(Type.Integer({ description: "Fiscal year, e.g. 2026" })),
    item: Type.Optional(
      Type.String({
        description: "Document type or metric to check, e.g. press_release or coal_production",
      }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = ["--ticker", params.ticker];
    if (params.period) args.push("--period", params.period);
    if (params.year !== undefined) args.push("--year", String(params.year));
    if (params.item) args.push("--item", params.item);
    return runChatTool(ctx, "documents_status", args);
  },
});

const operationalLookup = defineTool({
  name: "operational_lookup",
  label: "Operational Lookup",
  description:
    "Operational entries (production, sales volume, ASP, strip ratio, cash cost, ...) for one ticker/period, " +
    "each with value, unit, page, source document, and verification class; plus management-stated drivers " +
    "(verbatim quote, page, affected line). Also returns `analysis_context`: up to three secondary analyst-note " +
    "snippets covering cost, strip ratio, guidance, and risks that may not be in the release. Cite the note path " +
    "and label forward-looking statements as hypothesis.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    period: Type.String({ description: "Quarter: Q1-Q4" }),
    year: Type.Integer({ description: "Fiscal year, e.g. 2026" }),
    metric: Type.Optional(
      Type.String({ description: "Restrict to one metric, e.g. coal_production" }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = [
      "--ticker", params.ticker,
      "--period", params.period,
      "--year", String(params.year),
    ];
    if (params.metric) args.push("--metric", params.metric);
    return runChatTool(ctx, "operational_lookup", args);
  },
});

const analysisContext = defineTool({
  name: "analysis_context",
  label: "Analysis Context",
  description:
    "Secondary analyst-note snippets for one ticker/period: cash cost, strip ratio, guidance, mine-level detail, " +
    "and risks that may not be in the release. The chat runner prefetches this for focused questions; call it " +
    "when no context was supplied. Cite the note path and label forward-looking statements as hypothesis.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    period: Type.String({ description: "Quarter: Q1-Q4" }),
    year: Type.Integer({ description: "Fiscal year, e.g. 2026" }),
    topic: Type.Optional(
      Type.String({ description: "Optional metric/topic, e.g. cash cost or strip ratio" }),
    ),
    limit: Type.Optional(
      Type.Integer({ minimum: 1, maximum: 5, description: "Max snippets (default 4)" }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = [
      "--ticker", params.ticker,
      "--period", params.period,
      "--year", String(params.year),
    ];
    if (params.topic) args.push("--topic", params.topic);
    if (params.limit !== undefined) args.push("--limit", String(params.limit));
    return runChatTool(ctx, "analysis_context", args);
  },
});

const readDocument = defineTool({
  name: "read_document",
  label: "Read Document",
  description:
    "Verbatim text of one page (max 4000 chars) from a captured operational document, plus its URL and page " +
    "count. Use it to verify a quote or driver page before labelling a claim management_stated.",
  parameters: Type.Object({
    ticker: Type.String({ description: "IDX ticker, e.g. ITMG" }),
    doc_id: Type.String({
      description: "Document id from operational_lookup/documents_status, e.g. itmg_2026-08-10_news_release",
    }),
    page: Type.Integer({ description: "1-based page number" }),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    return runChatTool(ctx, "read_document", [
      "--ticker", params.ticker,
      "--doc-id", params.doc_id,
      "--page", String(params.page),
    ]);
  },
});

const listCompanies = defineTool({
  name: "list_companies",
  label: "List Companies",
  description:
    "Supported IDX tickers/periods plus layer completeness. Use it in desk mode to establish coverage and to " +
    "return an explicit unsupported answer with alternatives.",
  parameters: Type.Object({}),
  async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
    return runChatTool(ctx, "list_companies", []);
  },
});

const listDocuments = defineTool({
  name: "list_documents",
  label: "List Documents",
  description:
    "Metadata for analysis notes in the local vault (no content). Use it to find the document_id before " +
    "analysis_document, or to answer which notes cover a ticker/period.",
  parameters: Type.Object({
    ticker: Type.Optional(Type.String({ description: "IDX ticker, e.g. ITMG" })),
    period: Type.Optional(Type.String({ description: "Quarter: Q1-Q4" })),
    year: Type.Optional(Type.Integer({ description: "Fiscal year, e.g. 2026" })),
    kind: Type.Optional(
      Type.Union(
        [
          Type.Literal("company"),
          Type.Literal("sector"),
          Type.Literal("commodity"),
          Type.Literal("article"),
          Type.Literal("other"),
        ],
        { description: "Document kind" },
      ),
    ),
    limit: Type.Optional(
      Type.Integer({ minimum: 1, maximum: 200, description: "Max notes (default 50)" }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args: string[] = [];
    if (params.ticker) args.push("--ticker", params.ticker);
    if (params.period) args.push("--period", params.period);
    if (params.year !== undefined) args.push("--year", String(params.year));
    if (params.kind) args.push("--kind", params.kind);
    if (params.limit !== undefined) args.push("--limit", String(params.limit));
    return runChatTool(ctx, "list_documents", args);
  },
});

const analysisDocument = defineTool({
  name: "analysis_document",
  label: "Analysis Document",
  description:
    "Read one section (or the outline) of an analysis note, with stable section ids, 1-based line ranges and a " +
    "document ledger for note numbers. The note is secondary evidence: label note-derived claims analysis_note, " +
    "never fact. Use analysis_search scoped to the same document to prove note silence (not_in_document).",
  parameters: Type.Object({
    document_id: Type.String({
      description: "Document id from list_documents, e.g. itmg-q2-2026-analysis",
    }),
    section: Type.Optional(
      Type.String({ description: "Section id or heading, e.g. 1-executive-summary" }),
    ),
    next_line: Type.Optional(
      Type.Integer({ minimum: 1, description: "Continue a long section from this 1-based line" }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = ["--document-id", params.document_id];
    if (params.section) args.push("--section", params.section);
    if (params.next_line !== undefined) args.push("--next-line", String(params.next_line));
    return runChatTool(ctx, "analysis_document", args);
  },
});

const analysisSearch = defineTool({
  name: "analysis_search",
  label: "Analysis Search",
  description:
    "Search analysis-note lines with document/section/line/quote evidence. Scope with document_id to prove " +
    "whether one note discusses a topic; a scoped miss returns state=not_in_document with evidence. Snippets are " +
    "secondary evidence, not XBRL facts.",
  parameters: Type.Object({
    query: Type.String({ description: "Natural-language query, ticker, metric, or phrase" }),
    document_id: Type.Optional(
      Type.String({ description: "Restrict to one document id (required for note-silence evidence)" }),
    ),
    ticker: Type.Optional(Type.String({ description: "Restrict to notes mentioning a ticker" })),
    limit: Type.Optional(
      Type.Integer({ minimum: 1, maximum: 20, description: "Max matches (default 8)" }),
    ),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    const args = ["--query", params.query];
    if (params.document_id) args.push("--document-id", params.document_id);
    if (params.ticker) args.push("--ticker", params.ticker);
    if (params.limit !== undefined) args.push("--limit", String(params.limit));
    return runChatTool(ctx, "analysis_search", args);
  },
});

const compareSupported = defineTool({
  name: "compare_supported",
  label: "Compare Supported",
  description:
    "Bounded cited comparison matrix for 1-5 supported tickers and 1-4 metrics in one period. Returns per-cell " +
    "values/status/evidence, excluded[] reasons and a coverage block. It is NOT a ranking or screen: no score, " +
    "rank, recommendation, or best/top ordering.",
  parameters: Type.Object({
    tickers: Type.Array(Type.String(), {
      minItems: 1,
      maxItems: 5,
      description: "1-5 IDX tickers, e.g. ['ITMG','PTBA','PTPS','ESSA']",
    }),
    metrics: Type.Array(Type.String(), {
      minItems: 1,
      maxItems: 4,
      description: "1-4 canonical metrics, e.g. ['coal_production','revenue']",
    }),
    period: Type.String({ description: "Quarter: Q1-Q4" }),
    year: Type.Integer({ description: "Fiscal year, e.g. 2026" }),
  }),
  async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
    return runChatTool(ctx, "compare_supported", [
      "--tickers", params.tickers.join(","),
      "--metrics", params.metrics.join(","),
      "--period", params.period,
      "--year", String(params.year),
    ]);
  },
});

export default function (pi: ExtensionAPI) {
  // The budget is per user prompt / agent run, not per process. Interactive pi
  // sessions reuse the extension host across prompts, so without this reset the
  // counter from earlier prompts would eventually block every new question.
  pi.on("agent_start", () => {
    chatMode = false;
    chatToolCalls = 0;
  });
  pi.registerTool(duckdbQuery);
  pi.registerTool(notesSearch);
  pi.registerTool(notesFetch);
  pi.registerTool(companyFacts);
  pi.registerTool(metricSeries);
  pi.registerTool(periodDerive);
  pi.registerTool(documentsStatus);
  pi.registerTool(operationalLookup);
  pi.registerTool(analysisContext);
  pi.registerTool(readDocument);
  pi.registerTool(listCompanies);
  pi.registerTool(listDocuments);
  pi.registerTool(analysisDocument);
  pi.registerTool(analysisSearch);
  pi.registerTool(compareSupported);
}
