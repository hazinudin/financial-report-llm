import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";

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

export default function (pi: ExtensionAPI) {
  pi.registerTool(duckdbQuery);
  pi.registerTool(notesSearch);
  pi.registerTool(notesFetch);
}
