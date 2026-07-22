import { tool } from "@opencode-ai/plugin"

export default tool({
  description:
    "Hybrid semantic + keyword (BM25) search over the analysis/ Obsidian vault — equity research notes on IDX stocks: company analyses by ticker/quarter, commodities deep-dives, earnings forecasts, geopolitics, and saved articles (English and Indonesian). Returns ranked note excerpts with file path, section breadcrumb, Obsidian URI, and chunk `id`. Use this FIRST whenever a question involves companies, commodities, forecasts, or anything likely covered in the user's notes. To read the full text of a result without reading the entire file, pass the chunk `id` to the notes-fetch tool.",
  args: {
    query: tool.schema
      .string()
      .describe(
        "Search query — natural language (EN or ID), tickers, quarters, numbers. e.g. 'ammonia price sensitivity' or 'ELSA Q1 2026 margin comparison'"
      ),
    limit: tool.schema
      .number()
      .int()
      .min(1)
      .max(20)
      .optional()
      .describe("Max results to return (default 8)"),
    folder: tool.schema
      .string()
      .optional()
      .describe(
        "Restrict to a vault subfolder: articles | commodities | companies | forecast | geopolitics | root"
      ),
    ticker: tool.schema
      .string()
      .optional()
      .describe("Restrict to chunks mentioning an IDX ticker, e.g. ESSA"),
  },
  async execute(args, ctx) {
    const cmd = [
      `${ctx.directory}/.venv/bin/python`,
      "scripts/notes_rag/rag.py",
      "search",
      "--json",
      "-k",
      String(args.limit ?? 8),
    ]
    if (args.folder) cmd.push("--folder", args.folder)
    if (args.ticker) cmd.push("--ticker", args.ticker)
    cmd.push(args.query)

    const proc = Bun.spawn(cmd, {
      cwd: ctx.directory,
      stdout: "pipe",
      stderr: "pipe",
    })
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    if (code !== 0) {
      return `notes-search failed (exit ${code}). The index may be missing — build it with: .venv/bin/python scripts/notes_rag/rag.py index\n${stderr}`
    }
    return stdout.trim() || "No results."
  },
})
