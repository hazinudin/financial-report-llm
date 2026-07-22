import { tool } from "@opencode-ai/plugin"

export default tool({
  description:
    "Retrieve the full text of a single chunk from the RAG index by its chunk ID. Use this after notes-search returns results — pass the chunk `id` from a search hit to get the complete section text (~300-2500 chars, typically ~600 tokens) instead of reading the entire markdown file. This is the preferred way to read note content: search first with notes-search, then fetch the most relevant chunks. Avoids loading entire files into context when only one section is needed.",
  args: {
    id: tool.schema
      .string()
      .describe(
        "Chunk ID from a notes-search result (the `id` field in the JSON output)"
      ),
  },
  async execute(args, ctx) {
    const cmd = [
      `${ctx.directory}/.venv/bin/python`,
      "scripts/notes_rag/rag.py",
      "fetch",
      args.id,
      "--json",
    ]

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
      return `notes-fetch failed (exit ${code}). The chunk ID may be invalid — run notes-search first to get valid IDs.\n${stderr}`
    }
    return stdout.trim() || "Chunk not found."
  },
})