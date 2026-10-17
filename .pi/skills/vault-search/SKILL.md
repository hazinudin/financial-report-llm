---
name: vault-search
description: Search the analysis/ Obsidian vault of IDX equity research (company analyses by ticker/quarter, commodity deep-dives, earnings forecasts, geopolitics, saved articles) with hybrid BM25 + vector search. Use before answering any question about a company, commodity, forecast, or prior research that may already be in the user's notes.
---

# Vault search (`analysis/`)

The vault holds the user's own research. Search it **first** (via the `notes_search` tool)
for company/commodity/forecast questions — then fall back to the database or general
knowledge. Read whole sections with `notes_fetch` instead of loading entire files.

```
notes_search { "query": "which companies benefit from high ammonia prices" }
notes_search { "query": "gas cost assumption", "ticker": "ESSA", "limit": 5 }
notes_search { "query": "base case", "folder": "forecast" }
notes_fetch  { "id": "<chunk id from a search hit>" }
```

Hits include file path, section breadcrumb, an `obsidian://` URI, and a chunk `id`.
Filters: `folder` ∈ `articles | commodities | companies | forecast | geopolitics | root`;
`ticker` = IDX code; `limit` ≤ 20.

## CLI fallback / indexing

```bash
.venv/bin/python scripts/notes_rag/rag.py index            # incremental; re-run after editing notes
.venv/bin/python scripts/notes_rag/rag.py index --force    # full rebuild
.venv/bin/python scripts/notes_rag/rag.py search -k 5 --ticker ESSA "gas cost assumption"
.venv/bin/python scripts/notes_rag/rag.py search --json "ammonia"
.venv/bin/python scripts/notes_rag/rag.py fetch <chunk_id> --json
```

Embeddings use Ollama `bge-m3` (one-time: `ollama pull bge-m3`); the index lives in
`idx_data/notes_rag.duckdb`. If search returns nothing useful, the index may be stale —
re-run `index`.

When you use a note, cite its path (or obsidian link) so the user can verify.
