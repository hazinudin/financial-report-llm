# Notes RAG — Local Hybrid Search over the Analysis Vault

A self-contained Retrieval-Augmented Generation system for the `analysis/` Obsidian vault. Combines **BM25 keyword search** (DuckDB Full-Text Search) with **dense vector embeddings** (Ollama `bge-m3`) and fuses the results using **Reciprocal Rank Fusion (RRF)**. Everything runs locally — no API calls, no cloud services, no data leaves the machine.

Designed for equity research notes on Indonesian Stock Exchange (IDX) companies: company analyses by ticker/quarter, commodity deep-dives, earnings forecasts, geopolitics, and saved articles in English and Indonesian.

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
  - [`index` — Build / Update the Index](#index--build--update-the-index)
  - [`search` — Hybrid Search](#search--hybrid-search)
  - [`fetch` — Retrieve a Chunk by ID](#fetch--retrieve-a-chunk-by-id)
- [How It Works](#how-it-works)
  - [Chunking Strategy](#chunking-strategy)
  - [Metadata Extraction](#metadata-extraction)
  - [Embedding](#embedding)
  - [Storage](#storage)
  - [Hybrid Retrieval & Fusion](#hybrid-retrieval--fusion)
- [OpenCode Integration](#opencode-integration)
- [DuckDB Schema](#duckdb-schema)
- [Configuration](#configuration)
- [Workflow Recipes](#workflow-recipes)
- [Troubleshooting](#troubleshooting)
- [File Map](#file-map)

---

## Architecture

```
analysis/**/*.md  (Obsidian vault)
        │
        ▼  index (SHA256-based incremental)
┌───────────────────────────────────────────────┐
│  Chunker                                       │
│  • Split at markdown headers (##, ###, …)     │
│  • Build breadcrumb: "Note > Section > …"     │
│  • Merge tiny sections (<200 chars)           │
│  • Split huge sections (>2500 chars)          │
│  • Metadata: folder, tickers, period           │
└──────────────────┬────────────────────────────┘
                   ▼  embed via Ollama bge-m3 (local, multilingual)
┌───────────────────────────────────────────────┐
│  idx_data/notes_rag.duckdb  (single file)      │
│                                                │
│  chunks table                                  │
│    id, path, title, folder, breadcrumb,        │
│    text, tickers, period,                      │
│    embedding FLOAT[1024]                        │
│                                                │
│  files table                                   │
│    path, sha256  (for incremental updates)     │
│                                                │
│  FTS index  (built-in BM25 full-text search)   │
└──────────────────┬────────────────────────────┘
                   ▼  hybrid: cosine ⊕ BM25 → RRF fusion
          ┌────────┴─────────┐
          ▼                  ▼
    Human search       OpenCode agent tool
    (CLI output +       (notes-search.ts invokes
     obsidian:// URI    rag.py search --json)
     links)              → inject results into
                          agent context
```

**Why hybrid?** Dense vectors excel at semantic similarity ("which companies benefit from high gas prices?") but miss exact matches for tickers (`ESSA`), quarter labels (`Q1 2026`), and numbers. BM25 nails those but can't match meaning across vocabulary gaps. Fusing both via RRF combines the strengths.

---

## Prerequisites

### 1. Ollama with `bge-m3`

Install [Ollama](https://ollama.com) if not already present, then pull the embedding model:

```bash
ollama pull bge-m3
```

This is a one-time download (~1.2 GB). The model stays loaded in the Ollama server process across CLI invocations — only the first embed call incurs the model-load latency (~2-3s); subsequent calls are instant.

Verify:

```bash
ollama list | grep bge-m3
# bge-m3:latest   1.2 GB   ...
```

### 2. Python Dependencies

The project's `.venv` already includes everything needed:

- `duckdb` — vector storage + BM25 full-text search
- `ollama` — Python client for embedding requests

No additional installs required if you've set up the project venv.

---

## Quick Start

```bash
# 1. Build the index (one-time, then incremental updates)
uv run scripts/notes_rag/rag.py index

# 2. Search
uv run scripts/notes_rag/rag.py search "which companies benefit from high ammonia prices"

# 3. Search with filters
uv run scripts/notes_rag/rag.py search -k 5 --ticker ESSA "gas cost assumption"
uv run scripts/notes_rag/rag.py search --folder forecast "base case revenue"

# 4. Machine-readable output (for tools/scripts)
uv run scripts/notes_rag/rag.py search --json "ammonia price sensitivity"

# 5. Fetch full chunk text by ID (from search results)
uv run scripts/notes_rag/rag.py fetch 4376a15ad509181a
uv run scripts/notes_rag/rag.py fetch 4376a15ad509181a --json
```

After editing notes in Obsidian, re-run `index` — it detects changes via SHA256 and only re-embeds new/modified files.

---

## CLI Reference

### `index` — Build / Update the Index

```bash
uv run scripts/notes_rag/rag.py index [--force]
```

Scans `analysis/**/*.md` (excluding `.obsidian/` and dotfiles), computes SHA256 for each file, and:

- **New/modified files** → chunk → embed → insert into DuckDB
- **Deleted files** → remove their chunks from the index
- **Unchanged files** → skip (identified by hash match)
- Rebuilds the FTS index whenever any change is detected

| Flag | Description |
|------|-------------|
| `--force` | Re-embed everything from scratch (ignore existing hashes) |

**Output:**

```
Indexed 50 files / 828 chunks (3 changed files -> 47 chunks, 0 removed)
```

A no-op run (nothing changed) reports `0 changed files -> 0 chunks, 0 removed` and completes in milliseconds (no embedding calls).

### `search` — Hybrid Search

```bash
uv run scripts/notes_rag/rag.py search [OPTIONS] QUERY
```

Runs the query through both retrievers (vector cosine + BM25), fuses results via RRF, and returns ranked chunks with metadata and `obsidian://` links.

| Option | Description |
|--------|-------------|
| `QUERY` | Search query — natural language (EN or ID), tickers, quarters, numbers |
| `-k, --limit N` | Max results to return (default: 8) |
| `--folder F` | Restrict to a vault subfolder: `articles`, `commodities`, `companies`, `forecast`, `geopolitics`, or `root` (top-level notes) |
| `--ticker T` | Restrict to chunks mentioning an IDX ticker (e.g. `ESSA`, `MEDC`, `ELSA`) |
| `--quarter Q` | Restrict to chunks with period metadata (e.g. `"Q1 2026"`, `"FY 2025"`) |
| `--json` | Machine-readable JSON output (for tools/agents) |

**Human-readable output format:**

```
[1] analysis/ELSA Q1 2026 Analysis.md  (rrf=0.032, via bm25+vec)
    section: PT Elnusa Tbk (IDX: ELSA) — Q1 2026 > 7. Comparison > 7.2 Balance Sheet
    root | Q1 2026 | ELSA
    The balance sheet changes over the quarter reveal a cash accumulation story...
    obsidian://open?vault=analysis&file=ELSA%20Q1%202026%20Analysis
```

Each line shows:
- **Rank + path + RRF score + retrievers** that contributed (`bm25`, `vec`, or `bm25+vec`)
- **Section breadcrumb** (the header path within the note)
- **Metadata bits** (folder | period | tickers)
- **Snippet** (first 300 chars of the chunk, whitespace-collapsed)
- **obsidian:// URI** — paste into a browser or click in a terminal to open the note in Obsidian

**JSON output format** (with `--json`):

```json
[
  {
    "id": "4376a15ad509181a",
    "rank": 1,
    "rrf": 0.032,
    "via": "bm25+vec",
    "path": "analysis/ELSA Q1 2026 Analysis.md",
    "section": "PT Elnusa Tbk (IDX: ELSA) — Q1 2026 > 7. Comparison > 7.2 Balance Sheet",
    "folder": "root",
    "period": "Q1 2026",
    "tickers": ["ELSA"],
    "snippet": "The balance sheet changes over the quarter reveal...",
    "obsidian": "obsidian://open?vault=analysis&file=ELSA%20Q1%202026%20Analysis"
  }
]
```

The `id` field is the chunk ID — pass it to `fetch` to retrieve the full chunk text.

---

### `fetch` — Retrieve a Chunk by ID

```bash
uv run scripts/notes_rag/rag.py fetch <chunk_id> [--json]
```

Returns the full text of a single chunk (typically 200-2500 chars, ~600 tokens) by its ID. Use this after `search` to read a specific section without loading the entire markdown file into context.

| Argument | Description |
|----------|-------------|
| `chunk_id` | Chunk ID from a `search` result (the `id` field in JSON output) |
| `--json` | Machine-readable JSON output (for tools/agents) |

**Human-readable output:**

```
file:   analysis/articles/Rupiah & Ammonia - Meridian Research.md
section: THE DARK HORSE FROM US-IRAN WAR
meta:   articles

Such a supply crunch always causes prices to skyrocket. Urea and Ammonia
(Middle East) price were just $490/t and $480/t each before the war started.
Now they sit at $907/t and $700/t, indicating at least a 50% increase in
price in the span of just two months...
```

**JSON output format** (with `--json`):

```json
{
  "id": "4376a15ad509181a",
  "path": "analysis/articles/Rupiah & Ammonia - Meridian Research.md",
  "title": "Rupiah & Ammonia - Meridian Research",
  "section": "THE DARK HORSE FROM US-IRAN WAR",
  "folder": "articles",
  "tickers": [],
  "period": null,
  "text": "Such a supply crunch always causes prices to skyrocket...",
  "obsidian": "obsidian://open?vault=analysis&file=articles/Rupiah%20%26%20Ammonia%20-%20Meridian%20Research"
}
```

**Why use fetch instead of reading the whole file?** A typical analysis note is 300-500 lines (~5,000-8,000 tokens). A search + fetch retrieves just the relevant ~600-token chunk — a ~10x context reduction. This matters most when an AI agent is working through multiple questions in a session.

**Error handling**: If the chunk ID is invalid (typos, stale IDs after reindexing), the command exits with a clear message:

```
No chunk found with id 'badID123'. Run a search first to get valid IDs.
```

Note: chunk IDs are derived from file path + section breadcrumb, so they are **stable across reindexes** unless the note's filename or header structure changes.

---

## How It Works

### Chunking Strategy

The chunker respects markdown structure rather than blindly splitting at fixed character counts:

1. **Section splitting**: The note is split at every markdown header (`#` through `######`). Each chunk carries a breadcrumb like `"Note Title > 2. Revenue Analysis > 2.1 Revenue Dynamics"`.

2. **Tiny section merging**: Sections shorter than `MIN_CHARS` (default 200) are appended to the previous chunk, with the section header inserted as a label. This prevents fragments like a one-line table header from becoming a standalone chunk.

3. **Huge section splitting**: Sections longer than `MAX_CHARS` (default 2500) are split at paragraph boundaries (`\n\n`), respecting the limit as closely as possible without breaking paragraphs.

This approach preserves the semantic structure of financial analysis notes (thesis sections, financial tables, risk factors, etc.) as coherent units — far better for retrieval than fixed-size token windows that might cut a table in half.

### Metadata Extraction

Three metadata fields are extracted automatically and stored with each chunk:

| Field | Source | Example | Filter Flag |
|-------|--------|---------|-------------|
| **folder** | The subdirectory under `analysis/` (or `"root"` for top-level notes) | `forecast`, `articles`, `geopolitics` | `--folder` |
| **tickers** | 4-letter uppercase tokens found in filenames + the note body, cross-referenced against a ticker universe built from all filenames and `companies/$*.md` stubs | `ESSA`, `MEDC`, `ELSA` | `--ticker` |
| **period** | Quarter or fiscal year parsed from the filename via regex: `Q[1-4] YYYY` or `Full Year YYYY` / `FY YYYY` | `Q1 2026`, `FY 2025` | `--quarter` |

**Ticker detection** scans all vault filenames for `[A-Z]{4}` patterns plus the stems of `companies/$*.md` files (e.g. `$ESSA.md` → `ESSA`). Each chunk's text is then scanned for these tickers, so a forecast note mentioning `ESSA` will be tagged even if `ESSA` isn't in its filename.

### Embedding

- **Model**: `bge-m3` (BAAI's multilingual embedding model, 1024-dim)
- **Runtime**: Ollama (local, no API)
- **Input**: The chunk's text is prefixed with the note title and breadcrumb to give the embedding model structural context: `"{title}\n{breadcrumb}\n\n{chunk_text}"`
- **Batching**: Embeddings are requested in batches of 32 to balance throughput and Ollama request size
- **Multilingual**: `bge-m3` handles English and Indonesian content natively — a query in Indonesian ("produksi emas ARCI") retrieves Indonesian articles and English forecasts alike

### Storage

A single DuckDB file at `idx_data/notes_rag.duckdb` (~6-10 MB for the current vault) holds everything:

- **`chunks`** table: text, metadata, and a `FLOAT[1024]` embedding column
- **`files`** table: path + SHA256 hash (for incremental update detection)
- **FTS index**: DuckDB's built-in full-text search extension provides BM25 scoring

DuckDB supports `array_cosine_similarity(embedding, ?::FLOAT[1024])` natively for vector search, and the FTS extension provides `match_bm25(id, query)` for keyword search. At ~800 chunks, brute-force cosine over all vectors completes in <10ms — no approximate nearest neighbor index (HNSW, IVF) is needed. If the vault grows to tens of thousands of chunks, an ANN index would become worthwhile.

### Hybrid Retrieval & Fusion

Two retrievers run independently, each producing a candidate pool (`CANDIDATES = 20` by default):

1. **Vector retriever**: Cosine similarity between the query embedding and all chunk embeddings, filtered by any metadata constraints (`--folder`, `--ticker`, `--quarter`). Top 20 by score.

2. **BM25 retriever**: DuckDB FTS `match_bm25()` with the query string as-is, same metadata filters. Top 20 by BM25 score. Falls back gracefully to vector-only if FTS is unavailable.

Scores are fused using **Reciprocal Rank Fusion (RRF)** with `K = 60`:

```
fused_score(chunk) = Σ  1 / (K + rank_in_retriever)
```

Each retriever contributes `1/(60+rank)` for every chunk it returns. A chunk returned by both retrievers at rank 1 would score `2/61 ≈ 0.0328`; a chunk returned only by BM25 at rank 1 scores `1/61 ≈ 0.0164`. The `via` field in the output shows which retrievers contributed — `bm25+vec` means both agreed, which is typically a strong signal.

RRF is parameter-free and score-scale-agnostic, making it ideal for fusing retrievers with incomparable score distributions (cosine ∈ [0,1] vs BM25 ∈ [0, ∞)).

---

## OpenCode Integration

Two custom OpenCode tools expose the RAG system to AI agents in this project. After restarting OpenCode, the agent can invoke them like any other tool.

### `notes-search` — `.opencode/tool/notes-search.ts`

Wraps `rag.py search --json`. The tool's `description` field instructs the agent to use it **first** whenever a question involves companies, commodities, forecasts, or anything likely covered in the vault notes.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string (required) | Search query — natural language, tickers, keywords |
| `limit` | int (optional) | Max results (default 8) |
| `folder` | string (optional) | Subfolder filter |
| `ticker` | string (optional) | Ticker filter |

### `notes-fetch` — `.opencode/tool/notes-fetch.ts`

Wraps `rag.py fetch <id> --json`. Used after `notes-search` to retrieve the full text of a specific chunk (typically ~600 tokens) instead of reading the entire markdown file. The tool's `description` instructs the agent: *"search first with notes-search, then fetch the most relevant chunks."*

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string (required) | Chunk ID from a `notes-search` result (the `id` field in JSON output) |

### Agent workflow

```
notes-search("ammonia price sensitivity")
  → ranked results with chunk IDs + snippets
       │
       ▼
notes-fetch("4376a15ad509181a")   ← top result's full text (~600 tokens)
  → complete section text
       │
       ▼
  agent answers the question
```

**Note**: Tools are loaded at OpenCode session start. If you create or modify them mid-session, restart OpenCode for changes to take effect.

---

## DuckDB Schema

### `chunks` table

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR (PK) | SHA1 of `{path}::{chunk_index}::{breadcrumb}` (first 16 hex chars) |
| `path` | VARCHAR | Relative path from `analysis/` (e.g. `"forecast/ESSA_Q2_2026_Earnings_Forecast.md"`) |
| `title` | VARCHAR | Note filename without extension (e.g. `"ESSA_Q2_2026_Earnings_Forecast"`) |
| `folder` | VARCHAR | Subfolder name, or `"root"` for top-level notes |
| `breadcrumb` | VARCHAR | Header path within the note (e.g. `"Note Title > 2. Revenue > 2.1 Dynamics"`) |
| `text` | VARCHAR | The chunk's text content |
| `tickers` | VARCHAR | Comma-separated ticket symbols (e.g. `"ESSA,MEDC"`) |
| `period` | VARCHAR | Quarter or fiscal year (e.g. `"Q1 2026"`, `"FY 2025"`), or NULL |
| `embedding` | FLOAT[1024] | bge-m3 dense embedding vector |

### `files` table

| Column | Type | Description |
|--------|------|-------------|
| `path` | VARCHAR (PK) | Relative path from `analysis/` |
| `sha256` | VARCHAR | SHA256 hash of file content (for incremental updates) |

### FTS index

Created via `PRAGMA create_fts_index('chunks', 'id', 'text', 'title', 'breadcrumb')`. Indexes the `text`, `title`, and `breadcrumb` columns for BM25 scoring. Uses DuckDB's default Porter stemmer, which handles English and basic Indonesian stemming (e.g. `ammonia` matches `amonia`).

### Querying the database directly

```bash
duckdb -readonly idx_data/notes_rag.duckdb

# Count chunks per folder
SELECT folder, COUNT(*) FROM chunks GROUP BY folder ORDER BY count DESC;

# Find all chunks tagged with a ticker
SELECT path, breadcrumb FROM chunks WHERE ',' || tickers || ',' LIKE '%,ESSA,%' LIMIT 10;

# Inspect a specific chunk's embedding magnitude (sanity check)
SELECT path, array_length(embedding) AS dim FROM chunks LIMIT 5;
```

---

## Configuration

All settings are constants at the top of `rag.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `EMBED_MODEL` | `"bge-m3"` | Ollama embedding model name |
| `EMBED_DIM` | `1024` | Embedding dimensionality (must match the model) |
| `EMBED_BATCH` | `32` | Embedding request batch size |
| `MIN_CHARS` | `200` | Sections shorter than this merge into the previous chunk |
| `MAX_CHARS` | `2500` | Chunks longer than this split at paragraph boundaries |
| `SNIPPET_CHARS` | `300` | Snippet length in search output |
| `RRF_K` | `60` | RRF constant (higher = flatter fusion; 60 is the literature default) |
| `CANDIDATES` | `20` | Per-retriever candidate pool size for fusion |

**Paths** (derived from the script location):

| Path | Value |
|------|-------|
| `VAULT_DIR` | `{project_root}/analysis` |
| `DB_PATH` | `{project_root}/idx_data/notes_rag.duckdb` |
| `OBSIDIAN_VAULT` | `"analysis"` (used in `obsidian://` URIs) |

---

## Workflow Recipes

### Adding a new article

```bash
# 1. Drop the markdown file into the vault
cp ~/Downloads/new_research.md analysis/articles/

# 2. Re-index (incremental — only the new file is embedded)
uv run scripts/notes_rag/rag.py index
```

### After editing notes in Obsidian

```bash
uv run scripts/notes_rag/rag.py index
```

The indexer detects content changes via SHA256 and re-embeds only modified files. At ~17 chunks per file with `bge-m3`, re-embedding one note takes ~1-2 seconds.

### Full rebuild (after changing embedding model or chunking params)

```bash
uv run scripts/notes_rag/rag.py index --force
```

### Deleting a note

Just delete the file from `analysis/`, then re-index. The indexer detects files no longer on disk and removes their chunks + file hash entries.

### Searching by company

```bash
uv run scripts/notes_rag/rag.py search --ticker ESSA "dividend policy"
uv run scripts/notes_rag/rag.py search --ticker MEDC "power segment growth"
```

### Searching by quarter

```bash
uv run scripts/notes_rag/rag.py search --quarter "Q1 2026" "margin compression"
uv run scripts/notes_rag/rag.py search --quarter "FY 2025" "profit decline causes"
```

### Searching within a folder

```bash
uv run scripts/notes_rag/rag.py search --folder forecast "base case revenue"
uv run scripts/notes_rag/rag.py search --folder geopolitics "Hormuz closure impact"
uv run scripts/notes_rag/rag.py search --folder articles "ammonia price"
```

### Cross-lingual search (English ↔ Indonesian)

The `bge-m3` model and DuckDB's Porter stemmer handle both languages:

```bash
# English query retrieves Indonesian articles
uv run scripts/notes_rag/rag.py search "gold production forecast 2026"
# → retrieves "Harga Emas Melonjak, ARCI Bidik Produksi Naik 15% pada 2026"

# Indonesian query retrieves English analyses
uv run scripts/notes_rag/rag.py search "produksi emas ARCI tahun 2026"
# → retrieves "ARCI Gold Production Seasonality & 2026 Forecast"
```

### Search then fetch (token-efficient retrieval)

Instead of reading an entire 400-line analysis file, search first, then fetch only the relevant section:

```bash
# 1. Search to find the right section
uv run scripts/notes_rag/rag.py search --json -k 3 "ELSA Q1 2026 balance sheet"
# → returns chunk IDs + snippets

# 2. Fetch the full text of the most relevant chunk
uv run scripts/notes_rag/rag.py fetch 4376a15ad509181a
# → ~600 tokens instead of ~5,000 for the whole file
```

This is the pattern the OpenCode agent tools use — `notes-search` then `notes-fetch` — to minimize context window consumption.

### Using the obsidian:// links

Every search and fetch result includes an `obsidian://open?vault=analysis&file=...` URI. Click it (or paste into a browser) to jump directly to that note in Obsidian. The URI opens the note file; Obsidian's own search can then find the specific section.

---

## Troubleshooting

### "Index is empty"

Run the indexer first:
```bash
uv run scripts/notes_rag/rag.py index
```

### Embedding errors / model not found

Ensure `bge-m3` is pulled and Ollama is running:
```bash
ollama list | grep bge-m3
ollama serve  # if the daemon isn't running
```

### "BM25 unavailable" warning in search

The FTS extension failed to load. This is non-fatal — search falls back to vector-only. To fix:
```bash
duckdb idx_data/notes_rag.duckdb -c "INSTALL fts; LOAD fts;"
```

### Index out of sync with vault

If results seem stale or files appear missing, force a full rebuild:
```bash
uv run scripts/notes_rag/rag.py index --force
```

### Slow embeddings after stopping Ollama

The model stays in GPU memory across CLI invocations as long as the Ollama daemon is running. If you stop Ollama or it gets evicted, the next embed call will reload the model (~2-3s). This is one-time per daemon restart, not per search.

### Changing the embedding model

If you want to use a different Ollama embedding model (e.g. `nomic-embed-text`):
1. `ollama pull nomic-embed-text`
2. Update `EMBED_MODEL` and `EMBED_DIM` in `rag.py`
3. `uv run scripts/notes_rag/rag.py index --force` (must rebuild — dimensions differ)

---

## File Map

| File | Purpose |
|------|---------|
| `scripts/notes_rag/rag.py` | Main CLI — indexer + hybrid search + chunk fetch |
| `scripts/notes_rag/README.md` | This document |
| `.opencode/tool/notes-search.ts` | OpenCode custom tool that wraps `rag.py search --json` |
| `.opencode/tool/notes-fetch.ts` | OpenCode custom tool that wraps `rag.py fetch --json` |
| `idx_data/notes_rag.duckdb` | Generated index (DuckDB file; not committed to git) |

**Dependencies** (already in `pyproject.toml`):

- `duckdb` — storage, vector similarity, FTS
- `ollama` — embedding client

**Runtime**: Ollama daemon with `bge-m3` model pulled.