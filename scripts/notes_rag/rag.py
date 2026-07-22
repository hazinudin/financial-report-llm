#!/usr/bin/env python3
"""Local RAG over the analysis/ Obsidian vault.

Hybrid retrieval (BM25 + dense vector) across markdown notes:
- Embeddings: Ollama bge-m3 (local, multilingual EN/ID)
- Store: single DuckDB file (chunks, metadata, embeddings, FTS index)
- Fusion: Reciprocal Rank Fusion (RRF)

Usage:
    uv run scripts/notes_rag/rag.py index [--force]
    uv run scripts/notes_rag/rag.py search [-k N] [--folder F] [--ticker T] [--quarter Q] [--json] QUERY
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote

import duckdb
import ollama

ROOT = Path(__file__).resolve().parents[2]
VAULT_DIR = ROOT / "analysis"
DB_PATH = ROOT / "idx_data" / "notes_rag.duckdb"
OBSIDIAN_VAULT = "analysis"

EMBED_MODEL = "bge-m3"
EMBED_DIM = 1024
EMBED_BATCH = 32

MIN_CHARS = 200  # sections shorter than this merge into the previous chunk
MAX_CHARS = 2500  # chunks longer than this split on paragraph boundaries
SNIPPET_CHARS = 300
RRF_K = 60
CANDIDATES = 20  # per-retriever candidate pool for fusion

QUARTER_RE = re.compile(r"\b(Q[1-4])\s*(20\d{2})\b")
FY_RE = re.compile(r"\b(?:Full Year|FY)\s*(20\d{2})\b", re.IGNORECASE)
HEADER_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def vault_files() -> list[Path]:
    return sorted(
        p
        for p in VAULT_DIR.rglob("*.md")
        if ".obsidian" not in p.parts and not p.name.startswith(".")
    )


def detect_tickers(files: list[Path]) -> set[str]:
    """Ticker universe: 4-letter uppercase tokens in filenames + companies/$*.md stems."""
    tickers: set[str] = set()
    for p in files:
        tickers.update(re.findall(r"\b[A-Z]{4}\b", p.stem))
        if p.parent.name == "companies" and p.stem.startswith("$"):
            tickers.add(p.stem[1:].upper())
    return tickers


def split_sections(text: str) -> list[tuple[str, str]]:
    """Split markdown into (breadcrumb, body) sections at header boundaries."""
    sections: list[tuple[str, str]] = []
    stack: list[tuple[int, str]] = []
    crumb = ""
    buf: list[str] = []

    def flush() -> None:
        nonlocal buf
        body = "\n".join(buf).strip()
        if body:
            sections.append((crumb, body))
        buf = []

    for line in text.splitlines():
        m = HEADER_RE.match(line)
        if m:
            flush()
            level, title = len(m.group(1)), m.group(2).strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, title))
            crumb = " > ".join(t for _, t in stack)
        else:
            buf.append(line)
    flush()
    return sections


def chunk_text(text: str) -> list[tuple[str, str]]:
    """Merge tiny sections, split huge ones on paragraph breaks."""
    merged: list[tuple[str, str]] = []
    for crumb, body in split_sections(text):
        if merged and len(body) < MIN_CHARS:
            pc, pb = merged[-1]
            label = f"\n\n### {crumb}\n" if crumb and crumb != pc else "\n\n"
            merged[-1] = (pc, pb + label + body)
        else:
            merged.append((crumb, body))

    chunks: list[tuple[str, str]] = []
    for crumb, body in merged:
        if len(body) <= MAX_CHARS:
            chunks.append((crumb, body))
            continue
        buf = ""
        for para in re.split(r"\n\s*\n", body):
            if buf and len(buf) + len(para) + 2 > MAX_CHARS:
                chunks.append((crumb, buf))
                buf = para
            else:
                buf = f"{buf}\n\n{para}" if buf else para
        if buf:
            chunks.append((crumb, buf))
    return chunks


def period_of(stem: str) -> str | None:
    m = QUARTER_RE.search(stem)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    m = FY_RE.search(stem)
    if m:
        return f"FY {m.group(1)}"
    return None


def connect() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL fts; LOAD fts;")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS chunks (
            id VARCHAR PRIMARY KEY,
            path VARCHAR,
            title VARCHAR,
            folder VARCHAR,
            breadcrumb VARCHAR,
            text VARCHAR,
            tickers VARCHAR,
            period VARCHAR,
            embedding FLOAT[{EMBED_DIM}]
        )
    """)
    con.execute(
        "CREATE TABLE IF NOT EXISTS files (path VARCHAR PRIMARY KEY, sha256 VARCHAR)"
    )
    return con


def embed(texts: list[str]) -> list[list[float]]:
    out: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH):
        resp = ollama.embed(model=EMBED_MODEL, input=texts[i : i + EMBED_BATCH])
        out.extend(getattr(resp, "embeddings", None) or resp["embeddings"])
    return out


def cmd_index(force: bool) -> None:
    con = connect()
    if force:
        con.execute("DELETE FROM chunks")
        con.execute("DELETE FROM files")

    known = dict(con.execute("SELECT path, sha256 FROM files").fetchall())
    files = vault_files()
    tickers = detect_tickers(files)
    ticker_re = (
        re.compile(r"\b(" + "|".join(sorted(tickers)) + r")\b") if tickers else None
    )

    on_disk: set[str] = set()
    changed: list[tuple[Path, str]] = []
    for p in files:
        rel = p.relative_to(VAULT_DIR).as_posix()
        on_disk.add(rel)
        digest = sha256(p.read_text(encoding="utf-8", errors="replace"))
        if force or known.get(rel) != digest:
            changed.append((p, digest))
    removed = [rel for rel in known if rel not in on_disk]

    rows: list[tuple] = []
    for p, _digest in changed:
        rel = p.relative_to(VAULT_DIR).as_posix()
        text = p.read_text(encoding="utf-8", errors="replace")
        title = p.stem
        folder = p.parent.relative_to(VAULT_DIR).as_posix()
        folder = "root" if folder == "." else folder
        period = period_of(title)
        for i, (crumb, body) in enumerate(chunk_text(text)):
            tags = sorted(set(ticker_re.findall(f"{title}\n{body}"))) if ticker_re else []
            cid = hashlib.sha1(f"{rel}::{i}::{crumb}".encode()).hexdigest()[:16]
            rows.append((cid, rel, title, folder, crumb, body, ",".join(tags), period))

    if rows:
        vectors = embed([f"{r[2]}\n{r[4]}\n\n{r[5]}" for r in rows])
        for rel in {r[1] for r in rows}:
            con.execute("DELETE FROM chunks WHERE path = ?", [rel])
        con.executemany(
            "INSERT INTO chunks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(*r, v) for r, v in zip(rows, vectors)],
        )
    for rel in removed:
        con.execute("DELETE FROM chunks WHERE path = ?", [rel])
        con.execute("DELETE FROM files WHERE path = ?", [rel])
    if changed:
        con.executemany(
            "INSERT OR REPLACE INTO files VALUES (?, ?)",
            [(p.relative_to(VAULT_DIR).as_posix(), d) for p, d in changed],
        )

    if rows or removed or force:
        con.execute(
            "PRAGMA create_fts_index('chunks', 'id', 'text', 'title', 'breadcrumb', overwrite=1)"
        )

    n_files = con.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    n_chunks = con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    print(
        f"Indexed {n_files} files / {n_chunks} chunks "
        f"({len(changed)} changed files -> {len(rows)} chunks, {len(removed)} removed)"
    )


def cmd_search(args: argparse.Namespace) -> None:
    con = connect()
    if con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0:
        sys.exit("Index is empty. Run: uv run scripts/notes_rag/rag.py index")

    qvec = embed([args.query])[0]

    conds, params = [], []
    if args.folder:
        conds.append("folder = ?")
        params.append(args.folder)
    if args.ticker:
        conds.append("(',' || tickers || ',') ILIKE ?")
        params.append(f"%,{args.ticker.upper()},%")
    if args.quarter:
        conds.append("period = ?")
        params.append(args.quarter)
    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    vrows = con.execute(
        f"""SELECT id, array_cosine_similarity(embedding, ?::FLOAT[{EMBED_DIM}]) AS score
            FROM chunks {where} ORDER BY score DESC LIMIT {CANDIDATES}""",
        [qvec, *params],
    ).fetchall()

    bquery = args.query.replace("'", "''")
    try:
        brows = con.execute(
            f"""SELECT id, score FROM (
                    SELECT id, fts_main_chunks.match_bm25(id, ?) AS score
                    FROM chunks {where}
                ) WHERE score IS NOT NULL ORDER BY score DESC LIMIT {CANDIDATES}""",
            [bquery, *params],
        ).fetchall()
    except duckdb.Error as e:
        print(f"warning: BM25 unavailable ({e}), using vector only", file=sys.stderr)
        brows = []

    scores: dict[str, float] = {}
    sources: dict[str, set[str]] = {}
    for rank, (cid, _) in enumerate(vrows, 1):
        scores[cid] = scores.get(cid, 0.0) + 1 / (RRF_K + rank)
        sources.setdefault(cid, set()).add("vec")
    for rank, (cid, _) in enumerate(brows, 1):
        scores[cid] = scores.get(cid, 0.0) + 1 / (RRF_K + rank)
        sources.setdefault(cid, set()).add("bm25")

    top = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[: args.limit]
    if not top:
        print("No results.")
        return

    ids = [cid for cid, _ in top]
    meta = {
        r[0]: r
        for r in con.execute(
            f"""SELECT id, path, title, folder, breadcrumb, text, tickers, period
                FROM chunks WHERE id IN ({",".join("?" * len(ids))})""",
            ids,
        ).fetchall()
    }

    results = []
    for rank, (cid, score) in enumerate(top, 1):
        _, path, title, folder, crumb, text, tickers, period = meta[cid]
        snippet = re.sub(r"\s+", " ", text).strip()[:SNIPPET_CHARS]
        results.append(
            {
                "id": cid,
                "rank": rank,
                "rrf": round(score, 4),
                "via": "+".join(sorted(sources[cid])),
                "path": f"analysis/{path}",
                "section": crumb,
                "folder": folder,
                "period": period,
                "tickers": tickers.split(",") if tickers else [],
                "snippet": snippet,
                "obsidian": f"obsidian://open?vault={quote(OBSIDIAN_VAULT)}&file={quote(path[:-3])}",
            }
        )

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return
    for r in results:
        print(f"[{r['rank']}] {r['path']}  (rrf={r['rrf']}, via {r['via']})")
        if r["section"]:
            print(f"    section: {r['section']}")
        bits = [b for b in [r["folder"], r["period"], ",".join(r["tickers"])] if b]
        print(f"    {' | '.join(bits)}")
        print(f"    {r['snippet']}...")
        print(f"    {r['obsidian']}\n")


def cmd_fetch(args: argparse.Namespace) -> None:
    con = connect()
    row = con.execute(
        "SELECT id, path, title, folder, breadcrumb, text, tickers, period "
        "FROM chunks WHERE id = ?",
        [args.chunk_id],
    ).fetchone()
    if not row:
        sys.exit(f"No chunk found with id '{args.chunk_id}'. Run a search first to get valid IDs.")

    cid, path, title, folder, crumb, text, tickers, period = row
    obj = {
        "id": cid,
        "path": f"analysis/{path}",
        "title": title,
        "section": crumb,
        "folder": folder,
        "tickers": tickers.split(",") if tickers else [],
        "period": period,
        "text": text,
        "obsidian": f"obsidian://open?vault={quote(OBSIDIAN_VAULT)}&file={quote(path[:-3])}",
    }
    if args.json:
        print(json.dumps(obj, indent=2, ensure_ascii=False))
        return
    print(f"file:   analysis/{path}")
    if crumb:
        print(f"section: {crumb}")
    bits = [b for b in [folder, period, ",".join(obj["tickers"])] if b]
    if bits:
        print(f"meta:   {' | '.join(bits)}")
    print(f"\n{text}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Hybrid RAG search over the analysis/ vault"
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    ip = sub.add_parser("index", help="(Re)index the vault (incremental by default)")
    ip.add_argument("--force", action="store_true", help="Rebuild everything")

    sp = sub.add_parser("search", help="Hybrid BM25 + vector search")
    sp.add_argument("query")
    sp.add_argument("-k", "--limit", type=int, default=8)
    sp.add_argument(
        "--folder", help="articles | commodities | companies | forecast | geopolitics | root"
    )
    sp.add_argument("--ticker", help="IDX ticker, e.g. ESSA")
    sp.add_argument("--quarter", help='e.g. "Q1 2026" or "FY 2025"')
    sp.add_argument("--json", action="store_true", help="Machine-readable output")

    fp = sub.add_parser("fetch", help="Retrieve full text of a chunk by ID")
    fp.add_argument("chunk_id", help="Chunk ID from a search result")
    fp.add_argument("--json", action="store_true", help="Machine-readable output")

    args = ap.parse_args()

    if args.cmd == "index":
        cmd_index(args.force)
    elif args.cmd == "fetch":
        cmd_fetch(args)
    else:
        cmd_search(args)


if __name__ == "__main__":
    main()
