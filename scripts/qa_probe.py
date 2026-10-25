"""Ask the project's own pi agent a graded question set and capture answers.

Uses the exact invocation contract the analysis pipeline uses (pi_agent.run_agent)
with the read-only idx tools, so results reflect what a client-facing Q&A layer
would actually produce.
"""
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT / "scripts" / "analysis_pipeline"))
sys.path.insert(0, str(PROJECT / "scripts" / "report_pipeline"))

import pi_agent  # noqa: E402

SYSTEM = """You are a financial research assistant for Indonesian (IDX/IHSG) listed companies.

You answer ONLY from tool results. Rules:
1. Every number you state must come from a tool result. Name the table/note it came from.
2. If the tools have no data for a company or period, say exactly that. Never estimate
   or use general knowledge as a substitute for a figure.
3. Show arithmetic when you derive a number (e.g. subtracting two periods, computing a ratio).
4. Be concise: at most 150 words, tables allowed.
"""

QUESTIONS = [
    ("A1", "What were PTBA's related-party coal sales in H1 2026 as a share of consolidated revenue?"),
    ("A2", "For ITMG in Q2 2026, what is the 1-30 day overdue trade receivables bucket, in USD?"),
    ("A3", "Which two customers accounted for the largest share of ESSA's sales in H1 2026, and what was each share?"),
    ("B1", "Compare PTBA's operating cash flow and net profit for Q1 2026. Did cash conversion improve or deteriorate, and by how much?"),
    ("B2", "For ITMG H1 2026, is the reported effective tax rate positive or negative, and what is the arithmetic behind it?"),
    ("C1", "Across every company in the database, which had gross margin expand by more than 300bps year-over-year in Q2 2026?"),
    ("C2", "Which companies in the database have total liabilities exceeding total equity?"),
    ("D1", "What was ITMG's single-quarter revenue for Q2 2026 alone, in USD?"),
    ("E1", "What did DKFT report as revenue in Q1 2026?"),
    ("F1", "What was BBCA's net interest income in Q2 2026?"),
]

CFG = {
    "provider": "opencode-go",
    "model": "deepseek-v4.1-flash",
    "thinking": "off",
    "timeout_sec": 240,
    "approve": True,
    "load_skills": False,
    "load_context_files": False,
    "save_session": False,
    "tools": ["duckdb_query", "notes_search", "notes_fetch"],
}


def ask(item):
    qid, question = item
    run_dir = PROJECT / "idx_data" / "qa_probe" / qid
    res = pi_agent.run_agent(SYSTEM, question, cfg=CFG, run_dir=run_dir)
    usage = res.get("usage") or {}
    return {
        "id": qid,
        "q": question,
        "answer": (res.get("content") or "").strip(),
        "tool_calls": (res.get("pi_meta") or {}).get("tool_calls"),
        "duration_s": (res.get("pi_meta") or {}).get("duration_s"),
        "cost_usd": usage.get("cost_usd"),
        "error": res.get("error"),
        "trace": [
            {"tool": t.get("tool_name"), "args": t.get("arguments")}
            for t in (res.get("tool_trace") or [])
        ],
    }


if __name__ == "__main__":
    only = set(sys.argv[1:]) or None
    items = [i for i in QUESTIONS if only is None or i[0] in only]
    with ThreadPoolExecutor(max_workers=4) as pool:
        out = list(pool.map(ask, items))
    (PROJECT / "idx_data" / "qa_probe" / "answers.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    for r in out:
        print(f"\n===== {r['id']} (cost ${r['cost_usd']:.4f}, tools {r['tool_calls']}) =====")
        print(r["q"])
        print("---")
        print(r["answer"][:1200] or f"[EMPTY] error={r['error']}")
