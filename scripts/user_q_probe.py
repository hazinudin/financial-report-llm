import json, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT / "scripts" / "analysis_pipeline"))
import pi_agent  # noqa

SYSTEM = """You are a financial research assistant for Indonesian (IDX) listed companies.
Answer ONLY from tool results. Cite the table and row/note for every number.
If the data is not available, say so plainly and say where it would live.
When asked why something changed, separate what the filings SHOW (facts, arithmetic)
from what might EXPLAIN it (hypotheses, research), and label each. Never assert a cause
as fact. Be concise (max 200 words)."""

QS = [
 ("U1", "How much coal was produced by PTBA in Q2 2026?"),
 ("U2", "What might cause the significant COGS increase in PTPS Q1 2026?"),
 ("U3", "What might cause the decrease in net profit for PTPS Q1 2026?"),
]
CFG = {"provider":"commandcode","model":"deepseek/deepseek-v4.1-flash","thinking":"off","timeout_sec":300,
       "approve":True,"load_skills":False,"load_context_files":False,"save_session":False,
       "tools":["duckdb_query","notes_search","notes_fetch"]}

def ask(item):
    qid,q = item
    r = pi_agent.run_agent(SYSTEM,q,cfg=CFG,run_dir=PROJECT/"idx_data"/"user_q_probe"/qid)
    return {"id":qid,"q":q,"answer":(r.get("content") or "").strip(),
            "tools":(r.get("pi_meta") or {}).get("tool_calls"),
            "cost":(r.get("usage") or {}).get("cost_usd"),"error":r.get("error"),
            "trace":[{"tool":t.get("tool_name"),"args":t.get("arguments")} for t in (r.get("tool_trace") or [])]}

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as pool:
        out=list(pool.map(ask,QS))
    (PROJECT/"idx_data"/"user_q_probe"/"answers.json").write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding="utf-8")
    for r in out:
        print(f"\n===== {r['id']}  ${r['cost']:.4f}  {r['tools']} =====")
        print(r["q"]); print("-"*60); print(r["answer"][:2200] or f"[EMPTY] {r['error']}")
