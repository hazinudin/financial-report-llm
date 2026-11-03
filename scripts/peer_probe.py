"""Peer-comparison probe: cross-company questions over the palm-oil group."""
import json, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT / "scripts" / "analysis_pipeline"))
import pi_agent  # noqa

SYSTEM = """You are an equity research analyst covering Indonesian listed companies.
Answer ONLY from tool results. Every number must be traceable to a table and row.
Normalise units and currency explicitly and state what you did. If a company lacks
data, say so and exclude it rather than estimating. Max 250 words; tables preferred.
"""

GROUP = "AALI, BWPT, CSRA, DSNG, JARR, LSIP, OILS, PGUN, SGRO, SIMP, SSMS, TAPG"
QS = [
 ("P1", f"Rank these palm-oil companies by Q1 2026 gross margin, highest to lowest: {GROUP}. State each company's reporting unit and prove the margins are comparable."),
 ("P2", f"For {GROUP}: which had the highest year-over-year revenue growth in Q1 2026 vs Q1 2025? Give growth % and raw revenue."),
 ("P3", "Compare ANJT and AALI. What was each company's revenue in its latest reported period, expressed in IDR billions?"),
 ("P4", f"Which of {GROUP} had negative operating cash flow in Q1 2026?"),
 ("P5", f"Which of {GROUP} have net debt (loans minus cash) above 50% of equity in Q1 2026?"),
]
CFG = {"provider":"commandcode","model":"deepseek/deepseek-v4.1-flash","thinking":"off","timeout_sec":300,
       "approve":True,"load_skills":False,"load_context_files":False,"save_session":False,
       "tools":["duckdb_query","notes_search"]}

def ask(item):
    qid,q = item
    res = pi_agent.run_agent(SYSTEM,q,cfg=CFG,run_dir=PROJECT/"idx_data"/"peer_probe"/qid)
    return {"id":qid,"q":q,"answer":(res.get("content") or "").strip(),
            "tools":(res.get("pi_meta") or {}).get("tool_calls"),
            "cost":(res.get("usage") or {}).get("cost_usd"),"error":res.get("error"),
            "trace":[{"tool":t.get("tool_name"),"args":t.get("arguments")} for t in (res.get("tool_trace") or [])]}

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as pool:
        out = list(pool.map(ask, QS))
    (PROJECT/"idx_data"/"peer_probe"/"answers.json").write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding="utf-8")
    for r in out:
        print(f"\n===== {r['id']}  cost ${r['cost']:.4f}  tools {r['tools']} =====")
        print(r["answer"][:2600] or f"[EMPTY] {r['error']}")
