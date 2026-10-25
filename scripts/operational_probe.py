"""Fetch an issuer news release, extract operational metrics, reconcile to XBRL.

Numeric highlights are extracted deterministically (no LLM). Management's stated
drivers are captured as verbatim quotes with page refs, so an LLM never invents
a cause. Normalises whitespace because PDF text wraps mid-sentence.
"""
import json, re, subprocess
from pathlib import Path
import fitz
from curl_cffi import requests

URL = "https://itmg.co.id/files/media/News%20Release%20on%20Financial%20Results%20and%20Performance%206M26.pdf"
OUT = Path("idx_data/operational/test"); OUT.mkdir(parents=True, exist_ok=True)
TICKER, PERIOD, YEAR = "ITMG", "Q2", "2026"

pdf = OUT / f"{TICKER}_{PERIOD}_{YEAR}_news_release.pdf"
if not pdf.exists():
    r = requests.get(URL, impersonate="chrome131", timeout=60); r.raise_for_status()
    pdf.write_bytes(r.content)

doc = fitz.open(pdf)
pages = [re.sub(r"\s+", " ", p.get_text()) for p in doc]
full = re.sub(r"\s+", " ", " ".join(pages))
cut = full.find("MELAPORKAN HASIL DAN KINERJA")
en = full[:cut] if cut > 0 else full   # bilingual doc: EN half first

def g(pat, grp=1, text=en):
    m = re.search(pat, text)
    return m.group(grp) if m else None

def n(s):
    return float(s.replace(",", "")) if s else None

def where(frag):
    for i, t in enumerate(pages, 1):
        if frag in t: return i
    return None

ops = {
  "production": [
    {"metric": "coal_production", "scope": "Q2-2026", "value": n(g("production at ([\\d.]+) Mt in the second quarter")), "unit": "Mt",
     "page": where("production at 5.3 Mt")},
    {"metric": "coal_production", "scope": "6M-2026", "value": n(g("6M26 production volumes of ([\\d.]+) Mt")), "unit": "Mt",
     "prior": n(g("slightly lower than ([\\d.]+) Mt in 6M25")), "page": where("production volumes of 9.9 Mt")},
  ],
  "sales": [
    {"metric": "coal_sales_volume", "scope": "6M-2026", "value": n(g("total sales volume increased to ([\\d.]+) Mt")), "unit": "Mt",
     "prior": n(g("in 6M26 from ([\\d.]+) Mt in the same period")), "page": where("increased to 12.3 Mt")},
  ],
  "price": [
    {"metric": "asp", "scope": "6M-2026", "value": n(g("from \\$([\\d.]+)/ton in 6M25 to \\$([\\d.]+)/ton in 6M26", 2)), "unit": "USD/t",
     "prior": n(g("from \\$([\\d.]+)/ton in 6M25")), "page": where("/ton in 6M26")},
  ],
  "balance_sheet": [
    {"metric": "total_assets", "value": n(g("total assets stood at \\$([\\d,]+) million")), "unit": "USD m", "page": where("total assets stood at")},
    {"metric": "cash_and_equivalents", "value": n(g("Cash and cash equivalents were recorded at \\$([\\d,]+) million")), "unit": "USD m", "page": where("Cash and cash equivalents were recorded")},
    {"metric": "short_term_deposits", "value": n(g("short-term deposits reached \\$([\\d,]+) million")), "unit": "USD m", "page": where("short-term deposits reached")},
    {"metric": "total_liabilities", "value": n(g("Total liabilities slightly increased to \\$([\\d,]+) million")), "unit": "USD m", "page": where("Total liabilities slightly increased")},
    {"metric": "total_equity", "value": n(g("Total equity also rose slightly to \\$([\\d,]+) million")), "unit": "USD m", "page": where("Total equity also rose")},
  ],
  "other": [
    {"metric": "royalties_to_government", "value": n(g("royalties to the government increased by \\d+% year-on-year to \\$([\\d,]+) million")), "unit": "USD m", "page": where("royalties to the government")},
    {"metric": "treasury_share_purchase", "value": n(g("\\$([\\d,]+) million of treasury share purchase")), "unit": "USD m", "page": where("treasury share purchase")},
  ],
  "management_stated_drivers": [
    {"driver": "higher strip ratio", "affects": "cost_of_revenue",
     "quote": "higher coal mining and transportation costs, resulted from higher strip ratio", "page": where("higher strip ratio")},
    {"driver": "higher fuel costs (oil price)", "affects": "cost_of_revenue",
     "quote": "higher fuel costs in line with rising global oil prices", "page": where("higher fuel costs")},
    {"driver": "higher sales volume", "affects": "selling_expenses + royalties",
     "quote": "Selling expenses rose from $81 million to $88 million", "page": where("Selling expenses rose")},
    {"driver": "lower tax refunds", "affects": "operating_cash_flow",
     "quote": "lower tax refund receipts of $169 million", "page": where("tax refund receipts")},
    {"driver": "lower dividend payment", "affects": "financing_cash_flow",
     "quote": "lower dividend payment offset by treasury shares purchase", "page": where("lower dividend payment")},
  ],
}

def xbrl(pat, sheet="1311000"):
    q = (f"SELECT value_current FROM xlsx_statements WHERE ticker='{TICKER}' AND year={YEAR} "
         f"AND period='tw2' AND sheet_code='{sheet}' AND label_en ILIKE '{pat}' "
         f"AND value_current IS NOT NULL LIMIT 1;")
    o = subprocess.run(["duckdb","-readonly","idx_data/pipeline.duckdb","-csv","-noheader","-c",q],
                       capture_output=True, text=True).stdout.strip().splitlines()
    return float(o[0])/1000 if o else None   # USD thousand -> USD m

def by(metric, scope=None):
    for e in ops["production"] + ops["sales"] + ops["price"] + ops["balance_sheet"] + ops["other"]:
        if e["metric"] == metric and (scope is None or e.get("scope") == scope): return e["value"]
    return None

recon = {
  "revenue":        {"release": n(g("revenue of \\$([\\d,]+) million")), "xbrl": xbrl("sales and revenue")},
  "net_profit":     {"release": n(g("net profit of \\$([\\d,]+) million")), "xbrl": xbrl("total profit (loss)")},
  "operating_cf":   {"release": n(g("net cash inflow from operating activities of \\$([\\d,]+) million")),
                     "xbrl": xbrl("total net cash flows received from (used in) operating%", "1510000")},
  "total_equity":   {"release": by("total_equity"), "xbrl": xbrl("total equity", "1210000")},
  "total_assets":   {"release": by("total_assets"), "xbrl": xbrl("total assets", "1210000")},
}

prod6, sales6, asp = by("coal_production","6M-2026"), by("coal_sales_volume"), by("asp")
doc_out = {
  "ticker": TICKER, "period": PERIOD, "year": YEAR,
  "document": {"type": "press_release", "published": "2026-08-10", "url": URL,
               "pages": doc.page_count, "bytes": pdf.stat().st_size,
               "language": "en+id", "fetched_with": "curl_cffi impersonate=chrome131",
               "status": "captured"},
  "operational": ops,
  "reconciliation": recon,
  "derived": {
    "inventory_drawdown_mt": round(sales6 - prod6, 2) if (sales6 and prod6) else None,
    "implied_revenue_usd_m": round(sales6 * asp, 1) if (sales6 and asp) else None,
    "production_yoy_pct": round(100*(prod6/10.4 - 1), 1) if prod6 else None,
    "sales_yoy_pct": round(100*(sales6/11.7 - 1), 1) if sales6 else None,
    "asp_yoy_pct": round(100*(asp/78 - 1), 1) if asp else None,
  },
}
(OUT/f"{TICKER}_{PERIOD}_{YEAR}_operational.json").write_text(json.dumps(doc_out, indent=2), encoding="utf-8")

print(f"pages={doc.page_count}  english_chars={len(en)}")
for grp in ("production","sales","price","balance_sheet","other"):
    for e in ops[grp]:
        print(f"  {grp:14} {e['metric']:26} {e.get('scope',''):9} {str(e['value']):>8} {e['unit']:6} prior={e.get('prior')}  p{e['page']}")
for d in ops["management_stated_drivers"]:
    print(f"  driver         {d['driver']:<30} -> {d['affects']:<28} p{d['page']}")
print("\nRECONCILIATION  release vs XBRL (USD m)")
ok_all = True
for k, v in recon.items():
    r, x = v["release"], v["xbrl"]
    ok = r is not None and x is not None and abs(r - x) <= max(1.0, x * 0.005)
    ok_all &= ok
    print(f"  {k:14} release={str(r):>9}  xbrl={x if x is None else round(x,1)!s:>9}  [{'OK' if ok else 'CHECK'}]")
print("\nDERIVED:", json.dumps(doc_out["derived"]))
print("\nALL RECONCILED" if ok_all else "\nSOME MISMATCH")
