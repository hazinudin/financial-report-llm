#!/usr/bin/env bash
# Weekly Class B refresh for the four v0 tickers.
#
#   scan IR/news listings -> ingest new PDFs -> extract Q2 2026 -> enrich sheets
#
# Cron (Monday 06:15 local):
#   15 6 * * 1 cd /path/to/financial-report-llm && ./chat-app/cron.sh >> idx_data/chat/cron.log 2>&1
#
# Exits non-zero only if the scan itself fails. Per-ticker capture failures are
# recorded in the manifest as `not_captured` and are expected (JS-only pages).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export PYTHONPATH="chat-app${PYTHONPATH:+:$PYTHONPATH}"
LOG_DIR="idx_data/chat"
mkdir -p "$LOG_DIR"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
say() { printf '[%s] %s\n' "$(stamp)" "$*"; }

PY=(uv run python)

say "cron start"
say "scan: all configured tickers"
if ! "${PY[@]}" -m chat_app.ingest scan --all; then
  say "scan failed; stopping before extraction"
  exit 1
fi

# v0 operational target period. Bump this when a new quarter is captured.
PERIOD="${IDX_PERIOD:-Q2}"
YEAR="${IDX_YEAR:-2026}"

for ticker in ITMG PTBA PTPS ESSA; do
  manifest="$ROOT/idx_data/operational/$ticker/manifest.json"
  if [[ ! -f "$manifest" ]]; then
    say "$ticker: no manifest; skipping"
    continue
  fi
  captured=$("${PY[@]}" - "$manifest" <<'PY'
import json, sys
try:
    data = json.load(open(sys.argv[1]))
except Exception:
    print(0); raise SystemExit
print(sum(1 for d in data.get("documents", []) if d.get("status") == "captured"))
PY
)
  if [[ "${captured:-0}" -eq 0 ]]; then
    say "$ticker: no captured document; state stays not_captured"
    continue
  fi
  say "$ticker: extracting $PERIOD $YEAR"
  if ! "${PY[@]}" -m chat_app.extract_operational \
      --ticker "$ticker" --period "$PERIOD" --year "$YEAR"; then
    say "$ticker: extraction failed (artifact left untouched)"
  fi
done

say "enrich: additive pass over all fact sheets"
"${PY[@]}" -m chat_app.enrich --all || say "enrich failed"

say "cron done"
