/**
 * exa-budget — deterministic cost clamp for `exa_search`.
 *
 * The research agent chooses its own tool arguments, so "please use cheap
 * settings" in a prompt is advisory. This extension mutates `event.input`
 * in the `tool_call` hook (documented as mutable, applied before execution and
 * before `tool_execution_start`), so the clamp is enforced and visible in the
 * run trace.
 *
 * Levers (see .pi/npm/.../pi-exa-search/extensions/):
 *   searchType               auto -> instant   (avoids deep/deep-reasoning tiers)
 *   numResults               5 -> 3            (fewer billed result contents)
 *   highlightsMaxCharacters  800 -> 200        (highlights bill by content volume)
 *   includeDomains           config.yaml source_classification allowlist
 *
 * Override per-run with env vars:
 *   EXA_BUDGET_OFF=1                    disable entirely (control arm)
 *   EXA_BUDGET_SEARCH_TYPE=auto|instant|fast|neural|deep
 *   EXA_BUDGET_NUM_RESULTS=1..10
 *   EXA_BUDGET_HIGHLIGHTS=200..4000
 *   EXA_BUDGET_DOMAINS=0                disable the domain allowlist
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const SEARCH_TYPE = process.env.EXA_BUDGET_SEARCH_TYPE ?? "instant";
const NUM_RESULTS = Number(process.env.EXA_BUDGET_NUM_RESULTS ?? 3);
const HIGHLIGHTS = Number(process.env.EXA_BUDGET_HIGHLIGHTS ?? 200);
const OFF = process.env.EXA_BUDGET_OFF === "1";
const USE_DOMAINS = process.env.EXA_BUDGET_DOMAINS !== "0";

const DOMAINS_PATH = join(process.cwd(), ".pi", "exa-domains.json");
const AUDIT_PATH = join(process.cwd(), "idx_data", "analysis", "exa_budget.jsonl");

let includeDomains: string[] | undefined;
function loadDomains(): string[] | undefined {
  if (!USE_DOMAINS || !existsSync(DOMAINS_PATH)) return undefined;
  try {
    const parsed = JSON.parse(readFileSync(DOMAINS_PATH, "utf-8")) as {
      includeDomains?: string[];
    };
    const list = (parsed.includeDomains ?? []).filter(
      (d): d is string => typeof d === "string" && d.length > 0,
    );
    return list.length > 0 ? list : undefined;
  } catch {
    return undefined;
  }
}

export default function exaBudget(pi: ExtensionAPI) {
  pi.on("tool_call", async (event) => {
    if (OFF || event.toolName !== "exa_search") return;
    const input = event.input as Record<string, unknown>;

    if (!includeDomains) includeDomains = loadDomains();

    const before = {
      searchType: input.searchType ?? null,
      numResults: input.numResults ?? null,
      highlights: input.highlightsMaxCharacters ?? null,
      includeDomains: Array.isArray(input.includeDomains)
        ? (input.includeDomains as unknown[]).length
        : null,
      domainFilter: Array.isArray(input.domainFilter)
        ? (input.domainFilter as unknown[]).length
        : null,
    };

    input.searchType = SEARCH_TYPE;
    input.numResults = NUM_RESULTS;
    input.highlightsMaxCharacters = HIGHLIGHTS;
    // domainFilter can carry its own includes; drop it and let the allowlist
    // own inclusion. Model-supplied exclusions are preserved.
    delete input.domainFilter;
    if (includeDomains) input.includeDomains = [...includeDomains];

    try {
      appendFileSync(
        AUDIT_PATH,
        JSON.stringify({ ts: new Date().toISOString(), before, after: {
          searchType: input.searchType,
          numResults: input.numResults,
          highlights: input.highlightsMaxCharacters,
          includeDomains: Array.isArray(input.includeDomains)
            ? (input.includeDomains as unknown[]).length
            : null,
        }}) + "\n",
      );
    } catch {
      // audit is best-effort; never block a search because of it
    }
  });
}
