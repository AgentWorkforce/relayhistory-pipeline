/**
 * The only form production shape may take: aggregate counts. Strict allowlist, so a
 * string, an id or an unknown key fails rather than passing through.
 */
import type { CorpusParams } from "../corpus.ts";

const NUMBERS = new Set([
  "sessions", "events", "orgs", "workspaces", "whaleOrgEvents", "eventBytesP50", "eventBytesP95", "eventBytesP99", "eventBytesMax",
  "eventsPerSessionP50", "eventsPerSessionP95", "eventsPerSessionP99", "shareEventsOver4KB", "windowDays", "dailyNewEvents",
]);
const MIN_COUNT = 10;

export function checkShapeFile(shape: unknown): string[] {
  if (!shape || typeof shape !== "object" || Array.isArray(shape)) return ["shape must be a flat object"];
  const issues: string[] = [];
  for (const [k, v] of Object.entries(shape)) {
    if (!NUMBERS.has(k)) issues.push(`unknown key ${k.slice(0, 40)}`);
    else if (typeof v !== "number" || !Number.isFinite(v) || v < 0) issues.push(`${k} must be a non-negative number`);
  }
  const s = shape as Record<string, number>;
  for (const k of ["orgs", "workspaces"]) if (typeof s[k] === "number" && s[k] < MIN_COUNT) issues.push(`${k} under ${MIN_COUNT} could identify a tenant`);
  return issues;
}

/** Sizes a corpus to the file: only event totals are known, so sizes and sessions stay assumed. */
export function applyShape(p: CorpusParams, shape: { events: number; whaleOrgEvents: number }): CorpusParams {
  const whaleOrgShare = shape.whaleOrgEvents / shape.events;
  // org-0 also receives its even share of the non-whale sessions.
  const whaleShare = (whaleOrgShare - 1 / p.orgs) / (1 - 1 / p.orgs);
  return { ...p, sessions: Math.round(shape.events / p.meanEventsPerSession), whaleShare };
}
