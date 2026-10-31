/**
 * Tenant isolation and canary leak detection. Pure: callers feed it what the
 * live probes returned and the text they collected from logs and traces.
 */
import { canaryTag } from "./report.ts";

export interface Tenant {
  orgId: string;
  workspaceId: string;
}

export const tenantKey = (t: Tenant) => `${t.orgId}/${t.workspaceId}`;

/** One probe a caller made with one tenant's token. */
export interface ProbeObservation {
  /** search | by-id | list | manifest-status */
  surface: string;
  caller: Tenant;
  /** HTTP status or SQLSTATE-ish code the probe got. */
  status: number | string;
  /** Session ids the probe returned (search/list rows, or the by-id subject when it answered 2xx). */
  sessionIds: readonly string[];
  /** Raw response text, scanned for other tenants' canaries. Never stored in the report. */
  body?: string;
}

export interface CrossTenantHit {
  surface: string;
  caller: string;
  owner: string;
  /** session id, or the canary tag that matched */
  subject: string;
}

/**
 * Every returned id must belong to the caller's tenant, and no response body may
 * carry another tenant's canary. Unknown ids are reported too: a probe that
 * returns a session the run never planted is not evidence of isolation.
 */
export function crossTenantHits(
  probes: readonly ProbeObservation[],
  ownerOf: ReadonlyMap<string, Tenant>,
  canariesByTenant: ReadonlyMap<string, readonly string[]>,
): { hits: CrossTenantHit[]; unknownIds: string[] } {
  const hits: CrossTenantHit[] = [];
  const unknownIds = new Set<string>();
  for (const p of probes) {
    const caller = tenantKey(p.caller);
    for (const id of p.sessionIds) {
      const owner = ownerOf.get(id);
      if (!owner) unknownIds.add(id);
      else if (tenantKey(owner) !== caller) hits.push({ surface: p.surface, caller, owner: tenantKey(owner), subject: id });
    }
    if (p.body)
      for (const [owner, canaries] of canariesByTenant) {
        if (owner === caller) continue;
        for (const c of canaries) if (c && p.body.includes(c)) hits.push({ surface: p.surface, caller, owner, subject: canaryTag(c) });
      }
  }
  return { hits, unknownIds: [...unknownIds] };
}

/** A by-id/manifest probe for another tenant's object must be refused, not merely empty. */
export function refusedAsExpected(status: number | string): boolean {
  return status === 401 || status === 403 || status === 404 || status === "42501";
}

export interface LeakHit {
  source: string;
  canary: string;
  /** Character offset of the first hit, so an operator can find it without the report carrying the text. */
  offset: number;
  count: number;
}

/**
 * Scan collected log/trace text for planted canaries. Matching is exact and
 * also tries the JSON-escaped form, because a canary inside a JSON log string
 * may be escaped (quotes, backslashes, newlines).
 */
export function scanForCanaries(sources: ReadonlyMap<string, string>, canaries: readonly string[]): LeakHit[] {
  const hits: LeakHit[] = [];
  for (const [source, text] of sources) {
    for (const c of canaries) {
      if (c.length < 6) throw new RangeError("canaries must be at least 6 characters to avoid accidental matches");
      const forms = new Set([c, JSON.stringify(c).slice(1, -1)]);
      let first = -1;
      let count = 0;
      for (const f of forms) {
        let i = text.indexOf(f);
        if (i >= 0 && (first < 0 || i < first)) first = i;
        while (i >= 0) {
          count += 1;
          i = text.indexOf(f, i + f.length);
        }
      }
      if (count > 0) hits.push({ source, canary: canaryTag(c), offset: first, count });
    }
  }
  return hits;
}

/**
 * short_summary must be scrubbed: no planted secret and no transcript-only
 * canary may appear in it. Returns the tags of what leaked.
 */
export function summaryLeaks(summary: string | null | undefined, forbidden: readonly string[]): string[] {
  if (!summary) return [];
  return forbidden.filter((f) => summary.includes(f)).map(canaryTag);
}
