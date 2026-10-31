/**
 * Pre/post-T0 search across the legacy event pool and the store, modelled on PGlite with
 * today's rule: substring on the first 4,096 characters. Each event appears once, the
 * store's copy winning; a failing pool is named, never hidden.
 */
import type { PGlite } from "@electric-sql/pglite";
import type { Scope, SearchHit, SearchOptions, SessionSearchStore } from "./store.ts";

export const LEGACY_DDL = `CREATE TABLE IF NOT EXISTS legacy_events (
  org_id text, workspace_id text, harness text, session_id text, machine_id text, event_id text PRIMARY KEY, ts timestamptz, content text)`;
export const LEGACY_PREFIX_CHARS = 4096;

export async function legacySearch(db: PGlite, scope: Scope, o: SearchOptions, t0: string): Promise<SearchHit[]> {
  const limit = o.limit ?? 10;
  const like = `%${o.query.replace(/[\\%_]/g, "\\$&")}%`;
  const cursor = o.cursor ? JSON.parse(Buffer.from(o.cursor, "base64url").toString()) as [string, string] : null;
  const { rows } = await db.query<SearchHit & { ts: Date }>(
    `SELECT workspace_id, harness, session_id, machine_id, event_id, ts, left(content, 320) AS snippet FROM legacy_events
     WHERE org_id = $1 AND workspace_id = ANY($2) AND ts < $3 AND left(content, ${LEGACY_PREFIX_CHARS}) ILIKE $4
       ${cursor ? "AND (ts, event_id) < ($5::timestamptz, $6)" : ""}
     ORDER BY ts DESC, event_id DESC LIMIT ${limit + 1}`,
    [scope.orgId, scope.workspaceIds, t0, like, ...(cursor ?? [])]);
  return rows.map((r) => ({ ...r, ts: new Date(r.ts).toISOString(), citation: `/sessions/${r.session_id}?source=${r.harness}` }));
}

export async function federatedSearch(
  pools: { legacy: PGlite; store: SessionSearchStore; t0: string; legacyEnabled: boolean },
  scope: Scope, o: SearchOptions,
): Promise<{ hits: SearchHit[]; nextCursor: string | null; degradedPools: string[] }> {
  const limit = Math.min(o.limit ?? 10, 20);
  const degradedPools: string[] = [];
  const [legacy, store] = await Promise.all([
    pools.legacyEnabled
      ? legacySearch(pools.legacy, scope, { ...o, limit }, pools.t0).catch(() => { degradedPools.push("legacy_events"); return []; })
      : Promise.resolve([]),
    pools.store.search(scope, { ...o, limit: 20 }).then((r) => r.hits).catch(() => { degradedPools.push("transcript_store"); return [] as SearchHit[]; }),
  ]);
  const byId = new Map<string, SearchHit>();
  for (const h of [...legacy, ...store]) byId.set(h.event_id, h);
  const merged = [...byId.values()].sort((a, b) => (a.ts === b.ts ? (a.event_id < b.event_id ? 1 : -1) : a.ts < b.ts ? 1 : -1));
  const page = merged.slice(0, limit);
  const last = page.at(-1);
  return {
    hits: page, degradedPools,
    nextCursor: merged.length > limit && last ? Buffer.from(JSON.stringify([last.ts, last.event_id])).toString("base64url") : null,
  };
}
