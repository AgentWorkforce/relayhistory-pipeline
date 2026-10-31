/**
 * Proof-only SessionSearchStore: the one chokepoint of relayhistory-cloud #233. Scope
 * comes from server auth, never a client body; search text only ever travels as a
 * bound parameter. PROPOSED, not a contract: no deployment or migration exists.
 */
import { assertLocalTarget } from "../guard.ts";

export interface Scope { orgId: string; workspaceIds: string[] }
export interface SearchRecord {
  orgId: string; workspaceId: string; harness: string; sessionId: string; machineId: string;
  eventId: string; revision: number; chunkSeq: number; recordSeq: number; ts: string;
  kind: string; scrubVersion: number; text: string;
}
export interface SearchHit {
  workspace_id: string; harness: string; session_id: string; machine_id: string;
  event_id: string; ts: string; snippet: string; citation: string;
}
export interface SearchOptions { query: string; limit?: number; since?: string; until?: string; source?: string; machineId?: string; cursor?: string; tag?: string }
export interface ContextEvent { event_id: string; ts: string; kind: string; text: string; is_match: 0 | 1 }

const MAX_LIMIT = 20;
const MAX_QUERY = 500;
const SNIPPET_CHARS = 320;
const TABLE = "session_search_records";
const COLUMNS = ["org_id", "workspace_id", "harness", "session_id", "machine_id", "event_id", "revision", "chunk_seq", "record_seq", "ts", "kind", "scrub_version", "text"];

export const schemaSql = (tokenizer = "ngrams(3)") => `
CREATE TABLE IF NOT EXISTS ${TABLE} (
  org_id String, workspace_id String, harness LowCardinality(String),
  session_id String, machine_id String, event_id String,
  revision UInt64, chunk_seq UInt32, record_seq UInt32,
  ts DateTime64(3, 'UTC'), kind LowCardinality(String), scrub_version UInt16, text String,
  INDEX text_idx text TYPE text(tokenizer = ${tokenizer}, preprocessor = lower(text)),
  INDEX session_idx session_id TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, workspace_id, ts, session_id, chunk_seq, record_seq)
SETTINGS non_replicated_deduplication_window = 1000`;

export class ClickHouseHttp {
  constructor(private url: string, private user: string, private password: string) { assertLocalTarget(url); }
  async run(sql: string, params: Record<string, string | number> = {}, settings: Record<string, string | number> = {}, body?: string): Promise<string> {
    const u = new URL(this.url);
    for (const [k, v] of Object.entries(settings)) u.searchParams.set(k, String(v));
    for (const [k, v] of Object.entries(params)) u.searchParams.set(`param_${k}`, String(v));
    if (body !== undefined) u.searchParams.set("query", sql);
    const res = await fetch(u, {
      method: "POST", body: body ?? sql,
      headers: { "X-ClickHouse-User": this.user, "X-ClickHouse-Key": this.password },
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`clickhouse ${res.status}: ${text.slice(0, 300)}`);
    return text;
  }
  async rows<T>(sql: string, params: Record<string, string | number> = {}, settings: Record<string, string | number> = {}): Promise<T[]> {
    const out = await this.run(`${sql} FORMAT JSONEachRow`, params, settings);
    return out.split("\n").filter(Boolean).map((l) => JSON.parse(l) as T);
  }
}

const iso = (ms: string) => new Date(Number(ms)).toISOString();
const arrayLiteral = (xs: string[]) => `[${xs.map((x) => `'${x.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`).join(",")}]`;

export class SessionSearchStore {
  constructor(private reader: ClickHouseHttp, private writer: ClickHouseHttp) {}

  /** Refuses before any query: an unscoped search must not be expressible. */
  private guard(scope: Scope) {
    if (!scope.orgId || !scope.workspaceIds.length) throw new Error("search_scope_required");
    return {
      params: { org: scope.orgId, ws: arrayLiteral(scope.workspaceIds) },
      settings: { SQL_relay_org: scope.orgId, max_execution_time: 5 },
    };
  }

  async insert(records: SearchRecord[], dedupToken?: string): Promise<void> {
    if (!records.length) return;
    const body = records.map((r) => JSON.stringify({
      org_id: r.orgId, workspace_id: r.workspaceId, harness: r.harness, session_id: r.sessionId, machine_id: r.machineId,
      event_id: r.eventId, revision: r.revision, chunk_seq: r.chunkSeq, record_seq: r.recordSeq, ts: r.ts.replace("T", " ").replace("Z", ""),
      kind: r.kind, scrub_version: r.scrubVersion, text: r.text,
    })).join("\n");
    await this.writer.run(`INSERT INTO ${TABLE} (${COLUMNS.join(",")}) FORMAT JSONEachRow`, {}, {
      async_insert: 1, wait_for_async_insert: 1, async_insert_busy_timeout_max_ms: 200,
      ...(dedupToken ? { insert_deduplication_token: dedupToken, async_insert_deduplicate: 1 } : {}),
    }, body);
  }

  /**
   * Newest first, event level, like search_shared_sessions. ngrams(3) cannot serve a
   * term under three characters, so that form narrows by tenant only and scans.
   * The substring check is in this phase: hasAllTokens proves grams, not adjacency.
   */
  async search(scope: Scope, o: SearchOptions): Promise<{ hits: SearchHit[]; nextCursor: string | null }> {
    const q = o.query;
    if (!q || [...q].length > MAX_QUERY) throw new Error("invalid_query");
    const { params, settings } = this.guard(scope);
    const limit = Math.min(o.limit ?? 10, MAX_LIMIT);
    const filters = ["org_id = {org:String}", "workspace_id IN {ws:Array(String)}", "positionCaseInsensitiveUTF8(text, {q:String}) > 0"];
    const bound: Record<string, string | number> = { ...params, q };
    if ([...q].length >= 3) filters.push("hasAllTokens(text, {q:String})");
    if (o.since) { filters.push("ts >= parseDateTime64BestEffort({since:String}, 3)"); bound.since = o.since; }
    if (o.until) { filters.push("ts <= parseDateTime64BestEffort({until:String}, 3)"); bound.until = o.until; }
    if (o.source) { filters.push("harness = {source:String}"); bound.source = o.source; }
    if (o.machineId) { filters.push("machine_id = {machine:String}"); bound.machine = o.machineId; }
    if (o.cursor) {
      const [ts, id] = JSON.parse(Buffer.from(o.cursor, "base64url").toString()) as [string, string];
      filters.push("(ts, event_id) < (parseDateTime64BestEffort({cts:String}, 3), {cid:String})");
      bound.cts = ts; bound.cid = id;
    }
    const rows = await this.reader.rows<Omit<SearchHit, "citation" | "ts"> & { at: string }>(`
      SELECT workspace_id, harness, session_id, machine_id, event_id,
        toString(toUnixTimestamp64Milli(ts)) AS at,
        substringUTF8(text, greatest(positionCaseInsensitiveUTF8(text, {q:String}) - 80, 1), ${SNIPPET_CHARS}) AS snippet
      FROM ${TABLE} WHERE ${filters.join(" AND ")}
      ORDER BY ts DESC, event_id DESC LIMIT ${limit + 1}`, bound, { ...settings, ...(o.tag ? { log_comment: o.tag } : {}) });
    const page = rows.slice(0, limit).map(({ at, ...r }) => ({ ...r, ts: iso(at) }));
    const last = page.at(-1);
    return {
      hits: page.map((r) => ({ ...r, citation: `/sessions/${encodeURIComponent(r.session_id)}?source=${encodeURIComponent(r.harness)}` })),
      nextCursor: rows.length > limit && last ? Buffer.from(JSON.stringify([last.ts, last.event_id])).toString("base64url") : null,
    };
  }

  /** The latest in-session match, or a given timestamp, centred in a chronological window. */
  async context(scope: Scope, c: { sessionId: string; source?: string; query?: string; focusTs?: string; maxEvents?: number }): Promise<ContextEvent[]> {
    const { params, settings } = this.guard(scope);
    const session = "org_id = {org:String} AND workspace_id IN {ws:Array(String)} AND session_id = {sid:String}"
      + (c.source ? " AND harness = {source:String}" : "");
    const bound: Record<string, string | number> = { ...params, sid: c.sessionId, ...(c.source ? { source: c.source } : {}) };
    const half = Math.floor(Math.min(c.maxEvents ?? 30, 50) / 2);
    let focus = c.focusTs;
    if (!focus && c.query) {
      const m = await this.reader.rows<{ at: string }>(`
        SELECT toString(toUnixTimestamp64Milli(max(ts))) AS at, count() AS n FROM ${TABLE}
        WHERE ${session} AND positionCaseInsensitiveUTF8(text, {q:String}) > 0 HAVING n > 0`, { ...bound, q: c.query }, settings);
      if (!m[0]) return [];
      focus = iso(m[0].at);
    }
    if (!focus) focus = "2999-01-01T00:00:00.000Z";
    const window = (cmp: string, dir: string, n: number) => `
      SELECT event_id, toString(toUnixTimestamp64Milli(ts)) AS at, kind, text FROM ${TABLE}
      WHERE ${session} AND ts ${cmp} parseDateTime64BestEffort({f:String}, 3)
      ORDER BY ts ${dir}, chunk_seq ${dir}, record_seq ${dir} LIMIT ${n}`;
    const [before, after] = await Promise.all([
      this.reader.rows<Omit<ContextEvent, "ts"> & { at: string }>(window("<=", "DESC", half + 1), { ...bound, f: focus }, settings),
      this.reader.rows<Omit<ContextEvent, "ts"> & { at: string }>(window(">", "ASC", half), { ...bound, f: focus }, settings),
    ]);
    const lower = c.query?.toLowerCase();
    return [...before.reverse(), ...after].map(({ event_id, at, kind, text }) => ({
      event_id, ts: iso(at), kind, text, is_match: (lower && text.toLowerCase().includes(lower) ? 1 : 0) as 0 | 1,
    }));
  }

  async deleteSession(orgId: string, workspaceId: string, sessionId: string): Promise<void> {
    await this.writer.run(`DELETE FROM ${TABLE} WHERE org_id = {org:String} AND workspace_id = {ws:String} AND session_id = {sid:String}`,
      { org: orgId, ws: workspaceId, sid: sessionId }, { lightweight_deletes_sync: 2 });
  }
}

/** Readers: readonly = 2 and a row policy as a backstop only, per #233. Never the boundary. */
export async function installSchema(admin: ClickHouseHttp, password: string, tokenizer?: string) {
  for (const s of [
    `DROP TABLE IF EXISTS ${TABLE} SYNC`,
    "DROP ROW POLICY IF EXISTS search_tenant ON session_search_records",
    "DROP USER IF EXISTS search_reader", "DROP USER IF EXISTS search_writer",
    schemaSql(tokenizer),
    `CREATE USER search_reader IDENTIFIED WITH plaintext_password BY '${password}' SETTINGS readonly = 2`,
    `CREATE USER search_writer IDENTIFIED WITH plaintext_password BY '${password}'`,
    `GRANT SELECT ON ${TABLE} TO search_reader`,
    `GRANT INSERT, SELECT, ALTER DELETE ON ${TABLE} TO search_writer`,
    `CREATE ROW POLICY search_tenant ON ${TABLE} USING org_id = getSetting('SQL_relay_org') TO search_reader`,
  ]) await admin.run(s);
}
