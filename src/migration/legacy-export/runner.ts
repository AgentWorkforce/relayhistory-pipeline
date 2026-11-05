/**
 * One-shot legacy evidence export (conversation_turns, session_links,
 * session_outcomes): plan, export and verify, per org, resumable.
 *
 * Neon is read through a read-only session only. Objects land in the existing
 * canonical layout `{keyRoot}/{org}/v1/ws=…/m=rh-legacy-tables-v1/h=…/d=YYYY-MM-DD/s=…/`,
 * which is already org- and date-scoped (retention-ready: a per-plan TTL is a
 * prefix/date rule; no new layout). One manifest per (org, workspace, source,
 * session) scope, holding only legacy records plus the coverage marker.
 *
 * Idempotent: an unchanged chunk or manifest is never rewritten (exportUnit),
 * and a scope whose Neon fingerprint (per table: count, key digest, whole-row
 * digest and max time, computed in Neon) matches the checkpoint is skipped
 * without streaming, sealing or writing anything. Resumable: a per-(table, org)
 * keyset cursor and per-scope results live in the checkpoint (no secrets).
 */
import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { Queryable } from "../relayhistory-export.ts";
import { exportUnit, type ExportTarget } from "../neon-s3-sync/export-session.ts";
import type { PageHook } from "../neon-s3-sync/neon-reader.ts";
import {
  LEGACY_TABLES,
  legacyDiscoveryPage,
  legacyGroupScopes,
  legacyScopeFenced,
  legacyUnit,
  legacyUnitSource,
  type LegacyGroup,
  type LegacyScope,
  type LegacyTable,
} from "../neon-s3-sync/legacy-source.ts";
import { selectedPublicTurnPredicateSql } from "../../shared/legacy-turn-source.ts";
import {
  objectKey,
  parseManifest,
  readAll,
  type ObjectStore,
} from "../../../packages/object-store/src/index.ts";
import { parseRelayhistoryRecord, readRelayhistoryExport, type RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { scrubRecords } from "../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";

export interface Window {
  sinceMs?: number;
  untilMs?: number;
}

/** Checkpoint: cursors, scope results and counters. Never credentials or content. */
export class Checkpoint {
  readonly db: DatabaseSync;
  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS cursors(table_name TEXT,org TEXT,after TEXT,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(table_name,org));
CREATE TABLE IF NOT EXISTS scopes(key TEXT PRIMARY KEY,org TEXT NOT NULL,status TEXT NOT NULL,fingerprint TEXT,manifest_key TEXT,at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unbound(table_name TEXT,org TEXT,workspace TEXT,stored_source TEXT,session TEXT,turns INTEGER,PRIMARY KEY(table_name,org,workspace,stored_source,session));`);
  }
  cursor(table: string, org: string): { after: LegacyGroup | null; done: boolean } {
    const r = this.db.prepare("SELECT after,done FROM cursors WHERE table_name=? AND org=?").get(table, org) as
      | { after: string | null; done: number }
      | undefined;
    return { after: r?.after ? (JSON.parse(r.after) as LegacyGroup) : null, done: r?.done === 1 };
  }
  setCursor(table: string, org: string, after: LegacyGroup | null, done: boolean) {
    this.db
      .prepare(
        "INSERT INTO cursors VALUES(?,?,?,?) ON CONFLICT(table_name,org) DO UPDATE SET after=excluded.after,done=excluded.done",
      )
      .run(table, org, after ? JSON.stringify(after) : null, done ? 1 : 0);
  }
  scope(key: string) {
    return this.db.prepare("SELECT status,fingerprint,manifest_key FROM scopes WHERE key=?").get(key) as
      | { status: string; fingerprint: string | null; manifest_key: string | null }
      | undefined;
  }
  setScope(key: string, org: string, status: string, fingerprint: string | null, manifestKey: string | null) {
    this.db
      .prepare(
        "INSERT INTO scopes VALUES(?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status=excluded.status,fingerprint=excluded.fingerprint,manifest_key=excluded.manifest_key,at=excluded.at",
      )
      .run(key, org, status, fingerprint, manifestKey, new Date().toISOString());
  }
  recordUnbound(g: LegacyGroup, turns: number) {
    this.db
      .prepare("INSERT OR REPLACE INTO unbound VALUES(?,?,?,?,?,?)")
      .run(g.table, g.org_id, g.workspace_id, g.stored_source, g.session_id, turns);
  }
  exported(org?: string): { key: string; manifest_key: string }[] {
    return this.db
      .prepare(
        `SELECT key,manifest_key FROM scopes WHERE status='exported' ${org ? "AND org=?" : ""} ORDER BY key`,
      )
      .all(...(org ? [org] : [])) as { key: string; manifest_key: string }[];
  }
  close() {
    this.db.close();
  }
}

export const scopeKey = (s: LegacyScope) => JSON.stringify([s.orgId, s.workspaceId, s.source, s.sessionId]);
const scopeOf = (key: string): LegacyScope => {
  const [orgId, workspaceId, source, sessionId] = JSON.parse(key) as string[];
  return { orgId: orgId!, workspaceId: workspaceId!, source: source!, sessionId: sessionId! };
};
const sha = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");

const TURN_SCOPE = `t.org_id=$1 AND t.workspace_id=$2 AND t.session_id=$4 AND ${selectedPublicTurnPredicateSql("t", "$3")}`;
const SCOPE = "t.org_id=$1 AND t.workspace_id=$2 AND t.source=$3 AND t.session_id=$4";

/**
 * The verifier's independent Neon side (read only): per table, the row count
 * and md5 over the source keys in byte order (`convert_to(...,'UTF8')` sorts
 * as bytea, so the order never depends on a collation). These are also the
 * scope's fingerprint, together with the max row time.
 */
export const NEON_SCOPE_SQL = LEGACY_TABLES.map(
  (s) =>
    `SELECT '${s.table}' AS table_name,count(*)::text AS n,
  md5(coalesce(string_agg(t.${s.key}::text,',' ORDER BY convert_to(t.${s.key}::text,'UTF8')),'')) AS keys_md5,
  md5(coalesce(string_agg(md5(to_jsonb(t)::text),',' ORDER BY convert_to(t.${s.key}::text,'UTF8')),'')) AS rows_md5,
  coalesce(floor(extract(epoch FROM max(${s.time}))*1000)::bigint::text,'') AS max_ms
 FROM sessions.${s.table} t WHERE ${s.table === "conversation_turns" ? TURN_SCOPE : SCOPE}`,
).join("\nUNION ALL ");

export interface NeonScopeStats {
  /** rows_md5 covers every column, so the fingerprint sees an in-place update. */
  [table: string]: { n: number; keys_md5: string; rows_md5: string; max_ms: string };
}
export async function neonScopeStats(db: Queryable, s: LegacyScope): Promise<NeonScopeStats> {
  const { rows } = await db.query<{ table_name: string; n: string; keys_md5: string; rows_md5: string; max_ms: string }>(NEON_SCOPE_SQL, [
    s.orgId,
    s.workspaceId,
    s.source,
    s.sessionId,
  ]);
  return Object.fromEntries(rows.map((r) => [r.table_name, { n: Number(r.n), keys_md5: r.keys_md5, rows_md5: r.rows_md5, max_ms: r.max_ms }]));
}

export interface Counters {
  groups: number;
  scopes: number;
  exported: number;
  unchanged: number;
  skippedCheckpoint: number;
  fenced: number;
  unboundTurns: number;
  userUnknown: number;
  manifestsWritten: number;
  failed: number;
  rows: Record<LegacyTable, number>;
}
export const newCounters = (): Counters => ({
  groups: 0,
  scopes: 0,
  exported: 0,
  unchanged: 0,
  skippedCheckpoint: 0,
  fenced: 0,
  unboundTurns: 0,
  userUnknown: 0,
  manifestsWritten: 0,
  failed: 0,
  rows: { conversation_turns: 0, session_links: 0, session_outcomes: 0 },
});

export interface RunDeps {
  db: Queryable;
  target: ExportTarget;
  checkpoint: Checkpoint;
  hook: PageHook;
  log: (fields: Record<string, unknown>) => void;
  stopped?: () => boolean;
}

/**
 * Plan or export one org. `export=false` reads discovery and fingerprints
 * only (nothing is written to S3 or the checkpoint).
 */
export async function runOrg(
  deps: RunDeps,
  org: string,
  /** `refresh`: rediscover from the start (cursors ignored); unchanged fingerprints are still skipped.
   *  `force`: re-export every discovered scope even when its fingerprint is unchanged. */
  opts: { export: boolean; window?: Window; refresh?: boolean; force?: boolean; maxScopes?: number; pageGroups?: number },
  counters: Counters,
): Promise<void> {
  const seen = new Set<string>();
  for (const spec of LEGACY_TABLES) {
    const cur = opts.export ? deps.checkpoint.cursor(spec.table, org) : { after: null, done: false };
    if (cur.done && !opts.refresh) continue;
    let after = opts.refresh ? null : cur.after;
    for (;;) {
      if (deps.stopped?.()) return;
      const page = await legacyDiscoveryPage(deps.db, spec.table, org, after, opts.window, opts.pageGroups ?? 1000);
      await deps.hook(Math.ceil(page.length / 50), 0);
      for (const g of page) {
        counters.groups += 1;
        counters.rows[g.table] += Number(g.rows);
        const { scopes, unbound } = await legacyGroupScopes(deps.db, g);
        if (unbound) {
          counters.unboundTurns += unbound;
          if (opts.export) deps.checkpoint.recordUnbound(g, unbound);
        }
        for (const scope of scopes) {
          const key = scopeKey(scope);
          if (seen.has(key)) continue;
          seen.add(key);
          counters.scopes += 1;
          if (opts.maxScopes !== undefined && counters.scopes > opts.maxScopes) return;
          await exportScope(deps, scope, opts, counters);
        }
      }
      if (page.length) after = page.at(-1)!;
      const done = page.length < (opts.pageGroups ?? 1000);
      if (opts.export) deps.checkpoint.setCursor(spec.table, org, after, done);
      if (done) break;
    }
  }
}

async function exportScope(
  deps: RunDeps,
  scope: LegacyScope,
  opts: { export: boolean; force?: boolean },
  counters: Counters,
) {
  const key = scopeKey(scope);
  try {
    const stats = await neonScopeStats(deps.db, scope);
    const fingerprint = sha(JSON.stringify(LEGACY_TABLES.map((s) => [s.table, stats[s.table]])));
    const prior = deps.checkpoint.scope(key);
    if (!opts.force && prior?.fingerprint === fingerprint && (prior.status === "exported" || prior.status === "fenced")) {
      counters.skippedCheckpoint += 1;
      return;
    }
    if (await legacyScopeFenced(deps.db, scope)) {
      counters.fenced += 1;
      if (opts.export) deps.checkpoint.setScope(key, scope.orgId, "fenced", fingerprint, null);
      return;
    }
    if (!opts.export) return;
    const fresh = await legacyUnit(deps.db, scope);
    if (!fresh) return;
    // A re-export keeps the scope's first key: a moved d= would leave two legacy
    // manifests for one scope, which the reader refuses (gate 4).
    const pinned = /\/d=(\d{4})\/(\d{2})\/(\d{2})\//u.exec(prior?.manifest_key ?? "");
    const unit = pinned ? { ...fresh, firstMs: Date.UTC(+pinned[1]!, +pinned[2]! - 1, +pinned[3]!) } : fresh;
    const userStats = { userUnknown: 0 };
    const result = await exportUnit(deps.target, unit, legacyUnitSource(deps.db, deps.hook, 500, userStats));
    counters.userUnknown += userStats.userUnknown;
    if (result.manifests.length !== 1)
      throw Object.assign(new Error("a legacy scope must produce exactly one manifest"), { code: "legacy_manifest_count" });
    const m = result.manifests[0]!;
    if (m.written) counters.manifestsWritten += 1;
    else counters.unchanged += 1;
    counters.exported += 1;
    deps.checkpoint.setScope(key, scope.orgId, "exported", fingerprint, m.manifestKey);
  } catch (error) {
    counters.failed += 1;
    const code = String((error as { code?: unknown })?.code ?? (error as { name?: unknown })?.name ?? "unknown").slice(0, 64);
    // Interrupted or connection-level: stop before the page cursor advances.
    if (["interrupted", "unauthorized", "ECONNRESET", "EPIPE", "57P01", "08006", "08003"].includes(code)) throw error;
    // Recorded failure (fingerprint null): the cursor moves on; `--refresh` retries it.
    if (opts.export) deps.checkpoint.setScope(key, scope.orgId, "failed:" + code, null, deps.checkpoint.scope(key)?.manifest_key ?? null);
    deps.log({ message: "scope failed", error_code: code });
  }
}

const LEGACY_KINDS: Record<string, LegacyTable | "scope"> = {
  legacy_conversation_turn: "conversation_turns",
  legacy_session_link: "session_links",
  legacy_session_outcome: "session_outcomes",
  legacy_scope: "scope",
};
/** Order-independent digest of records, per table, over the exact stored record. */
export function recordDigests(records: Iterable<RelayhistoryStreamRecord | Record<string, unknown>>) {
  const by: Record<string, { n: number; hashes: string[]; keys: string[] }> = {};
  for (const r of records as Iterable<Record<string, unknown>>) {
    const table = LEGACY_KINDS[String(r.kind)];
    if (!table) continue;
    const slot = (by[table] ??= { n: 0, hashes: [], keys: [] });
    slot.n += 1;
    const { accepted_seq: _seq, ...rest } = r;
    slot.hashes.push(sha(canonicalJson(rest)));
    if (table !== "scope") slot.keys.push(String(r.record_id).slice(`legacy:${table}:`.length));
  }
  const out: Record<string, { n: number; content_sha256: string; keys_md5: string }> = {};
  for (const [table, s] of Object.entries(by)) {
    const keys = s.keys.sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
    out[table] = {
      n: s.n,
      content_sha256: sha(s.hashes.sort().join("\n")),
      keys_md5: createHash("md5").update(keys.join(",")).digest("hex"),
    };
  }
  return out;
}
function canonicalJson(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(",")}]`;
  if (v && typeof v === "object")
    return `{${Object.keys(v as object)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonicalJson((v as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  return typeof v === "bigint" ? v.toString() : JSON.stringify(v);
}

export interface ScopeVerdict {
  scope: LegacyScope;
  ok: boolean;
  problems: string[];
  neon: NeonScopeStats;
  s3: ReturnType<typeof recordDigests> | null;
}

/**
 * Verify one exported scope: (a) the independent Neon counts and key digests
 * equal the stored records', (b) the stored records' content digest equals a
 * fresh rebuild of the same lines from Neon passed through the same scrub
 * (`scrubRecords`), so a scrubbed secret is not a false mismatch, and (c) the
 * coverage marker is present exactly once.
 */
export async function verifyScope(
  db: Queryable,
  store: ObjectStore,
  manifestKey: string,
  scope: LegacyScope,
  decompress: (b: Uint8Array) => Uint8Array,
): Promise<ScopeVerdict> {
  const neon = await neonScopeStats(db, scope);
  const problems: string[] = [];
  let s3: ReturnType<typeof recordDigests> | null = null;
  try {
    const manifest = await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(manifestKey)))), manifestKey);
    const stored = await readRelayhistoryExport(manifest, store, decompress, {
      prefix: manifestKey.split("/v1/")[0],
    });
    s3 = recordDigests(stored);
  } catch (error) {
    problems.push("s3_unreadable:" + String((error as { code?: unknown })?.code ?? "error"));
  }
  if (s3) {
    for (const s of LEGACY_TABLES) {
      const n = neon[s.table]!,
        got = s3[s.table] ?? { n: 0, keys_md5: createHash("md5").update("").digest("hex"), content_sha256: "" };
      if (n.n !== got.n) problems.push(`${s.table}:count neon=${n.n} s3=${got.n}`);
      if (n.keys_md5 !== got.keys_md5) problems.push(`${s.table}:keys`);
    }
    if (s3.scope?.n !== 1) problems.push("coverage_marker");
    const unit = await legacyUnit(db, scope);
    if (unit) {
      const parts: Uint8Array[] = [];
      for await (const line of legacyUnitSource(db, async () => {}).lines(unit)) parts.push(line.bytes);
      const all = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
      let at = 0;
      for (const p of parts) {
        all.set(p, at);
        at += p.byteLength;
      }
      const rebuilt = new TextDecoder()
        .decode(scrubRecords(all).scrubbed)
        .split("\n")
        .filter(Boolean)
        .map((l) => parseRelayhistoryRecord(l));
      // received_at of the coverage marker follows lastMs; compare row tables only.
      const fresh = recordDigests(rebuilt);
      for (const s of LEGACY_TABLES)
        if ((fresh[s.table]?.content_sha256 ?? "") !== (s3[s.table]?.content_sha256 ?? ""))
          problems.push(`${s.table}:content`);
    }
  }
  return { scope, ok: problems.length === 0, problems, neon, s3 };
}

export async function verifyExported(
  db: Queryable,
  store: ObjectStore,
  checkpoint: Checkpoint,
  decompress: (b: Uint8Array) => Uint8Array,
  org?: string,
): Promise<{ ok: boolean; scopes: number; mismatched: ScopeVerdict[] }> {
  const mismatched: ScopeVerdict[] = [];
  let scopes = 0;
  for (const row of checkpoint.exported(org)) {
    scopes += 1;
    const v = await verifyScope(db, store, row.manifest_key, scopeOf(row.key), decompress);
    if (!v.ok) mismatched.push(v);
  }
  return { ok: mismatched.length === 0, scopes, mismatched };
}
