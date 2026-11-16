/**
 * One-shot legacy evidence export (conversation_turns, session_links,
 * session_outcomes): plan, export and verify, per org, resumable.
 *
 * Neon is read through a read-only session only. Objects land in the existing
 * canonical layout `{keyRoot}/{org}/v1/ws=…/m=srv_…/h=…/d=YYYY/MM/DD/s=…/`,
 * which is already org- and date-scoped (retention-ready: a per-plan TTL is a
 * prefix/date rule; no new layout). One manifest per (org, workspace, source,
 * session) scope, holding only legacy records plus the coverage marker.
 *
 * Idempotent: an unchanged chunk or manifest is never rewritten (exportUnit),
 * and a scope whose Neon fingerprint (per table: count, key digest, whole-row
 * digest and max time, computed in Neon) matches the checkpoint is skipped
 * without streaming, sealing or writing anything. Resumable: a
 * per-(table, org, discovery-window)
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
  legacyScopeOwner,
  legacyMachineId,
  legacyUnit,
  legacyUnitSource,
  type LegacyGroup,
  type LegacyScope,
  type LegacyTable,
} from "../neon-s3-sync/legacy-source.ts";
import { selectedPublicTurnPredicateSql } from "../../shared/legacy-turn-source.ts";
import {
  LEGACY_EXPORT_GENERATION,
  LEGACY_EXPORT_GENERATION_SETTING,
} from "../../shared/legacy-export-generation.ts";
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
CREATE TABLE IF NOT EXISTS scopes(key TEXT PRIMARY KEY,org TEXT NOT NULL,status TEXT NOT NULL,fingerprint TEXT,manifest_key TEXT,user_id TEXT,at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unbound(table_name TEXT,org TEXT,workspace TEXT,stored_source TEXT,session TEXT,turns INTEGER,PRIMARY KEY(table_name,org,workspace,stored_source,session));`);
    const columns = this.db
      .prepare("PRAGMA table_info(scopes)")
      .all() as { name: string }[];
    if (!columns.some((c) => c.name === "user_id"))
      this.db.exec("ALTER TABLE scopes ADD COLUMN user_id TEXT");
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
    return this.db.prepare("SELECT status,fingerprint,manifest_key,user_id FROM scopes WHERE key=?").get(key) as
      | { status: string; fingerprint: string | null; manifest_key: string | null; user_id: string | null }
      | undefined;
  }
  setScope(key: string, org: string, status: string, fingerprint: string | null, manifestKey: string | null, userId: string | null = null) {
    this.db
      .prepare(
        "INSERT INTO scopes(key,org,status,fingerprint,manifest_key,user_id,at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status=excluded.status,fingerprint=excluded.fingerprint,manifest_key=excluded.manifest_key,user_id=excluded.user_id,at=excluded.at",
      )
      .run(key, org, status, fingerprint, manifestKey, userId, new Date().toISOString());
  }
  recordUnbound(g: LegacyGroup, turns: number) {
    this.db
      .prepare("INSERT OR REPLACE INTO unbound VALUES(?,?,?,?,?,?)")
      .run(g.table, g.org_id, g.workspace_id, g.stored_source, g.session_id, turns);
  }
  exported(org?: string): { key: string; manifest_key: string; user_id: string | null }[] {
    return this.db
      .prepare(
        `SELECT key,manifest_key,user_id FROM scopes WHERE status='exported' ${org ? "AND org=?" : ""} ORDER BY key`,
      )
      .all(...(org ? [org] : [])) as { key: string; manifest_key: string; user_id: string | null }[];
  }
  retainedNonExported(org?: string): { key: string; status: string; manifest_key: string }[] {
    return this.db
      .prepare(
        `SELECT key,status,manifest_key FROM scopes WHERE status<>'exported' AND manifest_key IS NOT NULL ${org ? "AND org=?" : ""} ORDER BY key`,
      )
      .all(...(org ? [org] : [])) as { key: string; status: string; manifest_key: string }[];
  }
  statusCounts(org?: string): Record<string, number> {
    const rows = this.db
      .prepare(
        `SELECT status,count(*) AS n FROM scopes ${org ? "WHERE org=?" : ""} GROUP BY status ORDER BY status`,
      )
      .all(...(org ? [org] : [])) as { status: string; n: number }[];
    return Object.fromEntries(rows.map((r) => [r.status, Number(r.n)]));
  }
  unboundCount(org?: string): number {
    const row = this.db
      .prepare(
        `SELECT coalesce(sum(turns),0) AS n FROM unbound ${org ? "WHERE org=?" : ""}`,
      )
      .get(...(org ? [org] : [])) as { n: number };
    return Number(row.n);
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
  unboundTurnsOnly: number;
  unboundMultiOwner: number;
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
  unboundTurnsOnly: 0,
  unboundMultiOwner: 0,
  userUnknown: 0,
  manifestsWritten: 0,
  failed: 0,
  rows: { conversation_turns: 0, session_links: 0, session_outcomes: 0 },
});

const cursorName = (table: LegacyTable, window: Window | undefined) =>
  JSON.stringify([
    table,
    window?.sinceMs ?? null,
    window?.untilMs ?? null,
  ]);

export interface RunDeps {
  db: Queryable;
  /** Live Supabase privacy authority: ledger SELECT plus the narrow register RPC. */
  privacyDb: Queryable;
  target: ExportTarget;
  checkpoint: Checkpoint;
  hook: PageHook;
  log: (fields: Record<string, unknown>) => void;
  stopped?: () => boolean;
}

/**
 * A mapped legacy copy may exist only after Supabase is the active erasure
 * authority and its production eraser has completed a recent healthy sweep
 * through the mapping RPC. A missing probe is not compatibility: it means the
 * durable authority contract is absent, so exporting must fail closed.
 */
export async function assertLegacyErasureAuthorityReady(
  db: Queryable,
): Promise<void> {
  const installed = await db.query<{ installed: boolean }>(
    "SELECT to_regprocedure('sessions.legacy_export_privacy_authority_ready()') IS NOT NULL AS installed",
  );
  if (installed.rows[0]?.installed !== true)
    throw Object.assign(
      new Error("Supabase legacy erasure authority readiness probe is missing"),
      { code: "legacy_export_authority_not_active" },
    );
  try {
    await db.query("SELECT set_config($1,$2,false)", [
      LEGACY_EXPORT_GENERATION_SETTING,
      LEGACY_EXPORT_GENERATION,
    ]);
    const { rows } = await db.query<{ ready: boolean }>(
      "SELECT sessions.legacy_export_privacy_authority_ready() AS ready",
    );
    if (rows[0]?.ready !== true) throw new Error("authority readiness probe returned false");
  } catch (cause) {
    throw Object.assign(
      new Error("Supabase legacy erasure authority is not active"),
      { code: "legacy_export_authority_not_active", cause },
    );
  }
}

/** A legacy export is forbidden once either live session erasure or user
 * erasure has started. The operator-erasure global lock is held shared across
 * the S3 write so begin/complete cannot cross this check. */
export async function withLegacyPrivacyFence<T>(
  db: Queryable,
  scope: LegacyScope,
  userId: string,
  action: () => Promise<T>,
  registration?: { machineId: string; existing: boolean },
): Promise<{ fenced: true } | { fenced: false; value: T }> {
  let phase: "privacy" | "action" = "privacy";
  let transactionOpen = false;
  let actionValue: { set: false } | { set: true; value: T } = { set: false };
  try {
    // The login has no table-write grants, but this transaction must be able
    // to invoke the narrow SECURITY DEFINER authority-row register before a
    // PUT. The privilege preflight, not transaction_read_only, is the write
    // boundary on this connection.
    const begin = async () => {
      await db.query("BEGIN ISOLATION LEVEL READ COMMITTED");
      transactionOpen = true;
      await db.query("SET LOCAL lock_timeout='30s'");
      await db.query("SET LOCAL statement_timeout='15min'");
      // The connection idles while Neon is streamed and S3 is written. Bound
      // that lock hold: a wedged object-store request must eventually release
      // the shared privacy locks so an exclusive erase can proceed.
      await db.query("SET LOCAL idle_in_transaction_session_timeout='15min'");
      await db.query("SELECT pg_advisory_xact_lock_shared(961996,50000)");
      await db.query("SELECT pg_advisory_xact_lock_shared(1919249529,96)");
    };
    const commit = async () => {
      await db.query("COMMIT");
      transactionOpen = false;
    };
    const privacyQuery = `SELECT (
        EXISTS(SELECT 1 FROM sessions.session_erasure_receipts
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND source=$3 AND session_id=$4)
        OR EXISTS(SELECT 1 FROM sessions.upload_deletions
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND source=$3 AND session_id=$4)
        OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_deletions
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND source=$3 AND session_id=$4)
        OR EXISTS(SELECT 1 FROM sessions.upload_resets
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND user_id=$5)
        OR EXISTS(SELECT 1 FROM sessions.turn_session_fences
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND source=$3 AND session_id=$4)
        OR EXISTS(SELECT 1 FROM sessions.operator_user_erasures
          WHERE org_id=$1 AND CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END=CASE WHEN $2='' THEN 'default' ELSE $2 END AND user_id=$5)
      ) AS fenced`;
    const privacyParams = [
      scope.orgId,
      scope.workspaceId,
      scope.source,
      scope.sessionId,
      userId,
    ];
    const register = async () => {
      if (!registration) return;
      const expected = {
        org_id: scope.orgId,
        workspace_id: scope.workspaceId === "" ? "default" : scope.workspaceId,
        source: scope.source,
        session_id: scope.sessionId,
        legacy_user_id: userId,
        legacy_machine_id: registration.machineId,
      };
      const { rows } = await db.query<{ r: typeof expected }>(
        "SELECT sessions.ingest_legacy_export_scope_authority_register($1::jsonb) AS r",
        [JSON.stringify(expected)],
      );
      const got = rows[0]?.r;
      if (!got || (Object.keys(expected) as Array<keyof typeof expected>).some((k) => got[k] !== expected[k]))
        throw Object.assign(new Error("legacy export authority registration mismatch"), { code: "legacy_authority_register_mismatch" });
    };
    const fenced = async () => {
      // Registration is the durable proof that this scope can later be erased.
      // Recheck the authority in every transaction, including after the PUT,
      // so a mid-run fallback to Neon refuses the export.
      if (registration) {
        await db.query("SELECT set_config($1,$2,false)", [
          LEGACY_EXPORT_GENERATION_SETTING,
          LEGACY_EXPORT_GENERATION,
        ]);
        const { rows: readiness } = await db.query<{ ready: boolean }>(
          "SELECT sessions.legacy_export_privacy_authority_ready() AS ready",
        );
        if (readiness[0]?.ready !== true)
          throw Object.assign(new Error("Supabase legacy erasure authority is not active"), {
            code: "legacy_export_authority_not_active",
          });
      }
      const { rows } = await db.query<{ fenced: boolean }>(
        privacyQuery,
        privacyParams,
      );
      return rows[0]?.fenced !== false;
    };

    if (registration) {
      // Authority is durable before the first PUT. Existing checkpoints are
      // repaired even if already fenced; a new, already-fenced scope was never
      // exported and therefore must not acquire an authority row.
      await begin();
      if (!registration.existing && (await fenced())) {
        await commit();
        return { fenced: true };
      }
      await register();
      const fencedAfterRegistration = await fenced();
      await commit();
      if (fencedAfterRegistration) return { fenced: true };
    }

    // A separate transaction closes the registration-to-PUT window. Its
    // precheck catches an erasure committed immediately after registration;
    // its postcheck catches one committed while the object write is running.
    await begin();
    if (await fenced()) {
      await commit();
      return { fenced: true };
    }
    phase = "action";
    const value = await action();
    actionValue = { set: true, value };
    phase = "privacy";
    // Ordinary deletion fences need not take the operator's exclusive lock.
    // Recheck before releasing; a race is a failed export, never an accepted
    // checkpoint. The user-scoped legacy machine is swept by user erasure.
    if (await fenced())
      throw Object.assign(new Error("privacy fence raced legacy export"), {
        code: "legacy_privacy_race",
        privacyValue: value,
      });
    await commit();
    return { fenced: false, value };
  } catch (error) {
    let rollbackOk = true;
    if (transactionOpen)
      await db.query("ROLLBACK").then(
        () => {
          transactionOpen = false;
        },
        () => {
          rollbackOk = false;
        },
      );
    if ((error as { code?: unknown })?.code === "legacy_privacy_race") throw error;
    if (phase === "privacy" || !rollbackOk) {
      throw Object.assign(new Error("privacy fence unavailable"), {
        code: "privacy_fence_unavailable",
        cause: error,
        ...(actionValue.set ? { privacyValue: actionValue.value } : {}),
      });
    }
    throw error;
  }
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
  if (opts.export) await assertLegacyErasureAuthorityReady(deps.privacyDb);
  const seen = new Set<string>();
  for (const spec of LEGACY_TABLES) {
    const cursor = cursorName(spec.table, opts.window);
    const cur = opts.export ? deps.checkpoint.cursor(cursor, org) : { after: null, done: false };
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
          if (
            opts.maxScopes !== undefined &&
            counters.exported >= opts.maxScopes
          )
            return;
          const key = scopeKey(scope);
          if (seen.has(key)) continue;
          seen.add(key);
          counters.scopes += 1;
          await exportScope(deps, scope, opts, counters);
        }
      }
      if (page.length) after = page.at(-1)!;
      const done = page.length < (opts.pageGroups ?? 1000);
      if (opts.export) deps.checkpoint.setCursor(cursor, org, after, done);
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
  let boundUserId: string | null = null;
  try {
    const stats = await neonScopeStats(deps.db, scope);
    const fingerprint = sha(JSON.stringify(LEGACY_TABLES.map((s) => [s.table, stats[s.table]])));
    const prior = deps.checkpoint.scope(key);
    if (await legacyScopeFenced(deps.db, scope)) {
      counters.fenced += 1;
      if (opts.export) deps.checkpoint.setScope(key, scope.orgId, "fenced", fingerprint, null);
      return;
    }
    const owner = await legacyScopeOwner(deps.db, scope);
    const resolvedUserId = owner.userId;
    if (resolvedUserId === null) {
      if (owner.reason === "unbound_multi_owner") counters.unboundMultiOwner += 1;
      else if (owner.reason === "unbound_turns_only") counters.unboundTurnsOnly += 1;
      else counters.userUnknown += 1;
      if (opts.export)
        deps.checkpoint.setScope(
          key,
          scope.orgId,
          owner.reason,
          fingerprint,
          null,
        );
      return;
    }
    if (prior?.user_id && prior.user_id !== resolvedUserId)
      throw Object.assign(new Error("legacy scope owner changed"), {
        code: "legacy_user_changed",
      });
    if (prior?.manifest_key && !prior.user_id)
      throw Object.assign(new Error("pre-user-scoped checkpoint requires approved cleanup"), {
        code: "legacy_checkpoint_upgrade_required",
      });
    const userId = prior?.user_id ?? resolvedUserId;
    boundUserId = userId;
    // The authority key normalizes the empty/default workspace alias, but the
    // machine is evidence of the writer's actual stored workspace spelling.
    // Keep that hash exact so the eraser can reach an older `ws=_` export.
    const registration = { machineId: legacyMachineId(scope.workspaceId, userId), existing: !!prior?.manifest_key };
    if (!opts.force && prior?.fingerprint === fingerprint && prior.status === "exported") {
      const privacy = await withLegacyPrivacyFence(deps.privacyDb, scope, userId, async () => undefined, opts.export ? registration : undefined);
      if (privacy.fenced) {
        counters.fenced += 1;
        if (opts.export) deps.checkpoint.setScope(key, scope.orgId, "fenced", fingerprint, prior.manifest_key, userId);
      } else counters.skippedCheckpoint += 1;
      return;
    }
    if (!opts.export) {
      const privacy = await withLegacyPrivacyFence(
        deps.privacyDb,
        scope,
        userId,
        async () => undefined,
      );
      if (privacy.fenced) counters.fenced += 1;
      return;
    }
    const fresh = await legacyUnit(deps.db, scope);
    if (!fresh) return;
    // A re-export keeps the scope's first key: a moved d= would leave two legacy
    // manifests for one scope, which the reader refuses (gate 4).
    const pinned = /\/d=(\d{4})\/(\d{2})\/(\d{2})\//u.exec(prior?.manifest_key ?? "");
    const unit = pinned ? { ...fresh, firstMs: Date.UTC(+pinned[1]!, +pinned[2]! - 1, +pinned[3]!) } : fresh;
    const privacy = await withLegacyPrivacyFence(deps.privacyDb, scope, userId, () =>
      exportUnit(
        deps.target,
        unit,
        legacyUnitSource(deps.db, deps.hook, 500, undefined, userId),
      ),
      registration,
    );
    if (privacy.fenced) {
      counters.fenced += 1;
      deps.checkpoint.setScope(key, scope.orgId, "fenced", fingerprint, prior?.manifest_key ?? null, userId);
      return;
    }
    const result = privacy.value;
    if (result.manifests.length !== 1)
      throw Object.assign(new Error("a legacy scope must produce exactly one manifest"), { code: "legacy_manifest_count" });
    const m = result.manifests[0]!;
    if (m.written) counters.manifestsWritten += 1;
    else counters.unchanged += 1;
    counters.exported += 1;
    deps.checkpoint.setScope(key, scope.orgId, "exported", fingerprint, m.manifestKey, userId);
  } catch (error) {
    counters.failed += 1;
    const code = String((error as { code?: unknown })?.code ?? (error as { name?: unknown })?.name ?? "unknown").slice(0, 64);
    // Interrupted or connection-level: stop before the page cursor advances.
    if (["interrupted", "unauthorized", "ECONNRESET", "EPIPE", "57P01", "08006", "08003"].includes(code)) throw error;
    // Recorded failure (fingerprint null): the cursor moves on; `--refresh` retries it.
    if (opts.export) {
      const previous = deps.checkpoint.scope(key);
      const written = (error as { privacyValue?: { manifests?: Array<{ manifestKey?: unknown }> } })
        ?.privacyValue?.manifests?.[0]?.manifestKey;
      deps.checkpoint.setScope(
        key,
        scope.orgId,
        "failed:" + code,
        null,
        typeof written === "string" ? written : previous?.manifest_key ?? null,
        previous?.user_id ?? boundUserId,
      );
    }
    deps.log({ message: "scope failed", error_code: code });
    if (["legacy_privacy_race", "privacy_fence_unavailable"].includes(code)) throw error;
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
  privacyDb?: Queryable,
): Promise<{
  ok: boolean;
  scopes: number;
  mismatched: ScopeVerdict[];
  statuses: Record<string, number>;
  unboundTurns: number;
  unexplained: string[];
  privacyFenced: LegacyScope[];
}> {
  const mismatched: ScopeVerdict[] = [];
  const statuses = checkpoint.statusCounts(org);
  const unboundTurns = checkpoint.unboundCount(org);
  const unexplained = Object.entries(statuses)
    .filter(([status]) => status !== "exported" && status !== "fenced")
    .map(([status, n]) => `${status}:${n}`);
  for (const row of checkpoint.retainedNonExported(org))
    unexplained.push(`retained_manifest:${row.status}:${row.key}`);
  if (unboundTurns > 0) unexplained.push(`unbound_turns:${unboundTurns}`);
  let scopes = 0;
  const privacyFenced: LegacyScope[] = [];
  for (const row of checkpoint.exported(org)) {
    scopes += 1;
    const scope = scopeOf(row.key);
    if (!privacyDb || !row.user_id) {
      unexplained.push(`privacy_unverified:${row.key}`);
      continue;
    }
    const privacy = await withLegacyPrivacyFence(
      privacyDb,
      scope,
      row.user_id,
      async () => undefined,
    );
    if (privacy.fenced) {
      privacyFenced.push(scope);
      continue;
    }
    const v = await verifyScope(db, store, row.manifest_key, scope, decompress);
    if (!v.ok) mismatched.push(v);
  }
  return {
    ok:
      mismatched.length === 0 &&
      unexplained.length === 0 &&
      privacyFenced.length === 0,
    scopes,
    mismatched,
    statuses,
    unboundTurns,
    unexplained,
    privacyFenced,
  };
}
