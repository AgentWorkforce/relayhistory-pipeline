/**
 * 062110 search reconcile staging and staged-reconcile.ts: grants, the lease (takeover only after
 * expiry, the stale-lease SQLSTATE), a kill at every phase boundary resumed to the same result as
 * the in-memory reconcile, a source change mid-run, erasure during a run, and the rollback.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { CONSUMER_RUNTIME_FUNCTIONS, RUNTIME_APPLICATION_FUNCTIONS } from "../../hoopdb/migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";
import { streamIndexKey } from "../../session-read-api/canonical-stream-projection.ts";
import { S3_ACTIVITY_KINDS } from "../search-index.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { PGlite as SearchPGlite } from "@electric-sql/pglite";
import { pg_trgm as searchTrgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { adapterProjectionIo, type SearchProjectionState } from "../search-adapter.ts";
import { SEARCH_INDEX_DEFAULT_LIMITS, indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { readCanonicalScope } from "../../session-read-api/canonical-projection.ts";
import { streamCanonicalScope, type StreamedScope } from "../../session-read-api/canonical-stream-projection.ts";
import {
  SCOPE as FX, decompress, exportStreamScope, generate, prefixFor, type ExportedScope,
} from "../../session-read-api/__tests__/stream-fixture.ts";
import {
  STAGED_RECONCILE_STALE_LEASE,
  admittedManifestFingerprint,
  eraseStagedReconcileRuns,
  runStagedReconcile,
  type SourceStamp,
  type StagedBoundary,
  type StagedIdentity,
  type StagedReconcileDeps,
  type StagedReconcileOps,
  type StagedWant,
} from "../staged-reconcile.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const FILE = "20261105062110_search_reconcile_staging.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062110_search_reconcile_staging.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const RUNTIME = "search_consumer_runtime";

const WRITERS = [
  "sessions.search_reconcile_claim_v1(text,text,text,text,text,bigint,text,integer)",
  "sessions.search_reconcile_stage_batch_v1(bigint,uuid,text,smallint,jsonb,text)",
  "sessions.search_reconcile_advance_v1(bigint,uuid,text,text,text[],bigint,bigint,boolean)",
  "sessions.search_reconcile_diff_page_v1(bigint,uuid,text,text,integer,text)",
  "sessions.search_reconcile_finish_v1(bigint,uuid)",
  "sessions.search_reconcile_abandon_v1(bigint,uuid,text)",
  "sessions.search_reconcile_erase_scope_v1(text,text,text,text[])",
];
const HELPERS = ["sessions.search_reconcile_caller_v1()", "sessions.search_reconcile_lease_v1(bigint,uuid)"];
const TABLES = ["sessions.search_reconcile_runs_v1", "sessions.search_reconcile_stage_v1"];
const OTHER_ROLES = ["anon", "authenticated", "service_role", "bulls96_read_api", "bulls96_session_ingest", "staged_probe"];

function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;
async function migrated() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await runMigrations(client(db), { profile: LOCAL });
  return db;
}

// ---- the fixture: one scope, its wanted identities and its index ----------------------------

const SCOPE = { orgId: "org-a", workspaceId: "ws-a", harness: "claude", sessionId: "sess-1" };
const OWNER = { workspaceId: "ws-a", sessionId: "sess-1" };
const ALIAS = { workspaceId: "ws-a", sessionId: "sess-1-alias" };
type Event = { machineId: string; source: string; kind: string; eventId: string };
type Want = StagedWant & Event;
const ev = (i: number, kind = "history"): Event => ({ machineId: "m-1", source: "claude", kind, eventId: `e-${String(i).padStart(3, "0")}` });
const want = (e: Event, tsMs: number, fingerprint: string, owner = OWNER): Want => ({ ...e, ...owner, key: streamIndexKey(e), tsMs, fingerprint });
const row = (e: Event, tsMs: number, fingerprint: string, owner = OWNER): StagedIdentity => ({ ...e, ...owner, key: streamIndexKey(e), tsMs, fingerprint });

interface Fixture { wants: Want[]; index: StagedIdentity[] }
/** 30 wanted events and an index holding every case the reconcile distinguishes. */
function fixture(extra = 0): Fixture {
  const wants: Want[] = [];
  const index: StagedIdentity[] = [];
  for (let i = 0; i < 30 + extra; i++) {
    const w = want(ev(i), 1000 + i, `s3-${i}`);
    wants.push(w);
    if (i < 10) index.push(row(ev(i), 1000 + i, `s3-${i}`)); // current: nothing to do
    else if (i < 15) index.push(row(ev(i), 1000 + i, `stale-${i}`)); // stale fingerprint: rewritten
    else if (i < 18) index.push(row(ev(i), 5000 + i, `s3-${i}`)); // moved: inserted, old one deleted
    else if (i < 20) index.push(row(ev(i), 1000 + i, `s3-${i}`), row(ev(i), 1000 + i, `s3-${i}`, ALIAS)); // alias copy deleted
    // 20..: missing, inserted
  }
  for (let i = 100; i < 105; i++) index.push(row(ev(i), 2000 + i, `s3-${i}`)); // retracted: deleted
  for (let i = 110; i < 112; i++) index.push(row(ev(i), 2000 + i, `bf-${i}`)); // buffered, not wanted: kept
  for (let i = 120; i < 122; i++) index.push(row(ev(i, "tool_result"), 2000 + i, `x-${i}`)); // not regenerable: kept
  index.push(row(ev(25), 9999, "bf-25")); // buffered copy of a wanted key at another time: deleted
  return { wants, index };
}
const EXPECT = { inserted: 18, deleted: 11 };

/** The unstaged reconcile, in memory: streamedReconcile's diff of `wants` against `index`, applied. */
function inMemory(wants: Want[], index: StagedIdentity[]): { rows: string[]; inserted: number; deleted: number } {
  const existing = new Map<string, StagedIdentity[]>();
  for (const r of index) existing.set(r.key, [...(existing.get(r.key) ?? []), r]);
  const wanted = new Map(wants.map((w) => [w.key, w]));
  const toInsert = [...wanted.values()].filter((w) =>
    !(existing.get(w.key) ?? []).some((c) => c.sessionId === w.sessionId && c.workspaceId === w.workspaceId && c.tsMs === w.tsMs && c.fingerprint === w.fingerprint));
  const regenerable = new Set<string>([...S3_ACTIVITY_KINDS, ...wants.map((w) => w.kind)]);
  const deletes: StagedIdentity[] = [];
  for (const [key, list] of existing) {
    const current = wanted.get(key);
    for (const c of list) {
      if (!regenerable.has(c.kind)) continue;
      if (!current && c.fingerprint.startsWith("bf")) continue;
      if (!current || current.sessionId !== c.sessionId || current.workspaceId !== c.workspaceId || current.tsMs !== c.tsMs) deletes.push(c);
    }
  }
  const id = (r: Omit<StagedIdentity, "key" | "fingerprint">) => [r.machineId, r.source, r.kind, r.eventId, r.sessionId, r.workspaceId, r.tsMs].join("|");
  const out = new Map(index.map((r) => [id(r), r.fingerprint]));
  for (const w of toInsert) out.set(id(w), w.fingerprint);
  for (const d of deletes) out.delete(id(d));
  return { rows: [...out].map(([k, fp]) => `${k}|${fp}`).sort(), inserted: toInsert.length, deleted: deletes.length };
}
function memoryReconcile(f: Fixture): string[] {
  const r = inMemory(f.wants, f.index);
  expect([r.inserted, r.deleted]).toEqual([EXPECT.inserted, EXPECT.deleted]);
  return r.rows;
}
/** The in-memory result for `next`'s source over the index `prior` started from. */
const expectedFor = (next: Fixture, prior: Fixture) => inMemory(next.wants, prior.index).rows;

class Kill extends Error {}

describe("20261105062110 search reconcile staging", () => {
  let db: PGlite;
  let stamp: SourceStamp;
  let current: SourceStamp;
  let source: Fixture;
  const transact = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${RUNTIME}`);
      return work({ query: async (text, params) => (await tx.query(text, params)) as never });
    });
  const as = <T,>(role: string, run: (tx: SqlExecutor) => Promise<T>) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      return run({ query: async (text, params) => (await tx.query(text, params)) as never });
    });
  const expire = () => db.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
  const indexRows = async () =>
    (await db.query<{ r: string }>(
      "SELECT concat_ws('|', machine_id, source, kind, event_id, session_id, workspace_id, ts_ms::text, fingerprint) r FROM fake.idx")).rows.map((r) => r.r).sort();
  const runs = async () => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_runs_v1")).rows[0]!.n;
  const staged_ = async () => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_stage_v1")).rows[0]!.n;
  const commits = async () => (await db.query<{ n: number }>("SELECT count(*)::int n FROM fake.commits")).rows[0]!.n;
  const stampOf = (f: Fixture, epoch: string): SourceStamp => ({
    canonicalId: "sess-1", epoch,
    manifestFingerprint: admittedManifestFingerprint(f.wants.map((w) => ({ manifestKey: `k/${w.eventId}`, revision: "1" }))),
  });
  async function reset(f: Fixture, epoch = "7") {
    await db.exec("DELETE FROM sessions.search_reconcile_runs_v1; DELETE FROM fake.idx; DELETE FROM fake.commits;");
    for (const r of f.index)
      await db.query("INSERT INTO fake.idx VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", [r.machineId, r.source, r.kind, r.eventId, r.sessionId, r.workspaceId, r.tsMs, r.fingerprint]);
    source = f;
    stamp = stampOf(f, epoch);
    current = stamp;
  }

  const calls = { reads: 0, scanAfter: [] as Array<string | null> };
  const ops = (): StagedReconcileOps<Map<string, Want>> => ({
    async readSource() {
      calls.reads += 1;
      return new Map(source.wants.map((w) => [w.key, w]));
    },
    wanted: (r) => r.values(),
    regenerableKinds: (r) => [...S3_ACTIVITY_KINDS, ...[...r.values()].map((w) => w.kind)],
    async scanPage(tx, after, limit) {
      calls.scanAfter.push(after);
      const { rows } = await tx.query<StagedIdentity & { ord: string; ts_ms: string }>(
        `SELECT * FROM (
           SELECT machine_id AS "machineId", source, kind, event_id AS "eventId", session_id AS "sessionId", workspace_id AS "workspaceId",
                  ts_ms::text, fingerprint, concat_ws(chr(31), machine_id, source, kind, event_id, session_id, workspace_id, lpad(ts_ms::text, 20, '0')) COLLATE "C" AS ord
             FROM fake.idx) AS i
          WHERE $1::text IS NULL OR i.ord > $1::text
          ORDER BY i.ord LIMIT $2`, [after, limit]);
      return {
        rows: rows.map((r) => ({ machineId: r.machineId, source: r.source, kind: r.kind, eventId: r.eventId, sessionId: r.sessionId, workspaceId: r.workspaceId, tsMs: Number(r.ts_ms), fingerprint: r.fingerprint, key: streamIndexKey(r) })),
        next: rows.length < limit ? null : rows[rows.length - 1]!.ord,
      };
    },
    async regenerate(tx, r, keys) {
      let n = 0;
      for (const key of keys) {
        const w = r.get(key)!;
        await tx.query(
          `INSERT INTO fake.idx VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (machine_id, source, kind, event_id, session_id, workspace_id, ts_ms) DO UPDATE SET fingerprint = EXCLUDED.fingerprint`,
          [w.machineId, w.source, w.kind, w.eventId, w.sessionId, w.workspaceId, w.tsMs, w.fingerprint]);
        n += 1;
      }
      return n;
    },
    currentSource: async () => current,
    async remove(tx, ids) {
      let n = 0;
      for (const d of ids)
        n += (await tx.query<{ n: number }>(
          `WITH d AS (DELETE FROM fake.idx WHERE machine_id=$1 AND source=$2 AND kind=$3 AND event_id=$4 AND session_id=$5 AND workspace_id=$6 AND ts_ms=$7 RETURNING 1)
           SELECT count(*)::int n FROM d`, [d.machineId, d.source, d.kind, d.eventId, d.sessionId, d.workspaceId, d.tsMs])).rows[0]!.n;
      return n;
    },
    async commit(tx, _r, pass) {
      await tx.query("INSERT INTO fake.commits VALUES ($1,$2,$3)", [pass.complete, pass.inserted, pass.deleted]);
      return { complete: pass.complete, reasons: [] };
    },
  });
  const deps = (boundary?: StagedReconcileDeps["boundary"]): StagedReconcileDeps => ({
    transact, stageBatch: 7, scanPage: 6, writeKeys: 5, deletePage: 4, ...(boundary ? { boundary } : {}),
  });
  const killAt = (point: StagedBoundary, nth = 1) => {
    let seen = 0;
    return (p: StagedBoundary) => {
      if (p === point && ++seen === nth) throw new Kill(p);
    };
  };

  beforeAll(async () => {
    db = await migrated();
    await db.exec(`
      GRANT USAGE ON SCHEMA sessions TO ${RUNTIME}; -- as session_consumer_role.sql does
      CREATE SCHEMA fake;
      CREATE TABLE fake.idx (machine_id text, source text, kind text, event_id text, session_id text, workspace_id text, ts_ms bigint, fingerprint text,
        PRIMARY KEY (machine_id, source, kind, event_id, session_id, workspace_id, ts_ms));
      CREATE TABLE fake.commits (complete boolean, inserted bigint, deleted bigint);
      GRANT USAGE ON SCHEMA fake TO ${RUNTIME};
      GRANT SELECT, INSERT, UPDATE, DELETE ON fake.idx, fake.commits TO ${RUNTIME};
      CREATE ROLE staged_probe NOLOGIN;`);
  });
  afterAll(async () => {
    await db?.close();
  });

  test("objects: SECURITY DEFINER writers with a pinned search_path, owner-only helpers, RLS on both tables", async () => {
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("search_reconcile_runs_v1");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(CONSUMER_RUNTIME_FUNCTIONS.filter((f) => f.startsWith("sessions.search_reconcile_")).sort()).toEqual([...WRITERS].sort());
    expect(RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.search_reconcile_"))).toEqual([]);
    for (const fn of [...WRITERS, ...HELPERS]) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r.secdef).toBe(WRITERS.includes(fn));
      expect(r.cfg).toEqual(
        fn === WRITERS[0] ? ["search_path=pg_catalog, sessions, pg_temp", "lock_timeout=2s"]
        : [WRITERS.includes(fn) ? "search_path=pg_catalog, sessions, pg_temp" : "search_path=pg_catalog, pg_temp"]);
      expect(r.public_exec).toBe(false);
    }
    for (const t of TABLES) {
      const r = (await db.query<{ rls: boolean; persistence: string }>("SELECT relrowsecurity rls, relpersistence persistence FROM pg_class WHERE oid=$1::regclass", [t])).rows[0]!;
      expect(r).toEqual({ rls: true, persistence: "p" });
    }
    const code_ = SQL.split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n");
    expect(code_).not.toMatch(/CONCURRENTLY|EXTENSION|TEMP(ORARY)? TABLE|OR REPLACE|UNLOGGED|ALTER FUNCTION|DROP /i);
  });

  test("grants: only search_consumer_runtime executes the writers; nobody else; no direct table DML for the runtime role", async () => {
    const can = async (role: string, fn: string) =>
      (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of WRITERS) {
      expect(await can(RUNTIME, fn)).toBe(true);
      for (const role of OTHER_ROLES) expect(await can(role, fn)).toBe(false);
    }
    for (const fn of HELPERS) for (const role of [RUNTIME, ...OTHER_ROLES]) expect(await can(role, fn)).toBe(false);
    for (const t of TABLES)
      for (const role of [RUNTIME, ...OTHER_ROLES])
        for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"])
          expect((await db.query<{ ok: boolean }>("SELECT has_table_privilege($1, $2, $3) ok", [role, t, priv])).rows[0]!.ok).toBe(false);
    // Live calls: the runtime role runs a writer, cannot touch a table; another role cannot run a writer.
    await reset(fixture());
    const claimed = await as(RUNTIME, (tx) => tx.query("SELECT run_id FROM sessions.search_reconcile_claim_v1('org-a','ws-a','claude','sess-1','sess-1',7,'fp',60000)"));
    expect(claimed.rows.length).toBe(1);
    expect(await code(as(RUNTIME, (tx) => tx.query("SELECT 1 FROM sessions.search_reconcile_runs_v1")))).toBe("42501");
    expect(await code(as(RUNTIME, (tx) => tx.query("DELETE FROM sessions.search_reconcile_stage_v1")))).toBe("42501");
    expect(await code(as(RUNTIME, (tx) => tx.query("UPDATE sessions.search_reconcile_runs_v1 SET attempts = 0")))).toBe("42501");
    for (const role of ["service_role", "bulls96_read_api", "staged_probe"])
      expect(await code(as(role, (tx) => tx.query("SELECT * FROM sessions.search_reconcile_claim_v1('org-a','ws-a','claude','sess-2','sess-2',7,'fp',60000)")))).toBe("42501");
    expect(await code(as(RUNTIME, (tx) => tx.query("SELECT sessions.search_reconcile_caller_v1()")))).toBe("42501");
  });

  test("lease: a second claim is busy while the lease lives, takes over only after expiry, and the old nonce is then RH062", async () => {
    await reset(fixture());
    type Claim = { run_id: string; lease_nonce: string; attempts: number; restarted: boolean };
    const claim = async () => (await transact((tx) => tx.query<Claim>(
      "SELECT run_id::text, lease_nonce::text, attempts, restarted FROM sessions.search_reconcile_claim_v1('org-a','ws-a','claude','sess-1','sess-1',7,'fp',60000)"))).rows;
    const stage = (c: Claim) => transact((tx) => tx.query(
      "SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,'want',3::smallint,'[{\"k\":\"a\",\"o\":\"o\",\"t\":1,\"f\":\"f\"}]'::jsonb,'a')", [c.run_id, c.lease_nonce]));
    const first = (await claim())[0]!;
    expect(first.attempts).toBe(1);
    expect(await claim()).toEqual([]);
    expect(await code(stage(first))).toBeNull();
    expect(await claim()).toEqual([]);
    // Expired but not taken over: still the holder's; its next call renews the lease.
    await expire();
    expect(await code(stage(first))).toBeNull();
    expect(await claim()).toEqual([]);
    await expire();
    const second = (await claim())[0]!;
    expect([second.run_id, second.attempts, second.restarted]).toEqual([first.run_id, 2, false]);
    expect(second.lease_nonce).not.toBe(first.lease_nonce);
    expect(await code(stage(first))).toBe(STAGED_RECONCILE_STALE_LEASE);
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_finish_v1($1::bigint,$2::uuid)", [first.run_id, first.lease_nonce])))).toBe("RH062");
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,'x')", [first.run_id, first.lease_nonce])))).toBe("RH062");
    expect(await code(transact((tx) => tx.query("SELECT * FROM sessions.search_reconcile_diff_page_v1($1::bigint,$2::uuid,'insert',NULL,10,'bf')", [first.run_id, first.lease_nonce])))).toBe("RH062");
    // The new holder continues where the old one committed; phase order is enforced.
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_stage_v1")).rows[0]!.n).toBe(1);
    // Cursors only move forward; the keep prefix is 1 to 16 characters.
    const stageAt = (c: Claim, at: string) => transact((tx) => tx.query(
      "SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,'want',3::smallint,'[]'::jsonb,$3)", [c.run_id, c.lease_nonce, at]));
    expect(await code(stageAt(second, "b"))).toBeNull();
    expect(await code(stageAt(second, "b"))).toBeNull();
    expect(await code(stageAt(second, "a"))).toBe("55000");
    // Nor is a set cursor cleared mid-phase (only a phase change resets it).
    expect(await code(transact((tx) => tx.query(
      "SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,'want',3::smallint,'[]'::jsonb,NULL)", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'want',NULL,NULL,0,0,false)", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'want','a',NULL,0,0,false)", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'write',NULL,NULL,0,0,false)", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT sessions.search_reconcile_finish_v1($1::bigint,$2::uuid)", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT * FROM sessions.search_reconcile_diff_page_v1($1::bigint,$2::uuid,'insert',NULL,10,'bf')", [second.run_id, second.lease_nonce])))).toBe("55000");
    expect(await code(transact((tx) => tx.query("SELECT * FROM sessions.search_reconcile_diff_page_v1($1::bigint,$2::uuid,'insert',NULL,10,'')", [second.run_id, second.lease_nonce])))).toBe("22023");
    // A release frees the scope at once.
    await transact((tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'want','b',NULL,0,0,true)", [second.run_id, second.lease_nonce]));
    expect((await claim())[0]!.attempts).toBe(3);
  });

  test("an uninterrupted run equals the in-memory reconcile", async () => {
    const f = fixture();
    const expected = memoryReconcile(f);
    await reset(f);
    const out = await runStagedReconcile(SCOPE, stamp, ops(), deps());
    expect(out).toEqual({ status: "done", complete: true, inserted: EXPECT.inserted, wanted: EXPECT.inserted, deleted: EXPECT.deleted, reasons: [], attempts: 1, resumedFrom: null });
    expect(await indexRows()).toEqual(expected);
    expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, 1]);
    // A second run finds nothing to do.
    const again = await runStagedReconcile(SCOPE, stamp, ops(), deps());
    expect(again).toMatchObject({ status: "done", inserted: 0, deleted: 0 });
    expect(await indexRows()).toEqual(expected);
  });

  // Every boundary, and the second hit of every per-batch one: kill there, resume after expiry.
  const MATRIX: Array<[StagedBoundary, number, { phase: string; cursor: boolean } | null]> = [
    ["claimed", 1, null],
    ["want:batch", 1, { phase: "want", cursor: true }],
    ["want:batch", 3, { phase: "want", cursor: true }],
    ["want:done", 1, { phase: "have", cursor: false }],
    ["have:batch", 1, { phase: "have", cursor: true }],
    ["have:batch", 3, { phase: "have", cursor: true }],
    ["have:done", 1, { phase: "write", cursor: false }],
    ["write:page", 1, { phase: "write", cursor: true }],
    ["write:page", 2, { phase: "write", cursor: true }],
    ["write:done", 1, { phase: "final", cursor: false }],
    ["final:locked", 1, { phase: "final", cursor: false }],
    ["final:removed", 1, { phase: "final", cursor: false }],
    ["final:committed", 1, { phase: "final", cursor: false }],
    ["finished", 1, null],
  ];
  for (const [point, nth, resumed] of MATRIX)
    test(`kill at ${point} #${nth}: resumes at its cursor and ends equal to the in-memory reconcile`, async () => {
      const f = fixture();
      const expected = memoryReconcile(f);
      await reset(f);
      const before = await indexRows();
      expect(await runStagedReconcile(SCOPE, stamp, ops(), deps(killAt(point, nth))).then(() => "ran", (e) => (e instanceof Kill ? e.message : e))).toBe(point);
      if (point === "finished") {
        expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, 1]);
        expect(await indexRows()).toEqual(expected);
      } else {
        // The lease still lives: nobody else runs the scope until it expires.
        expect(await runStagedReconcile(SCOPE, stamp, ops(), deps())).toEqual({ status: "busy", reason: "run_leased" });
        expect(await commits()).toBe(0);
        // A kill inside the final transaction rolls back its deletes.
        if (point.startsWith("final:")) expect((await indexRows()).length).toBe(before.length + 13); // 3 moved + 10 missing are new rows;
        await expire();
      }
      calls.scanAfter = [];
      const out = await runStagedReconcile(SCOPE, stamp, ops(), deps());
      expect(await indexRows()).toEqual(expected);
      expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, point === "finished" ? 2 : 1]);
      if (point === "finished") {
        expect(out).toMatchObject({ status: "done", inserted: 0, deleted: 0, attempts: 1, resumedFrom: null });
        return;
      }
      expect(out).toMatchObject({ status: "done", complete: true, inserted: EXPECT.inserted, wanted: EXPECT.inserted, deleted: EXPECT.deleted, attempts: 2 });
      const from = (out as { resumedFrom: { phase: string; cursor: string | null } | null }).resumedFrom;
      expect(from === null ? null : { phase: from.phase, cursor: from.cursor !== null }).toEqual(resumed);
      // A resumed index scan continues after its committed cursor; a later phase never rescans.
      if (resumed?.phase === "have") expect(calls.scanAfter[0] !== null).toBe(resumed.cursor);
      if (resumed && resumed.phase !== "want" && resumed.phase !== "have") expect(calls.scanAfter).toEqual([]);
    });

  test("a lease that expires during the source read or inside the final step, with no takeover, still completes", async () => {
    const f = fixture();
    const expected = memoryReconcile(f);
    await reset(f);
    const base = ops();
    let removes = 0;
    const out = await runStagedReconcile(SCOPE, stamp, {
      ...base,
      async readSource() {
        await expire(); // the read outlived the lease
        return base.readSource();
      },
      async remove(tx, ids) {
        // Inside the final transaction: the lease passes its expiry while the run row is locked.
        if (removes++ === 0) {
          await tx.query("RESET ROLE");
          await tx.query("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
          await tx.query(`SET LOCAL ROLE ${RUNTIME}`);
        }
        return base.remove(tx, ids);
      },
    }, deps());
    expect(out).toMatchObject({ status: "done", complete: true, inserted: EXPECT.inserted, deleted: EXPECT.deleted, attempts: 1 });
    expect(removes).toBeGreaterThan(0);
    expect(await indexRows()).toEqual(expected);
    expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, 1]);
  });

  test("a takeover during the source read: the old holder stops with RH062, the taker's run converges", async () => {
    const f = fixture();
    const expected = memoryReconcile(f);
    await reset(f);
    const base = ops();
    const err = await runStagedReconcile(SCOPE, stamp, {
      ...base,
      async readSource() {
        await expire();
        const taken = await transact((tx) => tx.query("SELECT run_id FROM sessions.search_reconcile_claim_v1($1,$2,$3,$4,$5,$6::bigint,$7,60000)",
          [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId, stamp.canonicalId, stamp.epoch, stamp.manifestFingerprint]));
        expect(taken.rows.length).toBe(1);
        return base.readSource();
      },
    }, deps()).then(() => null, (e: { code?: string }) => e.code);
    expect(err).toBe(STAGED_RECONCILE_STALE_LEASE);
    expect(await commits()).toBe(0);
    await expire();
    expect(await runStagedReconcile(SCOPE, stamp, base, deps())).toMatchObject({ status: "done", complete: true, attempts: 3 });
    expect(await indexRows()).toEqual(expected);
  });

  test("an index scan whose cursor does not advance stops instead of paging forever", async () => {
    await reset(fixture());
    const stuck = { ...ops(), scanPage: async (_tx: SqlExecutor, after: string | null) => ({ rows: [], next: after ?? "x" }) };
    await expect(runStagedReconcile(SCOPE, stamp, stuck, deps())).rejects.toThrow("index scan cursor did not advance");
  });

  test("a source change mid-run abandons the run at the final check; the next tick restarts and converges on the new source", async () => {
    const f = fixture();
    await reset(f);
    const changed = fixture(4);
    const out = await runStagedReconcile(SCOPE, stamp, ops(), deps((p) => {
      if (p === "write:done") {
        source = changed;
        current = stampOf(changed, "8");
      }
    }));
    expect(out).toEqual({ status: "restart", reason: "source_changed" });
    const run = (await db.query<{ phase: string; last_error: string; lease_nonce: string | null }>(
      "SELECT phase, last_error, lease_nonce::text FROM sessions.search_reconcile_runs_v1")).rows;
    expect(run).toEqual([{ phase: "want", last_error: "source_changed", lease_nonce: null }]);
    expect([await staged_(), await commits()]).toEqual([0, 0]);
    // The next tick claims with the new stamp, restarts from nothing, and converges.
    stamp = current;
    const next = await runStagedReconcile(SCOPE, stamp, ops(), deps());
    expect(next).toMatchObject({ status: "done", complete: true, resumedFrom: null });
    expect(await indexRows()).toEqual(expectedFor(changed, f));
    expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, 1]);
  });

  test("a source change while a killed run waits: the takeover claim resets the run and restarts it", async () => {
    const f = fixture();
    await reset(f);
    expect(await runStagedReconcile(SCOPE, stamp, ops(), deps(killAt("have:batch", 2))).catch((e) => e instanceof Kill)).toBe(true);
    await expire();
    const changed = fixture(4);
    source = changed;
    stamp = current = stampOf(changed, "8");
    const out = await runStagedReconcile(SCOPE, stamp, ops(), deps());
    expect(out).toMatchObject({ status: "done", complete: true, attempts: 2, resumedFrom: null });
    expect(await indexRows()).toEqual(expectedFor(changed, f));
  });

  test("erase_scope during a run: the run and its stage rows go, the running holder gets RH062, a fresh run converges", async () => {
    const f = fixture();
    const expected = memoryReconcile(f);
    await reset(f);
    let erased = -1;
    const err = await runStagedReconcile(SCOPE, stamp, ops(), deps(async (p) => {
      if (p === "have:batch" && erased < 0) {
        expect(await staged_()).toBeGreaterThan(0);
        erased = await transact((tx) => eraseStagedReconcileRuns(tx, SCOPE, ["sess-1"]));
      }
    })).then(() => null, (e: { code?: string }) => e.code);
    expect(erased).toBe(1);
    expect(err).toBe(STAGED_RECONCILE_STALE_LEASE);
    expect([await runs(), await staged_(), await commits()]).toEqual([0, 0, 0]);
    // Erasure by the canonical id reaches an alias scope's run too.
    expect(await code(runStagedReconcile({ ...SCOPE, sessionId: "sess-1-alias" }, stamp, ops(), deps(killAt("want:done"))))).toBeNull();
    expect(await runs()).toBe(1);
    expect(await transact((tx) => eraseStagedReconcileRuns(tx, SCOPE, ["sess-1"]))).toBe(1);
    expect([await runs(), await staged_()]).toEqual([0, 0]);
    expect(await runStagedReconcile(SCOPE, stamp, ops(), deps())).toMatchObject({ status: "done", complete: true });
    expect(await indexRows()).toEqual(expected);
  });

  test("a refused pass 4 write deletes nothing and is not complete; a truncated scan abandons the run", async () => {
    const f = fixture();
    await reset(f);
    const refusing = { ...ops(), regenerate: async () => 0 };
    const before = await indexRows();
    const out = await runStagedReconcile(SCOPE, stamp, refusing, deps());
    expect(out).toMatchObject({ status: "done", complete: false, inserted: 0, wanted: EXPECT.inserted, deleted: 0, reasons: ["search_version_conflict"] });
    expect(await indexRows()).toEqual(before);
    const truncating = { ...ops(), scanPage: async () => ({ rows: [], next: null, truncated: true }) };
    expect(await runStagedReconcile(SCOPE, stamp, truncating, deps())).toEqual({ status: "incomplete", reasons: ["search_index_truncated"] });
    expect((await db.query<{ last_error: string }>("SELECT last_error FROM sessions.search_reconcile_runs_v1")).rows).toEqual([{ last_error: "search_index_truncated" }]);
    expect(await staged_()).toBe(0);
  });

  test("a held scope lock at the final step releases the lease: busy now, resumed at final on the next tick", async () => {
    const f = fixture();
    const expected = memoryReconcile(f);
    await reset(f);
    // Another session's transaction holds the scope's search lock: emulate it with a try-lock that fails once.
    let block = true;
    const out = await runStagedReconcile(SCOPE, stamp, ops(), {
      ...deps(),
      transact: (work) => transact((tx) => work({
        query: (text, params) => block && text.includes("pg_try_advisory_xact_lock") ? ((block = false), Promise.resolve({ rows: [{ ok: false }] })) as never : tx.query(text, params),
      })),
    });
    expect(out).toEqual({ status: "busy", reason: "scope_busy" });
    const next = await runStagedReconcile(SCOPE, stamp, ops(), deps());
    expect(next).toMatchObject({ status: "done", complete: true, attempts: 2, resumedFrom: { phase: "final", cursor: null } });
    expect(await indexRows()).toEqual(expected);
  });

  // The real base path: indexSearchScope's streamedReconcile on one Postgres search backend, the
  // staged run on another, over the same exported manifests. The search index is a separate
  // database, so its writes and deletes commit outside the staging transactions.
  describe("parity with the real streamedReconcile", () => {
    const NOW = new Date("2026-11-01T12:00:00.000Z");
    const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
    const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
    const closers: Array<() => Promise<unknown>> = [];
    afterAll(async () => {
      for (const c of closers.splice(0)) await c();
    });
    const fakeSupabase = (x: ExportedScope) => {
      const tx: SqlExecutor = {
        async query<R>(text: string) {
          if (text.includes("pg_catalog.pg_attribute") && text.includes("recorded_catalog_title")) return { rows: [{ supported: false }] as R[] };
          if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
          if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
            return { rows: x.keys.map((manifest_key) => ({ manifest_key, canonical_id: x.canonicalId })) as R[] };
          if (text.includes("AS canonical_id FROM sessions.session_index")) return { rows: [{ canonical_id: x.canonicalId }] as R[] };
          return { rows: [] as R[] };
        },
      };
      return { tx, transact: async <T,>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
    };
    async function backend() {
      const sdb = new SearchPGlite({ extensions: { pg_trgm: searchTrgm } });
      closers.push(() => sdb.close());
      await sdb.exec("CREATE EXTENSION pg_trgm");
      const tenants: TenantSqlRunner = (orgId, workspaceId, work) =>
        sdb.transaction(async (tx) => {
          await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
          return work(tx);
        });
      const state: SearchProjectionState = {
        async fromMs() { return 0; },
        async record() {},
        async partials() { return { sessions: [], overflow: false }; },
      };
      const adapter = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: sdb }, "writer", tenants);
      await adapter.index.ensureSchema();
      const directory: unknown[] = [];
      const catalog: unknown[] = [];
      const deps = (x: ExportedScope): SearchIndexDeps => ({
        transact: fakeSupabase(x).transact, reader: x.reader, store: x.store, adapter, prefixFor, decompress, now: () => NOW,
        limits: TINY, stream: UNLIMITED,
        writeEvents: async (_tx, _key, input) => { directory.push(input); return { written: true } as never; },
        writeCatalog: (async (_tx: unknown, _key: unknown, value: unknown) => { catalog.push(value); }) as never,
      });
      const scan = async (canonicalId: string) =>
        (await adapter.index.scanFingerprints({ orgId: FX.orgId, workspaceId: FX.workspaceId, source: FX.harness, sessionIds: [FX.sessionId, canonicalId] }, { fromMs: 0, limit: 100_000 })).rows
          .map((r) => JSON.stringify([r.sessionId, r.machineId, r.kind, r.eventId, r.tsMs, r.fingerprint])).sort();
      return { adapter, deps, scan, directory, catalog };
    }
    type Backend = Awaited<ReturnType<typeof backend>>;

    /** The staged run of `x` on backend `b`, its ops built from the real reader and adapter. */
    async function staged(b: Backend, x: ExportedScope, boundary?: StagedReconcileDeps["boundary"]) {
      const d = b.deps(x);
      const s3 = await readCanonicalScope(FX, x.keys, { ...d, decompress }, { ...SEARCH_INDEX_DEFAULT_LIMITS, ...TINY }, NOW.getTime(), x.canonicalId);
      expect(s3.partial && s3.reasons).toEqual(["scope_too_large"]);
      const admitted = s3.admitted!;
      const io = adapterProjectionIo(b.adapter, fakeSupabase(x).tx);
      const sourceStamp: SourceStamp = {
        canonicalId: x.canonicalId, epoch: "0",
        manifestFingerprint: admittedManifestFingerprint(admitted.map((a) => ({ manifestKey: a.key, revision: String(a.manifest.revision) }))),
      };
      const realOps: StagedReconcileOps<StreamedScope> = {
        async readSource() {
          const r = await streamCanonicalScope(FX, admitted, { store: x.store, prefixFor, decompress }, UNLIMITED, NOW.getTime(), 0);
          return r.complete && r.directorySessionId === x.canonicalId && r.events !== null ? r : null;
        },
        wanted: (r) => [...r.wanted].map(([key, w]) => ({ key, ...r.owners[w.owner]!, tsMs: w.tsMs, fingerprint: w.fingerprint })),
        regenerableKinds: (r) => [...S3_ACTIVITY_KINDS, ...r.kinds],
        async scanPage(_tx, after, limit) {
          const all = (await io.scan(FX, x.canonicalId, 0, 1_000_000)).rows
            .map((i) => ({ ...i, key: streamIndexKey(i) }))
            .sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1));
          const start = after === null ? 0 : Number(after);
          const rows = all.slice(start, start + limit);
          return { rows, next: start + rows.length >= all.length ? null : String(start + rows.length).padStart(12, "0") };
        },
        async regenerate(_tx, r, keys) {
          let n = 0;
          await r.regenerate(keys, 5000, async (rows) => { n += await io.upsert(rows, FX, x.canonicalId); });
          return n;
        },
        currentSource: async () => sourceStamp,
        remove: async (_tx, ids) => io.remove(FX, ids),
        async commit(_tx, r, pass) {
          if (pass.complete) {
            b.directory.push({ sourceSessionId: FX.sessionId, expectedEpoch: "0", sources: r.sources, events: r.events });
            b.catalog.push(r.catalog);
          }
          return { complete: pass.complete, reasons: [] };
        },
      };
      return runStagedReconcile(FX, sourceStamp, realOps, { transact, stageBatch: 7, scanPage: 9, writeKeys: 8, deletePage: 5, ...(boundary ? { boundary } : {}) });
    }

    let x1: ExportedScope;
    let x2: ExportedScope;
    let base: Backend;
    let baseResult: Awaited<ReturnType<typeof indexSearchScope>>;
    beforeAll(async () => {
      x1 = await exportStreamScope(generate(31), 1200);
      x2 = await exportStreamScope(generate(33), 1200);
      closers.push(x1.dispose, x2.dispose);
      base = await backend();
      expect(await indexSearchScope(FX, base.deps(x1))).toMatchObject({ complete: true });
      baseResult = await indexSearchScope(FX, base.deps(x2));
    });

    for (const point of [null, "want:batch", "have:batch", "write:page", "final:removed", "final:committed"] as const)
      test(`staged ${point ? `killed at ${point}, resumed` : "uninterrupted"}: the same index rows, directory events and catalog as streamedReconcile`, async () => {
        // The base pass streamed, inserted and deleted: the comparison covers both.
        expect(baseResult).toMatchObject({ complete: true, reasons: [] });
        expect(baseResult.stream?.passes).toBe(4);
        expect(baseResult.inserted).toBeGreaterThan(0);
        expect(baseResult.deleted).toBeGreaterThan(0);
        await db.exec("DELETE FROM sessions.search_reconcile_runs_v1");
        const b = await backend();
        expect(await indexSearchScope(FX, b.deps(x1))).toMatchObject({ complete: true });
        b.directory.length = 0;
        b.catalog.length = 0;
        if (point) {
          expect(await staged(b, x2, killAt(point)).then(() => "ran", (e) => (e instanceof Kill ? e.message : e))).toBe(point);
          await expire();
        }
        const out = await staged(b, x2);
        expect(out).toMatchObject({ status: "done", complete: true, reasons: [] });
        if (!point) expect(out).toMatchObject({ inserted: baseResult.inserted, deleted: baseResult.deleted });
        expect(await b.scan(x2.canonicalId)).toEqual(await base.scan(x2.canonicalId));
        // The arrays stand in for writes the real commit makes on the final transaction: a kill
        // after the commit callback leaves its rolled-back entry here, so compare the last one.
        expect(b.directory.length).toBe(point === "final:committed" ? 2 : 1);
        expect(b.directory.slice(-1)).toEqual(base.directory.slice(-1));
        expect(b.catalog.slice(-1)).toEqual(base.catalog.slice(-1));
        expect([await runs(), await staged_()]).toEqual([0, 0]);
      });
  });
});

describe("20261105062110 rollback", () => {
  test("the checksum-pinned down after the up leaves no table, no function and no ledger row; a rerun refuses", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    const db = await migrated();
    try {
      const later = (await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).reverse();
      if (later.length) {
        expect(await code(db.exec(DOWN))).toBe("55000");
        await db.exec("ROLLBACK");
      }
      for (const step of later) await db.exec(read(`supabase/rollbacks/${step.name.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`));
      // A run under a live lease refuses the down; an expired one does not.
      await db.exec("INSERT INTO sessions.search_reconcile_runs_v1 (org_id, workspace_id, harness, session_id, canonical_id, pointer_epoch, manifest_fingerprint, lease_nonce, lease_ms, lease_expires_at) VALUES ('o','w','h','s','s',1,'f',gen_random_uuid(),60000,clock_timestamp() + interval '1 minute')");
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      await db.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
      await db.exec(DOWN);
      for (const t of TABLES) expect((await db.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [t])).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'search\\_reconcile\\_%'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      // The up applies again cleanly after the down.
      await db.exec(SQL);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.search_reconcile_runs_v1')::text r")).rows[0]!.r).toBe("sessions.search_reconcile_runs_v1");
    } finally {
      await db.close();
    }
  });
});
