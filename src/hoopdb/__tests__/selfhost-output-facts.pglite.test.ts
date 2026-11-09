/**
 * Migration 20261105045020 (self-host output facts in Postgres) on PGlite with every migration:
 * the fail-closed role check, RLS on with no table grants, the writer functions as
 * search_consumer_runtime only (driven through postgresOutputFactsStore with the shared
 * builder's rows), the upsert, the stale-version rejection, the erase, the state rows, the
 * narrow readers as bulls96_read_api under the read Worker's tenant binding, and down/up.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { outputFactsFromScope, type OutputFactsScope } from "../../session-pipeline/output-facts.ts";
import { postgresOutputFactsStore } from "../../session-pipeline/output-facts-store.ts";
import { OUTPUT_FIXTURE_SCOPE, outputFixture } from "../../session-pipeline/__tests__/output-facts-fixture.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105045020_selfhost_output_facts.sql";
const DOWN = "supabase/rollbacks/20261105045020_selfhost_output_facts.down.sql";
const TABLES = ["session_event_facts", "session_output_facts", "output_facts_session_state", "output_facts_workspace_state"];
const WRITERS = ["output_facts_state", "output_facts_reconcile", "output_facts_record_state", "output_facts_erase"];
const READERS = ["output_facts_events_for", "output_facts_outputs_for", "output_facts_coverage_for"];
const HELPERS = ["output_facts_writer_scope", "output_facts_stored_version"];
const SCOPE = OUTPUT_FIXTURE_SCOPE;
const AT = new Date("2026-11-09T12:00:00Z");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  return db;
}

/** Every migration in order up to this one; `before` runs just before it. */
async function migrate(db: PGlite, before?: () => Promise<unknown>): Promise<{ code?: string } | null> {
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    const path = `supabase/migrations/${file}`;
    if (path === MIGRATION) {
      await before?.();
      return db.exec(read(MIGRATION)).then(() => null, (e: { code?: string }) => e);
    }
    await db.exec(read(path));
  }
  throw new Error("migration not found");
}

async function database(): Promise<PGlite> {
  const db = await fresh();
  const error = await migrate(db);
  if (error) throw error;
  // The post-migrate role bootstraps: the read API's own file, and for the consumer the one
  // grant of session_consumer_role.sql these functions need (schema USAGE). The full consumer
  // role file plus its verifier (which allowlists the four writers) run end to end in
  // consumer-service.test.ts ("writer role SQL").
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime");
  return db;
}

const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

async function as<T>(db: PGlite, role: string, tenant: [string, string] | null, work: () => Promise<T>): Promise<T> {
  await db.exec(`SET ROLE ${role}`);
  try {
    await db.exec("BEGIN");
    try {
      if (tenant) await db.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await db.exec("COMMIT");
      return out;
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  } finally {
    await db.exec("RESET ROLE");
  }
}

const consumer = <T>(db: PGlite, work: (store: ReturnType<typeof postgresOutputFactsStore>) => Promise<T>) =>
  as(db, "search_consumer_runtime", null, () => work(postgresOutputFactsStore(db as unknown as SqlExecutor)));
const facts = (scope: OutputFactsScope = SCOPE) => outputFactsFromScope(scope, outputFixture(), 1);
/** The same rows (same row_id: the identity excludes the tenant) under another workspace. */
const retenant = (f: ReturnType<typeof facts>, workspaceId: string) => ({
  ...f, events: f.events.map((e) => ({ ...e, workspace_id: workspaceId })), facts: f.facts.map((x) => ({ ...x, workspace_id: workspaceId })) });
const input = (f = facts()) => ({ eventFacts: f.events, outputFacts: f.facts, complete: f.complete, reasons: f.reasons });
const count = async (db: PGlite, table: string, where = "true") =>
  Number((await db.query<{ n: number | string }>(`SELECT count(*) AS n FROM sessions.${table} WHERE ${where}`)).rows[0]!.n);
const WINDOW = ["2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"];

describe("20261105045020 self-host output facts", () => {
  test("fails closed (55000) when the consumer runtime role exists elevated", async () => {
    const db = await fresh();
    expect((await migrate(db, () => db.exec("ALTER ROLE search_consumer_runtime INHERIT")))?.code).toBe("55000");
  }, 180_000);

  test("RLS on, no policy and no table grant; functions are SECURITY DEFINER with exactly their grants", async () => {
    const db = await database();
    const rls = await db.query<{ relname: string; relrowsecurity: boolean; policies: number | string }>(
      `SELECT c.relname, c.relrowsecurity, (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS policies
         FROM pg_class c WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname = ANY ($1) ORDER BY c.relname`, [TABLES]);
    expect(rls.rows.map((r) => [r.relname, r.relrowsecurity, Number(r.policies)])).toEqual(
      [...TABLES].sort().map((t) => [t, true, 0]));
    for (const role of ["search_consumer_runtime", "bulls96_read_api", "anon", "authenticated", "service_role"])
      for (const t of TABLES) {
        const { rows } = await db.query<{ ok: boolean }>(
          `SELECT has_table_privilege($1, $2, 'SELECT') OR has_table_privilege($1, $2, 'INSERT')
               OR has_table_privilege($1, $2, 'UPDATE') OR has_table_privilege($1, $2, 'DELETE') AS ok`, [role, `sessions.${t}`]);
        expect([role, t, rows[0]!.ok]).toEqual([role, t, false]);
      }
    const fns = await db.query<{ proname: string; prosecdef: boolean; consumer: boolean; reader: boolean; pub: boolean; cfg: string[] | null }>(
      `SELECT p.proname, p.prosecdef, p.proconfig AS cfg,
              has_function_privilege('search_consumer_runtime', p.oid, 'EXECUTE') AS consumer,
              has_function_privilege('bulls96_read_api', p.oid, 'EXECUTE') AS reader,
              EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0) AS pub
         FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace AND p.proname LIKE 'output\\_facts\\_%' ORDER BY p.proname`);
    const by = Object.fromEntries(fns.rows.map((r) => [r.proname, r]));
    expect(Object.keys(by).sort()).toEqual([...WRITERS, ...READERS, ...HELPERS].sort());
    for (const f of WRITERS) expect([f, by[f]!.prosecdef, by[f]!.consumer, by[f]!.reader, by[f]!.pub]).toEqual([f, true, true, false, false]);
    for (const f of READERS) expect([f, by[f]!.prosecdef, by[f]!.consumer, by[f]!.reader, by[f]!.pub]).toEqual([f, true, false, true, false]);
    for (const f of HELPERS) expect([f, by[f]!.prosecdef, by[f]!.consumer, by[f]!.reader, by[f]!.pub]).toEqual([f, false, false, false, false]);
    for (const r of fns.rows) expect(r.cfg?.some((c) => c.startsWith("search_path=pg_catalog"))).toBe(true);
    // Direct table access as either role: 42501.
    for (const role of ["search_consumer_runtime", "bulls96_read_api"])
      for (const t of TABLES) expect(await code(as(db, role, ["org-f4", "ws-f4"], () => db.query(`SELECT 1 FROM sessions.${t}`)))).toBe("42501");
  }, 180_000);

  test("writers refuse every role but the consumer (42501) and an invalid scope or row (22023)", async () => {
    const db = await database();
    const f = facts();
    for (const role of ["bulls96_read_api", "anon", "authenticated", "service_role"])
      expect(await code(as(db, role, ["org-f4", "ws-f4"], () => postgresOutputFactsStore(db as unknown as SqlExecutor).reconcile(SCOPE, input(f), 5)))).toBe("42501");
    // The owner (table owner, not the consumer) is refused by the writer's own caller check too.
    expect(await code(postgresOutputFactsStore(db as unknown as SqlExecutor).erase(SCOPE))).toBe("42501");
    // The canonical id outside the session set, or a row of another tenant/session: 22023, nothing written.
    expect(await code(consumer(db, (s) => s.reconcile({ ...SCOPE, canonicalId: "other" }, input(f), 5)))).toBe("22023");
    const foreign = { ...input(f), eventFacts: f.events.map((e, i) => (i === 0 ? { ...e, workspace_id: "ws-other" } : e)) };
    expect(await code(consumer(db, (s) => s.reconcile(SCOPE, foreign, 5)))).toBe("22023");
    const outside = { ...input(f), outputFacts: f.facts.map((x, i) => (i === 0 ? { ...x, session_id: "not-in-scope" } : x)) };
    expect(await code(consumer(db, (s) => s.reconcile(SCOPE, outside, 5)))).toBe("22023");
    expect(await count(db, "session_event_facts") + await count(db, "session_output_facts")).toBe(0);
  }, 180_000);

  test("upsert is idempotent, a complete pass deletes what it no longer carries, an incomplete one never deletes", async () => {
    const db = await database();
    const f = facts();
    expect(f.events.length).toBeGreaterThan(0);
    expect(f.facts.length).toBeGreaterThan(0);
    const first = await consumer(db, (s) => s.reconcile(SCOPE, input(f), 5));
    expect(first).toMatchObject({ applied: true, inserted: f.events.length + f.facts.length, deleted: 0 });
    // The same rows again: no row changes (fingerprint unchanged).
    expect(await consumer(db, (s) => s.reconcile(SCOPE, input(f), 5))).toMatchObject({ applied: true, inserted: 0, deleted: 0 });
    // The stored rows round-trip the builder's columns (timestamps, arrays, the canonical id).
    const ev = (await db.query<{ ts: string; files_touched: string[]; session_id: string; source_version: number | string }>(
      `SELECT to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS ts, files_touched, session_id, source_version
         FROM sessions.session_event_facts WHERE row_id = $1`, [f.events[0]!.row_id])).rows[0]!;
    expect(ev.ts).toBe(f.events[0]!.ts.padEnd(26, "0"));
    expect(ev.files_touched).toEqual(f.events[0]!.files_touched as string[]);
    expect(ev.session_id).toBe(SCOPE.canonicalId);
    expect(Number(ev.source_version)).toBe(5);
    // An incomplete pass that drops one fact deletes nothing.
    const fewer = { ...input(f), outputFacts: f.facts.slice(1) };
    expect(await consumer(db, (s) => s.reconcile(SCOPE, { ...fewer, complete: false }, 6))).toMatchObject({ applied: true, deleted: 0 });
    expect(await count(db, "session_output_facts")).toBe(f.facts.length);
    // The complete pass deletes exactly that fact.
    expect(await consumer(db, (s) => s.reconcile(SCOPE, fewer, 6))).toMatchObject({ applied: true, inserted: 0, deleted: 1 });
    expect(await count(db, "session_output_facts")).toBe(f.facts.length - 1);
    // Another workspace with the same identities (same row_id) is a separate row: tenant-qualified PK.
    const other = { ...SCOPE, workspaceId: "ws-other" };
    await consumer(db, (s) => s.reconcile(other, input(retenant(f, other.workspaceId)), 1));
    expect(await count(db, "session_output_facts", "workspace_id = 'ws-other'")).toBe(f.facts.length);
    expect(await count(db, "session_output_facts", "workspace_id = 'ws-f4'")).toBe(f.facts.length - 1);
  }, 180_000);

  test("a pass older than the stored source version is a no-op (reconcile and state)", async () => {
    const db = await database();
    const f = facts();
    await consumer(db, async (s) => {
      await s.reconcile(SCOPE, input(f), 7);
      expect(await s.recordState(SCOPE, { complete: true, at: AT, range: f.range ?? null, sourceVersion: 7 })).toBe(true);
    });
    // Stale: version 6 drops every fact; nothing changes and the result says so.
    const stale = await consumer(db, (s) => s.reconcile(SCOPE, { ...input(f), outputFacts: [] }, 6));
    expect(stale).toEqual({ applied: false, inserted: 0, deleted: 0, complete: false, reasons: expect.arrayContaining(["output_facts_stale_source_version"]) });
    expect(await count(db, "session_output_facts")).toBe(f.facts.length);
    expect(await consumer(db, (s) => s.recordState(SCOPE, { complete: false, at: AT, range: null, sourceVersion: 6 }))).toBe(false);
    expect(await consumer(db, (s) => s.previousState(SCOPE))).toEqual({ range: f.range ?? null, version: 7 });
    // The same version is not stale (a retry of the same pass).
    expect(await consumer(db, (s) => s.reconcile(SCOPE, input(f), 7))).toMatchObject({ applied: true });
  }, 180_000);

  test("state rows: one per session id, span and completeness recorded, pending never complete", async () => {
    const db = await database();
    const range = { min: "2026-11-01 10:00:00.000000", max: "2026-11-02 11:30:00.250000" };
    await consumer(db, (s) => s.recordState(SCOPE, { complete: true, at: AT, range, phase: "pending", sourceVersion: 3 }));
    expect(await count(db, "output_facts_session_state", "complete")).toBe(0);
    await consumer(db, (s) => s.recordState(SCOPE, { complete: true, at: AT, range, sourceVersion: 3 }));
    const rows = await db.query<{ session_id: string; complete: boolean; v: number | string }>(
      `SELECT session_id, complete, source_version AS v FROM sessions.output_facts_session_state ORDER BY session_id`);
    expect(rows.rows.map((r) => [r.session_id, r.complete, Number(r.v)])).toEqual([...SCOPE.sessionIds].sort().map((id) => [id, true, 3]));
    expect(await consumer(db, (s) => s.previousState(SCOPE))).toEqual({ range, version: 3 });
    // A scope with no state: no range, version 0.
    expect(await consumer(db, (s) => s.previousState({ ...SCOPE, workspaceId: "ws-none" }))).toEqual({ range: null, version: 0 });
  }, 180_000);

  test("erase removes the scope's facts and state rows only", async () => {
    const db = await database();
    const f = facts();
    const other = { ...SCOPE, workspaceId: "ws-other" };
    await consumer(db, async (s) => {
      await s.reconcile(SCOPE, input(f), 9);
      await s.recordState(SCOPE, { complete: true, at: AT, range: f.range ?? null, sourceVersion: 9 });
      await s.reconcile(other, input(retenant(f, other.workspaceId)), 1);
      await s.recordState(other, { complete: true, at: AT, range: f.range ?? null, sourceVersion: 1 });
    });
    await consumer(db, (s) => s.erase(SCOPE));
    for (const t of ["session_event_facts", "session_output_facts", "output_facts_session_state"]) {
      expect(await count(db, t, "workspace_id = 'ws-f4'")).toBe(0);
      expect(await count(db, t, "workspace_id = 'ws-other'")).toBeGreaterThan(0);
    }
    // A re-upload starts from a fresh (lower) epoch: no leftover version refuses it.
    expect(await consumer(db, (s) => s.reconcile(SCOPE, input(f), 1))).toMatchObject({ applied: true });
  }, 180_000);

  test("readers: bulls96_read_api reads only its bound tenant; the consumer cannot read", async () => {
    const db = await database();
    const f = facts();
    await consumer(db, async (s) => {
      await s.reconcile(SCOPE, input(f), 2);
      await s.recordState(SCOPE, { complete: true, at: AT, range: f.range ?? null, sourceVersion: 2 });
    });
    const events = (tenant: [string, string] | null, ws = "ws-f4", limit = 10000, after: string | null = null) =>
      as(db, "bulls96_read_api", tenant, () => db.query<{ row_id: string }>(
        "SELECT row_id FROM sessions.output_facts_events_for($1, $2, $3, $4, $5)", [ws, ...WINDOW, after, limit]));
    const outputs = (tenant: [string, string]) => as(db, "bulls96_read_api", tenant, () => db.query<{ row_id: string }>(
      "SELECT row_id FROM sessions.output_facts_outputs_for($1, $2, $3, NULL, 10000)", ["ws-f4", ...WINDOW]));
    expect((await events(["org-f4", "ws-f4"])).rows.map((r) => r.row_id)).toEqual(f.events.map((e) => e.row_id).sort());
    expect((await outputs(["org-f4", "ws-f4"])).rows.map((r) => r.row_id)).toEqual(f.facts.map((e) => e.row_id).sort());
    // Keyset paging.
    const page = (await events(["org-f4", "ws-f4"], "ws-f4", 1)).rows;
    expect(page.length).toBe(1);
    if (f.events.length > 1) expect((await events(["org-f4", "ws-f4"], "ws-f4", 1, page[0]!.row_id)).rows[0]!.row_id > page[0]!.row_id).toBe(true);
    const cov = await as(db, "bulls96_read_api", ["org-f4", "ws-f4"], () => db.query<{ complete_since: string | null; sessions: number | string; incomplete: number | string }>(
      "SELECT * FROM sessions.output_facts_coverage_for($1, $2, $3)", ["ws-f4", ...WINDOW]));
    expect(cov.rows.map((r) => [r.complete_since, Number(r.sessions), Number(r.incomplete)])).toEqual([[null, SCOPE.sessionIds.length, 0]]);
    // Another org bound to the same workspace id sees nothing; unbound or another workspace: 42501.
    expect((await events(["org-b", "ws-f4"])).rows).toEqual([]);
    expect(await code(events(null))).toBe("42501");
    expect(await code(events(["org-f4", "ws-f4"], "ws-b"))).toBe("42501");
    expect(await code(events(["org-f4", "ws-f4"], "ws-f4", 10001))).toBe("22023");
    expect(await code(as(db, "search_consumer_runtime", ["org-f4", "ws-f4"], () => db.query(
      "SELECT * FROM sessions.output_facts_events_for($1, $2, $3, NULL, 10)", ["ws-f4", ...WINDOW])))).toBe("42501");
  }, 180_000);

  test("down then up again", async () => {
    const db = await database();
    await db.exec(read(DOWN));
    for (const t of TABLES)
      expect((await db.query<{ t: string | null }>(`SELECT to_regclass('sessions.${t}')::text AS t`)).rows[0]!.t).toBeNull();
    expect(Number((await db.query<{ n: number | string }>(
      "SELECT count(*) AS n FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname LIKE 'output\\_facts\\_%'")).rows[0]!.n)).toBe(0);
    await db.exec(read(MIGRATION));
    await consumer(db, (s) => s.reconcile(SCOPE, input(), 1));
  }, 180_000);
});
