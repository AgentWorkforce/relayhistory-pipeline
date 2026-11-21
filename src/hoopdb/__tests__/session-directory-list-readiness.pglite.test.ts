/**
 * 20261105062200 session list readiness on PGlite with the full migration plan:
 * - inventory, sentinel and grants: two SECURITY DEFINER, STABLE functions with a pinned search_path, EXECUTE for
 *   bulls96_read_api alone (anon, authenticated, service_role and the runtime roles cannot), not runtime functions;
 * - the marker: events_ready equals event_projection_ready AND event_projection_version = 1 for a fresh row (pointer
 *   written, no index pass), a ready row, a row a pointer change made not ready again, and a wrong-version row;
 * - readyOnly hides exactly the rows that are not ready, and with readyOnly off each function returns exactly its
 *   neighbour's rows (list_session_directory_for, query_session_directory_for) for every filter;
 * - the predicate sits before the keyset and LIMIT: multi-page walks (plain and q) through the read API, with
 *   not-ready rows and equal timestamps on the page boundaries, give every ready row once, no short page with a
 *   nextCursor; a cursor replayed across a readyOnly change is invalid_cursor;
 * - the builder anchor refusal (55000) and the checksum-pinned rollback.
 * This file's rollback test takes every later upstream step off first (newest first), so a later slot does not
 * break it.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite, type Transaction } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf, RUNTIME_APPLICATION_FUNCTIONS } from "../migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { poolTenantRunner } from "../../session-read-api/liveness.ts";
import { memoryLiveness } from "../../session-read-api/__tests__/support.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062200_session_directory_list_readiness.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062200_session_directory_list_readiness.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const T = "timestamp with time zone";
const A12 = Array(12).fill("text[]").join(",");
const LIST = `sessions.list_session_directory_readiness_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer,boolean)`;
const QUERY = `sessions.query_session_directory_readiness_for(text,${A12},boolean,text[],text[],text,text[],text,${T},${T},${T},${T},${T},text,text,integer,boolean)`;
const OLD_LIST = `sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer)`;
const OLD_QUERY = `sessions.query_session_directory_for(text,${A12},boolean,text[],text[],text,text[],text,${T},${T},${T},${T},${T},text,text,integer)`;

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;
async function migrated() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await runMigrations(client(db), { profile: LOCAL });
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
  return db;
}

type Session = { id: string; last: string; created?: string; harness?: string; workspace?: string; org?: string; title?: string; project?: string; users?: string[]; agents?: string[]; state?: string; ready?: boolean; deleted?: boolean };
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions) {
    const org = s.org ?? "org-a", ws = s.workspace ?? "ws-a", harness = s.harness ?? "codex";
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, agent_names, project_id, state,
          user_ids, search_text, deleted_at, catalog_projected_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::text, $8::text[], $9::text, $10, $11::text[],
               sessions.session_directory_search_text($7::text, NULL, ($8::text[])[1], $9::text, NULL, NULL, NULL, NULL),
               CASE WHEN $12::boolean THEN now() END, now())`,
      [org, ws, harness, s.id, s.created ?? s.last, s.last, s.title ?? null, s.agents ?? [], s.project ?? null, s.state ?? "unknown", s.users ?? [], s.deleted ?? false],
    );
    if (s.deleted) continue;
    for (const user of s.users ?? [])
      await db.query("INSERT INTO sessions.session_directory_users VALUES ($1, $2, $3, $4, $5, $6)", [org, ws, harness, s.id, user, s.last]);
    for (const agent of s.agents ?? [])
      await db.query(
        `INSERT INTO sessions.session_directory_agents (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
         VALUES ($1, $2, $3, $4, $5, lower(btrim($5)), $6)`, [org, ws, harness, s.id, agent, s.last]);
    if (s.ready) await markReady(db, s.id, s.last, harness, org, ws);
  }
}
/** What an index pass writes when it completes a row (the session_directory_events_ready constraint's shape). */
const markReady = (db: PGlite, id: string, at: string, harness = "codex", org = "org-a", ws = "ws-a") =>
  db.query(
    `UPDATE sessions.session_directory SET event_projection_version = 1, event_projection_ready = true,
            event_projected_at = $1::timestamptz + interval '1 minute', event_projection_sources = '{"m1":1}'::jsonb
      WHERE org_id = $2 AND workspace_id = $3 AND harness = $4 AND session_id = $5`, [at, org, ws, harness, id]);

describe("20261105062200 session list readiness", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope: [string, string] = ["org-a", "ws-a"]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  type Row = Record<string, unknown> & { session_id: string; events_ready?: boolean; event_projected_at?: Date | null };
  /** Named arguments (every one, as directory.ts passes them); `fn` is the old or the readiness function. */
  const call = (fn: string, args: Record<string, unknown>, readyOnly?: boolean) =>
    as("bulls96_read_api", async (tx) => {
      const all = { p_workspace_id: "ws-a", p_limit: 200, ...args, ...(readyOnly === undefined ? {} : { p_ready_only: readyOnly }) };
      const keys = Object.keys(all);
      const typed = (k: string, i: number) => `${k} => $${i + 1}`;
      return (await tx.query<Row>(`SELECT * FROM sessions.${fn}(${keys.map(typed).join(", ")})`, keys.map((k) => (all as Record<string, unknown>)[k]))).rows;
    });
  const listNew = (args: Record<string, unknown>, readyOnly: boolean) => call("list_session_directory_readiness_for", LIST_DEFAULTS(args), readyOnly);
  const listOld = (args: Record<string, unknown>) => call("list_session_directory_for", args);
  const queryNew = (args: Record<string, unknown>, readyOnly: boolean) => call("query_session_directory_readiness_for", QUERY_DEFAULTS(args), readyOnly);
  const queryOld = (args: Record<string, unknown>) => call("query_session_directory_for", args);
  // The readiness functions take no DEFAULTs: every argument is passed, NULL where unused.
  const LIST_DEFAULTS = (a: Record<string, unknown>) => ({
    p_sources: null, p_not_sources: null, p_project_contains: null, p_user_ids: null, p_agent: null,
    p_last_ts_after: null, p_last_ts_before: null, p_first_ts_after: null, p_first_ts_before: null,
    p_session_harnesses: null, p_session_ids: null, p_cursor_last_activity_at: null, p_cursor_session_id: null, p_cursor_harness: null, ...a,
  });
  const QUERY_DEFAULTS = (a: Record<string, unknown>) => ({
    p_free: null, p_not_free: null, p_title: null, p_not_title: null, p_project: null, p_not_project: null, p_harness: null,
    p_not_harness: null, p_agent_terms: null, p_not_agent_terms: null, p_id: null, p_not_id: null, p_active: null,
    p_sources: null, p_not_sources: null, p_project_contains: null, p_user_ids: null, p_agent: null,
    p_last_ts_after: null, p_last_ts_before: null, p_first_ts_after: null, p_first_ts_before: null,
    p_cursor_last_activity_at: null, p_cursor_session_id: null, p_cursor_harness: null, ...a,
  });
  const strip = (rows: Row[]) => rows.map(({ events_ready: _r, event_projected_at: _p, ...rest }) => rest);
  const reset = () => db.exec("DELETE FROM sessions.session_directory_agents; DELETE FROM sessions.session_directory_users; DELETE FROM sessions.session_index; DELETE FROM sessions.session_directory");

  beforeAll(async () => { db = await migrated(); });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and grants: two definer functions, EXECUTE for bulls96_read_api only, not runtime functions; the read-API verifier passes", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`)).toEqual([
      "function:sessions.list_session_directory_readiness_for", "function:sessions.query_session_directory_readiness_for",
    ]);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    for (const fn of [LIST, QUERY]) {
      expect(RUNTIME_APPLICATION_FUNCTIONS).not.toContain(fn);
      const r = (await db.query<{ secdef: boolean; volatile: string; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.provolatile volatile, p.proconfig cfg, has_function_privilege('public', p.oid, 'EXECUTE') public_exec FROM pg_proc p WHERE p.oid = $1::regprocedure", [fn])).rows[0]!;
      expect(r).toEqual({ secdef: true, volatile: "s", cfg: ["search_path=pg_catalog, sessions, pg_temp"], public_exec: false });
      for (const role of ["anon", "authenticated", "service_role", "search_consumer_runtime", "bulls96_session_ingest", "bulls96_session_ingest_seeder", "session_directory_search_worker"])
        expect([fn, role, (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok]).toEqual([fn, role, false]);
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') ok", [fn])).rows[0]!.ok).toBe(true);
    }
    // The neighbours keep their grants.
    for (const fn of [OLD_LIST, OLD_QUERY])
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') ok", [fn])).rows[0]!.ok).toBe(true);
    for (const role of ["anon", "authenticated", "service_role"]) {
      expect([role, await code(as(role, (tx) => tx.query(`SELECT * FROM sessions.list_session_directory_readiness_for(p_workspace_id => 'ws-a', p_sources => NULL, p_not_sources => NULL, p_project_contains => NULL, p_user_ids => NULL, p_agent => NULL, p_last_ts_after => NULL, p_last_ts_before => NULL, p_first_ts_after => NULL, p_first_ts_before => NULL, p_session_harnesses => NULL, p_session_ids => NULL, p_cursor_last_activity_at => NULL, p_cursor_session_id => NULL, p_cursor_harness => NULL, p_limit => 10, p_ready_only => true)`)))]).toEqual([role, "42501"]);
    }
    // bulls96_read_api with no or another workspace binding is refused by the scope helper.
    expect(await code(listNew({ p_workspace_id: "ws-b" }, true))).toBe("42501");
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("the marker: fresh (pointer, no index pass), ready, re-dirtied by a pointer change, wrong version; readyOnly keeps only the ready row", async () => {
    await reset();
    await seed(db, [
      { id: "fresh", last: "2026-10-05T00:00:00Z" },
      { id: "ready", last: "2026-10-04T00:00:00Z", ready: true },
      { id: "redirty", last: "2026-10-03T00:00:00Z", ready: true },
      { id: "version0", last: "2026-10-02T00:00:00Z" },
    ]);
    const pointer = (id: string, key: string, revision: number) => db.query(
      `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision,event_count)
       VALUES('org-a','ws-a','codex',$1,'m1','u1','2026-10-01','2026-10-02','s3','b',$2,'ndjson','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',$3,1)`, [id, key, revision]);
    // A pointer write on a not-yet-indexed session: still not ready.
    await pointer("fresh", "key-fresh", 1);
    // A pointer for an indexed session, then the index pass completes it.
    await pointer("redirty", "key-redirty", 1);
    await markReady(db, "redirty", "2026-10-03T00:00:00Z");
    const projected = (await db.query<{ at: Date }>("SELECT event_projected_at at FROM sessions.session_directory WHERE session_id = 'redirty'")).rows[0]!.at;
    // A new revision (an upload) makes it not ready again; the last projection time stays as stored.
    await db.query("UPDATE sessions.session_index SET revision = 2 WHERE session_id = 'redirty'");
    // A ready-but-version-0 row cannot exist (the table constraint), so version 0 is always not ready.
    expect(await code(db.query("UPDATE sessions.session_directory SET event_projection_ready = true, event_projected_at = now(), event_projection_sources = '{\"m1\":1}' WHERE session_id = 'version0'"))).toBe("23514");
    const state = (await db.query<{ session_id: string; r: boolean; v: number }>(
      "SELECT session_id, event_projection_ready r, event_projection_version v FROM sessions.session_directory ORDER BY last_activity_at DESC")).rows;
    expect(state.map((s) => [s.session_id, s.r, s.v])).toEqual([["fresh", false, 0], ["ready", true, 1], ["redirty", false, 1], ["version0", false, 0]]);

    const all = await listNew({}, false);
    expect(all.map((r) => [r.session_id, r.events_ready])).toEqual([["fresh", false], ["ready", true], ["redirty", false], ["version0", false]]);
    expect(all.find((r) => r.session_id === "fresh")!.event_projected_at).toBeNull();
    expect(all.find((r) => r.session_id === "version0")!.event_projected_at).toBeNull();
    expect(new Date(all.find((r) => r.session_id === "ready")!.event_projected_at!).toISOString()).toBe("2026-10-04T00:01:00.000Z");
    expect(new Date(all.find((r) => r.session_id === "redirty")!.event_projected_at!).toISOString()).toBe(new Date(projected).toISOString());
    // events_ready is exactly the stored readiness predicate, row by row.
    for (const s of state) expect([s.session_id, all.find((r) => r.session_id === s.session_id)!.events_ready]).toEqual([s.session_id, s.r && s.v === 1]);
    expect((await listNew({}, true)).map((r) => r.session_id)).toEqual(["ready"]);
    expect((await queryNew({ p_id: ["e"] }, false)).map((r) => [r.session_id, r.events_ready])).toEqual([["fresh", false], ["ready", true], ["redirty", false], ["version0", false]]);
    expect((await queryNew({ p_id: ["e"] }, true)).map((r) => r.session_id)).toEqual(["ready"]);
    // NULL p_ready_only reads as off.
    expect((await listNew({}, null as never)).length).toBe(4);
  });

  const FIXTURE: Session[] = [
    { id: "s01", last: "2026-10-09T00:00:00Z", created: "2026-10-01T00:00:00Z", title: "alpha one", project: "github.com/acme/api", users: ["u1"], agents: ["Lead"], state: "active", ready: true },
    { id: "s02", harness: "claude", last: "2026-10-08T00:00:00Z", created: "2026-10-02T00:00:00Z", title: "alpha two", project: "github.com/acme/web", users: ["u2"], ready: false },
    { id: "s03", last: "2026-10-07T00:00:00Z", created: "2026-10-03T00:00:00Z", title: "alpha three", users: ["u1", "u2"], agents: ["worker"], ready: true },
    { id: "s04", harness: "gemini", last: "2026-10-06T00:00:00Z", created: "2026-10-04T00:00:00Z", title: "alpha four", project: "github.com/acme/api", ready: false },
    { id: "s05", last: "2026-10-05T00:00:00Z", created: "2026-10-04T12:00:00Z", title: "beta five", users: ["u1"], agents: ["Lead"], ready: true },
    { id: "s06", harness: "claude", last: "2026-10-04T00:00:00Z", created: "2026-10-01T12:00:00Z", title: "alpha six", users: ["u2"], ready: true },
    { id: "s07", last: "2026-10-03T00:00:00Z", created: "2026-10-02T12:00:00Z", title: "alpha seven", project: "github.com/acme/api", state: "active", ready: false },
    { id: "gone", last: "2026-10-10T00:00:00Z", title: "alpha gone", ready: false, deleted: true },
    { id: "other-ws", workspace: "ws-b", last: "2026-10-10T00:00:00Z", title: "alpha elsewhere", ready: true },
    { id: "other-org", org: "org-b", last: "2026-10-10T00:00:00Z", title: "alpha elsewhere", ready: true },
  ];
  const LIST_FILTERS: Array<Record<string, unknown>> = [
    {},
    { p_sources: ["codex"] }, { p_sources: ["codex", "claude"] }, { p_not_sources: ["claude"] },
    { p_user_ids: ["u1"] }, { p_user_ids: ["u1", "u2"] }, { p_agent: "lead" }, { p_project_contains: "acme/api" },
    { p_last_ts_after: "2026-10-04T00:00:00Z" }, { p_last_ts_before: "2026-10-06T00:00:00Z" },
    { p_first_ts_after: "2026-10-02T00:00:00Z" }, { p_first_ts_before: "2026-10-03T00:00:00Z" },
    { p_sources: ["codex"], p_user_ids: ["u1"], p_agent: "lead", p_last_ts_after: "2026-10-01T00:00:00Z", p_last_ts_before: "2026-10-10T00:00:00Z" },
    { p_session_harnesses: ["codex", "claude"], p_session_ids: ["s01", "s02"] },
    { p_cursor_last_activity_at: "2026-10-07T00:00:00Z", p_cursor_session_id: "s03", p_cursor_harness: "codex" },
    { p_limit: 2 }, { p_limit: 2, p_cursor_last_activity_at: "2026-10-08T00:00:00Z", p_cursor_session_id: "s02", p_cursor_harness: "claude" },
  ];
  const QUERY_FILTERS: Array<Record<string, unknown>> = [
    { p_free: ["alpha"] }, { p_free: ["alpha"], p_not_free: ["six"] }, { p_title: ["alpha"] }, { p_harness: ["codex"] },
    { p_free: ["alpha"], p_active: true }, { p_free: ["alpha"], p_sources: ["codex"], p_not_sources: ["gemini"] },
    { p_free: ["alpha"], p_user_ids: ["u2"] }, { p_free: ["alpha"], p_agent: "worker" }, { p_free: ["alpha"], p_project_contains: "acme" },
    { p_free: ["alpha"], p_last_ts_after: "2026-10-04T00:00:00Z", p_first_ts_before: "2026-10-04T00:00:00Z" },
    { p_free: ["alpha"], p_limit: 2, p_cursor_last_activity_at: "2026-10-07T00:00:00Z", p_cursor_session_id: "s03", p_cursor_harness: "codex" },
  ];

  test("readyOnly off: each readiness function returns exactly its neighbour's rows for every filter; on: exactly those rows that are ready", async () => {
    await reset();
    await seed(db, FIXTURE);
    const readyIds = new Set(FIXTURE.filter((s) => s.ready && !s.deleted).map((s) => s.id));
    for (const f of LIST_FILTERS) {
      const old = await listOld(f);
      const off = await listNew(f, false);
      expect([f, strip(off)]).toEqual([f, old]);
      for (const r of off) expect([r.session_id, r.events_ready]).toEqual([r.session_id, readyIds.has(r.session_id)]);
      // On, without a limit cut: the old rows filtered by readiness, in the same order.
      if (f.p_limit === undefined) expect([f, strip(await listNew(f, true))]).toEqual([f, old.filter((r) => readyIds.has(r.session_id as string))]);
    }
    for (const f of QUERY_FILTERS) {
      const old = await queryOld(f);
      const off = await queryNew(f, false);
      expect([f, strip(off)]).toEqual([f, old]);
      if (f.p_limit === undefined) expect([f, strip(await queryNew(f, true))]).toEqual([f, old.filter((r) => readyIds.has(r.session_id as string))]);
    }
    // The refusals are the neighbour's: 22023 for a bad value, the incomplete cursor and the limit.
    expect(await code(listNew({ p_limit: 0 }, true))).toBe("22023");
    expect(await code(listNew({ p_sources: ["Bad Name"] }, true))).toBe("22023");
    expect(await code(listNew({ p_cursor_session_id: "s01" }, true))).toBe("22023");
    expect(await code(queryNew({ p_free: ["ab"] }, true))).toBe("22023");
    expect(await code(queryNew({ p_free: ["alpha"], p_limit: 201 }, true))).toBe("22023");
  });

  test("readyOnly filters before the LIMIT: limit 2 returns the first 2 + 1 ready rows, not a page cut then filtered", async () => {
    await reset();
    await seed(db, FIXTURE);
    // The newest 3 rows are s01 (ready), s02 (not), s03 (ready): a post-filter of a 3-row cut would keep 2.
    expect((await listNew({ p_limit: 2 }, true)).map((r) => r.session_id)).toEqual(["s01", "s03", "s05"]);
    expect((await queryNew({ p_free: ["alpha"], p_limit: 2 }, true)).map((r) => r.session_id)).toEqual(["s01", "s03", "s06"]);
  });

  test("multi-page walks through the read API (plain and q, readyOnly and not): every row once, no short page with a nextCursor, ties and not-ready rows on page boundaries", async () => {
    await reset();
    // 23 sessions; equal timestamps in runs; readiness patterned so that not-ready rows sit on page edges for limits 2..5.
    const sessions: Session[] = [];
    for (let i = 0; i < 23; i++) {
      const tie = Math.floor(i / 3);
      sessions.push({
        id: `w${String(i).padStart(2, "0")}`, harness: i % 2 ? "claude" : "codex",
        last: new Date(Date.UTC(2026, 9, 20) - tie * 3_600_000).toISOString(), title: `gamma walk ${i}`,
        ready: ![1, 2, 3, 6, 7, 10, 11, 12, 13, 17, 20, 21].includes(i),
      });
    }
    await seed(db, sessions);
    const pool = {
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, p?: unknown[]) => db.query(text, p as unknown[]) as Promise<{ rows: unknown[] }>,
          release: () => void db.exec("RESET ROLE"),
        };
      },
    };
    const app = createReadApi({
      verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
      ch: {} as never,
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      directory: sqlDirectory(poolTenantRunner(pool)),
      directoryWorkspaces: new Set(["ws-a"]),
      sessionQuery: true,
      listReadiness: true,
    });
    const get = async (path: string) => {
      const r = await app.request(path, { headers: { authorization: "Bearer t" } });
      return { status: r.status, header: r.headers.get("x-relayhistory-ready-only"), body: (await r.json()) as Record<string, any> };
    };
    const order = [...sessions].sort((a, b) => (a.last < b.last ? 1 : a.last > b.last ? -1 : a.id < b.id ? 1 : a.id > b.id ? -1 : a.harness! < b.harness! ? 1 : -1));
    for (const q of ["", `&q=${encodeURIComponent("gamma walk")}`]) {
      for (const readyOnly of [true, false]) {
        const expected = order.filter((s) => !readyOnly || s.ready).map((s) => s.id);
        for (const limit of [1, 2, 3, 4, 5, 7]) {
          const seen: string[] = [];
          let cursor: string | null = null;
          for (let page = 0; page < 40; page++) {
            const r = await get(`/v1/sessions?workspace=ws-a&limit=${limit}${q}${readyOnly ? "&readyOnly=true" : ""}&includeReadiness=true${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
            expect([q, readyOnly, limit, r.status]).toEqual([q, readyOnly, limit, 200]);
            expect(r.header).toBe(readyOnly ? "applied" : null);
            if (readyOnly) for (const s of r.body.sessions) expect(s.eventsReady).toBe(true);
            if (r.body.nextCursor) expect([q, readyOnly, limit, page, r.body.sessions.length]).toEqual([q, readyOnly, limit, page, limit]);
            seen.push(...r.body.sessions.map((s: any) => s.sessionId));
            cursor = r.body.nextCursor;
            if (!cursor) break;
          }
          expect([q, readyOnly, limit, seen]).toEqual([q, readyOnly, limit, expected]);
        }
      }
    }
    // A cursor replayed across a readyOnly change is invalid_cursor, both ways, plain and q.
    for (const q of ["", `&q=${encodeURIComponent("gamma walk")}`]) {
      const on = (await get(`/v1/sessions?workspace=ws-a&limit=2${q}&readyOnly=true`)).body.nextCursor;
      const off = (await get(`/v1/sessions?workspace=ws-a&limit=2${q}`)).body.nextCursor;
      for (const [cursor, extra] of [[on, ""], [off, "&readyOnly=true"]] as const) {
        const r = await get(`/v1/sessions?workspace=ws-a&limit=2${q}${extra}&cursor=${encodeURIComponent(cursor)}`);
        expect([q, extra, r.status, r.body.error]).toEqual([q, extra, 400, { code: "bad_request", message: "cursor is not valid" }]);
      }
    }
  });

  test("the builder anchor: a list statement of another shape refuses 55000, never a misplaced predicate", async () => {
    await reset();
    await seed(db, FIXTURE);
    const builder = "sessions.session_directory_list_sql(integer, boolean, integer, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)";
    const src = (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid = $1::regprocedure", [builder])).rows[0]!.s;
    const err = await db.transaction(async (tx) => {
      await tx.exec(`CREATE OR REPLACE FUNCTION ${builder.replace(/\(.*\)$/u, "")}(
        p_source_count integer, p_has_project boolean, p_user_count integer, p_has_agent boolean,
        p_has_last_after boolean, p_has_last_before boolean, p_has_first_after boolean, p_has_first_before boolean,
        p_has_keys boolean, p_has_cursor boolean, p_has_not_sources boolean)
        RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $b$${src.replace("LIMIT $14", "LIMIT $14 ")}$b$`);
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query("SELECT set_config('bulls96_read.org_id','org-a',true), set_config('bulls96_read.workspace_id','ws-a',true)");
      const e = await tx.query(`SELECT * FROM sessions.list_session_directory_readiness_for(p_workspace_id => 'ws-a', p_sources => NULL, p_not_sources => NULL, p_project_contains => NULL, p_user_ids => NULL, p_agent => NULL, p_last_ts_after => NULL, p_last_ts_before => NULL, p_first_ts_after => NULL, p_first_ts_before => NULL, p_session_harnesses => NULL, p_session_ids => NULL, p_cursor_last_activity_at => NULL, p_cursor_session_id => NULL, p_cursor_harness => NULL, p_limit => 10, p_ready_only => true)`)
        .then(() => null, (x: { code?: string; message?: string }) => x);
      await tx.rollback();
      return e;
    }).catch((x) => x);
    expect(err?.code).toBe("55000");
    expect(err?.message).toContain("does not have the shape the readiness list extends");
    // Rolled back: the shipped builder is untouched and the function serves again.
    expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid = $1::regprocedure", [builder])).rows[0]!.s).toBe(src);
    expect((await listNew({}, true)).length).toBeGreaterThan(0);
  });
});

describe("20261105062200 rollback", () => {
  test("checksum-pinned; 062190's down refuses under it; it drops only the two readiness functions and its ledger row; a rerun refuses", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    const db = await migrated();
    const message = async (sql: string) => { const m = await db.exec(sql).then(() => null, (e: Error) => e.message); await db.exec("ROLLBACK").catch(() => {}); return m; };
    try {
      // Any later upstream step comes off first, newest first, so a later slot does not break this test.
      const later = (await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).map((s) => s.name).reverse();
      for (const name of later) {
        const file = `supabase/rollbacks/${name.slice("upstream:".length).replace(/\.sql$/u, ".down.sql")}`;
        if (!existsSync(join(root, file))) throw new Error(`a later step has no down file: ${file}`);
        expect(await message(DOWN)).toContain("a later migration is applied");
        await db.exec(sqlFile(file));
      }
      expect(await message(read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql"))).toContain("a later migration is applied");
      await db.exec(DOWN);
      for (const fn of [LIST, QUERY]) expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r).toBeNull();
      for (const fn of [OLD_LIST, OLD_QUERY]) expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r).not.toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name = $1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await message(DOWN)).toContain("own applied checksum required");
      // 062190 comes off next, as before this slot.
      await db.exec(read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql"));
    } finally { await db.close(); }
  });
});
