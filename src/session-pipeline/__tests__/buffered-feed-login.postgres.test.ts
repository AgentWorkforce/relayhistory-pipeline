/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, disposable trust-auth fixture only; the shared
 * owned_loopback_fixture_required guard admits only postgres://postgres@<loopback>[:port]/postgres and
 * throws at import, before any connection). The pg-proof workflow runs it on PostgreSQL 15, 16 and 17.
 * Without the variable every real-server test skips.
 *
 * Proves, on a real server, for 20261105043860_buffered_search_gate and 20261105043870_buffered_search_feed_cursor:
 *  (a) both apply through runMigrations (local profile: shim, bootstrap, every upstream migration, verify);
 *      down/up: every applied upstream step after 043870 (today 043880) is first rolled back in DESCENDING order
 *      from its supabase/rollbacks/<step>.down.sql (a missing file fails the test), so 043870 is the latest step;
 *      then 043860's down is refused (55000)
 *      while 043870 is applied and drops nothing; 043870's down then 043860's down remove exactly their objects,
 *      ledger rows and the runtime role's schema USAGE; a second runMigrations reapplies every rolled-back step with
 *      the ledger equal to before and every role validation 'pass'.
 *  (b) the login guard: the three feed RPCs raise 42501 unless session_user is buffered_search_feed_runtime with
 *      no SET ROLE (the creator, the creator under SET ROLE, and bulls96_read_api are refused); the genuine
 *      runtime login is admitted. buffered_search_gate_for admits only bulls96_read_api (with its tenant pins).
 *  (c) the ACL: EXECUTE on the three feed RPCs is held only by the runtime role plus the owner, on gate_for only by
 *      bulls96_read_api plus the owner; PUBLIC, anon, authenticated and service_role hold nothing; equal to
 *      src/hoopdb/sessions-function-acl.json.
 *  (d) assertBufferedFeedLogin against the real server: the genuine login's backend usename is the role and
 *      nothing is a member except (none here) the creator edge; SET SESSION AUTHORIZATION (backend_user),
 *      SET ROLE (session_user), a wrong timeout profile, an extra member edge and a role membership refuse.
 *
 * Limitations (trust auth, a plain postgres image):
 *  - Trust auth leaves system_user NULL on PG16+, so the genuine login is refused there with "system_user"
 *    (PG15 has no system_user and accepts). To reach the later PG16+ checks against the real catalog, a labelled
 *    seam substitutes ONLY the SYSTEM_USER_SQL row ("password:<role>" or a mismatched suffix); LOGIN_SQL and
 *    MEMBERS_SQL still run on the real backend.
 *  - The Supabase PG16+ creator edge (member postgres, grantor supabase_admin superuser, ADMIN only) cannot be
 *    built here: PG16 records a superuser-created membership with the bootstrap superuser as grantor and refuses
 *    GRANTED BY a role without ADMIN on it. That tolerance is covered by buffered-feed-login.test.ts; here the role
 *    has no member at all, and any real extra edge refuses.
 *  - The server guard does not see the backend identity: a superuser's SET SESSION AUTHORIZATION to the runtime
 *    role is ADMITTED by the RPCs (asserted below); assertBufferedFeedLogin is what refuses it (backend_user).
 *
 * Reshapes cluster roles in the disposable server only (no password anywhere: trust auth). (b) applies the read
 * role's operator step supabase/bootstrap/session_read_api_role.sql (bulls96_read_api LOGIN, settings, schema USAGE),
 * which is left in place. buffered_search_feed_runtime is given LOGIN with the timeout profile by applying
 * supabase/bootstrap/buffered_search_feed_runtime_role.sql and restored to NOLOGIN with no settings and no member
 * edge afterwards. Never run against a shared server.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ownedLoopbackFixture } from "../../shared/__tests__/owned-loopback-fixture.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { sessionsFunctionAcl, sessionsFunctionAclManifest } from "../../hoopdb/migrate-guards.ts";
import { LOGIN_SQL, MEMBERS_SQL, SYSTEM_USER_SQL, type LoginQuery } from "../../acquisition/db-login.ts";
import { BUFFERED_FEED_OPTIONS, BUFFERED_FEED_TIMEOUTS, BUFFERED_SEARCH_FEED_RUNTIME_ROLE as ROLE, BufferedFeedLoginError, assertBufferedFeedLogin } from "../buffered-feed-login.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// Refuse credentials, query options, remote endpoints and nonfixture identities at import, before connecting.
const fixtureUrl = url ? ownedLoopbackFixture(url) : undefined;
const root = join(import.meta.dir, "../../..");
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const READ = "bulls96_read_api";
const GATE_NAME = "upstream:20261105043860_buffered_search_gate.sql";
const FEED_NAME = "upstream:20261105043870_buffered_search_feed_cursor.sql";
const BK_NAME = "upstream:20261105043880_ingest_bookkeeping_cleanup.sql";
const FEED_FNS = ["sessions.buffered_search_feed_advance(jsonb)", "sessions.buffered_search_feed_page(jsonb)", "sessions.buffered_search_feed_stage(jsonb)"];
const GATE_FN = "sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)";
const FEED_GUARD = "buffered search feed: caller is not the buffered_search_feed_runtime login";
const GATE_GUARD = "buffered search gate: caller is not the read api role";
const GATE_TENANT = "buffered search gate: tenant is not the bound tenant";
const SINCE = "2026-11-06T00:00:00.000000Z";
/** psql meta-command lines (none expected) are dropped, as the other real-PG tests do. */
const sqlFile = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const ROLE_STEP = "supabase/bootstrap/buffered_search_feed_runtime_role.sql";
const ROLE_VERIFY = "supabase/bootstrap/buffered_search_feed_runtime_verify.sql";
const READ_STEP = "supabase/bootstrap/session_read_api_role.sql";
const sorted = (xs: readonly string[] | undefined) => [...(xs ?? [])].sort();

type Call = readonly [sql: string, params: unknown[]];
const feedCalls = (cursor: string): Record<"page" | "stage" | "advance", Call> => ({
  page: ["SELECT sessions.buffered_search_feed_page($1::jsonb) AS r", [JSON.stringify({ cursor, since: SINCE, limit: 1 })]],
  stage: ["SELECT sessions.buffered_search_feed_stage($1::jsonb) AS r", [JSON.stringify({ stageToken: randomUUID() })]],
  advance: ["SELECT sessions.buffered_search_feed_advance($1::jsonb) AS r",
    [JSON.stringify({ cursor, createdAt: "2026-11-06T00:00:01.000000Z", stageToken: randomUUID(), indexed: 0, skipped: 0 })]],
});
/** Malformed (cutover epoch not 64 hex): an admitted caller gets no rows, never an error. */
const gateCall: Call = [
  "SELECT * FROM sessions.buffered_search_gate_for($1::text,$2::text,$3::text,$4::text,$5::text,$6::text,$7::text,$8::text,$9::boolean,$10::text,$11::jsonb)",
  ["org-a", "ws-a", "user-a", "x", null, null, null, null, false, "y", "[]"],
];
const PINS = "SET LOCAL bulls96_read.org_id = 'org-a'; SET LOCAL bulls96_read.workspace_id = 'ws-a'";

/** Outcome of one statement: "accepted" (with rows) or "<sqlstate> <message>". */
type Outcome = { code: string; message?: string; rows?: any[] };

async function refusal(sql: LoginQuery): Promise<string> {
  try { await assertBufferedFeedLogin(sql); } catch (e) { return e instanceof BufferedFeedLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

/**
 * TEST SEAM (labelled; PG16+ only): trust auth leaves system_user NULL, so this wraps the REAL login connection and
 * replaces ONLY the SYSTEM_USER_SQL row with `systemUser`; LOGIN_SQL and MEMBERS_SQL run on the real backend.
 */
function systemUserSeam(real: pg.Client, systemUser: string, seen: string[]): LoginQuery {
  return {
    async query<T>(text: string, values?: unknown[]) {
      seen.push(text);
      if (text === SYSTEM_USER_SQL) return { rows: [{ system_user: systemUser }] as T[] };
      return (await real.query(text, values as unknown[])) as unknown as { rows: T[] };
    },
  };
}

describe.skipIf(!fixtureUrl)("buffered search gate/feed migrations and the feeder login (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name = "", version = 0;
  /** Discrete fields of the guarded URL (never a raw connectionString); trust auth: no password exists. */
  const config = (user: string, database: string, extra: Partial<pg.ClientConfig> = {}): pg.ClientConfig => ({
    host: fixtureUrl!.hostname.replace(/^\[(.*)\]$/, "$1"), port: Number(fixtureUrl!.port || 5432), user, database, ...extra,
  });
  /** A fresh backend on the scratch database: `user` is the authenticated login; `setup` runs first. */
  async function session<T>(user: string, setup: string | null, fn: (c: pg.Client) => Promise<T>, extra: Partial<pg.ClientConfig> = {}): Promise<T> {
    const c = new pg.Client(config(user, name, extra));
    await c.connect();
    try {
      if (setup) await c.query(setup);
      return await fn(c);
    } finally {
      await c.end().catch(() => undefined);
    }
  }
  const outcome = (c: pg.Client, [sql, params]: Call): Promise<Outcome> =>
    c.query(sql, params).then((r) => ({ code: "accepted", rows: r.rows }), (e: any) => ({ code: String(e?.code), message: String(e?.message) }));
  /** One call on its own backend (login `user`, after `setup`), inside a transaction (pins are SET LOCAL). */
  const call = (user: string, setup: string | null, c: Call) =>
    session(user, `BEGIN; ${setup ?? ""}`, async (s) => { const o = await outcome(s, c); await s.query("ROLLBACK").catch(() => undefined); return o; });
  const owner = async <T>(fn: (c: pg.PoolClient) => Promise<T>): Promise<T> => {
    const c = await pool.connect();
    try { return await fn(c); } finally { c.release(); }
  };
  const one = async <R = any>(sql: string, params: unknown[] = []): Promise<R> => (await pool.query(sql, params)).rows[0] as R;
  const ledger = async () => (await pool.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations")).rows.map((r) => r.name);
  const validationsPass = async () => {
    const rows = (await pool.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows;
    return rows.length > 0 && rows.every((r) => r.status === "pass");
  };
  /** A rollback file as ONE transaction on one backend (the files carry no BEGIN/COMMIT: psql -1). */
  const down = (file: string) => owner(async (c) => {
    await c.query("BEGIN");
    try {
      await c.query(sqlFile(`supabase/rollbacks/${file}`));
      await c.query("COMMIT");
      return { code: "accepted" } as Outcome;
    } catch (e: any) {
      await c.query("ROLLBACK").catch(() => undefined);
      return { code: String(e?.code), message: String(e?.message) } as Outcome;
    }
  });
  const migrate = () => owner((c) => runMigrations(c, { profile }));
  const verifyRole = () => pool.query(sqlFile(ROLE_VERIFY)).then(() => "pass", (e: any) => `${e?.code} ${e?.message}`);
  const roleShape = () => one(`SELECT r.rolcanlogin AS login, r.rolconfig AS config,
      (SELECT count(*)::int FROM pg_auth_members m WHERE m.roleid = r.oid) AS members,
      (SELECT count(*)::int FROM pg_auth_members m WHERE m.member = r.oid) AS memberships
    FROM pg_roles r WHERE r.rolname = $1`, [ROLE]);
  /** Non-superuser roles that can EXECUTE `fn` (effective privilege, so inherited grants count too). */
  const executors = async (fn: string) => (await pool.query<{ r: string }>(
    "SELECT rolname::text AS r FROM pg_roles WHERE NOT rolsuper AND has_function_privilege(oid, $1::regprocedure::oid, 'EXECUTE') ORDER BY 1", [fn])).rows.map((x) => x.r);

  /** The ACL proof (c), reused after the re-up in (a). */
  async function assertAcl() {
    const acl = await owner((c) => sessionsFunctionAcl(c));
    const manifest = await sessionsFunctionAclManifest();
    for (const fn of FEED_FNS) {
      expect([fn, sorted(acl[fn])]).toEqual([fn, ["<owner>:EXECUTE", `${ROLE}:EXECUTE`]]);
      expect([fn, sorted(acl[fn])]).toEqual([fn, sorted(manifest[fn])]);
      expect([fn, await executors(fn)]).toEqual([fn, [ROLE]]);
    }
    expect(sorted(acl[GATE_FN])).toEqual(["<owner>:EXECUTE", `${READ}:EXECUTE`]);
    expect(sorted(acl[GATE_FN])).toEqual(sorted(manifest[GATE_FN]));
    expect(await executors(GATE_FN)).toEqual([READ]);
    // Owner is the migrating superuser; every one is SECURITY DEFINER.
    for (const fn of [...FEED_FNS, GATE_FN])
      expect([fn, await one("SELECT p.proowner::regrole::text AS owner, p.prosecdef AS definer FROM pg_proc p WHERE p.oid = $1::regprocedure::oid", [fn])])
        .toEqual([fn, { owner: "postgres", definer: true }]);
    // PUBLIC (grantee 0), anon, authenticated, service_role: no entry and no effective EXECUTE.
    for (const fn of [...FEED_FNS, GATE_FN]) {
      expect([fn, acl[fn]!.filter((e) => /^(PUBLIC|anon|authenticated|service_role):/.test(e) || e === "<default>")]).toEqual([fn, []]);
      for (const grantee of ["anon", "authenticated", "service_role"])
        expect([fn, grantee, (await one("SELECT has_function_privilege($1::name, $2::regprocedure::oid, 'EXECUTE') AS x", [grantee, fn])).x]).toEqual([fn, grantee, false]);
    }
  }

  /**
   * The operator LOGIN step, applied from the committed file, then always restored: NOLOGIN, no settings, no
   * member/membership edges (so the verifier and a later runMigrations see the migrated bare role again).
   */
  async function withRuntimeLogin<T>(fn: () => Promise<T>): Promise<T> {
    expect(await verifyRole()).toBe("pass");
    await pool.query(sqlFile(ROLE_STEP));
    try {
      expect(await roleShape()).toMatchObject({ login: true, members: 0, memberships: 0 });
      expect(sorted((await roleShape()).config)).toEqual(["idle_in_transaction_session_timeout=10s", "lock_timeout=2s", "statement_timeout=5s"]);
      // The verifier accepts the approved LOGIN timeout profile.
      expect(await verifyRole()).toBe("pass");
      return await fn();
    } finally {
      await pool.query(`DO $$DECLARE m record; BEGIN
        FOR m IN SELECT u.rolname AS member, g.rolname AS grantee FROM pg_auth_members a JOIN pg_roles u ON u.oid = a.member JOIN pg_roles g ON g.oid = a.roleid
          WHERE a.roleid = '${ROLE}'::regrole OR a.member = '${ROLE}'::regrole LOOP
          EXECUTE format('REVOKE %I FROM %I CASCADE', m.grantee, m.member);
        END LOOP; END$$`);
      await pool.query(`ALTER ROLE ${ROLE} NOLOGIN; ALTER ROLE ${ROLE} RESET ALL`);
      expect(await roleShape()).toEqual({ login: false, config: null, members: 0, memberships: 0 });
      expect(await verifyRole()).toBe("pass");
    }
  }

  beforeAll(async () => {
    admin = new pg.Client(config("postgres", "postgres"));
    await admin.connect();
    version = Number((await admin.query("SELECT current_setting('server_version_num') AS v")).rows[0].v);
    name = "buffered_feed_login_" + randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    pool = new pg.Pool({ ...config("postgres", name), max: 3 });
    await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
    await migrate();
  }, 300000);
  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    if (admin) {
      if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)").catch(() => undefined);
      await admin.end().catch(() => undefined);
    }
  }, 60000);

  test("(a) 043860 and 043870 apply via runMigrations; later steps, then 043870, roll back before 043860; up again", async () => {
    const before = await ledger();
    expect(before).toEqual(expect.arrayContaining([GATE_NAME, FEED_NAME]));
    expect(await validationsPass()).toBe(true);
    // Every down refuses (55000) while a later upstream step is applied, so the steps after 043870 go first, latest
    // first, each from its committed rollback file. Names compare as COLLATE "C" in the guards; the default JS sort
    // (UTF-16 code units) orders these ASCII names identically.
    const upstream = () => ledger().then((ns) => ns.filter((n) => n.startsWith("upstream:")).sort());
    const later = (await upstream()).filter((n) => n > FEED_NAME).reverse();
    const downFile = (step: string) => {
      const file = step.slice("upstream:".length).replace(/\.sql$/, ".down.sql");
      if (!existsSync(join(root, "supabase/rollbacks", file)))
        throw new Error(`upstream step ${step} is applied after ${FEED_NAME} but has no rollback supabase/rollbacks/${file}`);
      return file;
    };
    const laterFiles = later.map(downFile);
    // The order today, documented: 043880 is the only step after 043870 on this branch.
    expect(later).toEqual(expect.arrayContaining([BK_NAME]));
    for (const [i, step] of later.entries()) {
      expect([step, await down(laterFiles[i]!)]).toEqual([step, { code: "accepted" }]);
      expect(await ledger()).not.toContain(step);
    }
    // 043870 must now be the latest upstream step, or its own down is (correctly) refused too.
    expect((await upstream()).at(-1)).toBe(FEED_NAME);
    await assertAcl();
    const gateFn = async () => (await one("SELECT to_regprocedure($1::text)::text AS r", [GATE_FN])).r as string | null;
    const feedFns = async () => (await one("SELECT count(to_regprocedure(f))::int AS n FROM unnest($1::text[]) f", [FEED_FNS])).n as number;
    const cursorTable = async () => (await one("SELECT to_regclass('sessions.buffered_search_feed_cursors')::text AS r")).r as string | null;
    const usage = async () => (await one("SELECT has_schema_privilege($1::name, 'sessions'::text, 'USAGE') AS u", [ROLE])).u as boolean;

    // 043860's down is refused while 043870 is applied, before anything is dropped.
    expect(await down("20261105043860_buffered_search_gate.down.sql")).toMatchObject({ code: "55000" });
    expect(await ledger()).toEqual(expect.arrayContaining([GATE_NAME, FEED_NAME]));
    expect(await gateFn()).toBe(GATE_FN);
    expect(await feedFns()).toBe(3);

    // 043870 down: the three functions, the cursor table, its ledger row and the role's schema USAGE go; the role stays.
    expect(await down("20261105043870_buffered_search_feed_cursor.down.sql")).toEqual({ code: "accepted" });
    expect(await ledger()).not.toContain(FEED_NAME);
    expect(await ledger()).toContain(GATE_NAME);
    expect(await feedFns()).toBe(0);
    expect(await cursorTable()).toBeNull();
    expect(await usage()).toBe(false);
    expect((await one("SELECT count(*)::int AS n FROM pg_roles WHERE rolname = $1", [ROLE])).n).toBe(1);
    expect(await gateFn()).toBe(GATE_FN);

    // 043860 down: only the gate reader and its ledger row.
    expect(await down("20261105043860_buffered_search_gate.down.sql")).toEqual({ code: "accepted" });
    expect(await ledger()).not.toContain(GATE_NAME);
    expect(await gateFn()).toBeNull();
    expect(sorted(await ledger())).toEqual(sorted(before.filter((n) => n !== GATE_NAME && n !== FEED_NAME && !later.includes(n))));

    // Up again: 043860, 043870 and every later step reapply through the migrator (prerequisites already present),
    // the ledger equals before, verify passes, ACL restored.
    await migrate();
    expect(sorted(await ledger())).toEqual(sorted(before));
    expect(await validationsPass()).toBe(true);
    expect(await gateFn()).toBe(GATE_FN);
    expect(await feedFns()).toBe(3);
    expect(await cursorTable()).toBe("sessions.buffered_search_feed_cursors");
    expect(await usage()).toBe(true);
  }, 300000);

  test("(c) EXECUTE: feed RPCs only the runtime role + owner; gate_for only bulls96_read_api + owner", async () => {
    await assertAcl();
  });

  test("(b) feed RPCs admit only the genuine runtime login; gate_for admits only bulls96_read_api", async () => {
    // The read role's approved shape (schema USAGE etc.), as the operator applies it after migrate; without it
    // bulls96_read_api cannot reach the sessions schema at all. Same file, same order as buffered-search-gate.pglite.test.ts.
    await pool.query(sqlFile(READ_STEP));
    const calls = feedCalls("login-proof");
    for (const [label, c] of Object.entries(calls)) {
      // The creator (owner, holds EXECUTE): refused by the session_user guard, directly and under SET ROLE.
      expect([label, "owner", await call("postgres", null, c)]).toEqual([label, "owner", { code: "42501", message: FEED_GUARD }]);
      expect([label, "owner SET ROLE", await call("postgres", `SET ROLE ${ROLE}`, c)]).toEqual([label, "owner SET ROLE", { code: "42501", message: FEED_GUARD }]);
      // bulls96_read_api (session_user is the read role): 42501 (it holds no EXECUTE either).
      expect([label, READ, (await call("postgres", `SET SESSION AUTHORIZATION ${READ}`, c)).code]).toEqual([label, READ, "42501"]);
      // anon / authenticated / service_role: 42501.
      for (const r of ["anon", "authenticated", "service_role"])
        expect([label, r, (await call("postgres", `SET SESSION AUTHORIZATION ${r}`, c)).code]).toEqual([label, r, "42501"]);
      // LIMITATION (documented): the server guard cannot see the backend identity, so a superuser's SET SESSION
      // AUTHORIZATION to the runtime role is admitted by the RPC; assertBufferedFeedLogin refuses it (test d).
      expect([label, "superuser SET SESSION AUTHORIZATION", (await call("postgres", `SET SESSION AUTHORIZATION ${ROLE}`, c)).code])
        .toEqual([label, "superuser SET SESSION AUTHORIZATION", "accepted"]);
    }
    // gate_for: the owner and SET ROLE into the read role hit the guard; the right session without pins, or with
    // another tenant's pins, is refused; bulls96_read_api with its pins is admitted (malformed input: no rows).
    expect(await call("postgres", PINS, gateCall)).toEqual({ code: "42501", message: GATE_GUARD });
    expect(await call("postgres", `SET ROLE ${READ}; ${PINS}`, gateCall)).toEqual({ code: "42501", message: GATE_GUARD });
    expect(await call("postgres", `SET SESSION AUTHORIZATION ${READ}`, gateCall)).toEqual({ code: "42501", message: GATE_TENANT });
    expect(await call("postgres", `SET SESSION AUTHORIZATION ${READ}; SET LOCAL bulls96_read.org_id = 'org-b'; SET LOCAL bulls96_read.workspace_id = 'ws-a'`, gateCall))
      .toEqual({ code: "42501", message: GATE_TENANT });
    for (const r of [ROLE, "anon", "authenticated", "service_role"])
      expect([r, (await call("postgres", `SET SESSION AUTHORIZATION ${r}; ${PINS}`, gateCall)).code]).toEqual([r, "42501"]);
    expect(await call("postgres", `SET SESSION AUTHORIZATION ${READ}; ${PINS}`, gateCall)).toEqual({ code: "accepted", rows: [] });

    // The genuine runtime LOGIN (the operator step applied): admitted by all three feed RPCs, refused by gate_for.
    await withRuntimeLogin(() => session(ROLE, null, async (login) => {
      expect((await login.query("SELECT session_user::text AS s, current_user::text AS c, current_setting('role') AS r")).rows[0]).toEqual({ s: ROLE, c: ROLE, r: "none" });
      const g = feedCalls("genuine-login");
      const page = await outcome(login, g.page);
      expect(page.code).toBe("accepted");
      expect(page.rows![0].r).toMatchObject({ cursor: "genuine-login", stages: [], after: null });
      expect((await outcome(login, g.stage)).rows?.[0].r).toMatchObject({ found: false, status: "gone", reason: "stage_absent" });
      expect((await outcome(login, g.advance)).rows?.[0].r).toEqual({ advanced: true });
      // Under SET ROLE to itself it is no longer the genuine-login shape.
      await login.query(`SET ROLE ${ROLE}`);
      expect(await outcome(login, feedCalls("genuine-login").page)).toEqual({ code: "42501", message: FEED_GUARD });
      await login.query("RESET ROLE");
      // The cursor table is not directly readable; gate_for is not executable by the feed login.
      expect((await outcome(login, ["SELECT * FROM sessions.buffered_search_feed_cursors", []])).code).toBe("42501");
      await login.query(`BEGIN; ${PINS}`);
      expect((await outcome(login, gateCall)).code).toBe("42501");
      await login.query("ROLLBACK");
    }, { options: BUFFERED_FEED_OPTIONS }));
  }, 120000);

  test("(d) assertBufferedFeedLogin against the real server", async () => {
    await withRuntimeLogin(async () => {
      // A superuser's SET SESSION AUTHORIZATION looks genuine to every SQL-visible signal but the backend usename.
      await session("postgres", Object.entries(BUFFERED_FEED_TIMEOUTS).map(([k, v]) => `SET ${k} = '${v}'`).join("; "), async (su) => {
        await su.query(`SET SESSION AUTHORIZATION ${ROLE}`);
        expect((await su.query("SELECT session_user::text AS s, current_user::text AS c, current_setting('role') AS r")).rows[0]).toEqual({ s: ROLE, c: ROLE, r: "none" });
        expect([version, await refusal(su)]).toEqual([version, "backend_user"]);
        await su.query(`RESET SESSION AUTHORIZATION; SET ROLE ${ROLE}`);
        expect([version, await refusal(su)]).toEqual([version, "session_user"]);
      });
      // The genuine login with the pinned startup options.
      await session(ROLE, null, async (login) => {
        const row = (await login.query(LOGIN_SQL)).rows[0];
        expect(row).toMatchObject({ session_user: ROLE, current_user: ROLE, role_setting: "none", backend_user: ROLE, members: 0, memberships: 0,
          rolsuper: false, rolbypassrls: false, rolinherit: false, ...BUFFERED_FEED_TIMEOUTS });
        if (version >= 160000) {
          // Trust auth: no authenticated identity, so the real check refuses (a password login would carry "<method>:<role>").
          expect((await login.query(SYSTEM_USER_SQL)).rows).toEqual([{ system_user: null }]);
          expect((await login.query(MEMBERS_SQL)).rows).toEqual([]);
        }
        expect([version, await refusal(login)]).toEqual([version, version >= 160000 ? "system_user" : "accepted"]);

        // Through the labelled system_user seam, the rest of the PG16+ path runs against the real catalog.
        const seen: string[] = [];
        expect([version, await refusal(systemUserSeam(login, `password:${ROLE}`, seen))]).toEqual([version, "accepted"]);
        expect(seen).toEqual(version >= 160000 ? [LOGIN_SQL, SYSTEM_USER_SQL, MEMBERS_SQL] : [LOGIN_SQL]);
        // A system_user whose identity suffix is another role (or has no method prefix) refuses on PG16+.
        for (const other of [`password:${READ}`, `password:${ROLE}x`, ROLE])
          expect([version, other, await refusal(systemUserSeam(login, other, []))])
            .toEqual([version, other, version >= 160000 ? "system_user" : "accepted"]);

        // An extra member edge (not the Supabase creator edge) refuses; the verifier refuses it too.
        await pool.query(`GRANT ${ROLE} TO ${READ}`);
        try {
          expect((await login.query(LOGIN_SQL)).rows[0].members).toBe(1);
          if (version >= 160000) {
            expect((await login.query(MEMBERS_SQL)).rows).toMatchObject([{ member: READ, grantor: "postgres", grantor_super: true, admin_option: false }]);
            expect(await refusal(systemUserSeam(login, `password:${ROLE}`, []))).toBe("role_members");
          }
          expect([version, await refusal(login)]).toEqual([version, version >= 160000 ? "system_user" : "role_members"]);
          expect(await verifyRole()).toMatch(/^55000 buffered search feed runtime (role membership or settings differ|has an unexpected member)$/);
        } finally {
          await pool.query(`REVOKE ${ROLE} FROM ${READ}`);
        }
        expect(await refusal(systemUserSeam(login, `password:${ROLE}`, []))).toBe("accepted");

        // The runtime role a member of another role refuses (before system_user on every version).
        await pool.query(`GRANT ${READ} TO ${ROLE}`);
        try {
          expect([version, await refusal(login)]).toEqual([version, "role_membership"]);
        } finally {
          await pool.query(`REVOKE ${READ} FROM ${ROLE}`);
        }
      }, { options: BUFFERED_FEED_OPTIONS });
      // The same login with a different timeout profile refuses.
      await session(ROLE, null, async (login) => {
        expect((await login.query("SHOW statement_timeout")).rows[0].statement_timeout).toBe("6s");
        expect([version, await refusal(login)]).toEqual([version, "timeouts"]);
      }, { options: "-c statement_timeout=6s" });
    });
  }, 120000);
});
