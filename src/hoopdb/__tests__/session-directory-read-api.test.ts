/**
 * The read API's tenant-setting directory functions (20261105034000):
 * sessions.list_session_directory_for and sessions.session_directory_activity_for, run AS
 * bulls96_read_api with the tenant bound the way the read Worker binds it
 * (TENANT_SETTINGS_SQL inside BEGIN READ ONLY). They must answer exactly what the
 * claim-bound originals answer for the same tenant, and refuse every other caller,
 * binding and workspace. Also: the bare-role bootstrap, the migration's fail-closed role
 * check, the role verifier's allow-list, the ACL, and up/down/up. PGlite, every migration.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_DIRECTORY_SEARCH_RPC } from "../search-index-contract.ts";
import { TENANT_SETTINGS_SQL, poolTenantRunner } from "../../session-read-api/liveness.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { memoryLiveness } from "../../session-read-api/__tests__/support.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105034000_session_directory_read_api.sql";
const DOWN = "supabase/rollbacks/20261105034000_session_directory_read_api.down.sql";
const BOOTSTRAP = "supabase/bootstrap/session_read_api_roles.sql";
const ROLE = sql("supabase/bootstrap/session_read_api_role.sql");
const VERIFY = sql("supabase/bootstrap/session_read_api_role_verify.sql");
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
const TOKEN_ISSUER = "https://issuer.test/session-directory";
const T = "timestamp with time zone";
const LIST_SIG = `sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer)`;
const ACTIVITY_SIG = `sessions.session_directory_activity_for(text,${T},${T})`;
const SCOPE_SIG = "sessions.session_directory_read_api_scope(text)";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  return db;
}

/** Every migration up to (not including) ours, then ours; returns its error, if any. */
async function migrate(db: PGlite): Promise<{ code?: string; message?: string } | null> {
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    const path = `supabase/migrations/${file}`;
    if (path === MIGRATION) return db.exec(read(MIGRATION)).then(() => null, (e: { code?: string; message?: string }) => e);
    await db.exec(read(path));
  }
  throw new Error("migration not found");
}

async function database(): Promise<PGlite> {
  const db = await fresh();
  await db.exec(read(BOOTSTRAP));
  const error = await migrate(db);
  if (error) throw error;
  await db.exec(ROLE);
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
  return db;
}

type Row = Record<string, unknown>;
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

type Session = { org?: string; workspace?: string; harness?: string; id: string; created: string; last: string; project?: string; users?: string[]; agents?: string[]; deleted?: boolean; title?: string };
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions) {
    const org = s.org ?? "org-a", ws = s.workspace ?? "ws-a", harness = s.harness ?? "codex", users = s.users ?? [];
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id, user_ids,
          agent_names, title, deleted_at, catalog_projected_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CASE WHEN $11 THEN now() END, now())`,
      [org, ws, harness, s.id, s.created, s.last, s.project ?? null, users, s.agents ?? [], s.title ?? null, s.deleted ?? false],
    );
    if (s.deleted) continue;
    for (const user of users)
      await db.query("INSERT INTO sessions.session_directory_users VALUES ($1, $2, $3, $4, $5, $6)", [org, ws, harness, s.id, user, s.last]);
    for (const agent of s.agents ?? [])
      await db.query(
        `INSERT INTO sessions.session_directory_agents (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
         VALUES ($1, $2, $3, $4, $5, lower(btrim($5)), $6)`,
        [org, ws, harness, s.id, agent, s.last],
      );
  }
}

const FIXTURE: Session[] = [
  { id: "s1", created: "2026-10-01T00:00:00Z", last: "2026-10-05T12:00:00Z", project: "github.com/acme/api", users: ["u1"], agents: ["Lead"], title: "First" },
  { id: "s2", harness: "claude", created: "2026-10-02T00:00:00Z", last: "2026-10-04T12:00:00Z", project: "github.com/acme/web", users: ["u2"] },
  { id: "s3", created: "2026-10-03T00:00:00Z", last: "2026-10-03T12:00:00Z", users: ["u1", "u2"], agents: ["worker"] },
  { id: "s4", harness: "claude", created: "2026-10-03T06:00:00Z", last: "2026-10-03T06:30:00Z" },
  { id: "gone", created: "2026-10-03T00:00:00Z", last: "2026-10-06T00:00:00Z", deleted: true },
  { id: "other-ws", workspace: "ws-b", created: "2026-10-01T00:00:00Z", last: "2026-10-06T00:00:00Z" },
  { id: "other-org", org: "org-b", created: "2026-10-01T00:00:00Z", last: "2026-10-06T00:00:00Z" },
];

/** As the read Worker: BEGIN READ ONLY, bind the tenant, run, COMMIT; as `role`. */
async function asReadApi<T>(db: PGlite, tenant: [string, string] | null, work: () => Promise<T>, role = "bulls96_read_api"): Promise<T> {
  await db.exec(`SET ROLE ${role}`);
  try {
    await db.exec("BEGIN READ ONLY");
    try {
      if (tenant) await db.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await db.exec("COMMIT");
      return out;
    } catch (error) {
      await db.exec("ROLLBACK");
      throw error;
    }
  } finally {
    await db.exec("RESET ROLE");
  }
}

/** As the search Worker with signed claims: the original RPCs. */
async function asWorker<T>(db: PGlite, org: string, workspace: string, work: () => Promise<T>): Promise<T> {
  const iat = Math.floor(Date.now() / 1000);
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({ role: WORKER, iss: TOKEN_ISSUER, aud: SESSION_DIRECTORY_SEARCH_RPC.audience, iat, exp: iat + 60, app_metadata: { org_id: org, workspace_ids: [workspace] } }),
  ]);
  await db.exec(`SET ROLE ${WORKER}`);
  try {
    return await work();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

type Args = { workspace?: string; sources?: string[]; notSources?: string[]; project?: string; users?: string[]; agent?: string; lastAfter?: string; lastBefore?: string; firstAfter?: string; firstBefore?: string; keys?: Array<[string, string]>; cursor?: [string, string, string]; limit?: number };
const LIST_ARGS = `p_workspace_id => $1, p_sources => $2, p_not_sources => $3, p_project_contains => $4,
  p_user_ids => $5, p_agent => $6, p_last_ts_after => $7, p_last_ts_before => $8, p_first_ts_after => $9,
  p_first_ts_before => $10, p_session_harnesses => $11, p_session_ids => $12,
  p_cursor_last_activity_at => $13, p_cursor_session_id => $14, p_cursor_harness => $15, p_limit => $16`;
const params = (a: Args) => [
  a.workspace ?? "ws-a", a.sources ?? null, a.notSources ?? null, a.project ?? null, a.users ?? null, a.agent ?? null,
  a.lastAfter ?? null, a.lastBefore ?? null, a.firstAfter ?? null, a.firstBefore ?? null,
  a.keys ? a.keys.map(([h]) => h) : null, a.keys ? a.keys.map(([, s]) => s) : null,
  a.cursor?.[0] ?? null, a.cursor?.[1] ?? null, a.cursor?.[2] ?? null, a.limit ?? 50,
];
const listFor = async (db: PGlite, a: Args) =>
  (await db.query<Row>(`SELECT * FROM sessions.list_session_directory_for(${LIST_ARGS})`, params(a))).rows;
const listOriginal = async (db: PGlite, a: Args) =>
  (await db.query<Row>(`SELECT * FROM sessions.list_session_directory(${LIST_ARGS})`, params(a))).rows;
const activityFor = async (db: PGlite, ws: string, since: string, until: string) =>
  (await db.query<Row>("SELECT * FROM sessions.session_directory_activity_for($1, $2, $3)", [ws, since, until])).rows;

describe("list_session_directory_for / session_directory_activity_for", () => {
  test("answer exactly what the claim-bound originals answer for the same tenant", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const shapes: Args[] = [
      {},
      { limit: 2 },
      { cursor: ["2026-10-04T12:00:00Z", "s2", "claude"], limit: 2 },
      { sources: ["claude"] },
      { notSources: ["claude"] },
      { project: "acme/A" },
      { users: ["u1"] },
      { users: ["u1", "u2"] },
      { agent: " lead " },
      { lastAfter: "2026-10-03T12:00:00Z", lastBefore: "2026-10-05T12:00:00Z" },
      { firstAfter: "2026-10-02T00:00:00Z", firstBefore: "2026-10-03T06:00:00Z" },
      { keys: [["codex", "s1"], ["claude", "s4"], ["codex", "gone"]] },
    ];
    for (const shape of shapes) {
      const expected = await asWorker(db, "org-a", "ws-a", () => listOriginal(db, shape));
      const actual = await asReadApi(db, ["org-a", "ws-a"], () => listFor(db, shape));
      expect(actual, JSON.stringify(shape)).toEqual(expected);
    }
    const all = await asReadApi(db, ["org-a", "ws-a"], () => listFor(db, {}));
    expect(all.map((r) => r.session_id)).toEqual(["s1", "s2", "s3", "s4"]);
    expect(all[0]).toMatchObject({ org_id: "org-a", workspace_id: "ws-a", title: "First", short_summary: null });
    for (const [since, until] of [["2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z"], ["2026-10-03T00:00:00Z", "2026-10-03T23:59:59Z"]] as const) {
      const expected = await asWorker(db, "org-a", "ws-a", async () =>
        (await db.query<Row>("SELECT * FROM sessions.session_directory_activity($1, $2, $3)", ["ws-a", since, until])).rows);
      expect(await asReadApi(db, ["org-a", "ws-a"], () => activityFor(db, "ws-a", since, until))).toEqual(expected);
    }
  }, 120_000);

  test("the bound tenant alone decides the rows: other workspaces and orgs never appear", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect((await asReadApi(db, ["org-a", "ws-b"], () => listFor(db, { workspace: "ws-b" }))).map((r) => r.session_id)).toEqual(["other-ws"]);
    expect((await asReadApi(db, ["org-b", "ws-a"], () => listFor(db, {}))).map((r) => r.session_id)).toEqual(["other-org"]);
    expect(await asReadApi(db, ["org-c", "ws-a"], () => listFor(db, {}))).toEqual([]);
    expect((await asReadApi(db, ["org-b", "ws-a"], () => activityFor(db, "ws-a", "2026-10-06T00:00:00Z", "2026-10-06T23:00:00Z")))).toEqual([
      { day: new Date("2026-10-06T00:00:00Z"), harness: "codex", sessions: 1 },
    ]);
  }, 120_000);

  test("refuse 42501: wrong role, no binding, an expired binding, another workspace", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    for (const role of [WORKER, "service_role", "authenticated", "anon"]) {
      // Not granted to these roles at all; the scope helper would refuse them too.
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => listFor(db, {}), role))).toBe("42501");
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activityFor(db, "ws-a", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"), role))).toBe("42501");
    }
    // The table owner (no SET ROLE): EXECUTE is held, the role guard refuses.
    await db.query(TENANT_SETTINGS_SQL, ["org-a", "ws-a"]);
    expect(await code(listFor(db, {}))).toBe("42501");
    expect(await code(db.query(`SELECT sessions.session_directory_read_api_scope('ws-a')`))).toBe("42501");
    await db.exec("SELECT set_config('bulls96_read.org_id', '', false), set_config('bulls96_read.workspace_id', '', false)");
    // No binding at all, then a binding that ended with its transaction (reads '').
    expect(await code(asReadApi(db, null, () => listFor(db, {})))).toBe("42501");
    await asReadApi(db, ["org-a", "ws-a"], async () => null);
    expect(await code(asReadApi(db, null, () => listFor(db, {})))).toBe("42501");
    expect(await code(asReadApi(db, null, () => activityFor(db, "ws-a", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")))).toBe("42501");
    // Half a binding.
    expect(await code(asReadApi(db, ["org-a", ""], () => listFor(db, {})))).toBe("42501");
    expect(await code(asReadApi(db, ["", "ws-a"], () => listFor(db, {})))).toBe("42501");
    // Another workspace than the bound one.
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => listFor(db, { workspace: "ws-b" })))).toBe("42501");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activityFor(db, "ws-b", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")))).toBe("42501");
    // The read role reaches neither the claim-bound originals nor the helper.
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => listOriginal(db, {})))).toBe("42501");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => db.query(`SELECT sessions.session_directory_read_api_scope('ws-a')`)))).toBe("42501");
  }, 120_000);

  test("the same validations as the originals: 22023 and agent_filter_not_ready", async () => {
    const db = await database();
    for (const bad of [{ limit: 0 }, { limit: 201 }, { sources: ["Bad Name"] }, { agent: "  " }, { lastAfter: "2026-10-05T00:00:00Z", lastBefore: "2026-10-04T00:00:00Z" }, { keys: [["codex", ""]] as Array<[string, string]> }]) {
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => listFor(db, bad))), JSON.stringify(bad)).toBe("22023");
    }
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => db.query("SELECT * FROM sessions.list_session_directory_for(p_workspace_id => 'ws-a', p_cursor_session_id => 's1')")))).toBe("22023");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activityFor(db, "ws-a", "2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z")))).toBe("22023");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activityFor(db, "ws-a", "2025-01-01T00:00:00Z", "2026-10-01T00:00:00Z")))).toBe("22023");
    await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = NULL");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => listFor(db, { agent: "lead" })))).toBe("0A000");
  }, 120_000);

  test("ACLs are exactly the manifest's, and the read role verifier passes with them", async () => {
    const db = await database();
    const manifest = JSON.parse(read("src/hoopdb/sessions-function-acl.json")).functions as Record<string, string[]>;
    expect(manifest[LIST_SIG]).toEqual(["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"]);
    expect(manifest[ACTIVITY_SIG]).toEqual(["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"]);
    expect(manifest[SCOPE_SIG]).toEqual(["<owner>:EXECUTE"]);
    const rows = (await db.query<{ fn: string; grantees: string[]; definer: boolean }>(
      `SELECT p.oid::regprocedure::text AS fn, p.prosecdef AS definer,
              array(SELECT CASE WHEN a.grantee = p.proowner THEN '<owner>' ELSE coalesce(r.rolname, 'PUBLIC') END || ':' || a.privilege_type
                      FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
                      LEFT JOIN pg_roles AS r ON r.oid = a.grantee ORDER BY 1) AS grantees
         FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' AND p.proname IN ('list_session_directory_for', 'session_directory_activity_for', 'session_directory_read_api_scope')
        ORDER BY 1`,
    )).rows;
    expect(rows).toEqual([
      { fn: LIST_SIG, definer: true, grantees: ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"] },
      { fn: ACTIVITY_SIG, definer: true, grantees: ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"] },
      { fn: SCOPE_SIG, definer: false, grantees: ["<owner>:EXECUTE"] },
    ].sort((a, b) => (a.fn < b.fn ? -1 : 1)));
    await db.exec(VERIFY);
    // Any other definer grant to the read role still fails the verifier.
    await db.exec(`GRANT EXECUTE ON FUNCTION sessions.session_directory_activity(text, timestamptz, timestamptz) TO bulls96_read_api`);
    expect(await code(db.exec(VERIFY))).toBe("P0001");
  }, 120_000);

  test("the boundary wrapper and manifest are in sync (scripts/sync-read-api-directory-boundary.ts --check)", () => {
    const r = Bun.spawnSync(["bun", "scripts/sync-read-api-directory-boundary.ts", "--check"], { cwd: root });
    expect(r.exitCode, r.stderr.toString()).toBe(0);
  });
});

describe("bulls96_read_api bare role: bootstrap before migrate, verified by the migration", () => {
  test("the migration creates and alters no role", () => {
    const text = read(MIGRATION).replace(/--[^\n]*/g, "");
    expect(text).not.toMatch(/\b(CREATE|ALTER|DROP)\s+ROLE\b/i);
  });

  test("without the bootstrap the migration fails closed, 42704, naming the bootstrap", async () => {
    const error = await migrate(await fresh());
    expect(error?.code).toBe("42704");
    expect(error?.message).toContain("bulls96_read_api");
  }, 120_000);

  test("an elevated or member role fails the migration closed, 55000", async () => {
    for (const breakIt of ["ALTER ROLE bulls96_read_api INHERIT", "ALTER ROLE bulls96_read_api BYPASSRLS", "GRANT service_role TO bulls96_read_api", "GRANT bulls96_read_api TO authenticated"]) {
      const db = await fresh();
      await db.exec(read(BOOTSTRAP));
      await db.exec(breakIt);
      expect((await migrate(db))?.code, breakIt).toBe("55000");
    }
  }, 240_000);

  test("the bootstrap creates NOLOGIN, is idempotent, accepts the LOGIN production role, and never alters an elevated one", async () => {
    const db = await fresh();
    await db.exec(read(BOOTSTRAP));
    expect((await db.query("SELECT rolcanlogin, rolinherit, rolbypassrls FROM pg_roles WHERE rolname = 'bulls96_read_api'")).rows).toEqual([
      { rolcanlogin: false, rolinherit: false, rolbypassrls: false },
    ]);
    await db.exec(read(BOOTSTRAP));
    expect(await migrate(db)).toBeNull();
    await db.exec(ROLE);
    await db.exec(read(BOOTSTRAP)); // production: the role is LOGIN already
    await db.exec(VERIFY);
    await db.exec("ALTER ROLE bulls96_read_api INHERIT");
    expect(await code(db.exec(read(BOOTSTRAP)))).toBe("55000");
    expect((await db.query("SELECT rolinherit FROM pg_roles WHERE rolname = 'bulls96_read_api'")).rows).toEqual([{ rolinherit: true }]);
  }, 120_000);

  test("up, down, up", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    await db.exec(read(DOWN));
    expect((await db.query(`SELECT to_regprocedure('${LIST_SIG}') AS a, to_regprocedure('${ACTIVITY_SIG}') AS b, to_regprocedure('${SCOPE_SIG}') AS c`)).rows).toEqual([{ a: null, b: null, c: null }]);
    expect((await db.query("SELECT count(*)::int AS n FROM sessions.session_directory")).rows).toEqual([{ n: 7 }]);
    await db.exec(read(MIGRATION));
    expect((await asReadApi(db, ["org-a", "ws-a"], () => listFor(db, {}))).map((r) => r.session_id)).toEqual(["s1", "s2", "s3", "s4"]);
  }, 120_000);
});

describe("the read API end to end over the real functions (src/session-read-api/directory.ts)", () => {
  test("GET /v1/sessions and /activity as bulls96_read_api: pages walk once through b96s1 cursors, tenant-bound", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const statements: string[] = [];
    const pool = {
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, p?: unknown[]) => {
            statements.push(text);
            return db.query(text, p as unknown[]) as Promise<{ rows: unknown[] }>;
          },
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
    });
    const get = async (path: string) => {
      const r = await app.request(path, { headers: { authorization: "Bearer t" } });
      return { status: r.status, body: (await r.json()) as Record<string, any> };
    };
    const seen: string[] = [];
    let cursor: string | null = null;
    do {
      const r = await get(`/v1/sessions?workspace=ws-a&limit=1${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
      expect(r.status).toBe(200);
      seen.push(...r.body.sessions.map((s: { sessionId: string }) => s.sessionId));
      cursor = r.body.nextCursor;
    } while (cursor);
    expect(seen).toEqual(["s1", "s2", "s3", "s4"]);
    const first = await get("/v1/sessions?workspace=ws-a&source=codex&user=u1");
    expect(first.body.sessions.map((s: { sessionId: string }) => s.sessionId)).toEqual(["s1", "s3"]);
    expect(first.body.sessions[0]).toMatchObject({ sessionId: "s1", source: "codex", title: "First", userIds: ["u1"] });
    const activity = await get("/v1/sessions/activity?workspace=ws-a&since=2026-10-03T00:00:00Z&until=2026-10-04T23:59:59Z");
    expect(activity.status).toBe(200);
    expect(activity.body.days).toEqual([
      { date: "2026-10-03", total: 4, bySource: { claude: 2, codex: 2 } },
      { date: "2026-10-04", total: 2, bySource: { claude: 1, codex: 1 } },
    ]);
    expect(statements.filter((s) => s.startsWith("BEGIN")).every((s) => s === "BEGIN READ ONLY")).toBe(true);
  }, 120_000);
});
