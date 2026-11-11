/**
 * Migration 20261105057000 (GET /v1/sessions/users) on PGlite with every migration:
 * sessions.session_directory_users_for runs AS bulls96_read_api with the tenant bound the way
 * the read Worker binds it. Covers per-user grouping (a multi-user session counts for each),
 * the list's [after, before) bounds, org/workspace isolation and the 42501 refusal, deleted
 * (erased) rows, top projects, the limit + 1 truncation row, the 22023 validations, the ACL,
 * the index range, the guarded down, and sqlDirectory end to end. Every id is synthetic.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { TENANT_SETTINGS_SQL, poolTenantRunner } from "../../session-read-api/liveness.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105057000_session_directory_users_aggregate.sql";
const DOWN = read("supabase/rollbacks/20261105057000_session_directory_users_aggregate.down.sql");
const SIG = "sessions.session_directory_users_for(text,timestamptz,timestamptz,integer)";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(opts: { skip?: string } = {}): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  const files = readdirSync(join(root, "supabase/migrations")).sort();
  expect(files).toContain(FILE);
  for (const file of files) if (file !== opts.skip) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  return db;
}

type Session = { org?: string; workspace?: string; id: string; last: string; users?: string[]; project?: string | null; deleted?: boolean; harness?: string };
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions)
    await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, project_id, user_ids, deleted_at)
       VALUES ($1, $2, $3, $4, $5, $5, 'secret title', $6, $7::text[], CASE WHEN $8::boolean THEN now() END)`,
      [s.org ?? "org-a", s.workspace ?? "ws-a", s.harness ?? "codex", s.id, s.last, s.project ?? null, s.users ?? [], s.deleted ?? false],
    );
}

/** As the read Worker: SET ROLE, BEGIN READ ONLY, bind the tenant, run, COMMIT. */
async function asReadApi<R>(db: PGlite, tenant: [string, string] | null, work: () => Promise<R>, role = "bulls96_read_api"): Promise<R> {
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

type Row = { user_id: string; sessions: number; last_session_at: string; top_projects: Array<{ name: string; sessions: number }> };
const users = (db: PGlite, after: string, before: string, o: { limit?: number; workspace?: string; tenant?: [string, string] | null; role?: string } = {}) =>
  asReadApi(db, o.tenant === undefined ? ["org-a", "ws-a"] : o.tenant, async () =>
    (await db.query<Row>(
      `SELECT user_id, sessions::int AS sessions, to_char(last_session_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_session_at, top_projects
         FROM sessions.session_directory_users_for($1, $2, $3, $4)`,
      [o.workspace ?? "ws-a", after, before, o.limit ?? 1000],
    )).rows, o.role);
const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

const AFTER = "2026-10-01T00:00:00Z";
const BEFORE = "2026-10-31T00:00:00Z";

describe("sessions.session_directory_users_for (057000)", () => {
  test("groups by each user id, counts multi-user sessions once per user, honours [after, before), deletion and tenant", async () => {
    const db = await database();
    await seed(db, [
      { id: "a1", last: "2026-10-01T00:00:00Z", users: ["u1"], project: "p-api" }, // at the after bound: counted
      { id: "a2", last: "2026-10-05T00:00:00Z", users: ["u1", "u2"], project: "p-web" }, // two users: one each
      { id: "a3", last: "2026-10-06T00:00:00Z", users: ["u1", "u1"], project: "p-web" }, // duplicate id counts once
      { id: "a4", last: "2026-10-07T00:00:00Z", users: ["u1"], project: null }, // null project: counted, no project
      { id: "a4e", last: "2026-10-06T12:00:00Z", users: ["u1"], project: "" }, // empty project: counted, no project
      { id: "a5", last: "2026-10-08T00:00:00Z", users: ["u2"], project: "p-api", harness: "claude" },
      { id: "a6", last: "2026-10-09T00:00:00Z", users: [] }, // no user: nobody
      { id: "early", last: "2026-09-30T23:59:59Z", users: ["u1"] },
      { id: "edge", last: BEFORE, users: ["u1"] }, // at the before bound: not counted (the list's < before)
      { id: "gone", last: "2026-10-10T00:00:00Z", users: ["u1", "u3"], deleted: true }, // erased
      { id: "ws-b", workspace: "ws-b", last: "2026-10-10T00:00:00Z", users: ["u1", "u4"] },
      { id: "org-b", org: "org-b", last: "2026-10-10T00:00:00Z", users: ["u1", "u5"] },
    ]);
    expect(await users(db, AFTER, BEFORE)).toEqual([
      { user_id: "u1", sessions: 5, last_session_at: "2026-10-07T00:00:00Z", top_projects: [{ name: "p-web", sessions: 2 }, { name: "p-api", sessions: 1 }] },
      { user_id: "u2", sessions: 2, last_session_at: "2026-10-08T00:00:00Z", top_projects: [{ name: "p-api", sessions: 1 }, { name: "p-web", sessions: 1 }] },
    ]);
    // The other workspace only through its own binding.
    expect((await users(db, AFTER, BEFORE, { workspace: "ws-b", tenant: ["org-a", "ws-b"] })).map((r) => [r.user_id, r.sessions])).toEqual([["u1", 1], ["u4", 1]]);
    // A binding for ws-a cannot read ws-b; a missing binding or another role refuses too.
    expect(await code(users(db, AFTER, BEFORE, { workspace: "ws-b" }))).toBe("42501");
    expect(await code(users(db, AFTER, BEFORE, { tenant: null }))).toBe("42501");
    expect(await code(users(db, AFTER, BEFORE, { role: "authenticated" }))).toBe("42501");
    // Another org bound to the same workspace id sees only its own rows.
    expect((await users(db, AFTER, BEFORE, { tenant: ["org-b", "ws-a"] })).map((r) => r.user_id)).toEqual(["u1", "u5"]);
  }, 120_000);

  test("top projects: at most 3, count desc then name; ties and the limit + 1 truncation row", async () => {
    const db = await database();
    const rows: Session[] = [];
    let i = 0;
    for (const [project, n] of [["p-d", 1], ["p-c", 2], ["p-b", 2], ["p-a", 1], ["p-e", 3]] as const)
      for (let k = 0; k < n; k++) rows.push({ id: `s${i++}`, last: `2026-10-${String(10 + k).padStart(2, "0")}T00:00:00Z`, users: ["top"], project });
    for (const u of ["z1", "z2", "z3"]) rows.push({ id: `s${i++}`, last: "2026-10-02T00:00:00Z", users: [u] });
    await seed(db, rows);
    const all = await users(db, AFTER, BEFORE);
    expect(all[0]).toEqual({ user_id: "top", sessions: 9, last_session_at: "2026-10-12T00:00:00Z", top_projects: [{ name: "p-e", sessions: 3 }, { name: "p-b", sessions: 2 }, { name: "p-c", sessions: 2 }] });
    expect(all.map((r) => r.user_id)).toEqual(["top", "z1", "z2", "z3"]);
    // limit 2: the function returns 3 rows (limit + 1) so the caller can mark truncation.
    expect((await users(db, AFTER, BEFORE, { limit: 2 })).map((r) => r.user_id)).toEqual(["top", "z1", "z2"]);
  }, 120_000);

  test("validations refuse 22023; EXECUTE is bulls96_read_api's alone; the scan is the workspace last-activity index range", async () => {
    const db = await database();
    expect(await code(users(db, BEFORE, AFTER))).toBe("22023");
    expect(await code(users(db, "2025-01-01T00:00:00Z", BEFORE))).toBe("22023");
    for (const limit of [0, 1001]) expect(await code(users(db, AFTER, BEFORE, { limit }))).toBe("22023");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => db.query("SELECT * FROM sessions.session_directory_users_for('ws-a', NULL, now(), 10)")))).toBe("22023");
    const acl = await db.query<{ role: string; allowed: boolean }>(
      `SELECT rolname AS role, has_function_privilege(oid, '${SIG}', 'EXECUTE') AS allowed FROM pg_roles
        WHERE rolname IN ('bulls96_read_api', 'anon', 'authenticated', 'service_role', 'search_consumer_runtime')`);
    expect(acl.rows.length).toBeGreaterThan(1);
    for (const r of acl.rows) expect(r.allowed).toBe(r.role === "bulls96_read_api");
    const secdef = await db.query<{ prosecdef: boolean }>(`SELECT prosecdef FROM pg_proc WHERE oid = '${SIG}'::regprocedure`);
    expect(secdef.rows[0]!.prosecdef).toBe(true);
    // The function's range predicate (as written in it) is an index range on (org, workspace, last_activity_at).
    await db.exec("SET enable_seqscan = off");
    const plan = (await db.query<{ "QUERY PLAN": string }>(
      `EXPLAIN SELECT d.harness, d.session_id, d.project_id, d.last_activity_at, d.user_ids FROM sessions.session_directory AS d
        WHERE d.org_id = 'org-a' AND d.workspace_id = 'ws-a' AND d.deleted_at IS NULL
          AND d.last_activity_at >= '${AFTER}' AND d.last_activity_at < '${BEFORE}'`)).rows.map((r) => r["QUERY PLAN"]).join("\n");
    expect(plan).toMatch(/Index (Only )?Scan using session_directory_\w+_idx on session_directory d/);
    expect(plan).toMatch(/Index Cond: \(\(org_id = 'org-a'::text\) AND \(workspace_id = 'ws-a'::text\) AND \(last_activity_at >= /);
  }, 120_000);

  test("the guarded down pins this migration's checksum and drops only the reader", async () => {
    const db = await database();
    expect(DOWN).toContain(createHash("sha256").update(read(`supabase/migrations/${FILE}`)).digest("hex"));
    // Without its ledger row the down refuses.
    await db.exec("CREATE TABLE sessions.__96bulls_migrations (name text PRIMARY KEY, sha256 text NOT NULL)");
    expect(await code(db.exec(DOWN))).toBe("55000");
    await db.exec("ROLLBACK");
    await db.query("INSERT INTO sessions.__96bulls_migrations VALUES ($1, $2)", [`upstream:${FILE}`, createHash("sha256").update(read(`supabase/migrations/${FILE}`)).digest("hex")]);
    // A later upstream step refuses it too.
    await db.query("INSERT INTO sessions.__96bulls_migrations VALUES ('upstream:20261105099000_later.sql', 'x')");
    expect(await code(db.exec(DOWN))).toBe("55000");
    await db.exec("ROLLBACK");
    await db.exec("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105099000_later.sql'");
    await db.exec(DOWN);
    expect((await db.query<{ p: string | null }>(`SELECT to_regprocedure('${SIG}')::text AS p`)).rows[0]!.p).toBeNull();
    expect((await db.query<{ p: string | null }>("SELECT to_regprocedure('sessions.session_directory_activity_for(text,timestamptz,timestamptz)')::text AS p")).rows[0]!.p).not.toBeNull();
    expect((await db.query("SELECT 1 FROM sessions.__96bulls_migrations")).rows).toEqual([]);
  }, 120_000);
});

describe("sqlDirectory.users", () => {
  const runner = (db: PGlite) => poolTenantRunner({
    async connect() {
      await db.exec("SET ROLE bulls96_read_api");
      return {
        query: (text: string, params?: unknown[]) => db.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
        release: () => void db.exec("RESET ROLE"),
      };
    },
  });

  test("returns the aggregate inside the tenant-bound transaction, and null when the function is not installed", async () => {
    const db = await database();
    await seed(db, [{ id: "a1", last: "2026-10-03T00:00:00Z", users: ["u1", "u2"], project: "p" }]);
    const rows = await sqlDirectory(runner(db)).users!("org-a", "ws-a", new Date(AFTER), new Date(BEFORE), 1000);
    expect(rows!.map((r) => [r.user_id, Number(r.sessions), new Date(r.last_session_at).toISOString(), r.top_projects])).toEqual([
      ["u1", 1, "2026-10-03T00:00:00.000Z", [{ name: "p", sessions: 1 }]],
      ["u2", 1, "2026-10-03T00:00:00.000Z", [{ name: "p", sessions: 1 }]],
    ]);
    const without = await database({ skip: FILE });
    expect(await sqlDirectory(runner(without)).users!("org-a", "ws-a", new Date(AFTER), new Date(BEFORE), 1000)).toBeNull();
  }, 120_000);
});
