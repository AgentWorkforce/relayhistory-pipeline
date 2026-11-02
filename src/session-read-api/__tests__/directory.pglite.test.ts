/**
 * Directory search (GET /v1/directory/search) end to end in PGlite with every
 * Supabase migration, the read role and its directory grant, executed AS the role
 * (SET ROLE) the way production logs in: the grant's verification, the RLS fence,
 * and the route's scope, erasure, tombstone, matching, pointer-only and paging rules.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { sqlDirectorySearch } from "../directory.ts";
import { poolTenantRunner } from "../liveness.ts";
import { memoryLiveness } from "./support.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const ROLE = sql("supabase/bootstrap/session_read_api_role.sql");
const DIRECTORY = sql("supabase/bootstrap/session_read_api_directory.sql");
const VERIFY = sql("supabase/bootstrap/session_read_api_role_verify.sql");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(opts: { role?: boolean; directory?: boolean } = {}): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  if (opts.role ?? true) await db.exec(ROLE);
  if (opts.directory ?? true) await db.exec(DIRECTORY);
  return db;
}

interface Session {
  org?: string;
  ws?: string;
  harness?: string;
  session: string;
  at?: string;
  title?: string | null;
  task?: string | null;
  repo?: string | null;
  search?: string;
  state?: string;
  deleted?: boolean;
  /** Pointer rows: "live" (default one live), "erased" (all tombstoned), "none", or "half" (one of two tombstoned). */
  pointers?: "live" | "erased" | "none" | "half";
}

async function pointer(db: PGlite, s: Required<Pick<Session, "org" | "ws" | "harness" | "session" | "at">>, machine: string, deleted: boolean) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, $3, $4, $5, 'u', $6::timestamptz, $6::timestamptz, 's3', 'bucket', 'k/' || $1 || $2 || $3 || $4 || $5,
             decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2,
             CASE WHEN $7 THEN now() END)`,
    [s.org, s.ws, s.harness, s.session, machine, s.at, deleted],
  );
}

/** One directory row and its pointers, written as the superuser (the projector's job in production). */
async function session(db: PGlite, input: Session) {
  const s = { org: input.org ?? "org-a", ws: input.ws ?? "ws-a", harness: input.harness ?? "codex", session: input.session, at: input.at ?? "2026-10-01T00:00:00.000001Z" };
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title, git_repo, state, search_text, deleted_at)
     VALUES ($1, $2, $3, $4, $5::timestamptz, $5::timestamptz, $6, $7, $8, $9, $10, CASE WHEN $11 THEN now() END)`,
    [s.org, s.ws, s.harness, s.session, s.at, input.title === undefined ? `title of ${s.session}` : input.title, input.task ?? null, input.repo ?? null, input.state ?? "done", input.deleted ? "" : (input.search ?? ""), input.deleted ?? false],
  );
  const p = input.pointers ?? "live";
  if (p === "live") await pointer(db, s, "m-1", false);
  if (p === "erased") {
    await pointer(db, s, "m-1", true);
    await pointer(db, s, "m-2", true);
  }
  if (p === "half") {
    await pointer(db, s, "m-1", true);
    await pointer(db, s, "m-2", false);
  }
}

/** A pool whose clients are the read role, as a production login is. */
function asRole(db: PGlite) {
  return {
    async connect() {
      await db.exec("SET ROLE bulls96_read_api");
      return {
        query: (text: string, params?: unknown[]) => db.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
        release: () => void db.exec("RESET ROLE"),
      };
    },
  };
}

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const noCh = {} as ClickHouseClient;

function api(db: PGlite, over: { identity?: VerifiedIdentity; allow?: string[] } = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    },
    ch: noCh,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    directory: sqlDirectorySearch(poolTenantRunner(asRole(db))),
    directoryWorkspaces: new Set(over.allow ?? ["ws-a"]),
  });
  return async (query: string) => {
    const r = await app.request(`/v1/directory/search?${query}`, { headers: { authorization: "Bearer good" } });
    return { status: r.status, body: (await r.json()) as Record<string, any> };
  };
}

const ids = (body: Record<string, any>) => (body.sessions as { session_id: string }[]).map((s) => s.session_id);

describe("session read API directory grant", () => {
  test("the grant passes the role verification and both bootstraps are idempotent", async () => {
    const db = await database();
    await db.exec(ROLE);
    await db.exec(DIRECTORY);
    await db.exec(DIRECTORY);
    await db.exec(VERIFY);
  }, 60_000);

  test("verification requires the directory grant: the phase A role alone fails", async () => {
    const db = await database({ directory: false });
    await expect(db.exec(VERIFY)).rejects.toThrow(/policy session_directory_read_api is missing/);
  }, 60_000);

  test("the directory grant refuses to run before the role exists", async () => {
    const db = await database({ role: false, directory: false });
    await expect(db.exec(DIRECTORY)).rejects.toThrow(/bulls96_read_api does not exist/);
  }, 60_000);

  test("a re-run narrows a wider hand-made grant back to the eleven columns", async () => {
    const db = await database();
    await db.exec("GRANT SELECT ON sessions.session_directory TO bulls96_read_api");
    await expect(db.exec(VERIFY)).rejects.toThrow("not in the approved shape");
    await db.exec(DIRECTORY);
    await expect(db.exec(VERIFY)).resolves.toBeDefined();
  }, 60_000);

  test.each([
    ["DROP POLICY session_directory_read_api ON sessions.session_directory", /session_directory_read_api is missing/],
    ["ALTER POLICY session_directory_read_api ON sessions.session_directory USING (true)", /not bound to the tenant settings/],
    [
      "ALTER POLICY session_directory_read_api ON sessions.session_directory USING ((org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true)) OR true)",
      /not bound to the tenant settings/,
    ],
    ["CREATE POLICY wide_open ON sessions.session_directory FOR SELECT USING (true)", /applies to PUBLIC/],
    ["CREATE POLICY extra ON sessions.session_directory_users FOR SELECT TO bulls96_read_api USING (true)", /unapproved policy extra/],
    ["GRANT SELECT (short_summary) ON sessions.session_directory TO bulls96_read_api", /column short_summary: SELECT is granted/],
    ["GRANT SELECT (user_ids, machine_ids) ON sessions.session_directory TO bulls96_read_api", /column user_ids: SELECT is granted/],
    ["REVOKE SELECT (search_text) ON sessions.session_directory FROM bulls96_read_api", /column search_text: SELECT is missing/],
    ["GRANT SELECT ON sessions.session_directory_users TO bulls96_read_api", /SELECT on sessions.session_directory_users/],
    ["GRANT UPDATE (title) ON sessions.session_directory TO bulls96_read_api", /write privilege on sessions.session_directory/],
    ["ALTER TABLE sessions.session_directory DISABLE ROW LEVEL SECURITY", /RLS is off on sessions.session_directory/],
    ["GRANT bulls96_read_api TO authenticated", /granted to authenticated/],
  ] as const)("verification fails on any widening of the directory grant or policy: %s", async (breakIt, why) => {
    const db = await database();
    await db.exec(breakIt);
    await expect(db.exec(VERIFY)).rejects.toThrow(why);
  }, 60_000);

  test("the role reads only the granted columns and cannot write", async () => {
    const db = await database();
    await session(db, { session: "s-1" });
    await db.exec("SET ROLE bulls96_read_api");
    for (const q of [
      "SELECT short_summary FROM sessions.session_directory",
      "SELECT user_ids FROM sessions.session_directory",
      "SELECT cost_usd_micros FROM sessions.session_directory",
      "SELECT * FROM sessions.session_directory",
      "SELECT * FROM sessions.session_directory_users",
      "SELECT * FROM sessions.session_briefs",
    ])
      await expect(db.query(q)).rejects.toThrow(/permission denied/);
    await expect(db.query("UPDATE sessions.session_directory SET title = 'x'")).rejects.toThrow();
    await db.exec("RESET ROLE");
  }, 60_000);

  test("directory rows are visible only for the tenant the transaction bound; unbound sees nothing", async () => {
    const db = await database();
    await session(db, { session: "s-1" });
    await session(db, { ws: "ws-b", session: "s-1" });
    await session(db, { org: "org-b", session: "s-1" });
    await db.exec("SET ROLE bulls96_read_api");
    expect((await db.query("SELECT session_id FROM sessions.session_directory")).rows).toHaveLength(0);
    await db.exec("BEGIN READ ONLY");
    await db.query("SELECT set_config('bulls96_read.org_id', 'org-a', true), set_config('bulls96_read.workspace_id', 'ws-a', true)");
    expect((await db.query("SELECT org_id, workspace_id FROM sessions.session_directory")).rows).toEqual([{ org_id: "org-a", workspace_id: "ws-a" }]);
    // A statement that names another tenant gets nothing: RLS fences it whatever the SQL says.
    expect((await db.query("SELECT session_id FROM sessions.session_directory WHERE workspace_id = 'ws-b'")).rows).toHaveLength(0);
    await db.exec("COMMIT");
    expect((await db.query("SELECT session_id FROM sessions.session_directory")).rows).toHaveLength(0);
    await db.exec("RESET ROLE");
  }, 60_000);
});

describe("directory search as the role", () => {
  test("matches title, task_title, git_repo and search_text, case-insensitively", async () => {
    const db = await database();
    await session(db, { session: "by-title", title: "Fix the Widget login", at: "2026-10-05T00:00:00.000001Z" });
    await session(db, { session: "by-task", title: "x", task: "widget rollout", at: "2026-10-04T00:00:00.000001Z" });
    await session(db, { session: "by-repo", title: "x", repo: "github.com/acme/widgets", at: "2026-10-03T00:00:00.000001Z" });
    await session(db, { session: "by-search", title: "x", search: "x model-a summary mentions WIDGET", at: "2026-10-02T00:00:00.000001Z" });
    await session(db, { session: "no-match", title: "unrelated", at: "2026-10-06T00:00:00.000001Z" });
    const r = await api(db)("workspace=ws-a&q=widget");
    expect(r.status).toBe(200);
    expect(ids(r.body)).toEqual(["by-title", "by-task", "by-repo", "by-search"]);
  }, 60_000);

  test("returns pointers only: exactly session_id, workspace_id, title, git_repo, last_activity_at, state", async () => {
    const db = await database();
    await session(db, { session: "s-1", title: "widget", repo: "acme/widgets", state: "active", task: "secret task", search: "widget summary text", at: "2026-10-01T12:34:56.789012Z" });
    const r = await api(db)("workspace=ws-a&q=widget");
    expect(r.body.sessions).toEqual([
      { session_id: "s-1", workspace_id: "ws-a", title: "widget", git_repo: "acme/widgets", last_activity_at: "2026-10-01T12:34:56.789012Z", state: "active" },
    ]);
    expect(Object.keys(r.body).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
    expect(JSON.stringify(r.body)).not.toContain("summary text");
    expect(JSON.stringify(r.body)).not.toContain("secret task");
  }, 60_000);

  test("scope: another workspace or org is never served, even with a matching title", async () => {
    const db = await database();
    await session(db, { session: "mine", title: "widget" });
    await session(db, { ws: "ws-b", session: "other-ws", title: "widget" });
    await session(db, { org: "org-b", session: "other-org", title: "widget" });
    expect(ids((await api(db)("workspace=ws-a&q=widget")).body)).toEqual(["mine"]);
    // The workspace parameter must equal the token's.
    expect((await api(db)("workspace=ws-b&q=widget")).status).toBe(403);
    // A token for ws-b (allowlisted) sees only ws-b.
    const wsB = api(db, { identity: { ...ID, workspaceId: "ws-b" }, allow: ["ws-a", "ws-b"] });
    expect(ids((await wsB("workspace=ws-b&q=widget")).body)).toEqual(["other-ws"]);
  }, 60_000);

  test("erased sessions are excluded: every pointer tombstoned, or no pointer at all", async () => {
    const db = await database();
    await session(db, { session: "live", title: "widget", at: "2026-10-04T00:00:00.000001Z" });
    await session(db, { session: "half", title: "widget", pointers: "half", at: "2026-10-03T00:00:00.000001Z" });
    await session(db, { session: "erased", title: "widget", pointers: "erased", at: "2026-10-02T00:00:00.000001Z" });
    await session(db, { session: "no-pointer", title: "widget", pointers: "none", at: "2026-10-01T00:00:00.000001Z" });
    expect(ids((await api(db)("workspace=ws-a&q=widget")).body)).toEqual(["live", "half"]);
  }, 60_000);

  test("tombstoned directory rows (deleted_at) are excluded", async () => {
    const db = await database();
    await session(db, { session: "live", title: "widget", at: "2026-10-02T00:00:00.000001Z" });
    await session(db, { session: "tombstoned", title: "widget", deleted: true, at: "2026-10-03T00:00:00.000001Z" });
    expect(ids((await api(db)("workspace=ws-a&q=widget")).body)).toEqual(["live"]);
  }, 60_000);

  test("the query is a literal substring: % _ and backslash match only themselves", async () => {
    const db = await database();
    await session(db, { session: "literal", title: "50%_off\\sale" });
    // Matches the same query read as a pattern (% any, _ one character, \\s an escaped s).
    await session(db, { session: "wildcard", title: "50xxoffsale" });
    expect(ids((await api(db)("workspace=ws-a&q=" + encodeURIComponent("50%_off\\s"))).body)).toEqual(["literal"]);
  }, 60_000);

  test("pagination: keyset pages are exact across equal timestamps, with no repeat and no gap", async () => {
    const db = await database();
    const same = "2026-10-02T00:00:00.000005Z";
    await session(db, { session: "s-a", title: "widget", at: "2026-10-03T00:00:00.000001Z" });
    await session(db, { session: "s-b", title: "widget", at: same });
    await session(db, { session: "s-c", title: "widget", at: same });
    await session(db, { session: "s-c", harness: "claude", title: "widget", at: same });
    await session(db, { session: "s-d", title: "widget", at: "2026-10-02T00:00:00.000004Z" });
    await session(db, { session: "s-e", title: "widget", at: "2026-10-01T00:00:00.000001Z" });
    const get = api(db);
    const seen: string[] = [];
    let cursor: string | null = null;
    let pages = 0;
    do {
      const r = await get(`workspace=ws-a&q=widget&limit=2${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
      expect(r.status).toBe(200);
      seen.push(...ids(r.body));
      cursor = r.body.nextCursor;
      pages++;
    } while (cursor && pages < 10);
    // Newest first; the tie at `same` is broken by session_id DESC then harness DESC (codex > claude).
    expect(seen).toEqual(["s-a", "s-c", "s-c", "s-b", "s-d", "s-e"]);
    expect(pages).toBe(3);
  }, 60_000);

  test("a full last page has no cursor; a page short of limit+1 ends the read", async () => {
    const db = await database();
    await session(db, { session: "s-1", title: "widget", at: "2026-10-02T00:00:00.000001Z" });
    await session(db, { session: "s-2", title: "widget", at: "2026-10-01T00:00:00.000001Z" });
    const get = api(db);
    const exact = await get("workspace=ws-a&q=widget&limit=2");
    expect(ids(exact.body)).toEqual(["s-1", "s-2"]);
    expect(exact.body.nextCursor).toBeNull();
    const first = await get("workspace=ws-a&q=widget&limit=1");
    expect(first.body.nextCursor).toStartWith("b96d1:");
  }, 60_000);
});
