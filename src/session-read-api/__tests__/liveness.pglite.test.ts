/**
 * The read API's Supabase role and liveness SQL, in PGlite with every Supabase
 * migration, executed AS the role (SET ROLE) the way production logs in.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { poolTenantRunner, sqlLiveness, sqlOperatorErased } from "../liveness.ts";
import { createReadApi } from "../app.ts";
import { SESSION_SOURCES_MAX, sqlSessionSources } from "../session-sources.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const ROLE = sql("supabase/bootstrap/session_read_api_role.sql");
const VERIFY = sql("supabase/bootstrap/session_read_api_role_verify.sql");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(withRole = true): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  if (withRole) await db.exec(ROLE);
  return db;
}

async function pointer(db: PGlite, p: { org?: string; ws?: string; harness?: string; session: string; machine?: string; created?: string; deleted?: boolean }) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, $3, $4, $5, 'u', $6::timestamptz, $6::timestamptz, 's3', 'bucket', 'k/' || $4 || $5,
             decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2,
             CASE WHEN $7 THEN now() END)`,
    [p.org ?? "org-a", p.ws ?? "ws-a", p.harness ?? "codex", p.session, p.machine ?? "m-1", p.created ?? "2026-10-01T00:00:00Z", p.deleted ?? false],
  );
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

describe("session read API role", () => {
  test("the bootstrap passes its own verification and is idempotent", async () => {
    const db = await database();
    await db.exec(ROLE);
    await db.exec(VERIFY);
  }, 60_000);

  test("verification fails without the policy, with an extra column, or with a write", async () => {
    for (const breakIt of [
      "DROP POLICY session_index_read_api ON sessions.session_index",
      "GRANT SELECT (title) ON sessions.session_index TO bulls96_read_api",
      "GRANT SELECT ON sessions.session_directory TO bulls96_read_api",
      "GRANT UPDATE (deleted_at) ON sessions.session_index TO bulls96_read_api",
      "ALTER ROLE bulls96_read_api BYPASSRLS",
      "ALTER ROLE bulls96_read_api RESET default_transaction_read_only",
    ]) {
      const db = await database();
      await db.exec(breakIt);
      await expect(db.exec(VERIFY)).rejects.toThrow("not in the approved shape");
    }
  }, 60_000);

  test("the role cannot read content columns or write", async () => {
    const db = await database();
    await pointer(db, { session: "s-1" });
    await db.exec("SET ROLE bulls96_read_api");
    await expect(db.query("SELECT title FROM sessions.session_index")).rejects.toThrow(/permission denied/);
    await expect(db.query("SELECT * FROM sessions.session_directory")).rejects.toThrow(/permission denied/);
    await expect(db.query("UPDATE sessions.session_index SET deleted_at = now()")).rejects.toThrow();
    await db.exec("RESET ROLE");
  }, 60_000);

  test("rows are visible only for the tenant the transaction bound; unbound sees nothing", async () => {
    const db = await database();
    await pointer(db, { session: "s-1" });
    await pointer(db, { ws: "ws-b", session: "s-1" });
    await pointer(db, { org: "org-b", session: "s-1" });
    await db.exec("SET ROLE bulls96_read_api");
    expect((await db.query("SELECT session_id FROM sessions.session_index")).rows).toHaveLength(0);
    await db.exec("BEGIN READ ONLY");
    await db.query("SELECT set_config('bulls96_read.org_id', 'org-a', true), set_config('bulls96_read.workspace_id', 'ws-a', true)");
    // Even a query that asks for another tenant's rows gets only the bound one's.
    const rows = (await db.query<{ workspace_id: string; org_id: string }>("SELECT org_id, workspace_id FROM sessions.session_index")).rows;
    expect(rows).toEqual([{ org_id: "org-a", workspace_id: "ws-a" }]);
    await db.exec("COMMIT");
    expect((await db.query("SELECT session_id FROM sessions.session_index")).rows).toHaveLength(0);
    await db.exec("RESET ROLE");
  }, 60_000);
});

describe("sqlLiveness as the role", () => {
  test("erased = every pointer row tombstoned; partial or absent is live", async () => {
    const db = await database();
    await pointer(db, { session: "gone", machine: "m-1", deleted: true });
    await pointer(db, { session: "gone", machine: "m-2", deleted: true });
    await pointer(db, { session: "half", machine: "m-1", deleted: true });
    await pointer(db, { session: "half", machine: "m-2" });
    await pointer(db, { session: "live" });
    // Same ids erased in another workspace must not leak in.
    await pointer(db, { ws: "ws-b", session: "live", deleted: true });
    const liveness = sqlLiveness(poolTenantRunner(asRole(db)));
    const erased = await liveness.erased("org-a", "ws-a", [
      { source: "codex", sessionId: "gone" },
      { source: "codex", sessionId: "half" },
      { source: "codex", sessionId: "live" },
      { source: "codex", sessionId: "absent" },
      { source: "claude", sessionId: "gone" },
    ]);
    expect([...erased]).toEqual(["codex\u0000gone"]);
  }, 60_000);

  test("livePointerSessions: ids with a live pointer in this workspace, any harness", async () => {
    const db = await database();
    await pointer(db, { session: "live" });
    await pointer(db, { session: "half", machine: "m-1", deleted: true });
    await pointer(db, { session: "half", harness: "claude", machine: "m-2" });
    await pointer(db, { session: "gone", deleted: true });
    // Live elsewhere only: another workspace, another org.
    await pointer(db, { ws: "ws-b", session: "other-ws" });
    await pointer(db, { org: "org-b", session: "other-org" });
    const liveness = sqlLiveness(poolTenantRunner(asRole(db)));
    const ids = ["live", "half", "gone", "other-ws", "other-org", "absent"];
    expect([...(await liveness.livePointerSessions("org-a", "ws-a", ids))].sort()).toEqual(["half", "live"]);
    expect([...(await liveness.livePointerSessions("org-a", "ws-b", ids))]).toEqual(["other-ws"]);
    expect([...(await liveness.livePointerSessions("org-a", "ws-a", []))]).toEqual([]);
  }, 60_000);

  test("sessionStart: earliest live created_at, pointer counts, optional harness", async () => {
    const db = await database();
    await pointer(db, { session: "s", machine: "m-1", created: "2026-10-02T00:00:00Z" });
    await pointer(db, { session: "s", machine: "m-2", created: "2026-10-01T00:00:00Z" });
    await pointer(db, { session: "s", harness: "claude", machine: "m-3", created: "2026-09-01T00:00:00Z", deleted: true });
    const liveness = sqlLiveness(poolTenantRunner(asRole(db)));
    expect(await liveness.sessionStart("org-a", "ws-a", "s")).toEqual({ pointers: 3, live: 2, createdAtMs: Date.parse("2026-10-01T00:00:00Z") });
    expect(await liveness.sessionStart("org-a", "ws-a", "s", "claude")).toEqual({ pointers: 1, live: 0, createdAtMs: null });
    expect(await liveness.sessionStart("org-a", "ws-b", "s")).toEqual({ pointers: 0, live: 0, createdAtMs: null });
  }, 60_000);
});

describe("sqlSessionSources as the role (GET /v1/sessions/:sessionId/sources)", () => {
  test("distinct live harnesses for the id in the bound tenant: 0, 1, 2; tombstones, other tenants and other ids never count", async () => {
    const db = await database();
    // One harness on two machines (one tombstoned): one source.
    await pointer(db, { session: "one", machine: "m-1" });
    await pointer(db, { session: "one", machine: "m-2", deleted: true });
    // Two harnesses live: ambiguous.
    await pointer(db, { session: "two", harness: "codex" });
    await pointer(db, { session: "two", harness: "claude", machine: "m-2" });
    await pointer(db, { session: "two", harness: "claude", machine: "m-3" });
    // A third harness only tombstoned: not a source.
    await pointer(db, { session: "two", harness: "opencode", machine: "m-4", deleted: true });
    // Erased (every pointer tombstoned).
    await pointer(db, { session: "gone", deleted: true });
    // The same id live elsewhere only.
    await pointer(db, { ws: "ws-b", session: "elsewhere", harness: "claude" });
    await pointer(db, { org: "org-b", session: "elsewhere", harness: "codex" });
    const sources = sqlSessionSources(poolTenantRunner(asRole(db)));
    const harnesses = async (org: string, ws: string, id: string) => (await sources(org, ws, id)).map((r) => r.harness);
    expect(await sources("org-a", "ws-a", "one")).toEqual([{ org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_ids: ["one"], canonical_ids: [] }]);
    expect(await harnesses("org-a", "ws-a", "two")).toEqual(["claude", "codex"]);
    expect(await harnesses("org-a", "ws-a", "gone")).toEqual([]);
    expect(await harnesses("org-a", "ws-a", "absent")).toEqual([]);
    expect(await harnesses("org-a", "ws-a", "elsewhere")).toEqual([]);
    expect(await harnesses("org-a", "ws-b", "elsewhere")).toEqual(["claude"]);
    expect(await harnesses("org-b", "ws-a", "elsewhere")).toEqual(["codex"]);
    // Ids are exact: no prefix or case folding.
    expect(await harnesses("org-a", "ws-a", "on")).toEqual([]);
    expect(await harnesses("org-a", "ws-a", "ONE")).toEqual([]);
  }, 60_000);

  test("the source RelayHistory id (canonical_session_id) and the physical id both match", async () => {
    const db = await database();
    await pointer(db, { session: "phys-1", harness: "claude" });
    await db.query("UPDATE sessions.session_index SET canonical_session_id = 'source-id' WHERE session_id = 'phys-1'");
    await pointer(db, { session: "source-id", harness: "codex" });
    const sources = sqlSessionSources(poolTenantRunner(asRole(db)));
    expect((await sources("org-a", "ws-a", "source-id")).map((r) => r.harness)).toEqual(["claude", "codex"]);
    expect((await sources("org-a", "ws-a", "phys-1")).map((r) => r.harness)).toEqual(["claude"]);
    // Every form of the key comes back for the fence check.
    expect(await sources("org-a", "ws-a", "source-id")).toEqual([
      { org_id: "org-a", workspace_id: "ws-a", harness: "claude", session_ids: ["phys-1"], canonical_ids: ["source-id"] },
      { org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_ids: ["source-id"], canonical_ids: [] },
    ]);
  }, 60_000);

  test("RLS still holds if the statement's own tenant binding were wrong: another bound tenant sees nothing", async () => {
    const db = await database();
    await pointer(db, { session: "s", harness: "claude" });
    // Bound to ws-b, asking for ws-a's rows: the policy admits none.
    const run = poolTenantRunner(asRole(db));
    const rows = await run("org-a", "ws-b", (q) => q.query("SELECT DISTINCT harness FROM sessions.session_index WHERE org_id = 'org-a' AND workspace_id = 'ws-a' AND session_id = 's' AND deleted_at IS NULL"));
    expect(rows.rows).toEqual([]);
  }, 60_000);

  test("bounded to SESSION_SOURCES_MAX, sorted by name", async () => {
    const db = await database();
    for (let i = 0; i < SESSION_SOURCES_MAX + 2; i++) await pointer(db, { session: "many", harness: `h${String(i).padStart(2, "0")}` });
    const got = (await sqlSessionSources(poolTenantRunner(asRole(db)))("org-a", "ws-a", "many")).map((r) => r.harness);
    expect(got).toHaveLength(SESSION_SOURCES_MAX);
    expect(got).toEqual([...got].sort());
    expect(got[0]).toBe("h00");
  }, 60_000);
});

describe("GET /v1/sessions/:sessionId/sources with the operator erase fence, as the role", () => {
  test("a fence on the physical alias hides the source when the caller asks the source id, and the reverse", async () => {
    const db = await database();
    // claude: physical P, source id X (aliased); codex: physical X. Both live (an erase pending its tombstone).
    await pointer(db, { session: "P", harness: "claude" });
    await db.query("UPDATE sessions.session_index SET canonical_session_id = 'X' WHERE session_id = 'P'");
    await pointer(db, { session: "X", harness: "codex" });
    const run = poolTenantRunner(asRole(db));
    const app = (fence: boolean) => createReadApi({
      verify: async () => ({ userId: "u", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
      ch: { database: "d", async command() { throw new Error("never"); }, async insert() { throw new Error("never"); }, async select() { throw new Error("never"); } },
      liveness: sqlLiveness(run),
      completeSinceMs: 0,
      directoryWorkspaces: "*",
      sessionSources: sqlSessionSources(run),
      ...(fence ? { operatorErased: sqlOperatorErased(run) } : {}),
    });
    const get = async (fence: boolean, id: string) => (await (await app(fence).request(`/v1/sessions/${id}/sources?workspace=ws-a`, { headers: { authorization: "Bearer t" } })).json()) as { sources: string[] };
    expect((await get(true, "X")).sources).toEqual(["claude", "codex"]);
    // The erase fence names the physical id only (as written for an alias), on claude.
    await db.query("INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id) VALUES ('org-a', 'ws-a', 'claude', 'P')");
    expect((await get(true, "X")).sources).toEqual(["codex"]);
    expect((await get(true, "P")).sources).toEqual([]);
    // Without the fence wired (the hosted entries), the live rows still answer.
    expect((await get(false, "X")).sources).toEqual(["claude", "codex"]);
    // A fence under the source id hides the codex copy whose physical id is X too.
    await db.query("INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id) VALUES ('org-a', 'ws-a', 'codex', 'X')");
    expect((await get(true, "X")).sources).toEqual([]);
  }, 60_000);
});
