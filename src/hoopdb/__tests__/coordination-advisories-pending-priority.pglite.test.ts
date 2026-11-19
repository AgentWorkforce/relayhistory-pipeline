/**
 * 062190 coordination_advisories_pending_for_v2: a new P1 is never hidden behind older LOW rows; the SQL rank and the
 * read API's advisoryPriority agree on a shared fixture; the limit clamp; grants; tenant isolation; v1 unchanged; the
 * read API's fallback to v1 on 42883; and the checksum-pinned rollback.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite, type Transaction } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf, RUNTIME_APPLICATION_FUNCTIONS } from "../migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { advisoryPriority } from "../../session-read-api/advisory-priority.ts";
import { sqlCoordinationAdvisories } from "../../session-read-api/coordination-advisories.ts";
import type { TenantRunner } from "../../session-read-api/liveness.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062190_coordination_advisories_pending_priority.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql");
const V1_SQL = read("supabase/migrations/20261105062000_coordination_advisories.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const V2 = "sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer)";
const V1 = "sessions.coordination_advisories_pending_for_v1(text,text,text,text)";

/** The shared fixture: every (urgency, kind) the ranking distinguishes, plus an unknown kind. */
const PRIORITY_FIXTURE: ReadonlyArray<{ urgency: "high" | "low"; kind: string }> = [
  "constraint_violation", "intent_conflict", "duplicate_work", "superseded_work", "concurrent_edit", "nearby_work",
  "similar_summary", "overlap", "planned_fleet", "stale_base", "made_up_kind",
].flatMap((kind) => (["low", "high"] as const).map((urgency) => ({ urgency, kind })));

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
  return db;
}
const hex = (n: number) => n.toString(16).padStart(16, "0");
const bodyOf = (src: string, name: string, tag: string) => {
  const start = src.search(new RegExp(`CREATE(?: OR REPLACE)? FUNCTION sessions\\.${name}\\(`));
  const open = src.indexOf(tag, start) + tag.length;
  return src.slice(open, src.indexOf(tag, open));
};

describe("20261105062190 coordination_advisories_pending_for_v2", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  /** Rows for recipient claude:s-a, created i minutes after a base, one advisory each. */
  const seed = async (org: string, ws: string, rows: Array<{ urgency: string; kind: string }>, first = 0) => {
    await as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_upsert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(rows.map((r, i) => ({
      advisory_id: hex(first + i), harness: "claude", session_id: "s-a", peer_harness: "codex", peer_session_id: "s-b",
      urgency: r.urgency, kind: r.kind, artifact: "a", evidence: [],
    })))]));
    for (let i = 0; i < rows.length; i++)
      await db.query("UPDATE sessions.coordination_advisories SET created_at = now() - interval '1 day' + make_interval(mins => $1) WHERE advisory_id = $2 AND org_id = $3", [first + i, hex(first + i), org]);
  };
  const v2 = (org: string, ws: string, limit: number | null = 10, scope: [string, string] = [org, ws], session = "s-a") =>
    as("bulls96_read_api", async (tx) => (await tx.query<{ advisory_id: string; urgency: string; kind: string }>(
      "SELECT * FROM sessions.coordination_advisories_pending_for_v2($1,$2,'claude',$3,$4)", [org, ws, session, limit])).rows, scope);
  const v1 = (org: string, ws: string) =>
    as("bulls96_read_api", async (tx) => (await tx.query<{ advisory_id: string }>(
      "SELECT * FROM sessions.coordination_advisories_pending_for_v1($1,$2,'claude','s-a')", [org, ws])).rows, [org, ws]);
  const reset = () => db.exec("DELETE FROM sessions.coordination_advisories");

  beforeAll(async () => { db = await migrated(); });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and grants: EXECUTE for bulls96_read_api only, definer, pinned search_path, not a runtime function; the read-API verifier passes", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`)).toEqual(["function:sessions.coordination_advisories_pending_for_v2"]);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(RUNTIME_APPLICATION_FUNCTIONS).not.toContain(V2);
    const r = (await db.query<{ secdef: boolean; volatile: string; cfg: string[]; public_exec: boolean }>(
      "SELECT p.prosecdef secdef, p.provolatile volatile, p.proconfig cfg, has_function_privilege('public', p.oid, 'EXECUTE') public_exec FROM pg_proc p WHERE p.oid = $1::regprocedure", [V2])).rows[0]!;
    expect(r).toEqual({ secdef: true, volatile: "s", cfg: ["search_path=pg_catalog, sessions, pg_temp"], public_exec: false });
    for (const role of ["service_role", "anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest", "bulls96_session_ingest_seeder"])
      expect([role, (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, V2])).rows[0]!.ok]).toEqual([role, false]);
    expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') ok", [V2])).rows[0]!.ok).toBe(true);
    expect(await code(as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_pending_for_v2('org-a','ws-a','claude','s-a',10)")))).toBe("42501");
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("12 LOW rows then 1 HIGH P1: v2 returns the P1 first and within the limit; v1 (oldest first, 10) still hides it", async () => {
    await reset();
    await seed("org-a", "ws-a", [...Array.from({ length: 12 }, () => ({ urgency: "low", kind: "nearby_work" })), { urgency: "high", kind: "duplicate_work" }]);
    const got = await v2("org-a", "ws-a", 10);
    expect(got.length).toBe(10);
    expect(got[0]).toMatchObject({ advisory_id: hex(12), urgency: "high", kind: "duplicate_work" });
    expect(got.slice(1).map((r) => r.advisory_id)).toEqual(Array.from({ length: 9 }, (_, i) => hex(i)));
    const old = await v1("org-a", "ws-a");
    expect(old.map((r) => r.advisory_id)).toEqual(Array.from({ length: 10 }, (_, i) => hex(i)));
  });

  test("the shared fixture: the SQL rank and advisoryPriority agree on every (urgency, kind), oldest first within a priority", async () => {
    await reset();
    await seed("org-a", "ws-a", PRIORITY_FIXTURE);
    const got = await v2("org-a", "ws-a", 50);
    const expected = PRIORITY_FIXTURE.map((r, i) => ({ ...r, i, p: advisoryPriority(r) }))
      .sort((a, b) => a.p - b.p || a.i - b.i).map((r) => hex(r.i));
    expect(got.map((r) => r.advisory_id)).toEqual(expected);
    // Every priority value is present, so the fixture exercises all three CASE branches.
    expect(new Set(PRIORITY_FIXTURE.map(advisoryPriority))).toEqual(new Set([1, 2, 3]));
  });

  test("p_limit is clamped to 1..50 (NULL is 10)", async () => {
    await reset();
    await seed("org-a", "ws-a", Array.from({ length: 60 }, () => ({ urgency: "low", kind: "overlap" })));
    for (const [limit, n] of [[0, 1], [-5, 1], [1, 1], [10, 10], [50, 50], [51, 50], [1000, 50], [null, 10]] as const)
      expect([limit, (await v2("org-a", "ws-a", limit)).length]).toEqual([limit, n]);
  });

  test("tenant isolation and the read scope; filters as v1 (acked, expired and older than 7 days excluded)", async () => {
    await reset();
    await seed("org-a", "ws-a", [{ urgency: "high", kind: "duplicate_work" }, { urgency: "low", kind: "overlap" }, { urgency: "low", kind: "overlap" }]);
    await seed("org-a", "ws-b", [{ urgency: "high", kind: "duplicate_work" }], 100);
    await seed("org-b", "ws-a", [{ urgency: "high", kind: "duplicate_work" }], 200);
    expect((await v2("org-a", "ws-a")).map((r) => r.advisory_id)).toEqual([hex(0), hex(1), hex(2)]);
    expect((await v2("org-a", "ws-b")).map((r) => r.advisory_id)).toEqual([hex(100)]);
    expect((await v2("org-b", "ws-a")).map((r) => r.advisory_id)).toEqual([hex(200)]);
    expect(await v2("org-a", "ws-a", 10, ["org-a", "ws-a"], "s-z")).toEqual([]);
    expect(await code(v2("org-b", "ws-a", 10, ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(v2("org-a", "ws-b", 10, ["org-a", "ws-a"]))).toBe("42501");
    await as("bulls96_read_api", (tx) => tx.query("SELECT sessions.coordination_advisories_ack_v1('org-a','ws-a',$1,'claude','s-a')", [hex(0)]), ["org-a", "ws-a"]);
    await db.query("UPDATE sessions.coordination_advisories SET status = 'expired' WHERE advisory_id = $1", [hex(1)]);
    await db.query("UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE advisory_id = $1", [hex(2)]);
    expect(await v2("org-a", "ws-a")).toEqual([]);
  });

  test("v1 is untouched: the 062000 body, its grants and its oldest-first order", async () => {
    expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid = $1::regprocedure", [V1])).rows[0]!.s)
      .toBe(bodyOf(V1_SQL, "coordination_advisories_pending_for_v1", "$coordination_advisories_pending_for$"));
    expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') ok", [V1])).rows[0]!.ok).toBe(true);
    expect(SQL).not.toMatch(/pending_for_v1\s*\(/);
  });

  test("the read API's source: v2 against the migrated tree; after the rollback, the v1 fallback with a counted warning", async () => {
    await reset();
    await seed("org-a", "ws-a", [...Array.from({ length: 11 }, () => ({ urgency: "low", kind: "overlap" })), { urgency: "high", kind: "intent_conflict" }]);
    const run: TenantRunner = (org, ws, work) => as("bulls96_read_api", (tx) => work({
      query: async <R,>(text: string, params?: unknown[]) => ({ rows: (await tx.query<R>(text, params)).rows }),
    }), [org, ws]);
    const warns: Record<string, unknown>[] = [];
    const source = sqlCoordinationAdvisories(run, (() => { throw new Error("no write"); }) as never, { warn: (f) => { warns.push(f); } });
    const recipient = { orgId: "org-a", workspaceId: "ws-a", source: "claude", sessionId: "s-a" };
    const first = await source.pending(recipient);
    expect(first[0]!.advisoryId).toBe(hex(11));
    expect(warns).toEqual([]);
    await db.exec(DOWN);
    const fallback = await source.pending(recipient);
    expect(fallback.map((a) => a.advisoryId)).toEqual(Array.from({ length: 10 }, (_, i) => hex(i)));
    expect(warns).toEqual([{ reason: "coordination_advisories_v1_fallback", error_code: "42883", fallbacks: 1 }]);
  });
});

describe("20261105062190 rollback", () => {
  test("checksum-pinned; 062170's down refuses under it; it drops only v2 and its ledger row; a rerun refuses", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    const db = await migrated();
    const message = async (sql: string) => { const m = await db.exec(sql).then(() => null, (e: Error) => e.message); await db.exec("ROLLBACK").catch(() => {}); return m; };
    try {
      expect((await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP)).toEqual([]);
      expect(await message(read("supabase/rollbacks/20261105062170_session_intel_workspace.down.sql"))).toContain("a later migration is applied");
      await db.exec(DOWN);
      expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [V2])).rows[0]!.r).toBeNull();
      expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [V1])).rows[0]!.r).not.toBeNull();
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.coordination_advisories')::text r")).rows[0]!.r).toBe("sessions.coordination_advisories");
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name = $1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await message(DOWN)).toContain("own applied checksum required");
    } finally { await db.close(); }
  });
});
