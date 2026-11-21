/**
 * 062170 session intelligence workspace readers (docs/session-intel-workspace-api.md): grants (bulls96_read_api only,
 * PUBLIC revoked, search_path pinned), the read-scope pin and tenant isolation, the dedupe of an advisory's two
 * recipient rows, acked and expired rows, titles only for claims, the per-session caps, bounds and the rollback.
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
import { advisoryItem, sqlCoordinationAdvisories } from "../../session-read-api/coordination-advisories.ts";
import type { TenantRunner } from "../../session-read-api/liveness.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062170_session_intel_workspace.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062170_session_intel_workspace.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const ALERTS = "sessions.session_intel_workspace_alerts_v1(text,text,integer)";
const CLAIMS = "sessions.session_intel_workspace_claims_v1(text,text,text[],integer)";

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
const ev = (session: string, i: number) => ({ sessionId: session, eventId: `e${i}`, ts: `2026-11-12T09:58:1${i}.000Z`, kind: "tool_result" });

type AlertRow = {
  advisory_id: string; urgency: string; kind: string; artifact: string; pair: unknown; recipients: unknown;
  created_at: Date; delivered_at: Date | null; evidence: unknown;
};
type ClaimRow = { source: string; session_id: string; kind: string; title: string; updated_at: Date };

describe("20261105062170 session intelligence workspace readers", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const upsert = (org: string, ws: string, list: unknown) =>
    as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_upsert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)]));
  const pair = (id: string, a: [string, string], b: [string, string], over: Record<string, unknown> = {}) => [
    { advisory_id: id, harness: a[0], session_id: a[1], peer_harness: b[0], peer_session_id: b[1], urgency: "high", kind: "duplicate_work", artifact: "supabase/migrations/062130_x.sql", evidence: [ev(a[1], 1), ev(b[1], 2)], ...over },
    { advisory_id: id, harness: b[0], session_id: b[1], peer_harness: a[0], peer_session_id: a[1], urgency: "high", kind: "duplicate_work", artifact: "supabase/migrations/062130_x.sql", evidence: [ev(a[1], 1), ev(b[1], 2)], ...over },
  ];
  const alerts = (org: string, ws: string, limit = 100, scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<AlertRow>("SELECT * FROM sessions.session_intel_workspace_alerts_v1($1,$2,$3)", [org, ws, limit])).rows, scope);
  const claims = (org: string, ws: string, keys: string[], limit = 2400, scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<ClaimRow>("SELECT * FROM sessions.session_intel_workspace_claims_v1($1,$2,$3::text[],$4)", [org, ws, keys, limit])).rows, scope);
  const ack = (org: string, ws: string, id: string, h: string, s: string) =>
    as("bulls96_read_api", (tx) => tx.query("SELECT sessions.coordination_advisories_ack_v1($1,$2,$3,$4,$5)", [org, ws, id, h, s]), [org, ws]);
  const evd = (session: string, claim = "the claim text, never returned") => ({ source: "claude", session_id: session, turn_start: 0, turn_end: 1, claim });
  const claimIn = (org: string, ws: string, list: unknown) =>
    as("service_role", (tx) => tx.query("SELECT * FROM sessions.relayfacts_claims_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)]));
  const decideIn = (org: string, ws: string, list: unknown) =>
    as("service_role", (tx) => tx.query("SELECT * FROM sessions.relayfacts_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)]));
  const reset = () => db.exec("DELETE FROM sessions.coordination_advisories; DELETE FROM sessions.relayfacts_claims_v1; DELETE FROM sessions.relayfacts_v1;");

  beforeAll(async () => { db = await migrated(); });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel, and grants: EXECUTE for bulls96_read_api only, definer with a pinned search_path, never a runtime function", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.session_intel_workspace_alerts_v1", "function:sessions.session_intel_workspace_claims_v1",
    ]);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.session_intel_workspace_"))).toEqual([]);
    for (const fn of [ALERTS, CLAIMS]) {
      const r = (await db.query<{ secdef: boolean; volatile: string; cfg: string[]; public_exec: boolean }>(
        `SELECT p.prosecdef secdef, p.provolatile volatile, p.proconfig cfg,
                has_function_privilege('public', p.oid, 'EXECUTE') public_exec
           FROM pg_proc p WHERE p.oid = $1::regprocedure`, [fn])).rows[0]!;
      expect([fn, r.secdef, r.volatile, r.cfg, r.public_exec]).toEqual([fn, true, "s", ["search_path=pg_catalog, sessions, pg_temp"], false]);
      for (const role of ["service_role", "anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest", "bulls96_session_ingest_seeder"])
        expect([fn, role, (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok]).toEqual([fn, role, false]);
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') ok", [fn])).rows[0]!.ok).toBe(true);
    }
    // No table privilege for the read role on the tables the readers read.
    for (const t of ["sessions.coordination_advisories", "sessions.relayfacts_claims_v1", "sessions.relayfacts_claim_evidence_v1", "sessions.relayfacts_v1", "sessions.relayfacts_evidence_v1"])
      expect([t, (await db.query<{ ok: boolean }>("SELECT has_table_privilege('bulls96_read_api', $1, 'SELECT') ok", [t])).rows[0]!.ok]).toEqual([t, false]);
    // service_role (the worker) cannot call them: the read scope requires the read API role.
    expect(await code(as("service_role", (tx) => tx.query("SELECT * FROM sessions.session_intel_workspace_alerts_v1('org-a','ws-a',10)")))).toBe("42501");
  });

  test("alerts: one row per advisory with both sessions, per-recipient acked, newest first; acked-by-both and expired excluded; tenant isolated", async () => {
    await reset();
    await upsert("org-a", "ws-a", pair(hex(1), ["claude", "s-a"], ["codex", "s-b"]));
    await upsert("org-a", "ws-a", pair(hex(2), ["claude", "s-c"], ["codex", "s-d"], { urgency: "low", kind: "nearby_work", artifact: "src", evidence: [] }));
    await upsert("org-a", "ws-a", pair(hex(3), ["claude", "s-e"], ["codex", "s-f"]));
    await upsert("org-a", "ws-a", pair(hex(4), ["claude", "s-g"], ["codex", "s-h"]));
    // Another workspace and another org hold rows too.
    await upsert("org-a", "ws-b", pair(hex(5), ["claude", "x-a"], ["codex", "x-b"]));
    await upsert("org-b", "ws-a", pair(hex(6), ["claude", "y-a"], ["codex", "y-b"]));
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '1 hour' WHERE advisory_id = '${hex(1)}';
                   UPDATE sessions.coordination_advisories SET created_at = now() - interval '30 minutes' WHERE advisory_id = '${hex(2)}';
                   UPDATE sessions.coordination_advisories SET status = 'expired' WHERE advisory_id = '${hex(4)}';`);
    // hex(1): one recipient acked, still open for the other. hex(3): both acked, closed.
    await ack("org-a", "ws-a", hex(1), "claude", "s-a");
    await ack("org-a", "ws-a", hex(3), "claude", "s-e");
    await ack("org-a", "ws-a", hex(3), "codex", "s-f");
    const rows = await alerts("org-a", "ws-a");
    expect(rows.map((r) => r.advisory_id)).toEqual([hex(2), hex(1)]);
    const [low, high] = rows;
    expect(low!.pair).toEqual([{ source: "claude", sessionId: "s-c" }, { source: "codex", sessionId: "s-d" }]);
    expect(low!.urgency).toBe("low");
    expect(low!.delivered_at).toBeNull();
    expect(low!.evidence).toEqual([]);
    expect(high!.pair).toEqual([{ source: "claude", sessionId: "s-a" }, { source: "codex", sessionId: "s-b" }]);
    expect(high!.recipients).toEqual([{ source: "claude", sessionId: "s-a", acked: true }, { source: "codex", sessionId: "s-b", acked: false }]);
    expect(high!.delivered_at).not.toBeNull();
    // The two recipient rows carry the same evidence: deduplicated.
    expect(high!.evidence).toEqual([ev("s-a", 1), ev("s-b", 2)]);
    expect(Object.keys(high!).sort()).toEqual(["advisory_id", "artifact", "created_at", "delivered_at", "evidence", "kind", "pair", "recipients", "urgency"]);
    expect((await alerts("org-a", "ws-b")).map((r) => r.advisory_id)).toEqual([hex(5)]);
    expect((await alerts("org-b", "ws-a")).map((r) => r.advisory_id)).toEqual([hex(6)]);
    // The limit bounds advisories, not recipient rows.
    expect((await alerts("org-a", "ws-a", 1)).map((r) => r.advisory_id)).toEqual([hex(2)]);
    // An advisory older than the 7-day retention but not yet swept is not open.
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE advisory_id = '${hex(2)}'`);
    expect((await alerts("org-a", "ws-a")).map((r) => r.advisory_id)).toEqual([hex(1)]);
  });

  test("advisories source with the priority switch on (062190's v2), migrated tree: a HIGH duplicate pair is priority 1; delivery.dm is false until the DM path marks the recipient's row, then true with its time; the peer's row stays false", async () => {
    await reset();
    await upsert("org-a", "ws-a", pair(hex(7), ["claude", "s-a"], ["codex", "s-b"]));
    const run: TenantRunner = (org, ws, work) => as("bulls96_read_api", (tx) => work({
      query: async <R,>(text: string, params?: unknown[]) => ({ rows: (await tx.query<R>(text, params)).rows }),
    }), [org, ws]);
    const source = sqlCoordinationAdvisories(run, (() => { throw new Error("no write"); }) as never, { priority: true });
    const recipient = { orgId: "org-a", workspaceId: "ws-a", source: "claude", sessionId: "s-a" };
    const item = (r: Parameters<typeof advisoryItem>[0]) => advisoryItem(r, new Set(), true) as ReturnType<typeof advisoryItem> & { priority: number; delivery: { dm: boolean; deliveredAt: string | null } };
    const before = (await source.pending(recipient)).map(item);
    expect(before.map((i) => [i.advisoryId, i.priority, i.delivery])).toEqual([[hex(7), 1, { dm: false, deliveredAt: null }]]);
    await as("service_role", (tx) => tx.query("SELECT sessions.coordination_advisories_mark_delivered_v1('org-a','ws-a',$1,'claude','s-a')", [hex(7)]));
    const after = (await source.pending(recipient)).map(item);
    expect(after[0]!.delivery.dm).toBe(true);
    expect(Date.parse(after[0]!.delivery.deliveredAt!)).toBeGreaterThan(Date.now() - 60_000);
    // The peer's row was not DMed: still dm false.
    expect((await source.pending({ ...recipient, source: "codex", sessionId: "s-b" })).map(item)[0]!.delivery).toEqual({ dm: false, deliveredAt: null });
  });

  test("alerts and claims refuse an org or workspace outside the read scope (42501) and out-of-bound arguments (22023)", async () => {
    expect(await code(alerts("org-b", "ws-a", 10, ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(alerts("org-a", "ws-b", 10, ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(claims("org-b", "ws-a", ["claude:s-1"], 10, ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(claims("org-a", "ws-b", ["claude:s-1"], 10, ["org-a", "ws-a"]))).toBe("42501");
    for (const limit of [0, 101]) expect(await code(alerts("org-a", "ws-a", limit))).toBe("22023");
    for (const limit of [0, 2401]) expect(await code(claims("org-a", "ws-a", ["claude:s-1"], limit))).toBe("22023");
    for (const keys of [[], ["no-colon"], ["Claude:s"], ["claude:"], ["claude:a\nb"], Array.from({ length: 301 }, (_, i) => `claude:s-${i}`)])
      expect([keys.slice(0, 2), await code(claims("org-a", "ws-a", keys))]).toEqual([keys.slice(0, 2), "22023"]);
  });

  test("claims: titles only (cut to 120), current intents (<= 3) and decisions (<= 5) per session, newest first; constraints and superseded excluded; tenant isolated", async () => {
    await reset();
    const long = "T".repeat(150);
    await claimIn("org-a", "ws-a", [
      ...Array.from({ length: 4 }, (_, i) => ({ kind: "intent", topic: "t", title: `intent ${i}`, statement: "STATEMENT never returned", evidence: [evd("s-1")] })),
      { kind: "intent", topic: "t", title: long, statement: "x", evidence: [evd("s-2"), evd("s-1:with:colons")] },
      { kind: "constraint", topic: "t", title: "a constraint", statement: "x", evidence: [evd("s-2")] },
      { kind: "intent", topic: "t", title: "old intent", statement: "x", status: "open", evidence: [evd("s-2")] },
    ]);
    await claimIn("org-a", "ws-b", [{ kind: "intent", topic: "t", title: "other workspace", statement: "x", evidence: [evd("s-1")] }]);
    await decideIn("org-a", "ws-a", Array.from({ length: 6 }, (_, i) => ({ topic: "t", title: `decision ${i}`, statement: "STATEMENT", evidence: [evd("s-1"), { ...evd("s-1"), turn_start: 2, turn_end: 3 }] })));
    // Make the order deterministic: higher i is newer.
    await db.exec(`UPDATE sessions.relayfacts_claims_v1 SET updated_at = now() - make_interval(mins => 10 - coalesce(substring(title from '\\d+$')::int, 0));
                   UPDATE sessions.relayfacts_v1 SET updated_at = now() - make_interval(mins => 10 - substring(title from '\\d+$')::int);`);
    // Supersede "old intent" by the long one (same kind, workspace).
    const ids = (await db.query<{ id: string; title: string }>("SELECT id, title FROM sessions.relayfacts_claims_v1 WHERE workspace_id = 'ws-a'")).rows;
    const id = (t: string) => ids.find((r) => r.title === t)!.id;
    await as("service_role", (tx) => tx.query("SELECT sessions.relayfacts_claims_supersede_v1('org-a','ws-a',$1::uuid,$2::uuid)", [id("old intent"), id(long)]));
    const rows = await claims("org-a", "ws-a", ["claude:s-1", "claude:s-2", "claude:s-1:with:colons", "codex:s-1", "claude:s-1"]);
    expect(rows.map((r) => [r.source, r.session_id, r.kind, r.title])).toEqual([
      ["claude", "s-1", "decision", "decision 5"], ["claude", "s-1", "decision", "decision 4"], ["claude", "s-1", "decision", "decision 3"],
      ["claude", "s-1", "decision", "decision 2"], ["claude", "s-1", "decision", "decision 1"],
      ["claude", "s-1", "intent", "intent 3"], ["claude", "s-1", "intent", "intent 2"], ["claude", "s-1", "intent", "intent 1"],
      ["claude", "s-1:with:colons", "intent", "T".repeat(120)],
      ["claude", "s-2", "intent", "T".repeat(120)],
    ]);
    expect(Object.keys(rows[0]!).sort()).toEqual(["kind", "session_id", "source", "title", "updated_at"]);
    expect(JSON.stringify(rows)).not.toMatch(/STATEMENT|never returned|constraint/);
    expect((await claims("org-a", "ws-b", ["claude:s-1"])).map((r) => r.title)).toEqual(["other workspace"]);
    expect(await claims("org-b", "ws-a", ["claude:s-1"])).toEqual([]);
    expect((await claims("org-a", "ws-a", ["claude:s-1"], 2)).length).toBe(2);
  });
});

describe("20261105062170 rollback", () => {
  test("checksum-pinned; 062190 comes off first; 062160's down refuses under it; it drops both readers and its ledger row only, and refuses on a rerun", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    const db = await migrated();
    const message = async (sql: string) => { const m = await db.exec(sql).then(() => null, (e: Error) => e.message); await db.exec("ROLLBACK"); return m; };
    try {
      // 20261105062180 (claims scheduler), 20261105062190 (the priority-first advisories reader), 20261105062200
      // (session list readiness) and 20261105062210 (T2 staged ACK) are the later steps: this down refuses under them;
      // they come off first (062210, 062200, 062190, then 062180).
      expect((await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).map((s) => s.name)).toEqual(["upstream:20261105062180_claims_scheduler.sql", "upstream:20261105062190_coordination_advisories_pending_priority.sql", "upstream:20261105062200_session_directory_list_readiness.sql", "upstream:20261105062210_lazy_ack_t2.sql"]);
      expect(await message(DOWN)).toContain("a later migration is applied");
      await db.exec(read("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql")); // 062210 (T2 staged ACK) sits above 062200: first.
      await db.exec(read("supabase/rollbacks/20261105062200_session_directory_list_readiness.down.sql"));
      expect(await message(DOWN)).toContain("a later migration is applied");
      await db.exec(read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql"));
      expect(await message(DOWN)).toContain("a later migration is applied");
      await db.exec(read("supabase/rollbacks/20261105062180_claims_scheduler.down.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
      expect(await message(read("supabase/rollbacks/20261105062160_session_intel_controls.down.sql"))).toContain("a later migration is applied");
      await db.exec(DOWN);
      for (const fn of [ALERTS, CLAIMS])
        expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      for (const t of ["sessions.coordination_advisories", "sessions.relayfacts_claims_v1", "sessions.relayfacts_v1", "sessions.session_intel_controls"])
        expect((await db.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [t])).rows[0]!.r).toBe(t);
      expect(await message(DOWN)).toContain("own applied checksum required");
      await db.exec("ROLLBACK").catch(() => {});
      // Re-applying restores both readers; then 062160 comes off after this down.
      await db.exec(SQL);
      expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [ALERTS])).rows[0]!.r).not.toBeNull();
    } finally { await db.close(); }
  });
});
