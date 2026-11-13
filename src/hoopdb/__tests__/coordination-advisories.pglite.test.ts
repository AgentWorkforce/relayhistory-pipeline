/** 062000 coordination advisories: role grants, tenant isolation, bounds, no-downgrade, retention and rollback. */
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

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062000_coordination_advisories.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062000_coordination_advisories.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const UPSERT = "sessions.coordination_advisories_upsert_v1(text,text,jsonb)";
const MARK = "sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text)";
const PENDING = "sessions.coordination_advisories_pending_for_v1(text,text,text,text)";
const ACK = "sessions.coordination_advisories_ack_v1(text,text,text,text,text)";
const SWEEP = "sessions.coordination_advisories_sweep_v1()";
const WORKER_FNS = [UPSERT, MARK, SWEEP];
const READER_FNS = [PENDING, ACK];
const ALL_FNS = [...WORKER_FNS, ...READER_FNS];

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
  return db;
}

type Adv = {
  advisory_id: string; harness: string; session_id: string; peer_harness: string; peer_session_id: string;
  urgency: string; kind: string; artifact: string; evidence: unknown;
};
const ev = (n: number) => Array.from({ length: n }, (_, i) => ({ sessionId: `s${i}`, eventId: `e${i}`, ts: "2026-11-12T09:58:12.000Z", kind: "tool_result" }));
const hex = (n: number) => n.toString(16).padStart(16, "0");
const adv = (over: Partial<Adv> = {}): Adv => ({
  advisory_id: hex(1), harness: "claude", session_id: "sess-a", peer_harness: "codex", peer_session_id: "sess-b",
  urgency: "high", kind: "concurrent_edit", artifact: "src/session-read-api", evidence: ev(2), ...over,
});

describe("20261105062000 coordination advisories", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const upsert = (org: string, ws: string, list: unknown) =>
    as("service_role", async (tx) => (await tx.query<{ inserted: number; upgraded: number; skipped: number }>(
      `SELECT * FROM ${UPSERT.replace("(text,text,jsonb)", "")}($1,$2,$3::jsonb)`, [org, ws, JSON.stringify(list)])).rows[0]!);
  const mark = (org: string, ws: string, id: string, h: string, s: string) =>
    as("service_role", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.coordination_advisories_mark_delivered_v1($1,$2,$3,$4,$5) AS ok", [org, ws, id, h, s])).rows[0]!.ok);
  const sweep = () => as("service_role", async (tx) => (await tx.query<{ expired: number; deleted: number }>(
    "SELECT * FROM sessions.coordination_advisories_sweep_v1()")).rows[0]!);
  const pending = (org: string, ws: string, h: string, s: string, scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<Record<string, unknown>>(
      "SELECT * FROM sessions.coordination_advisories_pending_for_v1($1,$2,$3,$4)", [org, ws, h, s])).rows, scope);
  const ack = (org: string, ws: string, id: string, h: string, s: string, scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.coordination_advisories_ack_v1($1,$2,$3,$4,$5) AS ok", [org, ws, id, h, s])).rows[0]!.ok, scope);
  const row = async (id: string, h = "claude", s = "sess-a") => (await db.query<Record<string, unknown>>(
    "SELECT * FROM sessions.coordination_advisories WHERE advisory_id=$1 AND harness=$2 AND session_id=$3", [id, h, s])).rows[0];
  const reset = () => db.exec("DELETE FROM sessions.coordination_advisories");

  beforeAll(async () => {
    db = await migrated();
    await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and runtime allowlist name exactly the table, the two indexes and the five functions", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.coordination_advisories_ack_v1", "function:sessions.coordination_advisories_mark_delivered_v1",
      "function:sessions.coordination_advisories_pending_for_v1", "function:sessions.coordination_advisories_sweep_v1",
      "function:sessions.coordination_advisories_upsert_v1", "index:coordination_advisories_pull",
      "index:coordination_advisories_sweep", "relation:sessions.coordination_advisories",
    ]);
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("coordination_advisories");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    const runtime = RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.coordination_advisories_")).sort();
    expect(runtime).toEqual([MARK, SWEEP, UPSERT].sort());
    // Columns: no transcript body, every id and tenant column NOT NULL.
    const cols = (await db.query<{ column_name: string; is_nullable: string }>(
      "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema='sessions' AND table_name='coordination_advisories' ORDER BY ordinal_position")).rows;
    expect(cols.map((c) => c.column_name)).toEqual(["advisory_id", "org_id", "workspace_id", "harness", "session_id", "peer_harness", "peer_session_id", "urgency", "kind", "artifact", "evidence", "created_at", "delivered_at", "acked_at", "status"]);
    expect(cols.filter((c) => c.is_nullable === "YES").map((c) => c.column_name)).toEqual(["delivered_at", "acked_at"]);
  });

  test("1. grants: PUBLIC cannot execute; each role runs only its own functions; no runtime role has a table privilege", async () => {
    for (const fn of ALL_FNS) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r.secdef).toBe(true);
      expect(r.cfg).toEqual(["search_path=pg_catalog, sessions, pg_temp"]);
      expect(r.public_exec).toBe(false);
    }
    await db.exec("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='coord_adv_probe') THEN CREATE ROLE coord_adv_probe NOLOGIN; END IF; END $$");
    const can = async (role: string, fn: string) =>
      (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of ALL_FNS) {
      expect(await can("coord_adv_probe", fn)).toBe(false);
      for (const r of ["anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest"]) expect(await can(r, fn)).toBe(false);
      expect(await can("service_role", fn)).toBe(WORKER_FNS.includes(fn));
      expect(await can("bulls96_read_api", fn)).toBe(READER_FNS.includes(fn));
    }
    for (const role of ["service_role", "bulls96_read_api", "coord_adv_probe"])
      for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
        expect((await db.query<{ ok: boolean }>("SELECT has_table_privilege($1, 'sessions.coordination_advisories', $2) ok", [role, priv])).rows[0]!.ok).toBe(false);
    // Calls across the role line are refused, and direct table access is refused.
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_sweep_v1()"), ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT sessions.coordination_advisories_upsert_v1('org-a','ws-a','[]'::jsonb)"), ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_pending_for_v1('org-a','ws-a','claude','sess-a')")))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT sessions.coordination_advisories_ack_v1('org-a','ws-a','0000000000000001','claude','sess-a')")))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT 1 FROM sessions.coordination_advisories")))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT 1 FROM sessions.coordination_advisories"), ["org-a", "ws-a"]))).toBe("42501");
    // The read-API role verifier still passes with the two new grants.
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("2. tenant isolation: readers see only their scope; another recipient's ack matches nothing", async () => {
    await reset();
    expect(await upsert("org-a", "ws-a", [adv(), adv({ harness: "codex", session_id: "sess-b", peer_harness: "claude", peer_session_id: "sess-a" })])).toEqual({ inserted: 2, upgraded: 0, skipped: 0 });
    expect(await upsert("org-b", "ws-b", [adv({ advisory_id: hex(2) })])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    // Same key under another tenant: never touched; both entries (one a batch duplicate) count as skipped.
    expect(await upsert("org-b", "ws-b", [adv({ urgency: "low" }), adv({ urgency: "high" })])).toEqual({ inserted: 0, upgraded: 0, skipped: 2 });
    expect((await row(hex(1)))!.org_id).toBe("org-a");
    expect((await pending("org-a", "ws-a", "claude", "sess-a")).map((r) => r.advisory_id)).toEqual([hex(1)]);
    expect((await pending("org-b", "ws-b", "claude", "sess-a")).map((r) => r.advisory_id)).toEqual([hex(2)]);
    expect(await pending("org-a", "ws-a", "claude", "sess-z")).toEqual([]);
    // The reader cannot step outside its request scope by naming another org or workspace.
    expect(await code(pending("org-b", "ws-a", "claude", "sess-a", ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(pending("org-a", "ws-b", "claude", "sess-a", ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(ack("org-b", "ws-b", hex(2), "claude", "sess-a", ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_pending_for_v1('org-a','ws-a','claude','sess-a')")))).toBe("42501");
    // Not the recipient: the peer session, an unknown session, and another tenant's id all match nothing.
    expect(await ack("org-a", "ws-a", hex(1), "codex", "sess-x")).toBe(false);
    expect(await ack("org-a", "ws-a", hex(2), "claude", "sess-a")).toBe(false);
    expect((await row(hex(1)))!.status).toBe("pending");
    expect((await row(hex(2)))!.status).toBe("pending");
    // The worker is tenant-filtered too.
    expect(await mark("org-b", "ws-b", hex(1), "claude", "sess-a")).toBe(false);
    expect((await row(hex(1)))!.status).toBe("pending");
    expect(await ack("org-a", "ws-a", hex(1), "codex", "sess-b")).toBe(true);
    expect((await row(hex(1), "codex", "sess-b"))!.status).toBe("acked");
    expect((await row(hex(1)))!.status).toBe("pending");
  });

  test("3. bounds: evidence, artifact, urgency, status, kind, ids and the list limit", async () => {
    await reset();
    expect(await code(upsert("org-a", "ws-a", [adv({ evidence: ev(11) })]))).toBe("22023");
    expect(await code(db.query("INSERT INTO sessions.coordination_advisories(advisory_id,org_id,workspace_id,harness,session_id,peer_harness,peer_session_id,urgency,kind,artifact,evidence) VALUES('0000000000000009','o','w','claude','s','codex','p','high','k','a',$1::jsonb)", [JSON.stringify(ev(11))]))).toBe("23514");
    expect(await upsert("org-a", "ws-a", [adv({ evidence: ev(10) })])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    for (const evidence of [
      {}, "x", [1], [{ sessionId: "s", eventId: "e", ts: "t" }], [{ sessionId: "s", eventId: "e", ts: "t", kind: 1 }],
      [{ sessionId: "s", eventId: "e", ts: "t", kind: "k", body: "transcript text" }], [[ev(1)[0]]], [null], [{}],
      [{ sessionId: "s", eventId: "e", ts: "t", kind: null }],
      [{ sessionId: "x".repeat(4100), eventId: "e", ts: "t", kind: "k" }],
    ]) expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(3), evidence })]))).toMatch(/^(22023|23514)$/);
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(3), artifact: "a".repeat(513) })]))).toBe("23514");
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: hex(3), artifact: "a".repeat(512) })])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(4), artifact: "" })]))).toBe("23514");
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(4), urgency: "medium" })]))).toBe("23514");
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(4), kind: "Bad Kind" })]))).toBe("23514");
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: "not-hex" })]))).toBe("23514");
    expect(await code(upsert("org-a", "ws-a", [adv({ advisory_id: hex(4), peer_harness: "claude", peer_session_id: "sess-a" })]))).toBe("23514");
    expect(await code(upsert("org-a", "ws-a", [{ ...adv({ advisory_id: hex(4) }), session_id: null }]))).toBe("23502");
    expect(await code(upsert("", "ws-a", [adv({ advisory_id: hex(4) })]))).toBe("22023");
    expect(await code(upsert("org-a", "ws-a", { not: "an array" }))).toBe("22023");
    expect(await code(upsert("org-a", "ws-a", Array.from({ length: 501 }, (_, i) => adv({ advisory_id: hex(1000 + i) }))))).toBe("22023");
    expect(await code(db.exec("UPDATE sessions.coordination_advisories SET status='bogus'"))).toBe("23514");
    // Status and timestamps stay consistent even for the owner.
    expect(await code(db.exec(`UPDATE sessions.coordination_advisories SET status='acked' WHERE advisory_id='${hex(1)}'`))).toBe("23514");
    expect(await code(db.exec(`UPDATE sessions.coordination_advisories SET status='delivered' WHERE advisory_id='${hex(1)}'`))).toBe("23514");
    await reset();
    // 12 pending: the reader returns 10, oldest first.
    expect(await upsert("org-a", "ws-a", Array.from({ length: 12 }, (_, i) => adv({ advisory_id: hex(100 + i) })))).toEqual({ inserted: 12, upgraded: 0, skipped: 0 });
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - make_interval(mins => 200 - (('x' || advisory_id)::bit(64)::bigint)::int)`);
    const listed = await pending("org-a", "ws-a", "claude", "sess-a");
    expect(listed.length).toBe(10);
    expect(listed.map((r) => r.advisory_id)).toEqual(Array.from({ length: 10 }, (_, i) => hex(100 + i)));
    expect(Object.keys(listed[0]!).sort()).toEqual(["advisory_id", "artifact", "created_at", "delivered_at", "evidence", "harness", "kind", "peer_harness", "peer_session_id", "session_id", "status", "urgency"]);
  });

  test("4. idempotent ack; no downgrade from upsert or mark-delivered after an ack; HIGH never becomes LOW", async () => {
    await reset();
    const id = hex(7);
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: id, urgency: "low" })])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    // A duplicate in one batch collapses to one row, HIGH first; an existing LOW pending row is upgraded.
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: hex(8), urgency: "low" }), adv({ advisory_id: hex(8), urgency: "high" })])).toEqual({ inserted: 1, upgraded: 0, skipped: 1 });
    expect((await row(hex(8)))!.urgency).toBe("high");
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: id, urgency: "high" })])).toEqual({ inserted: 0, upgraded: 1, skipped: 0 });
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: id, urgency: "low", artifact: "other" })])).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await row(id)).toMatchObject({ urgency: "high", status: "pending", artifact: "src/session-read-api", delivered_at: null });
    // Delivered: pending -> delivered; a second mark keeps the first delivered_at.
    expect(await mark("org-a", "ws-a", id, "claude", "sess-a")).toBe(true);
    const delivered = (await row(id))!;
    expect(delivered.status).toBe("delivered");
    expect(await mark("org-a", "ws-a", id, "claude", "sess-a")).toBe(true);
    expect((await row(id))!.delivered_at).toEqual(delivered.delivered_at);
    expect((await pending("org-a", "ws-a", "claude", "sess-a")).map((r) => [r.advisory_id, r.status])).toEqual([[id, "delivered"], [hex(8), "pending"]]);
    // Ack, twice: both match; acked_at and delivered_at do not move.
    expect(await ack("org-a", "ws-a", id, "claude", "sess-a")).toBe(true);
    const acked = (await row(id))!;
    expect(acked.status).toBe("acked");
    expect(acked.acked_at).not.toBeNull();
    expect(acked.delivered_at).toEqual(delivered.delivered_at);
    expect(await ack("org-a", "ws-a", id, "claude", "sess-a")).toBe(true);
    expect(await row(id)).toEqual(acked);
    // After the ack: neither the worker's upsert nor mark-delivered moves it back.
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: id, urgency: "high" })])).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await mark("org-a", "ws-a", id, "claude", "sess-a")).toBe(true);
    expect(await row(id)).toEqual(acked);
    expect((await pending("org-a", "ws-a", "claude", "sess-a")).map((r) => r.advisory_id)).toEqual([hex(8)]);
    // Ack of a never-delivered row sets delivered_at too.
    expect(await ack("org-a", "ws-a", hex(8), "claude", "sess-a")).toBe(true);
    const direct = (await row(hex(8)))!;
    expect(direct.status).toBe("acked");
    expect(direct.delivered_at).not.toBeNull();
    // An acked LOW is not upgraded.
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: hex(9), urgency: "low" })])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    expect(await ack("org-a", "ws-a", hex(9), "claude", "sess-a")).toBe(true);
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: hex(9), urgency: "high" })])).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect((await row(hex(9)))!.urgency).toBe("low");
  });

  test("5. retention: old unacked rows are expired then deleted, old acked rows deleted, younger rows untouched", async () => {
    await reset();
    expect(await sweep()).toEqual({ expired: 0, deleted: 0 });
    await upsert("org-a", "ws-a", [0, 1, 2, 3, 4, 5].map((i) => adv({ advisory_id: hex(200 + i) })));
    await mark("org-a", "ws-a", hex(201), "claude", "sess-a");
    await ack("org-a", "ws-a", hex(202), "claude", "sess-a");
    await mark("org-a", "ws-a", hex(204), "claude", "sess-a");
    await ack("org-a", "ws-a", hex(205), "claude", "sess-a");
    // 200 pending, 201 delivered, 202 acked: older than 7 days. 203 pending, 204 delivered, 205 acked: 6 days old.
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE advisory_id IN ('${hex(200)}','${hex(201)}','${hex(202)}')`);
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '6 days' WHERE advisory_id IN ('${hex(203)}','${hex(204)}','${hex(205)}')`);
    // An old row not yet swept is no longer listed.
    expect((await pending("org-a", "ws-a", "claude", "sess-a")).map((r) => r.advisory_id)).toEqual([hex(203), hex(204)]);
    const before = (await db.query("SELECT * FROM sessions.coordination_advisories WHERE created_at > now() - interval '7 days' ORDER BY advisory_id")).rows;
    expect(await sweep()).toEqual({ expired: 2, deleted: 3 });
    expect((await db.query("SELECT * FROM sessions.coordination_advisories ORDER BY advisory_id")).rows).toEqual(before);
    expect(await sweep()).toEqual({ expired: 0, deleted: 0 });
    // An expired row (as the owner left it) is never matched by mark, ack or upgraded by upsert, and the sweep deletes it.
    await upsert("org-a", "ws-a", [adv({ advisory_id: hex(210), urgency: "low" })]);
    await db.exec(`UPDATE sessions.coordination_advisories SET status='expired' WHERE advisory_id='${hex(210)}'`);
    expect(await mark("org-a", "ws-a", hex(210), "claude", "sess-a")).toBe(false);
    expect(await ack("org-a", "ws-a", hex(210), "claude", "sess-a")).toBe(false);
    expect(await upsert("org-a", "ws-a", [adv({ advisory_id: hex(210), urgency: "high" })])).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await sweep()).toEqual({ expired: 0, deleted: 1 });
    expect(await row(hex(210))).toBeUndefined();
  });
});

describe("20261105062000 rollback", () => {
  test("6. the checksum-pinned down after the up leaves no table, no function and no ledger row", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    const db = await migrated();
    try {
      const later = (await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).reverse();
      if (later.length) {
        expect(await code(db.exec(DOWN))).toBe("55000");
        await db.exec("ROLLBACK");
      }
      for (const step of later)
        await db.exec(read(`supabase/rollbacks/${step.name.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`));
      await db.exec(DOWN);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.coordination_advisories')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'coordination\\_advisories\\_%'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_class WHERE relnamespace='sessions'::regnamespace AND relname LIKE 'coordination\\_advisories%'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      // Its ledger row is gone, so a rerun refuses.
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
    } finally { await db.close(); }
  });
});
