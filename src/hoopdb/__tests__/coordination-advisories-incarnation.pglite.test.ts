/**
 * 062220 coordination advisories, the advisory instance: the column shape (nullable, no default, no backfill), the
 * legacy path through the REAL v1 writer after the migration, upsert_v2's insert and promotion rotation, no
 * resurrection, ack_v2's three-way result and the stale-instance cases, the v1 fences, pending_for_v3, grants, and the
 * checksum-pinned rollback that restores 062000's exact v1 bodies.
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

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062220_coordination_advisories_incarnation.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062220_coordination_advisories_incarnation.down.sql");
// 20261105062250 (mode p of the T2 gate row) sits above 062220: its down runs first.
const PREPLAN_DOWN = read("supabase/rollbacks/20261105062250_lazy_ack_preplan.down.sql");
const PREPLAN_STEP = "upstream:20261105062250_lazy_ack_preplan.sql";
const V1_SQL = read("supabase/migrations/20261105062000_coordination_advisories.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const UPSERT_V2 = "sessions.coordination_advisories_upsert_v2(text,text,jsonb)";
const ACK_V2 = "sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid)";
const PENDING_V3 = "sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer)";
const FENCED = [
  ["sessions.coordination_advisories_upsert_v1(text,text,jsonb)", "coordination_advisories_upsert_v1", "$coordination_advisories_upsert$"],
  ["sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text)", "coordination_advisories_mark_delivered_v1", "$coordination_advisories_mark_delivered$"],
  ["sessions.coordination_advisories_ack_v1(text,text,text,text,text)", "coordination_advisories_ack_v1", "$coordination_advisories_ack$"],
] as const;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

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
type Counts = { inserted: number; upgraded: number; skipped: number };
type Row = { advisory_id: string; urgency: string; status: string; delivered_at: Date | null; acked_at: Date | null; incarnation: string | null };

function harness(get: () => PGlite) {
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    get().transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const adv = (id: number, urgency: "high" | "low", kind = "duplicate_work", session = "s-a") => ({
    advisory_id: hex(id), harness: "claude", session_id: session, peer_harness: "codex", peer_session_id: "s-b",
    urgency, kind, artifact: "src/a.ts", evidence: [],
  });
  const upsert = (version: 1 | 2, list: unknown[], org = "org-a", ws = "ws-a") =>
    as("service_role", async (tx) => (await tx.query<Counts>(
      `SELECT * FROM sessions.coordination_advisories_upsert_v${version}($1,$2,$3::jsonb)`, [org, ws, JSON.stringify(list)])).rows[0]!);
  const mark = (id: number, org = "org-a", ws = "ws-a") =>
    as("service_role", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.coordination_advisories_mark_delivered_v1($1,$2,$3,'claude','s-a') AS ok", [org, ws, hex(id)])).rows[0]!.ok);
  const ackV1 = (id: number, org = "org-a", ws = "ws-a") =>
    as("bulls96_read_api", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.coordination_advisories_ack_v1($1,$2,$3,'claude','s-a') AS ok", [org, ws, hex(id)])).rows[0]!.ok, [org, ws]);
  const ackV2 = (id: number, incarnation: string | null, opts: { org?: string; ws?: string; session?: string; scope?: [string, string] } = {}) => {
    const org = opts.org ?? "org-a", ws = opts.ws ?? "ws-a";
    return as("bulls96_read_api", async (tx) => (await tx.query<{ r: string }>(
      "SELECT sessions.coordination_advisories_ack_v2($1,$2,$3,'claude',$4,$5::uuid) AS r", [org, ws, hex(id), opts.session ?? "s-a", incarnation])).rows[0]!.r,
    opts.scope ?? [org, ws]);
  };
  const pending = (version: 1 | 2 | 3, org = "org-a", ws = "ws-a", scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<Record<string, unknown>>(version === 1
      ? "SELECT * FROM sessions.coordination_advisories_pending_for_v1($1,$2,'claude','s-a')"
      : `SELECT * FROM sessions.coordination_advisories_pending_for_v${version}($1,$2,'claude','s-a',10)`, [org, ws])).rows, scope);
  const row = async (id: number, org = "org-a") => (await get().query<Row>(
    "SELECT advisory_id, urgency, status, delivered_at, acked_at, incarnation::text AS incarnation FROM sessions.coordination_advisories WHERE advisory_id=$1 AND org_id=$2 AND harness='claude' AND session_id='s-a'", [hex(id), org])).rows[0];
  const reset = () => get().exec("DELETE FROM sessions.coordination_advisories");
  return { as, adv, upsert, mark, ackV1, ackV2, pending, row, reset };
}

describe("20261105062220 coordination advisories: the advisory instance", () => {
  let db: PGlite;
  const h = harness(() => db);
  const { adv, upsert, mark, ackV1, ackV2, pending, row, reset } = h;

  beforeAll(async () => { db = await migrated(); });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and grants: v2 writer for service_role (and the runtime role), ack_v2 and pending_for_v3 for bulls96_read_api only; v1 grants kept; the read-API verifier passes", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`)).toEqual([
      "function:sessions.coordination_advisories_upsert_v1", "function:sessions.coordination_advisories_mark_delivered_v1",
      "function:sessions.coordination_advisories_ack_v1", "function:sessions.coordination_advisories_upsert_v2",
      "function:sessions.coordination_advisories_ack_v2", "function:sessions.coordination_advisories_pending_for_v3",
    ]);
    expect(SQL).toContain("SET LOCAL lock_timeout='5s';");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(RUNTIME_APPLICATION_FUNCTIONS).toContain(UPSERT_V2);
    for (const fn of [ACK_V2, PENDING_V3]) expect(RUNTIME_APPLICATION_FUNCTIONS).not.toContain(fn);
    const can = async (role: string, fn: string) => (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    const grants: Record<string, string[]> = {
      [UPSERT_V2]: ["service_role"], [ACK_V2]: ["bulls96_read_api"], [PENDING_V3]: ["bulls96_read_api"],
      [FENCED[0][0]]: ["service_role"], [FENCED[1][0]]: ["service_role"], [FENCED[2][0]]: ["bulls96_read_api"],
    };
    for (const [fn, roles] of Object.entries(grants)) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, has_function_privilege('public', p.oid, 'EXECUTE') public_exec FROM pg_proc p WHERE p.oid = $1::regprocedure", [fn])).rows[0]!;
      expect([fn, r]).toEqual([fn, { secdef: true, cfg: ["search_path=pg_catalog, sessions, pg_temp"], public_exec: false }]);
      for (const role of ["service_role", "bulls96_read_api", "anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest", "bulls96_session_ingest_seeder"])
        expect([fn, role, await can(role, fn)]).toEqual([fn, role, roles.includes(role)]);
    }
    expect(await code(h.as("service_role", (tx) => tx.query("SELECT sessions.coordination_advisories_ack_v2('org-a','ws-a',$1,'claude','s-a',gen_random_uuid())", [hex(1)])))).toBe("42501");
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("the column is uuid, nullable, with no default; rows written before the migration are not backfilled (they stay NULL)", async () => {
    const col = (await db.query<{ type: string; notnull: boolean; hasdef: boolean }>(
      "SELECT format_type(atttypid, atttypmod) AS type, attnotnull AS notnull, atthasdef AS hasdef FROM pg_attribute WHERE attrelid = 'sessions.coordination_advisories'::regclass AND attname = 'incarnation' AND NOT attisdropped")).rows;
    expect(col).toEqual([{ type: "uuid", notnull: false, hasdef: false }]);
    expect(SQL).not.toMatch(/\bUPDATE\s+sessions\.coordination_advisories\s+SET\s+incarnation/i);
    expect(SQL).not.toMatch(/ADD COLUMN incarnation uuid[^;]*DEFAULT/i);
    // Before: roll 062220 back, write pending, delivered and acked rows through 062000's v1, then re-apply 062220.
    await reset();
    await db.exec(PREPLAN_DOWN);
    await db.exec(DOWN);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
    expect(await upsert(1, [adv(1, "low"), adv(2, "high"), adv(3, "high")])).toEqual({ inserted: 3, upgraded: 0, skipped: 0 });
    expect(await mark(2)).toBe(true);
    expect(await ackV1(3)).toBe(true);
    await runMigrations(client(db), { profile: LOCAL });
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect((await db.query<{ status: string; incarnation: string | null }>(
      "SELECT status, incarnation::text AS incarnation FROM sessions.coordination_advisories ORDER BY advisory_id")).rows)
      .toEqual([{ status: "pending", incarnation: null }, { status: "delivered", incarnation: null }, { status: "acked", incarnation: null }]);
  });

  test("legacy path: a REAL upsert_v1 after the migration writes NULL, keeps NULL on promotion, and the legacy GET, mark-delivered and v1 ack work as before", async () => {
    await reset();
    expect(await upsert(1, [adv(1, "low"), adv(2, "low")])).toEqual({ inserted: 2, upgraded: 0, skipped: 0 });
    expect(await upsert(1, [adv(1, "high")])).toEqual({ inserted: 0, upgraded: 1, skipped: 0 });
    expect(await row(1)).toMatchObject({ urgency: "high", status: "pending", incarnation: null });
    expect((await pending(1)).map((r) => [r.advisory_id, r.status])).toEqual([[hex(1), "pending"], [hex(2), "pending"]]);
    expect((await pending(3)).map((r) => [r.advisory_id, r.incarnation])).toEqual([[hex(1), null], [hex(2), null]]);
    expect(await mark(1)).toBe(true);
    expect(await row(1)).toMatchObject({ status: "delivered", incarnation: null });
    expect(await ackV1(1)).toBe(true);
    expect(await ackV1(1)).toBe(true);
    expect(await row(1)).toMatchObject({ status: "acked", incarnation: null });
    // A legacy row acked through v1 is no instance: ack_v2 with any UUID is no_match, not already_acked.
    expect(await ackV2(1, crypto.randomUUID())).toBe("no_match");
    // ack_v2 never matches a legacy row, whatever incarnation it is given.
    expect(await ackV2(2, crypto.randomUUID())).toBe("no_match");
    expect(await row(2)).toMatchObject({ status: "pending", incarnation: null });
    expect((await pending(1)).map((r) => r.advisory_id)).toEqual([hex(2)]);
  });

  test("upsert_v2: a new row gets a UUID; a repeat or LOW changes nothing; v1's counts and bounds", async () => {
    await reset();
    expect(await upsert(2, [adv(1, "low"), adv(2, "high"), adv(2, "low")])).toEqual({ inserted: 2, upgraded: 0, skipped: 1 });
    const a = (await row(1))!, b = (await row(2))!;
    expect(a.incarnation).toMatch(UUID);
    expect(b.incarnation).toMatch(UUID);
    expect(a.incarnation).not.toBe(b.incarnation);
    expect(b.urgency).toBe("high");
    expect(await upsert(2, [adv(1, "low"), adv(2, "high"), adv(2, "low")])).toEqual({ inserted: 0, upgraded: 0, skipped: 3 });
    expect(await row(1)).toEqual(a);
    expect(await row(2)).toEqual(b);
    // Another tenant's key is never touched (and the same id there is a separate row of that tenant).
    expect(await upsert(2, [adv(1, "high")], "org-b")).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await row(1)).toEqual(a);
    for (const bad of [null, {}, Array.from({ length: 501 }, (_, i) => adv(i, "low"))])
      expect(await code(h.as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_upsert_v2('org-a','ws-a',$1::jsonb)", [JSON.stringify(bad)])))).toBe("22023");
    expect(await code(upsert(2, [adv(9, "low")], ""))).toBe("22023");
  });

  test("promotion LOW -> HIGH rotates the incarnation and resets to pending (delivered_at NULL), in one statement, also on a legacy row", async () => {
    await reset();
    await upsert(2, [adv(1, "low", "overlap")]);
    const inc1 = (await row(1))!.incarnation!;
    // A delivered LOW (the ack path sets delivered_at; mark_delivered_v1 cannot touch it).
    await db.query("UPDATE sessions.coordination_advisories SET status = 'delivered', delivered_at = now() WHERE advisory_id = $1", [hex(1)]);
    expect(await upsert(2, [adv(1, "high", "overlap")])).toEqual({ inserted: 0, upgraded: 1, skipped: 0 });
    const promoted = (await row(1))!;
    expect(promoted).toMatchObject({ urgency: "high", status: "pending", delivered_at: null, acked_at: null });
    expect(promoted.incarnation).toMatch(UUID);
    expect(promoted.incarnation).not.toBe(inc1);
    // A legacy LOW row (the real v1 writer) promoted by v2 becomes protected.
    await upsert(1, [adv(2, "low")]);
    expect((await row(2))!.incarnation).toBeNull();
    expect(await upsert(2, [adv(2, "high")])).toEqual({ inserted: 0, upgraded: 1, skipped: 0 });
    expect((await row(2))!.incarnation).toMatch(UUID);
    // HIGH again: nothing rotates.
    expect(await upsert(2, [adv(1, "high", "overlap")])).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await row(1)).toEqual(promoted);
  });

  test("acked and expired rows are never resurrected by upsert_v2", async () => {
    await reset();
    await upsert(2, [adv(1, "low"), adv(2, "low")]);
    const inc = (await row(1))!.incarnation!;
    expect(await ackV2(1, inc)).toBe("acked");
    const acked = (await row(1))!;
    await db.query("UPDATE sessions.coordination_advisories SET status = 'expired' WHERE advisory_id = $1", [hex(2)]);
    const expired = (await row(2))!;
    expect(await upsert(2, [adv(1, "high"), adv(2, "high")])).toEqual({ inserted: 0, upgraded: 0, skipped: 2 });
    expect(await row(1)).toEqual(acked);
    expect(await row(2)).toEqual(expired);
  });

  test("ack_v2: acked, then already_acked; no_match for a wrong, stale or NULL incarnation, another recipient or tenant, an expired row; the read scope", async () => {
    await reset();
    await upsert(2, [adv(1, "high"), adv(2, "high"), adv(3, "high", "duplicate_work", "s-z")]);
    const inc = (await row(1))!.incarnation!;
    expect(await ackV2(1, crypto.randomUUID())).toBe("no_match");
    expect(await ackV2(1, null)).toBe("no_match");
    expect(await ackV2(1, inc, { session: "s-z" })).toBe("no_match");
    expect(await ackV2(1, inc, { org: "org-b", scope: ["org-b", "ws-a"] })).toBe("no_match");
    expect(await ackV2(1, inc, { ws: "ws-b", scope: ["org-a", "ws-b"] })).toBe("no_match");
    expect((await row(1))!.status).toBe("pending");
    expect(await ackV2(1, inc)).toBe("acked");
    const acked = (await row(1))!;
    expect(acked).toMatchObject({ status: "acked", incarnation: inc });
    expect(acked.acked_at).not.toBeNull();
    expect(acked.delivered_at).not.toBeNull();
    expect(await ackV2(1, inc)).toBe("already_acked");
    expect(await row(1)).toEqual(acked);
    // Expired matches nothing.
    await db.query("UPDATE sessions.coordination_advisories SET status = 'expired' WHERE advisory_id = $1", [hex(2)]);
    expect(await ackV2(2, (await row(2))!.incarnation)).toBe("no_match");
    expect((await row(2))!.status).toBe("expired");
    // The org must match the read scope.
    expect(await code(ackV2(1, inc, { scope: ["org-b", "ws-a"] }))).toBe("42501");
  });

  test("stale instance: a LOW inc1 ack after the HIGH promotion matches nothing; after a sweep and re-creation under the same id the old instance matches nothing", async () => {
    await reset();
    await upsert(2, [adv(1, "low", "overlap")]);
    const inc1 = (await row(1))!.incarnation!;
    expect((await pending(3)).map((r) => [r.advisory_id, r.urgency, r.incarnation])).toEqual([[hex(1), "low", inc1]]);
    await upsert(2, [adv(1, "high", "overlap")]);
    const inc2 = (await row(1))!.incarnation!;
    expect(await ackV2(1, inc1)).toBe("no_match");
    expect(await row(1)).toMatchObject({ urgency: "high", status: "pending", incarnation: inc2 });
    // The Desktop that holds the LOW instance sees a new instance on the next pull.
    expect((await pending(3)).map((r) => [r.advisory_id, r.urgency, r.incarnation])).toEqual([[hex(1), "high", inc2]]);
    expect(await ackV2(1, inc2)).toBe("acked");
    // The old instance stays no_match after the new one is acked (not already_acked).
    expect(await ackV2(1, inc1)).toBe("no_match");
    expect(await ackV2(1, inc2)).toBe("already_acked");
    // Retention: the acked row ages out and the sweep deletes it; the next scan re-inserts the SAME id.
    await db.query("UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE advisory_id = $1", [hex(1)]);
    expect(await h.as("service_role", async (tx) => (await tx.query("SELECT * FROM sessions.coordination_advisories_sweep_v1()")).rows[0])).toEqual({ expired: 0, deleted: 1 });
    expect(await upsert(2, [adv(1, "high", "overlap")])).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    const inc3 = (await row(1))!.incarnation!;
    expect([inc1, inc2]).not.toContain(inc3);
    expect(await ackV2(1, inc2)).toBe("no_match");
    expect(await ackV2(1, inc1)).toBe("no_match");
    expect((await row(1))!.status).toBe("pending");
    expect(await ackV2(1, inc3)).toBe("acked");
  });

  test("v1 fences: an old v1 writer, v1 ack and v1 mark-delivered skip a protected row and leave it unchanged", async () => {
    await reset();
    await upsert(2, [adv(1, "low"), adv(2, "high")]);
    const low = (await row(1))!, high = (await row(2))!;
    // The old v1 writer would promote LOW -> HIGH: skipped and counted as skipped.
    expect(await upsert(1, [adv(1, "high"), adv(2, "high")])).toEqual({ inserted: 0, upgraded: 0, skipped: 2 });
    expect(await row(1)).toEqual(low);
    expect(await ackV1(1)).toBe(false);
    expect(await ackV1(2)).toBe(false);
    expect(await mark(1)).toBe(false);
    expect(await mark(2)).toBe(false);
    expect(await row(1)).toEqual(low);
    expect(await row(2)).toEqual(high);
    // Acked through ack_v2, the v1 paths still match nothing.
    expect(await ackV2(2, high.incarnation)).toBe("acked");
    const acked = (await row(2))!;
    expect(await ackV1(2)).toBe(false);
    expect(await mark(2)).toBe(false);
    expect(await row(2)).toEqual(acked);
    for (const [sig, name, tag] of FENCED) {
      const src = (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid = $1::regprocedure", [sig])).rows[0]!.s;
      expect(src).toBe(bodyOf(SQL, name, tag));
      // The one added predicate: removing it gives 062000's body back.
      expect(src.replace(" AND t.incarnation IS NULL", "")).toBe(bodyOf(V1_SQL, name, tag));
    }
  });

  test("a stale v1 mark_delivered after a promotion or a re-creation matches nothing", async () => {
    await reset();
    // A legacy LOW (the real v1 writer), promoted by the instance writer.
    await upsert(1, [adv(1, "low")]);
    await upsert(2, [adv(1, "high")]);
    expect(await mark(1)).toBe(false);
    expect(await row(1)).toMatchObject({ status: "pending", delivered_at: null });
    // Re-created under the same id after the sweep.
    await db.query("UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE advisory_id = $1", [hex(1)]);
    await h.as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_sweep_v1()"));
    await upsert(2, [adv(1, "high")]);
    expect(await mark(1)).toBe(false);
    expect(await row(1)).toMatchObject({ status: "pending", delivered_at: null });
  });

  test("pending_for_v3: pending_for_v2's rows, order, filters and scope, plus each row's incarnation (NULL for legacy)", async () => {
    await reset();
    await upsert(1, [adv(1, "low", "overlap")]);
    await upsert(2, [adv(2, "low", "overlap"), adv(3, "high", "intent_conflict"), adv(4, "high")]);
    for (const i of [1, 2, 3, 4])
      await db.query("UPDATE sessions.coordination_advisories SET created_at = now() - make_interval(mins => $1) WHERE advisory_id = $2", [10 - i, hex(i)]);
    await ackV2(4, (await row(4))!.incarnation);
    const v2 = await pending(2), v3 = await pending(3);
    expect(v3.map((r) => r.advisory_id)).toEqual([hex(3), hex(1), hex(2)]);
    expect(v3.map(({ incarnation: _i, ...rest }) => rest)).toEqual(v2);
    expect(v3.map((r) => r.incarnation)).toEqual([(await row(3))!.incarnation, null, (await row(2))!.incarnation]);
    expect(await code(pending(3, "org-b", "ws-a", ["org-a", "ws-a"]))).toBe("42501");
  });
});

describe("20261105062220 rollback", () => {
  test("checksum-pinned; 062190's down refuses under it; restores 062000's exact v1 bodies and ACLs, drops the three functions and the column, keeps the rows; a rerun refuses", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    const db = await migrated();
    const h = harness(() => db);
    const message = async (sql: string) => { const m = await db.exec(sql).then(() => null, (e: Error) => e.message); await db.exec("ROLLBACK").catch(() => {}); return m; };
    try {
      expect((await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).map((s) => s.name)).toEqual([PREPLAN_STEP]);
      expect(await message(DOWN)).toContain("a later migration is applied");
      await db.exec(PREPLAN_DOWN);
      expect(await message(read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql"))).toContain("a later migration is applied");
      await h.upsert(2, [h.adv(1, "high")]);
      await h.upsert(1, [h.adv(2, "high")]);
      const acl = async (sig: string) => (await db.query<{ acl: string | null }>("SELECT proacl::text acl FROM pg_proc WHERE oid = $1::regprocedure", [sig])).rows[0]!.acl;
      const before = await Promise.all(FENCED.map(([sig]) => acl(sig)));
      await db.exec(DOWN);
      for (const [sig, name, tag] of FENCED)
        expect((await db.query<{ s: string; d: boolean }>("SELECT prosrc s, prosecdef d FROM pg_proc WHERE oid = $1::regprocedure", [sig])).rows[0]!)
          .toEqual({ s: bodyOf(V1_SQL, name, tag), d: true });
      expect(await Promise.all(FENCED.map(([sig]) => acl(sig)))).toEqual(before);
      for (const fn of [UPSERT_V2, ACK_V2, PENDING_V3])
        expect((await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_attribute WHERE attrelid = 'sessions.coordination_advisories'::regclass AND attname = 'incarnation' AND NOT attisdropped")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name = $1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      // The rows stay and are legacy again: v1 acks the formerly protected one.
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.coordination_advisories")).rows[0]!.n).toBe(2);
      expect(await h.ackV1(1)).toBe(true);
      expect(await message(DOWN)).toContain("own applied checksum required");
    } finally { await db.close(); }
  });
});
