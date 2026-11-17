/** 062070 coordination advisor readers: service_role-only grants, tenant isolation, recent_v1 windows and rollback. */
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
const FILE = "20261105062070_coordination_advisor_readers.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062070_coordination_advisor_readers.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const FACTS = "(text,text,timestamp with time zone,timestamp with time zone,text,integer)";
const WRITES = `sessions.coordination_advisor_write_facts_v1${FACTS}`;
const OUTPUTS = `sessions.coordination_advisor_output_facts_v1${FACTS}`;
const USAGE = `sessions.coordination_advisor_usage_facts_v1${FACTS}`;
const RECENT = "sessions.coordination_advisories_recent_v1(text,text,timestamp with time zone)";
const ALL_FNS = [WRITES, OUTPUTS, USAGE, RECENT];

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
const hex = (n: number) => n.toString(16).padStart(16, "0");

describe("20261105062070 coordination advisor readers", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const facts = (fn: string, org: string, ws: string, from = "2026-11-01", to = "2026-12-01", limit = 100) =>
    as("service_role", async (tx) => (await tx.query<{ session_id: string; row_id: string }>(
      `SELECT session_id, row_id FROM ${fn.slice(0, fn.indexOf("("))}($1,$2,$3::timestamptz,$4::timestamptz,NULL,$5)`, [org, ws, from, to, limit])).rows);
  type Recent = { row_kind: string; harness: string; session_id: string; peer_harness: string | null; peer_session_id: string | null; latest_at: Date; sent_last_hour: number | null };
  const recent = (org: string, ws: string, since: string) =>
    as("service_role", async (tx) => (await tx.query<Recent>(
      "SELECT * FROM sessions.coordination_advisories_recent_v1($1,$2,$3::timestamptz) ORDER BY row_kind, harness, session_id, peer_session_id", [org, ws, since])).rows);
  const upsert = (org: string, ws: string, list: unknown[]) =>
    as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_upsert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)]));
  const adv = (id: number, session: string, peer: string) => ({
    advisory_id: hex(id), harness: "claude", session_id: session, peer_harness: "codex", peer_session_id: peer,
    urgency: "high", kind: "concurrent_edit", artifact: "src/a.ts", evidence: [],
  });

  beforeAll(async () => {
    db = await migrated();
    // Facts for two tenants: org-a/ws-a (one write, one plain event, one commit, one message), org-b/ws-a (one write).
    await db.exec(`INSERT INTO sessions.session_event_facts
      (org_id,workspace_id,ts,session_id,source,machine_id,kind,event_id,input_tokens,output_tokens,reasoning_tokens,
       cache_read_tokens,cache_create_tokens,files_touched,row_id,fingerprint,source_version) VALUES
      ('org-a','ws-a','2026-11-17T10:00:00Z','sa-write','codex','m','tool','e1',0,0,0,0,0,ARRAY['src/a.ts'],repeat('a',64),'f',1),
      ('org-a','ws-a','2026-11-17T10:01:00Z','sa-plain','codex','m','tool','e2',0,0,0,0,0,ARRAY[]::text[],repeat('b',64),'f',1),
      ('org-b','ws-a','2026-11-17T10:00:00Z','sb-write','codex','m','tool','e3',0,0,0,0,0,ARRAY['src/a.ts'],repeat('c',64),'f',1);
      INSERT INTO sessions.session_output_facts
      (org_id,workspace_id,fact_kind,ts,scope_source,source,session_id,fact_key,origin,row_key,row_id,fingerprint,source_version) VALUES
      ('org-a','ws-a','commit','2026-11-17T10:00:00Z','codex','codex','sa-commit','k','o','r',repeat('d',64),'f',1),
      ('org-a','ws-a','message','2026-11-17T10:00:00Z','codex','codex','sa-msg','k','o','r',repeat('e',64),'f',1),
      ('org-b','ws-a','commit','2026-11-17T10:00:00Z','codex','codex','sb-commit','k','o','r',repeat('f',64),'f',1);`);
  });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and runtime allowlist name exactly the four functions; no table", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.coordination_advisor_output_facts_v1", "function:sessions.coordination_advisor_usage_facts_v1",
      "function:sessions.coordination_advisor_write_facts_v1", "function:sessions.coordination_advisories_recent_v1",
    ]);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    for (const fn of ALL_FNS) expect(RUNTIME_APPLICATION_FUNCTIONS).toContain(fn);
  });

  test("1. grants: SECURITY DEFINER, pinned search_path, no PUBLIC; service_role only", async () => {
    for (const fn of ALL_FNS) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r.secdef).toBe(true);
      expect(r.cfg).toEqual(["search_path=pg_catalog, sessions, pg_temp"]);
      expect(r.public_exec).toBe(false);
    }
    await db.exec("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='coord_rdr_probe') THEN CREATE ROLE coord_rdr_probe NOLOGIN; END IF; END $$");
    const can = async (role: string, fn: string) =>
      (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of ALL_FNS) {
      for (const r of ["coord_rdr_probe", "anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest", "bulls96_read_api"])
        expect(await can(r, fn)).toBe(false);
      expect(await can("service_role", fn)).toBe(true);
    }
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_recent_v1('org-a','ws-a',now())"), ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.coordination_advisor_write_facts_v1('org-a','ws-a',now()-interval '1 day',now(),NULL,10)"), ["org-a", "ws-a"]))).toBe("42501");
    // The worker still cannot read the facts tables directly.
    expect(await code(as("service_role", (tx) => tx.query("SELECT 1 FROM sessions.coordination_advisories")))).toBe("42501");
  });

  test("2. tenant isolation and filters: each reader returns only its own tenant's matching rows", async () => {
    expect((await facts(WRITES, "org-a", "ws-a")).map((r) => r.session_id)).toEqual(["sa-write"]);
    expect((await facts(WRITES, "org-b", "ws-a")).map((r) => r.session_id)).toEqual(["sb-write"]);
    expect((await facts(USAGE, "org-a", "ws-a")).map((r) => r.session_id)).toEqual(["sa-write", "sa-plain"]);
    expect((await facts(OUTPUTS, "org-a", "ws-a")).map((r) => r.session_id)).toEqual(["sa-commit"]);
    expect((await facts(OUTPUTS, "org-b", "ws-a")).map((r) => r.session_id)).toEqual(["sb-commit"]);
    expect(await facts(USAGE, "org-a", "ws-b")).toEqual([]);
    expect(await facts(USAGE, "org-c", "ws-a")).toEqual([]);
    // Window and page bounds match the read-API originals.
    expect(await facts(USAGE, "org-a", "ws-a", "2026-11-17T10:00:30Z", "2026-11-17T10:01:00Z")).toEqual([]);
    expect((await facts(USAGE, "org-a", "ws-a", "2026-11-01", "2026-12-01", 1)).length).toBe(1);
    for (const fn of [WRITES, OUTPUTS, USAGE]) {
      expect(await code(facts(fn, "", "ws-a"))).toBe("22023");
      expect(await code(facts(fn, "org-a", "x".repeat(513)))).toBe("22023");
      expect(await code(facts(fn, "org-a", "ws-a", "2026-12-01", "2026-11-01"))).toBe("22023");
      expect(await code(facts(fn, "org-a", "ws-a", "2026-11-01", "2026-12-01", 10001))).toBe("22023");
    }
  });

  test("3. recent_v1: latest per pair since the window, per-recipient last-hour counts, tenant-isolated", async () => {
    await upsert("org-a", "ws-a", [adv(1, "s1", "s2"), adv(2, "s1", "s2"), adv(3, "s1", "s3")]);
    await upsert("org-b", "ws-a", [adv(4, "s1", "s2")]);
    // Age one org-a row past the hour but inside the cooldown window, and one past the 7-day retention.
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '3 hours' WHERE org_id='org-a' AND advisory_id='${hex(2)}'`);
    await db.exec(`UPDATE sessions.coordination_advisories SET created_at = now() - interval '8 days' WHERE org_id='org-a' AND advisory_id='${hex(3)}'`);
    const rows = await recent("org-a", "ws-a", new Date(Date.now() - 30 * 86_400_000).toISOString());
    // s1->s3 is older than the 7-day clamp, so even a 30-day window does not return it.
    expect(rows.map((r) => [r.row_kind, r.session_id, r.peer_session_id, r.sent_last_hour])).toEqual([
      ["pair", "s1", "s2", null],
      ["recipient", "s1", null, 1],
    ]);
    const latest = (await db.query<{ t: Date }>(`SELECT created_at t FROM sessions.coordination_advisories WHERE org_id='org-a' AND advisory_id='${hex(1)}'`)).rows[0]!.t;
    expect(new Date(rows[0]!.latest_at).getTime()).toBe(new Date(latest).getTime());
    // A window that starts after every row returns no pair rows; the hour count is independent of it.
    expect((await recent("org-a", "ws-a", new Date(Date.now() + 60_000).toISOString())).map((r) => r.row_kind)).toEqual(["recipient"]);
    // The other tenant's identical pair is its own.
    expect((await recent("org-b", "ws-a", "2026-01-01")).map((r) => [r.row_kind, r.sent_last_hour])).toEqual([["pair", null], ["recipient", 1]]);
    expect(await recent("org-c", "ws-a", "2026-01-01")).toEqual([]);
    expect(await code(recent("", "ws-a", "2026-01-01"))).toBe("22023");
    expect(await code(as("service_role", (tx) => tx.query("SELECT * FROM sessions.coordination_advisories_recent_v1('org-a','ws-a',NULL)")))).toBe("22023");
  });
});

describe("20261105062070 rollback", () => {
  test("4. the checksum-pinned down drops the four readers and the ledger row, keeps 062000, and refuses on a rerun", async () => {
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
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND (proname LIKE 'coordination\\_advisor\\_%' OR proname='coordination_advisories_recent_v1')")).rows[0]!.n).toBe(0);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.coordination_advisories')::text r")).rows[0]!.r).toBe("sessions.coordination_advisories");
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
    } finally { await db.close(); }
  });
});
