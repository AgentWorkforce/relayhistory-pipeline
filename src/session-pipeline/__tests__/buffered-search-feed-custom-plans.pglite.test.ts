/**
 * 20261105062330: plan_cache_mode=force_custom_plan on sessions.buffered_search_feed_stage(jsonb) and
 * sessions.ingest_buffer_gc_ordinals(uuid), through the real migrator, and its checksum-pinned rollback.
 *
 * Behaviour: auto_explain (nested statements, NOTICE level) records the plan PL/pgSQL actually executes for each
 * statement inside the stage RPC and gc_ordinals. The session is forced to plan_cache_mode=force_generic_plan (what a
 * long-lived feeder session reaches by itself after five calls under auto). With the function-level SET, every
 * execution after the fifth is still a custom plan (the call's values are literals in the plan, and the same-second
 * sibling count is a one-second ingest_stage_intents_cleanup_idx range); with it RESET (the rollback), the same
 * statements run generic plans ($n parameters). Synthetic, secret-free data only.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE as FEED_ROLE } from "../buffered-feed-role.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const FILE = "20261105062330_buffered_search_feed_custom_plans.sql";
const STEP = `upstream:${FILE}`;
const UP = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062330_buffered_search_feed_custom_plans.down.sql");
// 20261105062510 (the brief membership) is newest when present: its down runs first.
const BRIEF_MEMBERSHIP_DOWN = "supabase/rollbacks/20261105062510_session_brief_membership.down.sql";
// 20261105062500 (the brief summary v2 read) sits above 062350 when present: its down runs next.
const BRIEF_SUMMARY_V2_DOWN = "supabase/rollbacks/20261105062500_session_brief_summary_v2_read.down.sql";
// 20261105062350 (the page keyset) sits above 062330 when present: its down runs next.
const PAGE_KEYSET_DOWN = "supabase/rollbacks/20261105062350_buffered_search_feed_page_keyset.down.sql";
// 20261105062340 (the per-org erasure history) also sits above 062330 when present: its down runs next.
const HISTORY_DOWN = "supabase/rollbacks/20261105062340_ingest_org_erasure_history.down.sql";
const laterOff = async (db: PGlite) => {
  if ((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062510_session_brief_membership.sql'")).rows.length)
    await db.exec(read(BRIEF_MEMBERSHIP_DOWN));
  if ((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062500_session_brief_summary_v2_read.sql'")).rows.length)
    await db.exec(read(BRIEF_SUMMARY_V2_DOWN));
  if ((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062350_buffered_search_feed_page_keyset.sql'")).rows.length)
    await db.exec(read(PAGE_KEYSET_DOWN));
  if ((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062340_ingest_org_erasure_history.sql'")).rows.length)
    await db.exec(read(HISTORY_DOWN));
};
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const STAGE_RPC = "sessions.buffered_search_feed_stage(jsonb)";
const GC_ORDINALS = "sessions.ingest_buffer_gc_ordinals(uuid)";
const CUSTOM = "plan_cache_mode=force_custom_plan";
const STAGE_CONFIG = ["search_path=pg_catalog, sessions, pg_temp", "lock_timeout=2s"];
const GC_CONFIG = ["search_path=pg_catalog, pg_temp"];
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

const open: PGlite[] = [];
afterEach(async () => { for (const db of open.splice(0)) await db.close(); });
function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
async function migrated() {
  const db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
  open.push(db);
  await runMigrations(client(db), { profile: LOCAL });
  return db;
}
const config = async (db: PGlite, sig: string) =>
  (await db.query<{ c: string[] | null }>("SELECT proconfig c FROM pg_proc WHERE oid=$1::regprocedure", [sig])).rows[0]!.c;
const applied = async (db: PGlite) => (await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied;
const failure = (p: Promise<unknown>) => p.then(() => null, async (e: { code?: string; message?: string }) => ({ code: e.code, message: String(e.message) }));

describe("20261105062330 buffered search feed custom plans", () => {
  test("after every migration both functions carry force_custom_plan on top of their prior proconfig; nothing else changes", async () => {
    const db = await migrated();
    expect(await config(db, STAGE_RPC)).toEqual([...STAGE_CONFIG, CUSTOM]);
    expect(await config(db, GC_ORDINALS)).toEqual([...GC_CONFIG, CUSTOM]);
    expect(await applied(db)).toBe(true);
    expect((await db.query<{ sha256: string }>("SELECT sha256 FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.sha256).toBe(sha(UP));
    // ALTER ... SET only: no body, ACL or SECURITY DEFINER change (both still as their defining migrations left them).
    const meta = (await db.query<{ sig: string; secdef: boolean; volatile: string; acl: string | null }>(
      "SELECT oid::regprocedure::text sig, prosecdef secdef, provolatile volatile, proacl::text acl FROM pg_proc WHERE oid IN ($1::regprocedure,$2::regprocedure) ORDER BY 1",
      [STAGE_RPC, GC_ORDINALS])).rows;
    expect(meta.map((m) => [m.sig, m.secdef, m.volatile])).toEqual([[STAGE_RPC, true, "v"], [GC_ORDINALS, false, "v"]]);
    expect(UP.split("\n").filter((l) => !l.startsWith("--")).join("\n")).not.toMatch(/\b(?:GRANT|REVOKE|CREATE|DROP)\s/u);
  });

  test("the rollback RESETs exactly plan_cache_mode, drops its ledger row, refuses on a rerun and the migrator re-applies it", async () => {
    const db = await migrated();
    const shape = async () => (await db.query("SELECT oid::regprocedure::text sig, prosrc, proacl::text acl, proowner, prosecdef, provolatile FROM pg_proc WHERE oid IN ($1::regprocedure,$2::regprocedure) ORDER BY 1", [STAGE_RPC, GC_ORDINALS])).rows;
    await laterOff(db);
    const before = await shape();
    await db.exec(DOWN);
    expect(await shape()).toEqual(before);
    expect(await config(db, STAGE_RPC)).toEqual(STAGE_CONFIG);
    expect(await config(db, GC_ORDINALS)).toEqual(GC_CONFIG);
    expect(await applied(db)).toBe(false);
    expect((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows).toEqual([]);
    expect(await failure(db.exec(DOWN))).toMatchObject({ code: "55000", message: expect.stringContaining("own applied checksum required") });
    await db.exec("ROLLBACK").catch(() => {});
    await runMigrations(client(db), { profile: LOCAL });
    expect(await config(db, STAGE_RPC)).toEqual([...STAGE_CONFIG, CUSTOM]);
    expect(await config(db, GC_ORDINALS)).toEqual([...GC_CONFIG, CUSTOM]);
    expect(await applied(db)).toBe(true);
  });

  test("the rollback refuses while a later migration is applied, and with a different applied checksum", async () => {
    const db = await migrated();
    await laterOff(db);
    const later = "upstream:20261105099000_later_probe.sql";
    await db.query("INSERT INTO sessions.__96bulls_migrations SELECT (jsonb_populate_record(NULL::sessions.__96bulls_migrations, to_jsonb(m) || jsonb_build_object('name', $2::text))).* FROM sessions.__96bulls_migrations m WHERE m.name = $1", [STEP, later]);
    expect(await failure(db.exec(DOWN))).toMatchObject({ code: "55000", message: expect.stringContaining("a later migration is applied") });
    await db.exec("ROLLBACK").catch(() => {});
    await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [later]);
    await db.query("UPDATE sessions.__96bulls_migrations SET sha256=repeat('0',64) WHERE name=$1", [STEP]);
    expect(await failure(db.exec(DOWN))).toMatchObject({ code: "55000", message: expect.stringContaining("own applied checksum required") });
    await db.exec("ROLLBACK").catch(() => {});
    expect(await config(db, STAGE_RPC)).toEqual([...STAGE_CONFIG, CUSTOM]);
  });

  test("the up refuses (55000) without 045010's stage RPC", async () => {
    const db = await migrated();
    await laterOff(db);
    await db.exec("ALTER FUNCTION sessions.buffered_search_feed_stage(jsonb) RENAME TO buffered_search_feed_stage_moved");
    expect(await failure(db.exec("BEGIN;" + UP + "COMMIT;"))).toMatchObject({ code: "55000", message: expect.stringContaining("requires 20261105045010") });
    await db.exec("ROLLBACK").catch(() => {});
  });
});

describe("20261105062330 behaviour: the executed plans stay custom past the fifth call", () => {
  const ORG = "org-cp", WS = "ws-cp", ORIGIN = "origin-cp", USER = "user-cp";
  /** One acknowledged, live, active stage of (ORG, WS, ORIGIN, USER), plus `history` older intents of the same origin
   *  across other users and seconds (what the generic plan walks). Inserted with triggers and FKs off (replica). */
  async function seed(db: PGlite, history: number) {
    await db.exec("SET session_replication_role = replica");
    const attempt = async (batch: string, user: string) => {
      const claim = randomUUID();
      await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state,buffer_receipt_kind) VALUES($1,$2,$3,$4,$5,$6,'rhr_'||repeat('a',64),'committed',clock_timestamp(),'buffered','staged')", [ORG, WS, ORIGIN, batch, user, sha(batch)]);
      await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$7,'[]','[]',0,repeat('a',64))", [claim, ORG, WS, user, ORIGIN, batch, sha(batch)]);
      return claim;
    };
    const intent = async (batch: string, user: string, claim: string, createdAt: string, state: "preparing" | "acknowledged") => {
      const token = randomUUID();
      await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,10,'[[0]]',0,$11,$12::timestamptz+interval '5 minutes',$12)",
        [token, claim, ORG, WS, user, ORIGIN, batch, sha(batch), "k/" + token, sha(token), state, createdAt]);
      return token;
    };
    await db.exec("BEGIN");
    for (let i = 0; i < history; i++) {
      const batch = "h-" + i, user = i % 2 ? USER : "user-other";
      await intent(batch, user, await attempt(batch, user), new Date(Date.UTC(2026, 9, 1) + i * 37_000).toISOString(), "acknowledged");
    }
    const claim = await attempt("b-live", USER);
    const token = await intent("b-live", USER, claim, "2026-11-06T00:00:00.250Z", "acknowledged");
    await intent("b-live", USER, claim, "2026-11-06T00:00:00.750Z", "acknowledged"); // a same-second sibling
    await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,$2,$3,'b-live',$4,$5,$6)", [ORG, WS, ORIGIN, USER, sha("b-live"), token]);
    await db.exec("COMMIT");
    await db.exec("SET session_replication_role = origin");
    await db.exec("ANALYZE sessions.ingest_stage_intents; ANALYZE sessions.ingest_buffer_outbox; ANALYZE sessions.ingest_batches; ANALYZE sessions.ingest_claim_attempts");
    return token;
  }
  /** Call the stage RPC `n` times as the genuine feed login with the session forced to generic plans; return, per call,
   *  the nested plans auto_explain reported. */
  async function calls(db: PGlite, token: string, n: number) {
    const out: string[][] = [];
    await db.exec(`LOAD 'auto_explain'; SET auto_explain.log_min_duration = 0; SET auto_explain.log_nested_statements = on;
      SET auto_explain.log_level = notice; SET auto_explain.log_format = text`);
    await db.exec("SET plan_cache_mode = force_generic_plan");
    await db.exec("SET SESSION AUTHORIZATION " + FEED_ROLE);
    try {
      for (let i = 0; i < n; i++) {
        const plans: string[] = [];
        const r = await db.query<{ r: { found: boolean; sameSecond?: { siblings: number } } }>("SELECT sessions.buffered_search_feed_stage($1::jsonb) r",
          [JSON.stringify({ stageToken: token })], { onNotice: (m) => { if (String(m.message).includes("Query Text:")) plans.push(String(m.message)); } });
        expect(r.rows[0]!.r).toMatchObject({ found: true, sameSecond: { siblings: 1 } });
        out.push(plans);
      }
    } finally {
      await db.exec("SET SESSION AUTHORIZATION postgres; RESET plan_cache_mode; RESET auto_explain.log_min_duration");
    }
    return out;
  }
  const sibling = (plans: string[]) => plans.find((p) => p.includes("FROM sessions.ingest_stage_intents x"))!;
  const stageLookup = (plans: string[]) => plans.find((p) => p.includes("WHERE stage_token=p_stage"))!;
  const generic = (plan: string) => /\$\d/u.test(plan.slice(plan.indexOf("\n", plan.indexOf("Query Text:"))));

  test("with the function SET every execution is custom (literal values; the sibling count is a cleanup_idx range); RESET (the rollback) runs generic plans", async () => {
    const db = await migrated();
    const token = await seed(db, 400);
    const withSet = await calls(db, token, 8);
    for (const [i, plans] of withSet.entries()) {
      const s = sibling(plans), g = stageLookup(plans);
      expect([i, s === undefined, g === undefined]).toEqual([i, false, false]);
      expect([i, generic(s), generic(g)]).toEqual([i, false, false]);
      expect(s).toContain("ingest_stage_intents_cleanup_idx");
    }
    await laterOff(db);
    await db.exec(DOWN);
    const reset = await calls(db, token, 8);
    for (const [i, plans] of reset.entries()) {
      const s = sibling(plans), g = stageLookup(plans);
      expect([i, generic(s), generic(g)]).toEqual([i, true, true]);
    }
  });
});
