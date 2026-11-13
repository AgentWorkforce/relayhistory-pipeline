/**
 * The session-ingest Worker on a G4-only database: supabase/migrations up to and including
 * 20261105043800_lazy_ack_clock_no_key_update.sql (plus bootstrap), WITHOUT 043850 (#332
 * acquisition owner), 043860 (#338 buffered search gate), 043870 (#339 buffered search feed
 * cursor), 043880 (#327 bookkeeping cleanup), 043890 (#384), 043900 (#336 consumer function grants,
 * EXECUTE grants only), 044030 (#386 generation overlap: owner-only window, retired record and policy
 * trigger; replaces ingest_require_generation / ingest_lazy_guard), 044040 (records GIN and the session-first
 * owner read RPCs) and any later migration (pglite-schema.ts
 * derives POST_G4_MIGRATIONS from the sorted list). The app runs with the Worker's default flags
 * (canonical acceptance, async admission / babysitter routing / replica off, relayhistory
 * custody), and the buffered acceptance mode it is cut over to; every main sync ingest path
 * (acceptBatch inline + staged fan-out, slots, selection / reset, ordered controls, the
 * claim / stage / commit RPC set, #332's acquisition refusal mapping) runs against it while an
 * undefined-object guard (SQLSTATE 42883 / 42P01 / 42703 / 42704) watches every statement.
 * Async admission is proven elsewhere on G4; here one smoke case only. Frozen-supabase custody with
 * the real deploy settings (stack vars -> ingestSettings -> ingestApp, drain, the sync frozen receipt) is
 * g4-deploy-settings.pglite.test.ts; #387's frozen-fallback matrix also runs on G4 in
 * sync-admission-frozen-receipt.pglite.test.ts.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { sqlLedger } from "../ledger-sql.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { admitMessage } from "../admission-consumer.ts";
import { ADMISSION_ACCEPT_VALUE, type AdmissionMessage } from "../async-admission.ts";
import { fanout, type FanoutMessage } from "../staging.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "./ordered-support.ts";
import { G4_LAST_MIGRATION, POST_G4_MIGRATIONS, REPO_ROOT, applySupabaseSchema, guardUndefinedObjects, migrationFiles, type UndefinedObjectError } from "./pglite-schema.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

setDefaultTimeout(120000);
const gen = "a".repeat(64), epoch = "3".repeat(64);
const open: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
const read = (p: string) => readFileSync(join(REPO_ROOT, p), "utf8");

/** Every object the post-G4 migrations define, split into new (absent on G4) and replaced (prior form on G4). */
function postG4Objects() {
  const fn = /CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+sessions\.(\w+)/gi, table = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?sessions\.(\w+)/gi;
  const names = (sql: string, re: RegExp) => new Set([...sql.matchAll(re)].map((m) => m[1]!.toLowerCase()));
  const g4 = migrationFiles(REPO_ROOT, G4_LAST_MIGRATION).map((f) => read(`supabase/migrations/${f}`)).join("\n");
  const post = POST_G4_MIGRATIONS.map((f) => read(`supabase/migrations/${f}`)).join("\n");
  const g4Fns = names(g4, fn), g4Tables = names(g4, table);
  const postFns = [...names(post, fn)], postTables = [...names(post, table)];
  return {
    newFunctions: postFns.filter((n) => !g4Fns.has(n)).sort(),
    replacedFunctions: postFns.filter((n) => g4Fns.has(n)).sort(),
    newTables: postTables.filter((n) => !g4Tables.has(n)).sort(),
  };
}
const OBJECTS = postG4Objects();

/** A G4-only PGlite with the guard installed BEFORE anything runs, and the generation the Worker expects. */
async function g4Database() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(() => db.close());
  const undefinedObjects = guardUndefinedObjects(db);
  const applied = await applySupabaseSchema(db, { until: G4_LAST_MIGRATION });
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + gen + "','pass')");
  /** The RPCs the Worker's ledger called, and every jsonb payload it sent. */
  const rpcs = new Set<string>(), payloads: unknown[] = [];
  const run = async (s: string, p: unknown[] = []) => {
    for (const m of s.matchAll(/sessions\.(\w+)\(/g)) rpcs.add(m[1]!);
    if (typeof p[0] === "string") { try { payloads.push(JSON.parse(p[0])); } catch { /* not a payload */ } }
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as never; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  return { db, undefinedObjects, applied, rpcs, payloads, run };
}

/** No `acquisition` key at any depth of any payload the Worker sent (#332's claim hook stays unused). */
function hasAcquisitionKey(v: unknown): boolean {
  if (Array.isArray(v)) return v.some(hasAcquisitionKey);
  if (v && typeof v === "object") return Object.entries(v).some(([k, x]) => k === "acquisition" || k === "acquisitionEpoch" || hasAcquisitionKey(x));
  return false;
}
const expectClean = (errors: UndefinedObjectError[]) => expect(errors).toEqual([]);

describe("G4-only schema: the post-G4 objects are absent", () => {
  test("exactly the 55 G4 migrations are applied; HOOPDB_MIGRATE_UNTIL refuses an unknown name", async () => {
    const all = migrationFiles(REPO_ROOT, null), g4 = migrationFiles(REPO_ROOT, G4_LAST_MIGRATION);
    expect(g4).toHaveLength(55);
    expect(g4.at(-1)).toBe(G4_LAST_MIGRATION);
    for (const f of POST_G4_MIGRATIONS) { expect(all).toContain(f); expect(g4).not.toContain(f); }
    // POST_G4_MIGRATIONS is derived; it must equal exactly the files after the cutoff, read fresh here.
    const files = readdirSync(join(REPO_ROOT, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort();
    expect([...POST_G4_MIGRATIONS]).toEqual(files.slice(files.indexOf(G4_LAST_MIGRATION) + 1));
    expect([...g4, ...POST_G4_MIGRATIONS]).toEqual(files);
    // Drift alarm: a migration landing on main after G4 fails here, so its G4 impact is reviewed (the
    // absence checks and the function-body scan below already include it through the derived list).
    expect([...POST_G4_MIGRATIONS]).toEqual([
      "20261105043850_ingest_acquisition_owner.sql",
      "20261105043860_buffered_search_gate.sql",
      "20261105043870_buffered_search_feed_cursor.sql",
      "20261105043880_ingest_bookkeeping_cleanup.sql",
      "20261105043890_buffered_search_gate_retired_witness.sql",
      "20261105043900_session_consumer_function_grants.sql",
      "20261105043910_selfhost_session_selection_erase.sql",
      // A new table and read API functions only; no ingest path touches them.
      "20261105044000_read_api_tokens.sql",
      "20261105044010_selfhost_erase_lock_fences.sql",
      // Eight org_id btree indexes on existing ingest tables only; no function or column.
      "20261105044020_claim_bootstrap_org_indexes.sql",
      // #386 generation overlap: owner-only window, retired record and policy trigger; replaces
      // ingest_require_generation / ingest_lazy_guard (in OBJECTS.replacedFunctions below).
      "20261105044030_ingest_generation_overlap.sql",
      // A jsonb_path_ops GIN on ingest_claim_attempts(records) and CREATE OR REPLACE of the two owner
      // read RPCs (buffered_sessions_for / buffered_session_stages_for, in OBJECTS.replacedFunctions
      // below); the ingest Worker never calls either.
      "20261105044040_buffered_stages_session_first.sql",
      // Replaces the self-host erase and the G4 retire trigger function ingest_publication_retire_fence (in
      // OBJECTS.replacedFunctions below); both now clear the short_summary provenance columns with the summary.
      "20261105044050_selfhost_erase_short_summary_provenance.sql",
      // Replaces the same two functions again (already in OBJECTS.replacedFunctions); both now rebuild search_text
      // without the summary when they clear it.
      "20261105044070_selfhost_erase_search_text_rebuild.sql",
      // Owner-only claim-session side table, its policy row and an inert writer trigger on ingest_claim_attempts
      // (no ingest function body changes; the trigger does nothing until the policy enables it).
      "20261105044080_ingest_claim_sessions.sql",
      // Replaces the two buffered read RPCs again (already in OBJECTS.replacedFunctions; gate off they run 044040's
      // query) and 044080's post-G4 policy guard; adds a post-G4 column and an owner-only refresh trigger function.
      "20261105044090_claim_sessions_reader.sql",
      // Read-role-only summary RPC, recorded-title metadata column and invoker trigger;
      // no ingest RPC is replaced or calls them.
      "20261105044100_session_brief_summary_read.sql",
      // The digest narrative cache table, its purge function and read API functions only; no ingest path touches them.
      // (045000 landed on main; listed here in slot order, newest branch slot 046000 last.)
      "20261105045000_digest_narrative_cache.sql",
      // Replaces the feed's stage RPC (043870, post-G4) to add the sameSecond sibling counts; no ingest RPC changes.
      "20261105045010_buffered_search_feed_same_second.sql",
      // One EXECUTE grant to the self-host consumer on the 20261101050000 project-text index helper; no new object, no ingest change.
      "20261105045030_session_consumer_project_text_grant.sql",
      // Self-host output facts tables and their writer/read functions only; no ingest path touches them.
      "20261105045040_selfhost_output_facts.sql",
      // Lane F1: analysis jobs/requests/tickets/invocations/reservations tables,
      // the purge functions and their grant helper, the admit scope function and the request entry point.
      "20261105046000_analysis_jobs.sql",
      // Two read-role-only SECURITY DEFINER list/activity query functions and three owner-only helpers; no ingest path touches them.
      "20261105046010_session_directory_read_api_query.sql",
      // Two read-role-only SECURITY DEFINER coordination fact readers and two partial indexes; no ingest path touches them.
      "20261105047000_coordination_fact_readers.sql",
      // Read-role-only exact-session buffered stage RPC; default-off and never called by the ingest Worker.
      "20261105047010_buffered_exact_session_stages.sql",
      // Read-role-only paged exact-session buffered stage RPC (requires 047010); default-off and never called by the ingest Worker.
      "20261105047020_buffered_session_stages_paged.sql",
      // Owner-only frozen-copy progress table (no grant, no function) and the 041000 privacy guard trigger on frozen receipts; no G4 function names them.
      "20261105047030_frozen_copy_progress.sql",
      // Operator per-session erase: new tables and entry points; no ingest path calls them.
      "20261105050000_operator_session_erase.sql",
      // Its fence check also replaces publication scope / buffer GC for lock order and guards live writer tables.
      "20261105050010_operator_session_erase_fence_check.sql",
      "20261105050020_operator_session_erase_runtime.sql",
      // One read-role-only SECURITY DEFINER summary activity counter; no ingest path touches it.
      "20261105056000_session_summary_activity.sql",
      // One read-role-only SECURITY DEFINER per-user session aggregate; no ingest path touches it.
      "20261105057000_session_directory_users_aggregate.sql",
      // Server-owned upload consent: four new owner-only tables and two ingest-role entry points; no G4 function names them.
      "20261105057010_upload_consent.sql",
      // Replacement of the post-G4 self-host erase (043910, last 044070): it also deletes the erased copy's directory membership rows.
      "20261105057030_selfhost_erase_directory_membership.sql",
      // Sixteen support indexes for the per-user erase (foreign keys, un-owned slots, key_material, ord-seat candidate key); no function changes.
      "20261105058000_operator_user_erase_indexes.sql",
      // Operator per-user erase: an owner-only ledger table and entry points; it also replaces ingest_lock_fences
      // (RH041 while the user has an active erase), the buffered GC claim (skips such a user's stages) and the two
      // source-control begins (refused for such a user).
      "20261105058010_operator_user_erase.sql",
      // Publication provider: prepare and publish admit storage provider 'gcs' as well as 's3' (replacement only).
      "20261105059000_ingest_publication_gcs_provider.sql",
    ]);
    expect(() => migrationFiles(REPO_ROOT, "20261105043800_typo.sql")).toThrow(/names no migration/);
    expect(OBJECTS.newFunctions.length).toBeGreaterThan(30);
    expect(OBJECTS.replacedFunctions).toEqual(["buffered_session_stages_for", "buffered_sessions_for", "ingest_babysitter_retention_register", "ingest_buffer_gc_claim", "ingest_claim_batch_mode", "ingest_claim_buffered_batch", "ingest_commit_batch", "ingest_commit_buffered_batch", "ingest_lazy_guard", "ingest_lock_fences", "ingest_prepare_buffer_stage", "ingest_privacy_authority_begin", "ingest_publication_claim", "ingest_publication_guard_pointer", "ingest_publication_prepare", "ingest_publication_publish", "ingest_publication_retire_fence", "ingest_publication_scope", "ingest_require_generation", "ingest_source_control_begin"]);
  });

  test("no new table, function or column exists; the replaced RPCs exist in their prior form; no G4 body names a post-G4 object", async () => {
    const f = await g4Database();
    expect(f.applied).toEqual(migrationFiles(REPO_ROOT, G4_LAST_MIGRATION));
    const fnRows = (await f.db.query<{ proname: string }>("SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname=ANY($1)", [OBJECTS.newFunctions])).rows;
    expect(fnRows).toEqual([]);
    for (const t of OBJECTS.newTables) expect((await f.db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", ["sessions." + t])).rows[0]!.r).toBeNull();
    const cols = (await f.db.query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='sessions' AND ((table_name='ingest_claim_attempts' AND column_name='acquisition') OR (table_name='ingest_publication_heads' AND column_name='acquisition_epoch'))")).rows;
    expect(cols).toEqual([]);
    for (const name of OBJECTS.replacedFunctions)
      expect((await f.db.query<{ n: number }>("SELECT count(*)::int AS n FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname=$1", [name])).rows[0]!.n).toBeGreaterThan(0);
    // A plpgsql EXCEPTION block could swallow a 42883 the guard never sees: no G4 function body may
    // even name a post-G4 function or table (or the acquisition_epoch column).
    // Exception: 041000's authority apply names the analysis (046000) objects only
    // through its guarded integrations (to_regprocedure/to_regclass presence checks, ABI-mismatch
    // on partial presence, no-op when absent): not a swallowed 42883. Everything else must stay clean.
    // (041000's derived purge names the digest (045000) objects only through its guarded integration;
    // 045000 has landed, so both entry sets are live.)
    const guarded = new Set([
      "ingest_privacy_authority_apply -> session_analysis_jobs_v2",
      "ingest_privacy_authority_apply -> session_analysis_purge_authority",
      "ingest_privacy_authority_apply -> purge_session_analysis_for_owned",
      "ingest_privacy_authority_apply -> session_analysis_purge_grant",
      "ingest_privacy_authority_purge_derived -> purge_daily_digest_narratives",
      "ingest_privacy_authority_purge_derived -> daily_digest_narratives",
    ]);
    const bodies = (await f.db.query<{ proname: string; prosrc: string }>("SELECT p.proname, p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema')")).rows;
    const names = [...OBJECTS.newFunctions, ...OBJECTS.newTables, "acquisition_epoch"];
    const hits = bodies.flatMap((b) => names.filter((x) => new RegExp(`\\b${x}\\b`, "i").test(b.prosrc)).map((x) => `${b.proname} -> ${x}`));
    expect(hits.filter((h) => !guarded.has(h))).toEqual([]);
    // Pin the matched set exactly: a dead allowlist entry (or a new match) fails loudly.
    expect([...new Set(hits)].sort()).toEqual([...guarded].sort());
    expectClean(f.undefinedObjects);
  });

  test("the guard is causal: a ledger rewired to a post-G4 function fails 503 (not 409) and records 42883", async () => {
    const f = await g4Database();
    // Positive control on the raw client.
    await expect(f.db.query("SELECT sessions.ingest_acquisition_hooks_live()")).rejects.toMatchObject({ code: "42883" });
    expect(f.undefinedObjects.map((e) => e.code)).toEqual(["42883"]);
    f.undefinedObjects.splice(0);
    // Mutation: the Worker's claim, sent to the #332 admission instead. A hard dependency would look like this.
    const rewired = sqlLedger((s, p) => f.run(s.replace("sessions.ingest_claim_batch(", "sessions.ingest_acquisition_admit("), p));
    const fs = scratchStore(); open.push(fs.done);
    const app = createIngestApp({ identify: () => identity, deliver: { ledger: rewired, target: { store: fs.store, keyRoot: "96bulls" } }, log: () => {} });
    const res = await app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "m1" })], { batch_id: "mutant" }) }) });
    expect(res.status).toBe(503);
    expect(((await res.json()) as { refused?: unknown }).refused).toBeUndefined();
    expect(f.undefinedObjects.map((e) => e.code)).toEqual(["42883"]);
  });
});

describe("G4-only schema: canonical acceptance (the Worker's default flags)", () => {
  async function canonical() {
    const f = await g4Database();
    const ledger = sqlLedger(f.run);
    const fs = scratchStore(); open.push(fs.done);
    const target = { store: fs.store, keyRoot: "96bulls" };
    const sent: FanoutMessage[] = [];
    // worker.ts deliverDepsFor with default settings: slots on, inline budget, fan-out queue.
    const deliver = { ledger, target, slots: { maxPerUser: 4, maxPerOrg: 8, ttlSeconds: 90 }, inlineMaxSessions: 2, fanout: async (m: FanoutMessage[]) => void sent.push(...m) };
    const app = createIngestApp({ identify: () => identity, deliver, log: () => {} });
    const post = (b: unknown, path = "/v1/delivery/batches") => app.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
    const json = (path: string, body: unknown) => app.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    return { ...f, ledger, target, sent, app, post, json };
  }

  test("health, limits, inline accept, replay, conflict, staged fan-out, selection and reset", async () => {
    const f = await canonical();
    expect((await f.app.request("/healthz")).status).toBe(200);
    expect((await f.app.request("/v1/delivery/limits")).status).toBe(200);

    const b1 = await batch([record({ record_id: "c1" }), record({ record_id: "c2" })], { batch_id: "canon-1" });
    const first = await f.post(b1);
    expect(first.status).toBe(200);
    const receipt = (await first.json()) as { receiptId: string; acceptedRevisionIds: string[] };
    expect(receipt.acceptedRevisionIds).toEqual(["c1:r1", "c2:r1"]);
    // The same batch again replays the same receipt.
    const again = await f.post(b1);
    expect(again.status).toBe(200);
    expect(((await again.json()) as { receiptId: string }).receiptId).toBe(receipt.receiptId);
    // Same batch id, different content: the ledger's conflict, a 409, never acquisition_refused.
    const conflict = await f.post(await batch([record({ record_id: "c3" })], { batch_id: "canon-1" }));
    expect(conflict.status).toBe(409);
    const conflictBody = (await conflict.json()) as { error: { code: string; conflict?: { type: string } }; refused?: unknown };
    expect(conflictBody.error.code).toBe("delivery_conflict");
    expect(conflictBody.refused).toBeUndefined();
    expect((await f.db.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]!.n).toBe(2);

    // More sessions than the inline budget: staged, then the INGEST_FANOUT consumer writes them.
    const staged = await batch(["s-1", "s-2", "s-3"].map((s, i) => record({ record_id: `st${i}`, session_id: s })), { batch_id: "canon-staged" });
    expect((await f.post(staged)).status).toBe(200);
    expect(f.sent.length).toBeGreaterThan(0);
    for (const m of f.sent) await fanout(f.target, m, zstd, f.ledger);
    expect((await f.db.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions WHERE record_id LIKE 'st%'")).rows[0]!.n).toBe(3);

    const account = await deliveryAccount(TENANT);
    expect((await f.json("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "sess-1" }], enabled: false })).status).toBe(200);
    expect((await f.json("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "sess-1" }], enabled: true })).status).toBe(200);
    const reset = await f.json("/v1/delivery/sessions/reset", { account_id: account, request_id: "reset-canon", confirmation: "DELETE ALL SESSIONS" });
    expect(reset.status).toBe(200);

    for (const rpc of ["ingest_acquire_slot", "ingest_release_slot", "ingest_claim_batch", "ingest_commit_batch", "ingest_fanout_fence", "ingest_commit_revisions", "ingest_set_session_uploads", "ingest_reset_uploaded_sessions"])
      expect(f.rpcs).toContain(rpc);
    expect(f.payloads.some(hasAcquisitionKey)).toBe(false);
    expectClean(f.undefinedObjects);
  });
});

describe("G4-only schema: buffered acceptance (claim / stage / commit)", () => {
  async function buffered(asyncMode = false) {
    const f = await g4Database();
    await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
    await f.db.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)", [JSON.stringify({ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId, deletions: [], resets: [] })]);
    await f.db.exec("COMMIT");
    await f.db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)", [TENANT.orgId, gen, epoch]);
    await initializeFixtureSeatClock(f.db as never, gen);
    const ledger = orderedFixtureLedger(sqlLedger(f.run, { acceptanceMode: "buffered", expectedGeneration: gen }), f.db as never);
    const fs = scratchStore(); open.push(fs.done);
    const target = { store: fs.store, keyRoot: "96bulls" };
    const deliver = { ledger, acceptanceMode: "buffered" as const, target };
    const queue: AdmissionMessage[] = [];
    const app = createIngestApp({ identify: (req) => bindingIdentity(req), deliver, log: () => {}, ...(asyncMode ? { asyncAdmission: { mode: "on" as const, enqueue: async (m: AdmissionMessage) => { queue.push(m); } } } : {}) });
    const id = (extra: Record<string, unknown> = {}) => ({ [IDENTITY_HEADER]: encodeIdentity({ ...identity, ...extra } as never) });
    const post = (b: unknown, accept = false) => app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", ...id(accept ? { admission: ADMISSION_ACCEPT_VALUE } : {}) }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
    const control = async (path: string, body: Record<string, any>) => {
      const kind = path.endsWith("/reset") ? "reset" : "selection";
      const payload = kind === "reset" ? { request_id: body.request_id } : { origin_id: body.origin_id, sessions: body.sessions, enabled: body.enabled };
      const r = (await f.db.query("SELECT coalesce((SELECT max(version) FROM sessions.ingest_control_receipts WHERE org_id=$2 AND workspace_id=$3 AND user_id=$4 AND epoch=$1),0)::text AS v,encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",
        [epoch, TENANT.orgId, TENANT.workspaceId, TENANT.userId, kind, JSON.stringify(payload)])).rows[0] as { v: string; d: string };
      return app.request(path, { method: "POST", headers: { "content-type": "application/json", ...id({ control: { epoch, version: (BigInt(r.v) + 1n).toString(), digest: r.d, kind } }) }, body: JSON.stringify(body) });
    };
    const outbox = async (batchId: string) => (await f.db.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE batch_id=$1", [batchId])).rows[0]?.active_stage_token as string | undefined;
    return { ...f, ledger, deliver, queue, app, post, control, outbox };
  }

  test("sync acceptBatch: claim -> prepare -> PUT -> commit, replay, ordered selection and reset", async () => {
    const f = await buffered();
    const b = await batch([record({ record_id: "b1" }), record({ record_id: "b2" })], { batch_id: "buf-1" });
    const first = await f.post(b);
    expect(first.status).toBe(200);
    const receipt = (await first.json()) as { receiptId: string; acceptedRevisionIds: string[] };
    expect(receipt.acceptedRevisionIds).toEqual(["b1:r1", "b2:r1"]);
    expect(await f.outbox("buf-1")).toBeString();
    const again = await f.post(b);
    expect(again.status).toBe(200);
    expect(((await again.json()) as { receiptId: string }).receiptId).toBe(receipt.receiptId);

    const account = await deliveryAccount(TENANT);
    const sessions = [{ source: "claude", session_id: "sess-1" }];
    expect((await f.control("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions, enabled: false })).status).toBe(200);
    expect((await f.control("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions, enabled: true })).status).toBe(200);
    expect((await f.control("/v1/delivery/sessions/reset", { account_id: account, request_id: "reset-buf", confirmation: "DELETE ALL SESSIONS" })).status).toBe(200);
    // An upload after the reset is accepted through the same claim / stage / commit.
    const after = await f.post(await batch([record({ record_id: "b3" })], { batch_id: "buf-2" }));
    expect(after.status).toBe(200);
    expect(await f.outbox("buf-2")).toBeString();

    for (const rpc of ["ingest_claim_buffered_batch", "ingest_prepare_buffer_stage", "ingest_commit_buffered_batch", "ingest_set_session_uploads", "ingest_reset_uploaded_sessions"])
      expect(f.rpcs).toContain(rpc);
    expect(f.payloads.some(hasAcquisitionKey)).toBe(false);
    expectClean(f.undefinedObjects);
  });

  test("async admission smoke: queued ACK, the consumer admits through the same buffered RPCs", async () => {
    const f = await buffered(true);
    const res = await f.post(await batch([record({ record_id: "q1" })], { batch_id: "async-g4" }), true);
    expect(res.status).toBe(200);
    expect(f.queue).toHaveLength(1);
    expect(await admitMessage({ mode: "on", deliver: f.deliver, custodySource: "relayhistory", decompress: zstd }, f.queue[0]!, 1)).toMatchObject({ action: "ack", outcome: "admitted" });
    expect(await f.outbox("async-g4")).toBeString();
    expect(f.payloads.some(hasAcquisitionKey)).toBe(false);
    expectClean(f.undefinedObjects);
  });
});
