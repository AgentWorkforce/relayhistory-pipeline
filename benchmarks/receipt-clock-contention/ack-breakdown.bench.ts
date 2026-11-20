/** unit 1b: clock-hold breakdown of the staged ACK, history live. Throwaway loopback PG, synthetic data only.
 * Every measured commit runs inside one transaction that is ROLLED BACK (fixture rows, trigger/index changes included). */
import { afterAll, beforeAll, expect, test } from "bun:test";
import pg from "pg";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL!;
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const FN = ["ingest_lazy_ack_batch", "ingest_receipt_seat_records", "ingest_history_native_changed", "ingest_history_active", "ingest_history_fence_builds", "ingest_history_invalidate_scope", "ingest_history_scope_managed", "ingest_lazy_attempt"];
let admin: pg.Client, pool: pg.Pool, name: string, generation: string, scratch: ReturnType<typeof scratchStore>, serial = 0;
const tenant = (l: string) => ({ ...TENANT, orgId: "org-a", userId: `t-${l}-${++serial}` });
const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
  intercept?.(sql, params as unknown[]);
  const c = await pool.connect();
  try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
  catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
}, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
async function custody(t: any, raw: any) {
  const p = await prepareDelivery(t, raw);
  const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string;
  return { witness, sourceDigest };
}
const sessOf = (id: string, i: number, sessions: number) => `${id}-s${i % sessions}`;
const raw = async (t: any, id: string, n: number, origin: string, sessions: number) =>
  batch(Array.from({ length: n }, (_, i) => record({ record_id: `${id}-r${i}`, revision: 2, session_id: sessOf(id, i, sessions), origin_id: origin })), { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
async function capture(t: any, id: string, n: number, origin: string, sessions: number) {
  let payload: string | undefined; const r = await raw(t, id, n, origin, sessions);
  const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
  await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r).catch(() => {});
  if (!payload) throw Error("not captured"); return payload;
}
beforeAll(async () => {
  admin = new pg.Client({ connectionString: url }); await admin.connect();
  name = "ack_bd_" + crypto.randomUUID().replaceAll("-", ""); await admin.query("CREATE DATABASE " + name);
  const target = new URL(url); target.pathname = "/" + name;
  pool = new pg.Pool({ connectionString: target.href, max: 8 }); scratch = scratchStore();
  await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
  const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); }
  generation = (await derivedSchemaAssets()).expected.generation;
  await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
    privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
  await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
    capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
    allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
  await pool.query("UPDATE sessions.ingest_history_switch SET live=true WHERE singleton");
  // RCF_H=1: THROWAWAY-DB-ONLY experiment (no file, no branch): the set body's slot UPDATE always uses the round path
  // (one N-row UPDATE per round, so the statement trigger fires once per round with N-row transition tables) = design H.
  if (process.env.RCF_H === "1") {
    const def = (await pool.query("SELECT pg_get_functiondef('sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure) d")).rows[0].d as string;
    if (!def.includes("IF sessions.ingest_history_live() THEN")) throw Error("H patch anchor missing");
    await pool.query(def.replace("IF sessions.ingest_history_live() THEN", "IF false THEN"));
  }
  // Optional amplifier fixtures (committed; synthetic, other org): RCF_BIG_SLOTS filler slots, RCF_BIG_BUILDS abandoned builds,
  // RCF_LIVE_BUILDS in-flight builds (other sessions), RCF_BIG_OUTBOX outbox rows.
  const bigSlots = Number(process.env.RCF_BIG_SLOTS ?? 0), bigBuilds = Number(process.env.RCF_BIG_BUILDS ?? 0), liveBuilds = Number(process.env.RCF_LIVE_BUILDS ?? 0), bigOutbox = Number(process.env.RCF_BIG_OUTBOX ?? 0);
  if (bigSlots) await pool.query(`SET session_replication_role=replica;INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256,lineage,current_owner,current_source,current_session,current_revision,current_digest,current_reset_epoch,current_fence_epoch)
    SELECT encode(sha256(convert_to('filler'||g,'UTF8')),'hex'),'[]','org-filler','ws','o'||(g%1000),'r'||g,repeat('c',64),repeat('3',64),repeat('e',64),1,'u'||(g%5000),'claude','s'||(g%20000),1,repeat('f',64),0,0 FROM generate_series(1,${bigSlots}) g;SET session_replication_role=origin;ANALYZE sessions.ingest_lazy_native_slots`);
  if (bigOutbox) await pool.query(`SET session_replication_role=replica;INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner,state)
    SELECT encode(sha256(convert_to('ob'||g,'UTF8')),'hex'),1,repeat('c',64),repeat('3',64),'u',CASE WHEN g%10=0 THEN 'pending' ELSE 'complete' END FROM generate_series(1,${bigOutbox}) g;SET session_replication_role=origin;ANALYZE sessions.ingest_history_reconcile_outbox`);
  for (const [k, state, lease] of [[bigBuilds, "abandoned", "-interval '1 day'"], [liveBuilds, "keys", "+interval '1 hour'"]] as const) if (k)
    await pool.query(`SET session_replication_role=replica;INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,generation,lease_token,lease_until,state)
      SELECT gen_random_uuid(),'org-filler','ws','claude','s'||g,'${generation}',gen_random_uuid(),clock_timestamp()${lease},'${state}' FROM generate_series(1,${k}) g;SET session_replication_role=origin;ANALYZE sessions.ingest_history_scope_builds`);
}, 900000);
afterAll(async () => { await pool?.end(); scratch?.done(); if (admin) { await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); } }, 120000);

/** Managed scopes for every session of the batch (scope_current + keys) and one live build touching record 0. */
async function activeHistory(c: pg.PoolClient, id: string, n: number, sessions: number) {
  await c.query("SET LOCAL session_replication_role=replica");
  for (let s = 0; s < Math.min(n, sessions); s++) {
    const auth = crypto.randomUUID(), sess = `${id}-s${s}`;
    await c.query(`INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
      VALUES($1,$2,$3,'claude',$4,1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64))`, [auth, TENANT.orgId, TENANT.workspaceId, sess]);
    await c.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,'claude',$3,$4,true)", [TENANT.orgId, TENANT.workspaceId, sess, auth]);
    await c.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) SELECT $1||'-'||g,$2,repeat('b',64),1 FROM generate_series(1,$3::int) g", [auth, auth, Number(process.env.RCF_DERIVED ?? 20)]);
  }
  await c.query("INSERT INTO sessions.ingest_history_scope_keys SELECT $1,$2,$3||'-r'||g,'claude',$3||'-s'||(g%$4) FROM generate_series(0,$5-1) g ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, id, sessions, n]);
  const b = crypto.randomUUID();
  await c.query(`INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,generation,lease_token,lease_until,state) VALUES($1,$2,$3,'claude','other',$4,gen_random_uuid(),clock_timestamp()+interval '1 hour','keys')`, [b, TENANT.orgId, TENANT.workspaceId, generation]);
  await c.query("INSERT INTO sessions.ingest_history_scope_build_records VALUES($1,$2)", [b, `${id}-r0`]);
  await c.query("SET LOCAL session_replication_role=origin");
}

test("breakdown", async () => {
  const sizes = (process.env.RCF_SIZES ?? "1,10,100,500").split(",").map(Number);
  const scenarios = (process.env.RCF_SCENARIOS ?? "inactive,active,active_spread,notrigger,notrigger_noidx").split(",");
  const reps = Number(process.env.RCF_REPS ?? 4), explain = process.env.RCF_EXPLAIN === "1";
  const out: any[] = [];
  const IDX = "sessions.ingest_lazy_native_current_owner,sessions.ingest_lazy_native_slots_org_id_idx,sessions.ingest_lazy_native_slots_current_seat_key_idx,sessions.ingest_lazy_native_slots_unowned_origin_idx";
  for (const sc of scenarios) {
    // Scenario DDL is COMMITTED (throwaway DB) so it never invalidates plans inside a measured transaction; undone after.
    if (sc.startsWith("notrigger")) await pool.query("ALTER TABLE sessions.ingest_lazy_native_slots DISABLE TRIGGER ingest_history_native_update");
    let idxDefs: string[] = [];
    if (sc === "notrigger_noidx") {
      idxDefs = (await pool.query("SELECT indexdef FROM pg_indexes WHERE schemaname='sessions' AND indexname = ANY($1)", [IDX.split(",").map((x) => x.split(".")[1])])).rows.map((r) => r.indexdef);
      await pool.query("DROP INDEX " + IDX);
    }
    for (const mode of ["loop", "set"]) {
      await pool.query(`ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_set_based='${mode === "set" ? "on" : "off"}'`);
      const c = await pool.connect(); // one warm measuring connection per (scenario, mode); rep 0 is the warm-up
      try {
        for (const n of sizes) for (let rep = 0; rep < reps; rep++) {
          const t = tenant(`${sc}-${n}`); const origin = `o-${t.userId}`;
          const w = await raw(t, `warm-${t.userId}`, 1, origin, 1);
          expect((await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, w) } as never, t, w)).outcome).toBe("accepted_buffered");
          const id = `b-${t.userId}`, sessions = sc === "active_spread" ? n : Math.max(1, Math.ceil(n / 50));
          const payload = await capture(t, id, n, origin, sessions);
          const tag = `rcf|${sc}|${mode}|${n}|${rep}`;
          const snap = async () => Object.fromEntries((await c.query("SELECT funcname,total_time,self_time,calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname=ANY($1)", [FN])).rows.map((x) => [x.funcname, x]));
          const tab = async () => (await c.query("SELECT n_tup_upd::bigint u,n_tup_hot_upd::bigint h FROM pg_stat_xact_user_tables WHERE schemaname='sessions' AND relname='ingest_lazy_native_slots'")).rows[0] ?? { u: 0, h: 0 };
          await c.query("BEGIN");
          try {
            await c.query(`SET LOCAL application_name='${tag}'`);
            if (sc.startsWith("active")) await activeHistory(c, id, n, sessions);
            if (explain && rep === reps - 1) await c.query("LOAD 'auto_explain';SET LOCAL auto_explain.log_min_duration=0;SET LOCAL auto_explain.log_nested_statements=on;SET LOCAL auto_explain.log_analyze=off");
            const active = (await c.query("SELECT sessions.ingest_history_active() a")).rows[0].a;
            const b = await snap(), tb = await tab();
            await c.query("SET LOCAL ROLE bulls96_session_ingest");
            const t0 = performance.now(); const r = await c.query(COMMIT, [payload]); const t1 = performance.now();
            expect(r.rows[0].result.kind).toBe("committed");
            await c.query("RESET ROLE");
            const a = await snap(), ta = await tab();
            const d = (k: string, f = "total_time") => +(Number(a[k]?.[f] ?? 0) - Number(b[k]?.[f] ?? 0)).toFixed(3);
            out.push({ sc, mode, n, rep, active, explained: explain && rep === reps - 1, stmt: +(t1 - t0).toFixed(2), ack: d("ingest_lazy_ack_batch"), seat_records: d("ingest_receipt_seat_records"),
              trig: d("ingest_history_native_changed"), trig_calls: d("ingest_history_native_changed", "calls"), trig_self: d("ingest_history_native_changed", "self_time"),
              h_active: d("ingest_history_active"), fence: d("ingest_history_fence_builds"), invalidate: d("ingest_history_invalidate_scope"), inv_calls: d("ingest_history_invalidate_scope", "calls"),
              managed: d("ingest_history_scope_managed"), lazy_attempt: d("ingest_lazy_attempt"), upd: Number(ta.u) - Number(tb.u), hot: Number(ta.h) - Number(tb.h) });
          } finally { await c.query("ROLLBACK").catch(() => {}); }
        }
      } finally { c.release(true); }
    }
    if (sc.startsWith("notrigger")) await pool.query("ALTER TABLE sessions.ingest_lazy_native_slots ENABLE TRIGGER ingest_history_native_update");
    for (const def of idxDefs) await pool.query(def);
  }
  console.log("BD_JSON " + JSON.stringify(out));
}, 3600000);
