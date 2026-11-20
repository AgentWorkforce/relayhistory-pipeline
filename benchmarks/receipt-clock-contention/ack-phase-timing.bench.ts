/** Per-phase timing of the CURRENT staged-ACK body on a throwaway loopback PG. Synthetic data only. */
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
const raw = async (t: any, id: string, n: number, origin: string, sessions: number) =>
  batch(Array.from({ length: n }, (_, i) => record({ record_id: `${id}-r${i}`, revision: 2, session_id: `${id}-s${i % sessions}`, origin_id: origin })), { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
async function capture(t: any, id: string, n: number, origin: string, sessions: number) {
  let payload: string | undefined; const r = await raw(t, id, n, origin, sessions);
  const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
  await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r).catch(() => {});
  if (!payload) throw Error("not captured"); return payload;
}
beforeAll(async () => {
  admin = new pg.Client({ connectionString: url }); await admin.connect();
  name = "ack_timing_" + crypto.randomUUID().replaceAll("-", ""); await admin.query("CREATE DATABASE " + name);
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
  await pool.query("UPDATE sessions.ingest_history_switch SET live=$1 WHERE singleton", [process.env.RCF_LIVE !== "off"]);
  if (process.env.RCF_GATE === "on") await pool.query("ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_set_based='on'");
  console.log("GATE " + JSON.stringify((await pool.query("SELECT proconfig FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure")).rows[0].proconfig));
}, 300000);
afterAll(async () => { await pool?.end(); scratch?.done(); if (admin) { await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); } }, 60000);

test("per-phase timing of the current staged ACK", async () => {
  const FN = ["ingest_commit_buffered_batch", "ingest_lazy_ack_batch", "ingest_receipt_seat_records", "ingest_lazy_attempt_ack", "ingest_lazy_attempt", "ingest_lock_fences"];
  const out: any[] = [];
  for (const n of [1, 2, 3, 5, 10, 25, 100, 500]) for (let rep = 0; rep < 7; rep++) {
    const t = tenant(`n${n}`); const origin = `o-${t.userId}`;
    // warm: first delivery of the user adopts fence/seal (excluded from timing)
    const w = await raw(t, `warm-${t.userId}`, 1, origin, 1);
    expect((await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, w) } as never, t, w)).outcome).toBe("accepted_buffered");
    const payload = await capture(t, `b-${t.userId}`, n, origin, Math.max(1, Math.ceil(n / 50)));
    const c = await pool.connect();
    try {
      const snap = async () => Object.fromEntries((await c.query("SELECT funcname,total_time FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname=ANY($1)", [FN])).rows.map((x) => [x.funcname, Number(x.total_time)]));
      await c.query("BEGIN"); if (process.env.RCF_EXPLAIN && n === 500 && rep === 3) await c.query("LOAD 'auto_explain';SET LOCAL auto_explain.log_min_duration=1;SET LOCAL auto_explain.log_nested_statements=on;SET LOCAL auto_explain.log_analyze=on;SET LOCAL auto_explain.log_timing=on;SET LOCAL auto_explain.log_triggers=on"); const b = await snap(); await c.query("SET LOCAL ROLE bulls96_session_ingest");
      const t0 = performance.now(); const r = await c.query(COMMIT, [payload]); const t1 = performance.now();
      expect(r.rows[0].result.kind).toBe("committed");
      await c.query("RESET ROLE");
      const a = await snap(); const f: any = Object.fromEntries(FN.map((k) => [k, { total_time: (a[k] ?? 0) - (b[k] ?? 0) }]));
      const t2 = performance.now(); await c.query("COMMIT"); const t3 = performance.now();
      const ack = Number(f.ingest_lazy_ack_batch?.total_time ?? NaN), tail = Number(f.ingest_lazy_attempt_ack?.total_time ?? NaN);
      out.push({ n, rep, statement_ms: +(t1 - t0).toFixed(2), commit_fn_ms: +Number(f.ingest_commit_buffered_batch?.total_time).toFixed(2), ack_batch_ms: +ack.toFixed(2),
        seat_records_ms: +Number(f.ingest_receipt_seat_records?.total_time ?? 0).toFixed(2), attempt_ack_tail_ms: +tail.toFixed(2), COMMIT_ms: +(t3 - t2).toFixed(2) });
    } finally { await c.query("ROLLBACK").catch(() => {}); c.release(); }
  }
  console.log("TIMING_JSON " + JSON.stringify(out));
}, 900000);
