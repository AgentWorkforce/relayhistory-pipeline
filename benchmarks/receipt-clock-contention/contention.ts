/**
 * unit 1c: concurrent contention bench of the staged ACK on a THROWAWAY loopback PostgreSQL.
 * Synthetic data only. Never point this at a real database.
 *
 * Each worker is a distinct user + origin and replays one prepared batch: BEGIN; SET LOCAL lock_timeout 2s /
 * statement_timeout 5s / ROLE bulls96_session_ingest; ingest_origin_commit_buffered_batch; ROLLBACK (or COMMIT in
 * the validation cell). The ACK holds the clock to transaction end either way.
 * Clock wait: RAISE NOTICE timestamps immediately before/after the clock lock line (patched into the throwaway DB only).
 *
 * Usage: bun benchmarks/receipt-clock-contention/contention.ts '<json plan>'   plan = { cells:[{variant,state,c,n,seconds,commit?}], derived? }
 */
import pg from "pg";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL!;
{ const u = new URL(url); if (!["127.0.0.1", "localhost"].includes(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); }
type Cell = { variant: "loop" | "set" | "H"; state: "inactive" | "shared" | "distinct"; c: number; n: number; seconds: number; commit?: boolean; derived?: number };
const plan = JSON.parse(process.argv[2] ?? "{}") as { cells: Cell[]; maxC?: number };
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const CLOCK_LINE = "SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;";
const SIZES = [100, 500];
const SHARED_SCOPES = 10;

const admin = new pg.Client({ connectionString: url }); await admin.connect();
const name = "rcf_contention_" + crypto.randomUUID().replaceAll("-", "").slice(0, 12);
await admin.query("CREATE DATABASE " + name);
const target = new URL(url); target.pathname = "/" + name;
const pool = new pg.Pool({ connectionString: target.href, max: 24 });
const scratch = scratchStore();
const log = (...a: unknown[]) => console.error(new Date().toISOString(), ...a);
try {
  await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
  { const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); } }
  const generation = (await derivedSchemaAssets()).expected.generation;
  await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
    privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
  await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
    capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
    allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
  await pool.query("UPDATE sessions.ingest_history_switch SET live=true WHERE singleton");

  // Function variants (throwaway DB only): clock-wait notices around the clock lock line; H = round path forced while live.
  const base = (await pool.query("SELECT pg_get_functiondef($1::regprocedure) d", [ACK])).rows[0].d as string;
  if (base.split(CLOCK_LINE).length !== 2 || !base.includes("IF sessions.ingest_history_live() THEN")) throw Error("patch anchors");
  const instrumented = base.replace(CLOCK_LINE, "RAISE NOTICE 'rcfA %',extract(epoch FROM clock_timestamp());" + CLOCK_LINE + "RAISE NOTICE 'rcfB %',extract(epoch FROM clock_timestamp());");
  const install = async (v: Cell["variant"]) => {
    await pool.query(v === "H" ? instrumented.replace("IF sessions.ingest_history_live() THEN", "IF false THEN") : instrumented);
    await pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_set_based='${v === "loop" ? "off" : "on"}'`);
  };

  // Workers: distinct user + origin; per size one prepared batch (sessions of 50 records, distinct per worker).
  const maxC = plan.maxC ?? Math.max(...plan.cells.map((x) => x.c));
  const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await pool.connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
  const custody = async (t: any, raw: any) => {
    const p = await prepareDelivery(t, raw);
    const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
  };
  const mkRaw = async (t: any, id: string, n: number, origin: string, w: number) =>
    batch(Array.from({ length: n }, (_, i) => record({ record_id: `${id}-r${i}`, revision: 2, session_id: `w${w}-n${n}-s${Math.floor(i / 50)}`, origin_id: origin })), { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
  type Worker = { w: number; userId: string; payload: Record<number, string>; recordIds: Record<number, string[]>; sessions: Record<number, string[]> };
  const workers: Worker[] = [];
  const prepOne = async (w: number): Promise<Worker> => {
    const t = { ...TENANT, userId: `bench-u${w}` }, origin = `bench-o${w}`;
    const warm = await mkRaw(t, `warm-${w}`, 1, origin, w);
    const o = await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm);
    if (o.outcome !== "accepted_buffered") throw Error("warm " + o.outcome);
    const wk: Worker = { w, userId: t.userId, payload: {}, recordIds: {}, sessions: {} };
    for (const n of SIZES) {
      const id = `b-${w}-${n}`, raw = await mkRaw(t, id, n, origin, w);
      let payload: string | undefined;
      const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
      await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
      if (!payload) throw Error("capture " + id);
      wk.payload[n] = payload; wk.recordIds[n] = raw.records.map((r: any) => r.record_id); wk.sessions[n] = [...new Set(raw.records.map((r: any) => r.session_id as string))];
    }
    return wk;
  };
  for (let i = 0; i < maxC; i += 8) workers.push(...(await Promise.all(Array.from({ length: Math.min(8, maxC - i) }, (_, k) => prepOne(i + k)))));
  await pool.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()+interval '6 hours' WHERE cleanup_state='live' AND receipt_state='preparing'");
  log("prepared", workers.length, "workers");

  // History states (committed fixture rows, synthetic; FK checks off only while inserting them).
  const clearHistory = () => pool.query(`SET session_replication_role=replica;DELETE FROM sessions.ingest_history_derived_objects;DELETE FROM sessions.ingest_history_scope_current;
    DELETE FROM sessions.ingest_history_scope_keys;DELETE FROM sessions.ingest_history_scope_versions;DELETE FROM sessions.ingest_history_required_scopes;SET session_replication_role=origin`);
  const scope = async (session: string, derived: number) => {
    const auth = crypto.randomUUID();
    await pool.query(`SET session_replication_role=replica;INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
      VALUES('${auth}','${TENANT.orgId}','${TENANT.workspaceId}','claude','${session}',1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64));
      INSERT INTO sessions.ingest_history_scope_current VALUES('${TENANT.orgId}','${TENANT.workspaceId}','claude','${session}','${auth}',true);
      INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) SELECT '${auth}-'||g,'${auth}',repeat('b',64),1 FROM generate_series(1,${derived}) g;
      SET session_replication_role=origin`);
  };
  let current = "";
  const setState = async (state: Cell["state"], derived: number) => {
    const key = state + "/" + derived; if (key === current) return; current = key;
    await clearHistory();
    if (state === "shared") {
      for (let k = 0; k < SHARED_SCOPES; k++) await scope(`shared-${k}`, derived);
      for (const wk of workers) for (const n of SIZES)
        await pool.query("INSERT INTO sessions.ingest_history_scope_keys SELECT $1,$2,r,'claude','shared-'||(ord%$3) FROM unnest($4::text[]) WITH ORDINALITY u(r,ord) ON CONFLICT DO NOTHING",
          [TENANT.orgId, TENANT.workspaceId, SHARED_SCOPES, wk.recordIds[n]]);
    }
    if (state === "distinct") for (const wk of workers) for (const n of SIZES) for (const s of wk.sessions[n]) await scope(s, derived);
    await pool.query("ANALYZE");
  };

  const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return +s[Math.min(s.length - 1, Math.floor(p * (s.length - 1) + 0.5))].toFixed(1); };
  const results: unknown[] = [];
  let installed = "";
  for (const cell of plan.cells) {
    if (installed !== cell.variant) { await install(cell.variant); installed = cell.variant; }
    await setState(cell.state, cell.derived ?? 1);
    const samples: Array<{ ms: number; wait: number | null; code: string | null }> = [];
    let deadline = Infinity; // set when every worker has warmed up (barrier), not before
    const runWorker = async (wk: Worker) => {
      const c = new pg.Client({ connectionString: target.href }); await c.connect();
      let a: number | null = null, b: number | null = null;
      c.on("notice", (m: any) => { const [tag, v] = String(m.message).split(" "); if (tag === "rcfA") a = Number(v); if (tag === "rcfB") b = Number(v); });
      try {
        // one warm-up call per connection (plpgsql compile + plan cache), not sampled
        for (let warm = 0; warm < 2; warm++) { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); await c.query(COMMIT, [wk.payload[cell.n]]).catch(() => {}); await c.query("ROLLBACK"); }
        await barrier();
        while (Date.now() < deadline) {
          a = b = null;
          const t0 = performance.now(); let code: string | null = null;
          try {
            await c.query("BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest");
            const r = await c.query(COMMIT, [wk.payload[cell.n]]);
            if (r.rows[0].result.kind !== "committed") code = "kind:" + r.rows[0].result.kind;
          } catch (e: any) { code = String(e.code ?? e.message); }
          await c.query(cell.commit && !code ? "COMMIT" : "ROLLBACK").catch(() => {});
          const ms = performance.now() - t0;
          samples.push({ ms, wait: a !== null && b !== null ? (b - a) * 1000 : null, code });
          if (cell.commit && !code) break; // a committed batch cannot be replayed
        }
      } finally { await c.end(); }
    };
    let arrived = 0; let release!: () => void; const gate = new Promise<void>((r) => (release = r));
    const barrier = async () => { if (++arrived === cell.c) { deadline = Date.now() + cell.seconds * 1000; t0 = Date.now(); release(); } await gate; };
    let t0 = Date.now();
    await Promise.all(workers.slice(0, cell.c).map(runWorker));
    const span = (Date.now() - t0) / 1000;
    const ok = samples.filter((s) => !s.code), waits = samples.map((s) => s.wait).filter((x): x is number => x !== null);
    const codes: Record<string, number> = {}; for (const s of samples) if (s.code) codes[s.code] = (codes[s.code] ?? 0) + 1;
    const res = { ...cell, derived: cell.derived ?? 1, samples: samples.length, ok: ok.length, runSeconds: +span.toFixed(1),
      ackedRecordsPerSec: +(ok.length * cell.n / span).toFixed(0), acksPerSec: +(ok.length / span).toFixed(1),
      latency: { p50: pct(ok.map((s) => s.ms), 0.5), p95: pct(ok.map((s) => s.ms), 0.95), max: pct(ok.map((s) => s.ms), 1) },
      clockWait: { p50: pct(waits, 0.5), p95: pct(waits, 0.95), max: pct(waits, 1) }, errors: codes };
    results.push(res);
    console.log("CELL " + JSON.stringify(res));
  }
} finally {
  await pool.end(); scratch.done();
  await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end();
}
