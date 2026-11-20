/**
 * unit 1c: concurrent contention bench of the staged ACK on a THROWAWAY loopback PostgreSQL.
 * Synthetic data only. Never point this at a real database.
 *
 * Each worker is a distinct user + origin and replays one prepared batch: BEGIN; SET LOCAL lock_timeout 2s /
 * statement_timeout 5s / ROLE bulls96_session_ingest; ingest_origin_commit_buffered_batch; ROLLBACK (or COMMIT in
 * the validation cell). The ACK holds the clock to transaction end either way.
 * Clock wait: RAISE NOTICE timestamps immediately before/after the clock lock line (patched into the throwaway DB only).
 *
 * REAL-COMMIT variant: fresh unique batches per cell, each committed once; durable rows counted afterwards.
 * Usage: bun benchmarks/receipt-clock-contention/contention-commit.ts '<json plan>'   plan = { cells:[{variant,state,c,n,seconds,commit?}], derived? }
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
type Cell = { variant: "loop" | "set" | "H"; state: "inactive" | "shared"; c: number; n: number; k: number };
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
  const pct = (xs: number[], p: number) => { if (!xs.length) return null; const t = [...xs].sort((a, b) => a - b); return +t[Math.min(t.length - 1, Math.floor(p * (t.length - 1) + 0.5))].toFixed(1); };
  let installed = "";
  for (const [ci, cell] of plan.cells.entries()) {
    if (installed !== cell.variant) { await install(cell.variant); installed = cell.variant; }
    // Fresh workers (users/origins) per cell; K fresh batches each, claims + stages committed now, commits later.
    type W = { userId: string; payloads: string[]; records: string[] };
    const ws: W[] = [];
    const prep = async (w: number): Promise<W> => {
      const t = { ...TENANT, userId: `fc${ci}-u${w}` }, origin = `fc${ci}-o${w}`;
      const warm = await mkRaw(t, `fc${ci}-warm-${w}`, 1, origin, w);
      const o = await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm);
      if (o.outcome !== "accepted_buffered") throw Error("warm " + o.outcome);
      const out: W = { userId: t.userId, payloads: [], records: [] };
      for (let k = 0; k < cell.k; k++) {
        const id = `fc${ci}-b${w}-${k}`, raw = await mkRaw(t, id, cell.n, origin, w * 1000 + k);
        let payload: string | undefined;
        const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
        await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
        if (!payload) throw Error("capture " + id);
        out.payloads.push(payload); out.records.push(...raw.records.map((r: any) => r.record_id));
      }
      return out;
    };
    for (let i = 0; i < cell.c; i += 8) ws.push(...(await Promise.all(Array.from({ length: Math.min(8, cell.c - i) }, (_, j) => prep(i + j)))));
    await pool.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()+interval '6 hours' WHERE cleanup_state='live' AND receipt_state='preparing'");
    // History state for this cell (committed fixtures). shared = 10 managed scopes holding every fresh record in scope_keys.
    await pool.query(`SET session_replication_role=replica;DELETE FROM sessions.ingest_history_derived_objects;DELETE FROM sessions.ingest_history_scope_current;
      DELETE FROM sessions.ingest_history_scope_keys;DELETE FROM sessions.ingest_history_scope_versions;SET session_replication_role=origin`);
    if (cell.state === "shared") {
      for (let k = 0; k < SHARED_SCOPES; k++) {
        const auth = crypto.randomUUID();
        await pool.query(`SET session_replication_role=replica;INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
          VALUES('${auth}','${TENANT.orgId}','${TENANT.workspaceId}','claude','shared-${k}',1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64));
          INSERT INTO sessions.ingest_history_scope_current VALUES('${TENANT.orgId}','${TENANT.workspaceId}','claude','shared-${k}','${auth}',true);
          INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES('${auth}-1','${auth}',repeat('b',64),1);SET session_replication_role=origin`);
      }
      for (const w of ws) await pool.query("INSERT INTO sessions.ingest_history_scope_keys SELECT $1,$2,r,'claude','shared-'||(ord%$3) FROM unnest($4::text[]) WITH ORDINALITY u(r,ord) ON CONFLICT DO NOTHING",
        [TENANT.orgId, TENANT.workspaceId, SHARED_SCOPES, w.records]);
    }
    await pool.query("ANALYZE");
    const users = ws.map((w) => w.userId);
    const seatsBefore = Number((await pool.query("SELECT count(*) n FROM sessions.ingest_receipt_seats")).rows[0].n);
    const samples: Array<{ ms: number; wait: number | null; hold: number | null; code: string | null }> = [];
    let arrived = 0; let release!: () => void; const gate = new Promise<void>((r) => (release = r));
    const runWorker = async (w: W) => {
      const c = new pg.Client({ connectionString: target.href }); await c.connect();
      let a: number | null = null, b: number | null = null;
      c.on("notice", (m: any) => { const [tag, v] = String(m.message).split(" "); if (tag === "rcfA") a = Number(v); if (tag === "rcfB") b = Number(v); });
      try {
        // warm-up: compile the function on this connection with a call that is rolled back (does not commit any batch)
        await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); await c.query(COMMIT, [w.payloads[0]]).catch(() => {}); await c.query("ROLLBACK");
        if (++arrived === cell.c) release(); await gate;
        for (const payload of w.payloads) {
          a = b = null; const t0 = performance.now(); let code: string | null = null;
          try {
            await c.query("BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest");
            const r = await c.query(COMMIT, [payload]);
            if (r.rows[0].result.kind !== "committed") code = "kind:" + r.rows[0].result.kind;
            await c.query(code ? "ROLLBACK" : "COMMIT");
          } catch (e: any) { code = String(e.code ?? e.message); await c.query("ROLLBACK").catch(() => {}); }
          const endEpoch = Date.now() / 1000;
          samples.push({ ms: performance.now() - t0, wait: a !== null && b !== null ? (b - a) * 1000 : null, hold: b !== null ? (endEpoch - b) * 1000 : null, code });
        }
      } finally { await c.end(); }
    };
    const t0 = Date.now();
    await Promise.all(ws.map(runWorker));
    const wall = (Date.now() - t0) / 1000;
    // Independent durable count after the drain: committed staged batches of this cell's users, and new seats.
    const durable = (await pool.query("SELECT count(*) n FROM sessions.ingest_batches WHERE user_id=ANY($1) AND state='committed' AND buffer_receipt_kind='staged' AND batch_id NOT LIKE '%warm%'", [users])).rows[0].n;
    const durableRecords = (await pool.query("SELECT count(*) n FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_claim_attempts a USING(claim_token) WHERE a.user_id=ANY($1) AND a.batch_id NOT LIKE '%warm%'", [users])).rows[0].n;
    const seatsAfter = Number((await pool.query("SELECT count(*) n FROM sessions.ingest_receipt_seats")).rows[0].n);
    const ok = samples.filter((x) => !x.code), waits = samples.map((x) => x.wait).filter((x): x is number => x !== null);
    const codes: Record<string, number> = {}; for (const x of samples) if (x.code) codes[x.code] = (codes[x.code] ?? 0) + 1;
    const res = { ...cell, attempts: samples.length, okClient: ok.length, durableBatches: Number(durable), durableDecisionRows: Number(durableRecords), newSeats: seatsAfter - seatsBefore,
      wallSecondsInclDrain: +wall.toFixed(2), durableRecordsPerSec: +(Number(durableRecords) / wall).toFixed(0),
      latencyThroughCommit: { p50: pct(ok.map((x) => x.ms), 0.5), p95: pct(ok.map((x) => x.ms), 0.95), max: pct(ok.map((x) => x.ms), 1) },
      clockWait: { p50: pct(waits, 0.5), p95: pct(waits, 0.95), max: pct(waits, 1) },
      holdClockToCommitEnd: { p50: pct(ok.map((x) => x.hold).filter((x): x is number => x !== null), 0.5), p95: pct(ok.map((x) => x.hold).filter((x): x is number => x !== null), 0.95) },
      durableAcksPerSec: +(Number(durable) / wall).toFixed(1), errors: codes };
    console.log("FCELL " + JSON.stringify(res));
  }
} finally {
  await pool.end(); scratch.done();
  await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end();
}
