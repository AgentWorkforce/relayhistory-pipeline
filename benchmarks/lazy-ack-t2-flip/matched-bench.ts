/**
 * Matched main-vs-T2 staged-ACK benchmark on a THROWAWAY loopback PostgreSQL (synthetic data only). Every cell gets a
 * FRESH scratch database, created and dropped by this script, so the host never accumulates bench data.
 *
 * Arms (same tree, same harness): main = gate 'off' (043800's body, byte-identical by test); t2 = set_gate('fenced')
 * then set_mode('t2'). History: switch live (ingest_history_set_live(true)) with NO managed scope ("unmanaged history").
 * Load: C workers, each a distinct user + origin, deliver FRESH n-record batches back to back through the real upload
 * path (deliverBatch: claim, prepare, commit+ACK through ingest_origin_commit_buffered_batch); every ledger call is its
 * own transaction as the runtime role with SET LOCAL lock_timeout 2s / statement_timeout 5s, and application_name
 * names the worker and stage (w<i>-claim | w<i>-prepare | w<i>-commit).
 * Control: every CONTROL_MS a selection-disable for a dedicated control user via sessions.ingest_lazy_clear (a real
 * control entrypoint: ingest_lock_fences, then the seat clock FOR UPDATE, exactly the clock lock control_changed takes;
 * it changes no data here), as the owner with lock_timeout 2s; its wait and failures are recorded. Not the full
 * ordered-control path (which needs the frozen privacy authority).
 * Lock attribution: a sampler every SAMPLE_MS records each ungranted lock in the cell's database: waiter stage,
 * locktype, mode, relation or advisory key (mapped to the 50000 barrier, the native-origin and origin-claim keys of
 * each worker, or 'unmapped'), and its blockers' stages. transactionid waits are labelled 'attribution unknown' (the
 * row behind them is not recorded). Counts are samples, not distinct waits.
 * Space guard: before and during every cell, filesystem free space (statfs of the scratch path) and the cell's
 * database size and the cluster's WAL size are sampled; below STOP_FREE_BYTES the cell stops, drains, and drops its
 * database. Peak sizes are reported.
 * Reported per cell: attempted records and batches, committed rows (ACK decisions written, counted after the drain),
 * durable records/s over the measured duration, failures by stage and SQLSTATE, latency percentiles of successful
 * deliveries WITH the count behind each tail quantile, control wait percentiles and failures, lock-sample tables.
 * What it does NOT show: production transaction lengths, concurrency or tenant mix; network, Hyperdrive or fsync
 * cost (local macOS: fsync does not force stable media). Do not extrapolate shares of production time from it.
 *
 * Usage: SESSION_INGEST_TEST_PG_URL=<an owned, throwaway loopback fixture> bun benchmarks/lazy-ack-t2-flip/matched-bench.ts '<plan json>'
 *   plan = { c?, seconds?, controlMs?, sampleMs?, stopFreeBytes?, scratchPath?, cells: [{ arm: "main"|"t2", n }] }
 */
import pg from "pg";
import { isIP } from "node:net";
import { statfsSync } from "node:fs";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (!url) throw Error("SESSION_INGEST_TEST_PG_URL (an owned loopback fixture) is required");
// Loopback IP literals only (no names), no password.
const loopback = (h: string) => { const x = h.replace(/^\[|\]$/g, ""); return isIP(x) === 4 ? x.split(".")[0] === "127" : isIP(x) === 6 && x === "::1"; };
{ const u = new URL(url); if (!loopback(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); }
type Cell = { arm: "main" | "t2"; n: number };
const plan = JSON.parse(process.argv[2] ?? "{}") as { c?: number; seconds?: number; controlMs?: number; sampleMs?: number; stopFreeBytes?: number; scratchPath?: string; cells: Cell[] };
const C = plan.c ?? 8, SECONDS = plan.seconds ?? 20, CONTROL_MS = plan.controlMs ?? 500, SAMPLE_MS = plan.sampleMs ?? 50;
const STOP_FREE = plan.stopFreeBytes ?? 1_200_000_000, SCRATCH_PATH = plan.scratchPath ?? "/private/tmp";
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); const i = Math.min(s.length - 1, Math.floor(p * s.length)); return { v: Math.round(s[i]! * 10) / 10, atOrAbove: s.length - i }; };
const freeBytes = () => { const s = statfsSync(SCRATCH_PATH); return Number(s.bavail) * Number(s.bsize); };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const admin = new pg.Client({ connectionString: url }); await admin.connect();
const walBytes = async () => Number((await admin.query("SELECT coalesce(sum(size),0)::bigint b FROM pg_ls_waldir()")).rows[0].b);
const rows: unknown[] = [];

async function runCell(cell: Cell) {
  const name = "t2mb_" + crypto.randomUUID().replaceAll("-", "");
  const free0 = freeBytes();
  if (free0 < STOP_FREE * 1.5) return { cell, skipped: `free space ${free0} below 1.5x the stop threshold ${STOP_FREE}` };
  await admin.query("CREATE DATABASE " + name);
  const u = new URL(url!); u.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: u.href, max: C * 2 + 8 });
  pool.on("error", () => {});
  const scratch = scratchStore();
  const space = { minFree: free0, peakDb: 0, peakWal: await walBytes(), stoppedForSpace: false };
  try {
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    { const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); } }
    const generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await pool.query("SELECT sessions.ingest_history_set_live(true)");
    const historyState = (await pool.query("SELECT (SELECT live FROM sessions.ingest_history_switch) live,(SELECT count(*)::int FROM sessions.ingest_history_scope_current) managed")).rows[0];
    // Per-call transaction as the runtime role; the stage label goes into application_name for the sampler.
    const stageOf = (sql: string) => sql.includes("claim") ? "claim" : sql.includes("prepare") ? "prepare" : sql.includes("commit") ? "commit" : "other";
    const ledgerFor = (w: number, onFail: (stage: string, code: string) => void) => sqlLedger(async (sql, params) => {
      const c = await pool.connect();
      const stage = stageOf(sql);
      try {
        await c.query(`BEGIN;SET LOCAL application_name='w${w}-${stage}';SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest`);
        const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never;
      } catch (e: any) { onFail(stage, String(e?.code ?? "error")); await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
    }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
    async function custody(t: any, raw: any) {
      const p = await prepareDelivery(t, raw);
      const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
        originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
        originalRecordCount: raw.records.length, candidateCount: p.records.length,
        records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
          revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
        privacy: { complete: true, deletions: [], resets: [] } };
      return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
    }
    let serial = 0;
    const tenantOf = (w: number) => ({ ...TENANT, userId: `mb-u${w}` }), originOf = (w: number) => `mb-o${w}`;
    const deliver = async (w: number, n: number, ledger: ReturnType<typeof ledgerFor>) => {
      const t = tenantOf(w), origin = originOf(w), s = ++serial;
      const raw = await batch(Array.from({ length: n }, (_, i) => record({ record_id: `mb-${s}-${i}`, revision: 2, session_id: `mb-s${s}-${i % 7}`, origin_id: origin })),
        { batch_id: `mb-b${s}`, account_id: await deliveryAccount(t), origin_id: origin });
      return deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw);
    };
    const noFail = () => {};
    for (let w = 0; w < C; w++) await deliver(w, 1, ledgerFor(w, noFail)); // warm: first-seen adoption per user/origin
    // Control user: one warm delivery so its fence and seal exist.
    const CTL = C;
    await deliver(CTL, 1, ledgerFor(CTL, noFail));
    if (cell.arm === "t2") { await pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await pool.query("SELECT sessions.lazy_ack_t2_set_mode('t2')"); }
    const gate = (await pool.query("SELECT (SELECT c FROM unnest(proconfig) c WHERE c LIKE 'bulls96.lazy\\_ack\\_t2=%') g FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure")).rows[0].g;
    const mode = (await pool.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode;
    // Advisory key names: the barrier, and each worker's native-origin and origin-claim keys (bigint keys appear in
    // pg_locks as classid = high 32 bits, objid = low 32 bits, objsubid 1).
    const keyNames = new Map<string, string>([["961996/50000/2", "barrier(961996,50000)"], ["961996/1/2", "migrator(961996,1)"]]);
    for (let w = 0; w <= C; w++) {
      const r = (await pool.query(`SELECT k1,k2 FROM (SELECT hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0) k1,
        hashtextextended(jsonb_build_array('96bulls:ingest-origin-claim-v1',$1::text,$2::text,$4::text,$3::text)::text,0) k2) x`, [TENANT.orgId, TENANT.workspaceId, originOf(w), tenantOf(w).userId])).rows[0];
      for (const [k, label] of [[r.k1, "native-origin"], [r.k2, "origin-claim"]] as const) {
        const b = BigInt.asUintN(64, BigInt(k));
        keyNames.set(`${Number(b >> 32n)}/${Number(b & 0xffffffffn)}/1`, `${label}(w${w})`);
      }
    }
    const baseRows = Number((await pool.query("SELECT count(*)::bigint n FROM sessions.ingest_lazy_ack_decisions")).rows[0].n);
    // ---- measured window ----
    const lat: number[] = [], failures: Record<string, number> = {}, ctlWait: number[] = [], ctlFail: Record<string, number> = {};
    const ctlAttempts: Array<{ startMs: number; endMs: number; outcome: string; afterWindow: boolean }> = [];
    let ctlTimeouts: { statement_timeout: string; lock_timeout: string } | null = null;
    let okBatches = 0, attemptedBatches = 0, attemptedRecords = 0;
    const samples = new Map<string, number>(); let sampleTicks = 0;
    const t0 = performance.now(), end = Date.now() + SECONDS * 1000;
    let stop = false;
    const stopNow = () => Date.now() >= end || stop;
    const worker = async (w: number) => {
      const ledger = ledgerFor(w, (stage, code) => { failures[`${stage}:${code}`] = (failures[`${stage}:${code}`] ?? 0) + 1; });
      while (!stopNow()) {
        attemptedBatches++; attemptedRecords += cell.n;
        const s = performance.now();
        try { const o = await deliver(w, cell.n, ledger); if (o.outcome === "accepted_buffered") { okBatches++; lat.push(performance.now() - s); } else failures[`outcome:${o.outcome}`] = (failures[`outcome:${o.outcome}`] ?? 0) + 1; }
        catch { /* counted by stage in the ledger wrapper */ }
      }
    };
    const control = async () => {
      const body = JSON.stringify({ tenant: tenantOf(CTL), originId: originOf(CTL), enabled: false, sessions: [["claude", "mb-control-none"]] });
      while (!stopNow()) {
        await sleep(CONTROL_MS);
        if (stopNow()) break;
        const c = await pool.connect();
        const s = performance.now();
        let outcome = "ok";
        try {
          await c.query("BEGIN;SET LOCAL application_name='control-lazy_clear';SET LOCAL lock_timeout='2s'");
          if (!ctlTimeouts) ctlTimeouts = (await c.query("SELECT current_setting('statement_timeout') statement_timeout,current_setting('lock_timeout') lock_timeout")).rows[0];
          await c.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'selection')", [body]); await c.query("COMMIT"); ctlWait.push(performance.now() - s);
        }
        catch (e: any) { outcome = String(e?.code ?? "error"); ctlFail[outcome] = (ctlFail[outcome] ?? 0) + 1; await c.query("ROLLBACK").catch(() => {}); }
        finally {
          c.release();
          const e = performance.now();
          ctlAttempts.push({ startMs: Math.round(s - t0), endMs: Math.round(e - t0), outcome, afterWindow: e - t0 > SECONDS * 1000 });
        }
      }
    };
    const sampler = async () => {
      const s = await pool.connect();
      try {
        await s.query("SET application_name='mb-sampler'");
        while (!stopNow()) {
          sampleTicks++;
          const r = (await s.query(`SELECT w.application_name app,l.locktype,l.mode,l.relation::regclass::text rel,l.classid,l.objid,l.objsubid,
              (SELECT coalesce(array_agg(DISTINCT coalesce(nullif(b.application_name,''),'?')),'{}') FROM pg_stat_activity b WHERE b.pid=ANY(pg_blocking_pids(w.pid))) blockers
            FROM pg_stat_activity w JOIN pg_locks l ON l.pid=w.pid AND NOT l.granted WHERE w.datname=current_database()`)).rows;
          for (const x of r) {
            const stage = String(x.app).replace(/^w\d+-/, "");
            const obj = x.locktype === "advisory" ? (keyNames.get(`${x.classid}/${x.objid}/${x.objsubid}`) ?? `unmapped(${x.classid}/${x.objid}/${x.objsubid})`).replace(/\(w\d+\)/, "")
              : x.locktype === "transactionid" ? "attribution unknown" : x.locktype === "tuple" || x.locktype === "relation" ? String(x.rel) : String(x.locktype);
            const blockers = (x.blockers as string[]).map((b) => b.replace(/^w\d+-/, "")).sort().join("+") || "-";
            const k = `${stage} | ${x.locktype} ${x.mode} | ${obj} | blocked by ${blockers}`;
            samples.set(k, (samples.get(k) ?? 0) + 1);
          }
          const f = freeBytes(); space.minFree = Math.min(space.minFree, f);
          if (sampleTicks % 20 === 0) {
            space.peakDb = Math.max(space.peakDb, Number((await s.query("SELECT pg_database_size(current_database())::bigint b")).rows[0].b));
            space.peakWal = Math.max(space.peakWal, await walBytes());
          }
          if (f < STOP_FREE) { space.stoppedForSpace = true; stop = true; }
          await sleep(SAMPLE_MS);
        }
      } finally { s.release(); }
    };
    let drainEndMs = 0;
    await Promise.all([...Array.from({ length: C }, (_, w) => worker(w).then(() => { drainEndMs = Math.max(drainEndMs, performance.now() - t0); })), control(), sampler()]);
    const measuredMs = performance.now() - t0;
    const committedRows = Number((await pool.query("SELECT count(*)::bigint n FROM sessions.ingest_lazy_ack_decisions")).rows[0].n) - baseRows;
    space.peakDb = Math.max(space.peakDb, Number((await pool.query("SELECT pg_database_size(current_database())::bigint b")).rows[0].b));
    return {
      cell, c: C, seconds: SECONDS, measuredMs: Math.round(measuredMs), history: historyState, gate, mode,
      attempted: { batches: attemptedBatches, records: attemptedRecords }, ok: { batches: okBatches }, committedRows,
      durableRecordsPerSec: Math.round(committedRows / (measuredMs / 1000)),
      failures,
      latencyMs: { n: lat.length, p50: pct(lat, 0.5), p95: pct(lat, 0.95), p99: pct(lat, 0.99), max: pct(lat, 1) },
      window: { windowMs: SECONDS * 1000, workersDrainedAtMs: Math.round(drainEndMs) },
      controlAttempts: ctlAttempts, controlTimeouts: ctlTimeouts,
      control: { entrypoint: "sessions.ingest_lazy_clear(selection disable, control user)", n: ctlWait.length, p50: pct(ctlWait, 0.5), p95: pct(ctlWait, 0.95), max: pct(ctlWait, 1), failures: ctlFail },
      lockSamples: { ticks: sampleTicks, sampleMs: SAMPLE_MS, top: [...samples.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25) },
      space: { freeStart: free0, minFree: space.minFree, peakDbBytes: space.peakDb, peakWalBytes: space.peakWal, stoppedForSpace: space.stoppedForSpace },
    };
  } finally {
    await pool.end().catch(() => {}); scratch.done();
    await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)");
  }
}

try {
  for (const cell of plan.cells) {
    const r = await runCell(cell);
    console.log(JSON.stringify(r));
    rows.push(r);
    if ((r as any).space?.stoppedForSpace || (r as any).skipped) { console.log(JSON.stringify({ stopped: "space guard", after: cell })); break; }
  }
} finally { await admin.end(); }
if (process.env.RCF_OUT) await Bun.write(process.env.RCF_OUT, JSON.stringify({ host: "local loopback PostgreSQL 16 (throwaway)", plan, rows }, null, 1));
