/**
 * 062210 try-lock flip under sustained ingest load, on a THROWAWAY loopback PostgreSQL. Synthetic data only; the script
 * creates and drops its own database. Never point it at a real database (it refuses a non-loopback URL or a password).
 *
 * Load: C workers, each a distinct user + origin, deliver fresh batches of N records back to back through the real
 * upload path (deliverBatch: claim, prepare, commit+ACK), every ledger call its own autocommit transaction as the
 * runtime role with SET LOCAL lock_timeout 2s / statement_timeout 5s (the Worker's limits). Every one of those
 * transactions takes (961996,50000) shared, so the barrier is free only between them.
 * Flipper: gate 'fenced'; every GAP ms the operator helper (lazy-ack-t2-flip.ts: one autocommit statement per attempt,
 * jittered backoff) tries to toggle the mode within BUDGET ms. Control cells run the same load with no flipper.
 *
 * Reports per cell: flip outcomes (switched / no_switch), attempts and time to success; uploader deliveries, failures
 * by code and latency percentiles. What it does NOT show: production transaction lengths (network, fsync, Hyperdrive),
 * production concurrency, or client retry behaviour. Local macOS: fsync does not force stable media.
 *
 * Usage: SESSION_INGEST_TEST_PG_URL=<an owned, throwaway loopback fixture> bun benchmarks/lazy-ack-t2-flip/flip-bench.ts '<json plan>'
 *   plan = { seconds?, c?, gapMs?, cells: [{ n, budgetMs?: number|null, backoffMs?, maxBackoffMs? }] }  (budgetMs null = control)
 */
import pg from "pg";
import { isIP } from "node:net";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";
import { lazyAckT2FlipOutcome as flipLazyAckT2Mode, type FlipMode, type FlipResult } from "../../src/session-ingest/lazy-ack-t2-flip.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (!url) throw Error("SESSION_INGEST_TEST_PG_URL (an owned loopback fixture) is required");
// Loopback IP literals only (no names), no password.
const loopback = (h: string) => { const x = h.replace(/^\[|\]$/g, ""); return isIP(x) === 4 ? x.split(".")[0] === "127" : isIP(x) === 6 && x === "::1"; };
{ const u = new URL(url); if (!loopback(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); }
type Cell = { n: number; budgetMs?: number | null; backoffMs?: number; maxBackoffMs?: number };
const plan = JSON.parse(process.argv[2] ?? "{}") as { seconds?: number; c?: number; gapMs?: number; cells: Cell[] };
const SECONDS = plan.seconds ?? 30, C = plan.c ?? 8, GAP = plan.gapMs ?? 1000;
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return Math.round(s[Math.min(s.length - 1, Math.floor(p * s.length))]! * 10) / 10; };

const admin = new pg.Client({ connectionString: url }); await admin.connect();
const name = "t2flip_" + crypto.randomUUID().replaceAll("-", "");
await admin.query("CREATE DATABASE " + name);
const u = new URL(url); u.pathname = "/" + name;
const pool = new pg.Pool({ connectionString: u.href, max: C * 2 + 6 });
const scratch = scratchStore();
const out: unknown[] = [];
try {
  await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
  { const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); } }
  const generation = (await derivedSchemaAssets()).expected.generation;
  await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
    privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
  await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
    capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
    allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
  const ledger = sqlLedger(async (sql, params) => {
    const c = await pool.connect();
    try { await c.query("BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
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
    return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
  }
  let serial = 0;
  const deliver = async (w: number, n: number) => {
    const t = { ...TENANT, userId: `fb-u${w}` }, origin = `fb-o${w}`, s = ++serial;
    const raw = await batch(Array.from({ length: n }, (_, i) => record({ record_id: `fb-${s}-${i}`, revision: 2, session_id: `fb-s${s}-${i % 7}`, origin_id: origin })),
      { batch_id: `fb-b${s}`, account_id: await deliveryAccount(t), origin_id: origin });
    return deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw);
  };
  for (let w = 0; w < C; w++) await deliver(w, 1); // warm: first-seen adoption per user/origin
  await pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_t2='fenced'`);
  for (const cell of plan.cells) {
    const mode0 = (await pool.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode as FlipMode;
    const end = Date.now() + SECONDS * 1000;
    const lat: number[] = [], fails: Record<string, number> = {};
    let ok = 0;
    const worker = async (w: number) => {
      while (Date.now() < end) {
        const t0 = performance.now();
        try { const o = await deliver(w, cell.n); if (o.outcome === "accepted_buffered") { ok++; lat.push(performance.now() - t0); } else fails[o.outcome] = (fails[o.outcome] ?? 0) + 1; }
        catch (e: any) { const k = String(e?.code ?? e?.cause?.code ?? e?.name ?? "error"); fails[k] = (fails[k] ?? 0) + 1; }
      }
    };
    const flips: FlipResult[] = [];
    const flipper = async () => {
      if (cell.budgetMs === null || cell.budgetMs === undefined) return;
      let to: FlipMode = mode0 === "main" ? "t2" : "main";
      while (Date.now() + GAP < end) {
        await new Promise((r) => setTimeout(r, GAP));
        const r = await flipLazyAckT2Mode((sql, p) => pool.query(sql, p as never[]) as never, to,
          { budgetMs: cell.budgetMs!, ...(cell.backoffMs ? { backoffMs: cell.backoffMs } : {}), ...(cell.maxBackoffMs ? { maxBackoffMs: cell.maxBackoffMs } : {}) });
        flips.push(r);
        if (r.result === "switched") to = to === "main" ? "t2" : "main";
      }
    };
    await Promise.all([...Array.from({ length: C }, (_, w) => worker(w)), flipper()]);
    const sw = flips.filter((f) => f.result === "switched"), ns = flips.filter((f) => f.result === "no_switch");
    const row = {
      cell, c: C, seconds: SECONDS, gapMs: GAP,
      uploads: { ok, recordsPerSec: Math.round(ok * cell.n / SECONDS), fails, latencyMs: { p50: pct(lat, 0.5), p95: pct(lat, 0.95), p99: pct(lat, 0.99), max: pct(lat, 1) } },
      flips: cell.budgetMs == null ? null : {
        tries: flips.length, switched: sw.length, noSwitch: ns.length,
        attemptsToSuccess: { p50: pct(sw.map((f) => f.attempts), 0.5), p95: pct(sw.map((f) => f.attempts), 0.95), max: pct(sw.map((f) => f.attempts), 1) },
        msToSuccess: { p50: pct(sw.map((f) => f.elapsedMs), 0.5), p95: pct(sw.map((f) => f.elapsedMs), 0.95), max: pct(sw.map((f) => f.elapsedMs), 1) },
        attemptsPerNoSwitch: pct(ns.map((f) => f.attempts), 0.5),
      },
    };
    console.log(JSON.stringify(row));
    out.push(row);
  }
} finally {
  await pool.end(); scratch.done();
  await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end();
}
if (process.env.RCF_OUT) await Bun.write(process.env.RCF_OUT, JSON.stringify({ host: "local loopback PostgreSQL 16 (throwaway)", rows: out }, null, 1));
