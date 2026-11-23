/**
 * Seat clock hold attribution, on a THROWAWAY loopback PostgreSQL that the operator owns. Synthetic data only.
 * Adapted from benchmarks/seat-clock-contention/bench.ts at 1befee7a (same URL refusals, same schema build, same upload
 * path, same W1/W2 streams, same 2 s / 5 s limits as bulls96_session_ingest). Differences, all bench-only:
 *   - the lock sampler is replaced by server-side probes inside a probed copy of sessions.ingest_lazy_ack_batch (probe.ts),
 *     installed into the throwaway database after the migrations; nothing under supabase/ or src/ changes;
 *   - cell.body selects the base body with the T2 gate off ("main") or the T2 gate fenced and mode t2 ("t2"), set with
 *     the shipped owner setters on the template before any load;
 *   - controls default to none (attribution of the upload path alone).
 *
 * Measured span per successful commit (HOLD): from the server stamp right after the statement that took the clock in an
 * exclusive mode returned (main: the clock SELECT ... FOR NO KEY UPDATE; t2: the upgrade SELECT ... FOR NO KEY UPDATE)
 * to the client's COMMIT completion. Server stamps are moved to the client clock with a per-transaction offset (one
 * server clock reading bracketed by client stamps, after COMMIT). The hold is split into:
 *   in-ACK steps after the lock (probe deltas), post-ACK (ACK end -> commit statement returned to the client: the rest of
 *   ingest_commit_buffered_batch, the later AFTER triggers, origin-claim leave, result transfer), COMMIT round trip.
 * CLOCK WAIT = the clock-taking statement's own duration (main: clock_lock; t2: t2_upgrade).
 *
 * Usage: SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres \
 *   bun benchmarks/seat-clock-attribution/attribution.ts '<plan json>'
 *   plan = { runId?, totalRecords?, maxSeconds?, threads?, historyLive?, cells: [{ workload, n, c, body: "main"|"t2", label? }] }
 */
import pg from "pg";
import { isIP } from "node:net";
import { mkdirSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join } from "node:path";
import { TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";
import type { LoadInit, Stream, Txn } from "./load-worker.ts";
import { instrument, instrumentWrapper, STEPS } from "./probe.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (!url) throw Error("SESSION_INGEST_TEST_PG_URL (an owned loopback fixture) is required");
const loopback = (h: string) => { const x = h.replace(/^\[|\]$/g, ""); return isIP(x) === 4 ? x.split(".")[0] === "127" : isIP(x) === 6 && x === "::1"; };
{
  const u = new URL(url);
  if (!["postgres:", "postgresql:"].includes(u.protocol) || !loopback(u.hostname) || u.password || u.search || u.hash) throw Error("loopback throwaway fixture only");
  for (const v of ["PGHOST", "PGHOSTADDR", "PGPASSWORD", "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE"]) if (process.env[v] !== undefined) throw Error(`refusing to start with ${v} set`);
}
const SAFE_ID = /^[A-Za-z0-9._-]{1,80}$/;
type Workload = "W1" | "W2";
type Body = "main" | "t2" | "main-plain" | "t2-plain";
type Cell = { workload: Workload; n: number; c: number; body: Body; label?: string };
type Plan = { runId?: string; threads?: number; totalRecords?: number; maxSeconds?: number; historyLive?: boolean; cells: Cell[] };
const plan = JSON.parse(process.argv[2] ?? "{}") as Plan;
const RUN_ID = plan.runId ?? new Date().toISOString().replace(/[:.]/g, "-");
if (!SAFE_ID.test(RUN_ID) || RUN_ID.startsWith(".")) throw Error("runId: letters, digits, dot, dash, underscore only");
for (const c of plan.cells ?? []) { if (c.label !== undefined && (!SAFE_ID.test(c.label) || c.label.startsWith("."))) throw Error("bad label"); if (!["main", "t2", "main-plain", "t2-plain"].includes(c.body)) throw Error("cell.body: main | t2 | main-plain | t2-plain"); }
const TOTAL = plan.totalRecords ?? 64000, MAX_S = plan.maxSeconds ?? 300, THREADS = plan.threads ?? 8;
const OUT = join(import.meta.dir, "results", RUN_ID);
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const now = () => performance.timeOrigin + performance.now();
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return Math.round(s[Math.min(s.length - 1, Math.floor(p * s.length))]! * 100) / 100; };
const dist = (xs: number[]) => ({ n: xs.length, p50: pct(xs, 0.5), p95: pct(xs, 0.95), max: pct(xs, 1), mean: xs.length ? Math.round(xs.reduce((a, b) => a + b, 0) / xs.length * 100) / 100 : null });

mkdirSync(OUT, { recursive: true });
const admin = new pg.Client({ connectionString: url }); await admin.connect();
const serverVersion = (await admin.query("SELECT version() v")).rows[0].v as string;
const settings = (await admin.query("SELECT name,setting,unit FROM pg_settings WHERE name=ANY($1) ORDER BY name",
  [["max_connections", "shared_buffers", "fsync", "synchronous_commit", "wal_level", "default_transaction_isolation", "track_functions"]])).rows;
const dbUrl = (db: string) => { const u = new URL(url); u.pathname = "/" + db; return u.href; };
const created: string[] = [];
let generation = "";
const templates: Partial<Record<Body | "base", string>> = {};

async function buildTemplate() {
  const tpl = `scf_tpl_main_` + crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  await admin.query("CREATE DATABASE " + tpl); created.push(tpl);
  const c = new pg.Client({ connectionString: dbUrl(tpl) }); await c.connect();
  try {
    await c.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    await runMigrations(c as never, { profile });
    generation = (await derivedSchemaAssets()).expected.generation;
    await c.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 12 * 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await c.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    if (plan.historyLive !== undefined) await c.query("UPDATE sessions.ingest_history_switch SET live=$1 WHERE singleton", [plan.historyLive]);
    const live = (await c.query("SELECT live FROM sessions.ingest_history_switch WHERE singleton")).rows[0]?.live;
    templates.base = tpl;
    return { historyLive: live };
  } finally { await c.end(); }
}
// The roles are cluster-wide, so the migrator runs once (base). Each body template is a copy: "main" and "t2" get the
// probed function; "t2" and "t2-plain" also get the gate fenced and mode t2 by the shipped owner setters (no other
// transaction is open on the copy, so their try-lock succeeds). "main-plain" / "t2-plain" are unprobed (overhead check).
async function derive(body: Body) {
  const tpl = `scf_tpl_${body.replace("-", "_")}_` + crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  await admin.query(`CREATE DATABASE ${tpl} TEMPLATE ${templates.base}`); created.push(tpl);
  const c = new pg.Client({ connectionString: dbUrl(tpl) }); await c.connect();
  try {
    if (body.startsWith("t2")) { await c.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await c.query("SELECT sessions.lazy_ack_t2_set_mode('t2')"); }
    if (!body.endsWith("-plain")) {
      const def = (await c.query("SELECT pg_get_functiondef('sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure) d")).rows[0].d as string;
      await c.query(instrument(def));
      const w = (await c.query("SELECT pg_get_functiondef('sessions.ingest_origin_commit_buffered_batch(jsonb)'::regprocedure) d")).rows[0].d as string;
      await c.query(instrumentWrapper(w));
    }
    templates[body] = tpl;
    return { body, ...(await c.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows[0] };
  } finally { await c.end(); }
}

type Tenant = typeof TENANT;
const onMsg = <T>(w: Worker, kind: string) => new Promise<T>((res, rej) => {
  const f = (ev: MessageEvent) => { if ((ev.data as any).kind === kind) { w.removeEventListener("message", f); res(ev.data as T); } };
  w.addEventListener("message", f); w.addEventListener("error", (e) => rej(e), { once: true });
});

async function runCell(cell: Cell, index: number) {
  const db = `scf_run_${index}_` + crypto.randomUUID().replaceAll("-", "").slice(0, 8);
  await admin.query(`CREATE DATABASE ${db} TEMPLATE ${templates[cell.body]}`); created.push(db);
  const pool = new pg.Pool({ connectionString: dbUrl(db), max: 2 });
  const threads: Worker[] = [];
  try {
    const orgs = ["org-a", "org-b", "org-c", "org-d"];
    const tenantOf = (k: number): Tenant => ({ orgId: orgs[k % orgs.length]!, workspaceId: TENANT.workspaceId, userId: `scb-u${k}` });
    const batches = Math.ceil(TOTAL / cell.n);
    const T = Math.min(THREADS, cell.c);
    const per: Stream[][] = Array.from({ length: T }, () => []);
    let wk = 0;
    if (cell.workload === "W1") {
      const share = Math.floor(batches / cell.c), extra = batches - share * cell.c;
      for (let w = 0; w < cell.c; w++) per[w % T]!.push({ t: tenantOf(w), origin: `scb-o${w}`, workers: 1, batches: share + (w < extra ? 1 : 0), firstWorker: wk++ });
    } else {
      const hot = Math.max(1, Math.floor(cell.c / 2)), tail = Math.max(1, cell.c - hot);
      const hotBatches = Math.round(batches / 2), tailBatches = batches - hotBatches;
      const ht = Math.min(T, hot);
      for (let i = 0; i < ht; i++) {
        const w = Math.floor(hot / ht) + (i < hot % ht ? 1 : 0), b = Math.floor(hotBatches / ht) + (i < hotBatches % ht ? 1 : 0);
        per[i]!.push({ t: tenantOf(0), origin: "scb-hot", workers: w, batches: b, firstWorker: wk }); wk += w;
      }
      const share = Math.floor(tailBatches / tail), extra = tailBatches - share * tail;
      for (let k = 0; k < tail; k++) per[(k + ht) % T]!.push({ t: tenantOf(k + 1), origin: `scb-o${k + 1}`, workers: 1, batches: share + (k < extra ? 1 : 0), firstWorker: wk++ });
    }
    for (let i = 0; i < T; i++) {
      const w = new Worker(new URL("./load-worker.ts", import.meta.url).href); threads.push(w);
      w.postMessage({ kind: "init", init: { dbUrl: dbUrl(db), generation, originClaimFence: true, n: cell.n, streams: per[i]!, prefix: `scf-${index}-t${i}`, maxSeconds: MAX_S, epoch, identity } satisfies LoadInit });
      await onMsg(w, "ready");
    }
    const results = threads.map((w) => onMsg<any>(w, "done"));
    for (const w of threads) w.postMessage({ kind: "go" });
    const done = await Promise.all(results);
    const txns: Txn[] = done.flatMap((d) => d.txns);
    const start = Math.min(...done.map((d) => d.start)), lastDone = Math.max(...done.map((d) => d.lastDone));
    const durable = done.reduce((a, d) => a + d.durable, 0);
    const undrained = done.reduce((a, d) => a + d.undrained, 0);
    const nBatches = done.reduce((a, d) => a + d.batches, 0), tries = done.reduce((a, d) => a + d.tries, 0);
    const failures: Record<string, number> = {};
    for (const t of txns) if (!t.ok) { const k = `${t.stage}:${t.code}`; failures[k] = (failures[k] ?? 0) + 1; }
    const traced = txns.filter((t) => t.stage === "commit" && t.ok && t.trace && t.trace.tclock != null && t.wrap);
    const untraced = txns.filter((t) => t.stage === "commit" && t.ok && !t.trace).length;
    // Per traced commit, ms. Server stamps (us) -> client ms: (stamp - offset) / 1000.
    const rows = traced.map((t) => {
      const tr = t.trace, off = t.offsetUs!;
      const toClient = (us: number) => (us - off) / 1000;
      const acc = (tr.acc as number[]).map((x) => x / 1000);
      const lockStep = tr.t2 ? STEPS.indexOf("t2_upgrade") : STEPS.indexOf("clock_lock");
      const holdStart = toClient(tr.tclock), ackEnd = toClient(tr.tend);
      // in-ACK steps after the exclusive lock: every step recorded after the lock statement in body order.
      const after = tr.t2 ? ["t2_C2_number", "t2_D_seats", "t2_D_slots", "t2_D_decisions", "t2_E_count", "last_seat_update", "final_check"]
        : ["origin_lock", "stage_row", "attempt_check1", "attempt_row", "loop_fetch", "loop_key", "loop_slot_lock", "loop_checks", "loop_seat", "loop_slot_update", "loop_decision_insert", "loop_decision_check", "last_seat_update", "final_check"];
      const steps: Record<string, number> = Object.fromEntries(STEPS.map((s, i) => [s, acc[i]!]));
      return {
        hold: t.t1 - holdStart, wait: acc[lockStep]!, inAck: ackEnd - holdStart, postAck: t.ts! - ackEnd, commitRtt: t.t1 - t.ts!,
        preLock: holdStart - toClient(tr.t0) - acc[lockStep]!, steps, afterLock: after, n: tr.n, rtt: t.rttUs! / 1000,
        statement: t.ts! - t.tb!,
        // Whole commit statement as the caller sees it (tb -> ts), split on the server stamps.
        toWrapper: toClient(t.wrap.w0) - t.tb!, originClaimEnter: (t.wrap.w1 - t.wrap.w0) / 1000, commitBeforeAck: (tr.t0 - t.wrap.w1) / 1000,
        ackBeforeLock: holdStart - toClient(tr.t0) - acc[lockStep]!, nativeOriginLock: steps.origin_lock!, afterAck: (t.wrap.w2 - tr.tend) / 1000, toClientReturn: t.ts! - toClient(t.wrap.w2),
      };
    });
    const stepStats = Object.fromEntries(STEPS.map((s) => [s, dist(rows.map((r) => r.steps[s]!))]).filter(([, d]: any) => d.mean > 0));
    const seconds = (lastDone - start) / 1000;
    const result = {
      cell, threads: T, drained: undrained === 0, drainSeconds: Math.round(seconds * 100) / 100,
      durableRecords: durable, durableRecordsPerSec: Math.round(durable / seconds), retriesPerBatch: Math.round((tries - nBatches) / nBatches * 100) / 100,
      stageFailures: failures, commitsOk: txns.filter((t) => t.stage === "commit" && t.ok).length, traced: traced.length, untraced,
      holdMs: dist(rows.map((r) => r.hold)), clockWaitMs: dist(rows.map((r) => r.wait)), inAckAfterLockMs: dist(rows.map((r) => r.inAck)),
      postAckMs: dist(rows.map((r) => r.postAck)), commitRttMs: dist(rows.map((r) => r.commitRtt)), preLockMs: dist(rows.map((r) => r.preLock)),
      commitStatementMs: dist(rows.map((r) => r.statement)),
      // Client clock, every successful commit (traced or not; the only hold-side figure for the unprobed bodies).
      commitStatementAllMs: dist(txns.filter((t) => t.stage === "commit" && t.ok).map((t) => t.ts! - t.tb!)),
      commitWholeAllMs: dist(txns.filter((t) => t.stage === "commit" && t.ok).map((t) => t.t1 - t.t0)), offsetProbeRttMs: dist(rows.map((r) => r.rtt)),
      split: Object.fromEntries(["toWrapper", "originClaimEnter", "commitBeforeAck", "ackBeforeLock", "wait", "inAck", "afterAck", "toClientReturn", "commitRtt", "nativeOriginLock"].map((k) => [k, dist(rows.map((r: any) => r[k]))])),
      // Every commit ATTEMPT (ok or failed), client clock: statement time against the 2 s lock_timeout and 5 s statement_timeout.
      commitAttempts: (() => { const xs = txns.filter((t) => t.stage === "commit"); const st = (t: Txn) => (t.ts ?? t.t1) - (t.tb ?? t.t0);
        return { n: xs.length, over2s: xs.filter((t) => st(t) >= 2000).length, over5s: xs.filter((t) => st(t) >= 4990).length, failed: xs.filter((t) => !t.ok).length,
          statementMsOkAndFailed: dist(xs.map(st)) }; })(),
      afterLockSteps: rows[0]?.afterLock ?? [], stepMs: stepStats,
      lastSeat: (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock")).rows[0].s,
      seats: (await pool.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seats")).rows[0].n,
      decisions: (await pool.query("SELECT count(*)::int n FROM sessions.ingest_lazy_ack_decisions")).rows[0].n,
    };
    const tag = `${index}-${cell.body}-${cell.workload}-n${cell.n}-c${cell.c}${cell.label ? "-" + cell.label : ""}`;
    writeFileSync(join(OUT, `txns-${tag}.jsonl.gz`), gzipSync(txns.map((t) => JSON.stringify(t)).join("\n")));
    return result;
  } finally {
    for (const w of threads) w.terminate();
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS ${db} WITH(FORCE)`); created.splice(created.indexOf(db), 1);
  }
}

const out: unknown[] = [];
let meta: Record<string, unknown> = {};
try {
  const tpls: unknown[] = [await buildTemplate()];
  for (const b of [...new Set(plan.cells.map((c) => c.body))]) tpls.push(await derive(b));
  meta = { runId: RUN_ID, serverVersion, settings, templates: tpls, plan: { ...plan, totalRecords: TOTAL, maxSeconds: MAX_S }, client: { bun: Bun.version, cpus: navigator.hardwareConcurrency }, steps: STEPS };
  console.log(JSON.stringify({ meta }));
  for (const [i, cell] of plan.cells.entries()) {
    const r = await runCell(cell, i); out.push(r); console.log(JSON.stringify(r));
    writeFileSync(join(OUT, "summary.json"), JSON.stringify({ meta, rows: out }, null, 1));
  }
} finally {
  for (const db of [...created].reverse()) await admin.query(`DROP DATABASE IF EXISTS ${db} WITH(FORCE)`).catch(() => {});
  await admin.end();
}
