/**
 * Load thread of bench.ts (a Bun Worker). Owns a share of the streams: builds its backlog, waits for "go", delivers through
 * the real upload path (deliverBatch -> sqlLedger), one transaction per ledger call as bulls96_session_ingest with the
 * role's limits, and posts every transaction back. Synthetic data only; the database is the throwaway copy bench.ts made.
 */
import pg from "pg";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record, scratchStore } from "../../src/session-ingest/__tests__/support.ts";

declare const self: Worker;
type Tenant = { orgId: string; workspaceId: string; userId: string };
export type Stream = { t: Tenant; origin: string; workers: number; batches: number; firstWorker: number };
export type LoadInit = { dbUrl: string; generation: string; originClaimFence: boolean; n: number; streams: Stream[]; prefix: string; maxSeconds: number; epoch: string; identity: string };
export type Txn = { stage: string; worker: number; pid: number; t0: number; t1: number; ok: boolean; code?: string; n: number; batch: string };
type Job = { id: string; t: Tenant; n: number; raw: any; custody: any; done: boolean; tries: number };

const RT = "BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL application_name='scb:%s';SET LOCAL ROLE bulls96_session_ingest";
const now = () => performance.timeOrigin + performance.now();
const stageOf = (sql: string) => /claim_buffered/.test(sql) ? "claim" : /prepare_buffer_stage/.test(sql) ? "prepare" : /commit_buffered/.test(sql) ? "commit" : (sql.match(/sessions\.(\w+)/)?.[1] ?? "other");

let init: LoadInit;
let queues: { s: Stream; q: Job[] }[] = [];
let pool: pg.Pool;
const scratch = scratchStore();
const txns: Txn[] = [];
const pidOf = new WeakMap<pg.PoolClient, number>();

const ledgerFor = (worker: number, n: number, id: string) => sqlLedger(async (sql, params) => {
  const c = await pool.connect();
  let pid = pidOf.get(c); if (pid === undefined) { pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number; pidOf.set(c, pid); }
  const stage = stageOf(sql), t0 = now();
  try {
    await c.query(RT.replace("%s", stage)); const r = await c.query(sql, params as never); await c.query("COMMIT");
    txns.push({ stage, worker, pid, t0, t1: now(), ok: true, n, batch: id }); return r as never;
  } catch (e: any) {
    await c.query("ROLLBACK").catch(() => {});
    txns.push({ stage, worker, pid, t0, t1: now(), ok: false, code: String(e?.code ?? "client"), n, batch: id }); throw e;
  } finally { c.release(); }
}, { acceptanceMode: "buffered", expectedGeneration: init.generation, originClaimFence: init.originClaimFence });

async function custody(t: Tenant, raw: any) {
  const p = await prepareDelivery(t, raw);
  const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: init.identity, controlEpoch: init.epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
}
let serial = 0;
async function job(t: Tenant, origin: string, n: number): Promise<Job> {
  const id = `${init.prefix}-b${++serial}`;
  const raw = await batch(Array.from({ length: n }, (_, i) => record({ record_id: `${id}-r${i}`, revision: 2, session_id: `${id}-s${i % 5}`, origin_id: origin })),
    { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
  return { id, t, n, raw, custody: await custody(t, raw), done: false, tries: 0 };
}
const deliver = (j: Job, worker: number) => deliverBatch({ ledger: ledgerFor(worker, j.n, j.id), acceptanceMode: "buffered",
  target: { store: scratch.store, keyRoot: "96bulls" }, custody: j.custody } as never, j.t, j.raw);

self.onmessage = async (ev: MessageEvent) => {
  const m = ev.data as { kind: "init"; init: LoadInit } | { kind: "go" };
  if (m.kind === "init") {
    init = m.init;
    pool = new pg.Pool({ connectionString: init.dbUrl, max: init.streams.reduce((a, s) => a + s.workers, 0) + 2 });
    for (const s of init.streams) { const q: Job[] = []; for (let i = 0; i < s.batches; i++) q.push(await job(s.t, s.origin, init.n)); queues.push({ s, q }); }
    self.postMessage({ kind: "ready", batches: queues.reduce((a, x) => a + x.q.length, 0) });
    return;
  }
  const start = now(), deadline = start + init.maxSeconds * 1000, cpu0 = process.cpuUsage();
  const lags: number[] = []; let expect = now() + 10;
  const lagTimer = setInterval(() => { const t = now(); lags.push(Math.max(0, t - expect)); expect = t + 10; }, 10);
  const all = queues.flatMap((x) => x.q);
  const outcomes: Record<string, number> = {}; let durable = 0, lastDone = start;
  const loop = async (q: Job[], worker: number) => {
    while (now() < deadline) {
      const j = q.shift(); if (!j) return;
      j.tries++;
      try {
        const o = await deliver(j, worker);
        outcomes[o.outcome] = (outcomes[o.outcome] ?? 0) + 1;
        if (["accepted_buffered", "replay", "replay_fast"].includes(o.outcome)) { j.done = true; durable += j.n; lastDone = now(); continue; }
      } catch (e: any) { const k = "error:" + String(e?.code ?? e?.cause_code ?? e?.name ?? "error"); outcomes[k] = (outcomes[k] ?? 0) + 1; }
      q.push(j); await Bun.sleep(100 + Math.random() * 200); // client re-upload after a pause
    }
  };
  await Promise.all(queues.flatMap(({ s, q }) => Array.from({ length: s.workers }, (_, k) => loop(q, s.firstWorker + k))));
  clearInterval(lagTimer);
  const cpu = process.cpuUsage(cpu0);
  self.postMessage({ kind: "done", txns, outcomes, durable, lastDone, start, batches: all.length, records: all.reduce((a, j) => a + j.n, 0),
    undrained: all.filter((j) => !j.done).length, tries: all.reduce((a, j) => a + j.tries, 0), cpu, lags });
  await pool.end(); scratch.done();
};
