/**
 * Benchmark: one S3-ledger admission (claim -> canonical write -> commit) over the scratch fs store with an
 * injected per-request latency, against an origin whose revision index already holds `--existing` records.
 * Reports request counts, bytes and wall time per phase (deliverPrepared's timing_ms). Local only: no network.
 *
 *   bun scripts/bench/s3-ledger-admission.ts [--existing 210000] [--records 50] [--latency-ms 30] [--runs 5] [--parallel 16] [--concurrent 4] [--line-bytes 400]
 */
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { parseArgs } from "node:util";
import { createFsStore } from "../../packages/object-store/src/fs.ts";
import { objectKey, type ObjectStore } from "../../src/shared/vendor.ts";
import { deliverPrepared } from "../../src/session-ingest/deliver.ts";
import { REVISION_SHARDS, s3Ledger, s3LedgerKeys, sessionKeyHash } from "../../src/session-ingest/ledger-s3.ts";
import { prepareDelivery, deliveryAccount } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION, type HistoryExportRecord } from "../../src/session-ingest/vendor/delivery/delivery-contracts.ts";

const { values } = parseArgs({ options: {
  existing: { type: "string", default: "210000" }, records: { type: "string", default: "50" },
  "latency-ms": { type: "string", default: "30" }, runs: { type: "string", default: "5" }, parallel: { type: "string", default: "6" }, concurrent: { type: "string", default: "1" }, "line-bytes": { type: "string", default: "400" },
} });
const EXISTING = Number(values.existing), RECORDS = Number(values.records), LATENCY = Number(values["latency-ms"]), RUNS = Number(values.runs);
const ROOT = "96bulls", ORIGIN = "origin-1";
const TENANT = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const hex = (v: string) => createHash("sha256").update(v).digest("hex");

const base = process.env.TMPDIR ?? join(process.env.HOME ?? ".", ".agent-tmp", "tmp");
mkdirSync(base, { recursive: true, mode: 0o700 });
const dir = mkdtempSync(join(base, "bench-ledger-"));
const fs = createFsStore({ root: dir }) as unknown as ObjectStore;

interface Counter { gets: number; puts: number; heads: number; lists: number; bytesIn: number; bytesOut: number; casLost: number }
const fresh = (): Counter => ({ gets: 0, puts: 0, heads: 0, lists: 0, bytesIn: 0, bytesOut: 0, casLost: 0 });
let c = fresh();
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const store: ObjectStore = {
  ...fs, backend: fs.backend,
  head: async (k) => { c.heads++; await sleep(LATENCY); return fs.head(k); },
  get: async (k, r) => { c.gets++; await sleep(LATENCY); return fs.get(k, r); },
  getObject: async (k, r) => {
    c.gets++; await sleep(LATENCY);
    const got = await fs.getObject(k, r);
    c.bytesIn += got.meta.size ?? 0;
    return got;
  },
  list: async (p, cur) => { c.lists++; await sleep(LATENCY); return fs.list(p, cur); },
  presignGet: (k, t) => fs.presignGet(k, t),
  delete: async (k) => { await sleep(LATENCY); return fs.delete(k); },
  put: async (k, b, o) => {
    c.puts++; c.bytesOut += b.byteLength; await sleep(LATENCY);
    try { return await fs.put(k, b, o); } catch (e) { if (o?.ifMatch || o?.ifNoneMatch) c.casLost++; throw e; }
  },
};

// Seed: the authority marker, and an origin index with EXISTING records spread over the shards.
await fs.put(objectKey(`${ROOT}/.ledger/fence-authority.json`), new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 })), { contentType: "application/json" });
const keys = s3LedgerKeys(ROOT, TENANT);
const shards = new Map<string, Record<string, unknown>>();
const sk = sessionKeyHash("claude", "old-session");
for (let i = 0; i < EXISTING; i++) {
  const id = `old-${i}`;
  const k = keys.revisions(ORIGIN, id);
  const m = shards.get(k) ?? {};
  m[id] = [1, `${id}:r1`, hex(id), sk, 0, 0];
  shards.set(k, m);
}
for (const [k, records] of shards) {
  const shard = /\/r=([0-9a-f]+)\.json$/.exec(k)![1];
  await fs.put(objectKey(k), new TextEncoder().encode(JSON.stringify({ v: 1, originId: ORIGIN, shard, records })), { contentType: "application/json" });
}
const shardBytes = [...shards.values()].map((r) => JSON.stringify(r).length);
console.log(`seeded ${EXISTING} records in ${shards.size}/${REVISION_SHARDS} shards, ~${Math.round(shardBytes.reduce((a, b) => a + b, 0) / Math.max(1, shardBytes.length) / 1024)} KiB each`);

const ledger = s3Ledger(store, ROOT, { fenceAuthority: "required", parallel: Number(values.parallel) });
const deps = { ledger, acceptanceMode: "canonical" as const, target: { store, keyRoot: ROOT } };
const account = await deliveryAccount(TENANT);
const walls: number[] = [];
const CONCURRENT = Number(values.concurrent);
async function prepared(run: number, lane: number) {
  const records: HistoryExportRecord[] = Array.from({ length: RECORDS }, (_, i) => ({
    schema_version: 1, origin_id: ORIGIN, revision_id: `run${run}-l${lane}-r${i}:r1`, revision: 1, kind: "session_event", source: "claude",
    session_id: `sess-bench-${lane}`, operation: "upsert", record_id: `run${run}-l${lane}-r${i}`,
    payload: { text: `line ${i} `.padEnd(Number(values["line-bytes"]), "x"), ts_ms: 1_761_955_200_000 + run * 1000 + i },
  } as unknown as HistoryExportRecord));
  return prepareDelivery(TENANT, { schema_version: 1, origin_id: ORIGIN, batch_id: `bench-${run}-${lane}`, job_id: "job-1", generation: 1, destination_id: "dest-1", instance_id: "inst-1", account_id: account, mapping_version: DELIVERY_MAPPING_VERSION, records });
}
for (let run = 0; run < RUNS; run++) {
  const ps = await Promise.all(Array.from({ length: CONCURRENT }, (_, lane) => prepared(run, lane)));
  c = fresh();
  (globalThis as { Bun?: { gc(force: boolean): void } }).Bun?.gc(true);
  const heap0 = process.memoryUsage().heapUsed;
  let heapMax = heap0;
  const sampler = setInterval(() => { heapMax = Math.max(heapMax, process.memoryUsage().heapUsed); }, 5);
  const t0 = performance.now();
  const rs = await Promise.all(ps.map((p) => deliverPrepared(deps, TENANT, p).then((r) => ({ r, ms: performance.now() - t0 }))));
  const wall = performance.now() - t0;
  clearInterval(sampler);
  walls.push(wall);
  const r = rs[0]!.r;
  console.log(JSON.stringify({ run, outcome: r.outcome, wall_ms: Math.round(wall), per_admission_ms: rs.map((x) => Math.round(x.ms)), timing_ms: r.stats.timing_ms,
    requests: { get: c.gets, put: c.puts, head: c.heads, list: c.lists, cas_lost: c.casLost }, kib_in: Math.round(c.bytesIn / 1024), kib_out: Math.round(c.bytesOut / 1024), heap_peak_mib: Math.round((heapMax - heap0) / 2 ** 20) }));
}
walls.sort((a, b) => a - b);
console.log(`wall p50 ${Math.round(walls[Math.floor(walls.length / 2)]!)} ms over ${RUNS} runs (latency ${LATENCY} ms/request, ${RECORDS} records, ${EXISTING} existing, parallel ${values.parallel}, ${CONCURRENT} concurrent)`);
rmSync(dir, { recursive: true, force: true });
