/** Seal pool (seal-pool.ts): byte-identical to the pinned sealer, every fallback exactly today's. */
import { afterEach, expect, test } from "bun:test";
import { readAll, objectKey, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { resetScrubEngineForTests, sealRawChunk, selectScrubEngine } from "../../../packages/pipeline/src/lib/scrub-engine/index.ts";
import type { SealRawChunk } from "../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import { createSealPool, parseSealWorkers, SEAL_POOL_MAX_RESPAWNS, sealPoolDeps, sealPoolSize, type SealPool } from "../seal-pool.ts";
import { writeGroups } from "../writer.ts";
import { scratchStore } from "./support.ts";

const enc = new TextEncoder();
const cleanup: (() => unknown)[] = [];
afterEach(async () => { resetScrubEngineForTests(); for (const f of cleanup.splice(0)) await f(); });
function scratch(): ObjectStore { const s = scratchStore(); cleanup.push(s.done); return s.store; }
function pool(size = 2, workerUrl?: URL): SealPool { const p = createSealPool(size, workerUrl ? { workerUrl } : {}); cleanup.push(() => p.close()); return p; }

const LOC = { workspaceId: "ws_1", machineId: "mach-1", harness: "claude", sessionId: "s-synthetic-1", startDate: "2026-10-25" };
// Synthetic secrets only: a token-shaped value, an email, a secret key, a long line, unicode.
const corpus: string[] = [
  JSON.stringify({ kind: "session_event", payload: { text: "plain line" } }),
  JSON.stringify({ text: "token ghp_" + "A".repeat(36) + " and mail someone@example.com" }),
  JSON.stringify({ password: { value: "hunter2-synthetic" }, note: "nested secret" }),
  JSON.stringify({ text: "word ".repeat(20_000) }),
  JSON.stringify({ text: "ünïcødé   line sep and emoji 🚀" }),
  "not json at all, but a line",
];
const sources = [corpus.join("\n") + "\n", corpus[0] + "\n", corpus.slice(1, 4).join("\n") + "\n"].map((s) => enc.encode(s));
const input = (source: Uint8Array, seq = 0, traceId?: string): SealRawChunk => ({ location: LOC, format: "ndjson", seq, epoch: 0, startOffset: seq * 10, source, ...(traceId ? { traceId } : {}) });

async function stored(store: ObjectStore, key: string) {
  const o = await store.getObject(objectKey(key));
  return { bytes: Buffer.from(await readAll(o.body)).toString("hex"), meta: o.meta.metadata };
}

test("INGEST_SEAL_WORKERS: unset, empty and 0 are off; 1..32 on; anything else refused", () => {
  expect(parseSealWorkers(undefined)).toBe(0);
  expect(parseSealWorkers("")).toBe(0);
  expect(parseSealWorkers("0")).toBe(0);
  expect(parseSealWorkers("4")).toBe(4);
  expect(parseSealWorkers("32")).toBe(32);
  for (const bad of ["33", "-1", "1.5", "two", "4x"]) expect(() => parseSealWorkers(bad)).toThrow(/INGEST_SEAL_WORKERS/);
  expect(sealPoolSize({ INGEST_SEAL_WORKERS: "32" })).toBeLessThanOrEqual(32);
});

test("flag off: sealPoolDeps is exactly today's object and starts no pool", () => {
  selectScrubEngine({});
  let created = 0;
  const deps = sealPoolDeps({}, () => { created++; throw new Error("no"); });
  expect(deps).toEqual({});
  expect(created).toBe(0);
});

for (const engine of ["wasm-bindgen", "wasm"]) test(`refused with SCRUB_ENGINE=${engine}, before any engine is touched`, () => {
  let created = 0;
  // The env passed in is the only input: no process-wide engine selection or initialisation is needed to refuse.
  expect(() => sealPoolDeps({ INGEST_SEAL_WORKERS: "2", SCRUB_ENGINE: engine }, () => { created++; throw new Error("no"); }))
    .toThrow("INGEST_SEAL_WORKERS needs the TypeScript scrub engine");
  expect(created).toBe(0);
});

test("SCRUB_ENGINE unset or ts: the pool starts", () => {
  selectScrubEngine({});
  for (const env of [{ INGEST_SEAL_WORKERS: "1" }, { INGEST_SEAL_WORKERS: "1", SCRUB_ENGINE: "ts" }]) {
    let created = 0;
    const fake = { sealer: (() => {}) as never, size: 1, stats: () => ({}) as never, close: async () => {} };
    const deps = sealPoolDeps(env, () => { created++; return fake; });
    expect(created).toBe(1);
    expect(deps.sealPool).toBe(fake);
  }
});

test("byte identity: same key, bytes, metadata and entry as the pinned sealer", async () => {
  selectScrubEngine({});
  const p = pool(2);
  for (const [i, source] of sources.entries()) {
    const a = scratch(), b = scratch();
    const today = await sealRawChunk(a, input(source, i, "trace-" + i));
    const pooled = await p.sealer(b, input(source, i, "trace-" + i));
    expect(pooled).toEqual(today);
    expect(await stored(b, pooled.key)).toEqual(await stored(a, today.key));
  }
  expect(p.stats()).toMatchObject({ pooled: sources.length, fallback: 0, workerErrors: 0 });
});

test("concurrent seals across workers keep each result with its own input", async () => {
  selectScrubEngine({});
  const p = pool(3), a = scratch(), b = scratch();
  const inputs = Array.from({ length: 12 }, (_, i) => input(enc.encode(`${JSON.stringify({ text: `record ${i} ` + "x".repeat(i * 997) })}\n`), i));
  const pooled = await Promise.all(inputs.map((x) => p.sealer(b, x)));
  for (const [i, x] of inputs.entries()) expect(pooled[i]).toEqual(await sealRawChunk(a, x));
});

test("an existing identical object falls back to the pinned path: created=false, same entry", async () => {
  selectScrubEngine({});
  const p = pool(1), s = scratch(), x = input(sources[0]!);
  const first = await sealRawChunk(s, x);
  const again = await p.sealer(s, x);
  const pinnedAgain = await sealRawChunk(s, x);
  expect(again).toEqual(pinnedAgain);
  expect(again.created).toBe(false);
  expect(again.entry).toEqual(first.entry);
  expect(p.stats().fallback).toBe(1);
});

test("a sealer error is today's error, thrown from the pinned path", async () => {
  selectScrubEngine({});
  const p = pool(1), s = scratch();
  for (const bad of [new Uint8Array(0), new Uint8Array([0xff, 0xfe, 0x0a])]) {
    const today = await sealRawChunk(s, input(bad)).then(() => null, (e: Error) => [e.name, e.message, (e as { code?: string }).code]);
    const pooled = await p.sealer(s, input(bad)).then(() => null, (e: Error) => [e.name, e.message, (e as { code?: string }).code]);
    expect(today).not.toBeNull();
    expect(pooled).toEqual(today);
  }
  expect((await s.list(objectKey("ws_1"))).items.length).toBe(0);
});

test("a worker that dies is replaced, and its task is sealed on the main thread", async () => {
  selectScrubEngine({});
  const p = pool(1, new URL("./fixtures/seal-pool-dying-worker.ts", import.meta.url));
  const a = scratch(), b = scratch();
  const today = await sealRawChunk(a, input(sources[1]!));
  expect(await p.sealer(b, input(sources[1]!))).toEqual(today);
  expect(p.stats().workerErrors).toBeGreaterThanOrEqual(1);
  expect(p.stats().fallback).toBe(1);
});

test("a worker that keeps dying is retired after the respawn cap; seals then run on the main thread", async () => {
  selectScrubEngine({});
  const p = pool(2, new URL("./fixtures/seal-pool-dying-worker.ts", import.meta.url));
  const a = scratch(), b = scratch();
  for (let i = 0; i < 12; i++) {
    const x = input(enc.encode(`${JSON.stringify({ text: "retire " + i })}\n`), i);
    expect(await p.sealer(b, x)).toEqual(await sealRawChunk(a, x));
  }
  const stats = p.stats();
  expect(stats.retired).toBe(2);
  // Bounded: at most SEAL_POOL_MAX_RESPAWNS respawns per slot, then no more workers.
  expect(stats.respawned).toBe(2 * SEAL_POOL_MAX_RESPAWNS);
  expect(stats.workerErrors).toBe(2 * (SEAL_POOL_MAX_RESPAWNS + 1));
  expect(stats.fallback).toBe(12);
});

test("close resolves an in-flight seal (it completes on the main thread), and is idempotent", async () => {
  selectScrubEngine({});
  const p = createSealPool(1, { workerUrl: new URL("./fixtures/seal-pool-silent-worker.ts", import.meta.url) });
  const a = scratch(), b = scratch(), x = input(sources[1]!);
  const inFlight = p.sealer(b, x);
  await new Promise((r) => setTimeout(r, 50));
  await p.close();
  await p.close();
  expect(await inFlight).toEqual(await sealRawChunk(a, x));
  expect(p.stats().workerErrors).toBe(0);
});

test("a PUT failure other than already_exists is thrown as the store threw it", async () => {
  selectScrubEngine({});
  const p = pool(1), s = scratch();
  const boom = Object.assign(new Error("store down"), { name: "ObjectStoreError", code: "transient" });
  const failing = new Proxy(s, { get: (t, k) => (k === "put" ? async () => { throw boom; } : Reflect.get(t, k)) });
  await expect(p.sealer(failing, input(sources[1]!))).rejects.toBe(boom);
});

test("writer end to end: writeGroups with the pool writes exactly today's objects and manifests", async () => {
  selectScrubEngine({});
  const p = pool(2), a = scratch(), b = scratch();
  const at = new Date("2026-11-04T01:00:00Z");
  const line = (i: number) => JSON.stringify({ kind: "session_event", origin_id: "origin-a", record_id: `r${i}`, revision: "1", operation: "upsert", received_at: "2026-11-04T01:00:00Z", payload: { text: `visible ${i} ghp_${"B".repeat(36)}`, ts_ms: 1762218000000 + i } });
  const groups = () => [0, 1, 2].map((g) => ({ source: "claude", sessionId: `session:${g}`, lines: [line(g), line(g + 10)] }));
  const batch = { orgId: "org-a", workspaceId: "ws-a", userId: "user-a", originId: "origin-a", batchDigest: "a".repeat(64) };
  const now = () => at;
  const today = await writeGroups({ store: a, keyRoot: "96bulls", now }, batch, groups(), at);
  const pooled = await writeGroups({ store: b, keyRoot: "96bulls", now, rawChunkSealer: p.sealer }, batch, groups(), at);
  expect(pooled.parts).toEqual(today.parts);
  expect(pooled.manifests.map((m) => m.manifest)).toEqual(today.manifests.map((m) => m.manifest));
  const keys = async (s: ObjectStore) => (await s.list(objectKey("96bulls"))).items.map((i) => String(i.key)).sort();
  expect(await keys(b)).toEqual(await keys(a));
  for (const key of await keys(a)) expect((await stored(b, key)).bytes).toBe((await stored(a, key)).bytes);
});
