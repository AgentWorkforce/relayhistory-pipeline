/**
 * Log-mode admission (ledger-log-admit.ts, slice 2 of docs/design/s3-ledger-log-commit.md) on the self-host edge with
 * INGEST_LEDGER=s3, async admission and direct uploads, no database:
 *   - INGEST_LEDGER_LOG_COMMIT=on alone changes nothing: a stream without its mode marker is admitted by the legacy
 *     path, and the log stays empty;
 *   - a stream in log mode is admitted through its log (epoch entry, then a batches entry), never the legacy chain;
 *   - the legacy claim REFUSES a log-mode stream (logModeGuard), so a legacy writer can never commit behind the log;
 *   - the privacy gates run before the entry: a fence-deleted session's records are dropped, a pending reset is a
 *     retry, a reset completed after the upload began refuses it;
 *   - equivalence: the same uploads reach the same per-upload outcome on a legacy and a log-mode edge;
 *   - fault injection on the entry PUT (before / after landing): exactly one entry, exactly one admission.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { journalKey } from "../async-admission.ts";
import { enableLogMode, logKeys, readEntries, type LogEntry } from "../ledger-log.ts";
import { fenceAuthorityKey, s3LedgerKeys, s3LedgerObjects } from "../ledger-s3.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime } from "../selfhost-server.ts";
import { uploadAdmissionStatus, uploadDirectConfig } from "../upload-admission.ts";
import { encodeUploadWitness, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } from "../upload-inbox-contract.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const V = JSON.parse(readFileSync(join(import.meta.dir, "../../../docs/contracts/relay-upload-inbox-v1.vectors.json"), "utf8"));
const WITNESS_KEY = V.witness.testOnlyKey as string;
const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const ORIGIN = "origin-1";
const DIRECT = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" });

const cleanups: (() => unknown)[] = [];
afterEach(async () => { for (const f of cleanups.splice(0).reverse()) await f(); });

const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const putJson = (store: ObjectStore, key: string, v: unknown) => store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(v)), { contentType: "application/json" });
const writeAuthority = (store: ObjectStore) => putJson(store, fenceAuthorityKey(ROOT),
  { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: T0 });

/** Wraps `base`; `fault(key)` may throw before or after a PUT to make it fail or land-but-error. */
function faulty(base: ObjectStore) {
  let fault: ((key: string) => "before" | "after" | "ok") | undefined;
  const store = new Proxy(base, {
    get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (typeof v !== "function") return v;
      if (prop !== "put") return v.bind(target);
      return async (...args: unknown[]) => {
        const mode = fault?.(String(args[0])) ?? "ok";
        if (mode === "before") throw new ObjectStoreError("transient", "fs", "injected before", { status: 503 });
        const r = await v.apply(target, args);
        if (mode === "after") throw new ObjectStoreError("transient", "fs", "injected after", { status: 503 });
        return r;
      };
    },
  });
  return { store, setFault: (f: typeof fault) => { fault = f; } };
}

function edge(logCommit: "on" | "off", admission: "async" | "sync" = "async") {
  const s = scratchStore();
  cleanups.push(s.done);
  const f = faulty(s.store);
  let consumer: SelfhostAdmissionConsumer | undefined;
  let clock = T0;
  const prior = process.env.INGEST_LEDGER_LOG_COMMIT;
  process.env.INGEST_LEDGER_LOG_COMMIT = logCommit;
  try {
    const runtime = selfhostIngestRuntime({
      keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 32, ledger: "s3",
      ...(admission === "async" ? { admission: { mode: "async" as const, concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 20 },
        uploadDirect: { config: DIRECT, witnessKey: WITNESS_KEY, scanBatch: 16 } } : {}),
    }, { run: (async () => { throw new Error("SQL runner called on the S3 ledger path"); }) as never, store: f.store,
      verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} },
    { startConsumer: false, nowMs: () => clock, onConsumer: (c) => { consumer = c; },
      log: { info: () => {}, warn: () => {}, error: () => {} } as never });
    cleanups.push(() => runtime.close());
    const identity = { ...TENANT, scopes: [SYNC_SCOPE] } as unknown as IngestIdentity;
    const post = async (batchId: string, recordId: string) => {
      const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: recordId, session_id: `sess-${recordId}`, origin_id: ORIGIN })], { batch_id: batchId, origin_id: ORIGIN }) });
      const request = new Request("https://history.selfhost.test/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json" } });
      runtime.bind(request, identity);
      return runtime.app.fetch(request);
    };
    return { base: s.store, setFault: f.setFault, post, consumer: () => consumer!, advance: (ms: number) => { clock += ms; } };
  } finally {
    if (prior === undefined) delete process.env.INGEST_LEDGER_LOG_COMMIT; else process.env.INGEST_LEDGER_LOG_COMMIT = prior;
  }
}

/** A desktop upload of one batch: zstd JSON at its content-addressed key, the witness in its metadata. */
async function upload(store: ObjectStore, batchId: string, recordIds: string[], session?: (id: string) => string) {
  const b = await batch(recordIds.map((id) => record({ record_id: id, session_id: session ? session(id) : `sess-${id}`, origin_id: ORIGIN })),
    { batch_id: batchId, origin_id: ORIGIN, account_id: await deliveryAccount(TENANT) });
  const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: ROOT, ...TENANT }), { originId: ORIGIN, batchId, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
  const witness = encodeUploadWitness(parseUploadWitnessKey(WITNESS_KEY), { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
  await store.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
  return key;
}

const status = async (store: ObjectStore, key: string) =>
  ((await uploadAdmissionStatus({ store, keyRoot: ROOT }, DIRECT, TENANT, key)) as { body: { state: string; code?: string } }).body;
async function entries(store: ObjectStore): Promise<LogEntry[]> {
  const out: LogEntry[] = [];
  for await (const e of readEntries(store, logKeys(ROOT, TENANT, ORIGIN))) out.push(e);
  return out;
}
const batchesIn = (es: LogEntry[]) => es.flatMap((e) => (e.kind === "batches" ? e.batches : []));
async function payloadRecords(store: ObjectStore, es: LogEntry[]): Promise<string[]> {
  const ids: string[] = [];
  for (const b of batchesIn(es)) for (const p of b.payloads) ids.push(...((await json(store, p.key)) as { records: { record_id: string }[] }).records.map((r) => r.record_id));
  return ids.sort();
}
const enable = (store: ObjectStore) => enableLogMode(store, logKeys(ROOT, TENANT, ORIGIN), null, { writer: "operator", now: () => T0 });
/** One consumer round: past any backoff (the clock moves 10 min), scan, then admit what is due. */
async function drain(e: ReturnType<typeof edge>) { e.advance(600_000); await e.consumer().scan(); await e.consumer().settle(); }

describe("INGEST_LEDGER_LOG_COMMIT on, stream not in log mode", () => {
  test("on alone changes nothing: the legacy path admits, the log stays empty", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    const k = await upload(e.base, "b1", ["r1"]);
    await drain(e);
    expect(await status(e.base, k)).toMatchObject({ state: "admitted" });
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
    expect(await exists(e.base, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
  });
});

describe("a stream in log mode", () => {
  test("is admitted through its log (epoch, then batches), never the legacy chain; a conflicting batch id is refused", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const k1 = await upload(e.base, "b1", ["r1", "r2"]);
    const k2 = await upload(e.base, "b2", ["r3"]);
    await drain(e);
    expect([(await status(e.base, k1)).state, (await status(e.base, k2)).state]).toEqual(["admitted", "admitted"]);
    const es = await entries(e.base);
    expect(es.map((x) => x.kind)).toEqual(["open", "epoch", "batches", "batches"]);
    expect(batchesIn(es).map((b) => b.batchId)).toEqual(["b1", "b2"]);
    expect(await payloadRecords(e.base, es)).toEqual(["r1", "r2", "r3"]);
    // The legacy chain wrote nothing for these batches.
    expect(await exists(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))).toBe(false);
    // Same batch id, other content: refused as the legacy batch_conflict; no new entry.
    const k3 = await upload(e.base, "b1", ["r9"]);
    await drain(e);
    expect((await status(e.base, k3)).state).toBe("refused");
    expect((await entries(e.base)).length).toBe(4);
  });

  test("the legacy claim refuses it (logModeGuard): the synchronous POST is a 503 and writes nothing", async () => {
    const e = edge("on", "sync");
    await writeAuthority(e.base);
    await enable(e.base);
    const res = await e.post("bs", "rs");
    expect(res.status).toBe(503);
    expect(await exists(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "bs"))).toBe(false);
    expect((await entries(e.base)).map((x) => x.kind)).toEqual(["open"]);
  });

  test("privacy: a fence-deleted session's records are dropped; a pending reset is a retry; a later completed reset refuses", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const io = s3LedgerObjects(e.base, ROOT);
    const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
    await io.editFence(fenceKey, (f) => ({ next: { ...f, deleted: { [ORIGIN]: [["claude", "gone"]] } }, result: null }));
    const k1 = await upload(e.base, "b1", ["keep", "drop"], (id) => (id === "drop" ? "gone" : `sess-${id}`));
    await drain(e);
    expect(await status(e.base, k1)).toMatchObject({ state: "admitted" });
    let es = await entries(e.base);
    expect(await payloadRecords(e.base, es)).toEqual(["keep"]);
    expect(batchesIn(es)[0]!.sessions).toEqual([["claude", "sess-keep"]]);
    // A pending ordered reset: no admission, no entry, the upload stays pending.
    await io.editFence(fenceKey, (f) => ({ next: { ...f, pending: { p1: { kind: "reset", requestId: "rq1" } } }, result: null }));
    const k2 = await upload(e.base, "b2", ["r2"]);
    await drain(e);
    expect((await status(e.base, k2)).state).toBe("pending");
    expect((await entries(e.base)).length).toBe(es.length);
    // The reset clears, but the journal moved past the upload's witness (seq 0): refused, never admitted.
    await io.editFence(fenceKey, (f) => ({ next: { ...f, pending: {} }, result: null }));
    await putJson(e.base, journalKey(ROOT, TENANT), { v: 3, seq: 1, pending: [], ambiguous: [] });
    await drain(e);
    expect(await status(e.base, k2)).toMatchObject({ state: "refused", code: "delivery_privacy_superseded" });
    es = await entries(e.base);
    expect(batchesIn(es).map((b) => b.batchId)).toEqual(["b1"]);
  });

  test("fault injection on the entry PUT: failed-before and landed-but-503 each end in exactly one entry and one admission", async () => {
    for (const when of ["before", "after"] as const) {
      const e = edge("on");
      await writeAuthority(e.base);
      await enable(e.base);
      // Fail the FIRST batches entry PUT (entry 2; entry 1 is the consumer's epoch) on every attempt the ledger-log
      // makes in one call (LOG_PUT_ATTEMPTS), so the admission itself sees the outcome as unknown and retries.
      let left = 4;
      e.setFault((key) => (key.endsWith("/e/00000000000000000002.json") && left-- > 0 ? when : "ok"));
      const k = await upload(e.base, "b1", ["r1"]);
      for (let i = 0; i < 6 && (await status(e.base, k)).state !== "admitted"; i++) await drain(e);
      expect(await status(e.base, k)).toMatchObject({ state: "admitted" });
      const es = await entries(e.base);
      expect(batchesIn(es).map((b) => b.batchId)).toEqual(["b1"]);
      expect(await payloadRecords(e.base, es)).toEqual(["r1"]);
    }
  });
});

describe("equivalence: legacy edge against log-mode edge", () => {
  test("the same uploads reach the same per-upload outcome", async () => {
    const run = async (mode: "legacy" | "log") => {
      const e = edge("on");
      await writeAuthority(e.base);
      if (mode === "log") await enable(e.base);
      const ks = [await upload(e.base, "b1", ["r1", "r2"]), await upload(e.base, "b2", ["r3"])];
      await drain(e);
      ks.push(await upload(e.base, "b1", ["rX"])); // same batch id, other content
      await drain(e);
      return Promise.all(ks.map(async (k) => { const s = await status(e.base, k); return [s.state, s.code ?? null]; }));
    };
    const legacy = await run("legacy");
    const log = await run("log");
    expect(log.map(([s]) => s)).toEqual(legacy.map(([s]) => s));
    expect(log[2]).toEqual(legacy[2]);
  });
});
