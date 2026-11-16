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

function edge(logCommit: "on" | "off", admission: "async" | "sync" = "async", concurrency = 1) {
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
      ...(admission === "async" ? { admission: { mode: "async" as const, concurrency, scanIntervalMs: 15_000, maxAttempts: 20 },
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
/**
 * The records the logged batches carry. Slice 3's materializer applies each entry after the append and then deletes
 * its payload, so a payload still present is read directly and an applied one is read back from the canonical
 * session it landed in (this file's uploads put each record in `sess-<record id>`, or the session the test names).
 */
async function payloadRecords(store: ObjectStore, es: LogEntry[]): Promise<string[]> {
  const ids: string[] = [];
  const applied: string[] = [];
  for (const b of batchesIn(es)) for (const p of b.payloads) {
    if (await exists(store, p.key)) ids.push(...((await json(store, p.key)) as { records: { record_id: string }[] }).records.map((r) => r.record_id));
    else applied.push(...b.sessions.map(([, s]) => s));
  }
  for (const s of applied) ids.push(s.replace(/^sess-/, ""));
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
    // Both uploads are promoted in one round; when their object mtimes tie at millisecond resolution, the fair
    // selection falls back to key order (the hashed batch segment), so the two batches' relative order is not pinned.
    expect(batchesIn(es).map((b) => b.batchId).sort()).toEqual(["b1", "b2"]);
    expect(await payloadRecords(e.base, es)).toEqual(["r1", "r2", "r3"]);
    // Admission was the log entry; the legacy batch doc is written afterwards, by slice 3's materializer applying it
    // (the only legacy writer allowed on a log-mode stream; the guard test below pins that every other is refused).
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
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

describe("slice 4: replay before the gates, legacy batch docs, revision rules, one admission per stream at a time", () => {
  test("a retry of a logged batch is answered replay even while a reset is pending (no gate runs first)", async () => {
    const { LogStreams } = await import("../ledger-log-admit.ts");
    const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");
    const sc = scratchStore();
    cleanups.push(sc.done);
    await writeAuthority(sc.store);
    await enable(sc.store);
    const streams = new LogStreams({ store: sc.store, keyRoot: ROOT, writer: "w_test", nowMs: () => T0 });
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "r1", session_id: "sess-r1", origin_id: ORIGIN })],
      { batch_id: "b1", origin_id: ORIGIN, account_id: await deliveryAccount(TENANT) }));
    expect(await streams.admit(TENANT, prepared, { journalSeq: 0 })).toBe("accepted");
    // A pending ordered reset and a journal moved past the upload: a NEW batch would be held or refused...
    const io = s3LedgerObjects(sc.store, ROOT);
    await io.editFence(s3LedgerKeys(ROOT, TENANT).fence, (f) => ({ next: { ...f, pending: { p1: { kind: "reset", requestId: "rq1" } } }, result: null }));
    await putJson(sc.store, journalKey(ROOT, TENANT), { v: 3, seq: 1, pending: [], ambiguous: [] });
    // ...but the retry of the committed one is a replay (no legacy batch doc exists: only the window answers).
    expect(await exists(sc.store, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))).toBe(false);
    expect(await streams.admit(TENANT, prepared, { journalSeq: 0 })).toBe("replay");
    expect(batchesIn(await entries(sc.store)).map((b) => b.batchId)).toEqual(["b1"]);
  });

  test("a batch committed by the legacy path before the switch is a replay; the same id with other content is the 409", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "b1", ["r1"]);
    await drain(e);
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
    await enable(e.base);
    const same = await upload(e.base, "b1", ["r1"]);
    const other = await upload(e.base, "b1", ["rZ"]);
    await drain(e);
    expect(await status(e.base, k1)).toMatchObject({ state: "admitted" });
    expect(await status(e.base, same)).toMatchObject({ state: "admitted" });
    expect(await status(e.base, other)).toMatchObject({ state: "refused" });
    // Neither went into the log.
    expect(batchesIn(await entries(e.base))).toEqual([]);
  });

  test("two records at one revision with another revision id and digest: the legacy revision 409, nothing logged", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const b = await batch([record({ record_id: "dup", revision: 1, revision_id: "rev-a", session_id: "sess-dup", origin_id: ORIGIN, payload: { text: "one" } }),
      record({ record_id: "dup", revision: 1, revision_id: "rev-b", session_id: "sess-dup", origin_id: ORIGIN, payload: { text: "two" } })],
      { batch_id: "btw", origin_id: ORIGIN, account_id: await deliveryAccount(TENANT) });
    const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
    const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: ROOT, ...TENANT }), { originId: ORIGIN, batchId: "btw", objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
    const witness = encodeUploadWitness(parseUploadWitnessKey(WITNESS_KEY), { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
    await e.base.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
    await drain(e);
    const st = await status(e.base, key);
    expect(st.state).toBe("refused");
    expect(batchesIn(await entries(e.base))).toEqual([]);
  });

  test("concurrency 4: batches of one stream are admitted one at a time (no lost head+1 race, no extra epoch)", async () => {
    const e = edge("on", "async", 4);
    await writeAuthority(e.base);
    await enable(e.base);
    const ks = [];
    for (let i = 0; i < 6; i++) ks.push(await upload(e.base, `c${i}`, [`rc${i}`]));
    await drain(e);
    for (const k of ks) expect(await status(e.base, k)).toMatchObject({ state: "admitted" });
    const es = await entries(e.base);
    expect(es.filter((x) => x.kind === "epoch").length).toBe(1);
    expect(batchesIn(es).map((b) => b.batchId).sort()).toEqual(["c0", "c1", "c2", "c3", "c4", "c5"]);
  });
});

describe("slice 5: a skipped batch stays refused, retries write fresh payloads, refused traffic takes nothing, eviction", () => {
  const prep = async (batchId: string, recordId: string, origin = ORIGIN) => {
    const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");
    return prepareDelivery(TENANT, await batch([record({ record_id: recordId, session_id: `sess-${recordId}`, origin_id: origin })],
      { batch_id: batchId, origin_id: origin, account_id: await deliveryAccount(TENANT) }));
  };
  const listKeys = async (store: ObjectStore, prefix: string) => {
    const out: string[] = [];
    let c: string | undefined;
    do { const p = await store.list(objectKey(prefix.slice(0, -1)), c); for (const i of p.items) if (String(i.key).startsWith(prefix)) out.push(String(i.key)); c = p.nextCursor; } while (c);
    return out;
  };

  test("a replay of a batch the materializer skipped answers refused; the admitted outcome is never written over it", async () => {
    const { writeLogSkip } = await import("../ledger-log-materialize.ts");
    const { admissionStatus } = await import("../async-admission.ts");
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    // The client POST (async admission): queued with its receipt, then admitted through the log.
    const first = await e.post("b1", "r1");
    expect(first.status).toBe(200);
    const receiptId = ((await first.json()) as { receiptId: string }).receiptId;
    await drain(e);
    expect(await admissionStatus(e.base, ROOT, TENANT, ORIGIN, receiptId)).toMatchObject({ state: "admitted" });
    // The materializer then refused b1 for good at apply time, exactly as it records it: the skip record, the refused
    // outcome, the admitted outcome retired.
    const { outcomeKey, receiptDir, writeOutcome } = await import("../async-admission.ts");
    await writeLogSkip(e.base, logKeys(ROOT, TENANT, ORIGIN), "b1", "delivery_privacy_superseded", T0);
    const dir = receiptDir(ROOT, TENANT, ORIGIN, receiptId);
    await writeOutcome(e.base, dir, { v: 1, state: "refused", code: "delivery_privacy_superseded", atMs: T0 });
    await e.base.delete(objectKey(outcomeKey(dir, "admitted")));
    expect(await admissionStatus(e.base, ROOT, TENANT, ORIGIN, receiptId)).toMatchObject({ state: "refused" });
    // The client retries the same batch: the log answers replay, and the receipt must read refused, not admitted.
    expect((await e.post("b1", "r1")).status).toBe(200);
    await drain(e);
    expect(await admissionStatus(e.base, ROOT, TENANT, ORIGIN, receiptId)).toMatchObject({ state: "refused", code: "delivery_privacy_superseded" });
    // Exactly one logged batch: the replay appended nothing.
    expect(batchesIn(await entries(e.base)).map((b) => b.batchId)).toEqual(["b1"]);
  });

  test("the materializer writes the skip record on a final refusal, and a later replay reads refused", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    // Block materialization (canonical writes fail), so the batch is logged but not yet applied.
    e.setFault((key) => (key.includes("/s=") && key.endsWith("/manifest.json") ? "before" : "ok"));
    const k1 = await upload(e.base, "b1", ["r1"]);
    await drain(e);
    // A reset completes after the append: the materializer refuses the batch at apply time.
    await s3LedgerObjects(e.base, ROOT).editFence(s3LedgerKeys(ROOT, TENANT).fence,
      (f) => ({ next: { ...f, resets: (f.resets ?? 0) + 1, resetsCompleted: (f.resetsCompleted ?? 0) + 1 }, result: null }));
    e.setFault(undefined);
    await drain(e);
    expect(await exists(e.base, logKeys(ROOT, TENANT, ORIGIN).skipped("b1"))).toBe(true);
    expect(await status(e.base, k1)).toMatchObject({ state: "refused" });
    const again = await upload(e.base, "b1", ["r1"]);
    await drain(e);
    expect(await status(e.base, again)).toMatchObject({ state: "refused" });
  });

  test("a retry writes a FRESH payload: collecting the earlier attempt's identical-content payload loses nothing", async () => {
    const { LogStreams } = await import("../ledger-log-admit.ts");
    const sc = scratchStore();
    cleanups.push(sc.done);
    await writeAuthority(sc.store);
    await enable(sc.store);
    const streams = new LogStreams({ store: sc.store, keyRoot: ROOT, writer: "w_test", nowMs: () => T0 });
    const prepared = await prep("b1", "r1");
    // Attempt 1 puts its payload, then consent refuses at commit (nothing appended).
    let refuse = true;
    const consent = async (_d: ReadonlySet<string>, phase: "admit" | "commit") => { if (phase === "commit" && refuse) throw new Error("consent_revoked_for_test"); };
    await expect(streams.admit(TENANT, prepared, { journalSeq: 0, consent })).rejects.toThrow();
    const payloads = logKeys(ROOT, TENANT, ORIGIN).payloads;
    const [first] = await listKeys(sc.store, payloads);
    expect(first).toBeDefined();
    // Attempt 2 writes a different object; the orphan collector may take attempt 1's (deleted outright here).
    refuse = false;
    expect(await streams.admit(TENANT, prepared, { journalSeq: 0, consent })).toBe("accepted");
    const refs = batchesIn(await entries(sc.store)).flatMap((b) => b.payloads.map((p) => p.key));
    expect(refs.length).toBe(1);
    expect(refs[0]).not.toBe(first);
    await sc.store.delete(objectKey(first!));
    expect(await exists(sc.store, refs[0]!)).toBe(true);
  });

  test("refused or replayed traffic writes no epoch entry and no registry doc (the stream is taken only to write)", async () => {
    const { LogStreams } = await import("../ledger-log-admit.ts");
    const { logStreamRegistryKey } = await import("../ledger-log-materialize.ts");
    const sc = scratchStore();
    cleanups.push(sc.done);
    await writeAuthority(sc.store);
    await enable(sc.store);
    const streams = new LogStreams({ store: sc.store, keyRoot: ROOT, writer: "w_test", nowMs: () => T0 });
    // Refused by the journal: a reset completed after the upload began.
    await putJson(sc.store, journalKey(ROOT, TENANT), { v: 3, seq: 1, pending: [], ambiguous: [] });
    await expect(streams.admit(TENANT, await prep("b1", "r1"), { journalSeq: 0 })).rejects.toMatchObject({ status: 409 });
    // Held by a pending ordered reset (a retry).
    await s3LedgerObjects(sc.store, ROOT).editFence(s3LedgerKeys(ROOT, TENANT).fence, (f) => ({ next: { ...f, pending: { p1: { kind: "reset", requestId: "rq1" } } }, result: null }));
    await expect(streams.admit(TENANT, await prep("b2", "r2"), { journalSeq: 1 })).rejects.toMatchObject({ code: "ledger_control_pending" });
    expect((await entries(sc.store)).map((x) => x.kind)).toEqual(["open"]);
    expect(await exists(sc.store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(false);
  });

  test("cache eviction never drops a state whose admission is in flight (no extra epoch entry on its next batch)", async () => {
    const { LogStreams } = await import("../ledger-log-admit.ts");
    const sc = scratchStore();
    cleanups.push(sc.done);
    await writeAuthority(sc.store);
    const O2 = "origin-2";
    await enable(sc.store);
    await enableLogMode(sc.store, logKeys(ROOT, TENANT, O2), null, { writer: "operator", now: () => T0 });
    const streams = new LogStreams({ store: sc.store, keyRoot: ROOT, writer: "w_test", nowMs: () => T0, maxStreams: 1 });
    // A's first batch is held at its commit-phase consent (taken, payload put, entry not yet appended)...
    let release!: () => void;
    const held = new Promise<void>((r) => { release = r; });
    let reached!: () => void;
    const atCommit = new Promise<void>((r) => { reached = r; });
    const consent = async (_d: ReadonlySet<string>, phase: "admit" | "commit") => { if (phase === "commit") { reached(); await held; } };
    const a1 = streams.admit(TENANT, await prep("a1", "ra1"), { journalSeq: 0, consent });
    await atCommit;
    // ...while B's first batch takes its stream (the cache is full: max 1).
    expect(await streams.admit(TENANT, await prep("b1", "rb1", O2), { journalSeq: 0 })).toBe("accepted");
    release();
    expect(await a1).toBe("accepted");
    expect(await streams.admit(TENANT, await prep("a2", "ra2"), { journalSeq: 0 })).toBe("accepted");
    expect((await entries(sc.store)).filter((x) => x.kind === "epoch").length).toBe(1);
  });

  test("the CLI guard is an allowlist: mutations need --i-know-this-is-not-prod and a non-production-like bucket", async () => {
    const { mutationRefusal } = await import("../../../scripts/ledger-log-stream.ts");
    for (const command of ["switch", "close"]) {
      expect(mutationRefusal(command, "sessions-dev-bench", true, false)).toBe("not_declared_nonprod");
      for (const b of ["sessionsprod", "prod2-history", "prd-ledger", "relayhistoryproduction", "acme-live", "hosted-x", "customer-data"])
        expect(mutationRefusal(command, b, true, true)).toBe("bucket_name_production_like");
      expect(mutationRefusal(command, "sessions-dev-bench", true, true)).toBeUndefined();
      // Dry runs stay allowed.
      expect(mutationRefusal(command, "sessionsprod", false, false)).toBeUndefined();
    }
    expect(mutationRefusal("status", "sessionsprod", true, false)).toBeUndefined();
  });
});
