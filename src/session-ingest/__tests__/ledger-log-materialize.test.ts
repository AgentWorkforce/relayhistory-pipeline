/**
 * The log materializer (ledger-log-materialize.ts, slice 3 of docs/design/s3-ledger-log-commit.md) on the self-host
 * edge with INGEST_LEDGER=s3, async admission and direct uploads, no database:
 *   - equivalence: the same uploads on a legacy edge and a log-mode edge end in byte-equal read-side state (every
 *     object outside the log, the registry and the inboxes), and the same per-upload outcomes;
 *   - fault injection at every store write of the materializer, failing before or after the write: the state
 *     converges to the same bytes;
 *   - two materializers on one stream: the same state;
 *   - privacy at materialization: a session deleted after the entry is not written; a reset completed after the
 *     upload began skips the batch; erased payloads are skipped; applied payloads are deleted;
 *   - the sweeper applies a stream this process never kicked.
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
/** One consumer round: past any backoff (the clock moves 10 min), scan, then admit what is due (settle also waits
 * for the materializer the admissions kicked). */
async function drain(e: ReturnType<typeof edge>) { e.advance(600_000); await e.consumer().scan(); await e.consumer().settle(); }
void batchesIn; void payloadRecords;

/**
 * The read-side state: every canonical object (manifests, chunks) and the legacy ledger's revision index, owners,
 * rearm markers and fence, key -> bytes (hex). Legacy batch docs carry a random claim token, so they are compared by
 * their meaning ({state, digest, originId, batchId}). Left out: the log itself, the stream registry and the queues
 * (upload inbox, admission inbox objects and outcome markers), which only one mode or only the transport writes.
 */
async function readSide(store: ObjectStore): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(ROOT), cursor);
    for (const item of page.items) {
      const k = String(item.key);
      if (k.includes("/ledger/log/") || k.includes("/.ledger/log-streams/") || k.includes("/.upload-inbox/") || k.includes("/.admission-dlq/")) continue;
      // In the admission inbox only the user's ledger directory and fence are read-side state.
      if (k.includes("/.admission-inbox/") && !k.includes("/ledger/") && !k.endsWith("/upload-fences.json")) continue;
      const bytes = Buffer.from(await readAll((await store.getObject(objectKey(k))).body));
      if (k.includes("/ledger/batches/")) {
        const d = JSON.parse(bytes.toString()) as { state: string; digest: string; originId: string; batchId: string };
        out.set(k, JSON.stringify([d.state, d.digest, d.originId, d.batchId]));
      } else out.set(k, bytes.toString("hex"));
    }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}
async function keysUnder(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const p = await store.list(objectKey(prefix.replace(/\/$/, "")), cursor); for (const i of p.items) out.push(String(i.key)); cursor = p.nextCursor; } while (cursor);
  return out;
}
const rounds = async (e: ReturnType<typeof edge>, n: number) => { for (let i = 0; i < n; i++) await drain(e); };

/** The same uploads, in the same order, at the same clock. `hook` may set a fault before the uploads. */
async function scenario(mode: "legacy" | "log", hook?: (e: ReturnType<typeof edge>) => void | Promise<void>) {
  const e = edge("on");
  await writeAuthority(e.base);
  if (mode === "log") await enable(e.base);
  await hook?.(e);
  const ks = [await upload(e.base, "b1", ["r1", "r2"]), await upload(e.base, "b2", ["r3", "r4"], () => "sess-shared")];
  await rounds(e, 6);
  ks.push(await upload(e.base, "b3", ["r5"], () => "sess-shared"));
  ks.push(await upload(e.base, "b1", ["rX"])); // same batch id, other content: a conflict
  await rounds(e, 6);
  e.setFault(undefined);
  await rounds(e, 3);
  const outcomes = await Promise.all(ks.map(async (k) => { const s = await status(e.base, k); return [s.state, s.code ?? null]; }));
  return { e, state: await readSide(e.base), outcomes };
}
const sameState = (a: Map<string, string>, b: Map<string, string>, tag = "") => {
  expect([...a.keys()].sort()).toEqual([...b.keys()].sort());
  for (const [k, v] of b) expect([tag, k, a.get(k)]).toEqual([tag, k, v]);
};

describe("equivalence: log mode plus materialization against legacy admission", () => {
  test("byte-equal read-side state and the same per-upload outcomes; applied payloads deleted; every entry marked", async () => {
    const legacy = await scenario("legacy");
    const log = await scenario("log");
    expect(log.outcomes.map(([s]) => s)).toEqual(legacy.outcomes.map(([s]) => s));
    expect(log.outcomes.map(([s]) => s)).toEqual(["admitted", "admitted", "admitted", "refused"]);
    expect(legacy.state.size).toBeGreaterThan(3);
    sameState(log.state, legacy.state);
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    expect(await keysUnder(log.e.base, keys.payloads)).toEqual([]);
    expect((await keysUnder(log.e.base, keys.markers)).length).toBe((await entries(log.e.base)).length);
  });

  test("fault injection at every materializer write, failing before or after it: the same bytes as legacy", async () => {
    const legacy = await scenario("legacy");
    for (const when of ["before", "after"] as const) {
      // Each read-side write (canonical objects, the legacy ledger, markers, payload deletes are not PUTs) fails once.
      // The log entries, payloads, inbox and status objects are left alone: admission is slice 2's subject.
      const failed = new Set<string>();
      const log = await scenario("log", (e) => e.setFault((key) => {
        if (key.includes("/ledger/log/e/") || key.includes("/ledger/log/o=") && key.includes("/p/") || key.includes("/.upload-inbox/") || key.includes("/log-streams/")) return "ok";
        if (key.includes("/.admission-inbox/") && !key.includes("/ledger/")) return "ok";
        if (failed.has(key)) return "ok";
        failed.add(key);
        return when;
      }));
      expect(failed.size).toBeGreaterThan(5);
      expect(log.outcomes.map(([s]) => s)).toEqual(legacy.outcomes.map(([s]) => s));
      sameState(log.state, legacy.state, when);
    }
  }, 60_000); // every write fails once, before and after: a hundred-odd materializer rounds
});

describe("the materializer", () => {
  /** A second, independent materializer over the same store (another process). */
  async function second(store: ObjectStore) {
    const { LogMaterializer } = await import("../ledger-log-materialize.ts");
    const { selfhostS3Ledger } = await import("../selfhost-server.ts");
    return new LogMaterializer({ store, keyRoot: ROOT, nowMs: () => T0 + 1e9,
      deliver: { ledger: selfhostS3Ledger(ROOT, { store, run: (async () => { throw new Error("sql"); }) as never }, undefined, { logModeGuard: false }),
        acceptanceMode: "canonical", target: { store, keyRoot: ROOT } } });
  }

  test("two materializers on one stream, racing: the same state as legacy, applied once", async () => {
    const legacy = await scenario("legacy");
    let other: Awaited<ReturnType<typeof second>> | undefined;
    let raced = 0;
    const log = await scenario("log", async (e) => {
      other = await second(e.base);
      // Every legacy batch-doc PUT of the first materializer kicks the second on the same stream, mid-apply.
      e.setFault((key) => { if (key.includes("/ledger/batches/")) { raced++; other!.kick(TENANT, ORIGIN); } return "ok"; });
    });
    await other!.settle();
    // One kick per legacy batch-doc write of the first materializer (three batches, at least one write each).
    expect(raced).toBeGreaterThanOrEqual(3);
    sameState(log.state, legacy.state);
    sameState(await readSide(log.e.base), legacy.state);
    expect((await other!.materialize(TENANT, ORIGIN)).applied).toBe(0);
  });

  test("privacy at materialization: a session deleted after its entry is not written; a later reset skips the batch; an erased payload is skipped", async () => {
    const { eraseLogPayloads } = await import("../ledger-log-materialize.ts");
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const io = s3LedgerObjects(e.base, ROOT);
    const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
    // Hold materialization (every legacy batch doc PUT fails), admit, then change the fence and the journal.
    const hold = (on: boolean) => e.setFault((key) => (on && key.includes("/ledger/batches/") ? "before" : "ok"));
    hold(true);
    const k1 = await upload(e.base, "b1", ["keep", "drop"], (id) => (id === "drop" ? "gone" : `sess-${id}`));
    await rounds(e, 2);
    expect((await status(e.base, k1)).state).toBe("admitted");
    expect([...(await readSide(e.base)).keys()].some((k) => k.includes("/s=sess-keep/"))).toBe(false);
    await io.editFence(fenceKey, (f) => ({ next: { ...f, deleted: { [ORIGIN]: [["claude", "gone"]] } }, result: null }));
    hold(false);
    await rounds(e, 2);
    let side = [...(await readSide(e.base)).keys()];
    expect(side.some((k) => k.includes("/s=sess-keep/manifest.json"))).toBe(true);
    expect(side.some((k) => k.includes("/s=gone/"))).toBe(false);
    // A reset completed after the next upload began (its witness is seq 0): logged, then skipped at apply.
    hold(true);
    const k2 = await upload(e.base, "b2", ["late"], () => "sess-late");
    await rounds(e, 2);
    expect((await status(e.base, k2)).state).toBe("admitted");
    await putJson(e.base, journalKey(ROOT, TENANT), { v: 3, seq: 1, pending: [], ambiguous: [] });
    hold(false);
    await rounds(e, 2);
    side = [...(await readSide(e.base)).keys()];
    expect(side.some((k) => k.includes("/s=sess-late/"))).toBe(false);
    // The skip is final, so the upload's status reads refused, never admitted for data that was not written.
    expect(await status(e.base, k2)).toMatchObject({ state: "refused", code: "delivery_privacy_superseded" });
    // Erasure: a payload erased before its entry applies is skipped, never written. (The journal stays at seq 1, so
    // a fresh upload with witness seq 0 would be refused at admit: write the journal back first.)
    await putJson(e.base, journalKey(ROOT, TENANT), { v: 3, seq: 0, pending: [], ambiguous: [] });
    hold(true);
    await upload(e.base, "b3", ["erased"], () => "sess-erased");
    await rounds(e, 2);
    expect(await eraseLogPayloads(e.base, ROOT, TENANT)).toBe(1);
    hold(false);
    await rounds(e, 2);
    side = [...(await readSide(e.base)).keys()];
    expect(side.some((k) => k.includes("/s=sess-erased/"))).toBe(false);
    // Every entry applied, no payload left.
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    expect((await keysUnder(e.base, keys.markers)).length).toBe((await entries(e.base)).length);
    expect(await keysUnder(e.base, keys.payloads)).toEqual([]);
  });

  test("the sweeper applies a stream no kick reached (another process admitted it)", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    e.setFault((key) => (key.includes("/ledger/batches/") ? "before" : "ok"));
    await upload(e.base, "b1", ["r1"]);
    await rounds(e, 1);
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    const total = (await entries(e.base)).length;
    expect((await keysUnder(e.base, keys.markers)).length).toBeLessThan(total);
    // A fresh process (no kick of its own) sweeps the registry and applies the stream.
    e.setFault(undefined);
    const m = await second(e.base);
    await m.sweep();
    await m.settle();
    expect((await keysUnder(e.base, keys.markers)).length).toBe(total);
    expect([...(await readSide(e.base)).keys()].some((k) => k.includes("/s=sess-r1/manifest.json"))).toBe(true);
  });
});

describe("operator steps (ledger-log-stream-ops.ts)", () => {
  test("switch: dry run writes nothing; refused while a legacy batch is pending; apply opens and registers the stream", async () => {
    const { closeLogStream, logCommitProofKey, logModeNonProdKey, streamStatus, switchToLogMode } = await import("../ledger-log-stream-ops.ts");
    const { logStreamRegistryKey } = await import("../ledger-log-materialize.ts");
    const e = edge("on");
    await writeAuthority(e.base);
    // A legacy batch stuck pending (its commit fails): the switch must wait for the drain.
    e.setFault((key) => (key.includes("/s=") && key.endsWith("/manifest.json") ? "before" : "ok"));
    await upload(e.base, "b1", ["r1"]);
    await rounds(e, 1);
    expect((await streamStatus(e.base, ROOT, TENANT, ORIGIN)).legacyPending).toBe(1);
    // The allowlist, inside the operation: a bucket not declared non-production is refused first, whatever else holds.
    expect((await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", settleMs: 0 })).refused).toBe("bucket_not_declared_nonprod");
    await putJson(e.base, logModeNonProdKey(ROOT), { v: 1 });
    // No deployment-wide proof that every replica runs with the flag on: refused before anything else.
    expect((await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", settleMs: 0 })).refused).toBe("log_commit_not_proven_on");
    await putJson(e.base, logCommitProofKey(ROOT), { v: 1, mode: "on" });
    expect((await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", settleMs: 0 })).refused).toBe("legacy_batches_pending");
    e.setFault(undefined);
    await rounds(e, 2);
    const dry = await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: false, writer: "op", settleMs: 0 });
    expect([dry.applied, dry.refused]).toEqual([false, undefined]);
    expect(await exists(e.base, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
    const done = await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", settleMs: 0 });
    expect([done.applied, done.status.mode, done.status.legacyCommitted]).toEqual([true, "log", 1]);
    expect(await exists(e.base, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(true);
    // The next upload goes through the log and is applied.
    await upload(e.base, "b2", ["r2"]);
    await rounds(e, 2);
    let s = await streamStatus(e.base, ROOT, TENANT, ORIGIN);
    expect(s.materializedThrough).toBe(s.head!);
    // Close refuses while an entry is unapplied, then hands the stream back to legacy.
    e.setFault((key) => (key.includes("/ledger/batches/") ? "before" : "ok"));
    await upload(e.base, "b3", ["r3"]);
    await rounds(e, 1);
    expect((await closeLogStream(e.base, ROOT, TENANT, ORIGIN, { apply: true })).refused).toBe("entries_not_applied");
    e.setFault(undefined);
    await rounds(e, 2);
    expect((await closeLogStream(e.base, ROOT, TENANT, ORIGIN, { apply: false })).applied).toBe(false);
    const closed = await closeLogStream(e.base, ROOT, TENANT, ORIGIN, { apply: true });
    expect([closed.applied, closed.status.mode]).toEqual([true, "closed"]);
    // After close, a new batch is admitted by the legacy chain (the guard and the consumer both read the marker).
    s = await streamStatus(e.base, ROOT, TENANT, ORIGIN);
    const head = s.head;
    const k4 = await upload(e.base, "b4", ["r4"]);
    await rounds(e, 2);
    expect((await status(e.base, k4)).state).toBe("admitted");
    expect((await streamStatus(e.base, ROOT, TENANT, ORIGIN)).head).toBe(head);
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b4"))).toMatchObject({ state: "committed" });
  }, 30_000); // several consumer and materializer rounds: about 0.5 s alone, past bun's 5 s default on a loaded host
});

describe("slice 4: the admission snapshot, re-judged at apply", () => {
  test("a reset that completes between the append and the apply refuses the batch (no fence journal entry needed)", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const io = s3LedgerObjects(e.base, ROOT);
    const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
    e.setFault((key) => (key.includes("/ledger/batches/") ? "before" : "ok"));
    const k = await upload(e.base, "b1", ["pre"], () => "sess-pre");
    await rounds(e, 2);
    expect((await status(e.base, k)).state).toBe("admitted");
    const logged = (await entries(e.base)).flatMap((x) => (x.kind === "batches" ? x.batches : []));
    expect(logged[0]!.admitted).toMatchObject({ journalSeq: 0, resets: 0 });
    expect(Object.keys(logged[0]!.admitted!.epochs)).toHaveLength(1);
    // A reset recorded and completed in the fence only (the journal does not move): the snapshot is what refuses it.
    await io.editFence(fenceKey, (f) => ({ next: { ...f, resets: f.resets + 1, resetsCompleted: f.resetsCompleted + 1 }, result: null }));
    e.setFault(undefined);
    await rounds(e, 2);
    expect([...(await readSide(e.base)).keys()].some((x) => x.includes("/s=sess-pre/"))).toBe(false);
    expect(await status(e.base, k)).toMatchObject({ state: "refused", code: "delivery_privacy_superseded" });
  });

  test("a session whose epoch moved between the append and the apply (deleted, then re-enabled) has its records dropped", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const io = s3LedgerObjects(e.base, ROOT);
    const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
    e.setFault((key) => (key.includes("/ledger/batches/") ? "before" : "ok"));
    await upload(e.base, "b1", ["a", "b"], (id) => (id === "a" ? "sess-moved" : "sess-kept"));
    await rounds(e, 2);
    // Delete then re-enable sess-moved: no tombstone left, but its epoch moved past the snapshot.
    const { sessionKeyHash } = await import("../ledger-s3.ts");
    await io.editFence(fenceKey, (f) => ({ next: { ...f, epochs: { [ORIGIN]: { [sessionKeyHash("claude", "sess-moved")]: 7 } } }, result: null }));
    e.setFault(undefined);
    await rounds(e, 2);
    const side = [...(await readSide(e.base)).keys()];
    expect(side.some((x) => x.includes("/s=sess-moved/"))).toBe(false);
    expect(side.some((x) => x.includes("/s=sess-kept/manifest.json"))).toBe(true);
  });

  test("switch: a legacy batch that commits during the settle wait is reported as legacy_raced_switch", async () => {
    const { logCommitProofKey, logModeNonProdKey, switchToLogMode } = await import("../ledger-log-stream-ops.ts");
    const e = edge("on");
    await writeAuthority(e.base);
    await putJson(e.base, logModeNonProdKey(ROOT), { v: 1 });
    await putJson(e.base, logCommitProofKey(ROOT), { v: 1, mode: "on" });
    const r = await switchToLogMode(e.base, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", settleMs: 1, sleep: async () => {
      // A legacy writer that read "not log mode" just before entry 0 landed commits during the wait.
      const keys = s3LedgerKeys(ROOT, TENANT);
      await putJson(e.base, keys.batch(ORIGIN, "late"), { v: 1, originId: ORIGIN, batchId: "late", state: "committed" });
    } });
    expect([r.applied, r.refused]).toEqual([true, "legacy_raced_switch"]);
  });
});
