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

function edge(logCommit: "on" | "off", admission: "async" | "sync" = "async", opts: { logErase?: boolean } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const f = faulty(s.store);
  let consumer: SelfhostAdmissionConsumer | undefined;
  let clock = T0;
  const prior = process.env.INGEST_LEDGER_LOG_COMMIT;
  process.env.INGEST_LEDGER_LOG_COMMIT = logCommit;
  try {
    const runtime = selfhostIngestRuntime({
      keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 32, ledger: "s3", ...(opts.logErase ? { ledgerLogErase: true as const } : {}),
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
      if (k.includes("/ledger/log/") || k.includes("/ledger/log-index/") || k.includes("/.ledger/log-streams/") || k.includes("/.upload-inbox/") || k.includes("/.admission-dlq/")) continue;
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
        // The registry, the stream identity doc and the stream index are admission's writes (registerLogStream, before the take), as is
        // every log entry and payload: left alone, so admission's timing (and so received_at) matches legacy.
        if (key.includes("/ledger/log/e/") || key.includes("/ledger/log/o=") && (key.includes("/p/") || key.endsWith("/stream.json")) || key.includes("/ledger/log-index/") || key.includes("/.upload-inbox/") || key.includes("/log-streams/")) return "ok";
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
    const { eraseLogPayloads } = await import("../ledger-log-erase.ts");
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
    const k3 = await upload(e.base, "b3", ["erased"], () => "sess-erased");
    await rounds(e, 2);
    expect(await eraseLogPayloads(e.base, ROOT, TENANT)).toBe(1);
    hold(false);
    await rounds(e, 2);
    side = [...(await readSide(e.base)).keys()];
    expect(side.some((k) => k.includes("/s=sess-erased/"))).toBe(false);
    // No reset superseded it (the fence's resets did not move): a refused erasure (payload_erased, answered as admission_failed).
    expect(await status(e.base, k3)).toMatchObject({ state: "refused", code: "admission_failed" });
    // Every entry applied, no payload left.
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    expect((await keysUnder(e.base, keys.markers)).length).toBe((await entries(e.base)).length);
    expect(await keysUnder(e.base, keys.payloads)).toEqual([]);
  });

  for (const logErase of [true, false])
  test(`a reset whose erase took an admitted, unapplied payload: never written; answered ${logErase ? "superseded (INGEST_LEDGER_LOG_ERASE=on)" : "payload_erased with the materializer's flag off, as before"}`, async () => {
    const { eraseSupersededLogPayloads } = await import("../ledger-log-erase.ts");
    const { s3Ledger } = await import("../ledger-s3.ts");
    const e = edge("on", "async", { logErase });
    await writeAuthority(e.base);
    await enable(e.base);
    e.setFault((key) => (key.includes("/ledger/batches/") ? "before" : "ok"));
    const k1 = await upload(e.base, "b1", ["secret"], () => "sess-secret");
    await rounds(e, 2);
    expect((await status(e.base, k1)).state).toBe("admitted");
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    expect((await keysUnder(e.base, keys.payloads)).length).toBe(1);
    // The reset (the self-host route's S3 ledger, its SQL mirror left out): the payload goes before it answers.
    const ledger = s3Ledger(e.base, ROOT, { sleep: async () => {}, nowMs: () => T0, claimCompletesResets: false,
      resetLogErase: (t, r) => eraseSupersededLogPayloads(e.base, ROOT, t, r) });
    // (Its count is the owner stubs the held apply's legacy claim registered; not what this test is about.)
    await ledger.resetUploadedSessions(TENANT, "req-1");
    expect(await keysUnder(e.base, keys.payloads)).toEqual([]);
    e.setFault(undefined);
    await rounds(e, 2);
    expect([...(await readSide(e.base)).keys()].some((k) => k.includes("/s=sess-secret/"))).toBe(false);
    // On: the client-visible answer is the reset's (as the snapshot rule gives without the erase), not a bare erasure.
    // Off (the materializer's deps.logErase unset): a bare erasure, payload_erased (answered as admission_failed), with
    // no fence read on that path, exactly as before the erase existed.
    expect(await status(e.base, k1)).toMatchObject({ state: "refused", code: logErase ? "delivery_privacy_superseded" : "admission_failed" });
    expect((await keysUnder(e.base, keys.markers)).length).toBe((await entries(e.base)).length);
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

describe("Remaining 1: consent re-checked at apply time", () => {
  const prep = async (batchId: string, recordId: string) => {
    const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");
    return prepareDelivery(TENANT, await batch([record({ record_id: recordId, session_id: `sess-${recordId}`, origin_id: ORIGIN })],
      { batch_id: batchId, origin_id: ORIGIN, account_id: await deliveryAccount(TENANT) }));
  };
  async function materializer(store: ObjectStore, uploadConsent?: import("../upload-consent.ts").UploadConsentConfig) {
    const { LogMaterializer } = await import("../ledger-log-materialize.ts");
    const { selfhostS3Ledger } = await import("../selfhost-server.ts");
    return new LogMaterializer({ store, keyRoot: ROOT, nowMs: () => T0, ...(uploadConsent ? { uploadConsent } : {}),
      deliver: { ledger: selfhostS3Ledger(ROOT, { store, run: (async () => { throw new Error("sql"); }) as never }, undefined, { logModeGuard: false }),
        acceptanceMode: "canonical", target: { store, keyRoot: ROOT } } });
  }
  async function admitted(store: ObjectStore, batchId: string, recordId: string, consentCap?: import("../upload-consent.ts").OperatorSharingCap) {
    const { LogStreams } = await import("../ledger-log-admit.ts");
    const streams = new LogStreams({ store, keyRoot: ROOT, writer: "w_test", nowMs: () => T0 });
    // Consent passed at admit (the gate allowed it then); the cap rides in the payload.
    expect(await streams.admit(TENANT, await prep(batchId, recordId), { journalSeq: 0, consent: async () => {}, ...(consentCap ? { consentCap } : {}) })).toBe("accepted");
  }
  const fresh = async () => {
    const sc = scratchStore();
    cleanups.push(sc.done);
    await writeAuthority(sc.store);
    await enable(sc.store);
    return sc.store;
  };

  test("the payload carries the inbox's consent cap; the materializer re-runs the gate with it and skips a refused batch (payload deleted)", async () => {
    const store = await fresh();
    await admitted(store, "b1", "r1", { mode: "selected" });
    const [b] = batchesIn(await entries(store));
    expect((await json(store, b!.payloads[0]!.key)).consentCap).toEqual({ mode: "selected" });
    // No consent store in this process: an operator cap with named sessions refuses (consent_store_required).
    const r = await (await materializer(store)).materialize(TENANT, ORIGIN);
    expect(r.outcomes).toEqual(["skipped:upload_consent_required"]);
    expect([...(await readSide(store)).keys()].some((k) => k.includes("/s=sess-r1/"))).toBe(false);
    expect(await keysUnder(store, logKeys(ROOT, TENANT, ORIGIN).payloads)).toEqual([]);
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).skipped("b1"))).toBe(true);
  });

  test("a consent revoked between the append and the apply skips the batch; one still allowed is written", async () => {
    const { MemoryConsentStore } = await import("../upload-consent.ts");
    const store = await fresh();
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: [ORIGIN] }, { trusted: true, nowMs: T0 });
    await admitted(store, "b1", "r1");
    // Allowed now: written.
    const m = await materializer(store, { enforcement: "enforce", store: consent, nowMs: () => T0 });
    expect((await m.materialize(TENANT, ORIGIN)).outcomes).toEqual(["accepted"]);
    // Held after the next append, before its apply: skipped, never written.
    await admitted(store, "b2", "r2");
    consent.hold(TENANT);
    expect((await m.materialize(TENANT, ORIGIN)).outcomes).toEqual(["skipped:upload_consent_required"]);
    const side = [...(await readSide(store)).keys()];
    expect(side.some((k) => k.includes("/s=sess-r1/manifest.json"))).toBe(true);
    expect(side.some((k) => k.includes("/s=sess-r2/"))).toBe(false);
    expect(await keysUnder(store, logKeys(ROOT, TENANT, ORIGIN).payloads)).toEqual([]);
  });

  test("a consent store fault at apply is a retry (the cursor stays), never a skip", async () => {
    const { MemoryConsentStore } = await import("../upload-consent.ts");
    const store = await fresh();
    await admitted(store, "b1", "r1");
    const consent = new MemoryConsentStore();
    consent.snapshot = async () => { throw new Error("consent store down"); };
    const m = await materializer(store, { enforcement: "enforce", store: consent, nowMs: () => T0 });
    await expect(m.materialize(TENANT, ORIGIN)).rejects.toThrow();
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).skipped("b1"))).toBe(false);
    expect((await keysUnder(store, logKeys(ROOT, TENANT, ORIGIN).payloads)).length).toBe(1);
  });

  test("a consent skip is sticky: a run that crashed before its marker never applies the batch after consent is confirmed", async () => {
    const { MemoryConsentStore } = await import("../upload-consent.ts");
    const store = await fresh();
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: [ORIGIN] }, { trusted: true, nowMs: T0 });
    consent.hold(TENANT);
    await admitted(store, "b1", "r1");
    // Run 1 refuses (held), writes the skip record and the refused outcome, then dies before the entry's marker.
    const noMarks: ObjectStore = new Proxy(store, { get(t, prop, r) {
      const v = Reflect.get(t, prop, r);
      if (prop !== "put") return typeof v === "function" ? v.bind(t) : v;
      return async (...args: unknown[]) => { const m = /\/m\/(\d{20})\.json$/.exec(String(args[0])); if (m && Number(m[1]) >= 2) throw new Error("process died before the marker"); return v.apply(t, args); };
    } });
    const config = { enforcement: "enforce" as const, store: consent, nowMs: () => T0 };
    await expect((await materializer(noMarks, config)).materialize(TENANT, ORIGIN)).rejects.toThrow("process died before the marker");
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).skipped("b1"))).toBe(true);
    // Consent confirmed; run 2 replays the entry: still skipped, never written.
    await consent.apply(TENANT, { releaseHold: true }, { trusted: true, nowMs: T0 });
    expect((await (await materializer(store, config)).materialize(TENANT, ORIGIN)).outcomes).toEqual(["skipped:upload_consent_required"]);
    expect([...(await readSide(store)).keys()].some((k) => k.includes("/s=sess-r1/"))).toBe(false);
  });

  test("two materializers: one skips; the other, whose consent allows it, never writes the batch (before its first read or after it)", async () => {
    const { MemoryConsentStore } = await import("../upload-consent.ts");
    const { writeLogSkip } = await import("../ledger-log-materialize.ts");
    const { ObjectStoreError: OSE } = await import("../../shared/vendor.ts");
    const allow = new MemoryConsentStore();
    await allow.apply(TENANT, { mode: "all", confirmOrigins: [ORIGIN] }, { trusted: true, nowMs: T0 });
    const config = { enforcement: "enforce" as const, store: allow, nowMs: () => T0 };
    // (a) The other materializer skipped (and crashed before its marker) before this one read anything.
    let store = await fresh();
    await admitted(store, "b1", "r1");
    await writeLogSkip(store, logKeys(ROOT, TENANT, ORIGIN), "b1", "upload_consent_required", T0);
    expect((await (await materializer(store, config)).materialize(TENANT, ORIGIN)).outcomes).toEqual(["skipped:upload_consent_required"]);
    expect([...(await readSide(store)).keys()].some((k) => k.includes("/s=sess-r1/"))).toBe(false);
    // (b) The skip lands between this one's first read (absent) and its admit gate: a retry, nothing written, then skipped.
    store = await fresh();
    await admitted(store, "b1", "r1");
    const skipKey = logKeys(ROOT, TENANT, ORIGIN).skipped("b1");
    let reads = 0;
    const racing: ObjectStore = new Proxy(store, { get(t, prop, r) {
      const v = Reflect.get(t, prop, r);
      if (prop !== "getObject") return typeof v === "function" ? v.bind(t) : v;
      return async (...args: unknown[]) => {
        if (String(args[0]) === skipKey && reads++ === 0) {
          await writeLogSkip(store, logKeys(ROOT, TENANT, ORIGIN), "b1", "upload_consent_required", T0);
          throw new OSE("not_found", "fs", "absent at the first read");
        }
        return v.apply(t, args);
      };
    } });
    await expect((await materializer(racing, config)).materialize(TENANT, ORIGIN)).rejects.toThrow();
    expect([...(await readSide(store)).keys()].some((k) => k.includes("/s=sess-r1/"))).toBe(false);
    expect((await (await materializer(racing, config)).materialize(TENANT, ORIGIN)).outcomes).toEqual(["skipped:upload_consent_required"]);
    expect([...(await readSide(store)).keys()].some((k) => k.includes("/s=sess-r1/"))).toBe(false);
  });

  test("a batch mixing a capped and an uncapped payload is refused as payload_invalid, in either order", async () => {
    const store = await fresh();
    await admitted(store, "b1", "r1", { mode: "selected" });
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    // Rewrite the entry's batch with a second, uncapped payload (a hand-built entry: admission never writes one).
    const [b] = batchesIn(await entries(store));
    const p = await json(store, b!.payloads[0]!.key);
    const { consentCap: _cap, ...uncapped } = p;
    void _cap;
    const { putPayload, appendBatches, openStream } = await import("../ledger-log.ts");
    const capped = await putPayload(store, keys, new TextEncoder().encode(JSON.stringify({ ...p, attempt: "second" })));
    const ref = await putPayload(store, keys, new TextEncoder().encode(JSON.stringify(uncapped)));
    const s = (await openStream(store, keys))!;
    await appendBatches(store, s, [{ ...b!, batchId: "b2", payloads: [capped, ref] }], null, { writer: "w_test", now: () => T0 });
    // And the other order: uncapped first, then capped.
    await appendBatches(store, s, [{ ...b!, batchId: "b3", payloads: [
      await putPayload(store, keys, new TextEncoder().encode(JSON.stringify({ ...uncapped, attempt: "third" }))),
      await putPayload(store, keys, new TextEncoder().encode(JSON.stringify({ ...p, attempt: "fourth" }))),
    ] }], null, { writer: "w_test", now: () => T0 });
    const r = await (await materializer(store)).materialize(TENANT, ORIGIN);
    expect(r.outcomes).toEqual(["skipped:upload_consent_required", "skipped:payload_invalid", "skipped:payload_invalid"]);
  });

  test("a malformed cap in the payload is refused as payload_invalid", async () => {
    const store = await fresh();
    await admitted(store, "b1", "r1");
    const [b] = batchesIn(await entries(store));
    const p = await json(store, b!.payloads[0]!.key);
    await putJson(store, b!.payloads[0]!.key, { ...p, consentCap: { mode: "all" } });
    expect((await (await materializer(store)).materialize(TENANT, ORIGIN)).outcomes).toEqual(["skipped:payload_invalid"]);
  });
});

describe("Remaining 8: the stream registry holds no raw user or origin id outside the user's directory", () => {
  const fresh = () => { const sc = scratchStore(); cleanups.push(sc.done); return sc.store; };
  const raw = async (store: ObjectStore, key: string) => new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body));
  async function streams(store: ObjectStore) {
    const { listLogStreams } = await import("../ledger-log-materialize.ts");
    const out: { tenant: typeof TENANT; originId: string }[] = [];
    for await (const s of listLogStreams(store, ROOT)) out.push(s as never);
    return out;
  }

  test("a registry doc is a hash-keyed pointer; the ids live in the identity doc inside the user's directory", async () => {
    const { logStreamRegistryKey, registerLogStream } = await import("../ledger-log-materialize.ts");
    const { admissionUserDir } = await import("../async-admission.ts");
    const store = fresh();
    await registerLogStream(store, ROOT, TENANT, ORIGIN);
    await registerLogStream(store, ROOT, TENANT, ORIGIN); // idempotent
    const key = logStreamRegistryKey(ROOT, TENANT, ORIGIN);
    const body = await raw(store, key);
    expect(JSON.parse(body)).toEqual({ v: 2, root: logKeys(ROOT, TENANT, ORIGIN).root });
    for (const id of [TENANT.userId, ORIGIN]) { expect(body.includes(id)).toBe(false); expect(key.includes(id)).toBe(false); }
    expect(logKeys(ROOT, TENANT, ORIGIN).identity.startsWith(admissionUserDir(ROOT, TENANT) + "/")).toBe(true);
    expect(await json(store, logKeys(ROOT, TENANT, ORIGIN).identity)).toEqual({ v: 1, ...TENANT, originId: ORIGIN });
    expect(await streams(store)).toEqual([{ tenant: TENANT, originId: ORIGIN }]);
  });

  test("after the user's directory is erased, no object outside it names the user or the origin, and the stream is no longer listed", async () => {
    const { registerLogStream } = await import("../ledger-log-materialize.ts");
    const { admissionUserDir } = await import("../async-admission.ts");
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    await upload(e.base, "b1", ["r1"]);
    await rounds(e, 2);
    await registerLogStream(e.base, ROOT, TENANT, ORIGIN);
    const userDir = admissionUserDir(ROOT, TENANT) + "/";
    for (const k of await keysUnder(e.base, userDir)) await e.base.delete(objectKey(k));
    expect(await streams(e.base)).toEqual([]);
    // The registry and the deployment markers: nothing outside the user's directory carries the raw ids.
    for (const k of await keysUnder(e.base, `${ROOT}/.ledger/`)) {
      const body = await raw(e.base, k);
      for (const id of [TENANT.userId, ORIGIN]) { expect(k.includes(id)).toBe(false); expect(body.includes(id)).toBe(false); }
    }
  });

  test("a pointer that lies (another root, a root outside the inbox, or a doc copied under another key) names nothing", async () => {
    const { logStreamRegistryKey, logStreamsRoot, registerLogStream } = await import("../ledger-log-materialize.ts");
    const store = fresh();
    await registerLogStream(store, ROOT, TENANT, ORIGIN);
    const key = logStreamRegistryKey(ROOT, TENANT, ORIGIN);
    const other = logKeys(ROOT, { ...TENANT, userId: "user-2" }, ORIGIN).root;
    await putJson(store, key, { v: 2, root: other });
    expect(await streams(store)).toEqual([]);
    await putJson(store, key, { v: 2, root: `${ROOT}/elsewhere/ledger/log/o=${"0".repeat(32)}` });
    expect(await streams(store)).toEqual([]);
    await putJson(store, key, { v: 2, root: logKeys(ROOT, TENANT, ORIGIN).root });
    await putJson(store, `${logStreamsRoot(ROOT)}${"e".repeat(40)}.json`, { v: 2, root: logKeys(ROOT, TENANT, ORIGIN).root });
    expect(await streams(store)).toEqual([{ tenant: TENANT, originId: ORIGIN }]);
    // A copied pointer listed ON ITS OWN (the original gone): the key binding refuses it.
    await store.delete(objectKey(key));
    expect(await streams(store)).toEqual([]);
    // The original back, but the identity doc in its root names ANOTHER stream: the root binding refuses it.
    await store.delete(objectKey(`${logStreamsRoot(ROOT)}${"e".repeat(40)}.json`));
    await putJson(store, key, { v: 2, root: logKeys(ROOT, TENANT, ORIGIN).root });
    await putJson(store, logKeys(ROOT, TENANT, ORIGIN).identity, { v: 1, ...TENANT, originId: "origin-other" });
    expect(await streams(store)).toEqual([]);
    await putJson(store, logKeys(ROOT, TENANT, ORIGIN).identity, { v: 1, ...TENANT, userId: "user-2", originId: ORIGIN });
    expect(await streams(store)).toEqual([]);
  });

  test("migration: v1 docs are read until rewritten; dry run writes nothing; apply rewrites, deletes them and is idempotent", async () => {
    const { legacyLogStreamRegistryKey, logStreamRegistryKey, migrateLogStreamRegistry } = await import("../ledger-log-materialize.ts");
    const store = fresh();
    const O2 = "origin-2";
    const v1 = (originId: string) => ({ v: 1, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId });
    for (const o of [ORIGIN, O2]) await enableLogMode(store, logKeys(ROOT, TENANT, o), null, { writer: "operator", now: () => T0 });
    await putJson(store, legacyLogStreamRegistryKey(ROOT, TENANT, ORIGIN), v1(ORIGIN));
    await putJson(store, legacyLogStreamRegistryKey(ROOT, TENANT, O2), v1(O2));
    const listed = async () => (await streams(store)).map((s) => s.originId).sort();
    expect(await listed()).toEqual([ORIGIN, O2]);
    expect(await migrateLogStreamRegistry(store, ROOT, { apply: false })).toEqual({ legacy: 2, migrated: 0, dropped: 0, invalid: 0 });
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(false);
    // A crash mid-migration (one stream registered in v2, its v1 doc not yet deleted) lists it once.
    const { registerLogStream } = await import("../ledger-log-materialize.ts");
    await registerLogStream(store, ROOT, TENANT, ORIGIN);
    expect(await listed()).toEqual([ORIGIN, O2]);
    expect(await migrateLogStreamRegistry(store, ROOT, { apply: true })).toEqual({ legacy: 2, migrated: 2, dropped: 0, invalid: 0 });
    for (const o of [ORIGIN, O2]) {
      expect(await exists(store, legacyLogStreamRegistryKey(ROOT, TENANT, o))).toBe(false);
      expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, o))).toBe(true);
      expect(await exists(store, logKeys(ROOT, TENANT, o).identity)).toBe(true);
    }
    expect(await listed()).toEqual([ORIGIN, O2]);
    expect(await migrateLogStreamRegistry(store, ROOT, { apply: true })).toEqual({ legacy: 0, migrated: 0, dropped: 0, invalid: 0 });
  });

  test("migration: a v1 doc for an erased stream (no entry 0) is deleted, never re-registered into the erased directory", async () => {
    const { legacyLogStreamRegistryKey, logStreamRegistryKey, migrateLogStreamRegistry } = await import("../ledger-log-materialize.ts");
    const { admissionUserDir } = await import("../async-admission.ts");
    const store = fresh();
    const key = legacyLogStreamRegistryKey(ROOT, TENANT, ORIGIN);
    await putJson(store, key, { v: 1, ...TENANT, originId: ORIGIN });
    // Dry run: reported, nothing deleted or written.
    expect(await migrateLogStreamRegistry(store, ROOT, { apply: false })).toEqual({ legacy: 1, migrated: 0, dropped: 1, invalid: 0 });
    expect(await exists(store, key)).toBe(true);
    expect(await migrateLogStreamRegistry(store, ROOT, { apply: true })).toEqual({ legacy: 1, migrated: 0, dropped: 1, invalid: 0 });
    expect(await exists(store, key)).toBe(false);
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(false);
    expect(await keysUnder(store, admissionUserDir(ROOT, TENANT) + "/")).toEqual([]);
  });

  test("migration: a v1 doc with ids admission refuses is reported and left in place; the run still migrates the rest", async () => {
    const { legacyLogStreamRegistryKey, logStreamRegistryKey, logStreamsRoot, migrateLogStreamRegistry } = await import("../ledger-log-materialize.ts");
    const store = fresh();
    await enable(store);
    await putJson(store, legacyLogStreamRegistryKey(ROOT, TENANT, ORIGIN), { v: 1, ...TENANT, originId: ORIGIN });
    const bad = `${logStreamsRoot(ROOT)}${"b".repeat(40)}.json`;
    await putJson(store, bad, { v: 1, orgId: "../escape", workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId: ORIGIN });
    for (let i = 0; i < 2; i++)
      expect(await migrateLogStreamRegistry(store, ROOT, { apply: true })).toEqual({ legacy: i === 0 ? 2 : 1, migrated: i === 0 ? 1 : 0, dropped: 0, invalid: 1 });
    expect(await exists(store, bad)).toBe(true);
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(true);
  });
});

describe("slice 9: the user's stream index (written with log commit on, backfilled for older streams)", () => {
  const fresh = () => { const sc = scratchStore(); cleanups.push(sc.done); return sc.store; };
  const U2 = { ...TENANT, userId: "user-2" };

  test("backfill-index: dry run writes nothing; apply indexes every registered live stream (v1 or v2) that lacks it; idempotent; counts only", async () => {
    const { backfillLogIndex, legacyLogStreamRegistryKey, logStreamsRoot, registerLogStream } = await import("../ledger-log-materialize.ts");
    const { userLogStreams } = await import("../ledger-log-erase.ts");
    const store = fresh();
    // Two streams switched and registered (v2), one registered only in v1; then their index objects removed, as for
    // streams switched before the index existed. One more is already indexed.
    const a = logKeys(ROOT, TENANT, ORIGIN), b = logKeys(ROOT, TENANT, "origin-2"), c = logKeys(ROOT, U2, ORIGIN), d = logKeys(ROOT, TENANT, "origin-3");
    for (const [t, o] of [[TENANT, ORIGIN], [TENANT, "origin-2"], [U2, ORIGIN], [TENANT, "origin-3"]] as const) await enableLogMode(store, logKeys(ROOT, t, o), null, { writer: "operator", now: () => T0 });
    await registerLogStream(store, ROOT, TENANT, ORIGIN);
    await registerLogStream(store, ROOT, TENANT, "origin-3");
    await putJson(store, legacyLogStreamRegistryKey(ROOT, TENANT, "origin-2"), { v: 1, ...TENANT, originId: "origin-2" });
    await registerLogStream(store, ROOT, U2, ORIGIN);
    for (const k of [a, b, c]) await store.delete(objectKey(k.index));
    // A registered stream with no entry 0 (never switched, or its user's directory erased): skipped, nothing written.
    const gone = logKeys(ROOT, TENANT, "origin-gone");
    await putJson(store, legacyLogStreamRegistryKey(ROOT, TENANT, "origin-gone"), { v: 1, ...TENANT, originId: "origin-gone" });
    // A v1 doc with ids admission refuses: invalid, left alone.
    await putJson(store, `${logStreamsRoot(ROOT)}${"b".repeat(40)}.json`, { v: 1, orgId: "../escape", workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId: ORIGIN });
    expect((await userLogStreams(store, ROOT, TENANT)).map((k) => k.root)).toEqual([d.root]);

    const dry = await backfillLogIndex(store, ROOT, { apply: false });
    expect(dry).toEqual({ streams: 6, written: 3, present: 1, skipped: 1, corrupt: 0, invalid: 1 });
    for (const k of [a, b, c, gone]) expect(await exists(store, k.index)).toBe(false);
    expect(await backfillLogIndex(store, ROOT, { apply: true })).toEqual(dry);
    for (const k of [a, b, c, d]) expect(await exists(store, k.index)).toBe(true);
    expect(await exists(store, gone.index)).toBe(false);
    expect((await userLogStreams(store, ROOT, TENANT)).map((k) => k.root).sort()).toEqual([a.root, b.root, d.root].sort());
    expect((await userLogStreams(store, ROOT, U2)).map((k) => k.root)).toEqual([c.root]);
    // Rerun: everything present, nothing written.
    expect(await backfillLogIndex(store, ROOT, { apply: true })).toEqual({ streams: 6, written: 0, present: 4, skipped: 1, corrupt: 0, invalid: 1 });
    // The receipt carries counts only.
    const text = JSON.stringify(dry);
    for (const id of [TENANT.orgId, TENANT.userId, ORIGIN, a.root]) expect(text.includes(id)).toBe(false);
  });

  test("backfill-index: a stream whose entry 0 is gone but whose entries remain (LogCorrupt) is indexed all the same; a store fault stops the run", async () => {
    const { backfillLogIndex, registerLogStream } = await import("../ledger-log-materialize.ts");
    const base = fresh();
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    await enable(base);
    await registerLogStream(base, ROOT, TENANT, ORIGIN);
    await putJson(base, keys.entry(1), { v: 1 });
    await base.delete(objectKey(keys.entry(0)));
    await base.delete(objectKey(keys.index));
    expect(await backfillLogIndex(base, ROOT, { apply: true })).toEqual({ streams: 1, written: 1, present: 0, skipped: 0, corrupt: 1, invalid: 0 });
    expect(await exists(base, keys.index)).toBe(true);
    // A store fault on the index write: thrown (a rerun resumes), never counted as written.
    await base.delete(objectKey(keys.index));
    const f = faulty(base);
    f.setFault((key) => (key.includes("/ledger/log-index/") ? "before" : "ok"));
    await expect(backfillLogIndex(f.store, ROOT, { apply: true })).rejects.toMatchObject({ code: "transient" });
    expect(await exists(base, keys.index)).toBe(false);
  });

  test("an index PUT that fails fails registerLogStream before the identity doc and pointer, and enableLogMode before entry 0", async () => {
    const { logStreamRegistryKey, registerLogStream } = await import("../ledger-log-materialize.ts");
    const base = fresh();
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    const f = faulty(base);
    f.setFault((key) => (key.includes("/ledger/log-index/") ? "before" : "ok"));
    await expect(registerLogStream(f.store, ROOT, TENANT, ORIGIN)).rejects.toMatchObject({ code: "transient" });
    expect(await exists(base, keys.identity)).toBe(false);
    expect(await exists(base, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(false);
    await expect(enableLogMode(f.store, keys, null, { writer: "operator", now: () => T0 })).rejects.toMatchObject({ code: "transient" });
    expect(await exists(base, keys.entry(0))).toBe(false);
    // A PUT that landed but answered an error: the retry finds the same bytes and goes on (create-if-absent).
    f.setFault((key) => (key.includes("/ledger/log-index/") ? "after" : "ok"));
    await expect(enableLogMode(f.store, keys, null, { writer: "operator", now: () => T0 })).rejects.toMatchObject({ code: "transient" });
    f.setFault(undefined);
    await enableLogMode(f.store, keys, null, { writer: "operator", now: () => T0 });
    await registerLogStream(f.store, ROOT, TENANT, ORIGIN);
    expect(await exists(base, keys.entry(0))).toBe(true);
    expect(await exists(base, keys.index)).toBe(true);
  });

  test("INGEST_LEDGER_LOG_COMMIT=on with INGEST_LEDGER_LOG_ERASE off still (re)writes the index: admission indexes an unindexed stream", async () => {
    const e = edge("on");
    await writeAuthority(e.base);
    await enable(e.base);
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    await e.base.delete(objectKey(keys.index));
    await upload(e.base, "b1", ["r1"]);
    await rounds(e, 2);
    expect(await exists(e.base, keys.index)).toBe(true);
    expect((await keysUnder(e.base, keys.markers)).length).toBe((await entries(e.base)).length);
  });
});
