/**
 * The per-replica tenant limit on the self-host ingest edge with INGEST_LEDGER=s3 (tenant-limiter.ts), no database:
 *   - the synchronous POST /v1/delivery/batches over the cap answers the SQL slot refusal byte for byte: 429
 *     delivery_busy with Retry-After 2..10, the claim left pending, and its retry admits once the slot is free;
 *   - the async consumer over the cap DEFERS a direct upload (never refuses, drops or dead-letters it, even with
 *     maxAttempts 1) and admits it later.
 * A slot is held by gating the canonical writes of the first admission (the slot is taken after the claim, before
 * the writes), so the second one meets the cap deterministically.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { outcomeKey, receiptDir } from "../async-admission.ts";
import { fenceAuthorityKey, s3LedgerKeys } from "../ledger-s3.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime } from "../selfhost-server.ts";
import { uploadAdmissionStatus, uploadDirectConfig, uploadStatusKey } from "../upload-admission.ts";
import { encodeUploadWitness, parseUploadKey, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } from "../upload-inbox-contract.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const V = JSON.parse(readFileSync(join(import.meta.dir, "../../../docs/contracts/relay-upload-inbox-v1.vectors.json"), "utf8"));
const WITNESS_KEY = V.witness.testOnlyKey as string;
const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const CANONICAL = `${ROOT}/${TENANT.orgId}/v1/`;
const TENANT_B = { orgId: "org-b", workspaceId: "ws-b", userId: "user-9" };

const cleanups: (() => unknown)[] = [];
afterEach(async () => { for (const f of cleanups.splice(0).reverse()) await f(); });

const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const writeAuthority = (store: ObjectStore) => store.put(objectKey(fenceAuthorityKey(ROOT)),
  new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: T0 })), { contentType: "application/json" });

/** A store whose writes into the canonical layout wait on `open()` while `hold` is set. */
function gatedStore(base: ObjectStore) {
  let release: (() => void) | undefined;
  let gate: Promise<void> | undefined;
  let blocked = 0;
  const keys: string[] = [];
  const store = new Proxy(base, {
    get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (typeof v !== "function") return v;
      return async (...args: unknown[]) => {
        if (gate && /^put/i.test(String(prop)) && String(args[0]).startsWith(CANONICAL)) { blocked++; keys.push(String(args[0])); await gate; }
        return v.apply(target, args);
      };
    },
  });
  return {
    store,
    hold() { gate = new Promise<void>((r) => { release = r; }); },
    open() { const r = release; gate = undefined; release = undefined; r?.(); },
    blocked: () => blocked,
    gatedKeys: () => [...keys],
  };
}

async function until(cond: () => boolean | Promise<boolean>, what: string): Promise<void> {
  for (let i = 0; i < 400; i++) { if (await cond()) return; await new Promise((r) => setTimeout(r, 5)); }
  throw new Error(`timed out waiting for ${what}`);
}

function edge(opts: { admission?: { mode: "async"; concurrency: number; scanIntervalMs: number; maxAttempts: number }; direct?: boolean; ledger?: "s3";
  /** INGEST_S3_TENANT_LIMIT: on ({}, or with asyncMaxPerOrg) by default here; null = off. */
  limit?: { asyncMaxPerOrg?: number } | null; maxPerUser?: number }) {
  const s = scratchStore();
  cleanups.push(s.done);
  const g = gatedStore(s.store);
  const sql: string[] = [];
  const logged: Record<string, unknown>[] = [];
  let clock = T0;
  let consumer: SelfhostAdmissionConsumer | undefined;
  const runtime = selfhostIngestRuntime({
    keyRoot: ROOT, maxPerUser: opts.maxPerUser ?? 1, maxPerOrg: 32, ledger: opts.ledger ?? "s3",
    ...(opts.limit === null ? {} : { s3TenantLimit: opts.limit ?? {} }),
    ...(opts.admission ? { admission: opts.admission } : {}),
    ...(opts.direct ? { uploadDirect: { config: uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), witnessKey: WITNESS_KEY, scanBatch: 16 } } : {}),
  }, { run: (async (text: string) => { sql.push(text); throw new Error("SQL runner called on the S3 ledger path"); }) as never, store: g.store,
    verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} },
  { startConsumer: false, nowMs: () => clock, onConsumer: (c) => { consumer = c; },
    log: { info: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), warn: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), error: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}) } as never });
  cleanups.push(() => runtime.close());
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] } as unknown as IngestIdentity;
  const post = async (batchId: string, recordId: string) => {
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: recordId, session_id: `sess-${recordId}` })], { batch_id: batchId }) });
    const request = new Request("https://history.selfhost.test/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json" } });
    runtime.bind(request, identity);
    return runtime.app.fetch(request);
  };
  return { ...g, base: s.store, sql, logged, post, consumer: () => consumer!, advance: (ms: number) => { clock += ms; } };
}

describe("INGEST_LEDGER=s3, synchronous POST /v1/delivery/batches at the tenant cap", () => {
  test("over the cap: 429 delivery_busy with Retry-After 2..10 (the SQL slot refusal); the claim stays pending and the retry admits", async () => {
    const e = edge({});
    await writeAuthority(e.base);
    e.hold();
    const first = e.post("batch-1", "r1");
    await until(() => e.blocked() > 0, "the first batch to hold its slot");
    const busy = await e.post("batch-2", "r2");
    expect(busy.status).toBe(429);
    const after = Number(busy.headers.get("retry-after"));
    expect(Number.isInteger(after) && after >= 2 && after <= 10).toBe(true);
    expect(((await busy.json()) as { error: { code: string } }).error.code).toBe("delivery_busy");
    // Nothing of batch-2 reached the canonical layout; its claim is pending, so the retry resumes it.
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-2"))).toMatchObject({ state: "pending" });
    e.open();
    expect((await first).status).toBe(200);
    // The slot was released in finally: the retry of batch-2 is admitted.
    const retried = await e.post("batch-2", "r2");
    expect(retried.status).toBe(200);
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-2"))).toMatchObject({ state: "committed" });
    expect(e.sql).toEqual([]);
  });

  test("a failed admission releases its slot (finally): the next batch is not refused", async () => {
    const e = edge({});
    // No fence-authority marker: the claim refuses (503) before any slot; then a write fault after the slot.
    expect((await e.post("batch-x", "rx")).status).toBe(503);
    await writeAuthority(e.base);
    let fail = true;
    const original = e.base.put.bind(e.base);
    (e.base as { put: ObjectStore["put"] }).put = (async (key: Parameters<ObjectStore["put"]>[0], ...rest: unknown[]) => {
      if (fail && String(key).startsWith(CANONICAL)) throw Object.assign(new Error("store down"), { code: "unavailable" });
      return (original as (...a: unknown[]) => unknown)(key, ...rest);
    }) as ObjectStore["put"];
    expect((await e.post("batch-f", "rf")).status).toBe(503);
    fail = false;
    expect((await e.post("batch-ok", "rok")).status).toBe(200);
    expect(e.sql).toEqual([]);
  });
});

/** A desktop upload: zstd JSON delivery request at its content-addressed key, the witness in its metadata. */
async function upload(store: ObjectStore, batchId: string, recordId: string, tenant: typeof TENANT = TENANT, originId = "origin-1") {
  const b = await batch([record({ record_id: recordId, session_id: `sess-${recordId}`, origin_id: originId })], { batch_id: batchId, origin_id: originId, account_id: await deliveryAccount(tenant) });
  const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: ROOT, ...tenant }), { originId, batchId, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
  const witness = encodeUploadWitness(parseUploadWitnessKey(WITNESS_KEY), { v: 1, org: tenant.orgId, ws: tenant.workspaceId, user: tenant.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
  await store.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
  return key;
}

describe("INGEST_LEDGER=s3, the async consumer at the tenant cap (direct uploads)", () => {
  test("the pre-claim hint: the same machine's next upload is NOT started while it is at the cap (no deferral, no claim), and is admitted once the slot frees", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 1 }, direct: true });
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "batch-u1", "u1");
    const k2 = await upload(e.base, "batch-u2", "u2");
    await e.consumer().scan();
    e.hold();
    const settling = e.consumer().settle();
    await until(() => e.blocked() > 0, "the first admission to reach its writes");
    // The second was never started: no ledger batch doc exists for it, and nothing was deferred.
    expect(await exists(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-u2"))).toBe(false);
    expect(e.consumer().status().inFlight).toBe(1);
    e.open();
    await settling; // the first's end re-pumps: the second starts and is admitted, with no Retry-After wait
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    const status = async (k: string) => (await uploadAdmissionStatus({ store: e.base, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), TENANT, k)) as { body: { state: string } };
    expect([(await status(k1)).body.state, (await status(k2)).body.state]).toEqual(["admitted", "admitted"]);
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });

  test("over the cap across two machines of one user (the hint cannot see it): the race still defers (not refused, not dead-lettered even at maxAttempts 1) and admits later", async () => {
    // Per-org 2 so only the per-user cap (1) binds, and the two uploads sit in different machine directories.
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 1 }, direct: true, limit: { asyncMaxPerOrg: 2 } });
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "batch-u1", "u1");
    const k2 = await upload(e.base, "batch-u2", "u2", TENANT, "origin-2");
    await e.consumer().scan(); // promotes both uploads into the admission inbox (no ledger yet)
    e.hold();
    const settling = e.consumer().settle();
    await until(() => e.logged.some((f) => f.outcome === "deferred"), "the second admission to be deferred");
    e.open();
    await settling;
    const deferred = e.logged.filter((f) => f.route === "ingest.admission" && f.outcome === "deferred");
    expect(deferred).toEqual([{ route: "ingest.admission", outcome: "deferred", error_code: "tenant_limit", duration_ms: expect.any(Number), attempt: 1 }]);
    expect(e.logged.some((f) => f.outcome === "dead_lettered" || f.outcome === "refused")).toBe(false);
    expect(e.consumer().status()).toMatchObject({ backlog: 1, deadLetters: 0 });
    const status = async (k: string) => (await uploadAdmissionStatus({ store: e.base, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), TENANT, k)) as { body: { state: string } };
    const states = [(await status(k1)).body.state, (await status(k2)).body.state].sort();
    expect(states).toEqual(["admitted", "pending"]);
    // Not due yet: a settle before the Retry-After admits nothing.
    await e.consumer().settle();
    expect(e.consumer().status().backlog).toBe(1);
    // After the Retry-After (at most 10 s) the deferred upload is admitted.
    e.advance(11_000);
    await e.consumer().settle();
    expect([(await status(k1)).body.state, (await status(k2)).body.state]).toEqual(["admitted", "admitted"]);
    for (const [k, origin] of [[k1, "origin-1"], [k2, "origin-2"]] as const) {
      const handed = await json(e.base, uploadStatusKey(parseUploadKey(k) as never));
      expect(await exists(e.base, outcomeKey(receiptDir(ROOT, TENANT, origin, handed.receiptId), "admitted"))).toBe(true);
    }
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-u1"))).toMatchObject({ state: "committed" });
    expect(await json(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-2", "batch-u2"))).toMatchObject({ state: "committed" });
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });
});

describe("INGEST_S3_TENANT_LIMIT off (the default): byte-identical to before", () => {
  test("sync: a second concurrent batch of the same user is admitted (no 429 from a limiter)", async () => {
    const e = edge({ limit: null });
    await writeAuthority(e.base);
    e.hold();
    const first = e.post("batch-1", "r1");
    await until(() => e.blocked() > 0, "the first batch to reach its writes");
    const second = e.post("batch-2", "r2");
    await until(() => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size === 2, "the second batch to reach its writes too");
    e.open();
    expect([(await first).status, (await second).status]).toEqual([200, 200]);
    expect(e.sql).toEqual([]);
  });

  test("async: the consumer never defers; both uploads are in their writes at once and admitted", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 1 }, direct: true, limit: null });
    await writeAuthority(e.base);
    await upload(e.base, "batch-u1", "u1");
    await upload(e.base, "batch-u2", "u2");
    await e.consumer().scan();
    e.hold();
    const settling = e.consumer().settle();
    const sessions = () => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size;
    await until(() => sessions() === 2, "both admissions to reach their writes");
    e.open();
    await settling;
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
  });
});

describe("INGEST_S3_TENANT_LIMIT=on, INGEST_ASYNC_MAX_PER_ORG: one org's backlog never holds the whole consumer", () => {
  test("concurrency 4 (per-org default 2): tenant A's backlog is capped at 2 in flight while tenant B is admitted", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true, maxPerUser: 4 });
    await writeAuthority(e.base);
    const a: string[] = [];
    for (let i = 1; i <= 6; i++) a.push(await upload(e.base, `batch-a${i}`, `a${i}`));
    const b = await upload(e.base, "batch-b1", "b1", TENANT_B);
    await e.consumer().scan();
    e.hold(); // only org A's canonical writes wait
    const settling = e.consumer().settle();
    const config = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" });
    const stateOf = async (k: string, t: typeof TENANT) => ((await uploadAdmissionStatus({ store: e.base, keyRoot: ROOT }, config, t, k)) as { body: { state: string } }).body.state;
    await until(async () => (await stateOf(b, TENANT_B)) === "admitted", "tenant B to be admitted");
    await until(() => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size === 2, "two of A's admissions to reach their writes");
    // The pre-claim hint: the rest of A's backlog is left due, not started (so not deferred), at the org cap.
    expect(e.consumer().status().inFlight).toBe(2);
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    const inWrites = new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1]));
    expect(inWrites.size).toBe(2);
    expect(await Promise.all(a.map((k) => stateOf(k, TENANT)))).toEqual(Array(6).fill("pending"));
    e.open();
    await settling;
    // Each ending admission re-pumps the next (at most 2 of org A at once): all admitted without a Retry-After wait.
    expect(await Promise.all(a.map((k) => stateOf(k, TENANT)))).toEqual(Array(6).fill("admitted"));
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });
});
