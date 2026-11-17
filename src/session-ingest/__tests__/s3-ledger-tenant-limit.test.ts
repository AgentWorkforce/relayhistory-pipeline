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
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
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

function edge(opts: { admission?: { mode: "async"; concurrency: number; scanIntervalMs: number; maxAttempts: number; learnUsers?: true }; direct?: boolean; ledger?: "s3";
  /** INGEST_S3_TENANT_LIMIT: on ({}, or with asyncMaxPerOrg) by default here; null = off. */
  limit?: { asyncMaxPerOrg?: number } | null; maxPerUser?: number;
  /** Share another edge's store (a second process: its consumer has none of the first one's notify hints). */
  base?: ObjectStore; userHintMax?: number; learnedUserMax?: number }) {
  const s = opts.base ? { store: opts.base, done: () => {} } : scratchStore();
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
  { startConsumer: false, nowMs: () => clock, onConsumer: (c) => { consumer = c; }, ...(opts.userHintMax !== undefined ? { userHintMax: opts.userHintMax } : {}),
    ...(opts.learnedUserMax !== undefined ? { learnedUserMax: opts.learnedUserMax } : {}),
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

  test("scan-only (no writer hint): over the cap across two machines of one user, the race still defers (not refused, not dead-lettered even at maxAttempts 1) and admits later", async () => {
    // Per-org 2 so only the per-user cap (1) binds, and the two uploads sit in different machine directories.
    // userHintMax 0: no writer hint is kept, so the per-directory fallback applies (a key found only by a scan).
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 1 }, direct: true, limit: { asyncMaxPerOrg: 2 }, userHintMax: 0 });
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

describe("INGEST_S3_TENANT_LIMIT=on, the pre-claim hint's per-directory branches", () => {
  const statusOf = (e: ReturnType<typeof edge>) => async (k: string) =>
    ((await uploadAdmissionStatus({ store: e.base, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), TENANT, k)) as { body: { state: string } }).body.state;

  test("org cap above the user cap: the same machine's second upload is held back by the per-directory count alone (no deferral)", async () => {
    // Per-org 4, per-user 1: the org check cannot hold the second upload back; only the per-directory check can.
    const e = edge({ admission: { mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 1 }, direct: true, limit: { asyncMaxPerOrg: 4 }, maxPerUser: 1 });
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "batch-u1", "u1");
    const k2 = await upload(e.base, "batch-u2", "u2");
    await e.consumer().scan();
    e.hold();
    const settling = e.consumer().settle();
    await until(() => e.blocked() > 0, "one admission to reach its writes");
    await new Promise((r) => setTimeout(r, 30));
    expect(e.consumer().status().inFlight).toBe(1);
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    e.open();
    await settling;
    const state = statusOf(e);
    expect([await state(k1), await state(k2)]).toEqual(["admitted", "admitted"]);
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    expect(e.sql).toEqual([]);
  });

  test("scan-only: after a deferral, the deferred directory's sibling is not started before Retry-After, and starts as soon as an admission in that directory ends", async () => {
    // Per-user 2, per-org 4. origin-1 and origin-2 each hold one of the user's two slots. Then two more origin-2
    // uploads arrive: one starts (its directory count is 1 < 2) and is deferred by the user cap. The memo keeps the
    // other from starting until the in-flight origin-2 admission ends, which frees the slot.
    const e = edge({ admission: { mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true, limit: { asyncMaxPerOrg: 4 }, maxPerUser: 2, userHintMax: 0 });
    await writeAuthority(e.base);
    await upload(e.base, "batch-a", "a", TENANT, "origin-1");
    await upload(e.base, "batch-b", "b", TENANT, "origin-2");
    await e.consumer().scan();
    e.hold();
    const settling = e.consumer().settle();
    const sessions = () => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size;
    await until(() => sessions() === 2, "both slots to be held in their writes");
    const k3 = await upload(e.base, "batch-c", "c", TENANT, "origin-2");
    const k4 = await upload(e.base, "batch-d", "d", TENANT, "origin-2");
    await e.consumer().scan(); // promotes c and d; the pump starts one of them
    const deferrals = () => e.logged.filter((f) => f.route === "ingest.admission" && f.outcome === "deferred").length;
    await until(() => deferrals() === 1, "one of the late uploads to be deferred");
    await new Promise((r) => setTimeout(r, 50));
    // The memo: the sibling was never started (no second deferral, no ledger doc for it).
    expect(deferrals()).toBe(1);
    const docs = await Promise.all(["batch-c", "batch-d"].map((b) => exists(e.base, s3LedgerKeys(ROOT, TENANT).batch("origin-2", b))));
    expect(docs.filter(Boolean).length).toBe(1);
    e.open();
    await settling; // b ends (not deferred) and clears the memo: the sibling starts at once, with no clock advance
    const state = statusOf(e);
    expect([await state(k3), await state(k4)].sort()).toEqual(["admitted", "pending"]);
    expect(deferrals()).toBe(1);
    // The deferred one is admitted after its Retry-After.
    e.advance(11_000);
    await e.consumer().settle();
    expect([await state(k3), await state(k4)]).toEqual(["admitted", "admitted"]);
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });

  test("the writer hint: one user's uploads from many machines never run more than the per-user cap at once, with no deferral", async () => {
    // Per-user 2, per-org 8, concurrency 8, five machines of one user. The per-directory count alone would start
    // all five (each directory holds one) and the limiter would defer three; the writer hint counts them together.
    const e = edge({ admission: { mode: "async", concurrency: 8, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true, limit: { asyncMaxPerOrg: 8 }, maxPerUser: 2 });
    await writeAuthority(e.base);
    const keys: string[] = [];
    for (let i = 1; i <= 5; i++) keys.push(await upload(e.base, `batch-m${i}`, `m${i}`, TENANT, `origin-${i}`));
    let peak = 0;
    e.hold();
    const settling = e.consumer().scan().then(() => e.consumer().settle());
    const sessions = () => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size;
    await until(() => sessions() === 2, "the user's two slots to be held in their writes");
    await new Promise((r) => setTimeout(r, 50));
    peak = Math.max(peak, e.consumer().status().inFlight);
    expect(peak).toBe(2);
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    e.open();
    await settling;
    // No starvation: every machine's upload is admitted, without one deferral or a clock advance.
    const state = statusOf(e);
    expect(await Promise.all(keys.map(state))).toEqual(["admitted", "admitted", "admitted", "admitted", "admitted"]);
    // Each terminal outcome dropped its hint: nothing lingers.
    expect(e.consumer().userHints()).toBe(0);
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });

  for (const learnUsers of [true, false] as const)
  test(`scan-only, one user on two machines: ${learnUsers ? "SELFHOST_ADMISSION_LEARN_USERS=on learns the user, so a later pass starts nothing while the user is at the cap" : "flag off (control): a later pass starts more and the limiter defers them again"}`, async () => {
    // Per-user 2, per-org 8, concurrency 8, userHintMax 0 (no notify hints: the pre-restart backlog a scan finds).
    // Three uploads on each of two machines of one user. The first pass starts two per directory (four); the limiter
    // admits two and defers two. With the flag the deferred two taught both directories their user, so after their
    // Retry-After the next pass sees the user at its cap (the two held admissions moved to the user's group) and starts
    // nothing; without it each directory reads below the cap and the pass burns more claims on deferrals.
    const e = edge({ admission: { mode: "async", concurrency: 8, scanIntervalMs: 15_000, maxAttempts: 20, ...(learnUsers ? { learnUsers: true as const } : {}) },
      direct: true, limit: { asyncMaxPerOrg: 8 }, maxPerUser: 2, userHintMax: 0 });
    await writeAuthority(e.base);
    const keys: string[] = [];
    for (const origin of ["origin-1", "origin-2"]) for (let i = 1; i <= 3; i++) keys.push(await upload(e.base, `batch-${origin}-${i}`, `${origin}-${i}`, TENANT, origin));
    await e.consumer().scan();
    e.hold();
    const settling = e.consumer().settle();
    const deferrals = () => e.logged.filter((f) => f.route === "ingest.admission" && f.outcome === "deferred").length;
    const sessions = () => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size;
    await until(() => sessions() === 2 && deferrals() === 2, "two admissions held at the user's cap and two deferred");
    // Past the deferrals' Retry-After, with the user's two admissions still held: the next pass.
    e.advance(11_000);
    await e.consumer().scan();
    await new Promise((r) => setTimeout(r, 80));
    if (learnUsers) {
      expect(deferrals()).toBe(2);
      expect(e.consumer().status().inFlight).toBe(2);
    } else {
      expect(deferrals()).toBeGreaterThan(2);
    }
    e.open();
    await settling;
    for (let i = 0; i < 10 && (await Promise.all(keys.map(statusOf(e)))).some((s) => s !== "admitted"); i++) { e.advance(11_000); await e.consumer().scan(); await e.consumer().settle(); }
    expect(await Promise.all(keys.map(statusOf(e)))).toEqual(keys.map(() => "admitted"));
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    // Every counter drained: nothing held, no group or org count left behind (moved counts included).
    expect(e.consumer().learnedUsers()).toMatchObject({ held: 0, groups: 0, orgs: 0 });
    if (learnUsers) expect(e.consumer().learnedUsers().directories).toBe(2);
    else expect(e.consumer().learnedUsers().directories).toBe(0);
    expect(e.sql).toEqual([]);
  });

  test("learnUsers, learnedUserMax 1: a second directory evicts the first, which falls back to its directory group; everything drains to zero", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 8, scanIntervalMs: 15_000, maxAttempts: 20, learnUsers: true },
      direct: true, limit: { asyncMaxPerOrg: 8 }, maxPerUser: 2, userHintMax: 0, learnedUserMax: 1 });
    await writeAuthority(e.base);
    const keys: string[] = [];
    for (const origin of ["origin-1", "origin-2"]) for (let i = 1; i <= 3; i++) keys.push(await upload(e.base, `batch-ev-${origin}-${i}`, `ev-${origin}-${i}`, TENANT, origin));
    await e.consumer().scan();
    for (let i = 0; i < 12 && (await Promise.all(keys.map(statusOf(e)))).some((s) => s !== "admitted"); i++) { await e.consumer().settle(); e.advance(11_000); await e.consumer().scan(); }
    expect(await Promise.all(keys.map(statusOf(e)))).toEqual(keys.map(() => "admitted"));
    // Bounded: never more than one learned directory, and no counter left behind by an eviction mid-flight.
    expect(e.consumer().learnedUsers()).toEqual({ directories: 1, held: 0, groups: 0, orgs: 0 });
    expect(e.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 0 });
    expect(e.sql).toEqual([]);
  });

  test("SELFHOST_ADMISSION_LEARN_USERS without the tenant limit is refused at startup (it would do nothing)", () => {
    expect(() => edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 20, learnUsers: true }, direct: true, limit: null }))
      .toThrow("SELFHOST_ADMISSION_LEARN_USERS=on needs INGEST_S3_TENANT_LIMIT=on");
  });

  test("the writer hint never holds back another user of the same org", async () => {
    const other = { ...TENANT, userId: "user-2" };
    const e = edge({ admission: { mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true, limit: { asyncMaxPerOrg: 4 }, maxPerUser: 1 });
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "batch-x1", "x1", TENANT, "origin-1");
    const k2 = await upload(e.base, "batch-x2", "x2", TENANT, "origin-2");
    const k3 = await upload(e.base, "batch-y1", "y1", other, "origin-9");
    e.hold();
    const settling = e.consumer().scan().then(() => e.consumer().settle());
    const sessions = () => new Set(e.gatedKeys().map((k) => /\/s=([^/]+)\//.exec(k)?.[1])).size;
    // One of user-1's and user-2's single upload run together; user-1's second waits for its own slot.
    await until(() => sessions() === 2, "one admission per user in its writes");
    await new Promise((r) => setTimeout(r, 30));
    expect(e.consumer().status().inFlight).toBe(2);
    e.open();
    await settling;
    expect([await statusOf(e)(k1), await statusOf(e)(k2)]).toEqual(["admitted", "admitted"]);
    const st = (await uploadAdmissionStatus({ store: e.base, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), other, k3)) as { body: { state: string } };
    expect(st.body.state).toBe("admitted");
    expect(e.logged.some((f) => f.outcome === "deferred")).toBe(false);
  });

  test("the hint map is bounded: past userHintMax the oldest key loses its hint and falls back to its directory (the limiter still defers, never over-admits)", async () => {
    // userHintMax 1, per-user 1, two machines: the second hint evicts the first, so the first key is counted by its
    // directory; both start, and the limiter defers one (safe), then it is admitted after its Retry-After.
    const e = edge({ admission: { mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true, limit: { asyncMaxPerOrg: 4 }, maxPerUser: 1, userHintMax: 1 });
    await writeAuthority(e.base);
    const k1 = await upload(e.base, "batch-b1", "b1", TENANT, "origin-1");
    const k2 = await upload(e.base, "batch-b2", "b2", TENANT, "origin-2");
    e.hold();
    const settling = e.consumer().scan().then(() => e.consumer().settle());
    await until(() => e.logged.some((f) => f.outcome === "deferred"), "the evicted key's start to be deferred");
    e.open();
    await settling;
    e.advance(11_000);
    await e.consumer().settle();
    expect([await statusOf(e)(k1), await statusOf(e)(k2)]).toEqual(["admitted", "admitted"]);
    expect(e.logged.filter((f) => f.outcome === "deferred").length).toBe(1);
    expect(e.logged.some((f) => f.outcome === "dead_lettered" || f.outcome === "refused")).toBe(false);
  });

  test("a status-less store failure logs a closed store_cause class, never the error text", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true });
    await writeAuthority(e.base);
    await upload(e.base, "batch-c", "c");
    await e.consumer().scan();
    const batchDoc = s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-c");
    let fail = true;
    const original = e.base.put.bind(e.base);
    (e.base as { put: ObjectStore["put"] }).put = (async (key: Parameters<ObjectStore["put"]>[0], ...rest: unknown[]) => {
      if (fail && String(key) === batchDoc) {
        fail = false;
        throw new ObjectStoreError("transient", "s3", "secret key path: request failed", { cause: Object.assign(new Error("socket hang up at https://bucket/secret"), { code: "ECONNRESET" }), transportCause: "econnreset" });
      }
      return (original as (...a: unknown[]) => unknown)(key, ...rest);
    }) as ObjectStore["put"];
    await e.consumer().settle();
    const retry = e.logged.find((f) => f.route === "ingest.admission" && f.outcome === "retry");
    expect(retry).toMatchObject({ error_code: "ledger_store_transient", stage: expect.any(String), store_cause: "econnreset" });
    expect(retry).not.toHaveProperty("store_status");
    expect(JSON.stringify(e.logged)).not.toContain("secret");
    expect(e.sql).toEqual([]);
  });

  test("a store failure in the claim logs store_status and stage on the consumer's retry line", async () => {
    const e = edge({ admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 20 }, direct: true });
    await writeAuthority(e.base);
    await upload(e.base, "batch-s", "s");
    await e.consumer().scan();
    const batchDoc = s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-s");
    let fail = true;
    const original = e.base.put.bind(e.base);
    (e.base as { put: ObjectStore["put"] }).put = (async (key: Parameters<ObjectStore["put"]>[0], ...rest: unknown[]) => {
      if (fail && String(key) === batchDoc) { fail = false; throw new ObjectStoreError("transient", "fs", "secret key path", { status: 503 }); }
      return (original as (...a: unknown[]) => unknown)(key, ...rest);
    }) as ObjectStore["put"];
    await e.consumer().settle();
    const retry = e.logged.find((f) => f.route === "ingest.admission" && f.outcome === "retry");
    expect(retry).toMatchObject({ error_code: expect.any(String), stage: expect.any(String), store_status: 503 });
    expect(JSON.stringify(e.logged)).not.toContain("secret key path");
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
    // B's status is written before its admitOne settles, so wait for B to leave the in-flight set too.
    await until(() => e.consumer().status().inFlight === 2, "tenant B's admission to settle");
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
