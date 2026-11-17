/**
 * INGEST_S3_ERASE_FENCE (issue #613, s3-erase-fence.ts): with INGEST_LEDGER=s3, an operator per-session erase
 * blocks re-admission of the erased session on every admission path, with no database (the SQL erase service and
 * the SQL write guard are in-memory fakes; the store is a scratch fs store):
 *   - erase, then re-upload of the same session: 409 operator_session_erased on the synchronous POST, and a
 *     terminal refusal in the async consumer of ANOTHER process (no SQL erase fence there, only the tombstones);
 *   - the physical layout pair is fenced too (a session id that maps to the same object namespace);
 *   - both guards installed: an erased session is refused by whichever answers first;
 *   - fail closed: a tombstone write failure is a 503 from the erase route (no receipt), and a tombstone read
 *     failure is a 503 with nothing written;
 *   - flag off: the S3 path still refuses the erase parts, no tombstone is read or written, and the writes of an
 *     upload sequence are byte-identical to the same sequence with the flag on and no erase;
 *   - the selfhost-config refusal matrix.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { objectKey, ObjectStoreError, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { ARTIFACT_KEYS, SessionEraseServiceError, type EraseKey } from "../../session-erase/receipt.ts";
import type { SessionWriteGuard } from "../../session-erase/runtime.ts";
import { SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { fenceAuthorityKey } from "../ledger-s3.ts";
import { sessionEraseTombstoneKey, s3EraseFenceMode } from "../s3-erase-fence.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime } from "../selfhost-server.ts";
import { SESSION_ERASE_PREFIX, type SessionEraseService } from "../session-erase.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const SECRET = "test-only-erase-route-secret-0123456789";
const CANONICAL = `${ROOT}/${TENANT.orgId}/v1/`;
const TOMBSTONES = `${ROOT}/.ledger/session-erase/`;

const cleanups: (() => unknown)[] = [];
afterEach(async () => { for (const f of cleanups.splice(0).reverse()) await f(); });

const writeAuthority = (store: ObjectStore) => store.put(objectKey(fenceAuthorityKey(ROOT)),
  new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: T0 })), { contentType: "application/json" });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));

/** The SQL erase ledger, in memory: begin records the fence and answers an in_progress receipt. */
function fakeSqlErase() {
  const erased = new Set<string>();
  const overCap = new Set<string>();
  const calls: EraseKey[] = [];
  const id = (k: EraseKey) => JSON.stringify([k.orgId, k.workspaceId, k.source, k.sessionId]);
  const receipt = (k: EraseKey, requestId: string) => ({ ...k, requestId, status: "in_progress", requestedAt: new Date(T0).toISOString(), completedAt: null,
    unreadable: true, unreadableAt: new Date(T0).toISOString(), bytesErased: false, bytesErasedAt: null, uploadsFenced: true, legacyScope: "excluded",
    artifacts: Object.fromEntries(ARTIFACT_KEYS.map(a => [a, { count: 0, state: "pending" }])), failure: null });
  const service: SessionEraseService = {
    async erase(key, request) {
      calls.push(key); erased.add(id(key));
      // The owner cap (RH010 at the fence step): the SQL fence and a failed receipt are kept, the begin throws.
      if (overCap.has(id(key))) throw new SessionEraseServiceError("too_many_owners");
      return receipt(key, request.requestId);
    },
    async receipt(key) { return erased.has(id(key)) ? receipt(key, "req") : null; },
  };
  /** The SQL write guard, in memory: refuses a pair fenced in SQL (`fence` adds one only SQL knows, an alias). */
  const guardCalls: [string, string][][] = [];
  const guard: SessionWriteGuard = async (tenant, sessions, work) => {
    guardCalls.push(sessions.map(s => [...s] as [string, string]));
    if (sessions.some(([source, session]) => erased.has(JSON.stringify([tenant.orgId, tenant.workspaceId, source, session]))))
      throw Object.assign(new (await import("../vendor/delivery/delivery-contracts.ts")).DeliveryError("operator_session_erased", 409, "sql"), {});
    return work();
  };
  return { service, guard, calls, guardCalls, fence: (k: EraseKey) => erased.add(id(k)), overCap: (k: EraseKey) => overCap.add(id(k)) };
}

/** sha256 of the bytes, with the ledger's random claim token (ledger-s3.ts newToken, a UUID per claim) masked. */
const digest = (bytes: Uint8Array) => createHash("sha256")
  .update(new TextDecoder().decode(bytes).replace(/"claimToken":"[0-9a-f-]{36}"/g, '"claimToken":"<token>"')).digest("hex");

/** Records every put (key and sha256 of the bytes) and can fail chosen operations. */
function recordingStore(base: ObjectStore) {
  const puts: string[] = [];
  const heads: string[] = [];
  let fail: ((op: string, key: string) => boolean) | undefined;
  const store = new Proxy(base, {
    get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (typeof v !== "function") return v;
      return async (...args: unknown[]) => {
        const op = String(prop), key = String(args[0]);
        if (fail?.(op, key)) throw new ObjectStoreError("transient", "test", "injected store failure");
        if (op === "head") heads.push(key);
        const result = await v.apply(target, args);
        if (/^put/i.test(op)) puts.push(`${key} ${args[1] instanceof Uint8Array ? digest(args[1]) : typeof args[1]}`);
        return result;
      };
    },
  });
  return { store, puts, heads, failWhen: (f: typeof fail) => { fail = f; } };
}

function edge(opts: { fence: boolean; base?: ObjectStore; admission?: { mode: "async"; concurrency: number; scanIntervalMs: number; maxAttempts: number };
  sql?: ReturnType<typeof fakeSqlErase> | null }) {
  const s = opts.base ? { store: opts.base, done: () => {} } : scratchStore();
  cleanups.push(s.done);
  const r = recordingStore(s.store);
  const logged: Record<string, unknown>[] = [];
  let consumer: SelfhostAdmissionConsumer | undefined;
  const sql = opts.sql === null ? undefined : opts.sql ?? fakeSqlErase();
  const runtime = selfhostIngestRuntime({
    keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 32, ledger: "s3",
    ...(opts.fence ? { s3EraseFence: true as const } : {}),
    ...(sql ? { sessionErase: { enabled: true as const, secrets: [SECRET] } } : {}),
    ...(opts.admission ? { admission: opts.admission } : {}),
  }, { run: (async () => { throw new Error("SQL runner called on the S3 ledger path"); }) as never, store: r.store,
    verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {},
    ...(sql ? { withSessionWrites: sql.guard, sessionEraseService: sql.service } : {}) },
  { startConsumer: false, nowMs: () => T0, onConsumer: (c) => { consumer = c; },
    log: { info: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), warn: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), error: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}) } as never });
  cleanups.push(() => runtime.close());
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] } as unknown as IngestIdentity;
  const post = async (batchId: string, recordId: string, sessionId: string) => {
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: recordId, session_id: sessionId })], { batch_id: batchId }) });
    const request = new Request("https://history.selfhost.test/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json" } });
    runtime.bind(request, identity);
    return runtime.app.fetch(request);
  };
  const erase = (sessionId: string, source = "claude") => runtime.app.fetch(new Request(
    `https://history.selfhost.test${SESSION_ERASE_PREFIX}/organizations/${TENANT.orgId}/workspaces/${TENANT.workspaceId}/sources/${source}/sessions/${encodeURIComponent(sessionId)}`,
    { method: "POST", headers: { authorization: `Bearer ${SECRET}`, "content-type": "application/json" }, body: JSON.stringify({ requestId: "req-1", actor: { kind: "user", id: "operator-1" } }) }));
  return { ...r, base: s.store, sql, logged, post, erase, consumer: () => consumer! };
}
const key = (sessionId: string): EraseKey => ({ orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId });
const errorCode = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;

describe("INGEST_S3_ERASE_FENCE=on: an operator erase blocks re-admission on the S3 ledger", () => {
  test("synchronous path: erase, then re-upload of the same session is refused 409 operator_session_erased, nothing written", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    expect((await e.post("batch-1", "r1", "sess-1")).status).toBe(200);
    const erased = await e.erase("sess-1");
    expect(erased.status).toBe(202);
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess-1"))).toBe(true);
    expect(await json(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess-1")))
      .toEqual({ v: 1, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId: "sess-1", atMs: T0 });
    const before = e.puts.length;
    const again = await e.post("batch-2", "r2", "sess-1");
    expect(again.status).toBe(409);
    expect(await errorCode(again)).toBe("operator_session_erased");
    expect(e.puts.slice(before).filter(p => p.startsWith(CANONICAL))).toEqual([]);
    // Refused by the tombstone before the SQL guard was asked.
    expect(e.sql!.guardCalls.length).toBe(1);
    // Another session is unaffected.
    expect((await e.post("batch-3", "r3", "sess-2")).status).toBe(200);
  });

  test("async consumer in another process (tombstones only, no SQL erase fence): the re-upload is refused terminally, never admitted", async () => {
    const eraser = edge({ fence: true });
    await writeAuthority(eraser.base);
    expect((await eraser.erase("sess-1")).status).toBe(202);
    const consumerEdge = edge({ fence: true, base: eraser.base, sql: null, admission: { mode: "async", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3 } });
    const accepted = await consumerEdge.post("batch-2", "r2", "sess-1");
    expect(accepted.status).toBe(200);
    expect(accepted.headers.get("x-relay-admission")).toContain("queued");
    const before = consumerEdge.puts.length;
    await consumerEdge.consumer().scan();
    await consumerEdge.consumer().settle();
    expect(consumerEdge.logged.filter(f => f.route === "ingest.admission").map(f => [f.outcome, f.error_code]))
      .toContainEqual(["refused", "operator_session_erased"]);
    expect(consumerEdge.logged.some(f => f.outcome === "admitted")).toBe(false);
    expect(consumerEdge.puts.slice(before).filter(p => p.startsWith(CANONICAL))).toEqual([]);
    expect(consumerEdge.consumer().status()).toMatchObject({ backlog: 0 });
    // The same consumer still admits another session.
    expect((await consumerEdge.post("batch-3", "r3", "sess-2")).status).toBe(200);
    await consumerEdge.consumer().scan();
    await consumerEdge.consumer().settle();
    expect(consumerEdge.logged.some(f => f.route === "ingest.admission" && f.outcome === "admitted")).toBe(true);
  });

  test("the physical layout pair is fenced too: a session id that maps to the erased object namespace is refused", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    expect((await e.erase("sess:1")).status).toBe(202);
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess:1"))).toBe(true);
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess_1"))).toBe(true);
    const physical = await e.post("batch-1", "r1", "sess_1");
    expect(physical.status).toBe(409);
    expect(await errorCode(physical)).toBe("operator_session_erased");
  });

  test("both guards installed: refused by whichever answers first (the S3 tombstone, or the SQL fence for a pair only SQL knows)", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    // Tombstoned: the S3 guard answers first, the SQL guard is never asked.
    expect((await e.erase("sess-1")).status).toBe(202);
    const s3First = await e.post("batch-1", "r1", "sess-1");
    expect([s3First.status, await errorCode(s3First)]).toEqual([409, "operator_session_erased"]);
    expect(e.sql!.guardCalls).toEqual([]);
    // Fenced only in SQL (an alias id the erase begin fenced; no tombstone): the S3 guard passes, the SQL guard refuses.
    e.sql!.fence(key("alias-1"));
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "alias-1"))).toBe(false);
    const sqlSecond = await e.post("batch-2", "r2", "alias-1");
    expect([sqlSecond.status, await errorCode(sqlSecond)]).toEqual([409, "operator_session_erased"]);
    expect(e.sql!.guardCalls).toEqual([[["claude", "alias-1"]]]);
  });
});

describe("INGEST_S3_ERASE_FENCE=on fails closed", () => {
  test("a tombstone write failure: the erase route answers 503 unavailable (no receipt); the retry writes it and succeeds", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    e.failWhen((op, k) => /^put/i.test(op) && k.startsWith(TOMBSTONES));
    const failed = await e.erase("sess-1");
    expect(failed.status).toBe(503);
    expect(await failed.json()).toEqual({ error: "unavailable", feature: "session_erase" });
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess-1"))).toBe(false);
    e.failWhen(undefined);
    expect((await e.erase("sess-1")).status).toBe(202);
    // Idempotent: a second erase meets the existing (create-only) tombstone and still answers the receipt.
    expect((await e.erase("sess-1")).status).toBe(202);
    // A create-only put of identical bytes is an idempotent success; different bytes (another atMs) are already_exists,
    // also success: either way one tombstone, never overwritten with other content.
    expect(new Set(e.puts.filter(p => p.startsWith(TOMBSTONES))).size).toBe(1);
  });

  test("a partial tombstone write (the first pair written, the second fails): 503; the retry completes it, idempotently", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    // "sess:1" has two pairs (canonical and the physical "sess_1"); fail only the physical one.
    const physical = sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess_1");
    e.failWhen((op, k) => /^put/i.test(op) && k === physical);
    const failed = await e.erase("sess:1");
    expect(failed.status).toBe(503);
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess:1"))).toBe(true);
    expect(await exists(e.base, physical)).toBe(false);
    e.failWhen(undefined);
    expect((await e.erase("sess:1")).status).toBe(202);
    expect((await e.erase("sess:1")).status).toBe(202);
    expect(await exists(e.base, physical)).toBe(true);
    expect(new Set(e.puts.filter(p => p.startsWith(TOMBSTONES)).map(p => p.split(" ")[0])).size).toBe(2);
    const refused = await e.post("batch-1", "r1", "sess_1");
    expect([refused.status, await errorCode(refused)]).toEqual([409, "operator_session_erased"]);
  });

  test("too_many_owners (the SQL fence is kept, contract section 7): the tombstones are written, the 503 is unchanged, and uploads stay refused", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    e.sql!.overCap(key("sess-1"));
    const capped = await e.erase("sess-1");
    expect(capped.status).toBe(503);
    expect(JSON.stringify(await capped.json())).toContain("too_many_owners");
    expect(await exists(e.base, sessionEraseTombstoneKey(ROOT, TENANT, "claude", "sess-1"))).toBe(true);
    // Another process (no SQL fence) refuses the session from the tombstone alone.
    const other = edge({ fence: true, base: e.base, sql: null });
    const refused = await other.post("batch-1", "r1", "sess-1");
    expect([refused.status, await errorCode(refused)]).toEqual([409, "operator_session_erased"]);
    // A tombstone write failure on that path is a 503 unavailable (retried), never a silent too_many_owners.
    e.sql!.overCap(key("sess-2"));
    e.failWhen((op, k) => /^put/i.test(op) && k.startsWith(TOMBSTONES));
    const failed = await e.erase("sess-2");
    expect(failed.status).toBe(503);
    expect(await failed.json()).toEqual({ error: "unavailable", feature: "session_erase" });
  });

  test("a tombstone read failure: 503, nothing written; a later read admits", async () => {
    const e = edge({ fence: true });
    await writeAuthority(e.base);
    e.failWhen((op, k) => op === "head" && k.startsWith(TOMBSTONES));
    const before = e.puts.length;
    const failed = await e.post("batch-1", "r1", "sess-1");
    expect(failed.status).toBe(503);
    expect(e.puts.slice(before).filter(p => p.startsWith(CANONICAL))).toEqual([]);
    e.failWhen(undefined);
    expect((await e.post("batch-1", "r1", "sess-1")).status).toBe(200);
  });
});

describe("INGEST_S3_ERASE_FENCE off (the default): unchanged", () => {
  test("the S3 path still refuses the operator erase parts, and selection and consent with the flag on", () => {
    const parts = { run: (async () => { throw new Error("unused"); }) as never, store: scratchStore().store, verify: async () => { throw new Error("unused"); },
      ready: async () => true, close: async () => {} };
    const sql = fakeSqlErase();
    const base = { keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 32, ledger: "s3" as const };
    expect(() => selfhostIngestRuntime(base, { ...parts, withSessionWrites: sql.guard })).toThrow(/INGEST_LEDGER=s3 refuses SQL-only features/);
    expect(() => selfhostIngestRuntime(base, { ...parts, sessionEraseService: sql.service })).toThrow(/INGEST_LEDGER=s3 refuses SQL-only features/);
    expect(() => selfhostIngestRuntime({ ...base, s3EraseFence: true, sessionSelection: true }, parts)).toThrow(/INGEST_LEDGER=s3 refuses SQL-only features/);
    expect(() => selfhostIngestRuntime({ ...base, s3EraseFence: true, uploadConsent: "enforce" }, parts)).toThrow(/INGEST_LEDGER=s3 refuses SQL-only features/);
    expect(() => selfhostIngestRuntime({ ...base, ledger: undefined, s3EraseFence: true }, parts)).toThrow(/INGEST_S3_ERASE_FENCE=on needs INGEST_LEDGER=s3/);
    expect(() => selfhostIngestRuntime({ ...base, s3EraseFence: true, ledgerLogCommit: "on" }, parts)).toThrow(/INGEST_S3_ERASE_FENCE=on refuses INGEST_LEDGER_LOG_COMMIT=on/);
  });

  test("an s3 process without the flag logs that it does not check the tombstones; with the flag, no such line", () => {
    const off = edge({ fence: false, sql: null });
    expect(off.logged).toContainEqual({ ledger: "s3", s3_erase_fence: "off" });
    const on = edge({ fence: true, sql: null });
    expect(on.logged.some(f => f.s3_erase_fence === "off")).toBe(false);
  });

  test("no tombstone is read or written, and the writes are byte-identical to the flag on with no erase", async () => {
    const run = async (fence: boolean) => {
      const e = edge({ fence, sql: null });
      await writeAuthority(e.base);
      const statuses = [];
      for (const [b, r, sId] of [["batch-1", "r1", "sess-1"], ["batch-2", "r2", "sess:2"], ["batch-1", "r1", "sess-1"]] as const) {
        const res = await e.post(b, r, sId);
        statuses.push([res.status, await res.text()]);
      }
      return { e, statuses };
    };
    const off = await run(false);
    const on = await run(true);
    expect(off.e.heads.filter(k => k.startsWith(TOMBSTONES))).toEqual([]);
    expect(off.e.puts.filter(p => p.startsWith(TOMBSTONES))).toEqual([]);
    expect(on.e.heads.filter(k => k.startsWith(TOMBSTONES)).length).toBeGreaterThan(0);
    expect(off.statuses).toEqual(on.statuses);
    expect(off.e.puts).toEqual(on.e.puts);
  });
});

describe("INGEST_S3_ERASE_FENCE config (selfhost-config.ts)", () => {
  const enabled = {
    RELAYHISTORY_SELFHOST_INGEST: "true", RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
    RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test", RELAYCONTROL_INTROSPECT_SECRET: "s",
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase", LOCKER_BUCKET: "transcripts", INGEST_LEDGER: "s3",
  };
  const erase = { SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_FENCE_ENFORCED: "1" };

  test("the mode: off by default, exactly on, anything else refuses", () => {
    for (const v of [undefined, "", "off"]) expect(s3EraseFenceMode(v)).toBe("off");
    expect(s3EraseFenceMode("on")).toBe("on");
  });

  test("off: unset, empty or off leaves the settings exactly as before (no s3EraseFence field), and erase stays refused on s3", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    const base = selfhostIngestConfig(enabled);
    expect("s3EraseFence" in base).toBe(false);
    for (const v of ["", "off"]) expect(selfhostIngestConfig({ ...enabled, INGEST_S3_ERASE_FENCE: v })).toEqual(base);
    expect(() => selfhostIngestConfig({ ...enabled, ...erase })).toThrow(/INGEST_LEDGER=s3 refuses the operator session erase/);
    expect(() => selfhostIngestConfig({ ...enabled, SESSION_ERASE_FENCE_ENFORCED: "1" })).toThrow(/INGEST_LEDGER=s3 refuses the operator session erase/);
  });

  test("on: lifts the per-session erase route and its fence only", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    const on = selfhostIngestConfig({ ...enabled, ...erase, INGEST_S3_ERASE_FENCE: "on" });
    expect(on).toMatchObject({ ledger: "s3", s3EraseFence: true, eraseFenceEnforced: true, sessionErase: { enabled: true } });
    expect(selfhostIngestConfig({ ...enabled, SESSION_ERASE_FENCE_ENFORCED: "1", INGEST_S3_ERASE_FENCE: "on" })).toMatchObject({ s3EraseFence: true, eraseFenceEnforced: true });
    // A reader-only process (the async consumer: no SQL erase runtime there) takes the flag alone.
    expect(selfhostIngestConfig({ ...enabled, SELFHOST_INGEST_ADMISSION: "async", INGEST_S3_ERASE_FENCE: "on" })).toMatchObject({ s3EraseFence: true });
  });

  test("on: every refusal that remains", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    const on = { ...enabled, INGEST_S3_ERASE_FENCE: "on" };
    for (const v of ["ON", "On", " on", "true", "1", "yes"]) expect(() => selfhostIngestConfig({ ...enabled, INGEST_S3_ERASE_FENCE: v })).toThrow(/^96bulls: INGEST_S3_ERASE_FENCE must be off or on$/);
    expect(() => selfhostIngestConfig({ ...on, ...erase, SESSION_ERASE_BYTES_ENABLED: "1" })).toThrow(/INGEST_LEDGER=s3 refuses the operator session erase/);
    expect(() => selfhostIngestConfig({ ...on, RELAYHISTORY_SELFHOST_SESSION_SELECTION: "true" })).toThrow(/INGEST_LEDGER=s3 refuses RELAYHISTORY_SELFHOST_SESSION_SELECTION/);
    expect(() => selfhostIngestConfig({ ...on, INGEST_UPLOAD_CONSENT: "enforce" })).toThrow(/INGEST_LEDGER=s3 refuses INGEST_UPLOAD_CONSENT/);
    expect(() => selfhostIngestConfig({ ...on, INGEST_LEDGER: "sql" })).toThrow(/INGEST_S3_ERASE_FENCE=on needs INGEST_LEDGER=s3/);
    // INGEST_LEDGER unset or empty is the sql default: refused the same way.
    const { INGEST_LEDGER: _unset, ...noLedger } = on;
    expect(() => selfhostIngestConfig(noLedger)).toThrow(/INGEST_S3_ERASE_FENCE=on needs INGEST_LEDGER=s3/);
    expect(() => selfhostIngestConfig({ ...on, INGEST_LEDGER: "" })).toThrow(/INGEST_S3_ERASE_FENCE=on needs INGEST_LEDGER=s3/);
    expect(() => selfhostIngestConfig({ ...on, INGEST_LEDGER_LOG_COMMIT: "on" })).toThrow(/INGEST_S3_ERASE_FENCE=on refuses INGEST_LEDGER_LOG_COMMIT=on/);
  });
});
