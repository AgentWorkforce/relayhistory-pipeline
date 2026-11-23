/**
 * INGEST_ASYNC_WITNESS_REQUIRED (default off): with INGEST_ASYNC_ADMISSION=on, a queue-eligible batch (signed
 * queued-v1 opt-in, no relayhistory-built custody envelope) whose pre-body privacy-journal witness is missing is a
 * retryable 503 before the in-flight lease, the consent precheck, the turn register and any ledger call, instead of
 * the synchronous SQL path. Off, or for any other request, behaviour is unchanged (docs/design/s3-first-async-ingest.md).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { logLine, WITNESS_FAILURE_VALUES } from "../../shared/log.ts";
import { ObjectStoreError, objectKey } from "../../shared/vendor.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, signRequest, SYNC_SCOPE, WITNESS_FAILURES, type IngestAppDeps } from "../app.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, asyncWitnessRequiredMode, journalKey, type AdmissionMessage, type AsyncAdmissionMode } from "../async-admission.ts";
import { HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS } from "../http-admission-stop.ts";
import { ingestApp, ingestSettings, type IngestWorkerEnv } from "../worker.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../custody-protocol.ts";
import { createInflightLease, type InflightLease } from "../inflight-lease.ts";
import type { Ledger } from "../ledger.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../turns.ts";
import type { UploadConsentStore } from "../upload-consent.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
const seeds: Promise<void>[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
const T0 = 1_800_000_000_000;
const optIn = { [IDENTITY_HEADER]: encodeIdentity({ ...identity, admission: ADMISSION_ACCEPT_VALUE }) };
const plain = { [IDENTITY_HEADER]: encodeIdentity(identity) };

const RETRY_AFTER = String(HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS);
/** The object store's answer while it is down: a real ObjectStoreError, as the S3 adapter throws for a 503. */
const storeDown = () => new ObjectStoreError("transient", "s3", "s3 down: 96bulls/org-a/private-key", { status: 503 });

/**
 * A scratch store whose writes are counted. `broken`: every getObject throws `broken()`, so the pre-body journal
 * witness (the only read before the body) is missing and the request is witness-less.
 */
function store(broken: (() => Error) | undefined, puts: string[], seed?: (s: ReturnType<typeof scratchStore>["store"]) => Promise<void>) {
  const s = scratchStore();
  cleanups.push(s.done);
  if (seed) seeds.push(seed(s.store));
  return new Proxy(s.store, {
    get(target, key) {
      const v = Reflect.get(target, key, target);
      if (key === "getObject" && broken) return async () => { throw broken(); };
      if (key === "put" && typeof v === "function") return (...args: unknown[]) => { puts.push("put"); return (v as (...a: unknown[]) => unknown).apply(target, args); };
      return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
    },
  });
}

/** Every SQL-capable or lock-capable dependency, counted. */
function harness(opts: { flag?: "on"; mode?: AsyncAdmissionMode; broken?: (() => Error) | false; turns?: boolean; selfhost?: boolean; keyRoot?: string;
  seed?: (s: ReturnType<typeof scratchStore>["store"]) => Promise<void> } = {}) {
  const memory = new MemoryLedger();
  const ledgerCalls: string[] = [];
  const registers: unknown[] = [];
  (memory as MemoryLedger & Ledger).registerTurnSessions = async (input) => { registers.push(input); return []; };
  (memory as MemoryLedger & Ledger).turnFenceCapability = async () => true;
  const ledger = new Proxy(memory, {
    get(target, key, receiver) {
      const v = Reflect.get(target, key, receiver);
      return typeof v === "function" ? (...args: unknown[]) => { ledgerCalls.push(String(key)); return (v as (...a: unknown[]) => unknown).apply(target, args); } : v;
    },
  }) as MemoryLedger & Ledger;
  const consent: string[] = [];
  const consentStore: UploadConsentStore = {
    snapshot: async () => { consent.push("snapshot"); return { held: false, policy: null, originConfirmed: false, sessions: new Map() }; },
    apply: async () => { consent.push("apply"); return { revision: 1 }; },
  };
  const inner = createInflightLease();
  const leaseCalls: string[] = [];
  const lease: InflightLease = {
    acquire: (...a) => { leaseCalls.push("acquire"); return inner.acquire(...a); },
    track: (...a) => { leaseCalls.push("track"); return inner.track(...a); },
    holders: (...a) => inner.holders(...a),
    entries: () => inner.entries(),
    get counters() { return inner.counters; },
  };
  const sent: AdmissionMessage[] = [];
  const logged: Record<string, unknown>[] = [];
  const puts: string[] = [];
  const target = store(opts.broken === false ? undefined : opts.broken ?? storeDown, puts, opts.seed);
  const mode = opts.mode ?? "on";
  const deps: IngestAppDeps = {
    identify: (req) => bindingIdentity(req),
    log: (f) => logged.push(f),
    deliver: { ledger, acceptanceMode: "buffered", target: { store: target, keyRoot: opts.keyRoot ?? "96bulls" } },
    uploadConsent: { enforcement: "report", store: consentStore, nowMs: () => T0 },
    inflightLease: { mode: "observe", lease, now: () => T0 },
    ...(mode !== "off" ? { asyncAdmission: { mode, enqueue: async (m: AdmissionMessage) => { sent.push(m); }, nowMs: () => T0,
      ...(opts.selfhost ? { selfhost: { identityOf: (req: Request) => bindingIdentity(req) } } : {}) } } : {}),
    ...(opts.turns ? { turnFenceRegister: true as const } : {}),
    ...(opts.flag ? { asyncWitnessRequired: opts.flag } : {}),
  };
  const app = createIngestApp(deps);
  return { app, memory, ledgerCalls, registers, consent, leaseCalls, sent, logged, puts };
}

const post = async (h: ReturnType<typeof harness>, b: unknown, headers: Record<string, string> = optIn, path = "/v1/delivery/batches") =>
  (await Promise.all(seeds.splice(0)), h.app.request(path, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify({ protocolVersion: 1, batch: b }) }));

async function turnsBatch() {
  const SESSION = "7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a5b";
  const record_id = turnRecordId(TENANT.workspaceId, "claude", SESSION, 0);
  const revision = 1_793_000_000_000_000;
  const turn = { schema_version: 1, origin_id: TURN_ORIGIN_ID, record_id, revision_id: `turnrev:${record_id.slice(5)}:${revision}`, revision, kind: TURN_KIND, source: "claude", session_id: SESSION, operation: "upsert",
    payload: { id: "00000000-aaaa-4bbb-8ccc-dddddddddddd", org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, source: "claude", session_id: SESSION, turn_index: 0, session_owner: TENANT.userId,
      role: "user", content: "turn 0", actor_name: "Ada", actor_role: "owner", metadata_json: JSON.stringify({ nativeCli: "claude", relaySession: { sessionId: SESSION } }), ts: "2026-11-05T12:00:00.123Z", tagged: true } };
  const empty = await batch([]);
  return { schema_version: 1, origin_id: TURN_ORIGIN_ID, batch_id: "turns-1", job_id: "relayhistory-turns", generation: 1, destination_id: "relayhistory", instance_id: "relayhistory-api",
    account_id: empty.account_id, mapping_version: empty.mapping_version, records: [turn] };
}

/**
 * The synchronous path was taken: the claim ran. MemoryLedger has no buffer stage, so in buffered mode the request
 * then ends at the buffer step (503 buffer_not_configured, after the claim); never at the witness refusal.
 */
function synchronous(h: ReturnType<typeof harness>, res: Response) {
  expect(h.memory.claims).toHaveLength(1);
  expect(h.sent).toHaveLength(0);
  expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
  expect(h.logged.some((f) => f.stage === "admission_witness")).toBe(false);
  // The witness catch stays silent with the flag off: no failure class anywhere.
  expect(h.logged.some((f) => Object.hasOwn(f, "witness_failure"))).toBe(false);
  expect(h.logged.find((f) => f.outcome === "delivery_unavailable")).toMatchObject({ stage: "buffer", error_code: "buffer_not_configured" });
}

describe("asyncWitnessRequiredMode", () => {
  test("off/unset/blank is off; on in any case, trimmed, is on; anything else refuses", () => {
    expect(asyncWitnessRequiredMode(undefined)).toBe("off");
    expect(asyncWitnessRequiredMode("")).toBe("off");
    expect(asyncWitnessRequiredMode(" OFF ")).toBe("off");
    expect(asyncWitnessRequiredMode("on")).toBe("on");
    expect(asyncWitnessRequiredMode(" ON\t")).toBe("on");
    for (const bad of ["yes", "1", "true", "drain", "enforce"]) expect(() => asyncWitnessRequiredMode(bad)).toThrow(/INGEST_ASYNC_WITNESS_REQUIRED/);
  });

  test("Worker settings: unset is absent; on sets it; an invalid value refuses startup", async () => {
    const { ingestSettings } = await import("../worker.ts");
    const buffered = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn",
      INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: "a".repeat(64), INGEST_INLINE_MAX_SESSIONS: "0", INGEST_ASYNC_ADMISSION: "on", INGEST_ADMISSION: {} as never };
    const off = await ingestSettings(buffered);
    expect(off).not.toBeNull();
    expect(Object.hasOwn(off!, "asyncWitnessRequired")).toBe(false);
    expect(Object.hasOwn((await ingestSettings({ ...buffered, INGEST_ASYNC_WITNESS_REQUIRED: "off" }))!, "asyncWitnessRequired")).toBe(false);
    expect((await ingestSettings({ ...buffered, INGEST_ASYNC_WITNESS_REQUIRED: "on" }))?.asyncWitnessRequired).toBe(true);
    await expect(ingestSettings({ ...buffered, INGEST_ASYNC_WITNESS_REQUIRED: "yes" })).rejects.toThrow(/INGEST_ASYNC_WITNESS_REQUIRED/);
  });
});

describe("request path", () => {
  test("off (the default): a witness-less opted-in batch takes the synchronous path, exactly as before", async () => {
    const h = harness();
    const res = await post(h, await batch([record({ record_id: "r1" })]));
    synchronous(h, res);
    expect(h.consent[0]).toBe("snapshot");
    expect(h.leaseCalls).toEqual(["track"]);
  });

  test("on: the same batch is a 503 delivery_unavailable, Retry-After as the HTTP admission stop, before any lease, consent, ledger or object write", async () => {
    const h = harness({ flag: "on" });
    const res = await post(h, await batch([record({ record_id: "r1", payload: { text: "private words" } })]));
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe(RETRY_AFTER);
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    const body = await res.json() as { error: { code: string; message: string }; correlationId: unknown };
    expect(body.error).toEqual({ code: "delivery_unavailable", message: "Durable delivery is temporarily unavailable" });
    expect(typeof body.correlationId).toBe("string");
    expect(JSON.stringify(body)).not.toContain("privacy_journal");
    expect(h.ledgerCalls).toEqual([]);
    expect(h.memory.claims).toHaveLength(0);
    expect(h.consent).toEqual([]);
    expect(h.leaseCalls).toEqual([]);
    expect(h.puts).toEqual([]);
    expect(h.sent).toHaveLength(0);
    // The log line, through the real allowlisted logLine: the failure class and what the store answered.
    const refusal = h.logged.find((f) => f.outcome === "delivery_unavailable");
    expect(refusal).toBeDefined();
    const line = JSON.parse(logLine("info", "session ingest", refusal!)) as Record<string, unknown>;
    expect(line.outcome).toBe("delivery_unavailable");
    expect(line.stage).toBe("admission_witness");
    expect(line.error_code).toBe("privacy_journal_unavailable");
    expect(line.witness_failure).toBe("store_read");
    expect(line.store_status).toBe(503);
    expect(line.route).toBe("delivery.batches");
    expect(JSON.stringify(line)).not.toContain("private words");
    expect(JSON.stringify(line)).not.toContain("private-key");
    expect(JSON.stringify(line)).not.toContain("s3 down");
  });

  test("on: the turns route is refused before the turn register", async () => {
    const h = harness({ flag: "on", turns: true });
    const res = await post(h, await turnsBatch(), optIn, "/v1/turns/batches");
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe(RETRY_AFTER);
    expect(h.registers).toHaveLength(0);
    expect(h.ledgerCalls).toEqual([]);
    expect(h.consent).toEqual([]);
    expect(h.leaseCalls).toEqual([]);
    expect(h.logged.find((f) => f.outcome === "delivery_unavailable")).toMatchObject({ route: "turns.batches", stage: "admission_witness", error_code: "privacy_journal_unavailable" });
  });

  test("off: the same turns batch reaches the turn register (the refusal above is the flag's)", async () => {
    const h = harness({ turns: true });
    await post(h, await turnsBatch(), optIn, "/v1/turns/batches");
    expect(h.registers).toHaveLength(1);
  });

  test("on with the witness present: the queued ACK, unchanged", async () => {
    const h = harness({ flag: "on", broken: false });
    const res = await post(h, await batch([record({ record_id: "r1" })]));
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"queued"}');
    expect(h.sent).toHaveLength(1);
    expect(h.memory.claims).toHaveLength(0);
  });

  test("on: a caller without the signed opt-in stays synchronous", async () => {
    const h = harness({ flag: "on" });
    synchronous(h, await post(h, await batch([record({ record_id: "r1" })]), plain));
    // A loose (unsigned) opt-in header is not the signed identity.
    const loose = harness({ flag: "on" });
    synchronous(loose, await post(loose, await batch([record({ record_id: "r1" })]), { ...plain, "x-relay-ingest-admission": ADMISSION_ACCEPT_VALUE }));
  });

  for (const mode of ["off", "drain"] as const) {
    test(`on with INGEST_ASYNC_ADMISSION=${mode}: no effect, the synchronous path`, async () => {
      const h = harness({ flag: "on", mode });
      synchronous(h, await post(h, await batch([record({ record_id: "r1" })])));
    });
  }

  test("on: a relayhistory-built custody envelope is not refused by the flag (same answer as off)", async () => {
    const raw = await batch([record({ record_id: "r1" })]);
    const envelope = JSON.stringify({ protocol: "relay-ingest-custody-v1", original: { bodyBase64: Buffer.from(JSON.stringify({ protocolVersion: 1, batch: raw })).toString("base64"), contentEncoding: "identity" },
      witness: { records: [], privacy: null, originalRecordCount: 1 }, sourceDigest: "a".repeat(64) });
    const answer = async (flag?: "on") => {
      const h = harness(flag ? { flag } : {});
      const res = await h.app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": CURRENT_CUSTODY_CONTENT_TYPE, ...optIn }, body: envelope });
      const body = await res.json() as { error?: unknown };
      return { status: res.status, error: body.error, stages: h.logged.map((f) => f.stage), leased: h.leaseCalls.length, consent: h.consent.length > 0 };
    };
    const on = await answer("on"), off = await answer();
    expect(on).toEqual(off);
    expect(on.stages).not.toContain("admission_witness");
    // It went past the place the flag refuses: the lease and the consent precheck ran.
    expect(on.leased).toBe(1);
    expect(on.consent).toBe(true);
  });
});

describe("the refusal line's witness failure class (closed set, through the real logLine)", () => {
  test("the log allowlist's set equals the app's", () => {
    expect([...WITNESS_FAILURE_VALUES].sort()).toEqual([...WITNESS_FAILURES].sort());
  });

  const refusalLine = async (h: ReturnType<typeof harness>) => {
    const res = await post(h, await batch([record({ record_id: "r1" })]));
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe(RETRY_AFTER);
    expect(h.ledgerCalls).toEqual([]);
    const refusal = h.logged.find((f) => f.outcome === "delivery_unavailable");
    expect(refusal).toMatchObject({ stage: "admission_witness", error_code: "privacy_journal_unavailable" });
    return JSON.parse(logLine("info", "session ingest", refusal!)) as Record<string, unknown>;
  };

  test("a status-less store failure: store_read with its transport class", async () => {
    const line = await refusalLine(harness({ flag: "on", broken: () => new ObjectStoreError("transient", "s3", "connect reset to bucket/org-a", { transportCause: "econnreset" }) }));
    expect(line.witness_failure).toBe("store_read");
    expect(line.store_cause).toBe("econnreset");
    expect(Object.hasOwn(line, "store_status")).toBe(false);
  });

  test("an invalid journal object: journal_invalid (retries cannot work until it is repaired)", async () => {
    for (const bytes of ["not json", JSON.stringify({ v: 2, seq: 1 })]) {
      const line = await refusalLine(harness({ flag: "on", broken: false,
        seed: (s) => s.put(objectKey(journalKey("96bulls", TENANT)), new TextEncoder().encode(bytes), { contentType: "application/json" }).then(() => {}) }));
      expect(line.witness_failure).toBe("journal_invalid");
      expect(Object.hasOwn(line, "store_status") || Object.hasOwn(line, "store_cause")).toBe(false);
      expect(JSON.stringify(line)).not.toContain("privacy-journal");
    }
  });

  test("a key root the tenant-segment rule rejects: tenant_rejected (never works)", async () => {
    const line = await refusalLine(harness({ flag: "on", broken: false, keyRoot: "Not_A_Root" }));
    expect(line.witness_failure).toBe("tenant_rejected");
    expect(JSON.stringify(line)).not.toContain("Not_A_Root");
  });
});

describe("self-host is untouched by the option", () => {
  test("self-host with async admission on: the same answer whatever asyncWitnessRequired says", async () => {
    const answer = async (flag?: "on") => {
      const h = harness({ ...(flag ? { flag } : {}), selfhost: true });
      const res = await post(h, await batch([record({ record_id: "r1" })]));
      return { status: res.status, retryAfter: res.headers.get("retry-after"), body: (await res.json() as { error?: unknown }).error,
        lines: h.logged.filter((f) => f.outcome === "delivery_unavailable").map((f) => JSON.parse(logLine("info", "m", f)) as Record<string, unknown>).map(({ ts: _ts, correlation_id: _c, duration_ms: _d, ...rest }) => rest),
        leased: h.leaseCalls, consent: h.consent };
    };
    const on = await answer("on"), off = await answer();
    expect(on).toEqual(off);
    // Self-host's own refusal, after the lease and the consent precheck, and without Retry-After.
    expect(off.status).toBe(503);
    expect(off.retryAfter).toBeNull();
    expect(off.lines[0]).toMatchObject({ stage: "admission_witness", error_code: "privacy_journal_unavailable" });
    expect(off.leased.length).toBeGreaterThan(0);
  });
});

describe("the deployed wiring (worker.ts ingestApp)", () => {
  const env: IngestWorkerEnv = {
    HYPERDRIVE: { connectionString: "postgres://synthetic-invalid-unused" },
    INGEST_FANOUT: { async send() { throw Error("queue_unused"); }, async sendBatch() { throw Error("queue_unused"); } },
    INGEST_ADMISSION: { async send() { throw Error("admission_queue_unused"); }, async sendBatch() { throw Error("admission_queue_unused"); } } as never,
    S3_ACCESS_KEY_ID: "synthetic", S3_SECRET_ACCESS_KEY: "synthetic", INGEST_SIGNING_KEY: "synthetic", INGEST_S3_BUCKET: "synthetic", INGEST_KMS_KEY_ARN: "synthetic",
    INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: "a".repeat(64), INGEST_INLINE_MAX_SESSIONS: "0", INGEST_ASYNC_ADMISSION: "on",
  };
  /** var -> ingestSettings -> ingestApp -> a signed, opted-in POST while the journal store is down. */
  const send = async (flag?: string) => {
    const e = { ...env, ...(flag !== undefined ? { INGEST_ASYNC_WITNESS_REQUIRED: flag } : {}) };
    const settings = (await ingestSettings(e))!;
    expect(settings).not.toBeNull();
    const sql: string[] = [];
    const run = (async (text: string) => { sql.push(text); throw Object.assign(new Error("db down"), { code: "08006" }); }) as never;
    const puts: string[] = [];
    const app = ingestApp(e, settings, run, { store: store(storeDown, puts), keyRoot: settings.keyRoot, sleep: async () => {} } as never);
    const identity = encodeIdentity({ ...identity_, admission: ADMISSION_ACCEPT_VALUE });
    const body = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "r1" })]) }));
    const res = await app.request("/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json", [IDENTITY_HEADER]: identity,
      [SIGNATURE_HEADER]: signRequest(settings.signingKey, Math.floor(Date.now() / 1000), "POST", "/v1/delivery/batches", identity, body) } });
    return { status: res.status, retryAfter: res.headers.get("retry-after"), code: (await res.json() as { error?: { code: string } }).error?.code, sql, puts };
  };
  const identity_ = identity;

  test("on: the witness-less opted-in batch is the refusal, before any SQL", async () => {
    const on = await send("on");
    expect(on.status).toBe(503);
    expect(on.code).toBe("delivery_unavailable");
    expect(on.retryAfter).toBe(RETRY_AFTER);
    expect(on.sql).toEqual([]);
    expect(on.puts).toEqual([]);
  });

  test("off or unset: the same batch takes the synchronous path and reaches the ledger", async () => {
    for (const flag of [undefined, "off"]) {
      const off = await send(flag);
      expect(off.sql.length).toBeGreaterThan(0);
      expect(off.retryAfter).not.toBe(RETRY_AFTER);
    }
  });
});
