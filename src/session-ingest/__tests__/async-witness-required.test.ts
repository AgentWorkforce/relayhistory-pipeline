/**
 * INGEST_ASYNC_WITNESS_REQUIRED (default off): with INGEST_ASYNC_ADMISSION=on, a queue-eligible batch (signed
 * queued-v1 opt-in, no relayhistory-built custody envelope) whose pre-body privacy-journal witness is missing is a
 * retryable 503 before the in-flight lease, the consent precheck, the turn register and any ledger call, instead of
 * the synchronous SQL path. Off, or for any other request, behaviour is unchanged (docs/design/s3-first-async-ingest.md).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { logLine } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE, type IngestAppDeps } from "../app.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, asyncWitnessRequiredMode, type AdmissionMessage, type AsyncAdmissionMode } from "../async-admission.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../custody-protocol.ts";
import { createInflightLease, type InflightLease } from "../inflight-lease.ts";
import type { Ledger } from "../ledger.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../turns.ts";
import type { UploadConsentStore } from "../upload-consent.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
const T0 = 1_800_000_000_000;
const optIn = { [IDENTITY_HEADER]: encodeIdentity({ ...identity, admission: ADMISSION_ACCEPT_VALUE }) };
const plain = { [IDENTITY_HEADER]: encodeIdentity(identity) };

/**
 * A scratch store whose writes are counted. `broken`: every getObject fails, so the pre-body journal witness (the
 * only read before the body) is missing and the request is witness-less.
 */
function store(broken: boolean, puts: string[]) {
  const s = scratchStore();
  cleanups.push(s.done);
  return new Proxy(s.store, {
    get(target, key) {
      const v = Reflect.get(target, key, target);
      if (key === "getObject" && broken) return async () => { throw Object.assign(new Error("s3 down"), { code: "transient" }); };
      if (key === "put" && typeof v === "function") return (...args: unknown[]) => { puts.push("put"); return (v as (...a: unknown[]) => unknown).apply(target, args); };
      return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
    },
  });
}

/** Every SQL-capable or lock-capable dependency, counted. */
function harness(opts: { flag?: "on"; mode?: AsyncAdmissionMode; broken?: boolean; turns?: boolean } = {}) {
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
  const target = store(opts.broken ?? true, puts);
  const mode = opts.mode ?? "on";
  const deps: IngestAppDeps = {
    identify: (req) => bindingIdentity(req),
    log: (f) => logged.push(f),
    deliver: { ledger, acceptanceMode: "buffered", target: { store: target, keyRoot: "96bulls" } },
    uploadConsent: { enforcement: "report", store: consentStore, nowMs: () => T0 },
    inflightLease: { mode: "observe", lease, now: () => T0 },
    ...(mode !== "off" ? { asyncAdmission: { mode, enqueue: async (m: AdmissionMessage) => { sent.push(m); }, nowMs: () => T0 } } : {}),
    ...(opts.turns ? { turnFenceRegister: true as const } : {}),
    ...(opts.flag ? { asyncWitnessRequired: opts.flag } : {}),
  };
  const app = createIngestApp(deps);
  return { app, memory, ledgerCalls, registers, consent, leaseCalls, sent, logged, puts };
}

const post = async (h: ReturnType<typeof harness>, b: unknown, headers: Record<string, string> = optIn, path = "/v1/delivery/batches") =>
  h.app.request(path, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });

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

  test("on: the same batch is a 503 delivery_unavailable, no Retry-After, before any lease, consent, ledger or object write", async () => {
    const h = harness({ flag: "on" });
    const res = await post(h, await batch([record({ record_id: "r1", payload: { text: "private words" } })]));
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBeNull();
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
    // The log line, through the real allowlisted logLine: existing keys only.
    const refusal = h.logged.find((f) => f.outcome === "delivery_unavailable");
    expect(refusal).toBeDefined();
    const line = JSON.parse(logLine("info", "session ingest", refusal!)) as Record<string, unknown>;
    expect(line.outcome).toBe("delivery_unavailable");
    expect(line.stage).toBe("admission_witness");
    expect(line.error_code).toBe("privacy_journal_unavailable");
    expect(line.route).toBe("delivery.batches");
    expect(JSON.stringify(line)).not.toContain("private words");
  });

  test("on: the turns route is refused before the turn register", async () => {
    const h = harness({ flag: "on", turns: true });
    const res = await post(h, await turnsBatch(), optIn, "/v1/turns/batches");
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBeNull();
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
