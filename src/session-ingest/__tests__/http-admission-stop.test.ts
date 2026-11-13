/** INGEST_HTTP_ADMISSION_STOPPED_ORGS (http-admission-stop.ts, decision 0005): the per-org stop for the Worker's
 * HTTP upload admission, the fence-authority marker's `workerHttpAdmission: "off"` precondition. A listed org's
 * batches, turns and recovery POSTs are a retryable 503 with Retry-After and no ledger call; an unlisted org and
 * every control or read route are unchanged; a malformed list refuses Worker startup. */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, signRequest, SYNC_SCOPE } from "../app.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { HTTP_ADMISSION_STOPPED_ORGS_VAR, HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS, httpAdmissionStoppedOrgs } from "../http-admission-stop.ts";
import { ingestApp, ingestSettings, type IngestWorkerEnv } from "../worker.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

/** An app over the real SQL ledger whose every statement is recorded and then fails (a 503 the stop must pre-empt). */
function harness(stopped: ReadonlySet<string> | undefined, acceptanceMode?: "buffered") {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql: string[] = [];
  const ledger = sqlLedger(async (text: string) => { sql.push(text); throw Object.assign(new Error("db down"), { code: "08006" }); });
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...(acceptanceMode ? { acceptanceMode } : {}) } as never,
    log: (fields) => logged.push(fields),
    identify: (r) => bindingIdentity(r),
    ...(stopped ? { httpAdmissionStoppedOrgs: stopped } : {}),
  });
  const send = async (path: string, body: string, headers: Record<string, string> = {}, tenant = TENANT) => {
    const res = await app.fetch(new Request(`https://x${path}`, {
      method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...tenant, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-stop", ...headers },
    }));
    const text = await res.text();
    return { status: res.status, retryAfter: res.headers.get("retry-after"), text, json: JSON.parse(text) as { error?: { code: string } } };
  };
  return { app, sql, logged, send };
}

const deliveryBody = async () => JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "s1" })]) });
const turnsBody = async () => JSON.stringify(await batch([record({ record_id: "t1" })]));
async function recoveryBody() {
  const raw = await batch([record({ record_id: "r1" })], { batch_id: "stop-recovery" });
  const prepared = await prepareDelivery(TENANT, raw);
  return JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody: {} });
}

describe("httpAdmissionStoppedOrgs (the var's parser)", () => {
  test("unset or blank: none", () => {
    for (const raw of [undefined, "", "   "]) expect(httpAdmissionStoppedOrgs(raw)).toBeUndefined();
  });
  test("a list: trimmed, de-duplicated org ids", () => {
    expect([...httpAdmissionStoppedOrgs(" org-a , org_b.1,org-a")!]).toEqual(["org-a", "org_b.1"]);
  });
  test("malformed: throws, naming the var but never an entry", () => {
    for (const raw of ["a,,b", "org-a,", ",org-a", ".hidden", "org a", "org:org-a", "ws:x", "a/b", "x".repeat(129), "é"]) {
      let message = "";
      try { httpAdmissionStoppedOrgs(raw); } catch (error) { message = (error as Error).message; }
      expect(message).toContain(HTTP_ADMISSION_STOPPED_ORGS_VAR);
      expect(message).not.toContain(raw);
    }
  });
});

describe("the Worker settings", () => {
  const full: IngestWorkerEnv = {
    HYPERDRIVE: { connectionString: "postgres://synthetic-invalid-unused" },
    INGEST_FANOUT: { async send() { throw Error("queue_unused"); }, async sendBatch() { throw Error("queue_unused"); } },
    S3_ACCESS_KEY_ID: "synthetic", S3_SECRET_ACCESS_KEY: "synthetic", INGEST_SIGNING_KEY: "synthetic", INGEST_S3_BUCKET: "synthetic", INGEST_KMS_KEY_ARN: "synthetic",
  };
  test("unset: no key at all (byte-for-byte as before)", async () => {
    const settings = await ingestSettings(full);
    expect(settings && Object.hasOwn(settings, "httpAdmissionStoppedOrgs")).toBe(false);
    expect(Object.hasOwn((await ingestSettings({ ...full, INGEST_HTTP_ADMISSION_STOPPED_ORGS: "" }))!, "httpAdmissionStoppedOrgs")).toBe(false);
  });
  test("a list: carried to the app", async () => {
    expect([...(await ingestSettings({ ...full, INGEST_HTTP_ADMISSION_STOPPED_ORGS: "org-a,org-b" }))!.httpAdmissionStoppedOrgs!]).toEqual(["org-a", "org-b"]);
  });
  test("malformed: refuses startup (every request 503 not_configured), never off", async () => {
    await expect(ingestSettings({ ...full, INGEST_HTTP_ADMISSION_STOPPED_ORGS: "org-a,,org-b" })).rejects.toThrow(HTTP_ADMISSION_STOPPED_ORGS_VAR);
  });
  /** The deployed wiring (worker.ts ingestApp): var -> ingestSettings -> ingestApp -> a signed POST is refused. */
  test("end to end through ingestApp: a listed org's signed batch is the stop, an unlisted org reaches the ledger", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const sql: string[] = [];
    const run = (async (text: string) => { sql.push(text); throw Object.assign(new Error("db down"), { code: "08006" }); }) as never;
    const post = async (orgId: string) => {
      const env = { ...full, INGEST_HTTP_ADMISSION_STOPPED_ORGS: `other-org,${orgId}` };
      const settings = (await ingestSettings(env))!;
      const app = ingestApp(env, settings, run, { store: s.store, keyRoot: settings.keyRoot, sleep: async () => {} } as never);
      const identity = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] });
      const body = new TextEncoder().encode(await deliveryBody());
      const res = await app.request("/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json", [IDENTITY_HEADER]: identity,
        [SIGNATURE_HEADER]: signRequest(settings.signingKey, Math.floor(Date.now() / 1000), "POST", "/v1/delivery/batches", identity, body) } });
      return { status: res.status, retryAfter: res.headers.get("retry-after"), json: await res.json() as { error?: { code: string } } };
    };
    const stopped = await post(TENANT.orgId);
    expect(stopped.status).toBe(503);
    expect(stopped.json.error?.code).toBe("delivery_unavailable");
    expect(stopped.retryAfter).toBe(String(HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS));
    expect(sql).toEqual([]);
    // Control: the same signed request for an org NOT listed gets past the stop and reaches the (failing) ledger,
    // so the 503 above is the stop and not a signature or settings failure.
    await post("unrelated-org");
    expect(sql.length).toBeGreaterThan(0);
  });
});

describe("a stopped org's HTTP admission", () => {
  const entries = [
    { path: "/v1/delivery/batches", route: "delivery.batches", body: deliveryBody, headers: {}, mode: undefined },
    { path: "/v1/turns/batches", route: "turns.batches", body: turnsBody, headers: {}, mode: undefined },
    { path: "/v1/delivery/recovery", route: "delivery.recovery", body: recoveryBody, headers: { "content-type": RECOVERY_CONTENT_TYPE }, mode: "buffered" as const },
  ];

  test("every entry: 503 delivery_unavailable, Retry-After, no ledger statement, no org id in the answer", async () => {
    for (const e of entries) {
      const h = harness(new Set([TENANT.orgId]), e.mode);
      const r = await h.send(e.path, await e.body(), e.headers);
      expect(r.status).toBe(503);
      expect(r.json.error?.code).toBe("delivery_unavailable");
      expect(r.retryAfter).toBe(String(HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS));
      expect(h.sql).toEqual([]);
      expect(r.text).not.toContain(TENANT.orgId);
      expect(h.logged.find((f) => f.route === e.route)).toMatchObject({ outcome: "delivery_unavailable", stage: "http_admission", error_code: "http_admission_stopped" });
    }
  });

  test("a malformed body is still the stop (refused before the body is parsed)", async () => {
    const h = harness(new Set([TENANT.orgId]));
    const r = await h.send("/v1/delivery/batches", "{not json");
    expect(r.status).toBe(503);
    expect(r.retryAfter).toBe(String(HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS));
  });

  test("an unlisted org is unchanged: it reaches the ledger exactly as with no stop configured", async () => {
    for (const e of entries) {
      const stopped = harness(new Set(["some-other-org"]), e.mode);
      const none = harness(undefined, e.mode);
      const body = await e.body();
      const a = await stopped.send(e.path, body, e.headers);
      const b = await none.send(e.path, body, e.headers);
      expect(a.status).toBe(b.status);
      expect(a.retryAfter).toBe(b.retryAfter);
      expect(a.json).toEqual(b.json);
      expect(stopped.sql).toEqual(none.sql);
      // Past the stop: the batch reaches the ledger (its first statement fails, 503 with no Retry-After); the
      // turns and recovery fixtures stop at their own body validation, which runs only after the stop check.
      if (e.route === "delivery.batches") expect(none.sql.length).toBeGreaterThan(0);
      else expect(b.json.error?.code).toBe("invalid_delivery");
      expect(stopped.logged.find((f) => f.route === e.route)?.stage).not.toBe("http_admission");
    }
  });

  test("controls and reads for a stopped org are unchanged", async () => {
    const requests: [string, string, string][] = [
      ["POST", "/v1/delivery/sessions/selection", JSON.stringify({ account_id: TENANT.workspaceId, origin_id: "o", sessions: [{ source: "claude", session_id: "s" }], enabled: false })],
      ["POST", "/v1/delivery/sessions/reset", JSON.stringify({ account_id: TENANT.workspaceId, request_id: "r", confirmation: "reset" })],
      ["POST", "/v1/delivery/sessions/selection", "{not json"],
      ["GET", "/v1/delivery/limits", ""],
      ["GET", "/healthz", ""],
    ];
    for (const [method, path, body] of requests) {
      const answers = [];
      for (const stopped of [new Set([TENANT.orgId]), undefined]) {
        const h = harness(stopped);
        const res = await h.app.fetch(new Request(`https://x${path}`, {
          method, ...(method === "POST" ? { body } : {}), headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-stop" },
        }));
        answers.push({ status: res.status, retryAfter: res.headers.get("retry-after"), text: await res.text(), sql: h.sql });
      }
      expect(answers[0]).toEqual(answers[1]);
      expect(answers[0]!.text).not.toContain("http_admission");
    }
  });
});
