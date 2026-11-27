/**
 * The per-org admission hold through the TypeScript callers (migration 20261105062320,
 * docs/design/org-admission-hold.md sections 4 and 6.3, acceptance test 10).
 *
 * - SQLSTATE RH042 (one fixed message) is `ledger_org_admission_held`, a retryable 503 `delivery_unavailable` with the
 *   HTTP stop's Retry-After (300 s) on every delivery path, with nothing written.
 * - The admission queue consumer defers it at that fixed pace (never a terminal outcome) and raises an alert.
 * - Selection and reset controls of a sealed org pass the SQL mirror gate only on the mirror-role connection, which the
 *   Worker uses only beside the S3 fence mirror (INGEST_CONTROL_MIRROR_ROLE), fail closed.
 *
 * The SQL model below raises exactly what 062320 raises (RH042, the fixed message); the PostgreSQL proofs of the
 * functions themselves live with the migration.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { mkdirSync, mkdtempSync, readdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { logLine } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, definitelyNotApplied, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { admitMessage, backoffSeconds, type OrgAdmissionHoldAlertFields } from "../admission-consumer.ts";
import { enqueueAdmission, INBOX_NAMESPACE } from "../async-admission.ts";
import { deliverBatch, IngestUnavailable, isOrgAdmissionHeld, ORG_ADMISSION_HELD_RETRY_AFTER_SECONDS, OrgAdmissionHeld } from "../deliver.ts";
import { HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS } from "../http-admission-stop.ts";
import { LedgerUnavailableError, ORG_ADMISSION_HELD_CODE, ORG_ADMISSION_HELD_MESSAGE, ORG_ADMISSION_HELD_SQLSTATE } from "../ledger.ts";
import { ledgerErrorCodeFrom, sqlLedger } from "../ledger-sql.ts";
import { isDeletedForAdmission, s3ControlMirror, s3LedgerKeys, validFence, type FenceDoc, type S3FenceMirror } from "../ledger-s3.ts";
import { admitInline, UploadAdmissionRuntimeError } from "../upload-admission-hosted.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { batch, record, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";

const ROOT = "96bulls";
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const unzstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));

/** A scratch fs store whose every written file can be counted (nothing written = no file at all). */
function countedStore(): { store: ObjectStore; files: () => string[] } {
  const base = process.env.TMPDIR ?? join(process.env.HOME ?? ".", ".agent-tmp", "tmp");
  mkdirSync(base, { recursive: true, mode: 0o700 });
  const root = mkdtempSync(join(base, "hold-"));
  cleanups.push(() => rmSync(root, { recursive: true, force: true }));
  const files = (dir = root): string[] => readdirSync(dir).flatMap((n) => {
    const p = join(dir, n);
    return statSync(p).isDirectory() ? files(p) : [p.slice(root.length + 1)];
  });
  return { store: createFsStore({ root }) as unknown as ObjectStore, files: () => files() };
}

/** What 062320 raises for a refusal: RH042 and exactly its message (which also quotes nothing of the caller's). */
const held = () => Object.assign(new Error(ORG_ADMISSION_HELD_MESSAGE), { code: ORG_ADMISSION_HELD_SQLSTATE, severity: "ERROR" });

/** A SQL run that refuses every statement with RH042, and records what it was asked. */
function heldRun() {
  const sql: string[] = [];
  return { sql, run: async (text: string) => { sql.push(text); throw held(); } };
}

const GEN = "a".repeat(64);

describe("RH042 in the SQL ledger", () => {
  test("maps to ledger_org_admission_held by the SQLSTATE alone; the message is never carried", async () => {
    expect(ledgerErrorCodeFrom({ code: "RH042" } as never)).toBe(ORG_ADMISSION_HELD_CODE);
    const { run } = heldRun();
    const err = await sqlLedger(run).claim({ tenant: TENANT } as never).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(LedgerUnavailableError);
    expect((err as LedgerUnavailableError).code).toBe(ORG_ADMISSION_HELD_CODE);
    expect((err as LedgerUnavailableError).db?.db_error_code).toBe("RH042");
    expect((err as Error).message).not.toContain(ORG_ADMISSION_HELD_MESSAGE);
    expect(isOrgAdmissionHeld(err)).toBe(true);
    // Other SQLSTATEs the hold raises keep their existing classes: 40001 (outside READ COMMITTED) and 55000.
    expect(ledgerErrorCodeFrom({ code: "40001" } as never)).toBe("ledger_sqlstate_40001");
    expect(isOrgAdmissionHeld(new LedgerUnavailableError("ledger_sqlstate_40001"))).toBe(false);
  });

  test("OrgAdmissionHeld: a 503 delivery_unavailable with the HTTP stop's Retry-After, and definitely not applied", () => {
    const e = new OrgAdmissionHeld("claim");
    expect(e).toBeInstanceOf(IngestUnavailable);
    expect(e).toBeInstanceOf(DeliveryError);
    expect(e.status).toBe(503);
    expect(e.code).toBe("delivery_unavailable");
    expect(e.cause_code).toBe(ORG_ADMISSION_HELD_CODE);
    expect(e.retryAfterSeconds).toBe(HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS);
    expect(ORG_ADMISSION_HELD_RETRY_AFTER_SECONDS).toBe(300);
    // An ordered disable refused by the hold releases its own S3 pending entry (app.ts abortOrdered).
    expect(definitelyNotApplied(new LedgerUnavailableError(ORG_ADMISSION_HELD_CODE, { db_error_class: "sql", db_error_code: "RH042", db_error_basis: "code" } as never))).toBe(true);
  });
});

describe("a held org's batch over HTTP (deliverBatch via sqlLedger): retryable 503 with Retry-After, nothing written", () => {
  for (const mode of ["canonical", "buffered"] as const) {
    test(`${mode}: POST /v1/delivery/batches`, async () => {
      const { store, files } = countedStore();
      const { sql, run } = heldRun();
      const logged: Record<string, unknown>[] = [];
      const ledger = sqlLedger(run, mode === "buffered" ? { acceptanceMode: "buffered", expectedGeneration: GEN } : {});
      const app = createIngestApp({
        deliver: { ledger, target: { store, keyRoot: ROOT, sleep: async () => {} }, ...(mode === "buffered" ? { acceptanceMode: "buffered", fanout: async () => {} } : {}) } as never,
        log: (fields) => logged.push(fields),
        identify: (r) => bindingIdentity(r),
      });
      const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "h1" }), record({ record_id: "h2" })]) });
      const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
        method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-hold" },
      }));
      const text = await res.text();
      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe("300");
      expect((JSON.parse(text) as { error: { code: string } }).error.code).toBe("delivery_unavailable");
      // One refused claim statement and nothing after it; the store holds nothing at all.
      expect(sql.length).toBe(1);
      expect(sql[0]).toContain(mode === "buffered" ? "ingest_claim_buffered_batch" : "ingest_claim_batch");
      expect(files()).toEqual([]);
      const route = logged.find((f) => f.route === "delivery.batches")!;
      expect(route).toMatchObject({ outcome: "delivery_unavailable", stage: "claim", error_code: ORG_ADMISSION_HELD_CODE, db_error_code: "RH042" });
      // Codes only: neither the answer nor the route line carries the database message or the org.
      expect(text).not.toContain("admission held");
      expect(text).not.toContain(TENANT.orgId);
      expect(logLine("info", "session ingest", route)).not.toContain(ORG_ADMISSION_HELD_MESSAGE);
    });
  }

  test("deliverBatch itself throws OrgAdmissionHeld at the claim stage", async () => {
    const { store, files } = countedStore();
    const { run } = heldRun();
    const err = await deliverBatch({ ledger: sqlLedger(run), target: { store, keyRoot: ROOT, sleep: async () => {} } } as never, TENANT, await batch([record({ record_id: "d1" })]))
      .then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(OrgAdmissionHeld);
    expect((err as OrgAdmissionHeld).stage).toBe("claim");
    expect((err as OrgAdmissionHeld).db?.db_error_code).toBe("RH042");
    expect(files()).toEqual([]);
  });
});

/** One acknowledged inbox message for TENANT (as the HTTP path and the upload Lambda leave it). */
async function inboxMessage(store: ObjectStore, id: string) {
  const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: id })]));
  const key = await enqueueAdmission({ store, keyRoot: ROOT, enqueue: async () => {} }, TENANT, prepared, Date.now(), 0);
  return { key, prepared };
}

describe("the admission queue consumer: RH042 defers at a fixed pace and alerts, never a terminal outcome", () => {
  const settings = { bucket: "b", region: "us-east-1", keyRoot: ROOT, kmsKeyArn: "a", accessKeyId: "k", secretAccessKey: "s", signingKey: "x", maxPerUser: 4, maxPerOrg: 32,
    inlineMaxSessions: 0, replicaMode: "off" as const, acceptanceMode: "buffered" as const, expectedGeneration: GEN, asyncAdmission: "on" as const };

  async function consume(attempts: number) {
    const { admitQueueBatch } = await import("../worker.ts");
    const { store, files } = countedStore();
    const { key } = await inboxMessage(store, "q1");
    const before = files().sort();
    const results: string[] = [];
    const message = { body: { v: 1, kind: "ingest-admission", key }, attempts, ack: () => results.push("ack"), retry: (o?: { delaySeconds?: number }) => results.push(`retry:${o?.delaySeconds}`) };
    const { sql, run } = heldRun();
    const lines: string[] = [];
    const err = spyOn(process.stderr, "write").mockImplementation(((chunk: string) => { lines.push(String(chunk)); return true; }) as never);
    const out = spyOn(process.stdout, "write").mockImplementation(((chunk: string) => { lines.push(String(chunk)); return true; }) as never);
    try {
      await admitQueueBatch([message as never], { INGEST_ACCEPTANCE_MODE: "buffered" }, settings as never, () => ({ run, end: async () => {} }), { store, keyRoot: ROOT });
    } finally {
      err.mockRestore();
      out.mockRestore();
    }
    const logged = lines.flatMap((l) => l.split("\n")).filter(Boolean).map((l) => JSON.parse(l) as Record<string, unknown>);
    return { results, logged, sql, before, after: files().sort(), raw: lines.join("") };
  }

  test("every attempt: retry after 300 s (not the backoff), the alert line, and the inbox untouched", async () => {
    for (const attempts of [1, 4, 40]) {
      const r = await consume(attempts);
      expect(r.results).toEqual([`retry:${ORG_ADMISSION_HELD_RETRY_AFTER_SECONDS}`]);
      if (attempts === 1) expect(backoffSeconds(attempts)).not.toBe(ORG_ADMISSION_HELD_RETRY_AFTER_SECONDS);
      expect(r.sql.length).toBe(1);
      expect(r.sql[0]).toContain("ingest_claim_buffered_batch");
      expect(r.logged.find((f) => f.route === "ingest.admission")).toMatchObject({ outcome: "retry", error_code: ORG_ADMISSION_HELD_CODE, stage: "claim", attempt: attempts });
      const alert = r.logged.find((f) => f.route === "ingest.admission_hold_alert")!;
      expect(alert).toMatchObject({ level: "error", error_code: ORG_ADMISSION_HELD_CODE, stage: "claim", org_id: TENANT.orgId, attempt: attempts });
      // No outcome, no tombstone: the acknowledged bytes stay for the retry (or an operator), exactly as they were.
      expect(r.after).toEqual(r.before);
      expect(r.after.some((f) => f.includes(INBOX_NAMESPACE))).toBe(true);
      expect(r.raw).not.toContain(ORG_ADMISSION_HELD_MESSAGE);
    }
  });

  test("admitMessage: the alert sink gets the closed fields; a throwing sink never changes the deferral", async () => {
    const { store } = countedStore();
    const { key } = await inboxMessage(store, "q2");
    const { run } = heldRun();
    const deliver = { ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: GEN }), acceptanceMode: "buffered", target: { store, keyRoot: ROOT, sleep: async () => {} }, fanout: async () => {} } as never;
    const alerts: OrgAdmissionHoldAlertFields[] = [];
    const base = { mode: "on" as const, deliver, custodySource: "relayhistory" as const, decompress: unzstd };
    const a = await admitMessage({ ...base, alert: (f) => alerts.push(f) }, { v: 1, kind: "ingest-admission", key }, 3);
    expect(a).toMatchObject({ action: "retry", delaySeconds: 300, code: ORG_ADMISSION_HELD_CODE, stage: "claim", alert: "org_admission_held" });
    expect(alerts).toEqual([{ route: "ingest.admission_hold_alert", error_code: ORG_ADMISSION_HELD_CODE, stage: "claim", org_id: TENANT.orgId, attempt: 3 }]);
    const b = await admitMessage({ ...base, alert: () => { throw new Error("sink down"); } }, { v: 1, kind: "ingest-admission", key }, 3);
    expect(b).toMatchObject({ action: "retry", delaySeconds: 300, code: ORG_ADMISSION_HELD_CODE });
  });

  test("any other claim failure keeps the backoff and raises no hold alert", async () => {
    const { store } = countedStore();
    const { key } = await inboxMessage(store, "q3");
    const deliver = { ledger: sqlLedger(async () => { throw Object.assign(new Error("x"), { code: "40001" }); }, { acceptanceMode: "buffered", expectedGeneration: GEN }),
      acceptanceMode: "buffered", target: { store, keyRoot: ROOT, sleep: async () => {} }, fanout: async () => {} } as never;
    const alerts: unknown[] = [];
    const r = await admitMessage({ mode: "on", deliver, custodySource: "relayhistory", decompress: unzstd, alert: (f) => alerts.push(f) }, { v: 1, kind: "ingest-admission", key }, 2);
    expect(r).toMatchObject({ action: "retry", delaySeconds: backoffSeconds(2) });
    expect("alert" in r).toBe(false);
    expect(alerts).toEqual([]);
  });

  test("the hosted upload-admission path (admitInline): a coded retry, never an outcome", async () => {
    const { store, files } = countedStore();
    const { key } = await inboxMessage(store, "u1");
    const before = files().sort();
    const { run } = heldRun();
    const err = await admitInline({ ledger: sqlLedger(run), target: { store, keyRoot: ROOT, sleep: async () => {} } } as never)({ v: 1, kind: "ingest-admission", key })
      .then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(UploadAdmissionRuntimeError);
    expect((err as UploadAdmissionRuntimeError).code).toBe(`admission_${ORG_ADMISSION_HELD_CODE}`);
    expect(files().sort()).toEqual(before);
  });
});

// ---- controls: the mirror gate (section 6.3) ----

const MIRROR_ROLE = "bulls96_session_ingest_mirror";
const GATED = ["ingest_set_session_uploads", "ingest_selfhost_set_session_uploads", "ingest_reset_uploaded_sessions"];

/**
 * The SQL side of the gate: one database, an org that may be sealed, and two logins. While sealed, a gated control is
 * refused (RH042) unless `pg_has_role(session_user, MIRROR_ROLE, 'MEMBER')`; otherwise it applies. Every statement is
 * recorded per login.
 */
function gatedSql() {
  const state = { sealed: true, deleted: new Set<string>(), resets: [] as string[] };
  const seen: Record<"main" | "mirror", string[]> = { main: [], mirror: [] };
  const login = (name: "main" | "mirror", roles: string[]) => async (text: string, params: unknown[]) => {
    seen[name].push(text);
    const fn = /sessions\.(\w+)\(/.exec(text)?.[1] ?? "";
    if (!GATED.includes(fn)) throw Object.assign(new Error("unexpected"), { code: "42883" });
    if (state.sealed && !roles.includes(MIRROR_ROLE)) throw held();
    const input = JSON.parse(String(params[0])) as { sessions?: [string, string][]; enabled?: boolean; requestId?: string };
    if (fn === "ingest_reset_uploaded_sessions") { state.resets.push(input.requestId!); return { rows: [{ result: { deleted: state.deleted.size } }] }; }
    for (const [, id] of input.sessions ?? []) { if (input.enabled) state.deleted.delete(id); else state.deleted.add(id); }
    return { rows: [{ result: { updated: (input.sessions ?? []).length } }] };
  };
  return { state, seen, main: login("main", ["bulls96_session_ingest"]), mirror: login("mirror", ["bulls96_session_ingest", MIRROR_ROLE]) };
}

/** The real S3 mirror, whose next `fail` setSessionUploads / resetUploadedSessions calls fail (an S3 write dropped). */
function flakyMirror(store: ObjectStore) {
  const real = s3ControlMirror(store, ROOT);
  const ctl = { fail: 0 };
  const flaky = <A extends unknown[], R>(f: (...a: A) => Promise<R>) => async (...a: A): Promise<R> => {
    if (ctl.fail > 0) { ctl.fail--; throw new Error("s3 write dropped"); }
    return f(...a);
  };
  const mirror: S3FenceMirror = { ...real, setSessionUploads: flaky(real.setSessionUploads), resetUploadedSessions: flaky(real.resetUploadedSessions) };
  return { mirror, ctl };
}

function controlApp(o: { mirrorRole: boolean }) {
  const { store } = countedStore();
  const sql = gatedSql();
  const { mirror, ctl } = flakyMirror(store);
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger: sqlLedger(sql.main), target: { store, keyRoot: ROOT } },
    log: (f) => logged.push(f),
    signingKey: SIGNING_KEY,
    s3FenceMirror: mirror,
    ...(o.mirrorRole ? { controlLedger: sqlLedger(sql.mirror) } : {}),
  });
  const post = async (path: string, value: unknown) => {
    const body = JSON.stringify(value);
    const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] } as never, "POST", path, enc(body)) }));
    return { status: res.status, retryAfter: res.headers.get("retry-after"), json: (await res.json()) as Record<string, unknown> };
  };
  return { store, sql, post, ctl, logged };
}
const sel = async (sessionId: string, enabled: boolean) => ({ account_id: (await batch([])).account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: sessionId }], enabled });
const resetBody = async (requestId: string) => ({ account_id: (await batch([])).account_id, request_id: requestId, confirmation: "DELETE ALL SESSIONS" });
const SELECT = "/v1/delivery/sessions/selection", RESET = "/v1/delivery/sessions/reset";
async function fenceOf(store: ObjectStore): Promise<FenceDoc | null> {
  try { return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(s3LedgerKeys(ROOT, TENANT).fence))).body))); } catch { return null; }
}
const s3Deleted = async (store: ObjectStore, id: string) => { const f = await fenceOf(store); return f !== null && validFence(f) && isDeletedForAdmission(f, "origin-1", "claude", id); };

describe("controls of a sealed org without the mirror role: refused (RH042), retryable", () => {
  test("selection disable and enable, and reset: 503 with Retry-After 300; SQL applies nothing", async () => {
    const { sql, post, logged } = controlApp({ mirrorRole: false });
    for (const [path, body] of [[SELECT, await sel("s1", false)], [SELECT, await sel("s1", true)], [RESET, await resetBody("r1")]] as const) {
      const r = await post(path, body);
      expect(r.status).toBe(503);
      expect(r.retryAfter).toBe("300");
      expect((r.json.error as { code: string }).code).toBe("delivery_unavailable");
    }
    expect(sql.state.deleted.size).toBe(0);
    expect(sql.state.resets).toEqual([]);
    expect(sql.seen.mirror).toEqual([]);
    expect(sql.seen.main.length).toBe(3);
    expect(logged.filter((f) => f.route === "delivery.sessions.selection" || f.route === "delivery.sessions.reset").map((f) => f.outcome))
      .toEqual([ORG_ADMISSION_HELD_CODE, ORG_ADMISSION_HELD_CODE, ORG_ADMISSION_HELD_CODE]);
  });

  test("unsealed: the same caller proceeds on the main connection (the gate binds only while sealed)", async () => {
    const { sql, post, store } = controlApp({ mirrorRole: false });
    sql.state.sealed = false;
    expect((await post(SELECT, await sel("s1", false))).status).toBe(200);
    expect(sql.state.deleted.has("s1")).toBe(true);
    expect(await s3Deleted(store, "s1")).toBe(true);
  });
});

describe("controls of a sealed org on the mirror-role connection (INGEST_CONTROL_MIRROR_ROLE=on)", () => {
  test("selection and reset apply over the mirror login, mirrored in S3; the main login is never used for them", async () => {
    const { sql, post, store } = controlApp({ mirrorRole: true });
    expect((await post(SELECT, await sel("s1", false))).status).toBe(200);
    expect(sql.state.deleted.has("s1")).toBe(true);
    expect(await s3Deleted(store, "s1")).toBe(true);
    expect((await post(SELECT, await sel("s1", true))).status).toBe(200);
    expect(sql.state.deleted.has("s1")).toBe(false);
    expect(await s3Deleted(store, "s1")).toBe(false);
    expect((await post(RESET, await resetBody("r1"))).status).toBe(200);
    expect(sql.state.resets).toEqual(["r1"]);
    expect(await fenceOf(store)).toMatchObject({ resets: 1 });
    expect(sql.seen.main).toEqual([]);
    expect(sql.seen.mirror.length).toBe(3);
  });

  test("a disable whose S3 write fails: 503 and no SQL at all; the retry applies both", async () => {
    const { sql, post, store, ctl } = controlApp({ mirrorRole: true });
    ctl.fail = 1;
    const r = await post(SELECT, await sel("s2", false));
    expect(r.status).toBe(503);
    expect(r.retryAfter).toBeNull();
    expect(sql.seen.mirror).toEqual([]);
    expect(sql.state.deleted.has("s2")).toBe(false);
    expect((await post(SELECT, await sel("s2", false))).status).toBe(200);
    expect(sql.state.deleted.has("s2")).toBe(true);
    expect(await s3Deleted(store, "s2")).toBe(true);
  });

  test("a re-enable whose S3 lift fails after SQL: 503 (S3 stays deleted, fail closed); the retry replays SQL and lifts", async () => {
    const { sql, post, store, ctl } = controlApp({ mirrorRole: true });
    expect((await post(SELECT, await sel("s3", false))).status).toBe(200);
    ctl.fail = 1;
    expect((await post(SELECT, await sel("s3", true))).status).toBe(503);
    expect(sql.state.deleted.has("s3")).toBe(false);
    expect(await s3Deleted(store, "s3")).toBe(true);
    expect((await post(SELECT, await sel("s3", true))).status).toBe(200);
    expect(await s3Deleted(store, "s3")).toBe(false);
    expect(sql.seen.mirror.length).toBe(3);
  });

  test("a reset whose S3 write fails: 503 and no SQL; the retry resets both", async () => {
    const { sql, post, store, ctl } = controlApp({ mirrorRole: true });
    ctl.fail = 1;
    expect((await post(RESET, await resetBody("r2"))).status).toBe(503);
    expect(sql.state.resets).toEqual([]);
    expect((await post(RESET, await resetBody("r2"))).status).toBe(200);
    expect(sql.state.resets).toEqual(["r2"]);
    expect(await fenceOf(store)).toMatchObject({ resets: 1 });
  });

  test("the mirror-role ledger without the S3 mirror refuses to build the app", () => {
    const sql = gatedSql();
    expect(() => createIngestApp({ deliver: { ledger: sqlLedger(sql.main), target: { store: {} as never, keyRoot: ROOT } }, controlLedger: sqlLedger(sql.mirror) }))
      .toThrow("control mirror role needs the S3 fence mirror");
  });
});

describe("the Worker settings: INGEST_CONTROL_MIRROR_ROLE (fail closed)", () => {
  const full = {
    HYPERDRIVE: { connectionString: "postgres://synthetic-invalid-unused" },
    INGEST_FANOUT: { async send() { throw Error("queue_unused"); }, async sendBatch() { throw Error("queue_unused"); } },
    S3_ACCESS_KEY_ID: "synthetic", S3_SECRET_ACCESS_KEY: "synthetic", INGEST_SIGNING_KEY: "synthetic", INGEST_S3_BUCKET: "synthetic", INGEST_KMS_KEY_ARN: "synthetic",
  };
  const MIRROR_DB = { connectionString: "postgres://synthetic-mirror-invalid-unused" };

  test("unset or off: no key, even with the binding present (never used)", async () => {
    const { ingestSettings } = await import("../worker.ts");
    for (const v of [undefined, "", "off", " off "]) {
      const s = (await ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB, ...(v === undefined ? {} : { INGEST_CONTROL_MIRROR_ROLE: v }) }))!;
      expect(Object.hasOwn(s, "controlMirrorRole")).toBe(false);
    }
  });

  test("on with the mirror and the binding: on", async () => {
    const { ingestSettings } = await import("../worker.ts");
    expect((await ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: "on", HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB }))!.controlMirrorRole).toBe(true);
  });

  test("on without the S3 mirror, without the binding, or any other value: refuses startup", async () => {
    const { ingestSettings } = await import("../worker.ts");
    await expect(ingestSettings({ ...full, INGEST_CONTROL_MIRROR_ROLE: "on", HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB })).rejects.toThrow("needs INGEST_S3_FENCE_MIRROR=on");
    await expect(ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: "on" })).rejects.toThrow("HYPERDRIVE_CONTROL_MIRROR");
    await expect(ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: "on", HYPERDRIVE_CONTROL_MIRROR: { connectionString: "" } })).rejects.toThrow("HYPERDRIVE_CONTROL_MIRROR");
    for (const v of ["ON", "1", "true", "yes"])
      await expect(ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: v, HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB })).rejects.toThrow("INGEST_CONTROL_MIRROR_ROLE must be off or on");
  });

  test("ingestApp: controls on the control connection, batches on the main one; a missing or unasked-for connection refuses", async () => {
    const { ingestSettings, ingestApp } = await import("../worker.ts");
    const { store } = countedStore();
    const settings = (await ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: "on", HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB }))!;
    const sql = gatedSql();
    const env = { ...full, INGEST_S3_FENCE_MIRROR: "on", INGEST_CONTROL_MIRROR_ROLE: "on", HYPERDRIVE_CONTROL_MIRROR: MIRROR_DB };
    const app = ingestApp(env as never, { ...settings, signingKey: SIGNING_KEY }, sql.main, { store, keyRoot: ROOT }, undefined, sql.mirror);
    const body = JSON.stringify(await sel("w1", false));
    const res = await app.fetch(new Request(`https://x${SELECT}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] } as never, "POST", SELECT, enc(body)) }));
    expect(res.status).toBe(200);
    expect(sql.seen.mirror.length).toBe(1);
    expect(sql.seen.main).toEqual([]);
    expect(await s3Deleted(store, "w1")).toBe(true);
    // A batch stays on the main login (here sealed-refused by the model's unknown-function answer: never the mirror).
    const bbody = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "w2" })]) });
    await app.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body: bbody, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] } as never, "POST", "/v1/delivery/batches", enc(bbody)) }));
    expect(sql.seen.mirror.length).toBe(1);
    expect(sql.seen.main.length).toBeGreaterThan(0);

    expect(() => ingestApp(env as never, settings, sql.main, { store, keyRoot: ROOT })).toThrow("needs the S3 fence mirror and its connection");
    const off = (await ingestSettings({ ...full, INGEST_S3_FENCE_MIRROR: "on" }))!;
    expect(() => ingestApp(full as never, off, sql.main, { store, keyRoot: ROOT }, undefined, sql.mirror)).toThrow("without INGEST_CONTROL_MIRROR_ROLE=on");
  });
});
