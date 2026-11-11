/** INGEST_CLAIM_TIMEOUT_BUSY (default off, independent of INGEST_CLAIM_LOCK_BUSY): a claim whose single
 * autocommit statement hit statement_timeout or was cancelled (57014, `ledger_timeout`) is the same 429
 * `ClaimLockBusy` (`delivery_busy`, Retry-After in CLAIM_LOCK_RETRY_AFTER) instead of the 503. Only the claim
 * stage converts; every later stage and every other cause stays 503; the route line keeps
 * `error_code: ledger_timeout` / `db_error_code: 57014` (separable from the 55P03 lock wait) and the lock
 * alert still fires. Off: byte-identical 503, also with only INGEST_CLAIM_LOCK_BUSY=on. */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, lockAlertFields, SYNC_SCOPE } from "../app.ts";
import { backoffSeconds } from "../admission-consumer.ts";
import { enqueueAdmission } from "../async-admission.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { CLAIM_LOCK_RETRY_AFTER, ClaimLockBusy, deliverBatch, IngestUnavailable } from "../deliver.ts";
import { dbErrorDiagnosis } from "../db-error-class.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { DeliveryBusyError } from "../vendor/delivery/delivery-contracts.ts";
import { logLine } from "../../shared/log.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);

type Flags = { claimLockBusy?: true; claimTimeoutBusy?: true; random?: () => number };

/** The error the real SQL ledger raises for a driver error carrying `code` (its own mapping and diagnosis). */
async function ledgerError(code: string): Promise<unknown> {
  const pgErr = Object.assign(new Error(`canceling statement due to statement timeout ${TENANT.orgId}`), { code });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/** One POST /v1/delivery/batches through the real SQL ledger whose every statement fails with `sqlstate`. */
async function post(sqlstate: string, flags: Flags) {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql: string[] = [];
  const ledger = sqlLedger(async (text: string) => { sql.push(text); throw Object.assign(new Error(`canceling statement due to statement timeout ${TENANT.orgId}`), { code: sqlstate }); });
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...flags } as never,
    log: (fields) => logged.push(fields),
    identify: (r) => bindingIdentity(r),
  });
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "t1" }), record({ record_id: "t2" })]) });
  const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
    method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-timeout" },
  }));
  const text = await res.text();
  return { status: res.status, retryAfter: res.headers.get("retry-after"), text, json: JSON.parse(text) as { error: { code: string } }, route: logged.find((f) => f.route === "delivery.batches")!, logged, sql };
}

/** One signed buffered-mode archive-recovery POST (lock-alert.test.ts harness); `arm` makes one ledger method fail. */
async function recovery(arm: (ledger: MemoryLedger) => void, flags: Flags) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "timeout-busy" })], { batch_id: "timeout-busy" });
  const prepared = await prepareDelivery(TENANT, raw);
  const sourceDigest = "d".repeat(64);
  const custody = {
    sourceDigest,
    witness: {
      version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
      tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
      privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
      records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true as const, deletions: [], resets: [] },
    },
  };
  ledger.prepareBufferStage = async () => ({ stageToken: crypto.randomUUID(), expiresAt: new Date(Date.now() + 60_000).toISOString() });
  ledger.commitBuffered = async () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true });
  arm(ledger);
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", ...flags } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: { "content-type": RECOVERY_CONTENT_TYPE, ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) } }));
  return { status: res.status, retryAfter: res.headers.get("retry-after"), json: (await res.json()) as { error?: { code: string } }, route: logged.find((f) => f.route === "delivery.recovery"), alerts: logged.filter((f) => f.route === "ingest.lock_alert") };
}

const ON = { claimTimeoutBusy: true as const };

describe("claim statement-timeout busy (INGEST_CLAIM_TIMEOUT_BUSY)", () => {
  test("on: a claim 57014 -> 429 delivery_busy, Retry-After in [5,10], one statement, cause kept separable", async () => {
    for (const flags of [ON, { ...ON, claimLockBusy: true as const }]) {
      const r = await post("57014", flags);
      expect(r.status).toBe(429);
      expect(r.json.error.code).toBe("delivery_busy");
      const after = Number(r.retryAfter);
      expect(Number.isInteger(after) && after >= CLAIM_LOCK_RETRY_AFTER.min && after <= CLAIM_LOCK_RETRY_AFTER.max).toBe(true);
      expect(r.sql.length).toBe(1); // the Worker never retries the claim itself
      // Separable from the 55P03 lock wait: error_code ledger_timeout, db_error_code 57014.
      expect(r.route).toMatchObject({ outcome: "delivery_busy", stage: "claim", error_code: "ledger_timeout", db_error_code: "57014", correlation_id: "corr-timeout" });
      expect(logLine("info", "session ingest", r.route)).not.toContain("canceling statement");
      expect(r.text).not.toContain(TENANT.orgId);
    }
  });

  test("on: Retry-After spans exactly CLAIM_LOCK_RETRY_AFTER (5..10), out-of-range jitter falls back to the minimum", async () => {
    const got: (string | null)[] = [];
    for (const r of [0, 0.17, 0.5, 0.99, 0.999999, Number.NaN, 1, -1]) got.push((await post("57014", { ...ON, random: () => r })).retryAfter);
    expect(got).toEqual(["5", "6", "8", "10", "10", "5", "5", "5"]);
  });

  test("on: only 57014 converts; 55P03 (without INGEST_CLAIM_LOCK_BUSY), 55000, 57P01, 08006 stay 503", async () => {
    for (const code of ["55P03", "40P01", "55000", "57P01", "08006"]) {
      const r = await post(code, ON);
      expect(r.status).toBe(503);
      expect(r.retryAfter).toBeNull();
      expect(r.json.error.code).toBe("delivery_unavailable");
      expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage: "claim" });
    }
  });

  test("default unchanged: flag unset, and only INGEST_CLAIM_LOCK_BUSY=on, leave a claim 57014 the byte-identical 503", async () => {
    const off = await post("57014", {});
    const lockOnly = await post("57014", { claimLockBusy: true });
    for (const r of [off, lockOnly]) {
      expect(r.status).toBe(503);
      expect(r.retryAfter).toBeNull();
      expect(r.json.error.code).toBe("delivery_unavailable");
      expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage: "claim", error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.sql.length).toBe(1);
    }
    const norm = (r: Awaited<ReturnType<typeof post>>) => ({ text: r.text.replace(/"requestId":"[^"]*"/g, ""), route: { ...r.route, duration_ms: 0 } });
    expect(norm(lockOnly)).toEqual(norm(off));
  });

  test("on: the buffered lazy_claim lock alert still fires for the 429 (sqlstate 57014)", async () => {
    const thrown = await ledgerError("57014");
    const r = await recovery((l) => { l.claim = async () => { throw thrown; }; }, ON);
    expect(r.status).toBe(429);
    expect(r.json.error?.code).toBe("delivery_busy");
    expect(r.route).toMatchObject({ stage: "claim", error_code: "ledger_timeout", db_error_code: "57014" });
    expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: "lazy_claim", sqlstate: "57014" }]);
    const busy = new ClaimLockBusy("ledger_timeout", dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" })), 7);
    expect(busy).toBeInstanceOf(DeliveryBusyError);
    expect(lockAlertFields(busy, "buffered")).toEqual({ route: "ingest.lock_alert", stage: "lazy_claim", sqlstate: "57014" });
  });

  test("on: 57014 at buffer_intent and buffer_receipt stays 503 (alerts unchanged)", async () => {
    const thrown = await ledgerError("57014");
    const intent = await recovery((l) => { l.prepareBufferStage = async () => { throw thrown; }; }, { ...ON, claimLockBusy: true });
    expect(intent.status).toBe(503);
    expect(intent.retryAfter).toBeNull();
    expect(intent.route).toMatchObject({ outcome: "delivery_unavailable", stage: "buffer_intent", error_code: "ledger_timeout", db_error_code: "57014" });
    expect(intent.alerts).toEqual([{ route: "ingest.lock_alert", stage: "staged_intent", sqlstate: "57014" }]);
    const receipt = await recovery((l) => { l.commitBuffered = async () => { throw thrown; }; }, { ...ON, claimLockBusy: true });
    expect(receipt.status).toBe(503);
    expect(receipt.retryAfter).toBeNull();
    expect(receipt.route).toMatchObject({ outcome: "delivery_unavailable", stage: "buffer_receipt", error_code: "ledger_timeout", db_error_code: "57014" });
    expect(receipt.alerts).toEqual([{ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "57014" }]);
  });

  test("on: 57014 at slot and commit stays a 503 IngestUnavailable; a successful claim is unchanged", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const thrown = await ledgerError("57014");
    const base = { target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...ON, claimLockBusy: true as const };
    const slotLedger = new MemoryLedger();
    slotLedger.acquireSlot = async () => { throw thrown; };
    const slotErr = await deliverBatch({ ...base, ledger: slotLedger, slots: { maxPerUser: 4, maxPerOrg: 32, ttlSeconds: 90 } } as never, TENANT, await batch([record({ record_id: "s1" })])).then(() => null, (e: unknown) => e);
    expect(slotErr).toBeInstanceOf(IngestUnavailable);
    expect(slotErr).not.toBeInstanceOf(DeliveryBusyError);
    expect((slotErr as IngestUnavailable).stage).toBe("slot");
    expect((slotErr as IngestUnavailable).cause_code).toBe("ledger_timeout");

    const commitLedger = new MemoryLedger();
    commitLedger.commit = async () => { throw thrown; };
    const commitErr = await deliverBatch({ ...base, ledger: commitLedger } as never, TENANT, await batch([record({ record_id: "s2" })])).then(() => null, (e: unknown) => e);
    expect(commitErr).toBeInstanceOf(IngestUnavailable);
    expect((commitErr as IngestUnavailable).stage).toBe("commit");
    expect((commitErr as IngestUnavailable).cause_code).toBe("ledger_timeout");

    const ok = await deliverBatch({ ...base, ledger: new MemoryLedger() } as never, TENANT, await batch([record({ record_id: "s3" })]));
    expect(ok.outcome).toBeDefined();
  });

  test("stage guard: a ledger_timeout IngestUnavailable labelled with another stage, thrown from the claim, is not converted", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const ledger = new MemoryLedger();
    const d57 = dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" }));
    ledger.claim = async () => { throw new IngestUnavailable("buffer_receipt", "ledger_timeout", d57); };
    const err = await deliverBatch({ ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...ON } as never, TENANT, await batch([record({ record_id: "g1" })])).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(IngestUnavailable);
    expect(err).not.toBeInstanceOf(DeliveryBusyError);
    expect((err as IngestUnavailable).stage).toBe("buffer_receipt");
  });

  test("worker flag: INGEST_CLAIM_TIMEOUT_BUSY=on (any case) reaches the deliver deps; unset/off/garbage and INGEST_CLAIM_LOCK_BUSY alone leave it off", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const run = async () => ({ rows: [] });
    const unset = (await ingestSettings(full))!;
    expect("claimTimeoutBusy" in unset).toBe(false);
    expect("claimTimeoutBusy" in deliverDepsFor(unset, run, target as never)).toBe(false);
    for (const v of ["on", "ON", " On "]) {
      const on = (await ingestSettings({ ...full, INGEST_CLAIM_TIMEOUT_BUSY: v }))!;
      expect(on.claimTimeoutBusy).toBe(true);
      expect(on.claimLockBusy).toBeUndefined(); // independent flags
      expect(deliverDepsFor(on, run, target as never).claimTimeoutBusy).toBe(true);
    }
    for (const v of ["", "off", "OFF", "1", "true", "yes", "onn", "garbage"]) {
      const s = (await ingestSettings({ ...full, INGEST_CLAIM_TIMEOUT_BUSY: v }))!;
      expect("claimTimeoutBusy" in s).toBe(false);
    }
    const lockOnly = (await ingestSettings({ ...full, INGEST_CLAIM_LOCK_BUSY: "on" }))!;
    expect(lockOnly.claimLockBusy).toBe(true);
    expect("claimTimeoutBusy" in lockOnly).toBe(false);
    expect("claimTimeoutBusy" in deliverDepsFor(lockOnly, run, target as never)).toBe(false);
  });
});

/** One INGEST_ADMISSION message through the real Worker queue handler (claim-lock-busy.test.ts harness). */
async function consume(sqlstate: string, attempts: number, flags: { claimLockBusy?: true; claimTimeoutBusy?: true }) {
  const { admitQueueBatch } = await import("../worker.ts");
  const s = scratchStore();
  cleanups.push(s.done);
  const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "q1" })]));
  const key = await enqueueAdmission({ store: s.store, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, Date.now(), 0);
  const results: string[] = [];
  const message = { body: { v: 1, kind: "ingest-admission", key }, attempts, ack: () => results.push("ack"), retry: (o?: { delaySeconds?: number }) => results.push(`retry:${o?.delaySeconds}`) };
  const settings = { bucket: "b", region: "us-east-1", keyRoot: "96bulls", kmsKeyArn: "a", accessKeyId: "k", secretAccessKey: "s", signingKey: "x", maxPerUser: 4, maxPerOrg: 32, inlineMaxSessions: 0, replicaMode: "off" as const, acceptanceMode: "buffered" as const, expectedGeneration: "a".repeat(64), asyncAdmission: "drain" as const, ...flags };
  const sql: string[] = [];
  const connect = () => ({ run: async (t: string) => { sql.push(t); throw Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code: sqlstate }); }, end: async () => {} });
  const lines: string[] = [];
  const err = spyOn(process.stderr, "write").mockImplementation(((chunk: string) => { lines.push(String(chunk)); return true; }) as never);
  const out = spyOn(process.stdout, "write").mockImplementation(((chunk: string) => { lines.push(String(chunk)); return true; }) as never);
  try {
    await admitQueueBatch([message as never], { INGEST_ACCEPTANCE_MODE: "buffered" }, settings as never, connect, { store: s.store, keyRoot: "96bulls" });
  } finally {
    err.mockRestore();
    out.mockRestore();
  }
  const logged = lines.flatMap((l) => l.split("\n")).filter(Boolean).map((l) => {
    const { ts: _ts, duration_ms: _d, ...rest } = JSON.parse(l) as Record<string, unknown>;
    return rest;
  });
  return { results, logged, sql };
}

describe("claim statement-timeout busy on the admission consumer (the flag changes nothing there)", () => {
  test("off and on: a claim 57014 retries with backoffSeconds(attempts), same code, stage, db fields and alert", async () => {
    for (const attempts of [1, 3, 7]) {
      const off = await consume("57014", attempts, {});
      const on = await consume("57014", attempts, { claimTimeoutBusy: true, claimLockBusy: true });
      expect(off.results).toEqual([`retry:${backoffSeconds(attempts)}`]);
      expect(on.results).toEqual(off.results);
      expect(on.sql.length).toBe(1);
      expect(on.logged).toEqual(off.logged);
      expect(JSON.stringify(on.logged)).not.toContain("delivery_busy");
      expect(off.logged.find((f) => f.route === "ingest.admission")).toMatchObject({ outcome: "retry", error_code: "ledger_timeout", stage: "claim", db_error_code: "57014", attempt: attempts });
    }
  });
});
