/**
 * INGEST_BUFFER_TIMEOUT_BUSY (default off, independent of INGEST_BUFFER_LOCK_BUSY; buffered acceptance only):
 * a `buffer_intent` (ingest_prepare_buffer_stage) or `buffer_receipt` (ingest_commit_buffered_batch) ledger call
 * whose single autocommit statement hit statement_timeout or was cancelled (57014 by code, `ledger_timeout`),
 * still standing at the end of the stage, is the 429 `BufferLockBusy` (`delivery_busy`, Retry-After in
 * CLAIM_LOCK_RETRY_AFTER) instead of the 503. It is never retried: INGEST_BUFFER_LOCK_RETRY re-issues a lock
 * wait only, so a lock wait retried into a 57014 is converted. The route line keeps `error_code: ledger_timeout` /
 * `db_error_code: 57014` and the lock alert still fires. Every other cause and stage is unchanged; off: the same 503.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, lockAlertFields, SYNC_SCOPE } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { BufferLockBusy, type BufferLockRetryFields, CLAIM_LOCK_RETRY_AFTER, ClaimLockBusy, deliverBatch, IngestUnavailable } from "../deliver.ts";
import { dbErrorDiagnosis, dbErrorDiagnosisFrom, dbErrorSnapshot } from "../db-error-class.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { ledgerErrorCodeFrom, sqlLedger } from "../ledger-sql.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { DeliveryBusyError, DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);

type Flags = {
  bufferTimeoutBusy?: true; bufferLockBusy?: true; bufferLockRetry?: true; claimLockBusy?: true; claimTimeoutBusy?: true;
  random?: () => number; sleep?: (ms: number) => Promise<void>; bufferLockRetryLog?: (fields: BufferLockRetryFields) => void;
};
type Stage = "buffer_intent" | "buffer_receipt";
const METHOD = { buffer_intent: "prepareBufferStage", buffer_receipt: "commitBuffered" } as const;

/** The error the real SQL ledger raises for a driver error carrying `code` (its own mapping and diagnosis). */
async function ledgerError(code: string): Promise<unknown> {
  const pgErr = Object.assign(new Error(`canceling statement due to statement timeout ${TENANT.orgId}`), { code });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/** The real SQL ledger's error for a code-less client TimeoutError (`ledger_timeout`, basis none). */
async function nameOnlyTimeout(): Promise<unknown> {
  const err = Object.assign(new Error("request aborted"), { name: "TimeoutError" });
  try { await sqlLedger(async () => { throw err; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/**
 * One signed buffered-mode archive-recovery POST (buffer-lock-busy.test.ts harness). `failures[stage]` lists what
 * each successive call of that stage's ledger method throws (undefined: succeed); every call's input is recorded.
 * The sleep and random seams are counted unless the flags bring their own.
 */
async function recovery(failures: Partial<Record<Stage, unknown[]>>, flags: Flags, arm?: (ledger: MemoryLedger) => void) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "buffer-timeout" })], { batch_id: "buffer-timeout" });
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
  const calls: Record<Stage, unknown[]> = { buffer_intent: [], buffer_receipt: [] };
  const stageToken = "00000000-0000-4000-8000-000000000001";
  const expiresAt = new Date(Date.now() + 60_000).toISOString();
  const answer = { buffer_intent: () => ({ stageToken, expiresAt }), buffer_receipt: () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true }) };
  for (const stage of ["buffer_intent", "buffer_receipt"] as const) {
    (ledger as unknown as Record<string, unknown>)[METHOD[stage]] = async (input: unknown) => {
      const n = calls[stage].push(structuredClone(input));
      const fail = failures[stage]?.[n - 1];
      if (fail !== undefined) throw fail;
      return answer[stage]();
    };
  }
  arm?.(ledger);
  const sleeps: number[] = [];
  const retries: BufferLockRetryFields[] = [];
  let randoms = 0;
  const seams = {
    random: flags.random ?? (() => { randoms++; return 0.5; }),
    sleep: flags.sleep ?? (async (ms: number) => { sleeps.push(ms); }),
    bufferLockRetryLog: flags.bufferLockRetryLog ?? ((fields: BufferLockRetryFields) => { retries.push(fields); }),
  };
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", ...flags, ...seams } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const res = await app.fetch(new Request(`https://x${path}`, {
    method: "POST", body,
    headers: { "content-type": RECOVERY_CONTENT_TYPE, "x-correlation-id": "corr-buffer-timeout", ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) },
  }));
  const text = await res.text();
  return {
    status: res.status, retryAfter: res.headers.get("retry-after"), text, json: JSON.parse(text) as { error?: { code: string } },
    route: logged.find((f) => f.route === "delivery.recovery")!, alerts: logged.filter((f) => f.route === "ingest.lock_alert"),
    calls, sleeps, retries, randoms: () => randoms,
  };
}

/** A buffered deliverBatch straight through deliver.ts: `fail[stage]` is thrown by every call of that stage. */
async function direct(stage: Stage, thrown: unknown, flags: Flags) {
  const s = scratchStore();
  cleanups.push(s.done);
  const calls: Record<Stage, number> = { buffer_intent: 0, buffer_receipt: 0 };
  const sleeps: number[] = [];
  const retries: BufferLockRetryFields[] = [];
  const ledger = Object.assign(new MemoryLedger(), {
    prepareBufferStage: async () => {
      calls.buffer_intent++;
      if (stage === "buffer_intent") throw thrown;
      return { stageToken: "00000000-0000-4000-8000-000000000002", expiresAt: new Date(Date.now() + 60_000).toISOString() };
    },
    commitBuffered: async () => { calls.buffer_receipt++; if (stage === "buffer_receipt") throw thrown; return undefined; },
  });
  const error = await deliverBatch({
    ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered",
    sleep: async (ms: number) => { sleeps.push(ms); }, bufferLockRetryLog: (f: BufferLockRetryFields) => { retries.push(f); }, ...flags,
  } as never, TENANT, await batch([record({ record_id: "t1" })])).then(() => null, (e: unknown) => e);
  return { error, calls, sleeps, retries };
}

const ON = { bufferTimeoutBusy: true as const };
const BUSY = { bufferLockBusy: true as const };
const RETRY = { bufferLockRetry: true as const };
const STAGES: Stage[] = ["buffer_intent", "buffer_receipt"];
const ALERT_STAGE = { buffer_intent: "staged_intent", buffer_receipt: "staged_ack" } as const;
const TIMEOUT_DB = { db_error_class: "timeout", db_error_code: "57014", db_error_basis: "code" } as const;
const inRange = (v: string | null) => { const n = Number(v); return Number.isInteger(n) && n >= CLAIM_LOCK_RETRY_AFTER.min && n <= CLAIM_LOCK_RETRY_AFTER.max; };
const norm = (r: Awaited<ReturnType<typeof recovery>>) => ({ status: r.status, retryAfter: r.retryAfter, text: r.text.replace(/"requestId":"[^"]*"/g, ""), route: { ...r.route, duration_ms: 0 } });

describe("INGEST_BUFFER_TIMEOUT_BUSY on (alone)", () => {
  test("the real ledger's 57014 is ledger_timeout with a code-basis diagnosis (the converted input)", async () => {
    const e = await ledgerError("57014");
    expect(e).toBeInstanceOf(LedgerUnavailableError);
    expect((e as LedgerUnavailableError).code).toBe("ledger_timeout");
    expect((e as LedgerUnavailableError).db).toEqual(TIMEOUT_DB);
  });

  test("deliver: a 57014 at both stages is a BufferLockBusy keeping the stage, cause and db code; Retry-After 5..10 for random 0 / 0.5 / 0.999999; one call", async () => {
    for (const stage of STAGES) {
      const got: number[] = [];
      for (const v of [0, 0.5, 0.999999]) {
        const r = await direct(stage, await ledgerError("57014"), { ...ON, random: () => v });
        expect(r.error).toBeInstanceOf(BufferLockBusy);
        expect(r.error).toBeInstanceOf(ClaimLockBusy);
        expect(r.error).toBeInstanceOf(DeliveryBusyError);
        const b = r.error as BufferLockBusy;
        expect([b.status, b.code, b.stage, b.lock, b.cause_code]).toEqual([429, "delivery_busy", stage, "ledger_buffer", "ledger_timeout"]);
        expect(b.db).toEqual(TIMEOUT_DB);
        got.push(b.retryAfterSeconds);
        expect(r.calls[stage]).toBe(1);
        expect(r.sleeps).toEqual([]);
        expect(r.retries).toEqual([]);
      }
      expect(got).toEqual([5, 8, 10]);
    }
  });

  test("HTTP: 429 delivery_busy with Retry-After, the route line keeps ledger_timeout / 57014, and the lock alert fires", async () => {
    for (const stage of STAGES) {
      const r = await recovery({ [stage]: [await ledgerError("57014")] }, ON);
      expect(r.status).toBe(429);
      expect(r.json.error?.code).toBe("delivery_busy");
      expect(inRange(r.retryAfter)).toBe(true);
      expect(r.route).toMatchObject({ outcome: "delivery_busy", stage, error_code: "ledger_timeout", db_error_code: "57014", db_error_basis: "code", correlation_id: "corr-buffer-timeout" });
      expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "57014" }]);
      expect(r.text).not.toContain(TENANT.orgId);
      expect(r.calls[stage].length).toBe(1);
      expect(r.sleeps).toEqual([]);
      expect(r.retries).toEqual([]);
      expect(r.randoms()).toBe(1); // the Retry-After only
    }
  });

  test("lockAlertFields: a converted buffer-stage 57014 alerts at staged_intent / staged_ack", () => {
    const d = dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" }));
    for (const stage of STAGES)
      expect(lockAlertFields(new BufferLockBusy(stage, "ledger_timeout", d, 7), "buffered")).toEqual({ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "57014" });
  });

  test("a clean delivery is identical with the flag on", async () => {
    const clean = await recovery({}, {});
    const on = await recovery({}, ON);
    expect(norm(on)).toEqual(norm(clean));
    expect(on.randoms()).toBe(0);
  });
});

describe("INGEST_BUFFER_TIMEOUT_BUSY off", () => {
  test("a 57014 at both stages is the same 503 instance, also with LOCK_BUSY on, with RETRY on, or with both", async () => {
    for (const stage of STAGES)
      for (const flags of [{}, BUSY, RETRY, { ...BUSY, ...RETRY }, { claimTimeoutBusy: true as const }]) {
        const thrown = await ledgerError("57014");
        const r = await direct(stage, thrown, flags);
        expect(r.error).toBeInstanceOf(IngestUnavailable);
        expect(r.error).not.toBeInstanceOf(DeliveryBusyError);
        expect((r.error as IngestUnavailable).stage).toBe(stage);
        expect((r.error as IngestUnavailable).cause_code).toBe("ledger_timeout");
        expect(r.calls[stage]).toBe(1);
        expect(r.sleeps).toEqual([]);
        expect(r.retries).toEqual([]);
      }
  });

  test("the IngestUnavailable itself propagates unchanged (same instance) at both stages, in every other flag state", async () => {
    const d = dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" }));
    for (const stage of STAGES)
      for (const flags of [{}, BUSY, RETRY, { ...BUSY, ...RETRY }]) {
        const thrown = new IngestUnavailable(stage, "ledger_timeout", d);
        const r = await direct(stage, thrown, flags);
        expect(r.error).toBe(thrown);
      }
  });

  test("HTTP: the 503 delivery_unavailable without Retry-After; byte-identical with LOCK_BUSY or RETRY on", async () => {
    for (const stage of STAGES) {
      const thrown = await ledgerError("57014");
      const off = await recovery({ [stage]: [thrown] }, {});
      expect(off.status).toBe(503);
      expect(off.retryAfter).toBeNull();
      expect(off.text).toBe('{"error":{"code":"delivery_unavailable","message":"Durable delivery is temporarily unavailable"},"correlationId":"corr-buffer-timeout"}');
      expect(off.route).toMatchObject({ outcome: "delivery_unavailable", stage, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(off.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "57014" }]);
      for (const flags of [BUSY, RETRY, { ...BUSY, ...RETRY }]) {
        const r = await recovery({ [stage]: [thrown] }, flags);
        expect(norm(r)).toEqual(norm(off));
        expect(r.calls[stage].length).toBe(1);
        expect(r.retries).toEqual([]);
      }
    }
  });
});

describe("INGEST_BUFFER_TIMEOUT_BUSY with INGEST_BUFFER_LOCK_RETRY", () => {
  test("a 57014 is never retried (one call, no pause, no retry line) and is the 429", async () => {
    for (const stage of STAGES) {
      const r = await recovery({ [stage]: [await ledgerError("57014"), undefined] }, { ...ON, ...RETRY });
      expect(r.status).toBe(429);
      expect(r.route).toMatchObject({ outcome: "delivery_busy", stage, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.calls[stage].length).toBe(1);
      expect(r.sleeps).toEqual([]);
      expect(r.retries).toEqual([]);
      expect(r.randoms()).toBe(1);
    }
  });

  test("a lock wait then a 57014: one retry, one retry line with outcome failed, then the 429 (LOCK_BUSY off)", async () => {
    for (const stage of STAGES) {
      const r = await recovery({ [stage]: [await ledgerError("55P03"), await ledgerError("57014")] }, { ...ON, ...RETRY });
      expect(r.status).toBe(429);
      expect(r.json.error?.code).toBe("delivery_busy");
      expect(inRange(r.retryAfter)).toBe(true);
      expect(r.route).toMatchObject({ outcome: "delivery_busy", stage, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "57014" }]);
      expect(r.calls[stage].length).toBe(2);
      expect(r.calls[stage][1]).toEqual(r.calls[stage][0]);
      expect(r.sleeps).toEqual([150]);
      expect(r.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage, db_error_code: "55P03", outcome: "failed", error_code: "ledger_timeout", sqlstate: "57014" }]);
      expect(r.randoms()).toBe(2); // the pause, then the Retry-After
    }
  });

  test("a lock wait retried into another lock wait stays the 503 without LOCK_BUSY (this flag converts 57014 only)", async () => {
    const r = await recovery({ buffer_receipt: [await ledgerError("55P03"), await ledgerError("55P03")] }, { ...ON, ...RETRY });
    expect(r.status).toBe(503);
    expect(r.route).toMatchObject({ stage: "buffer_receipt", error_code: "ledger_lock_timeout", db_error_code: "55P03" });
    expect(r.calls.buffer_receipt.length).toBe(2);
  });
});

describe("INGEST_BUFFER_TIMEOUT_BUSY on: negatives", () => {
  test("a name-only client timeout (ledger_timeout, basis none) stays the 503", async () => {
    const thrown = await nameOnlyTimeout();
    expect((thrown as LedgerUnavailableError).code).toBe("ledger_timeout");
    for (const stage of STAGES)
      for (const flags of [ON, { ...ON, ...BUSY, ...RETRY }]) {
        const r = await recovery({ [stage]: [thrown] }, flags);
        expect(r.status).toBe(503);
        expect(r.retryAfter).toBeNull();
        expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage, error_code: "ledger_timeout" });
        expect(r.route.db_error_code).not.toBe("57014");
        expect(r.calls[stage].length).toBe(1);
      }
  });

  test("ledger_timeout without a diagnosis or with 57014 by message only stays as it is", async () => {
    const byMessage = Object.freeze({ db_error_class: "timeout" as const, db_error_code: "57014", db_error_basis: "message" as const });
    const nameOnly = dbErrorSnapshot({ name: "TimeoutError" });
    for (const stage of STAGES)
      for (const thrown of [
        new IngestUnavailable(stage, "ledger_timeout"),
        new IngestUnavailable(stage, "ledger_timeout", byMessage),
        new LedgerUnavailableError(ledgerErrorCodeFrom(nameOnly), dbErrorDiagnosisFrom(nameOnly)),
      ]) {
        const r = await direct(stage, thrown, ON);
        expect(r.error).toBeInstanceOf(IngestUnavailable);
        expect(r.error).not.toBeInstanceOf(DeliveryBusyError);
        if (thrown instanceof IngestUnavailable) expect(r.error).toBe(thrown);
      }
  });

  test("a 57014 IngestUnavailable labelled with a mismatched stage stays as it is", async () => {
    const d = dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" }));
    for (const [stage, label] of [["buffer_intent", "buffer_receipt"], ["buffer_receipt", "buffer_intent"], ["buffer_intent", "claim"], ["buffer_receipt", "commit"]] as const) {
      const thrown = new IngestUnavailable(label, "ledger_timeout", d);
      const r = await direct(stage, thrown, { ...ON, ...RETRY });
      expect(r.error).toBe(thrown);
      expect(r.calls[stage]).toBe(1);
    }
  });

  test("a 55P03 / 40P01 with LOCK_BUSY off stays the 503", async () => {
    for (const stage of STAGES)
      for (const code of ["55P03", "40P01"]) {
        const r = await recovery({ [stage]: [await ledgerError(code)] }, ON);
        expect(r.status).toBe(503);
        expect(r.retryAfter).toBeNull();
        expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage, error_code: "ledger_lock_timeout", db_error_code: code });
      }
  });

  test("other SQLSTATEs stay the 503", async () => {
    for (const stage of STAGES)
      for (const code of ["08006", "57P01", "55000", "40001"]) {
        const r = await recovery({ [stage]: [await ledgerError(code)] }, { ...ON, ...BUSY });
        expect(r.status).toBe(503);
        expect(r.route.outcome).toBe("delivery_unavailable");
      }
  });

  test("a non-IngestUnavailable error propagates unchanged; a plain Error is the same 503 as with the flag off", async () => {
    for (const stage of STAGES) {
      // A DeliveryError other than IngestUnavailable passes through runStage: the same instance comes out.
      const refused = new DeliveryError("invalid_batch", 422, "refused");
      const r = await direct(stage, refused, { ...ON, ...BUSY, ...RETRY });
      expect(r.error).toBe(refused);
      expect(r.calls[stage]).toBe(1);
      // A non-delivery error (even one carrying code 57014) is wrapped by runStage exactly as with the flag off.
      for (const make of [() => new TypeError("not a db error"), () => Object.assign(new Error("boom"), { code: "57014" })]) {
        const on = await direct(stage, make(), { ...ON, ...BUSY, ...RETRY });
        const off = await direct(stage, make(), {});
        expect(on.error).toBeInstanceOf(IngestUnavailable);
        expect(on.error).not.toBeInstanceOf(DeliveryBusyError);
        const pick = (e: unknown) => { const u = e as IngestUnavailable; return [u.stage, u.cause_code, u.db, u.status]; };
        expect(pick(on.error)).toEqual(pick(off.error));
        expect(on.calls[stage]).toBe(1);
        expect(on.sleeps).toEqual([]);
      }
    }
  });
});

describe("claim stage independence", () => {
  test("INGEST_BUFFER_TIMEOUT_BUSY does not convert a claim-stage 57014", async () => {
    const thrown = await ledgerError("57014");
    let claims = 0;
    const r = await recovery({}, { ...ON, ...BUSY, ...RETRY }, (l) => { l.claim = async () => { claims++; throw thrown; }; });
    expect(r.status).toBe(503);
    expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage: "claim", error_code: "ledger_timeout", db_error_code: "57014" });
    expect(claims).toBe(1);
  });

  test("INGEST_CLAIM_TIMEOUT_BUSY does not convert a buffer-stage 57014; with both on, each converts its own stage", async () => {
    for (const stage of STAGES) {
      const claimOnly = await recovery({ [stage]: [await ledgerError("57014")] }, { claimTimeoutBusy: true, claimLockBusy: true });
      expect(claimOnly.status).toBe(503);
      expect(claimOnly.route).toMatchObject({ stage, error_code: "ledger_timeout" });
      const both = await recovery({ [stage]: [await ledgerError("57014")] }, { ...ON, claimTimeoutBusy: true });
      expect(both.status).toBe(429);
      expect(both.route).toMatchObject({ stage, outcome: "delivery_busy" });
    }
    const thrown = await ledgerError("57014");
    const claim = await recovery({}, { ...ON, claimTimeoutBusy: true }, (l) => { l.claim = async () => { throw thrown; }; });
    expect(claim.status).toBe(429);
    expect(claim.route).toMatchObject({ stage: "claim", outcome: "delivery_busy", error_code: "ledger_timeout" });
  });
});

describe("config", () => {
  test("Worker: INGEST_BUFFER_TIMEOUT_BUSY=on (any case, trimmed) reaches the deliver deps alone; anything else leaves it off", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const off = (await ingestSettings(full))!;
    expect(Object.hasOwn(off, "bufferTimeoutBusy")).toBe(false);
    expect(Object.hasOwn(deliverDepsFor(off, async () => ({ rows: [] }), target as never), "bufferTimeoutBusy")).toBe(false);
    for (const v of ["on", "ON", " On ", "on\t"]) {
      const on = (await ingestSettings({ ...full, INGEST_BUFFER_TIMEOUT_BUSY: v }))!;
      expect(on).toEqual({ ...off, bufferTimeoutBusy: true } as never);
      const deps = deliverDepsFor(on, async () => ({ rows: [] }), target as never);
      expect(deps.bufferTimeoutBusy).toBe(true);
      for (const other of ["bufferLockBusy", "bufferLockRetry", "claimLockBusy", "claimTimeoutBusy"]) expect(Object.hasOwn(deps, other)).toBe(false);
    }
    for (const v of ["", " ", "off", "OFF", "1", "true", "yes", "o n"]) {
      const c = (await ingestSettings({ ...full, INGEST_BUFFER_TIMEOUT_BUSY: v }))!;
      expect(c).toEqual(off);
    }
    // The sibling flags do not turn it on.
    const siblings = (await ingestSettings({ ...full, INGEST_BUFFER_LOCK_BUSY: "on", INGEST_BUFFER_LOCK_RETRY: "on", INGEST_CLAIM_TIMEOUT_BUSY: "on" }))!;
    expect(Object.hasOwn(siblings, "bufferTimeoutBusy")).toBe(false);
  });

  test("self-host: not read; the settings are exactly as before", () => {
    const enabled: NodeJS.ProcessEnv = {
      RELAYHISTORY_SELFHOST_INGEST: "true",
      RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
      RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test",
      RELAYCONTROL_INTROSPECT_SECRET: "s",
      RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
      SESSION_POSTGRES_TLS_PROFILE: "supabase",
      LOCKER_BUCKET: "transcripts",
    };
    const base = selfhostIngestConfig(enabled);
    const on = selfhostIngestConfig({ ...enabled, INGEST_BUFFER_TIMEOUT_BUSY: "on" });
    expect(on).toEqual(base);
    expect(Object.hasOwn(on, "bufferTimeoutBusy")).toBe(false);
  });
});
