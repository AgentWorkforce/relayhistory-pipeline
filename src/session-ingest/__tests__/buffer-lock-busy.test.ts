/**
 * INGEST_BUFFER_LOCK_BUSY / INGEST_BUFFER_LOCK_RETRY (each default off, independent; buffered acceptance only).
 * A `buffer_intent` (ingest_prepare_buffer_stage) or `buffer_receipt` (ingest_commit_buffered_batch) ledger call
 * whose single autocommit statement lost its lock wait (55P03 / 40P01, cause `ledger_lock_timeout`):
 * - BUSY on: the 429 `BufferLockBusy` (a `ClaimLockBusy`: `delivery_busy`, Retry-After in CLAIM_LOCK_RETRY_AFTER)
 *   instead of the 503; the route line keeps the real stage and `ledger_lock_timeout`; the lock alert still fires.
 * - RETRY on: the same call with the same input once more after a 50..250 ms pause (injected random + sleep);
 *   a second failure propagates as before (503, or with BUSY the 429). Each retry, and only a retry, writes one
 *   `ingest.buffer_lock_retry` warn line (stage, the causing SQLSTATE, outcome, and the second failure's codes).
 * - BUSY on: a converted 40P01 also raises the lock alert (55P03 always did); a 40P01 that stays 503 does not.
 * Nothing else converts or retries: no other cause, no other stage. Both off: byte-identical (golden).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, lockAlertFields, SYNC_SCOPE } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { BUFFER_LOCK_RETRY_PAUSE_MS, BufferLockBusy, type BufferLockRetryFields, CLAIM_LOCK_RETRY_AFTER, ClaimLockBusy, deliverBatch, IngestUnavailable } from "../deliver.ts";
import { dbErrorDiagnosis } from "../db-error-class.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { DeliveryBusyError } from "../vendor/delivery/delivery-contracts.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);

type Flags = { bufferLockBusy?: true; bufferLockRetry?: true; claimLockBusy?: true; claimTimeoutBusy?: true; random?: () => number; sleep?: (ms: number) => Promise<void>; bufferLockRetryLog?: (fields: BufferLockRetryFields) => void };
type Stage = "buffer_intent" | "buffer_receipt";
const METHOD = { buffer_intent: "prepareBufferStage", buffer_receipt: "commitBuffered" } as const;

/** The error the real SQL ledger raises for a driver error carrying `code` (its own mapping and diagnosis). */
async function ledgerError(code: string): Promise<unknown> {
  const pgErr = Object.assign(new Error(`could not obtain lock ${TENANT.orgId}`), { code });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/**
 * One signed buffered-mode archive-recovery POST (lock-alert.test.ts harness). `failures[stage]` lists what each
 * successive call of that stage's ledger method throws (undefined: succeed); every call's input is recorded.
 * The sleep and random seams are counted unless the flags bring their own.
 */
async function recovery(failures: Partial<Record<Stage, unknown[]>>, flags: Flags, arm?: (ledger: MemoryLedger) => void) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "buffer-lock" })], { batch_id: "buffer-lock" });
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
    headers: { "content-type": RECOVERY_CONTENT_TYPE, "x-correlation-id": "corr-buffer-lock", ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) },
  }));
  const text = await res.text();
  return {
    status: res.status, retryAfter: res.headers.get("retry-after"), text, json: JSON.parse(text) as { error?: { code: string } },
    route: logged.find((f) => f.route === "delivery.recovery")!, alerts: logged.filter((f) => f.route === "ingest.lock_alert"),
    calls, sleeps, retries, randoms: () => randoms,
  };
}

const BUSY = { bufferLockBusy: true as const };
const RETRY = { bufferLockRetry: true as const };
const STAGES: Stage[] = ["buffer_intent", "buffer_receipt"];
const ALERT_STAGE = { buffer_intent: "staged_intent", buffer_receipt: "staged_ack" } as const;
/** Normalised for comparison: the per-request id and the route's wall time. */
const norm = (r: Awaited<ReturnType<typeof recovery>>) => ({ status: r.status, retryAfter: r.retryAfter, text: r.text.replace(/"requestId":"[^"]*"/g, ""), route: { ...r.route, duration_ms: 0 } });

describe("both flags off (golden)", () => {
  test("a 55P03 / 40P01 at buffer_intent and buffer_receipt is the byte-identical 503: one call, no pause, no jitter", async () => {
    for (const stage of STAGES)
      for (const code of ["55P03", "40P01"]) {
        const thrown = await ledgerError(code);
        const r = await recovery({ [stage]: [thrown] }, {});
        expect(r.status).toBe(503);
        expect(r.retryAfter).toBeNull();
        expect(r.text).toBe('{"error":{"code":"delivery_unavailable","message":"Durable delivery is temporarily unavailable"},"correlationId":"corr-buffer-lock"}');
        expect({ ...r.route, duration_ms: 0 }).toEqual({
          route: "delivery.recovery", outcome: "delivery_unavailable", correlation_id: "corr-buffer-lock", duration_ms: 0,
          org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, bytes_raw: r.route.bytes_raw,
          stage, error_code: "ledger_lock_timeout", db_error_class: "unknown", db_error_code: code, db_error_basis: "none",
        });
        expect(typeof r.route.bytes_raw).toBe("number");
        expect(r.alerts).toEqual(code === "55P03" ? [{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "55P03" }] : []);
        expect(r.calls[stage].length).toBe(1);
        expect(r.sleeps).toEqual([]);
        expect(r.retries).toEqual([]);
        expect(r.randoms()).toBe(0);
      }
  });

  test("the claim flags alone leave the buffer stages unchanged, and a clean delivery is identical in every flag state", async () => {
    const thrown = await ledgerError("55P03");
    for (const stage of STAGES) {
      const off = await recovery({ [stage]: [thrown] }, {});
      const claimOnly = await recovery({ [stage]: [thrown] }, { claimLockBusy: true, claimTimeoutBusy: true });
      expect(norm(claimOnly)).toEqual(norm(off));
    }
    const clean = await recovery({}, {});
    expect(clean.status).toBe(200);
    for (const flags of [BUSY, RETRY, { ...BUSY, ...RETRY }]) {
      const r = await recovery({}, flags);
      expect(norm(r)).toEqual(norm(clean));
      expect(r.calls.buffer_intent.length).toBe(1);
      expect(r.calls.buffer_receipt.length).toBe(1);
      expect(r.sleeps).toEqual([]);
      expect(r.retries).toEqual([]);
      expect(r.randoms()).toBe(0);
    }
  });
});

describe("INGEST_BUFFER_LOCK_BUSY", () => {
  test("on: 55P03 / 40P01 at both stages -> 429 delivery_busy, Retry-After in [5,10], real stage on the route line, one call", async () => {
    for (const stage of STAGES)
      for (const code of ["55P03", "40P01"]) {
        const r = await recovery({ [stage]: [await ledgerError(code)] }, BUSY);
        expect(r.status).toBe(429);
        expect(r.json.error?.code).toBe("delivery_busy");
        const after = Number(r.retryAfter);
        expect(Number.isInteger(after) && after >= CLAIM_LOCK_RETRY_AFTER.min && after <= CLAIM_LOCK_RETRY_AFTER.max).toBe(true);
        expect(r.route).toMatchObject({ outcome: "delivery_busy", stage, error_code: "ledger_lock_timeout", db_error_code: code, correlation_id: "corr-buffer-lock" });
        // The converted wait stays visible to the lock alert for 55P03 AND 40P01 (the DB-error alert counts only 503s).
        expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: code }]);
        expect(r.text).not.toContain(TENANT.orgId);
        expect(r.calls[stage].length).toBe(1); // BUSY alone never retries
        expect(r.sleeps).toEqual([]);
        expect(r.retries).toEqual([]);
      }
  });

  test("on: Retry-After spans exactly CLAIM_LOCK_RETRY_AFTER (5..10); out-of-range jitter falls back to the minimum", async () => {
    const thrown = await ledgerError("55P03");
    const got: (string | null)[] = [];
    for (const v of [0, 0.17, 0.5, 0.99, 0.999999, Number.NaN, 1, -1]) got.push((await recovery({ buffer_receipt: [thrown] }, { ...BUSY, random: () => v })).retryAfter);
    expect(got).toEqual(["5", "6", "8", "10", "10", "5", "5", "5"]);
  });

  test("on: every other cause at the buffer stages stays the 503", async () => {
    for (const stage of STAGES)
      for (const thrown of [await ledgerError("57014"), await ledgerError("08006"), await ledgerError("57P01"), await ledgerError("55000"), new Error("boom")]) {
        const r = await recovery({ [stage]: [thrown] }, { ...BUSY, claimLockBusy: true, claimTimeoutBusy: true });
        expect(r.status).toBe(503);
        expect(r.retryAfter).toBeNull();
        expect(r.json.error?.code).toBe("delivery_unavailable");
        expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage });
        expect(r.route.error_code).not.toBe("ledger_lock_timeout");
      }
  });

  test("on: a lock wait at any other stage is not converted (claim without its own flag, buffer_put, canonical commit, a mislabelled stage)", async () => {
    const thrown = await ledgerError("55P03");
    const claim = await recovery({}, BUSY, (l) => { l.claim = async () => { throw thrown; }; });
    expect(claim.status).toBe(503);
    expect(claim.route).toMatchObject({ stage: "claim", error_code: "ledger_lock_timeout" });

    // buffer_put (object store) failing with a code-carrying error: never a ledger_lock_timeout, stays 503.
    const s = scratchStore();
    cleanups.push(s.done);
    const put = await deliverBatch({
      ledger: Object.assign(new MemoryLedger(), {
        prepareBufferStage: async () => ({ stageToken: "00000000-0000-4000-8000-000000000002", expiresAt: new Date(Date.now() + 60_000).toISOString() }),
        commitBuffered: async () => undefined,
      }),
      target: { store: Object.assign(Object.create(s.store), { put: async () => { throw Object.assign(new Error("x"), { code: "ledger_lock_timeout" }); } }), keyRoot: "96bulls", sleep: async () => {} },
      acceptanceMode: "buffered", ...BUSY, ...RETRY, sleep: async () => {},
    } as never, TENANT, await batch([record({ record_id: "p1" })])).then(() => null, (e: unknown) => e);
    expect(put).toBeInstanceOf(IngestUnavailable);
    expect(put).not.toBeInstanceOf(DeliveryBusyError);
    expect((put as IngestUnavailable).stage).toBe("buffer_put");

    const s2 = scratchStore();
    cleanups.push(s2.done);
    let commits = 0;
    const canonical = Object.assign(new MemoryLedger(), { commit: async () => { commits++; throw thrown; } });
    const commitErr = await deliverBatch({ ledger: canonical, target: { store: s2.store, keyRoot: "96bulls", sleep: async () => {} }, ...BUSY, ...RETRY, sleep: async () => {} } as never,
      TENANT, await batch([record({ record_id: "c1" })])).then(() => null, (e: unknown) => e);
    expect(commitErr).toBeInstanceOf(IngestUnavailable);
    expect(commitErr).not.toBeInstanceOf(DeliveryBusyError);
    expect((commitErr as IngestUnavailable).stage).toBe("commit");
    expect(commits).toBe(1);

    // Stage guard: a lock-wait IngestUnavailable labelled with another stage, thrown from the stage call, stays as it is.
    const d = dbErrorDiagnosis(Object.assign(new Error(""), { code: "55P03" }));
    const mislabelled = await recovery({ buffer_intent: [new IngestUnavailable("claim", "ledger_lock_timeout", d)] }, { ...BUSY, ...RETRY });
    expect(mislabelled.status).toBe(503);
    expect(mislabelled.route).toMatchObject({ stage: "claim", error_code: "ledger_lock_timeout" });
    expect(mislabelled.calls.buffer_intent.length).toBe(1);
  });

  test("BufferLockBusy is a ClaimLockBusy (route line, lock alert, admission consumer) with the real stage and lock ledger_buffer", () => {
    const d = dbErrorDiagnosis(Object.assign(new Error(""), { code: "55P03" }));
    const busy = new BufferLockBusy("buffer_receipt", "ledger_lock_timeout", d, 7);
    expect(busy).toBeInstanceOf(ClaimLockBusy);
    expect(busy).toBeInstanceOf(DeliveryBusyError);
    expect([busy.status, busy.code, busy.stage, busy.lock, busy.retryAfterSeconds, busy.cause_code]).toEqual([429, "delivery_busy", "buffer_receipt", "ledger_buffer", 7, "ledger_lock_timeout"]);
    expect(lockAlertFields(busy, "buffered")).toEqual({ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "55P03" });
    const claim = new ClaimLockBusy("ledger_lock_timeout", d, 7);
    expect([claim.stage, claim.lock]).toEqual(["claim", "ledger_claim"]);
  });

  test("lock alert for 40P01: only a buffer-stage BufferLockBusy (the flag's conversion); the 503 and the claim stay as before", () => {
    const deadlock = dbErrorDiagnosis(Object.assign(new Error(""), { code: "40P01" }));
    for (const stage of STAGES) {
      for (const mode of ["buffered", "canonical", undefined])
        expect(lockAlertFields(new BufferLockBusy(stage, "ledger_lock_timeout", deadlock, 7), mode)).toEqual({ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "40P01" });
      // Flag off: the same 40P01 is a 503 IngestUnavailable, counted by the DB-error alert and (as before) no lock alert.
      expect(lockAlertFields(new IngestUnavailable(stage, "ledger_lock_timeout", deadlock), "buffered")).toBeUndefined();
    }
    // Claim alerting unchanged: a 40P01 claim converted by INGEST_CLAIM_LOCK_BUSY still emits no lock alert.
    expect(lockAlertFields(new ClaimLockBusy("ledger_lock_timeout", deadlock, 7), "buffered")).toBeUndefined();
    expect(lockAlertFields(new IngestUnavailable("claim", "ledger_lock_timeout", deadlock), "buffered")).toBeUndefined();
    // Other SQLSTATEs on a BufferLockBusy do not alert (only 55P03 / 57014 / converted 40P01).
    for (const code of ["40001", "55000", "08006"])
      expect(lockAlertFields(new BufferLockBusy("buffer_receipt", "ledger_lock_timeout", dbErrorDiagnosis(Object.assign(new Error(""), { code })), 7), "buffered")).toBeUndefined();
  });
});

describe("INGEST_BUFFER_LOCK_RETRY", () => {
  test("on: a first-attempt lock wait then success -> the normal receipt, exactly two calls with identical input", async () => {
    for (const stage of STAGES)
      for (const code of ["55P03", "40P01"]) {
        const clean = await recovery({}, {});
        const r = await recovery({ [stage]: [await ledgerError(code)] }, RETRY);
        expect(norm(r)).toEqual(norm(clean));
        expect(r.status).toBe(200);
        expect(r.calls[stage].length).toBe(2);
        expect(r.calls[stage][1]).toEqual(r.calls[stage][0]);
        expect(r.alerts).toEqual([]);
        // The one trace of the absorbed lock wait: exactly one retry line, closed fields only.
        expect(r.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage, db_error_code: code, outcome: "ok" }]);
        expect(JSON.stringify(r.retries)).not.toContain(TENANT.orgId);
        expect(r.sleeps).toEqual([150]); // 50 + floor(0.5 * 201)
        expect(r.randoms()).toBe(1);
        // The other stage ran once, as normal.
        expect(r.calls[stage === "buffer_intent" ? "buffer_receipt" : "buffer_intent"].length).toBe(1);
      }
  });

  test("on, second attempt fails too: the 503 with BUSY off, the 429 with BUSY on; never a third call", async () => {
    for (const stage of STAGES) {
      const a = await ledgerError("55P03");
      const b = await ledgerError("40P01");
      const off = await recovery({ [stage]: [a, b] }, RETRY);
      expect(off.status).toBe(503);
      expect(off.retryAfter).toBeNull();
      expect(off.route).toMatchObject({ outcome: "delivery_unavailable", stage, error_code: "ledger_lock_timeout", db_error_code: "40P01" });
      expect(off.calls[stage].length).toBe(2);
      expect(off.sleeps.length).toBe(1);
      expect(off.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage, db_error_code: "55P03", outcome: "failed", error_code: "ledger_lock_timeout", sqlstate: "40P01" }]);

      const on = await recovery({ [stage]: [a, a, a] }, { ...RETRY, ...BUSY });
      expect(on.status).toBe(429);
      expect(on.json.error?.code).toBe("delivery_busy");
      expect(on.route).toMatchObject({ outcome: "delivery_busy", stage, error_code: "ledger_lock_timeout", db_error_code: "55P03" });
      expect(on.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT_STAGE[stage], sqlstate: "55P03" }]);
      expect(on.calls[stage].length).toBe(2);
      expect(on.sleeps.length).toBe(1);
      expect(on.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage, db_error_code: "55P03", outcome: "failed", error_code: "ledger_lock_timeout", sqlstate: "55P03" }]);
      expect(on.randoms()).toBe(2); // the pause, then the Retry-After
    }
  });

  test("on, the retry fails with another cause: that cause propagates unchanged (no busy answer)", async () => {
    const r = await recovery({ buffer_receipt: [await ledgerError("55P03"), await ledgerError("57014")] }, { ...RETRY, ...BUSY });
    expect(r.status).toBe(503);
    expect(r.route).toMatchObject({ stage: "buffer_receipt", error_code: "ledger_timeout", db_error_code: "57014" });
    expect(r.calls.buffer_receipt.length).toBe(2);
    expect(r.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage: "buffer_receipt", db_error_code: "55P03", outcome: "failed", error_code: "ledger_timeout", sqlstate: "57014" }]);
  });

  test("on: never a retry for any other cause or stage; no pause, no jitter", async () => {
    for (const stage of STAGES)
      for (const thrown of [await ledgerError("57014"), await ledgerError("08006"), await ledgerError("55000"), new Error("boom")]) {
        const r = await recovery({ [stage]: [thrown] }, RETRY);
        expect(r.status).toBe(503);
        expect(r.calls[stage].length).toBe(1);
        expect(r.sleeps).toEqual([]);
        expect(r.retries).toEqual([]);
        expect(r.randoms()).toBe(0);
      }
    let claims = 0;
    const thrown = await ledgerError("55P03");
    const claim = await recovery({}, RETRY, (l) => { l.claim = async () => { claims++; throw thrown; }; });
    expect(claim.status).toBe(503);
    expect(claims).toBe(1);
    expect(claim.sleeps).toEqual([]);
    expect(claim.retries).toEqual([]);
  });

  test("the retry line: one warn line through the shared logger by default, allowlisted fields only; a throwing logger changes nothing", async () => {
    const written: string[] = [];
    const write = process.stderr.write;
    process.stderr.write = ((chunk: string | Uint8Array) => { written.push(String(chunk)); return true; }) as typeof process.stderr.write;
    let r: Awaited<ReturnType<typeof recovery>>;
    try {
      // No seam: deliver's default (log.warn) writes the line.
      const s = scratchStore();
      cleanups.push(s.done);
      let n = 0;
      const ledger = Object.assign(new MemoryLedger(), {
        prepareBufferStage: async () => { if (n++ === 0) throw await ledgerError("40P01"); return { stageToken: "00000000-0000-4000-8000-000000000003", expiresAt: new Date(Date.now() + 60_000).toISOString() }; },
        commitBuffered: async () => { throw await ledgerError("08006"); },
      });
      await deliverBatch({ ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", ...RETRY, sleep: async () => {} } as never,
        TENANT, await batch([record({ record_id: "w1" })])).then(() => null, (e: unknown) => e);
    } finally {
      process.stderr.write = write;
    }
    const lines = written.flatMap((w) => w.split("\n")).filter(Boolean).map((l) => JSON.parse(l) as Record<string, unknown>).filter((l) => l.route === "ingest.buffer_lock_retry");
    expect(lines.length).toBe(1);
    expect(lines[0]).toMatchObject({ level: "warn", message: "session ingest buffer lock retry", route: "ingest.buffer_lock_retry", stage: "buffer_intent", db_error_code: "40P01", outcome: "ok" });
    expect(Object.keys(lines[0]!).sort()).toEqual(["db_error_code", "deployment", "deployment_id", "level", "message", "outcome", "route", "service", "stage", "ts"]);
    expect(JSON.stringify(lines)).not.toContain(TENANT.orgId);
    expect(JSON.stringify(lines)).not.toContain(TENANT.workspaceId);

    // A throwing logger never changes the delivery.
    r = await recovery({ buffer_receipt: [await ledgerError("55P03")] }, { ...RETRY, bufferLockRetryLog: () => { throw new Error("logger down"); } });
    expect(r.status).toBe(200);
    expect(r.calls.buffer_receipt.length).toBe(2);
  });

  test("the pause takes the injected random and sleep: 50..250 ms inclusive; out-of-range jitter falls back to the minimum", async () => {
    const thrown = await ledgerError("55P03");
    const got: number[] = [];
    for (const v of [0, 0.5, 0.999999, Number.NaN, 1, -1]) {
      const r = await recovery({ buffer_intent: [thrown] }, { ...RETRY, random: () => v, sleep: async (ms) => { got.push(ms); } });
      expect(r.status).toBe(200);
    }
    expect(got).toEqual([50, 150, 250, 50, 50, 50]);
    expect(BUFFER_LOCK_RETRY_PAUSE_MS).toEqual({ min: 50, max: 250 });
  });

  test("the retry waits for the sleep to settle before re-issuing", async () => {
    const thrown = await ledgerError("55P03");
    const order: string[] = [];
    const r = await recovery({ buffer_receipt: [thrown] }, { ...RETRY, sleep: async () => { order.push("sleep-start"); await Promise.resolve(); order.push("sleep-end"); } },
      (l) => { const commit = l.commitBuffered!.bind(l); l.commitBuffered = async (input) => { order.push("call"); return commit(input); }; });
    expect(r.status).toBe(200);
    expect(order).toEqual(["call", "sleep-start", "sleep-end", "call"]);
  });
});

describe("config", () => {
  test("Worker: INGEST_BUFFER_LOCK_BUSY / INGEST_BUFFER_LOCK_RETRY=on (any case, trimmed) reach the deliver deps; anything else leaves them off", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const off = (await ingestSettings(full))!;
    const offDeps = deliverDepsFor(off, async () => ({ rows: [] }), target as never);
    for (const key of ["bufferLockBusy", "bufferLockRetry"]) {
      expect(Object.hasOwn(off, key)).toBe(false);
      expect(Object.hasOwn(offDeps, key)).toBe(false);
    }
    for (const [envVar, key] of [["INGEST_BUFFER_LOCK_BUSY", "bufferLockBusy"], ["INGEST_BUFFER_LOCK_RETRY", "bufferLockRetry"]] as const) {
      for (const v of ["on", "ON", " On ", "on\t"]) {
        const on = (await ingestSettings({ ...full, [envVar]: v }))!;
        expect(on[key]).toBe(true);
        expect(deliverDepsFor(on, async () => ({ rows: [] }), target as never)[key]).toBe(true);
        const other = key === "bufferLockBusy" ? "bufferLockRetry" : "bufferLockBusy";
        expect(Object.hasOwn(on, other)).toBe(false);
        expect(on).toEqual({ ...off, [key]: true } as never);
      }
      for (const v of ["", " ", "off", "OFF", "1", "true", "yes", "o n"]) {
        const c = (await ingestSettings({ ...full, [envVar]: v }))!;
        expect(c).toEqual(off);
      }
    }
  });

  test("self-host: not read (canonical acceptance only, the buffer stages never run); the settings are exactly as before", () => {
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
    const on = selfhostIngestConfig({ ...enabled, INGEST_BUFFER_LOCK_BUSY: "on", INGEST_BUFFER_LOCK_RETRY: "on" });
    expect(on).toEqual(base);
    expect(Object.hasOwn(on, "bufferLockBusy")).toBe(false);
    expect(Object.hasOwn(on, "bufferLockRetry")).toBe(false);
  });
});
