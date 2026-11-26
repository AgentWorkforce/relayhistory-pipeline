/**
 * INGEST_LOCK_RETRY_BUDGET_MS (deliver.ts withLockRetry; default off, buffered acceptance only): the buffered claim,
 * buffer_intent and buffer_receipt ledger calls are re-issued with the same input, after jittered exponential pauses,
 * while one per-delivery budget lasts, when they fail 55P03 / 40P01 or with a server-reported 57014. Proven here:
 * - unset / 0: byte-identical (golden text, one call, no pause, no jitter, no new route-line field);
 * - 55P03 then success, 40P01 then success (each covered stage), 57014 (code basis) then success; a name-only
 *   client timeout is never retried;
 * - exhaustion answers exactly what the flag-off delivery answers (503, or the 429 with the busy flags);
 * - a successful call is never repeated, and no other stage (buffer_put) is re-run by a retry;
 * - pause jitter bounds, the budget accounting and the budget vs deadline refusal (deliver.ts and worker.ts);
 * - PGlite (every migration, origin fence on): a first attempt that did the work and was rolled back is repeated into
 *   exactly one committed batch, one stage intent and one outbox row (the idempotency the retry relies on).
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { prepareDelivery, deliveryAccount } from "../vendor/delivery/prepare.ts";
import {
  deliverBatch, IngestUnavailable, LOCK_RETRY, LOCK_RETRY_BUDGET_MS_MAX, LOCK_RETRY_BUDGET_MS_MIN, LOCK_RETRY_OUTCOMES,
  lockRetryBudgetRefusal, lockRetryFieldsOf, lockRetryPauseMs, type BufferLockRetryFields,
} from "../deliver.ts";
import { resetOriginFenceHint, sqlLedger } from "../ledger-sql.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { lockRetryBudgetMs } from "../worker.ts";
import { LOCK_RETRY_OUTCOME_VALUES, sanitizeLogField } from "../../shared/log.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(120_000);
const cleanups: (() => unknown)[] = [];
afterEach(async () => { resetOriginFenceHint(); for (const f of cleanups.splice(0).reverse()) await f(); });
const enc = (s: string) => new TextEncoder().encode(s);

type Stage = "claim" | "buffer_intent" | "buffer_receipt";
const STAGES: Stage[] = ["claim", "buffer_intent", "buffer_receipt"];
const METHOD = { claim: "claim", buffer_intent: "prepareBufferStage", buffer_receipt: "commitBuffered" } as const;
const BUDGET = 10_000;

/** The error the real SQL ledger raises for a driver error carrying `code` (its own mapping and diagnosis). */
async function ledgerError(code: string | undefined, name?: string): Promise<unknown> {
  const pgErr = Object.assign(new Error("could not obtain lock"), code ? { code } : {}, name ? { name } : {});
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

type Flags = Record<string, unknown> & { lockRetryBudgetMs?: number };

/**
 * One signed buffered-mode archive-recovery POST (buffer-lock-busy.test.ts harness, plus the claim). `failures[stage]`
 * lists what each successive call of that stage throws (undefined: succeed). A fake clock advances by every pause and
 * by `attemptMs` per covered call, so the budget accounting is deterministic.
 */
async function recovery(failures: Partial<Record<Stage, unknown[]>>, flags: Flags, opts: { attemptMs?: number; random?: () => number } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "lock-retry" })], { batch_id: "lock-retry" });
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
  let now = 1_000_000;
  const calls: Record<Stage, unknown[]> = { claim: [], buffer_intent: [], buffer_receipt: [] };
  const stageToken = "00000000-0000-4000-8000-000000000001";
  const expiresAt = new Date(Date.now() + 60_000).toISOString();
  const custodyAck = { version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true };
  const originalClaim = ledger.claim.bind(ledger);
  const answer = {
    claim: (input: unknown) => originalClaim(input as never),
    buffer_intent: async () => ({ stageToken, expiresAt }),
    buffer_receipt: async () => custodyAck,
  };
  for (const stage of STAGES) {
    (ledger as unknown as Record<string, unknown>)[METHOD[stage]] = async (input: unknown) => {
      const n = calls[stage].push(structuredClone(input));
      now += opts.attemptMs ?? 0;
      const fail = failures[stage]?.[n - 1];
      if (fail !== undefined) throw fail;
      return answer[stage](input);
    };
  }
  let puts = 0;
  const store = Object.assign(Object.create(s.store), { put: (...a: unknown[]) => { puts++; return (s.store.put as (...x: unknown[]) => unknown)(...a); } });
  const sleeps: number[] = [];
  const oneShot: BufferLockRetryFields[] = [];
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: {
      ledger, target: { store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered",
      random: opts.random ?? (() => 0.5), sleep: async (ms: number) => { sleeps.push(ms); now += ms; }, lockRetryClock: () => now,
      bufferLockRetryLog: (f: BufferLockRetryFields) => { oneShot.push(f); }, ...flags,
    } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const res = await app.fetch(new Request(`https://x${path}`, {
    method: "POST", body,
    headers: { "content-type": RECOVERY_CONTENT_TYPE, "x-correlation-id": "corr-lock-retry", ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) },
  }));
  const text = await res.text();
  return {
    status: res.status, retryAfter: res.headers.get("retry-after"), text, json: JSON.parse(text) as { error?: { code: string } },
    route: logged.find((f) => f.route === "delivery.recovery")!, alerts: logged.filter((f) => f.route === "ingest.lock_alert"),
    calls, sleeps, oneShot, puts: () => puts, elapsed: () => now - 1_000_000,
  };
}
type Run = Awaited<ReturnType<typeof recovery>>;
/** Normalised for comparison: the per-request id and the route's wall time. */
const norm = (r: Run) => ({ status: r.status, retryAfter: r.retryAfter, text: r.text.replace(/"requestId":"[^"]*"/g, ""), route: { ...r.route, duration_ms: 0 } });
const BUSY_FLAGS = { claimLockBusy: true, claimTimeoutBusy: true, bufferLockBusy: true } as const;
const noRetryFields = (r: Run) => { expect(r.route).not.toHaveProperty("lock_retry_attempts"); expect(r.route).not.toHaveProperty("lock_retry_outcome"); };

describe("budget off (unset or 0): byte-identical", () => {
  test("a 55P03 / 40P01 / 57014 at each covered stage answers exactly as before: one call, no pause, no new field", async () => {
    for (const stage of STAGES)
      for (const code of ["55P03", "40P01", "57014"])
        for (const busy of [{}, BUSY_FLAGS]) {
          const thrown = await ledgerError(code);
          const unset = await recovery({ [stage]: [thrown] }, { ...busy });
          const zero = await recovery({ [stage]: [thrown] }, { ...busy, lockRetryBudgetMs: 0 });
          expect(norm(zero)).toEqual(norm(unset));
          for (const r of [unset, zero]) {
            expect(r.calls[stage].length).toBe(1);
            expect(r.sleeps).toEqual([]);
            noRetryFields(r);
          }
          if (!("bufferLockBusy" in busy)) expect(unset.text).toBe('{"error":{"code":"delivery_unavailable","message":"Durable delivery is temporarily unavailable"},"correlationId":"corr-lock-retry"}');
        }
  });

  test("a clean delivery is identical with the budget unset, 0 and set (apart from the two new route fields)", async () => {
    const unset = await recovery({}, {});
    const zero = await recovery({}, { lockRetryBudgetMs: 0 });
    const on = await recovery({}, { lockRetryBudgetMs: BUDGET });
    expect(unset.status).toBe(200);
    expect(norm(zero)).toEqual(norm(unset));
    noRetryFields(unset);
    const { lock_retry_attempts, ...rest } = on.route;
    expect(lock_retry_attempts).toBe(0);
    expect(on.route).not.toHaveProperty("lock_retry_outcome");
    expect({ ...norm(on), route: { ...rest, duration_ms: 0 } }).toEqual(norm(unset));
  });

  test("the canonical (non-buffered) path never retries, even with a budget", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    let commits = 0;
    const thrown = await ledgerError("55P03");
    const ledger = Object.assign(new MemoryLedger(), { commit: async () => { commits++; throw thrown; } });
    const err = await deliverBatch({ ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, lockRetryBudgetMs: BUDGET, sleep: async () => {} } as never,
      TENANT, await batch([record({ record_id: "c1" })])).then(() => null, (e: unknown) => e);
    expect((err as IngestUnavailable).stage).toBe("commit");
    expect(commits).toBe(1);
    expect(lockRetryFieldsOf(err)).toBeUndefined();
  });
});

describe("budget on: retried into a success", () => {
  for (const code of ["55P03", "40P01", "57014"])
    test(`${code} then success at each covered stage: 200, the same input twice, one jittered pause, succeeded_after_retry`, async () => {
      for (const stage of STAGES) {
        const r = await recovery({ [stage]: [await ledgerError(code)] }, { lockRetryBudgetMs: BUDGET, ...BUSY_FLAGS });
        expect(r.status).toBe(200);
        expect(r.calls[stage].length).toBe(2);
        expect(r.calls[stage][1]).toEqual(r.calls[stage][0]);
        for (const other of STAGES.filter((s) => s !== stage)) expect(r.calls[other].length).toBe(1);
        expect(r.sleeps).toEqual([lockRetryPauseMs(1, () => 0.5)]);
        expect(r.route).toMatchObject({ outcome: "accepted_buffered", lock_retry_attempts: 1, lock_retry_outcome: "succeeded_after_retry" });
        expect(r.alerts).toEqual([]);
        expect(r.puts()).toBe(1); // buffer_put is never re-run by a retry
        expect(r.oneShot).toEqual([]); // the budget supersedes INGEST_BUFFER_LOCK_RETRY
      }
    });

  test("several failures across stages share one budget; pauses grow exponentially", async () => {
    const l = await ledgerError("55P03");
    const d = await ledgerError("40P01");
    const r = await recovery({ claim: [l, l], buffer_intent: [d], buffer_receipt: [l] }, { lockRetryBudgetMs: BUDGET, bufferLockRetry: true }, { attemptMs: 250 });
    expect(r.status).toBe(200);
    expect([r.calls.claim.length, r.calls.buffer_intent.length, r.calls.buffer_receipt.length]).toEqual([3, 2, 2]);
    expect(r.sleeps).toEqual([1, 2, 3, 4].map((k) => lockRetryPauseMs(k, () => 0.5)));
    expect(r.route).toMatchObject({ lock_retry_attempts: 4, lock_retry_outcome: "succeeded_after_retry" });
    expect(r.oneShot).toEqual([]);
  });
});

describe("budget on: what is never retried", () => {
  test("a name-only client timeout, a connection error, 55000, 08006 and a plain error: one call, not_retryable, the same answer as flag off", async () => {
    for (const stage of STAGES)
      for (const thrown of [await ledgerError(undefined, "TimeoutError"), await ledgerError("08006"), await ledgerError("55000"), await ledgerError("57P01"), new Error("boom")]) {
        const off = await recovery({ [stage]: [thrown] }, { ...BUSY_FLAGS });
        const on = await recovery({ [stage]: [thrown] }, { ...BUSY_FLAGS, lockRetryBudgetMs: BUDGET });
        expect(on.calls[stage].length).toBe(1);
        expect(on.sleeps).toEqual([]);
        const { lock_retry_attempts, lock_retry_outcome, ...rest } = on.route;
        expect([lock_retry_attempts, lock_retry_outcome]).toEqual([0, "not_retryable"]);
        expect({ ...norm(on), route: { ...rest, duration_ms: 0 } }).toEqual(norm(off));
      }
  });

  test("a retry that meets a non-retryable error stops at once with not_retryable", async () => {
    const r = await recovery({ buffer_receipt: [await ledgerError("55P03"), await ledgerError("55000")] }, { lockRetryBudgetMs: BUDGET });
    expect(r.status).toBe(503);
    expect(r.calls.buffer_receipt.length).toBe(2);
    expect(r.route).toMatchObject({ error_code: "ledger_sqlstate_55000", lock_retry_attempts: 1, lock_retry_outcome: "not_retryable" });
  });

  test("a lock-wait IngestUnavailable labelled with another stage, thrown from the stage call, is not retried", async () => {
    const d = (await ledgerError("55P03") as { db: never }).db;
    const r = await recovery({ buffer_intent: [new IngestUnavailable("claim", "ledger_lock_timeout", d)] }, { lockRetryBudgetMs: BUDGET });
    expect(r.calls.buffer_intent.length).toBe(1);
    expect(r.route).toMatchObject({ stage: "claim", lock_retry_outcome: "not_retryable" });
  });

  test("no retry after the receipt committed: a failure after the acknowledgement is answered once, the commit is never repeated", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    let commits = 0;
    const ledger = Object.assign(new MemoryLedger(), {
      prepareBufferStage: async () => ({ stageToken: "00000000-0000-4000-8000-000000000002", expiresAt: new Date(Date.now() + 60_000).toISOString() }),
      // Committed, but the ACK fails the custody check after the call returned (custody_ack_missing).
      commitBuffered: async () => { commits++; return undefined; },
    });
    const raw = await batch([record({ record_id: "ack" })]);
    const prepared = await prepareDelivery(TENANT, raw);
    const custody = { sourceDigest: "d".repeat(64), witness: { batchDigest: prepared.batchDigest } };
    const sleeps: number[] = [];
    const err = await deliverBatch({ ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", custody,
      lockRetryBudgetMs: BUDGET, sleep: async (ms: number) => { sleeps.push(ms); } } as never, TENANT, raw).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(IngestUnavailable);
    expect([(err as IngestUnavailable).stage, (err as IngestUnavailable).cause_code]).toEqual(["buffer_receipt", "custody_ack_missing"]);
    expect(commits).toBe(1);
    expect(sleeps).toEqual([]);
    expect(lockRetryFieldsOf(err)).toEqual({ lock_retry_attempts: 0 });
  });

  test("a retried receipt re-checks upload consent after its pause; a refusal there ends the delivery (not_retryable)", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const thrown = await ledgerError("55P03");
    let commits = 0;
    const phases: string[] = [];
    const ledger = Object.assign(new MemoryLedger(), {
      prepareBufferStage: async () => ({ stageToken: "00000000-0000-4000-8000-000000000003", expiresAt: new Date(Date.now() + 60_000).toISOString() }),
      commitBuffered: async () => { commits++; throw thrown; },
    });
    const refused = new Error("consent narrowed");
    const consentGate = async (_d: ReadonlySet<string>, phase: string) => { phases.push(phase); if (phases.length === 3) throw refused; };
    const err = await deliverBatch({ ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", consentGate,
      lockRetryBudgetMs: BUDGET, sleep: async () => {} } as never, TENANT, await batch([record({ record_id: "consent" })])).then(() => null, (e: unknown) => e);
    expect(phases).toEqual(["admit", "commit", "commit"]);
    expect(commits).toBe(1); // the refused re-check stops the repeat before the second commit call
    expect((err as IngestUnavailable).stage).toBe("consent_commit");
    expect(lockRetryFieldsOf(err)).toEqual({ lock_retry_attempts: 1, lock_retry_outcome: "not_retryable" });
  });
});

describe("budget on: exhaustion keeps today's answer", () => {
  test("a lock wait that never clears answers exactly the flag-off answer (503, or 429 delivery_busy with the busy flags), exhausted", async () => {
    for (const stage of STAGES)
      for (const busy of [{}, BUSY_FLAGS]) {
        const thrown = await ledgerError("55P03");
        const always = Array.from({ length: 50 }, () => thrown);
        const off = await recovery({ [stage]: always }, { ...busy });
        const on = await recovery({ [stage]: always }, { ...busy, lockRetryBudgetMs: BUDGET }, { attemptMs: 250 });
        expect(on.status).toBe("bufferLockBusy" in busy ? 429 : 503);
        if ("bufferLockBusy" in busy) expect(on.json.error?.code).toBe("delivery_busy");
        const { lock_retry_attempts, lock_retry_outcome, ...rest } = on.route;
        expect(lock_retry_outcome).toBe("exhausted");
        expect(lock_retry_attempts).toBe(on.calls[stage].length - 1);
        expect({ ...norm(on), route: { ...rest, duration_ms: 0 } }).toEqual(norm(off));
        expect(on.alerts).toEqual(off.alerts);
        // Bounded: never more than the cap, and the retries never spent more than the budget.
        expect(on.calls[stage].length).toBeLessThanOrEqual(LOCK_RETRY.maxRetries + 1);
        // (Each stage's first attempt is not a retry; at most three of them, 250 ms each.)
        expect(on.elapsed()).toBeLessThanOrEqual(BUDGET + 3 * 250);
      }
  });

  test("the retry cap binds when attempts are instant; a slow lock wait binds on the budget", async () => {
    const thrown = await ledgerError("40P01");
    const always = Array.from({ length: 50 }, () => thrown);
    const fast = await recovery({ buffer_intent: always }, { lockRetryBudgetMs: BUDGET });
    expect(fast.calls.buffer_intent.length).toBe(LOCK_RETRY.maxRetries + 1);
    expect(fast.route).toMatchObject({ lock_retry_attempts: LOCK_RETRY.maxRetries, lock_retry_outcome: "exhausted" });
    // Every attempt takes the full 5 s statement_timeout: only one retry fits a 10 s budget.
    const timeout = await ledgerError("57014");
    const slow = await recovery({ claim: Array.from({ length: 5 }, () => timeout) }, { lockRetryBudgetMs: BUDGET, ...BUSY_FLAGS }, { attemptMs: LOCK_RETRY.attemptMaxMs });
    expect(slow.calls.claim.length).toBe(2);
    expect(slow.status).toBe(429); // claimTimeoutBusy applies to the last 57014 as today
    expect(slow.route).toMatchObject({ stage: "claim", error_code: "ledger_timeout", db_error_code: "57014", lock_retry_attempts: 1, lock_retry_outcome: "exhausted" });
    // The smallest budget admits exactly one retry of a worst-case attempt.
    const min = await recovery({ buffer_receipt: Array.from({ length: 5 }, () => timeout) }, { lockRetryBudgetMs: LOCK_RETRY_BUDGET_MS_MIN }, { attemptMs: LOCK_RETRY.attemptMaxMs, random: () => 0.999999 });
    expect(min.calls.buffer_receipt.length).toBe(2);
    expect(min.status).toBe(503);
  });

  test("a refused budget refuses the delivery before any ledger call (never runs unbounded)", async () => {
    for (const bad of [LOCK_RETRY_BUDGET_MS_MAX + 1, 1, -1, 1.5]) {
      const r = await recovery({}, { lockRetryBudgetMs: bad });
      expect(r.status).toBe(503);
      expect(r.route).toMatchObject({ stage: "precheck", error_code: "lock_retry_budget_refused" });
      expect(r.calls.claim.length).toBe(0);
    }
  });
});

describe("jitter and budget bounds", () => {
  test("retry k pauses an integer in [ceil(c/2), c], c = min(1000, 50 * 2^(k-1)); out-of-range random falls back to the floor", () => {
    for (let k = 1; k <= 12; k++) {
      const c = Math.min(LOCK_RETRY.pauseMaxMs, LOCK_RETRY.pauseBaseMs * 2 ** (k - 1));
      const lo = Math.ceil(c / 2);
      expect(lockRetryPauseMs(k, () => 0)).toBe(lo);
      expect(lockRetryPauseMs(k, () => 0.999999)).toBe(c);
      for (let i = 0; i < 200; i++) {
        const p = lockRetryPauseMs(k, Math.random);
        expect(Number.isInteger(p) && p >= lo && p <= c).toBe(true);
      }
      for (const v of [Number.NaN, 1, -1, Infinity]) expect(lockRetryPauseMs(k, () => v)).toBe(lo);
    }
    expect([1, 2, 3, 4, 5, 6, 7].map((k) => lockRetryPauseMs(k, () => 0.999999))).toEqual([50, 100, 200, 400, 800, 1000, 1000]);
  });

  test("the budget range follows the deadlines: max = min(client 30 s, Worker 90 s) - 15 s baseline - 5 s margin", () => {
    expect([LOCK_RETRY_BUDGET_MS_MIN, LOCK_RETRY_BUDGET_MS_MAX]).toEqual([5_050, 10_000]);
    expect(lockRetryBudgetRefusal(0)).toBeUndefined();
    expect(lockRetryBudgetRefusal(LOCK_RETRY_BUDGET_MS_MIN)).toBeUndefined();
    expect(lockRetryBudgetRefusal(LOCK_RETRY_BUDGET_MS_MAX)).toBeUndefined();
    expect(lockRetryBudgetRefusal(LOCK_RETRY_BUDGET_MS_MIN - 1)).toBe("lock_retry_budget_below_one_attempt");
    expect(lockRetryBudgetRefusal(LOCK_RETRY_BUDGET_MS_MAX + 1)).toBe("lock_retry_budget_exceeds_deadline");
    for (const bad of [-1, 1.5, Number.NaN, Infinity]) expect(lockRetryBudgetRefusal(bad)).toBe("lock_retry_budget_not_integer");
    // A shorter client timeout (or Worker deadline) refuses a budget that the default admits.
    expect(lockRetryBudgetRefusal(8_000, { clientTimeoutMs: 25_000, workerDeadlineMs: 90_000 })).toBe("lock_retry_budget_exceeds_deadline");
    expect(lockRetryBudgetRefusal(5_050, { clientTimeoutMs: 30_000, workerDeadlineMs: 20_000 })).toBe("lock_retry_budget_exceeds_deadline");
    expect(lockRetryBudgetRefusal(5_000, { clientTimeoutMs: 25_000, workerDeadlineMs: 90_000 })).toBe("lock_retry_budget_below_one_attempt");
  });

  test("INGEST_LOCK_RETRY_BUDGET_MS: unset / empty / 0 is off; a valid buffered value is admitted; anything else refuses startup", () => {
    for (const off of [undefined, "", " ", "0"]) expect(lockRetryBudgetMs(off, "buffered")).toBeUndefined();
    expect(lockRetryBudgetMs("7000", "buffered")).toBe(7000);
    expect(lockRetryBudgetMs(" 10000 ", "buffered")).toBe(10_000);
    for (const bad of ["10001", "5049", "60000", "abc", "-5", "1e4", "7000.5", "0x10", "+7000"])
      expect(() => lockRetryBudgetMs(bad, "buffered")).toThrow(/INGEST_LOCK_RETRY_BUDGET_MS must be/);
    expect(() => lockRetryBudgetMs("7000", "canonical")).toThrow(/needs buffered acceptance/);
    // The refusal never echoes the value.
    try { lockRetryBudgetMs("98765", "buffered"); } catch (e) { expect(String(e)).not.toContain("98765"); }
  });

  test("the log allowlist carries exactly the two fields, closed values only", () => {
    expect([...LOCK_RETRY_OUTCOME_VALUES].sort()).toEqual([...LOCK_RETRY_OUTCOMES].sort());
    for (const v of LOCK_RETRY_OUTCOMES) expect(sanitizeLogField("lock_retry_outcome", v)).toBe(v);
    expect(sanitizeLogField("lock_retry_outcome", "free text")).toBeUndefined();
    expect(sanitizeLogField("lock_retry_attempts", 3)).toBe(3);
    expect(sanitizeLogField("lock_retry_attempts", "3")).toBeUndefined();
    expect(sanitizeLogField("lock_retry_attempts", -1)).toBeUndefined();
  });
});

describe("PGlite: a rolled-back first attempt repeats into exactly one result (origin claim fence on)", () => {
  type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
  async function witness(f: Fixture, raw: any) {
    const p = await prepareDelivery(TENANT, raw);
    const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: TENANT,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    const d = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
    return { witness: w, sourceDigest: d };
  }
  const count = async (f: Fixture, table: string) => (await f.db.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${table}`)).rows[0]!.n;

  for (const [fn, code] of [["ingest_origin_claim_buffered_batch", "55P03"], ["ingest_origin_prepare_buffer_stage", "40P01"], ["ingest_origin_commit_buffered_batch", "57014"]] as const)
    test(`${fn}: the first call runs in full, is rolled back and answered ${code}; the retry yields one committed batch`, async () => {
      const f = await authorityFixture();
      cleanups.push(f.close);
      await f.seal();
      const raw = await batch([record({ record_id: "pg-" + code, revision: 2, session_id: "pg-s" })], { batch_id: "pg-" + code, account_id: await deliveryAccount(TENANT) });
      const custody = await witness(f, raw);
      let injected = 0;
      const seen: string[] = [];
      // The server's behaviour on 55P03 / 40P01 / 57014: the statement did (part of) its work, then its transaction aborted.
      const run = async (sql: string, params: unknown[]) => {
        seen.push(/sessions\.(\w+)\(/.exec(sql)?.[1] ?? sql);
        if (injected === 0 && sql.includes(`sessions.${fn}(`)) {
          injected++;
          await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
          await f.db.query(sql, params as never);
          await f.db.exec("ROLLBACK");
          throw Object.assign(new Error("injected"), { code });
        }
        return f.run(sql, params);
      };
      const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
      const sleeps: number[] = [];
      const result = await deliverBatch({ ...f.deps, ledger, custody, lockRetryBudgetMs: BUDGET, sleep: async (ms: number) => { sleeps.push(ms); } } as never, TENANT, raw);
      expect(result.outcome).toBe("accepted_buffered");
      expect(result.stats.lock_retry).toEqual({ lock_retry_attempts: 1, lock_retry_outcome: "succeeded_after_retry" });
      expect(injected).toBe(1);
      expect(seen.filter((s) => s === fn).length).toBe(2);
      expect(sleeps.length).toBe(1);
      expect(await count(f, "ingest_batches WHERE state='committed'")).toBe(1);
      expect(await count(f, "ingest_stage_intents")).toBe(1);
      expect(await count(f, "ingest_buffer_outbox")).toBe(1);
      expect(await count(f, "ingest_origin_claim_marks")).toBe(0);
      // And the client's own later retry of the same batch is the ordinary replay.
      const again = sqlLedger(f.run, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
      expect((await deliverBatch({ ...f.deps, ledger: again, custody } as never, TENANT, raw)).outcome).toBe("replay");
    });
});
