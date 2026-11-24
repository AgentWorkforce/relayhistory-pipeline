/**
 * Unit C, both parts (default off; buffered only; docs/design/buffer-commit-statement-budget.md).
 * Part 1, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: unset is unit A's or the base statement byte for byte; set, the
 * buffered commit (and only it) is BEGIN + SET LOCAL (one flight), the unchanged call, COMMIT; validation fails closed.
 * Part 2, INGEST_BUFFER_TIMEOUT_BUSY: at buffer_intent / buffer_receipt only, a 57014 by code joins the lock class
 * (retry once under INGEST_BUFFER_LOCK_RETRY, 429 with Retry-After 5-10 s under INGEST_BUFFER_LOCK_BUSY); flag off
 * byte-identical; a 57014 elsewhere and every 55P03 unchanged; the lock alert keeps sqlstate 57014 (real logLine).
 * The real-database proof is buffer-commit-statement-budget.postgres.test.ts.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { CLAIM_LOCK_RETRY_AFTER, type BufferLockRetryFields } from "../deliver.ts";
import { BUFFER_COMMIT_BUDGET_MS_MAX, BUFFER_COMMIT_BUDGET_MS_MIN, BUFFER_COMMIT_IDLE_IN_TRANSACTION_MS, bufferCommitBudgetOpen, bufferCommitLockWaitSql, resetOriginFenceHint, sqlLedger, type SqlRun } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { logLine } from "../../shared/log.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";
import { STACK_BUFFER_COMMIT_BUDGET_MS } from "../../../stacks/cloudflare/session-ingest.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { cleanups.splice(0).forEach((f) => f()); resetOriginFenceHint(); });
const enc = (s: string) => new TextEncoder().encode(s);
const G = "a".repeat(64);
const pgError = (code: string) => Object.assign(new Error(`statement failed ${TENANT.orgId}`), { code });
type Sent = { text: string; params: unknown[] };
const recorder = (answer: (text: string) => Record<string, unknown>[] | Error = () => []) => {
  const sent: Sent[] = [];
  const run: SqlRun = async (text, params) => { sent.push({ text, params: [...params] }); const a = answer(text); if (a instanceof Error) throw a; return { rows: a }; };
  return { sent, run };
};
const COMMIT_INPUT = { tenant: TENANT, originId: "o", batchId: "b", batchDigest: "d".repeat(64), claimToken: "00000000-0000-4000-8000-000000000002", stageToken: "00000000-0000-4000-8000-000000000001" };
const committed = (t: string) => t.includes("commit_buffered") ? [{ result: { kind: "committed" } }] : [];
const CALL = (fn = "ingest_commit_buffered_batch") => `SELECT sessions.${fn}($1::jsonb) AS result`;
const BODY = JSON.stringify({ ...COMMIT_INPUT, generation: G });
const ledgerWith = (run: SqlRun, extra: Record<string, unknown> = {}) => sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, ...extra } as never);

describe("Part 1: statement budget", () => {
  test("unset: the base statement; unit A alone: unit A's statement; both: three flights, the call unchanged", async () => {
    const trace = async (extra: Record<string, unknown>) => { const r = recorder(committed); await ledgerWith(r.run, extra).commitBuffered!(COMMIT_INPUT as never); return r.sent; };
    expect(await trace({})).toEqual([{ text: CALL(), params: [BODY] }]);
    expect(await trace({ bufferCommitLockWaitMs: 3000 })).toEqual([{ text: bufferCommitLockWaitSql("ingest_commit_buffered_batch"), params: [BODY, "3000ms"] }]);
    expect(await trace({ bufferCommitBudgetMs: 6000 })).toEqual([
      { text: "BEGIN; SET LOCAL statement_timeout = '6000ms'; SET LOCAL idle_in_transaction_session_timeout = '1000ms'", params: [] },
      { text: CALL(), params: [BODY] }, { text: "COMMIT", params: [] }]);
    expect(await trace({ bufferCommitBudgetMs: 7000, bufferCommitLockWaitMs: 4000 })).toEqual([
      { text: "BEGIN; SET LOCAL statement_timeout = '7000ms'; SET LOCAL idle_in_transaction_session_timeout = '1000ms'; SET LOCAL lock_timeout = '4000ms'", params: [] },
      { text: CALL(), params: [BODY] }, { text: "COMMIT", params: [] }]);
    expect(BUFFER_COMMIT_IDLE_IN_TRANSACTION_MS).toBe(1000);
  });

  test("set: prepare and claim are unchanged (one plain statement each)", async () => {
    const trace = async (extra: Record<string, unknown>) => {
      const r = recorder(committed); const l = ledgerWith(r.run, extra);
      await l.prepareBufferStage!({ ...COMMIT_INPUT } as never).catch(() => {}); await l.claim({ tenant: TENANT } as never).catch(() => {});
      return r.sent;
    };
    const off = await trace({});
    expect(off.map((s) => s.text)).toEqual([CALL("ingest_prepare_buffer_stage"), CALL("ingest_claim_buffered_batch")]);
    expect(await trace({ bufferCommitBudgetMs: 6000, bufferCommitLockWaitMs: 3000 })).toEqual(off);
  });

  test("failures: the call fails -> ROLLBACK, first error kept; the opening flight fails -> ROLLBACK; COMMIT fails -> ROLLBACK, mapped", async () => {
    for (const [failing, code, mapped] of [["CALL", "57014", "ledger_timeout"], ["CALL", "55P03", "ledger_lock_timeout"], ["BEGIN", "08006", "ledger_connection"], ["COMMIT", "40001", "ledger_sqlstate_40001"], ["COMMIT", "ECONNRESET", "ledger_connection"]] as const) {
      const r = recorder((t) => (failing === "CALL" ? t.includes("commit_buffered") : t.startsWith(failing)) ? pgError(code) : committed(t));
      const e = await ledgerWith(r.run, { bufferCommitBudgetMs: 6000 }).commitBuffered!(COMMIT_INPUT as never).catch((x: unknown) => x);
      expect((e as LedgerUnavailableError).code).toBe(mapped);
      expect(r.sent.at(-1)!.text).toBe("ROLLBACK");
      expect(r.sent.filter((s) => s.text === "ROLLBACK")).toHaveLength(1);
    }
    // A failing ROLLBACK never masks the first error.
    const r = recorder((t) => t === "ROLLBACK" ? pgError("08006") : t.includes("commit_buffered") ? pgError("57014") : []);
    const e = await ledgerWith(r.run, { bufferCommitBudgetMs: 6000 }).commitBuffered!(COMMIT_INPUT as never).catch((x: unknown) => x);
    expect((e as LedgerUnavailableError).code).toBe("ledger_timeout");
  });

  test("origin entry point absent: each attempt is its own transaction, the fallback carries the same budget", async () => {
    const r = recorder((t) => t.includes("ingest_origin_commit") ? Object.assign(new Error("function sessions.ingest_origin_commit_buffered_batch(jsonb) does not exist"), { code: "42883" }) : committed(t));
    await ledgerWith(r.run, { originClaimFence: true, bufferCommitBudgetMs: 6000 }).commitBuffered!(COMMIT_INPUT as never);
    expect(r.sent.map((s) => s.text.split(" ")[0] + (s.text.includes("origin") ? ":origin" : ""))).toEqual(["BEGIN;", "SELECT:origin", "ROLLBACK", "BEGIN;", "SELECT", "COMMIT"]);
  });

  test("validation: range, integer, buffered only, and the lock wait must end 1 s before the budget", async () => {
    expect([BUFFER_COMMIT_BUDGET_MS_MIN, BUFFER_COMMIT_BUDGET_MS_MAX]).toEqual([5001, 7000]);
    expect(STACK_BUFFER_COMMIT_BUDGET_MS).toEqual({ min: 5001, max: 7000, roleLockTimeout: 2000, headroom: 1000 });
    const run = async () => ({ rows: [] });
    for (const ms of [5000, 7001, 0, 6000.5, Number.NaN, "6000" as never]) expect(() => ledgerWith(run, { bufferCommitBudgetMs: ms })).toThrow("ledger_buffer_commit_budget_invalid");
    expect(() => sqlLedger(run, { bufferCommitBudgetMs: 6000 })).toThrow("ledger_buffer_commit_budget_invalid");
    for (const ms of [5001, 6000, 7000]) expect(() => ledgerWith(run, { bufferCommitBudgetMs: ms })).not.toThrow();
    expect(() => ledgerWith(run, { bufferCommitBudgetMs: 5001, bufferCommitLockWaitMs: 4000 })).not.toThrow();
    expect(() => ledgerWith(run, { bufferCommitBudgetMs: 5001, bufferCommitLockWaitMs: 4002 })).toThrow(); // A's own range refuses first
    expect(bufferCommitBudgetOpen(6000)).not.toContain("lock_timeout");
    const { ingestSettings, bufferCommitBudgetMs } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const buffered = { ...full, INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: G, INGEST_INLINE_MAX_SESSIONS: "0" };
    const off = (await ingestSettings(buffered))!;
    expect(Object.hasOwn(off, "bufferCommitBudgetMs")).toBe(false);
    expect(await ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: "6000" })).toEqual({ ...off, bufferCommitBudgetMs: 6000 });
    const msg = "INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS must be an integer from 5001 to 7000 (buffered acceptance only)";
    expect(await ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: "" })).toEqual(off); // empty = unset
    for (const bad of [" ", "5000", "7001", "6000 ", " 6000", "+6000", "6000ms", "6s", "6e3", "6000.0", "06000", "60000"]) {
      expect(() => bufferCommitBudgetMs(bad)).toThrow(msg);
      await expect(ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: bad })).rejects.toThrow(msg);
    }
    await expect(ingestSettings({ ...full, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: "6000" })).rejects.toThrow("needs buffered acceptance");
    await expect(ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: "5001", INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "4000" })).resolves.toMatchObject({ bufferCommitBudgetMs: 5001, bufferCommitLockWaitMs: 4000 });
  });
});

// ---- Part 2 through the real deliver path (the recovery route; buffer-lock-busy.test.ts harness) ----
type Flags = { bufferLockBusy?: true; bufferLockRetry?: true; bufferTimeoutBusy?: true; claimTimeoutBusy?: true };
type Where = "buffer_intent" | "buffer_receipt" | "claim";
/** The error the real SQL ledger raises for a driver error with `code` (or a name-only client timeout). */
async function ledgerError(code: string, nameOnly = false): Promise<unknown> {
  const e = nameOnly ? Object.assign(new Error("timeout"), { name: "TimeoutError" }) : pgError(code);
  try { await sqlLedger(async () => { throw e; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}
/** One signed buffered recovery POST: the first `times` calls of stage `where` throw the real ledger error for `code`. */
async function recovery(code: string, flags: Flags, where: Where = "buffer_receipt", times = 99, nameOnly = false) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "commit-budget" })], { batch_id: "commit-budget" });
  const prepared = await prepareDelivery(TENANT, raw);
  const sourceDigest = "d".repeat(64);
  const custody = { sourceDigest, witness: {
    version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
    tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
    privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
    records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true as const, deletions: [], resets: [] } } };
  const stageToken = "00000000-0000-4000-8000-000000000001";
  const thrown = await ledgerError(code, nameOnly);
  const calls: Record<Where, number> = { buffer_intent: 0, buffer_receipt: 0, claim: 0 };
  const L = ledger as unknown as Record<string, any>;
  const answers: Record<"buffer_intent" | "buffer_receipt", () => unknown> = {
    buffer_intent: () => ({ stageToken, expiresAt: new Date(Date.now() + 60_000).toISOString() }),
    buffer_receipt: () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true }) };
  L.prepareBufferStage = async () => { if (where === "buffer_intent" && ++calls.buffer_intent <= times) throw thrown; return answers.buffer_intent(); };
  L.commitBuffered = async () => { if (where === "buffer_receipt" && ++calls.buffer_receipt <= times) throw thrown; return answers.buffer_receipt(); };
  const claim = L.claim.bind(ledger);
  L.claim = async (i: unknown) => { if (where === "claim" && ++calls.claim <= times) throw thrown; return claim(i); };
  const retries: BufferLockRetryFields[] = [];
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered", ...flags,
      random: () => 0.5, sleep: async () => {}, bufferLockRetryLog: (f: BufferLockRetryFields) => { retries.push(f); } } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body,
    headers: { "content-type": RECOVERY_CONTENT_TYPE, "x-correlation-id": "corr-commit-budget", ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) } }));
  const text = await res.text();
  const route = logged.find((f) => f.route === "delivery.recovery")!;
  return { status: res.status, retryAfter: res.headers.get("retry-after"), text: text.replace(/"requestId":"[^"]*"/g, ""), route: { ...route, duration_ms: 0 },
    alerts: logged.filter((f) => f.route === "ingest.lock_alert"), retries, calls: calls[where] };
}
const BUFFER: ("buffer_intent" | "buffer_receipt")[] = ["buffer_intent", "buffer_receipt"];
const ALERT = { buffer_intent: "staged_intent", buffer_receipt: "staged_ack" } as const;
const COMBOS: Flags[] = [{}, { bufferLockRetry: true }, { bufferLockBusy: true }, { bufferLockBusy: true, bufferLockRetry: true }];
const strip = (r: Awaited<ReturnType<typeof recovery>>) => ({ status: r.status, retryAfter: r.retryAfter, text: r.text, route: r.route, alerts: r.alerts, retries: r.retries, calls: r.calls });
const EXPECTED_RA = String(CLAIM_LOCK_RETRY_AFTER.min + Math.floor(0.5 * (CLAIM_LOCK_RETRY_AFTER.max - CLAIM_LOCK_RETRY_AFTER.min + 1)));

describe("Part 2: INGEST_BUFFER_TIMEOUT_BUSY", () => {
  test("flag off: a buffer-stage 57014 is byte-identical to the base in every retry/busy combination (one attempt, 503, no Retry-After)", async () => {
    for (const where of BUFFER) for (const flags of COMBOS) {
      const r = await recovery("57014", flags, where);
      expect(r.status).toBe(503); expect(r.retryAfter).toBeNull(); expect(r.calls).toBe(1); expect(r.retries).toEqual([]);
      expect(r.route).toMatchObject({ stage: where, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT[where], sqlstate: "57014" }]);
      // And the flag on with neither retry nor busy changes nothing either.
      if (!flags.bufferLockBusy && !flags.bufferLockRetry) expect(strip(await recovery("57014", { bufferTimeoutBusy: true }, where))).toEqual(strip(r));
    }
  });

  test("flag on, retry alone: one retry, then the 503 with ledger_timeout; the retry line names 57014", async () => {
    for (const where of BUFFER) {
      const r = await recovery("57014", { bufferTimeoutBusy: true, bufferLockRetry: true }, where);
      expect(r.calls).toBe(2); expect(r.status).toBe(503); expect(r.retryAfter).toBeNull();
      expect(r.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage: where, db_error_code: "57014", outcome: "failed", error_code: "ledger_timeout", sqlstate: "57014" }]);
      const ok = await recovery("57014", { bufferTimeoutBusy: true, bufferLockRetry: true }, where, 1);
      expect(ok.status).toBe(200); expect(ok.calls).toBe(2);
      expect(ok.retries).toEqual([{ route: "ingest.buffer_lock_retry", stage: where, db_error_code: "57014", outcome: "ok" }]);
    }
  });

  test("flag on, busy alone: the 429 with the lock-busy Retry-After; the first error is kept on the route line", async () => {
    for (const where of BUFFER) {
      const r = await recovery("57014", { bufferTimeoutBusy: true, bufferLockBusy: true }, where);
      expect(r.calls).toBe(1); expect(r.status).toBe(429);
      expect(r.retryAfter).toBe(EXPECTED_RA);
      expect(Number(r.retryAfter)).toBeGreaterThanOrEqual(CLAIM_LOCK_RETRY_AFTER.min); expect(Number(r.retryAfter)).toBeLessThanOrEqual(CLAIM_LOCK_RETRY_AFTER.max);
      // The same header as today's lock-busy answer for a 55P03 at the same stage.
      expect((await recovery("55P03", { bufferLockBusy: true }, where)).retryAfter).toBe(r.retryAfter);
      expect(r.route).toMatchObject({ stage: where, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.retries).toEqual([]);
    }
  });

  test("flag on, both: retry once (bounded), then the 429; retry exhausted keeps the first error and the alert input", async () => {
    for (const where of BUFFER) {
      const r = await recovery("57014", { bufferTimeoutBusy: true, bufferLockBusy: true, bufferLockRetry: true }, where);
      expect(r.calls).toBe(2); expect(r.status).toBe(429); expect(r.retryAfter).toBe(EXPECTED_RA);
      expect(r.retries).toHaveLength(1);
      expect(r.route).toMatchObject({ stage: where, error_code: "ledger_timeout", db_error_code: "57014" });
      expect(r.alerts).toEqual([{ route: "ingest.lock_alert", stage: ALERT[where], sqlstate: "57014" }]);
      // Rendered through the real logger allowlist: the alert line keeps stage and sqlstate, nothing else.
      const line = JSON.parse(logLine("info", "session ingest", r.alerts[0]!));
      expect(line).toMatchObject({ route: "ingest.lock_alert", stage: ALERT[where], sqlstate: "57014" });
      expect(JSON.stringify(line)).not.toContain(TENANT.orgId);
    }
  });

  test("never: a 57014 at the claim, a name-only client timeout, or any 55P03 changes because of the flag", async () => {
    for (const flags of COMBOS) {
      // Claim: today's handling (claimTimeoutBusy is the claim's own flag), identical with the buffer flag on.
      expect(strip(await recovery("57014", { ...flags, bufferTimeoutBusy: true }, "claim"))).toEqual(strip(await recovery("57014", flags, "claim")));
      for (const where of BUFFER) {
        expect(strip(await recovery("57014", { ...flags, bufferTimeoutBusy: true }, where, 99, true))).toEqual(strip(await recovery("57014", flags, where, 99, true)));
        expect(strip(await recovery("55P03", { ...flags, bufferTimeoutBusy: true }, where))).toEqual(strip(await recovery("55P03", flags, where)));
      }
    }
    const claim = await recovery("57014", { bufferTimeoutBusy: true, bufferLockBusy: true, bufferLockRetry: true }, "claim");
    expect(claim.status).toBe(503); expect(claim.calls).toBe(1);
  });

  test("Worker: INGEST_BUFFER_TIMEOUT_BUSY on (any case) reaches the deliver deps, buffered only; anything else is off", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const buffered = { ...full, INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: G, INGEST_INLINE_MAX_SESSIONS: "0" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const off = (await ingestSettings(buffered))!;
    expect(Object.hasOwn(deliverDepsFor(off, async () => ({ rows: [] }), target as never), "bufferTimeoutBusy")).toBe(false);
    for (const v of ["on", "ON", " On "]) {
      const on = (await ingestSettings({ ...buffered, INGEST_BUFFER_TIMEOUT_BUSY: v }))!;
      expect(on).toEqual({ ...off, bufferTimeoutBusy: true });
      expect(deliverDepsFor(on, async () => ({ rows: [] }), target as never).bufferTimeoutBusy).toBe(true);
    }
    for (const v of ["", "off", "1", "true"]) expect(await ingestSettings({ ...buffered, INGEST_BUFFER_TIMEOUT_BUSY: v })).toEqual(off);
    await expect(ingestSettings({ ...full, INGEST_BUFFER_TIMEOUT_BUSY: "on" })).rejects.toThrow("INGEST_BUFFER_TIMEOUT_BUSY needs buffered acceptance");
  });
});
