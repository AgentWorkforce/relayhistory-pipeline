/**
 * INGEST_BUFFER_COMMIT_LOCK_WAIT_MS (default unset; buffered only; docs/design/buffer-commit-lock-wait.md).
 * - Unset: every ledger statement is exactly the base text with exactly one parameter (golden strings).
 * - Set: only the buffered commit (per-user and origin entry points) changes, to bufferCommitLockWaitSql with the
 *   value as `<ms>ms`; still one statement per call. The claim and the stage prepare are unchanged.
 * - Validation: the ledger, the Worker settings and their wiring refuse anything outside 2001..4000 (fail closed).
 * - Class behaviour through the real deliver path: a commit that ends 55P03 under the option is handled exactly like
 *   today's 55P03 (retry and busy flags, each on and off); one that ends 57014 stays ledger_timeout, never retried
 *   or converted, in every flag state.
 * The real-lock proof is buffer-commit-lock-wait.postgres.test.ts.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { BufferLockRetryFields } from "../deliver.ts";
import { BUFFER_COMMIT_LOCK_WAIT_MS_MAX, BUFFER_COMMIT_LOCK_WAIT_MS_MIN, bufferCommitLockWaitSql, resetOriginFenceHint, sqlLedger, type SqlRun } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { batch, MemoryLedger, record, scratchStore, SIGNING_KEY, signedHeaders, TENANT } from "./support.ts";
import { STACK_BUFFER_COMMIT_LOCK_WAIT_MS } from "../../../stacks/cloudflare/session-ingest.ts";

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
const committed = () => [{ result: { kind: "committed" } }];

describe("unset (the default): the base statements, byte for byte", () => {
  test("commit, prepare and claim send exactly the one-parameter base text, one statement each", async () => {
    for (const originClaimFence of [false, true]) {
      const { sent, run } = recorder((t) => t.includes("commit") ? committed() : []);
      const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, ...(originClaimFence ? { originClaimFence } : {}) });
      await ledger.commitBuffered!(COMMIT_INPUT as never);
      await ledger.prepareBufferStage!({ ...COMMIT_INPUT } as never).catch(() => {});
      await ledger.claim({ tenant: TENANT } as never).catch(() => {});
      const fn = (name: string) => originClaimFence ? `ingest_origin_${name}` : `ingest_${name}`;
      expect(sent.map((s) => s.text)).toEqual([
        `SELECT sessions.${fn("commit_buffered_batch")}($1::jsonb) AS result`,
        `SELECT sessions.${fn("prepare_buffer_stage")}($1::jsonb) AS result`,
        `SELECT sessions.${fn("claim_buffered_batch")}($1::jsonb) AS result`,
      ]);
      for (const s of sent) expect(s.params).toHaveLength(1);
      expect(sent[0]!.params[0]).toBe(JSON.stringify({ ...COMMIT_INPUT, generation: G }));
    }
  });

  test("an explicit undefined is the same as absent", async () => {
    const a = recorder(committed), b = recorder(committed);
    await sqlLedger(a.run, { acceptanceMode: "buffered", expectedGeneration: G }).commitBuffered!(COMMIT_INPUT as never);
    await sqlLedger(b.run, { acceptanceMode: "buffered", expectedGeneration: G, bufferCommitLockWaitMs: undefined }).commitBuffered!(COMMIT_INPUT as never);
    expect(b.sent).toEqual(a.sent);
  });
});

describe("set: only the buffered commit changes", () => {
  test("the commit is ONE statement with the setting; prepare and claim are byte-identical to unset", async () => {
    for (const originClaimFence of [false, true]) {
      const trace = async (ms?: number) => {
        const { sent, run } = recorder((t) => t.includes("commit") ? committed() : []);
        const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, ...(originClaimFence ? { originClaimFence } : {}), ...(ms === undefined ? {} : { bufferCommitLockWaitMs: ms }) });
        await ledger.commitBuffered!(COMMIT_INPUT as never);
        const commit = [...sent]; sent.length = 0;
        await ledger.prepareBufferStage!({ ...COMMIT_INPUT } as never).catch(() => {});
        await ledger.claim({ tenant: TENANT } as never).catch(() => {});
        return { commit, rest: sent };
      };
      const off = await trace(), on = await trace(3000);
      const fn = originClaimFence ? "ingest_origin_commit_buffered_batch" : "ingest_commit_buffered_batch";
      expect(on.commit).toEqual([{ text: bufferCommitLockWaitSql(fn), params: [JSON.stringify({ ...COMMIT_INPUT, generation: G }), "3000ms"] }]);
      expect(on.commit[0]!.text).toBe(`SELECT sessions.${fn}(CASE WHEN set_config('lock_timeout', $2, true) IS NOT NULL THEN $1::jsonb END) AS result`);
      expect(on.rest).toEqual(off.rest);
    }
  });

  test("origin entry point absent (42883 naming it): the per-user fallback carries the same setting", async () => {
    const { sent, run } = recorder((t) => t.includes("ingest_origin_commit")
      ? Object.assign(new Error("function sessions.ingest_origin_commit_buffered_batch(jsonb) does not exist"), { code: "42883" })
      : committed());
    await sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, originClaimFence: true, bufferCommitLockWaitMs: 4000 }).commitBuffered!(COMMIT_INPUT as never);
    expect(sent.map((s) => [s.text, s.params[1]])).toEqual([
      [bufferCommitLockWaitSql("ingest_origin_commit_buffered_batch"), "4000ms"],
      [bufferCommitLockWaitSql("ingest_commit_buffered_batch"), "4000ms"],
    ]);
  });

  test("errors map as before: 55P03 -> ledger_lock_timeout, 57014 -> ledger_timeout, with the SQLSTATE in the diagnosis", async () => {
    for (const [code, mapped] of [["55P03", "ledger_lock_timeout"], ["40P01", "ledger_lock_timeout"], ["57014", "ledger_timeout"]] as const) {
      const { run } = recorder(() => pgError(code));
      const e = await sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, bufferCommitLockWaitMs: 3000 }).commitBuffered!(COMMIT_INPUT as never).catch((x: unknown) => x);
      expect(e).toBeInstanceOf(LedgerUnavailableError);
      expect((e as LedgerUnavailableError).code).toBe(mapped);
      expect((e as LedgerUnavailableError).db?.db_error_code).toBe(code);
    }
  });
});

describe("validation (fail closed)", () => {
  test("the bounds are the same everywhere they are written", () => {
    expect([BUFFER_COMMIT_LOCK_WAIT_MS_MIN, BUFFER_COMMIT_LOCK_WAIT_MS_MAX]).toEqual([2001, 4000]);
    expect(STACK_BUFFER_COMMIT_LOCK_WAIT_MS).toEqual({ min: BUFFER_COMMIT_LOCK_WAIT_MS_MIN, max: BUFFER_COMMIT_LOCK_WAIT_MS_MAX });
  });

  test("the ledger refuses to build outside 2001..4000, a non-integer, or outside buffered acceptance", () => {
    const run = async () => ({ rows: [] });
    for (const ms of [2000, 4001, 0, -3000, 3000.5, Number.NaN, Number.POSITIVE_INFINITY, 5000, "3000" as never])
      expect(() => sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, bufferCommitLockWaitMs: ms })).toThrow("ledger_buffer_commit_lock_wait_invalid");
    expect(() => sqlLedger(run, { bufferCommitLockWaitMs: 3000 })).toThrow("ledger_buffer_commit_lock_wait_invalid");
    expect(() => sqlLedger(run, { acceptanceMode: "canonical", bufferCommitLockWaitMs: 3000 })).toThrow("ledger_buffer_commit_lock_wait_invalid");
    for (const ms of [2001, 3000, 4000]) expect(() => sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: G, bufferCommitLockWaitMs: ms })).not.toThrow();
  });

  test("Worker: unset is absent; 2001..4000 reaches the ledger; anything else, empty included, refuses the settings", async () => {
    const { ingestSettings, deliverDepsFor, bufferCommitLockWaitMs } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const buffered = { ...full, INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: G, INGEST_INLINE_MAX_SESSIONS: "0" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const off = (await ingestSettings(buffered))!;
    expect(Object.hasOwn(off, "bufferCommitLockWaitMs")).toBe(false);
    for (const [raw, ms] of [["2001", 2001], ["3000", 3000], ["4000", 4000]] as const) {
      expect(bufferCommitLockWaitMs(raw)).toBe(ms);
      const on = (await ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: raw }))!;
      expect(on).toEqual({ ...off, bufferCommitLockWaitMs: ms });
      const { sent, run } = recorder(committed);
      await deliverDepsFor(on, run, target as never).ledger.commitBuffered!(COMMIT_INPUT as never);
      expect(sent).toEqual([{ text: bufferCommitLockWaitSql("ingest_commit_buffered_batch"), params: [JSON.stringify({ ...COMMIT_INPUT, generation: G }), `${ms}ms`] }]);
    }
    const plain = recorder(committed);
    await deliverDepsFor(off, plain.run, target as never).ledger.commitBuffered!(COMMIT_INPUT as never);
    expect(plain.sent.map((s) => s.text)).toEqual(["SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result"]);
    const message = "INGEST_BUFFER_COMMIT_LOCK_WAIT_MS must be an integer from 2001 to 4000 (buffered acceptance only)";
    for (const bad of ["", " ", "2000", "4001", "5000", "0", "02500", "3000 ", " 3000", "+3000", "-3000", "3000ms", "3s", "3e3", "3000.0", "0xBB8", "3,000", "３０００", "30000", "9999"]) {
      expect(() => bufferCommitLockWaitMs(bad)).toThrow(message);
      await expect(ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: bad })).rejects.toThrow(message);
    }
    // The refusal never echoes the value.
    await expect(ingestSettings({ ...buffered, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "9999" })).rejects.not.toThrow("9999");
    // Canonical acceptance has no buffered commit: refused, never ignored.
    await expect(ingestSettings({ ...full, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "3000" })).rejects.toThrow("INGEST_BUFFER_COMMIT_LOCK_WAIT_MS needs buffered acceptance");
  });

  test("Worker fetch with an invalid live value: every request is 503 not_configured, no database call", async () => {
    const worker = (await import("../worker.ts")).default;
    const env = { HYPERDRIVE: { connectionString: "postgres://127.0.0.1:1/none" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign",
      INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn", INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: G, INGEST_INLINE_MAX_SESSIONS: "0",
      INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "6000" };
    // Control: the same env with a valid value passes the settings and reaches authentication (401).
    const valid = await worker.fetch(new Request("https://x/v1/delivery/limits"), { ...env, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "3000" } as never, { waitUntil: () => {} });
    expect(valid.status).toBe(401);
    const res = await worker.fetch(new Request("https://x/v1/delivery/limits"), env as never, { waitUntil: () => {} });
    expect(res.status).toBe(503);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("not_configured");
  });
});

// ---- class behaviour through the real deliver path (the recovery route; buffer-lock-busy.test.ts harness) ----
type Flags = { bufferLockBusy?: true; bufferLockRetry?: true };
/** One signed buffered recovery POST whose buffered commit is the REAL sqlLedger commit over a run that fails `code`. */
async function recovery(code: string, flags: Flags, ms?: number) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "commit-wait" })], { batch_id: "commit-wait" });
  const prepared = await prepareDelivery(TENANT, raw);
  const sourceDigest = "d".repeat(64);
  const custody = { sourceDigest, witness: {
    version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
    tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
    privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
    records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true as const, deletions: [], resets: [] } } };
  const stageToken = "00000000-0000-4000-8000-000000000001";
  (ledger as unknown as Record<string, unknown>).prepareBufferStage = async () => ({ stageToken, expiresAt: new Date(Date.now() + 60_000).toISOString() });
  const sql = recorder(() => pgError(code));
  const real = sqlLedger(sql.run, { acceptanceMode: "buffered", expectedGeneration: G, ...(ms === undefined ? {} : { bufferCommitLockWaitMs: ms }) });
  (ledger as unknown as Record<string, unknown>).commitBuffered = real.commitBuffered;
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
    headers: { "content-type": RECOVERY_CONTENT_TYPE, "x-correlation-id": "corr-commit-wait", ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, enc(body)) } }));
  const text = await res.text();
  const route = logged.find((f) => f.route === "delivery.recovery")!;
  return { status: res.status, retryAfter: res.headers.get("retry-after"), text: text.replace(/"requestId":"[^"]*"/g, ""), route: { ...route, duration_ms: 0 },
    alerts: logged.filter((f) => f.route === "ingest.lock_alert"), retries, sent: sql.sent };
}
const FLAG_STATES: Flags[] = [{}, { bufferLockRetry: true }, { bufferLockBusy: true }, { bufferLockBusy: true, bufferLockRetry: true }];

describe("class behaviour through the real deliver path", () => {
  test("option set, the wait ends by lock timeout (55P03): exactly today's handling in every retry/busy state", async () => {
    for (const flags of FLAG_STATES) {
      const today = await recovery("55P03", flags);
      const option = await recovery("55P03", flags, 3500);
      const { sent: a, ...todayRest } = today, { sent: b, ...optionRest } = option;
      expect(optionRest).toEqual(todayRest);
      const attempts = flags.bufferLockRetry ? 2 : 1;
      expect(a).toHaveLength(attempts); expect(b).toHaveLength(attempts);
      for (const s of b) expect([s.text, s.params[1]]).toEqual([bufferCommitLockWaitSql("ingest_commit_buffered_batch"), "3500ms"]);
      expect(option.status).toBe(flags.bufferLockBusy ? 429 : 503);
      expect(option.route).toMatchObject({ stage: "buffer_receipt", error_code: "ledger_lock_timeout", db_error_code: "55P03" });
      expect(option.retries).toHaveLength(flags.bufferLockRetry ? 1 : 0);
      expect(option.alerts).toEqual([{ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "55P03" }]);
    }
  });

  test("option set, the statement budget is reached (57014): ledger_timeout, one attempt, never retried or converted", async () => {
    for (const flags of FLAG_STATES) {
      const today = await recovery("57014", flags);
      const option = await recovery("57014", flags, 4000);
      const { sent: a, ...todayRest } = today, { sent: b, ...optionRest } = option;
      expect(optionRest).toEqual(todayRest);
      expect(a).toHaveLength(1); expect(b).toHaveLength(1);
      expect(option.status).toBe(503);
      expect(option.retryAfter).toBeNull();
      expect(option.text).toBe('{"error":{"code":"delivery_unavailable","message":"Durable delivery is temporarily unavailable"},"correlationId":"corr-commit-wait"}');
      expect(option.route).toMatchObject({ outcome: "delivery_unavailable", stage: "buffer_receipt", error_code: "ledger_timeout", db_error_code: "57014" });
      expect(option.retries).toEqual([]);
      expect(option.alerts).toEqual([{ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "57014" }]);
    }
  });
});
