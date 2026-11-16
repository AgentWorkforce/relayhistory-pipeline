/** INGEST_CLAIM_LOCK_WAIT_MS (default absent): each claim attempt runs in its own short transaction whose
 * lock_timeout is SET LOCAL to the configured ms, so a claim queued behind the same user's in-flight claim
 * gives up quickly (55P03 -> ledger_lock_timeout -> with claimLockBusy the existing 429) instead of waiting
 * the role's 2 s. Absent: the claim is the unchanged single autocommit statement. The real-lock proof is
 * claim-lock-wait.postgres.test.ts. */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { CLAIM_IDLE_IN_TRANSACTION_MS, CLAIM_LOCK_WAIT_MS_MAX, CLAIM_LOCK_WAIT_MS_MIN, CLAIM_LOCK_WAIT_SET, inClaimLockWait, sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const pgError = (code: string) => Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code });

/** One POST /v1/delivery/batches through the real SQL ledger: BEGIN/set_config/ROLLBACK succeed, the claim fails with `sqlstate`. */
async function post(sqlstate: string, ledgerOptions: { claimLockWaitMs?: number }, flags: { claimLockBusy?: true; claimTimeoutBusy?: true }) {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql: { text: string; params: unknown[] }[] = [];
  const ledger = sqlLedger(async (text: string, params: unknown[]) => {
    sql.push({ text, params });
    if (text === "BEGIN" || text === "ROLLBACK" || text === CLAIM_LOCK_WAIT_SET) return { rows: [] };
    throw pgError(sqlstate);
  }, ledgerOptions);
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...flags } as never,
    log: () => {},
    identify: (r) => bindingIdentity(r),
  });
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "c1" })]) });
  const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
    method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }) },
  }));
  return { status: res.status, retryAfter: res.headers.get("retry-after"), sql };
}

describe("claim lock wait (INGEST_CLAIM_LOCK_WAIT_MS)", () => {
  test("absent: the claim is the unchanged single autocommit statement (no BEGIN, no set_config)", async () => {
    const r = await post("55P03", {}, { claimLockBusy: true });
    expect(r.sql.map((q) => q.text)).toHaveLength(1);
    expect(r.sql[0]!.text).toStartWith("SELECT sessions.ingest_claim_batch(");
    expect(r.status).toBe(429);
  });

  test("set: BEGIN, SET LOCAL lock_timeout, the same claim statement, then ROLLBACK when the lock is held -> the existing 429", async () => {
    const r = await post("55P03", { claimLockWaitMs: 250 }, { claimLockBusy: true });
    expect(r.sql.map((q) => q.text.slice(0, 38))).toEqual([
      "BEGIN",
      CLAIM_LOCK_WAIT_SET.slice(0, 38),
      "SELECT sessions.ingest_claim_batch($1:",
      "ROLLBACK",
    ]);
    // Lock wait and the stranded-transaction bound in ONE statement, both parameterised.
    expect(r.sql[1]!.text).toBe("SELECT set_config('lock_timeout', $1, true), set_config('idle_in_transaction_session_timeout', $2, true)");
    expect(r.sql[1]!.params).toEqual(["250ms", `${CLAIM_IDLE_IN_TRANSACTION_MS}ms`]);
    expect(CLAIM_IDLE_IN_TRANSACTION_MS).toBe(1000);
    expect(r.status).toBe(429);
    const after = Number(r.retryAfter);
    expect(after >= 5 && after <= 10).toBe(true);
  });

  test("set, claimLockBusy off: a held lock is the unchanged 503 (the wait only gets shorter)", async () => {
    const r = await post("55P03", { claimLockWaitMs: 250 }, {});
    expect(r.status).toBe(503);
    expect(r.sql.at(-1)!.text).toBe("ROLLBACK");
  });

  test("(b) set: a 57014 claim statement timeout under the wrapper is the 429 with claimTimeoutBusy, rolled back", async () => {
    const r = await post("57014", { claimLockWaitMs: 250 }, { claimTimeoutBusy: true });
    expect(r.status).toBe(429);
    expect(r.sql.map((q) => q.text)).toEqual(["BEGIN", CLAIM_LOCK_WAIT_SET, expect.stringContaining("ingest_claim_batch"), "ROLLBACK"]);
  });

  test("(a) buffered claim under the wrapper with the 55000 generation retry: BEGIN..ROLLBACK, then BEGIN..COMMIT on the other generation", async () => {
    const G = "a".repeat(64), H = "b".repeat(64);
    const sql: { text: string; params: unknown[] }[] = [];
    const ledger = sqlLedger(async (text: string, params: unknown[]) => {
      sql.push({ text, params });
      if (text === "BEGIN" || text === "ROLLBACK" || text === "COMMIT" || text === CLAIM_LOCK_WAIT_SET) return { rows: [] };
      if (text.includes("ingest_claim_buffered_batch")) {
        if (JSON.parse(String(params[0])).generation === G) throw pgError("55000");
        return { rows: [{ result: { kind: "replay", receiptId: `rhr_${"c".repeat(64)}` } }] };
      }
      throw pgError("XX000");
    }, { acceptanceMode: "buffered", expectedGeneration: G, acceptGenerations: [G, H], claimLockWaitMs: 250 });
    const r = await ledger.claim({ tenant: TENANT } as never);
    expect(r).toEqual({ kind: "replay", receiptId: `rhr_${"c".repeat(64)}` });
    expect(sql.map((q) => q.text.includes("ingest_claim_buffered_batch") ? "CLAIM" : q.text === CLAIM_LOCK_WAIT_SET ? "SET" : q.text))
      .toEqual(["BEGIN", "SET", "CLAIM", "ROLLBACK", "BEGIN", "SET", "CLAIM", "COMMIT"]);
    expect(sql.filter((q) => q.text.includes("ingest_claim_buffered_batch")).map((q) => JSON.parse(String(q.params[0])).generation)).toEqual([G, H]);
  });

  test("(d) a COMMIT failure after a successful claim is the 503, never the 429 (the claim's own lock wait did not fail)", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const sql: string[] = [];
    const ledger = sqlLedger(async (text: string) => {
      sql.push(text);
      if (text === "BEGIN" || text === "ROLLBACK" || text === CLAIM_LOCK_WAIT_SET) return { rows: [] };
      if (text === "COMMIT") throw pgError("40001");
      if (text.includes("ingest_claim_batch")) return { rows: [{ result: { kind: "replay", receiptId: `rhr_${"c".repeat(64)}` } }] };
      throw pgError("XX000");
    }, { claimLockWaitMs: 250 });
    const app = createIngestApp({
      deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, claimLockBusy: true, claimTimeoutBusy: true } as never,
      log: () => {},
      identify: (r) => bindingIdentity(r),
    });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "d1" })]) });
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }) },
    }));
    expect(res.status).toBe(503);
    expect(sql.slice(0, 5)).toEqual(["BEGIN", CLAIM_LOCK_WAIT_SET, expect.stringContaining("ingest_claim_batch"), "COMMIT", "ROLLBACK"]);
  });

  test("(c) only the claim is wrapped: prepare_buffer_stage, commit and acquireSlot send no BEGIN", async () => {
    const G = "a".repeat(64);
    const sql: string[] = [];
    const ledger = sqlLedger(async (text: string) => { sql.push(text); throw pgError("55P03"); },
      { acceptanceMode: "buffered", expectedGeneration: G, claimLockWaitMs: 250 });
    await ledger.prepareBufferStage({ tenant: TENANT, claimToken: "0".repeat(8) + "-0000-0000-0000-" + "0".repeat(12) } as never).catch(() => {});
    await ledger.commitBuffered({ tenant: TENANT, claimToken: "0".repeat(8) + "-0000-0000-0000-" + "0".repeat(12) } as never).catch(() => {});
    await ledger.commit({ tenant: TENANT } as never).catch(() => {});
    await ledger.acquireSlot({ tenant: TENANT, holder: "h", maxPerUser: 4, maxPerOrg: 32, ttlSeconds: 30 } as never).catch(() => {});
    expect(sql).toHaveLength(4);
    expect(sql.some((t) => t === "BEGIN" || t === CLAIM_LOCK_WAIT_SET)).toBe(false);
  });

  test("a free lock: BEGIN, set_config, the claim, COMMIT; the claim's value is returned", async () => {
    const seen: string[] = [];
    const run = async (text: string) => { seen.push(text); return { rows: [] }; };
    const value = await inClaimLockWait(run, 100, async () => { seen.push("CLAIM"); return 42; });
    expect(value).toBe(42);
    expect(seen).toEqual(["BEGIN", CLAIM_LOCK_WAIT_SET, "CLAIM", "COMMIT"]);
  });

  test("a typed claim error passes through unchanged and the transaction is rolled back", async () => {
    for (const thrown of [new LedgerUnavailableError("ledger_lock_timeout"), new DeliveryError("forbidden", 403, "x")]) {
      const seen: string[] = [];
      const run = async (text: string) => { seen.push(text); return { rows: [] }; };
      const got = await inClaimLockWait(run, 100, async () => { throw thrown; }).catch((e: unknown) => e);
      expect(got).toBe(thrown);
      expect(seen).toEqual(["BEGIN", CLAIM_LOCK_WAIT_SET, "ROLLBACK"]);
    }
  });

  test("a failing BEGIN, set_config or COMMIT is a coded LedgerUnavailableError, rolled back, never a raw driver error", async () => {
    for (const failing of ["BEGIN", CLAIM_LOCK_WAIT_SET, "COMMIT"]) {
      const seen: string[] = [];
      const run = async (text: string) => { seen.push(text); if (text === failing) throw pgError("40001"); return { rows: [] }; };
      const got = await inClaimLockWait(run, 100, async () => 1).catch((e: unknown) => e);
      expect(got).toBeInstanceOf(LedgerUnavailableError);
      expect(String((got as Error).message)).not.toContain(TENANT.orgId);
      expect(seen.at(-1)).toBe("ROLLBACK");
    }
  });

  test("a rejected ROLLBACK never masks the claim's own error", async () => {
    const thrown = new LedgerUnavailableError("ledger_lock_timeout");
    const run = async (text: string) => { if (text === "ROLLBACK") throw pgError("08006"); return { rows: [] }; };
    expect(await inClaimLockWait(run, 100, async () => { throw thrown; }).catch((e: unknown) => e)).toBe(thrown);
  });

  test("worker: INGEST_CLAIM_LOCK_WAIT_MS reaches the SQL ledger; unset is the unchanged single statement; out of range refuses the settings", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    expect((await ingestSettings(full))!.claimLockWaitMs).toBeUndefined();
    const on = (await ingestSettings({ ...full, INGEST_CLAIM_LOCK_WAIT_MS: "250" }))!;
    expect(on.claimLockWaitMs).toBe(250);
    const seen: string[] = [];
    const deps = deliverDepsFor(on, async (text) => { seen.push(text); throw pgError("55P03"); }, target as never);
    await deps.ledger.claim({ tenant: TENANT } as never).catch(() => {});
    expect(seen[0]).toBe("BEGIN");
    for (const bad of ["24", "2001", "abc", "12.5", "1e2", "0x100", "+250", "-250"])
      await expect(ingestSettings({ ...full, INGEST_CLAIM_LOCK_WAIT_MS: bad })).rejects.toThrow("INGEST_CLAIM_LOCK_WAIT_MS must be an integer from 25 to 2000");
  });

  test("the option is bounded: outside 25..2000 ms or not an integer, the ledger refuses to build", () => {
    const run = async () => ({ rows: [] });
    for (const ms of [CLAIM_LOCK_WAIT_MS_MIN - 1, CLAIM_LOCK_WAIT_MS_MAX + 1, 0, -1, 12.5, Number.NaN])
      expect(() => sqlLedger(run, { claimLockWaitMs: ms })).toThrow(LedgerUnavailableError);
    for (const ms of [CLAIM_LOCK_WAIT_MS_MIN, 250, CLAIM_LOCK_WAIT_MS_MAX]) expect(() => sqlLedger(run, { claimLockWaitMs: ms })).not.toThrow();
  });
});
