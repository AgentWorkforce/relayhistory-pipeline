/** INGEST_CLAIM_LOCK_WAIT_MS (default absent): each claim attempt runs in its own short transaction whose
 * lock_timeout is SET LOCAL to the configured ms, so a claim queued behind the same user's in-flight claim
 * gives up quickly (55P03 -> ledger_lock_timeout -> with claimLockBusy the existing 429) instead of waiting
 * the role's 2 s. Absent: the claim is the unchanged single autocommit statement. The real-lock proof is
 * claim-lock-wait.postgres.test.ts. */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { CLAIM_LOCK_WAIT_MS_MAX, CLAIM_LOCK_WAIT_MS_MIN, inClaimLockWait, sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const pgError = (code: string) => Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code });

/** One POST /v1/delivery/batches through the real SQL ledger: BEGIN/set_config/ROLLBACK succeed, the claim fails with `sqlstate`. */
async function post(sqlstate: string, ledgerOptions: { claimLockWaitMs?: number }, flags: { claimLockBusy?: true }) {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql: { text: string; params: unknown[] }[] = [];
  const ledger = sqlLedger(async (text: string, params: unknown[]) => {
    sql.push({ text, params });
    if (text === "BEGIN" || text === "ROLLBACK" || text.startsWith("SELECT set_config('lock_timeout'")) return { rows: [] };
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
      "SELECT set_config('lock_timeout', $1, ",
      "SELECT sessions.ingest_claim_batch($1:",
      "ROLLBACK",
    ]);
    expect(r.sql[1]!.params).toEqual(["250ms"]);
    expect(r.status).toBe(429);
    const after = Number(r.retryAfter);
    expect(after >= 5 && after <= 10).toBe(true);
  });

  test("set, claimLockBusy off: a held lock is the unchanged 503 (the wait only gets shorter)", async () => {
    const r = await post("55P03", { claimLockWaitMs: 250 }, {});
    expect(r.status).toBe(503);
    expect(r.sql.at(-1)!.text).toBe("ROLLBACK");
  });

  test("a free lock: BEGIN, set_config, the claim, COMMIT; the claim's value is returned", async () => {
    const seen: string[] = [];
    const run = async (text: string) => { seen.push(text); return { rows: [] }; };
    const value = await inClaimLockWait(run, 100, async () => { seen.push("CLAIM"); return 42; });
    expect(value).toBe(42);
    expect(seen).toEqual(["BEGIN", "SELECT set_config('lock_timeout', $1, true)", "CLAIM", "COMMIT"]);
  });

  test("a typed claim error passes through unchanged and the transaction is rolled back", async () => {
    for (const thrown of [new LedgerUnavailableError("ledger_lock_timeout"), new DeliveryError("forbidden", 403, "x")]) {
      const seen: string[] = [];
      const run = async (text: string) => { seen.push(text); return { rows: [] }; };
      const got = await inClaimLockWait(run, 100, async () => { throw thrown; }).catch((e: unknown) => e);
      expect(got).toBe(thrown);
      expect(seen).toEqual(["BEGIN", "SELECT set_config('lock_timeout', $1, true)", "ROLLBACK"]);
    }
  });

  test("a failing BEGIN, set_config or COMMIT is a coded LedgerUnavailableError, rolled back, never a raw driver error", async () => {
    for (const failing of ["BEGIN", "SELECT set_config('lock_timeout', $1, true)", "COMMIT"]) {
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
    for (const bad of ["24", "2001", "abc", "12.5"])
      await expect(ingestSettings({ ...full, INGEST_CLAIM_LOCK_WAIT_MS: bad })).rejects.toThrow("INGEST_CLAIM_LOCK_WAIT_MS must be an integer from 25 to 2000");
  });

  test("the option is bounded: outside 25..2000 ms or not an integer, the ledger refuses to build", () => {
    const run = async () => ({ rows: [] });
    for (const ms of [CLAIM_LOCK_WAIT_MS_MIN - 1, CLAIM_LOCK_WAIT_MS_MAX + 1, 0, -1, 12.5, Number.NaN])
      expect(() => sqlLedger(run, { claimLockWaitMs: ms })).toThrow(LedgerUnavailableError);
    for (const ms of [CLAIM_LOCK_WAIT_MS_MIN, 250, CLAIM_LOCK_WAIT_MS_MAX]) expect(() => sqlLedger(run, { claimLockWaitMs: ms })).not.toThrow();
  });
});
