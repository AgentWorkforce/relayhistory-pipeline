/** INGEST_CLAIM_LOCK_BUSY (default off): a claim whose single autocommit statement lost its lock wait
 * (55P03 / 40P01, `ledger_lock_timeout`) is a 429 `delivery_busy` with Retry-After 5-10 s instead of the
 * 503, so a concurrent same-tenant claim backs off past the holder instead of stacking behind it. Off:
 * byte-identical 503. 57014 and every other cause stay 503 with the flag on. */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { CLAIM_LOCK_RETRY_AFTER, ClaimLockBusy, deliverBatch, IngestUnavailable } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { DELIVERY_BUSY_RETRY_AFTER, DeliveryBusyError } from "../vendor/delivery/delivery-contracts.ts";
import { logLine } from "../../shared/log.ts";
import { batch, MemoryLedger, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

/** One POST /v1/delivery/batches through the real SQL ledger whose every statement fails with `sqlstate`. */
async function post(sqlstate: string, flags: { claimLockBusy?: true; random?: () => number }) {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql: string[] = [];
  const ledger = sqlLedger(async (text: string) => { sql.push(text); throw Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code: sqlstate }); });
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...flags } as never,
    log: (fields) => logged.push(fields),
    identify: (r) => bindingIdentity(r),
  });
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "c1" }), record({ record_id: "c2" })]) });
  const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
    method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-claim" },
  }));
  return { status: res.status, retryAfter: res.headers.get("retry-after"), json: (await res.json()) as { error: { code: string } }, route: logged.find((f) => f.route === "delivery.batches")!, logged, sql };
}

describe("claim lock-wait busy (INGEST_CLAIM_LOCK_BUSY)", () => {
  test("default off: a claim 55P03 is the unchanged 503 (no Retry-After, same route line, one statement)", async () => {
    const r = await post("55P03", {});
    expect(r.status).toBe(503);
    expect(r.retryAfter).toBeNull();
    expect(r.json.error.code).toBe("delivery_unavailable");
    expect(r.route).toMatchObject({ outcome: "delivery_unavailable", stage: "claim", error_code: "ledger_lock_timeout", db_error_code: "55P03" });
    expect(r.sql.length).toBe(1);
  });

  test("on: 55P03 and 40P01 on the claim -> 429 delivery_busy, Retry-After in [5,10], no retry in the Worker, diagnosis kept", async () => {
    for (const code of ["55P03", "40P01"]) {
      const r = await post(code, { claimLockBusy: true });
      expect(r.status).toBe(429);
      expect(r.json.error.code).toBe("delivery_busy");
      const after = Number(r.retryAfter);
      expect(Number.isInteger(after) && after >= 5 && after <= 10).toBe(true);
      expect(r.sql.length).toBe(1); // one claim statement: the Worker itself never waits again or retries
      expect(r.route).toMatchObject({ outcome: "delivery_busy", stage: "claim", error_code: "ledger_lock_timeout", db_error_code: code, correlation_id: "corr-claim" });
      const line = logLine("info", "session ingest", r.route);
      expect(line).not.toContain("canceling statement");
      expect(JSON.stringify(r.json)).not.toContain(TENANT.orgId);
    }
  });

  test("on: 57014 (statement timeout) and a connection failure on the claim stay 503", async () => {
    for (const code of ["57014", "08006"]) {
      const r = await post(code, { claimLockBusy: true });
      expect(r.status).toBe(503);
      expect(r.retryAfter).toBeNull();
      expect(r.json.error.code).toBe("delivery_unavailable");
    }
  });

  test("on: Retry-After spans exactly 5..10 and stays inside the 429 contract", async () => {
    expect(CLAIM_LOCK_RETRY_AFTER.min).toBeGreaterThanOrEqual(5); // >= role statement_timeout (5 s)
    expect(CLAIM_LOCK_RETRY_AFTER.min).toBeGreaterThanOrEqual(DELIVERY_BUSY_RETRY_AFTER.min);
    expect(CLAIM_LOCK_RETRY_AFTER.max).toBeLessThanOrEqual(DELIVERY_BUSY_RETRY_AFTER.max);
    const got: (string | null)[] = [];
    // An out-of-range jitter source (NaN, 1, negative) falls back to the minimum, never outside the bounds.
    for (const r of [0, 0.17, 0.5, 0.99, 0.999999, Number.NaN, 1, -1]) got.push((await post("55P03", { claimLockBusy: true, random: () => r })).retryAfter);
    expect(got).toEqual(["5", "6", "8", "10", "10", "5", "5", "5"]);
  });

  test("the lazy_claim lock alert still fires for a 429 ClaimLockBusy in buffered mode (and only there)", async () => {
    const { lockAlertFields } = await import("../app.ts");
    const { dbErrorDiagnosis } = await import("../db-error-class.ts");
    const d55 = dbErrorDiagnosis(Object.assign(new Error(""), { code: "55P03" }));
    const busy = new ClaimLockBusy("ledger_lock_timeout", d55, 7);
    expect(busy).toBeInstanceOf(DeliveryBusyError);
    expect(busy).not.toBeInstanceOf(IngestUnavailable);
    expect(busy.status).toBe(429);
    expect(lockAlertFields(busy, "buffered")).toEqual({ route: "ingest.lock_alert", stage: "lazy_claim", sqlstate: "55P03" });
    expect(lockAlertFields(busy, undefined)).toBeUndefined();
  });

  test("on: only the claim stage converts; a later-stage lock wait stays 503 and a successful claim is unchanged", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const ledger = new MemoryLedger();
    const lockErr = await (async () => { try { await sqlLedger(async () => { throw Object.assign(new Error("x"), { code: "55P03" }); }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; } })();
    ledger.commit = async () => { throw lockErr; };
    const deps = { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, claimLockBusy: true as const };
    const err = await deliverBatch(deps as never, TENANT, await batch([record({ record_id: "c3" })])).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(IngestUnavailable);
    expect((err as IngestUnavailable).stage).toBe("commit");

    const ok = new MemoryLedger();
    const r = await deliverBatch({ ...deps, ledger: ok } as never, TENANT, await batch([record({ record_id: "c4" })]));
    expect(r.outcome).toBeDefined();
  });

  test("worker flag: INGEST_CLAIM_LOCK_BUSY=on (any case) enables it and reaches the deliver deps; unset/off/other values leave it off", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const off = (await ingestSettings(full))!;
    expect(off.claimLockBusy).toBeUndefined();
    expect("claimLockBusy" in deliverDepsFor(off, async () => ({ rows: [] }), target as never)).toBe(false);
    for (const v of ["on", "ON", " On "]) {
      const on = (await ingestSettings({ ...full, INGEST_CLAIM_LOCK_BUSY: v }))!;
      expect(on.claimLockBusy).toBe(true);
      expect(deliverDepsFor(on, async () => ({ rows: [] }), target as never).claimLockBusy).toBe(true);
    }
    for (const v of ["", "off", "1", "true", "yes"]) expect((await ingestSettings({ ...full, INGEST_CLAIM_LOCK_BUSY: v }))?.claimLockBusy).toBeUndefined();
  });
});
