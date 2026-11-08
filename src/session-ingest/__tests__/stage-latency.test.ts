/** INGEST_STAGE_LATENCY_LOG (default off): one `{ route: "ingest.stage_latency", stage, outcome, duration_ms,
 * sqlstate? }` line per deliver stage through `DeliverDeps.stageLatency`. Diagnostic only: the delivery result,
 * errors and status are identical with the observer on, off or throwing. */
import { afterEach, describe, expect, test } from "bun:test";
import { deliverBatch, IngestUnavailable, type StageLatencyFields } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { logLine } from "../../shared/log.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

function setup(observe?: (f: StageLatencyFields) => void) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const deps = { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...(observe ? { stageLatency: observe } : {}) };
  return { ledger, deps };
}
async function ledgerError(code: string): Promise<unknown> {
  const pgErr = Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}
const strip = (r: unknown) => JSON.parse(JSON.stringify(r, (k, v) => (k === "timing" || k.endsWith("_ms") ? undefined : v)));

describe("per-stage latency lines (INGEST_STAGE_LATENCY_LOG)", () => {
  test("success: one ok line per stage, in order, closed fields only; the result equals the observer-off result", async () => {
    const lines: StageLatencyFields[] = [];
    const on = setup((f) => lines.push(f));
    const raw = await batch([record({ record_id: "lat-1" })], { batch_id: "lat-1" });
    const withLines = await deliverBatch(on.deps as never, TENANT, raw);
    const off = setup();
    const without = await deliverBatch(off.deps as never, TENANT, await batch([record({ record_id: "lat-1" })], { batch_id: "lat-1" }));
    expect(strip(withLines)).toEqual(strip(without));
    expect(lines.length).toBeGreaterThan(1);
    expect(lines.map((l) => l.stage)).toContain("claim");
    expect(lines.map((l) => l.stage)).toContain("commit");
    for (const l of lines) {
      expect(Object.keys(l).sort()).toEqual(["duration_ms", "outcome", "route", "stage"]);
      expect(l).toMatchObject({ route: "ingest.stage_latency", outcome: "ok" });
      expect(Number.isInteger(l.duration_ms) && l.duration_ms >= 0).toBe(true);
    }
    // Rendered through the real allowlist: the fields survive; no tenant data, no ids.
    const line = JSON.parse(logLine("info", "session ingest", { ...lines[0]! }));
    expect(line).toMatchObject({ route: "ingest.stage_latency", stage: lines[0]!.stage, outcome: "ok" });
    expect(typeof line.duration_ms).toBe("number");
    expect(JSON.stringify(lines)).not.toContain(TENANT.orgId);
    expect(JSON.stringify(lines)).not.toContain(TENANT.userId);
  });

  test("a 55P03 ledger failure: an unavailable line with the SQLSTATE; the same IngestUnavailable is thrown", async () => {
    const lines: StageLatencyFields[] = [];
    const { ledger, deps } = setup((f) => lines.push(f));
    const thrown = await ledgerError("55P03");
    ledger.claim = async () => { throw thrown; };
    const e = await deliverBatch(deps as never, TENANT, await batch([record({ record_id: "lat-2" })])).catch((x) => x);
    expect(e).toBeInstanceOf(IngestUnavailable);
    expect(e).toMatchObject({ stage: "claim", cause_code: "ledger_lock_timeout" });
    expect(lines.at(-1)).toMatchObject({ route: "ingest.stage_latency", stage: "claim", outcome: "unavailable", sqlstate: "55P03" });
    expect(JSON.stringify(lines)).not.toContain(TENANT.orgId);
  });

  test("a non-database failure: unavailable with no sqlstate", async () => {
    const lines: StageLatencyFields[] = [];
    const { ledger, deps } = setup((f) => lines.push(f));
    ledger.claim = async () => { throw new Error(`boom ${TENANT.orgId}`); };
    await expect(deliverBatch(deps as never, TENANT, await batch([record({ record_id: "lat-3" })]))).rejects.toBeInstanceOf(IngestUnavailable);
    expect(lines.at(-1)).toMatchObject({ stage: "claim", outcome: "unavailable" });
    expect(lines.at(-1)!.sqlstate).toBeUndefined();
    expect(JSON.stringify(lines)).not.toContain(TENANT.orgId);
  });

  test("a typed DeliveryError: refused, no sqlstate, the same error rethrown", async () => {
    const lines: StageLatencyFields[] = [];
    const { ledger, deps } = setup((f) => lines.push(f));
    const refusal = new DeliveryError("delivery_conflict", 409, "conflict");
    ledger.claim = async () => { throw refusal; };
    const e = await deliverBatch(deps as never, TENANT, await batch([record({ record_id: "lat-4" })])).catch((x) => x);
    expect(e).toBe(refusal);
    expect(lines.at(-1)).toEqual({ route: "ingest.stage_latency", stage: "claim", outcome: "refused", duration_ms: lines.at(-1)!.duration_ms });
  });

  test("an IngestUnavailable raised inside a stage is rethrown unchanged and labelled unavailable, not refused", async () => {
    const lines: StageLatencyFields[] = [];
    const { ledger, deps } = setup((f) => lines.push(f));
    const inner = new IngestUnavailable("claim", "ledger_timeout");
    ledger.claim = async () => { throw inner; };
    const e = await deliverBatch(deps as never, TENANT, await batch([record({ record_id: "lat-6" })])).catch((x) => x);
    expect(e).toBe(inner);
    expect(lines.at(-1)).toMatchObject({ stage: "claim", outcome: "unavailable" });
  });

  test("a throwing observer never changes the delivery", async () => {
    let calls = 0;
    const { deps } = setup(() => { calls++; throw new Error("observer down"); });
    const r = await deliverBatch(deps as never, TENANT, await batch([record({ record_id: "lat-5" })]));
    expect(r.outcome).toBeDefined();
    expect(calls).toBeGreaterThan(1);
  });

  test("worker flag: INGEST_STAGE_LATENCY_LOG=on (any case) enables it; unset/off/other values leave it off", async () => {
    const { ingestSettings } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    expect((await ingestSettings(full))?.stageLatencyLog).toBeUndefined();
    for (const v of ["on", "ON", "On"]) expect((await ingestSettings({ ...full, INGEST_STAGE_LATENCY_LOG: v }))?.stageLatencyLog).toBe(true);
    for (const v of ["", "off", "1", "true", "yes"]) expect((await ingestSettings({ ...full, INGEST_STAGE_LATENCY_LOG: v }))?.stageLatencyLog).toBeUndefined();
  });
});
