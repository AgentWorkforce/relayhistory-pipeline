/** The 55P03 / 57014 lock-wait alert line (runbooks/history-620-operations.md, "Lock-wait alert"):
 * one `{ route: "ingest.lock_alert", stage, sqlstate }` line per failed request on the lazy claim or
 * the staged ACK (and, since 053000, the staged-intent call that queues behind it), through the injected logger; nothing else changes (status, envelope, route line). */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, IDENTITY_HEADER, lockAlertFields, SYNC_SCOPE } from "../app.ts";
import { IngestUnavailable } from "../deliver.ts";
import { dbErrorDiagnosis } from "../db-error-class.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { logLine } from "../../shared/log.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };

/** The error the real SQL ledger raises for a driver error carrying `code` (its own mapping and diagnosis). */
async function ledgerError(code: string): Promise<unknown> {
  const pgErr = Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/** One signed buffered-mode archive-recovery POST; `arm` makes one ledger method fail. */
async function recovery(arm: (ledger: MemoryLedger) => void) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "lock-alert" })], { batch_id: "lock-alert" });
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
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered" } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: { "content-type": RECOVERY_CONTENT_TYPE, ...signedHeaders(identity, "POST", path, enc(body)) } }));
  return { status: res.status, json: (await res.json()) as { error?: { code: string } }, logged };
}

describe("lock-wait alert line (55P03 / 57014)", () => {
  test("staged_ack: commit_buffered_batch 57014 -> one { route, stage: staged_ack, sqlstate } line; 503 unchanged", async () => {
    const thrown = await ledgerError("57014");
    const r = await recovery((l) => { l.commitBuffered = async () => { throw thrown; }; });
    expect(r.status).toBe(503);
    expect(r.json.error?.code).toBe("delivery_unavailable");
    const alerts = r.logged.filter((f) => f.route === "ingest.lock_alert");
    expect(alerts).toEqual([{ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "57014" }]);
    // The route's own 503 line is unchanged (deliver stage name, closed DB diagnosis).
    expect(r.logged.find((f) => f.route === "delivery.recovery")).toMatchObject({ outcome: "delivery_unavailable", stage: "buffer_receipt", error_code: "ledger_timeout", db_error_code: "57014" });
    // Rendered through the real allowlist: the fields survive; no tenant data, no ids.
    const line = JSON.parse(logLine("info", "session ingest", alerts[0]!));
    expect(line).toMatchObject({ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "57014" });
    for (const k of ["org_id", "workspace_id", "correlation_id", "session_id", "machine_id"]) expect(line[k]).toBeUndefined();
    expect(JSON.stringify(line)).not.toContain(TENANT.orgId);
  });

  test("lazy_claim: buffered claim 55P03 -> one { route, stage: lazy_claim, sqlstate } line; 503 unchanged", async () => {
    const thrown = await ledgerError("55P03");
    const r = await recovery((l) => { l.claim = async () => { throw thrown; }; });
    expect(r.status).toBe(503);
    expect(r.json.error?.code).toBe("delivery_unavailable");
    expect(r.logged.filter((f) => f.route === "ingest.lock_alert")).toEqual([{ route: "ingest.lock_alert", stage: "lazy_claim", sqlstate: "55P03" }]);
    expect(r.logged.find((f) => f.route === "delivery.recovery")).toMatchObject({ stage: "claim", error_code: "ledger_lock_timeout", db_error_code: "55P03" });
  });

  test("staged_intent: prepare_buffer_stage 55P03 -> one { route, stage: staged_intent, sqlstate } line; 503 unchanged", async () => {
    const thrown = await ledgerError("55P03");
    const r = await recovery((l) => { l.prepareBufferStage = async () => { throw thrown; }; });
    expect(r.status).toBe(503);
    expect(r.json.error?.code).toBe("delivery_unavailable");
    expect(r.logged.filter((f) => f.route === "ingest.lock_alert")).toEqual([{ route: "ingest.lock_alert", stage: "staged_intent", sqlstate: "55P03" }]);
    expect(r.logged.find((f) => f.route === "delivery.recovery")).toMatchObject({ stage: "buffer_intent", error_code: "ledger_lock_timeout", db_error_code: "55P03" });
  });

  test("no alert line for other SQLSTATEs, other stages, or a non-buffered claim", async () => {
    const deadlock = await ledgerError("40P01");
    const r = await recovery((l) => { l.claim = async () => { throw deadlock; }; });
    expect(r.status).toBe(503);
    expect(r.logged.filter((f) => f.route === "ingest.lock_alert")).toEqual([]);
    const d55 = dbErrorDiagnosis(Object.assign(new Error(""), { code: "55P03" }));
    const d57 = dbErrorDiagnosis(Object.assign(new Error(""), { code: "57014" }));
    expect(lockAlertFields(new IngestUnavailable("claim", "ledger_lock_timeout", d55), "buffered")).toEqual({ route: "ingest.lock_alert", stage: "lazy_claim", sqlstate: "55P03" });
    expect(lockAlertFields(new IngestUnavailable("buffer_receipt", "ledger_timeout", d57), undefined)).toEqual({ route: "ingest.lock_alert", stage: "staged_ack", sqlstate: "57014" });
    expect(lockAlertFields(new IngestUnavailable("claim", "ledger_lock_timeout", d55), undefined)).toBeUndefined(); // legacy ingest_claim_batch
    expect(lockAlertFields(new IngestUnavailable("commit", "ledger_timeout", d57), "buffered")).toBeUndefined();
    expect(lockAlertFields(new IngestUnavailable("buffer_intent", "ledger_timeout", d57), "buffered")).toEqual({ route: "ingest.lock_alert", stage: "staged_intent", sqlstate: "57014" });
    expect(lockAlertFields(new IngestUnavailable("buffer_intent", "ledger_lock_timeout"), "buffered")).toBeUndefined(); // no SQLSTATE: no alert
    expect(lockAlertFields(new IngestUnavailable("buffer_put", "ledger_timeout", d57), "buffered")).toBeUndefined();
    expect(lockAlertFields(new IngestUnavailable("claim", "ledger_lock_timeout"), "buffered")).toBeUndefined();
    expect(lockAlertFields(Object.assign(new Error(""), { code: "55P03" }), "buffered")).toBeUndefined();
  });
});
