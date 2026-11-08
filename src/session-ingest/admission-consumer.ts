/**
 * The INGEST_ADMISSION consumer (docs/design/s3-first-async-ingest.md): one message, one
 * acknowledged inbox object, admitted through the UNCHANGED buffered path (deliverPrepared:
 * claim -> stage intent -> PUT -> receipt ACK) with the upload-time privacy gate between the
 * claim and any write. Every Supabase lock wait now happens here, retried by the queue,
 * never in front of a Desktop.
 *
 * Outcomes: `ack` after a terminal outcome is durably recorded (admitted / refused) and the
 * inbox content tombstoned; `retry` for anything transient (ledger lock/timeout, baseline
 * pending, generation mismatch, slot busy, S3) and for a malformed message or inbox (which
 * then walks to the DLQ for an operator, never silently dropped).
 */
import type { DeliverDeps } from "./deliver.ts";
import { deliverPrepared, IngestUnavailable } from "./deliver.ts";
import { LedgerUnavailableError, type FrozenWitnessInput, type LedgerTenant } from "./ledger.ts";
import { DeliveryBusyError, DeliveryError } from "./vendor/delivery/delivery-contracts.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";
import {
  INBOX_NAMESPACE,
  PrivacyControlPending,
  assertNoLaterPrivacyControl,
  decodeInbox,
  readInbox,
  receiptDir,
  tombstoneInbox,
  writeOutcome,
  type AsyncAdmissionMode,
} from "./async-admission.ts";

export interface AdmissionConsumerDeps {
  mode: AsyncAdmissionMode;
  /** The same deliver deps the request app uses (buffered ledger, slots, target, generation). */
  deliver: DeliverDeps;
  custodySource: "relayhistory" | "shadow" | "frozen-supabase";
  decompress: (bytes: Uint8Array) => Uint8Array;
  nowMs?: () => number;
}

export type AdmissionAction =
  | { action: "ack"; outcome: "admitted" | "refused" | "already_terminal"; code: string; lagMs?: number }
  | { action: "retry"; delaySeconds: number; code: string; stage?: string; error?: unknown };

/** Exponential, 15 s .. 5 min, by the queue's delivery attempt (1-based). */
export function backoffSeconds(attempts: number): number {
  const n = Number.isSafeInteger(attempts) && attempts > 0 ? attempts : 1;
  return Math.min(300, 15 * 2 ** Math.min(n - 1, 5));
}

function witnessInput(tenant: LedgerTenant, p: PreparedDelivery): FrozenWitnessInput {
  return { kind: "batch", tenant, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: p.receipt.acceptedRevisionIds.length,
    candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id ?? null })) };
}

function classify(error: unknown, attempts: number): AdmissionAction | { terminal: string } {
  // A reset / re-enable is in flight for this user: wait for it (never admit across it).
  if (error instanceof IngestUnavailable && error.cause_code === new PrivacyControlPending().code) return { action: "retry", delaySeconds: 15, code: "privacy_control_pending", stage: error.stage, error };
  if (error instanceof DeliveryBusyError) return { action: "retry", delaySeconds: error.retryAfterSeconds, code: error.code, error };
  if (error instanceof IngestUnavailable) return { action: "retry", delaySeconds: backoffSeconds(attempts), code: error.cause_code, stage: error.stage, error };
  // 403 (another user's record), 409 (batch id / revision conflict, privacy superseded), 413/422 (budget), 400: the same answer forever.
  if (error instanceof DeliveryError && error.status !== 503 && error.status !== 429) return { terminal: error.code };
  if (error instanceof LedgerUnavailableError) return { action: "retry", delaySeconds: backoffSeconds(attempts), code: error.code, error };
  return { action: "retry", delaySeconds: backoffSeconds(attempts), code: (error as { code?: unknown })?.code && typeof (error as { code?: unknown }).code === "string" && /^[A-Za-z0-9_]{1,64}$/.test((error as { code: string }).code) ? (error as { code: string }).code : "admission_failed", error };
}

export async function admitMessage(deps: AdmissionConsumerDeps, body: unknown, attempts: number): Promise<AdmissionAction> {
  // off: the journal is not being raised, so nothing may be admitted; wait for drain/on (or the DLQ).
  if (deps.mode === "off") return { action: "retry", delaySeconds: 300, code: "async_admission_off" };
  const { store, keyRoot } = deps.deliver.target;
  const m = body as { v?: unknown; kind?: unknown; key?: unknown };
  if (!m || m.v !== 1 || m.kind !== "ingest-admission" || typeof m.key !== "string" || !m.key.startsWith(`${keyRoot}/${INBOX_NAMESPACE}/`))
    return { action: "retry", delaySeconds: 300, code: "admission_message_invalid" };
  const key = m.key;
  let raw: Uint8Array | null;
  try {
    raw = await readInbox(store, key, deps.decompress);
  } catch (error) {
    return { action: "retry", delaySeconds: backoffSeconds(attempts), code: "inbox_unavailable", error };
  }
  // The send follows a successful PUT, so a missing object is an operator problem: DLQ, never a silent ack.
  if (!raw) return { action: "retry", delaySeconds: 300, code: "inbox_missing" };
  let decoded;
  try {
    decoded = await decodeInbox(keyRoot, key, raw);
  } catch {
    return { action: "retry", delaySeconds: 300, code: "admission_invalid" };
  }
  if (!decoded) return { action: "ack", outcome: "already_terminal", code: "tombstoned" };
  const { tenant, prepared, receivedAtMs, journalSeq } = decoded;
  const now = (deps.nowMs ?? Date.now)();
  const lagMs = Math.max(0, now - receivedAtMs);
  const dir = receiptDir(keyRoot, tenant, prepared.originId, prepared.receipt.receiptId);
  const finish = async (state: "admitted" | "refused", code: string): Promise<AdmissionAction> => {
    try {
      await writeOutcome(store, dir, { v: 1, state, code, atMs: now });
      await tombstoneInbox(store, key);
    } catch (error) {
      // Re-running is idempotent (claim replay / the same refusal); the outcome must not be lost.
      return { action: "retry", delaySeconds: backoffSeconds(attempts), code: "outcome_unavailable", error };
    }
    return { action: "ack", outcome: state, code, lagMs };
  };
  try {
    // No age limit: an acknowledged upload stays admissible for as long as its message (or an
    // operator's redrive) exists; only an outcome retires its bytes.
    const ledger = deps.deliver.ledger;
    let custody;
    if (deps.custodySource === "frozen-supabase") {
      // relayhistory's two frozen-mode reads, now off its request path: the legacy (Neon) receipt
      // of a retried pre-cutover batch, then the current-source witness (refuses while a control is pending).
      if (ledger.frozenReceipt) {
        const stored = await ledger.frozenReceipt({ tenant, originId: prepared.originId, batchId: prepared.batchId });
        if (stored) return finish(stored.digest === prepared.batchDigest ? "admitted" : "refused", stored.digest === prepared.batchDigest ? "legacy_replay" : "delivery_conflict");
      }
      if (!ledger.frozenWitness) throw new IngestUnavailable("fence", "custody_source_unavailable");
      custody = await ledger.frozenWitness(witnessInput(tenant, prepared));
      if (custody.witness.originalRecordCount !== prepared.receipt.acceptedRevisionIds.length) throw new IngestUnavailable("fence", "custody_record_count");
    }
    const result = await deliverPrepared({
      ...deps.deliver,
      ...(custody ? { custody } : {}),
      admissionGate: () => assertNoLaterPrivacyControl(store, keyRoot, tenant, journalSeq, (deps.nowMs ?? Date.now)()),
    }, tenant, prepared);
    return finish("admitted", result.outcome);
  } catch (error) {
    const c = classify(error, attempts);
    if ("terminal" in c) return finish("refused", c.terminal);
    return c;
  }
}
