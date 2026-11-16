/**
 * Log-mode admission (docs/design/s3-ledger-log-commit.md, slice 2): admits one prepared batch into its stream's
 * numbered log instead of the legacy claim -> canonical writes -> commit chain. Only a stream whose entry 0 (the
 * durable mode marker) exists is admitted here; every other stream keeps the legacy path, unchanged.
 *
 * Per batch, in order (rev 3 of s3-ledger-group-commit.md, per member):
 *   1. the user's fence: a pending ordered reset or an unfinished reset is a retry; deleted sessions (tombstones
 *      plus pending ordered disables) have their records dropped, as the legacy judge drops them;
 *   2. the privacy journal: a reset / re-enable completed after the upload began refuses the batch (409);
 *      one in flight is a retry;
 *   3. consent at admit, with the dropped sessions;
 *   4. the payload PUT (content addressed, idempotent);
 *   5. consent again at commit, just before the entry;
 *   6. the entry append: the one request that commits. Replay (same batchId and digest) and conflict (same batchId,
 *      another digest) are answered from the stream's remembered-batches window.
 *
 * Single writer per stream: this process takes the stream with an epoch entry the first time it writes to it, and
 * keeps the opened state. A LogFenced (another writer took the stream) drops the cached state, so the next attempt
 * reopens and takes the stream again; the queue retry gives the backoff. A LogUnavailable keeps the state, so the
 * retry resends exactly the pending entry first.
 *
 * Visibility: ledger-log-materialize.ts applies the entries to the read-side state after the append (the consumer
 * kicks it; its sweeper catches the rest), so a batch is durable at the append and readable after materialization.
 * The first take registers the stream (registerLogStream) so the sweeper finds it.
 */
import { objectKey, type ObjectStore } from "../shared/vendor.ts";
import { assertNoLaterPrivacyControl, PrivacyControlPending } from "./async-admission.ts";
import { batchIdConflict, IngestUnavailable } from "./deliver.ts";
import { LedgerUnavailableError, type LedgerTenant } from "./ledger.ts";
import { appendBatches, LogFenced, logKeys, openStream, putPayload, takeStream, type LogState } from "./ledger-log.ts";
import { admissionFenceView, hasPendingReset, isDeletedForAdmission, s3LedgerObjects } from "./ledger-s3.ts";
import { registerLogStream } from "./ledger-log-materialize.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";

export interface LogAdmitDeps {
  store: ObjectStore;
  keyRoot: string;
  /** This process's writer id (recorded in epoch entries). */
  writer: string;
  nowMs?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export interface LogAdmitGates {
  /** The upload's privacy-journal seq (the inbox witness): a later completed reset refuses it. */
  journalSeq: number;
  /** Consent, as the legacy consentGate: throws UploadConsentRequired to hold the batch. */
  consent?: (dropped: ReadonlySet<string>, phase: "admit" | "commit") => Promise<void>;
}

export type LogAdmitOutcome = "accepted" | "replay";

/** How many open streams one process keeps (each holds its dedupe window): beyond it, idle ones are reopened. */
export const LOG_STREAMS_MAX = 4096;

const pairKey = (source: string, sessionId: string) => JSON.stringify([source, sessionId]);

/** The records' sessions, distinct, in first-seen order. */
function sessionsOf(p: PreparedDelivery): [string, string][] {
  const out: [string, string][] = [];
  const seen = new Set<string>();
  for (const r of p.records) {
    if (typeof r.session_id !== "string") continue;
    const k = pairKey(r.source, r.session_id);
    if (!seen.has(k)) { seen.add(k); out.push([r.source, r.session_id]); }
  }
  return out;
}

export class LogStreams {
  readonly #states = new Map<string, LogState>();
  readonly #objects: ReturnType<typeof s3LedgerObjects>;
  constructor(readonly deps: LogAdmitDeps) {
    this.#objects = s3LedgerObjects(deps.store, deps.keyRoot, { ...(deps.nowMs ? { nowMs: deps.nowMs } : {}), ...(deps.sleep ? { sleep: deps.sleep } : {}) });
  }

  #w() { return { writer: this.deps.writer, ...(this.deps.nowMs ? { now: this.deps.nowMs } : {}), ...(this.deps.sleep ? { sleep: this.deps.sleep } : {}) }; }
  #id(tenant: LedgerTenant, originId: string) { return logKeys(this.deps.keyRoot, tenant, originId).root; }

  /**
   * Is this stream in log mode? Entry 0 is the durable marker; the close marker (operator rollback) hands it back to
   * legacy, and drops the cached state. Both are read every time (two HEADs in parallel).
   */
  async isLogMode(tenant: LedgerTenant, originId: string): Promise<boolean> {
    const keys = logKeys(this.deps.keyRoot, tenant, originId);
    const [zero, closed] = await Promise.all([this.deps.store.head(objectKey(keys.entry(0))), this.deps.store.head(objectKey(keys.closed))]);
    if (closed !== undefined) { this.#states.delete(this.#id(tenant, originId)); return false; }
    return zero !== undefined;
  }

  async #owned(tenant: LedgerTenant, originId: string): Promise<LogState> {
    const id = this.#id(tenant, originId);
    const cached = this.#states.get(id);
    if (cached) return cached;
    const opened = await openStream(this.deps.store, logKeys(this.deps.keyRoot, tenant, originId));
    if (!opened) throw new LedgerUnavailableError("ledger_log_not_enabled");
    const state = await takeStream(this.deps.store, opened, this.#w());
    await registerLogStream(this.deps.store, this.deps.keyRoot, tenant, originId);
    // Bounded: drop the oldest states with nothing pending. A dropped stream is reopened and taken again on its next
    // batch, which is safe (a new epoch entry), only slower. A state with a pending entry is kept until it resolves.
    if (this.#states.size >= LOG_STREAMS_MAX)
      for (const [k, s] of this.#states) { if (this.#states.size < LOG_STREAMS_MAX) break; if (!s.pending) this.#states.delete(k); }
    this.#states.set(id, state);
    return state;
  }

  /** Admit one prepared batch (the module header's steps 1-6). Throws for a retry or a terminal refusal. */
  async admit(tenant: LedgerTenant, prepared: PreparedDelivery, gates: LogAdmitGates): Promise<LogAdmitOutcome> {
    const { store, keyRoot } = this.deps;
    const now = this.deps.nowMs ?? Date.now;
    const sessions = sessionsOf(prepared);
    // 1. The fence (read once, before any write).
    const read = await this.#objects.readFence(this.#objects.keysFor(tenant).fence);
    const fence = admissionFenceView(read);
    if (hasPendingReset(fence)) throw new LedgerUnavailableError("ledger_control_pending");
    if (fence.resets > fence.resetsCompleted) throw new LedgerUnavailableError("ledger_reset_in_flight");
    const deleted = new Set(sessions.filter(([s, id]) => isDeletedForAdmission(fence, prepared.originId, s, id)).map(([s, id]) => pairKey(s, id)));
    // 2. The privacy journal.
    try { await assertNoLaterPrivacyControl(store, keyRoot, tenant, gates.journalSeq, now()); }
    catch (error) {
      if (error instanceof PrivacyControlPending) throw new IngestUnavailable("admission_gate", error.code);
      throw error;
    }
    // 3. Consent at admit, with the sessions the fence dropped.
    if (gates.consent) await gates.consent(deleted, "admit");
    const kept = prepared.records.filter((r) => typeof r.session_id !== "string" || !deleted.has(pairKey(r.source, r.session_id)));
    // 4. The payload: the kept records, exactly as prepared.
    const state = await this.#owned(tenant, prepared.originId);
    // A batch id the window already holds is answered without writing a payload (a replay, or the batch-id 409),
    // so a retried or conflicting upload leaves no orphan payload behind. The append still decides any race.
    const known = state.remembered.get(prepared.batchId);
    if (known !== undefined) {
      if (known !== prepared.batchDigest) throw batchIdConflict(prepared, known);
      return "replay";
    }
    const ref = await putPayload(store, state.keys, new TextEncoder().encode(JSON.stringify({ v: 1, journalSeq: gates.journalSeq, receivedAtMs: now(), receipt: prepared.receipt, records: kept })));
    // 5. Consent again, just before the commit.
    if (gates.consent) await gates.consent(deleted, "commit");
    // 6. The commit.
    try {
      const r = await appendBatches(store, state, [{ batchId: prepared.batchId, digest: prepared.batchDigest, receiptId: prepared.receipt.receiptId,
        sessions: sessions.filter(([s, id]) => !deleted.has(pairKey(s, id))), payloads: [ref] }], null, this.#w());
      if (r.conflicts.length > 0) throw batchIdConflict(prepared, state.remembered.get(prepared.batchId) ?? "");
      return r.admitted.length > 0 ? "accepted" : "replay";
    } catch (error) {
      if (error instanceof LogFenced) this.#states.delete(this.#id(tenant, prepared.originId));
      throw error;
    }
  }
}
