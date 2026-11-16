/**
 * Log-mode admission (docs/design/s3-ledger-log-commit.md, slices 2 and 4): admits one prepared batch into its
 * stream's numbered log instead of the legacy claim -> canonical writes -> commit chain. Only a stream whose entry 0
 * (the durable mode marker) exists is admitted here; every other stream keeps the legacy path, unchanged.
 *
 * Per batch, in this order (the legacy core answers a replay before any gate, deliver.ts:398-410; so does this):
 *   0. the precheck: the tenant segment and the delivery budget, as deliverPrepared's precheck;
 *   1. the stream, READ-ONLY (the cached owned state, or a fresh open that is not taken), and any entry this process
 *      left unresolved settled first, so the window is current. Nothing is taken here: traffic answered as a replay,
 *      a conflict or a refusal writes no epoch entry and no registry doc;
 *   2. replay / conflict, BEFORE any gate: the stream's remembered-batches window, then the legacy batch doc (a batch
 *      committed before the switch, or one the window evicted and the materializer committed, is still a replay;
 *      the same id with another digest is the 409), so a retry of a committed batch is never refused or held by a
 *      control that landed after it committed;
 *   3. the user's fence: a pending ordered reset or an unfinished reset is a retry; deleted sessions (tombstones plus
 *      pending ordered disables) have their records dropped; the revision rules (ledger-s3 judge, the claim's exact
 *      rules: two records at one revision with another revision id and digest) refuse the batch as the legacy 409;
 *   4. the privacy journal: a reset / re-enable completed after the upload began refuses it (409); in flight retries;
 *   5. consent at admit, with the dropped sessions (upload-consent sessionKeys, as the legacy core passes them);
 *   6. the take (only now, the first write of this process to the stream: an epoch entry, the registry doc) and the
 *      window looked at again (another writer may have appended this batch since the open); then the payload PUT
 *      (content addressed; a per-attempt nonce makes every attempt a fresh object, so orphan collection never takes a
 *      stale identical payload between a retry's PUT and its append). A replay, a conflict or a refusal writes nothing;
 *   7. consent again at commit, just before the entry;
 *   8. the entry append: the one request that commits. It carries the admission snapshot (LogBatch.admitted: the
 *      journal seq, the fence's resets, each written session's epoch) the materializer re-judges against.
 *
 * Owner stubs (ingest_session_ownership) are registered by the materializer, through the legacy claim it runs at apply
 * time, never here. A reset that begins between the append and the apply cannot list a log-only session's stub, but
 * it needs not: nothing of that batch is written yet, and the materializer refuses the batch when the fence's resets
 * moved past the snapshot (and drops a session whose epoch moved), so the reset never misses written data.
 *
 * Single writer per stream: this process takes the stream with an epoch entry the first time it writes to it, and
 * keeps the opened state. Admissions of one stream run one at a time (a per-stream promise chain), so two batches of
 * one stream never race for head+1 inside this process. A LogFenced (another writer took the stream) drops the cached
 * state (only if it is still the state that failed), so the next attempt reopens and takes the stream again; the
 * queue retry gives the backoff. A LogUnavailable keeps the state, so the retry resends exactly the pending entry.
 *
 * Tenant slots: the legacy core holds a tenant slot across its claim; the log path takes none. Its concurrency per
 * stream is one (the chain above), which bounds it at least as tightly per user and origin.
 *
 * Visibility: ledger-log-materialize.ts applies the entries to the read-side state after the append (the consumer
 * kicks it; its sweeper catches the rest), so a batch is durable at the append and readable after materialization.
 * The first take registers the stream (registerLogStream) so the sweeper finds it.
 */
import { objectKey, type ObjectStore } from "../shared/vendor.ts";
import { assertNoLaterPrivacyControl, PrivacyControlPending } from "./async-admission.ts";
import { batchIdConflict, IngestUnavailable, revisionConflict } from "./deliver.ts";
import { LedgerUnavailableError, type LedgerTenant } from "./ledger.ts";
import { appendBatches, LogFenced, logKeys, openStream, putPayload, settlePending, takeStream, type LogAdmitSnapshot, type LogState } from "./ledger-log.ts";
import { admissionFenceView, batchPairs, fenceEpoch, hasPendingReset, judge, liveRecords, s3LedgerObjects, sessionKeyHash } from "./ledger-s3.ts";
import { readLogSkip, registerLogStream } from "./ledger-log-materialize.ts";
import { sessionKey } from "./upload-consent.ts";
import { assertTenantSegment } from "./writer.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";

export interface LogAdmitDeps {
  store: ObjectStore;
  keyRoot: string;
  /** This process's writer id (recorded in epoch entries). One per process lifetime: same id = same owner. */
  writer: string;
  nowMs?: () => number;
  sleep?: (ms: number) => Promise<void>;
  /** How many open streams to keep (default LOG_STREAMS_MAX). */
  maxStreams?: number;
  /** deliverPrepared's budget precheck (assertWithinBudget over the deliver deps); absent = no budget. */
  budget?: (records: PreparedDelivery["records"], receivedAt: Date) => void;
}

export interface LogAdmitGates {
  /** The upload's privacy-journal seq (the inbox witness): a later completed reset refuses it. */
  journalSeq: number;
  /** Consent, as the legacy consentGate: throws UploadConsentRequired to hold the batch. */
  consent?: (dropped: ReadonlySet<string>, phase: "admit" | "commit") => Promise<void>;
}

/** As the legacy core answers: accepted_all_deleted when the fence dropped every session the batch writes. */
export type LogAdmitOutcome = "accepted" | "accepted_all_deleted" | "replay";

/**
 * How many open streams one process keeps (each holds a dedupe window of up to LOG_REMEMBERED_BATCHES pairs).
 * Beyond it the oldest states with nothing pending are dropped (reopened and taken again on their next batch).
 */
export const LOG_STREAMS_MAX = 1024;

/** The payload's per-attempt nonce: each admission attempt writes its own payload object (see ledger-log-materialize). */
const attemptNonce = () => crypto.randomUUID().replace(/-/g, "");

const pairKey = (source: string, sessionId: string) => JSON.stringify([source, sessionId]);

/** The records' sessions, distinct, in first-seen order (a record with no session, or an empty id, names none). */
function sessionsOf(p: PreparedDelivery): [string, string][] {
  const out: [string, string][] = [];
  const seen = new Set<string>();
  for (const r of p.records) {
    if (typeof r.session_id !== "string" || r.session_id === "") continue;
    const k = pairKey(r.source, r.session_id);
    if (!seen.has(k)) { seen.add(k); out.push([r.source, r.session_id]); }
  }
  return out;
}

export class LogStreams {
  readonly #states = new Map<string, LogState>();
  readonly #chains = new Map<string, Promise<unknown>>();
  readonly #objects: ReturnType<typeof s3LedgerObjects>;
  constructor(readonly deps: LogAdmitDeps) {
    this.#objects = s3LedgerObjects(deps.store, deps.keyRoot, { ...(deps.nowMs ? { nowMs: deps.nowMs } : {}), ...(deps.sleep ? { sleep: deps.sleep } : {}) });
  }

  #w() { return { writer: this.deps.writer, ...(this.deps.nowMs ? { now: this.deps.nowMs } : {}), ...(this.deps.sleep ? { sleep: this.deps.sleep } : {}) }; }
  #id(tenant: LedgerTenant, originId: string) { return logKeys(this.deps.keyRoot, tenant, originId).root; }
  /** Drop a cached state, but only if it is still `state` (a later reopen is never thrown away by a stale failure). */
  #drop(id: string, state?: LogState) { if (state === undefined || this.#states.get(id) === state) this.#states.delete(id); }

  /**
   * Is this stream in log mode? Entry 0 is the durable marker; the close marker (operator rollback) hands it back to
   * legacy, and drops the cached state. Both are read every time (two HEADs in parallel). Unreadable: a retry.
   */
  async isLogMode(tenant: LedgerTenant, originId: string): Promise<boolean> {
    const keys = logKeys(this.deps.keyRoot, tenant, originId);
    let zero, closed;
    try { [zero, closed] = await Promise.all([this.deps.store.head(objectKey(keys.entry(0))), this.deps.store.head(objectKey(keys.closed))]); }
    catch { throw new LedgerUnavailableError("ledger_log_mode_unreadable"); }
    if (closed !== undefined) { this.#drop(this.#id(tenant, originId)); return false; }
    return zero !== undefined;
  }

  /**
   * The refusal code of a batch the materializer skipped for good (its skip record), or undefined. The consumer reads
   * it before and after writing a log-mode admitted outcome, so a replay of a skipped batch answers refused.
   */
  skipped(tenant: LedgerTenant, originId: string, batchId: string): Promise<string | undefined> {
    return readLogSkip(this.deps.store, logKeys(this.deps.keyRoot, tenant, originId), batchId);
  }

  /** The stream as this process knows it: the cached (owned) state, or a fresh read-only open (NOT taken, not cached). */
  async #view(tenant: LedgerTenant, originId: string): Promise<LogState> {
    const cached = this.#states.get(this.#id(tenant, originId));
    if (cached) return cached;
    const opened = await openStream(this.deps.store, logKeys(this.deps.keyRoot, tenant, originId));
    if (!opened) throw new LedgerUnavailableError("ledger_log_not_enabled");
    return opened;
  }

  /** Owned for writing: the cached state, or `view` taken now (an epoch entry), registered and cached. */
  async #owned(tenant: LedgerTenant, originId: string, view: LogState): Promise<LogState> {
    const id = this.#id(tenant, originId);
    const cached = this.#states.get(id);
    if (cached) return cached;
    const state = await takeStream(this.deps.store, view, this.#w());
    await registerLogStream(this.deps.store, this.deps.keyRoot, tenant, originId);
    // Bounded: drop the oldest states with nothing pending and no admission in flight (a stream whose chain is running
    // keeps its state, so an in-flight admission is never fenced by its own process's reopen). A dropped stream is
    // reopened and taken again on its next batch: safe (a new epoch entry), only slower.
    const max = this.deps.maxStreams ?? LOG_STREAMS_MAX;
    if (this.#states.size >= max)
      for (const [k, s] of this.#states) { if (this.#states.size < max) break; if (!s.pending && !this.#chains.has(k)) this.#states.delete(k); }
    this.#states.set(id, state);
    return state;
  }

  /** The window's answer for this batch: replay, a 409 (thrown), or undefined (unknown). */
  #fromWindow(state: LogState, prepared: PreparedDelivery): "replay" | undefined {
    const known = state.remembered.get(prepared.batchId);
    if (known === undefined) return undefined;
    if (known !== prepared.batchDigest) throw batchIdConflict(prepared, known);
    return "replay";
  }

  /** Admit one prepared batch (the module header's steps). Throws for a retry or a terminal refusal. Serial per stream. */
  admit(tenant: LedgerTenant, prepared: PreparedDelivery, gates: LogAdmitGates): Promise<LogAdmitOutcome> {
    const id = this.#id(tenant, prepared.originId);
    const run = (this.#chains.get(id) ?? Promise.resolve()).catch(() => {}).then(() => this.#admitOne(tenant, prepared, gates));
    const tail = run.catch(() => {});
    this.#chains.set(id, tail);
    void tail.finally(() => { if (this.#chains.get(id) === tail) this.#chains.delete(id); });
    return run;
  }

  async #admitOne(tenant: LedgerTenant, prepared: PreparedDelivery, gates: LogAdmitGates): Promise<LogAdmitOutcome> {
    const { store, keyRoot } = this.deps;
    const now = this.deps.nowMs ?? Date.now;
    // 0. The precheck, as the legacy core's.
    assertTenantSegment(keyRoot, tenant.orgId);
    this.deps.budget?.(prepared.records, new Date(now()));
    const id = this.#id(tenant, prepared.originId);
    // 1. The stream, read-only: nothing is taken (no epoch entry, no registry write) for traffic answered as a
    // replay, a conflict or a refusal. An entry this process left unresolved is settled first, so the window reflects
    // what landed before it answers.
    let state = await this.#view(tenant, prepared.originId);
    try {
      if (state.pending) await settlePending(store, state, this.#w());
      // 2. Replay / conflict before any gate: the window, then the legacy batch doc.
      if (this.#fromWindow(state, prepared) === "replay") return "replay";
      const keys = this.#objects.keysFor(tenant);
      const [stored, read] = await Promise.all([this.#objects.readBatch(keys.batch(prepared.originId, prepared.batchId)), this.#objects.readFence(keys.fence)]);
      if (stored) {
        if (stored.doc.digest !== prepared.batchDigest) throw batchIdConflict(prepared, stored.doc.digest);
        if (stored.doc.state === "committed") return "replay";
        // A legacy claim still in flight (the switch drains them first, so only a crash leaves one): retry.
        throw new LedgerUnavailableError("ledger_batch_pending");
      }
      // 3. The fence and the revision rules (the claim's own judge over the shards of the live records).
      const fence = admissionFenceView(read);
      if (hasPendingReset(fence)) throw new LedgerUnavailableError("ledger_control_pending");
      if (fence.resets > fence.resetsCompleted) throw new LedgerUnavailableError("ledger_reset_in_flight");
      const sessions = sessionsOf(prepared);
      const claimInput = { originId: prepared.originId, sessions, records: prepared.records.map((r) => ({ source: r.source, sessionId: r.session_id,
        recordId: r.record_id, revisionId: r.revision_id, revision: r.revision, digest: r.digest })) };
      const judged = judge(claimInput, keys, fence, await this.#objects.readShards(prepared.originId, keys, liveRecords(claimInput, fence)));
      if (judged.twins > 0) throw revisionConflict(prepared.originId, judged.conflicts, judged.twins);
      const deleted = new Set(judged.deleted.map(([s, sid]) => pairKey(s, sid)));
      const dropped: ReadonlySet<string> = new Set(judged.deleted.map(([s, sid]) => sessionKey(s, sid)));
      // 4. The privacy journal.
      try { await assertNoLaterPrivacyControl(store, keyRoot, tenant, gates.journalSeq, now()); }
      catch (error) {
        if (error instanceof PrivacyControlPending) throw new IngestUnavailable("admission_gate", error.code);
        throw error;
      }
      // 5. Consent at admit, with the sessions the fence dropped.
      if (gates.consent) await gates.consent(dropped, "admit");
      const kept = prepared.records.filter((r) => typeof r.session_id !== "string" || !deleted.has(pairKey(r.source, r.session_id)));
      const written = sessions.filter(([s, sid]) => !deleted.has(pairKey(s, sid)));
      // The admission snapshot the materializer re-judges against (the legacy claim's batch-doc fields).
      const epochs: Record<string, number> = {};
      for (const [s, sid] of batchPairs(written, [])) { const k = sessionKeyHash(s, sid); epochs[k] = fenceEpoch(fence, prepared.originId, k); }
      const admitted: LogAdmitSnapshot = { journalSeq: gates.journalSeq, resets: fence.resets, epochs };
      // Only now take the stream (its first write here: an epoch entry), and look again: another writer may have
      // appended this batch between the read-only open and the take.
      state = await this.#owned(tenant, prepared.originId, state);
      if (this.#fromWindow(state, prepared) === "replay") return "replay";
      // 6. The payload: the kept records, exactly as prepared, plus this attempt's nonce (a fresh object per attempt).
      const ref = await putPayload(store, state.keys, new TextEncoder().encode(JSON.stringify({ v: 1, journalSeq: gates.journalSeq, attempt: attemptNonce(), receipt: prepared.receipt, records: kept })));
      // 7. Consent again, just before the commit.
      if (gates.consent) await gates.consent(dropped, "commit");
      // 8. The commit.
      const r = await appendBatches(store, state, [{ batchId: prepared.batchId, digest: prepared.batchDigest, receiptId: prepared.receipt.receiptId,
        sessions: written, payloads: [ref], admitted }], null, this.#w());
      if (r.conflicts.length > 0) throw batchIdConflict(prepared, state.remembered.get(prepared.batchId) ?? "");
      if (r.admitted.length === 0) return "replay";
      return written.length === 0 && sessions.length > 0 ? "accepted_all_deleted" : "accepted";
    } catch (error) {
      if (error instanceof LogFenced) this.#drop(id, state);
      throw error;
    }
  }
}
