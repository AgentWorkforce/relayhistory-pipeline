import type {CurrentCustodyPacket,CurrentCustodyAck} from "./custody-protocol.ts";
import { sessionKey } from "./upload-consent.ts";
import {validatedCustodyAck} from "./custody-ack.ts";
/**
 * `POST /v1/delivery/batches` without Neon: every accepted batch is in S3
 * before the response, and there is no fallback path.
 *
 *   prepare (CPU: validate, normalize, scrub payloads, digests, receipt; vendored relayhistory code)
 *   -> claim  (Supabase, one round trip: fences, replay / 409, stale records, pending)
 *   -> write  (S3: scrubbed chunks + manifest CAS + pointer, m=srv_ layout; a batch wider
 *             than one invocation is staged as one object and fanned out by the queue)
 *   -> replica (optional, transition only: one Cloudflare Queue send; Neon applies it later)
 *   -> commit (Supabase, one round trip: committed, written revisions, fence recheck)
 *   -> 200 receipt
 *
 * Status semantics are relayhistory's: 400/403/413 from prepare, 409
 * `delivery_conflict` (batch_id or record_revision, same bodies), 503
 * `delivery_unavailable` for anything transient (ledger or S3 unreachable,
 * replica send failed while required). A 503 is safe to retry: the claim is
 * `pending` until commit, and a retry with the same digest resumes the same
 * idempotent write (create-only chunks and pointers, manifest CAS with the
 * replay extension).
 *
 * Replay fast path (old Desktops re-send whole histories at old revisions):
 * the claim reports which records are already stored at or above their
 * revision (`stale`, #250 rules), and they are not written again. A batch of
 * nothing but stale or deleted records is committed by the claim itself: one
 * Supabase round trip, no S3, no slot, no lock.
 *
 * 429 `delivery_busy` comes only from the per-tenant slot limit, with
 * relayhistory's jittered `Retry-After` (2-10 s), never from waiting on a
 * lock. A slot is taken only for a batch that has something to write, after
 * the claim; a refused batch stays `pending` and its retry resumes it.
 * Exception, default off (INGEST_CLAIM_LOCK_BUSY=on, `DeliverDeps.claimLockBusy`):
 * a claim whose single autocommit statement lost its lock wait (55P03/40P01,
 * `ledger_lock_timeout`) is a 429 `ClaimLockBusy` with `Retry-After` 5-10 s instead
 * of a 503. The statement rolled back, so nothing was written and the retry is the
 * same idempotent claim; 57014 (`ledger_timeout`) and every other cause stay 503.
 * Second exception, default off and independent (INGEST_CLAIM_TIMEOUT_BUSY=on,
 * `DeliverDeps.claimTimeoutBusy`): a claim whose statement hit statement_timeout or was
 * cancelled (`ledger_timeout` with SQLSTATE 57014 by code only; name-only client timeouts
 * stay 503) is the same 429 `ClaimLockBusy`. It is the same rolled back single statement.
 * Hypothesis, pending operator pg_stat_activity/lock/plan evidence: several sub-2 s lock
 * waits on the per-user fence row add up to the 5 s statement_timeout.
 * Buffered acceptance, two more exceptions, each default off and independent:
 * INGEST_BUFFER_LOCK_RETRY=on (`DeliverDeps.bufferLockRetry`) re-issues a `buffer_intent` /
 * `buffer_receipt` ledger call that failed with `ledger_lock_timeout` (55P03/40P01) once, after a
 * 50-250 ms jittered pause; INGEST_BUFFER_LOCK_BUSY=on (`DeliverDeps.bufferLockBusy`) answers that
 * failure (after the retry, if any) as the 429 `BufferLockBusy` (a `ClaimLockBusy`, Retry-After 5-10 s)
 * instead of a 503. Both calls are one autocommit statement whose error rolled it back whole
 * (ingest_prepare_buffer_stage is idempotent per claim and stage key; ingest_commit_buffered_batch
 * answers a repeat with the same receipt). Every other stage and cause stays as it was.
 * Each retry writes one warn line, `route` `ingest.buffer_lock_retry` (BufferLockRetryFields).
 * Time bound (role lock_timeout 2 s, statement_timeout 5 s, supabase/bootstrap/session_ingest_role.sql):
 * lock_timeout applies to each lock acquisition, so one attempt that ends in 55P03/40P01 can take up to
 * the 5 s statement_timeout (past that it is a 57014, never retried). The retry adds at most 250 ms + 5 s
 * = 5.25 s per stage (a stage worst case of about 10.25 s); a request where both buffer stages retry adds
 * at most about 10.5 s, and holds its Hyperdrive connection for that long.
 * INGEST_BUFFER_COMMIT_LOCK_WAIT_MS (default unset; ledger-sql.ts bufferCommitLockWaitSql) raises only the
 * `buffer_receipt` commit's per-lock wait to 2001..4000 ms inside the same one statement. The 5 s
 * statement_timeout is unchanged and still bounds each attempt, so the bounds above hold; a failed attempt
 * holds its connection up to the configured wait instead of 2 s, and a wait that reaches the statement budget
 * ends 57014 (`ledger_timeout`), which this stage neither retries nor converts unless INGEST_BUFFER_TIMEOUT_BUSY is on.
 * INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS (default unset; ledger-sql.ts inBufferCommitBudget) gives the commit its own
 * statement budget (5001..7000 ms) in its own transaction; each commit attempt is then bounded by that instead of 5 s.
 */
import { log } from "../shared/log.ts";
import type { Ledger, LedgerTenant, RevisionConflict, SlotRequest } from "./ledger.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { STORE_TRANSPORT_CAUSES } from "../shared/vendor.ts";

const STORE_CAUSE_SET: ReadonlySet<string> = new Set(STORE_TRANSPORT_CAUSES);
import type { DbErrorDiagnosis } from "./db-error-class.ts";
import { advertisedDeliveryLimits } from "./advertised-limits.ts";
import {
  DELIVERY_BUSY_RETRY_AFTER,
  DeliveryBusyError,
  DeliveryError,
  MAX_DELIVERY_CONFLICTS,
  type DeliveryReceipt,
  type DeliveryRecordRevisionConflict,
  type HistoryExportBatch,
} from "./vendor/delivery/delivery-contracts.ts";
import { prepareDelivery, type PreparedDelivery } from "./vendor/delivery/prepare.ts";
import { batchMarkerKey, replicaMessage, sendReplica, stagedReplicaMessage, type ReplicaOutcome, type ReplicaSink } from "./replica.ts";
import { CAS_ATTEMPTS, GROUP_ATTEMPTS, IngestWriteError, assertTenantSegment, groupRecords, liveMachineId, writeBatchObjects, type WriteResult, type WriteTarget } from "./writer.ts";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { prepareBufferedStage, putBufferedStage, fanoutMessages, stageGroups, type FanoutMessage } from "./staging.ts";
import { MAX_FETCH_REDIRECTS, babysitterStub, bucketBodies, bucketRefs, planBabysitterBuckets, planKey, worstCaseSubrequests, writeBucketObjects, type BucketRefs } from "./babysitter-retention.ts";

export interface DeliverDeps {
  /** Default-off self-host operator fence: persist an obligation around actual object writes. */
  withSessionWrites?: import("../session-erase/runtime.ts").SessionWriteGuard;
  /** Verified private source packet, bound to this request only. */
  custody?:CurrentCustodyPacket;
  ledger: Ledger;
  acceptanceMode?: IngestAcceptanceMode;
  target: WriteTarget;
  replica?: ReplicaSink;
  now?: () => Date;
  /** Per-tenant in-flight batch limits; unset: no limit. */
  slots?: Omit<SlotRequest, "tenant" | "holder">;
  /** Unique per request (the slot lease holder). */
  requestId?: () => string;
  /**
   * Sessions written inline per request (the Worker subrequest budget). A
   * batch with more is staged (staging.ts) and fanned out by the queue stage.
   * Unset: always inline.
   */
  inlineMaxSessions?: number;
  /** Enqueue fan-out messages (INGEST_FANOUT). Required with `inlineMaxSessions`. */
  fanout?: (messages: FanoutMessage[]) => Promise<void>;
  /**
   * INGEST_BABYSITTER_RETENTION_ROUTING: write Babysitter upserts to retention buckets
   * (babysitter-retention.ts), registered before the PUT; the record itself continues
   * as a payload-free stub in its own session, in every acceptance mode (the SQL
   * publication derives every group's session from its claim records, so groups keep
   * owners). Requires the Neon replica off (its consumer rebuilds rows from lines).
   */
  babysitterRetention?: boolean;
  /**
   * The proven effective subrequest cap of this Worker (INGEST_SUBREQUEST_CAP). Routing is
   * inactive without it: an unproven cap is never assumed. With it, a routed batch whose
   * worst case (babysitter-retention.ts `worstCaseSubrequests`) exceeds it is refused
   * before the claim with a typed 422.
   */
  subrequestCap?: number;
  /** Redirects each S3 fetch may follow in the worst case (INGEST_SUBREQUEST_REDIRECTS; default 20, the fetch maximum). */
  subrequestRedirects?: number;
  /** The current compiled schema generation (INGEST_EXPECT_GENERATION); every evidence write is fenced on it. Routing is inactive without it. */
  retentionGeneration?: string;
  /**
   * INGEST_STAGE_LATENCY_LOG=on: one `StageLatencyFields` line per deliver stage, successes and
   * failures alike. Unset (the default): no line, no behaviour change.
   */
  stageLatency?: StageLatencyObserver;
  /**
   * INGEST_CLAIM_LOCK_BUSY=on: a claim-stage lock-wait failure (cause `ledger_lock_timeout`,
   * SQLSTATE 55P03/40P01) is refused as a 429 `ClaimLockBusy` with `Retry-After` in
   * CLAIM_LOCK_RETRY_AFTER instead of a 503, so a second concurrent claim for the same
   * tenant backs off past the holder's statement_timeout instead of stacking behind it.
   * Unset (the default): the 503 `IngestUnavailable`, unchanged.
   */
  claimLockBusy?: true;
  /**
   * INGEST_CLAIM_TIMEOUT_BUSY=on: a claim-stage statement timeout / cancel (cause `ledger_timeout`,
   * SQLSTATE 57014 by code only; name-only client timeouts stay 503) is refused as the same 429 `ClaimLockBusy` (Retry-After in CLAIM_LOCK_RETRY_AFTER).
   * Independent of `claimLockBusy`; the route line keeps `error_code: ledger_timeout` /
   * `db_error_code: 57014`, so the two causes stay separable. Unset (the default): the 503, unchanged.
   */
  claimTimeoutBusy?: true;
  /**
   * INGEST_BUFFER_LOCK_BUSY=on (buffered acceptance): a `buffer_intent` / `buffer_receipt` ledger failure with
   * cause `ledger_lock_timeout` (SQLSTATE 55P03/40P01) is refused as a 429 `BufferLockBusy` (a `ClaimLockBusy`
   * that keeps the real stage) with `Retry-After` in CLAIM_LOCK_RETRY_AFTER instead of a 503. Every other
   * cause and stage is unchanged. Unset (the default): the 503 `IngestUnavailable`, unchanged.
   */
  bufferLockBusy?: true;
  /**
   * INGEST_BUFFER_LOCK_RETRY=on (buffered acceptance): on that same failure, the same ledger call with the
   * same input is issued exactly once more after a pause in BUFFER_LOCK_RETRY_PAUSE_MS (jitter from
   * `random`). A second failure propagates as before (with `bufferLockBusy`, the 429; otherwise the 503).
   * Every other cause and stage is never retried. Unset (the default): one call, unchanged.
   */
  bufferLockRetry?: true;
  /**
   * INGEST_BUFFER_TIMEOUT_BUSY=on (buffered acceptance): at `buffer_intent` / `buffer_receipt` only, a failure with
   * cause `ledger_timeout` and SQLSTATE 57014 BY CODE (a name-only client timeout never qualifies) is handled like the
   * lock wait above: retried once with `bufferLockRetry`, answered 429 BufferLockBusy with `bufferLockBusy`. The
   * route line keeps `error_code: ledger_timeout` / `db_error_code: 57014`, so the classes stay separable. Without
   * either of those flags it changes nothing. Unset (the default): a 57014 there stays the 503, unchanged.
   */
  bufferTimeoutBusy?: true;
  /**
   * INGEST_ADVERTISED_MAX_RECORDS (advertised-limits.ts): the maxRecords this Worker publishes in receipts and on
   * GET /v1/delivery/limits. Advertising only: acceptance stays MAX_DELIVERY_RECORDS. Unset (the default): 500.
   */
  advertisedMaxRecords?: number;
  /** Test seam for the ClaimLockBusy / BufferLockBusy jitter and the buffer retry pause: a value in [0, 1). Default Math.random. */
  random?: () => number;
  /** Test seam for the INGEST_BUFFER_LOCK_RETRY pause. Default: a setTimeout promise. */
  sleep?: (ms: number) => Promise<void>;
  /** Test seam for the one INGEST_BUFFER_LOCK_RETRY line per retry. Default: `log.warn` (src/shared/log.ts allowlist). */
  bufferLockRetryLog?: (fields: BufferLockRetryFields) => void;
  /**
   * Async admission only (async-admission.ts): runs AFTER a `claimed`/`resume` claim and BEFORE
   * any slot, S3 write or receipt, and throws to refuse. The consumer uses it for the
   * upload-time privacy fence (a reset or re-enable that started after the upload refuses
   * the batch). Never set on the request path. A replay / replay_fast claim never calls it:
   * those admit nothing new.
   */
  admissionGate?: () => Promise<void>;
  /**
   * Upload consent (upload-consent.ts): runs on the request path AND in the async consumer, AFTER a
   * `claimed`/`resume` claim and BEFORE any slot or S3 write, then once more right before the receipt
   * commit, so a narrowing that lands while the batch is in flight still refuses it before it is visible.
   * `dropped` names the sessions the claim's deletion fence acknowledges without storing (as
   * `upload-consent.ts` `sessionKey`s). Throws `UploadConsentRequired` to refuse. A replay / replay_fast
   * claim never calls it: those admit nothing new.
   */
  consentGate?: (dropped: ReadonlySet<string>, phase: "admit" | "commit") => Promise<void>;
}

/** Whether this deployment routes Babysitter upserts to retention buckets (see `DeliverDeps.babysitterRetention`). */
export function babysitterRoutingActive(deps: Pick<DeliverDeps, "babysitterRetention" | "replica" | "subrequestCap" | "retentionGeneration">): boolean {
  return deps.babysitterRetention === true && (!deps.replica || deps.replica.mode === "off")
    && Number.isSafeInteger(deps.subrequestCap) && deps.subrequestCap! > 0
    && typeof deps.retentionGeneration === "string" && /^[0-9a-f]{64}$/.test(deps.retentionGeneration);
}

/**
 * A routed batch that cannot fit the proven subrequest cap, refused BEFORE the claim
 * (nothing written: no ledger row, no S3 object). A typed 422, not a 503: replaying the
 * same immutable batch can never fit, and released Desktops (released-v2026.11.4-beta.1
 * and later) isolate a 400/422-rejected batch locally into smaller deterministic child
 * batches, which is the supported client reduction. The server never splits a batch and
 * never changes an identity.
 */
export class DeliveryBudgetExceeded extends DeliveryError {
  constructor() {
    super("delivery_budget_exceeded", 422, "Delivery batch exceeds this destination's per-request budget; send it in smaller batches");
  }
}

/** Refuses (DeliveryBudgetExceeded) a routed batch over the cap; a no-op when routing is inactive or nothing routes. */
export function assertWithinBudget(deps: Pick<DeliverDeps, "babysitterRetention" | "replica" | "subrequestCap" | "subrequestRedirects" | "acceptanceMode" | "inlineMaxSessions" | "retentionGeneration">, records: PreparedDelivery["records"], receivedAt: Date): void {
  if (!babysitterRoutingActive(deps)) return;
  // Bucket PUTs: one per bucket holding an upsert line (tombstone-only buckets write no object).
  const plan = planBabysitterBuckets(records, receivedAt.getTime());
  const buckets = new Set(plan.routed.map((r) => planKey(plan.of(r)!))).size;
  if (buckets === 0) return;
  const sessions = groupRecords(records, receivedAt).groups.length;
  const mode = deps.acceptanceMode === "buffered" ? "buffered" : deps.inlineMaxSessions !== undefined && sessions > deps.inlineMaxSessions ? "staged" : "inline";
  const worst = worstCaseSubrequests({ buckets, sessions, mode, redirects: deps.subrequestRedirects ?? MAX_FETCH_REDIRECTS }, GROUP_ATTEMPTS, CAS_ATTEMPTS);
  if (worst > deps.subrequestCap!) throw new DeliveryBudgetExceeded();
}

export type DeliverOutcome = "accepted" | "accepted_buffered" | "accepted_staged" | "replay" | "replay_fast" | "accepted_all_deleted";

export interface DeliverResult {
  custodyAck?:CurrentCustodyAck;
  receipt: DeliveryReceipt;
  outcome: DeliverOutcome;
  /** Ids, counts and codes only: what the request log line may carry. */
  stats: {
    records: number;
    sessions: number;
    deleted_sessions: number;
    /** Records already stored at or above their revision (not written again). */
    stale: number;
    manifests_written: number;
    replica: ReplicaOutcome;
    rearmed: number;
    timing_ms: Record<string, number>;
  };
}

/** Code only: storage and ledger messages can quote keys. */
export class IngestUnavailable extends DeliveryError {
  /** `db`: set only for a ledger (database) cause; the closed diagnosis (class, safe code, basis). */
  /**
   * `storeStatus`: set only for an object-store ledger cause (ledger-s3.ts `ledger_store_*`), the store's HTTP status
   * (an integer 100..599), so the admission consumer's retry line can say what the store answered. Never a key or body.
   */
  readonly storeStatus?: number;
  /** `storeCause`: a status-less store failure's closed transport class (STORE_TRANSPORT_CAUSES), never text. */
  readonly storeCause?: string;
  constructor(readonly stage: string, readonly cause_code: string, readonly db?: DbErrorDiagnosis, storeStatus?: number, storeCause?: string) {
    super("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
    if (typeof storeStatus === "number" && Number.isSafeInteger(storeStatus) && storeStatus >= 100 && storeStatus <= 599) this.storeStatus = storeStatus;
    if (typeof storeCause === "string" && STORE_CAUSE_SET.has(storeCause)) this.storeCause = storeCause;
  }
}

/**
 * Retry-After for a claim lock-wait refusal, in seconds: inside the 429 contract
 * (DELIVERY_BUSY_RETRY_AFTER, 2-10) but at least the role statement_timeout (5 s,
 * supabase/bootstrap/session_ingest_role.sql), so the retry arrives after the holding
 * claim has committed or been cancelled.
 */
export const CLAIM_LOCK_RETRY_AFTER = Object.freeze({ min: 5, max: DELIVERY_BUSY_RETRY_AFTER.max });

/**
 * A claim refused because its statement lost the lock wait to a concurrent claim for the
 * same tenant (INGEST_CLAIM_LOCK_BUSY=on). A 429 `delivery_busy` (lock `ledger_claim`):
 * the single autocommit claim statement rolled back, so the claim wrote nothing (for the buffer stages
 * see BufferLockBusy: earlier stages' writes remain). Keeps the stage, cause
 * and database diagnosis of the 503 it replaces, for the route line and the lock alert.
 */
export class ClaimLockBusy extends DeliveryBusyError {
  readonly stage: string = "claim";
  constructor(readonly cause_code: string, readonly db: DbErrorDiagnosis | undefined, retryAfterSeconds: number, lock: "ledger_claim" | "ledger_buffer" = "ledger_claim") {
    super(lock, retryAfterSeconds);
  }
}

export type BufferLockStage = "buffer_intent" | "buffer_receipt";

/**
 * A buffered stage-intent / receipt ledger call refused because its single autocommit statement lost a
 * lock wait (INGEST_BUFFER_LOCK_BUSY=on). The same 429 `delivery_busy` answer as `ClaimLockBusy` (it is
 * one, so the route line, the lock alert and the admission consumer treat it alike) with lock
 * `ledger_buffer` and the real stage. This statement rolled back, but earlier stages' writes remain: at
 * `buffer_intent` the committed (pending) claim, and at `buffer_receipt` also the staged object (`buffer_put`)
 * and the stage-intent row. A client retry resumes the same claim and stage key; without one, the stage object
 * and intent stay orphaned until the intent expires (120 s) or buffered garbage collection reclaims them.
 */
export class BufferLockBusy extends ClaimLockBusy {
  constructor(override readonly stage: BufferLockStage, cause_code: string, db: DbErrorDiagnosis | undefined, retryAfterSeconds: number) {
    super(cause_code, db, retryAfterSeconds, "ledger_buffer");
  }
}

/** INGEST_BUFFER_LOCK_RETRY pause before the one re-issue, in ms (inclusive). The whole retry adds at most
 * this pause plus one more attempt, which the role statement_timeout caps at 5 s: about 5.25 s per stage. */
export const BUFFER_LOCK_RETRY_PAUSE_MS = Object.freeze({ min: 50, max: 250 });

/**
 * The one warn line per INGEST_BUFFER_LOCK_RETRY retry (written on the retry path only, never without the flag):
 * the stage, the SQLSTATE that caused the retry (`db_error_code`, 55P03 / 40P01), and how the retry ended.
 * `outcome` `failed` adds the second failure's cause (`error_code`) and, for a database failure, its SQLSTATE
 * (`sqlstate`). Closed values only: no tenant data, no ids, no messages. Diagnostic only.
 */
export interface BufferLockRetryFields {
  route: "ingest.buffer_lock_retry";
  stage: BufferLockStage;
  db_error_code?: string;
  outcome: "ok" | "failed";
  error_code?: string;
  sqlstate?: string;
}

function bufferLockRetryPauseMs(random: () => number): number {
  const { min, max } = BUFFER_LOCK_RETRY_PAUSE_MS;
  const r = random();
  return min + Math.floor((Number.isFinite(r) && r >= 0 && r < 1 ? r : 0) * (max - min + 1));
}

function claimLockRetryAfter(random: () => number): number {
  const { min, max } = CLAIM_LOCK_RETRY_AFTER;
  const r = random();
  return min + Math.floor((Number.isFinite(r) && r >= 0 && r < 1 ? r : 0) * (max - min + 1));
}

function causeCode(error: unknown): string {
  if (error instanceof LedgerUnavailableError || error instanceof IngestWriteError) return error.code;
  const code = (error as { code?: unknown })?.code;
  if (typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code)) return code;
  const name = (error as { name?: unknown })?.name;
  return typeof name === "string" && /^[A-Za-z]{1,64}$/.test(name) ? name : "Error";
}

const pair = (source: string, sessionId: string | null) => JSON.stringify([source, sessionId ?? ""]);

function withLimits(prepared: PreparedDelivery, deps: Pick<DeliverDeps, "advertisedMaxRecords">): DeliveryReceipt {
  return { ...prepared.receipt, limits: { ...advertisedDeliveryLimits(deps.advertisedMaxRecords) } };
}

export function revisionConflict(originId: string, conflicts: RevisionConflict[], count: number): DeliveryError {
  const items: DeliveryRecordRevisionConflict[] = conflicts.slice(0, MAX_DELIVERY_CONFLICTS).map((c) => ({ type: "record_revision", originId, ...c }));
  return new DeliveryError("delivery_conflict", 409, "Delivery record revision was reused with different content", items[0], items, count);
}

export function batchIdConflict(prepared: PreparedDelivery, currentDigest: string): DeliveryError {
  return new DeliveryError("delivery_conflict", 409, "Delivery batch identity was reused with different content", {
    type: "batch_id",
    originId: prepared.originId,
    batchId: prepared.batchId,
    submittedDigest: prepared.batchDigest,
    currentDigest,
  });
}

/** One per-stage latency line (INGEST_STAGE_LATENCY_LOG=on; `DeliverDeps.stageLatency`): the deliver stage
 * name, how it ended, its wall time and, for a ledger (database) failure, the SQLSTATE. Closed values
 * only: no tenant data, no ids, no messages. Diagnostic only. */
export interface StageLatencyFields {
  route: "ingest.stage_latency";
  stage: string;
  /** ok | refused (a typed DeliveryError, e.g. 403 P0W01 or 409) | unavailable (the 503 IngestUnavailable). */
  outcome: "ok" | "refused" | "unavailable";
  duration_ms: number;
  /** Present only for a database failure with a syntactically valid SQLSTATE (db-error-class.ts). */
  sqlstate?: string;
}
export type StageLatencyObserver = (fields: StageLatencyFields) => void;

const SQLSTATE_SHAPE = /^[0-9A-Z]{5}$/;

/** The observer never changes the delivery: a throwing observer is ignored. */
function observeStage(observe: StageLatencyObserver | undefined, fields: StageLatencyFields): void {
  if (!observe) return;
  try { observe(fields); } catch { /* diagnostic only */ }
}

async function runStage<T>(name: string, work: () => Promise<T>, observe?: StageLatencyObserver): Promise<T> {
  const t0 = observe ? Date.now() : 0;
  try {
    const value = await work();
    observeStage(observe, { route: "ingest.stage_latency", stage: name, outcome: "ok", duration_ms: Date.now() - t0 });
    return value;
  } catch (error) {
    // IngestUnavailable is itself a DeliveryError (503): rethrown unchanged as before, but labelled unavailable.
    const passthrough = error instanceof DeliveryError;
    const db = error instanceof LedgerUnavailableError || error instanceof IngestUnavailable ? error.db : undefined;
    const sqlstate = db && SQLSTATE_SHAPE.test(db.db_error_code) ? db.db_error_code : undefined;
    const outcome = passthrough && !(error instanceof IngestUnavailable) ? "refused" : "unavailable";
    observeStage(observe, { route: "ingest.stage_latency", stage: name, outcome, duration_ms: Date.now() - t0, ...(sqlstate ? { sqlstate } : {}) });
    if (passthrough) throw error;
    const storeStatus = error instanceof LedgerUnavailableError ? error.storeStatus : undefined;
    const storeCause = error instanceof LedgerUnavailableError ? error.storeCause : undefined;
    throw new IngestUnavailable(name, causeCode(error), db, storeStatus, storeCause);
  }
}

/** The consent gate as its own stage: a store failure is a 503 (no ACK), never an admit. */
function consentStage(deps: Pick<DeliverDeps, "stageLatency" | "consentGate">, dropped: ReadonlySet<string>, phase: "admit" | "commit"): Promise<void> {
  return runStage(phase === "admit" ? "consent_gate" : "consent_commit", () => deps.consentGate!(dropped, phase), deps.stageLatency);
}

/** The stage wrapper bound to this delivery's observer (undefined, the default: no latency lines). */
function stagesFor(deps: Pick<DeliverDeps, "stageLatency">) {
  return <T>(name: string, work: () => Promise<T>): Promise<T> => runStage(name, work, deps.stageLatency);
}

/** SQLSTATE 57014 by code only: `ledger_timeout` is also derived from a code-less error whose name
 * matches /timeout/i (a client/pool TimeoutError); those, and a 57014 seen only in a message, stay 503. */
function isClaimStatementTimeout(error: IngestUnavailable): boolean {
  return error.cause_code === "ledger_timeout" && error.db !== undefined &&
    error.db.db_error_code === "57014" && error.db.db_error_basis === "code";
}

/** The claim stage; with `claimLockBusy` its lock-wait 503, and with `claimTimeoutBusy` its statement-timeout
 * 503, becomes a 429 ClaimLockBusy (nothing else changes). */
async function claimStage<T>(deps: Pick<DeliverDeps, "stageLatency" | "claimLockBusy" | "claimTimeoutBusy" | "random">, work: () => Promise<T>): Promise<T> {
  try {
    return await runStage("claim", work, deps.stageLatency);
  } catch (error) {
    if (error instanceof IngestUnavailable && error.stage === "claim" &&
      ((deps.claimLockBusy && error.cause_code === "ledger_lock_timeout") || (deps.claimTimeoutBusy && isClaimStatementTimeout(error))))
      throw new ClaimLockBusy(error.cause_code, error.db, claimLockRetryAfter(deps.random ?? Math.random));
    throw error;
  }
}

const sqlstateOf = (error: unknown): string | undefined => {
  const db = error instanceof IngestUnavailable ? error.db : undefined;
  return db && SQLSTATE_SHAPE.test(db.db_error_code) ? db.db_error_code : undefined;
};

/** The retry line never changes the delivery: a throwing logger is ignored. */
function bufferLockRetryLine(deps: Pick<DeliverDeps, "bufferLockRetryLog">, fields: BufferLockRetryFields): void {
  try { (deps.bufferLockRetryLog ?? ((f: BufferLockRetryFields) => log.warn("session ingest buffer lock retry", { ...f })))(fields); } catch { /* diagnostic only */ }
}

/** The failures a buffer stage retries / converts: its lock wait, and with `timeoutBusy` its statement timeout (57014 by code). */
const isBufferLockWait = (error: unknown, stage: BufferLockStage, timeoutBusy?: true): error is IngestUnavailable =>
  error instanceof IngestUnavailable && error.stage === stage &&
  (error.cause_code === "ledger_lock_timeout" || (timeoutBusy === true && isClaimStatementTimeout(error)));

/**
 * A buffered ledger stage (`buffer_intent` / `buffer_receipt`). Neither flag (the default): exactly the plain
 * stage. With `bufferLockRetry`, a lock-wait failure (`ledger_lock_timeout`) re-runs the SAME `work` once
 * after a jittered pause and writes one `ingest.buffer_lock_retry` warn line with the retry's outcome (each
 * attempt is also its own stage-latency line when INGEST_STAGE_LATENCY_LOG is on); with `bufferLockBusy`, a lock-wait
 * failure still standing is a 429 BufferLockBusy. Every other error propagates unchanged.
 */
async function bufferLedgerStage<T>(
  deps: Pick<DeliverDeps, "stageLatency" | "bufferLockBusy" | "bufferLockRetry" | "bufferTimeoutBusy" | "random" | "sleep" | "bufferLockRetryLog">,
  name: BufferLockStage,
  work: () => Promise<T>,
): Promise<T> {
  if (!deps.bufferLockBusy && !deps.bufferLockRetry) return runStage(name, work, deps.stageLatency);
  const random = deps.random ?? Math.random;
  let error: unknown;
  try {
    return await runStage(name, work, deps.stageLatency);
  } catch (first) {
    error = first;
  }
  if (deps.bufferLockRetry && isBufferLockWait(error, name, deps.bufferTimeoutBusy)) {
    const first = sqlstateOf(error);
    await (deps.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))))(bufferLockRetryPauseMs(random));
    try {
      const value = await runStage(name, work, deps.stageLatency);
      bufferLockRetryLine(deps, { route: "ingest.buffer_lock_retry", stage: name, ...(first ? { db_error_code: first } : {}), outcome: "ok" });
      return value;
    } catch (second) {
      error = second;
      const code = error instanceof IngestUnavailable ? error.cause_code : error instanceof DeliveryError ? error.code : undefined;
      const sqlstate = sqlstateOf(error);
      bufferLockRetryLine(deps, { route: "ingest.buffer_lock_retry", stage: name, ...(first ? { db_error_code: first } : {}), outcome: "failed",
        ...(code ? { error_code: code } : {}), ...(sqlstate ? { sqlstate } : {}) });
    }
  }
  if (deps.bufferLockBusy && isBufferLockWait(error, name, deps.bufferTimeoutBusy))
    throw new BufferLockBusy(name, error.cause_code, error.db, claimLockRetryAfter(random));
  throw error;
}

export async function deliverBatch(deps: DeliverDeps, tenant: LedgerTenant, batch: HistoryExportBatch): Promise<DeliverResult> {
  const started = Date.now();
  // 403 delivery_account_mismatch, 413 delivery_too_large, exactly as relayhistory.
  const prepared = await prepareDelivery({ orgId: tenant.orgId, workspaceId: tenant.workspaceId }, batch);
  return deliverPreparedCore(deps, tenant, prepared, started, "prepare");
}

/**
 * Lands an already-prepared relayhistory delivery through the same claim, fence,
 * stale-record, buffered-write and receipt path as a live public batch.
 *
 * This is intentionally a narrow internal boundary. Callers must authenticate
 * and validate the prepared object (including its original receipt and current
 * source custody) before invoking it; the public route continues through
 * `prepareDelivery` above.
 */
export async function deliverPrepared(deps: DeliverDeps, tenant: LedgerTenant, prepared: PreparedDelivery): Promise<DeliverResult> {
  return deliverPreparedCore(deps, tenant, prepared, Date.now(), "precheck");
}

async function deliverPreparedCore(deps: DeliverDeps, tenant: LedgerTenant, prepared: PreparedDelivery, started: number, firstTiming: "prepare" | "precheck"): Promise<DeliverResult> {
  const stage = stagesFor(deps);
  const now = deps.now ?? (() => new Date());
  const timing: Record<string, number> = {};
  let t = started;
  const mark = (name: string) => {
    timing[name] = Date.now() - t;
    t = Date.now();
  };
  await stage("precheck", async () => assertTenantSegment(deps.target.keyRoot, tenant.orgId));
  // Babysitter retention budget, before the claim: an over-budget routed batch writes nothing.
  assertWithinBudget(deps, prepared.records, now());
  mark(firstTiming);

  const sessions = [...new Map(prepared.records.filter((r) => r.session_id).map((r) => [pair(r.source, r.session_id), [r.source, r.session_id!] as [string, string]])).values()];
  const claim = await claimStage(deps, () =>
    deps.ledger.claim({
      tenant,
      ...(deps.custody?{custody:deps.custody}:{}),
      originId: prepared.originId,
      batchId: prepared.batchId,
      batchDigest: prepared.batchDigest,
      receiptId: prepared.receipt.receiptId,
      sessions,
      records: prepared.records.map((r) => ({ source: r.source, sessionId: r.session_id, recordId: r.record_id, revisionId: r.revision_id, revision: r.revision, digest: r.digest })),
    }),
  );
  mark("claim");
  const base = { records: prepared.records.length, sessions: sessions.length, deleted_sessions: 0, stale: 0, manifests_written: 0, replica: "off" as ReplicaOutcome, rearmed: 0 };
  if (claim.kind === "replay") {
    // Content-addressed: the same digest recomputes the same receipt. A mismatch is corruption, never a 200.
    if (claim.receiptId !== prepared.receipt.receiptId) throw new IngestUnavailable("replay", "receipt_mismatch");
    return { custodyAck:claim.custodyAck,receipt: withLimits(prepared, deps), outcome: "replay", stats: { ...base, timing_ms: timing } };
  }
  if (claim.kind === "batch_conflict") throw batchIdConflict(prepared, claim.currentDigest);
  if (claim.kind === "revision_conflict") throw revisionConflict(prepared.originId, claim.conflicts, claim.count);
  if (claim.kind === "replay_fast")
    return { custodyAck:claim.custodyAck,receipt: withLimits(prepared, deps), outcome: "replay_fast", stats: { ...base, deleted_sessions: claim.deleted.length, stale: claim.stale.length, timing_ms: timing } };

  if (deps.admissionGate) { await stage("admission_gate", deps.admissionGate); mark("admission_gate"); }
  const dropped: ReadonlySet<string> = new Set(claim.deleted.map(([s, id]) => sessionKey(s, id)));
  if (deps.consentGate) await consentStage(deps, dropped, "admit");
  // The deletion filter (records of sessions the user deleted from this origin are
  // acknowledged, not stored) and the replay fast path (records already stored at or
  // above their revision are acknowledged, not written again).
  const deleted = new Set(claim.deleted.map(([s, id]) => pair(s, id)));
  const stale = new Set(claim.stale);
  const keep: number[] = [];
  prepared.records.forEach((r, i) => {
    if (!stale.has(i) && !(r.session_id && deleted.has(pair(r.source, r.session_id)))) keep.push(i);
  });
  const write = () => writeAndCommit(deps, tenant, prepared, sessions, deleted, keep, claim.resets, claim.claimToken, { ...base, stale: stale.size }, timing, mark, now);
  const work = () => deps.withSessionWrites && keep.length
    ? deps.withSessionWrites(tenant, groupRecords(keep.map(i => prepared.records[i]!), now()).groups.map(g => [g.source, g.sessionId] as [string, string]), write)
    : write();
  if (!deps.slots) return work();
  const holder = (deps.requestId ?? (() => crypto.randomUUID()))();
  const slot = await stage("slot", () => deps.ledger.acquireSlot({ tenant, holder, ...deps.slots! }));
  // The claim stays pending: the retry resumes it.
  if (!slot.ok) throw new DeliveryBusyError("tenant_slots", slot.retryAfterSeconds);
  try {
    return await work();
  } finally {
    // A lost release expires by TTL.
    await deps.ledger.releaseSlot(tenant, holder).catch(() => {});
  }
}

async function writeAndCommit(
  deps: DeliverDeps,
  tenant: LedgerTenant,
  prepared: PreparedDelivery,
  sessions: [string, string][],
  deleted: Set<string>,
  keep: number[],
  resets: number,
  claimToken: string,
  base: Omit<DeliverResult["stats"], "timing_ms">,
  timing: Record<string, number>,
  mark: (name: string) => void,
  now: () => Date,
): Promise<DeliverResult> {
  const stage = stagesFor(deps);
  const machineId = liveMachineId(tenant.workspaceId, prepared.originId, tenant.userId);
  let written: WriteResult | null = null;
  let staged = false;
  let replicaOutcome: ReplicaOutcome = "off";
  const receivedAt = now();
  const kept = keep.map((i) => prepared.records[i]!);
  // Babysitter retention (babysitter-retention.ts), the same in every acceptance mode:
  // register the buckets, write the full evidence to them, then let the record travel
  // today's path as a payload-free stub in its OWN session. No mixed object (session
  // chunk, staged batch, buffered stage, canonical generation) ever holds the payload.
  const plan = babysitterRoutingActive(deps) ? planBabysitterBuckets(kept, receivedAt.getTime()) : null;
  let refs: BucketRefs | null = null;
  if (plan && plan.entries.length > 0) {
    if (!deps.ledger.registerBabysitterRetention) throw new IngestUnavailable("babysitter_retention", "retention_not_configured");
    // Registered BEFORE the bucket PUT, so no bucket exists without its row. SQL issues the refs,
    // bound to this claim's fence/reset epochs and the current generation (40001/55000 otherwise).
    // Register records each bucket object's write obligation (one dispatched invocation, this
    // claim) BEFORE the PUT (write-obligation.ts), so a lost response is never "nothing written".
    const bodies = bucketBodies(plan, receivedAt);
    const registered = await stage("babysitter_retention", () =>
      deps.ledger.registerBabysitterRetention!({ tenant, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest,
        claimToken, generation: deps.retentionGeneration!, resetEpoch: resets,
        buckets: plan.requests, records: plan.entries }),
    );
    refs = await stage("babysitter_retention", async () => bucketRefs(plan, registered.objects));
    mark("babysitter_retention");
    // One create-only object per bucket (idempotent by batch digest): never collapsed. Each PUT
    // runs under an enforced wall deadline; its definite outcome (acked with its own version id,
    // or refused) is recorded on its invocation; an unknown one stays dispatched and fails the
    // request (the client retries; the sweeper never treats it as nothing written).
    if (plan.requests.length > 0) {
      const wrote = await stage("babysitter_buckets", () =>
        writeBucketObjects(deps.target, { orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId, originId: prepared.originId, batchDigest: prepared.batchDigest }, plan, refs!, bodies),
      );
      if (wrote.outcomes.length > 0 && deps.ledger.babysitterRetentionWritten)
        await stage("babysitter_buckets", async () => { await deps.ledger.babysitterRetentionWritten!({ tenant, originId: prepared.originId, generation: deps.retentionGeneration!, claimToken, objects: wrote.outcomes }); });
      if (wrote.unknown !== null) await stage("babysitter_buckets", async () => { throw wrote.unknown; });
    }
    mark("babysitter_buckets");
  }
  const records = plan && refs ? kept.map((r) => { const b = plan.of(r); return b ? babysitterStub(r, refs!.get(planKey(b))!) : r; }) : kept;
  const grouped = keep.length > 0 ? groupRecords(records, receivedAt) : null;
  if (deps.acceptanceMode === "buffered") {
    if (!grouped || !deps.ledger.prepareBufferStage || !deps.ledger.commitBuffered || deps.replica?.mode && deps.replica.mode !== "off")
      throw new IngestUnavailable("buffer", "buffer_not_configured");
    const attempt = {tenant, originId:prepared.originId, batchId:prepared.batchId, batchDigest:prepared.batchDigest, claimToken};
    const preparedStage = await stage("buffer_prepare", async () => prepareBufferedStage(deps.target.keyRoot,
      {claim_token:claimToken,batch_digest:prepared.batchDigest,received_at:receivedAt.toISOString(),org_id:tenant.orgId,workspace_id:tenant.workspaceId,origin_id:prepared.originId,user_id:tenant.userId},
      grouped.groups, grouped.index.map(indexes => indexes.map(i => keep[i]!))));
    const intent = await bufferLedgerStage(deps, "buffer_intent", () => deps.ledger.prepareBufferStage!({...attempt,...preparedStage.spec}));
    mark("buffer_intent");
    await stage("buffer_put", () => putBufferedStage(deps.target.store,preparedStage));
    mark("buffer_put");
    if (deps.consentGate) await consentStage(deps, new Set(deleted), "commit");
    const ack=await bufferLedgerStage(deps, "buffer_receipt", () => deps.ledger.commitBuffered!({...attempt,stageToken:intent.stageToken}));
    let custodyAck:CurrentCustodyAck|undefined;
    if(deps.custody){try{custodyAck=validatedCustodyAck(ack,deps.custody)}catch{throw new IngestUnavailable("buffer_receipt","custody_ack_missing")}}
    mark("buffer_receipt");
    return {custodyAck,receipt:withLimits(prepared, deps),outcome:"accepted_buffered",stats:{...base,deleted_sessions:deleted.size,timing_ms:timing}};
  }
  if (grouped && deps.inlineMaxSessions !== undefined && grouped.groups.length > deps.inlineMaxSessions) {
    // Too wide for this invocation: one staged object now, the sessions from the queue.
    if (!deps.fanout) throw new IngestUnavailable("staging", "fanout_not_configured");
    const header = { claim_token: claimToken, batch_digest: prepared.batchDigest, received_at: receivedAt.toISOString(), org_id: tenant.orgId, workspace_id: tenant.workspaceId, origin_id: prepared.originId, user_id: tenant.userId };
    const key = await stage("staging", () => stageGroups(deps.target, header, grouped.groups));
    await stage("fanout", () => deps.fanout!(fanoutMessages(key, grouped.groups.length)));
    staged = true;
    mark("staging");
    if (deps.replica && deps.replica.mode !== "off") {
      const message = stagedReplicaMessage(tenant, prepared, { key, groups: grouped.groups.length, index: grouped.index }, resets, now(), keep);
      replicaOutcome = await stage("replica", () => sendReplica(deps.replica, message, prepared, batchMarkerKey(deps.target.keyRoot, tenant, prepared.originId, prepared.batchId)));
      mark("replica");
    }
  } else if (keep.length > 0) {
    written = await stage("objects", () =>
      writeBatchObjects(deps.target, {
        orgId: tenant.orgId,
        workspaceId: tenant.workspaceId,
        userId: tenant.userId,
        originId: prepared.originId,
        batchDigest: prepared.batchDigest,
        records,
      }),
    );
    mark("objects");
  }

  let replica: ReplicaOutcome = replicaOutcome;
  if (written && deps.replica && deps.replica.mode !== "off") {
    const message = replicaMessage(tenant, prepared, written, resets, now(), keep);
    replica = await stage("replica", () => sendReplica(deps.replica, message, prepared, batchMarkerKey(deps.target.keyRoot, tenant, prepared.originId, prepared.batchId)));
    mark("replica");
  }

  const writtenSessions = sessions.filter(([s, id]) => !deleted.has(pair(s, id)));
  if (deps.consentGate) await consentStage(deps, new Set(deleted), "commit");
  const committed = await stage("commit", () =>
    deps.ledger.commit({
      tenant,
      originId: prepared.originId,
      batchId: prepared.batchId,
      batchDigest: prepared.batchDigest,
      claimToken,
      sessions: writtenSessions,
      machineId,
      // A staged batch's revisions become stored only when the fan-out has written them
      // (staging.ts fanout -> commitRevisions); committing them here would make a
      // dead-lettered slice's records stale forever.
      records: staged ? [] : keep.map((i) => prepared.records[i]!).map((r) => ({ source: r.source, sessionId: r.session_id, recordId: r.record_id, revisionId: r.revision_id, revision: r.revision, digest: r.digest })),
    }),
  );
  mark("commit");
  return {
    receipt: withLimits(prepared, deps),
    outcome: staged ? "accepted_staged" : written ? "accepted" : "accepted_all_deleted",
    stats: {
      ...base,
      deleted_sessions: deleted.size,
      manifests_written: written?.manifests.filter((m) => m.written).length ?? 0,
      replica,
      rearmed: committed.rearmed,
      timing_ms: timing,
    },
  };
}
