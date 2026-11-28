/**
 * One SQS batch of S3 object events: pointer first (Supabase), then the
 * search index (ClickHouse) once per session scope the batch touched.
 *
 * At-least-once and idempotent: every step re-reads S3 and is fenced or
 * diffed, so a redelivered or reordered message converges to the same state.
 * Within a batch, events for one key collapse to the latest (S3 sequencer)
 * and events for one session collapse to one search-index pass. A failure
 * fails exactly the messages that fed the failed key or scope (SQS partial
 * batch response); SQS redelivers them after the visibility timeout and moves
 * them to the DLQ after `maxReceiveCount`.
 */
import { errorCodeFields, errorFields, log, searchIndexErrorDetail } from "../shared/log.ts";
import { dbErrorReasonField } from "../shared/db-error-reason.ts";
import { parseQueueBody, sequencerAfter, type LandedEvent, type LargeReadHoldBody, type QueueOrigin } from "./s3-events.ts";
import { indexPointer, type PointerDeps, type PointerOutcome, type SessionScope } from "./pointer.ts";
import { coverageProven, indexSearchScope, searchIndexPhaseMetrics, type SearchIndexDeps } from "./search-index.ts";
import { LEGACY_PROVENANCE_INVALID } from "../session-read-api/legacy-provenance.ts";
import { streamDeadlinePassed } from "../session-read-api/canonical-stream-projection.ts";
import { captureRefusal, classifyReplayFailure, produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { LargeJobState, LargeRoute } from "./large-read-worker.ts";
import { DEADLINE_OVER_BOUND, DEADLINE_ROUTE, LARGE_READ_UNCLAIMED_MS, decideHold, deadlineMarked, holdId, sameSource, hitStreamDeadline, markDeadline, type DeadlineIsolationDeps, type HoldDecision } from "./search-deadline-isolation.ts";
import { revisionDedupId } from "./revision-events.ts";
import { bufferedFeedQueryField, feedBufferedStageByKey, isBufferedStageKey, type BufferedFeedDeps } from "./buffered-search-feed.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
  /** The SQS `SentTimestamp` attribute (epoch ms, a decimal string in the Lambda event): telemetry only. */
  sentTimestamp?: unknown;
  /**
   * The SQS receipt handle: passed through only while `ConsumerDeps.retryVisibility` is set (lambda.ts
   * queueRecords), and used only as the ChangeMessageVisibility argument. Never logged.
   */
  receiptHandle?: string;
  /** FIFO only: the record's MessageGroupId (revision-events.ts revisionGroupId). */
  groupId?: string;
  /** SQS SentTimestamp (ms since epoch): when the message was enqueued. */
  sentAt?: number;
  /** SQS ApproximateReceiveCount. */
  receiveCount?: number;
}

/** Pointer-stage error codes whose messages are retried after `retryVisibility.seconds`, not the queue's full timeout. */
export const FAST_RETRY_CODES: ReadonlySet<string> = new Set(["pointer_move_pending"]);

/** A ChangeMessageVisibility call that has not settled after this long is abandoned (the message keeps its full timeout). */
export const RETRY_VISIBILITY_TIMEOUT_MS = 2_000;

/**
 * Time kept back for returning the batch response after the calls. With less than
 * RETRY_VISIBILITY_TIMEOUT_MS + this left in the invocation no call is made, so a near-deadline batch still
 * returns its partial response (and acknowledges its successes) before Lambda times out.
 */
export const RETRY_VISIBILITY_MARGIN_MS = 1_000;

export interface BatchResponse {
  batchItemFailures: { itemIdentifier: string }[];
}

export interface ConsumerDeps {
  pointer: PointerDeps;
  /** Absent while the search index is switched off: only pointers are written. */
  search?: SearchIndexDeps;
  /** Explicitly absent by default; independent Node artifact publication stage. */
  replay?: ReplayProducerDeps;
  /**
   * Absent by default (SESSION_LARGE_READ_HANDOFF_ENABLED). `delegated: true` means durable
   * canonical-epoch handoff; `jobState` reads the job behind a delegated route, once per routed scope.
   */
  large?: { route: (scope: SessionScope) => Promise<LargeRoute>; jobState?: (scope: SessionScope) => Promise<LargeJobState | null> };
  /**
   * Absent by default (SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true; search-deadline-isolation.ts): a scope whose
   * S3-event pass hit the stream deadline is marked and sent to the large-read job, and while its mark lives
   * its later passes go there too instead of streaming inline again. Absent: exactly the previous behavior.
   */
  deadlineIsolation?: DeadlineIsolationDeps;
  /** Tests only: the S3-event search pass (default indexSearchScope). */
  searchPass?: typeof indexSearchScope;
  /**
   * Put a manifest back on the queue after a delay (SQS DelaySeconds): used when
   * another pass holds the session, so the event is neither lost nor failed.
   */
  requeue?: (manifestKey: string, oldestEventTime?: string, hold?: LargeReadHoldBody) => Promise<void>;
  /** Clock for `event_age_ms` and `s3_event_age_ms` only (default Date.now). */
  now?: () => number;
  /**
   * Absent by default (INGEST_BUFFERED_SEARCH_FEED=on only): a buffered-stage object event
   * (`…/buffered-batches/v1/…`) feeds that stage into the search index instead of reaching the
   * pointer stage, which ignores such keys anyway. Absent: exactly the previous behavior.
   */
  bufferedFeed?: BufferedFeedDeps;
  /**
   * Absent by default (SEARCH_JOBS_ENABLED): after the batch, the follow-on claim of open `search-index` jobs
   * (docs/handoffs/incremental-search-indexing.md §5.1; lambda.ts drainSearchJobs). Its failures are logged and
   * never fail a message of the batch. Absent: exactly the previous behavior.
   */
  searchJobs?: { drain(): Promise<unknown> };
  /**
   * Absent by default (SESSION_REVISION_QUEUE_URL): the batch came from the per-session FIFO queue
   * (revision-events.ts). A FIFO queue has no per-message delay and must not reorder a group, so
   * work that would be requeued (busy, stream deferred, pointer liveness) instead fails the
   * group's messages after `retryLater` shortens their visibility. SQS then redelivers them, in
   * order and ahead of the group's later arrivals, with which they coalesce. Any failure in a group
   * also fails every later message of that group in the batch (FIFO partial-batch rule).
   */
  ordered?: { retryLater(messageIds: readonly string[], seconds: number): Promise<void>; busyRetrySeconds: number; now?: () => number };
  /** Test seam only: replaces coverageProven (the durable proof for the superseded-only skip). Absent in the runtimes. */
  searchCoverage?: (scope: SessionScope) => Promise<boolean>;
  /** Test seam only: replaces indexSearchScope for the search stage. Absent (always, in the runtimes): indexSearchScope. */
  searchScope?: typeof indexSearchScope;
  /**
   * SESSION_PIPELINE_RETRY_VISIBILITY_S (default off; absent, failures behave and log exactly as before): a
   * message that failed only with a FAST_RETRY_CODES pointer error gets its visibility set to `seconds` before
   * it is returned in batchItemFailures, so SQS redelivers it then instead of after the queue's full
   * visibility timeout. A message that also fed any other failure of the batch (another key, an invalid body,
   * a buffered stage, the large-read route, search or replay) keeps the full timeout. Best-effort: a rejected
   * or timed-out call logs a warn and the message still fails with the normal timeout. `remainingMs` (the
   * invocation's remaining time, set per invocation by lambda.ts invocationDeps) is checked right before the
   * calls: too little left and none is made; otherwise each is bounded by
   * min(RETRY_VISIBILITY_TIMEOUT_MS, remaining - RETRY_VISIBILITY_MARGIN_MS). Needs
   * sqs:ChangeMessageVisibility on the source queue for the Lambda role.
   */
  retryVisibility?: {
    seconds: number;
    change: (receiptHandle: string, seconds: number, timeoutMs: number) => Promise<void>;
    remainingMs?: () => number;
  };
}

/** Set each message's visibility to `seconds`, bounded and best-effort: never throws, never changes which messages fail. */
async function shortenRetries(handles: readonly (string | undefined)[], rv: NonNullable<ConsumerDeps["retryVisibility"]>): Promise<void> {
  // Rechecked here, after every stage of the batch: it may have used most of the invocation.
  const remaining = rv.remainingMs?.();
  let bound = RETRY_VISIBILITY_TIMEOUT_MS;
  if (remaining !== undefined && Number.isFinite(remaining)) {
    if (remaining < RETRY_VISIBILITY_TIMEOUT_MS + RETRY_VISIBILITY_MARGIN_MS) {
      log.warn("retry visibility not shortened", { stage: "session-indexed", reason: "retry_visibility_deadline", count: handles.length });
      return;
    }
    bound = Math.min(RETRY_VISIBILITY_TIMEOUT_MS, remaining - RETRY_VISIBILITY_MARGIN_MS);
  }
  await Promise.allSettled(
    handles.map(async (handle) => {
      if (!handle) {
        log.warn("retry visibility not shortened", { stage: "session-indexed", reason: "no_receipt_handle" });
        return;
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<"timeout">((resolve) => (timer = setTimeout(() => resolve("timeout"), bound)));
      try {
        const settled = await Promise.race([rv.change(handle, rv.seconds, bound).then(() => "ok" as const), timeout]);
        if (settled === "timeout") log.warn("retry visibility not shortened", { stage: "session-indexed", reason: "change_visibility_timeout" });
      } catch (error) {
        log.warn("retry visibility not shortened", { stage: "session-indexed", reason: "change_visibility_failed", ...errorFields(error) });
      } finally {
        clearTimeout(timer);
      }
    }),
  );
}

/*
 * Latency fields (the backport lineage's ecb5d981) on "pointer indexed" (per key), "search index reconciled" and
 * "search index partial" (per scope), each over the SQS messages behind the line:
 * - `s3_event_age_ms`: floor(log time - the oldest upload event time covered), clamped at 0: an S3 ObjectCreated
 *   record's eventTime, or the `oldest_event_time` a requeued body carries, so it stays end to end across busy,
 *   deferred and batch-deadline requeues. Deletes are not counted. Omitted when any covered upload has no usable time.
 * - `event_age_ms`: floor(log time - the oldest SQS SentTimestamp of the messages), clamped at 0; omitted when any
 *   message has none.
 * - `queue_origin`: `s3` when every message is an S3 notification, else `requeue` when any is this consumer's
 *   requeue, else `sweep`.
 * Telemetry only: no stage sees them, and no decision depends on them.
 */
/** The upload event times a key or scope covers: the oldest known, and whether any was unknown. */
interface Covered {
  oldestMs?: number;
  unknown: boolean;
  /** Any message that is not a deadline-isolation hold (an upload, a delete, a sweep or another requeue). */
  plain?: boolean;
  /** The deadline-isolation holds among the messages. */
  holds?: LargeReadHoldBody[];
}

function cover(into: Covered, event: LandedEvent): void {
  if (event.hold) (into.holds ??= []).push(event.hold);
  else into.plain = true;
  if (event.deleted) return;
  const at = event.oldestEventTime ?? event.eventTime;
  const ms = at === undefined ? NaN : Date.parse(at);
  if (!Number.isFinite(ms)) into.unknown = true;
  else if (into.oldestMs === undefined || ms < into.oldestMs) into.oldestMs = ms;
}

function merge(into: Covered, from: Covered): void {
  if (from.unknown) into.unknown = true;
  if (from.plain) into.plain = true;
  if (from.holds?.length) (into.holds ??= []).push(...from.holds);
  if (from.oldestMs !== undefined && (into.oldestMs === undefined || from.oldestMs < into.oldestMs)) into.oldestMs = from.oldestMs;
}

const knownOldest = (c: Covered): number | undefined => (c.unknown ? undefined : c.oldestMs);

function ageMs(now: number, since: number): number | undefined {
  const age = Math.floor(now - since);
  return Number.isFinite(age) ? Math.max(0, age) : undefined;
}

interface Message {
  sentMs?: number;
  origin: QueueOrigin;
}

/** A `SentTimestamp` attribute as epoch ms: a positive safe integer (number or decimal string), else undefined. */
export function sentTimestampMs(value: unknown): number | undefined {
  const n = typeof value === "number" ? value : typeof value === "string" && /^[0-9]{1,16}$/.test(value) ? Number(value) : NaN;
  return Number.isSafeInteger(n) && n > 0 ? n : undefined;
}

interface LagFields {
  event_age_ms?: number;
  s3_event_age_ms?: number;
  queue_origin?: QueueOrigin;
}

function lagFields(covered: Covered, ids: readonly string[], messages: ReadonlyMap<string, Message>, now: number): LagFields {
  const out: LagFields = {};
  let oldestSent: number | undefined;
  let sentUnknown = false;
  let seen = false;
  let allS3 = true;
  let anyRequeue = false;
  for (const id of ids) {
    const m = messages.get(id);
    if (!m) continue;
    seen = true;
    if (m.sentMs === undefined) sentUnknown = true;
    else if (oldestSent === undefined || m.sentMs < oldestSent) oldestSent = m.sentMs;
    if (m.origin !== "s3") allS3 = false;
    if (m.origin === "requeue") anyRequeue = true;
  }
  if (seen && !sentUnknown && oldestSent !== undefined) {
    const age = ageMs(now, oldestSent);
    if (age !== undefined) out.event_age_ms = age;
  }
  const oldest = knownOldest(covered);
  if (oldest !== undefined) {
    const age = ageMs(now, oldest);
    if (age !== undefined) out.s3_event_age_ms = age;
  }
  if (seen) out.queue_origin = allS3 ? "s3" : anyRequeue ? "requeue" : "sweep";
  return out;
}

/** The carried time for a requeue body, or undefined. */
function carriedTime(c: Covered): string | undefined {
  const oldest = knownOldest(c);
  return oldest === undefined ? undefined : new Date(oldest).toISOString();
}

/** FIFO busy retry default: short, because the group is blocked meanwhile and nothing else can run it. */
export const ORDERED_BUSY_RETRY_SECONDS = 5;
export const ORDERED_RETRY_CAP_SECONDS = 300;
export const orderedRetrySeconds = (base: number, receiveCount = 1) =>
  Math.min(ORDERED_RETRY_CAP_SECONDS, base * 2 ** Math.min(16, Math.max(0, receiveCount - 1)));

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

/**
 * Enqueue a deadline-isolated scope's large-read job and decide what this delivery does:
 * - "inline": the source is unavailable to the job (erased, blocked, no live pointer), or (with `progress`) its own job
 *   failed, is missing, or was not claimed within LARGE_READ_UNCLAIMED_MS of the oldest upload the message carries:
 *   the caller runs (or has run) the normal pass;
 * - "done": the job holds the scope (no `progress`), or is complete for the incarnation the enqueue captured, or has
 *   been held LARGE_READ_HOLD_MAX_MS while running with a live lease: ack;
 * - "held": the message was requeued as a new SQS message (`requeue`, before this one is acked) to look again later.
 * Errors propagate (enqueue, requeue): the caller fails the scope's messages, so no delivery is acked without a successor.
 * A failed read of the job row is unknown progress: held within LARGE_READ_UNCLAIMED_MS, then inline (never released).
 */
async function routeDeadlineScope(
  isolation: DeadlineIsolationDeps,
  scope: SessionScope,
  trigger: "first_hit" | "marked",
  phase: Record<string, unknown>,
  started: number,
  anchorMs: number,
  requeue: ((hold?: LargeReadHoldBody) => Promise<void>) | undefined,
  incoming: { holdOnly: boolean; holds: readonly LargeReadHoldBody[] } = { holdOnly: false, holds: [] },
): Promise<"inline" | "done" | "held"> {
  const ids = { org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId };
  const { enqueued, identity, source } = await isolation.enqueue(scope);
  if (!enqueued) {
    log.info("search index over-bound, large-read unavailable", { stage: "search-index", ...ids, reason: DEADLINE_OVER_BOUND, outcome: trigger, route: "inline" });
    return "inline";
  }
  // A delivery made only of holds whose incarnation was replaced between the obsolete check and this enqueue: the
  // enqueue captured the newer incarnation (an upsert identical to that incarnation's own routing, and a no-op on its
  // row when it is already enqueued). Its job row now exists, so the newer work is covered: ack, no mark, no hold.
  if (incoming.holdOnly && isolation.current && !incoming.holds.some((h) => sameSource(h, source))) {
    log.info("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "enqueue_race" });
    return "done";
  }
  const routedLine = (state: string) => log.info("search index over-bound, routed to large-read", {
    stage: "search-index", ...ids, reason: DEADLINE_OVER_BOUND, outcome: trigger, route: DEADLINE_ROUTE, state,
    ...(trigger === "marked" ? { ...phase, duration_ms: Date.now() - started } : {}),
  });
  if (!isolation.progress) {
    routedLine("routed");
    if (isolation.jobState) {
      try {
        const job = await isolation.jobState(identity ?? scope);
        if (!job || job.state === "failed")
          log.warn("large scope job will not be processed", { stage: "large-read", ...ids, state: job?.state ?? "missing", attempt: job?.attempts, reason: job ? "large_job_failed" : "large_job_missing" });
      } catch (error) {
        log.warn("large scope job state unavailable", { stage: "large-read", ...ids, ...errorFields(error) });
      }
    }
    return "done";
  }
  const age = Math.max(0, (isolation.now ?? Date.now)() - anchorMs);
  // A failed read of the job row is unknown progress: held only within LARGE_READ_UNCLAIMED_MS, then inline (decideHold).
  let row: Awaited<ReturnType<NonNullable<DeadlineIsolationDeps["progress"]>>> | undefined;
  try {
    row = await isolation.progress(identity ?? scope);
  } catch (error) {
    log.warn("large scope job state unavailable", { stage: "large-read", ...ids, ...errorFields(error) });
    row = undefined;
  }
  const decision: HoldDecision = decideHold(row, source, age);
  if (decision.kind === "inline") {
    log.warn("large-read job unclaimed", { stage: "large-read", ...ids, reason: `large_job_${decision.why}`, age_ms: age, route: "inline" });
    return "inline";
  }
  if (decision.kind === "done") { routedLine("complete"); return "done"; }
  if (decision.kind === "release") {
    log.warn("large-read hold released", { stage: "large-read", ...ids, reason: "large_hold_max", state: "running", age_ms: age });
    return "done";
  }
  if (!requeue) throw new Error("large-read hold and no requeue");
  // The next hold waits for the incarnation this enqueue captured; its sequence follows the highest incoming hold for
  // that incarnation, so a retry of this delivery mints the same hold id. No captured incarnation: a plain requeue.
  const prior = incoming.holds.filter((h) => sameSource(h, source)).reduce((m, h) => Math.max(m, h.seq), 0);
  const holdScope = identity ?? scope;
  await requeue(source ? { epoch: source.epoch, fingerprint: source.fingerprint, seq: prior + 1, id: holdId(holdScope, source, prior + 1) } : undefined);
  routedLine(decision.why === "running" ? "held_running" : decision.why === "unknown" ? "held_unknown" : "held_queued");
  return "held";
}

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  // retryVisibility only: messages with a fast-retry pointer failure, and messages with any other failure.
  const fastRetry = new Set<string>();
  const slowRetry = new Set<string>();
  /** Fail `id`; with retryVisibility armed, also note whether this failure allows the short redelivery. */
  const fail = (id: string, fast = false) => {
    failed.add(id);
    if (deps.retryVisibility) (fast ? fastRetry : slowRetry).add(id);
  };
  const ordered = deps.ordered;
  const now = ordered?.now ?? deps.now ?? Date.now;
  const messages = new Map<string, Message>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[]; covered: Covered }>();
  /** Consumer pickup: the batch arrived, before any parse or pointer work (the 7s pickup budget ends here). */
  const pickedUpAt = now();
  const recordOf = new Map(records.map((r) => [r.messageId, r]));
  /** Every revision event behind a key, in arrival order, for the per-upload latency lines. */
  const revisionsByKey = new Map<string, { event: LandedEvent; messageId: string }[]>();
  /** Messages whose work is handed back to the FIFO queue with a short visibility (ordered mode). */
  const retry = new Set<string>();
  /**
   * Run this work again later. Ordered (FIFO): fail the group's messages with a short visibility (no requeue: it would
   * reorder the group). Standard: requeue, carrying the oldest upload event time (T124 telemetry).
   */
  const later = async (manifestKey: string, messageIds: readonly string[], why: string, carried?: string) => {
    if (ordered) {
      for (const id of messageIds) { retry.add(id); fail(id); }
      log.info("revision retry scheduled", { stage: "revision-events", reason: why, count: messageIds.length,
        delay_seconds: orderedRetrySeconds(ordered.busyRetrySeconds, Math.max(...messageIds.map((id) => recordOf.get(id)?.receiveCount ?? 1))) });
      return;
    }
    if (!deps.requeue) throw new Error(`${why} and no requeue`);
    await deps.requeue(manifestKey, carried);
  };
  for (const record of records) {
    // Events carry the provider of the Locker the pointer stage reads from (s3 on the
    // Lambda, LOCKER_PROVIDER on self-host), which is what the row's storage_provider records.
    const parsed = parseQueueBody(record.body, deps.pointer.reader.provider);
    if (parsed.kind === "test") continue;
    if (parsed.kind === "invalid") {
      // Never succeeds on retry; the DLQ keeps it for an operator.
      log.warn("object event unreadable", { stage: "object-landed", reason: "invalid_body" });
      fail(record.messageId);
      continue;
    }
    const sentMs = sentTimestampMs(record.sentTimestamp);
    messages.set(record.messageId, { origin: parsed.origin, ...(sentMs === undefined ? {} : { sentMs }) });
    for (const event of parsed.events) {
      if (event.revision) {
        const list = revisionsByKey.get(event.key) ?? [];
        list.push({ event, messageId: record.messageId });
        revisionsByKey.set(event.key, list);
      }
      const held = byKey.get(event.key);
      if (!held) {
        const covered: Covered = { unknown: false };
        cover(covered, event);
        byKey.set(event.key, { event, messageIds: [record.messageId], covered });
      } else {
        held.messageIds.push(record.messageId);
        cover(held.covered, event);
        // FIFO: arrival order is commit order within the group, so the later event wins (both are
        // hints; the live manifest is re-read either way).
        const newer = ordered && event.revision && !event.sequencer ? true : sequencerAfter(event.sequencer, held.event.sequencer);
        if (newer) held.event = event;
      }
    }
  }

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string; covered: Covered; revisions: Revised[]; live: LiveKey[] }>();
  for (const { event: carriedEvent, messageIds, covered } of byKey.values()) {
    const started = Date.now();
    /** This key's pointer work starts (consumer clock). */
    const pointerStartedAt = now();
    // The carried time is telemetry only: no stage sees it.
    const { oldestEventTime: _carried, hold: _hold, ...event } = carriedEvent;
    if (deps.bufferedFeed && isBufferedStageKey(event.key, deps.bufferedFeed.keyRoot)) {
      // KEEP-UP. A delete event is a GC removal: index-row deletes are their own lane, so it is a no-op here.
      if (event.deleted) continue;
      try {
        const fed = await feedBufferedStageByKey(event.key, deps.bufferedFeed);
        if (fed.outcome === "requeue") {
          // The PUT lands before the receipt commits (or a GC lease is in flight): look again later.
          await later(event.key, messageIds, "buffered stage pending");
        }
        // A same-second refusal is not coverage: warn (keep-up never writes the refusal ledger; the backfill does).
        if (fed.outcome === "skipped" && fed.reason === "same_second_tie")
          log.warn("buffered stage search feed refused", { stage: "buffered-search-feed", reason: fed.reason, count: fed.siblings });
        log.info("buffered stage search feed", { stage: "buffered-search-feed", outcome: fed.outcome,
          ...(fed.outcome === "indexed" ? { count: fed.rows, deleted: fed.deleted } : { reason: fed.reason }), duration_ms: Date.now() - started });
      } catch (error) {
        log.error("buffered stage search feed failed", { stage: "buffered-search-feed", ...errorFields(error), ...bufferedFeedQueryField(error), duration_ms: Date.now() - started });
        for (const id of messageIds) fail(id);
      }
      continue;
    }
    try {
      const outcome = await indexPointer(event, deps.pointer);
      if (outcome.status === "ignored") {
        log.info("object event ignored", { stage: "object-landed", reason: outcome.reason });
        continue;
      }
      // With pre-read enabled a delete can complete without changing a pointer row (no row, or the row
      // names another key), so the stamp alone cannot prove that a previously read put stayed live. Check
      // successful inserts/rekeys after their lock is released. A missing key is reconciled through the
      // normal queue path; without a requeue binding, fail this message so its source queue retries it.
      if (deps.pointer.preRead && (outcome.status === "indexed" || outcome.status === "moved") && !(await deps.pointer.exists(outcome.manifestKey))) {
        await later(outcome.manifestKey, messageIds, "pointer changed after pre-read");
        log.info("pointer liveness changed, requeued", { stage: "session-indexed", outcome: outcome.status, org_id: outcome.scope.orgId,
          workspace_id: outcome.scope.workspaceId, harness: outcome.scope.harness, session_id: outcome.scope.sessionId });
      }
      log.info("pointer indexed", {
        stage: "session-indexed",
        outcome: outcome.status,
        org_id: outcome.scope.orgId,
        workspace_id: outcome.scope.workspaceId,
        harness: outcome.scope.harness,
        session_id: outcome.scope.sessionId,
        revision: outcome.revision,
        duration_ms: Date.now() - started,
        ...lagFields(covered, messageIds, messages, now()),
      });
      const revisions = checkRevisions(revisionsByKey.get(event.key) ?? [], outcome, recordOf, now, { startedAt: pointerStartedAt, doneAt: now() });
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      // Every key the pointer stage just read, with the live ETag it read (absent for a delete): the physical-currency check.
      const liveKey: LiveKey = { key: event.key, ...(outcome.status !== "deleted" && outcome.live?.etag ? { etag: outcome.live.etag } : {}), deleted: outcome.status === "deleted" };
      if (held) {
        held.messageIds.push(...messageIds);
        held.revisions.push(...revisions);
        held.live.push(liveKey);
        merge(held.covered, covered);
      } else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey, revisions, live: [liveKey], covered: { ...covered, ...(covered.holds ? { holds: [...covered.holds] } : {}) } });
    } catch (error) {
      const { fields, code } = errorCodeFields(error);
      // A 40001 / XX000 (the error's own string code, never the name fallback) also logs which known failure it was
      // (a closed slug, never the message; best-effort, never throws).
      log.error("pointer index failed", { stage: "session-indexed", ...fields, ...dbErrorReasonField(error, code), duration_ms: Date.now() - started });
      for (const id of messageIds) fail(id, FAST_RETRY_CODES.has(fields.error_code as string));
    }
  }

  const delegated=new Set<string>();
  if(deps.large&&(deps.search||deps.replay))for(const {scope,messageIds} of scopes.values()){
    if(messageIds.every(id=>failed.has(id)))continue;
    const ids={org_id:scope.orgId,workspace_id:scope.workspaceId,harness:scope.harness,session_id:scope.sessionId};
    let routed:LargeRoute;
    try{routed=await deps.large.route(scope)}
    catch(error){
      // route_session_large_read captures first, so it raises the same refusals as the replay
      // stage (#179). A capacity/superseded refusal is not retryable here: log it and let search
      // and replay apply their own limits. A permanently ambiguous canonical source (55000 with
      // capture's exact message) is deterministic too: retrying only reaches the DLQ with search
      // skipped every time. Anything else (55000 durable handoff unavailable or source epoch
      // missing, 22023, connection errors) fails the batch and is retried.
      const refusal=classifyReplayFailure(error);
      if(refusal){log.warn("large scope handoff refused",{stage:"large-read",...ids,...errorFields(error),reason:refusal.reason,error_code:refusal.code});continue}
      if(captureRefusal(error)==="capture_ambiguous_source"){log.warn("large scope handoff refused",{stage:"large-read",...ids,...errorFields(error),reason:"large_handoff_ambiguous_source",error_code:"55000"});continue}
      log.error("large scope handoff failed",{stage:"large-read",...ids,...errorFields(error)});for(const id of messageIds)fail(id);continue;
    }
    if(!routed.delegated)continue;
    delegated.add(scopeId(scope));
    if(!deps.large.jobState)continue;
    // Search/replay are skipped for a delegated scope, so a job that will never run is a gap:
    // `failed` is never claimed again until the source changes. Make it visible.
    try{
      const job=await deps.large.jobState(routed.identity??scope);
      if(job&&job.state!=="failed")log.info("large scope delegated",{stage:"large-read",...ids,state:job.state,attempt:job.attempts});
      else log.warn("large scope job will not be processed",{stage:"large-read",...ids,state:job?.state??"missing",attempt:job?.attempts,reason:job?"large_job_failed":"large_job_missing"});
    }catch(error){log.warn("large scope job state unavailable",{stage:"large-read",...ids,...errorFields(error)})}
  }
  if (deps.search) {
    for (const { scope, messageIds, manifestKey, covered, revisions, live } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      // The covering search pass's start on the consumer clock (ordered.now in tests). NOT the pickup endpoint:
      // pickup is pickedUpAt (batch arrival, before any pointer work); this comes after pointer work and routing.
      const passStartedAt = now();
      // DURABLE proof is required, not inferred from the pointer: an 'unchanged' pointer can be this message's OWN
      // earlier attempt, which advanced the pointer and then crashed its search pass (Pipeline review of 16feaf75).
      // A failed proof read runs the pass.
      // The search stage's proof (alreadyCurrent) compares manifest keys and REVISION numbers only, so it can still match
      // an indexed PUT A after A was replaced by B at the same key and revision (B's notification lost; the revision
      // fence keeps the pointer row at A). So first a PHYSICAL check against the real pointer rows: for every key of
      // this batch, the row's stored manifest_etag must be the live object the pointer pass just read (Pipeline
      // review of b41e49bf). Otherwise no proof: run the pass, which reads the live object.
      // Computed once: it gates the outer skip below AND the search stage's own skip-current (Pipeline review of
      // 1cb42716). alreadyCurrent compares keys and revision numbers only, so after a same-key, same-revision
      // replacement (B's notification lost, the pointer row still at A) it would credit a delayed A as current and
      // skip before reading B. Without physical currency the pass runs with skip-current OFF and reads the live object.
      // Both queue paths whenever skip-current is on (standard-path parity, Skip 252405604327505920); ordered mode also
      // for its own superseded-only skip. Flag off on the standard path: not computed, no extra statement.
      const physicallyCurrent = (ordered && revisions.length > 0) || deps.search.skipCurrent
        ? await pointerRowsAreLive(deps.pointer.db, scope, live).catch(() => false)
        : undefined;
      if (ordered && supersededOnly(messageIds, revisions) && physicallyCurrent === true &&
          await (deps.searchCoverage ?? ((s: SessionScope) => coverageProven(s, deps.search!)))(scope).catch(() => false)) {
        // Every message is a stamped revision whose write is no longer live. The newer live write has its
        // own message in this group: already processed (FIFO holds later messages until earlier ones are
        // deleted), in this batch, or still to come. So this pass would only repeat it. Independent of
        // SEARCH_INDEX_SKIP_CURRENT_ENABLED: no search state is read.
        log.info("search index skipped", {
          stage: "search-index", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId,
          reason: "superseded_in_group", ...passFields(messageIds, recordOf, now), duration_ms: Date.now() - started,
        });
        continue;
      }
      const phase = searchIndexPhaseMetrics();
      const phaseFields = () => ({
        source_read_ms: phase.sourceReadMs,
        index_scan_ms: phase.indexScanMs,
        locked_apply_ms: phase.lockedApplyMs,
        write_calls: phase.writeCalls,
        write_batches: phase.writeBatches,
      });
      try {
        // SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true only: a scope marked by an earlier stream_deadline pass goes to
        // the large-read job before any read or lock (one bounded ClickHouse read, one autocommit enqueue). A source
        // the enqueue reports unavailable (erased, blocked, no live pointer) runs the normal pass below.
        // The hold's clock starts at the oldest upload the message carries (kept across requeues), else the mark.
        let obsoleteInline = false;
        const holdRequeue = deps.requeue ? (hold?: LargeReadHoldBody) => deps.requeue!(manifestKey, carriedTime(covered), hold) : undefined;
        const incoming = { holdOnly: !covered.plain && (covered.holds?.length ?? 0) > 0, holds: covered.holds ?? [] };
        // SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED, a delivery made only of holds: before any enqueue, mark or hold, one
        // STABLE read of the current incarnation. None of the holds' incarnations current: the hold is obsolete. It never
        // enqueues, marks or holds for the newer incarnation: acked once that incarnation's job row exists (its own
        // routing covers it); held once more (same expectation) while it does not, within LARGE_READ_UNCLAIMED_MS; past
        // that, the normal inline pass runs with no enqueue, mark or hold. A failed read runs the checks below.
        if (deps.deadlineIsolation?.current && incoming.holdOnly) {
          let now: Awaited<ReturnType<NonNullable<DeadlineIsolationDeps["current"]>>> | undefined;
          try { now = await deps.deadlineIsolation.current(scope); } catch (error) {
            log.warn("large scope source unavailable", { stage: "large-read", org_id: scope.orgId, session_id: scope.sessionId, ...errorFields(error) });
          }
          if (now && !incoming.holds.some((h) => sameSource(h, now!.source))) {
            const ids = { org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId };
            let covers = false;
            try {
              const row = deps.deadlineIsolation.progress ? await deps.deadlineIsolation.progress(now.identity) : null;
              covers = !!row && row.state !== "failed" && sameSource(row.source, now.source);
            } catch (error) {
              log.warn("large scope job state unavailable", { stage: "large-read", ...ids, ...errorFields(error) });
            }
            const anchor = knownOldest(covered) ?? started;
            const age = Math.max(0, (deps.deadlineIsolation.now ?? Date.now)() - anchor);
            if (covers) {
              log.info("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "covered" });
              continue;
            }
            if (age <= LARGE_READ_UNCLAIMED_MS && holdRequeue) {
              // Same expectation, next sequence: the check repeats without touching the newer incarnation.
              const top = incoming.holds.reduce((a, h) => (h.seq > a.seq ? h : a));
              await holdRequeue({ epoch: top.epoch, fingerprint: top.fingerprint, seq: top.seq + 1, id: holdId(now.identity, top, top.seq + 1) });
              log.info("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "awaiting_newer", age_ms: age });
              continue;
            }
            log.warn("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "inline", age_ms: age });
            obsoleteInline = true;
          }
        }
        const mark = deps.deadlineIsolation && !obsoleteInline ? await deadlineMarked(deps.deadlineIsolation, scope) : null;
        if (deps.deadlineIsolation && mark) {
          const routed = await routeDeadlineScope(deps.deadlineIsolation, scope, "marked", phaseFields(), started, knownOldest(covered) ?? mark.atMs, holdRequeue, incoming);
          if (routed !== "inline") continue;
        }
        // SEARCH_INDEX_STREAMING_CH=1 only (the backport lineage's 6264d119 / 51f876b6): past the invocation's
        // stream deadline no scope of the batch is started (whole read or streamed); the rest are requeued. A
        // requeue that fails fails that scope's messages for redelivery (the catch below), never silently.
        if (deps.search.stream && streamDeadlinePassed(deps.search.streamDeadline)) {
          await later(manifestKey, messageIds, "batch past its stream deadline", carriedTime(covered));
          log.info("search index past the batch deadline, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_batch_deadline" });
          continue;
        }
        const result = await (deps.searchPass ?? deps.searchScope ?? indexSearchScope)(scope, {
          ...deps.search, metrics: phase, ...(physicallyCurrent === false && deps.search.skipCurrent ? { skipCurrent: false } : {}),
        });
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          await later(manifestKey, messageIds, "scope busy", carriedTime(covered));
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.skipped) {
          // SEARCH_INDEX_SKIP_CURRENT_ENABLED: a complete pass already covers the live source set; done, not retried.
          log.info("search index skipped", {
            stage: "search-index", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId,
            reason: result.reasons.join(","), ...phaseFields(), ...passFields(messageIds, recordOf, now), duration_ms: Date.now() - started,
          });
          logSearchable(scope, revisions, now, pickedUpAt, passStartedAt, true);
          continue;
        }
        if (result.deferred) {
          // This invocation already streamed a scope (streamSlot): stream this one in a later invocation.
          await later(manifestKey, messageIds, "stream deferred", carriedTime(covered));
          log.info("search index stream deferred, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_deferred" });
          continue;
        }
        if (result.reasons.includes(LEGACY_PROVENANCE_INVALID))
          log.warn("canonical legacy evidence dropped", {
            stage: "search-index",
            reason: LEGACY_PROVENANCE_INVALID,
          });
        // SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true only: the first deadline hit marks the scope and hands it to the
        // large-read job, after the pass returned (its transaction and scope lock are released).
        if (deps.deadlineIsolation && hitStreamDeadline(result) && !obsoleteInline) {
          await markDeadline(deps.deadlineIsolation, scope);
          await routeDeadlineScope(deps.deadlineIsolation, scope, "first_hit", phaseFields(), started, knownOldest(covered) ?? started, holdRequeue, incoming);
        }
        const lag = lagFields(covered, messageIds, messages, now());
        if (result.partial)
          log.warn("search index partial", {
            stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, manifests: result.manifests,
            // scope_too_large, then the streaming budget that stopped it, if one did.
            reason: result.reasons.join(",") || "scope_too_large",
            ...(result.stream ? { stream: result.stream } : {}),
            contender_seats: result.contenderSeats,
            ...lag,
          });
        log.info("search index reconciled", {
          stage: "search-index",
          org_id: scope.orgId,
          workspace_id: scope.workspaceId,
          harness: scope.harness,
          session_id: scope.sessionId,
          manifests: result.manifests,
          count: result.rows,
          inserted: result.inserted,
          deleted: result.deleted,
          ok: result.complete,
          reason: result.reasons.join(",") || undefined,
          // Only on a read pass refused for acceptance (search-index.ts SearchIndexResult.contenderSeats).
          contender_seats: result.contenderSeats,
          ...phaseFields(),
          ...passFields(messageIds, recordOf, now),
          duration_ms: Date.now() - started,
          ...lag,
        });
        if (result.complete) logSearchable(scope, revisions, now, pickedUpAt, passStartedAt, false);
      } catch (error) {
        log.error("search index failed", {
          stage: "search-index",
          org_id: scope.orgId,
          session_id: scope.sessionId,
          ...errorFields(error),
          ...searchIndexErrorDetail(error),
          ...phaseFields(),
          duration_ms: Date.now() - started,
        });
        for (const id of messageIds) fail(id);
      }
    }
  }
  if (deps.replay) {
    for (const { scope, messageIds } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every(id => failed.has(id))) continue;
      const started = Date.now();
      try {
        const result = await produceCanonicalReplay(scope, deps.replay);
        log.info("canonical replay materialized", { stage: "replay-pages", org_id: scope.orgId, session_id: scope.sessionId,
          ok: result.published, reason: result.reason, count: result.rows, objects: result.objects, duration_ms: Date.now()-started });
      } catch (error) {
        // Retrying cannot fix a superseded source or a capacity refusal: log it and
        // keep the message (its pointer and search stages already succeeded).
        const refusal = classifyReplayFailure(error);
        const fields = { stage: "replay-pages", org_id: scope.orgId, session_id: scope.sessionId, ok: false,
          ...errorFields(error), duration_ms: Date.now()-started };
        if (refusal?.kind === "superseded") {
          // The newer source write has its own object event, which re-runs this scope.
          log.info("canonical replay superseded", { ...fields, reason: refusal.reason, error_code: refusal.code });
          continue;
        }
        if (refusal?.kind === "capacity") {
          // Oversize scopes go to the large-read path before search/replay run: deps.large
          // (route_session_large_read -> session_large_read_jobs, default off) delegates them
          // above. A capacity refusal here is a scope that was not delegated (handoff off,
          // metadata under the route thresholds, or the route itself refused on capacity);
          // it is logged, not retried.
          log.warn("canonical replay refused", { ...fields, reason: refusal.reason, error_code: refusal.code });
          continue;
        }
        log.error("canonical replay publication failed", fields);
        for (const id of messageIds) fail(id);
      }
    }
  }
  if (ordered) {
    // FIFO partial-batch rule: once a message of a group fails, every later message of that group in
    // this batch fails too, so SQS never deletes a later revision while an earlier one is redelivered.
    // Followers only wait behind the failure, so they come back quickly. A message that failed on its own
    // (unreadable, a pointer error) keeps the queue's normal visibility and retry count toward the DLQ.
    const tripped = new Set<string>();
    const followers = new Set<string>();
    for (const record of records) {
      if (record.groupId === undefined) continue;
      if (tripped.has(record.groupId)) { if (!failed.has(record.messageId)) followers.add(record.messageId); failed.add(record.messageId); }
      else if (failed.has(record.messageId)) tripped.add(record.groupId);
    }
    const shorten = [...failed].filter((id) => retry.has(id) || followers.has(id));
    // Every busy round counts toward the queue's maxReceiveCount, so the delay doubles per receive
    // (base, 2x, 4x ... capped at ORDERED_RETRY_CAP_SECONDS): a long-held lock backs off instead of
    // walking the message into the DLQ.
    const bySeconds = new Map<number, string[]>();
    for (const id of shorten) {
      const seconds = orderedRetrySeconds(ordered.busyRetrySeconds, recordOf.get(id)?.receiveCount);
      bySeconds.set(seconds, [...(bySeconds.get(seconds) ?? []), id]);
    }
    for (const [seconds, ids] of bySeconds) {
      try { await ordered.retryLater(ids, seconds); }
      // Visibility is an optimization: without it the messages come back after the queue's full timeout.
      catch (error) { log.warn("revision retry visibility failed", { stage: "revision-events", count: ids.length, ...errorFields(error) }); }
    }
  }
  if (deps.searchJobs) {
    try {
      await deps.searchJobs.drain();
    } catch (error) {
      log.error("search jobs follow-on failed", { stage: "search-index", ...errorFields(error) });
    }
  }
  // Last, after the search-jobs follow-on (which never fails a message), so a 60 s visibility cannot lapse
  // while this invocation still runs; the remaining-time recheck covers a follow-on that ran long.
  // Standard queue only: an ordered (FIFO) batch shortens its own visibility above (ordered.retryLater).
  if (deps.retryVisibility && !ordered && fastRetry.size > 0) {
    const handles = new Map(records.map((r) => [r.messageId, r.receiptHandle]));
    const ids = [...fastRetry].filter((id) => failed.has(id) && !slowRetry.has(id));
    await shortenRetries(ids.map((id) => handles.get(id)), deps.retryVisibility);
  }
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}

const unquoteEtag = (v: string | null | undefined) => (v ? v.replace(/^"|"$/g, "") : undefined);

/**
 * Physical currency of the pointer rows: for every manifest key behind this scope's batch, the stored pointer row
 * names the SAME object (ETag) that the pointer pass just read as live. A key without a live stamp, or a row that
 * names another object, is no proof.
 */
/** A key the pointer stage read in this batch: its live ETag (absent when unknown) or a delete. */
interface LiveKey { key: string; etag?: string; deleted: boolean }

async function pointerRowsAreLive(db: PointerDeps["db"], scope: SessionScope, keys: readonly LiveKey[]): Promise<boolean> {
  const liveByKey = new Map<string, string | undefined>();
  // A delete in the batch: the pass must run (erase); no physical proof.
  if (keys.some((k) => k.deleted)) return false;
  for (const k of keys) liveByKey.set(k.key, unquoteEtag(k.etag));
  if (liveByKey.size === 0 || [...liveByKey.values()].some((e) => e === undefined)) return false;
  const { rows } = await db.query<{ manifest_key: string; manifest_etag: string | null }>(
    `SELECT manifest_key, manifest_etag FROM sessions.session_index
      WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND deleted_at IS NULL AND manifest_key = ANY($5::text[])`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, [...liveByKey.keys()]]);
  if (rows.length !== liveByKey.size) return false;
  return rows.every((row) => unquoteEtag(row.manifest_etag) === liveByKey.get(row.manifest_key));
}

/**
 * True when every message behind a scope is a stamped revision event (a versionId or ETag) whose write the
 * pointer pass found not live, and none is a delete. Unstamped events (S3 or reconcile bodies that lack
 * both) and deletes always run their pass.
 */
function supersededOnly(messageIds: readonly string[], revisions: readonly Revised[]): boolean {
  if (revisions.length === 0) return false;
  const stale = new Set<string>();
  for (const r of revisions) {
    if (r.live || r.event.deleted || (!r.event.version && !r.event.etag)) return false;
    // Coverage must be PROVEN, not assumed from a message that may be lost (Pipeline review, T126). Only an
    // `unchanged` pointer proves an earlier pass already advanced this key to the live write. FIFO deletes that
    // earlier message only after its batch (search pass included) succeeded, so that pass covered it. If THIS
    // pass advanced the pointer, it is the first to see the live write and must run the search pass itself:
    // the live write's own event may never come, and reconcile sees the pointer matching S3 and sends nothing.
    if (r.pointerStatus !== "unchanged") return false;
    stale.add(r.messageId);
  }
  return messageIds.every((id) => stale.has(id));
}

/** The event's physical-write digest: the same value as its FIFO MessageDeduplicationId. */
const incarnation = (e: LandedEvent) => revisionDedupId({ key: e.key, ...(e.version ? { version: e.version } : {}), ...(e.etag ? { etag: e.etag } : {}),
  ...(e.deleted ? { deleted: true } : {}), ...(e.revision?.committedAt ? { committedAt: e.revision.committedAt } : {}) });

/** One revision event that reached a completed pointer pass: kept for its first-appearance line. */
interface Revised {
  event: LandedEvent;
  messageId: string;
  /** The live manifest carries this event's write (not superseded). */
  live: boolean;
  sentAt?: number;
  receiveCount?: number;
  /** This key's pointer work: start and completion on the consumer clock. */
  pointer: { startedAt: number; doneAt: number };
  /** The pointer pass's outcome for this key: `unchanged` means an EARLIER pass already advanced it to the live write. */
  pointerStatus: string;
  /** The live manifest's stamps as the pointer pass read them under the lock (absent for a delete). */
  liveStamp?: { etag?: string; version?: string };
}

/**
 * The commit check for revision events. The pointer pass read the live manifest under the lock;
 * an event names one physical write (S3 versionId, else ETag). An event whose write is not the
 * live one is superseded. It is logged (coalescing is countable) and never published. It costs
 * nothing extra: the pass already indexed whatever is live, which for an erased incarnation is
 * the new incarnation or nothing.
 */
function checkRevisions(revs: readonly { event: LandedEvent; messageId: string }[], outcome: Extract<PointerOutcome, { scope: SessionScope }>,
  recordOf: ReadonlyMap<string, QueueRecord>, now: () => number, pointer: { startedAt: number; doneAt: number }): Revised[] {
  const out: Revised[] = [];
  const live = outcome.live;
  for (const { event, messageId } of revs) {
    const record = recordOf.get(messageId);
    const same = !event.deleted && live !== undefined && outcome.status !== "deleted" &&
      (event.version && live.version ? event.version === live.version : event.etag !== undefined && event.etag === live.etag);
    if (!same && !event.deleted) {
      log.info("revision superseded", {
        stage: "revision-events", org_id: outcome.scope.orgId, workspace_id: outcome.scope.workspaceId, harness: outcome.scope.harness, session_id: outcome.scope.sessionId,
        revision: event.revision?.revision, event_source: event.revision?.source, incarnation_sha256: incarnation(event),
        ...(outcome.status === "deleted" ? { superseded_by_delete: true } : outcome.revision !== undefined ? { superseded_by_revision: outcome.revision } : {}),
        ...(record?.sentAt !== undefined ? { group_lag_ms: Math.max(0, now() - record.sentAt) } : {}),
      });
    }
    out.push({ event, messageId, live: same, pointer, pointerStatus: outcome.status, ...(live ? { liveStamp: live } : {}), ...(record?.sentAt !== undefined ? { sentAt: record.sentAt } : {}), ...(record?.receiveCount !== undefined ? { receiveCount: record.receiveCount } : {}) });
  }
  return out;
}

/** Queue timing for a pass line: the oldest enqueue among its messages, and the highest receive count. */
function passFields(messageIds: readonly string[], recordOf: ReadonlyMap<string, QueueRecord>, now: () => number) {
  let sentAt: number | undefined;
  let receives: number | undefined;
  for (const id of messageIds) {
    const r = recordOf.get(id);
    if (r?.sentAt !== undefined && (sentAt === undefined || r.sentAt < sentAt)) sentAt = r.sentAt;
    if (r?.receiveCount !== undefined) receives = Math.max(receives ?? 0, r.receiveCount);
  }
  if (sentAt === undefined) return {};
  return { enqueued_at_ms: sentAt, queue_wait_ms: Math.max(0, now() - sentAt), ...(receives !== undefined ? { receive_count: receives } : {}), coalesced: messageIds.length };
}

/**
 * First appearance, one line per live upload (initial vs append measured separately): the
 * upload's own commit time to the end of the pass that made it searchable. A superseded event
 * has no line of its own; the upload that superseded it reports.
 */
function logSearchable(scope: SessionScope, revisions: readonly Revised[], now: () => number, pickedUpAt: number, passStartedAt: number, alreadyCurrent: boolean) {
  const seen = new Set<string>();
  for (const r of revisions) {
    if (!r.live) continue;
    const id = `${r.event.key}\u001f${r.event.version ?? r.event.etag ?? ""}`;
    if (seen.has(id)) continue;
    seen.add(id);
    const committed = r.event.revision?.committedAt ? Date.parse(r.event.revision.committedAt) : NaN;
    const kind = r.event.revision?.initial === true ? "initial" : r.event.revision?.initial === false ? "append" : r.event.revision?.revision === 1 ? "initial" : r.event.revision?.revision ? "append" : "unknown";
    log.info("revision searchable", {
      stage: "revision-events", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId,
      revision: r.event.revision?.revision, upload_kind: kind, event_source: r.event.revision?.source, already_current: alreadyCurrent,
      incarnation_sha256: incarnation(r.event),
      // Legs, each on the consumer clock: enqueue/commit -> PICKUP (the batch arrived, before any pointer work: the 7s
      // budget) -> this key's pointer work (pointer_ms; the 2s budget) -> the covering search pass's start -> its
      // completion (*_searchable_ms, queue_wait_ms).
      picked_up_at_ms: pickedUpAt,
      pointer_started_at_ms: r.pointer.startedAt,
      pointer_ms: Math.max(0, r.pointer.doneAt - r.pointer.startedAt),
      pass_started_at_ms: passStartedAt,
      ...(Number.isFinite(committed) ? { commit_to_pickup_ms: Math.max(0, pickedUpAt - committed) } : {}),
      ...(r.sentAt !== undefined ? { pickup_wait_ms: Math.max(0, pickedUpAt - r.sentAt) } : {}),
      ...(Number.isFinite(committed) ? { commit_to_start_ms: Math.max(0, passStartedAt - committed), commit_to_searchable_ms: Math.max(0, now() - committed) } : {}),
      ...(r.sentAt !== undefined ? { enqueued_at_ms: r.sentAt, start_wait_ms: Math.max(0, passStartedAt - r.sentAt), queue_wait_ms: Math.max(0, now() - r.sentAt) } : {}),
      ...(r.receiveCount !== undefined ? { receive_count: r.receiveCount } : {}),
    });
  }
}
