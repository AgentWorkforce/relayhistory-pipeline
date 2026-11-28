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
import type { LargeReadSource } from "./search-deadline-isolation.ts";
import { indexPointer, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, searchIndexPhaseMetrics, type SearchIndexDeps } from "./search-index.ts";
import { LEGACY_PROVENANCE_INVALID } from "../session-read-api/legacy-provenance.ts";
import { streamDeadlinePassed } from "../session-read-api/canonical-stream-projection.ts";
import { captureRefusal, classifyReplayFailure, produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { LargeJobState, LargeRoute } from "./large-read-worker.ts";
import { DEADLINE_OVER_BOUND, DEADLINE_ROUTE, LARGE_READ_UNCLAIMED_MS, decideHold, deadlineMarked, holdId, sameSource, hitStreamDeadline, markDeadline, type DeadlineIsolationDeps, type HoldDecision } from "./search-deadline-isolation.ts";
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

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

/**
 * A hold-only delivery whose expected incarnation has no job to act on (expected enqueue: obsolete or no_job), or whose
 * incarnation is no longer current. It never enqueues, marks or holds for another incarnation:
 * - "done" (ack) only when the CURRENT incarnation's job row exists and is not failed: that incarnation's own work is
 *   durably held (covered);
 * - "held": within LARGE_READ_UNCLAIMED_MS of the carried upload time, requeued once more for the SAME expectation
 *   (seq + 1), never for the newer one;
 * - "inline": past that bound (or without a requeue), the normal inline pass runs, with no enqueue, mark or hold.
 * A failed read of the current source or of the job row is not coverage.
 */
async function settleUnmatchedHold(
  isolation: DeadlineIsolationDeps,
  scope: SessionScope,
  holds: readonly LargeReadHoldBody[],
  anchorMs: number,
  requeue: ((hold?: LargeReadHoldBody) => Promise<void>) | undefined,
  why: string,
): Promise<"inline" | "done" | "held"> {
  const ids = { org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId };
  let covers = false;
  let canonical: SessionScope = scope;
  try {
    const now = isolation.current ? await isolation.current(scope) : null;
    if (now) {
      canonical = now.identity;
      const row = isolation.progress ? await isolation.progress(now.identity) : null;
      covers = !!row && row.state !== "failed" && sameSource(row.source, now.source);
    }
  } catch (error) {
    log.warn("large scope job state unavailable", { stage: "large-read", ...ids, ...errorFields(error) });
  }
  const age = Math.max(0, (isolation.now ?? Date.now)() - anchorMs);
  if (covers) {
    log.info("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "covered", state: why });
    return "done";
  }
  if (age <= LARGE_READ_UNCLAIMED_MS && requeue) {
    const top = holds.reduce((a, h) => (h.seq > a.seq ? h : a));
    await requeue({ epoch: top.epoch, fingerprint: top.fingerprint, seq: top.seq + 1, id: holdId(canonical, top, top.seq + 1) });
    log.info("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "awaiting_newer", state: why, age_ms: age });
    return "held";
  }
  log.warn("large-read hold obsolete", { stage: "large-read", ...ids, reason: "large_hold_obsolete", outcome: "inline", state: why, age_ms: age });
  return "inline";
}

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
  // A delivery made only of holds acts only on its OWN exact job (Pipeline f1c6515df1fd, ae37e77b3809): the expected
  // enqueue never inserts, never advances or renews another incarnation's job, and the next hold (if any) waits for
  // the same expected incarnation. Originating work (an upload, a first hit, a mixed batch) keeps the ordinary enqueue.
  let enqueued: boolean, identity: SessionScope | undefined, source: LargeReadSource | undefined;
  if (incoming.holdOnly && isolation.enqueueExpected && incoming.holds.length > 0) {
    const expected = incoming.holds.reduce((a, h) => (h.seq > a.seq ? h : a));
    const r = await isolation.enqueueExpected(scope, { epoch: expected.epoch, fingerprint: expected.fingerprint });
    if (r.kind === "unavailable") {
      log.info("search index over-bound, large-read unavailable", { stage: "search-index", ...ids, reason: DEADLINE_OVER_BOUND, outcome: trigger, route: "inline" });
      return "inline";
    }
    if (r.kind === "obsolete" || r.kind === "no_job")
      return settleUnmatchedHold(isolation, scope, incoming.holds, anchorMs, requeue, r.kind === "obsolete" ? `expected_${r.reason}` : `expected_${r.reason}`);
    enqueued = true; identity = r.identity; source = r.source;
  } else {
    ({ enqueued, identity, source } = await isolation.enqueue(scope));
  }
  if (!enqueued) {
    log.info("search index over-bound, large-read unavailable", { stage: "search-index", ...ids, reason: DEADLINE_OVER_BOUND, outcome: trigger, route: "inline" });
    return "inline";
  }
  // A delivery made only of holds whose incarnation was replaced between the obsolete check and this enqueue: the
  // enqueue captured the newer incarnation (an upsert identical to that incarnation's own routing, and a no-op on its
  // row when it is already enqueued). Its job row now exists, so the newer work is covered: ack, no mark, no hold.
  // No captured source is NOT evidence of a newer incarnation: it takes decideHold's unknown-target rule (hold within
  // LARGE_READ_UNCLAIMED_MS, then inline), never an obsolete ack.
  if (incoming.holdOnly && isolation.current && source !== undefined && !incoming.holds.some((h) => sameSource(h, source))) {
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
  const now = deps.now ?? Date.now;
  const messages = new Map<string, Message>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[]; covered: Covered }>();
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
      const held = byKey.get(event.key);
      if (!held) {
        const covered: Covered = { unknown: false };
        cover(covered, event);
        byKey.set(event.key, { event, messageIds: [record.messageId], covered });
      } else {
        held.messageIds.push(record.messageId);
        cover(held.covered, event);
        if (sequencerAfter(event.sequencer, held.event.sequencer)) held.event = event;
      }
    }
  }

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string; covered: Covered }>();
  for (const { event: carriedEvent, messageIds, covered } of byKey.values()) {
    const started = Date.now();
    // The carried time is telemetry only: no stage sees it.
    const { oldestEventTime: _carried, hold: _hold, ...event } = carriedEvent;
    if (deps.bufferedFeed && isBufferedStageKey(event.key, deps.bufferedFeed.keyRoot)) {
      // KEEP-UP. A delete event is a GC removal: index-row deletes are their own lane, so it is a no-op here.
      if (event.deleted) continue;
      try {
        const fed = await feedBufferedStageByKey(event.key, deps.bufferedFeed);
        if (fed.outcome === "requeue") {
          // The PUT lands before the receipt commits (or a GC lease is in flight): look again later.
          if (!deps.requeue) throw new Error("buffered stage pending and no requeue");
          await deps.requeue(event.key);
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
        if (!deps.requeue) throw new Error("pointer changed after pre-read and no requeue");
        await deps.requeue(outcome.manifestKey);
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
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) {
        held.messageIds.push(...messageIds);
        merge(held.covered, covered);
      } else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey, covered: { ...covered, ...(covered.holds ? { holds: [...covered.holds] } : {}) } });
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
    for (const { scope, messageIds, manifestKey, covered } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
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
            const settled = await settleUnmatchedHold(deps.deadlineIsolation, scope, incoming.holds, knownOldest(covered) ?? started, holdRequeue, "incarnation_moved");
            if (settled !== "inline") continue;
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
          if (!deps.requeue) throw new Error("batch past its stream deadline and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
          log.info("search index past the batch deadline, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_batch_deadline" });
          continue;
        }
        const result = await (deps.searchPass ?? indexSearchScope)(scope, { ...deps.search, metrics: phase });
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          if (!deps.requeue) throw new Error("scope busy and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.skipped) {
          // SEARCH_INDEX_SKIP_CURRENT_ENABLED: a complete pass already covers the live source set; done, not retried.
          log.info("search index skipped", {
            stage: "search-index", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId,
            reason: result.reasons.join(","), ...phaseFields(), duration_ms: Date.now() - started,
          });
          continue;
        }
        if (result.deferred) {
          // This invocation already streamed a scope (streamSlot): stream this one in a later invocation.
          if (!deps.requeue) throw new Error("stream deferred and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
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
          duration_ms: Date.now() - started,
          ...lag,
        });
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
  if (deps.searchJobs) {
    try {
      await deps.searchJobs.drain();
    } catch (error) {
      log.error("search jobs follow-on failed", { stage: "search-index", ...errorFields(error) });
    }
  }
  // Last, after the search-jobs follow-on (which never fails a message), so a 60 s visibility cannot lapse
  // while this invocation still runs; the remaining-time recheck covers a follow-on that ran long.
  if (deps.retryVisibility && fastRetry.size > 0) {
    const handles = new Map(records.map((r) => [r.messageId, r.receiptHandle]));
    const ids = [...fastRetry].filter((id) => failed.has(id) && !slowRetry.has(id));
    await shortenRetries(ids.map((id) => handles.get(id)), deps.retryVisibility);
  }
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}
