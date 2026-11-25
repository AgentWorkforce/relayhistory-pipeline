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
 *
 * With `searchHandoff` armed (SESSION_PIPELINE_SEARCH_QUEUE_URL, default off) the two stages run
 * from two queues: the pointer queue's batches hand each scope to the search queue after the
 * pointer stage, and the search queue's records run only the search stage (ConsumerDeps.searchHandoff).
 *
 * Latency fields on "pointer indexed" (per key), "search index reconciled" and
 * "search index partial" (per scope), each over the SQS messages behind the line:
 * - `s3_event_age_ms`: floor(log time - the oldest upload event time covered),
 *   clamped at 0. The time is an S3 `ObjectCreated:*` record's `eventTime`, or the
 *   `oldest_event_time` a requeued reconcile body carries forward, so it stays end
 *   to end across requeues. Deletes carry no upload and are not counted. Omitted
 *   when any covered non-delete event has no usable time (a sweep body, a requeue
 *   written without a time, a missing or unparseable `eventTime`) rather than guessed.
 * - `event_age_ms`: floor(log time - the oldest SQS `SentTimestamp` of the
 *   messages behind the line), clamped at 0. A requeue is a new message, so this
 *   is the wait since the latest enqueue, not since the upload. Omitted when any
 *   covered message has no `SentTimestamp` that is a positive integer.
 * - `queue_origin`: `s3` when every covered message is an S3 notification, else
 *   `requeue` when any is a search pass this consumer requeued (busy, deferred,
 *   batch deadline), else `sweep` (a reconcile body that marks neither).
 * All three are telemetry only: no stage sees them and nothing else depends on them.
 */
import { errorFields, log } from "../shared/log.ts";
import { parseQueueBody, sequencerAfter, type LandedEvent, type QueueOrigin } from "./s3-events.ts";
import { indexPointer, resolveScope, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";
import { streamDeadlinePassed } from "./search-index-stream.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
  /** The SQS `SentTimestamp` attribute (epoch ms, a decimal string in the Lambda event): telemetry only. */
  sentTimestamp?: unknown;
  /**
   * The SQS receipt handle: passed through only while `ConsumerDeps.retryVisibility` is set, and
   * used only as the ChangeMessageVisibility argument. Never logged.
   */
  receiptHandle?: string;
  /**
   * Set by lambda.ts queueRecords only while ConsumerDeps.searchHandoff is armed, when the record's
   * eventSourceARN is the configured search queue: the record is a search pass, not a pointer event.
   */
  fromSearchQueue?: true;
}

/** Pointer-stage error codes whose messages are retried after `retryVisibility.seconds`, not the queue's full timeout. */
export const FAST_RETRY_CODES: ReadonlySet<string> = new Set(["pointer_move_pending"]);

/** A ChangeMessageVisibility call that has not settled after this long is abandoned (the message keeps its full timeout). */
export const RETRY_VISIBILITY_TIMEOUT_MS = 2_000;

/**
 * Time kept back for returning the batch response after the calls. With less than
 * RETRY_VISIBILITY_TIMEOUT_MS + this left in the invocation no call is made, so a near-deadline
 * batch still returns its partial response (and acknowledges its successes) before Lambda times out.
 */
export const RETRY_VISIBILITY_MARGIN_MS = 1_000;

export interface BatchResponse {
  batchItemFailures: { itemIdentifier: string }[];
}

export interface ConsumerDeps {
  pointer: PointerDeps;
  /** Absent while the search index is switched off: only pointers are written. */
  search?: SearchIndexDeps;
  /**
   * Put a manifest back on the queue after a delay (SQS DelaySeconds): used when
   * another pass holds the session, so the event is neither lost nor failed.
   */
  requeue?: (manifestKey: string, oldestEventTime?: string) => Promise<void>;
  /**
   * SESSION_PIPELINE_RETRY_VISIBILITY_S (default off; absent, failures behave and log exactly as
   * before): a message that failed only with a FAST_RETRY_CODES pointer error gets its visibility
   * set to `seconds` before it is returned in batchItemFailures, so SQS redelivers it then instead
   * of after the queue's full visibility timeout. Best-effort: a rejected or timed-out call logs a
   * warn and the message still fails with the normal timeout. Needs sqs:ChangeMessageVisibility on
   * the source queue for the Lambda role. `remainingMs` (the invocation's remaining time, set per
   * invocation by lambda.ts invocationDeps) is checked right before the calls: too little left and
   * none is made; otherwise each is bounded by min(RETRY_VISIBILITY_TIMEOUT_MS, remaining - margin).
   */
  retryVisibility?: {
    seconds: number;
    change: (receiptHandle: string, seconds: number, timeoutMs: number) => Promise<void>;
    remainingMs?: () => number;
  };
  /**
   * SESSION_PIPELINE_SEARCH_QUEUE_URL (default off; absent, the search stage runs inline after the
   * pointer stage exactly as before). Armed, with `search` set: a pointer-queue batch runs the
   * pointer stage, then `send`s one requeue-shaped body per scope to the search queue instead of
   * running the search stage (a failed send fails that scope's messages). A record marked
   * `fromSearchQueue` skips the pointer stage: its scope is rebuilt read-only from the key
   * (pointer.ts resolveScope) and the search stage runs for it, with busy, deferred and
   * stream-deadline requeues going to the search queue (`requeue`), not to `ConsumerDeps.requeue`.
   */
  searchHandoff?: {
    send: (manifestKey: string, oldestEventTime?: string) => Promise<void>;
    requeue: (manifestKey: string, oldestEventTime?: string) => Promise<void>;
  };
  /** Clock for `event_age_ms` and `s3_event_age_ms` only (default Date.now). */
  now?: () => number;
}

/** The upload event times a key or scope covers: the oldest known, and whether any was unknown. */
interface Covered {
  oldestMs?: number;
  unknown: boolean;
}

function cover(into: Covered, event: LandedEvent): void {
  if (event.deleted) return;
  const at = event.oldestEventTime ?? event.eventTime;
  const ms = at === undefined ? NaN : Date.parse(at);
  if (!Number.isFinite(ms)) into.unknown = true;
  else if (into.oldestMs === undefined || ms < into.oldestMs) into.oldestMs = ms;
}

function merge(into: Covered, from: Covered): void {
  if (from.unknown) into.unknown = true;
  if (from.oldestMs !== undefined && (into.oldestMs === undefined || from.oldestMs < into.oldestMs)) into.oldestMs = from.oldestMs;
}

/** The oldest covered upload time when every covered upload's time is known. */
function knownOldest(c: Covered): number | undefined {
  return c.unknown ? undefined : c.oldestMs;
}

/** floor(now - since), clamped at 0 (clock skew); undefined when not finite. */
function ageMs(now: number, since: number): number | undefined {
  const age = Math.floor(now - since);
  return Number.isFinite(age) ? Math.max(0, age) : undefined;
}

/** One SQS message's telemetry: its `SentTimestamp` (when usable) and its origin. */
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

/** The latency fields of one log line over the messages `ids` and the upload times `covered` (see the file comment). */
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

/** The carried time for a requeue body, or undefined (the body is then exactly as before). */
function carriedTime(c: Covered): string | undefined {
  const oldest = knownOldest(c);
  return oldest === undefined ? undefined : new Date(oldest).toISOString();
}

/** Set each message's visibility to `seconds`, bounded and best-effort: never throws, never changes which messages fail. */
async function shortenRetries(handles: readonly (string | undefined)[], rv: NonNullable<ConsumerDeps["retryVisibility"]>): Promise<void> {
  // Rechecked here, after both stages: the batch may have used most of the invocation.
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

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  // retryVisibility only: messages with a fast-retry pointer failure, and messages with any other failure.
  const fastRetry = new Set<string>();
  const slowRetry = new Set<string>();
  const now = deps.now ?? Date.now;
  const messages = new Map<string, Message>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[]; covered: Covered }>();
  // searchHandoff only: search-queue records, collapsed by key the same way, never given to the pointer stage.
  const searchByKey: typeof byKey = new Map();
  for (const record of records) {
    const parsed = parseQueueBody(record.body);
    if (parsed.kind === "test") continue;
    if (parsed.kind === "invalid") {
      // Never succeeds on retry; the DLQ keeps it for an operator.
      log.warn("object event unreadable", { stage: "object-landed", reason: "invalid_body" });
      failed.add(record.messageId);
      continue;
    }
    const sentMs = sentTimestampMs(record.sentTimestamp);
    messages.set(record.messageId, { origin: parsed.origin, ...(sentMs === undefined ? {} : { sentMs }) });
    const into = deps.searchHandoff && record.fromSearchQueue ? searchByKey : byKey;
    for (const event of parsed.events) {
      const held = into.get(event.key);
      if (!held) {
        const covered: Covered = { unknown: false };
        cover(covered, event);
        into.set(event.key, { event, messageIds: [record.messageId], covered });
      } else {
        held.messageIds.push(record.messageId);
        cover(held.covered, event);
        if (sequencerAfter(event.sequencer, held.event.sequencer)) held.event = event;
      }
    }
  }

  type Scopes = Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string; covered: Covered }>;
  const scopes: Scopes = new Map();
  const addScope = (into: Scopes, scope: SessionScope, messageIds: readonly string[], manifestKey: string, covered: Covered) => {
    const id = scopeId(scope);
    const held = into.get(id);
    if (held) {
      held.messageIds.push(...messageIds);
      merge(held.covered, covered);
    } else into.set(id, { scope, messageIds: [...messageIds], manifestKey, covered: { ...covered } });
  };
  for (const { event, messageIds, covered } of byKey.values()) {
    const started = Date.now();
    // The carried time is telemetry only: no stage sees it.
    const { oldestEventTime: _carried, ...landed } = event;
    try {
      const outcome = await indexPointer(landed, deps.pointer);
      if (outcome.status === "ignored") {
        log.info("object event ignored", { stage: "object-landed", reason: outcome.reason });
        continue;
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
      addScope(scopes, outcome.scope, messageIds, outcome.manifestKey, covered);
    } catch (error) {
      const fields = errorFields(error);
      log.error("pointer index failed", { stage: "session-indexed", ...fields, duration_ms: Date.now() - started });
      for (const id of messageIds) failed.add(id);
      if (deps.retryVisibility) for (const id of messageIds) (FAST_RETRY_CODES.has(fields.error_code as string) ? fastRetry : slowRetry).add(id);
    }
  }

  const runSearch = async (search: SearchIndexDeps, scopes: Scopes, requeue: ConsumerDeps["requeue"]) => {
    for (const { scope, messageIds, manifestKey, covered } of scopes.values()) {
      if (messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      try {
        // SEARCH_INDEX_STREAMING_CH=1 only: past the invocation's stream deadline no scope of the
        // batch is started (whole read or streamed); the rest are requeued instead.
        if (search.stream && streamDeadlinePassed(search.streamDeadline)) {
          if (!requeue) throw new Error("batch past its stream deadline and no requeue");
          await requeue(manifestKey, carriedTime(covered));
          log.info("search index past the batch deadline, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_batch_deadline" });
          continue;
        }
        const result = await indexSearchScope(scope, search);
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          if (!requeue) throw new Error("scope busy and no requeue");
          await requeue(manifestKey, carriedTime(covered));
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.deferred) {
          // SEARCH_INDEX_STREAMING_CH=1 only: this invocation already streamed a scope; stream this one later.
          if (!requeue) throw new Error("stream deferred and no requeue");
          await requeue(manifestKey, carriedTime(covered));
          log.info("search index stream deferred, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_deferred" });
          continue;
        }
        const lag = lagFields(covered, messageIds, messages, now());
        if (result.partial)
          log.warn("search index partial", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, manifests: result.manifests, reason: result.streamed ? result.reasons.join(",") : "scope_too_large", contender_seats: result.contenderSeats, ...lag });
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
          duration_ms: Date.now() - started,
          ...lag,
        });
      } catch (error) {
        log.error("search index failed", {
          stage: "search-index",
          org_id: scope.orgId,
          session_id: scope.sessionId,
          ...errorFields(error),
          duration_ms: Date.now() - started,
        });
        for (const id of messageIds) failed.add(id);
        if (deps.retryVisibility) for (const id of messageIds) slowRetry.add(id);
      }
    }
  };

  const handoff = deps.searchHandoff;
  if (deps.search && !handoff) await runSearch(deps.search, scopes, deps.requeue);
  if (deps.search && handoff) {
    // Pointer-queue scopes: hand the search pass to the search queue instead of running it here.
    for (const { scope, messageIds, manifestKey, covered } of scopes.values()) {
      if (messageIds.every((id) => failed.has(id))) continue;
      try {
        await handoff.send(manifestKey, carriedTime(covered));
        log.info("search index handed off", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "search_handoff" });
      } catch (error) {
        log.error("search index handoff failed", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "search_handoff", ...errorFields(error) });
        for (const id of messageIds) failed.add(id);
        if (deps.retryVisibility) for (const id of messageIds) slowRetry.add(id);
      }
    }
    // Search-queue records: the scope from the key alone (read-only, no pointer write), then the search stage.
    const searchScopes: Scopes = new Map();
    for (const { event, messageIds, covered } of searchByKey.values()) {
      try {
        const resolved = await resolveScope(event, deps.pointer);
        if ("status" in resolved) {
          log.info("object event ignored", { stage: "object-landed", reason: resolved.reason });
          continue;
        }
        addScope(searchScopes, resolved.scope, messageIds, event.key, covered);
      } catch (error) {
        log.error("search index failed", { stage: "search-index", ...errorFields(error) });
        for (const id of messageIds) failed.add(id);
        if (deps.retryVisibility) for (const id of messageIds) slowRetry.add(id);
      }
    }
    await runSearch(deps.search, searchScopes, handoff.requeue);
  }
  if (deps.retryVisibility && fastRetry.size > 0) {
    // A message with any other failure (invalid body, another key, the search stage) keeps the full timeout.
    const handles = new Map(records.map((r) => [r.messageId, r.receiptHandle]));
    const ids = [...fastRetry].filter((id) => failed.has(id) && !slowRetry.has(id));
    await shortenRetries(ids.map((id) => handles.get(id)), deps.retryVisibility);
  }
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}
