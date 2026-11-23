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
import { indexPointer, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";
import { streamDeadlinePassed } from "./search-index-stream.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
  /** The SQS `SentTimestamp` attribute (epoch ms, a decimal string in the Lambda event): telemetry only. */
  sentTimestamp?: unknown;
}

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

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  const now = deps.now ?? Date.now;
  const messages = new Map<string, Message>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[]; covered: Covered }>();
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
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) {
        held.messageIds.push(...messageIds);
        merge(held.covered, covered);
      } else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey, covered: { ...covered } });
    } catch (error) {
      log.error("pointer index failed", { stage: "session-indexed", ...errorFields(error), duration_ms: Date.now() - started });
      for (const id of messageIds) failed.add(id);
    }
  }

  if (deps.search) {
    for (const { scope, messageIds, manifestKey, covered } of scopes.values()) {
      if (messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      try {
        // SEARCH_INDEX_STREAMING_CH=1 only: past the invocation's stream deadline no scope of the
        // batch is started (whole read or streamed); the rest are requeued instead.
        if (deps.search.stream && streamDeadlinePassed(deps.search.streamDeadline)) {
          if (!deps.requeue) throw new Error("batch past its stream deadline and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
          log.info("search index past the batch deadline, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_batch_deadline" });
          continue;
        }
        const result = await indexSearchScope(scope, deps.search);
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          if (!deps.requeue) throw new Error("scope busy and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.deferred) {
          // SEARCH_INDEX_STREAMING_CH=1 only: this invocation already streamed a scope; stream this one later.
          if (!deps.requeue) throw new Error("stream deferred and no requeue");
          await deps.requeue(manifestKey, carriedTime(covered));
          log.info("search index stream deferred, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_deferred" });
          continue;
        }
        const lag = lagFields(covered, messageIds, messages, now());
        if (result.partial)
          log.warn("search index partial", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, manifests: result.manifests, reason: result.streamed ? result.reasons.join(",") : "scope_too_large", ...lag });
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
      }
    }
  }
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}
