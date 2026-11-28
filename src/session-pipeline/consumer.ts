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
import { errorFields, log, searchIndexErrorDetail } from "../shared/log.ts";
import { parseQueueBody, sequencerAfter, type LandedEvent } from "./s3-events.ts";
import { indexPointer, type PointerDeps, type PointerOutcome, type SessionScope } from "./pointer.ts";
import { indexSearchScope, searchIndexPhaseMetrics, type SearchIndexDeps } from "./search-index.ts";
import { LEGACY_PROVENANCE_INVALID } from "../session-read-api/legacy-provenance.ts";
import { captureRefusal, classifyReplayFailure, produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { LargeJobState, LargeRoute } from "./large-read-worker.ts";
import { revisionDedupId } from "./revision-events.ts";
import { feedBufferedStageByKey, isBufferedStageKey, type BufferedFeedDeps } from "./buffered-search-feed.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
  /** FIFO only: the record's MessageGroupId (revision-events.ts revisionGroupId). */
  groupId?: string;
  /** SQS SentTimestamp (ms since epoch): when the message was enqueued. */
  sentAt?: number;
  /** SQS ApproximateReceiveCount. */
  receiveCount?: number;
}

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
   * Put a manifest back on the queue after a delay (SQS DelaySeconds): used when
   * another pass holds the session, so the event is neither lost nor failed.
   */
  requeue?: (manifestKey: string) => Promise<void>;
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
  /** Test seam only: replaces indexSearchScope for the search stage. Absent (always, in the runtimes): indexSearchScope. */
  searchScope?: typeof indexSearchScope;
}

/** FIFO busy retry default: short, because the group is blocked meanwhile and nothing else can run it. */
export const ORDERED_BUSY_RETRY_SECONDS = 5;
export const ORDERED_RETRY_CAP_SECONDS = 300;
export const orderedRetrySeconds = (base: number, receiveCount = 1) =>
  Math.min(ORDERED_RETRY_CAP_SECONDS, base * 2 ** Math.min(16, Math.max(0, receiveCount - 1)));

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[] }>();
  const ordered = deps.ordered;
  const now = ordered?.now ?? Date.now;
  const recordOf = new Map(records.map((r) => [r.messageId, r]));
  /** Every revision event behind a key, in arrival order, for the per-upload latency lines. */
  const revisionsByKey = new Map<string, { event: LandedEvent; messageId: string }[]>();
  /** Messages whose work is handed back to the FIFO queue with a short visibility (ordered mode). */
  const retry = new Set<string>();
  const later = async (manifestKey: string, messageIds: readonly string[], why: string) => {
    if (ordered) {
      for (const id of messageIds) { retry.add(id); failed.add(id); }
      log.info("revision retry scheduled", { stage: "revision-events", reason: why, count: messageIds.length,
        delay_seconds: orderedRetrySeconds(ordered.busyRetrySeconds, Math.max(...messageIds.map((id) => recordOf.get(id)?.receiveCount ?? 1))) });
      return;
    }
    if (!deps.requeue) throw new Error(`${why} and no requeue`);
    await deps.requeue(manifestKey);
  };
  for (const record of records) {
    // Events carry the provider of the Locker the pointer stage reads from (s3 on the
    // Lambda, LOCKER_PROVIDER on self-host), which is what the row's storage_provider records.
    const parsed = parseQueueBody(record.body, deps.pointer.reader.provider);
    if (parsed.kind === "test") continue;
    if (parsed.kind === "invalid") {
      // Never succeeds on retry; the DLQ keeps it for an operator.
      log.warn("object event unreadable", { stage: "object-landed", reason: "invalid_body" });
      failed.add(record.messageId);
      continue;
    }
    for (const event of parsed.events) {
      if (event.revision) {
        const list = revisionsByKey.get(event.key) ?? [];
        list.push({ event, messageId: record.messageId });
        revisionsByKey.set(event.key, list);
      }
      const held = byKey.get(event.key);
      if (!held) byKey.set(event.key, { event, messageIds: [record.messageId] });
      else {
        held.messageIds.push(record.messageId);
        // FIFO: arrival order is commit order within the group, so the later event wins (both are
        // hints; the live manifest is re-read either way).
        const newer = ordered && event.revision && !event.sequencer ? true : sequencerAfter(event.sequencer, held.event.sequencer);
        if (newer) held.event = event;
      }
    }
  }

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string; revisions: Revised[] }>();
  for (const { event, messageIds } of byKey.values()) {
    const started = Date.now();
    if (deps.bufferedFeed && isBufferedStageKey(event.key)) {
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
        log.error("buffered stage search feed failed", { stage: "buffered-search-feed", ...errorFields(error), duration_ms: Date.now() - started });
        for (const id of messageIds) failed.add(id);
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
      });
      const revisions = checkRevisions(revisionsByKey.get(event.key) ?? [], outcome, recordOf, now);
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) { held.messageIds.push(...messageIds); held.revisions.push(...revisions); }
      else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey, revisions });
    } catch (error) {
      log.error("pointer index failed", { stage: "session-indexed", ...errorFields(error), duration_ms: Date.now() - started });
      for (const id of messageIds) failed.add(id);
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
      log.error("large scope handoff failed",{stage:"large-read",...ids,...errorFields(error)});for(const id of messageIds)failed.add(id);continue;
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
    for (const { scope, messageIds, manifestKey, revisions } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      if (ordered && supersededOnly(messageIds, revisions)) {
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
        const result = await (deps.searchScope ?? indexSearchScope)(scope, { ...deps.search, metrics: phase });
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          await later(manifestKey, messageIds, "scope busy");
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.skipped) {
          // SEARCH_INDEX_SKIP_CURRENT_ENABLED: a complete pass already covers the live source set; done, not retried.
          log.info("search index skipped", {
            stage: "search-index", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId,
            reason: result.reasons.join(","), ...phaseFields(), ...passFields(messageIds, recordOf, now), duration_ms: Date.now() - started,
          });
          logSearchable(scope, revisions, now, true);
          continue;
        }
        if (result.deferred) {
          // This invocation already streamed a scope (streamSlot): stream this one in a later invocation.
          await later(manifestKey, messageIds, "stream deferred");
          log.info("search index stream deferred, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "stream_deferred" });
          continue;
        }
        if (result.reasons.includes(LEGACY_PROVENANCE_INVALID))
          log.warn("canonical legacy evidence dropped", {
            stage: "search-index",
            reason: LEGACY_PROVENANCE_INVALID,
          });
        if (result.partial)
          log.warn("search index partial", {
            stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, manifests: result.manifests,
            // scope_too_large, then the streaming budget that stopped it, if one did.
            reason: result.reasons.join(",") || "scope_too_large",
            ...(result.stream ? { stream: result.stream } : {}),
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
          ...phaseFields(),
          ...passFields(messageIds, recordOf, now),
          duration_ms: Date.now() - started,
        });
        if (result.complete) logSearchable(scope, revisions, now, false);
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
        for (const id of messageIds) failed.add(id);
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
        for (const id of messageIds) failed.add(id);
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
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
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
}

/**
 * The commit check for revision events. The pointer pass read the live manifest under the lock;
 * an event names one physical write (S3 versionId, else ETag). An event whose write is not the
 * live one is superseded. It is logged (coalescing is countable) and never published. It costs
 * nothing extra: the pass already indexed whatever is live, which for an erased incarnation is
 * the new incarnation or nothing.
 */
function checkRevisions(revs: readonly { event: LandedEvent; messageId: string }[], outcome: Extract<PointerOutcome, { scope: SessionScope }>,
  recordOf: ReadonlyMap<string, QueueRecord>, now: () => number): Revised[] {
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
    out.push({ event, messageId, live: same, ...(record?.sentAt !== undefined ? { sentAt: record.sentAt } : {}), ...(record?.receiveCount !== undefined ? { receiveCount: record.receiveCount } : {}) });
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
function logSearchable(scope: SessionScope, revisions: readonly Revised[], now: () => number, alreadyCurrent: boolean) {
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
      ...(Number.isFinite(committed) ? { commit_to_searchable_ms: Math.max(0, now() - committed) } : {}),
      ...(r.sentAt !== undefined ? { enqueued_at_ms: r.sentAt, queue_wait_ms: Math.max(0, now() - r.sentAt) } : {}),
      ...(r.receiveCount !== undefined ? { receive_count: r.receiveCount } : {}),
    });
  }
}
