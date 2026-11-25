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
import { indexPointer, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, searchIndexPhaseMetrics, type SearchIndexDeps } from "./search-index.ts";
import { LEGACY_PROVENANCE_INVALID } from "../session-read-api/legacy-provenance.ts";
import { captureRefusal, classifyReplayFailure, produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { LargeJobState, LargeRoute } from "./large-read-worker.ts";
import { feedBufferedStageByKey, isBufferedStageKey, type BufferedFeedDeps } from "./buffered-search-feed.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
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

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

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
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[] }>();
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
    for (const event of parsed.events) {
      const held = byKey.get(event.key);
      if (!held) byKey.set(event.key, { event, messageIds: [record.messageId] });
      else {
        held.messageIds.push(record.messageId);
        if (sequencerAfter(event.sequencer, held.event.sequencer)) held.event = event;
      }
    }
  }

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string }>();
  for (const { event, messageIds } of byKey.values()) {
    const started = Date.now();
    if (deps.bufferedFeed && isBufferedStageKey(event.key)) {
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
        log.error("buffered stage search feed failed", { stage: "buffered-search-feed", ...errorFields(error), duration_ms: Date.now() - started });
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
      });
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) held.messageIds.push(...messageIds);
      else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey });
    } catch (error) {
      const fields = errorFields(error);
      log.error("pointer index failed", { stage: "session-indexed", ...fields, duration_ms: Date.now() - started });
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
    for (const { scope, messageIds, manifestKey } of scopes.values()) {
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
        const result = await indexSearchScope(scope, { ...deps.search, metrics: phase });
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          if (!deps.requeue) throw new Error("scope busy and no requeue");
          await deps.requeue(manifestKey);
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
          await deps.requeue(manifestKey);
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
          duration_ms: Date.now() - started,
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
