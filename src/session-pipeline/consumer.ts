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
import { errorFields, log } from "../shared/log.ts";
import { parseQueueBody, sequencerAfter, type LandedEvent } from "./s3-events.ts";
import { indexPointer, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
}

export interface BatchResponse {
  batchItemFailures: { itemIdentifier: string }[];
}

export interface ConsumerDeps {
  pointer: PointerDeps;
  /** Absent while the search index is switched off: only pointers are written. */
  search?: SearchIndexDeps;
}

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[] }>();
  for (const record of records) {
    const parsed = parseQueueBody(record.body);
    if (parsed.kind === "test") continue;
    if (parsed.kind === "invalid") {
      // Never succeeds on retry; the DLQ keeps it for an operator.
      log.warn("object event unreadable", { stage: "object-landed", reason: "invalid_body" });
      failed.add(record.messageId);
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

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[] }>();
  for (const { event, messageIds } of byKey.values()) {
    const started = Date.now();
    try {
      const outcome = await indexPointer(event, deps.pointer);
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
      });
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) held.messageIds.push(...messageIds);
      else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds] });
    } catch (error) {
      log.error("pointer index failed", { stage: "session-indexed", ...errorFields(error), duration_ms: Date.now() - started });
      for (const id of messageIds) failed.add(id);
    }
  }

  if (deps.search) {
    for (const { scope, messageIds } of scopes.values()) {
      if (messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      try {
        const result = await indexSearchScope(scope, deps.search);
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
