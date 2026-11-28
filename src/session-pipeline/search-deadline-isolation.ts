/**
 * SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true only (lambda.ts, the hosted ClickHouse binding with
 * SEARCH_INDEX_STREAMING_CH=1 and SESSION_LARGE_READ_HANDOFF_ENABLED=1). Default off.
 *
 * The problem: a scope under the large-read route thresholds but too slow to stream within one
 * invocation ends every S3-event pass `scope_too_large,stream_deadline` after most of the
 * invocation (~220 s), holding the scope lock across that streamed read, and every later event,
 * busy or batch-deadline requeue for it runs the same pass again. SEARCH_INDEX_SKIP_OVER_BOUND does
 * not catch it: its stream guard admits any scope the streamed path could take, and a deadline pass
 * leaves scanned_rows > 0.
 *
 * The isolation:
 * 1. The first S3-event pass that ends partial with `stream_deadline` writes a mark for the scope
 *    (`search_ingest_state`, name `deadline_over_bound/<sha256 of the scope>`), after the pass
 *    returned (its transaction and lock are closed), and enqueues the scope's large-read job
 *    (sessions.enqueue_session_large_read, the same SECURITY DEFINER function and service_role grant
 *    the route uses, without the route's size threshold).
 * 2. A later S3-event pass for a marked scope (mark written under SKIP_STATE_MAX_AGE_MS ago) reads the
 *    mark with one bounded ClickHouse read before anything else, outside any Postgres transaction, and
 *    enqueues the large-read job instead of running the inline pass: no manifest read, no lock, no
 *    requeue. The large-read worker (large-read-worker.ts runLargeReadOnce) runs the whole read on the
 *    large budget without a stream deadline, under its own lease and source epoch/fingerprint checks,
 *    and records the complete search state.
 * 3. Anything uncertain runs the normal pass: a failed or unparseable mark read, an expired mark, or an
 *    enqueue that reports the source unavailable (no live pointers, an erasure, a block). So erasure,
 *    fences and revision ordering stay exactly with the inline pass and the large worker's existing
 *    checks; this module only decides which of the two runs a scope.
 *
 * The mark is not tied to a revision on purpose: a live session's revision moves on every upload, and a
 * scope only grows, so the next revision would hit the deadline too. It expires after
 * SKIP_STATE_MAX_AGE_MS, so at most one inline deadline pass per scope per window runs again.
 */
import { createHash } from "node:crypto";
import { STREAM_DEADLINE } from "../session-read-api/canonical-stream-projection.ts";
import { errorFields, log } from "../shared/log.ts";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import type { SessionScope } from "./pointer.ts";
import { SKIP_STATE_MAX_AGE_MS, type SearchIndexResult } from "./search-index.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { chNow } from "./vendor/search-index/reconcile.ts";
import { INGEST_STATE_TABLE } from "./vendor/search-index/schema.ts";

/** `search_ingest_state` names of deadline marks; no other state name starts with it. */
export const DEADLINE_MARK_PREFIX = "deadline_over_bound/";
/** The log reason (and route field) of a scope sent to the large-read job by this module. */
export const DEADLINE_OVER_BOUND = "stream_deadline_over_bound";
export const DEADLINE_ROUTE = "large-read";
const MARK_TIMEOUT_MS = 5_000;
/** A marked scope's job may sit unclaimed this long, with no worker progress anywhere, before the inline pass is used again. */
export const LARGE_READ_UNCLAIMED_MS = 5 * 60 * 1000;

export interface DeadlineIsolationDeps {
  ch: ClickHouseClient;
  /** sessions.enqueue_session_large_read for the scope (enqueueLargeRead). */
  enqueue: (scope: SessionScope) => Promise<{ enqueued: boolean; identity?: SessionScope }>;
  /**
   * The routed scope's job row (large-read-worker.ts largeReadJobState): a `failed` job is logged as a warn, since
   * it is not claimed again until the source changes and the mark keeps the inline pass off for up to
   * SKIP_STATE_MAX_AGE_MS. Absent: not read.
   */
  jobState?: (scope: SessionScope) => Promise<{ state: string; attempts: number } | null>;
  /**
   * The routed job's queue state and whether any large-read worker made progress on any job within `withinMs`
   * (largeReadProgress). With it, a marked delivery whose mark is older than LARGE_READ_UNCLAIMED_MS, whose job is
   * still `queued`, and with no worker progress anywhere in that window falls back to the inline pass (warn
   * "large-read job unclaimed"): a missing or stalled worker degrades to today's behaviour, not to an unsearchable
   * scope for the mark's lifetime. Absent: no check, always routed.
   */
  progress?: (scope: SessionScope, withinMs: number) => Promise<{ state: string; workerSeen: boolean } | null>;
  /** Clock for the mark's age (default Date.now). */
  now?: () => number;
}

export function deadlineMarkName(scope: SessionScope): string {
  return DEADLINE_MARK_PREFIX + createHash("sha256").update(JSON.stringify([scope.orgId, scope.workspaceId, scope.harness, scope.sessionId])).digest("hex");
}

/** The pass ended partial because the invocation's stream deadline stopped it. */
export function hitStreamDeadline(result: SearchIndexResult): boolean {
  return result.partial === true && !result.busy && !result.deferred && !result.skipped && result.reasons.includes(STREAM_DEADLINE);
}

/** The scope's live mark (its write time), or null. One bounded read; any failure or malformed row is null (the normal pass). */
export async function deadlineMarked(deps: DeadlineIsolationDeps, scope: SessionScope): Promise<{ atMs: number } | null> {
  let rows: { value: unknown; at_ms: unknown }[];
  try {
    rows = await deps.ch.select<{ value: unknown; at_ms: unknown }>(
      `SELECT value, toUnixTimestamp64Milli(updated_at) AS at_ms
         FROM ${INGEST_STATE_TABLE}
        WHERE name = {name:String}
        ORDER BY updated_at DESC
        LIMIT 1`,
      { params: { name: deadlineMarkName(scope) }, timeoutMs: MARK_TIMEOUT_MS },
    );
  } catch (error) {
    log.warn("search index deadline mark unavailable", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "deadline_mark_read_failed", ...errorFields(error) });
    return null;
  }
  const row = rows[0];
  if (row === undefined || typeof row.value !== "string" || row.value === "") return null;
  const atMs = Number(row.at_ms);
  const age = Math.abs((deps.now ?? Date.now)() - atMs);
  if (!Number.isFinite(age) || !(age < SKIP_STATE_MAX_AGE_MS)) return null;
  try {
    const v = JSON.parse(row.value) as Record<string, unknown> | null;
    return typeof v === "object" && v !== null && v.v === 1 && v.reason === STREAM_DEADLINE ? { atMs } : null;
  } catch {
    return null;
  }
}

/** Write the scope's mark. Best effort: a failure is logged and the next deadline pass writes it again. */
export async function markDeadline(deps: DeadlineIsolationDeps, scope: SessionScope): Promise<boolean> {
  try {
    await deps.ch.insert(
      INGEST_STATE_TABLE,
      [{ name: deadlineMarkName(scope), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: chNow(new Date((deps.now ?? Date.now)())) }],
      { timeoutMs: MARK_TIMEOUT_MS },
    );
    return true;
  } catch (error) {
    log.warn("search index deadline mark not written", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "deadline_mark_write_failed", ...errorFields(error) });
    return false;
  }
}

/**
 * sessions.enqueue_session_large_read: one autocommit statement (no caller transaction), no size threshold.
 * `identity` is the job's canonical scope (jobs are keyed by canonical session id), only when it names the same
 * org, workspace and harness.
 */
export async function enqueueLargeRead(db: SqlExecutor, s: SessionScope): Promise<{ enqueued: boolean; identity?: SessionScope }> {
  const r = (await db.query<{ r: { enqueued?: unknown; identity?: { orgId?: unknown; workspaceId?: unknown; source?: unknown; sessionId?: unknown } } }>(
    "SELECT sessions.enqueue_session_large_read($1,$2,$3,$4) r", [s.orgId, s.workspaceId, s.harness, s.sessionId])).rows[0]?.r;
  if (!r || typeof r.enqueued !== "boolean") throw new Error("large_enqueue_invalid");
  const i = r.identity;
  if (!r.enqueued || typeof i?.sessionId !== "string" || !i.sessionId || i.orgId !== s.orgId || i.workspaceId !== s.workspaceId || i.source !== s.harness)
    return { enqueued: r.enqueued };
  return { enqueued: true, identity: { ...s, sessionId: i.sessionId } };
}

/**
 * The job row's state for `s` (its canonical identity) and, in the same statement, whether a worker made progress on
 * any job within `withinMs`: a job updated in that window that is not a fresh enqueue (claimed, finished, retried or
 * failed). Enqueue alone (state queued, attempts 0, no error) never counts as progress.
 */
export async function largeReadProgress(db: SqlExecutor, s: SessionScope, withinMs: number): Promise<{ state: string; workerSeen: boolean } | null> {
  const row = (await db.query<{ state: string; worker_seen: boolean }>(
    `SELECT j.state, EXISTS(SELECT 1 FROM sessions.session_large_read_jobs w
                             WHERE w.updated_at > clock_timestamp() - make_interval(secs => $5::double precision / 1000)
                               AND (w.state <> 'queued' OR w.attempts > 0 OR w.last_error IS NOT NULL)) AS worker_seen
       FROM sessions.session_large_read_jobs j
      WHERE j.org_id=$1 AND j.workspace_id=$2 AND j.harness=$3 AND j.session_id=$4`,
    [s.orgId, s.workspaceId, s.harness, s.sessionId, withinMs],
  )).rows[0];
  return row ? { state: String(row.state), workerSeen: row.worker_seen === true } : null;
}
