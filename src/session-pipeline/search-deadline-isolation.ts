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
/** A routed scope's own job may stay unclaimed this long after the oldest upload its message carries; then the inline pass runs. */
export const LARGE_READ_UNCLAIMED_MS = 5 * 60 * 1000;
/** A routed delivery is held at most this long while its job runs with a live lease; then it is acked (the worker finishes alone). */
export const LARGE_READ_HOLD_MAX_MS = 30 * 60 * 1000;

/** The source incarnation a job was enqueued for (capture_session_replay_pages source). */
export interface LargeReadSource { epoch: string; fingerprint: string }
/** One scope's job row as the hold reads it. */
export interface LargeReadProgress { state: string; leaseLive: boolean; source: LargeReadSource }
export type HoldDecision =
  | { kind: "done" }
  | { kind: "hold"; why: "queued" | "running" | "unknown" }
  | { kind: "inline"; why: "unclaimed" | "failed" | "missing" | "unknown" }
  | { kind: "release" };

/**
 * What a routed delivery does once its job's row is read, for the incarnation `target` the enqueue captured. Only a
 * row for `target` (same source epoch and fingerprint) counts as this upload's job; fail closed otherwise:
 * - complete for `target`: done (the large read indexed this source; ack);
 * - running for `target` with a live lease (the worker heartbeats it): hold, until LARGE_READ_HOLD_MAX_MS from
 *   `anchorMs`, then release (ack; the live worker finishes alone);
 * - failed: inline at once (never claimed again until the source changes);
 * - no row: inline;
 * - `p` undefined (the row could not be read): hold until LARGE_READ_UNCLAIMED_MS, then inline (unknown progress never
 *   holds past the unclaimed bound);
 * - anything else (queued, superseded, any state for another incarnation including a live lease, an expired lease, or
 *   no captured `target`): hold until LARGE_READ_UNCLAIMED_MS from `anchorMs`, then inline.
 * Only this job's row decides; no other job's progress enters.
 */
export function decideHold(p: LargeReadProgress | null | undefined, target: LargeReadSource | undefined, ageMs: number): HoldDecision {
  if (p === undefined) return ageMs > LARGE_READ_UNCLAIMED_MS ? { kind: "inline", why: "unknown" } : { kind: "hold", why: "unknown" };
  if (!p) return { kind: "inline", why: "missing" };
  const current = target !== undefined && p.source.epoch === target.epoch && p.source.fingerprint === target.fingerprint;
  if (p.state === "failed") return { kind: "inline", why: "failed" };
  if (current && p.state === "complete") return { kind: "done" };
  if (current && p.state === "running" && p.leaseLive) return ageMs > LARGE_READ_HOLD_MAX_MS ? { kind: "release" } : { kind: "hold", why: "running" };
  return ageMs > LARGE_READ_UNCLAIMED_MS ? { kind: "inline", why: "unclaimed" } : { kind: "hold", why: "queued" };
}

export interface DeadlineIsolationDeps {
  ch: ClickHouseClient;
  /** sessions.enqueue_session_large_read for the scope (enqueueLargeRead). */
  enqueue: (scope: SessionScope) => Promise<{ enqueued: boolean; identity?: SessionScope; source?: LargeReadSource }>;
  /**
   * The routed scope's job row (large-read-worker.ts largeReadJobState): a `failed` job is logged as a warn, since
   * it is not claimed again until the source changes and the mark keeps the inline pass off for up to
   * SKIP_STATE_MAX_AGE_MS. Absent: not read.
   */
  jobState?: (scope: SessionScope) => Promise<{ state: string; attempts: number } | null>;
  /**
   * This scope's job row (largeReadProgress), read after each enqueue. With it, a routed delivery is held (requeued as a
   * NEW message, never acked without a successor) until its own job is complete for the incarnation the enqueue
   * captured; a job not claimed within LARGE_READ_UNCLAIMED_MS of the oldest upload the message carries gets the
   * inline pass instead (decideHold). Absent: routed and acked at once, as before.
   */
  progress?: (scope: SessionScope) => Promise<LargeReadProgress | null>;
  /**
   * The scope's current source incarnation and canonical identity (captureLargeReadSource: one STABLE read, no write),
   * or null when the source is unavailable (erased, blocked, no live pointer). With it, a delivery made only of holds
   * whose expected incarnation is no longer current is obsolete: it never enqueues, writes a mark or holds for the
   * newer incarnation (consumer.ts). Absent: no obsolete check.
   */
  current?: (scope: SessionScope) => Promise<{ source: LargeReadSource; identity: SessionScope } | null>;
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
export async function enqueueLargeRead(db: SqlExecutor, s: SessionScope): Promise<{ enqueued: boolean; identity?: SessionScope; source?: LargeReadSource }> {
  const r = (await db.query<{ r: { enqueued?: unknown; identity?: { orgId?: unknown; workspaceId?: unknown; source?: unknown; sessionId?: unknown }; source?: { epoch?: unknown; fingerprint?: unknown } } }>(
    "SELECT sessions.enqueue_session_large_read($1,$2,$3,$4) r", [s.orgId, s.workspaceId, s.harness, s.sessionId])).rows[0]?.r;
  if (!r || typeof r.enqueued !== "boolean") throw new Error("large_enqueue_invalid");
  const i = r.identity;
  if (!r.enqueued || typeof i?.sessionId !== "string" || !i.sessionId || i.orgId !== s.orgId || i.workspaceId !== s.workspaceId || i.source !== s.harness)
    return { enqueued: r.enqueued };
  const src = r.source;
  const source = src && (typeof src.epoch === "string" || typeof src.epoch === "number") && typeof src.fingerprint === "string"
    ? { epoch: String(src.epoch), fingerprint: src.fingerprint } : undefined;
  return { enqueued: true, identity: { ...s, sessionId: i.sessionId }, ...(source ? { source } : {}) };
}

/** One scope's job row (its canonical identity): state, whether a running lease is still live, and its source incarnation. */
export async function largeReadProgress(db: SqlExecutor, s: SessionScope): Promise<LargeReadProgress | null> {
  const row = (await db.query<{ state: string; lease_live: boolean; source_epoch: string; source_fingerprint: string }>(
    `SELECT state, (state = 'running' AND lease_expires_at > clock_timestamp()) AS lease_live,
            source_epoch::text AS source_epoch, source_fingerprint
       FROM sessions.session_large_read_jobs
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4`,
    [s.orgId, s.workspaceId, s.harness, s.sessionId],
  )).rows[0];
  return row ? { state: String(row.state), leaseLive: row.lease_live === true, source: { epoch: String(row.source_epoch), fingerprint: String(row.source_fingerprint) } } : null;
}

/** The capture's source incarnation and canonical identity (sessions.capture_session_replay_pages, STABLE), or null. */
export async function captureLargeReadSource(db: SqlExecutor, s: SessionScope): Promise<{ source: LargeReadSource; identity: SessionScope } | null> {
  const c = (await db.query<{ c: { exists?: unknown; blocked?: unknown; identity?: { sessionId?: unknown }; source?: { epoch?: unknown; fingerprint?: unknown } } }>(
    "SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c", [s.orgId, s.workspaceId, s.harness, s.sessionId])).rows[0]?.c;
  if (!c || c.exists !== true || c.blocked === true) return null;
  const epoch = c.source?.epoch, fingerprint = c.source?.fingerprint, sessionId = c.identity?.sessionId;
  if ((typeof epoch !== "string" && typeof epoch !== "number") || typeof fingerprint !== "string" || typeof sessionId !== "string" || !sessionId) return null;
  return { source: { epoch: String(epoch), fingerprint }, identity: { ...s, sessionId } };
}

/** Same incarnation. */
export function sameSource(a: LargeReadSource | undefined, b: LargeReadSource | undefined): boolean {
  return a !== undefined && b !== undefined && a.epoch === b.epoch && a.fingerprint === b.fingerprint;
}

/**
 * A hold's id: sha256 over a canonical encoding of the scope, the incarnation it waits for and its sequence. A retry of
 * the same check (same incoming sequence) mints the same id; the next check (sequence + 1) a new one.
 */
export function holdId(scope: SessionScope, source: LargeReadSource, seq: number): string {
  return createHash("sha256").update(JSON.stringify(["large-read-hold/v1", [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId], [source.epoch, source.fingerprint], seq])).digest("hex");
}
