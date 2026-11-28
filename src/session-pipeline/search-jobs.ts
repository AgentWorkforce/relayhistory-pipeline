/**
 * The `search-index` stage's consumer: the committed search wake (docs/handoffs/incremental-search-indexing.md,
 * design M1 rev 9, milestone M2).
 *
 *   enqueueSearchWake        §3.1  the one-row-per-scope coalescing upsert (catch-up, replay)
 *   claimSearchJobs          §3.2  claim up to n visible jobs: a lease per row, a counted attempt, the claimed epoch
 *   settleSearchJob          §3.3  done | {retry} | exhausted, lease-bound and claimed-epoch aware (+ §3.3b completion)
 *   replaySearchDeadLetter   §3.3b the stage-aware replay of one scope letter
 *   catchUpTick              §6    one tick of the global keyset cursor over stale scopes
 *   runSearchJob             §4    the pass: committed-state guard, current check, erase only on authority, the work
 *   parkSearchJobs           §8    the park path: every row into its scope letter, in one fenced transaction
 *   searchStageDownPrecondition / archiveSearchStage  §8  the down migration's precondition and the sealed archive
 *
 * Only this module settles `search-index` rows: the generic queue consumers (postgresStageConsumer, runWorker) never
 * register the stage (a plain delete on done would lose a newer wake that arrived during the pass). The hosted
 * consumer is the session-pipeline Lambda (lambda.ts, SEARCH_JOBS_ENABLED, default off).
 *
 * Roles (least privilege). The runtime boundary of the stage is exactly two SECURITY DEFINER functions of migration
 * 20261105062300: sessions.claim_search_jobs_v1 (claimSearchJobs) and sessions.settle_search_job_v1 (settleSearchJob).
 * (plus the probe lane, sessions.claim_search_probe_v1 / claimSearchProbe). The runtime role (the Lambda) holds
 * EXECUTE on those three only; it needs no search-index row DML (the queue guard
 * refuses it) and never sees the owner-only claim receipts (sessions.search_job_claims). Settlement takes the claimed
 * epoch, message and attempt from the server's receipt and derives the backoff and exhaustion from server policy.
 * Operator paths, run with owner (or, for the archive, service_role) credentials and never granted to the runtime
 * role: enqueueSearchWake, replaySearchDeadLetter, catchUpTick and parkSearchJobs (TypeScript over the guarded queue,
 * which admits search-index writes only from the schema owner) and archiveSearchStage (archive_search_stage_v1,
 * EXECUTE to service_role only). A future scheduled catch-up needs its own scoped grant.
 */
import { deadLetterId, isStageMessage, type StageMessage } from "@rhc/lib/pipeline/dlq.ts";
import { pipelineEventKey } from "@rhc/lib/pipeline/events.ts";
import type { PipelineStatus } from "@rhc/lib/pipeline/trace.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { STAGE_REGISTRY } from "@rhc/lib/pipeline/registry.ts";
import { errorFields, log } from "../shared/log.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { scopeLockKey, type SessionScope } from "./pointer.ts";
import {
  ALREADY_CURRENT,
  NOT_COMMITTED,
  alreadyCurrent,
  indexSearchScope,
  scopeEverCommitted,
  type SearchIndexDeps,
  type SearchIndexResult,
} from "./search-index.ts";

export const SEARCH_INDEX_STAGE = "search-index" as const;
/** §4.5: held acceptance retries this many times (about 2.5 h with the registry backoff), then exhausts. */
export const SEARCH_JOB_MAX_ATTEMPTS = STAGE_REGISTRY[SEARCH_INDEX_STAGE].maxAttempts;
/** §5.1: jobs one follow-on or scheduled claim takes. */
export const SEARCH_JOBS_DEFAULT_CLAIM = 5;
/** §6: scopes one catch-up tick visits. */
export const SEARCH_CATCHUP_DEFAULT_PAGE = 500;
/** The server's lease (claim_search_jobs_v1); a worker that dies leaves its rows to be reclaimed after it. Mirrors SQL. */
export const SEARCH_JOB_LEASE_SECONDS = 300;
/** `scope_busy`: retried after this short delay (coalesced into the one row). */
export const SEARCH_JOB_BUSY_RETRY_SECONDS = 30;

/** §4.5: the registry backoff, 30 s · 2^(n-1) capped at 900 s, for the attempt that just failed (settle_search_job_v1's policy). */
export const searchJobBackoffSeconds = (attempt: number) => Math.min(900, 30 * 2 ** Math.max(0, attempt - 1));

export type SearchClock = { source: "publish" | "pointer"; at: string };
export interface SearchWakePayload { epoch: number; clock: SearchClock }
export type SearchWakeMessage = StageMessage<SearchWakePayload>;

/** A SQL executor, with the transaction method of a pool or PGlite when the caller is not already inside one. */
export type SearchSql = SqlExecutor & { transaction?: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T> };

/** Runs `work` in one transaction: the executor's own, or the caller's when it passed a transaction-scoped one. */
function atomically<T>(sql: SearchSql, work: (tx: SqlExecutor) => Promise<T>): Promise<T> {
  return typeof sql.transaction === "function" ? sql.transaction(work) : work(sql);
}

/** Design §2: `search␟<org>␟<ws>␟<harness>␟<canonical session id>`. */
export const searchWorkKey = (s: { orgId: string; workspaceId: string; harness: string; sessionId: string }) =>
  ["search", s.orgId, s.workspaceId, s.harness, s.sessionId].join("\u001f");

/** The scope's one dead letter (§3.3, §3.3b): the repository's canonical id, keyed by the scope's work key. */
export const searchLetterId = (m: Pick<StageMessage, "ctx" | "workKey">) => deadLetterId(SEARCH_INDEX_STAGE, m.ctx, m.workKey);

const SAFE_EPOCH = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n) && n >= 0;

/** A well-formed §2 wake: a StageMessage of this stage, its own scope's work key, an epoch and a clock. Throws otherwise. */
export function checkedSearchWake(message: unknown): SearchWakeMessage {
  const m = message as SearchWakeMessage;
  const fail = (why: string): never => { throw new Error(`search wake: ${why}`); };
  if (!isStageMessage(m) || m.stage !== SEARCH_INDEX_STAGE) fail("not a search-index StageMessage");
  const { harness, sessionId } = m.ctx;
  if (typeof harness !== "string" || !harness || typeof sessionId !== "string" || !sessionId) fail("scope incomplete");
  const scope = { orgId: m.ctx.orgId, workspaceId: m.ctx.workspaceId, harness: harness!, sessionId: sessionId! };
  if (Object.values(scope).some((v) => v.includes("\u001f"))) fail("ambiguous scope");
  if (m.workKey !== searchWorkKey(scope)) fail("work key is not the scope's");
  const p = m.payload as Partial<SearchWakePayload> | null;
  if (!p || !SAFE_EPOCH(p.epoch)) fail("epoch missing");
  const clock = p!.clock;
  if (!clock || (clock.source !== "publish" && clock.source !== "pointer") || !Number.isFinite(Date.parse(clock.at))) fail("clock missing");
  return m;
}

const canonicalScope = (m: SearchWakeMessage): SessionScope =>
  ({ orgId: m.ctx.orgId, workspaceId: m.ctx.workspaceId, harness: m.ctx.harness!, sessionId: m.ctx.sessionId! });

/** One stage event, written in the caller's transaction (the same shape as the pipeline's event sinks). */
async function recordEvent(tx: SqlExecutor, m: StageMessage, status: PipelineStatus, attempt: number, discriminator: string,
  extra: { error?: { code: string; retryable: boolean }; attrs?: Record<string, unknown> } = {}) {
  const attemptNo = Math.max(1, Math.floor(attempt) || 1);
  await tx.query(
    `INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
     VALUES ($1::uuid, 'search-index', $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8, $9, $10, $11)`,
    [m.ctx.traceId, status, extra.error ? JSON.stringify({ name: "SearchJobError", ...extra.error }) : null,
      JSON.stringify({ work_key: m.workKey, ...extra.attrs }), attemptNo,
      pipelineEventKey(m.ctx.traceId, SEARCH_INDEX_STAGE, status, attemptNo, discriminator),
      m.ctx.orgId, m.ctx.workspaceId, m.ctx.machineId ?? null, m.ctx.harness ?? null, m.ctx.sessionId ?? null],
  );
}

// ---------------------------------------------------------------------------------------------------------
// §3.1 enqueue
// ---------------------------------------------------------------------------------------------------------

/**
 * §3.1 over a jsonb array of StageMessages: only a STRICTLY newer epoch replaces the message (and with it the row's
 * trace_id, so the row's trace is always its message's), resets the budget, clears dlq and reopens an unleased row
 * now. An equal or older epoch (a catch-up re-visit, a replay) changes nothing: in particular it never pulls an
 * unleased row's visible_at forward, so a retry backoff holds.
 */
const ENQUEUE_SQL = `
INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message, visible_at)
SELECT m->>'stage', m->>'workKey', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
       m->'ctx'->>'sessionId', NULL, m, now()
  FROM jsonb_array_elements($1::jsonb) AS x(m)
ON CONFLICT (work_key) WHERE stage = 'search-index'
DO UPDATE SET
  message = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                 THEN EXCLUDED.message ELSE pipeline_jobs.message END,
  -- The trace travels with the message: replaced only when the message is.
  trace_id = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                  THEN EXCLUDED.trace_id ELSE pipeline_jobs.trace_id END,
  -- A NEW epoch is new work and gets its own budget. The active lease is kept; settlement compares epochs.
  attempts = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                  THEN 0 ELSE pipeline_jobs.attempts END,
  dlq = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
             THEN false ELSE pipeline_jobs.dlq END,
  -- Only a strictly newer epoch makes an unleased row visible now. A leased row keeps its lease (settlement
  -- reopens it if the wanted epoch moved on during the pass); an equal or older epoch keeps any backoff.
  visible_at = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                         AND pipeline_jobs.lease_id IS NULL
                    THEN least(pipeline_jobs.visible_at, now()) ELSE pipeline_jobs.visible_at END
RETURNING id::text AS id`;

/** §3.1: coalesce one wake into its scope's single row (inserted, or merged by max epoch). */
export async function enqueueSearchWake(sql: SqlExecutor, message: StageMessage): Promise<{ id: string }> {
  const checked = checkedSearchWake(message);
  const { rows } = await sql.query<{ id: string }>(ENQUEUE_SQL, [JSON.stringify([checked])]);
  return { id: rows[0]!.id };
}

async function enqueueSearchWakes(sql: SqlExecutor, messages: readonly SearchWakeMessage[]): Promise<number> {
  if (!messages.length) return 0;
  const { rows } = await sql.query<{ id: string }>(ENQUEUE_SQL, [JSON.stringify(messages.map((m) => checkedSearchWake(m)))]);
  return rows.length;
}

// ---------------------------------------------------------------------------------------------------------
// §3.2 claim, §3.3 settlement: the runtime boundary (migration 20261105062300)
// ---------------------------------------------------------------------------------------------------------

/** What a claim hands the pass and its settlement: the lease and the server's stored claim-time snapshot. */
export interface ClaimedSearchJob {
  id: string;
  lease: string;
  /** The attempt this claim counts (1 on the first delivery of an epoch). */
  attempts: number;
  claimedEpoch: number;
  claimedMessage: SearchWakeMessage;
}

/**
 * §3.2 through sessions.claim_search_jobs_v1 (SECURITY DEFINER): up to n visible, non-dlq search-index rows, a lease
 * per row (the server's 300 s), a counted attempt, and the claim receipt the server keeps. The server clamps n to
 * 1..10. What is returned is the STORED receipt (message, epoch, attempt); settlement reads it back from the server,
 * never from here.
 */
export async function claimSearchJobs(sql: SqlExecutor, n: number): Promise<ClaimedSearchJob[]> {
  const limit = Math.max(0, Math.floor(n));
  if (!limit) return [];
  const { rows } = await sql.query<{ id: string; lease: string; attempts: number; epoch: string; message: SearchWakeMessage }>(
    `SELECT c.job_id::text AS id, c.lease_id::text AS lease, c.attempts, c.epoch::text AS epoch, c.message
       FROM sessions.claim_search_jobs_v1($1::integer) AS c`,
    [limit]);
  return rows
    .map((r) => ({ id: r.id, lease: r.lease, attempts: Number(r.attempts), claimedEpoch: Number(r.epoch), claimedMessage: r.message }))
    .sort((a, b) => Number(BigInt(a.id) - BigInt(b.id)));
}

/**
 * The PROBE lane, through sessions.claim_search_probe_v1 (SECURITY DEFINER): at most one held job whose hourly probe
 * is due, only if the fleet probe bucket has a token. Separate from claimSearchJobs, so a probe never takes an
 * ordinary slot. Settled like any claim.
 */
export async function claimSearchProbe(sql: SqlExecutor): Promise<ClaimedSearchJob | undefined> {
  const { rows } = await sql.query<{ id: string; lease: string; attempts: number; epoch: string; message: SearchWakeMessage }>(
    `SELECT c.job_id::text AS id, c.lease_id::text AS lease, c.attempts, c.epoch::text AS epoch, c.message
       FROM sessions.claim_search_probe_v1() AS c`);
  const r = rows[0];
  return r ? { id: r.id, lease: r.lease, attempts: Number(r.attempts), claimedEpoch: Number(r.epoch), claimedMessage: r.message } : undefined;
}

/**
 * What the caller asks for. The server decides the rest: `{ retry }`'s delay is ignored (server backoff
 * min(900, 30 * 2^(attempt-1)) s) and `exhausted` is a retry the server exhausts only at its own max attempts (12).
 * `busy` (scope lock contention) never spends the failure budget and never parks: the server gives back the counted
 * attempt and holds the job 30 s, escalating at 2 h and then at most hourly. At 24 h of same-epoch contention it HOLDS
 * the job: one probe claim an hour, until a newer epoch or an operator (resumeSearchContention) releases it.
 */
export type SearchSettleOutcome = "done" | { retry: number } | "exhausted" | "busy";
/** A busy settlement's contention state, as the server reports it (search_job_contention). */
export interface SearchBusyState {
  escalated: boolean;
  /** Held by the 24 h cap (now or before); `held_now` the settlement that set it; `probe` an hourly probe's. */
  held: boolean;
  held_now: boolean;
  probe: boolean;
  contended_for_s: number;
  contention_count: number;
  delay_seconds: number;
}
export type SearchSettleResult =
  | { applied: false }
  | { applied: true; action: "deleted" | "reopened" | "retried" | "parked" | "parked_reopened" | "busy_reopened" }
  | ({ applied: true; action: "busy" } & SearchBusyState);
/**
 * The job and its lease. claimedEpoch, claimedMessage and attempts are accepted for compatibility (the accepted
 * tests and the Lambda pass a whole ClaimedSearchJob) and IGNORED: the server's claim receipt is authoritative.
 */
type SettleClaim = { id: string | number; lease: string; claimedEpoch?: unknown; claimedMessage?: unknown; attempts?: unknown };

/**
 * §3.3 (+ §3.3b completion) through sessions.settle_search_job_v1 (SECURITY DEFINER, one transaction): bound to the
 * stage, the job, the current unexpired lease and the receipt's recorded epoch and message. A lost, expired or
 * replaced lease, another stage's job or a second settlement returns `{ applied: false }` and changes nothing.
 */
export async function settleSearchJob(sql: SqlExecutor, claim: SettleClaim, outcome: SearchSettleOutcome): Promise<SearchSettleResult> {
  const wire = outcome === "done" || outcome === "busy" ? outcome : "retry";
  const { rows } = await sql.query<{ r: SearchSettleResult }>(
    "SELECT sessions.settle_search_job_v1($1::bigint, $2::uuid, $3::text) AS r", [String(claim.id), claim.lease, wire]);
  const r = rows[0]?.r;
  if (!r || r.applied !== true) return { applied: false };
  return r;
}

/**
 * Operator path (service_role or owner credentials; never the runtime role): release a job held by the 24 h contention
 * cap through sessions.resume_search_contention_v1, which restarts its contention episode and writes an audit event
 * naming the operator and the database identities. `{ applied: false }` while a lease is live or when it is not held.
 */
export async function resumeSearchContention(sql: SqlExecutor, jobId: string | number, operator: string): Promise<{ applied: boolean; reason?: string }> {
  const { rows } = await sql.query<{ r: { applied: boolean; reason?: string } }>(
    "SELECT sessions.resume_search_contention_v1($1::bigint, $2::text) AS r", [String(jobId), operator]);
  const r = rows[0]?.r;
  return r?.applied === true ? { applied: true } : { applied: false, ...(r?.reason ? { reason: r.reason } : {}) };
}

// ---------------------------------------------------------------------------------------------------------
// §3.3b replay
// ---------------------------------------------------------------------------------------------------------

/**
 * §3.3b: claim one `dead` scope letter for replay and re-enqueue its message through §3.1 (it coalesces with any
 * newer row), in one transaction. A letter that is not `dead` (replaying, resolved, absent) is a no-op.
 */
export async function replaySearchDeadLetter(sql: SearchSql, letterId: string, opts: { replayId?: string } = {}): Promise<{ replayed: boolean }> {
  const replayId = opts.replayId ?? crypto.randomUUID();
  return atomically(sql, async (tx) => {
    const { rows } = await tx.query<{ message: StageMessage; replay_count: number }>(
      `UPDATE sessions.pipeline_dead_letters
          SET state = 'replaying', replay_count = replay_count + 1, last_replay_id = $2::uuid, last_replay_at = now(), resolution = NULL
        WHERE id = $1 AND stage = 'search-index' AND state = 'dead'
       RETURNING message, replay_count`,
      [letterId, replayId]);
    if (!rows.length) return { replayed: false };
    const message = checkedSearchWake(rows[0]!.message);
    if (searchLetterId(message) !== letterId) throw new Error("search replay: the letter's message names another scope");
    await enqueueSearchWakes(tx, [message]);
    await recordEvent(tx, message, "replayed", 1, `${message.workKey}@${message.payload.epoch}:replay:${replayId}`,
      { attrs: { epoch: message.payload.epoch, dead_letter_id: letterId, replay_id: replayId, replay_count: rows[0]!.replay_count } });
    return { replayed: true };
  });
}

// ---------------------------------------------------------------------------------------------------------
// §6 catch-up
// ---------------------------------------------------------------------------------------------------------

export interface CatchUpSummary {
  visited: number;
  enqueued: number;
  /** Managed scopes whose publish clock is unprovable: no job, one clock_unprovable event each (§2). */
  refused: number;
  wrapped: boolean;
  lap: number;
}

/**
 * §6: one tick of the global keyset cursor, in one transaction. The page is the next `pageSize` stale scopes
 * (directory not ready, at least one live pointer row) after the cursor; each gets the §3.1 enqueue with its §2
 * clock. The cursor moves past every scope it returned, whatever happened to it; a short page wraps it to the
 * minimum and increments `lap`.
 */
export async function catchUpTick(deps: { sql: SearchSql; pageSize?: number }): Promise<CatchUpSummary> {
  const pageSize = Math.max(1, Math.floor(deps.pageSize ?? SEARCH_CATCHUP_DEFAULT_PAGE));
  return atomically(deps.sql, async (tx) => {
    let cursor = (await tx.query<{ org_id: string; workspace_id: string; harness: string; session_id: string; lap: string }>(
      "SELECT org_id, workspace_id, harness, session_id, lap::text AS lap FROM sessions.search_catchup_cursor FOR UPDATE")).rows[0];
    if (!cursor) {
      await tx.query("INSERT INTO sessions.search_catchup_cursor DEFAULT VALUES ON CONFLICT DO NOTHING");
      cursor = { org_id: "", workspace_id: "", harness: "", session_id: "", lap: "0" };
    }
    const { rows } = await tx.query<{
      org_id: string; workspace_id: string; harness: string; session_id: string; epoch: string;
      managed: boolean; publish_at: string | null; pointer_at: string | null; trace_id: string | null; revision: string | null;
    }>(
      `SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.event_projection_epoch::text AS epoch,
              live.managed, live.pointer_at, live.trace_id, live.revision, pub.publish_at
         FROM sessions.session_directory d
         CROSS JOIN LATERAL (
           SELECT to_char(max(i.indexed_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS pointer_at,
                  (array_agg(i.trace_id::text ORDER BY i.indexed_at DESC))[1] AS trace_id,
                  max(i.revision)::text AS revision,
                  bool_or(i.manifest_key LIKE '%/upload-generations/%'
                          OR EXISTS (SELECT 1 FROM sessions.ingest_publication_heads h WHERE h.current_key = i.manifest_key)) AS managed
             FROM sessions.session_index i
            WHERE i.org_id = d.org_id AND i.workspace_id = d.workspace_id AND i.harness = d.harness
              AND coalesce(i.canonical_session_id, i.session_id) = d.session_id AND i.deleted_at IS NULL
           HAVING count(*) > 0
         ) live
         -- §2's publish clock: the latest current live update of the canonical scope. Each head contributes only its
         -- OWN attempt: the attempt's tenant, user and origin equal that head's (attempt.origin_id = head.origin_id);
         -- heads of different origins may sit in one scope, but no head is ever clocked by another origin's attempt.
         CROSS JOIN LATERAL (
           SELECT to_char(max(a.published_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS publish_at
             FROM sessions.ingest_publication_heads h
             JOIN sessions.ingest_publication_images i ON i.manifest_key = h.current_key
             JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
              AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (h.org_id, h.workspace_id, h.user_id, h.origin_id)
             JOIN sessions.session_index p ON p.manifest_key = h.current_key AND p.deleted_at IS NULL
            WHERE h.state = 'live' AND h.org_id = d.org_id AND h.workspace_id = d.workspace_id AND h.physical_source = d.harness
              AND coalesce(p.canonical_session_id, p.session_id) = d.session_id
         ) pub
        WHERE d.event_projection_ready IS NOT TRUE
          AND (d.org_id, d.workspace_id, d.harness, d.session_id) > ($1, $2, $3, $4)
        ORDER BY d.org_id, d.workspace_id, d.harness, d.session_id
        LIMIT $5`,
      [cursor.org_id, cursor.workspace_id, cursor.harness, cursor.session_id, pageSize]);
    const wakes: SearchWakeMessage[] = [];
    let refused = 0;
    for (const r of rows) {
      const scope = { orgId: r.org_id, workspaceId: r.workspace_id, harness: r.harness, sessionId: r.session_id };
      const epoch = Number(r.epoch);
      if (!SAFE_EPOCH(epoch) || Object.values(scope).some((v) => v.includes("\u001f"))) {
        log.warn("search catch-up scope skipped", { stage: SEARCH_INDEX_STAGE, org_id: scope.orgId, reason: "scope_unrepresentable" });
        continue;
      }
      const traceId = r.trace_id && /^[0-9a-f-]{36}$/i.test(r.trace_id) ? r.trace_id : crypto.randomUUID();
      const message: SearchWakeMessage = {
        v: 1, stage: SEARCH_INDEX_STAGE,
        ctx: { traceId, orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: scope.sessionId },
        workKey: searchWorkKey(scope),
        ...(r.revision !== null && Number.isSafeInteger(Number(r.revision)) ? { revision: Number(r.revision) } : {}),
        payload: { epoch, clock: { source: "pointer", at: "" } },
      };
      if (r.managed) {
        // §2: a managed scope takes the publish clock, never the pointer clock. Unprovable: fail closed for
        // search only (no job), recorded as a durable refusal.
        if (!r.publish_at) {
          refused += 1;
          await recordEvent(tx, message, "skipped", 1, `catchup:${message.workKey}@${epoch}`,
            { error: { code: "clock_unprovable", retryable: false }, attrs: { epoch, source: "catchup" } });
          log.error("search catch-up clock unprovable", { stage: SEARCH_INDEX_STAGE, org_id: scope.orgId, workspace_id: scope.workspaceId,
            harness: scope.harness, session_id: scope.sessionId, epoch, reason: "clock_unprovable" });
          continue;
        }
        message.payload.clock = { source: "publish", at: r.publish_at };
      } else if (r.pointer_at) {
        message.payload.clock = { source: "pointer", at: r.pointer_at };
      } else continue;
      wakes.push(message);
    }
    const enqueued = await enqueueSearchWakes(tx, wakes);
    const wrapped = rows.length < pageSize;
    const last = rows.at(-1);
    const lap = Number(cursor.lap) + (wrapped ? 1 : 0);
    const next = wrapped || !last ? { org_id: "", workspace_id: "", harness: "", session_id: "" } : last;
    await tx.query(
      `UPDATE sessions.search_catchup_cursor SET org_id = $1, workspace_id = $2, harness = $3, session_id = $4, lap = $5::bigint, updated_at = now()`,
      [next.org_id, next.workspace_id, next.harness, next.session_id, String(lap)]);
    return { visited: rows.length, enqueued, refused, wrapped, lap };
  });
}

// ---------------------------------------------------------------------------------------------------------
// §4 the pass
// ---------------------------------------------------------------------------------------------------------

export type SearchJobOutcome = "indexed" | "unchanged" | "not_committed" | "erased" | "held" | "busy";
export interface SearchJobResult {
  outcome: SearchJobOutcome;
  /** §0: clock_timestamp at the search-state write − the publish clock; only for `source: publish`. */
  commitToSearchableMs?: number;
  /** The same for a pointer clock (non-managed catch-up): a separate series, never in the commit SLO. */
  pointerToSearchableMs?: number;
  reasons?: string[];
  search?: SearchIndexResult;
}

const BUSY: SearchJobResult = { outcome: "busy", reasons: ["scope_busy"] };

/** The physical scope indexSearchScope takes for a canonical scope: the canonical id as a layout key component. */
export const physicalSearchScope = (canonical: SessionScope): SessionScope => ({ ...canonical, sessionId: layoutComponent(canonical.sessionId) });

/**
 * §4: the decision in one transaction under the scope lock then the canonical lock (both try-locks; `busy` when
 * either is held), then indexSearchScope for the work.
 *   §4.2 no session_index row at all (live or tombstoned) and no fence: `not_committed`, nothing written;
 *   §4.3 alreadyCurrent: `unchanged`, one SQL read;
 *   §4.4 erase only when (a) no live row for the scope or a canonical sibling, (b) a fence or every row tombstoned,
 *        (c) the directory epoch under the lock is at or above the job's;
 *   §4.5 relayhistory_acceptance_missing (or any incomplete pass): `held`, never marked ready.
 */
export async function runSearchJob(message: StageMessage, deps: { search: SearchIndexDeps; now?: () => number }): Promise<SearchJobResult> {
  const m = checkedSearchWake(message);
  const canonical = canonicalScope(m);
  const scope = physicalSearchScope(canonical);
  const wanted = m.payload.epoch;
  const decision = await deps.search.transact(async (tx): Promise<SearchJobResult | "work"> => {
    const lock = await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [scopeLockKey(scope, "search")]);
    if (!lock.rows[0]?.ok) return BUSY;
    const canonicalLock = await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS ok",
      [["96bulls:directory-events", canonical.orgId, canonical.workspaceId, canonical.harness, canonical.sessionId].join("\u001f")]);
    if (!canonicalLock.rows[0]?.ok) return { outcome: "busy", reasons: ["canonical_scope_busy"] };
    const { rows } = await tx.query<{ live: number; keys: number; rows: number; epoch: string | null }>(
      `SELECT count(*) FILTER (WHERE i.deleted_at IS NULL)::int AS live,
              count(*) FILTER (WHERE i.deleted_at IS NULL AND i.session_id = $5)::int AS keys,
              count(*)::int AS rows,
              (SELECT d.event_projection_epoch::text FROM sessions.session_directory d
                WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4) AS epoch
         FROM sessions.session_index i
        WHERE i.org_id = $1 AND i.workspace_id = $2 AND i.harness = $3
          AND (coalesce(i.canonical_session_id, i.session_id) = $4 OR i.session_id = $5)`,
      [canonical.orgId, canonical.workspaceId, canonical.harness, canonical.sessionId, scope.sessionId]);
    const state = rows[0] ?? { live: 0, keys: 0, rows: 0, epoch: null };
    if (state.live === 0) {
      // §4.2: absence is never authority.
      if (!(await scopeEverCommitted(tx, scope, canonical.sessionId))) return { outcome: "not_committed", reasons: [NOT_COMMITTED] };
      // §4.4 (c): the pass never acts on a future it was not woken for; a lower directory epoch cannot happen
      // (epochs are monotonic), so it is retried rather than trusted.
      if (state.epoch === null || BigInt(state.epoch) < BigInt(wanted)) return { outcome: "busy", reasons: ["epoch_behind_wake"] };
      return "work";
    }
    // §4.3: one SQL read; a repeated hint costs nothing else.
    if (state.keys > 0 && (await alreadyCurrent(tx, scope, { canonicalId: canonical.sessionId }, state.keys, deps.search.outputFacts ? deps.search.ch : undefined)))
      return { outcome: "unchanged", reasons: [ALREADY_CURRENT] };
    return "work";
  });
  if (decision !== "work") return decision;
  // §4.2 again, under the final write locks: the managed pass opts in to the committed-state guard (the shared
  // S3-event path never does), so a scope that lost its rows between the decision and the work erases nothing.
  // The skip-current shadow probe (SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED) is for the S3-event path only: this
  // path's log line (searchJobLogFields) carries none of its fields, so a probe here would be locked work no
  // reading could see. The hosted runtime hands both paths the same search deps; the flag is dropped here.
  const { skipCurrentShadow: _shadowIsEventPathOnly, ...search } = deps.search;
  const result = await indexSearchScope(scope, { ...search, committedGuard: true });
  const out = (outcome: SearchJobOutcome): SearchJobResult => ({ outcome, reasons: result.reasons, search: result });
  if (result.busy || result.deferred) return out("busy");
  if (result.reasons.includes(NOT_COMMITTED)) return out("not_committed");
  if (!result.complete) return out("held");
  if (result.skipped) return out("unchanged");
  const done = out(result.reasons.includes("erased") ? "erased" : "indexed");
  const elapsed = (deps.now ?? Date.now)() - Date.parse(m.payload.clock.at);
  if (m.payload.clock.source === "publish") done.commitToSearchableMs = elapsed;
  else done.pointerToSearchableMs = elapsed;
  return done;
}

// ---------------------------------------------------------------------------------------------------------
// §8 park, precondition, archive
// ---------------------------------------------------------------------------------------------------------

/**
 * §8 park path (consumer flag OFF): in ONE fenced transaction, every search-index row goes into its scope letter
 * (the current message, state dead, un-resolved) and is deleted. The parked count is logged with the operator.
 */
export async function parkSearchJobs(sql: SearchSql, opts: { operator: string }): Promise<{ parked: number }> {
  const parked = await atomically(sql, async (tx) => {
    await tx.query("LOCK TABLE sessions.pipeline_jobs IN SHARE ROW EXCLUSIVE MODE");
    const { rows } = await tx.query<{ id: string; message: StageMessage }>(
      "SELECT id::text AS id, message FROM sessions.pipeline_jobs WHERE stage = 'search-index' ORDER BY id");
    if (!rows.length) return 0;
    const letters = rows.map((r) => ({ id: r.id, letter: searchLetterId(r.message) }));
    const inserted = await tx.query<{ id: string; dead_count: number }>(
      `INSERT INTO sessions.pipeline_dead_letters AS d
              (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
       SELECT x->>'letter', 'search-index', j.trace_id, j.org_id, j.workspace_id, j.session_id, j.machine_id, j.message, now(), now()
         FROM jsonb_array_elements($1::jsonb) AS x JOIN sessions.pipeline_jobs j ON j.id = (x->>'id')::bigint AND j.stage = 'search-index'
       ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, state = 'dead', last_dead_at = EXCLUDED.last_dead_at,
              dead_count = d.dead_count + 1, resolved_at = NULL, resolution = NULL
       RETURNING d.id, d.dead_count`,
      [JSON.stringify(letters)]);
    if (inserted.rows.length !== rows.length) throw new Error("search park: letter count differs from the parked rows");
    const deleted = await tx.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index' RETURNING id");
    if (deleted.rows.length !== rows.length) throw new Error("search park: deleted count differs from the parked rows");
    // A parked row's claim receipt (a straggler's lease) goes with it: no settlement can apply to a deleted row.
    await tx.query("DELETE FROM sessions.search_job_claims WHERE job_id = ANY($1::bigint[])", [rows.map((r) => r.id)]);
    await tx.query("DELETE FROM sessions.search_job_contention WHERE job_id = ANY($1::bigint[])", [rows.map((r) => r.id)]);
    const deadCount = new Map(inserted.rows.map((r) => [r.id, r.dead_count]));
    for (const r of rows) {
      const letter = searchLetterId(r.message);
      const epoch = (r.message.payload as { epoch?: unknown } | null)?.epoch;
      await recordEvent(tx, r.message, "dead_lettered", 1, `${r.message.workKey}@${String(epoch)}:park:${deadCount.get(letter)}`,
        { error: { code: "search_index_parked", retryable: true }, attrs: { epoch, dead_letter_id: letter } });
    }
    return rows.length;
  });
  // The parked count, with the operator and the time (the log line's own timestamp); there is no audit table.
  log.warn("search jobs parked", { stage: SEARCH_INDEX_STAGE, count: parked, state: "parked", reason: `operator:${opts.operator}` });
  return { parked };
}

/**
 * §8 [rev 6]: the down migration's precondition: 0 search-index rows in pipeline_jobs, pipeline_events AND
 * pipeline_dead_letters, and no claim receipt or contention row.
 */
export async function searchStageDownPrecondition(sql: SqlExecutor): Promise<boolean> {
  const { rows } = await sql.query<{ empty: boolean }>(
    `SELECT NOT EXISTS (SELECT 1 FROM sessions.pipeline_jobs WHERE stage = 'search-index')
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_events WHERE stage = 'search-index')
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_dead_letters WHERE stage = 'search-index')
        AND NOT EXISTS (SELECT 1 FROM sessions.search_job_claims)
        AND NOT EXISTS (SELECT 1 FROM sessions.search_job_contention) AS empty`);
  return rows[0]?.empty === true;
}

/**
 * §8 [rev 7, rev 9]: the failure-safe archive through its only write path, sessions.archive_search_stage_v1()
 * (SECURITY DEFINER, one transaction). It throws, changing nothing, while a search-index letter is not resolved or
 * a search-index job exists.
 */
export async function archiveSearchStage(sql: SqlExecutor): Promise<{ letters: number; events: number; sha256: string }> {
  const { rows } = await sql.query<{ archived: { batchId: string; letters: number; events: number; sha256: string } }>(
    "SELECT sessions.archive_search_stage_v1() AS archived");
  const a = rows[0]!.archived;
  log.warn("search stage archived", { stage: SEARCH_INDEX_STAGE, count: a.letters + a.events, state: "archived" });
  return { letters: Number(a.letters), events: Number(a.events), sha256: a.sha256 };
}

// ---------------------------------------------------------------------------------------------------------
// The settlement a pass result maps to (the consumer loop, lambda.ts)
// ---------------------------------------------------------------------------------------------------------

/**
 * What the consumer loop asks a pass to be settled with: done, or a retry (named `exhausted` past the attempt budget,
 * for the log). The delay and the exhaustion are the server's (settle_search_job_v1); these values are advisory.
 */
export function searchSettlementFor(result: SearchJobResult | { error: unknown }, attempts: number): SearchSettleOutcome {
  if ("error" in result || result.outcome === "held") {
    return attempts >= SEARCH_JOB_MAX_ATTEMPTS ? "exhausted" : { retry: searchJobBackoffSeconds(attempts) };
  }
  // Scope lock contention is the bounded `busy`; any other not-yet (an epoch behind the wake, a deferred stream) is a
  // short retry.
  if (result.outcome === "busy")
    return result.reasons?.some((r) => r === "scope_busy" || r === "canonical_scope_busy") ? "busy" : { retry: SEARCH_JOB_BUSY_RETRY_SECONDS };
  return "done";
}

/** Log fields of one settled pass (ids, codes, counts and durations only). */
export function searchJobLogFields(job: ClaimedSearchJob, result: SearchJobResult | { error: unknown }) {
  const ctx = job.claimedMessage.ctx;
  return {
    stage: SEARCH_INDEX_STAGE, org_id: ctx.orgId, workspace_id: ctx.workspaceId, harness: ctx.harness, session_id: ctx.sessionId,
    epoch: job.claimedEpoch, attempt: job.attempts,
    ...("error" in result
      ? { outcome: "error", ...errorFields(result.error) }
      : { outcome: result.outcome, reason: result.reasons?.join(",") || undefined,
          ...(result.commitToSearchableMs !== undefined ? { commit_to_searchable_ms: result.commitToSearchableMs } : {}),
          ...(result.pointerToSearchableMs !== undefined ? { pointer_to_searchable_ms: result.pointerToSearchableMs } : {}) }),
  };
}
