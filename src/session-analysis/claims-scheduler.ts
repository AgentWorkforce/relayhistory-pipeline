/**
 * The live claims scheduler (Session Intelligence M1-3, docs/session-analysis-runner.md, Claims scheduler): a
 * side tick of SERVICE=session-analysis (runner.ts) that enqueues an analysis for live sessions so their claims
 * (relayfacts, claims.ts) stay current. The Lambda path never runs it.
 *
 * Off unless every gate holds: SESSION_ANALYSIS_ENABLED, SUMMARY_DECISION_RECORDS_ENABLED,
 * SESSION_INTEL_CLAIMS_ENABLED and SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED each exactly "true", and a model is
 * configured. Off, the runner's tick is the analysis tick itself: no query, no log line.
 *
 * Per session it also needs the M3-1 'claims' control on for the workspace (src/shared/session-intel-controls.ts,
 * fail-closed: any error reads as off, counted). The candidates query already narrows to workspaces whose
 * control row is on; the control is read again here through the shared reader, and session_analysis_schedule_v1
 * checks it once more in the database (`control_off`, counted as skipped_control).
 *
 * The control also stops scheduled jobs already queued: before the worker runs a claimed job,
 * scheduledJobGate asks session_analysis_schedule_job_v1 whether the scheduler enqueued it and, if so, whether the
 * control is still on; off, the job is failed with `claims_control_off` (not retried) and no provider call is made.
 *
 * Enqueue is sessions.session_analysis_schedule_v1 (migration 20261105062180): the same admission checks as a
 * user request (analysis_request_v1's own blocks), plus the durable budgets, counted in its ledger under the
 * per-org lock. Over budget is skipped and counted, never deferred. The scheduler never selects a model: jobs
 * carry the pinned versions of src/session-analysis/versions.ts.
 *
 * Fail-open: a scheduler failure is caught here, counted and logged; it never fails the analysis tick, a
 * summary, a brief, a user-requested analysis or the read API (none of them call it).
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { SessionIntelControlsReader } from "../shared/session-intel-controls.ts";
import { log } from "../shared/log.ts";

/** Tuning bounds: a value out of range (or not a plain decimal) falls back to its default. */
export const CLAIMS_SCHEDULER_BOUNDS = {
  intervalSeconds: { min: 30, max: 3_600, fallback: 120 },
  minIntervalSeconds: { min: 60, max: 86_400, fallback: 600 },
  workspaceHourlyCap: { min: 1, max: 100, fallback: 5 },
  globalDailyCap: { min: 1, max: 1_000, fallback: 40 },
  dailyTokenCeiling: { min: 1, max: 50_000_000, fallback: 5_600_000 },
} as const;

/** Directory activity within this window makes a session live. */
export const CLAIMS_SCHEDULER_LIVE_SECONDS = 7_200;
/** Candidates read per tick; the database budgets still decide how many are enqueued. */
export const CLAIMS_SCHEDULER_CANDIDATES_MAX = 50;
/**
 * engine/session-evidence.ts MAXIMUM_INPUT_BYTES (not exported there; the engine is a pinned copy). A test reads the
 * engine source and keeps the two equal.
 */
export const CLAIMS_SCHEDULER_INPUT_BYTES = 480 * 1024;
/** The full analysis call has no output cap of its own (engine/model.ts sets one only for the summary). */
export const CLAIMS_SCHEDULER_RESPONSE_ALLOWANCE_TOKENS = 16_384;
/**
 * The worst-case reservation per job: the analysis input cap at about 4 bytes per token, plus the response
 * allowance. A reservation, not actual spend: reconciling against the job's recorded tokens is a follow-up.
 */
export const CLAIMS_SCHEDULER_RESERVE_TOKENS =
  Math.ceil(CLAIMS_SCHEDULER_INPUT_BYTES / 4) + CLAIMS_SCHEDULER_RESPONSE_ALLOWANCE_TOKENS;

export interface ClaimsSchedulerSettings {
  intervalSeconds: number;
  minIntervalSeconds: number;
  workspaceHourlyCap: number;
  globalDailyCap: number;
  dailyTokenCeiling: number;
}

const on = (env: NodeJS.ProcessEnv, name: string) => env[name] === "true";

function bounded(env: NodeJS.ProcessEnv, name: string, b: { min: number; max: number; fallback: number }): number {
  const raw = env[name];
  if (raw === undefined || !/^\d{1,9}$/.test(raw)) return b.fallback;
  const n = Number(raw);
  return n >= b.min && n <= b.max ? n : b.fallback;
}

export function claimsSchedulerSettings(env: NodeJS.ProcessEnv = process.env): ClaimsSchedulerSettings {
  const b = CLAIMS_SCHEDULER_BOUNDS;
  return {
    intervalSeconds: bounded(env, "SESSION_INTEL_CLAIMS_SCHEDULER_INTERVAL_SECONDS", b.intervalSeconds),
    minIntervalSeconds: bounded(env, "SESSION_INTEL_CLAIMS_SCHEDULER_MIN_INTERVAL_SECONDS", b.minIntervalSeconds),
    workspaceHourlyCap: bounded(env, "SESSION_INTEL_CLAIMS_WORKSPACE_HOURLY_CAP", b.workspaceHourlyCap),
    globalDailyCap: bounded(env, "SESSION_INTEL_CLAIMS_GLOBAL_DAILY_CAP", b.globalDailyCap),
    dailyTokenCeiling: bounded(env, "SESSION_INTEL_CLAIMS_DAILY_TOKEN_CEILING", b.dailyTokenCeiling),
  };
}

/** Every environment gate, each exactly "true". The model and the control are checked at run time. */
export function claimsSchedulerEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return on(env, "SESSION_ANALYSIS_ENABLED") && on(env, "SUMMARY_DECISION_RECORDS_ENABLED")
    && on(env, "SESSION_INTEL_CLAIMS_ENABLED") && on(env, "SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED");
}

export const CANDIDATES_SQL =
  "SELECT org_id, workspace_id, session_id FROM sessions.session_analysis_schedule_candidates_v1($1, $2, $3)";
export const SCHEDULE_SQL =
  "SELECT sessions.session_analysis_schedule_v1($1, $2, $3, $4, $5, $6, $7, $8) AS r";
export const SCHEDULED_JOB_SQL = "SELECT sessions.session_analysis_schedule_job_v1($1::uuid) AS gate";

/** runAnalysisJob's admit hook: run the claimed job, or fail it with a controlled code before any provider call. */
export type AnalysisJobAdmission = { run: true } | { run: false; code: string; retryable: boolean };

/**
 * The worker's gate for scheduled jobs. A job the scheduler did not enqueue always runs. A scheduled job runs only
 * while its workspace 'claims' control is on; off is `claims_control_off`, not retried. Without migration 062180
 * (42883) no job is scheduled, so every job runs. Any other error fails the job as retryable
 * (`claims_scheduler_gate_failed`): the control is default-deny, and the job is tried again after its backoff.
 */
export function scheduledJobGate(db: SqlExecutor, counts?: Record<string, number>): (job: { id: string }) => Promise<AnalysisJobAdmission> {
  const count = (k: string) => { if (counts) counts[k] = (counts[k] ?? 0) + 1; };
  return async (job) => {
    let gate: unknown;
    try {
      gate = (await db.query<{ gate: unknown }>(SCHEDULED_JOB_SQL, [job.id])).rows[0]?.gate;
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === "42883") return { run: true };
      count("scheduled_gate_failed");
      return { run: false, code: "claims_scheduler_gate_failed", retryable: true };
    }
    if (gate === "unscheduled" || gate === "on") return { run: true };
    if (gate === "off") { count("scheduled_control_off"); return { run: false, code: "claims_control_off", retryable: false }; }
    count("scheduled_gate_failed");
    return { run: false, code: "claims_scheduler_gate_failed", retryable: true };
  };
}

export interface ClaimsSchedulerCounts {
  selected: number;
  enqueued: number;
  skipped_budget: number;
  skipped_control: number;
  /** In flight, reused or no longer found: nothing to do, nothing spent. */
  skipped: number;
  failed: number;
}

const zero = (): ClaimsSchedulerCounts =>
  ({ selected: 0, enqueued: 0, skipped_budget: 0, skipped_control: 0, skipped: 0, failed: 0 });

/** One scheduler pass. Never throws: every failure is a count. */
export async function claimsSchedulerTick(deps: {
  db: SqlExecutor;
  controls: SessionIntelControlsReader;
  settings: ClaimsSchedulerSettings;
}): Promise<ClaimsSchedulerCounts> {
  const counts = zero();
  const s = deps.settings;
  let candidates: { org_id: string; workspace_id: string; session_id: string }[];
  try {
    candidates = (await deps.db.query<{ org_id: string; workspace_id: string; session_id: string }>(
      CANDIDATES_SQL, [CLAIMS_SCHEDULER_LIVE_SECONDS, s.minIntervalSeconds, CLAIMS_SCHEDULER_CANDIDATES_MAX])).rows;
  } catch {
    counts.failed += 1;
    return counts;
  }
  counts.selected = candidates.length;
  const control = new Map<string, boolean>();
  for (const c of candidates) {
    const key = JSON.stringify([c.org_id, c.workspace_id]);
    let claims = control.get(key);
    if (claims === undefined) {
      try { claims = (await deps.controls.read(c.org_id, c.workspace_id)).get("claims") === true; }
      catch { claims = false; }
      control.set(key, claims);
    }
    if (!claims) { counts.skipped_control += 1; continue; }
    try {
      const r = (await deps.db.query<{ r: { outcome?: unknown } }>(SCHEDULE_SQL, [
        c.org_id, c.workspace_id, c.session_id, s.workspaceHourlyCap, s.globalDailyCap, s.minIntervalSeconds,
        s.dailyTokenCeiling, CLAIMS_SCHEDULER_RESERVE_TOKENS,
      ])).rows[0]?.r;
      const outcome = r?.outcome;
      if (outcome === "enqueued") counts.enqueued += 1;
      else if (outcome === "over_budget") counts.skipped_budget += 1;
      else if (outcome === "control_off") counts.skipped_control += 1;
      else if (outcome === "in_flight" || outcome === "reused" || outcome === "not_found") counts.skipped += 1;
      else counts.failed += 1;
    } catch {
      counts.failed += 1;
    }
  }
  return counts;
}

/**
 * Wraps the runner's analysis tick: after it, at most once per interval, one scheduler pass in its own
 * try/catch, logged as counts. The analysis tick's result and errors pass through untouched.
 */
export function withClaimsScheduler<T>(
  tick: () => Promise<T>,
  scheduler: { run: () => Promise<ClaimsSchedulerCounts>; intervalMs: number; now?: () => number },
): () => Promise<T> {
  const now = scheduler.now ?? Date.now;
  let last = -Infinity;
  return async () => {
    let result: T;
    try {
      result = await tick();
    } finally {
      const at = now();
      if (at - last >= scheduler.intervalMs) {
        last = at;
        try {
          const c = await scheduler.run();
          // A pass with any failure is a warning (same fields), so alerting on warn sees it.
          (c.failed > 0 ? log.warn : log.info)("claims scheduler tick", {
            stage: "claims-scheduler",
            selected: c.selected, enqueued: c.enqueued, skipped_budget: c.skipped_budget,
            skipped_control: c.skipped_control, skipped: c.skipped, failed: c.failed,
            duration_ms: now() - at,
          });
        } catch {
          log.warn("claims scheduler tick failed", { stage: "claims-scheduler", failed: 1, duration_ms: now() - at });
        }
      }
    }
    return result;
  };
}
