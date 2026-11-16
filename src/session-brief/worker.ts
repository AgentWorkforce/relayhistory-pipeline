/**
 * The session-brief stage: model-written titles and descriptions, the port of
 * relayhistory-cloud's brief job (runSessionAnalysisTick, kind 'brief') onto 96Bulls.
 *
 * Fed by session-indexed: the HoopDb indexer arms a debounced wakeup per logical session
 * (store.ts armWakeup). In canonical mode (SESSION_BRIEF_CANONICAL_INPUT=true) there is no
 * indexer fanout; the sweep and the parked-wakeup recoveries in briefTick arm instead.
 * Each tick claims due wakeups and, per session:
 *   1. reads the live pointers; none left: done (deletion already removed the brief);
 *   2. applies relayhistory's limits: one attempt per session per 2 minutes (throttle,
 *      not counted), 30 per session per UTC day, and the org's daily pool of distinct
 *      sessions (rate_limited until the next UTC midnight; the previous brief stays;
 *      in canonical mode a parked wakeup is re-armed earlier by new pointer revisions or
 *      once admission is open again, see briefTick);
 *   3. builds the summary input from the scrubbed raw (input.ts); quarantined, empty and
 *      ineligible sessions are recorded without a model call;
 *   4. reuses the stored brief when the input fingerprint is unchanged (no model call);
 *   5. otherwise calls the model through relayhistory's summary prompt and contract, then
 *      scrubs, bounds and records the brief and projects it into the directory.
 * A retryable failure (provider 429/5xx/timeout, a missing chunk) is retried once after a
 * backoff; anything else, or a second failure, records `failed` with its code.
 *
 * Logs carry ids, statuses, counts and controlled error codes only: never transcript
 * text, the model input or output, or a credential.
 */
import { createHash } from "node:crypto";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { captureSafely, type AnalyticsPort } from "../shared/analytics.ts";
import { log } from "../shared/log.ts";
import { MODEL_PATTERN, type BriefConfig } from "./config.ts";
import { buildBriefInput } from "./input.ts";
import { summarizeSessionInput, type AnalysisModelTransport } from "./engine/model.ts";
import { SessionAnalysisProviderError } from "./engine/transport.ts";
import {
  BriefRejected,
  admit,
  claimWakeups,
  finishWakeup,
  livePointers,
  pruneAttempts,
  pruneStaleWakeups,
  rearmAdmissibleParked,
  rearmChangedParked,
  recordBrief,
  rescheduleWakeup,
  scrubBrief,
  storedBrief,
  sweepUnbriefed,
  sweepUnbriefedRegen,
  briefCoverage,
  briefSessionQuiet,
  type BriefCoverageWrite,
  type ClaimedWakeup,
} from "./store.ts";
import type { BriefRegen } from "./regen.ts";
import { AUDIT_RELATION, BRIEF_COVERAGE_COLUMNS, hasColumns, hasRelation } from "../summary-regen/detect.ts";
import { logRegenQueued, purgeRegenAudit, writeRegenAudit, type RegenAuditEntry } from "../summary-regen/audit.ts";
import { maxDueAttempts } from "../summary-regen/backoff.ts";
import { regenActive, type RegenReason } from "../summary-regen/settings.ts";
import {
  SESSION_BRIEF_CANONICAL_ENGINE_VERSION,
  SESSION_BRIEF_ENGINE_VERSION,
  SESSION_BRIEF_PROMPT_VERSION,
} from "./versions.ts";

export interface BriefDeps {
  db: HoopDb;
  reader: ManifestReader;
  store: ObjectStore;
  /** Null when no model is configured: nothing is claimed. */
  transport: AnalysisModelTransport | null;
  config: BriefConfig;
  /** Explicit self-host canonical-object input. Absent preserves the raw/export reader. */
  canonicalInput?: import("./canonical-input.ts").CanonicalBriefInput;
  /**
   * Summaries regeneration flags (src/session-brief/regen.ts). Absent or null: today's behaviour,
   * with no extra query and no new column written.
   */
  regen?: BriefRegen | null;
}

/** The regeneration settings when a coverage flag is on and the session_briefs columns exist. */
async function activeRegen(deps: BriefDeps): Promise<BriefRegen | null> {
  const r = deps.regen;
  if (!r || !regenActive(r.settings)) return null;
  return (await hasColumns(deps.db, "sessions.session_briefs", BRIEF_COVERAGE_COLUMNS)) ? r : null;
}

export type BriefResult =
  | "generated"
  | "reused"
  | "throttled"
  | "rate_limited"
  | "quarantined"
  | "empty"
  | "ineligible"
  | "retrying"
  | "failed"
  | "gone"
  | "stale"
  /** SUMMARY_REGEN_TURNS_ENABLED: fewer than N new turns while the session is active; due again after idle. */
  | "deferred";

const fingerprint = (input: unknown) =>
  createHash("sha256").update(JSON.stringify(input)).digest("hex");

/** Retry backoff after the n-th attempt: 60 s, then doubling, at most an hour. */
export const retryDelaySeconds = (attempt: number) =>
  Math.min(3_600, 60 * 2 ** Math.max(0, attempt - 1));

function failureOf(error: unknown): { code: string; retryable: boolean } {
  if (error instanceof SessionAnalysisProviderError)
    return { code: error.code, retryable: error.retryable };
  if (error instanceof BriefRejected) return { code: "brief_scrub_rejected", retryable: false };
  if (error instanceof PipelineError) {
    const code = /^[a-z][a-z0-9_]{0,63}$/.test(error.code) ? error.code : "session_collection_failed";
    return { code, retryable: error.retryable };
  }
  // relayhistory processor: a TypeError is the model's invalid output.
  if (error instanceof TypeError) return { code: "model_output_invalid", retryable: false };
  return { code: "brief_processing_failed", retryable: true };
}

/** One claimed wakeup, start to finish. Never throws for a session-level failure. */
export async function briefSession(deps: BriefDeps, w: ClaimedWakeup): Promise<BriefResult> {
  const { db, config } = deps;
  let canonicalSessionId: string | undefined;
  const publish = (revisions: Record<string, number>, outcome: Parameters<typeof recordBrief>[3], coverage?: BriefCoverageWrite) =>
    recordBrief(db, w, revisions, outcome, coverage ? { canonicalSessionId, coverage } : { canonicalSessionId });
  const pointers = await livePointers(db, w);
  if (!pointers.length) {
    await finishWakeup(db, w, { idleSeconds: config.idleSeconds, maxWaitSeconds: config.maxWaitSeconds });
    return "gone";
  }
  const stored = await storedBrief(db, w);
  if (stored && stored.attemptedAgoSeconds < config.throttleSeconds) {
    await rescheduleWakeup(
      db,
      w,
      { seconds: Math.ceil(config.throttleSeconds - stored.attemptedAgoSeconds) },
      { refundAttempt: true },
    );
    return "throttled";
  }
  const sourceRevisions = Object.fromEntries(pointers.map((p) => [p.machineId, p.revision]));
  const regen = await activeRegen(deps);
  const window = { idleSeconds: config.idleSeconds, maxWaitSeconds: config.maxWaitSeconds };
  // After a committed outcome, cleanup is separate: a cleanup failure never rewrites the
  // committed brief (the lease lapses and the wakeup is reclaimed, then reused or throttled).
  const finish = async () => {
    try {
      await finishWakeup(db, w, window);
    } catch (error) {
      log.warn("session brief wakeup cleanup failed", {
        stage: "session-brief",
        session_id: w.sessionId,
        error_code: error instanceof Error ? error.name : "unknown",
      });
    }
  };
  try {
    const built = deps.canonicalInput ? await deps.canonicalInput(db, w, pointers) : await buildBriefInput({ ...deps, readerMaxRuleset: config.readerMaxRuleset }, w, pointers);
    canonicalSessionId = built.canonicalSessionId;
    if (built.kind !== "ready") {
      await publish(built.sourceRevisions, { status: built.kind, error: built.error });
      await finish();
      return built.kind;
    }
    const print = fingerprint(built.input);
    // A brief from another prompt (v2 had no state) is regenerated even for the same input.
    if (stored?.title && stored.briefFingerprint === print && stored.promptVersion === SESSION_BRIEF_PROMPT_VERSION &&
        (!deps.canonicalInput || stored.engineVersion === SESSION_BRIEF_CANONICAL_ENGINE_VERSION)) {
      const { written } = await publish(built.sourceRevisions, {
        status: "reused",
        fingerprint: print,
      }, regen ? { configHash: regen.configHash } : undefined);
      await finish();
      return written ? "reused" : "stale";
    }
    // Summaries regeneration (1): a brief that already covers a known number of user turns is
    // regenerated before idle only when N more arrived; otherwise the wakeup waits for the idle
    // window. An ended or idle session, a first brief or a legacy row (covered_turns NULL) goes
    // ahead as today. Placed before admission, so a deferral never spends a cap or the budget.
    const turnsNow = built.input.messages.filter((m) => m.role === "user").length;
    let regenerationReason: RegenReason | undefined;
    if (regen?.settings.turns.enabled && stored?.title && stored.status === "ready") {
      const prior = await briefCoverage(db, w);
      if (prior && prior.coveredTurns !== null) {
        if (turnsNow - prior.coveredTurns >= regen.settings.turns.threshold) regenerationReason = "turns_gained";
        else {
          const quiet = await briefSessionQuiet(db, w, regen.settings.turns.idleSeconds);
          if (!quiet) {
            await rescheduleWakeup(db, w, { seconds: regen.settings.turns.idleSeconds }, { refundAttempt: true });
            log.info("session brief deferred", { stage: "session-brief", session_id: w.sessionId, count: turnsNow - prior.coveredTurns });
            return "deferred";
          }
          regenerationReason = quiet;
        }
      }
    }
    // Only a real model call is admitted and counted: empty, ineligible and reused
    // outcomes above never spend the per-session cap, the org budget or the global ceiling.
    const admission = await admit(db, w, {
      sessionDailyCap: config.sessionDailyCap,
      budget: config.backgroundDailyBudget,
      globalDailyCap: config.globalDailyCap,
    });
    if (!admission.admitted) {
      const { written } = await publish(built.sourceRevisions, {
        status: "rate_limited",
        error:
          admission.reason === "capped"
            ? "session_brief_capped"
            : admission.reason === "global"
              ? "session_brief_global_capped"
              : "session_brief_rate_limited",
      });
      // Canonical mode: a refusal dropped as stale (the pointers moved while the input was
      // built) is not recorded, so no parked-wakeup recovery would match it; retry after the
      // idle window instead of parking it until midnight. A recorded refusal parks as before.
      await rescheduleWakeup(db, w, !written && deps.canonicalInput ? { seconds: config.idleSeconds } : { nextUtcDay: true },
        { refundAttempt: true });
      return "rate_limited";
    }
    const transport = deps.transport;
    if (!transport) throw new SessionAnalysisProviderError("model_not_configured", false);
    const startedAt = Date.now();
    const summary = await summarizeSessionInput(built.input, transport, config.model);
    const scrubbed = scrubBrief(summary.summary);
    // The provider's reported model id, unless it is not one the column accepts.
    const model = MODEL_PATTERN.test(summary.model) ? summary.model : config.model;
    const engineVersion = deps.canonicalInput ? SESSION_BRIEF_CANONICAL_ENGINE_VERSION : SESSION_BRIEF_ENGINE_VERSION;
    const { written } = await publish(built.sourceRevisions, {
      status: "ready",
      text: {
        ...scrubbed,
        fingerprint: print,
        promptVersion: SESSION_BRIEF_PROMPT_VERSION,
        engineVersion,
        model,
        messageCount: built.input.messages.length,
        shortenedMessages: built.input.messages.filter((m) => m.truncated).length,
        inputTokens: summary.inputTokens,
        outputTokens: summary.outputTokens,
        durationMs: Date.now() - startedAt,
        generatedAt: new Date(),
      },
    }, regen ? { configHash: regen.configHash, coveredTurns: turnsNow, pipelineVersion: engineVersion } : undefined);
    await finish();
    log.info("session brief generated", {
      stage: "session-brief",
      org_id: w.orgId,
      workspace_id: w.workspaceId,
      harness: w.harness,
      session_id: w.sessionId,
      model_id: model,
      tokens_in: summary.inputTokens,
      tokens_out: summary.outputTokens,
      count: built.input.messages.length,
      ok: written,
      ...(regenerationReason ? { regeneration_reason: regenerationReason } : {}),
    });
    return written ? "generated" : "stale";
  } catch (error) {
    const failure = failureOf(error);
    if (failure.retryable && w.attempts < config.maxAttempts) {
      await rescheduleWakeup(db, w, { seconds: retryDelaySeconds(w.attempts) }, { error: failure.code });
      log.warn("session brief retrying", {
        stage: "session-brief",
        session_id: w.sessionId,
        error_code: failure.code,
        retryable: true,
        attempt: w.attempts,
      });
      return "retrying";
    }
    // Summaries regeneration (2): a final failure goes on the backoff ladder (retry_attempts,
    // next_retry_at) for sweepUnbriefed. The transport's retryable flag is not consulted here, so a
    // model_auth_failed (non-retryable in the transport) is retried at this layer only.
    await publish(sourceRevisions, { status: "failed", error: failure.code }, regen
      ? { configHash: regen.configHash, ...(regen.settings.retry.enabled
        ? { retry: { backoffSeconds: regen.settings.retry.backoffSeconds, configReset: regen.settings.retry.configReset } } : {}) }
      : undefined);
    await finish();
    log.warn("session brief failed", {
      stage: "session-brief",
      session_id: w.sessionId,
      error_code: failure.code,
      retryable: failure.retryable,
      attempt: w.attempts,
    });
    return "failed";
  }
}

/**
 * sweepUnbriefed with the regeneration predicates ((2) retry ladder, (2b) config change): one log
 * line per regeneration (session id hashed) and, with SUMMARY_REGEN_AUDIT_ENABLED and the table
 * present, one batch audit insert. Returns the number armed, as sweepUnbriefed does.
 */
async function sweepRegen(db: HoopDb, regen: BriefRegen, options: Parameters<typeof sweepUnbriefed>[2]): Promise<number> {
  const { retry, audit } = regen.settings;
  const rows = await sweepUnbriefedRegen(db, 100, {
    ...options,
    regen: { configHash: regen.configHash, configReset: retry.configReset, retry: retry.enabled, maxAttempts: maxDueAttempts(retry.backoffSeconds) },
  });
  const entries: RegenAuditEntry[] = [];
  for (const row of rows) {
    if (!row.reason) continue;
    const entry: RegenAuditEntry = {
      orgId: row.org_id,
      workspaceId: row.workspace_id,
      harness: row.harness,
      sessionId: row.session_id,
      system: "brief",
      kind: "brief",
      reason: row.reason,
      priorStatus: row.prior_status,
      priorFailureCode: row.prior_failure_code,
      priorPipelineVersion: row.prior_pipeline_version,
    };
    logRegenQueued(entry, "session-brief");
    entries.push(entry);
  }
  // The wakeups are armed and logged; a failed audit write is logged and never stops the tick.
  if (audit.enabled && entries.length && (await hasRelation(db, AUDIT_RELATION))) {
    await writeRegenAudit(db, entries).catch((error: unknown) =>
      log.warn("summary regeneration audit write failed", { stage: "session-brief", count: entries.length, error_code: error instanceof Error ? error.name : "unknown" }));
  }
  return rows.length;
}

/**
 * A `tick` for runWorker. Off unless SESSION_BRIEF_ENABLED=true; with no model configured
 * it prunes and sweeps but claims nothing, so wakeups wait (and expire after
 * SESSION_WAKE_MAX_AGE_HOURS) instead of spending their attempts.
 *
 * Housekeeping, at most once a minute: prune stale wakeups and old admission rows; in
 * canonical mode (SESSION_BRIEF_CANONICAL_INPUT=true) then re-arm rate_limited-parked
 * wakeups with new pointer revisions, then make due those that admit() would now admit.
 * The sweep follows on its own interval; in canonical mode it also counts a rate_limited
 * brief as unbriefed, so a parked wakeup pruned after SESSION_WAKE_MAX_AGE_HOURS is armed
 * again. Outside canonical mode none of these recoveries runs.
 */
export function briefTick(deps: Omit<BriefDeps, "db">, analytics?: AnalyticsPort) {
  let lastSweep = 0;
  let lastPrune = 0;
  let lastAuditPurge = 0;
  const canonical = deps.canonicalInput !== undefined;
  return async (db: HoopDb, now: Date): Promise<Record<BriefResult, number> | null> => {
    if (!deps.config.enabled) return null;
    const full: BriefDeps = { ...deps, db };
    if (now.getTime() - lastPrune >= 60_000) {
      lastPrune = now.getTime();
      const pruned = await pruneStaleWakeups(db, deps.config.wakeMaxAgeHours, 5000, deps.config.workspaceIds);
      await pruneAttempts(db, deps.config.workspaceIds);
      if (pruned) log.info("stale session brief wakeups dropped", { stage: "session-brief", count: pruned });
      // Summaries regeneration (4): audit retention, at most hourly, bounded per call by the SQL function.
      const auditRetention = deps.regen?.settings.audit.enabled ? deps.regen.settings.audit.retentionDays : 0;
      if (auditRetention && now.getTime() - lastAuditPurge >= 3_600_000 && (await hasRelation(db, AUDIT_RELATION))) {
        lastAuditPurge = now.getTime();
        const purged = await purgeRegenAudit(db, auditRetention);
        if (purged) log.info("summary regeneration audit purged", { stage: "session-brief", count: purged });
      }
      // Canonical mode only (no indexer arms wakeups there): recover wakeups parked until
      // the next UTC midnight by a rate_limited refusal (issue #494). Counts only are logged.
      if (canonical) {
        const { config } = deps;
        const changed = await rearmChangedParked(db, 100, { idleSeconds: config.idleSeconds,
          maxWaitSeconds: config.maxWaitSeconds, workspaceIds: config.workspaceIds });
        if (changed) log.info("session brief parked wakeups re-armed", { stage: "session-brief", count: changed, reason: "new_input" });
        const admissible = await rearmAdmissibleParked(db, config.batch, { sessionDailyCap: config.sessionDailyCap,
          budget: config.backgroundDailyBudget, globalDailyCap: config.globalDailyCap,
          throttleSeconds: config.throttleSeconds, workspaceIds: config.workspaceIds });
        if (admissible) log.info("session brief parked wakeups re-armed", { stage: "session-brief", count: admissible, reason: "admission_open" });
      }
    }
    if (deps.config.sweepSeconds > 0 && now.getTime() - lastSweep >= deps.config.sweepSeconds * 1000) {
      lastSweep = now.getTime();
      const sweepOptions = { includeChanged: canonical,
        refreshCanonicalInput: canonical, recoverJsonlInputFailures: canonical,
        recoverRateLimited: canonical, workspaceIds: deps.config.workspaceIds };
      const regen = await activeRegen(full);
      const armed = regen
        ? await sweepRegen(db, regen, sweepOptions)
        : await sweepUnbriefed(db, 100, sweepOptions);
      if (armed) log.info("session brief sweep armed", { stage: "session-brief", count: armed });
    }
    if (!deps.transport) return null;
    const started = performance.now();
    const counts = {} as Record<BriefResult, number>;
    const claimed = await claimWakeups(db, deps.config.batch, deps.config.leaseSeconds, deps.config.workspaceIds);
    for (const w of claimed) {
      const result = await briefSession(full, w);
      counts[result] = (counts[result] ?? 0) + 1;
    }
    // Only allowlisted log fields (src/shared/log.ts): the per-outcome map is returned, not logged.
    if (claimed.length) {
      captureSafely(analytics, {
        kind: "session_brief.tick_completed",
        claimed: claimed.length,
        generated: counts.generated ?? 0,
        reused: counts.reused ?? 0,
        rateLimited: counts.rate_limited ?? 0,
        throttled: counts.throttled ?? 0,
        retrying: counts.retrying ?? 0,
        failed: counts.failed ?? 0,
        durationMs: performance.now() - started,
      });
      log.info("session brief tick", {
        stage: "session-brief",
        claimed: claimed.length,
        failed: counts.failed ?? 0,
        skipped: (counts.throttled ?? 0) + (counts.rate_limited ?? 0) + (counts.deferred ?? 0),
      });
    }
    return counts;
  };
}
