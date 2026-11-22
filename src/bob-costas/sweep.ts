/**
 * Summary sweep: finds indexed sessions with no summary row for a kind (or a
 * ready summary still under an older master key, after a rotation) and
 * queues `summary-short` / `summary-long` for them through the normal pipeline
 * queue (retries, DLQ and spans as usual). Covers rows removed by migration 0004
 * (plaintext summaries deleted) and any summary that was never produced.
 * Skips a session that already has a job waiting for that stage, so repeated
 * sweeps never pile up work. Ids only; nothing here reads session content.
 */
import { postgresPipelineQueue, type StageMessage } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import type { WorkspaceKeyring } from "./summary-objects.ts";
import { AUDIT_RELATION, hasColumns, hasRelation, SUMMARY_COVERAGE_COLUMNS } from "../summary-regen/detect.ts";
import { logRegenQueued, purgeRegenAudit, writeRegenAudit, type RegenAuditEntry } from "../summary-regen/audit.ts";
import { maxDueAttempts } from "../summary-regen/backoff.ts";
import { regenActive, type RegenReason } from "../summary-regen/settings.ts";
import { bobCostasRegenFromEnv, REGEN_WORK_SUFFIX, type BobCostasRegen } from "./regen.ts";
import { boundedSweepFromEnv, boundedWalks, findMissingPage, findRegenPage, type BoundedSweepSettings } from "./sweep-bounded.ts";

export const SWEEP_STAGES = ["summary-short", "summary-long"] as const;
const KIND = { "summary-short": "short", "summary-long": "long" } as const;

interface MissingRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  machine_id: string;
  manifest_key: string;
  trace_id: string;
  revision: string;
  session_uuid: string | null;
}

export async function findMissingSummaries(db: Pick<HoopDb, "query">, stage: (typeof SWEEP_STAGES)[number], limit: number, currentKeyId: string): Promise<MissingRow[]> {
  const r = await db.query<MissingRow>(
    `SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.manifest_key,
            i.trace_id::text AS trace_id, i.revision::text AS revision, e.session_uuid::text AS session_uuid
       FROM sessions.session_index i
       LEFT JOIN sessions.session_index_ext e USING (org_id, workspace_id, harness, session_id, machine_id)
      WHERE i.deleted_at IS NULL AND i.manifest_key IS NOT NULL
        -- Needs work: no row, or a ready row still under an older key (rotation).
        AND NOT EXISTS (SELECT 1 FROM sessions.session_summaries s
                         WHERE s.org_id=i.org_id AND s.workspace_id=i.workspace_id AND s.harness=i.harness
                           AND s.session_id=i.session_id AND s.machine_id=i.machine_id AND s.kind=$1
                           AND (s.status <> 'ready' OR s.key_id = $4))
        -- Not already queued.
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_jobs j
                         WHERE j.stage=$2 AND NOT j.dlq AND j.org_id=i.org_id AND j.workspace_id=i.workspace_id
                           AND j.session_id=i.session_id AND j.machine_id=i.machine_id)
        -- Not dead-lettered: a permanent failure waits for a person (replay), not every sweep.
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_dead_letters d
                         WHERE d.stage=$2 AND d.resolved_at IS NULL AND d.org_id=i.org_id AND d.workspace_id=i.workspace_id
                           AND d.session_id=i.session_id AND d.machine_id=i.machine_id)
      ORDER BY i.indexed_at DESC NULLS LAST
      LIMIT $3`,
    [KIND[stage], stage, limit, currentKeyId],
  );
  return r.rows;
}

export function sweepMessage(stage: (typeof SWEEP_STAGES)[number], row: MissingRow): StageMessage {
  const revision = Number(row.revision);
  return {
    v: 1,
    stage,
    ctx: {
      traceId: row.trace_id,
      orgId: row.org_id,
      workspaceId: row.workspace_id,
      machineId: row.machine_id,
      sessionId: row.session_id,
      harness: row.harness,
      ...(row.session_uuid ? { session_uuid: row.session_uuid } : {}),
    } as StageMessage["ctx"],
    workKey: `${row.manifest_key}@summary-sweep:${revision}`,
    revision,
    payload: {
      traceId: row.trace_id,
      workspaceId: row.workspace_id,
      machineId: row.machine_id,
      harness: row.harness,
      sessionId: row.session_id,
      manifestKey: row.manifest_key,
    },
  };
}

interface RegenDueRow extends MissingRow {
  reason: "config_changed" | "retry_backoff" | "turns" | "stamp";
  ended: boolean;
  prior_status: string;
  prior_failure_code: string | null;
  prior_pipeline_version: string | null;
}

/**
 * Summaries regeneration (push A): ready rows that are due again although their revision is
 * summarised. One query per stage, only when a coverage flag is on and hoopdb 0006 is applied:
 * - config_changed (2b): degraded under a different summariser config hash;
 * - retry_backoff (2): degraded, still on the ladder (attempts <= L) and next_retry_at passed;
 *   a terminal row (next_retry_at NULL) is never selected, however far the clock moves;
 * - turns (1): an ended (done/abandoned) or idle session whose row is behind the index revision;
 * - stamp (1): the same, but the row is uncovered (covered_turns NULL, written before the migration)
 *   and current by revision. It is queued only so the handler stamps its coverage with the guarded
 *   UPDATE (no model call); it is not a regeneration, so it is neither logged as one nor audited.
 * The same live-job and DLQ guards as the missing sweep, so a due row is queued once.
 */
export async function findRegenDue(
  db: Pick<HoopDb, "query">,
  stage: (typeof SWEEP_STAGES)[number],
  limit: number,
  regen: BobCostasRegen,
  now: Date,
): Promise<RegenDueRow[]> {
  const { turns, retry } = regen.settings;
  const r = await db.query<RegenDueRow>(
    `SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.manifest_key,
            i.trace_id::text AS trace_id, i.revision::text AS revision, e.session_uuid::text AS session_uuid,
            CASE WHEN $4::boolean AND s.degraded AND s.config_hash IS DISTINCT FROM $5 THEN 'config_changed'
                 WHEN $6::boolean AND s.degraded AND s.attempts BETWEEN 1 AND $7 AND s.next_retry_at <= $8::timestamptz THEN 'retry_backoff'
                 WHEN s.covered_turns IS NULL AND s.revision = i.revision THEN 'stamp'
                 ELSE 'turns' END AS reason,
            (i.state IN ('done', 'abandoned')) AS ended,
            s.status AS prior_status, s.failure_code AS prior_failure_code, s.pipeline_version AS prior_pipeline_version
       FROM sessions.session_index i
       JOIN sessions.session_summaries s
         ON s.org_id=i.org_id AND s.workspace_id=i.workspace_id AND s.harness=i.harness
        AND s.session_id=i.session_id AND s.machine_id=i.machine_id AND s.kind=$1
       LEFT JOIN sessions.session_index_ext e
         ON e.org_id=i.org_id AND e.workspace_id=i.workspace_id AND e.harness=i.harness
        AND e.session_id=i.session_id AND e.machine_id=i.machine_id
      WHERE i.deleted_at IS NULL AND i.manifest_key IS NOT NULL
        AND s.status = 'ready' AND s.revision <= i.revision
        AND (   ($4::boolean AND s.degraded AND s.config_hash IS DISTINCT FROM $5)
             OR ($6::boolean AND s.degraded AND s.attempts BETWEEN 1 AND $7 AND s.next_retry_at <= $8::timestamptz)
             OR ($9::boolean AND (i.state IN ('done', 'abandoned') OR i.last_activity_at < $10::timestamptz)
                 AND (s.covered_turns IS NULL OR s.revision < i.revision)))
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_jobs j
                         WHERE j.stage=$2 AND NOT j.dlq AND j.org_id=i.org_id AND j.workspace_id=i.workspace_id
                           AND j.session_id=i.session_id AND j.machine_id=i.machine_id)
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_dead_letters d
                         WHERE d.stage=$2 AND d.resolved_at IS NULL AND d.org_id=i.org_id AND d.workspace_id=i.workspace_id
                           AND d.session_id=i.session_id AND d.machine_id=i.machine_id)
      ORDER BY i.indexed_at DESC NULLS LAST
      LIMIT $3`,
    [
      KIND[stage],
      stage,
      limit,
      retry.configReset,
      regen.configHash,
      retry.enabled,
      maxDueAttempts(retry.backoffSeconds),
      now.toISOString(),
      turns.enabled,
      new Date(now.getTime() - turns.idleSeconds * 1000).toISOString(),
    ],
  );
  return r.rows;
}

function regenReason(reason: "config_changed" | "retry_backoff" | "turns", ended: boolean): RegenReason {
  if (reason !== "turns") return reason;
  return ended ? "ended_uncovered" : "idle_uncovered";
}

export function regenSweepMessage(stage: (typeof SWEEP_STAGES)[number], row: RegenDueRow): StageMessage {
  const base = sweepMessage(stage, row);
  const suffix =
    row.reason === "config_changed"
      ? REGEN_WORK_SUFFIX.config
      : row.reason === "retry_backoff"
        ? REGEN_WORK_SUFFIX.retry
        : row.reason === "stamp"
          ? REGEN_WORK_SUFFIX.stamp
          : REGEN_WORK_SUFFIX.turns;
  return { ...base, workKey: `${base.workKey}${suffix}` };
}

/** A `tick` for runWorker: sweeps at most every `everyMs`, the first time at once. */
export function summarySweep(opts: {
  keyring: WorkspaceKeyring;
  everyMs?: number;
  limit?: number;
  /** Regeneration flags; undefined reads them from the environment, null forces them off. */
  regen?: BobCostasRegen | null;
  /** SUMMARY_SWEEP_BOUNDED (sweep-bounded.ts); undefined reads it from the environment, null forces it off. */
  bounded?: BoundedSweepSettings | null;
}) {
  const everyMs = opts.everyMs ?? 300_000;
  const limit = opts.limit ?? 100;
  const regenConfig = opts.regen === undefined ? bobCostasRegenFromEnv() : opts.regen;
  const coverage = regenConfig && regenActive(regenConfig.settings) ? regenConfig : null;
  const auditOn = regenConfig?.settings.audit.enabled === true;
  const boundedConfig = opts.bounded === undefined ? boundedSweepFromEnv() : opts.bounded;
  const walks = boundedConfig ? boundedWalks(boundedConfig) : null;
  let last = 0;
  let lastPurge = 0;
  return async (db: HoopDb, now: Date) => {
    if (now.getTime() - last < everyMs) return;
    last = now.getTime();
    const queue = postgresPipelineQueue(db);
    // The master key id is the same for every workspace (hkdf1:<current master>).
    const currentKeyId = (await opts.keyring.currentKey({ orgId: "-", workspaceId: "-" })).keyId;
    if (walks) {
      // Bounded: one keyset page per pass, the same checks on that page only (sweep-bounded.ts).
      for (const stage of SWEEP_STAGES)
        await walks.run(`missing:${stage}`, now, limit,
          (bound) => findMissingPage(db, stage, KIND[stage], boundedConfig!.pageRows, currentKeyId, bound),
          async (rows, pass) => {
            for (const row of rows) await queue.send(await enrichMessage(sweepMessage(stage, row)));
            if (rows.length) log.info("summary sweep queued", { stage, count: rows.length, reason: `bounded_${pass}` });
          });
    } else {
      for (const stage of SWEEP_STAGES) {
        const rows = await findMissingSummaries(db, stage, limit, currentKeyId);
        for (const row of rows) await queue.send(await enrichMessage(sweepMessage(stage, row)));
        if (rows.length) log.info("summary sweep queued", { stage, count: rows.length });
      }
    }
    // Audit retention (4): at most hourly, bounded per call by the SQL function; self-host may run
    // Bob Costas without the brief worker, whose prune tick purges the same table.
    if (auditOn && now.getTime() - lastPurge >= 3_600_000 && (await hasRelation(db, AUDIT_RELATION))) {
      lastPurge = now.getTime();
      // Best effort, like the audit write: a failing purge never aborts the sweep's regen pass.
      const purged = await purgeRegenAudit(db, regenConfig!.settings.audit.retentionDays).catch((error: unknown) => {
        log.warn("summary regeneration audit purge failed", { stage: "summary-sweep", error_code: error instanceof Error ? error.name : "unknown" });
        return 0;
      });
      if (purged) log.info("summary regeneration audit purged", { stage: "summary-sweep", count: purged });
    }
    if (!coverage || !(await hasColumns(db, "sessions.session_summaries", SUMMARY_COVERAGE_COLUMNS))) return;
    const audit = coverage.settings.audit.enabled && (await hasRelation(db, AUDIT_RELATION));
    const queueRegen = async (stage: (typeof SWEEP_STAGES)[number], rows: RegenDueRow[]) => {
      const entries: RegenAuditEntry[] = [];
      let stamps = 0;
      for (const row of rows) {
        await queue.send(await enrichMessage(regenSweepMessage(stage, row)));
        if (row.reason === "stamp") {
          stamps += 1;
          continue;
        }
        const entry: RegenAuditEntry = {
          orgId: row.org_id,
          workspaceId: row.workspace_id,
          harness: row.harness,
          sessionId: row.session_id,
          system: "bob_costas",
          kind: KIND[stage],
          reason: regenReason(row.reason, row.ended),
          priorStatus: row.prior_status,
          priorFailureCode: row.prior_failure_code,
          priorPipelineVersion: row.prior_pipeline_version,
        };
        logRegenQueued(entry, stage);
        entries.push(entry);
      }
      if (stamps) log.info("summary coverage stamp queued", { stage, count: stamps });
      // One batch insert per stage; the flag gates only this write, never the enqueue or the log.
      if (audit && entries.length) {
        await writeRegenAudit(db, entries).catch((error: unknown) =>
          log.warn("summary regeneration audit write failed", { stage, count: entries.length, error_code: error instanceof Error ? error.name : "unknown" }));
      }
    };
    for (const stage of SWEEP_STAGES) {
      if (walks)
        await walks.run(`regen:${stage}`, now, limit,
          (bound) => findRegenPage(db, stage, KIND[stage], boundedConfig!.pageRows, coverage, now, bound),
          (rows) => queueRegen(stage, rows));
      else await queueRegen(stage, await findRegenDue(db, stage, limit, coverage, now));
    }
  };
}
