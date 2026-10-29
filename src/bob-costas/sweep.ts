/**
 * Summary sweep: finds indexed sessions with no summary row for a kind and
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

export async function findMissingSummaries(db: Pick<HoopDb, "query">, stage: (typeof SWEEP_STAGES)[number], limit: number): Promise<MissingRow[]> {
  const r = await db.query<MissingRow>(
    `SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.manifest_key,
            i.trace_id::text AS trace_id, i.revision::text AS revision, e.session_uuid::text AS session_uuid
       FROM sessions.session_index i
       LEFT JOIN sessions.session_index_ext e USING (org_id, workspace_id, harness, session_id, machine_id)
      WHERE i.deleted_at IS NULL AND i.manifest_key IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM sessions.session_summaries s
                         WHERE s.org_id=i.org_id AND s.workspace_id=i.workspace_id AND s.harness=i.harness
                           AND s.session_id=i.session_id AND s.machine_id=i.machine_id AND s.kind=$1)
        AND NOT EXISTS (SELECT 1 FROM sessions.pipeline_jobs j
                         WHERE j.stage=$2 AND NOT j.dlq AND j.org_id=i.org_id AND j.workspace_id=i.workspace_id
                           AND j.session_id=i.session_id AND j.machine_id=i.machine_id)
      ORDER BY i.indexed_at DESC NULLS LAST
      LIMIT $3`,
    [KIND[stage], stage, limit],
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

/** A `tick` for runWorker: sweeps at most every `everyMs`, the first time at once. */
export function summarySweep(opts: { everyMs?: number; limit?: number } = {}) {
  const everyMs = opts.everyMs ?? 300_000;
  const limit = opts.limit ?? 100;
  let last = 0;
  return async (db: HoopDb, now: Date) => {
    if (now.getTime() - last < everyMs) return;
    last = now.getTime();
    const queue = postgresPipelineQueue(db);
    for (const stage of SWEEP_STAGES) {
      const rows = await findMissingSummaries(db, stage, limit);
      for (const row of rows) await queue.send(await enrichMessage(sweepMessage(stage, row)));
      if (rows.length) log.info("summary sweep queued", { stage, count: rows.length });
    }
  };
}
