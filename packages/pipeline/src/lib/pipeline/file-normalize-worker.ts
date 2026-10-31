/**
 * The `file-normalize` consumer for a role with no table access
 * (`docs/event-count-writeback.md`). Semantics match `pollPostgresStage`, and the
 * same `handleStageQueue` runs each delivery, but every database touch goes through
 * the stage-fixed `sessions.file_normalize_*` functions. After the claim, each one is
 * bound to the held job and lease, so the sink, dead letter store and sender here are
 * built per delivery: they have nothing to name but that job.
 */
import {
  handleStageQueue,
  stageQueueName,
  type DeadLetter,
  type DeadLetterStore,
  type StageHandler,
  type StageJob,
  type StageMessage,
  type StageQueueDeps,
} from "./dlq.js";
import { PipelineError } from "./errors.js";
import type { PipelineEvent, PipelineEventSink } from "./events.js";
import type { PollResult, SqlExecutor } from "./queue.js";
import { STAGE_REGISTRY } from "./registry.js";
import {
  fileNormalizeHandler,
  type SessionTransform,
  type StageSenders,
} from "./stages.js";

const STAGE = "file-normalize" as const;

export interface FileNormalizeOptions {
  sql: SqlExecutor;
  transform: SessionTransform;
  /** Jobs claimed per poll. Default 10. */
  batchSize?: number;
  /** Keep above the slowest transform. Default 300. */
  leaseSeconds?: number;
  /** Wraps the handler, e.g. in a span. */
  wrap?: (handler: StageHandler) => StageHandler;
  deps?: Pick<StageQueueDeps, "reporter" | "logger" | "retryDelaySeconds" | "now">;
}

/**
 * Refuses a login that could reach the tables: a credential meant to be
 * queue-limited that isn't should stop the worker, not run quietly.
 */
export async function assertQueueLimitedRole(sql: SqlExecutor): Promise<void> {
  const { rows } = await sql.query<{ broad: boolean }>(
    `SELECT r.rolsuper OR r.rolbypassrls
            OR has_table_privilege(current_user, 'sessions.pipeline_jobs', 'SELECT, INSERT, UPDATE, DELETE')
            OR has_table_privilege(current_user, 'sessions.session_index', 'SELECT, INSERT, UPDATE, DELETE') AS broad
       FROM pg_roles r WHERE r.rolname = current_user`,
  );
  if (rows[0]?.broad !== false)
    throw new PipelineError(
      "worker_role_too_broad",
      "the file-normalize login must have no table access",
      false,
    );
}

function jobSink(sql: SqlExecutor, job: StageJob): PipelineEventSink {
  return {
    kind: "file-normalize",
    async record(events: PipelineEvent[]) {
      for (const event of events) {
        if (event.stage !== STAGE) continue;
        await sql.query(
          `SELECT sessions.file_normalize_record_event($1::bigint, $2::uuid, $3, $4::integer,
             $5::jsonb, $6::jsonb, $7::integer, $8)`,
          [
            job.id,
            job.leaseId,
            event.status,
            event.attempt,
            event.error ? JSON.stringify(event.error) : null,
            JSON.stringify(event.attrs ?? {}),
            event.durationMs ?? null,
            event.idempotencyKey,
          ],
        );
      }
    },
    byTrace: async () => {
      throw new PipelineError("not_supported", "this role reads no events", false);
    },
  };
}

function jobLetters(sql: SqlExecutor, job: StageJob): DeadLetterStore {
  const unsupported = async (): Promise<never> => {
    throw new PipelineError("not_supported", "replay runs elsewhere", false);
  };
  return {
    async quarantine(message: StageMessage, at: Date): Promise<DeadLetter> {
      const { rows } = await sql.query<{ letter_id: string; letter_dead_count: number }>(
        "SELECT * FROM sessions.file_normalize_quarantine($1::bigint, $2::uuid)",
        [job.id, job.leaseId],
      );
      const stamp = at.toISOString();
      return {
        id: rows[0]!.letter_id,
        stage: STAGE,
        traceId: message.ctx.traceId,
        workspaceId: message.ctx.workspaceId,
        message,
        state: "dead",
        firstDeadAt: stamp,
        lastDeadAt: stamp,
        deadCount: rows[0]!.letter_dead_count,
        replayCount: 0,
      };
    },
    async resolve() {
      await sql.query("SELECT sessions.file_normalize_resolve($1::bigint, $2::uuid)", [
        job.id,
        job.leaseId,
      ]);
    },
    list: unsupported,
    claimForReplay: unsupported,
    releaseReplay: unsupported,
  };
}

function jobSenders(sql: SqlExecutor, job: StageJob): StageSenders {
  return {
    async send(stage, message) {
      if (stage !== "normalized-ready")
        throw new PipelineError(
          "stage_not_allowed",
          "this worker sends only normalized-ready",
          false,
        );
      await sql.query(
        "SELECT sessions.file_normalize_enqueue_ready($1::bigint, $2::uuid, $3::jsonb)",
        [job.id, job.leaseId, JSON.stringify(message)],
      );
    },
  };
}

/** Claims and handles one batch. Call in a loop; it never waits for work. */
export async function pollFileNormalize(
  options: FileNormalizeOptions,
): Promise<PollResult> {
  const { sql, transform } = options;
  const leaseSeconds = Math.max(1, Math.floor(options.leaseSeconds ?? 300));
  const leaseId = crypto.randomUUID();
  const { rows: claimed } = await sql.query<{ id: string }>(
    "SELECT id::text AS id FROM sessions.file_normalize_claim($1::integer, $2::integer, $3::uuid)",
    [Math.max(1, options.batchSize ?? 10), leaseSeconds, leaseId],
  );
  const result: PollResult = {
    claimed: claimed.length,
    acked: 0,
    retried: 0,
    exhausted: 0,
  };
  for (const [index, { id }] of claimed.entries()) {
    if (index + 1 < claimed.length)
      await sql.query(
        "SELECT sessions.file_normalize_renew($1::bigint[], $2::uuid, $3::integer)",
        [claimed.slice(index + 1).map((r) => r.id), leaseId, leaseSeconds],
      );
    const {
      rows: [row],
    } = await sql.query<{ dlq: boolean; attempts: number; message: StageMessage }>(
      "SELECT dlq, attempts, message FROM sessions.file_normalize_start($1::bigint, $2::uuid, $3::integer)",
      [id, leaseId, leaseSeconds],
    );
    // The lease was lost meanwhile, and the job is someone else's now.
    if (!row) continue;
    const job: StageJob = { id, leaseId };
    const handler = fileNormalizeHandler(transform, jobSenders(sql, job));
    let outcome: { retry?: number } | undefined;
    await handleStageQueue(
      {
        queue: stageQueueName(STAGE, row.dlq ? "-dlq" : ""),
        messages: [
          {
            body: row.message,
            attempts: row.attempts,
            job,
            ack: () => {
              outcome ??= {};
            },
            retry: (retry?: { delaySeconds?: number }) => {
              outcome ??= { retry: retry?.delaySeconds ?? 0 };
            },
          },
        ],
      },
      {
        ...options.deps,
        handlers: { [STAGE]: options.wrap ? options.wrap(handler) : handler },
        sink: jobSink(sql, job),
        deadLetters: jobLetters(sql, job),
      },
    );
    // Neither ack nor retry: leave the lease to run out and redeliver.
    if (!outcome) continue;
    if (outcome.retry === undefined) {
      const { rows } = await sql.query<{ ok: boolean }>(
        "SELECT sessions.file_normalize_ack($1::bigint, $2::uuid) AS ok",
        [id, leaseId],
      );
      if (rows[0]?.ok) result.acked += 1;
      continue;
    }
    const { rows } = await sql.query<{ state: string }>(
      "SELECT sessions.file_normalize_retry($1::bigint, $2::uuid, $3::integer) AS state",
      [id, leaseId, Math.min(900, Math.floor(outcome.retry))],
    );
    if (rows[0]?.state === "exhausted") result.exhausted += 1;
    else if (rows[0]?.state === "retried") result.retried += 1;
  }
  return result;
}

/** The SQL's budget is fixed in the function; a test pins it to the registry. */
export const FILE_NORMALIZE_MAX_ATTEMPTS = STAGE_REGISTRY[STAGE].maxAttempts;
