/**
 * `PipelineQueue`: how a producer hands a stage its work, whatever carries it.
 *
 * - `cloudflare`: one Cloudflare Queue per stage (binding from `STAGE_REGISTRY`);
 *   the platform retries and moves exhausted messages to `<queue>-dlq`, and the
 *   Worker's `queue()` handler runs `handleStageQueue`.
 * - `postgres`: one table, `sessions.pipeline_jobs`, for every stage. Consumers
 *   claim with `FOR UPDATE SKIP LOCKED` under a lease, so any number of them can
 *   poll one stage without double delivery; an exhausted job is marked `dlq` and
 *   delivered once more as the stage's DLQ, which quarantines it in
 *   `pipeline_dead_letters`. Unlike a Cloudflare DLQ, a dead-letter delivery
 *   that fails is retried until it lands, never dropped.
 * - `memory`: tests and local runs.
 *
 * Both real backends feed the same `handleStageQueue`, so retry, permanent
 * failure, quarantine, events and replay behave the same on either. Messages
 * carry ids and object keys only, never session content.
 */
import {
  handleStageQueue,
  isStageMessage,
  stageQueueName,
  type StageMessage,
  type StageQueueDeps,
} from "./dlq.js";
import { PipelineError } from "./errors.js";
import { isQueuedStage, STAGE_REGISTRY, type QueuedStage } from "./registry.js";

export interface SendOptions {
  /** Hold the message this long before its first delivery. */
  delaySeconds?: number;
}

export interface PipelineQueue {
  readonly backend: "cloudflare" | "postgres" | "memory";
  /** Routes by `message.stage`, which must be a queued stage. */
  send(message: StageMessage, options?: SendOptions): Promise<void>;
  sendBatch(
    messages: readonly StageMessage[],
    options?: SendOptions,
  ): Promise<void>;
}

/** What the Postgres backend needs: PGlite, `pg` and the Neon pool all fit. */
export interface SqlExecutor {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}

function checked(message: StageMessage): QueuedStage {
  if (!isStageMessage(message) || !isQueuedStage(message.stage))
    throw new PipelineError(
      "invalid_stage_message",
      "not a message for a queued pipeline stage",
      false,
    );
  return message.stage;
}

function delayOf(options?: SendOptions) {
  const delay = options?.delaySeconds ?? 0;
  return Number.isFinite(delay) && delay > 0 ? Math.floor(delay) : 0;
}

/** Cloudflare caps a batch at 100 messages. */
const CLOUDFLARE_BATCH = 100;

export function cloudflarePipelineQueue(
  bindings: Partial<Record<QueuedStage, Queue<StageMessage>>>,
): PipelineQueue {
  const producer = (stage: QueuedStage) => {
    const queue = bindings[stage];
    // Retryable: the binding can arrive with the next deploy.
    if (!queue)
      throw new PipelineError(
        "stage_queue_unbound",
        `no queue bound for pipeline stage ${stage}`,
        true,
      );
    return queue;
  };
  return {
    backend: "cloudflare",
    async send(message, options) {
      const delaySeconds = delayOf(options);
      await producer(checked(message)).send(message, {
        contentType: "json",
        ...(delaySeconds ? { delaySeconds } : {}),
      });
    },
    async sendBatch(messages, options) {
      const delaySeconds = delayOf(options);
      const byStage = new Map<QueuedStage, StageMessage[]>();
      for (const message of messages) {
        const stage = checked(message);
        byStage.set(stage, [...(byStage.get(stage) ?? []), message]);
      }
      for (const [stage, group] of byStage) {
        const queue = producer(stage);
        for (let at = 0; at < group.length; at += CLOUDFLARE_BATCH)
          await queue.sendBatch(
            group.slice(at, at + CLOUDFLARE_BATCH).map((body) => ({
              body,
              contentType: "json" as const,
              ...(delaySeconds ? { delaySeconds } : {}),
            })),
          );
      }
    },
  };
}

export interface PostgresQueueOptions {
  /** Schema holding `pipeline_jobs`. Default `sessions`. */
  schema?: string;
}

function table(options: PostgresQueueOptions) {
  const schema = options.schema ?? "sessions";
  if (!/^[a-z_][a-z0-9_]*$/.test(schema))
    throw new Error(`bad pipeline queue schema ${schema}`);
  return `${schema}.pipeline_jobs`;
}

export function postgresPipelineQueue(
  sql: SqlExecutor,
  options: PostgresQueueOptions = {},
): PipelineQueue {
  const jobs = table(options);
  const insert = async (
    messages: readonly StageMessage[],
    send?: SendOptions,
  ) => {
    if (!messages.length) return;
    for (const message of messages) checked(message);
    await sql.query(
      `INSERT INTO ${jobs} (stage, work_key, trace_id, org_id, workspace_id,
                            session_id, machine_id, message, visible_at)
       SELECT m->>'stage', m->>'workKey', (m->'ctx'->>'traceId')::uuid,
              m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
              m->'ctx'->>'sessionId', m->'ctx'->>'machineId', m,
              now() + make_interval(secs => $2::integer)
         FROM jsonb_array_elements($1::jsonb) AS m`,
      [JSON.stringify(messages), delayOf(send)],
    );
  };
  return {
    backend: "postgres",
    send: (message, send) => insert([message], send),
    sendBatch: insert,
  };
}

export interface PostgresConsumerOptions extends PostgresQueueOptions {
  sql: SqlExecutor;
  stage: QueuedStage;
  deps: StageQueueDeps;
  /** Jobs claimed per poll. Default 10. */
  batchSize?: number;
  /**
   * How long a claim holds. A consumer that dies mid-batch leaves its jobs to
   * be claimed again after this; keep it above the slowest handler. Default 300.
   */
  leaseSeconds?: number;
  /** Deliveries before a job is dead-lettered. Default from `STAGE_REGISTRY`. */
  maxAttempts?: number;
}

export interface PollResult {
  claimed: number;
  acked: number;
  retried: number;
  /** Jobs that ran out of attempts and were handed to the DLQ path. */
  exhausted: number;
}

type ClaimedJob = {
  id: string;
  dlq: boolean;
  attempts: number;
  message: unknown;
};

/**
 * Claims up to `batchSize` visible jobs for one stage and runs them through
 * `handleStageQueue` one at a time, counting an attempt only when a job starts,
 * applying each ack or retry before the next starts, and renewing the lease on
 * the jobs still waiting, so a batch never outlives its claims. Call it on an interval (or in a loop while
 * `claimed > 0`); it never waits for work.
 */
export async function pollPostgresStage(
  options: PostgresConsumerOptions,
): Promise<PollResult> {
  const jobs = table(options);
  const { sql, stage } = options;
  const leaseSeconds = Math.max(1, Math.floor(options.leaseSeconds ?? 300));
  const maxAttempts = Math.max(
    1,
    options.maxAttempts ?? STAGE_REGISTRY[stage].maxAttempts,
  );
  const leaseId = crypto.randomUUID();
  // The claim only leases the jobs; an attempt is counted when each one
  // starts (below), so jobs that wait behind one that crashes its consumer do
  // not spend their attempts without ever reaching a handler.
  const { rows: claimed } = await sql.query<{ id: string }>(
    `WITH next AS (
       SELECT id FROM ${jobs}
        WHERE stage = $1 AND visible_at <= now()
        ORDER BY visible_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
     )
     UPDATE ${jobs} AS j
        SET visible_at = now() + make_interval(secs => $3::integer),
            lease_id = $4::uuid
       FROM next
      WHERE j.id = next.id
     RETURNING j.id::text AS id`,
    [stage, Math.max(1, options.batchSize ?? 10), leaseSeconds, leaseId],
  );
  const result: PollResult = {
    claimed: claimed.length,
    acked: 0,
    retried: 0,
    exhausted: 0,
  };
  for (const [index, { id }] of claimed.entries()) {
    // Earlier jobs took time: extend the claim on the ones still waiting.
    if (index + 1 < claimed.length)
      await sql.query(
        `UPDATE ${jobs}
            SET visible_at = now() + make_interval(secs => $3::integer)
          WHERE id = ANY($1::bigint[]) AND lease_id = $2::uuid`,
        [claimed.slice(index + 1).map((r) => r.id), leaseId, leaseSeconds],
      );
    // Start it: count the attempt and renew its lease. A main-queue job that
    // already had every delivery it is allowed (its consumer died or overran
    // the lease without an outcome) starts as a DLQ delivery instead, so a job
    // that kills its consumer still reaches quarantine. No row: the lease was
    // lost meanwhile and the job is someone else's now.
    const {
      rows: [row],
    } = await sql.query<ClaimedJob>(
      `UPDATE ${jobs}
          SET dlq = dlq OR attempts >= $4,
              attempts = CASE WHEN NOT dlq AND attempts >= $4 THEN 1
                              ELSE attempts + 1 END,
              visible_at = now() + make_interval(secs => $3::integer)
        WHERE id = $1::bigint AND lease_id = $2::uuid
        RETURNING id::text AS id, dlq, attempts, message`,
      [id, leaseId, leaseSeconds, maxAttempts],
    );
    if (!row) continue;
    let outcome: { retry?: number } | undefined;
    // Main-queue and DLQ deliveries keep their Cloudflare queue names.
    await handleStageQueue(
      {
        queue: stageQueueName(stage, row.dlq ? "-dlq" : ""),
        messages: [
          {
            body: row.message,
            attempts: row.attempts,
            ack: () => {
              outcome ??= {};
            },
            retry: (retry?: { delaySeconds?: number }) => {
              outcome ??= { retry: retry?.delaySeconds ?? 0 };
            },
          },
        ],
      },
      options.deps,
    );
    // Neither ack nor retry: leave the lease to run out and redeliver.
    if (!outcome) continue;
    if (outcome.retry === undefined) {
      await sql.query(
        `DELETE FROM ${jobs} WHERE id = $1::bigint AND lease_id = $2::uuid`,
        [row.id, leaseId],
      );
      result.acked += 1;
      continue;
    }
    const exhausted = !row.dlq && row.attempts >= maxAttempts;
    // An exhausted job becomes a DLQ delivery right away, with its own
    // attempt count; a DLQ delivery retries until quarantine succeeds.
    await sql.query(
      `UPDATE ${jobs}
          SET visible_at = now() + make_interval(secs => $3::integer),
              lease_id = NULL,
              dlq = dlq OR $4::boolean,
              attempts = CASE WHEN $4::boolean THEN 0 ELSE attempts END
        WHERE id = $1::bigint AND lease_id = $2::uuid`,
      [row.id, leaseId, exhausted ? 0 : Math.floor(outcome.retry), exhausted],
    );
    if (exhausted) result.exhausted += 1;
    else result.retried += 1;
  }
  return result;
}

/** Everything sent, by stage; `drain` runs it through the real consumer. */
export class MemoryPipelineQueue implements PipelineQueue {
  readonly backend = "memory" as const;
  readonly sent: StageMessage[] = [];

  async send(message: StageMessage) {
    checked(message);
    this.sent.push(structuredClone(message));
  }

  async sendBatch(messages: readonly StageMessage[]) {
    for (const message of messages) await this.send(message);
  }

  take(stage?: QueuedStage) {
    const taken = this.sent.filter((m) => !stage || m.stage === stage);
    for (const message of taken)
      this.sent.splice(this.sent.indexOf(message), 1);
    return taken;
  }
}
