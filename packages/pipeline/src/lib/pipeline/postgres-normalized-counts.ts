/**
 * `NormalizedCountStore` over `sessions.record_normalized_event_count(job, lease)`.
 * The call runs as `session_count_writer`, through `SET LOCAL ROLE`, on one
 * connection, so this store holds no more than that role's EXECUTE for it. Database
 * errors become pipeline codes here, so retry and quarantine follow the design's
 * table; any other failure stays retryable, and its text is never forwarded.
 */
import type { StageJob } from "./dlq.js";
import { PipelineError } from "./errors.js";
import type {
  NormalizedCountOutcome,
  NormalizedCountStore,
} from "./normalized-ready.js";
import type { SqlExecutor } from "./queue.js";

/** One connection for one transaction: `pool.connect()`. */
export type ConnectSql = () => Promise<SqlExecutor & { release?(err?: Error): void }>;

function pipelineError(cause: unknown): PipelineError {
  switch ((cause as { code?: unknown }).code) {
    case "CN001":
      return new PipelineError("count_index_behind", "the index has not reached this revision", true);
    case "CN004":
      return new PipelineError("count_lease_lost", "this lease no longer holds the job", true);
    case "CN002":
      return new PipelineError("count_conflict", "the count does not match its job or the indexed revision", false);
    case "22023":
      return new PipelineError("invalid_normalized_ready", "the database refused this record", false);
    case "CN005":
      return new PipelineError("count_guard_refused", "the guard refused this write", false);
    default:
      return new PipelineError("count_write_failed", "the count could not be written", true);
  }
}

export function postgresNormalizedCountStore(
  connect: ConnectSql,
): NormalizedCountStore {
  return {
    async record(job: StageJob) {
      const client = await connect();
      let failure: Error | undefined;
      try {
        await client.query("BEGIN");
        await client.query("SET LOCAL ROLE session_count_writer");
        const { rows } = await client.query<{ outcome: NormalizedCountOutcome }>(
          "SELECT sessions.record_normalized_event_count($1::bigint, $2::uuid) AS outcome",
          [job.id, job.leaseId],
        );
        await client.query("COMMIT");
        return rows[0]!.outcome;
      } catch (cause) {
        failure = cause as Error;
        await client.query("ROLLBACK").catch(() => {});
        throw pipelineError(cause);
      } finally {
        client.release?.(failure);
      }
    },
  };
}
