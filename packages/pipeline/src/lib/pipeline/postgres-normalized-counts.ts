/**
 * `NormalizedCountStore` over `sessions.record_normalized_event_count`. The caller's
 * role needs EXECUTE on that function only (`session_count_writer`). Database errors
 * become pipeline codes here, so retry and quarantine follow the design's table; any
 * other failure stays retryable, and its text is never forwarded.
 */
import { PipelineError } from "./errors.js";
import type {
  NormalizedCountOutcome,
  NormalizedCountStore,
  NormalizedReady,
} from "./normalized-ready.js";
import type { SqlExecutor } from "./queue.js";

export function postgresNormalizedCountStore(
  sql: SqlExecutor,
): NormalizedCountStore {
  return {
    async record(ready: NormalizedReady) {
      try {
        const { rows } = await sql.query<{ outcome: NormalizedCountOutcome }>(
          `SELECT sessions.record_normalized_event_count(
             $1, $2, $3, $4, $5, $6::bigint, $7::integer, $8,
             decode($9, 'hex'), decode($10, 'hex'), $11::integer) AS outcome`,
          [
            ready.org_id,
            ready.workspace_id,
            ready.harness,
            ready.session_id,
            ready.machine_id,
            ready.manifest_revision,
            ready.normalized_version,
            ready.work_key,
            ready.source_content_sha256,
            ready.output_sha256,
            ready.convergence_event_count,
          ],
        );
        return rows[0]!.outcome;
      } catch (cause) {
        const code = (cause as { code?: unknown }).code;
        if (code === "CN001")
          throw new PipelineError(
            "count_index_behind",
            "the index has not reached this revision",
            true,
          );
        if (code === "CN002")
          throw new PipelineError(
            "count_conflict",
            "the count does not match the indexed revision",
            false,
          );
        if (code === "22023")
          throw new PipelineError(
            "invalid_normalized_ready",
            "the database refused this record",
            false,
          );
        throw new PipelineError(
          "count_write_failed",
          "the count could not be written",
          true,
        );
      }
    },
  };
}
