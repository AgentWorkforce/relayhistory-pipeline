/**
 * The typed record between normalization and the sessions index
 * (`docs/event-count-writeback.md`, #58). The transform has no database, so once
 * its object is published it sends this record, and the `normalized-ready` stage
 * writes the count through a tenant-scoped function. The record carries ids,
 * hashes and counts, never content.
 *
 * Only an `applied` outcome makes a count authoritative. A record with
 * `output_divergent` is refused: the same input and version produced different
 * bytes, and a person should look. The remedy is a version bump, not forcing it.
 */
import type { StageHandler, StageJob, StageMessage } from "./dlq.js";
import { PipelineError } from "./errors.js";
import type { NormalizedOutput } from "./stages.js";
import { objectKeyIdentity } from "./trace.js";

export interface NormalizedReady {
  v: 1;
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  machine_id: string;
  manifest_key: string;
  manifest_revision: number;
  work_key: string;
  trace_id: string;
  source_content_sha256: string;
  normalized_version: number;
  output_key: string;
  output_sha256: string;
  output_bytes: number;
  convergence_event_count: number;
  unprojected_records: number;
  output_reused: boolean;
  output_divergent: boolean;
}

export type NormalizedCountOutcome = "applied" | "replayed" | "stale" | "deleted";

export interface NormalizedCountStore {
  /**
   * Applies the held job's record and acks the job in one transaction. Everything
   * written comes from the job's row. Throws `count_index_behind` or
   * `count_lease_lost` (retryable) or `count_conflict` (not).
   */
  record(job: StageJob): Promise<NormalizedCountOutcome>;
}

const invalid = (field: string): never => {
  throw new PipelineError(
    "invalid_normalized_ready",
    `normalized-ready field ${field} is invalid`,
    false,
  );
};

const STRINGS = [
  "org_id",
  "workspace_id",
  "harness",
  "session_id",
  "machine_id",
  "manifest_key",
  "work_key",
  "trace_id",
  "output_key",
] as const;
const HASHES = ["source_content_sha256", "output_sha256"] as const;
const COUNTS = [
  "output_bytes",
  "convergence_event_count",
  "unprojected_records",
] as const;
const REVISIONS = ["manifest_revision", "normalized_version"] as const;
const FLAGS = ["output_reused", "output_divergent"] as const;
const FIELDS = new Set<string>([
  "v",
  ...STRINGS,
  ...HASHES,
  ...COUNTS,
  ...REVISIONS,
  ...FLAGS,
]);
/** `integer` in the database. */
const MAX_COUNT = 2_147_483_647;

/** Strict: an unknown or missing field never reaches the database. */
export function parseNormalizedReady(value: unknown): NormalizedReady {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return invalid("payload");
  const record = value as Record<string, unknown>;
  for (const key of Object.keys(record))
    if (!FIELDS.has(key)) invalid("unknown");
  if (record.v !== 1) invalid("v");
  for (const key of STRINGS)
    if (typeof record[key] !== "string" || !record[key]) invalid(key);
  for (const key of HASHES)
    if (
      typeof record[key] !== "string" ||
      !/^[0-9a-f]{64}$/.test(record[key] as string)
    )
      invalid(key);
  for (const key of COUNTS) {
    const count = record[key];
    if (!Number.isSafeInteger(count) || (count as number) < 0) invalid(key);
  }
  if ((record.convergence_event_count as number) > MAX_COUNT)
    invalid("convergence_event_count");
  for (const key of REVISIONS)
    if (!Number.isSafeInteger(record[key]) || (record[key] as number) < 1)
      invalid(key);
  for (const key of FLAGS)
    if (typeof record[key] !== "boolean") invalid(key);
  return record as unknown as NormalizedReady;
}

/**
 * Identity comes from the authenticated context, and `manifest_revision` from the
 * result, which `normalize` has bound to this message. Object metadata can name an
 * earlier revision, so it is never used.
 */
export function buildNormalizedReady(
  message: StageMessage,
  output: NormalizedOutput,
): NormalizedReady {
  const { ctx } = message;
  const manifestKey = (message.payload as { manifestKey?: unknown }).manifestKey;
  if (message.revision !== undefined && message.revision !== output.revision)
    invalid("manifest_revision");
  return parseNormalizedReady({
    v: 1,
    org_id: ctx.orgId,
    workspace_id: ctx.workspaceId,
    harness: ctx.harness,
    session_id: ctx.sessionId,
    machine_id: ctx.machineId,
    manifest_key: manifestKey,
    manifest_revision: output.revision,
    work_key: message.workKey,
    trace_id: ctx.traceId,
    source_content_sha256: output.contentSha256,
    normalized_version: output.normalizedVersion,
    output_key: output.outputKey,
    output_sha256: output.outputSha256,
    output_bytes: output.outputBytes,
    convergence_event_count: output.convergenceEventCount,
    unprojected_records: output.unprojectedRecords,
    output_reused: output.outputReused,
    output_divergent: output.outputDivergent,
  });
}

export function normalizedReadyHandler(
  store: NormalizedCountStore,
): StageHandler {
  return async (message, recorder, _attempt, job) => {
    // The count is written from a leased job's row, so only the Postgres queue can
    // deliver this stage. Quarantined, it waits for replay on that backend.
    if (!job)
      throw new PipelineError(
        "count_no_job_lease",
        "normalized-ready needs a leased queue job",
        false,
      );
    const ready = parseNormalizedReady(message.payload);
    const { ctx } = message;
    const key = objectKeyIdentity(ready.manifest_key);
    if (
      ready.org_id !== ctx.orgId ||
      ready.workspace_id !== ctx.workspaceId ||
      ready.harness !== ctx.harness ||
      ready.session_id !== ctx.sessionId ||
      ready.machine_id !== ctx.machineId ||
      ready.work_key !== message.workKey ||
      ready.manifest_revision !== message.revision ||
      key?.workspaceId !== ctx.workspaceId ||
      key?.harness !== ctx.harness ||
      key?.sessionId !== ctx.sessionId ||
      key?.machineId !== ctx.machineId
    )
      invalid("identity");
    if (ready.output_divergent)
      throw new PipelineError(
        "count_conflict",
        "normalize produced different bytes for the same input and version",
        false,
      );
    const outcome = await store.record(job);
    recorder.log.info("normalized count", {
      outcome,
      convergence_event_count: ready.convergence_event_count,
      unprojected_records: ready.unprojected_records,
      revision: ready.manifest_revision,
    });
    if (outcome !== "applied") return { status: "skipped", reason: outcome };
  };
}
