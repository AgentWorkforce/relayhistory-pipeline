/**
 * The vendored relayhistory-cloud surface 96bulls builds on, re-exported from
 * one place so an upstream rename touches one file. Pinned at the PR #211 head
 * (`vendor/relayhistory-cloud`); nothing here is modified.
 */
export { createPipelineRoutes, pipelineOpsRoutes, type PipelineRouteDeps } from "@rhc/routes/pipeline.ts";
export { healthRoutes } from "@rhc/routes/health.ts";
export { requireAuth } from "@rhc/middleware/auth.ts";
export { observability } from "@rhc/middleware/observability.ts";
export { TRACE_ID_HEADER, traceIdFromHeaders, isTraceId, type TraceContext } from "@rhc/lib/pipeline/trace.ts";
export {
  postgresPipelineQueue,
  pollPostgresStage,
  type PipelineQueue,
  type PollResult,
  type SqlExecutor,
} from "@rhc/lib/pipeline/queue.ts";
export { postgresDeadLetterStore } from "@rhc/lib/pipeline/postgres-dead-letters.ts";
export { pipelineQueueDeps, type PipelineImplementations } from "@rhc/lib/pipeline/index.ts";
export { PipelineError } from "@rhc/lib/pipeline/errors.ts";
export {
  handleStageQueue,
  type StageHandler,
  type StageMessage,
  type StageQueueDeps,
  type DeadLetterStore,
} from "@rhc/lib/pipeline/dlq.ts";
export {
  type PipelineEvent,
  type PipelineEventSink,
  type TraceScope,
  type TraceEvents,
  toRow,
  fromRow,
  MAX_TRACE_EVENTS,
  PipelineRecorder,
} from "@rhc/lib/pipeline/events.ts";
export { STAGE_REGISTRY, type QueuedStage } from "@rhc/lib/pipeline/registry.ts";
export type { PipelineStage } from "@rhc/lib/pipeline/trace.ts";
export {
  type RawPointer,
  type SessionPointerIndex,
  type ObjectLandedPayload,
  manifestWorkKey,
  sessionIndexedHandler,
} from "@rhc/lib/pipeline/stages.ts";
export {
  rawStoreFromEnv,
  manifestReaderFromEnv,
  objectStoreManifestReader,
  orgResolverFromEnv,
  prefixAllowed,
  type ManifestReader,
  type RawStore,
} from "@rhc/lib/pipeline/adapter.ts";
export { reconcileManifests } from "@rhc/lib/pipeline/reconcile.ts";
export { SCRUB_RULESET, SCRUB_RULESET_VERSION, sealRawChunk } from "@rhc/lib/pipeline/raw-chunks.ts";
export { scrubText, scrubJson } from "@rhc/lib/scrub.ts";
export type { Bindings, AppEnv } from "@rhc/env.ts";

export {
  createS3Store,
  manifestKey,
  rawChunkKey,
  parseSessionKey,
  computeContentSha256,
  validateManifest,
  parseManifest,
  pointerFields,
  objectKey,
  readAll,
  type ManifestV1,
  type ChunkEntry,
  type SessionLocation,
  type ObjectStore,
  type ObjectKey,
  type RawFormat,
} from "@rhc-object-store";
