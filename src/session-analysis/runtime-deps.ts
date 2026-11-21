/**
 * The one builder of analysisTick's dependencies (worker.ts), shared by every runtime that runs
 * the session-analysis job processor:
 *
 *   - the session-pipeline Lambda (`{"sessionAnalysis": true}`, src/session-pipeline/lambda.ts
 *     runtimeFromSecret), and
 *   - the EKS runner (SERVICE=session-analysis, ./runner.ts, docs/session-analysis-runner.md).
 *
 * Both hand it their own connection, store, credentials and model key; everything else (the
 * canonical evidence read, the zstd bound, the org-prefix layout, the model transport, the
 * decision-records sink, the orphan-ticket sweeper's versioned eraser) is built here, once, so
 * the two runtimes cannot drift. The processor itself (processor.ts runAnalysisJob) is not forked.
 */
import { zstdDecompressSync } from "node:zlib";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import { createAnalysisTransport } from "../session-brief/engine/transport.ts";
import { sessionIntelControls } from "../shared/session-intel-controls.ts";
import { postgresClaimsPort, sessionIntelClaimsEnabled } from "./claims.ts";
import { scheduledJobGate } from "./claims-scheduler.ts";
import { decisionRecordsEnabled, postgresDecisionRecordsSink } from "./decision-records.ts";
import { analysisConfig, type analysisTick } from "./worker.ts";

export type AnalysisTickDeps = Parameters<typeof analysisTick>[0];

export interface AnalysisRuntimeInputs {
  /** The worker's HoopDb connection (service_role or the configured runtime role). */
  db: SqlExecutor;
  /** One transaction on that connection (the pipeline's `transaction(db, work)`). */
  transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
  /** The pipeline bucket's store: canonical reads and normalized/session-analysis-v2/* puts. */
  store: ObjectStore;
  /** The pipeline (canonical) bucket and ITS region (the signing region, not the caller's). */
  bucket: string;
  region: string;
  /** PIPELINE_KEY_PREFIX: the bare key root; the layout is always org-from-prefix. */
  keyPrefix: string;
  /** READER_MAX_RULESET, already parsed (reader-policy.ts readerMaxRuleset). */
  readerMaxRuleset?: number;
  /** The model key, or null for no model (then a tick claims nothing). */
  modelKey: string | null;
  /**
   * Credentials for the sweeper's eraser, read on every erase call so each step signs with
   * current credentials (Lambda: the execution role's env; EKS: the Pod Identity cache).
   */
  credentials: () => AwsCredentials | Promise<AwsCredentials>;
  /** SESSION_ANALYSIS_* and SUMMARY_DECISION_RECORDS_ENABLED are read from here. */
  env?: NodeJS.ProcessEnv;
  /** Test-only port; production always uses createS3Eraser. */
  eraserFactory?: (config: S3EraserConfig) => PagedEraser;
}

export function analysisTickDeps(inputs: AnalysisRuntimeInputs): AnalysisTickDeps {
  const env = inputs.env ?? process.env;
  const makeEraser = inputs.eraserFactory ?? createS3Eraser;
  const key = inputs.modelKey;
  const eraser = async () => makeEraser({ bucket: inputs.bucket, region: inputs.region, credentials: await inputs.credentials() });
  return {
    config: analysisConfig(env),
    db: inputs.db,
    store: inputs.store,
    canonical: {
      store: inputs.store,
      provider: "s3",
      bucket: inputs.bucket,
      prefix: { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: inputs.keyPrefix },
      readerMaxRuleset: inputs.readerMaxRuleset,
      decompress: (bytes, maxBytes) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: maxBytes })),
    },
    transport: key ? createAnalysisTransport({ apiKey: () => key }) : null,
    // SUMMARY_DECISION_RECORDS_ENABLED is the outer gate (decisions, unchanged); SESSION_INTEL_CLAIMS_ENABLED
    // inside it adds the claims port (claims.ts) next to it.
    ...(decisionRecordsEnabled(env)
      ? {
          decisionRecords: postgresDecisionRecordsSink(inputs.db),
          ...(sessionIntelClaimsEnabled(env)
            ? { claims: postgresClaimsPort(inputs.db, sessionIntelControls(inputs.db)) }
            : {}),
        }
      : {}),
    // The claims kill switch's worker side, whenever analysis runs, whatever the claims flags say: a job the
    // claims scheduler enqueued is failed as claims_control_off while its workspace control is off. Without
    // migration 062180 (42883) every job runs; user-requested jobs always run (claims-scheduler.ts).
    scheduledJobGate,
    transact: inputs.transact,
    // The bucket is versioned: the sweeper erases, by exact version id and in resumable strict
    // pages, every version of a ticketed result key that no published job owns.
    // Built per call so each step signs with the current credentials.
    eraser: {
      eraseStep: async (prefix, opts) => (await eraser()).eraseStep(prefix, opts),
      verifyErased: async (prefix, opts) => (await eraser()).verifyErased(prefix, opts),
    },
  };
}
