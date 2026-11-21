/**
 * The session-analysis tick: claim up to `batch` due analysis jobs and run each one
 * (relayhistory worker.ts runSessionAnalysisTick, kind "analysis"). Off unless
 * SESSION_ANALYSIS_ENABLED=true and a model transport is configured. Runtimes (both build their
 * deps with runtime-deps.ts analysisTickDeps, so they run one path):
 *   - the session pipeline Lambda's `{"sessionAnalysis": true}` invocation
 *     (src/session-pipeline/lambda.ts), driven by the stack's one-minute
 *     `session-analysis-job-schedule`, DISABLED unless
 *     `transcriptStore.sessionPipeline.analysisJobs.enabled`. Its model key is the pipeline
 *     secret's `openai_api_key` only (operator-loaded from 1Password; never on disk);
 *   - the EKS runner, SERVICE=session-analysis (runner.ts, docs/session-analysis-runner.md),
 *     default off, its key from a mounted Secret file.
 * Each tick
 * first sweeps orphan publication tickets (store.ts sweepAnalysisTickets).
 *
 * Evidence comes from the trusted producer read of the canonical scope; the pointer and
 * harness reads use the worker's own HoopDb connection, never caller input.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import {
  canonicalProducerSnapshot,
  type CanonicalDeps,
  type CanonicalIdentity,
  type CanonicalPointer,
} from "../session-read-api/canonical.ts";
import { SESSION_HARNESSES_SQL } from "../session-read-api/session-turns-source.ts";
import { NEON_FREE_SQL } from "../shared/never-in-neon.ts";
import { SESSION_POINTERS_SQL } from "../session-read-api/session-timing-source.ts";
import type { AnalysisModelTransport } from "./engine/model.ts";
import type { AnalysisEvidenceDeps } from "./evidence.ts";
import { type ClaimsPort, claimsHook } from "./claims.ts";
import { type DecisionRecordsSink, decisionRecordsHook } from "./decision-records.ts";
import { type AnalysisJob, type AnalysisJobOutput, runAnalysisJob } from "./processor.ts";
import { postgresAnalysisJobStore, sweepAnalysisTickets } from "./store.ts";
import type { PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";

/** Condition (d)'s database half (062120); the worker's own connection, never caller input. */
export const CUTOVER_FREE_SQL = "SELECT sessions.session_analysis_cutover_free($1, $2, $3, $4, $5::timestamptz) AS free";

export interface AnalysisConfig {
  enabled: boolean;
  batch: number;
  leaseSeconds: number;
  /** SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS=true: analyze scopes without the export's coverage marker. */
  assumeNoLegacyTurns: boolean;
  /** SESSION_ANALYSIS_CUTOVER_PROOF=on: accept the scoped per-org cutover proof (condition (d)). */
  cutoverProof: boolean;
}

/**
 * SESSION_ANALYSIS_ENABLED ("true" only), SESSION_ANALYSIS_BATCH (1-20, default 2),
 * SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS ("true" only; default off),
 * SESSION_ANALYSIS_CUTOVER_PROOF ("on" only; default off).
 */
export function analysisConfig(env: NodeJS.ProcessEnv = process.env): AnalysisConfig {
  const raw = env.SESSION_ANALYSIS_BATCH?.trim() ?? "";
  const batch = /^\d{1,2}$/.test(raw) && Number(raw) >= 1 && Number(raw) <= 20 ? Number(raw) : 2;
  return {
    enabled: env.SESSION_ANALYSIS_ENABLED === "true",
    batch,
    leaseSeconds: 600,
    assumeNoLegacyTurns: env.SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS === "true",
    cutoverProof: env.SESSION_ANALYSIS_CUTOVER_PROOF === "on",
  };
}

const hex = (value: unknown) =>
  value instanceof Uint8Array ? Buffer.from(value).toString("hex") : String(value).replace(/^\\x/u, "");

/** Canonical pointers of one identity, read with the worker's connection. */
async function pointers(db: SqlExecutor, identity: CanonicalIdentity): Promise<CanonicalPointer[]> {
  const { rows } = await db.query<Record<string, unknown>>(WORKER_POINTERS_SQL, [
    identity.orgId,
    identity.workspaceId,
    identity.source,
    identity.sessionId,
  ]);
  return rows.map((r) => ({ ...(r as unknown as CanonicalPointer), content_sha256: hex(r.content_sha256), deleted_at: null }));
}

/** Shared with the read API's turns routes (src/shared/never-in-neon.ts). */
export { neonDerivedManifest } from "../shared/never-in-neon.ts";

/**
 * The worker's pointer read: SESSION_POINTERS_SQL plus manifest_etag, so the canonical
 * snapshot refuses any manifest version other than the one the pointer names (canonical.ts
 * manifest_mismatch) and the never-in-Neon verdict is bound to that exact version.
 */
export const WORKER_POINTERS_SQL = SESSION_POINTERS_SQL.replace("p.manifest_key, p.content_sha256,", "p.manifest_key, p.manifest_etag, p.content_sha256,");

export function canonicalEvidenceDeps(
  db: SqlExecutor,
  canonical: Omit<CanonicalDeps, "pointers">,
  /** The trusted producer read; injectable for tests only. */
  producer: typeof canonicalProducerSnapshot = canonicalProducerSnapshot,
): AnalysisEvidenceDeps & { anchorManifestKey(identity: CanonicalIdentity): Promise<string> } {
  const deps: CanonicalDeps = { ...canonical, pointers: (identity) => pointers(db, identity) };
  return {
    async harnesses(scope) {
      const { rows } = await db.query<{ harness: string; live: number }>(SESSION_HARNESSES_SQL, [
        scope.orgId,
        scope.workspaceId,
        scope.sessionId,
      ]);
      return rows.filter((r) => Number(r.live) > 0).map((r) => String(r.harness));
    },
    async snapshot(identity) {
      const snap = await producer(deps, identity);
      return {
        exists: snap.exists,
        generation: snap.generation,
        legacyCoverage: snap.legacyCoverage,
        neonDerived: snap.neonDerived,
        legacyRowsMerged: snap.legacyRowsMerged,
        ...(snap.legacyDroppedReason !== undefined ? { legacyDroppedReason: snap.legacyDroppedReason } : {}),
        rows: snap.rows,
      };
    },
    /**
     * The DATABASE half of the never-in-Neon proof (sessions.session_analysis_neon_free, a
     * SECURITY DEFINER function: the runtime role never reads the frozen custody tables). The
     * objects half is the evidence snapshot's own neonDerived (evidence.ts), bound to the
     * exact manifest versions it read; there is no second manifest read here.
     */
    async neonFree(identity) {
      const { rows } = await db.query<{ free: boolean }>(NEON_FREE_SQL, [identity.orgId, identity.workspaceId, identity.sessionId]);
      return rows[0]?.free === true;
    },
    /** Condition (d): sessions.session_analysis_cutover_free (SECURITY DEFINER; never reads Neon). */
    async cutoverFree(identity) {
      const { rows } = await db.query<{ free: boolean }>(CUTOVER_FREE_SQL, [
        identity.orgId,
        identity.workspaceId,
        identity.source,
        identity.sessionId,
        identity.earliestEvidenceAt,
      ]);
      return rows[0]?.free === true;
    },
    async anchorManifestKey(identity) {
      const first = (await pointers(db, identity))[0];
      if (!first) throw Object.assign(new Error("analysis_anchor_missing"), { retryable: true });
      return first.manifest_key;
    },
  };
}

/** Decisions exactly as before; then, only with the claims port, intents and constraints. Never throws. */
function relayfactsHook(decisions: DecisionRecordsSink, claims: ClaimsPort | undefined, counts: Record<string, number>) {
  const decided = decisionRecordsHook(decisions, counts);
  if (!claims) return decided;
  const claimed = claimsHook(claims, counts);
  return async (job: AnalysisJob, output: AnalysisJobOutput) => {
    await decided(job, output);
    await claimed(job, output);
  };
}

export function analysisTick(deps: {
  config: AnalysisConfig;
  db: SqlExecutor;
  store: ObjectStore;
  canonical: Omit<CanonicalDeps, "pointers">;
  transport: AnalysisModelTransport | null;
  /** One HoopDb transaction (the pipeline's `transaction(db, work)`); absent = no sweep. */
  transact?: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
  /** The shared strict paged eraser (createS3Eraser); see sweepAnalysisTickets. Absent = no sweep. */
  eraser?: PagedEraser;
  /**
   * Relayfacts (SUMMARY_DECISION_RECORDS_ENABLED): each completed job's decisions become
   * decision records in that job's workspace. Absent = no extractor, no call, no count.
   */
  decisionRecords?: DecisionRecordsSink;
  /**
   * Relayfacts claims (SESSION_INTEL_CLAIMS_ENABLED inside SUMMARY_DECISION_RECORDS_ENABLED): after the
   * unchanged decisionRecords hook, constraints and intents through relayfacts_claims_insert_v1 plus the
   * narrow supersession step (claims.ts). Absent = decisionRecords exactly as before.
   */
  claims?: ClaimsPort;
  /**
   * The scheduled-job gate (claims-scheduler.ts scheduledJobGate), wired whenever analysis runs (runtime-deps.ts,
   * whatever the claims flags say): a job the claims scheduler enqueued is failed as `claims_control_off` instead of
   * run while its workspace 'claims' control is off (counted `cancelled` and `scheduled_control_off`). Absent =
   * every claimed job runs, as before.
   */
  scheduledJobGate?: (db: SqlExecutor, counts: Record<string, number>) => (job: AnalysisJob) => Promise<
    { run: true } | { run: false; code: string; retryable: boolean }>;
  /**
   * The EKS runner's drain signal (runner.ts): true once SIGTERM arrived, so no further job is
   * claimed in this tick; the job in flight still finishes. Absent (the Lambda) = never stops early.
   */
  stopping?: () => boolean;
}) {
  return async (): Promise<Record<string, number>> => {
    const counts: Record<string, number> = { completed: 0, failed: 0, "lease-lost": 0, idle: 0 };
    // Never claim (spend an attempt) without a model.
    if (!deps.config.enabled || !deps.transport) return counts;
    const evidence = canonicalEvidenceDeps(deps.db, deps.canonical);
    const store = postgresAnalysisJobStore({
      db: deps.db,
      store: deps.store,
      anchorManifestKey: (job, harness) =>
        evidence.anchorManifestKey({ orgId: job.orgId, workspaceId: job.workspaceId, source: harness, sessionId: job.sessionId }),
    });
    // Orphan publications first (erased, lost or crashed): their objects never outlive a tick.
    // Only with the every-version eraser: without it, tickets wait (never a plain delete).
    if (deps.transact && deps.eraser)
      counts.swept = await sweepAnalysisTickets(deps.store, {
        transact: deps.transact,
        eraser: deps.eraser,
        onError: () => { counts.sweep_failed = (counts.sweep_failed ?? 0) + 1; },
      });
    const admit = deps.scheduledJobGate?.(deps.db, counts);
    for (let i = 0; i < deps.config.batch; i += 1) {
      if (deps.stopping?.()) break;
      const outcome = await runAnalysisJob(
        store,
        {
          evidence,
          transport: deps.transport,
          assumeNoLegacyTurns: deps.config.assumeNoLegacyTurns,
          cutoverProof: deps.config.cutoverProof,
          ...(deps.decisionRecords ? { onCompleted: relayfactsHook(deps.decisionRecords, deps.claims, counts) } : {}),
          ...(admit ? { admit } : {}),
        },
        { leaseSeconds: deps.config.leaseSeconds },
      );
      counts[outcome] = (counts[outcome] ?? 0) + 1;
      if (outcome === "idle") break;
    }
    return counts;
  };
}
