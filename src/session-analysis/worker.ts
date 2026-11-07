/**
 * The session-analysis tick: claim up to `batch` due analysis jobs and run each one
 * (relayhistory worker.ts runSessionAnalysisTick, kind "analysis"). Off unless
 * SESSION_ANALYSIS_ENABLED=true and a model transport is configured. Runtime: the session
 * pipeline Lambda's `{"sessionAnalysis": true}` invocation (src/session-pipeline/lambda.ts),
 * driven by the stack's one-minute `session-analysis-job-schedule`, DISABLED unless
 * `transcriptStore.sessionPipeline.analysisJobs.enabled`. The model key is the pipeline
 * secret's `openai_api_key` only (operator-loaded from 1Password; never on disk). Each tick
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
import { runAnalysisJob } from "./processor.ts";
import { postgresAnalysisJobStore, sweepAnalysisTickets } from "./store.ts";
import type { PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";

export interface AnalysisConfig {
  enabled: boolean;
  batch: number;
  leaseSeconds: number;
  /** SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS=true: analyze scopes without the export's coverage marker. */
  assumeNoLegacyTurns: boolean;
}

/**
 * SESSION_ANALYSIS_ENABLED ("true" only), SESSION_ANALYSIS_BATCH (1-20, default 2),
 * SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS ("true" only; default off).
 */
export function analysisConfig(env: NodeJS.ProcessEnv = process.env): AnalysisConfig {
  const raw = env.SESSION_ANALYSIS_BATCH?.trim() ?? "";
  const batch = /^\d{1,2}$/.test(raw) && Number(raw) >= 1 && Number(raw) <= 20 ? Number(raw) : 2;
  return {
    enabled: env.SESSION_ANALYSIS_ENABLED === "true",
    batch,
    leaseSeconds: 600,
    assumeNoLegacyTurns: env.SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS === "true",
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
      const snap = await canonicalProducerSnapshot(deps, identity);
      return {
        exists: snap.exists,
        generation: snap.generation,
        legacyCoverage: snap.legacyCoverage,
        ...(snap.neonDerived ? { neonDerived: true } : {}),
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
    async anchorManifestKey(identity) {
      const first = (await pointers(db, identity))[0];
      if (!first) throw Object.assign(new Error("analysis_anchor_missing"), { retryable: true });
      return first.manifest_key;
    },
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
    for (let i = 0; i < deps.config.batch; i += 1) {
      const outcome = await runAnalysisJob(
        store,
        { evidence, transport: deps.transport, assumeNoLegacyTurns: deps.config.assumeNoLegacyTurns },
        { leaseSeconds: deps.config.leaseSeconds },
      );
      counts[outcome] = (counts[outcome] ?? 0) + 1;
      if (outcome === "idle") break;
    }
    return counts;
  };
}
