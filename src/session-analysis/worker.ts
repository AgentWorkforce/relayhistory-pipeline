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
import { objectStoreManifestReader } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { RELAYHISTORY_EXPORT_EXTENSION } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import type { ManifestV1 } from "../shared/vendor.ts";
import { SESSION_POINTERS_SQL } from "../session-read-api/session-timing-source.ts";
import type { AnalysisModelTransport } from "./engine/model.ts";
import type { AnalysisEvidenceDeps } from "./evidence.ts";
import { runAnalysisJob } from "./processor.ts";
import { postgresAnalysisJobStore, sweepAnalysisTickets } from "./store.ts";

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
  const { rows } = await db.query<Record<string, unknown>>(SESSION_POINTERS_SQL, [
    identity.orgId,
    identity.workspaceId,
    identity.source,
    identity.sessionId,
  ]);
  return rows.map((r) => ({ ...(r as unknown as CanonicalPointer), content_sha256: hex(r.content_sha256), deleted_at: null }));
}

/** A manifest written by the Neon export/sync (signed export marker or a backfill epoch). */
export function neonDerivedManifest(manifest: ManifestV1): boolean {
  return (
    manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION] !== undefined ||
    manifest.raw.epochs.some((epoch) => epoch.reason === "backfill")
  );
}

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
      return { exists: snap.exists, generation: snap.generation, legacyCoverage: snap.legacyCoverage, rows: snap.rows };
    },
    /**
     * Never in Neon: sessions.session_analysis_neon_free (sealed freeze, absent from the
     * frozen copy, born after the seal) AND no manifest of the scope is Neon-derived. Any
     * unreadable manifest is doubt, and doubt is false (the coverage marker stays required).
     */
    async neonFree(identity) {
      const { rows } = await db.query<{ free: boolean }>("SELECT sessions.session_analysis_neon_free($1, $2, $3) AS free", [
        identity.orgId,
        identity.workspaceId,
        identity.sessionId,
      ]);
      if (rows[0]?.free !== true) return false;
      const reader = objectStoreManifestReader(canonical.store, { provider: canonical.provider as "s3", bucket: canonical.bucket, maxBytes: 1024 * 1024 });
      for (const p of await pointers(db, identity)) {
        const read = await reader.read(p.manifest_key).catch(() => null);
        if (!read || neonDerivedManifest(read.manifest)) return false;
      }
      return true;
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
    if (deps.transact) counts.swept = await sweepAnalysisTickets(deps.store, { transact: deps.transact });
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
