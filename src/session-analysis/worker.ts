/**
 * The session-analysis tick: claim up to `batch` due analysis jobs and run each one
 * (relayhistory worker.ts runSessionAnalysisTick, kind "analysis"). Off unless
 * SESSION_ANALYSIS_ENABLED=true and a model transport is configured, so wiring it into a
 * runtime changes nothing on its own. The runtime wiring (Lambda + one-minute EventBridge
 * tick, OPENAI_API_KEY from Secrets Manager) is a later, separately approved change.
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
import { SESSION_POINTERS_SQL } from "../session-read-api/session-timing-source.ts";
import type { AnalysisModelTransport } from "./engine/model.ts";
import type { AnalysisEvidenceDeps } from "./evidence.ts";
import { runAnalysisJob } from "./processor.ts";
import { postgresAnalysisJobStore } from "./store.ts";

export interface AnalysisConfig {
  enabled: boolean;
  batch: number;
  leaseSeconds: number;
}

/** SESSION_ANALYSIS_ENABLED ("true" only), SESSION_ANALYSIS_BATCH (1-20, default 2). */
export function analysisConfig(env: NodeJS.ProcessEnv = process.env): AnalysisConfig {
  const raw = env.SESSION_ANALYSIS_BATCH?.trim() ?? "";
  const batch = /^\d{1,2}$/.test(raw) && Number(raw) >= 1 && Number(raw) <= 20 ? Number(raw) : 2;
  return { enabled: env.SESSION_ANALYSIS_ENABLED === "true", batch, leaseSeconds: 600 };
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
      return { exists: snap.exists, generation: snap.generation, rows: snap.rows };
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
    for (let i = 0; i < deps.config.batch; i += 1) {
      const outcome = await runAnalysisJob(store, { evidence, transport: deps.transport }, { leaseSeconds: deps.config.leaseSeconds });
      counts[outcome] = (counts[outcome] ?? 0) + 1;
      if (outcome === "idle") break;
    }
    return counts;
  };
}
