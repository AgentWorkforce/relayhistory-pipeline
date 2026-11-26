/**
 * The backfill's read side (docs/proposals/relayfacts-features.md, "The onboarding backfill"): the
 * session directory, canonical evidence and a local session analysis, all through the read API's own
 * read-only path.
 *
 * - Database: the read API role (bulls96_read_api), one READ ONLY transaction per call, tenant-bound with
 *   the same settings the read API sets (TENANT_SETTINGS_SQL). It calls only the read API's SECURITY
 *   DEFINER readers; it has no table privilege and writes nothing.
 * - Objects: the read API's raw-store credentials (PIPELINE_RAW_S3_*), reads only.
 * - Analysis: the pipeline's own processSessionAnalysis with the pinned versions (versions.ts), run
 *   locally with the configured model key. Nothing is written back.
 *
 * Every credential is read from the environment once and then deleted from it. Nothing is logged that
 * holds one.
 */
import { zstdDecompressSync } from "node:zlib";
import pg from "pg";
import { rawStoreFromEnv } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { canonicalProducerSnapshot } from "../../session-read-api/canonical.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../../session-read-api/liveness.ts";
import { sqlCanonicalPointers } from "../../session-read-api/session-timing-source.ts";
import { sqlSessionHarnesses } from "../../session-read-api/session-turns-source.ts";
import { createAnalysisTransport } from "../../session-brief/engine/transport.ts";
import type { AnalysisEvidenceDeps } from "../../session-analysis/evidence.ts";
import { processSessionAnalysis, type AnalysisJob } from "../../session-analysis/processor.ts";
import {
  SESSION_ANALYSIS_MODEL_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  SESSION_ANALYSIS_SUMMARY_VERSION,
  SESSION_ANALYSIS_VERSION,
} from "../../session-analysis/versions.ts";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";

export interface DirectoryRow {
  harness: string;
  sessionId: string;
  createdAt: string;
  lastActivityAt: string;
  projectId: string | null;
  gitRepo: string | null;
  gitBranch: string | null;
  taskTitle: string | null;
  parentSessionIds: string[];
  userIds: string[];
  eventCount: number;
}

export interface BackfillConfig {
  orgId: string;
  workspaceId: string;
}

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : String(v));

/** Read and remove one variable; refuse when it is missing. */
function take(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is not set`);
  delete env[name];
  return value;
}

export function tenantRunner(pool: pg.Pool): TenantRunner {
  return async (orgId, workspaceId, work) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN READ ONLY");
      await client.query("SELECT set_config('statement_timeout', '30000', true)");
      await client.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      const out = await work(client as never);
      await client.query("ROLLBACK");
      return out;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}

const DIRECTORY_SQL = `SELECT harness, session_id, created_at, last_activity_at, project_id, git_repo, git_branch,
       task_title, parent_session_ids, user_ids, event_count
  FROM sessions.list_session_directory_for($1, p_last_ts_after => $2, p_first_ts_before => $3,
       p_cursor_last_activity_at => $4, p_cursor_session_id => $5, p_cursor_harness => $6, p_limit => 200)`;

/** Every directory row active in [since, until), newest first, paged to exhaustion. */
export async function* directoryRows(run: TenantRunner, cfg: BackfillConfig, since: string, until: string): AsyncGenerator<DirectoryRow> {
  let cursor: [string | null, string | null, string | null] = [null, null, null];
  for (;;) {
    const { rows } = await run(cfg.orgId, cfg.workspaceId, (q) =>
      q.query<Record<string, unknown>>(DIRECTORY_SQL, [cfg.workspaceId, since, until, ...cursor]));
    for (const r of rows) yield {
      harness: String(r.harness),
      sessionId: String(r.session_id),
      createdAt: iso(r.created_at),
      lastActivityAt: iso(r.last_activity_at),
      projectId: (r.project_id as string | null) ?? null,
      gitRepo: (r.git_repo as string | null) ?? null,
      gitBranch: (r.git_branch as string | null) ?? null,
      taskTitle: (r.task_title as string | null) ?? null,
      parentSessionIds: (r.parent_session_ids as string[] | null) ?? [],
      userIds: (r.user_ids as string[] | null) ?? [],
      eventCount: Number(r.event_count ?? 0),
    };
    if (rows.length < 200) return;
    const last = rows[rows.length - 1]!;
    cursor = [iso(last.last_activity_at), String(last.session_id), String(last.harness)];
  }
}

export interface BackfillSource {
  run: TenantRunner;
  cfg: BackfillConfig;
  /** One local analysis of one session; `assumeNoLegacyTurns` analyses a scope without the legacy coverage marker. */
  analyze(harness: string, sessionId: string, options: { assumeNoLegacyTurns: boolean; requestId: string }): Promise<{ snapshot: SessionAnalysisSnapshot; inputTokens: number; outputTokens: number }>;
  /** The session's canonical rows (no model call). */
  canonical(harness: string, sessionId: string): Promise<Array<{ table: string; row: Record<string, unknown> }>>;
  close(): Promise<void>;
}

/** Sources that need no model key (survey, locate). */
export function backfillReadSource(env: NodeJS.ProcessEnv = process.env): BackfillSource {
  env.SESSION_ANALYSIS_OPENAI_API_KEY ??= "unused-no-model";
  return backfillSource(env);
}

export function backfillSource(env: NodeJS.ProcessEnv = process.env): BackfillSource {
  const cfg: BackfillConfig = { orgId: take(env, "RF_ORG_ID"), workspaceId: take(env, "RF_WORKSPACE_ID") };
  const modelKey = take(env, "SESSION_ANALYSIS_OPENAI_API_KEY");
  const keyPrefix = env.PIPELINE_KEY_PREFIX?.trim() || "96bulls";
  const raw = rawStoreFromEnv({
    PIPELINE_RAW_S3_BUCKET: take(env, "PIPELINE_RAW_S3_BUCKET"),
    PIPELINE_RAW_S3_REGION: take(env, "PIPELINE_RAW_S3_REGION"),
    PIPELINE_RAW_S3_ACCESS_KEY_ID: take(env, "PIPELINE_RAW_S3_ACCESS_KEY_ID"),
    PIPELINE_RAW_S3_SECRET_ACCESS_KEY: take(env, "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"),
  } as never);
  if (!raw) throw new Error("raw store not configured");
  // pg reads PGHOST, PGPORT, PGDATABASE and PGUSER itself; the password is taken here so it can leave
  // the environment before any connection is opened (pg reads the environment lazily, per connect).
  const password = take(env, "PGPASSWORD");
  const pool = new pg.Pool({ max: 8, password, ssl: { rejectUnauthorized: false }, idleTimeoutMillis: 30_000 });
  const run = tenantRunner(pool);
  const pointers = sqlCanonicalPointers(run);
  const harnesses = sqlSessionHarnesses(run);
  const canonicalDeps = {
    ...raw,
    prefix: { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: keyPrefix },
    pointers: pointers.pointers,
    decompress: (bytes: Uint8Array, maxBytes: number) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: maxBytes })),
  };
  const evidence: AnalysisEvidenceDeps = {
    async harnesses(scope) {
      return (await harnesses(scope)).filter((h) => h.live > 0).map((h) => h.harness);
    },
    snapshot(identity) {
      return canonicalProducerSnapshot(canonicalDeps, identity) as never;
    },
  };
  const transport = createAnalysisTransport({ apiKey: () => modelKey });
  return {
    run,
    cfg,
    async analyze(harness, sessionId, options) {
      const job: AnalysisJob = {
        id: options.requestId,
        orgId: cfg.orgId,
        workspaceId: cfg.workspaceId,
        sessionId,
        analysisVersion: SESSION_ANALYSIS_VERSION,
        schemaVersion: SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
        modelVersion: SESSION_ANALYSIS_MODEL_VERSION,
        summaryVersion: SESSION_ANALYSIS_SUMMARY_VERSION,
        summaryModelVersion: SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
        attempts: 1,
        leaseToken: "local",
        sourceRevision: "local",
      };
      const out = await processSessionAnalysis(job, { evidence, transport, assumeNoLegacyTurns: options.assumeNoLegacyTurns });
      if (out.harness !== harness) throw new Error(`analysis harness ${out.harness} differs from the directory row's ${harness}`);
      return { snapshot: out.result.analysis, inputTokens: out.inputTokens, outputTokens: out.outputTokens };
    },
    async canonical(harness, sessionId) {
      const snap = await canonicalProducerSnapshot(canonicalDeps, { orgId: cfg.orgId, workspaceId: cfg.workspaceId, source: harness, sessionId } as never);
      return (snap as unknown as { rows: Array<{ table: string; row: Record<string, unknown> }> }).rows;
    },
    close: () => pool.end(),
  };
}
