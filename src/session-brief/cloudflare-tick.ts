/**
 * The session brief Cloudflare Worker's invocation logic (the entry is
 * src/session-brief/cloudflare.ts, which exports the `scheduled` handler and nothing else:
 * workerd treats every named export of the entry module as an entrypoint and refuses the
 * script otherwise). See stacks/cloudflare/session-brief.ts,
 * runbooks/sessions-prod-session-brief-worker.md). The same brief engine the container
 * service runs (src/session-brief/main.ts, SERVICE=session-brief), one bounded tick per
 * cron invocation instead of a poll loop:
 *
 *   cron (every minute) --> scheduled():
 *       gate: the brief migration is ledgered and the role validation marker is `pass`
 *       briefTick(): prune stale wakeups, claim up to SESSION_BRIEF_BATCH due wakeups under a
 *       SESSION_BRIEF_LEASE_SECONDS lease, brief each one (src/session-brief/worker.ts)
 *         -> Supabase through HYPERDRIVE (bulls96_runtime, the pipeline's runtime role)
 *         -> S3 transcripts, read-only (the transcript-reader key, SigV4 GET)
 *         -> OpenAI Responses API (OPENAI_API_KEY)
 *
 * Bindings and settings (the stack declares them; secret values are created out of band):
 *   HYPERDRIVE                       Supabase session pooler as bulls96_runtime (no password here)
 *   OPENAI_API_KEY                   Secrets Store binding (`await .get()`)
 *   LOCKER_ACCESS_KEY_ID / LOCKER_SECRET_ACCESS_KEY   Secrets Store bindings: transcript-reader
 *   LOCKER_BUCKET, LOCKER_REGION     the sessions-prod transcripts bucket
 *   SESSION_BRIEF_*                  src/session-brief/config.ts (plain vars; the stack sets the defaults)
 *
 * Off unless SESSION_BRIEF_ENABLED is exactly "true". Until HYPERDRIVE, the bucket and the
 * S3 key are present, nothing runs. Without OPENAI_API_KEY the tick prunes but claims
 * nothing, as the container does: a wakeup is never spent without a model.
 *
 * Runtime bounds (Workers: 128 MB per isolate, cron wall time 15 minutes):
 *   - memory: SESSION_BRIEF_MAX_SESSION_BYTES (the stack sets 16 MiB) caps the raw bytes one
 *     session may read, checked against the manifest's chunk sizes before any chunk is
 *     fetched; a larger session is recorded `failed` / `session_too_large` (no model call,
 *     nothing spent) instead of crashing the isolate and the rest of its batch;
 *   - time: one batch per invocation, each model call bounded by the engine's 20 s timeout;
 *     the lease (300 s) outlasts a full batch, so a claim never lapses mid-tick, and a tick
 *     that overruns the next cron only overlaps it (claims are SKIP LOCKED, leases fenced).
 */
import pg from "pg";
import { buildLockerStore } from "../shared/locker.ts";
import { objectStoreManifestReader, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { errorFields, log } from "../shared/log.ts";
import { briefConfig, modelKey, type BriefConfig } from "./config.ts";
import type { AnalysisModelTransport } from "./engine/model.ts";
import { createAnalysisTransport } from "./engine/transport.ts";
import { briefTick, type BriefResult } from "./worker.ts";

/** A Secrets Store binding (a plain string in local runs and tests). */
export interface SecretBinding {
  get(): Promise<string>;
}

export interface BriefWorkerEnv {
  HYPERDRIVE?: { connectionString: string };
  OPENAI_API_KEY?: SecretBinding | string;
  LOCKER_ACCESS_KEY_ID?: SecretBinding | string;
  LOCKER_SECRET_ACCESS_KEY?: SecretBinding | string;
  LOCKER_BUCKET?: string;
  LOCKER_REGION?: string;
  [setting: `SESSION_BRIEF_${string}`]: string | undefined;
}

interface Ctx {
  waitUntil(work: Promise<unknown>): void;
}

/**
 * The migration that creates the brief tables, as the migrator ledgers it
 * (src/hoopdb/migrate.ts schemaSteps: `upstream:<file>` for supabase/migrations).
 */
export const BRIEF_MIGRATION = "upstream:20261102000000_session_briefs.sql";
/** src/shared/db.ts ROLE_VALIDATION_TABLE / ROLE_VALIDATION_NAME (not imported: db.ts reads a file at load). */
export const ROLE_VALIDATION_TABLE = "sessions.__96bulls_role_validation";
export const ROLE_VALIDATION_NAME = "session_directory_search_worker";

export const BRIEF_GATE_SQL = `SELECT
    EXISTS (SELECT 1 FROM sessions.__96bulls_migrations WHERE name = $1) AS ledgered,
    (SELECT status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $2) AS marker`;

async function secret(value: SecretBinding | string | undefined): Promise<string | undefined> {
  if (value === undefined || typeof value === "string") return value?.trim() || undefined;
  try {
    return (await value.get())?.trim() || undefined;
  } catch {
    // An unset Secrets Store secret: not configured yet.
    return undefined;
  }
}

/** Only the plain SESSION_BRIEF_* vars reach briefConfig; bindings never do. */
export function briefVars(env: BriefWorkerEnv): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(env)) if (k.startsWith("SESSION_BRIEF_") && typeof v === "string") out[k] = v;
  return out;
}

export type BriefWorkerSettings =
  | { kind: "disabled" }
  | { kind: "not_configured"; missing: string[] }
  | {
      kind: "ready";
      config: BriefConfig;
      /** Null: no model configured (prune only, claim nothing). Never logged. */
      apiKey: string | null;
      locker: { bucket: string; region: string; accessKeyId: string; secretAccessKey: string };
      connectionString: string;
    };

/** What this invocation may do. Reads the Secrets Store only when briefs are enabled. */
export async function briefWorkerSettings(env: BriefWorkerEnv): Promise<BriefWorkerSettings> {
  const config = briefConfig(briefVars(env));
  if (!config.enabled) return { kind: "disabled" };
  const [accessKeyId, secretAccessKey, openai] = await Promise.all([
    secret(env.LOCKER_ACCESS_KEY_ID),
    secret(env.LOCKER_SECRET_ACCESS_KEY),
    secret(env.OPENAI_API_KEY),
  ]);
  const missing = [
    ...(env.HYPERDRIVE?.connectionString ? [] : ["HYPERDRIVE"]),
    ...(env.LOCKER_BUCKET ? [] : ["LOCKER_BUCKET"]),
    ...(accessKeyId ? [] : ["LOCKER_ACCESS_KEY_ID"]),
    ...(secretAccessKey ? [] : ["LOCKER_SECRET_ACCESS_KEY"]),
  ];
  if (missing.length) return { kind: "not_configured", missing };
  return {
    kind: "ready",
    config,
    apiKey: modelKey({ OPENAI_API_KEY: openai }),
    locker: { bucket: env.LOCKER_BUCKET!, region: env.LOCKER_REGION || "us-east-1", accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey! },
    connectionString: env.HYPERDRIVE?.connectionString ?? "",
  };
}

/**
 * HoopDb over Hyperdrive: a small pg pool for this invocation only (Hyperdrive pools the
 * real connections). Two connections: the tick's queries plus recordBrief's transaction.
 */
export function hyperdriveHoopDb(connectionString: string): HoopDb {
  const pool = new pg.Pool({ connectionString, max: 2 });
  // A dropped socket surfaces on the next query as a coded failure, never as an uncaught error.
  // Once the invocation ends the pool, idle sockets closing under workerd emit `error` too:
  // that is the close itself, not a failure, so it is not logged.
  let ending = false;
  pool.on("error", (err) => {
    if (!ending) log.error("session brief db error", { stage: "session-brief", error_code: (err as { code?: string }).code ?? "db_error" });
  });
  return {
    pool,
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const r = await pool.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
    end: () => {
      ending = true;
      return pool.end();
    },
  };
}

/**
 * The claim gate: the brief tables' migration is ledgered and the role validation marker
 * is `pass` (a migrator in progress sets it `pending`: the Worker pauses until it is done).
 * Any error closes it. Lighter than the container's gate (no phase 2 re-run, no generation
 * match: the Worker carries no migration files), like the #121 pipeline Lambda, which has none.
 */
export async function briefGateOpen(db: Pick<HoopDb, "query">): Promise<{ open: boolean; reason: string }> {
  try {
    const row = (await db.query<{ ledgered: boolean; marker: string | null }>(BRIEF_GATE_SQL, [BRIEF_MIGRATION, ROLE_VALIDATION_NAME])).rows[0];
    if (!row?.ledgered) return { open: false, reason: "schema_pending" };
    if (row.marker !== "pass") return { open: false, reason: "validation_pending" };
    return { open: true, reason: "ok" };
  } catch (err) {
    return { open: false, reason: `gate_error:${errorFields(err).error_code}` };
  }
}

export interface BriefWorkerDeps {
  openDb: (connectionString: string) => HoopDb;
  locker: (s: Extract<BriefWorkerSettings, { kind: "ready" }>["locker"]) => { store: ObjectStore; reader: ManifestReader };
  transport: (apiKey: string) => AnalysisModelTransport;
  now: () => Date;
}

export const defaultBriefWorkerDeps: BriefWorkerDeps = {
  openDb: hyperdriveHoopDb,
  locker: (s) => {
    // The shared Locker builder, static-key branch (no pod identity in a Worker).
    const raw = buildLockerStore({
      LOCKER_BUCKET: s.bucket,
      LOCKER_REGION: s.region,
      LOCKER_ACCESS_KEY_ID: s.accessKeyId,
      LOCKER_SECRET_ACCESS_KEY: s.secretAccessKey,
    });
    return { store: raw.store, reader: objectStoreManifestReader(raw.store, { provider: raw.provider, bucket: raw.bucket }) };
  },
  transport: (apiKey) => createAnalysisTransport({ apiKey: () => apiKey }),
  now: () => new Date(),
};

export type BriefInvocation =
  | { status: "disabled" | "not_configured" | "gated" | "model_not_configured"; reason?: string }
  | { status: "ticked"; counts: Record<BriefResult, number> };

/** One invocation: settings, gate, one briefTick. Never throws for a session-level failure. */
export async function runBriefInvocation(env: BriefWorkerEnv, ctx: Ctx, deps: BriefWorkerDeps = defaultBriefWorkerDeps): Promise<BriefInvocation> {
  const settings = await briefWorkerSettings(env);
  if (settings.kind === "disabled") return { status: "disabled" };
  if (settings.kind === "not_configured") {
    log.warn("session brief worker not configured", { stage: "session-brief", reason: settings.missing.join(",") });
    return { status: "not_configured", reason: settings.missing.join(",") };
  }
  const db = deps.openDb(settings.connectionString);
  try {
    const gate = await briefGateOpen(db);
    if (!gate.open) {
      log.warn("session brief worker paused: gate closed; claiming nothing", { stage: "session-brief", reason: gate.reason });
      return { status: "gated", reason: gate.reason };
    }
    const { store, reader } = deps.locker(settings.locker);
    const transport = settings.apiKey ? deps.transport(settings.apiKey) : null;
    // A fresh tick per invocation: its prune runs once per invocation (once a minute on the cron).
    const tick = briefTick({ reader, store, transport, config: settings.config });
    const counts = await tick(db, deps.now());
    if (!transport) return { status: "model_not_configured" };
    return { status: "ticked", counts: counts ?? ({} as Record<BriefResult, number>) };
  } finally {
    ctx.waitUntil(db.end().catch(() => {}));
  }
}
