/**
 * The Babysitter retention sweep as a PHASE of the E3 transcript-erasure process
 * (`neon-s3-erasure`): no new Lambda, no new process, no new S3 or IAM credential.
 * It runs beside the erasure job in the same process and uses that process's
 * eraser (the only key that can delete, and it holds no GetObject), its
 * `--execute` gate and its stop signal. docs/babysitter-retention.md.
 *
 *   BABYSITTER_RETENTION_SWEEP   off (default) | dry_run | on
 *   INGEST_EXPECT_GENERATION     64 hex, the compiled schema generation (required unless off)
 *   BABYSITTER_RETENTION_INTERVAL_MS (default 600000, the erasure sweep cadence) / _LIMIT (default 50)
 *
 * Its only extra input is the registry login, `retentionDatabaseUrl`, read from the
 * SAME stdin credential JSON the process already reads (never a file, env or argv),
 * and only when the phase is not off: the dedicated least-privilege role
 * `bulls96_babysitter_retention` (EXECUTE on the retention claim/fence/complete RPCs and
 * nothing else; NOLOGIN until the operator step supabase/bootstrap/babysitter_retention_role.sql)
 * over the Supabase session pooler with the pinned CA. The full ingest login is
 * refused: the read-only erasure host must never hold a cross-tenant write credential.
 * The erasure job's own database session is session-level read-only (#289 E3), and
 * claim/complete write, so they cannot share it.
 *
 * THIS IS A NEW CONSUMING CREDENTIAL BINDING WITH WRITE AUTHORITY (lease and audit rows),
 * default OFF, and needs its own live credential-consumer decision before use. Contract:
 *  - lifetime: read once, from the first stdin line, at process start, only when the phase
 *    is on AND the process runs with --execute; parsed, handed to one pg.Pool (max 2), the
 *    field deleted from the parsed object; held only in that pool's memory until shutdown
 *    (pool.end() in the process's finally). Never logged, written, or put in argv/env.
 *  - role pin: the URL user must be bulls96_babysitter_retention (the ingest login is
 *    refused), the host the session pooler with the pinned CA, and the connected
 *    current_user is checked before any other call.
 *  - fixed call allowlist (ALLOWED_SQL), enforced on every statement:
 *      SELECT current_user::text AS c                                         (no arguments)
 *      SELECT sessions.ingest_babysitter_retention_claim($1::jsonb) AS result     {generation, limit 1..200}
 *      SELECT sessions.ingest_babysitter_retention_fence($1::jsonb) AS result     {generation, leaseToken} (before every page of deletes)
 *      SELECT sessions.ingest_babysitter_retention_complete($1::jsonb) AS result  {generation, leaseToken, outcome erased|progress|released, versions, checkpoint}
 *      SELECT sessions.ingest_babysitter_retention_obligation($1::jsonb) AS result {generation, leaseToken, action, objects, checkpoint, strayFound}
 *    and the role itself can EXECUTE only those four functions (20261105042000).
 *  - bounds: statement and lock timeout 1.8 s, connect 1 s, pool max 2, claim 1..200,
 *    tick budget 45 s, lease 5 min (erase starts with >= 2 min left, cut at lease - 1 min),
 *    interval >= 60 s.
 *  - error sanitization: codes only (SQLSTATE class / fixed names); no message, key,
 *    URL or id is logged.
 *  - shutdown: SIGINT/SIGTERM set the process's stop flag; the loop finishes its current
 *    tick, the sleep wakes within 1 s, and the pool is ended.
 *  - dry run / no --execute: without --execute the phase is OFF (no stdin read, no
 *    connection, no database or S3 write). BABYSITTER_RETENTION_SWEEP=dry_run with
 *    --execute claims and releases (lease and audit rows only) and never calls the eraser.
 * E3's own read-only session and grants are untouched.
 */
import pg from "pg";
import { SUPABASE_PROD_CA_2021 } from "../../session-pipeline/supabase-ca.ts";
import { babysitterRetentionTick, sqlBabysitterRetentionLedger, sweepMode, type SweepMode, type TickResult } from "../../session-ingest/babysitter-retention.ts";
import { isPagedEraser } from "./eraser.ts";

const POOLER = "aws-0-us-east-1.pooler.supabase.com";
/** The phase's own least-privilege role: EXECUTE on the retention claim/fence/complete RPCs only. */
const RETENTION_ROLE = "bulls96_babysitter_retention";
/** Never accepted here: it can register, reset and commit ingest for every tenant. */
const INGEST_ROLE = "bulls96_session_ingest";

export interface PhaseSettings {
  mode: SweepMode;
  generation: string;
  intervalMs: number;
  limit: number;
  keyRoot: string;
}

/** The phase settings; `mode: "off"` needs nothing else. Without `--execute` the phase is off. */
export function phaseSettings(e: (k: string) => string | undefined, execute: boolean, keyRoot: string): PhaseSettings {
  const requested = sweepMode(e("BABYSITTER_RETENTION_SWEEP"));
  // Without --execute nothing is connected or written at all (not even a dry-run lease).
  const mode: SweepMode = execute ? requested : "off";
  const generation = e("INGEST_EXPECT_GENERATION")?.trim() ?? "";
  if (mode !== "off" && !/^[0-9a-f]{64}$/.test(generation)) throw new Error("INGEST_EXPECT_GENERATION (64 hex) required for the retention phase");
  const intervalMs = Number(e("BABYSITTER_RETENTION_INTERVAL_MS") ?? "600000");
  const limit = Number(e("BABYSITTER_RETENTION_LIMIT") ?? "50");
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 60_000) throw new Error("BABYSITTER_RETENTION_INTERVAL_MS must be >= 60000");
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200) throw new Error("BABYSITTER_RETENTION_LIMIT must be 1..200");
  return { mode, generation, intervalMs, limit, keyRoot };
}

/** The registry login: only the pooler (pinned CA) as the ingest role, or an opted-in loopback fixture. */
export function poolConfig(dbUrl: unknown, allowLoopback: boolean): pg.PoolConfig {
  if (typeof dbUrl !== "string") throw new Error("retentionDatabaseUrl required on stdin for the retention phase");
  const u = new URL(dbUrl);
  if (!["postgres:", "postgresql:"].includes(u.protocol) || u.search || u.hash) throw new Error("retentionDatabaseUrl must be a plain postgres URL");
  const loopback = ["127.0.0.1", "localhost", "[::1]"].includes(u.hostname);
  if (!(u.hostname === POOLER && u.port === "5432") && !(allowLoopback && loopback)) throw new Error("retentionDatabaseUrl must be the Supabase session pooler");
  const user = decodeURIComponent(u.username);
  const role = user.split(".")[0];
  if (role === INGEST_ROLE) throw new Error("retentionDatabaseUrl must not be the ingest login (cross-tenant write credential)");
  if (role !== RETENTION_ROLE || (user !== RETENTION_ROLE && !user.startsWith(`${RETENTION_ROLE}.`))) throw new Error("retentionDatabaseUrl must log in as the retention role");
  return {
    connectionString: dbUrl,
    max: 2,
    connectionTimeoutMillis: 1000,
    options: "-c statement_timeout=1800 -c lock_timeout=1800",
    application_name: "96bulls-babysitter-retention",
    ...(loopback ? {} : { ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: POOLER } }),
  };
}

/** Every statement the phase may send (the role can EXECUTE only the two RPCs as well). */
export const ALLOWED_SQL: ReadonlySet<string> = new Set([
  "SELECT current_user::text AS c",
  "SELECT sessions.ingest_babysitter_retention_claim($1::jsonb) AS result",
  "SELECT sessions.ingest_babysitter_retention_fence($1::jsonb) AS result",
  "SELECT sessions.ingest_babysitter_retention_complete($1::jsonb) AS result",
  "SELECT sessions.ingest_babysitter_retention_obligation($1::jsonb) AS result",
]);

/** The only way the phase talks to the database: a statement outside ALLOWED_SQL is refused unsent. */
export function allowlistedQuery(pool: Pick<pg.Pool, "query">) {
  return async (text: string, params: unknown[] = []) => {
    if (!ALLOWED_SQL.has(text)) throw Object.assign(new Error("babysitter retention: statement not allowed"), { code: "call_not_allowed" });
    return (await pool.query(text, params as never[])) as unknown as { rows: Record<string, unknown>[] };
  };
}

export interface BabysitterPhase {
  tick(): Promise<TickResult>;
  /** Ticks every `intervalMs` until `stop()`; `once` runs one tick. Never throws: codes are logged. */
  loop(stop: () => boolean, once?: boolean): Promise<void>;
  close(): Promise<void>;
}

/**
 * The phase over an open registry pool and the erasure process's eraser. Logs counts
 * and codes only (the tick result), under the process's own logger.
 */
export async function startBabysitterPhase(
  settings: PhaseSettings,
  pool: Pick<pg.Pool, "query" | "end">,
  eraser: unknown,
  log: (fields: Record<string, unknown>) => void,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
): Promise<BabysitterPhase> {
  if (!isPagedEraser(eraser)) throw new Error("the retention phase needs the strict paged eraser");
  const query = allowlistedQuery(pool);
  const who = (await query("SELECT current_user::text AS c")).rows[0] as { c: string };
  if (who.c !== RETENTION_ROLE) throw new Error("the retention phase is connected as the wrong role");
  const run = query;
  const ledger = sqlBabysitterRetentionLedger(run, settings.generation);
  const tick = () => babysitterRetentionTick({ ledger, eraser, keyRoot: settings.keyRoot, mode: settings.mode, limit: settings.limit, generation: settings.generation });
  return {
    tick,
    async loop(stop, once = false) {
      while (!stop()) {
        try {
          log({ message: "babysitter retention tick", ...(await tick()) });
        } catch (error) {
          // Codes only: database and storage messages can quote keys.
          const code = (error as { code?: unknown })?.code;
          log({ message: "babysitter retention tick failed", error_code: typeof code === "string" ? code : (error as Error)?.name ?? "Error" });
        }
        if (once) return;
        await sleep(settings.intervalMs);
      }
    },
    close: async () => { await pool.end().catch(() => {}); },
  };
}
