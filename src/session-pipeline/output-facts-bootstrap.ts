/**
 * One-shot output facts bootstrap for self-host
 * (SERVICE=session-pipeline-output-facts-bootstrap), run as the schema owner (the same
 * identity and secret as SERVICE=session-pipeline-consumer-bootstrap).
 *
 * Sessions ingested before RELAYHISTORY_SELFHOST_OUTPUT_FACTS was turned on have no facts,
 * and nothing marks a workspace's facts complete. This job closes both gaps, in two phases:
 *
 * Phase A, reprojection (enqueue, never inline). The migration 045040 writers refuse every
 * role except search_consumer_runtime, the owner included, so this job never writes facts.
 * It reuses the consumer's own durable replay queue, sessions.search_consumer_pending: one
 * row per live manifest key of every live directory session whose facts state is missing,
 * incomplete or older than the directory's event_projection_epoch, inserted with
 * next_at = now() and ON CONFLICT DO NOTHING (an existing backoff or dead-letter row is left
 * exactly as it is). The running consumer (runtime role, facts flag on) drains those rows
 * on its next tick as reconcile bodies, so every scope goes through processBatch ->
 * indexSearchScope and its facts are written by the consumer itself, in the same
 * transaction as the directory events. Keyset batches of ENQUEUE_BATCH keys, one statement
 * each. The job then polls until no live session owes facts, or until the consumer has no
 * live pending row left for this backend (nothing more will change), bounded by
 * RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_DEADLINE_SECONDS (default 7200).
 *
 * Phase B, workspace watermark. In one transaction under an advisory lock (one statement,
 * snapshotted after the lock), as the table owner: a workspace (every distinct org/workspace in session_directory) is
 * complete when every live directory session (deleted_at IS NULL) has a state row with
 * complete = true and source_version >= its event_projection_epoch. Each complete workspace
 * without a row gets output_facts_workspace_state(complete_since = the earliest min_ts of
 * its live sessions' state rows, or now() when it has no live session or none of them
 * carries a timestamp; marked_at = now(); marked_by = 'bootstrap'). An existing row is
 * never touched (ON CONFLICT DO NOTHING), so complete_since is never lowered and a rerun
 * is a no-op for marked workspaces. An incomplete workspace never gets a row.
 *
 * Exit codes: 0 done (or incomplete with ..._ALLOW_PARTIAL=true); 1 failed (rerun);
 * 2 refused or misconfigured (fix, then rerun); 3 incomplete workspaces remain (no
 * watermark written for them; rerun once the consumer has caught up).
 *
 * Logs carry counts only: never an org, workspace, session id or manifest key.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { log, errorFields } from "../shared/log.ts";
import { initScrubEngine } from "@rhc/lib/scrub-engine/index.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { migrationChecksumMatches, resolveRenamedLedger } from "../hoopdb/migrate-guards.ts";
import { postgresTlsOptions } from "../shared/postgres-tls.ts";
import { supabasePool } from "./lambda.ts";
import { CONSUMER_OUTPUT_FACTS_VAR, consumerServiceConfig } from "./consumer-config.ts";

export const SERVICE = "session-pipeline-output-facts-bootstrap";
export const DEADLINE_VAR = "RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_DEADLINE_SECONDS";
export const ALLOW_PARTIAL_VAR = "RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_ALLOW_PARTIAL";
/** The ledger name of migration 045040; the job refuses unless it is recorded. */
export const OUTPUT_FACTS_MIGRATION = "upstream:20261105045040_selfhost_output_facts.sql";
export const DEFAULT_DEADLINE_SECONDS = 7200;
export const MAX_DEADLINE_SECONDS = 86_400;
export const ENQUEUE_BATCH = 500;
export const POLL_INTERVAL_MS = 15_000;
/** last_error_code on the pending rows this job inserts (an operator can tell them apart). */
export const PENDING_MARKER = "output_facts_bootstrap";
export const MARKED_BY = "bootstrap";

export const EXIT = { ok: 0, failed: 1, refused: 2, incomplete: 3 } as const;

/** A refusal before any write: misconfiguration, missing migration, wrong identity. Exit 2. */
export class BootstrapRefused extends Error {
  readonly code = "output_facts_bootstrap_refused";
}

export interface BootstrapOptions {
  deadlineMs: number;
  allowPartial: boolean;
}

/** The job's own knobs. Strict: a typo is a refusal, never a silent default. */
export function bootstrapOptions(env: NodeJS.ProcessEnv): BootstrapOptions {
  const rawDeadline = env[DEADLINE_VAR];
  let seconds = DEFAULT_DEADLINE_SECONDS;
  if (rawDeadline !== undefined && rawDeadline !== "") {
    seconds = Number(rawDeadline);
    if (!/^[0-9]+$/.test(rawDeadline) || !Number.isInteger(seconds) || seconds < 1 || seconds > MAX_DEADLINE_SECONDS)
      throw new BootstrapRefused(`${DEADLINE_VAR} must be an integer number of seconds, 1..${MAX_DEADLINE_SECONDS}`);
  }
  const rawPartial = env[ALLOW_PARTIAL_VAR];
  if (rawPartial !== undefined && rawPartial !== "" && rawPartial !== "true" && rawPartial !== "false")
    throw new BootstrapRefused(`${ALLOW_PARTIAL_VAR} must be true or false`);
  return { deadlineMs: seconds * 1000, allowPartial: rawPartial === "true" };
}

/**
 * The env gate: the facts flag exactly "true" and a complete, enabled consumer config
 * (the job reuses its secret, backend id and key prefix). Any config error is a refusal.
 */
export function bootstrapConfig(env: NodeJS.ProcessEnv) {
  if (env[CONSUMER_OUTPUT_FACTS_VAR] !== "true")
    throw new BootstrapRefused(`${CONSUMER_OUTPUT_FACTS_VAR} must be exactly true for the output facts bootstrap`);
  let config: ReturnType<typeof consumerServiceConfig>;
  try {
    config = consumerServiceConfig(env);
  } catch (error) {
    // Config errors name the variable, never its value.
    throw new BootstrapRefused((error as Error).message);
  }
  if (!config.enabled)
    throw new BootstrapRefused("the output facts bootstrap needs the pipeline consumer configuration (RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER=true)");
  return { config, options: bootstrapOptions(env) };
}

interface Expected {
  names: string[];
  shas?: Record<string, string>;
}

/**
 * 045040 is planned by this image and recorded in the ledger with a checksum the migrator
 * accepts: the same rename-alias and checksum rules as readiness (schemaVerdict).
 */
export async function assertOutputFactsLedger(db: SqlExecutor, expected: Expected): Promise<void> {
  if (!expected.names.includes(OUTPUT_FACTS_MIGRATION))
    throw new BootstrapRefused("this image does not plan migration 045040");
  const { rows: table } = await db.query<{ t: string | null }>("SELECT to_regclass('sessions.__96bulls_migrations')::text AS t");
  if (!table[0]?.t) throw new BootstrapRefused("migration ledger missing: run hoopdb-migrate first");
  const physical = new Map(
    (await db.query<{ name: string; sha256: string }>("SELECT name, sha256 FROM sessions.__96bulls_migrations")).rows.map((r) => [r.name, r.sha256]),
  );
  const shas = expected.shas ?? {};
  const { effective } = resolveRenamedLedger(physical, (n) => (Object.hasOwn(shas, n) ? shas[n] : undefined));
  const recorded = effective.get(OUTPUT_FACTS_MIGRATION);
  const planned = shas[OUTPUT_FACTS_MIGRATION];
  if (recorded === undefined || (planned !== undefined && !migrationChecksumMatches(OUTPUT_FACTS_MIGRATION, recorded, planned)))
    throw new BootstrapRefused("migration 045040 is not recorded in the ledger: run hoopdb-migrate first");
}

/** Every relation this job reads or writes, all of which it must reach as their owner. */
export const BOOTSTRAP_RELATIONS = [
  "sessions.session_directory",
  "sessions.session_index",
  "sessions.output_facts_session_state",
  "sessions.output_facts_workspace_state",
  "sessions.search_consumer_pending",
] as const;

/**
 * Owner proof: each relation exists (search_consumer_pending comes from the consumer
 * bootstrap) and the current role owns it without FORCE ROW LEVEL SECURITY, or is a
 * superuser. The 045040 tables have RLS on and no policy or grant, so only that identity
 * can read the state rows or write the watermark. Catalog lookup by name, not to_regclass,
 * so a role without USAGE on the schema is refused (exit 2) rather than erroring (exit 1).
 */
export async function assertBootstrapOwner(db: SqlExecutor): Promise<void> {
  const { rows } = await db.query<{ missing: number | string; denied: number | string }>(
    `SELECT count(*) FILTER (WHERE c.oid IS NULL) AS missing,
            count(*) FILTER (WHERE c.oid IS NOT NULL AND NOT (r.rolsuper OR (c.relowner = r.oid AND NOT c.relforcerowsecurity))) AS denied
       FROM unnest($1::text[]) AS n(name)
       CROSS JOIN pg_roles AS r
       LEFT JOIN pg_namespace AS ns ON ns.nspname = split_part(n.name, '.', 1)
       LEFT JOIN pg_class AS c ON c.relnamespace = ns.oid AND c.relname = split_part(n.name, '.', 2) AND c.relkind IN ('r', 'p')
      WHERE r.rolname = current_user`,
    [[...BOOTSTRAP_RELATIONS]],
  );
  if (!rows.length) throw new BootstrapRefused("output facts bootstrap: current role not found");
  const missing = Number(rows[0]!.missing);
  const denied = Number(rows[0]!.denied);
  if (missing) throw new BootstrapRefused("output facts bootstrap: a required table is missing (run hoopdb-migrate and session-pipeline-consumer-bootstrap first)");
  if (denied) throw new BootstrapRefused("output facts bootstrap: current role does not own the sessions tables (use the owner secret)");
}

const likeEscape = (value: string) => value.replace(/[\\%_]/g, (c) => `\\${c}`);

/**
 * A live directory session owes facts: no state row, an incomplete one, or one older than
 * the directory's event epoch. `d` is session_directory, `s` the LEFT JOINed state row.
 */
const STATE_JOIN = `LEFT JOIN sessions.output_facts_session_state AS s
      ON s.org_id = d.org_id AND s.workspace_id = d.workspace_id AND s.source = d.harness AND s.session_id = d.session_id`;
const OWES = "NOT (s.complete IS TRUE AND s.source_version >= d.event_projection_epoch)";

/**
 * Phase A: queue every live manifest of every owing live session for the running consumer.
 * Keyset batches by manifest key (one statement each); returns the rows actually inserted
 * and the keys considered.
 */
export async function enqueueOwingScopes(
  db: SqlExecutor,
  backendId: string,
  keyPrefix: string,
  batch = ENQUEUE_BATCH,
): Promise<{ keys: number; enqueued: number; batches: number }> {
  let after = "";
  let keys = 0;
  let enqueued = 0;
  let batches = 0;
  for (;;) {
    const { rows } = await db.query<{ manifest_key: string; inserted: boolean }>(
      `WITH owing AS (
         SELECT DISTINCT i.manifest_key
           FROM sessions.session_index AS i
           JOIN sessions.session_directory AS d
             ON d.org_id = i.org_id AND d.workspace_id = i.workspace_id AND d.harness = i.harness
            AND d.session_id = coalesce(i.canonical_session_id, i.session_id)
           ${STATE_JOIN}
          WHERE i.deleted_at IS NULL AND d.deleted_at IS NULL
            AND i.manifest_key LIKE $2 ESCAPE '\\' AND i.manifest_key > $3
            AND ${OWES}
          ORDER BY i.manifest_key
          LIMIT $4::int),
       queued AS (
         INSERT INTO sessions.search_consumer_pending
           (backend_id, manifest_key, attempts, next_at, last_error_code, dead, updated_at)
         SELECT $1, manifest_key, 0, now(), $5, false, now() FROM owing
         ON CONFLICT (backend_id, manifest_key) DO NOTHING
         RETURNING manifest_key)
       SELECT o.manifest_key, EXISTS (SELECT 1 FROM queued AS q WHERE q.manifest_key = o.manifest_key) AS inserted
         FROM owing AS o ORDER BY o.manifest_key`,
      [backendId, `${likeEscape(keyPrefix)}/%`, after, batch, PENDING_MARKER],
    );
    if (!rows.length) break;
    batches += 1;
    keys += rows.length;
    enqueued += rows.filter((r) => r.inserted).length;
    after = rows[rows.length - 1]!.manifest_key;
    if (rows.length < batch) break;
  }
  return { keys, enqueued, batches };
}

export interface Progress {
  /** Live directory sessions that still owe facts. */
  owing: number;
  /** Live (not dead) pending rows for this backend: work the consumer still has queued. */
  inFlight: number;
  /** Dead-lettered pending rows for this backend (never retried without a new revision). */
  dead: number;
}

export async function bootstrapProgress(db: SqlExecutor, backendId: string): Promise<Progress> {
  const { rows } = await db.query<{ owing: number | string; inFlight: number | string; dead: number | string }>(
    `SELECT (SELECT count(*) FROM sessions.session_directory AS d ${STATE_JOIN}
              WHERE d.deleted_at IS NULL AND ${OWES}) AS owing,
            (SELECT count(*) FROM sessions.search_consumer_pending WHERE backend_id = $1 AND dead IS NOT TRUE) AS "inFlight",
            (SELECT count(*) FROM sessions.search_consumer_pending WHERE backend_id = $1 AND dead IS TRUE) AS dead`,
    [backendId],
  );
  return { owing: Number(rows[0]!.owing), inFlight: Number(rows[0]!.inFlight), dead: Number(rows[0]!.dead) };
}

/** Phase B's advisory lock, inside its transaction (released on COMMIT/ROLLBACK). */
export async function watermarkLock(db: SqlExecutor): Promise<void> {
  await db.query("SET LOCAL lock_timeout = '30s'");
  await db.query("SELECT pg_advisory_xact_lock(hashtextextended('output-facts-bootstrap', 0))");
}

export interface MarkSummary {
  workspaces: number;
  complete: number;
  incomplete: number;
  /** Watermark rows written by this run. */
  marked: number;
  /** Complete workspaces that already had a watermark (left unchanged). */
  alreadyMarked: number;
  /** Incomplete workspaces that carry a watermark from an earlier run or the operator. */
  markedIncomplete: number;
}

/**
 * Phase B: one statement, run by the caller inside its owner transaction (after
 * watermarkLock). Never lowers or rewrites an existing watermark; never marks an
 * incomplete workspace.
 */
export async function markCompleteWorkspaces(db: SqlExecutor): Promise<MarkSummary> {
  const { rows } = await db.query<Record<keyof MarkSummary, number | string>>(
    `WITH live AS (
       SELECT d.org_id, d.workspace_id, NOT (${OWES}) AS ok, s.min_ts
         FROM sessions.session_directory AS d ${STATE_JOIN}
        WHERE d.deleted_at IS NULL),
     tally AS (
       SELECT w.org_id, w.workspace_id,
              count(l.org_id) FILTER (WHERE NOT l.ok) AS owing,
              min(l.min_ts) AS min_ts
         FROM (SELECT DISTINCT org_id, workspace_id FROM sessions.session_directory) AS w
         LEFT JOIN live AS l ON l.org_id = w.org_id AND l.workspace_id = w.workspace_id
        GROUP BY w.org_id, w.workspace_id),
     marked AS (
       INSERT INTO sessions.output_facts_workspace_state (org_id, workspace_id, complete_since, marked_at, marked_by)
       SELECT t.org_id, t.workspace_id, coalesce(t.min_ts, now()), now(), $1 FROM tally AS t WHERE t.owing = 0
       ON CONFLICT (org_id, workspace_id) DO NOTHING
       RETURNING 1)
     SELECT (SELECT count(*) FROM tally) AS workspaces,
            (SELECT count(*) FROM tally WHERE owing = 0) AS complete,
            (SELECT count(*) FROM tally WHERE owing > 0) AS incomplete,
            (SELECT count(*) FROM marked) AS marked,
            (SELECT count(*) FROM tally AS t JOIN sessions.output_facts_workspace_state AS w
                ON w.org_id = t.org_id AND w.workspace_id = t.workspace_id WHERE t.owing > 0) AS "markedIncomplete"`,
    [MARKED_BY],
  );
  const r = rows[0]!;
  const complete = Number(r.complete);
  const marked = Number(r.marked);
  return {
    workspaces: Number(r.workspaces),
    complete,
    incomplete: Number(r.incomplete),
    marked,
    alreadyMarked: complete - marked,
    markedIncomplete: Number(r.markedIncomplete),
  };
}

export interface OutputFactsBootstrapIo {
  enqueue: () => Promise<{ keys: number; enqueued: number; batches: number }>;
  progress: () => Promise<Progress>;
  /** Phase B in its own owner transaction with the advisory lock. */
  mark: () => Promise<MarkSummary>;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
}

export interface BootstrapOutcome {
  code: (typeof EXIT)[keyof typeof EXIT];
  enqueue: { keys: number; enqueued: number; batches: number };
  progress: Progress;
  deadlineReached: boolean;
  mark: MarkSummary;
}

/** Phase A then Phase B. Exported for tests (a fake consumer drives `progress`). */
export async function runOutputFactsBootstrap(
  io: OutputFactsBootstrapIo,
  opts: BootstrapOptions & { pollMs?: number },
): Promise<BootstrapOutcome> {
  const pollMs = opts.pollMs ?? POLL_INTERVAL_MS;
  const start = io.now();
  const enqueue = await io.enqueue();
  log.info(`output facts bootstrap queued ${enqueue.enqueued} of ${enqueue.keys} owing manifests`, {
    service: SERVICE, stage: "reproject", count: enqueue.keys, enqueued: enqueue.enqueued, skipped: enqueue.keys - enqueue.enqueued,
  });
  let progress = await io.progress();
  let deadlineReached = false;
  // Done when nothing owes facts, or when the consumer has drained every live pending row
  // (whatever still owes then has no manifest the consumer can project, or dead-lettered).
  while (progress.owing > 0 && progress.inFlight > 0) {
    const left = opts.deadlineMs - (io.now() - start);
    if (left <= 0) {
      deadlineReached = true;
      break;
    }
    await io.sleep(Math.min(pollMs, left));
    progress = await io.progress();
    log.info(`output facts bootstrap waiting: ${progress.owing} sessions owe facts, ${progress.inFlight} pending`, {
      service: SERVICE, stage: "reproject", count: progress.owing, state: "waiting",
    });
  }
  log.info(`output facts bootstrap reprojection ended: ${progress.owing} owing, ${progress.inFlight} pending, ${progress.dead} dead`, {
    service: SERVICE, stage: "reproject", count: progress.owing, failed: progress.dead, outcome: deadlineReached ? "deadline" : "drained",
  });
  const mark = await io.mark();
  log.info(
    `output facts bootstrap watermark: ${mark.workspaces} workspaces, ${mark.complete} complete, ${mark.marked} marked, ` +
      `${mark.alreadyMarked} already marked, ${mark.incomplete} incomplete, ${mark.markedIncomplete} incomplete but marked earlier`,
    { service: SERVICE, stage: "watermark", count: mark.workspaces, inserted: mark.marked, skipped: mark.alreadyMarked, failed: mark.incomplete },
  );
  const code = mark.incomplete > 0 && !opts.allowPartial ? EXIT.incomplete : EXIT.ok;
  if (mark.incomplete > 0)
    log.warn(`output facts bootstrap: ${mark.incomplete} workspaces incomplete; no watermark written for them`, {
      service: SERVICE, stage: "watermark", failed: mark.incomplete, outcome: opts.allowPartial ? "partial_allowed" : "incomplete",
    });
  return { code, enqueue, progress, deadlineReached, mark };
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<number> {
  let gate: ReturnType<typeof bootstrapConfig>;
  try {
    gate = bootstrapConfig(env);
  } catch (error) {
    log.error(`output facts bootstrap refused: ${(error as Error).message}`, { service: SERVICE, ...errorFields(error), outcome: "refused" });
    return EXIT.refused;
  }
  const { config, options } = gate;
  if (!config.enabled) return EXIT.refused; // narrowed by bootstrapConfig; keeps the type checker honest
  let db: ReturnType<typeof supabasePool> | undefined;
  try {
    db = supabasePool(config.secret.hoopdb_url, 1, postgresTlsOptions());
    const client = await db.pool.connect();
    try {
      const owner: SqlExecutor = {
        query: async <R>(text: string, params?: unknown[]): Promise<{ rows: R[] }> => {
          const r = await client.query(text, params as unknown[]);
          return { rows: r.rows as R[] };
        },
      };
      await assertBootstrapOwner(owner); // first: catalog-only, so a wrong identity is a clean refusal
      await assertOutputFactsLedger(owner, await expectedSchema());
      const outcome = await runOutputFactsBootstrap(
        {
          enqueue: () => enqueueOwingScopes(owner, config.backendId, config.keyPrefix),
          progress: () => bootstrapProgress(owner, config.backendId),
          mark: async () => {
            // READ COMMITTED: the single mark statement snapshots after the lock is held.
            await owner.query("BEGIN");
            try {
              await watermarkLock(owner);
              const summary = await markCompleteWorkspaces(owner);
              await owner.query("COMMIT");
              return summary;
            } catch (error) {
              await owner.query("ROLLBACK").catch(() => {});
              throw error;
            }
          },
          now: () => Date.now(),
          sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
        },
        options,
      );
      return outcome.code;
    } finally {
      client.release();
    }
  } catch (error) {
    if (error instanceof BootstrapRefused) {
      log.error(`output facts bootstrap refused: ${error.message}`, { service: SERVICE, error_code: error.code, outcome: "refused" });
      return EXIT.refused;
    }
    log.error("output facts bootstrap failed; safe to rerun", { service: SERVICE, ...errorFields(error), outcome: "failed" });
    return EXIT.failed;
  } finally {
    // Close on every path: a failed one-shot Job must not hold its socket open.
    await db?.end().catch(() => {});
  }
}

// SCRUB_ENGINE (docs/scrub-engine.md): select, and for wasm verify the pinned engine, before serving.
if (import.meta.main) {
  await initScrubEngine();
  process.exit(await serve());
}
