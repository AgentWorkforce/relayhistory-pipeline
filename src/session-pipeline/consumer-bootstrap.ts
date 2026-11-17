/**
 * One-shot schema bootstrap for the self-host search-index writer
 * (SERVICE=session-pipeline-consumer-bootstrap).
 *
 * This is the ONLY entrypoint that uses the schema-owner/migrator identity.
 * In one transaction it runs, in order: an owner check, an advisory lock, the
 * adapter `ensureSchema`, the projection-state `ensureSchema`, the service
 * tables, the dedicated writer role SQL, and the role verify SQL. Then it
 * commits and exits zero. Any failure rolls back and exits non-zero. The
 * role SQL holds no transaction statements of its own, so a failed verify
 * rolls back role, grants, policies and tables together. The dormant path
 * exits zero without touching the database.
 *
 * The long-running reconciler (SERVICE=session-pipeline-consumer) never
 * holds owner credentials: it connects as the dedicated `search_consumer_runtime`
 * role created here (password set separately by the operator; shape enforced
 * by the verify step). The reader stays bulls96_read_api; after both search
 * tables pass their forced tenant-policy checks, this job grants that role only
 * schema USAGE, table SELECT for transcript events, and the projection-state
 * columns its completeness check reads. It grants no writes or membership.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { log } from "../shared/log.ts";
import { initScrubEngine } from "@rhc/lib/scrub-engine/index.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { postgresTlsOptions } from "../shared/postgres-tls.ts";
import {
  composeSearchService,
  searchSecret,
} from "../shared/search-composition.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { writerTransactionSql } from "./transaction-sql.ts";
import { supabasePool } from "./lambda.ts";
import { writerComposition, writerSearchEnv, type WriterProjectionState } from "./consumer-service.ts";
import {
  CONSUMER_BACKEND_ID_VAR,
  consumerServiceConfig,
} from "./consumer-config.ts";

const ROLE_SQL_PATH = fileURLToPath(new URL("../../supabase/bootstrap/session_consumer_role.sql", import.meta.url));
const ROLE_VERIFY_SQL_PATH = fileURLToPath(new URL("../../supabase/bootstrap/session_consumer_role_verify.sql", import.meta.url));

/**
 * The bootstrap SQL files are written for `psql -v ON_ERROR_STOP=1` (the
 * repo's operator pattern) and start with a `\set` line. The node driver
 * sends them as SQL, so strip psql meta-commands first. DO blocks and
 * dollar-quoted bodies pass through untouched.
 */
export function stripPsqlMeta(sql: string): string {
  return sql
    .split("\n")
    .filter((line) => !/^\s*\\/.test(line))
    .join("\n");
}

export interface BootstrapIo {
  owner: SqlExecutor;
  lock: () => Promise<void>;
  checkOwner: () => Promise<void>;
  ensureIndexSchema: () => Promise<void>;
  ensureProjectionState: (owner: SqlExecutor) => Promise<void>;
  ensureServiceTables: (owner: SqlExecutor) => Promise<void>;
  ensureRole: (owner: SqlExecutor) => Promise<void>;
  verifyRole: (owner: SqlExecutor) => Promise<void>;
  generation: string;
}

/** Advisory single-writer lock for the bootstrap transaction. Exported so the
 * real lock statement (not a mock) runs against real Postgres in tests: the
 * function name is hashtextextended, and a typo fails every run with 42883. */
export async function bootstrapLock(owner: SqlExecutor): Promise<void> {
  await owner.query("SET LOCAL lock_timeout = '30s'");
  await owner.query("SELECT pg_advisory_xact_lock(hashtextextended('search-consumer-bootstrap', 0))");
}

/**
 * Owner proof, not a superuser demand: the invoker must own the schemas it
 * writes and, while the dedicated role still has to be created or promoted,
 * be able to do that. A CNPG hoop owner or a managed-platform CREATEROLE
 * owner passes; a bare reader or an unrelated role fails here, before any
 * DDL. On RDS and Cloud SQL the owner creates search_consumer_runtime itself
 * through the bootstrap chain (supabase/bootstrap/session_consumer_roles.sql,
 * same pattern as hoopdb_runtime_role_rds.sql) and keeps the unavoidable
 * ADMIN creator edge: the managed-platform trampolines deliberately never
 * provision the consumer role, because a trampoline copy would leave the
 * owner unable to promote it.
 *
 * CREATEROLE (or superuser) is demanded only when the role SQL will ALTER
 * the role: the role is absent, NOLOGIN (the bare role awaiting promotion),
 * INHERIT, or lacks exactly the one global statement_timeout=60s claim row.
 * Once the role is claimed (converged), the role SQL changes no attribute or
 * setting, so schema ownership alone passes: an owner that dropped its
 * temporary CREATEROLE after the first bootstrap can rerun the Job. The
 * convergence predicate mirrors the conditional claim block in
 * session_consumer_role.sql; the role SQL itself still fails closed on any
 * other role state.
 */
export async function bootstrapCheckOwner(owner: SqlExecutor): Promise<void> {
  const { rows } = await owner.query<{ ok: boolean; canCreateRole: boolean; roleConverged: boolean }>(
    `SELECT
       bool_and(n.nspowner = current_user::regrole) AS ok,
       EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND (rolsuper OR rolcreaterole)) AS "canCreateRole",
       EXISTS (
         SELECT 1 FROM pg_catalog.pg_roles r
          WHERE r.rolname = 'search_consumer_runtime' AND r.rolcanlogin AND NOT r.rolinherit
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting s
                         WHERE s.setrole = r.oid AND s.setdatabase = 0
                           AND s.setconfig = ARRAY['statement_timeout=60s']::text[])
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting s
                             WHERE s.setrole = r.oid
                               AND NOT (s.setdatabase = 0 AND s.setconfig = ARRAY['statement_timeout=60s']::text[]))
       ) AS "roleConverged"
     FROM pg_namespace n WHERE n.nspname IN ('sessions', 'search_index')`,
  );
  if (!rows[0]?.ok)
    throw Object.assign(new Error("pipeline consumer bootstrap: current role does not own the sessions/search_index schemas"), {
      code: "42501",
    });
  if (!rows[0]?.canCreateRole && rows[0]?.roleConverged !== true)
    throw Object.assign(
      new Error("pipeline consumer bootstrap: current role may not create roles (needs CREATEROLE or superuser)"),
      { code: "42501" },
    );
}

/** Run every owner step once, in order. Exported for tests. */
export async function runBootstrap(io: BootstrapIo): Promise<{ generation: string }> {
  await io.checkOwner();
  await io.lock();
  await io.ensureIndexSchema();
  await io.ensureProjectionState(io.owner);
  await io.ensureServiceTables(io.owner);
  await io.ensureRole(io.owner);
  await io.verifyRole(io.owner);
  return { generation: io.generation };
}

/**
 * Owner-created service tables plus, via the role SQL, the least-privilege
 * runtime grants. Pending rows are keyed by (backend_id, manifest_key): the
 * manifest key is unique per reconciliation unit, so no scope columns are
 * needed and the ON CONFLICT target always matches the primary key.
 * Idempotent; safe to re-run on every deploy.
 */
export async function ensureServiceTables(owner: SqlExecutor): Promise<void> {
  await owner.query(`CREATE TABLE IF NOT EXISTS sessions.search_consumer_pending(
    backend_id text NOT NULL, manifest_key text NOT NULL,
    manifest_etag text, manifest_version text,
    attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
    next_at timestamptz NOT NULL, last_error_code text NOT NULL DEFAULT '',
    dead boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL,
    PRIMARY KEY(backend_id,manifest_key))`);
  await owner.query(`CREATE INDEX IF NOT EXISTS search_consumer_pending_due_idx
    ON sessions.search_consumer_pending(backend_id,next_at)`);
  await owner.query(`CREATE TABLE IF NOT EXISTS sessions.search_consumer_progress(
    backend_id text NOT NULL PRIMARY KEY,
    last_complete_at timestamptz, updated_at timestamptz NOT NULL,
    reconcile_cursor text, reconcile_since timestamptz, reconcile_window_minutes integer,
    reconcile_key_prefix text)`);
  // A rerun against a table created before the cursor columns existed.
  for (const column of ["reconcile_cursor text", "reconcile_since timestamptz", "reconcile_window_minutes integer", "reconcile_key_prefix text"])
    await owner.query(`ALTER TABLE sessions.search_consumer_progress ADD COLUMN IF NOT EXISTS ${column}`);
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<never> {
  const config = consumerServiceConfig(env);
  if (!config.enabled) {
    // A default-off one-shot Job must not report failure when disabled: exit
    // zero without touching the database.
    log.info("pipeline consumer bootstrap dormant; nothing to do", {
      service: "session-pipeline-consumer-bootstrap",
    });
    process.exit(0);
    throw new Error("unreachable");
  }
  const expected = await expectedSchema();
  const db = supabasePool(config.secret.hoopdb_url, 1, postgresTlsOptions());
  const roleSql = readFileSync(ROLE_SQL_PATH, "utf8");
  const roleVerifySql = readFileSync(ROLE_VERIFY_SQL_PATH, "utf8");
  const client = await db.pool.connect();
  try {
    const owner: SqlExecutor = {
      query: async <R>(text: string, params?: unknown[]): Promise<{ rows: R[] }> => {
        const r = await client.query(text, params as unknown[]);
        return { rows: r.rows as R[] };
      },
    };
    const composition = writerComposition(config.backendId, config.floorMs);
    const writerSql = writerTransactionSql({ query: (text: string, params?: unknown[]) => owner.query(text, params) });
    // The Postgres index schema is always created: the consumer role, its
    // verifier and the serve-time tenant gate name transcript_events, and with
    // OpenSearch selected it simply stays empty.
    const binding = composeSearchService(
      composition,
      {
        env: { SEARCH_INDEX_BACKEND: "postgres", [CONSUMER_BACKEND_ID_VAR]: config.backendId },
        sql: writerSql.sql,
        secrets: searchSecret(config.secret.search),
      },
      "writer",
    );
    const remoteIndex = config.searchBackend === "postgres"
      ? undefined
      : composeSearchService(
          writerComposition(config.backendId, config.floorMs, config.searchBackend, env),
          { env: writerSearchEnv(config, env), sql: writerSql.sql, secrets: searchSecret(config.secret.search) },
          "writer",
        ).index;
    const state = binding.state as WriterProjectionState;
    await owner.query("BEGIN");
    try {
      await runBootstrap({
        owner,
        lock: () => bootstrapLock(owner),
        checkOwner: () => bootstrapCheckOwner(owner),
        ensureIndexSchema: async () => {
          await binding.index.ensureSchema();
          await remoteIndex?.ensureSchema();
        },
        ensureProjectionState: (o) => state.ensureSchema(o),
        ensureServiceTables: (o) => ensureServiceTables(o),
        ensureRole: (o) => o.query(stripPsqlMeta(roleSql)).then(() => {}),
        verifyRole: (o) => o.query(stripPsqlMeta(roleVerifySql)).then(() => {}),
        generation: expected.generation,
      });
      await owner.query("COMMIT");
    } catch (error) {
      await owner.query("ROLLBACK").catch(() => {});
      throw error;
    }
  } finally {
    client.release();
    // The pool must close on the failure path too: without this a failed
    // one-shot Job holds its socket open instead of exiting.
    await db.end().catch(() => {});
  }
  log.info("pipeline consumer schema bootstrapped", { generation: expected.generation, backendId: config.backendId });
  process.exit(0);
  throw new Error("unreachable");
}

// SCRUB_ENGINE (docs/scrub-engine.md): select, and for wasm verify the pinned engine, before serving.
if (import.meta.main) {
  await initScrubEngine();
  await serve();
}
