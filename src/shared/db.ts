/** HoopDb access: a pg Pool that satisfies the vendored SqlExecutor, plus readiness gates. */
import pg from "pg";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { log } from "./log.ts";
import { required } from "./env.ts";

export interface HoopDb extends SqlExecutor {
  pool: pg.Pool;
  end(): Promise<void>;
}

/**
 * Runs `work` in one transaction on one pooled connection: COMMIT when it
 * resolves, ROLLBACK when it throws (then rethrows). Used by the migration
 * assertion ledger so its row and the pipeline_jobs row commit together.
 */
export async function inTransaction<T>(db: Pick<HoopDb, "pool">, work: (tx: SqlExecutor) => Promise<T>): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const tx: SqlExecutor = {
      async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
        const r = await client.query(text, params as unknown[]);
        return { rows: r.rows as R[] };
      },
    };
    const result = await work(tx);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export function openHoopDb(url = required("HOOPDB_URL")): HoopDb {
  const pool = new pg.Pool({ connectionString: url, max: 8, application_name: process.env.OTEL_SERVICE_NAME ?? "96bulls" });
  pool.on("error", (err) => log.error("hoopdb pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  return {
    pool,
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const r = await pool.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
    end: () => pool.end(),
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Blocks until HoopDb answers. Services start before the database in a plain container stack. */
export async function waitForHoopDb(db: HoopDb, timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    try {
      await db.query("SELECT 1");
      return;
    } catch (err) {
      attempt += 1;
      if (Date.now() > deadline) throw err;
      if (attempt % 5 === 1) log.info("waiting for hoopdb", { attempt });
      await sleep(2_000);
    }
  }
}

/** Plan steps that provision or check roles (`shim:`, `bootstrap:`, `verify:`): never schema, never awaited. */
export function isPrerequisiteStep(name: string): boolean {
  return name.startsWith("shim:") || name.startsWith("bootstrap:") || name.startsWith("verify:");
}

/**
 * Readiness marker written by the migrator (src/hoopdb/migrate.ts) or an external
 * verifier: one row per validated role. `status` is 'pending' from the moment a migrator
 * starts (before any migration step) until phase 2
 * (supabase/bootstrap/session_directory_search_worker_verify.sql) passes for that
 * `generation` (the migration-set digest), in the same transaction.
 */
export const ROLE_VALIDATION_TABLE = "sessions.__96bulls_role_validation";
export const ROLE_VALIDATION_NAME = "session_directory_search_worker";

/** What a reader requires: every schema step ledgered, and PASS for its own generation. */
export interface ExpectedSchema {
  names: string[];
  generation: string;
}

export type SchemaVerdict =
  | { ok: true }
  | { ok: false; reason: "schema_pending"; count: number }
  | { ok: false; reason: "validation_pending" };

/**
 * Is HoopDb ready for this image? Missing ledger rows: schema_pending. A marker that
 * is absent, 'pending', or 'pass' for any other generation: validation_pending. Only
 * 'pass' for exactly `expected.generation` is ready. Fails closed: a missing ledger is
 * schema_pending and a missing marker table validation_pending; other errors propagate (callers treat them as
 * not ready).
 */
export async function schemaVerdict(
  db: { query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }> },
  expected: ExpectedSchema,
): Promise<SchemaVerdict> {
  let rows: { name: string }[];
  try {
    rows = (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations")).rows;
  } catch (err) {
    if ((err as { code?: string }).code === "42P01")
      return { ok: false, reason: "schema_pending", count: expected.names.filter((n) => !isPrerequisiteStep(n)).length };
    throw err;
  }
  const have = new Set(rows.map((r) => r.name));
  // The role shim, bootstrap and verify steps are not schema; the marker covers verify.
  const missing = expected.names.filter((n) => !isPrerequisiteStep(n) && !have.has(n));
  if (missing.length) return { ok: false, reason: "schema_pending", count: missing.length };
  let marker: { generation: string; status: string } | undefined;
  try {
    marker = (
      await db.query<{ generation: string; status: string }>(
        `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1`,
        [ROLE_VALIDATION_NAME],
      )
    ).rows[0];
  } catch (err) {
    if ((err as { code?: string }).code === "42P01") return { ok: false, reason: "validation_pending" };
    throw err;
  }
  if (!marker || marker.status !== "pass" || marker.generation !== expected.generation)
    return { ok: false, reason: "validation_pending" };
  return { ok: true };
}

/**
 * Blocks until HoopDb is ready for this build (schemaVerdict): every migration
 * recorded in `sessions.__96bulls_migrations` and the Worker role validation marked
 * PASS for this build's generation.
 */
export async function waitForSchema(db: HoopDb, expected: ExpectedSchema, timeoutMs = 300_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  let reason = "unreachable";
  for (;;) {
    try {
      const v = await schemaVerdict(db, expected);
      if (v.ok) return;
      reason = v.reason;
    } catch {
      // ledger not there yet
      reason = "schema_pending";
    }
    attempt += 1;
    if (Date.now() > deadline) throw new Error(`96bulls: hoopdb schema did not become ready (${reason})`);
    if (attempt % 5 === 1) log.info("waiting for hoopdb schema", { attempt, reason });
    await sleep(2_000);
  }
}
