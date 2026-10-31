/**
 * HoopDb migrations, in order: the Supabase role shim and the search Worker role
 * bootstrap (plain Postgres only; the migrator is that database's superuser), the vendored relayhistory-cloud `sessions` migrations exactly as pinned, then
 * the 96bulls migrations. Each file is recorded with its sha256 in a ledger and
 * never re-applied; a changed applied file fails the run.
 *
 * Runs as a one-shot container (`96b-hoopdb-migrate`) or a Kubernetes Job. No
 * Helm hooks. Idempotent and safe to run on every `up`.
 */
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { openHoopDb, waitForHoopDb, ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE, ROLE_VERIFY_SQL_PATH, type ExpectedSchema } from "../shared/db.ts";
import { log } from "../shared/log.ts";
import { flag, optional } from "../shared/env.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
// The relayhistory-cloud sessions migrations, moved into this repo by #1/#6 (provenance: supabase/README.md).
const UPSTREAM_MIGRATIONS = join(ROOT, "supabase/migrations");
const SHIM = join(ROOT, "supabase/tests/supabase_shim.sql");
// Privileged and idempotent. On Supabase and CNPG a deploy step runs it as a superuser
// before migrate (runbooks/session-directory-search-worker-bootstrap.md); here it then
// only verifies the bare role spec, on every run (never ledgered), and fails closed if
// the role is absent or differs.
const WORKER_BOOTSTRAP = join(ROOT, "supabase/bootstrap/session_directory_search_worker.sql");
// Phase 2, after every migration: the role's exact direct-grant allowlist. Read-only, any
// role that reads the catalogs; runs everywhere, on every migrate, never ledgered.
// The one phase 2 source, shared with the live readiness gate (src/shared/db.ts).
const WORKER_VERIFY = ROLE_VERIFY_SQL_PATH;
const OURS = join(ROOT, "src/hoopdb/migrations");

export const MIGRATION_LEDGER = "sessions.__96bulls_migrations";

interface Step {
  name: string;
  path: string;
}

export async function plan(): Promise<Step[]> {
  const upstream = (await readdir(UPSTREAM_MIGRATIONS)).filter((f) => f.endsWith(".sql")).sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  return [
    ...(flag("HOOPDB_SUPABASE_SHIM", true) ? [{ name: "shim:supabase_roles", path: SHIM }] : []),
    ...(flag("HOOPDB_ROLE_BOOTSTRAP", flag("HOOPDB_SUPABASE_SHIM", true))
      ? [{ name: "bootstrap:session_directory_search_worker", path: WORKER_BOOTSTRAP }]
      : []),
    ...upstream.map((f) => ({ name: `upstream:${f}`, path: join(UPSTREAM_MIGRATIONS, f) })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
    ...(flag("HOOPDB_ROLE_VERIFY", true)
      ? [{ name: "verify:session_directory_search_worker", path: WORKER_VERIFY }]
      : []),
  ];
}

/** Names the services wait for before they start consuming. */
export async function expectedMigrationNames(): Promise<string[]> {
  return (await plan()).map((s) => s.name);
}

/**
 * The migration-set generation: sha256 over every schema step (name and file sha256)
 * plus the phase 2 verifier, which defines the approved grants. Independent of the
 * shim/bootstrap/verify flags, so the migrator, an external verifier and every reader
 * compute the same value for the same image.
 */
export async function generation(): Promise<string> {
  const upstream = (await readdir(UPSTREAM_MIGRATIONS)).filter((f) => f.endsWith(".sql")).sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  const steps: Step[] = [
    ...upstream.map((f) => ({ name: `upstream:${f}`, path: join(UPSTREAM_MIGRATIONS, f) })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
    { name: "verify:session_directory_search_worker", path: WORKER_VERIFY },
  ];
  const parts: string[][] = [];
  for (const s of steps) parts.push([s.name, createHash("sha256").update(await readFile(s.path, "utf8")).digest("hex")]);
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

/** What readiness requires of HoopDb for this image (see schemaVerdict). */
export async function expectedSchema(): Promise<ExpectedSchema> {
  return { names: await expectedMigrationNames(), generation: await generation() };
}

/** The migrator's "pending first" write, under its advisory lock, before any step. */
export const MARKER_PENDING_SQL = `INSERT INTO ${ROLE_VALIDATION_TABLE} (name, generation, status) VALUES ($1, $2, 'pending')
       ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = 'pending', updated_at = now()`;

export interface MigrationClient {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

export interface RunOptions {
  steps?: Step[];
  /** Test seam: runs after the last migration commit, before phase 2. */
  beforeVerify?: () => Promise<void>;
}

/**
 * Applies the plan on one connection. Readiness marker (ROLE_VALIDATION_TABLE):
 *   1. under the advisory lock, before any step runs, the row is set to
 *      ('pending', <generation>): from here every reader is non-ready;
 *   2. only a successful phase 2, in the same transaction, sets it to 'pass' for the
 *      generation it validated;
 *   3. any failure, crash or a disabled verifier (HOOPDB_ROLE_VERIFY=false) leaves
 *      it 'pending'. With the verifier disabled only an external verifier
 *      (supabase/bootstrap/session_directory_search_worker_mark_pass.sql) can pass it.
 */
export async function runMigrations(client: MigrationClient, opts: RunOptions = {}): Promise<{ applied: number; skipped: number }> {
  let applied = 0;
  let skipped = 0;
  // One migrator at a time; every marker transition happens while this lock is held.
  await client.query("SELECT pg_advisory_lock(961996, 1)");
  try {
    await client.query("CREATE SCHEMA IF NOT EXISTS sessions");
    await client.query(`CREATE TABLE IF NOT EXISTS ${MIGRATION_LEDGER} (
      name text PRIMARY KEY,
      sha256 text NOT NULL,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${ROLE_VALIDATION_TABLE} (
      name text PRIMARY KEY,
      generation text NOT NULL,
      status text NOT NULL CHECK (status IN ('pending', 'pass')),
      updated_at timestamptz NOT NULL DEFAULT now()
    )`);
    await client.query(`REVOKE ALL ON ${ROLE_VALIDATION_TABLE} FROM PUBLIC`);
    const steps = opts.steps ?? (await plan());
    const gen = await generation();
    // Pending first: committed before any migration can become visible.
    await client.query(MARKER_PENDING_SQL, [ROLE_VALIDATION_NAME, gen]);
    log.info("role validation pending", { reason: gen });
    const ledger = new Map<string, string>(
      (await client.query<{ name: string; sha256: string }>(`SELECT name, sha256 FROM ${MIGRATION_LEDGER}`)).rows.map(
        (r) => [r.name, r.sha256],
      ),
    );
    // A HoopDb built from the old vendored pin cannot be moved forward in place:
    // upstream changed 20261025000600 after it was applied there. Say so plainly.
    if ([...ledger.keys()].some((n) => n.startsWith("vendor:"))) {
      throw Object.assign(
        new Error("96bulls: this HoopDb was built from the old vendored relayhistory-cloud pin; recreate it (local: `96bulls down <stage> --purge`, then `up`)"),
        { code: "legacy_schema" },
      );
    }
    for (const step of steps) {
      const sql = await readFile(step.path, "utf8");
      // The role checks (phase 1 bootstrap, phase 2 verify) run on every migrate, each in
      // its own transaction, and are never recorded in the ledger.
      if (step.name.startsWith("bootstrap:") || step.name.startsWith("verify:")) {
        const verify = step.name.startsWith("verify:");
        if (verify && opts.beforeVerify) await opts.beforeVerify();
        await client.query("BEGIN");
        try {
          await client.query(sql);
          if (verify) {
            const passed = await client.query(
              `UPDATE ${ROLE_VALIDATION_TABLE} SET status = 'pass', updated_at = now()
                WHERE name = $1 AND generation = $2 AND status = 'pending' RETURNING name`,
              [ROLE_VALIDATION_NAME, gen],
            );
            if (passed.rows.length !== 1) throw Object.assign(new Error("96bulls: role validation marker was not pending for this generation"), { code: "marker_lost" });
          }
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          log.error("role check failed", { reason: step.name, error_code: (err as { code?: string }).code ?? "role_check_failed" });
          throw err;
        }
        log.info(verify ? "role validation passed" : "role check passed", { reason: verify ? gen : step.name });
        continue;
      }
      const sha = createHash("sha256").update(sql).digest("hex");
      const seen = ledger.get(step.name);
      if (seen === sha) {
        skipped += 1;
        continue;
      }
      if (seen && seen !== sha) {
        throw new Error(`96bulls: migration ${step.name} changed after it was applied; add a new migration instead`);
      }
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query(`INSERT INTO ${MIGRATION_LEDGER} (name, sha256) VALUES ($1, $2)`, [step.name, sha]);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        log.error("migration failed", { reason: step.name, error_code: (err as { code?: string }).code ?? "migration_failed" });
        throw err;
      }
      applied += 1;
      log.info("migration applied", { reason: step.name });
    }
    if (!steps.some((s) => s.name.startsWith("verify:")))
      log.info("role validation left pending: in-process verifier disabled; an external verifier must pass it", { reason: gen });
  } finally {
    await client.query("SELECT pg_advisory_unlock(961996, 1)").catch(() => undefined);
  }
  log.info("hoopdb migrations complete", { count: applied, skipped });
  return { applied, skipped };
}

export async function migrate(): Promise<{ applied: number; skipped: number }> {
  const db = openHoopDb();
  await waitForHoopDb(db);
  const client = await db.pool.connect();
  try {
    return await runMigrations(client);
  } finally {
    client.release();
    await db.end();
  }
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "hoopdb-migrate";
  if (process.argv.includes("--generation")) {
    // For an external verifier: the generation its PASS record must name.
    generation().then((g) => {
      console.log(g);
      process.exit(0);
    });
  } else
    migrate()
      .then(() => process.exit(0))
      .catch((err) => {
        log.error("hoopdb migrate exited", { error_code: (err as { code?: string }).code ?? "migrate_failed" });
        if (optional("DEBUG", "") === "1") console.error(err);
        process.exit(1);
      });
}
