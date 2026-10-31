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
import { openHoopDb, waitForHoopDb } from "../shared/db.ts";
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
const WORKER_VERIFY = join(ROOT, "supabase/bootstrap/session_directory_search_worker_verify.sql");
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

export async function migrate(): Promise<{ applied: number; skipped: number }> {
  const db = openHoopDb();
  await waitForHoopDb(db);
  const client = await db.pool.connect();
  let applied = 0;
  let skipped = 0;
  try {
    await client.query("CREATE SCHEMA IF NOT EXISTS sessions");
    await client.query(`CREATE TABLE IF NOT EXISTS ${MIGRATION_LEDGER} (
      name text PRIMARY KEY,
      sha256 text NOT NULL,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    // One migrator at a time.
    await client.query("SELECT pg_advisory_lock(961996, 1)");
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
    for (const step of await plan()) {
      const sql = await readFile(step.path, "utf8");
      // The role checks (phase 1 bootstrap, phase 2 verify) run on every migrate, each in
      // its own transaction, and are never recorded in the ledger.
      if (step.name.startsWith("bootstrap:") || step.name.startsWith("verify:")) {
        await client.query("BEGIN");
        try {
          await client.query(sql);
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          log.error("role check failed", { reason: step.name, error_code: (err as { code?: string }).code ?? "role_check_failed" });
          throw err;
        }
        log.info("role check passed", { reason: step.name });
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
    await client.query("SELECT pg_advisory_unlock(961996, 1)");
  } finally {
    client.release();
    await db.end();
  }
  log.info("hoopdb migrations complete", { count: applied, skipped });
  return { applied, skipped };
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "hoopdb-migrate";
  migrate()
    .then(() => process.exit(0))
    .catch((err) => {
      log.error("hoopdb migrate exited", { error_code: (err as { code?: string }).code ?? "migrate_failed" });
      if (optional("DEBUG", "") === "1") console.error(err);
      process.exit(1);
    });
}
