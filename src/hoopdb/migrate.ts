/**
 * HoopDb migrations, in order: the Supabase role shim (plain Postgres only),
 * the vendored relayhistory-cloud `sessions` migrations exactly as pinned, then
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
const VENDOR_MIGRATIONS = join(ROOT, "vendor/relayhistory-cloud/supabase/migrations");
const SHIM = join(ROOT, "vendor/relayhistory-cloud/supabase/tests/supabase_shim.sql");
const OURS = join(ROOT, "src/hoopdb/migrations");

export const MIGRATION_LEDGER = "sessions.__96bulls_migrations";

interface Step {
  name: string;
  path: string;
}

export async function plan(): Promise<Step[]> {
  const vendor = (await readdir(VENDOR_MIGRATIONS)).filter((f) => f.endsWith(".sql")).sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  return [
    ...(flag("HOOPDB_SUPABASE_SHIM", true) ? [{ name: "shim:supabase_roles", path: SHIM }] : []),
    ...vendor.map((f) => ({ name: `vendor:${f}`, path: join(VENDOR_MIGRATIONS, f) })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
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
    for (const step of await plan()) {
      const sql = await readFile(step.path, "utf8");
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
