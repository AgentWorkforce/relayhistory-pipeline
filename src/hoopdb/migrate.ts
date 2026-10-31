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
import { optional } from "../shared/env.ts";

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

/**
 * The database-mode contract. The migrator never infers the target from a hostname:
 * HOOPDB_DB_MODE is required and has no default.
 *   local       the Docker stack: plain Postgres, the migrator is its superuser.
 *   selfhosted  CNPG or Cloud SQL: plain Postgres, a least-privilege migration owner.
 *   supabase    a Supabase project. The plain-Postgres role shim and the privileged
 *               role bootstrap are refused outright (supabase_shim.sql: "Never apply
 *               this to a Supabase project"); the roles are platform or operator steps.
 * HOOPDB_SUPABASE_SHIM must be set explicitly ("true" or "false") in local and
 * selfhosted mode, and may only be unset or "false" in supabase mode.
 * HOOPDB_ROLE_BOOTSTRAP defaults to the shim choice and is likewise refused in supabase
 * mode. HOOPDB_RUNTIME_ROLE (required) names the role the services connect as: it gets
 * SELECT on the ledger and the readiness marker, nothing else.
 * Resolved before any connection; an invalid profile writes nothing anywhere.
 */
export type DbMode = "local" | "selfhosted" | "supabase";
export interface MigrationProfile {
  mode: DbMode;
  shim: boolean;
  bootstrap: boolean;
  verify: boolean;
  runtimeRole: string;
}

function profileError(code: string, message: string): Error {
  return Object.assign(new Error(`96bulls: ${message}`), { code });
}

function strictFlag(env: Record<string, string | undefined>, name: string): boolean | undefined {
  const v = env[name];
  if (v === undefined || v === "") return undefined;
  if (v === "1" || v.toLowerCase() === "true") return true;
  if (v === "0" || v.toLowerCase() === "false") return false;
  throw profileError("db_profile_invalid", `${name} must be "true" or "false" (got "${v}")`);
}

export function migrationProfile(env: Record<string, string | undefined> = process.env): MigrationProfile {
  const mode = env.HOOPDB_DB_MODE;
  if (mode !== "local" && mode !== "selfhosted" && mode !== "supabase")
    throw profileError("db_mode_invalid", "HOOPDB_DB_MODE must be local, selfhosted or supabase; there is no default");
  const shim = strictFlag(env, "HOOPDB_SUPABASE_SHIM");
  const bootstrap = strictFlag(env, "HOOPDB_ROLE_BOOTSTRAP");
  const verify = strictFlag(env, "HOOPDB_ROLE_VERIFY") ?? true;
  const runtimeRole = env.HOOPDB_RUNTIME_ROLE ?? "";
  if (!/^[A-Za-z_][A-Za-z0-9_$-]{0,62}$/.test(runtimeRole))
    throw profileError("runtime_role_unset", "HOOPDB_RUNTIME_ROLE must name the role the services connect as");
  if (mode === "supabase") {
    if (shim) throw profileError("shim_forbidden", "the plain-Postgres role shim never runs in supabase mode");
    if (bootstrap) throw profileError("bootstrap_forbidden", "the privileged role bootstrap never runs from the migrator in supabase mode");
    return { mode, shim: false, bootstrap: false, verify, runtimeRole };
  }
  if (shim === undefined)
    throw profileError("shim_unset", `HOOPDB_SUPABASE_SHIM must be set explicitly in ${mode} mode`);
  return { mode, shim, bootstrap: bootstrap ?? shim, verify, runtimeRole };
}

async function schemaSteps(): Promise<Step[]> {
  const upstream = (await readdir(UPSTREAM_MIGRATIONS)).filter((f) => f.endsWith(".sql")).sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  return [
    ...upstream.map((f) => ({ name: `upstream:${f}`, path: join(UPSTREAM_MIGRATIONS, f) })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
  ];
}

export async function plan(profile: MigrationProfile): Promise<Step[]> {
  return [
    ...(profile.shim ? [{ name: "shim:supabase_roles", path: SHIM }] : []),
    ...(profile.bootstrap ? [{ name: "bootstrap:session_directory_search_worker", path: WORKER_BOOTSTRAP }] : []),
    ...(await schemaSteps()),
    ...(profile.verify ? [{ name: "verify:session_directory_search_worker", path: WORKER_VERIFY }] : []),
  ];
}

/** Names the services wait for: the schema steps, independent of the migrator's profile. */
export async function expectedMigrationNames(): Promise<string[]> {
  return (await schemaSteps()).map((s) => s.name);
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
  /** Defaults to migrationProfile(process.env), which throws before any write. */
  profile?: MigrationProfile;
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
  const profile = opts.profile ?? migrationProfile();
  // One migrator at a time; every marker transition happens while this lock is held.
  await client.query("SELECT pg_advisory_lock(961996, 1)");
  try {
    const steps = opts.steps ?? (await plan(profile));
    // Read-only preflight: nothing below writes until both checks pass.
    const runtime = await runtimeRolePreflight(client, profile);
    await authorityPreflight(client, steps);
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
    // The runtime role reads the ledger and the marker for readiness: USAGE on the schema
    // (needed to reach them, creates nothing) and SELECT on those two tables, nothing else.
    if (runtime === "dedicated") {
      const grant = await client.query<{ sql: string }>(
        "SELECT format('GRANT USAGE ON SCHEMA sessions TO %I; GRANT SELECT ON %s, %s TO %I', $3::text, $1::text, $2::text, $3::text) AS sql",
        [MIGRATION_LEDGER, ROLE_VALIDATION_TABLE, profile.runtimeRole],
      );
      await client.query(grant.rows[0]!.sql);
    }
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

/**
 * The runtime role must exist. Either it is the migrator itself (owner-run; local and
 * selfhosted only), or it is a dedicated role that is not a superuser, not BYPASSRLS and
 * not the search Worker. It then gets SELECT on the ledger and the marker. Anything else
 * fails closed before any write.
 */
async function runtimeRolePreflight(client: MigrationClient, profile: MigrationProfile): Promise<"owner" | "dedicated"> {
  const r = (
    await client.query<{ rolname: string; rolsuper: boolean; rolbypassrls: boolean; me: string }>(
      "SELECT rolname, rolsuper, rolbypassrls, current_user AS me FROM pg_roles WHERE rolname = $1",
      [profile.runtimeRole],
    )
  ).rows[0];
  if (!r) throw profileError("runtime_role_missing", `HOOPDB_RUNTIME_ROLE ${profile.runtimeRole} does not exist; nothing was written`);
  if (r.rolname === r.me) {
    if (profile.mode === "supabase")
      throw profileError("runtime_role_unsafe", "in supabase mode the services need a dedicated runtime role, not the migration owner");
    return "owner";
  }
  if (r.rolsuper || r.rolbypassrls || r.rolname === ROLE_VALIDATION_NAME)
    throw profileError("runtime_role_unsafe", `HOOPDB_RUNTIME_ROLE ${r.rolname} must not be a superuser, BYPASSRLS or the search Worker`);
  return "dedicated";
}

/**
 * One migration authority. Refuses, before any write, when:
 * - Supabase's CLI history (supabase_migrations.schema_migrations) records any of these
 *   migrations, meaning `supabase db push` was used as well (mixed_migration_authority);
 * - the sessions schema exists (sessions.session_index) without a ledger that records it
 *   (unledgered_schema);
 * - the ledger skips a step that a later recorded step depends on (ledger_gap).
 * Nothing is ever imported into, or marked in, the ledger. Resolving it is the decision
 * point in runbooks/session-directory-search-worker-bootstrap.md ("One migration authority").
 */
async function authorityPreflight(client: MigrationClient, steps: Step[]): Promise<void> {
  const reg = async (name: string) =>
    (await client.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [name])).rows[0]?.r ?? null;
  const schema = steps.filter((st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"));
  let cliHistory: string | null;
  try {
    cliHistory = await reg("supabase_migrations.schema_migrations");
  } catch (err) {
    // The schema exists but the migrator may not even look inside: it can't prove there
    // is no second authority.
    if ((err as { code?: string }).code === "42501")
      throw profileError("mixed_migration_authority", "supabase_migrations exists but the migrator cannot read it; one authority only, see the runbook decision point");
    throw err;
  }
  if (cliHistory) {
    const versions = schema.filter((st) => st.name.startsWith("upstream:")).map((st) => st.name.slice("upstream:".length).split("_")[0]!);
    let hit: { version: string }[];
    try {
      hit = (
        await client.query<{ version: string }>(
          "SELECT version FROM supabase_migrations.schema_migrations WHERE version = ANY($1::text[]) ORDER BY version",
          [versions],
        )
      ).rows;
    } catch (err) {
      // Present but unreadable by the migrator: it can't prove there is no second authority.
      if ((err as { code?: string }).code === "42501")
        throw profileError("mixed_migration_authority", "supabase_migrations.schema_migrations exists but the migrator cannot read it; one authority only, see the runbook decision point");
      throw err;
    }
    if (hit.length)
      throw profileError("mixed_migration_authority", `supabase db push recorded ${hit.map((h) => h.version).join(", ")}; one authority only, see the runbook decision point`);
  }
  const ledgered = (await reg(MIGRATION_LEDGER))
    ? new Set((await client.query<{ name: string }>(`SELECT name FROM ${MIGRATION_LEDGER}`)).rows.map((r) => r.name))
    : new Set<string>();
  const firstUpstream = schema.find((st) => st.name.startsWith("upstream:"));
  if ((await reg("sessions.session_index")) && firstUpstream && !ledgered.has(firstUpstream.name))
    throw profileError("unledgered_schema", "sessions schema exists without a ledger that records it; nothing is imported, see the runbook decision point");
  for (const group of ["upstream:", "96bulls:"]) {
    const names = schema.filter((st) => st.name.startsWith(group)).map((st) => st.name);
    const lastLedgered = names.map((n) => ledgered.has(n)).lastIndexOf(true);
    const gap = lastLedgered > 0 ? names.slice(0, lastLedgered).find((n) => !ledgered.has(n)) : undefined;
    if (gap) throw profileError("ledger_gap", `${gap} is not recorded but a later step is; nothing is marked, see the runbook decision point`);
  }
}

export async function migrate(): Promise<{ applied: number; skipped: number }> {
  // Before any connection: an invalid profile fails here, having written nothing.
  const profile = migrationProfile();
  const db = openHoopDb();
  await waitForHoopDb(db);
  const client = await db.pool.connect();
  try {
    return await runMigrations(client, { profile });
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
