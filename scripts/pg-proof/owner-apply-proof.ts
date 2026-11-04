/** DESTRUCTIVE SYNTHETIC FIXTURE ONLY: refuses any container without the exact ownership label.
 * No production CLI/auth/network path. Run only with --owned-fixture. */
import pg from "pg";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readFile, writeFile, mkdir } from "node:fs/promises";
const root = fileURLToPath(new URL("../..", import.meta.url));
if (process.argv.length !== 3 || process.argv[2] !== "--owned-fixture")
  throw Error("owned_fixture_argument_required");
const container = "owner-apply-proof-pg-20261104";
const metadata = JSON.parse(
  execFileSync("docker", ["inspect", "--format", "{{json .}}", container], {
    encoding: "utf8",
    maxBuffer: 1024 * 1024,
  }),
);
const binding = metadata.NetworkSettings?.Ports?.["5432/tcp"];
if (
  metadata.Config?.Labels?.["migration.owner"] !== "codex-owner-apply" ||
  !Array.isArray(binding) ||
  binding.length !== 1 ||
  binding[0].HostIp !== "127.0.0.1" ||
  !/^\d+$/u.test(binding[0].HostPort)
)
  throw Error("owned_fixture_identity");
const port = Number(binding[0].HostPort);
const {
  withOwnerSession,
  ownerFencedClient,
  ownerIdentity,
  ownerProfile,
  wrapperSource,
  refreshOwnerWrapper,
} = await import(root + "/src/hoopdb/owner-operator-apply.ts");
const { runMigrations, generation } = await import(
  root + "/src/hoopdb/migrate.ts"
);
const { functionBoundaryEvidence } = await import(
  root + "/src/hoopdb/migrate-guards.ts"
);
const admin = new pg.Client({
  host: "127.0.0.1",
  port,
  user: "proof_admin",
  database: "postgres",
  password: "synthetic-owner-proof-only",
});
await admin.connect();
const server = (
  await admin.query(
    "SELECT current_user AS me,session_user AS login,current_setting('server_version_num')::int AS version",
  )
).rows[0];
if (
  server.me !== "proof_admin" ||
  server.login !== "proof_admin" ||
  server.version < 170000 ||
  server.version >= 180000
)
  throw Error("owned_fixture_database_identity");
const read = (p: string) => readFile(root + "/" + p, "utf8");
await admin.query(
  "DROP SCHEMA IF EXISTS hoopdb_boundary CASCADE;DROP SCHEMA IF EXISTS sessions CASCADE;ALTER ROLE postgres SUPERUSER;SET ROLE postgres;CREATE SCHEMA sessions;GRANT CREATE ON SCHEMA public TO postgres;REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC;ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;",
);
await admin.query(await read("supabase/tests/supabase_shim.sql"));
await admin.query(
  await read("supabase/bootstrap/session_directory_search_worker.sql"),
);
await admin.query(await read("supabase/bootstrap/session_token_verifier.sql"));
await admin.query(
  "DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_runtime') THEN CREATE ROLE bulls96_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION; END IF; END $$",
);
await admin.query(
  "CREATE EXTENSION IF NOT EXISTS vector;CREATE EXTENSION IF NOT EXISTS pg_trgm;CREATE EXTENSION IF NOT EXISTS btree_gin;",
);
await admin.query(
  (
    await read(
      "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
    )
  )
    .split("\n")
    .filter((l) => !l.startsWith("\\"))
    .join("\n")
    .replaceAll(":'runtime_role'", "'bulls96_runtime'"),
);
await admin.query(
  "RESET ROLE;ALTER ROLE postgres NOSUPERUSER;ALTER DEFAULT PRIVILEGES FOR ROLE proof_admin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
);
const events: any[] = [];
function client(startup = true) {
  return new pg.Client({
    host: "127.0.0.1",
    port,
    user: "cli_login_postgres",
    database: "postgres",
    password: "synthetic-cli-proof-only",
    options: startup
      ? "-c role=postgres -c statement_timeout=30000"
      : undefined,
  });
}
let entered = false;
try {
  await withOwnerSession(client(false), async () => {
    entered = true;
  });
  throw Error("missing startup role accepted");
} catch (e: any) {
  if (e.code !== "owner_session_identity") throw e;
  events.push({ case: "no-startup-reset-refused", entered });
}
const c = client();
await withOwnerSession(c, async (pid: number) => {
  const guard = ownerFencedClient(c, pid);
  const first = await ownerIdentity(c, pid, false);
  await c.query("BEGIN;RESET ROLE;COMMIT");
  const afterReset = await ownerIdentity(c, pid, false);
  try {
    await guard.query(
      "RESET ROLE;CREATE TABLE sessions.should_not_exist(id int)",
    );
    throw Error("role reset accepted");
  } catch (e: any) {
    if (e.code !== "owner_sql_role_control") throw e;
  }
  const absent =
    (await c.query("SELECT to_regclass('sessions.should_not_exist') AS t"))
      .rows[0].t === null;
  // Existing wrapper stays same-owner/ACL/defaults; force an SQL failure inside refresh.
  const defaults = async () =>
    (
      await c.query(
        "SELECT defaclrole::regrole::text AS role,defaclnamespace::regnamespace::text AS ns,defaclobjtype,defaclacl::text FROM pg_default_acl ORDER BY 1,2,3",
      )
    ).rows;
  const defaultsBefore = JSON.stringify(await defaults());
  const wrapper = await wrapperSource();
  const old = wrapper.sql.replace(
    "$owner_apply_boundary$;",
    "\n-- fixture stale source\n$owner_apply_boundary$;",
  );
  await c.query(old);
  const before = await functionBoundaryEvidence(
    c,
    "global-default-revoke",
    "supabase",
    "bulls96_runtime",
  );
  const injected = {
    query: async (sql: string, params?: unknown[]) => {
      if (sql === wrapper.sql)
        throw Object.assign(Error("fixture SQL failure"), {
          code: "fixture_refresh",
        });
      return c.query(sql, params);
    },
  };
  try {
    await refreshOwnerWrapper(injected, pid, wrapper);
    throw Error("failure injection ignored");
  } catch (e: any) {
    if (e.code !== "fixture_refresh") throw e;
  }
  const failed = await functionBoundaryEvidence(
    c,
    "global-default-revoke",
    "supabase",
    "bulls96_runtime",
  );
  if (before.event_trigger.prosrc_sha256 !== failed.event_trigger.prosrc_sha256)
    throw Error("failed refresh changed body");
  await refreshOwnerWrapper(c, pid, wrapper);
  const fresh = await functionBoundaryEvidence(
    c,
    "event-trigger",
    "supabase",
    "bulls96_runtime",
  );
  if (!fresh.satisfied) throw Error("wrapper not exact after refresh");
  const queries: string[] = [];
  const traced = {
    query: async <R>(sql: string, params?: unknown[]) => {
      queries.push(sql);
      return guard.query<R>(sql, params);
    },
  };
  const firstSql = await read(
    "supabase/migrations/20261025000000_sessions_index.sql",
  );
  let injectedMigration = false;
  const failClient = {
    query: async <R>(sql: string, params?: unknown[]) => {
      if (sql === firstSql && !injectedMigration) {
        injectedMigration = true;
        throw Object.assign(Error("fixture migration failed"), {
          code: "fixture_migration",
        });
      }
      return traced.query<R>(sql, params);
    },
  };
  try {
    await runMigrations(failClient, { profile: ownerProfile() });
    throw Error("migration failure injection ignored");
  } catch (e: any) {
    if (e.code !== "fixture_migration") throw e;
  }
  const failedMarker = (
    await c.query(
      "SELECT generation,status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'",
    )
  ).rows[0];
  const failedLedger = (
    await c.query(
      "SELECT count(*)::int AS n FROM sessions.__96bulls_migrations",
    )
  ).rows[0].n;
  if (failedMarker?.status !== "pending" || failedLedger !== 0)
    throw Error("failed migration advanced ledger/pass");
  await ownerIdentity(c, pid, false);
  const result = await runMigrations(traced, { profile: ownerProfile() });
  try {
    await runMigrations(traced, {
      profile: ownerProfile(),
      beforeVerify: async () => {
        throw Object.assign(Error("fixture verify failed"), {
          code: "fixture_verify",
        });
      },
    });
    throw Error("verify failure ignored");
  } catch (e: any) {
    if (e.code !== "fixture_verify") throw e;
  }
  const verifyFailed = (
    await c.query(
      "SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'",
    )
  ).rows[0].status;
  if (verifyFailed !== "pending") throw Error("failed verifier left pass");
  const retry = await runMigrations(traced, { profile: ownerProfile() });
  const defaultsUnchanged = defaultsBefore === JSON.stringify(await defaults());
  const owners = (
    await c.query(
      "SELECT count(*)::int AS foreign FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='sessions' AND t.relowner<>(SELECT oid FROM pg_roles WHERE rolname='postgres')",
    )
  ).rows[0];
  const marker = (
    await c.query(
      "SELECT generation,status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'",
    )
  ).rows;
  events.push({
    case: "startup-reset-owner-and-canonical-migrator",
    pid,
    firstOwner: first.owner,
    afterResetOwner: afterReset.owner,
    resetDeniedBeforeSql: absent,
    refreshFailureRolledBack: true,
    wrapperExact: fresh.event_trigger.verified,
    result,
    foreignObjects: owners.foreign,
    transactions: queries.filter((q) => q === "BEGIN").length,
    sourceGeneration: await generation(),
    marker,
    migrationFailurePending: failedMarker.status,
    migrationFailureLedger: failedLedger,
    verifierFailurePending: verifyFailed,
    retry,
    defaultsUnchanged,
  });
});
await admin.end();
const output = root + "/.96bulls/owner-apply-proof";
await mkdir(output, { recursive: true });
await writeFile(
  output + "/proof.json",
  JSON.stringify(
    {
      sourceRoot: root,
      sourceSha: execFileSync("git", ["rev-parse", "HEAD"], {
        encoding: "utf8",
      }).trim(),
      serverVersion: server.version,
      port,
      events,
    },
    null,
    2,
  ),
);
console.log(JSON.stringify(events));
