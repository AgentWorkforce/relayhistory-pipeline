/**
 * Real-PostgreSQL proof of the migrator's role, readiness and authority guards.
 *
 * Runs against ONE disposable PostgreSQL server (CI: the pg-proof workflow's service
 * container; locally: a throwaway loopback cluster). Never point it at a real database:
 * it creates and drops roles and databases. Trust auth only; no credential is used.
 *
 * Env:
 *   PGPROOF_HOST / PGPROOF_PORT   the server (default 127.0.0.1:5432)
 *   PGPROOF_SUPERUSER             a superuser (default postgres)
 *   PGPROOF_PSQL                  psql binary (default psql)
 *   PGPROOF_HEAD                  the commit under test (recorded in the results)
 *   PGPROOF_OUT                   the JSON results file (default pg-proof.json)
 *
 * Exit 0 only if every case passes. The JSON records head, server version and each
 * case's name, pass and detail, so a reviewer can verify the run mechanically.
 */
import { spawnSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  inTransaction,
  openHoopDb,
  schemaVerdict,
} from "../../src/shared/db.ts";
import {
  expectedSchema,
  generation,
  MARKER_PENDING_SQL,
} from "../../src/hoopdb/migrate.ts";
import { NON_CREATE_STEPS } from "../../src/hoopdb/migration-sentinels.ts";
import {
  RUNTIME_APPLICATION_FUNCTIONS,
  sessionsFunctionAcl,
} from "../../src/hoopdb/migrate-guards.ts";
import { gateOpen, workerTick } from "../../src/shared/worker.ts";
import { hoopdbModeEnv } from "../../stacks/aws/eks.ts";
import { gcpHoopdbModeEnv } from "../../stacks/gcp/config.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const HOST = process.env.PGPROOF_HOST ?? "127.0.0.1";
const PORT = process.env.PGPROOF_PORT ?? "5432";
const SUPER = process.env.PGPROOF_SUPERUSER ?? "postgres";
const PSQL = process.env.PGPROOF_PSQL ?? "psql";
const OUT = process.env.PGPROOF_OUT ?? "pg-proof.json";
const W = "session_directory_search_worker";
const functionManifestForRuntime = (role: string) => {
  const base = JSON.parse(
    readFileSync(join(ROOT, "src/hoopdb/sessions-function-acl.json"), "utf8"),
  ).functions as Record<string, string[]>;
  return Object.fromEntries(
    Object.entries(base).map(([fn, entries]) => [
      fn,
      RUNTIME_APPLICATION_FUNCTIONS.includes(
        fn as (typeof RUNTIME_APPLICATION_FUNCTIONS)[number],
      )
        ? [...entries, `${role}:EXECUTE`]
        : entries,
    ]),
  );
};
const url = (user: string, db: string) =>
  `postgres://${user}@${HOST}:${PORT}/${db}`;
const BOOT = join(
  ROOT,
  "supabase/bootstrap/session_directory_search_worker.sql",
);
const RUNTIME_BOOT = join(ROOT, "supabase/bootstrap/hoopdb_runtime_role.sql");
const SUPABASE_RUNTIME_BOOT = join(
  ROOT,
  "supabase/bootstrap/hoopdb_runtime_role_supabase.sql",
);
const CLOUDSQL_ROLES_BOOT = join(
  ROOT,
  "supabase/bootstrap/cloudsql_migration_roles.sql",
);
const MARK_PASS = join(
  ROOT,
  "supabase/bootstrap/session_directory_search_worker_mark_pass.sql",
);
const SELF = {
  HOOPDB_DB_MODE: "selfhosted",
  HOOPDB_SUPABASE_SHIM: "true",
  HOOPDB_RUNTIME_ROLE: "g6_rt",
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
};
const FN_DEFAULT_BOOT = join(
  ROOT,
  "supabase/bootstrap/hoopdb_function_default_revoke.sql",
);

interface Case {
  name: string;
  pass: boolean;
  detail: string;
}
const cases: Case[] = [];

async function sql(
  db: string,
  text: string,
  user = SUPER,
): Promise<pg.QueryResult> {
  const c = new pg.Client({ connectionString: url(user, db) });
  await c.connect();
  try {
    return await c.query(text);
  } finally {
    await c.end();
  }
}
/** SQLSTATE of a statement run as `user`, or "ok". */
async function attempt(
  db: string,
  text: string,
  user = SUPER,
): Promise<string> {
  try {
    await sql(db, text, user);
    return "ok";
  } catch (e) {
    return (e as { code?: string }).code ?? "error";
  }
}
function psql(
  db: string,
  args: string[],
  user = SUPER,
): { ok: boolean; out: string } {
  const r = spawnSync(
    PSQL,
    [
      "-X",
      "-q",
      "-v",
      "ON_ERROR_STOP=1",
      "-h",
      HOST,
      "-p",
      PORT,
      "-U",
      user,
      "-d",
      db,
      ...args,
    ],
    { encoding: "utf8" },
  );
  return { ok: r.status === 0, out: `${r.stdout}${r.stderr}` };
}
/** Runs the real migrator as `hoop`; returns its exit status and the error_code it logged. */
function migrate(
  db: string,
  env: Record<string, string>,
  user = "hoop",
): { exit: number; code: string } {
  const clean = Object.fromEntries(
    Object.entries(process.env).filter(([k]) => !k.startsWith("HOOPDB_")),
  );
  const r = spawnSync("bun", ["src/hoopdb/migrate.ts"], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...clean, HOOPDB_URL: url(user, db), ...env },
  });
  const codes = [
    ...`${r.stdout}${r.stderr}`.matchAll(/"error_code":"([^"]+)"/g),
  ].map((m) => m[1]!);
  return { exit: r.status ?? -1, code: codes.at(-1) ?? "" };
}
/** A fresh database owned by hoop; the selfhosted runtime-role bootstrap runs IN it for each role (it revokes TEMPORARY from PUBLIC there and grants the role CONNECT). */
/** With runtime roles, also the selfhosted function-default bootstrap (owner hoop), unless fnRevoke is false. */
async function fresh(
  db: string,
  runtimeRoles: string[] = [],
  fnRevoke = true,
): Promise<void> {
  await sql("postgres", `DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
  await sql("postgres", `CREATE DATABASE ${db} OWNER hoop`);
  if (runtimeRoles.length && fnRevoke) {
    const b = psql(db, ["-v", "migration_owner=hoop", "-f", FN_DEFAULT_BOOT]);
    if (!b.ok)
      throw new Error(`function default bootstrap failed in ${db}: ${b.out}`);
  }
  for (const r of runtimeRoles) {
    const b = psql(db, [
      "-v",
      `runtime_role=${r}`,
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    if (!b.ok) throw new Error(`runtime bootstrap failed in ${db}: ${b.out}`);
  }
}
const SCHEMA_FILES = [
  ...readdirSync(join(ROOT, "supabase/migrations"))
    .filter((f) => f.endsWith(".sql"))
    .sort()
    .map((f) => ({
      name: `upstream:${f}`,
      path: join(ROOT, "supabase/migrations", f),
    })),
  ...readdirSync(join(ROOT, "src/hoopdb/migrations"))
    .filter((f) => f.endsWith(".sql"))
    .sort()
    .map((f) => ({
      name: `96bulls:${f}`,
      path: join(ROOT, "src/hoopdb/migrations", f),
    })),
];
/** Applies schema steps [0, upTo] as hoop and writes ledger rows for [0, ledgered). */
async function prefixDb(
  db: string,
  upTo: number,
  ledgered: number,
): Promise<void> {
  for (let i = 0; i <= upTo; i++) {
    const r = psql(db, ["-1", "-f", SCHEMA_FILES[i]!.path], "hoop");
    if (!r.ok) throw new Error(`applying ${SCHEMA_FILES[i]!.name}: ${r.out}`);
  }
  await sql(
    db,
    "CREATE TABLE sessions.__96bulls_migrations (name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
    "hoop",
  );
  for (let i = 0; i < ledgered; i++) {
    const sha = createHash("sha256")
      .update(readFileSync(SCHEMA_FILES[i]!.path, "utf8"))
      .digest("hex");
    await sql(
      db,
      `INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ('${SCHEMA_FILES[i]!.name}', '${sha}')`,
      "hoop",
    );
  }
}
/** Ledger + marker + grants on the approved objects, or "absent". */
async function snapshot(db: string): Promise<string> {
  // Relation names resolve at parse time, so only reference the tables that exist.
  const has = (
    await sql(
      db,
      "SELECT to_regclass('sessions.__96bulls_migrations') IS NOT NULL AS l, to_regclass('sessions.__96bulls_role_validation') IS NOT NULL AS m",
    )
  ).rows[0];
  if (!has.l && !has.m) return "absent";
  const parts = [
    has.l
      ? "coalesce((SELECT string_agg(name || sha256 || applied_at, ',' ORDER BY name) FROM sessions.__96bulls_migrations), '')"
      : "'no-ledger'",
    has.m
      ? "coalesce((SELECT string_agg(name || generation || status || updated_at, ',') FROM sessions.__96bulls_role_validation), '')"
      : "'no-marker'",
    "coalesce((SELECT string_agg(relname || coalesce(relacl::text, ''), ',' ORDER BY relname) FROM pg_class WHERE relname IN ('__96bulls_migrations', '__96bulls_role_validation')), '')",
    "coalesce((SELECT nspacl::text FROM pg_namespace WHERE nspname = 'sessions'), '')",
  ];
  return (await sql(db, `SELECT md5(${parts.join(" || ")}) AS s`)).rows[0]
    .s as string;
}

function historicalShortSummary(current: string): string {
  const replaceOnce = (source: string, from: string, to: string) => {
    if (source.split(from).length !== 2)
      throw new Error(
        "historical 020000 reconstruction no longer has one exact source match",
      );
    return source.replace(from, to);
  };
  let old = replaceOnce(
    current,
    `-- refused. It is a member of no role. Allowed members are \`authenticator\` under the
-- existing contract and, only when the migrator binds exact \`supabase\` mode on PG16+,
-- the unavoidable postgres creator edge granted by superuser supabase_admin with ADMIN
-- true and INHERIT/SET false. It owns no objects and holds no explicit privileges, except USAGE on schema`,
    `-- refused. It is a member of no role. Its only member may be \`authenticator\`,
-- NOINHERIT, without ADMIN OPTION (and on PostgreSQL 16+ without INHERIT on the grant).
-- So no other role, the migration owner included, holds membership or ADMIN OPTION on
-- it. It owns no objects and holds no explicit privileges, except USAGE on schema`,
  );
  old = replaceOnce(
    old,
    `  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
`,
    "",
  );
  const newGrantGuard = `  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT r.rolname, r.rolinherit, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS r ON r.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY r.rolname, g.rolname' USING v_oid LOOP
      IF v_mode = 'supabase'
         AND v_row.rolname = 'postgres'
         AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL;
      ELSIF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT r.rolname, r.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS r ON r.oid = m.member
                  WHERE m.roleid = v_oid LOOP
      IF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  END IF;`;
  const oldGrantGuard = `  FOR v_row IN SELECT r.rolname, r.rolinherit, m.admin_option FROM pg_auth_members AS m
                 JOIN pg_roles AS r ON r.oid = m.member
                WHERE m.roleid = v_oid LOOP
    IF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
      v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
    END IF;
  END LOOP;`;
  old = replaceOnce(old, newGrantGuard, oldGrantGuard);
  return replaceOnce(
    old,
    "  IF v_version >= 160000 THEN",
    "  IF current_setting('server_version_num')::int >= 160000 THEN",
  );
}

async function sessionsCatalogSnapshot(
  db: string,
): Promise<Record<string, string>> {
  const queries: Record<string, string> = {
    namespace: `SELECT nspname, pg_get_userbyid(nspowner) owner, nspacl::text
                  FROM pg_namespace WHERE nspname='sessions' ORDER BY nspname`,
    default_acl: `SELECT pg_get_userbyid(d.defaclrole) owner,
                         coalesce(n.nspname, '') namespace,
                         d.defaclobjtype, d.defaclacl::text
                    FROM pg_default_acl d
                    LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace
                   WHERE d.defaclnamespace=0 OR n.nspname='sessions'
                   ORDER BY 1,2,3,4`,
    class: `SELECT c.relname, c.relkind, c.relpersistence, pg_get_userbyid(c.relowner) owner,
                   c.relacl::text, c.reloptions::text, c.relrowsecurity, c.relforcerowsecurity,
                   c.relreplident, pg_get_expr(c.relpartbound, c.oid) partbound
              FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname='sessions' ORDER BY c.relname`,
    attribute: `SELECT c.relname, a.attnum, a.attname, a.atttypid::regtype::text,
                       a.attnotnull, a.attidentity, a.attgenerated, a.attacl::text,
                       pg_get_expr(d.adbin,d.adrelid) default_expr
                  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                  JOIN pg_attribute a ON a.attrelid=c.oid
                  LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
                 WHERE n.nspname='sessions' AND a.attnum>0 AND NOT a.attisdropped
                 ORDER BY c.relname,a.attnum`,
    proc: `SELECT p.proname, pg_get_function_identity_arguments(p.oid) args,
                  pg_get_function_result(p.oid) result, p.prosrc, p.proconfig::text,
                  p.prosecdef, p.proleakproof, p.provolatile, p.proparallel, p.proacl::text
             FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='sessions' ORDER BY p.proname,args`,
    constraints: `SELECT c.relname, x.conname, x.contype, x.condeferrable,
                         x.condeferred, x.convalidated, pg_get_constraintdef(x.oid,true) def
                    FROM pg_constraint x LEFT JOIN pg_class c ON c.oid=x.conrelid
                    JOIN pg_namespace n ON n.oid=x.connamespace
                   WHERE n.nspname='sessions' ORDER BY c.relname,x.conname`,
    indexes: `SELECT t.relname table_name, i.relname index_name, pg_get_indexdef(i.oid) def
                FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
                JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
               WHERE n.nspname='sessions' ORDER BY t.relname,i.relname`,
    triggers: `SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid,true) def
                 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                 JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='sessions' AND NOT t.tgisinternal ORDER BY c.relname,t.tgname`,
    policies: `SELECT c.relname, p.polname, p.polpermissive, p.polcmd,
                      ARRAY(SELECT pg_get_userbyid(x) FROM unnest(p.polroles) x ORDER BY 1)::text roles,
                      pg_get_expr(p.polqual,p.polrelid) qual,
                      pg_get_expr(p.polwithcheck,p.polrelid) with_check
                 FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid
                 JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='sessions' ORDER BY c.relname,p.polname`,
    comments: `SELECT pg_describe_object(d.classoid,d.objoid,d.objsubid) object, d.description
                 FROM pg_description d
                WHERE (d.classoid='pg_class'::regclass AND d.objoid IN
                         (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions'))
                   OR (d.classoid='pg_proc'::regclass AND d.objoid IN
                         (SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions'))
                ORDER BY 1,2`,
  };
  const out: Record<string, string> = {};
  for (const [name, query] of Object.entries(queries))
    out[name] = createHash("sha256")
      .update(JSON.stringify((await sql(db, query, "hoop")).rows))
      .digest("hex");
  return out;
}
async function verdict(
  db: string,
  user = "hoop",
  opts: Parameters<typeof schemaVerdict>[2] = {},
) {
  const h = openHoopDb(url(user, db));
  try {
    return await schemaVerdict(
      await expectedSchema(),
      (w) => inTransaction(h, w),
      opts,
    );
  } finally {
    await h.end();
  }
}
async function check(
  name: string,
  fn: () => Promise<[boolean, string]>,
): Promise<void> {
  try {
    const [pass, detail] = await fn();
    cases.push({ name, pass, detail });
  } catch (e) {
    cases.push({ name, pass: false, detail: `threw: ${(e as Error).message}` });
  }
  const c = cases.at(-1)!;
  console.log(`${c.pass ? "PASS" : "FAIL"}  ${name}  ${c.detail}`);
}
const fmt = (o: unknown) => JSON.stringify(o);

const MAJOR =
  (Number(
    (await sql("postgres", "SHOW server_version_num")).rows[0]
      .server_version_num,
  ) /
    10000) |
  0;
const version = (await sql("postgres", "SHOW server_version")).rows[0]
  .server_version as string;
console.log(
  `# PostgreSQL ${version}, head ${process.env.PGPROOF_HEAD ?? "unknown"}`,
);

// Clean slate: this harness's own databases and roles from any earlier run.
for (const d of (
  await sql(
    "postgres",
    "SELECT datname FROM pg_database WHERE datname LIKE 'g6\\_%'",
  )
).rows)
  await sql("postgres", `DROP DATABASE ${d.datname} WITH (FORCE)`);
for (const r of [
  "g6_rt",
  "g6_rt2",
  "g6_rt3",
  "g6_bad",
  "g6_m",
  "g6_never",
  "g6_workload",
  "g6_second_workload",
  "g6_workload_rt",
  "bulls96_runtime_workload",
  "bulls96_workload",
  "g6_other",
  "g6_caller",
  "g6_unrel",
  "g6_probe",
  "g6_other_creator",
  "g6_can_create",
  "g6_nobody",
  "supabase_admin",
  "ops_admin",
  "g6_cloud_owner",
  "g6_cloud_rt",
  "cloudsqladmin",
  "cloudsqlsuperuser",
  "bulls96_cloudsql_role_seed",
]) {
  for (const d of (
    await sql("postgres", "SELECT datname FROM pg_database WHERE datallowconn")
  ).rows)
    await attempt(
      d.datname,
      `DO $x$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${r}') THEN EXECUTE 'DROP OWNED BY ${r}'; END IF; END $x$`,
    );
  await attempt("postgres", `DROP ROLE IF EXISTS ${r}`);
}

// Setup: a CNPG-shaped owner (no superuser, no CREATEROLE), the Supabase role stand-ins,
// the Worker role and the dedicated runtime role, all by the superuser.
await sql(
  "postgres",
  `DO $x$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hoop') THEN CREATE ROLE hoop LOGIN; END IF; END $x$;
  ALTER ROLE hoop NOSUPERUSER NOCREATEROLE NOCREATEDB NOBYPASSRLS`,
);

// Cloud SQL is pinned to PostgreSQL 17. Prove its complete fresh-database profile with
// the migration owner as a real non-superuser session (not SET ROLE): the disposable
// CREATEROLE trampoline must leave every compatibility role membership-free, the generic
// BYPASSRLS shim stays off, and the full migrator must pass.
await check(
  "Cloud SQL PG17: non-superuser operator bootstrap and full profile pass without the generic shim",
  async () => {
    if (MAJOR < 17)
      return [
        true,
        `skipped: Cloud SQL proof is pinned to PostgreSQL 17 (server is ${MAJOR})`,
      ];
    await sql(
      "postgres",
      `CREATE ROLE cloudsqlsuperuser NOLOGIN;
       CREATE ROLE cloudsqladmin SUPERUSER NOLOGIN;
       CREATE ROLE g6_cloud_owner LOGIN CREATEROLE NOBYPASSRLS;
       GRANT cloudsqlsuperuser TO g6_cloud_owner;
       CREATE ROLE g6_cloud_rt LOGIN NOCREATEROLE NOCREATEDB NOBYPASSRLS`,
    );
    await sql("postgres", "CREATE DATABASE g6_cloud OWNER g6_cloud_owner");
    await sql(
      "g6_cloud",
      `REVOKE TEMPORARY ON DATABASE g6_cloud FROM PUBLIC;
       GRANT CONNECT ON DATABASE g6_cloud TO g6_cloud_rt;
       ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`,
    );
    await sql(
      "g6_cloud",
      "ALTER DEFAULT PRIVILEGES FOR ROLE g6_cloud_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
      "g6_cloud_owner",
    );
    const boot = psql(
      "g6_cloud",
      ["-f", CLOUDSQL_ROLES_BOOT],
      "g6_cloud_owner",
    );
    const phase1 = psql("g6_cloud", ["-f", BOOT], "g6_cloud_owner");
    const env = {
      ...Object.fromEntries(
        gcpHoopdbModeEnv("g6_cloud_rt", true).map((e) => [e.name, e.value]),
      ),
      HOOPDB_SUPABASE_SHIM: "false",
    };
    const m = migrate("g6_cloud", env, "g6_cloud_owner");
    const again = psql(
      "g6_cloud",
      ["-f", CLOUDSQL_ROLES_BOOT],
      "g6_cloud_owner",
    );
    const targets = (
      await sql(
        "g6_cloud",
        `SELECT role.rolname, role.rolbypassrls,
                (SELECT count(*)::int FROM pg_auth_members membership
                  WHERE membership.roleid = role.oid OR membership.member = role.oid) AS memberships
           FROM pg_roles role
          WHERE role.rolname IN ('anon', 'authenticated', 'service_role', 'session_directory_search_worker')
          ORDER BY 1`,
      )
    ).rows;
    const seed = (
      await sql(
        "g6_cloud",
        "SELECT count(*)::int AS n FROM pg_roles WHERE rolname = 'bulls96_cloudsql_role_seed'",
      )
    ).rows[0].n;
    const v = await verdict("g6_cloud", "g6_cloud_owner");
    const ok =
      boot.ok &&
      phase1.ok &&
      again.ok &&
      m.exit === 0 &&
      v.ok &&
      seed === 0 &&
      targets.length === 4 &&
      targets.every(
        (role) => role.memberships === 0 && role.rolbypassrls === false,
      );
    return [
      ok,
      `bootstrap=${boot.ok} phase1=${phase1.ok} rerun=${again.ok} migrate=${fmt(m)} verdict=${fmt(v)} seed=${seed} targets=${fmt(targets)}`,
    ];
  },
);
if (MAJOR >= 17) {
  await attempt("postgres", "DROP DATABASE IF EXISTS g6_cloud WITH (FORCE)");
  for (const role of [
    "session_directory_search_worker",
    "service_role",
    "authenticated",
    "anon",
    "g6_cloud_rt",
    "g6_cloud_owner",
    "cloudsqladmin",
    "cloudsqlsuperuser",
    "bulls96_cloudsql_role_seed",
  ])
    await attempt("postgres", `DROP ROLE IF EXISTS ${role}`);
  await sql(
    "postgres",
    "ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT EXECUTE ON FUNCTIONS TO PUBLIC",
  );
}
if (
  !psql("postgres", ["-f", join(ROOT, "supabase/tests/supabase_shim.sql")]).ok
)
  throw new Error("shim setup failed");
if (!psql("postgres", ["-f", BOOT]).ok)
  throw new Error("worker bootstrap failed");

await fresh("g6_boot");
await check(
  "runtime-role bootstrap creates a dedicated role and is idempotent",
  async () => {
    const a = psql("g6_boot", [
      "-v",
      "runtime_role=g6_rt",
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    const b = psql("g6_boot", [
      "-v",
      "runtime_role=g6_rt",
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    const r = (
      await sql(
        "postgres",
        "SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = 'g6_rt'",
      )
    ).rows[0];
    return [
      a.ok &&
        b.ok &&
        fmt(r) ===
          fmt({
            rolcanlogin: true,
            rolsuper: false,
            rolcreatedb: false,
            rolcreaterole: false,
            rolreplication: false,
            rolbypassrls: false,
          }),
      fmt(r),
    ];
  },
);
await check(
  "runtime-role bootstrap refuses a non-superuser creator (42501)",
  async () => {
    const r = psql(
      "g6_boot",
      [
        "-v",
        "runtime_role=g6_never",
        "-v",
        "runtime_login=true",
        "-f",
        RUNTIME_BOOT,
      ],
      "hoop",
    );
    return [
      !r.ok && r.out.includes("may not create"),
      r.out.trim().split("\n").at(-1) ?? "",
    ];
  },
);
await check(
  "runtime-role bootstrap preserves the exact optional workload membership and rejects member/option drift",
  async () => {
    await sql(
      "postgres",
      "CREATE ROLE g6_workload LOGIN INHERIT; CREATE ROLE g6_second_workload LOGIN INHERIT",
    );
    const args = [
      "-v",
      "runtime_role=g6_workload_rt",
      "-v",
      "runtime_login=false",
      "-v",
      "workload_role=g6_workload",
      "-f",
      RUNTIME_BOOT,
    ];
    const create = psql("g6_boot", args);
    await sql(
      "postgres",
      MAJOR >= 16
        ? "GRANT g6_workload_rt TO g6_workload WITH INHERIT TRUE, SET TRUE"
        : "GRANT g6_workload_rt TO g6_workload",
    );
    const rerun = psql("g6_boot", args);

    await sql("postgres", "GRANT g6_workload_rt TO g6_second_workload");
    const second = psql("g6_boot", args);
    await sql("postgres", "REVOKE g6_workload_rt FROM g6_second_workload");

    await sql(
      "postgres",
      "GRANT g6_workload_rt TO g6_workload WITH ADMIN OPTION",
    );
    const admin = psql("g6_boot", args);
    await sql(
      "postgres",
      "REVOKE ADMIN OPTION FOR g6_workload_rt FROM g6_workload",
    );

    let wrongInherit: { ok: boolean; out: string };
    let wrongSet = { ok: false, out: "PG15 has no per-edge SET option" };
    if (MAJOR >= 16) {
      await sql(
        "postgres",
        "GRANT g6_workload_rt TO g6_workload WITH INHERIT FALSE, SET TRUE",
      );
      wrongInherit = psql("g6_boot", args);
      await sql(
        "postgres",
        "GRANT g6_workload_rt TO g6_workload WITH INHERIT TRUE, SET FALSE",
      );
      wrongSet = psql("g6_boot", args);
      await sql(
        "postgres",
        "GRANT g6_workload_rt TO g6_workload WITH INHERIT TRUE, SET TRUE",
      );
    } else {
      await sql("postgres", "ALTER ROLE g6_workload NOINHERIT");
      wrongInherit = psql("g6_boot", args);
      await sql("postgres", "ALTER ROLE g6_workload INHERIT");
    }
    const recovered = psql("g6_boot", args);
    const refused = (r: { ok: boolean; out: string }) =>
      !r.ok && r.out.includes("does not match the spec");
    return [
      create.ok &&
        rerun.ok &&
        refused(second) &&
        refused(admin) &&
        refused(wrongInherit) &&
        (MAJOR < 16 || refused(wrongSet)) &&
        recovered.ok,
      `create=${create.ok} rerun=${rerun.ok} second=${refused(second)} admin=${refused(admin)} inherit=${refused(wrongInherit)} set=${MAJOR < 16 ? "n/a" : refused(wrongSet)} recovered=${recovered.ok}`,
    ];
  },
);

// Its CONNECT grant on g6_boot would be a reference from another database (refused by design).
await attempt("postgres", "DROP DATABASE IF EXISTS g6_boot WITH (FORCE)");

// (a) Database mode, before any connection writes.
await fresh("g6_mode");
for (const [name, env, code] of [
  [
    "supabase mode refuses the shim before any write",
    { ...SELF, HOOPDB_DB_MODE: "supabase", HOOPDB_SUPABASE_SHIM: "true" },
    "shim_forbidden",
  ],
  [
    "no HOOPDB_DB_MODE is refused",
    { HOOPDB_SUPABASE_SHIM: "true", HOOPDB_RUNTIME_ROLE: "g6_rt" },
    "db_mode_invalid",
  ],
  [
    "selfhosted without an explicit shim choice is refused",
    {
      HOOPDB_DB_MODE: "selfhosted",
      HOOPDB_RUNTIME_ROLE: "g6_rt",
      HOOPDB_RUNTIME_ROLE_LOGIN: "true",
    },
    "shim_unset",
  ],
] as const)
  await check(name, async () => {
    const m = migrate("g6_mode", env as Record<string, string>);
    const empty =
      (
        await sql(
          "g6_mode",
          "SELECT count(*)::int AS n FROM pg_namespace WHERE nspname = 'sessions'",
        )
      ).rows[0].n === 0;
    return [
      m.exit !== 0 && m.code === code && empty,
      `${fmt(m)} empty=${empty}`,
    ];
  });

// Worker bootstrap proofs (absent, wrong attributes), before any DDL.
await check(
  "migrate fails closed (42704) when the Worker role is absent",
  async () => {
    await fresh("g6_w", ["g6_rt"]);
    for (const d of (
      await sql(
        "postgres",
        "SELECT datname FROM pg_database WHERE datallowconn",
      )
    ).rows)
      await attempt(
        d.datname,
        `DO $x$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${W}') THEN EXECUTE 'DROP OWNED BY ${W}'; END IF; END $x$`,
      );
    await sql("postgres", `DROP ROLE IF EXISTS ${W}`);
    const m = migrate("g6_w", { ...SELF, HOOPDB_ROLE_BOOTSTRAP: "false" });
    const applied = (
      await sql(
        "g6_w",
        "SELECT to_regclass('sessions.session_directory') IS NOT NULL AS d",
      )
    ).rows[0].d;
    const ok = m.code === "42704" && applied === true; // 010000 applied, 020000 refused before its DDL
    const short = (
      await sql(
        "g6_w",
        "SELECT count(*)::int AS n FROM information_schema.columns WHERE column_name = 'short_summary'",
      )
    ).rows[0].n;
    psql("postgres", ["-f", BOOT]);
    return [ok && short === 0, `${fmt(m)} short_summary_columns=${short}`];
  },
);
await check(
  "migrate fails closed (55000) on a Worker role with wrong attributes",
  async () => {
    await fresh("g6_w", ["g6_rt"]);
    await sql("postgres", `ALTER ROLE ${W} INHERIT`);
    const m = migrate("g6_w", SELF);
    await sql("postgres", `ALTER ROLE ${W} NOINHERIT`);
    return [m.exit !== 0 && m.code === "55000", fmt(m)];
  },
);

// One migrated database per cluster: a runtime or Worker reference from another database
// is refused by design, so the Worker-case databases go before the runtime cases.
await attempt("postgres", "DROP DATABASE IF EXISTS g6_w");
await attempt("postgres", "DROP DATABASE IF EXISTS g6_mode");

// (b) Runtime role: required, existing, dedicated, exact least privilege.
await fresh("g6_rt_db", ["g6_rt"]);
await check(
  "clean migrate as hoop with the dedicated runtime role",
  async () => {
    const m = migrate("g6_rt_db", SELF);
    const again = migrate("g6_rt_db", SELF);
    const v = await verdict("g6_rt_db");
    return [
      m.exit === 0 && again.exit === 0 && v.ok,
      `${fmt(m)} rerun=${fmt(again)} verdict=${fmt(v)}`,
    ];
  },
);
const base = await snapshot("g6_rt_db");
for (const [name, env, code] of [
  [
    "runtime role unset: refused, nothing changed",
    { ...SELF, HOOPDB_RUNTIME_ROLE: "" },
    "runtime_role_unset",
  ],
  [
    "runtime role nonexistent: refused, nothing changed",
    { ...SELF, HOOPDB_RUNTIME_ROLE: "g6_nope" },
    "runtime_role_missing",
  ],
  [
    "the owner as runtime role in selfhosted: refused, nothing changed",
    { ...SELF, HOOPDB_RUNTIME_ROLE: "hoop" },
    "runtime_role_unsafe",
  ],
  [
    "runtime login mismatch: refused, nothing changed",
    { ...SELF, HOOPDB_RUNTIME_ROLE_LOGIN: "false" },
    "runtime_role_unsafe",
  ],
] as const)
  await check(name, async () => {
    const m = migrate("g6_rt_db", env as Record<string, string>);
    const same = (await snapshot("g6_rt_db")) === base;
    return [
      m.exit !== 0 && m.code === code && same,
      `${fmt(m)} unchanged=${same}`,
    ];
  });
const HOSTILE: Array<[string, string]> = [
  ["CREATEROLE", "ALTER ROLE g6_bad CREATEROLE"],
  ["CREATEDB", "ALTER ROLE g6_bad CREATEDB"],
  ["BYPASSRLS", "ALTER ROLE g6_bad BYPASSRLS"],
  ["a membership", "GRANT service_role TO g6_bad"],
  [
    "a member WITH ADMIN OPTION",
    "CREATE ROLE g6_m NOLOGIN; GRANT g6_bad TO g6_m WITH ADMIN OPTION",
  ],
  [
    "an owned object",
    "CREATE TABLE public.g6_own (x int); ALTER TABLE public.g6_own OWNER TO g6_bad",
  ],
  ["CREATE on schema sessions", "GRANT CREATE ON SCHEMA sessions TO g6_bad"],
  ["CREATE on the database", "GRANT CREATE ON DATABASE g6_rt_db TO g6_bad"],
  ["unapproved table privilege", "GRANT TRUNCATE ON sessions.session_index TO g6_bad"],
  [
    "a column privilege",
    "GRANT SELECT (org_id) ON sessions.session_directory TO g6_bad",
  ],
  [
    "unapproved EXECUTE",
    "GRANT EXECUTE ON FUNCTION sessions.session_directory_readiness() TO g6_bad",
  ],
  [
    "EXECUTE on a trigger function (no exemption)",
    "GRANT EXECUTE ON FUNCTION sessions.pipeline_jobs_observability() TO g6_bad",
  ],
  [
    "effective TEMPORARY through PUBLIC",
    "GRANT TEMPORARY ON DATABASE g6_rt_db TO PUBLIC",
  ],
  [
    "no effective CONNECT",
    "REVOKE CONNECT ON DATABASE g6_rt_db FROM PUBLIC; REVOKE CONNECT ON DATABASE g6_rt_db FROM g6_bad",
  ],
  [
    "a PUBLIC table privilege outside sessions",
    "CREATE TABLE public.g6_pub (x int); GRANT SELECT ON public.g6_pub TO PUBLIC",
  ],
  [
    "a PUBLIC sequence privilege outside sessions",
    "CREATE SEQUENCE public.g6_pub_seq; GRANT USAGE ON SEQUENCE public.g6_pub_seq TO PUBLIC",
  ],
  [
    "EXECUTE on a SECURITY DEFINER function outside sessions (PUBLIC)",
    "CREATE FUNCTION public.g6_definer() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION public.g6_definer() TO PUBLIC",
  ],
  [
    "default privileges in sessions granting PUBLIC",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO PUBLIC",
  ],
  [
    "USAGE on another schema through PUBLIC",
    "CREATE SCHEMA g6_extra; GRANT USAGE ON SCHEMA g6_extra TO PUBLIC",
  ],
  [
    "a default ACL",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO g6_bad",
  ],
  [
    "GRANT OPTION on an approved table",
    "GRANT SELECT ON sessions.__96bulls_migrations TO g6_bad WITH GRANT OPTION",
  ],
];
for (const [name, setup] of HOSTILE)
  await check(
    `hostile runtime role with ${name}: refused, ledger/marker/grants unchanged`,
    async () => {
      psql("g6_rt_db", [
        "-v",
        "runtime_role=g6_bad",
        "-v",
        "runtime_login=true",
        "-f",
        RUNTIME_BOOT,
      ]);
      await sql("g6_rt_db", setup);
      const before = await snapshot("g6_rt_db");
      const m = migrate("g6_rt_db", { ...SELF, HOOPDB_RUNTIME_ROLE: "g6_bad" });
      const same = (await snapshot("g6_rt_db")) === before;
      for (const undo of [
        "REVOKE TEMPORARY ON DATABASE g6_rt_db FROM PUBLIC",
        "GRANT CONNECT ON DATABASE g6_rt_db TO PUBLIC",
        "DROP TABLE IF EXISTS public.g6_pub",
        "DROP SEQUENCE IF EXISTS public.g6_pub_seq",
        "DROP FUNCTION IF EXISTS public.g6_definer()",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions REVOKE SELECT ON TABLES FROM PUBLIC",
        "DROP SCHEMA IF EXISTS g6_extra",
      ])
        await attempt("g6_rt_db", undo);
      await attempt(
        "g6_rt_db",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions REVOKE SELECT ON TABLES FROM g6_bad",
      );
      await attempt("g6_rt_db", "DROP OWNED BY g6_bad");
      await attempt("postgres", "DROP ROLE IF EXISTS g6_m");
      await attempt("postgres", "DROP ROLE g6_bad");
      return [
        m.exit !== 0 && m.code === "runtime_role_unsafe" && same,
        `${fmt(m)} unchanged=${same}`,
      ];
    },
  );
await check(
  "the dedicated runtime role gets exact application DML/sequence/RPC access, read-only readiness metadata, no DDL/maintenance/ownership/BYPASSRLS, live gate ok",
  async () => {
    const t = [
      "sessions.__96bulls_migrations",
      "sessions.__96bulls_role_validation",
    ];
    const got: Record<string, string> = {};
    for (const tb of t) {
      got[`${tb} SELECT`] = await attempt(
        "g6_rt_db",
        `SELECT count(*) FROM ${tb}`,
        "g6_rt",
      );
      for (const [k, q] of [
        ["INSERT", `INSERT INTO ${tb} (name) VALUES ('x')`],
        ["UPDATE", `UPDATE ${tb} SET name = name`],
        ["DELETE", `DELETE FROM ${tb}`],
        ["TRUNCATE", `TRUNCATE ${tb}`],
        ["ALTER", `ALTER TABLE ${tb} ADD COLUMN x int`],
        ["DROP", `DROP TABLE ${tb}`],
      ])
        got[`${tb} ${k}`] = await attempt("g6_rt_db", q!, "g6_rt");
    }
    got["CREATE TABLE"] = await attempt(
      "g6_rt_db",
      "CREATE TABLE sessions.g6_x (x int)",
      "g6_rt",
    );
    const acl = (
      await sql(
        "g6_rt_db",
        `SELECT
           has_table_privilege('g6_rt', 'sessions.pipeline_jobs', 'SELECT, INSERT, UPDATE, DELETE') AS queue_dml,
           has_table_privilege('g6_rt', 'sessions.claimless_uses', 'UPDATE') AS claimless_update,
           has_table_privilege('g6_rt', 'sessions.migration_assertion_uses', 'SELECT, INSERT, UPDATE, DELETE') AS assertion_cleanup,
           has_table_privilege('g6_rt', 'sessions.migration_assertion_chunks', 'SELECT, INSERT, UPDATE, DELETE') AS chunk_cleanup,
           has_table_privilege('g6_rt', 'sessions.migration_base_read_uses', 'SELECT, INSERT, UPDATE, DELETE') AS base_read_cleanup,
           has_table_privilege('g6_rt', 'sessions.migration_ingest_audit', 'UPDATE, DELETE') AS audit_mutation,
           has_table_privilege('g6_rt', 'sessions.pipeline_jobs', 'TRUNCATE') AS queue_truncate,
           has_sequence_privilege('g6_rt', 'sessions.pipeline_jobs_id_seq', 'USAGE') AS queue_sequence,
           has_function_privilege('g6_rt', 'sessions.upsert_session_index(jsonb)', 'EXECUTE') AS index_rpc,
           has_function_privilege('g6_rt', 'sessions.pipeline_events_maintain(interval,integer)', 'EXECUTE') AS maintenance_rpc,
           has_function_privilege('g6_rt', 'sessions.pipeline_jobs_observability()', 'EXECUTE') AS trigger_execute`,
      )
    ).rows[0];
    const role = (
      await sql(
        "g6_rt_db",
        "SELECT rolbypassrls, (SELECT count(*)::int FROM pg_shdepend WHERE refobjid = r.oid AND deptype = 'o') AS owned FROM pg_roles r WHERE rolname = 'g6_rt'",
      )
    ).rows[0];
    const v = await verdict("g6_rt_db", "g6_rt");
    const ok =
      Object.entries(got).every(([k, c]) =>
        k.endsWith("SELECT") ? c === "ok" : c === "42501",
      ) &&
      acl.queue_dml === true &&
      acl.claimless_update === true &&
      acl.assertion_cleanup === true &&
      acl.chunk_cleanup === true &&
      acl.base_read_cleanup === true &&
      acl.audit_mutation === false &&
      acl.queue_truncate === false &&
      acl.queue_sequence === true &&
      acl.index_rpc === true &&
      acl.maintenance_rpc === false &&
      acl.trigger_execute === false &&
      role.rolbypassrls === false &&
      role.owned === 0 &&
      v.ok;
    return [ok, `${fmt(got)} acl=${fmt(acl)} ${fmt(role)} verdict=${fmt(v)}`];
  },
);

// (2) The full ordered sequence: one contiguous prefix, no unledgered footprint. These run
// in their own database with their own runtime role (g6_rt already holds approved grants
// in g6_rt_db, and a reference from another database is refused by design).
const SEQ = { ...SELF, HOOPDB_RUNTIME_ROLE: "g6_rt2" };
await check(
  "helm (a): only session_directory present: refused before any write",
  async () => {
    await fresh("g6_seq", ["g6_rt2"]);
    await sql(
      "g6_seq",
      "CREATE SCHEMA sessions AUTHORIZATION hoop; CREATE TABLE sessions.session_directory (x int); ALTER TABLE sessions.session_directory OWNER TO hoop",
    );
    const m = migrate("g6_seq", SEQ);
    return [
      m.code === "unledgered_schema" && (await snapshot("g6_seq")) === "absent",
      `${fmt(m)} ledger=${await snapshot("g6_seq")}`,
    ];
  },
);
await check(
  "helm (b): a 96bulls ledger row without upstream rows: refused, never backfilled",
  async () => {
    await fresh("g6_seq", ["g6_rt2"]);
    await sql("g6_seq", `CREATE SCHEMA sessions AUTHORIZATION hoop`);
    await sql(
      "g6_seq",
      `CREATE TABLE sessions.__96bulls_migrations (name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
    ALTER TABLE sessions.__96bulls_migrations OWNER TO hoop;
    INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ('96bulls:0001_96bulls_observability.sql', 'x')`,
    );
    const before = await snapshot("g6_seq");
    const m = migrate("g6_seq", SEQ);
    return [
      m.code === "ledger_gap" && (await snapshot("g6_seq")) === before,
      fmt(m),
    ];
  },
);
await check(
  "a recorded prefix whose later steps were applied unrecorded: refused, nothing marked",
  async () => {
    await fresh("g6_seq", ["g6_rt2"]);
    migrate("g6_seq", SEQ);
    await sql(
      "g6_seq",
      "DELETE FROM sessions.__96bulls_migrations WHERE name LIKE '96bulls:%'",
    );
    const before = await snapshot("g6_seq");
    const m = migrate("g6_seq", SEQ);
    return [
      m.code === "unledgered_schema" && (await snapshot("g6_seq")) === before,
      fmt(m),
    ];
  },
);
await check(
  "supabase db push history naming these migrations: mixed_migration_authority",
  async () => {
    await fresh("g6_seq", ["g6_rt2"]);
    await sql(
      "g6_seq",
      "CREATE SCHEMA supabase_migrations; CREATE TABLE supabase_migrations.schema_migrations (version text PRIMARY KEY); INSERT INTO supabase_migrations.schema_migrations VALUES ('20261025000000'); GRANT USAGE ON SCHEMA supabase_migrations TO hoop; GRANT SELECT ON supabase_migrations.schema_migrations TO hoop",
    );
    const m = migrate("g6_seq", SEQ);
    return [
      m.code === "mixed_migration_authority" &&
        (await snapshot("g6_seq")) === "absent",
      fmt(m),
    ];
  },
);
await attempt("postgres", "DROP DATABASE IF EXISTS g6_seq");

await check(
  "PostgreSQL 17+: a runtime role with MAINTAIN on a relation is refused (version-guarded; MAINTAIN does not exist on 15)",
  async () => {
    if (MAJOR < 17)
      return [true, `skipped: PostgreSQL ${MAJOR} has no MAINTAIN privilege`];
    psql("g6_rt_db", [
      "-v",
      "runtime_role=g6_bad",
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    await sql("g6_rt_db", "GRANT MAINTAIN ON sessions.session_index TO g6_bad");
    const before = await snapshot("g6_rt_db");
    const m = migrate("g6_rt_db", { ...SELF, HOOPDB_RUNTIME_ROLE: "g6_bad" });
    const same = (await snapshot("g6_rt_db")) === before;
    await attempt("g6_rt_db", "DROP OWNED BY g6_bad");
    await attempt("postgres", "DROP ROLE g6_bad");
    return [
      m.code === "runtime_role_unsafe" && same,
      `${fmt(m)} unchanged=${same}`,
    ];
  },
);
await check(
  "ACL manifest regression: every sessions function's ACL is exactly the committed manifest",
  async () => {
    const want = functionManifestForRuntime("g6_rt");
    const h = openHoopDb(url("hoop", "g6_rt_db"));
    try {
      const got = await sessionsFunctionAcl(h);
      const diff = Object.keys({ ...want, ...got }).filter(
        (k) => fmt(want[k]) !== fmt(got[k]),
      );
      return [
        diff.length === 0,
        diff.length
          ? `differs: ${diff.join(", ")}`
          : `${Object.keys(got).length} functions match`,
      ];
    } finally {
      await h.end();
    }
  },
);
/** Creates sessions.g6_late() as hoop (no migrator), then has an unrelated authenticated-member role call it. */
async function lateFunctionProbe(db: string, runtimeUser: string) {
  const expected = await expectedSchema();
  const rt = openHoopDb(url(runtimeUser, db));
  const health = {
    schemaReady: async () =>
      (await schemaVerdict(expected, (w) => inTransaction(rt, w))).ok,
  };
  try {
    const before = await gateOpen(health);
    await sql(
      db,
      "CREATE FUNCTION sessions.g6_late() RETURNS int LANGUAGE sql AS 'SELECT 42'",
      "hoop",
    );
    await sql(
      "postgres",
      "DO $x$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'g6_unrel') THEN CREATE ROLE g6_unrel LOGIN; END IF; END $x$; GRANT authenticated TO g6_unrel",
    );
    await sql(db, `GRANT CONNECT ON DATABASE ${db} TO g6_unrel`);
    const direct = await attempt(db, "SELECT sessions.g6_late()", "g6_unrel");
    const verdict = await schemaVerdict(expected, (w) => inTransaction(rt, w));
    const after = await gateOpen(health);
    // Every worker claim path (each stage claim and the side tick) runs only past the gate.
    const ran: string[] = [];
    const tick = await workerTick({
      stages: ["capture", "index", "summary"],
      gate: () => gateOpen(health),
      poll: async (st) => (ran.push(st), 1),
      tick: async () => void ran.push("tick"),
    });
    const claims = ran.join(",") || "none";
    await sql(db, "DROP FUNCTION sessions.g6_late()", "hoop");
    const recovered = await schemaVerdict(expected, (w) =>
      inTransaction(rt, w),
    );
    await attempt(db, `REVOKE CONNECT ON DATABASE ${db} FROM g6_unrel`);
    await attempt("postgres", "DROP ROLE IF EXISTS g6_unrel");
    return {
      before,
      direct,
      verdict,
      after,
      claims,
      gated: tick.gated,
      recovered,
    };
  } finally {
    await rt.end();
  }
}
await check(
  "selfhosted with the owner-scoped revoke: a sessions function created after PASS without the migrator is NOT directly invocable (42501); the gate stays ready",
  async () => {
    const p = await lateFunctionProbe("g6_rt_db", "g6_rt");
    return [
      p.before &&
        p.direct === "42501" &&
        p.verdict.ok &&
        p.after &&
        p.claims === "capture,index,summary,tick" &&
        p.recovered.ok,
      fmt(p),
    ];
  },
);
await check(
  "residual where the control is absent (built-in default restored): such a function IS directly invocable (not prevented); the live gate gives 55000 and claims stop",
  async () => {
    // Undo the owner-scoped revoke: back to PostgreSQL's built-in PUBLIC EXECUTE for new functions.
    await sql(
      "g6_rt_db",
      "ALTER DEFAULT PRIVILEGES FOR ROLE hoop GRANT EXECUTE ON FUNCTIONS TO PUBLIC",
    );
    const p = await lateFunctionProbe("g6_rt_db", "g6_rt");
    const restored = psql("g6_rt_db", [
      "-v",
      "migration_owner=hoop",
      "-f",
      FN_DEFAULT_BOOT,
    ]).ok;
    const ok =
      p.before &&
      p.direct === "ok" &&
      !p.verdict.ok &&
      "error_code" in p.verdict &&
      p.verdict.error_code === "55000" &&
      !p.after &&
      p.claims === "none" &&
      p.gated &&
      p.recovered.ok &&
      restored;
    return [ok, `${fmt(p)} control_restored=${restored}`];
  },
);
await check(
  "selfhosted bootstrap revokes TEMPORARY from PUBLIC: an unrelated role loses it database-wide; the owner keeps it",
  async () => {
    await fresh("g6_tmp");
    await attempt("postgres", "CREATE ROLE g6_other LOGIN");
    const q = async () =>
      (
        await sql(
          "g6_tmp",
          "SELECT has_database_privilege('g6_other', 'g6_tmp', 'TEMPORARY') AS other, has_database_privilege('hoop', 'g6_tmp', 'TEMPORARY') AS owner, has_database_privilege('g6_rt3', 'g6_tmp', 'CONNECT') AS rt_connect",
        ).catch(async () =>
          sql(
            "g6_tmp",
            "SELECT has_database_privilege('g6_other', 'g6_tmp', 'TEMPORARY') AS other, has_database_privilege('hoop', 'g6_tmp', 'TEMPORARY') AS owner, null::boolean AS rt_connect",
          ),
        )
      ).rows[0];
    const before = await q();
    const b = psql("g6_tmp", [
      "-v",
      "runtime_role=g6_rt3",
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    const after = await q();
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_tmp WITH (FORCE)");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_other");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_rt3");
    return [
      b.ok &&
        before.other === true &&
        after.other === false &&
        after.owner === true &&
        after.rt_connect === true,
      `before=${fmt(before)} after=${fmt(after)}`,
    ];
  },
);
await check(
  "PUBLIC EXECUTE on sessions functions is revoked in the validation transaction; a trigger still fires for a caller without EXECUTE",
  async () => {
    await sql(
      "g6_rt_db",
      "GRANT EXECUTE ON FUNCTION sessions.repo_visibility_ids(text, uuid) TO PUBLIC",
    );
    const m = migrate("g6_rt_db", SELF);
    const pub = (
      await sql(
        "g6_rt_db",
        "SELECT count(*)::int AS n FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'sessions' AND (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0))",
      )
    ).rows[0].n;
    await sql(
      "g6_rt_db",
      `CREATE TABLE public.g6_audit (x int);
    CREATE TRIGGER g6_append_only BEFORE UPDATE OR DELETE ON public.g6_audit FOR EACH ROW EXECUTE FUNCTION sessions.migration_ingest_audit_append_only();
    INSERT INTO public.g6_audit VALUES (1);
    DO $x$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'g6_caller') THEN CREATE ROLE g6_caller LOGIN; END IF; END $x$;
    GRANT CONNECT ON DATABASE g6_rt_db TO g6_caller; GRANT DELETE, SELECT ON public.g6_audit TO g6_caller`,
    );
    const exec = (
      await sql(
        "g6_rt_db",
        "SELECT has_function_privilege('g6_caller', 'sessions.migration_ingest_audit_append_only()', 'EXECUTE') AS e",
      )
    ).rows[0].e;
    const fired = await attempt(
      "g6_rt_db",
      "DELETE FROM public.g6_audit",
      "g6_caller",
    );
    const left = (
      await sql("g6_rt_db", "SELECT count(*)::int AS n FROM public.g6_audit")
    ).rows[0].n;
    await attempt("g6_rt_db", "DROP TABLE public.g6_audit");
    await attempt("g6_rt_db", "DROP OWNED BY g6_caller");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_caller");
    return [
      m.exit === 0 &&
        pub === 0 &&
        exec === false &&
        fired !== "ok" &&
        left === 1,
      `${fmt(m)} public_exec_fns=${pub} caller_exec=${exec} delete=${fired} rows_left=${left}`,
    ];
  },
);
await check(
  "guardrail: a validation failure rolls the in-transaction revoke back (state unchanged)",
  async () => {
    await sql(
      "g6_rt_db",
      "GRANT EXECUTE ON FUNCTION sessions.pipeline_jobs_observability() TO PUBLIC",
    );
    await sql("g6_rt_db", `GRANT SELECT ON sessions.session_index TO ${W}`);
    const acl = async () =>
      (
        await sql(
          "g6_rt_db",
          "SELECT proacl::text AS a FROM pg_proc WHERE oid = 'sessions.pipeline_jobs_observability()'::regprocedure",
        )
      ).rows[0].a as string;
    const ledger = async () =>
      (
        await sql(
          "g6_rt_db",
          "SELECT md5(string_agg(name || sha256, ',' ORDER BY name)) AS l FROM sessions.__96bulls_migrations",
        )
      ).rows[0].l as string;
    const before = { acl: await acl(), ledger: await ledger() };
    const m = migrate("g6_rt_db", SELF);
    const after = { acl: await acl(), ledger: await ledger() };
    await sql("g6_rt_db", `REVOKE SELECT ON sessions.session_index FROM ${W}`);
    const fixed = migrate("g6_rt_db", SELF);
    const revoked = (await acl()) !== before.acl;
    return [
      m.code === "55000" &&
        fmt(after) === fmt(before) &&
        fixed.exit === 0 &&
        revoked,
      `${fmt(m)} unchanged=${fmt(after) === fmt(before)} fixed=${fmt(fixed)} revoked_after_fix=${revoked}`,
    ];
  },
);

// Readiness: live gate, race, verifier off + mark_pass, crash.
await check(
  "post-PASS drift with no migrator: not ready (55000), then ready once revoked",
  async () => {
    await sql("g6_rt_db", `GRANT SELECT ON sessions.session_index TO ${W}`);
    const drift = await verdict("g6_rt_db");
    await sql("g6_rt_db", `REVOKE SELECT ON sessions.session_index FROM ${W}`);
    const fixed = await verdict("g6_rt_db");
    return [
      !drift.ok &&
        "error_code" in drift &&
        drift.error_code === "55000" &&
        fixed.ok,
      `${fmt(drift)} ${fmt(fixed)}`,
    ];
  },
);
await check(
  "race: a pending upsert committed after phase 2, before the final read: not ready",
  async () => {
    const other = new pg.Client({ connectionString: url("hoop", "g6_rt_db") });
    await other.connect();
    const gen = await generation();
    const v = await verdict("g6_rt_db", "hoop", {
      afterVerify: async () => {
        await other.query(MARKER_PENDING_SQL, [
          "session_directory_search_worker",
          gen,
        ]);
      },
    });
    await other.end();
    return [
      !v.ok && "reason" in v && v.reason === "validation_pending",
      fmt(v),
    ];
  },
);
await check(
  "verifier off stays pending; mark_pass.sql refuses a wrong generation and passes this one",
  async () => {
    const m = migrate("g6_rt_db", { ...SELF, HOOPDB_ROLE_VERIFY: "false" });
    const pending = await verdict("g6_rt_db");
    const wrong = psql(
      "g6_rt_db",
      [
        "-v",
        `generation=${"f".repeat(64)}`,
        "-v",
        "db_mode=selfhosted",
        "-f",
        MARK_PASS,
      ],
      "hoop",
    );
    const right = psql(
      "g6_rt_db",
      [
        "-v",
        `generation=${await generation()}`,
        "-v",
        "db_mode=selfhosted",
        "-f",
        MARK_PASS,
      ],
      "hoop",
    );
    const again = psql(
      "g6_rt_db",
      [
        "-v",
        `generation=${await generation()}`,
        "-v",
        "db_mode=selfhosted",
        "-f",
        MARK_PASS,
      ],
      "hoop",
    );
    const ready = await verdict("g6_rt_db");
    const ok =
      m.exit === 0 &&
      !pending.ok &&
      !wrong.ok &&
      right.ok &&
      !again.ok &&
      ready.ok;
    return [
      ok,
      `pending=${fmt(pending)} wrong=${wrong.ok} right=${right.ok} again=${again.ok} ready=${fmt(ready)}`,
    ];
  },
);
await check(
  "phase 2 drift fails the migrator and leaves the marker pending",
  async () => {
    await sql("g6_rt_db", `GRANT SELECT ON sessions.session_index TO ${W}`);
    const m = migrate("g6_rt_db", SELF);
    const st = (
      await sql(
        "g6_rt_db",
        "SELECT status FROM sessions.__96bulls_role_validation",
      )
    ).rows[0].status;
    await sql("g6_rt_db", `REVOKE SELECT ON sessions.session_index FROM ${W}`);
    const fixed = migrate("g6_rt_db", SELF);
    return [
      m.code === "55000" && st === "pending" && fixed.exit === 0,
      `${fmt(m)} status=${st} fixed=${fmt(fixed)}`,
    ];
  },
);
// The crash case migrates a database of its own: the Worker's and runtime role's grants in
// g6_rt_db would otherwise be references from another database, which phase 2 refuses.
await attempt("postgres", "DROP DATABASE IF EXISTS g6_rt_db");
await check(
  "crash (SIGKILL) mid-migration: not ready; a restart passes",
  async () => {
    await fresh("g6_crash", ["g6_rt2"]);
    const locker = new pg.Client({ connectionString: url(SUPER, "g6_crash") });
    await locker.connect();
    await locker.query(
      "BEGIN; LOCK TABLE pg_catalog.pg_extension IN ACCESS EXCLUSIVE MODE",
    );
    const clean = Object.fromEntries(
      Object.entries(process.env).filter(([k]) => !k.startsWith("HOOPDB_")),
    );
    const child = spawn("bun", ["src/hoopdb/migrate.ts"], {
      cwd: ROOT,
      env: { ...clean, HOOPDB_URL: url("hoop", "g6_crash"), ...SEQ },
      stdio: "ignore",
    });
    const exited = new Promise((r) =>
      child.exitCode !== null ? r(null) : child.once("exit", r),
    );
    let waiting = 0;
    for (let i = 0; i < 200 && waiting === 0; i++) {
      await new Promise((r) => setTimeout(r, 50));
      waiting = (
        await sql(
          "postgres",
          "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = 'g6_crash' AND usename = 'hoop' AND wait_event_type = 'Lock'",
        )
      ).rows[0].n;
    }
    child.kill("SIGKILL");
    await exited;
    await locker.query("ROLLBACK");
    await locker.end();
    await sql(
      "postgres",
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'g6_crash' AND pid <> pg_backend_pid()",
    );
    const killed = await verdict("g6_crash");
    const restart = migrate("g6_crash", SEQ);
    const ready = await verdict("g6_crash");
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_crash");
    return [
      waiting > 0 && !killed.ok && restart.exit === 0 && ready.ok,
      `blocked=${waiting} killed=${fmt(killed)} restart=${fmt(restart)} ready=${fmt(ready)}`,
    ];
  },
);

const leaves = (v: unknown): unknown[] =>
  v !== null && typeof v === "object" ? Object.values(v).flatMap(leaves) : [v];
await check(
  "supabase activation gate: refused unless HOOPDB_FUNCTION_BOUNDARY_MODE selects a catalog-proven control (names and booleans); (A) owner-scoped global revoke with the mode selected passes",
  async () => {
    await fresh("g6_supa", ["g6_rt2"], false);
    const SUPA = {
      HOOPDB_DB_MODE: "supabase",
      HOOPDB_RUNTIME_ROLE: "g6_rt2",
      HOOPDB_RUNTIME_ROLE_LOGIN: "true",
    };
    const none = migrate("g6_supa", SUPA);
    const absent = (await snapshot("g6_supa")) === "absent";
    await sql(
      "g6_supa",
      "ALTER DEFAULT PRIVILEGES FOR ROLE hoop REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
    );
    const modeUnset = migrate("g6_supa", SUPA);
    const selected = migrate("g6_supa", {
      ...SUPA,
      HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
    });
    const check = spawnSync(
      "bun",
      ["src/hoopdb/migrate.ts", "--activation-check"],
      {
        cwd: ROOT,
        encoding: "utf8",
        env: {
          ...process.env,
          HOOPDB_URL: url("hoop", "g6_supa"),
          HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
        },
      },
    );
    const evidence = JSON.parse(check.stdout.trim().split("\n").at(-1) ?? "{}");
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_supa WITH (FORCE)");
    const ok =
      none.code === "function_boundary_unproven" &&
      absent &&
      modeUnset.code === "function_boundary_unproven" &&
      selected.exit === 0 &&
      check.status === 0 &&
      evidence.satisfied === true &&
      evidence.owner_function_default_exact === true &&
      evidence.sessions_unexposed_and_no_untrusted_usage === false &&
      // Names and booleans only: every leaf (through the creator list) is a string or boolean.
      leaves(evidence).every(
        (x) => x === null || ["string", "boolean"].includes(typeof x),
      );
    return [
      ok,
      `none=${fmt(none)} nothing_written=${absent} mode_unset=${fmt(modeUnset)} mode_selected=${fmt(selected)} evidence=${fmt(evidence)}`,
    ];
  },
);

await check(
  "the old HOOPDB_FUNCTION_BOUNDARY_APPROVED name is refused, and event-trigger mode is refused outside supabase (selfhosted, cloudsql)",
  async () => {
    await fresh("g6_fb", ["g6_rt2"]);
    const old = migrate("g6_fb", {
      ...SEQ,
      HOOPDB_FUNCTION_BOUNDARY_APPROVED: "global-default-revoke",
    });
    const et = migrate("g6_fb", {
      ...SEQ,
      HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
    });
    const etCloud = migrate("g6_fb", {
      ...SEQ,
      HOOPDB_DB_MODE: "cloudsql",
      HOOPDB_SUPABASE_SHIM: "false",
      HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
    });
    const absent = (await snapshot("g6_fb")) === "absent";
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_fb WITH (FORCE)");
    return [
      old.code === "db_profile_invalid" &&
        et.code === "function_boundary_mode_unsupported" &&
        etCloud.code === "function_boundary_mode_unsupported" &&
        absent,
      `old=${fmt(old)} event_trigger_selfhosted=${fmt(et)} event_trigger_cloudsql=${fmt(etCloud)} nothing_written=${absent}`,
    ];
  },
);
await check(
  "selfhosted is gated: refused without the owner-scoped revoke (function_boundary_unproven); the bootstrap installs it and the gate passes",
  async () => {
    await fresh("g6_fb", ["g6_rt2"], false);
    const without = migrate("g6_fb", SEQ);
    const absent = (await snapshot("g6_fb")) === "absent";
    const b1 = psql("g6_fb", [
      "-v",
      "migration_owner=hoop",
      "-f",
      FN_DEFAULT_BOOT,
    ]);
    const b2 = psql("g6_fb", [
      "-v",
      "migration_owner=hoop",
      "-f",
      FN_DEFAULT_BOOT,
    ]); // idempotent
    const withIt = migrate("g6_fb", SEQ);
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_fb WITH (FORCE)");
    return [
      without.code === "function_boundary_unproven" &&
        absent &&
        b1.ok &&
        b2.ok &&
        withIt.exit === 0,
      `without=${fmt(without)} nothing_written=${absent} bootstrap_twice=${b1.ok && b2.ok} with=${fmt(withIt)}`,
    ];
  },
);
await check(
  "blast radius: owner functions created later in sessions AND in a second schema are not PUBLIC-executable; intended grants survive the revoke and the next migrate (ACL manifest)",
  async () => {
    await fresh("g6_fb", ["g6_rt2"]);
    const want = functionManifestForRuntime("g6_rt2");
    const aclOk = async () => {
      const h = openHoopDb(url("hoop", "g6_fb"));
      try {
        const got = await sessionsFunctionAcl(h);
        return Object.keys({ ...want, ...got }).every(
          (k) => fmt(want[k]) === fmt(got[k]),
        );
      } finally {
        await h.end();
      }
    };
    const m1 = migrate("g6_fb", SEQ);
    const after1 = await aclOk();
    await sql(
      "g6_fb",
      "CREATE FUNCTION sessions.g6_future() RETURNS int LANGUAGE sql AS 'SELECT 1'; CREATE SCHEMA g6_second; CREATE FUNCTION g6_second.g6_future() RETURNS int LANGUAGE sql AS 'SELECT 1'",
      "hoop",
    );
    await attempt("postgres", "CREATE ROLE g6_probe NOLOGIN");
    const r = (
      await sql(
        "g6_fb",
        `SELECT has_function_privilege('g6_probe', 'sessions.g6_future()', 'EXECUTE') AS s_exec,
              has_function_privilege('g6_probe', 'g6_second.g6_future()', 'EXECUTE') AS o_exec,
              (SELECT bool_or(a.grantee = 0) FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE p.oid IN ('sessions.g6_future()'::regprocedure, 'g6_second.g6_future()'::regprocedure)) AS any_public`,
      )
    ).rows[0];
    await sql(
      "g6_fb",
      "DROP FUNCTION sessions.g6_future(); DROP SCHEMA g6_second CASCADE",
      "hoop",
    );
    const m2 = migrate("g6_fb", SEQ);
    const after2 = await aclOk();
    await attempt("postgres", "DROP ROLE IF EXISTS g6_probe");
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_fb WITH (FORCE)");
    const ok =
      m1.exit === 0 &&
      after1 &&
      r.s_exec === false &&
      r.o_exec === false &&
      r.any_public === false &&
      m2.exit === 0 &&
      after2;
    return [
      ok,
      `migrate=${fmt(m1)} manifest_after_revoke=${after1} sessions_fn_exec=${r.s_exec} second_schema_fn_exec=${r.o_exec} any_public=${r.any_public} next_migrate=${fmt(m2)} manifest_after_next=${after2}`,
    ];
  },
);
let BOUNDARY_EVIDENCE: unknown = null;
const activationCheck = (
  db: string,
  extra: Record<string, string> = {},
  user = "hoop",
) => {
  const r = spawnSync("bun", ["src/hoopdb/migrate.ts", "--activation-check"], {
    cwd: ROOT,
    encoding: "utf8",
    env: {
      ...process.env,
      HOOPDB_URL: url(user, db),
      HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
      ...extra,
    },
  });
  return {
    status: r.status,
    evidence: JSON.parse(r.stdout.trim().split("\n").at(-1) || "{}"),
  };
};
for (const [name, poison, shown] of [
  [
    "an extra grantee (authenticated EXECUTE)",
    "GRANT EXECUTE ON FUNCTIONS TO authenticated",
    "authenticated:EXECUTE",
  ],
  [
    "a grant option",
    "GRANT EXECUTE ON FUNCTIONS TO authenticated WITH GRANT OPTION",
    "authenticated:EXECUTE*",
  ],
] as const)
  await check(
    `exact default ACL: a poisoned owner default (PUBLIC revoked plus ${name}) fails the bootstrap before any write and activation-check refuses`,
    async () => {
      await fresh("g6_pz", ["g6_rt2"], false);
      await sql(
        "g6_pz",
        `ALTER DEFAULT PRIVILEGES FOR ROLE hoop REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE hoop ${poison}`,
      );
      const acls = async () =>
        (
          await sql(
            "g6_pz",
            "SELECT coalesce(string_agg(defaclrole::regrole::text || '=' || defaclacl::text, ';' ORDER BY 1), '') AS a FROM pg_default_acl",
          )
        ).rows[0].a as string;
      const before = await acls();
      const boot = psql("g6_pz", [
        "-v",
        "migration_owner=hoop",
        "-f",
        FN_DEFAULT_BOOT,
      ]);
      const same = (await acls()) === before;
      const chk = activationCheck("g6_pz");
      const m = migrate("g6_pz", SEQ);
      const absent = (await snapshot("g6_pz")) === "absent";
      await attempt("postgres", "DROP DATABASE IF EXISTS g6_pz WITH (FORCE)");
      const ok =
        !boot.ok &&
        boot.out.includes("poisoned function default") &&
        same &&
        chk.status === 1 &&
        chk.evidence.owner_function_default_exact === false &&
        (chk.evidence.owner_function_default as string[]).includes(shown) &&
        m.code === "function_boundary_unproven" &&
        absent;
      return [
        ok,
        `bootstrap_refused=${!boot.ok} acl_unchanged=${same} evidence_default=${fmt(chk.evidence.owner_function_default)} migrate=${fmt(m)} nothing_written=${absent}`,
      ];
    },
  );
await check(
  "after a correct bootstrap, a function created later in sessions (by the owner or a superuser) is not executable by PUBLIC, authenticated or the runtime role",
  async () => {
    await fresh("g6_bh", ["g6_rt2"]);
    const m = migrate("g6_bh", SEQ);
    const chk = activationCheck("g6_bh");
    BOUNDARY_EVIDENCE = chk.evidence;
    await sql(
      "g6_bh",
      "CREATE FUNCTION sessions.g6_by_owner() RETURNS int LANGUAGE sql AS 'SELECT 1'",
      "hoop",
    );
    await sql(
      "g6_bh",
      "CREATE FUNCTION sessions.g6_by_superuser() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    await attempt("postgres", "CREATE ROLE g6_nobody NOLOGIN");
    const r = (
      await sql(
        "g6_bh",
        `SELECT p.proname,
              EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0) AS public_exec,
              has_function_privilege('g6_nobody', p.oid, 'EXECUTE') AS nobody,
              has_function_privilege('authenticated', p.oid, 'EXECUTE') AS authenticated,
              has_function_privilege('g6_rt2', p.oid, 'EXECUTE') AS runtime
         FROM pg_proc p WHERE p.proname IN ('g6_by_owner', 'g6_by_superuser') ORDER BY 1`,
      )
    ).rows;
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_bh WITH (FORCE)");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_nobody");
    const ok =
      m.exit === 0 &&
      chk.status === 0 &&
      r.length === 2 &&
      r.every(
        (x) => !x.public_exec && !x.nobody && !x.authenticated && !x.runtime,
      );
    return [
      ok,
      `migrate=${fmt(m)} activation_check=${chk.status} functions=${fmt(r)}`,
    ];
  },
);
await check(
  "pre-creation creator coverage: a CREATE-capable role that has created no function yet fails the gate (evidence names it)",
  async () => {
    await fresh("g6_pc", ["g6_rt2"]);
    const m1 = migrate("g6_pc", SEQ);
    await attempt("postgres", "CREATE ROLE g6_can_create NOLOGIN");
    await sql(
      "g6_pc",
      "GRANT CREATE, USAGE ON SCHEMA sessions TO g6_can_create",
    );
    const before = await snapshot("g6_pc");
    const m2 = migrate("g6_pc", SEQ);
    const same = (await snapshot("g6_pc")) === before;
    const chk = activationCheck("g6_pc");
    const named = (
      chk.evidence.sessions_create_roles as Array<{
        role: string;
        function_default_exact: boolean;
      }>
    ).find((c) => c.role === "g6_can_create");
    const boot = psql("g6_pc", [
      "-v",
      "migration_owner=hoop",
      "-f",
      FN_DEFAULT_BOOT,
    ]);
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_pc WITH (FORCE)");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_can_create");
    const ok =
      m1.exit === 0 &&
      m2.code === "function_boundary_unproven" &&
      same &&
      chk.status === 1 &&
      named?.function_default_exact === false &&
      chk.evidence.sessions_functions_owned_by_owner === true &&
      !boot.ok &&
      boot.out.includes(
        "can CREATE in sessions without an exact-safe function default",
      );
    return [
      ok,
      `first=${fmt(m1)} gate=${fmt(m2)} unchanged=${same} creator=${fmt(named)} bootstrap_refused=${!boot.ok}`,
    ];
  },
);
// Option (ii), supabase mode: only a role that is rolsuper (catalog, gate time) AND named
// in SUPABASE_PLATFORM_SUPERUSERS is excluded from creator coverage. Here hoop is the
// (non-superuser) owner, and the cluster superuser postgres is a NON-platform superuser,
// so it stays covered and gets its own exact default.
let PLATFORM_EVIDENCE: unknown = null;
const SUPA_GATE = {
  HOOPDB_DB_MODE: "supabase",
  HOOPDB_RUNTIME_ROLE: "g6_rt2",
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
};
const EXACT_HOOP_AND_POSTGRES =
  "ALTER DEFAULT PRIVILEGES FOR ROLE hoop REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC";
const supaCase = async (
  db: string,
  setup: string,
  body: () => Promise<[boolean, string]>,
): Promise<[boolean, string]> => {
  await fresh(db, ["g6_rt2"], false);
  await sql(db, setup);
  try {
    return await body();
  } finally {
    await attempt("postgres", `DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
    await attempt("postgres", "DROP ROLE IF EXISTS supabase_admin");
    await attempt("postgres", "DROP ROLE IF EXISTS ops_admin");
  }
};
await check(
  "platform trust root (supabase): supabase_admin as a superuser with no safe default is excluded and listed with its attributes; the same catalog fails selfhosted and with no mode (revert-sensitive)",
  () =>
    supaCase(
      "g6_pt1",
      `${EXACT_HOOP_AND_POSTGRES}; CREATE ROLE supabase_admin SUPERUSER NOLOGIN`,
      async () => {
        const m = migrate("g6_pt1", SUPA_GATE);
        const supa = activationCheck("g6_pt1", { HOOPDB_DB_MODE: "supabase" });
        const self = activationCheck("g6_pt1", {
          HOOPDB_DB_MODE: "selfhosted",
        });
        const unset = activationCheck("g6_pt1");
        const selfMigrate = migrate("g6_pt1", {
          ...SUPA_GATE,
          HOOPDB_DB_MODE: "selfhosted",
          HOOPDB_SUPABASE_SHIM: "false",
        });
        PLATFORM_EVIDENCE = supa.evidence;
        const ex = supa.evidence.excluded_platform_superusers as Array<
          Record<string, unknown>
        >;
        const covers = (e: {
          sessions_create_roles?: Array<{
            role: string;
            function_default_exact: boolean;
          }>;
        }) =>
          e.sessions_create_roles?.some(
            (c) => c.role === "supabase_admin" && !c.function_default_exact,
          ) === true;
        const ok =
          m.exit === 0 &&
          supa.status === 0 &&
          ex.length === 1 &&
          ex[0]!.role === "supabase_admin" &&
          ex[0]!.rolsuper === true &&
          ex[0]!.rolcanlogin === false &&
          ex[0]!.rolbypassrls === false &&
          JSON.stringify(ex[0]!.function_default) ===
            JSON.stringify(["<built-in>"]) &&
          self.status === 1 &&
          covers(self.evidence) &&
          (self.evidence.excluded_platform_superusers as unknown[]).length ===
            0 &&
          unset.status === 1 &&
          covers(unset.evidence) &&
          selfMigrate.code === "function_boundary_unproven";
        return [
          ok,
          `supabase_migrate=${fmt(m)} supabase_check=${supa.status} excluded=${fmt(ex)} selfhosted_check=${self.status} no_mode_check=${unset.status} selfhosted_migrate=${fmt(selfMigrate)}`,
        ];
      },
    ),
);
await check(
  "platform trust root (supabase): a non-platform superuser (ops_admin) with no safe default stays covered: refused before any write",
  () =>
    supaCase(
      "g6_pt2",
      `${EXACT_HOOP_AND_POSTGRES}; CREATE ROLE ops_admin SUPERUSER NOLOGIN`,
      async () => {
        const m = migrate("g6_pt2", SUPA_GATE);
        const absent = (await snapshot("g6_pt2")) === "absent";
        const c = activationCheck("g6_pt2", { HOOPDB_DB_MODE: "supabase" });
        const ok =
          m.code === "function_boundary_unproven" &&
          absent &&
          c.status === 1 &&
          (c.evidence.excluded_platform_superusers as unknown[]).length === 0 &&
          (
            c.evidence.sessions_create_roles as Array<{
              role: string;
              superuser: boolean;
              function_default_exact: boolean;
            }>
          ).some(
            (x) =>
              x.role === "ops_admin" &&
              x.superuser &&
              !x.function_default_exact,
          );
        return [
          ok,
          `migrate=${fmt(m)} nothing_written=${absent} check=${c.status} excluded=${fmt(c.evidence.excluded_platform_superusers)}`,
        ];
      },
    ),
);
await check(
  "platform trust root (supabase): a role NAMED supabase_admin that is not a superuser, with CREATE on sessions and no safe default, is covered: refused, nothing changed",
  () =>
    supaCase("g6_pt3", EXACT_HOOP_AND_POSTGRES, async () => {
      const m1 = migrate("g6_pt3", SUPA_GATE);
      await attempt(
        "postgres",
        "CREATE ROLE supabase_admin NOSUPERUSER NOLOGIN",
      );
      await sql(
        "g6_pt3",
        "GRANT CREATE, USAGE ON SCHEMA sessions TO supabase_admin",
      );
      const before = await snapshot("g6_pt3");
      const m2 = migrate("g6_pt3", SUPA_GATE);
      const same = (await snapshot("g6_pt3")) === before;
      const c = activationCheck("g6_pt3", { HOOPDB_DB_MODE: "supabase" });
      await attempt(
        "g6_pt3",
        "REVOKE ALL ON SCHEMA sessions FROM supabase_admin",
      );
      const ok =
        m1.exit === 0 &&
        m2.code === "function_boundary_unproven" &&
        same &&
        c.status === 1 &&
        (c.evidence.excluded_platform_superusers as unknown[]).length === 0 &&
        (
          c.evidence.sessions_create_roles as Array<{
            role: string;
            superuser: boolean;
            function_default_exact: boolean;
          }>
        ).some(
          (x) =>
            x.role === "supabase_admin" &&
            !x.superuser &&
            !x.function_default_exact,
        );
      return [
        ok,
        `first=${fmt(m1)} gate=${fmt(m2)} unchanged=${same} check=${c.status}`,
      ];
    }),
);
await check(
  "platform trust root (supabase): the non-superuser migration owner with no safe default is refused even with supabase_admin excluded",
  () =>
    supaCase(
      "g6_pt4",
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC; CREATE ROLE supabase_admin SUPERUSER NOLOGIN",
      async () => {
        const m = migrate("g6_pt4", SUPA_GATE);
        const absent = (await snapshot("g6_pt4")) === "absent";
        const c = activationCheck("g6_pt4", { HOOPDB_DB_MODE: "supabase" });
        const ok =
          m.code === "function_boundary_unproven" &&
          absent &&
          c.status === 1 &&
          c.evidence.owner_function_default_exact === false &&
          (c.evidence.excluded_platform_superusers as Array<{ role: string }>)
            .map((x) => x.role)
            .join() === "supabase_admin";
        return [
          ok,
          `migrate=${fmt(m)} nothing_written=${absent} owner_default=${fmt(c.evidence.owner_function_default)}`,
        ];
      },
    ),
);
await check(
  "platform trust root: no environment variable widens the exclusion (ops_admin superuser named in hostile env vars stays covered)",
  () =>
    supaCase(
      "g6_pt5",
      `${EXACT_HOOP_AND_POSTGRES}; CREATE ROLE ops_admin SUPERUSER NOLOGIN`,
      async () => {
        const hostile = {
          SUPABASE_PLATFORM_SUPERUSERS: "ops_admin",
          HOOPDB_SUPABASE_PLATFORM_SUPERUSERS: "ops_admin",
          HOOPDB_PLATFORM_SUPERUSERS: "ops_admin",
          HOOPDB_EXCLUDED_SUPERUSERS: "ops_admin",
        };
        const m = migrate("g6_pt5", { ...SUPA_GATE, ...hostile });
        const c = activationCheck("g6_pt5", {
          HOOPDB_DB_MODE: "supabase",
          ...hostile,
        });
        const ok =
          m.code === "function_boundary_unproven" &&
          c.status === 1 &&
          (c.evidence.excluded_platform_superusers as unknown[]).length === 0;
        return [
          ok,
          `migrate=${fmt(m)} check=${c.status} excluded=${fmt(c.evidence.excluded_platform_superusers)}`,
        ];
      },
    ),
);
await check(
  "a sessions-scoped function default granting authenticated EXECUTE (global defaults exact) fails the gate and the bootstrap, nothing changed",
  async () => {
    await fresh("g6_sd", ["g6_rt2"]);
    const m1 = migrate("g6_sd", SEQ);
    await sql(
      "g6_sd",
      "ALTER DEFAULT PRIVILEGES FOR ROLE hoop IN SCHEMA sessions GRANT EXECUTE ON FUNCTIONS TO authenticated",
    );
    const before = await snapshot("g6_sd");
    const m2 = migrate("g6_sd", SEQ);
    const same = (await snapshot("g6_sd")) === before;
    const chk = activationCheck("g6_sd");
    const boot = psql("g6_sd", [
      "-v",
      "migration_owner=hoop",
      "-f",
      FN_DEFAULT_BOOT,
    ]);
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_sd WITH (FORCE)");
    const ok =
      m1.exit === 0 &&
      m2.code === "function_boundary_unproven" &&
      same &&
      chk.status === 1 &&
      chk.evidence.owner_function_default_exact === true &&
      JSON.stringify(chk.evidence.sessions_schema_function_defaults) ===
        JSON.stringify(["hoop>authenticated:EXECUTE"]) &&
      !boot.ok &&
      boot.out.includes("sessions-scoped function default");
    return [
      ok,
      `first=${fmt(m1)} gate=${fmt(m2)} unchanged=${same} schema_defaults=${fmt(chk.evidence.sessions_schema_function_defaults)} bootstrap_refused=${!boot.ok}`,
    ];
  },
);
await check(
  "creator coverage: a sessions function created by another role escapes the owner-scoped default; the gate refuses it before any write",
  async () => {
    await fresh("g6_fb", ["g6_rt2"]);
    const m1 = migrate("g6_fb", SEQ);
    await attempt("postgres", "CREATE ROLE g6_other_creator LOGIN");
    await sql(
      "g6_fb",
      "GRANT CREATE, USAGE ON SCHEMA sessions TO g6_other_creator; GRANT CONNECT ON DATABASE g6_fb TO g6_other_creator",
    );
    await sql(
      "g6_fb",
      "CREATE FUNCTION sessions.g6_foreign() RETURNS int LANGUAGE sql AS 'SELECT 1'",
      "g6_other_creator",
    );
    const before = await snapshot("g6_fb");
    const m2 = migrate("g6_fb", SEQ);
    const same = (await snapshot("g6_fb")) === before;
    const chk = spawnSync(
      "bun",
      ["src/hoopdb/migrate.ts", "--activation-check"],
      {
        cwd: ROOT,
        encoding: "utf8",
        env: {
          ...process.env,
          HOOPDB_URL: url("hoop", "g6_fb"),
          HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
        },
      },
    );
    const ev = JSON.parse(chk.stdout.trim().split("\n").at(-1) ?? "{}");
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_fb WITH (FORCE)");
    await attempt("postgres", "DROP ROLE IF EXISTS g6_other_creator");
    const ok =
      m1.exit === 0 &&
      m2.code === "function_boundary_unproven" &&
      same &&
      chk.status === 1 &&
      ev.sessions_functions_owned_by_owner === false &&
      ev.owner_function_default_exact === true;
    return [
      ok,
      `first=${fmt(m1)} after_foreign_function=${fmt(m2)} unchanged=${same} evidence=${fmt(ev)}`,
    ];
  },
);
await check(
  "PostgreSQL 17+: MAINTAIN through PUBLIC (not granted to the role) is refused (version-guarded)",
  async () => {
    if (MAJOR < 17)
      return [true, `skipped: PostgreSQL ${MAJOR} has no MAINTAIN privilege`];
    await fresh("g6_mt", ["g6_rt2"]);
    const m1 = migrate("g6_mt", SEQ);
    await sql("g6_mt", "GRANT MAINTAIN ON sessions.session_index TO PUBLIC");
    const before = await snapshot("g6_mt");
    const m2 = migrate("g6_mt", SEQ);
    const same = (await snapshot("g6_mt")) === before;
    const direct = (
      await sql(
        "g6_mt",
        "SELECT EXISTS (SELECT 1 FROM pg_class c, aclexplode(c.relacl) a JOIN pg_roles r ON r.oid = a.grantee WHERE c.oid = 'sessions.session_index'::regclass AND r.rolname = 'g6_rt2' AND a.privilege_type = 'MAINTAIN') AS d",
      )
    ).rows[0].d;
    await attempt("postgres", "DROP DATABASE IF EXISTS g6_mt WITH (FORCE)");
    return [
      m1.exit === 0 &&
        m2.code === "runtime_role_unsafe" &&
        same &&
        direct === false,
      `first=${fmt(m1)} via_public=${fmt(m2)} unchanged=${same} direct_grant_to_role=${direct}`,
    ];
  },
);
for (const [name, env] of [
  [
    "AWS CNPG fixture env (hoopdbModeEnv)",
    Object.fromEntries(
      hoopdbModeEnv(true, "g6_rt2", true).map((e) => [e.name, e.value]),
    ),
  ],
] as const)
  await check(
    `${name} passes the gate after the selfhosted bootstraps`,
    async () => {
      await fresh("g6_fx", ["g6_rt2"]);
      const m = migrate("g6_fx", env as Record<string, string>);
      await attempt("postgres", "DROP DATABASE IF EXISTS g6_fx WITH (FORCE)");
      return [
        m.exit === 0 &&
          (env as Record<string, string>).HOOPDB_FUNCTION_BOUNDARY_MODE ===
            "global-default-revoke",
        `env=${fmt(env)} migrate=${fmt(m)}`,
      ];
    },
  );

for (const name of NON_CREATE_STEPS) {
  const i = SCHEMA_FILES.findIndex((st) => st.name === name);
  await check(
    `sentinel ${name}: applied and ledgered is accepted (no false positive)`,
    async () => {
      await fresh("g6_sent", ["g6_rt2"]);
      await prefixDb("g6_sent", i, i + 1);
      const m = migrate("g6_sent", SEQ);
      return [i > 0 && m.exit === 0, fmt(m)];
    },
  );
  await check(
    `sentinel ${name}: applied but unledgered is refused before any write`,
    async () => {
      await fresh("g6_sent", ["g6_rt2"]);
      await prefixDb("g6_sent", i, i);
      const before = await snapshot("g6_sent");
      const m = migrate("g6_sent", SEQ);
      const same = (await snapshot("g6_sent")) === before;
      return [
        m.code === "unledgered_schema" && same,
        `${fmt(m)} unchanged=${same}`,
      ];
    },
  );
  await attempt("postgres", "DROP DATABASE IF EXISTS g6_sent WITH (FORCE)");
}

await attempt("postgres", "DROP DATABASE IF EXISTS g6_rt_db");
await attempt("postgres", "DROP DATABASE IF EXISTS g6_mode");
await attempt("postgres", "DROP DATABASE IF EXISTS g6_w");

await check(
  "020000 historical checksum alias is guard-only: old and current files leave identical catalog snapshots",
  async () => {
    const currentPath = join(
      ROOT,
      "supabase/migrations/20261030020000_session_directory_short_summary.sql",
    );
    const current = readFileSync(currentPath, "utf8");
    const historical = historicalShortSummary(current);
    const historicalHash = createHash("sha256")
      .update(historical)
      .digest("hex");
    const currentHash = createHash("sha256").update(current).digest("hex");
    const historicalPath = `/tmp/96bulls-020000-historical-${process.pid}.sql`;
    writeFileSync(historicalPath, historical);
    const target = SCHEMA_FILES.findIndex(
      (f) =>
        f.name ===
        "upstream:20261030020000_session_directory_short_summary.sql",
    );
    let oldSnap: Record<string, string> = {};
    let newSnap: Record<string, string> = {};
    let applied = true;
    try {
      for (const db of ["g6_alias_old", "g6_alias_new"]) {
        await attempt("postgres", `DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
        await sql("postgres", `CREATE DATABASE ${db} OWNER hoop`);
        for (let i = 0; i < target; i++) {
          const r = psql(db, ["-1", "-f", SCHEMA_FILES[i]!.path], "hoop");
          if (!r.ok)
            throw new Error(`${db} prefix ${SCHEMA_FILES[i]!.name}: ${r.out}`);
        }
      }
      const oldApply = psql(
        "g6_alias_old",
        ["-1", "-f", historicalPath],
        "hoop",
      );
      const newApply = psql("g6_alias_new", ["-1", "-f", currentPath], "hoop");
      applied = oldApply.ok && newApply.ok;
      if (!applied) throw new Error(`old=${oldApply.out}\nnew=${newApply.out}`);
      oldSnap = await sessionsCatalogSnapshot("g6_alias_old");
      newSnap = await sessionsCatalogSnapshot("g6_alias_new");
    } finally {
      unlinkSync(historicalPath);
      await attempt(
        "postgres",
        "DROP DATABASE IF EXISTS g6_alias_old WITH (FORCE)",
      );
      await attempt(
        "postgres",
        "DROP DATABASE IF EXISTS g6_alias_new WITH (FORCE)",
      );
    }
    const exactOld =
      historicalHash ===
      "c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4";
    const exactCurrent =
      currentHash ===
      "b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce";
    const identical = JSON.stringify(oldSnap) === JSON.stringify(newSnap);
    return [
      applied && exactOld && exactCurrent && identical,
      `old_sha=${historicalHash} current_sha=${currentHash} catalog_identical=${identical} categories=${Object.keys(oldSnap).join(",")}`,
    ];
  },
);

// Final destructive-to-the-disposable-cluster proof: model Supabase's exact PG16+
// bootstrap identity. The bootstrap superuser is OID 10 and named supabase_admin;
// postgres is the non-superuser CREATEROLE migration owner. This is intentionally last,
// because every earlier proof uses the stock postgres superuser.
await check(
  "Supabase role creator compatibility: exact edge passes end to end; mode and option mutations fail closed",
  async () => {
    await sql("postgres", "CREATE ROLE bootstrap_super SUPERUSER LOGIN");
    await sql(
      "postgres",
      "ALTER ROLE postgres RENAME TO supabase_admin; CREATE ROLE postgres LOGIN CREATEROLE",
      "bootstrap_super",
    );
    await sql("postgres", "DROP ROLE bootstrap_super", "supabase_admin");
    await sql(
      "postgres",
      "DROP ROLE session_directory_search_worker",
      "supabase_admin",
    );
    await sql(
      "postgres",
      "CREATE DATABASE g6_supabase_edge OWNER postgres",
      "supabase_admin",
    );
    const shim = psql(
      "g6_supabase_edge",
      ["-f", join(ROOT, "supabase/tests/supabase_shim.sql")],
      "supabase_admin",
    );
    if (!shim.ok)
      throw new Error(`Supabase role-edge shim failed: ${shim.out}`);
    await sql(
      "g6_supabase_edge",
      `ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
       CREATE SCHEMA extensions;
       CREATE TABLE extensions.pg_stat_statements (calls bigint);
       GRANT SELECT ON extensions.pg_stat_statements TO PUBLIC;
       CREATE ROLE relayhistory_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
       GRANT TEMPORARY ON DATABASE g6_supabase_edge TO relayhistory_runtime`,
      "postgres",
    );
    await sql(
      "g6_supabase_edge",
      "ALTER ROLE relayhistory_runtime BYPASSRLS",
      "supabase_admin",
    );
    const runtimeBoot = psql(
      "g6_supabase_edge",
      [
        "-v",
        "runtime_role=bulls96_runtime",
        "-v",
        "runtime_login=true",
        "-f",
        SUPABASE_RUNTIME_BOOT,
      ],
      "postgres",
    );
    const boot = psql(
      "g6_supabase_edge",
      [
        "-f",
        join(
          ROOT,
          "supabase/bootstrap/session_directory_search_worker_supabase.sql",
        ),
      ],
      "postgres",
    );
    const exactEdges =
      Number(version.split(".")[0]) >= 16
        ? (
            await sql(
              "g6_supabase_edge",
              `SELECT g.rolname AS target, u.rolname AS member, x.rolname AS grantor,
                      m.admin_option, m.inherit_option, m.set_option
                 FROM pg_auth_members m
                 JOIN pg_roles g ON g.oid=m.roleid
                 JOIN pg_roles u ON u.oid=m.member
                 JOIN pg_roles x ON x.oid=m.grantor
                WHERE g.rolname IN ('bulls96_runtime','session_directory_search_worker')
                ORDER BY 1`,
              "postgres",
            )
          ).rows
        : [];
    const supabase = {
      HOOPDB_DB_MODE: "supabase",
      HOOPDB_SUPABASE_SHIM: "false",
      HOOPDB_RUNTIME_ROLE: "bulls96_runtime",
      HOOPDB_RUNTIME_ROLE_LOGIN: "true",
      HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
    };
    const legacyDrift = migrate(
      "g6_supabase_edge",
      { ...supabase, HOOPDB_RUNTIME_ROLE: "relayhistory_runtime" },
      "postgres",
    );
    await sql(
      "g6_supabase_edge",
      "GRANT SELECT ON extensions.pg_stat_statements TO bulls96_runtime",
      "postgres",
    );
    const directUnreachableRelation = migrate(
      "g6_supabase_edge",
      supabase,
      "postgres",
    );
    await sql(
      "g6_supabase_edge",
      "REVOKE SELECT ON extensions.pg_stat_statements FROM bulls96_runtime",
      "postgres",
    );
    const migrated = migrate("g6_supabase_edge", supabase, "postgres");
    const targetMigration =
      "upstream:20261030020000_session_directory_short_summary.sql";
    const historicalSha =
      "c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4";
    // Parameterized separately because sql() intentionally has a minimal text-only API.
    const aliasClient = new pg.Client({
      connectionString: url("postgres", "g6_supabase_edge"),
    });
    await aliasClient.connect();
    await aliasClient.query(
      "UPDATE sessions.__96bulls_migrations SET sha256 = $1 WHERE name = $2",
      [historicalSha, targetMigration],
    );
    await aliasClient.end();
    const aliasAccepted = migrate("g6_supabase_edge", supabase, "postgres");
    const aliasCheck = activationCheck(
      "g6_supabase_edge",
      { HOOPDB_DB_MODE: "supabase" },
      "postgres",
    );
    const aliasLedger = (
      await sql(
        "g6_supabase_edge",
        `SELECT sha256 FROM sessions.__96bulls_migrations
          WHERE name = '${targetMigration}'`,
        "postgres",
      )
    ).rows[0]?.sha256;
    await sql(
      "g6_supabase_edge",
      `UPDATE sessions.__96bulls_migrations SET sha256 = '${"d".repeat(64)}'
        WHERE name = '${targetMigration}'`,
      "postgres",
    );
    const thirdSha = migrate("g6_supabase_edge", supabase, "postgres");
    await sql(
      "g6_supabase_edge",
      `UPDATE sessions.__96bulls_migrations SET sha256 = '${historicalSha}'
        WHERE name = '${targetMigration}'`,
      "postgres",
    );
    const wrongMode = migrate(
      "g6_supabase_edge",
      {
        ...supabase,
        HOOPDB_DB_MODE: "selfhosted",
        HOOPDB_SUPABASE_SHIM: "false",
      },
      "postgres",
    );

    await sql(
      "g6_supabase_edge",
      "GRANT TEMPORARY ON DATABASE g6_supabase_edge TO bulls96_runtime",
      "postgres",
    );
    const directTemp = migrate("g6_supabase_edge", supabase, "postgres");
    await sql(
      "g6_supabase_edge",
      "REVOKE TEMPORARY ON DATABASE g6_supabase_edge FROM bulls96_runtime",
      "postgres",
    );

    await sql(
      "g6_supabase_edge",
      "GRANT USAGE ON SCHEMA extensions TO PUBLIC",
      "postgres",
    );
    const reachablePublicRelation = migrate(
      "g6_supabase_edge",
      supabase,
      "postgres",
    );
    await sql(
      "g6_supabase_edge",
      "REVOKE USAGE ON SCHEMA extensions FROM PUBLIC",
      "postgres",
    );

    const workloadArgs = [
      "-v",
      "runtime_role=bulls96_runtime_workload",
      "-v",
      "runtime_login=false",
      "-v",
      "workload_role=bulls96_workload",
      "-f",
      SUPABASE_RUNTIME_BOOT,
    ];
    const workloadCreate = psql("g6_supabase_edge", workloadArgs, "postgres");
    await sql(
      "g6_supabase_edge",
      "CREATE ROLE bulls96_workload LOGIN",
      "postgres",
    );
    await sql(
      "g6_supabase_edge",
      MAJOR >= 16
        ? "GRANT bulls96_runtime_workload TO bulls96_workload WITH INHERIT TRUE, SET TRUE"
        : "GRANT bulls96_runtime_workload TO bulls96_workload",
      MAJOR >= 16 ? "postgres" : "supabase_admin",
    );
    const workloadRerun = psql("g6_supabase_edge", workloadArgs, "postgres");

    let mutationRefused = true;
    if (Number(version.split(".")[0]) >= 16) {
      await sql(
        "g6_supabase_edge",
        `REVOKE session_directory_search_worker FROM postgres GRANTED BY supabase_admin;
         GRANT session_directory_search_worker TO postgres
           WITH ADMIN OPTION, INHERIT FALSE, SET TRUE GRANTED BY supabase_admin`,
        "supabase_admin",
      );
      const mutated = psql(
        "g6_supabase_edge",
        [
          "-c",
          "BEGIN; SELECT set_config('hoopdb.db_mode','supabase',true);",
          "-f",
          BOOT,
          "-c",
          "ROLLBACK",
        ],
        "postgres",
      );
      mutationRefused =
        !mutated.ok && mutated.out.includes("granted to postgres");
    }
    const edgeShape =
      Number(version.split(".")[0]) < 16 ||
      (exactEdges.length === 2 &&
        exactEdges.every(
          (e) =>
            e.member === "postgres" &&
            e.grantor === "supabase_admin" &&
            e.admin_option === true &&
            e.inherit_option === false &&
            e.set_option === false,
        ));
    return [
      runtimeBoot.ok &&
        boot.ok &&
        edgeShape &&
        legacyDrift.code === "runtime_role_unsafe" &&
        directUnreachableRelation.code === "runtime_role_unsafe" &&
        migrated.exit === 0 &&
        aliasAccepted.exit === 0 &&
        aliasCheck.status === 0 &&
        JSON.stringify(aliasCheck.evidence.migration_checksum_aliases) ===
          JSON.stringify([
            {
              migration: targetMigration,
              ledger_sha: historicalSha,
              accepted_as_alias_of:
                "b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce",
            },
          ]) &&
        aliasLedger === historicalSha &&
        thirdSha.code === "migration_changed" &&
        directTemp.code === "runtime_role_unsafe" &&
        reachablePublicRelation.code === "runtime_role_unsafe" &&
        workloadCreate.ok &&
        workloadRerun.ok &&
        (Number(version.split(".")[0]) < 16 ||
          wrongMode.code === "runtime_role_unsafe") &&
        mutationRefused,
      `runtime_boot=${runtimeBoot.ok} worker_boot=${boot.ok} edges=${fmt(exactEdges)} legacy_drift=${fmt(legacyDrift)} direct_unreachable=${fmt(directUnreachableRelation)} migrated=${fmt(migrated)} alias=${fmt(aliasAccepted)} alias_check=${fmt(aliasCheck)} alias_ledger=${aliasLedger} third_sha=${fmt(thirdSha)} direct_temp=${fmt(directTemp)} reachable_relation=${fmt(reachablePublicRelation)} workload_create=${workloadCreate.ok} workload_rerun=${workloadRerun.ok} wrong_mode=${fmt(wrongMode)} mutation_refused=${mutationRefused}`,
    ];
  },
);

// Event-trigger mode (supabase only), on the same Supabase-shaped cluster as above:
// supabase_admin is the bootstrap superuser, postgres the non-superuser migration owner.
// supautils lets Supabase's privileged postgres CREATE EVENT TRIGGER and then owns it as
// postgres; plain PostgreSQL does not, so the harness emulates that one step by granting
// postgres SUPERUSER only for the bootstrap and revoking it right after (the resulting
// catalog matches supautils: trigger and function owned by postgres). Without the
// emulation the bootstrap must fail closed.
const ET_BOOT = join(
  ROOT,
  "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
);
let EVENT_TRIGGER_EVIDENCE: unknown = null;
await check(
  "event-trigger mode (Supabase-shaped): fails closed without supautils; with it a fresh full migrate passes; the trigger enforces the boundary; every drift fails the gate before any write",
  async () => {
    const DB = "g6_supabase_et";
    const SA = "supabase_admin";
    const out: Record<string, unknown> = {};
    const ok: Record<string, boolean> = {};
    // One migrated database per cluster (cross-database role references): drop the
    // previous Supabase-shaped database first.
    await sql(
      "postgres",
      "DROP DATABASE IF EXISTS g6_supabase_edge WITH (FORCE)",
      SA,
    );
    for (const role of [
      "session_directory_search_worker",
      "bulls96_runtime",
      "bulls96_runtime_workload",
      "bulls96_workload",
      "relayhistory_runtime",
    ])
      await attempt("postgres", `DROP ROLE IF EXISTS ${role}`, SA);
    await sql("postgres", `CREATE DATABASE ${DB} OWNER postgres`, SA);
    const shim = psql(
      DB,
      ["-f", join(ROOT, "supabase/tests/supabase_shim.sql")],
      SA,
    );
    if (!shim.ok) throw new Error(`shim failed: ${shim.out}`);
    await sql(
      DB,
      `CREATE SCHEMA extensions;
       CREATE TABLE extensions.pg_stat_statements (calls bigint);
       GRANT SELECT ON extensions.pg_stat_statements TO PUBLIC`,
      "postgres",
    );
    const runtimeBoot = psql(
      DB,
      [
        "-v",
        "runtime_role=bulls96_runtime",
        "-v",
        "runtime_login=true",
        "-f",
        SUPABASE_RUNTIME_BOOT,
      ],
      "postgres",
    );
    const workerBoot = psql(
      DB,
      [
        "-f",
        join(
          ROOT,
          "supabase/bootstrap/session_directory_search_worker_supabase.sql",
        ),
      ],
      "postgres",
    );
    ok.role_boots = runtimeBoot.ok && workerBoot.ok;
    if (!ok.role_boots)
      out.role_boots = {
        runtime: runtimeBoot.out.slice(-300),
        worker: workerBoot.out.slice(-300),
      };
    const SUPA_ET = {
      HOOPDB_DB_MODE: "supabase",
      HOOPDB_SUPABASE_SHIM: "false",
      HOOPDB_RUNTIME_ROLE: "bulls96_runtime",
      HOOPDB_RUNTIME_ROLE_LOGIN: "true",
      HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
    };
    const ETCHECK = {
      HOOPDB_DB_MODE: "supabase",
      HOOPDB_RUNTIME_ROLE: "bulls96_runtime",
      HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
    };
    const bootArgs = ["-v", "runtime_role=bulls96_runtime", "-f", ET_BOOT];
    const absentObjects = async () =>
      (
        await sql(
          DB,
          "SELECT to_regnamespace('hoopdb_boundary') IS NULL AND NOT EXISTS (SELECT 1 FROM pg_event_trigger) AS absent",
          SA,
        )
      ).rows[0]!.absent === true;
    // 1. No supautils: the non-superuser postgres cannot create it; nothing is left behind.
    const noSupautils = psql(DB, bootArgs, "postgres");
    ok.no_supautils_fails_closed =
      !noSupautils.ok &&
      noSupautils.out.includes("CREATE EVENT TRIGGER was refused") &&
      (await absentObjects());
    // Gate before any trigger: refused, nothing written.
    const before = migrate(DB, SUPA_ET, "postgres");
    ok.no_trigger_refused =
      before.code === "function_boundary_unproven" &&
      (await snapshot(DB)) === "absent";
    // 2. Emulated supautils install.
    const install = async () => {
      await sql("postgres", "ALTER ROLE postgres SUPERUSER", SA);
      const r = psql(DB, bootArgs, "postgres");
      await sql("postgres", "ALTER ROLE postgres NOSUPERUSER", SA);
      return r;
    };
    const boot = await install();
    const rerun = psql(DB, bootArgs, "postgres"); // idempotent without superuser
    ok.bootstrap = boot.ok && rerun.ok;
    // 3. Fresh full migrate with the trigger, then the activation check.
    const migrated = migrate(DB, SUPA_ET, "postgres");
    const chk = activationCheck(DB, ETCHECK, "postgres");
    EVENT_TRIGGER_EVIDENCE = chk.evidence;
    out.migrated = migrated;
    if (migrated.exit !== 0) {
      await sql("postgres", `DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`, SA);
      return [
        false,
        `stopped: migrate=${fmt(migrated)} ok=${fmt(ok)} out=${fmt(out)} noSupautils=${noSupautils.out.slice(-300)} boot=${boot.out.slice(-400)} check=${fmt(chk.evidence)}`,
      ];
    }
    ok.fresh_full_migrate =
      migrated.exit === 0 &&
      chk.status === 0 &&
      chk.evidence.event_trigger?.verified === true;
    // The exact manifest still holds (plus the runtime application ACL).
    const acl = (
      await sql(
        DB,
        `SELECT count(*)::int AS n FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace
            AND (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0))`,
        "postgres",
      )
    ).rows[0]!.n;
    ok.no_public_after_migrate = acl === 0;
    // 4. Behaviour.
    const tryMsg = async (stmt: string, user = "postgres") => {
      try {
        await sql(DB, stmt, user);
        return null;
      } catch (e) {
        return (e as Error).message;
      }
    };
    const fails = async (stmt: string, user = "postgres") =>
      /sessions function boundary/.test((await tryMsg(stmt, user)) ?? "");
    await sql(
      DB,
      "CREATE FUNCTION sessions.g6_et_new() RETURNS int LANGUAGE sql AS 'SELECT 1'",
      "postgres",
    );
    const newPriv = (
      await sql(
        DB,
        `SELECT has_function_privilege('anon', 'sessions.g6_et_new()', 'EXECUTE') AS anon,
                has_function_privilege('authenticated', 'sessions.g6_et_new()', 'EXECUTE') AS authenticated,
                has_function_privilege('bulls96_runtime', 'sessions.g6_et_new()', 'EXECUTE') AS runtime,
                (SELECT NOT EXISTS (SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee = 0) AND proacl IS NOT NULL
                   FROM pg_proc WHERE oid = 'sessions.g6_et_new()'::regprocedure) AS no_public`,
        "postgres",
      )
    ).rows[0]!;
    ok.new_function_owner_only =
      !newPriv.anon &&
      !newPriv.authenticated &&
      !newPriv.runtime &&
      newPriv.no_public === true;
    ok.grant_public_aborts = await fails(
      "GRANT EXECUTE ON FUNCTION sessions.g6_et_new() TO PUBLIC",
    );
    ok.grant_anon_aborts = await fails(
      "GRANT EXECUTE ON FUNCTION sessions.g6_et_new() TO anon",
    );
    ok.grant_authenticated_unmanifested_aborts = await fails(
      "GRANT EXECUTE ON FUNCTION sessions.g6_et_new() TO authenticated",
    );
    ok.grant_all_functions_public_aborts = await fails(
      "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO PUBLIC",
    );
    ok.security_definer_aborts = await fails(
      "ALTER FUNCTION sessions.g6_et_new() SECURITY DEFINER",
    );
    await sql(DB, "CREATE ROLE g6_et_other LOGIN", "postgres");
    // PostgreSQL itself requires membership in the new owner and the new owner's CREATE
    // on the schema; with both, the trigger aborts the transfer.
    await sql(
      "postgres",
      MAJOR >= 16
        ? "GRANT g6_et_other TO postgres WITH INHERIT TRUE, SET TRUE"
        : "GRANT g6_et_other TO postgres",
      SA,
    );
    await sql(
      DB,
      "GRANT USAGE, CREATE ON SCHEMA sessions TO g6_et_other",
      "postgres",
    );
    const ownerTo = await tryMsg(
      "ALTER FUNCTION sessions.g6_et_new() OWNER TO g6_et_other",
    );
    out.owner_to = ownerTo;
    ok.owner_to_aborts = /sessions function boundary/.test(ownerTo ?? "");
    await sql(DB, "REVOKE ALL ON SCHEMA sessions FROM g6_et_other", "postgres");
    await sql("postgres", "REVOKE g6_et_other FROM postgres", SA);
    // Blast radius: another role's function DDL outside sessions is unaffected.
    ok.other_schema_unaffected =
      (await tryMsg(`GRANT CREATE ON DATABASE ${DB} TO g6_et_other`)) ===
        null &&
      (await tryMsg(
        "CREATE SCHEMA g6_et_own; CREATE FUNCTION g6_et_own.f() RETURNS int LANGUAGE sql AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION g6_et_own.f() TO PUBLIC; ALTER DEFAULT PRIVILEGES IN SCHEMA g6_et_own GRANT EXECUTE ON FUNCTIONS TO PUBLIC",
        "g6_et_other",
      )) === null;

    ok.sessions_default_aborts = await fails(
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT EXECUTE ON FUNCTIONS TO anon",
    );
    // CREATE OR REPLACE: a new and a manifested function keep their ACL and pass.
    const def = (
      await sql(
        DB,
        "SELECT pg_get_functiondef('sessions.jwt_org_id()'::regprocedure) AS d",
        "postgres",
      )
    ).rows[0]!.d as string;
    const aclOf = async (fn: string) =>
      (
        await sql(
          DB,
          `SELECT proacl::text AS a FROM pg_proc WHERE oid = '${fn}'::regprocedure`,
          "postgres",
        )
      ).rows[0]!.a;
    const jwtBefore = await aclOf("sessions.jwt_org_id()");
    const replaceNew = await tryMsg(
      "CREATE OR REPLACE FUNCTION sessions.g6_et_new() RETURNS int LANGUAGE sql AS 'SELECT 2'",
    );
    const replaceManifested = await tryMsg(def);
    ok.create_or_replace =
      replaceNew === null &&
      replaceManifested === null &&
      (await aclOf("sessions.jwt_org_id()")) === jwtBefore;
    // Another CREATE-capable role: its function is aborted (owner is not the migration owner).
    await sql(
      DB,
      "GRANT USAGE, CREATE ON SCHEMA sessions TO g6_et_other",
      "postgres",
    );
    ok.other_creator_aborted = await fails(
      "CREATE FUNCTION sessions.g6_by_other() RETURNS int LANGUAGE sql AS 'SELECT 1'",
      "g6_et_other",
    );
    // ... and while it can CREATE without an exact default the gate refuses.
    ok.other_creator_gate =
      migrate(DB, SUPA_ET, "postgres").code === "function_boundary_unproven";
    await sql(DB, "REVOKE ALL ON SCHEMA sessions FROM g6_et_other", "postgres");
    // A hostile second event trigger that GRANTs to PUBLIC after ours: its GRANT fires ours.
    await sql(
      DB,
      `CREATE FUNCTION public.g6_hostile() RETURNS event_trigger LANGUAGE plpgsql AS $h$
         DECLARE r record; BEGIN
           FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE FUNCTION' AND schema_name = 'sessions' LOOP
             EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', r.objid::regprocedure);
           END LOOP; END $h$;
       CREATE EVENT TRIGGER zzz_g6_hostile ON ddl_command_end WHEN TAG IN ('CREATE FUNCTION') EXECUTE FUNCTION public.g6_hostile()`,
      SA,
    );
    ok.hostile_trigger_aborted = await fails(
      "CREATE FUNCTION sessions.g6_after_hostile() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    const withHostile = activationCheck(DB, ETCHECK, "postgres");
    ok.hostile_trigger_reported = (
      withHostile.evidence.other_event_triggers as Array<{ name: string }>
    ).some((t) => t.name === "zzz_g6_hostile");
    await sql(
      DB,
      "DROP EVENT TRIGGER zzz_g6_hostile; DROP FUNCTION public.g6_hostile()",
      SA,
    );
    await sql(DB, "DROP FUNCTION sessions.g6_et_new()", "postgres");
    ok.remigrate = migrate(DB, SUPA_ET, "postgres").exit === 0;
    // 5. Drift: each fails the gate (and the activation check) before any write.
    const drift = async (
      name: string,
      apply: string,
      applyAs: string,
      restore: () => Promise<void>,
    ) => {
      await sql(DB, apply, applyAs);
      const ledgerBefore = (
        await sql(
          DB,
          "SELECT count(*)::int AS n FROM sessions.__96bulls_migrations",
          "postgres",
        )
      ).rows[0]!.n;
      const m = migrate(DB, SUPA_ET, "postgres");
      const c = activationCheck(DB, ETCHECK, "postgres");
      const ledgerAfter = (
        await sql(
          DB,
          "SELECT count(*)::int AS n FROM sessions.__96bulls_migrations",
          "postgres",
        )
      ).rows[0]!.n;
      out[name] = {
        migrate: m,
        check: c.status,
        verified: c.evidence.event_trigger?.verified,
      };
      ok[`drift_${name}`] =
        ["function_boundary_unproven", "runtime_role_unsafe"].includes(
          m.code,
        ) &&
        c.status === 1 &&
        c.evidence.event_trigger?.verified === false &&
        ledgerBefore === ledgerAfter;
      await restore();
    };
    const reinstall = async () => {
      await sql(
        DB,
        "DROP EVENT TRIGGER IF EXISTS hoopdb_sessions_function_boundary",
        SA,
      );
      const r = await install();
      if (!r.ok) throw new Error(`reinstall failed: ${r.out}`);
    };
    await drift(
      "disabled",
      "ALTER EVENT TRIGGER hoopdb_sessions_function_boundary DISABLE",
      SA,
      async () => {
        await sql(
          DB,
          "ALTER EVENT TRIGGER hoopdb_sessions_function_boundary ENABLE",
          SA,
        );
      },
    );
    await drift(
      "body_replaced",
      "CREATE OR REPLACE FUNCTION hoopdb_boundary.sessions_function_boundary() RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $f$ BEGIN END $f$",
      "postgres",
      async () => {
        const r = psql(DB, bootArgs, "postgres");
        if (!r.ok) throw new Error(`restore failed: ${r.out.slice(-600)}`);
      },
    );
    await drift(
      "function_owner_changed",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() OWNER TO g6_et_other",
      SA,
      async () => {
        await sql(
          DB,
          "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() OWNER TO postgres",
          SA,
        );
      },
    );
    await drift(
      "trigger_owner_changed",
      "UPDATE pg_event_trigger SET evtowner = 'supabase_admin'::regrole WHERE evtname = 'hoopdb_sessions_function_boundary'",
      SA,
      async () => {
        await sql(
          DB,
          "UPDATE pg_event_trigger SET evtowner = 'postgres'::regrole WHERE evtname = 'hoopdb_sessions_function_boundary'",
          SA,
        );
      },
    );
    await drift(
      "tag_missing",
      "DROP EVENT TRIGGER hoopdb_sessions_function_boundary; CREATE EVENT TRIGGER hoopdb_sessions_function_boundary ON ddl_command_end WHEN TAG IN ('CREATE FUNCTION', 'GRANT') EXECUTE FUNCTION hoopdb_boundary.sessions_function_boundary(); UPDATE pg_event_trigger SET evtowner = 'postgres'::regrole WHERE evtname = 'hoopdb_sessions_function_boundary'",
      SA,
      reinstall,
    );
    await drift(
      "security_definer",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() SECURITY DEFINER",
      "postgres",
      async () => {
        await sql(
          DB,
          "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() SECURITY INVOKER",
          "postgres",
        );
      },
    );
    await drift(
      "public_executable",
      "GRANT EXECUTE ON FUNCTION hoopdb_boundary.sessions_function_boundary() TO PUBLIC",
      "postgres",
      async () => {
        await sql(
          DB,
          "REVOKE EXECUTE ON FUNCTION hoopdb_boundary.sessions_function_boundary() FROM PUBLIC",
          "postgres",
        );
      },
    );
    await drift(
      "schema_usable_by_public",
      "GRANT USAGE ON SCHEMA hoopdb_boundary TO PUBLIC",
      "postgres",
      async () => {
        await sql(
          DB,
          "REVOKE USAGE ON SCHEMA hoopdb_boundary FROM PUBLIC",
          "postgres",
        );
      },
    );
    await drift(
      "function_setting_added",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() SET work_mem = '64MB'",
      "postgres",
      async () => {
        await sql(
          DB,
          "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() RESET work_mem",
          "postgres",
        );
      },
    );
    // The runtime role written into the body must be the migrator's runtime role, and a
    // rerun naming another one is refused (nothing changed).
    const mismatch = activationCheck(
      DB,
      { ...ETCHECK, HOOPDB_RUNTIME_ROLE: "g6_et_other" },
      "postgres",
    );
    ok.runtime_role_mismatch_unverified =
      mismatch.status === 1 &&
      mismatch.evidence.event_trigger?.runtime_role_setting ===
        "bulls96_runtime" &&
      mismatch.evidence.event_trigger?.runtime_role_setting_matches === false;
    const otherRuntime = psql(
      DB,
      ["-v", "runtime_role=g6_et_other", "-f", ET_BOOT],
      "postgres",
    );
    ok.rebootstrap_other_runtime_refused =
      !otherRuntime.ok &&
      otherRuntime.out.includes("already names runtime role");
    ok.final_remigrate = migrate(DB, SUPA_ET, "postgres").exit === 0;
    // Mode binding: event-trigger is refused outside supabase.
    ok.selfhosted_refused =
      migrate(DB, { ...SUPA_ET, HOOPDB_DB_MODE: "selfhosted" }, "postgres")
        .code === "function_boundary_mode_unsupported";
    await sql("postgres", `DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`, SA);
    await attempt("postgres", "DROP ROLE IF EXISTS g6_et_other", SA);
    await attempt("postgres", "DROP ROLE IF EXISTS bulls96_runtime", SA);
    const failedKeys = Object.entries(ok)
      .filter(([, v]) => !v)
      .map(([k]) => k);
    return [
      failedKeys.length === 0,
      `failed=${fmt(failedKeys)} checks=${Object.keys(ok).length} details=${fmt(out)}`,
    ];
  },
);

const failed = cases.filter((c) => !c.pass).length;
const results = {
  head: process.env.PGPROOF_HEAD ?? "unknown",
  // The catalog evidence of a correctly bootstrapped selfhosted database: the owner's
  // global function default and every role able to CREATE in sessions, with its default.
  function_boundary_evidence: BOUNDARY_EVIDENCE,
  // Supabase mode with a platform superuser (supabase_admin) excluded from coverage.
  supabase_platform_trust_root_evidence: PLATFORM_EVIDENCE,
  // Supabase-shaped database with the reviewed event trigger: --activation-check output.
  event_trigger_boundary_evidence: EVENT_TRIGGER_EVIDENCE,
  pg_version: version,
  pg_major: Number(version.split(".")[0]),
  generation: await generation(),
  harness_sha256: createHash("sha256")
    .update(await Bun.file(fileURLToPath(import.meta.url)).text())
    .digest("hex"),
  finished_at: new Date().toISOString(),
  passed: cases.length - failed,
  failed,
  cases,
};
writeFileSync(OUT, `${JSON.stringify(results, null, 2)}\n`);
console.log(`# ${results.passed} passed, ${failed} failed -> ${OUT}`);
process.exit(failed ? 1 : 0);
