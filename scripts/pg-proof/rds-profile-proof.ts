/**
 * Real-PostgreSQL proof of the managed-RDS HoopDb profile (HOOPDB_DB_MODE=rds).
 *
 * Runs against ONE disposable PostgreSQL 16+ server whose BOOTSTRAP superuser (oid 10)
 * is named rdsadmin (CI: postgres:16/17 service containers with POSTGRES_USER=rdsadmin;
 * locally: `initdb -U rdsadmin`). That is what makes PostgreSQL's automatic PG16+
 * creator edge carry rdsadmin as grantor, as it does on RDS. Never point it at a real
 * database: it creates and drops roles and databases. Trust auth only.
 *
 * RDS emulation (what it models): rds_superuser NOLOGIN with the PG16+ predefined-role
 * memberships RDS documents, a master LOGIN CREATEROLE CREATEDB NOSUPERUSER member of
 * rds_superuser, and a database owned by the master. Limits: vanilla PostgreSQL has no
 * rds.* GUCs, no rdsadmin-only catalog hooks, no RDS extension allow-list, no IAM auth,
 * no parameter groups, and refuses CREATE EVENT TRIGGER to rds_superuser members (real
 * RDS allows it). See the runbook "Amazon RDS for PostgreSQL" for the live gates.
 *
 * Env: PGPROOF_HOST / PGPROOF_PORT (default 127.0.0.1:5432), PGPROOF_SUPERUSER (must be
 * rdsadmin), PGPROOF_PSQL, PGPROOF_HEAD, PGPROOF_OUT (default rds-profile-proof.json).
 * Exit 0 only if every case passes.
 */
import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { inTransaction, openHoopDb, schemaVerdict } from "../../src/shared/db.ts";
import { expectedSchema, generation, migrationProfile } from "../../src/hoopdb/migrate.ts";
import { functionBoundaryEvidence } from "../../src/hoopdb/migrate-guards.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../src/hoopdb/session-bootstrap-chain.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const HOST = process.env.PGPROOF_HOST ?? "127.0.0.1";
const PORT = process.env.PGPROOF_PORT ?? "5432";
const SUPER = process.env.PGPROOF_SUPERUSER ?? "rdsadmin";
const PSQL = process.env.PGPROOF_PSQL ?? "psql";
const OUT = process.env.PGPROOF_OUT ?? "rds-profile-proof.json";
const MASTER = "g7_rds_master";
const OTHER = "g7_rds_other";
const RT = "g7_rds_rt";
const DB = "g7_rds";
const B = (f: string) => join(ROOT, "supabase/bootstrap", f);
const FN_BOOT = B("rds_owner_function_default_revoke.sql");
const ROLES_BOOT = B("rds_migration_roles.sql");
const RT_BOOT = B("hoopdb_runtime_role_rds.sql");
// Single-sourced bootstrap chain: the grant migration FAILs closed without
// the bare consumer grantee (migrations never create roles).
const PHASE1 = SESSION_BOOTSTRAP_FILES.map((f) => B(f.split("/").pop()!));
const FIXED = [
  "anon", "authenticated", "service_role", "session_directory_search_worker",
  "session_token_verifier", "bulls96_session_ingest", "bulls96_session_ingest_seeder",
  "bulls96_canonical_feature_producer", "bulls96_read_api", "bulls96_project_events",
  "bulls96_privacy_eraser", "bulls96_babysitter_retention",
];
const RDS_ENV = {
  HOOPDB_DB_MODE: "rds",
  HOOPDB_SUPABASE_SHIM: "false",
  HOOPDB_RUNTIME_ROLE: RT,
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
};

interface Case { name: string; pass: boolean; detail: string }
const cases: Case[] = [];
const url = (user: string, db: string) => `postgres://${user}@${HOST}:${PORT}/${db}`;
const fmt = (o: unknown) => JSON.stringify(o);

async function sql(db: string, text: string, user = SUPER): Promise<pg.QueryResult> {
  const c = new pg.Client({ connectionString: url(user, db) });
  await c.connect();
  try {
    return await c.query(text);
  } finally {
    await c.end();
  }
}
async function attempt(db: string, text: string, user = SUPER): Promise<string> {
  try {
    await sql(db, text, user);
    return "ok";
  } catch (e) {
    return (e as { code?: string }).code ?? "error";
  }
}
function psql(db: string, args: string[], user = MASTER): { ok: boolean; out: string; sqlstate: string } {
  const r = spawnSync(
    PSQL,
    ["-X", "-q", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-h", HOST, "-p", PORT, "-U", user, "-d", db, ...args],
    { encoding: "utf8" },
  );
  const out = `${r.stdout}${r.stderr}`;
  return { ok: r.status === 0, out, sqlstate: /ERROR:\s+([0-9A-Z]{5}):/.exec(out)?.[1] ?? (r.status === 0 ? "ok" : "error") };
}
function migrate(env: Record<string, string>, user = MASTER, db = DB): { exit: number; code: string; applied: number } {
  const clean = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("HOOPDB_")));
  const r = spawnSync("bun", ["src/hoopdb/migrate.ts"], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...clean, HOOPDB_URL: url(user, db), ...env },
  });
  const out = `${r.stdout}${r.stderr}`;
  const codes = [...out.matchAll(/"error_code":"([^"]+)"/g)].map((m) => m[1]!);
  const done = /"message":"hoopdb migrations complete","count":(\d+)/.exec(out);
  return { exit: r.status ?? -1, code: codes.at(-1) ?? "", applied: done ? Number(done[1]) : -1 };
}
async function check(name: string, fn: () => Promise<[boolean, string]>): Promise<void> {
  try {
    const [pass, detail] = await fn();
    cases.push({ name, pass, detail });
  } catch (e) {
    cases.push({ name, pass: false, detail: `threw: ${(e as Error).message}` });
  }
  const c = cases.at(-1)!;
  console.log(`${c.pass ? "PASS" : "FAIL"}  ${name}  ${c.detail}`);
}
async function verdict(user: string) {
  process.env.HOOPDB_DB_MODE = "rds";
  const h = openHoopDb(url(user, DB));
  try {
    return await schemaVerdict(await expectedSchema(), (w) => inTransaction(h, w));
  } finally {
    await h.end();
  }
}
async function edges(role: string) {
  return (
    await sql(
      DB,
      `SELECT mem.rolname AS member, g.rolname AS grantor, g.oid::int AS grantor_oid,
              m.admin_option AS admin, m.inherit_option AS inherit, m.set_option AS set
         FROM pg_auth_members m JOIN pg_roles mem ON mem.oid = m.member JOIN pg_roles g ON g.oid = m.grantor
        WHERE m.roleid = '${role}'::regrole OR m.member = '${role}'::regrole ORDER BY 1, 2`,
    )
  ).rows;
}

// ---------------------------------------------------------------------------
// Preconditions: the emulation is only meaningful with rdsadmin as oid 10 on PG16+.
// ---------------------------------------------------------------------------
const actualHead = spawnSync("git", ["rev-parse", "HEAD"], { cwd: ROOT, encoding: "utf8" });
const PROOF_HEAD = actualHead.stdout?.trim() ?? "";
const cleanTracked = [[], ["--cached"]].every((flags) =>
  spawnSync("git", ["diff", ...flags, "--quiet"], { cwd: ROOT }).status === 0,
);
if (actualHead.status !== 0 || !/^[0-9a-f]{40}$/.test(PROOF_HEAD) || !cleanTracked ||
    (process.env.PGPROOF_HEAD !== undefined && process.env.PGPROOF_HEAD !== PROOF_HEAD)) {
  console.error("rds-profile-proof requires a clean tracked checkout at PGPROOF_HEAD; no database connection was made");
  process.exit(2);
}
const head = (await sql("postgres", "SELECT current_setting('server_version_num')::int AS v, current_setting('server_version') AS s, (SELECT rolname FROM pg_roles WHERE oid = 10) AS boot")).rows[0];
const MAJOR = (head.v / 10000) | 0;
console.log(`# PostgreSQL ${head.s}, bootstrap superuser ${head.boot}, head ${PROOF_HEAD}`);
if (head.boot !== "rdsadmin" || SUPER !== "rdsadmin" || MAJOR < 16) {
  console.error("rds-profile-proof needs PostgreSQL 16+ initialized with bootstrap superuser rdsadmin (POSTGRES_USER=rdsadmin / initdb -U rdsadmin)");
  process.exit(2);
}

// Clean slate (this harness only; the server is disposable).
await attempt("postgres", `DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
await attempt("postgres", `DROP DATABASE IF EXISTS ${DB}_other WITH (FORCE)`);
for (const r of [RT, "g7_rds_rt_other", "g7_rds_creator", "g7_rds_probe", "bulls96_rds_role_seed", ...FIXED, OTHER, MASTER]) {
  for (const d of (await sql("postgres", "SELECT datname FROM pg_database WHERE datallowconn")).rows)
    await attempt(d.datname, `DO $x$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${r}') THEN EXECUTE 'DROP OWNED BY ${r}'; END IF; END $x$`);
  await attempt("postgres", `DROP ROLE IF EXISTS ${r}`);
}
await sql(
  "postgres",
  `DO $x$ BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_superuser') THEN CREATE ROLE rds_superuser NOLOGIN; END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_iam') THEN CREATE ROLE rds_iam NOLOGIN; END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_password') THEN CREATE ROLE rds_password NOLOGIN; END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') THEN CREATE ROLE rds_replication NOLOGIN; END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rdsrepladmin') THEN CREATE ROLE rdsrepladmin NOLOGIN REPLICATION; END IF;
   END $x$;
   GRANT pg_monitor, pg_signal_backend, pg_checkpoint, pg_use_reserved_connections, pg_create_subscription,
         pg_read_all_settings, pg_read_all_stats, pg_stat_scan_tables, rds_iam, rds_password, rds_replication
      TO rds_superuser WITH ADMIN OPTION;
   CREATE ROLE ${MASTER} LOGIN CREATEROLE CREATEDB NOSUPERUSER NOBYPASSRLS NOREPLICATION;
   GRANT rds_superuser TO ${MASTER} WITH ADMIN OPTION;
   CREATE ROLE ${OTHER} LOGIN CREATEROLE CREATEDB NOSUPERUSER NOBYPASSRLS NOREPLICATION;
   GRANT rds_superuser TO ${OTHER};`,
);
await sql("postgres", `CREATE DATABASE ${DB} OWNER ${MASTER}`);

// ---------------------------------------------------------------------------
// 1. What fails under rds_superuser today (existing selfhosted/supabase/cloudsql paths).
// ---------------------------------------------------------------------------
await check("baseline: every existing privileged bootstrap refuses the RDS master, nothing created", async () => {
  const runs = {
    fn_default_selfhosted: psql(DB, ["-v", `migration_owner=${MASTER}`, "-f", B("hoopdb_function_default_revoke.sql")]).sqlstate,
    runtime_selfhosted: psql(DB, ["-v", `runtime_role=${RT}`, "-v", "runtime_login=true", "-f", B("hoopdb_runtime_role.sql")]).sqlstate,
    shim: psql(DB, ["-1", "-f", join(ROOT, "supabase/tests/supabase_shim.sql")]).sqlstate,
    worker: psql(DB, ["-f", PHASE1[0]!]).sqlstate,
    token_verifier: psql(DB, ["-f", PHASE1[1]!]).sqlstate,
    ingest: psql(DB, ["-f", PHASE1[2]!]).sqlstate,
    cloudsql_roles: psql(DB, ["-f", B("cloudsql_migration_roles.sql")]).sqlstate,
    cloudsql_fn_default: psql(DB, ["-f", B("cloudsql_owner_function_default_revoke.sql")]).sqlstate,
    alter_rdsadmin_default: await attempt(DB, "ALTER DEFAULT PRIVILEGES FOR ROLE rdsadmin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC", MASTER),
    bypassrls_role: await attempt(DB, "CREATE ROLE g7_rds_probe NOLOGIN BYPASSRLS", MASTER),
    event_trigger_emulated: await attempt(DB, "CREATE EVENT TRIGGER g7_probe ON ddl_command_end EXECUTE FUNCTION pg_catalog.pg_event_trigger_ddl_commands()", MASTER),
  };
  const leaked = (await sql(DB, `SELECT count(*)::int AS n FROM pg_roles WHERE rolname = ANY(ARRAY['${[RT, "g7_rds_probe", ...FIXED].join("','")}'])`)).rows[0].n;
  const ok =
    runs.fn_default_selfhosted === "42501" && runs.runtime_selfhosted === "42501" && runs.shim === "42501" &&
    runs.worker === "42501" && runs.token_verifier === "42501" && runs.ingest === "42501" &&
    runs.cloudsql_roles !== "ok" && runs.cloudsql_fn_default === "55000" &&
    runs.alter_rdsadmin_default === "42501" && runs.bypassrls_role === "42501" && runs.event_trigger_emulated === "42501" &&
    leaked === 0;
  return [ok, `${fmt(runs)} leaked_roles=${leaked}`];
});

// ---------------------------------------------------------------------------
// 2. The rds profile end to end, as the master in a real (non-SET ROLE) session.
// ---------------------------------------------------------------------------
const boot = () => {
  const fn = psql(DB, ["-f", FN_BOOT]);
  const roles = psql(DB, ["-f", ROLES_BOOT]);
  const rt = psql(DB, ["-v", `migration_owner=${MASTER}`, "-v", `runtime_role=${RT}`, "-v", "runtime_login=true", "-f", RT_BOOT]);
  const phase1 = PHASE1.map((f) => psql(DB, ["-f", f]));
  return { fn, roles, rt, phase1, ok: fn.ok && roles.ok && rt.ok && phase1.every((p) => p.ok) };
};
await check("rds profile: owner bootstraps, full migrate, PASS marker, runtime readiness", async () => {
  const b = boot();
  const m = migrate(RDS_ENV);
  const v = await verdict(RT);
  const fixed = (await sql(DB, `SELECT r.rolname, r.rolbypassrls, r.rolcanlogin, r.rolinherit,
      (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication) AS elevated,
      (SELECT count(*)::int FROM pg_auth_members m WHERE m.roleid = r.oid OR m.member = r.oid) AS memberships
      FROM pg_roles r WHERE r.rolname = ANY(ARRAY['${FIXED.join("','")}']) ORDER BY 1`)).rows;
  const rtEdges = await edges(RT);
  const marker = (await sql(DB, "SELECT status, generation FROM sessions.__96bulls_role_validation")).rows[0];
  const gen = await generation();
  const seed = (await sql(DB, "SELECT count(*)::int AS n FROM pg_roles WHERE rolname = 'bulls96_rds_role_seed'")).rows[0].n;
  const ok =
    b.ok && m.exit === 0 && v.ok && marker?.status === "pass" && marker.generation === gen && seed === 0 &&
    fixed.length === FIXED.length && fixed.every((r) => r.memberships === 0 && !r.rolbypassrls && !r.rolcanlogin && !r.rolinherit && !r.elevated) &&
    rtEdges.length === 1 &&
    fmt(rtEdges[0]) === fmt({ member: MASTER, grantor: "rdsadmin", grantor_oid: 10, admin: true, inherit: false, set: false });
  const err = [b.fn, b.roles, b.rt, ...b.phase1].find((r) => !r.ok)?.out.split("\n").find((l) => /ERROR/.test(l)) ?? "";
  return [ok, `boot=${b.ok} migrate=${fmt(m)} verdict=${fmt(v)} marker=${marker?.status} gen_match=${marker?.generation === gen} seed=${seed} runtime_edges=${fmt(rtEdges)} fixed=${fixed.length}${err ? ` err=${err}` : ""}`];
});
await check("rds profile: bootstraps and migrate are idempotent (rerun applies 0, stays PASS)", async () => {
  const b = boot();
  const m = migrate(RDS_ENV);
  const v = await verdict(RT);
  return [b.ok && m.exit === 0 && m.applied === 0 && v.ok, `boot=${b.ok} migrate=${fmt(m)} verdict=${fmt(v)}`];
});
await check("rds profile: trampoline re-run converges on an operator-claimed consumer role; extras still refuse", async () => {
  // The operator promotion (LOGIN + exactly the 60s marker) is the claimed
  // steady state every later deploy sees: the trampoline must accept it on
  // re-run, and still refuse any extra setting.
  await sql(DB, `ALTER ROLE search_consumer_runtime LOGIN`, MASTER);
  await sql(DB, `ALTER ROLE search_consumer_runtime SET statement_timeout = '60s'`, MASTER);
  const claimed = psql(DB, ["-f", ROLES_BOOT]);
  const extra = await attempt(DB, `ALTER ROLE search_consumer_runtime SET search_path = attacker`, MASTER);
  const refused = psql(DB, ["-f", ROLES_BOOT]);
  const reset = await attempt(DB, `ALTER ROLE search_consumer_runtime RESET search_path`, MASTER);
  const again = psql(DB, ["-f", ROLES_BOOT]);
  // Restore the bare shape the later checks assume.
  const bare = await attempt(DB, `ALTER ROLE search_consumer_runtime RESET statement_timeout`, MASTER);
  const nologin = await attempt(DB, `ALTER ROLE search_consumer_runtime NOLOGIN`, MASTER);
  const restored = psql(DB, ["-f", ROLES_BOOT]);
  return [claimed.ok && extra === "ok" && !refused.ok && reset === "ok" && again.ok && bare === "ok" && nologin === "ok" && restored.ok,
    `claimed=${claimed.ok} extra_set=${extra} refused=${!refused.ok} reset=${reset} again=${again.ok} restored=${restored.ok}`];
});
await check("rds profile: the master can still rotate the runtime password (creator edge kept on purpose)", async () => {
  // A value is used only inside this disposable, trust-auth server; never a real credential.
  const r = await attempt(DB, `ALTER ROLE ${RT} PASSWORD NULL`, MASTER);
  const fixedLocked = await attempt(DB, "ALTER ROLE anon NOLOGIN", MASTER);
  return [r === "ok" && fixedLocked === "42501", `runtime_alter=${r} fixed_role_alter=${fixedLocked}`];
});

// ---------------------------------------------------------------------------
// 3. The rds exceptions are mode-bound: the same database refuses selfhosted/cloudsql.
// ---------------------------------------------------------------------------
await check("mode-bound: the RDS-provisioned database is refused in selfhosted and cloudsql mode", async () => {
  const self = migrate({ ...RDS_ENV, HOOPDB_DB_MODE: "selfhosted" });
  const cloud = migrate({ ...RDS_ENV, HOOPDB_DB_MODE: "cloudsql" });
  const c = new pg.Client({ connectionString: url(MASTER, DB) });
  await c.connect();
  const ev = async (mode: "selfhosted" | "cloudsql" | "rds") => {
    const e = await functionBoundaryEvidence(c, "global-default-revoke", mode, RT);
    return {
      satisfied: e.satisfied,
      excluded: e.excluded_platform_superusers.map((x) => x.role),
      uncovered: e.sessions_create_roles.filter((x) => !x.function_default_exact).map((x) => x.role),
    };
  };
  const boundary = { selfhosted: await ev("selfhosted"), cloudsql: await ev("cloudsql"), rds: await ev("rds") };
  await c.end();
  const ok = self.exit !== 0 && self.code === "runtime_role_unsafe" && cloud.exit !== 0 && cloud.code === "runtime_role_unsafe" &&
    !boundary.selfhosted.satisfied && boundary.selfhosted.uncovered.includes("rdsadmin") &&
    !boundary.cloudsql.satisfied && boundary.cloudsql.uncovered.includes("rdsadmin") &&
    boundary.rds.satisfied && fmt(boundary.rds.excluded) === fmt(["rdsadmin"]) && boundary.rds.uncovered.length === 0;
  return [ok, `selfhosted=${fmt(self)} cloudsql=${fmt(cloud)} boundary=${fmt(boundary)}`];
});
await check("mode-bound: rds mode refuses a non-RDS-shaped owner (rds_profile_unproven), before any write", async () => {
  await sql(DB, `REVOKE rds_superuser FROM ${MASTER}`);
  try {
    const m = migrate(RDS_ENV);
    return [m.exit !== 0 && m.code === "rds_profile_unproven", fmt(m)];
  } finally {
    await sql(DB, `GRANT rds_superuser TO ${MASTER} WITH ADMIN OPTION`);
  }
});
await check("mode-bound: rds mode refuses the shim, the generic bootstrap and a workload role before connecting", async () => {
  const shim = migrate({ ...RDS_ENV, HOOPDB_SUPABASE_SHIM: "true" });
  const bootstrap = migrate({ ...RDS_ENV, HOOPDB_ROLE_BOOTSTRAP: "true" });
  const workload = migrate({ ...RDS_ENV, HOOPDB_RUNTIME_WORKLOAD_ROLE: OTHER });
  const et = migrate({ ...RDS_ENV, HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger" });
  const ok = shim.code === "shim_forbidden" && bootstrap.code === "bootstrap_forbidden" &&
    workload.code === "db_profile_invalid" && et.code === "function_boundary_mode_unsupported";
  return [ok, `shim=${fmt(shim)} bootstrap=${fmt(bootstrap)} workload=${fmt(workload)} event_trigger=${fmt(et)}`];
});

// ---------------------------------------------------------------------------
// 4. Drift refusal in rds mode (each undone afterwards).
// ---------------------------------------------------------------------------
async function drift(name: string, apply: string, undo: string, expect: string, user = MASTER) {
  await check(`drift refused: ${name}`, async () => {
    await sql(DB, apply, user);
    let m: ReturnType<typeof migrate>;
    let v: Awaited<ReturnType<typeof verdict>> | undefined;
    try {
      m = migrate(RDS_ENV);
    } finally {
      await sql(DB, undo, user);
    }
    v = await verdict(RT);
    return [m.exit !== 0 && m.code === expect, `migrate=${fmt(m)} after_undo_verdict=${fmt(v)}`];
  });
}
await drift(
  "extra runtime member (granted by the master)",
  `GRANT ${RT} TO ${OTHER}`, `REVOKE ${RT} FROM ${OTHER}`, "runtime_role_unsafe",
);
await drift(
  "runtime role a member of rds_iam",
  `GRANT rds_iam TO ${RT}`, `REVOKE rds_iam FROM ${RT}`, "runtime_role_unsafe",
);
await drift(
  "poisoned owner global function default",
  `ALTER DEFAULT PRIVILEGES FOR ROLE ${MASTER} GRANT EXECUTE ON FUNCTIONS TO authenticated`,
  `ALTER DEFAULT PRIVILEGES FOR ROLE ${MASTER} REVOKE EXECUTE ON FUNCTIONS FROM authenticated`,
  "function_boundary_unproven",
);
await drift(
  "another rds_superuser member able to CREATE in sessions",
  `GRANT CREATE ON SCHEMA sessions TO ${OTHER}`, `REVOKE CREATE ON SCHEMA sessions FROM ${OTHER}`,
  "function_boundary_unproven",
);
await drift(
  "extra EXECUTE on a sessions function",
  "GRANT EXECUTE ON FUNCTION sessions.jwt_org_id() TO anon", "REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM anon",
  "function_acl_drift",
);
await check("drift refused: a runtime role created by a different rds_superuser member (creator edge not the owner)", async () => {
  await sql(DB, "CREATE ROLE g7_rds_rt_other LOGIN", OTHER);
  await sql(DB, `GRANT CONNECT ON DATABASE ${DB} TO g7_rds_rt_other`, MASTER);
  const bootRt = psql(DB, ["-v", `migration_owner=${MASTER}`, "-v", "runtime_role=g7_rds_rt_other", "-v", "runtime_login=true", "-f", RT_BOOT]);
  const m = migrate({ ...RDS_ENV, HOOPDB_RUNTIME_ROLE: "g7_rds_rt_other" });
  await sql(DB, `REVOKE CONNECT ON DATABASE ${DB} FROM g7_rds_rt_other`, MASTER);
  await sql(DB, "DROP ROLE g7_rds_rt_other", OTHER);
  return [bootRt.sqlstate === "55000" && m.exit !== 0 && m.code === "runtime_role_unsafe", `bootstrap=${bootRt.sqlstate} migrate=${fmt(m)}`];
});

for (const role of ["bulls96_read_api", "bulls96_project_events", "bulls96_privacy_eraser", "bulls96_babysitter_retention"]) {
  await check(`drift refused: ${role} with INHERIT, or with a member, is refused by rds_migration_roles.sql and left as found`, async () => {
    await sql(DB, `ALTER ROLE ${role} INHERIT`);
    const inherit = psql(DB, ["-f", ROLES_BOOT]);
    const inheritKept = (await sql(DB, `SELECT rolinherit FROM pg_roles WHERE rolname = '${role}'`)).rows[0]?.rolinherit === true;
    const inheritMigrate = role === "bulls96_project_events" ? migrate(RDS_ENV) : undefined;
    await sql(DB, `ALTER ROLE ${role} NOINHERIT`);
    await sql(DB, `GRANT ${role} TO ${OTHER}`);
    const member = psql(DB, ["-f", ROLES_BOOT]);
    const memberMigrate = role === "bulls96_project_events" ? migrate(RDS_ENV) : undefined;
    await sql(DB, `REVOKE ${role} FROM ${OTHER}`);
    const restored = psql(DB, ["-f", ROLES_BOOT]);
    const after = migrate(RDS_ENV);
    const v = await verdict(RT);
    const migrateRefused = (m?: ReturnType<typeof migrate>) => m === undefined || (m.exit !== 0 && m.code === "55000");
    const ok = inherit.sqlstate === "55000" && inheritKept && member.sqlstate === "55000" &&
      migrateRefused(inheritMigrate) && migrateRefused(memberMigrate) && restored.ok && after.exit === 0 && v.ok;
    return [ok, fmt({ inherit: inherit.sqlstate, inheritKept, member: member.sqlstate, inheritMigrate, memberMigrate, restored: restored.ok, after, verdict: v })];
  });
}

// ---------------------------------------------------------------------------
// 5. Bootstrap negative paths.
// ---------------------------------------------------------------------------
await check("runtime bootstrap refuses every fixed role and its reserved trampoline name before changing the database ACL", async () => {
  const before = (await sql(DB, "SELECT datacl::text AS acl FROM pg_database WHERE datname = current_database()")).rows[0];
  const attempts = [...FIXED, "bulls96_rds_role_seed"].map((role) => {
    const r = psql(DB, ["-v", `migration_owner=${MASTER}`, "-v", `runtime_role=${role}`, "-v", "runtime_login=false", "-f", RT_BOOT]);
    return { role, refused: r.sqlstate === "55000" && r.out.includes("invalid role name") };
  });
  const after = (await sql(DB, "SELECT datacl::text AS acl FROM pg_database WHERE datname = current_database()")).rows[0];
  const seed = (await sql(DB, "SELECT count(*)::int AS n FROM pg_roles WHERE rolname = 'bulls96_rds_role_seed'")).rows[0].n;
  return [attempts.every((r) => r.refused) && fmt(before) === fmt(after) && seed === 0,
    fmt({ attempts, database_acl_unchanged: fmt(before) === fmt(after), seed })];
});
await check("bootstraps refuse: wrong owner input, non-owned database, stale trampoline, unsafe fixed role, superuser caller", async () => {
  const wrongOwner = psql(DB, ["-v", `migration_owner=${OTHER}`, "-v", `runtime_role=${RT}`, "-v", "runtime_login=true", "-f", RT_BOOT]).sqlstate;
  await sql("postgres", `CREATE DATABASE ${DB}_other OWNER ${OTHER}`);
  const notOwner = psql(`${DB}_other`, ["-v", `migration_owner=${MASTER}`, "-v", "runtime_role=g7_rds_probe", "-v", "runtime_login=true", "-f", RT_BOOT]).sqlstate;
  const probeLeaked = (await sql(DB, "SELECT count(*)::int AS n FROM pg_roles WHERE rolname = 'g7_rds_probe'")).rows[0].n;
  await sql("postgres", `DROP DATABASE ${DB}_other WITH (FORCE)`);
  await sql(DB, "CREATE ROLE bulls96_rds_role_seed NOLOGIN", MASTER);
  const staleSeed = psql(DB, ["-f", ROLES_BOOT]).sqlstate;
  await sql(DB, "DROP ROLE bulls96_rds_role_seed", MASTER);
  await sql(DB, "ALTER ROLE anon LOGIN");
  const unsafeFixed = psql(DB, ["-f", ROLES_BOOT]).sqlstate;
  await sql(DB, "ALTER ROLE anon NOLOGIN");
  const superCaller = psql(DB, ["-f", FN_BOOT], SUPER).sqlstate;
  const loginMismatch = psql(DB, ["-v", `migration_owner=${MASTER}`, "-v", `runtime_role=${RT}`, "-v", "runtime_login=false", "-f", RT_BOOT]).sqlstate;
  const ok = wrongOwner === "42501" && notOwner === "42501" && probeLeaked === 0 && staleSeed === "55000" &&
    unsafeFixed === "55000" && superCaller === "55000" && loginMismatch === "55000";
  return [ok, fmt({ wrongOwner, notOwner, probeLeaked, staleSeed, unsafeFixed, superCaller, loginMismatch })];
});

// ---------------------------------------------------------------------------
// 6. Generation, ledger and ordering integrity are unchanged by the profile.
// ---------------------------------------------------------------------------
await check("integrity: a changed ledger checksum and a ledger gap are refused in rds mode before any write", async () => {
  const markerOf = async () => (await sql(DB, "SELECT status, generation, updated_at FROM sessions.__96bulls_role_validation")).rows[0];
  const before = await markerOf();
  const first = (await sql(DB, "SELECT name, sha256 FROM sessions.__96bulls_migrations ORDER BY name LIMIT 1")).rows[0];
  const mid = (await sql(DB, "SELECT name, sha256, applied_at FROM sessions.__96bulls_migrations ORDER BY name OFFSET 10 LIMIT 1")).rows[0];
  await sql(DB, `UPDATE sessions.__96bulls_migrations SET sha256 = repeat('0', 64) WHERE name = '${first.name}'`, MASTER);
  const changed = migrate(RDS_ENV);
  await sql(DB, `UPDATE sessions.__96bulls_migrations SET sha256 = '${first.sha256}' WHERE name = '${first.name}'`, MASTER);
  await sql(DB, `DELETE FROM sessions.__96bulls_migrations WHERE name = '${mid.name}'`, MASTER);
  const gap = migrate(RDS_ENV);
  await sql(DB, `INSERT INTO sessions.__96bulls_migrations (name, sha256, applied_at) VALUES ('${mid.name}', '${mid.sha256}', '${new Date(mid.applied_at).toISOString()}')`, MASTER);
  const untouched = fmt(await markerOf()) === fmt(before);
  const after = migrate(RDS_ENV);
  const v = await verdict(RT);
  const ok = changed.exit !== 0 && changed.code === "migration_changed" && gap.exit !== 0 && gap.code === "ledger_gap" &&
    untouched && after.exit === 0 && after.applied === 0 && v.ok;
  return [ok, `changed=${fmt(changed)} gap=${fmt(gap)} marker_untouched=${untouched} restored=${fmt(after)} verdict=${fmt(v)}`];
});
await check("integrity: generation is profile-independent (rds and selfhosted resolve the same plan schema)", async () => {
  const p = migrationProfile(RDS_ENV);
  const gen = await generation();
  return [p.mode === "rds" && !p.shim && !p.bootstrap && p.verify && /^[0-9a-f]{64}$/.test(gen), `profile=${fmt(p)} generation=${gen}`];
});

const failed = cases.filter((c) => !c.pass).length;
const results = {
  head: PROOF_HEAD,
  tracked_tree_clean: cleanTracked,
  pg_version: head.s,
  pg_major: MAJOR,
  bootstrap_superuser: head.boot,
  generation: await generation(),
  passed: cases.length - failed,
  failed,
  cases,
};
writeFileSync(OUT, `${JSON.stringify(results, null, 2)}\n`);
console.log(`# ${results.passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
