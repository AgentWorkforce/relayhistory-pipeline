/**
 * HOOPDB_RDS_LAYOUT unset (the legacy master-owner preflight) against a database a
 * dedicated, non-admin owner holds: what a later plain `helm upgrade` produces when it
 * drops configure-rds.sh's `rdsLayout: separate-database` overlay. The refusal keeps
 * error_code rds_profile_unproven and names the value to restore; every other
 * rds_profile_unproven message is unchanged. Runs the real preflight query against real
 * PGlite catalogs, and the real runMigrations path for the profile wiring.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { MIGRATION_LEDGER, migrationProfile, operatorLine, rdsOwnerPreflight, runMigrations, type MigrationClient } from "../migrate.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((d) => d.close().catch(() => undefined)));
});
const client = (db: PGlite): MigrationClient => ({
  async query<R>(text: string, params?: unknown[]) {
    return (await db.query<R>(text, params as never)) as { rows: R[] };
  },
});

/** PGlite's database is not named hoopdb, so the line must name the real one. */
const lineFor = (dbname: string) =>
  `HOOPDB_RDS_LAYOUT is unset but database ${JSON.stringify(dbname)} and its sessions schema are owned by "hoopdb_owner" (a dedicated owner): set sessions-pipeline.worker.migrations.rdsLayout: separate-database in your values file (scripts/selfhost/configure-rds.sh writes it) and run helm upgrade again`;
const dbName = async (db: PGlite) => (await db.query<{ d: string }>("SELECT current_database() AS d")).rows[0]!.d;

/** The separate-database layout configure-rds.sh builds: hoopdb_owner owns the database and sessions. */
async function separateLayoutDb(mutate = "", login = "hoopdb_owner"): Promise<PGlite> {
  const db = new PGlite();
  open.push(db);
  const dbname = (await db.query<{ d: string }>("SELECT current_database() AS d")).rows[0]!.d;
  await db.exec(`CREATE ROLE rdsadmin SUPERUSER NOLOGIN;
    CREATE ROLE rds_superuser NOLOGIN;
    CREATE ROLE hoopdb_owner LOGIN NOINHERIT;
    CREATE ROLE master LOGIN CREATEROLE;
    GRANT rds_superuser TO master;
    CREATE ROLE sessions_pipeline_runtime LOGIN NOINHERIT;
    ALTER DATABASE ${dbname} OWNER TO hoopdb_owner;
    CREATE SCHEMA sessions AUTHORIZATION hoopdb_owner;
    ${mutate.replaceAll("$DB", dbname)}`);
  await db.exec(`SET SESSION AUTHORIZATION ${login}`);
  return db;
}

async function refusal(work: Promise<unknown>): Promise<{ code?: string; message: string; line?: string }> {
  try {
    await work;
  } catch (e) {
    return { code: (e as { code?: string }).code, message: (e as Error).message, line: operatorLine(e) };
  }
  throw new Error("expected a refusal");
}

const RDS_ENV = {
  HOOPDB_DB_MODE: "rds",
  HOOPDB_RUNTIME_ROLE: "sessions_pipeline_runtime",
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
};

describe("migrator: HOOPDB_RDS_LAYOUT unset with a dedicated database owner", () => {
  test("the profile records only an unset layout; explicit values carry no marker", () => {
    expect(migrationProfile(RDS_ENV)).toMatchObject({ rdsLayout: "master-owner", rdsLayoutDefaulted: true });
    expect(migrationProfile({ ...RDS_ENV, HOOPDB_RDS_LAYOUT: "" }).rdsLayoutDefaulted).toBe(true);
    expect(migrationProfile({ ...RDS_ENV, HOOPDB_RDS_LAYOUT: "master-owner" })).not.toHaveProperty("rdsLayoutDefaulted");
    expect(migrationProfile({ ...RDS_ENV, HOOPDB_RDS_LAYOUT: "separate-database" })).not.toHaveProperty("rdsLayoutDefaulted");
  });

  test("runMigrations names the dropped overlay, keeps error_code rds_profile_unproven, writes nothing", async () => {
    const db = await separateLayoutDb();
    const LINE = lineFor(await dbName(db));
    const r = await refusal(runMigrations(client(db), { profile: migrationProfile(RDS_ENV) }));
    expect(r).toEqual({ code: "rds_profile_unproven", message: `96bulls: ${LINE}`, line: LINE });
    expect(r.message).not.toMatch(/postgres(ql)?:\/\/|password/i);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [MIGRATION_LEDGER])).rows[0]!.r).toBeNull();
  });

  test("the preflight alone, logged in as the owner or as another plain login", async () => {
    const owner = await separateLayoutDb();
    const LINE = lineFor(await dbName(owner));
    expect(await refusal(rdsOwnerPreflight(client(owner), true))).toEqual({
      code: "rds_profile_unproven", message: `96bulls: ${LINE}`, line: LINE,
    });
    // The URL logs in as some other plain role: the database is still the dedicated owner's.
    const other = await separateLayoutDb("CREATE ROLE some_login LOGIN", "some_login");
    expect((await refusal(rdsOwnerPreflight(client(other), true))).line).toBe(LINE);
  });

  test("the line names the real database, never a hardcoded hoopdb", async () => {
    const db = await separateLayoutDb();
    const name = await dbName(db);
    expect(name).not.toBe("hoopdb");
    const r = await refusal(rdsOwnerPreflight(client(db), true));
    expect(r.line).toContain(`database ${JSON.stringify(name)} and its sessions schema`);
    expect(r.line).not.toContain("hoopdb is owned");
  });

  const LEGACY = /^96bulls: rds mode needs PostgreSQL 16\+, a non-superuser CREATEROLE rds_superuser member as migration owner and the fixed rdsadmin SUPERUSER: \{"owner_super":(true|false),"owner_createrole":(true|false),"owner_rds_superuser":(true|false),"server_version_num":\d+,"rdsadmin_super":(true|false)\}; nothing was written$/;
  const cases: [string, string, string, boolean][] = [
    ["HOOPDB_RDS_LAYOUT=master-owner set explicitly", "", "hoopdb_owner", false],
    ["the database owned by the RDS master", "ALTER DATABASE $DB OWNER TO master", "hoopdb_owner", true],
    ["the database owned by a CREATEROLE owner", "ALTER ROLE hoopdb_owner CREATEROLE", "hoopdb_owner", true],
    ["the database owned by an rds_superuser member", "GRANT rds_superuser TO hoopdb_owner", "hoopdb_owner", true],
    ["the master login failing another proof (rdsadmin not SUPERUSER)", "ALTER ROLE rdsadmin NOSUPERUSER", "master", true],
    // Unproven layout evidence: no overlay advice without the RDS shape AND sessions ownership.
    ["no RDS shape (rdsadmin absent)", "DROP ROLE rdsadmin", "hoopdb_owner", true],
    ["rdsadmin present but not SUPERUSER", "ALTER ROLE rdsadmin NOSUPERUSER", "hoopdb_owner", true],
    ["no sessions schema", "DROP SCHEMA sessions", "hoopdb_owner", true],
    ["sessions owned by someone other than the database owner", "ALTER SCHEMA sessions OWNER TO master", "hoopdb_owner", true],
    ["neither the RDS shape nor sessions ownership", "DROP ROLE rdsadmin; DROP SCHEMA sessions", "hoopdb_owner", true],
  ];
  for (const [name, mutate, login, defaulted] of cases)
    test(`other rds_profile_unproven cases keep the old message: ${name}`, async () => {
      const r = await refusal(rdsOwnerPreflight(client(await separateLayoutDb(mutate, login)), defaulted));
      expect(r.code).toBe("rds_profile_unproven");
      expect(r.message).toMatch(LEGACY);
      expect(r.line).toBeUndefined();
    });

  test("runMigrations, default RDS layout, without rdsadmin or sessions: the original refusal, no helm advice", async () => {
    const db = await separateLayoutDb("DROP ROLE rdsadmin; DROP SCHEMA sessions");
    const r = await refusal(runMigrations(client(db), { profile: migrationProfile(RDS_ENV) }));
    expect(r.code).toBe("rds_profile_unproven");
    expect(r.message).toMatch(LEGACY);
    expect(r.line).toBeUndefined();
    expect(r.message).not.toMatch(/separate-database|helm upgrade/);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [MIGRATION_LEDGER])).rows[0]!.r).toBeNull();
  });

  test("operatorLine ignores anything but a non-empty string", () => {
    expect(operatorLine(new Error("x"))).toBeUndefined();
    expect(operatorLine(Object.assign(new Error("x"), { operatorLine: 7 }))).toBeUndefined();
    expect(operatorLine(Object.assign(new Error("x"), { operatorLine: "" }))).toBeUndefined();
    expect(operatorLine(null)).toBeUndefined();
  });
});
