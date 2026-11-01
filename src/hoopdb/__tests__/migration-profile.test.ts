/**
 * The migrator's database-mode contract, its runtime-role grant and its single-authority
 * guard, on in-process PGlite plus the pure profile resolver.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { schemaVerdict, type Transact } from "../../shared/db.ts";
import {
  inventoryOf,
  sessionsFunctionAcl,
  SESSIONS_FUNCTION_EVENT_TRIGGER,
} from "../migrate-guards.ts";
import { gateOpen, workerTick } from "../../shared/worker.ts";
import {
  MIGRATION_SENTINELS,
  NON_CREATE_STEPS,
} from "../migration-sentinels.ts";
import {
  expectedSchema,
  migrate,
  migrationProfile,
  plan,
  runMigrations,
  type MigrationClient,
  type MigrationProfile,
} from "../migrate.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
/** What supabase/bootstrap/hoopdb_function_default_revoke.sql does for the PGlite owner (postgres). */
const OWNER_FN_DEFAULT_REVOKE =
  "ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC";
/** What the selfhosted runtime-role bootstrap does to the database ACL. */
const NO_PUBLIC_TEMP =
  "DO $t$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()); END $t$";
const LOCAL: MigrationProfile = {
  mode: "local",
  shim: true,
  bootstrap: true,
  verify: true,
  runtimeRole: "postgres",
};

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  return db;
}
function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length)
        return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const transact =
  (db: PGlite): Transact =>
  (work) =>
    db.transaction((tx) => work(tx as never)) as never;
const failure = async (run: Promise<unknown>) =>
  (await run.then(
    () => null,
    (caught: unknown) => caught,
  )) as { code?: string; message?: string } | null;
/** The ledger and the marker exactly as stored, or null when the table is absent. */
async function snapshot(db: PGlite) {
  const read = async (sql: string) =>
    (
      await db.query<{ t: string | null }>(
        `SELECT to_regclass('${sql}')::text AS t`,
      )
    ).rows[0]?.t
      ? (await db.query(`SELECT * FROM ${sql} ORDER BY 1`)).rows
      : null;
  return {
    ledger: await read("sessions.__96bulls_migrations"),
    marker: await read("sessions.__96bulls_role_validation"),
  };
}

describe("HOOPDB_DB_MODE profile (no default, no host inference)", () => {
  const base = {
    HOOPDB_RUNTIME_ROLE: "hoop_runtime",
    HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  };
  const cases: Array<[string, Record<string, string>, string]> = [
    ["no mode", { ...base, HOOPDB_SUPABASE_SHIM: "true" }, "db_mode_invalid"],
    [
      "an unknown mode",
      { ...base, HOOPDB_DB_MODE: "cloud", HOOPDB_SUPABASE_SHIM: "true" },
      "db_mode_invalid",
    ],
    [
      "supabase with the shim forced on",
      { ...base, HOOPDB_DB_MODE: "supabase", HOOPDB_SUPABASE_SHIM: "true" },
      "shim_forbidden",
    ],
    [
      "supabase with the bootstrap forced on",
      { ...base, HOOPDB_DB_MODE: "supabase", HOOPDB_ROLE_BOOTSTRAP: "1" },
      "bootstrap_forbidden",
    ],
    [
      "local without an explicit shim choice",
      { ...base, HOOPDB_DB_MODE: "local" },
      "shim_unset",
    ],
    [
      "selfhosted without an explicit shim choice",
      { ...base, HOOPDB_DB_MODE: "selfhosted" },
      "shim_unset",
    ],
    [
      "a non-boolean flag",
      { ...base, HOOPDB_DB_MODE: "local", HOOPDB_SUPABASE_SHIM: "yes" },
      "db_profile_invalid",
    ],
    [
      "no runtime role",
      { HOOPDB_DB_MODE: "local", HOOPDB_SUPABASE_SHIM: "true" },
      "runtime_role_unset",
    ],
  ];
  for (const [name, env, code] of cases)
    test(`refuses ${name} (${code})`, () => {
      expect(() => migrationProfile(env)).toThrow(
        expect.objectContaining({ code }),
      );
    });

  test("resolves explicit profiles; supabase never runs the shim or the bootstrap", () => {
    expect(
      migrationProfile({ ...base, HOOPDB_DB_MODE: "supabase" }),
    ).toMatchObject({
      mode: "supabase",
      shim: false,
      bootstrap: false,
      verify: true,
      runtimeRole: "hoop_runtime",
    });
    expect(
      migrationProfile({
        ...base,
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_SUPABASE_SHIM: "false",
      }).shim,
    ).toBe(false);
    expect(
      migrationProfile({
        ...base,
        HOOPDB_DB_MODE: "local",
        HOOPDB_SUPABASE_SHIM: "true",
      }),
    ).toMatchObject({
      shim: true,
      bootstrap: true,
    });
    expect(
      migrationProfile({
        ...base,
        HOOPDB_DB_MODE: "selfhosted",
        HOOPDB_SUPABASE_SHIM: "true",
        HOOPDB_ROLE_BOOTSTRAP: "false",
      }),
    ).toMatchObject({ shim: true, bootstrap: false });
  });

  test("migrate() refuses supabase+shim before opening any connection", async () => {
    const saved = { ...process.env };
    try {
      Object.assign(process.env, {
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_SUPABASE_SHIM: "true",
        HOOPDB_RUNTIME_ROLE: "hoop_runtime",
        // Nothing listens here: reaching the network would fail differently.
        HOOPDB_URL: "postgres://nobody@127.0.0.1:1/none",
      });
      const error = await failure(migrate());
      expect(error?.code).toBe("shim_forbidden");
      delete process.env.HOOPDB_DB_MODE;
      expect((await failure(migrate()))?.code).toBe("db_mode_invalid");
    } finally {
      for (const k of Object.keys(process.env))
        if (!(k in saved)) delete process.env[k];
      Object.assign(process.env, saved);
    }
  });
});

describe("runtime role: required, existing, SELECT only", () => {
  test("unset: the migrator fails before any write; ledger and marker unchanged", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const before = await snapshot(db);
    const saved = { ...process.env };
    try {
      Object.assign(process.env, {
        HOOPDB_DB_MODE: "local",
        HOOPDB_SUPABASE_SHIM: "true",
      });
      delete process.env.HOOPDB_RUNTIME_ROLE;
      expect((await failure(runMigrations(client(db))))?.code).toBe(
        "runtime_role_unset",
      );
    } finally {
      for (const k of Object.keys(process.env))
        if (!(k in saved)) delete process.env[k];
      Object.assign(process.env, saved);
    }
    expect(await snapshot(db)).toEqual(before);
  });

  test("nonexistent: fails before any write, on a fresh database (nothing created) and a migrated one (unchanged)", async () => {
    const empty = await fresh();
    const error = await failure(
      runMigrations(client(empty), {
        profile: { ...LOCAL, runtimeRole: "g6_nope" },
      }),
    );
    expect(error?.code).toBe("runtime_role_missing");
    expect(await snapshot(empty)).toEqual({ ledger: null, marker: null });
    expect(
      (
        await empty.query(
          "SELECT 1 FROM pg_namespace WHERE nspname = 'sessions'",
        )
      ).rows,
    ).toEqual([]);
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const before = await snapshot(db);
    expect(
      (
        await failure(
          runMigrations(client(db), {
            profile: { ...LOCAL, runtimeRole: "g6_nope" },
          }),
        )
      )?.code,
    ).toBe("runtime_role_missing");
    expect(await snapshot(db)).toEqual(before);
  });

  test("unsafe runtime roles are refused before any write: superuser, BYPASSRLS, the search Worker, and the owner in supabase mode", async () => {
    const db = await fresh();
    await db.exec(
      "CREATE ROLE g6_super SUPERUSER NOLOGIN; CREATE ROLE g6_bypass BYPASSRLS NOLOGIN;",
    );
    for (const role of ["g6_super", "g6_bypass"])
      expect(
        (
          await failure(
            runMigrations(client(db), {
              profile: { ...LOCAL, runtimeRole: role },
            }),
          )
        )?.code,
      ).toBe("runtime_role_unsafe");
    await runMigrations(client(db), { profile: LOCAL }); // creates the Worker via the bootstrap
    const before = await snapshot(db);
    expect(
      (
        await failure(
          runMigrations(client(db), {
            profile: {
              ...LOCAL,
              runtimeRole: "session_directory_search_worker",
            },
          }),
        )
      )?.code,
    ).toBe("runtime_role_unsafe");
    const supabaseOwner: MigrationProfile = {
      mode: "supabase",
      shim: false,
      bootstrap: false,
      verify: true,
      runtimeRole: "postgres",
    };
    expect(
      (await failure(runMigrations(client(db), { profile: supabaseOwner })))
        ?.code,
    ).toBe("runtime_role_unsafe");
    expect(await snapshot(db)).toEqual(before);
  });

  test("a dedicated runtime role gets SELECT on the ledger and marker only: no DML, no DDL, owns nothing, NOBYPASSRLS, and the live gate passes as it", async () => {
    const db = await fresh();
    await db.exec(`CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}`);
    await runMigrations(client(db), {
      profile: { ...LOCAL, runtimeRole: "g6_rt", runtimeLogin: false },
    });
    await runMigrations(client(db), {
      profile: { ...LOCAL, runtimeRole: "g6_rt", runtimeLogin: false },
    }); // idempotent
    const tables = [
      "sessions.__96bulls_migrations",
      "sessions.__96bulls_role_validation",
    ];
    for (const t of tables) {
      const p = (
        await db.query<Record<string, boolean>>(
          `SELECT has_table_privilege('g6_rt', $1, 'SELECT') AS s, has_table_privilege('g6_rt', $1, 'INSERT') AS i,
                  has_table_privilege('g6_rt', $1, 'UPDATE') AS u, has_table_privilege('g6_rt', $1, 'DELETE') AS d,
                  has_table_privilege('g6_rt', $1, 'TRUNCATE') AS t, has_table_privilege('g6_rt', $1, 'REFERENCES') AS r,
                  has_table_privilege('g6_rt', $1, 'TRIGGER') AS g`,
          [t],
        )
      ).rows[0];
      expect(p).toEqual({
        s: true,
        i: false,
        u: false,
        d: false,
        t: false,
        r: false,
        g: false,
      });
    }
    const role = (
      await db.query<Record<string, unknown>>(
        `SELECT rolsuper, rolbypassrls, rolcreaterole, rolcreatedb,
                has_schema_privilege('g6_rt', 'sessions', 'CREATE') AS schema_create,
                (SELECT count(*)::int FROM pg_class WHERE relowner = r.oid) AS owned_relations,
                (SELECT count(*)::int FROM pg_shdepend WHERE refobjid = r.oid AND deptype = 'o') AS owned
           FROM pg_roles r WHERE rolname = 'g6_rt'`,
      )
    ).rows[0];
    expect(role).toEqual({
      rolsuper: false,
      rolbypassrls: false,
      rolcreaterole: false,
      rolcreatedb: false,
      schema_create: false,
      owned_relations: 0,
      owned: 0,
    });
    await db.exec("SET ROLE g6_rt");
    try {
      expect(
        (await db.query("SELECT count(*) FROM sessions.__96bulls_migrations"))
          .rows,
      ).toHaveLength(1);
      for (const sql of [
        "INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ('x', 'x')",
        "UPDATE sessions.__96bulls_role_validation SET status = 'pass'",
        "DELETE FROM sessions.__96bulls_migrations",
        "TRUNCATE sessions.__96bulls_role_validation",
        "ALTER TABLE sessions.__96bulls_migrations ADD COLUMN x int",
        "DROP TABLE sessions.__96bulls_role_validation",
        "CREATE TABLE sessions.g6_rt_owned (x int)",
      ])
        expect((await failure(db.exec(sql)))?.code, sql).toBe("42501");
      expect(await schemaVerdict(await expectedSchema(), transact(db))).toEqual(
        { ok: true },
      );
    } finally {
      await db.exec("RESET ROLE");
    }
  });
});

describe("one migration authority: never import or mark an unledgered backlog", () => {
  test("supabase db push history naming these migrations: refused before any write", async () => {
    const db = await fresh();
    await db.exec(`CREATE SCHEMA supabase_migrations;
      CREATE TABLE supabase_migrations.schema_migrations (version text PRIMARY KEY);
      INSERT INTO supabase_migrations.schema_migrations VALUES ('20261025000000');`);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("mixed_migration_authority");
    expect(error?.message).toContain("20261025000000");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
    // Unrelated CLI history is not ours to judge.
    await db.exec(
      "DELETE FROM supabase_migrations.schema_migrations; INSERT INTO supabase_migrations.schema_migrations VALUES ('20200101000000')",
    );
    expect(
      await failure(runMigrations(client(db), { profile: LOCAL })),
    ).toBeNull();
  });

  test("a sessions schema applied without the ledger: refused, nothing imported", async () => {
    const db = await fresh();
    await db.exec(
      readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
    );
    await db.exec(
      readFileSync(
        join(root, "supabase/migrations/20261025000000_sessions_index.sql"),
        "utf8",
      ),
    );
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("unledgered_schema");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
  });

  test("a ledger with a gap: refused, nothing marked, marker unchanged", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      "DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261030010000_session_directory.sql'",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("ledger_gap");
    expect(error?.message).toContain(
      "upstream:20261030010000_session_directory.sql",
    );
    expect(await snapshot(db)).toEqual(before);
  });
});

test("the runbook's 20261030020000 checksum is the file's real sha256", () => {
  const file = readFileSync(
    join(
      root,
      "supabase/migrations/20261030020000_session_directory_short_summary.sql",
    ),
  );
  const sha = createHash("sha256").update(file).digest("hex");
  expect(
    readFileSync(
      join(root, "runbooks/session-directory-search-worker-bootstrap.md"),
      "utf8",
    ),
  ).toContain(sha);
});

describe("runtime role: the exact least-privilege allowlist (hostile roles refused before any write)", () => {
  const GOOD: MigrationProfile = {
    ...LOCAL,
    mode: "selfhosted",
    runtimeRole: "g6_rt",
    runtimeLogin: false,
    functionBoundary: "global-default-revoke",
  };
  /** Ledger, marker and every grant on the approved objects. */
  const state = async (db: PGlite) => ({
    ...(await snapshot(db)),
    acl: (
      await db.query(
        `SELECT c.relname AS obj, c.relacl::text AS acl FROM pg_class c WHERE c.relname IN ('__96bulls_migrations', '__96bulls_role_validation')
         UNION ALL SELECT 'schema sessions', nspacl::text FROM pg_namespace WHERE nspname = 'sessions' ORDER BY 1`,
      )
    ).rows,
  });
  /** A database migrated with a good dedicated role, so hostile variants run against the full schema. */
  async function migrated(): Promise<PGlite> {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    await runMigrations(client(db), { profile: { ...GOOD, mode: "local" } });
    return db;
  }
  const HOSTILE: Array<[string, string, string, Partial<MigrationProfile>?]> = [
    ["SUPERUSER", "ALTER ROLE g6_bad SUPERUSER", "SUPERUSER"],
    ["CREATEDB", "ALTER ROLE g6_bad CREATEDB", "CREATEDB"],
    ["CREATEROLE", "ALTER ROLE g6_bad CREATEROLE", "CREATEROLE"],
    ["REPLICATION", "ALTER ROLE g6_bad REPLICATION", "REPLICATION"],
    ["BYPASSRLS", "ALTER ROLE g6_bad BYPASSRLS", "BYPASSRLS"],
    [
      "LOGIN when configured NOLOGIN",
      "ALTER ROLE g6_bad LOGIN",
      "LOGIN (configured NOLOGIN)",
    ],
    [
      "NOLOGIN when configured LOGIN",
      "",
      "NOLOGIN (configured LOGIN)",
      { runtimeLogin: true },
    ],
    ["a membership", "GRANT service_role TO g6_bad", "member of service_role"],
    [
      "an undocumented member",
      "CREATE ROLE g6_member NOLOGIN; GRANT g6_bad TO g6_member",
      "granted to g6_member",
    ],
    [
      "the workload member WITH ADMIN OPTION",
      "CREATE ROLE g6_wl NOLOGIN; GRANT g6_bad TO g6_wl WITH ADMIN OPTION",
      "granted to g6_wl WITH ADMIN OPTION",
      { workloadRole: "g6_wl" },
    ],
    [
      "an owned object",
      "CREATE TABLE public.g6_owned (x int); ALTER TABLE public.g6_owned OWNER TO g6_bad",
      "owns table g6_owned",
    ],
    [
      "CREATE on a schema",
      "GRANT CREATE ON SCHEMA sessions TO g6_bad",
      "CREATE on schema sessions",
    ],
    [
      "CREATE on the database",
      "DO $g$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO g6_bad', current_database()); END $g$",
      "CREATE on the database",
    ],
    [
      "existing DML",
      "GRANT INSERT, UPDATE ON sessions.session_index TO g6_bad",
      "INSERT,UPDATE on sessions.session_index",
    ],
    [
      "a column privilege",
      "GRANT SELECT (org_id) ON sessions.session_directory TO g6_bad",
      "column on sessions.session_directory",
    ],
    [
      "a sequence privilege",
      "CREATE SEQUENCE sessions.g6_seq; GRANT USAGE ON SEQUENCE sessions.g6_seq TO g6_bad",
      "privileges on sequence sessions.g6_seq",
    ],
    [
      "unapproved EXECUTE (direct)",
      "GRANT EXECUTE ON FUNCTION sessions.upsert_session_index(jsonb) TO g6_bad",
      "EXECUTE on sessions.upsert_session_index(jsonb)",
    ],
    [
      "EXECUTE on a trigger function (no exemption)",
      "GRANT EXECUTE ON FUNCTION sessions.pipeline_jobs_observability() TO g6_bad",
      "EXECUTE on sessions.pipeline_jobs_observability()",
    ],
    [
      "effective TEMPORARY (PUBLIC TEMP restored)",
      "DO $g$ BEGIN EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO PUBLIC', current_database()); END $g$",
      "TEMPORARY on the database",
    ],
    [
      "TEMPORARY granted to the role itself",
      "DO $g$ BEGIN EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO g6_bad', current_database()); END $g$",
      "TEMPORARY is not approved",
    ],
    [
      "no effective CONNECT",
      "DO $g$ BEGIN EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', current_database()); END $g$",
      "no CONNECT on the database (required)",
    ],
    [
      "a PUBLIC table privilege outside sessions",
      "CREATE TABLE public.g6_pub (x int); GRANT SELECT ON public.g6_pub TO PUBLIC",
      "SELECT on g6_pub",
    ],
    [
      "a PUBLIC sequence privilege outside sessions",
      "CREATE SEQUENCE public.g6_pub_seq; GRANT USAGE ON SEQUENCE public.g6_pub_seq TO PUBLIC",
      "privileges on sequence g6_pub_seq",
    ],
    [
      "EXECUTE on a SECURITY DEFINER function outside sessions (through PUBLIC)",
      "CREATE FUNCTION public.g6_definer() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION public.g6_definer() TO PUBLIC",
      "EXECUTE on g6_definer()",
    ],
    [
      "USAGE on another schema through PUBLIC",
      "CREATE SCHEMA g6_extra; GRANT USAGE ON SCHEMA g6_extra TO PUBLIC",
      "USAGE on schema g6_extra (only sessions and public are accepted)",
    ],
    [
      "MAINTAIN on a relation (PostgreSQL 17+)",
      "GRANT MAINTAIN ON sessions.session_index TO g6_bad",
      "MAINTAIN on sessions.session_index",
    ],
    [
      "MAINTAIN through PUBLIC (PostgreSQL 17+)",
      "GRANT MAINTAIN ON sessions.session_index TO PUBLIC",
      "MAINTAIN on sessions.session_index",
    ],
    [
      "default privileges in sessions granting PUBLIC",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO PUBLIC",
      "default privileges in schema sessions grant PUBLIC",
    ],
    [
      "a default ACL naming it",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO g6_bad",
      "has privileges on default privileges",
    ],
    [
      "more than SELECT on an approved table",
      "GRANT INSERT ON sessions.__96bulls_migrations TO g6_bad",
      "INSERT is not approved",
    ],
    [
      "GRANT OPTION on an approved table",
      "GRANT SELECT ON sessions.__96bulls_migrations TO g6_bad WITH GRANT OPTION",
      "WITH GRANT OPTION is not approved",
    ],
  ];
  for (const [name, setup, problem, extra] of HOSTILE)
    test(`refuses a runtime role with ${name}; ledger, marker and grants unchanged`, async () => {
      const db = await migrated();
      await db.exec("CREATE ROLE g6_bad NOLOGIN");
      if (setup) await db.exec(setup);
      const before = await state(db);
      const error = await failure(
        runMigrations(client(db), {
          profile: { ...GOOD, runtimeRole: "g6_bad", ...extra },
        }),
      );
      expect(error?.code).toBe("runtime_role_unsafe");
      expect(error?.message).toContain(problem);
      expect(await state(db)).toEqual(before);
    });

  test("the documented workload membership (no ADMIN) is accepted", async () => {
    const db = await migrated();
    await db.exec(
      "CREATE ROLE g6_bad NOLOGIN; CREATE ROLE g6_wl LOGIN; GRANT g6_bad TO g6_wl",
    );
    expect(
      await failure(
        runMigrations(client(db), {
          profile: { ...GOOD, runtimeRole: "g6_bad", workloadRole: "g6_wl" },
        }),
      ),
    ).toBeNull();
  });

  test("selfhosted refuses the migration owner as the runtime role (owner-run is local only)", async () => {
    const db = await migrated();
    const before = await state(db);
    const error = await failure(
      runMigrations(client(db), {
        profile: { ...GOOD, runtimeRole: "postgres" },
      }),
    );
    expect(error?.code).toBe("runtime_role_unsafe");
    expect(error?.message).toContain(
      "dedicated runtime role, not the migration owner",
    );
    expect(await state(db)).toEqual(before);
  });

  test("checked again after the last migration: a step that grants the role EXECUTE on a new function keeps PASS unwritten", async () => {
    const db = await migrated();
    const dir = mkdtempSync(join(tmpdir(), "g6-exec-"));
    writeFileSync(
      join(dir, "9999_public_fn.sql"),
      "CREATE FUNCTION sessions.g6_public_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION sessions.g6_public_fn() TO g6_rt;",
    );
    const steps = await plan(GOOD);
    const withFn = [
      ...steps.slice(0, -1),
      {
        name: "96bulls:9999_public_fn.sql",
        path: join(dir, "9999_public_fn.sql"),
        sentinel: { evidence: "test step", sql: "SELECT false AS applied" },
      },
      ...steps.slice(-1),
    ];
    const error = await failure(
      runMigrations(client(db), { profile: GOOD, steps: withFn }),
    );
    expect(error?.code).toBe("runtime_role_unsafe");
    expect(error?.message).toContain("EXECUTE on sessions.g6_public_fn()");
    expect(
      (
        await db.query<{ status: string }>(
          "SELECT status FROM sessions.__96bulls_role_validation",
        )
      ).rows[0]?.status,
    ).toBe("pending");
  });
});

describe("the full ordered migration sequence: one contiguous prefix, no unledgered footprint", () => {
  test("helm repro (a): only session_directory present: refused before any write (no ledger rows, no 42P07)", async () => {
    const db = await fresh();
    await db.exec(
      "CREATE SCHEMA sessions; CREATE TABLE sessions.session_directory (x int)",
    );
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("unledgered_schema");
    expect(error?.message).toContain("relation sessions.session_directory");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
  });

  test("helm repro (b): a first 96bulls ledger row without the upstream rows is never backfilled", async () => {
    const db = await fresh();
    await db.exec(`CREATE SCHEMA sessions; CREATE TABLE sessions.__96bulls_migrations (
        name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
      INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ('96bulls:0001_96bulls_observability.sql', 'x');`);
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("ledger_gap");
    expect(await snapshot(db)).toEqual(before);
  });

  test("a contiguous prefix whose unrecorded later steps were applied anyway: refused, nothing marked", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      "DELETE FROM sessions.__96bulls_migrations WHERE name LIKE '96bulls:%'",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("unledgered_schema");
    expect(await snapshot(db)).toEqual(before);
  });

  const FOOTPRINTS: Array<[string, string, string]> = [
    [
      "an index",
      "CREATE TABLE public.g6_t (x int); CREATE INDEX session_directory_agent_recent_idx ON public.g6_t (x)",
      "index session_directory_agent_recent_idx",
    ],
    [
      "a function",
      "CREATE SCHEMA sessions; CREATE FUNCTION sessions.jwt_org_id() RETURNS text LANGUAGE sql AS 'SELECT 1::text'",
      "function sessions.jwt_org_id",
    ],
    [
      "a later migration's table",
      "CREATE SCHEMA sessions; CREATE TABLE sessions.migration_assertion_uses (x int)",
      "relation sessions.migration_assertion_uses",
    ],
  ];
  for (const [name, setup, problem] of FOOTPRINTS)
    test(`detects ${name} of an unrecorded migration before any write`, async () => {
      const inventory = (await plan(LOCAL)).filter(
        (st) => st.name.includes(":2026") || st.name.startsWith("96bulls:"),
      );
      const all = inventory.flatMap((st) =>
        inventoryOf(readFileSync(st.path, "utf8")),
      );
      const expectedName = problem.split(" ")[1]!;
      expect(all.some((o) => o.name === expectedName)).toBe(true); // the example is really in the inventory
      const db = await fresh();
      await db.exec(setup);
      const error = await failure(
        runMigrations(client(db), { profile: LOCAL }),
      );
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(problem);
      expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
    });

  test("the inventory covers every object a full migration leaves in the sessions schema", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const steps = (await plan(LOCAL)).filter(
      (st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
    );
    const inv = new Set(
      steps
        .flatMap((st) => inventoryOf(readFileSync(st.path, "utf8")))
        .map(
          (o) =>
            `${o.kind}:${o.name.includes(".") || o.kind === "index" || o.kind === "trigger" || o.kind === "policy" ? o.name : `public.${o.name}`}`,
        ),
    );
    const live = (
      await db.query<{ k: string }>(`
        SELECT 'relation:sessions.' || c.relname AS k FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
           AND c.relname NOT IN ('__96bulls_migrations', '__96bulls_role_validation')
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('a', 'i'))
        UNION ALL
        SELECT 'index:' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'sessions' AND c.relkind IN ('i', 'I')
           AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = c.oid)
           AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)
        UNION ALL
        SELECT 'function:sessions.' || p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'sessions'
        UNION ALL
        SELECT 'type:sessions.' || t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = 'sessions' AND t.typtype IN ('e', 'd', 'c') AND (t.typtype <> 'c' OR t.typrelid = 0
           OR (SELECT relkind FROM pg_class WHERE oid = t.typrelid) = 'c')
        UNION ALL
        SELECT 'trigger:' || tg.tgname FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'sessions' AND NOT tg.tgisinternal AND tg.tgparentid = 0
        UNION ALL
        SELECT 'policy:' || po.polname FROM pg_policy po JOIN pg_class c ON c.oid = po.polrelid JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'sessions'
        ORDER BY 1`)
    ).rows.map((r) => r.k);
    const missing = live.filter((k) => !inv.has(k));
    expect(missing).toEqual([]);
    expect(live.length).toBeGreaterThan(40);
  });
});

describe("PUBLIC EXECUTE in sessions: revoked inside the validation transaction", () => {
  const publicExec = async (db: PGlite, fn: string) =>
    (
      await db.query<{ p: boolean }>(
        `SELECT p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE') AS p
           FROM pg_proc p WHERE p.oid = $1::regprocedure`,
        [fn],
      )
    ).rows[0]?.p;

  test("a clean run leaves no sessions function PUBLIC-executable, trigger functions included; triggers still fire for a caller without EXECUTE", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const anyPublic = (
      await db.query<{ fn: string }>(
        `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE n.nspname = 'sessions' AND (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0))`,
      )
    ).rows;
    expect(anyPublic).toEqual([]);
    // A caller with no EXECUTE on the trigger function still fires it.
    await db.exec(`CREATE TABLE public.g6_audit (x int);
      CREATE TRIGGER g6_append_only BEFORE UPDATE OR DELETE ON public.g6_audit
        FOR EACH ROW EXECUTE FUNCTION sessions.migration_ingest_audit_append_only();
      INSERT INTO public.g6_audit VALUES (1);
      CREATE ROLE g6_caller NOLOGIN; GRANT DELETE, SELECT ON public.g6_audit TO g6_caller;`);
    expect(
      (
        await db.query<{ e: boolean }>(
          "SELECT has_function_privilege('g6_caller', 'sessions.migration_ingest_audit_append_only()', 'EXECUTE') AS e",
        )
      ).rows[0]?.e,
    ).toBe(false);
    await db.exec("SET ROLE g6_caller");
    const fired = await failure(db.exec("DELETE FROM public.g6_audit"));
    await db.exec("RESET ROLE");
    expect(fired).not.toBeNull(); // the append-only trigger refused the delete
    expect((await db.query("SELECT 1 FROM public.g6_audit")).rows).toHaveLength(
      1,
    );
  });

  test("a PUBLIC EXECUTE grant on a sessions function is revoked on the next run (default-ACL protection)", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.repo_visibility_ids(text, uuid) TO PUBLIC",
    );
    expect(
      await publicExec(db, "sessions.repo_visibility_ids(text, uuid)"),
    ).toBe(true);
    await runMigrations(client(db), { profile: LOCAL });
    expect(
      await publicExec(db, "sessions.repo_visibility_ids(text, uuid)"),
    ).toBe(false);
  });

  test("guardrail: when validation fails, the revoke rolls back with it (state unchanged)", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.pipeline_jobs_observability() TO PUBLIC",
    );
    await db.exec(
      "GRANT SELECT ON sessions.session_index TO session_directory_search_worker",
    ); // phase 2 drift
    const acl = async () =>
      (
        await db.query<{ a: string }>(
          "SELECT proacl::text AS a FROM pg_proc WHERE oid = 'sessions.pipeline_jobs_observability()'::regprocedure",
        )
      ).rows[0]?.a;
    const before = { acl: await acl(), ledger: (await snapshot(db)).ledger };
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("55000");
    expect({ acl: await acl(), ledger: (await snapshot(db)).ledger }).toEqual(
      before,
    );
    expect(await publicExec(db, "sessions.pipeline_jobs_observability()")).toBe(
      true,
    );
  });
});

describe("per-step catalog sentinels", () => {
  /** Shim, Worker bootstrap, then the schema steps [0, upTo] applied directly, and the ledger holding [0, ledgered). */
  async function prefix(upTo: number, ledgered: number): Promise<PGlite> {
    const db = await fresh();
    const steps = (await plan(LOCAL)).filter(
      (st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
    );
    await db.exec(
      readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
    );
    await db.exec(
      readFileSync(
        join(root, "supabase/bootstrap/session_directory_search_worker.sql"),
        "utf8",
      ),
    );
    for (let i = 0; i <= upTo; i++)
      await db.exec(readFileSync(steps[i]!.path, "utf8"));
    await db.exec(`CREATE SCHEMA IF NOT EXISTS sessions; CREATE TABLE sessions.__96bulls_migrations (
      name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`);
    for (let i = 0; i < ledgered; i++)
      await db.query(
        "INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ($1, $2)",
        [
          steps[i]!.name,
          createHash("sha256")
            .update(readFileSync(steps[i]!.path, "utf8"))
            .digest("hex"),
        ],
      );
    return db;
  }
  const sentinel = async (db: PGlite, name: string) =>
    (await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[name]!.sql))
      .rows[0]?.applied;

  test("the table names every schema step, and nothing else", async () => {
    const steps = (await plan(LOCAL))
      .filter(
        (st) =>
          st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
      )
      .map((st) => st.name);
    expect(Object.keys(MIGRATION_SENTINELS).sort()).toEqual([...steps].sort());
    for (const n of NON_CREATE_STEPS) expect(steps).toContain(n);
  });

  test("each sentinel is false before its step and true from it on", async () => {
    const steps = (await plan(LOCAL)).filter(
      (st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
    );
    const db = await fresh();
    await db.exec(
      readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
    );
    await db.exec(
      readFileSync(
        join(root, "supabase/bootstrap/session_directory_search_worker.sql"),
        "utf8",
      ),
    );
    for (const st of steps) {
      expect(await sentinel(db, st.name), `${st.name} before`).toBe(false);
      await db.exec(readFileSync(st.path, "utf8"));
      expect(await sentinel(db, st.name), `${st.name} after`).toBe(true);
    }
    for (const st of steps)
      expect(await sentinel(db, st.name), `${st.name} at the end`).toBe(true);
  });

  for (const name of NON_CREATE_STEPS)
    test(`${name}: applied and ledgered is accepted; applied but unledgered is refused before any write`, async () => {
      const steps = (await plan(LOCAL)).filter(
        (st) =>
          st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
      );
      const i = steps.findIndex((st) => st.name === name);
      expect(i).toBeGreaterThan(0);
      const honest = await prefix(i, i + 1);
      expect(
        await failure(runMigrations(client(honest), { profile: LOCAL })),
      ).toBeNull();
      const hidden = await prefix(i, i);
      const before = await snapshot(hidden);
      const error = await failure(
        runMigrations(client(hidden), { profile: LOCAL }),
      );
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`(${name})`);
      expect(await snapshot(hidden)).toEqual(before);
    });
});

test("the runbook's sentinel table documents every schema step", async () => {
  const runbook = readFileSync(
    join(root, "runbooks/session-directory-search-worker-bootstrap.md"),
    "utf8",
  );
  for (const name of Object.keys(MIGRATION_SENTINELS))
    expect(runbook, name).toContain(`| \`${name}\``);
});

describe("function boundary: ACL manifest, residual, activation gate", () => {
  const MANIFEST = JSON.parse(
    readFileSync(join(root, "src/hoopdb/sessions-function-acl.json"), "utf8"),
  ).functions;

  test("ACL manifest regression: every sessions function's ACL after a clean migrate is exactly the committed manifest", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    expect(await sessionsFunctionAcl(client(db))).toEqual(MANIFEST);
    for (const entries of Object.values(MANIFEST) as string[][])
      expect(
        entries.some((e) => e.startsWith("PUBLIC") || e === "<default>"),
      ).toBe(false);
  });

  test("residual: a sessions function created after PASS, outside the migrator, IS directly invocable (not prevented); the live gate gives 55000 and claims stop", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    const health = {
      schemaReady: async () => (await schemaVerdict(expected, transact(db))).ok,
    };
    expect(await gateOpen(health)).toBe(true);
    await db.exec(`CREATE FUNCTION sessions.g6_late() RETURNS int LANGUAGE sql AS 'SELECT 42';
      CREATE ROLE g6_unrel NOLOGIN; GRANT authenticated TO g6_unrel;`);
    await db.exec("SET ROLE g6_unrel");
    const direct = (
      await db.query<{ v: number }>("SELECT sessions.g6_late() AS v")
    ).rows[0]?.v;
    await db.exec("RESET ROLE");
    expect(direct).toBe(42); // the residual: PostgreSQL's built-in PUBLIC EXECUTE
    const v = await schemaVerdict(expected, transact(db));
    expect(v).toEqual({
      ok: false,
      reason: "role_validation_failed",
      error_code: "55000",
    });
    expect(await gateOpen(health)).toBe(false); // the worker claims nothing
    await runMigrations(client(db), { profile: LOCAL }); // the next migrate revokes it
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
  });

  /** A Supabase-shaped database: platform roles and the Worker pre-provisioned, a dedicated runtime role, no PUBLIC TEMP. */
  async function supabaseLike(): Promise<PGlite> {
    const db = await fresh();
    await db.exec(
      readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
    );
    await db.exec(
      readFileSync(
        join(root, "supabase/bootstrap/session_directory_search_worker.sql"),
        "utf8",
      ),
    );
    await db.exec(`CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}`);
    return db;
  }
  const SUPA: MigrationProfile = {
    mode: "supabase",
    shim: false,
    bootstrap: false,
    verify: true,
    runtimeRole: "g6_rt",
    runtimeLogin: false,
  };

  test("supabase activation gate: refused before any write unless HOOPDB_FUNCTION_BOUNDARY_MODE selects a catalog-proven control; evidence is names and booleans", async () => {
    const db = await supabaseLike();
    const none = await failure(runMigrations(client(db), { profile: SUPA }));
    expect(none?.code).toBe("function_boundary_unproven");
    const evidence = JSON.parse(none!.message!.match(/(\{.*\})/)![1]!);
    expect(evidence).toEqual({
      migration_owner: "postgres",
      sessions_unexposed_and_no_untrusted_usage: false,
      owner_function_default: ["<built-in>"],
      owner_function_default_exact: false,
      sessions_create_roles: [
        {
          role: "postgres",
          superuser: true,
          is_migration_owner: true,
          function_default: ["<built-in>"],
          function_default_exact: false,
        },
      ],
      sessions_create_roles_covered: false,
      sessions_schema_function_defaults: [],
      sessions_functions_owned_by_owner: true,
      event_trigger_present: false,
      mode: "none",
      satisfied: false,
    });
    // Names and booleans only (in arrays and objects too).
    const leaves = (v: unknown): unknown[] =>
      v !== null && typeof v === "object"
        ? Object.values(v as object).flatMap(leaves)
        : [v];
    for (const v of leaves(evidence))
      expect(["string", "boolean"]).toContain(typeof v);
    // Mode selected but the control absent: still refused.
    expect(
      (
        await failure(
          runMigrations(client(db), {
            profile: { ...SUPA, functionBoundary: "global-default-revoke" },
          }),
        )
      )?.code,
    ).toBe("function_boundary_unproven");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
  });

  test("supabase activation gate (A): an owner-scoped global default revoke, with the mode selected, passes; new owner functions are no longer PUBLIC-executable", async () => {
    const db = await supabaseLike();
    await db.exec(
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
    );
    expect(
      await failure(
        runMigrations(client(db), {
          profile: { ...SUPA, functionBoundary: "global-default-revoke" },
        }),
      ),
    ).toBeNull();
    await db.exec(
      "CREATE FUNCTION sessions.g6_after() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    expect(
      (
        await db.query<{ e: boolean }>(
          "SELECT has_function_privilege('g6_rt', 'sessions.g6_after()', 'EXECUTE') AS e",
        )
      ).rows[0]?.e,
    ).toBe(false);
    // Blast radius: the owner's global default reaches every schema (why approval is required).
    await db.exec(
      "CREATE FUNCTION public.g6_elsewhere() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    expect(
      (
        await db.query<{ e: boolean }>(
          "SELECT has_function_privilege('g6_rt', 'public.g6_elsewhere()', 'EXECUTE') AS e",
        )
      ).rows[0]?.e,
    ).toBe(false);
  });

  test("event-trigger mode is not implemented: refused even with a trigger of the right name present", async () => {
    const db = await supabaseLike();
    await db.exec(`CREATE FUNCTION public.g6_revoke_sessions_public() RETURNS event_trigger LANGUAGE plpgsql AS $f$
      BEGIN EXECUTE 'REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC'; END $f$;
      CREATE EVENT TRIGGER ${SESSIONS_FUNCTION_EVENT_TRIGGER} ON ddl_command_end WHEN TAG IN ('CREATE FUNCTION')
        EXECUTE FUNCTION public.g6_revoke_sessions_public();`);
    expect(() =>
      migrationProfile({
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_RUNTIME_ROLE: "g6_rt",
        HOOPDB_RUNTIME_ROLE_LOGIN: "false",
        HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
      }),
    ).toThrow(
      expect.objectContaining({ code: "function_boundary_mode_unimplemented" }),
    );
    // Even forced through a profile object, a trigger's name never satisfies the gate.
    expect(
      (
        await failure(
          runMigrations(client(db), {
            profile: { ...SUPA, functionBoundary: "event-trigger" },
          }),
        )
      )?.code,
    ).toBe("function_boundary_unproven");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
  });

  test("the old HOOPDB_FUNCTION_BOUNDARY_APPROVED name is refused (stale config fails closed)", () => {
    expect(() =>
      migrationProfile({
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_RUNTIME_ROLE: "g6_rt",
        HOOPDB_RUNTIME_ROLE_LOGIN: "false",
        HOOPDB_FUNCTION_BOUNDARY_APPROVED: "global-default-revoke",
      }),
    ).toThrow(expect.objectContaining({ code: "db_profile_invalid" }));
    expect(
      migrationProfile({
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_RUNTIME_ROLE: "g6_rt",
        HOOPDB_RUNTIME_ROLE_LOGIN: "false",
        HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
      }).functionBoundary,
    ).toBe("global-default-revoke");
  });

  test("selfhosted is gated too: refused without the owner-scoped revoke; passes with it (only local is ungated)", async () => {
    const db = await fresh();
    await db.exec(`CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}`);
    const SELFP: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    expect(
      (await failure(runMigrations(client(db), { profile: SELFP })))?.code,
    ).toBe("function_boundary_unproven");
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
    await db.exec(OWNER_FN_DEFAULT_REVOKE);
    expect(
      await failure(runMigrations(client(db), { profile: SELFP })),
    ).toBeNull();
    // Local mode needs no control.
    const loc = await fresh();
    expect(
      await failure(runMigrations(client(loc), { profile: LOCAL })),
    ).toBeNull();
  });

  test("creator coverage: a sessions function owned by any other role escapes the owner-scoped default, so the gate refuses it before any write", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    const SELFP: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    await runMigrations(client(db), { profile: SELFP });
    await db.exec(`CREATE ROLE g6_other_creator NOLOGIN; GRANT CREATE, USAGE ON SCHEMA sessions TO g6_other_creator;
      SET ROLE g6_other_creator; CREATE FUNCTION sessions.g6_foreign() RETURNS int LANGUAGE sql AS 'SELECT 1'; RESET ROLE;`);
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: SELFP }));
    expect(error?.code).toBe("function_boundary_unproven");
    expect(JSON.parse(error!.message!.match(/(\{.*\})/)![1]!)).toMatchObject({
      owner_function_default_exact: true,
      sessions_functions_owned_by_owner: false,
      satisfied: false,
    });
    expect(await snapshot(db)).toEqual(before);
  });

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
    test(`exact default ACL: a poisoned owner default (PUBLIC revoked plus ${name}) is refused before any write`, async () => {
      const db = await fresh();
      await db.exec(
        readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
      );
      await db.exec(
        `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}; ALTER DEFAULT PRIVILEGES FOR ROLE postgres ${poison}`,
      );
      const SELFP: MigrationProfile = {
        ...LOCAL,
        mode: "selfhosted",
        runtimeRole: "g6_rt",
        runtimeLogin: false,
        functionBoundary: "global-default-revoke",
      };
      const error = await failure(
        runMigrations(client(db), { profile: SELFP }),
      );
      expect(error?.code).toBe("function_boundary_unproven");
      const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
      expect(ev.owner_function_default).toContain(shown);
      expect(ev.owner_function_default_exact).toBe(false);
      expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
    });

  test("pre-creation creator coverage: a CREATE-capable role that has created no function yet fails the gate", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    const SELFP: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    await runMigrations(client(db), { profile: SELFP });
    await db.exec(
      "CREATE ROLE g6_can_create NOLOGIN; GRANT CREATE, USAGE ON SCHEMA sessions TO g6_can_create",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: SELFP }));
    expect(error?.code).toBe("function_boundary_unproven");
    const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
    expect(ev.sessions_functions_owned_by_owner).toBe(true); // no function yet
    expect(ev.sessions_create_roles).toContainEqual({
      role: "g6_can_create",
      superuser: false,
      is_migration_owner: false,
      function_default: ["<built-in>"],
      function_default_exact: false,
    });
    expect(ev.sessions_create_roles_covered).toBe(false);
    expect(await snapshot(db)).toEqual(before);
    // Its own exact-safe default covers it.
    await db.exec(
      "ALTER DEFAULT PRIVILEGES FOR ROLE g6_can_create REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
    );
    expect(
      await failure(runMigrations(client(db), { profile: SELFP })),
    ).toBeNull();
  });

  test("a schema-scoped function default IN sessions that grants another role refuses the gate before any write, even with exact global defaults", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    const SELFP: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    await runMigrations(client(db), { profile: SELFP });
    await db.exec(
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT EXECUTE ON FUNCTIONS TO authenticated",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: SELFP }));
    expect(error?.code).toBe("function_boundary_unproven");
    const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
    expect(ev.owner_function_default_exact).toBe(true);
    expect(ev.sessions_create_roles_covered).toBe(true);
    expect(ev.sessions_schema_function_defaults).toEqual([
      "postgres>authenticated:EXECUTE",
    ]);
    expect(await snapshot(db)).toEqual(before);
    await db.exec(
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions REVOKE EXECUTE ON FUNCTIONS FROM authenticated",
    );
    expect(
      await failure(runMigrations(client(db), { profile: SELFP })),
    ).toBeNull();
  });

  test("blast radius of the owner-scoped revoke: new owner functions in sessions AND another schema are not PUBLIC-executable; intended grants survive (ACL manifest) the revoke and the next migrate", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    const SELFP: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    await runMigrations(client(db), { profile: SELFP });
    expect(await sessionsFunctionAcl(client(db))).toEqual(MANIFEST);
    await db.exec(`CREATE FUNCTION sessions.g6_future() RETURNS int LANGUAGE sql AS 'SELECT 1';
      CREATE SCHEMA g6_second; CREATE FUNCTION g6_second.g6_future() RETURNS int LANGUAGE sql AS 'SELECT 1';
      CREATE ROLE g6_probe NOLOGIN;`);
    for (const fn of ["sessions.g6_future()", "g6_second.g6_future()"]) {
      const r = (
        await db.query<{ e: boolean; pub: boolean }>(
          `SELECT has_function_privilege('g6_probe', $1::regprocedure, 'EXECUTE') AS e,
                EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE p.oid = $1::regprocedure AND a.grantee = 0) AS pub`,
          [fn],
        )
      ).rows[0];
      expect(r, fn).toEqual({ e: false, pub: false });
    }
    await db.exec(
      "DROP FUNCTION sessions.g6_future(); DROP SCHEMA g6_second CASCADE",
    );
    await runMigrations(client(db), { profile: SELFP });
    expect(await sessionsFunctionAcl(client(db))).toEqual(MANIFEST);
  });
});

describe("every worker claim path runs the live gate first", () => {
  test("table-driven over each stage claim and the side tick: the gate runs before every path, and once closed nothing further runs", async () => {
    const stages = ["capture", "index", "summary"] as const;
    const cases: Array<[string, number]> = [
      ["gate open throughout", Infinity],
      ...stages.map(
        (st, i) => [`gate closes before stage ${st}`, i] as [string, number],
      ),
      ["gate closes before the side tick", stages.length],
    ];
    for (const [name, closeAt] of cases) {
      const log: string[] = [];
      let checks = 0;
      const r = await workerTick({
        stages,
        gate: async () => {
          log.push("gate");
          return checks++ < closeAt;
        },
        poll: async (st) => (log.push(`claim:${st}`), 1),
        tick: async () => void log.push("tick"),
      });
      // Every claim path is immediately preceded by a gate check.
      for (let i = 0; i < log.length; i++)
        if (log[i] !== "gate") expect(log[i - 1], name).toBe("gate");
      const paths = [...stages.map((st) => `claim:${st}`), "tick"];
      const ran = log.filter((x) => x !== "gate");
      expect(ran, name).toEqual(
        paths.slice(0, Math.min(closeAt, paths.length)),
      );
      expect(r.gated, name).toBe(closeAt < paths.length);
    }
  });

  test("with the real gate: a sessions function created after PASS closes every claim path (no claim, no tick)", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    const health = {
      schemaReady: async () => (await schemaVerdict(expected, transact(db))).ok,
    };
    const ran: string[] = [];
    const io = {
      stages: ["a", "b"],
      gate: () => gateOpen(health),
      poll: async (st: string) => (ran.push(st), 1),
      tick: async () => void ran.push("tick"),
    };
    expect(await workerTick(io)).toEqual({ claimed: 2, gated: false });
    await db.exec(
      "CREATE FUNCTION sessions.g6_late2() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    ran.length = 0;
    expect(await workerTick(io)).toEqual({ claimed: 0, gated: true });
    expect(ran).toEqual([]);
  });
});
