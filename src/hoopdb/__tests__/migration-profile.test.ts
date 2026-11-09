/**
 * The migrator's database-mode contract, its runtime-role grant and its single-authority
 * guard, on in-process PGlite plus the pure profile resolver.
 */
import { afterEach, describe, expect, setDefaultTimeout, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { schemaVerdict, type Transact } from "../../shared/db.ts";
import {
  CLOUDSQL_PLATFORM_SUPERUSERS,
  functionBoundaryEvidence,
  isRdsCreatorEdge,
  RDS_PLATFORM_SUPERUSERS,
  isSupabaseCreatorEdge,
  isSupabasePublicTemp,
  isRuntimeWorkloadEdge,
  resolveRenamedLedger,
  renameAliasRepairMissing,
  inventoryOf,
  MIGRATION_CHECKSUM_ALIASES,
  MIGRATION_RENAME_ALIASES,
  migrationChecksumAlias,
  migrationChecksumMatches,
  RUNTIME_APPLICATION_FUNCTIONS,
  sessionsFunctionAcl,
  sessionsFunctionAclPreview,
  SESSIONS_FUNCTION_EVENT_TRIGGER,
  SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256,
  SESSIONS_FUNCTION_EVENT_TRIGGER_TAGS,
  SUPABASE_PLATFORM_SUPERUSERS,
} from "../migrate-guards.ts";
import * as migrateGuards from "../migrate-guards.ts";
import { gateOpen, workerTick } from "../../shared/worker.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import {
  ADOPTABLE_INDEXES,
  MIGRATION_SENTINELS,
  NON_CREATE_STEPS,
} from "../migration-sentinels.ts";
import {
  expectedSchema,
  inspectMigrations,
  generation,
  migrationLedgerSha256,
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

test("managed-provider bootstrap fixed-role inventories stay equal", () => {
  const declaredRoles = (file: string) => {
    const sql = readFileSync(join(root, "supabase/bootstrap", file), "utf8");
    const inventory = /FOREACH\s+v_role\s+IN\s+ARRAY\s+ARRAY\[([\s\S]*?)\]\s+LOOP/.exec(sql);
    if (!inventory) throw new Error(`${file}: fixed-role validation inventory not found`);
    const roles = [...inventory[1]!.matchAll(/'([^']+)'::name/g)].map((m) => m[1]!).sort();
    expect(roles.length).toBeGreaterThan(0);
    return roles;
  };
  expect(declaredRoles("rds_migration_roles.sql")).toEqual(
    declaredRoles("cloudsql_migration_roles.sql"),
  );
});

/**
 * Upstream plan steps ordered after `after` (default 20261105044020): a database "before" that step must hold none of
 * them, since an unrecorded step with a later upstream step recorded is a ledger gap.
 */
const CB_STEP = "upstream:20261105044020_claim_bootstrap_org_indexes.sql";
const laterUpstream = <T extends { name: string }>(steps: T[], after: string = CB_STEP) =>
  steps.filter((s) => s.name.startsWith("upstream:") && s.name > after);
/** Roll back every upstream step after 044020, newest first, so 044020 is the newest applied step again. */
async function rollbackLaterThan044020(db: PGlite, profile: MigrationProfile) {
  for (const { name } of laterUpstream(await plan(profile)).reverse())
    await db.exec(readFileSync(join(root, "supabase/rollbacks", `${name.slice("upstream:".length).replace(/\.sql$/, "")}.down.sql`), "utf8"));
}

type PlanStep = Awaited<ReturnType<typeof plan>>[number];
type FunctionAclManifest = Awaited<ReturnType<typeof migrateGuards.sessionsFunctionAclManifest>>;
type RuntimeAclCatalog = ReturnType<typeof migrateGuards.runtimeApplicationAclCatalog>;

/**
 * The previous release's ACL view: the current function ACL manifest and runtime application ACL catalog minus
 * everything the later steps CREATE. A previous release's binary shipped the manifest and catalog of its day, which
 * could not yet list a function, table or sequence a later step creates; the current ones do, so on a database without
 * them the ACL install regclass/regprocedure-casts a missing object (42P01/42883) and the non-local exact-ACL check
 * refuses (function_acl_drift). Relations: CREATE [UNLOGGED|TEMP] TABLE/SEQUENCE in sessions, plus the
 * <table>_<column>_seq a serial/identity column of such a table implies. Functions: CREATE FUNCTION in sessions
 * (catalog entries and manifest keys are matched by name, before the argument list).
 */
function previousReleaseAcl(laterSteps: ReadonlyArray<{ path: string }>, manifest: FunctionAclManifest, catalog: RuntimeAclCatalog) {
  const laterFns = new Set(laterSteps.flatMap((s) =>
    [...readFileSync(s.path, "utf8").matchAll(/\bCREATE\s+FUNCTION\s+(sessions\.\w+)\s*\(/gi)].map((m) => m[1]!.toLowerCase())));
  const laterRels = new Set(laterSteps.flatMap((s) =>
    [...readFileSync(s.path, "utf8").matchAll(
      /\bCREATE\s+(?:(?:UNLOGGED|TEMP|TEMPORARY)\s+)?(?:TABLE|SEQUENCE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(sessions\.\w+)/gi,
    )].map((m) => m[1]!.toLowerCase())));
  const previousManifest: FunctionAclManifest = Object.fromEntries(Object.entries(manifest).filter(([fn]) => !laterFns.has(fn.slice(0, fn.indexOf("(")))));
  const previousCatalog: RuntimeAclCatalog = {
    tables: Object.fromEntries(Object.entries(catalog.tables).filter(([rel]) => !laterRels.has(rel))),
    sequences: catalog.sequences.filter((rel) => !laterRels.has(rel) && ![...laterRels].some((t) => rel.startsWith(`${t}_`) && rel.endsWith("_seq"))),
    rlsTables: catalog.rlsTables.filter((rel) => !laterRels.has(rel)),
    // Its runtime EXECUTE grants likewise could not name a function a later step creates ($1::regprocedure).
    functions: catalog.functions.filter((fn) => !laterFns.has(fn.slice(0, fn.indexOf("(")))),
  };
  return { laterFns, laterRels, previousManifest, previousCatalog };
}

/**
 * Model the previous release that stopped just before `cutoff`: `steps` is the full plan without `cutoff` and without
 * every later upstream step (a ledger gap otherwise); `later` names those later steps. When there are later steps,
 * sessionsFunctionAclManifest and runtimeApplicationAclCatalog are spied to return the previous release's ACL view
 * (previousReleaseAcl) until `restore()`; with no later step nothing is mocked (the current manifest and catalog are
 * that release's). `current` replaces the manifest/catalog the view is derived from (the regression test's synthetic
 * later slot); by default it is the real one. Every truncated `runMigrations` in this file goes through this model,
 * with `restore()` in a `finally` right after it.
 */
async function previousReleaseModel(
  full: PlanStep[],
  cutoff: string,
  current?: { manifest: FunctionAclManifest; catalog: RuntimeAclCatalog },
) {
  const laterSteps = laterUpstream(full, cutoff);
  const later = laterSteps.map((s) => s.name);
  const steps = full.filter((s) => s.name !== cutoff && !later.includes(s.name));
  const { previousManifest, previousCatalog } = previousReleaseAcl(
    laterSteps,
    current?.manifest ?? await migrateGuards.sessionsFunctionAclManifest(),
    current?.catalog ?? migrateGuards.runtimeApplicationAclCatalog(),
  );
  const manifestSpy = later.length ? spyOn(migrateGuards, "sessionsFunctionAclManifest").mockResolvedValue(previousManifest) : undefined;
  const catalogSpy = later.length ? spyOn(migrateGuards, "runtimeApplicationAclCatalog").mockReturnValue(previousCatalog) : undefined;
  return {
    steps,
    later,
    previousManifest,
    previousCatalog,
    restore: () => {
      manifestSpy?.mockRestore();
      catalogSpy?.mockRestore();
    },
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
      { ...base, HOOPDB_DB_MODE: "aurora", HOOPDB_SUPABASE_SHIM: "true" },
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
      "cloudsql without an explicit shim choice",
      { ...base, HOOPDB_DB_MODE: "cloudsql" },
      "shim_unset",
    ],
    [
      "rds with the shim forced on",
      { ...base, HOOPDB_DB_MODE: "rds", HOOPDB_SUPABASE_SHIM: "true" },
      "shim_forbidden",
    ],
    [
      "rds with the generic bootstrap forced on",
      { ...base, HOOPDB_DB_MODE: "rds", HOOPDB_ROLE_BOOTSTRAP: "true" },
      "bootstrap_forbidden",
    ],
    [
      "rds with a workload role",
      { ...base, HOOPDB_DB_MODE: "rds", HOOPDB_RUNTIME_WORKLOAD_ROLE: "app_login" },
      "db_profile_invalid",
    ],
    [
      "rds with the event-trigger boundary",
      { ...base, HOOPDB_DB_MODE: "rds", HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger" },
      "function_boundary_mode_unsupported",
    ],
    [
      "rds without an explicit runtime login choice",
      { HOOPDB_DB_MODE: "rds", HOOPDB_RUNTIME_ROLE: "hoop_runtime" },
      "runtime_role_login_unset",
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
    expect(
      migrationProfile({
        ...base,
        HOOPDB_DB_MODE: "cloudsql",
        HOOPDB_SUPABASE_SHIM: "true",
        HOOPDB_ROLE_BOOTSTRAP: "false",
        HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
      }),
    ).toMatchObject({
      mode: "cloudsql",
      shim: true,
      bootstrap: false,
      functionBoundary: "global-default-revoke",
    });
    // rds: never the shim nor the generic bootstrap, with or without an explicit "false".
    for (const shim of [{}, { HOOPDB_SUPABASE_SHIM: "false" }])
      expect(
        migrationProfile({
          ...base,
          ...shim,
          HOOPDB_DB_MODE: "rds",
          HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
        }),
      ).toEqual({
        mode: "rds",
        shim: false,
        bootstrap: false,
        verify: true,
        runtimeRole: "hoop_runtime",
        runtimeLogin: true,
        functionBoundary: "global-default-revoke",
        // Unset HOOPDB_RDS_LAYOUT keeps today's master-owner contract.
        rdsLayout: "master-owner",
      });
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

  test("readiness binds the explicit rds database mode", async () => {
    const saved = process.env.HOOPDB_DB_MODE;
    try {
      process.env.HOOPDB_DB_MODE = "rds";
      expect(await expectedSchema()).toMatchObject({ dbMode: "rds" });
    } finally {
      if (saved === undefined) delete process.env.HOOPDB_DB_MODE;
      else process.env.HOOPDB_DB_MODE = saved;
    }
  });

  test("readiness binds the explicit cloudsql database mode", async () => {
    const saved = process.env.HOOPDB_DB_MODE;
    try {
      process.env.HOOPDB_DB_MODE = "cloudsql";
      expect(await expectedSchema()).toMatchObject({ dbMode: "cloudsql" });
    } finally {
      if (saved === undefined) delete process.env.HOOPDB_DB_MODE;
      else process.env.HOOPDB_DB_MODE = saved;
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

  test("a dedicated runtime role gets the exact service ACL plus read-only readiness metadata, no DDL, ownership or BYPASSRLS, and the live gate passes", async () => {
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
    expect(
      (
        await db.query<Record<string, boolean>>(
          `SELECT has_table_privilege('g6_rt', 'sessions.pipeline_jobs', 'SELECT, INSERT, UPDATE, DELETE') AS queue_dml,
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
      ).rows[0],
    ).toEqual({
      queue_dml: true,
      claimless_update: true,
      assertion_cleanup: true,
      chunk_cleanup: true,
      base_read_cleanup: true,
      audit_mutation: false,
      queue_truncate: false,
      queue_sequence: true,
      index_rpc: true,
      maintenance_rpc: false,
      trigger_execute: false,
    });
    expect(
      (
        await db.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM pg_policy p
            WHERE p.polname = 'bulls96_trusted_backend'
              AND (SELECT oid FROM pg_roles WHERE rolname = 'g6_rt') = ANY(p.polroles)`,
        )
      ).rows[0]?.n,
    ).toBe(30);
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

  test("a gap inside the 96bulls lane: refused, nothing marked, marker unchanged", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      "DELETE FROM sessions.__96bulls_migrations WHERE name = '96bulls:0003_fanout_ledger.sql'",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("ledger_gap");
    expect(error?.message).toContain("96bulls:0003_fanout_ledger.sql");
    expect(error?.message).toContain("96bulls:0004_summary_pointers.sql");
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
  expect(MIGRATION_CHECKSUM_ALIASES).toEqual([
    {
      migration: "upstream:20261030020000_session_directory_short_summary.sql",
      ledger_sha:
        "c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4",
      accepted_as_alias_of: sha,
    },
  ]);
  const [alias] = MIGRATION_CHECKSUM_ALIASES;
  expect(
    migrationChecksumMatches(
      alias!.migration,
      alias!.ledger_sha,
      alias!.accepted_as_alias_of,
    ),
  ).toBe(true);
  expect(
    migrationChecksumMatches(
      "upstream:20261030010000_session_directory.sql",
      alias!.ledger_sha,
      alias!.accepted_as_alias_of,
    ),
  ).toBe(false);
  expect(migrationChecksumMatches(alias!.migration, "d".repeat(64), sha)).toBe(
    false,
  );
});

test("generation() is exactly the migration and verifier file bytes, with no registry input", async () => {
  // Recomputed from files only, deliberately importing no alias/registry constant: any
  // alias map or other non-file input added to generation() fails this test.
  const sql = (dir: string) => readdirSync(join(root, dir)).filter((f) => f.endsWith(".sql")).sort();
  const files: [string, string][] = [
    ...sql("supabase/migrations").map((f): [string, string] => [`upstream:${f}`, `supabase/migrations/${f}`]),
    ...sql("src/hoopdb/migrations").map((f): [string, string] => [`96bulls:${f}`, `src/hoopdb/migrations/${f}`]),
    ...([
      ["session_directory_search_worker", "session_directory_search_worker_verify.sql"],
      ["session_token_verifier", "session_token_verifier_verify.sql"],
      ["session_ingest_seeder", "session_ingest_seeder_verify.sql"],
      ["session_ingest", "session_ingest_verify.sql"],
      ["canonical_project_events", "canonical_project_events_verify.sql"],
      ["acquisition_installer", "acquisition_installer_verify.sql"],
      ["buffered_search_feed_runtime", "buffered_search_feed_runtime_verify.sql"],
    ] as const).map(([n, f]): [string, string] => [`verify:${n}`, `supabase/bootstrap/${f}`]),
  ];
  const parts = files.map(([name, path]) => [
    name,
    createHash("sha256").update(readFileSync(join(root, path), "utf8")).digest("hex"),
  ]);
  expect(await generation()).toBe(createHash("sha256").update(JSON.stringify(parts)).digest("hex"));
});

describe("append-only renamed migrations: adopted in memory only when every checksum matches", () => {
  /** The planned file's sha256; undefined for a name this tree has no file for (the migrator's plan lookup). */
  const fileSha = (name: string) => {
    const path = join(root, "supabase/migrations", name.slice("upstream:".length));
    return existsSync(path) ? createHash("sha256").update(readFileSync(path)).digest("hex") : undefined;
  };
  const [event, ingest] = MIGRATION_RENAME_ALIASES;
  const original = () =>
    new Map<string, string>([
      ["upstream:20261102000000_session_briefs.sql", "a".repeat(64)],
      [event!.ledger_name, event!.ledger_sha],
      [ingest!.ledger_name, ingest!.ledger_sha],
    ]);

  test("the alias map is pinned, and each release sha is the file's real sha256", () => {
    expect(MIGRATION_RENAME_ALIASES).toEqual([
      {
        ledger_name: "upstream:20261103010000_session_directory_event_metadata.sql",
        ledger_sha:
          "fe07b2ef2b3e18634b024f35f4272bee9100ea39d0383ce33e8c6d969b77a476",
        migration: "upstream:20261104021000_session_directory_event_metadata.sql",
        accepted_as_alias_of:
          "4fb1337524c8335dd5bd507f60ea896a41a031ace0c7a2da8a8f7dedc1245e01",
      },
      {
        ledger_name: "upstream:20261102010000_session_ingest_ledger.sql",
        ledger_sha:
          "21a0be7dc69afa7d3b1d1b27e31fee9778c1a6a0c3b20246e3029fa2f1356fa6",
        migration: "upstream:20261104022000_session_ingest_ledger.sql",
        accepted_as_alias_of:
          "69f4157ea3a615344aa1b383fc6af7405c1ff9f1568e7c8ab97e58fbc78fe1b5",
        repair: "upstream:20261104040000_ingest_s3_proof_seed.sql",
        repair_sha:
          "18ea54ed1aa50e5ea44bab01a65177e2fc9c92919ac23b973f820e8af8b75b86",
      },
    ]);
    for (const a of MIGRATION_RENAME_ALIASES)
      expect(fileSha(a.migration)).toBe(a.accepted_as_alias_of);
    expect(fileSha(ingest!.repair)).toBe(ingest!.repair_sha);
    // The repair sorts after both release names, so it always runs after them.
    expect(ingest!.repair > ingest!.migration && ingest!.repair > event!.migration).toBe(true);
  });

  /** Codes of each refusal, by alias index. */
  const codes = (r: ReturnType<typeof resolveRenamedLedger>) => r.refusals.map((x) => [x.alias.migration, x.code]);

  test("the ingest row is not adopted unless its forward repair is planned with the pinned sha", () => {
    for (const repairSha of ["f".repeat(64), undefined]) {
      const ledger = original();
      const before = new Map(ledger);
      const sha = (name: string) => (name === ingest!.repair ? repairSha : fileSha(name));
      expect(renameAliasRepairMissing(ingest!, sha)).toBe(true);
      expect(renameAliasRepairMissing(event!, sha)).toBe(false);
      const r = resolveRenamedLedger(ledger, sha);
      expect(r.adopted).toEqual([event!]);
      expect(codes(r)).toEqual([[ingest!.migration, "ledger_alias_repair_missing"]]);
      expect(r.effective.get(ingest!.ledger_name)).toBe(before.get(ingest!.ledger_name));
      expect(r.effective.has(ingest!.migration)).toBe(false);
      expect(ledger).toEqual(before);
    }
    expect(renameAliasRepairMissing(ingest!, fileSha)).toBe(false);
  });

  test("both original rows are adopted under their release names, with the release file sha", () => {
    const ledger = original();
    const before = new Map(ledger);
    const r = resolveRenamedLedger(ledger, fileSha);
    expect(r.adopted).toEqual([event!, ingest!]);
    expect(r.refusals).toEqual([]);
    expect(r.effective).toEqual(
      new Map([
        ["upstream:20261102000000_session_briefs.sql", "a".repeat(64)],
        [event!.migration, event!.accepted_as_alias_of],
        [ingest!.migration, ingest!.accepted_as_alias_of],
      ]),
    );
    expect(ledger).toEqual(before); // the physical map is never modified
  });

  test("an original row with another sha (an earlier draft) refuses as ledger_unknown_step", () => {
    const ledger = original();
    ledger.set(ingest!.ledger_name, "d".repeat(64));
    const r = resolveRenamedLedger(ledger, fileSha);
    expect(r.adopted).toEqual([event!]);
    expect(codes(r)).toEqual([[ingest!.migration, "ledger_unknown_step"]]);
    expect(r.effective.get(ingest!.ledger_name)).toBe("d".repeat(64));
    expect(r.effective.has(ingest!.migration)).toBe(false);
  });

  test("a release file with another sha (a future edit) or not planned adopts nothing", () => {
    const ledger = original();
    const originals = new Set<string>(MIGRATION_RENAME_ALIASES.map((a) => a.ledger_name));
    for (const sha of [(n: string) => (originals.has(n) ? undefined : "e".repeat(64)), () => undefined]) {
      const r = resolveRenamedLedger(ledger, sha);
      expect(r.adopted).toEqual([]);
      expect(r.refusals.map((x) => x.code)).toEqual(["ledger_unknown_step", "ledger_unknown_step"]);
      expect(r.effective).toEqual(ledger);
    }
  });

  test("both the original and the release name recorded is a collision, whatever the shas", () => {
    for (const originalSha of [event!.ledger_sha, "d".repeat(64)])
      for (const releaseSha of [event!.accepted_as_alias_of, event!.ledger_sha, "d".repeat(64)]) {
        const ledger = original();
        ledger.set(event!.ledger_name, originalSha);
        ledger.set(event!.migration, releaseSha);
        const r = resolveRenamedLedger(ledger, fileSha);
        expect(r.adopted).toEqual([ingest!]);
        expect(codes(r)).toEqual([[event!.migration, "ledger_alias_collision"]]);
        expect(r.effective.has(event!.migration)).toBe(false);
      }
  });

  test("the release name physically carrying the original sha is hand-edited history: migration_changed", () => {
    const ledger = new Map([[event!.migration, event!.ledger_sha], [ingest!.migration, ingest!.accepted_as_alias_of]]);
    const r = resolveRenamedLedger(ledger, fileSha);
    expect(r.adopted).toEqual([]);
    expect(codes(r)).toEqual([[event!.migration, "migration_changed"]]);
    expect(r.effective).toEqual(new Map([[ingest!.migration, ingest!.accepted_as_alias_of]]));
  });

  test("a release name with an unknown checksum is refused without changing the physical ledger", () => {
    for (const alias of MIGRATION_RENAME_ALIASES) {
      const ledger = new Map(MIGRATION_RENAME_ALIASES.map((a) => [a.migration as string, a.accepted_as_alias_of as string]));
      ledger.set(alias.migration, "0".repeat(64));
      const before = new Map(ledger);
      const result = resolveRenamedLedger(ledger, fileSha);
      expect(codes(result)).toEqual([[alias.migration, "migration_changed"]]);
      expect(result.effective.has(alias.migration)).toBe(false);
      expect(result.adopted).toEqual([]);
      expect(ledger).toEqual(before);
    }
  });

  test("a current ledger passes through unchanged, and a plan holding the original name skips the alias", () => {
    const current = new Map(MIGRATION_RENAME_ALIASES.map((a) => [a.migration as string, a.accepted_as_alias_of as string]));
    expect(resolveRenamedLedger(current, fileSha)).toEqual({ effective: current, adopted: [], refusals: [] });
    const old = original();
    const oldPlan = (name: string) => (name === event!.ledger_name ? event!.ledger_sha : name === ingest!.ledger_name ? ingest!.ledger_sha : undefined);
    expect(resolveRenamedLedger(old, oldPlan)).toEqual({ effective: old, adopted: [], refusals: [] });
  });

  test("the checksum guard never accepts an original sha under the release name (only adoption does)", () => {
    for (const a of MIGRATION_RENAME_ALIASES) {
      expect(migrationChecksumMatches(a.migration, a.ledger_sha, a.accepted_as_alias_of)).toBe(false);
      expect(migrationChecksumAlias(a.migration, a.ledger_sha, a.accepted_as_alias_of)).toBeUndefined();
      expect(migrationChecksumMatches(a.migration, a.accepted_as_alias_of, a.accepted_as_alias_of)).toBe(true);
      expect(migrationChecksumMatches(a.ledger_name, a.ledger_sha, a.accepted_as_alias_of)).toBe(false);
      expect(migrationChecksumMatches(a.migration, "d".repeat(64), a.accepted_as_alias_of)).toBe(false);
      expect(migrationChecksumMatches(a.migration, a.ledger_sha, "e".repeat(64))).toBe(false);
    }
    expect(
      migrationChecksumMatches(event!.migration, ingest!.ledger_sha, event!.accepted_as_alias_of),
    ).toBe(false);
  });

  /** Rewrites a fully migrated ledger to what an image with the original names recorded. */
  async function recordedUnderOriginalNames(db: PGlite) {
    for (const a of MIGRATION_RENAME_ALIASES)
      await db.query(
        "UPDATE sessions.__96bulls_migrations SET name = $1, sha256 = $2 WHERE name = $3",
        [a.ledger_name, a.ledger_sha, a.migration],
      );
  }
  /** Runs `work`, returning the JSON log lines it wrote to stdout. */
  async function logged(work: () => Promise<unknown>) {
    const lines: Record<string, unknown>[] = [];
    const spy = spyOn(process.stdout, "write").mockImplementation(((chunk: unknown) => {
      for (const line of String(chunk).split("\n"))
        if (line.startsWith("{")) lines.push(JSON.parse(line));
      return true;
    }) as never);
    try {
      return { result: await work(), lines };
    } finally {
      spy.mockRestore();
    }
  }

  test("a database migrated under the original names migrates again with nothing pending and no ledger write", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await recordedUnderOriginalNames(db);
    const before = await snapshot(db);
    const { result, lines } = await logged(() =>
      runMigrations(client(db), { profile: LOCAL }),
    );
    expect(result).toMatchObject({ applied: 0 });
    const adopted = lines.filter((l) => l.message === "adopted renamed migration ledger row");
    expect(adopted.map(({ ledger_name, ledger_sha, migration, accepted_as_alias_of }) =>
      ({ ledger_name, ledger_sha, migration, accepted_as_alias_of }))).toEqual(
      [event!, ingest!].map(({ ledger_name, ledger_sha, migration, accepted_as_alias_of }) =>
        ({ ledger_name, ledger_sha, migration, accepted_as_alias_of })),
    );
    // Adoption already carries the release sha: no checksum alias is involved for them.
    const accepted = lines.filter((l) => l.message === "accepted historical migration checksum");
    expect(accepted.map((l) => l.migration)).not.toContain(event!.migration);
    expect(accepted.map((l) => l.migration)).not.toContain(ingest!.migration);
    // The original rows stay exactly as recorded; only the marker generation is rewritten.
    expect((await snapshot(db)).ledger).toEqual(before.ledger);
  });

  test("readiness adopts the original rows like the migrator, and stays schema_pending when they do not match", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    expect(expected.shas?.[ingest!.repair]).toBe(ingest!.repair_sha);
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    await recordedUnderOriginalNames(db);
    expect((await runMigrations(client(db), { profile: LOCAL })).applied).toBe(0);
    const before = await snapshot(db);
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    // Without per-step shas nothing is adopted: both release names are missing.
    const { shas: _shas, ...noShas } = expected;
    expect(await schemaVerdict(noShas, transact(db))).toEqual({ ok: false, reason: "schema_pending", count: 2 });
    // Another planned repair sha: the ingest row is not adopted by readiness either, and
    // the applied repair row no longer matches its planned sha (the migrator's checksum guard).
    expect(
      await schemaVerdict({ ...expected, shas: { ...expected.shas, [ingest!.repair]: "f".repeat(64) } }, transact(db)),
    ).toEqual({ ok: false, reason: "schema_pending", count: 2 });
    // A wrong recorded sha on an original row is never adopted.
    await db.query(
      "UPDATE sessions.__96bulls_migrations SET sha256 = $1 WHERE name = $2",
      ["d".repeat(64), ingest!.ledger_name],
    );
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: false, reason: "schema_pending", count: 1 });
    expect((await snapshot(db)).marker).toEqual(before.marker);
  });

  test("CLI authority at each original or current rename version refuses without ledger or marker writes", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await recordedUnderOriginalNames(db);
    await db.exec("CREATE SCHEMA supabase_migrations; CREATE TABLE supabase_migrations.schema_migrations(version text PRIMARY KEY)");
    const before = await snapshot(db);
    for (const alias of MIGRATION_RENAME_ALIASES) {
      for (const name of [alias.ledger_name, alias.migration]) {
        const version = name.slice("upstream:".length).split("_")[0]!;
        await db.query("INSERT INTO supabase_migrations.schema_migrations(version) VALUES($1)", [version]);
        expect((await failure(runMigrations(client(db), { profile: LOCAL })))?.code).toBe("mixed_migration_authority");
        expect(await snapshot(db)).toEqual(before);
        expect((await db.query("SELECT version FROM supabase_migrations.schema_migrations")).rows).toEqual([{ version }]);
        await db.query("DELETE FROM supabase_migrations.schema_migrations WHERE version=$1", [version]);
      }
    }
    expect((await runMigrations(client(db), { profile: LOCAL })).applied).toBe(0);
    expect((await snapshot(db)).ledger).toEqual(before.ledger);
  });

  test("readiness refuses original/current collisions and bad original hashes without changing physical history or marker", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    const currentOnly = await snapshot(db);
    for (const alias of MIGRATION_RENAME_ALIASES) {
      for (const sha of [alias.ledger_sha, "0".repeat(64)]) {
        await db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)", [alias.ledger_name, sha]);
        const before = await snapshot(db);
        expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: false, reason: "schema_pending", count: 1 });
        expect((await failure(runMigrations(client(db), { profile: LOCAL })))?.code).toBe("ledger_alias_collision");
        expect(await snapshot(db)).toEqual(before);
        // Removing only our injected fixture row restores the unchanged current-only baseline.
        await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [alias.ledger_name]);
        expect(await snapshot(db)).toEqual(currentOnly);
        expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
      }
    }
  });

  test("an exact original ingest row without its exact forward repair refuses as ledger_alias_repair_missing, nothing marked", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await recordedUnderOriginalNames(db);
    const dir = mkdtempSync(join(tmpdir(), "alias-repair-"));
    const changed = join(dir, ingest!.repair.slice("upstream:".length));
    writeFileSync(changed, `${readFileSync(join(root, "supabase/migrations", ingest!.repair.slice("upstream:".length)), "utf8")}\n-- edited\n`);
    const steps = (await plan(LOCAL)).map((st) => (st.name === ingest!.repair ? { ...st, path: changed } : st));
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL, steps }));
    expect(error?.code).toBe("ledger_alias_repair_missing");
    expect(error?.message).toContain(ingest!.ledger_name);
    expect(await snapshot(db)).toEqual(before);
  });

  test("an original name with another sha still refuses as ledger_unknown_step, nothing marked", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await recordedUnderOriginalNames(db);
    await db.query(
      "UPDATE sessions.__96bulls_migrations SET sha256 = $1 WHERE name = $2",
      ["d".repeat(64), ingest!.ledger_name],
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("ledger_unknown_step");
    expect(error?.message).toContain(ingest!.ledger_name);
    expect(error?.message).not.toContain(event!.ledger_name);
    expect(await snapshot(db)).toEqual(before);
  });

  test("a release-name row carrying the original sha (hand-edited history) refuses as migration_changed, nothing written, readiness pending", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    for (const a of [event!, ingest!]) {
      await db.query(
        "UPDATE sessions.__96bulls_migrations SET sha256 = $1 WHERE name = $2",
        [a.ledger_sha, a.migration],
      );
      const before = await snapshot(db);
      const error = await failure(runMigrations(client(db), { profile: LOCAL }));
      expect(error?.code).toBe("migration_changed");
      expect(error?.message).toContain(a.migration);
      expect(await snapshot(db)).toEqual(before);
      expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: false, reason: "schema_pending", count: 1 });
      await db.query(
        "UPDATE sessions.__96bulls_migrations SET sha256 = $1 WHERE name = $2",
        [a.accepted_as_alias_of, a.migration],
      );
      expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    }
  });
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
      "granted to g6_wl by postgres with ADMIN=true",
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
      "unapproved table privilege",
      "GRANT TRUNCATE ON sessions.session_index TO g6_bad",
      "TRUNCATE on sessions.session_index",
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
      "GRANT EXECUTE ON FUNCTION sessions.session_directory_readiness() TO g6_bad",
      "EXECUTE on sessions.session_directory_readiness()",
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
      "a direct table privilege in an inaccessible schema",
      "CREATE SCHEMA g6_hidden; CREATE TABLE g6_hidden.g6_direct (x int); GRANT SELECT ON g6_hidden.g6_direct TO g6_bad",
      "has privileges on table g6_hidden.g6_direct",
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

  test("Supabase PG16+ creator-edge predicate has no wildcard or mutable option", () => {
    const cfg = { mode: "supabase" as const };
    const edge = {
      rolname: "postgres",
      grantor: "supabase_admin",
      grantor_super: true,
      admin_option: true,
      inherit_option: false,
      set_option: false,
    };
    expect(isSupabaseCreatorEdge(cfg, 170011, edge)).toBe(true);
    expect(isSupabaseCreatorEdge({ mode: "selfhosted" }, 170011, edge)).toBe(
      false,
    );
    expect(isSupabaseCreatorEdge(cfg, 150019, edge)).toBe(false);
    expect(
      isSupabaseCreatorEdge(
        { ...cfg, workload: "authenticator" },
        170011,
        edge,
      ),
    ).toBe(true);
    for (const mutation of [
      { rolname: "relayhistory_owner" },
      { grantor: "ops_admin" },
      { grantor_super: false },
      { admin_option: false },
      { inherit_option: true },
      { set_option: true },
    ])
      expect(isSupabaseCreatorEdge(cfg, 170011, { ...edge, ...mutation })).toBe(
        false,
      );
  });

  test("RDS PG16+ creator-edge predicate is bound to rds mode, the connected owner and rdsadmin", () => {
    const cfg = { mode: "rds" as const };
    const edge = {
      rolname: "rds_master",
      grantor: "rdsadmin",
      grantor_super: true,
      admin_option: true,
      inherit_option: false,
      set_option: false,
    };
    expect(isRdsCreatorEdge(cfg, 170011, edge, "rds_master")).toBe(true);
    for (const mode of ["local", "selfhosted", "cloudsql", "supabase"] as const)
      expect(isRdsCreatorEdge({ mode }, 170011, edge, "rds_master")).toBe(false);
    expect(isRdsCreatorEdge(cfg, 150019, edge, "rds_master")).toBe(false);
    expect(isRdsCreatorEdge(cfg, 170011, edge, "someone_else")).toBe(false);
    for (const mutation of [
      { rolname: "other_rds_superuser_member" },
      { grantor: "rds_master" },
      { grantor: "supabase_admin" },
      { grantor_super: false },
      { admin_option: false },
      { inherit_option: true },
      { set_option: true },
    ])
      expect(
        isRdsCreatorEdge(cfg, 170011, { ...edge, ...mutation }, "rds_master"),
      ).toBe(false);
    // The Supabase predicate never matches in rds mode, and vice versa.
    expect(
      isSupabaseCreatorEdge(cfg, 170011, {
        ...edge,
        rolname: "postgres",
        grantor: "supabase_admin",
      }),
    ).toBe(false);
  });

  test("runtime workload edge is exact, inheriting and granted only by the hosted creator or a superuser", () => {
    const cfg = {
      mode: "supabase" as const,
      workload: "bulls96_workload",
    };
    const edge = {
      rolname: "bulls96_workload",
      grantor: "postgres",
      grantor_super: false,
      admin_option: false,
      inherit_option: true,
      set_option: true,
    };
    expect(isRuntimeWorkloadEdge(cfg, edge, true)).toBe(true);
    expect(isRuntimeWorkloadEdge(cfg, edge)).toBe(false);
    expect(
      isRuntimeWorkloadEdge(
        { mode: "selfhosted", workload: "bulls96_workload" },
        { ...edge, grantor: "cluster_admin", grantor_super: true },
      ),
    ).toBe(true);
    for (const mutation of [
      { rolname: "second_workload" },
      { grantor: "app_owner" },
      { admin_option: true },
      { inherit_option: false },
      { set_option: false },
    ])
      expect(isRuntimeWorkloadEdge(cfg, { ...edge, ...mutation }, true)).toBe(
        false,
      );
    expect(isRuntimeWorkloadEdge({ mode: "supabase" }, edge)).toBe(false);
  });

  test("Supabase PUBLIC TEMP compatibility accepts only the frozen indirect baseline", () => {
    const cfg = { mode: "supabase" as const };
    const evidence = {
      effective: true,
      direct: false,
      public: true,
      public_grantable: false,
    };
    expect(isSupabasePublicTemp(cfg, evidence)).toBe(true);
    expect(isSupabasePublicTemp({ mode: "selfhosted" }, evidence)).toBe(false);
    expect(
      isSupabasePublicTemp({ ...cfg, workload: "authenticator" }, evidence),
    ).toBe(true);
    for (const mutation of [
      { effective: false },
      { direct: true },
      { public: false },
      { public_grantable: true },
    ])
      expect(isSupabasePublicTemp(cfg, { ...evidence, ...mutation })).toBe(
        false,
      );
  });
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
    await db.exec("CREATE ROLE g6_wl LOGIN; GRANT g6_rt TO g6_wl");
    expect(
      await failure(
        runMigrations(client(db), {
          profile: { ...GOOD, workloadRole: "g6_wl" },
        }),
      ),
    ).toBeNull();
  });

  test("an altered runtime RLS policy fails closed before the marker can be rewritten", async () => {
    const db = await migrated();
    await db.exec(
      "ALTER POLICY bulls96_trusted_backend ON sessions.pipeline_jobs USING (false)",
    );
    const before = await state(db);
    const error = await failure(
      runMigrations(client(db), { profile: GOOD }),
    );
    expect(error?.code).toBe("runtime_role_unsafe");
    expect(error?.message).toContain(
      "unapproved policy bulls96_trusted_backend on sessions.pipeline_jobs",
    );
    expect(await state(db)).toEqual(before);
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

describe("the ordered migration lanes: contiguous prefixes, no unledgered footprint", () => {
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

  test("a later upstream prefix extends an already-complete earlier 96bulls lane", async () => {
    const db = await fresh();
    const current = await plan(LOCAL);
    const oldImage = current.filter(
      (step) =>
        !step.name.startsWith("verify:") &&
        (!step.name.startsWith("upstream:") ||
          step.name <=
            "upstream:20261030000000_relayhistory_machines_compat.sql"),
    );
    await runMigrations(client(db), {
      profile: { ...LOCAL, verify: false },
      steps: oldImage,
    });
    const before = await db.query<{ name: string }>(
      "SELECT name FROM sessions.__96bulls_migrations ORDER BY name",
    );
    expect(before.rows.some(({ name }) => name.startsWith("96bulls:"))).toBe(
      true,
    );
    expect(
      before.rows.some(
        ({ name }) =>
          name === "upstream:20261030010000_session_directory.sql",
      ),
    ).toBe(false);

    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await schemaVerdict(await expectedSchema(), transact(db))).toEqual({
      ok: true,
    });
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
    // Single-sourced bootstrap chain: the grant migration FAILs closed
    // without the bare consumer grantee.
    for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(readFileSync(join(root, f), "utf8"));
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
    // Single-sourced bootstrap chain: the grant migration FAILs closed
    // without the bare consumer grantee.
    for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(readFileSync(join(root, f), "utf8"));
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

  // dogpatch P1: a login that is a member OF bulls96_acquisition_installer could SET ROLE to it and call the
  // SECURITY DEFINER acquisition RPCs directly. Both the role bootstrap and the 043850 admission block refuse
  // any inbound member except Supabase's PG16+ creator edge (postgres, granted by superuser supabase_admin,
  // ADMIN TRUE, INHERIT FALSE, SET FALSE). PGlite is PostgreSQL 16+, so the edge itself is exercised here.
  const ACQ = "upstream:20261105043850_ingest_acquisition_owner.sql";
  const INSTALLER = "bulls96_acquisition_installer";
  // PGlite's bootstrap superuser is postgres, and PG16+ records every superuser grant as made by the bootstrap
  // superuser, so the Supabase catalog shape (grantor supabase_admin, Supabase's bootstrap superuser) is forged
  // by a superuser catalog UPDATE of the grantor. Test fixture only.
  const creatorEdge = (opts: string, grantorSuper = true) =>
    `CREATE ROLE supabase_admin SUPERUSER NOLOGIN; SET ROLE supabase_admin; GRANT ${INSTALLER} TO postgres WITH ${opts}; RESET ROLE;
     UPDATE pg_auth_members SET grantor = 'supabase_admin'::regrole WHERE roleid = '${INSTALLER}'::regrole AND member = 'postgres'::regrole;` +
    (grantorSuper ? "" : " ALTER ROLE supabase_admin NOSUPERUSER;");
  const SUPABASE_EDGE = creatorEdge("ADMIN TRUE, INHERIT FALSE, SET FALSE");
  const INBOUND: [string, string][] = [
    ["a LOGIN member", `CREATE ROLE g6_attacker LOGIN; GRANT ${INSTALLER} TO g6_attacker`],
    ["a NOLOGIN member", `CREATE ROLE g6_attacker NOLOGIN; GRANT ${INSTALLER} TO g6_attacker WITH INHERIT FALSE, SET TRUE`],
    ["the creator-edge shape with SET TRUE", creatorEdge("ADMIN TRUE, INHERIT FALSE, SET TRUE")],
    ["the creator-edge shape with INHERIT TRUE", creatorEdge("ADMIN TRUE, INHERIT TRUE, SET FALSE")],
    ["the creator-edge shape without ADMIN", creatorEdge("ADMIN FALSE, INHERIT FALSE, SET FALSE")],
    ["the creator-edge shape granted by a non-superuser supabase_admin", creatorEdge("ADMIN TRUE, INHERIT FALSE, SET FALSE", false)],
    ["the creator edge plus a LOGIN member", `${SUPABASE_EDGE} CREATE ROLE g6_attacker LOGIN; GRANT ${INSTALLER} TO g6_attacker`],
  ];
  const acquisitionPrefix = async () => {
    const steps = (await plan(LOCAL)).filter(
      (st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
    );
    const i = steps.findIndex((st) => st.name === ACQ);
    expect(i).toBeGreaterThan(0);
    return { db: await prefix(i - 1, i), path: steps[i]!.path };
  };
  const applyAcquisition = async (db: PGlite, path: string) => {
    await db.exec("BEGIN");
    try { await db.exec(readFileSync(path, "utf8")); await db.exec("COMMIT"); }
    catch (error) { await db.exec("ROLLBACK"); throw error; }
  };
  const installerGrants = async (db: PGlite) =>
    (await db.query<{ n: number }>(
      "SELECT count(*)::int n FROM pg_proc p, aclexplode(p.proacl) a WHERE a.grantee = $1::regrole AND a.privilege_type = 'EXECUTE'",
      [INSTALLER],
    )).rows[0]!.n;
  const BOOTSTRAP = join(root, "supabase/bootstrap/session_ingest_roles.sql");

  for (const [label, sql] of INBOUND) {
    test(`installer inbound membership (${label}): the role bootstrap refuses 55000`, async () => {
      const { db } = await acquisitionPrefix();
      await db.exec(sql);
      const error = await failure(db.exec(readFileSync(BOOTSTRAP, "utf8")));
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(`bootstrap ${INSTALLER}: the role has an unexpected member`);
    });

    test(`installer inbound membership (${label}): the 043850 admission block refuses 55000 before any RPC grant`, async () => {
      const { db, path } = await acquisitionPrefix();
      await db.exec(sql);
      const error = await failure(applyAcquisition(db, path));
      expect(error?.code).toBe("55000");
      expect(error?.message).toBe(`acquisition: role ${INSTALLER} has an unexpected member`);
      expect(await installerGrants(db)).toBe(0);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.ingest_acquisition_owners')::text r")).rows[0]!.r).toBeNull();
    });
  }

  test("installer inbound membership: the exact Supabase PG16+ creator edge is accepted by the bootstrap and the 043850 admission block", async () => {
    const { db, path } = await acquisitionPrefix();
    await db.exec(SUPABASE_EDGE);
    expect(await failure(db.exec(readFileSync(BOOTSTRAP, "utf8")))).toBeNull();
    expect(await failure(applyAcquisition(db, path))).toBeNull();
    expect(await installerGrants(db)).toBe(4);
  });

  test("installer inbound membership: with no member, the bootstrap and the 043850 admission block pass; adding a member afterwards makes the bootstrap refuse", async () => {
    const { db, path } = await acquisitionPrefix();
    expect(await failure(db.exec(readFileSync(BOOTSTRAP, "utf8")))).toBeNull();
    expect(await failure(applyAcquisition(db, path))).toBeNull();
    expect(await installerGrants(db)).toBe(4);
    await db.exec(INBOUND[0]![1]);
    expect((await failure(db.exec(readFileSync(BOOTSTRAP, "utf8"))))?.code).toBe("55000");
  });

  // PGlite is PostgreSQL 16+, and server_version_num is a read-only preset GUC, so the pre-16 branches are
  // exercised by pinning the version the file reads to 150000 (exactly one substitution each). Everything else
  // runs as shipped. CI's real PostgreSQL 15 job covers the unsimulated path.
  const pre16 = (sql: string, from: string, to: string) => {
    expect(sql.split(from).length).toBe(2);
    return sql.replace(from, to);
  };
  const BOOTSTRAP_PRE16 = () =>
    pre16(readFileSync(BOOTSTRAP, "utf8"), "v_version int := current_setting('server_version_num')::int;", "v_version int := 150000;");
  const PRE16_MEMBERS: [string, string, string][] = [
    ["a LOGIN member", `CREATE ROLE g6_attacker LOGIN; GRANT ${INSTALLER} TO g6_attacker`, "g6_attacker"],
    ["the exact Supabase creator edge (no creator-edge exception before PG16)", SUPABASE_EDGE, "postgres"],
    ["two members (the first by rolname is named)", `CREATE ROLE "Zz Attacker" LOGIN; CREATE ROLE g6_attacker LOGIN; GRANT ${INSTALLER} TO "Zz Attacker", g6_attacker`, `"Zz Attacker"`],
  ];
  for (const [label, sql, named] of PRE16_MEMBERS)
    test(`installer inbound membership, simulated pre-16 (${label}): the role bootstrap refuses 55000 naming the member`, async () => {
      const { db } = await acquisitionPrefix();
      await db.exec(sql);
      const error = await failure(db.exec(BOOTSTRAP_PRE16()));
      expect(error?.code).toBe("55000");
      expect(error?.message).toBe(`bootstrap ${INSTALLER}: the role has an unexpected member ${named}`);
    });

  test("installer inbound membership, simulated pre-16: with no member, the bootstrap and the 043850 admission block pass", async () => {
    const { db, path } = await acquisitionPrefix();
    expect(await failure(db.exec(BOOTSTRAP_PRE16()))).toBeNull();
    const migration = pre16(readFileSync(path, "utf8"), "IF current_setting('server_version_num')::int>=160000 THEN", "IF 150000>=160000 THEN");
    await db.exec("BEGIN");
    await db.exec(migration);
    await db.exec("COMMIT");
    expect(await installerGrants(db)).toBe(4);
  });

  test("installer inbound membership, simulated pre-16: the 043850 admission block refuses any member 55000 before any RPC grant", async () => {
    const { db, path } = await acquisitionPrefix();
    await db.exec(SUPABASE_EDGE);
    const migration = pre16(readFileSync(path, "utf8"), "IF current_setting('server_version_num')::int>=160000 THEN", "IF 150000>=160000 THEN");
    await db.exec("BEGIN");
    const error = await failure(db.exec(migration));
    await db.exec("ROLLBACK");
    expect(error?.code).toBe("55000");
    expect(error?.message).toBe(`acquisition: role ${INSTALLER} has an unexpected member`);
    expect(await installerGrants(db)).toBe(0);
  });

  test("installer inbound membership (PG16+ path): the bootstrap names the member", async () => {
    const { db } = await acquisitionPrefix();
    await db.exec(INBOUND[0]![1]);
    expect((await failure(db.exec(readFileSync(BOOTSTRAP, "utf8"))))?.message).toBe(
      `bootstrap ${INSTALLER}: the role has an unexpected member g6_attacker`,
    );
  });

  // Reviewer batch P1(b): the acquisition installer verifier is phase 2's last step, so a member added after
  // 043850 ran (which the per-migration admission block can no longer see) keeps the marker pending.
  const ACQ_VERIFY = "verify:acquisition_installer";
  const MARKER = "SELECT status FROM sessions.__96bulls_role_validation WHERE name = 'session_directory_search_worker'";
  const marker = async (db: PGlite) => (await db.query<{ status: string }>(MARKER)).rows[0]?.status;
  const VERIFIER = join(root, "supabase/bootstrap/acquisition_installer_verify.sql");
  // INHERIT FALSE, SET TRUE: the member gains nothing implicitly but can SET ROLE to the installer.
  const SET_ONLY_MEMBER = `CREATE ROLE g6_attacker LOGIN; GRANT ${INSTALLER} TO g6_attacker WITH INHERIT FALSE, SET TRUE`;

  const FEED_VERIFY = "verify:buffered_search_feed_runtime";
  test("acquisition installer verifier: wired in phase 2 of every verifying plan, just before the buffered feed runtime verifier (last); absent with verify off", async () => {
    for (const profile of [LOCAL, { ...LOCAL, bootstrap: false }, { ...LOCAL, mode: "selfhosted" as const, shim: false, bootstrap: false }]) {
      const verify = (await plan(profile)).filter((st) => st.name.startsWith("verify:")).map((st) => st.name);
      expect(verify.slice(-2)).toEqual([ACQ_VERIFY, FEED_VERIFY]);
      expect(verify.filter((n) => n === ACQ_VERIFY)).toHaveLength(1);
      expect(verify.filter((n) => n === FEED_VERIFY)).toHaveLength(1);
      expect((await plan(profile)).find((st) => st.name === ACQ_VERIFY)?.path).toBe(VERIFIER);
      expect((await plan(profile)).find((st) => st.name === FEED_VERIFY)?.path).toBe(join(root, "supabase/bootstrap/buffered_search_feed_runtime_verify.sql"));
    }
    expect((await plan({ ...LOCAL, verify: false })).some((st) => st.name.startsWith("verify:"))).toBe(false);
  });

  test("acquisition installer verifier, full migrate path: a SET-only LOGIN member is refused 55000 by the bootstrap, then by the verifier with the bootstrap off; the marker stays pending; removing it passes", async () => {
    const db = await fresh();
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await marker(db)).toBe("pass");
    const grants = await installerGrants(db);
    expect(grants).toBe(4);
    await db.exec(SET_ONLY_MEMBER);
    const can = (await db.query<{ set: boolean; usage: boolean }>(
      `SELECT pg_has_role('g6_attacker', '${INSTALLER}', 'SET') AS set, pg_has_role('g6_attacker', '${INSTALLER}', 'USAGE') AS usage`,
    )).rows[0];
    expect(can).toEqual({ set: true, usage: false });
    const boot = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(boot?.code).toBe("55000");
    expect(boot?.message).toBe(`bootstrap ${INSTALLER}: the role has an unexpected member g6_attacker`);
    expect(await marker(db)).toBe("pending");
    // Every schema step is ledgered, so with the bootstrap off only phase 2 runs: the acquisition verifier refuses.
    const seen: string[] = [];
    const verifierOnly = await failure(runMigrations(client(db), {
      profile: { ...LOCAL, bootstrap: false },
      beforeVerify: async () => { seen.push("verify"); },
    }));
    expect(verifierOnly?.code).toBe("55000");
    expect(verifierOnly?.message).toBe("acquisition installer has an unexpected member");
    // Phase 2 stops at the acquisition verifier: every verifier up to and including it ran (the buffered feed
    // runtime verifier, after it, did not).
    const verifyNames = (await plan(LOCAL)).filter((st) => st.name.startsWith("verify:")).map((st) => st.name);
    expect(seen.length).toBe(verifyNames.indexOf("verify:acquisition_installer") + 1);
    expect(await marker(db)).toBe("pending");
    expect(await installerGrants(db)).toBe(grants);
    await db.exec("DROP ROLE g6_attacker");
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await marker(db)).toBe("pass");
  });

  test("buffered feed runtime verifier, full migrate path: a SET-only LOGIN member is refused 55000 by the bootstrap, then by the last verifier with the bootstrap off; removing it passes", async () => {
    const FEED = "buffered_search_feed_runtime";
    const db = await fresh();
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await marker(db)).toBe("pass");
    await db.exec(`CREATE ROLE g6_feed_attacker LOGIN; GRANT ${FEED} TO g6_feed_attacker WITH INHERIT FALSE, SET TRUE`);
    const boot = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(boot?.code).toBe("55000");
    expect(boot?.message).toBe(`bootstrap ${FEED}: the role has an unexpected member g6_feed_attacker`);
    expect(await marker(db)).toBe("pending");
    const seen: string[] = [];
    const verifierOnly = await failure(runMigrations(client(db), {
      profile: { ...LOCAL, bootstrap: false },
      beforeVerify: async () => { seen.push("verify"); },
    }));
    expect(verifierOnly?.code).toBe("55000");
    expect(verifierOnly?.message).toBe("buffered search feed runtime has an unexpected member");
    expect(seen.length).toBe((await plan(LOCAL)).filter((st) => st.name.startsWith("verify:")).length);
    expect(await marker(db)).toBe("pending");
    await db.exec("DROP ROLE g6_feed_attacker");
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await marker(db)).toBe("pass");
  });

  test("acquisition installer verifier, fresh database: a member that predates the first migrate is refused 55000 by the bootstrap with zero installer grants and no acquisition tables", async () => {
    const db = await fresh();
    await db.exec(`CREATE ROLE ${INSTALLER} NOLOGIN NOINHERIT; ${SET_ONLY_MEMBER}`);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("55000");
    expect(error?.message).toBe(`bootstrap ${INSTALLER}: the role has an unexpected member g6_attacker`);
    expect(await installerGrants(db)).toBe(0);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.ingest_acquisition_owners')::text r")).rows[0]!.r).toBeNull();
    expect((await snapshot(db)).marker?.every((r) => (r as { status: string }).status === "pending") ?? true).toBe(true);
    await db.exec("DROP ROLE g6_attacker");
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await installerGrants(db)).toBe(4);
    expect(await marker(db)).toBe("pass");
  });

  test("acquisition installer verifier: the exact Supabase creator edge passes only with hoopdb.db_mode=supabase", async () => {
    const db = await fresh();
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    await db.exec(SUPABASE_EDGE);
    const verify = async (mode: string) => {
      await db.exec("BEGIN");
      try {
        await db.query("SELECT set_config('hoopdb.db_mode', $1, true)", [mode]);
        return await failure(db.exec(readFileSync(VERIFIER, "utf8")));
      } finally { await db.exec("ROLLBACK"); }
    };
    expect(await verify("supabase")).toBeNull();
    for (const mode of ["local", "selfhosted", ""])
      expect(await verify(mode)).toMatchObject({ code: "55000", message: "acquisition installer has an unexpected member" });
  });

  // Reviewer batch P2-a: operationUuid is global. The same uuid replayed in another scope is operation_conflict
  // (40001) before any write, whatever the scope. The concurrent (held-open) cases are in the real-PostgreSQL suite.
  test("acquisition operationUuid and target grant id are global: the same uuid or target in another scope, one after the other, is operation_conflict / target_mismatch / revoked_before_install with zero writes; non-READ COMMITTED is invalid_request", async () => {
    const db = await fresh();
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    const US = "\x1f";
    const sha = (s: string) => createHash("sha256").update(s).digest("hex");
    const FIELDS = ["operationUuid", "orgId", "workspaceId", "source", "sessionId", "revision", "expectedRevision", "expectedOwnerEpoch",
      "expectedPredecessorGrantId", "ownerKind", "ownerUserId", "ownerOriginId", "bindingUuid", "installationId", "grantSha256", "expiresAt", "issuerKeyId"];
    const install = (op: string, source: string) => {
      const g = "rhg_" + "0123456789abcdef".repeat(4);
      const p: Record<string, string> = { operationUuid: op, orgId: "org_test", workspaceId: "ws_test", source, sessionId: "sess-op-1",
        revision: "1", expectedRevision: "0", expectedOwnerEpoch: "0", expectedPredecessorGrantId: "", ownerKind: "cloud",
        ownerUserId: "user_test", ownerOriginId: "origin_test", bindingUuid: crypto.randomUUID(), installationId: "",
        grantSha256: sha(["relayhistory-acquisition-grant-v1", "org_test", "ws_test", source, "sess-op-1", op, g].join(US)),
        expiresAt: "", issuerKeyId: "test-key-1" };
      const a = ["relayhistory-acquisition-assertion-install-v1", ...FIELDS.map((f) => p[f])].join(US);
      const evidenceSha256 = sha("synthetic-evidence" + US + a);
      return { ...p, evidenceSha256, requestSha256: sha(["relayhistory-acquisition-request-v1", sha(a), evidenceSha256].join(US)) };
    };
    const TABLES = ["owners", "grants", "grant_origins", "revoked_grants", "revoked_lineages", "operations"];
    const state = async () => {
      const out: Record<string, unknown> = {};
      for (const t of TABLES)
        out[t] = (await db.query<{ j: unknown }>(`SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text), '[]') j FROM sessions.ingest_acquisition_${t} x`)).rows[0]!.j;
      return out;
    };
    await db.exec("SELECT sessions.ingest_acquisition_set_hooks(true); UPDATE sessions.ingest_acquisition_switch SET install_enabled = true");
    for (const source of ["droid", "codex"]) {
      const scope = JSON.stringify({ orgId: "org_test", workspaceId: "ws_test", source });
      await db.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb)", [scope]);
      await db.query("SELECT sessions.ingest_acquisition_enroll_complete($1::jsonb)", [scope]);
    }
    const revoke = (op: string, source: string, session: string, target: string, lineage: string) => {
      const p: Record<string, string> = { operationUuid: op, orgId: "org_test", workspaceId: "ws_test", source, sessionId: session,
        revision: "1", targetGrantId: target, targetOwnerEpoch: "1", targetOwnerKind: "cloud", targetBindingUuid: lineage,
        targetInstallationId: "", issuerKeyId: "test-key-1" };
      const a = ["relayhistory-acquisition-assertion-revoke-v1", ...REVOKE_FIELDS.map((f) => p[f])].join(US);
      const evidenceSha256 = sha("synthetic-evidence" + US + a);
      return { ...p, evidenceSha256, requestSha256: sha(["relayhistory-acquisition-request-v1", sha(a), evidenceSha256].join(US)) };
    };
    const REVOKE_FIELDS = ["operationUuid", "orgId", "workspaceId", "source", "sessionId", "revision", "targetGrantId",
      "targetOwnerEpoch", "targetOwnerKind", "targetBindingUuid", "targetInstallationId", "issuerKeyId"];
    const asInstaller = async (body: Record<string, string>, fn = "install", iso = "") => {
      await db.exec(`BEGIN${iso ? " ISOLATION LEVEL " + iso : ""}; SET LOCAL ROLE ${INSTALLER}`);
      try {
        const r = (await db.query<{ r: unknown }>(`SELECT sessions.ingest_acquisition_${fn}($1::jsonb) r`, [JSON.stringify(body)])).rows[0]!.r;
        await db.exec("COMMIT");
        return r;
      } catch (error) { await db.exec("ROLLBACK"); throw error; }
    };
    const op = crypto.randomUUID();
    const first = install(op, "droid");
    expect(await asInstaller(first)).toMatchObject({ outcome: "installed", grantId: op });
    const before = await state();
    expect(await failure(asInstaller(install(op, "codex")))).toMatchObject({ code: "40001", message: "acquisition: operation_conflict" });
    expect(await state()).toEqual(before);
    // The original request still replays, and a fresh uuid installs in the other scope.
    expect(await asInstaller(first)).toMatchObject({ outcome: "replay", grantId: op });
    // The same scope with any other field (here a new binding) is also operation_conflict.
    expect(await failure(asInstaller(install(op, "droid")))).toMatchObject({ code: "40001", message: "acquisition: operation_conflict" });
    expect(await asInstaller(install(crypto.randomUUID(), "codex"))).toMatchObject({ outcome: "installed" });
    // Revoke-vs-revoke: the same operation uuid in another scope is operation_conflict; the same target grant id
    // (revoke-before-install) from another scope is target_mismatch; neither writes a fence or an operation.
    const rop = crypto.randomUUID(), target = crypto.randomUUID(), lineage = crypto.randomUUID();
    const r1 = revoke(rop, "droid", "sess-rv-1", target, lineage);
    expect(await asInstaller(r1, "revoke")).toEqual({ outcome: "revoked_pending", revision: "0", ownerEpoch: "0", state: null });
    const held = await state();
    expect(await failure(asInstaller(revoke(rop, "codex", "sess-rv-1", crypto.randomUUID(), lineage), "revoke"))).toMatchObject({ code: "40001", message: "acquisition: operation_conflict" });
    expect(await failure(asInstaller(revoke(crypto.randomUUID(), "codex", "sess-rv-1", target, lineage), "revoke"))).toMatchObject({ code: "42501", message: "acquisition: target_mismatch" });
    expect(await state()).toEqual(held);
    expect(await asInstaller(r1, "revoke")).toMatchObject({ outcome: "replay" });
    // Install-vs-revoke on one grant id: the revoked target never installs in either scope; a revoke from another
    // scope naming an installed grant is target_mismatch.
    for (const source of ["droid", "codex"])
      expect(await failure(asInstaller(install(target, source)))).toMatchObject({ code: "55000", message: "acquisition: revoked_before_install" });
    expect(await state()).toEqual(held);
    expect(await failure(asInstaller(revoke(crypto.randomUUID(), "codex", "sess-op-1", op, first.bindingUuid), "revoke"))).toMatchObject({ code: "42501", message: "acquisition: target_mismatch" });
    expect(await state()).toEqual(held);
    // A retained snapshot (REPEATABLE READ / SERIALIZABLE) is refused before any lock or write.
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      expect(await failure(asInstaller(install(crypto.randomUUID(), "codex"), "install", iso))).toMatchObject({ code: "22023", message: "acquisition: invalid_request" });
      expect(await failure(asInstaller(revoke(crypto.randomUUID(), "codex", "sess-rv-2", crypto.randomUUID(), lineage), "revoke", iso))).toMatchObject({ code: "22023", message: "acquisition: invalid_request" });
    }
    expect(await state()).toEqual(held);
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
  const manifestWithRuntime = (role: string) =>
    Object.fromEntries(
      Object.entries(MANIFEST).map(([fn, entries]) => [
        fn,
        RUNTIME_APPLICATION_FUNCTIONS.includes(
          fn as (typeof RUNTIME_APPLICATION_FUNCTIONS)[number],
        )
          ? [...(entries as string[]), `${role}:EXECUTE`]
          : entries,
      ]),
    );

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
    // Single-sourced bootstrap chain: the grant migration FAILs closed
    // without the bare consumer grantee.
    for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(readFileSync(join(root, f), "utf8"));
    await db.exec(`CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}`);
    return db;
  }
  const ABSENT_EVENT_TRIGGER = {
    name: "hoopdb_sessions_function_boundary",
    present: false,
    event: null,
    tags: [],
    tags_exact: false,
    enabled: null,
    enabled_ok: false,
    owner: null,
    owner_is_migration_owner: false,
    function: null,
    function_is_reviewed: false,
    function_owner: null,
    function_owner_is_migration_owner: false,
    schema_owner_is_migration_owner: false,
    prosrc_sha256: null,
    prosrc_exact: false,
    proconfig: [],
    proconfig_exact: false,
    security_definer: null,
    volatility: null,
    language: null,
    returns: null,
    function_acl: [],
    function_public_execute: true,
    runtime_role_setting: null,
    runtime_role_setting_matches: false,
    schema_private: false,
    verified: false,
  };
  /** The reviewed bootstrap, without psql meta-commands (PGlite runs plain SQL). */
  const ET_BOOT = readFileSync(
    join(
      root,
      "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
    ),
    "utf8",
  )
    .split("\n")
    .filter((l) => !l.startsWith("\\"))
    .join("\n")
    .replaceAll(":'runtime_role'", "'g6_rt'");
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
      db_mode: "supabase",
      excluded_platform_superusers: [],
      sessions_schema_function_defaults: [],
      sessions_functions_owned_by_owner: true,
      event_trigger_present: false,
      event_trigger: ABSENT_EVENT_TRIGGER,
      other_event_triggers: [],
      mode: "none",
      satisfied: false,
    });
    // Names, booleans, hashes and nulls only (in arrays and objects too).
    const leaves = (v: unknown): unknown[] =>
      v !== null && typeof v === "object"
        ? Object.values(v as object).flatMap(leaves)
        : [v];
    for (const v of leaves(evidence))
      expect(v === null || ["string", "boolean"].includes(typeof v)).toBe(true);
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
    // Blast radius: the owner's global default reaches every schema (why an operator decides it).
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

  // Option (ii): in supabase mode only, a role that is BOTH rolsuper (catalog, at gate
  // time) AND named in SUPABASE_PLATFORM_SUPERUSERS is the platform trust root and is left
  // out of creator coverage, with its attributes in the evidence.
  const SUPA_ET: MigrationProfile = {
    ...SUPA,
    functionBoundary: "event-trigger",
  };
  const SUPA_A: MigrationProfile = {
    ...SUPA,
    functionBoundary: "global-default-revoke",
  };
  const platformList = [...SUPABASE_PLATFORM_SUPERUSERS];

  test("window-two down/reapply through the real migrator: later migrations refuse out-of-order rollback and the next run restores the gap-free prefix", async () => {
    const db = await supabaseLike();
    await db.exec(OWNER_FN_DEFAULT_REVOKE);
    expect(await failure(runMigrations(client(db), { profile: SUPA_A }))).toBeNull();
    const B = "upstream:20261105024000_session_brief_documents.sql";
    const J = "upstream:20261105025000_canonical_feature_journal.sql";
    const L = "upstream:20261105026000_canonical_history_lineage.sql";
    const C = "upstream:20261105030000_canonical_feature_consumer_acks.sql";
    const D = "upstream:20261105031000_desktop_catalog_forget_reproject.sql";
    const K = "upstream:20261105032000_session_large_read_routing_fixes.sql";
    const P = "upstream:20261105033000_canonical_project_events.sql";
    const S = "upstream:20261105035000_semantic_copy_state.sql";
    const E = "upstream:20261105036000_canonical_consumer_lease.sql";
    const F = "upstream:20261105037000_session_directory_document_pool.sql";
    const R = "upstream:20261105034000_session_directory_read_api.sql";
    const T = "upstream:20261105038000_claim_attempt_records_retirement.sql";
    const G = "upstream:20261105039000_buffered_gc_single_candidate.sql";
    const Z = "upstream:20261105040000_ingest_lazy_source_tables.sql";
    const Y = "upstream:20261105041000_ingest_privacy_authority.sql";
    const H = "upstream:20261105043000_brief_titles.sql";
    const N = "upstream:20261105043500_lazy_claim_clock_narrowing.sql";
    const O = "upstream:20261105043700_owner_buffered_read.sql";
    const Q = "upstream:20261105043800_lazy_ack_clock_no_key_update.sql";
    const W = "upstream:20261105043860_buffered_search_gate.sql";
    const X = "upstream:20261105043870_buffered_search_feed_cursor.sql";
    const CG = "upstream:20261105043900_session_consumer_function_grants.sql";
    const SE = "upstream:20261105043910_selfhost_session_selection_erase.sql";
    const SL = "upstream:20261105044010_selfhost_erase_lock_fences.sql";
    const BK = "upstream:20261105043880_ingest_bookkeeping_cleanup.sql";
    const V = "upstream:20261105044030_ingest_generation_overlap.sql";
    const GR = "upstream:20261105043890_buffered_search_gate_retired_witness.sql";
    const TK = "upstream:20261105044000_read_api_tokens.sql";
    const CB = "upstream:20261105044020_claim_bootstrap_org_indexes.sql";
    const SF = "upstream:20261105044040_buffered_stages_session_first.sql";
    const SP = "upstream:20261105044050_selfhost_erase_short_summary_provenance.sql";
    const DG = "upstream:20261105045000_digest_narrative_cache.sql";
    const names = async () => (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations")).rows.map((x) => x.name);
    const passed = async () => (await db.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows.every((x) => x.status === "pass");
    const rollback = async (name: string) => {
      try { return await db.exec(readFileSync(join(root, `supabase/rollbacks/${name}`), "utf8")); }
      catch (error) { await db.exec("ROLLBACK"); throw error; }
    };
    expect(await names()).toEqual(expect.arrayContaining([B, J, L, R, C, D, K, P, S, E, F, T, G, Z, Y, H, N, O, Q, W, X, BK, GR, CG, SE, SL, TK, CB, V, SF, SP, DG]));
    expect(await names()).toEqual(expect.arrayContaining([B, J, L, R, C, D, K, P, S, E, F, T, G, Z, Y, H, N, O, Q, W, X, BK, GR, CG, SE, SL, CB, V, SF, SP]));
    expect(await passed()).toBe(true);
    const outOfOrder = await failure(
      rollback("20261105024000_session_brief_documents.down.sql"),
    );
    expect(outOfOrder?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([B, J, L, R, C, D, K, P, S, E, F, T, G, Z, Y, H, N, O, Q, W, X, BK, GR, CG, SE, SL, TK, V, DG]));
    // P3b (034000) refuses while a later lane (035000, 036000, 037000) is applied.
    expect((await failure(rollback("20261105034000_session_directory_read_api.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(R);
    // 033000 cannot be removed under 034000 or any later ledgered step.
    expect((await failure(rollback("20261105033000_canonical_project_events.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(P);
    // Later lanes remain applied: 035000, 032000, 031000 and 030000 all refuse while it is applied.
    expect((await failure(rollback("20261105035000_semantic_copy_state.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105031000_desktop_catalog_forget_reproject.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105030000_canonical_feature_consumer_acks.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([C, D, K, S, E]));
    const relation = async (name: string) =>
      (await db.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [name])).rows[0]?.r ?? null;
    const lazySrc = async () =>
      (await db.query<{ s: string }>("SELECT string_agg(prosrc, '' ORDER BY oid) s FROM pg_proc WHERE oid IN ('sessions.ingest_lazy_user(jsonb,text,jsonb)'::regprocedure, 'sessions.ingest_lazy_begin(jsonb,text)'::regprocedure)")).rows[0]!.s;
    const catalogRpc = async () =>
      (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.session_directory_desktop_catalog(text,text,text)'::regprocedure")).rows[0]!.s;
    const routeRpc = async () =>
      (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.route_session_large_read(text,text,text,text)'::regprocedure")).rows[0]!.s;
    // 030000 cannot roll back under 031000/032000/035000/036000; 031000 cannot roll back under 032000;
    // 032000 cannot roll back under 035000/036000 or any later ledgered step.
    expect((await failure(rollback("20261105030000_canonical_feature_consumer_acks.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.canonical_feature_copy_state")).toBe("sessions.canonical_feature_copy_state");
    expect((await failure(rollback("20261105031000_desktop_catalog_forget_reproject.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.session_desktop_catalog_reproject")).toBe("sessions.session_desktop_catalog_reproject");
    expect((await failure(rollback("20261105032000_session_large_read_routing_fixes.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.session_large_read_scope_errors")).toBe("sessions.session_large_read_scope_errors");
    const LATER = "upstream:20261105099000_rollback_guard_probe.sql";
    await db.query(
      "INSERT INTO sessions.__96bulls_migrations SELECT (jsonb_populate_record(NULL::sessions.__96bulls_migrations, to_jsonb(m) || jsonb_build_object('name', $2::text))).* FROM sessions.__96bulls_migrations m WHERE m.name = $1",
      [K, LATER],
    );
    expect((await failure(rollback("20261105032000_session_large_read_routing_fixes.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.session_large_read_scope_errors")).toBe("sessions.session_large_read_scope_errors");
    await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name = $1", [LATER]);
    // 20261105045000 (digest narrative cache) is the newest: its own down drops the table first.
    // (044050 rolls back next, explicitly below, so no helper call here.)
    expect(await names()).toContain(DG);
    expect(await relation("sessions.daily_digest_narratives")).toBe("sessions.daily_digest_narratives");
    await rollback("20261105045000_digest_narrative_cache.down.sql");
    expect(await names()).not.toContain(DG);
    expect(await relation("sessions.daily_digest_narratives")).toBeNull();
    // 20261105044040 (session-first buffered stages) is the newest: 044030's down refuses (55000) while it is
    // 20261105044050 (erase clears the short_summary provenance) is the newest: 044040's, 044030's and 044010's downs refuse
    // (55000) while it is applied, and its own down restores 044010's exact erase (the lock, without the provenance clear)
    // and 060000's exact retire trigger function (still SECURITY DEFINER, both triggers kept).
    const eraseSrc = async () => (await db.query<{ s: string | null }>("SELECT prosrc s FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_selfhost_set_session_uploads(jsonb)')")).rows[0]!.s;
    const retireSrc = async () => (await db.query<{ s: string | null }>("SELECT prosrc s FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_publication_retire_fence()') AND prosecdef")).rows[0]?.s ?? null;
    const retireTriggers = async () => (await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_trigger WHERE tgname IN ('ingest_publication_retire_version','ingest_publication_retire_reset') AND tgfoid=to_regprocedure('sessions.ingest_publication_retire_fence()')")).rows[0]!.n;
    expect(await eraseSrc()).toContain("short_summary_generated_at=NULL");
    expect(await retireSrc()).toContain("short_summary_generated_at=NULL");
    expect(await retireTriggers()).toBe(2);
    expect((await failure(rollback("20261105044040_buffered_stages_session_first.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105044030_ingest_generation_overlap.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105044010_selfhost_erase_lock_fences.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([SF, V, SL]));
    await rollback("20261105044050_selfhost_erase_short_summary_provenance.down.sql");
    expect(await names()).not.toContain(SP);
    expect(await eraseSrc()).not.toContain("short_summary_generated_at");
    expect(await eraseSrc()).toContain("ingest_lock_fences(v_org,v_ws,v_user)");
    expect(await retireSrc()).toContain("UPDATE sessions.session_directory d SET short_summary=NULL,\n   deleted_at=");
    expect(await retireSrc()).not.toContain("short_summary_generated_at");
    expect(await retireTriggers()).toBe(2);
    // Its ledger row is gone, so running it again refuses and changes nothing.
    expect((await failure(rollback("20261105044050_selfhost_erase_short_summary_provenance.down.sql")))?.code).toBe("55000");
    // 20261105044040 (session-first buffered stages) is then the newest: 044030's down refuses (55000) while it is
    // applied; its own down restores 043700's exact two reader bodies (same ACL, still SECURITY DEFINER) and
    // keeps the records GIN, and a second run refuses (its ledger row is gone).
    const readerSrc = async () =>
      (await db.query<{ s: string }>("SELECT string_agg(prosrc, '' ORDER BY oid) s FROM pg_proc WHERE oid IN (to_regprocedure('sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,text,boolean,text)'), to_regprocedure('sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)'))")).rows[0]!.s;
    const readerAcl = async () =>
      (await db.query<{ a: string }>("SELECT string_agg(proname || ':' || prosecdef || ':' || coalesce(proacl::text, ''), ',' ORDER BY proname) a FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname IN ('buffered_sessions_for', 'buffered_session_stages_for')")).rows[0]!.a;
    const original = readFileSync(join(root, "supabase/migrations/20261105043700_owner_buffered_read.sql"), "utf8");
    const aclBefore = await readerAcl();
    expect(await names()).toContain(SF);
    expect(await readerSrc()).toContain("count(*) OVER (PARTITION BY rc.stage_token, rc.ord)");
    expect(await relation("sessions.ingest_claim_attempts_records_gin_idx")).toBe("sessions.ingest_claim_attempts_records_gin_idx");
    expect((await failure(rollback("20261105044030_ingest_generation_overlap.down.sql")))?.code).toBe("55000");
    await rollback("20261105044040_buffered_stages_session_first.down.sql");
    expect(await names()).not.toContain(SF);
    expect(await readerSrc()).not.toContain("count(*) OVER (PARTITION BY");
    for (const [fn, tag] of [["buffered_sessions_for", "buffered_sessions"], ["buffered_session_stages_for", "buffered_stages"]] as const) {
      const body = original.match(new RegExp(`CREATE FUNCTION sessions\\.${fn}\\([\\s\\S]*?AS \\$${tag}\\$([\\s\\S]*?)\\$${tag}\\$;`))![1]!;
      expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname = $1", [fn])).rows[0]!.s).toBe(body);
    }
    expect(await readerAcl()).toBe(aclBefore);
    expect(await relation("sessions.ingest_claim_attempts_records_gin_idx")).toBe("sessions.ingest_claim_attempts_records_gin_idx");
    expect((await failure(rollback("20261105044040_buffered_stages_session_first.down.sql")))?.code).toBe("55000");
    // 20261105044030 (generation overlap) is then the newest: 044020, 044010, 044000, 043910, 043900 and 043880 refuse (55000)
    // while it is applied, so it rolls back FIRST; its own down restores the exact strict 050000
    // ingest_require_generation and 055000 ingest_lazy_guard, drops the window and retired tables, the policy
    // retirement trigger and its six functions, and marks the validation pending (the fence bodies changed).
    const overlapFns = async () =>
      (await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'ingest_generation_overlap_%'")).rows[0]!.n;
    const fenceSrc = async () =>
      (await db.query<{ s: string }>("SELECT string_agg(prosrc, '' ORDER BY oid) s FROM pg_proc WHERE oid IN ('sessions.ingest_require_generation(text)'::regprocedure, 'sessions.ingest_lazy_guard(text)'::regprocedure)")).rows[0]!.s;
    expect(await names()).toContain(V);
    expect(await overlapFns()).toBe(6);
    expect(await fenceSrc()).toContain("ingest_generation_overlap_admits");
    expect((await failure(rollback("20261105044020_claim_bootstrap_org_indexes.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105044010_selfhost_erase_lock_fences.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105044000_read_api_tokens.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043910_selfhost_session_selection_erase.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043900_session_consumer_function_grants.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043880_ingest_bookkeeping_cleanup.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([CB, SL, SE, CG, BK]));
    expect(await relation("sessions.read_api_tokens")).toBe("sessions.read_api_tokens");
    await rollback("20261105044030_ingest_generation_overlap.down.sql");
    expect(await names()).not.toContain(V);
    expect(await overlapFns()).toBe(0);
    expect(await relation("sessions.ingest_generation_overlap_windows")).toBeNull();
    expect(await relation("sessions.ingest_generation_overlap_retired")).toBeNull();
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_trigger WHERE tgname='ingest_generation_overlap_policy_retire'")).rows[0]!.n).toBe(0);
    expect(await fenceSrc()).not.toContain("ingest_generation_overlap");
    expect(await fenceSrc()).toContain("AND generation=p_expected AND status='pass' FOR SHARE;");
    expect(await passed()).toBe(false);
    // 20261105044020 (claim-bootstrap org_id indexes) is then the newest: 044010's and 044000's downs refuse (55000)
    // while it is applied; its own down drops the eight indexes first.
    expect((await failure(rollback("20261105044010_selfhost_erase_lock_fences.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105044000_read_api_tokens.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(SL);
    expect(await relation("sessions.read_api_tokens")).toBe("sessions.read_api_tokens");
    expect(await relation("sessions.ingest_seed_captures_org_id_idx")).toBe("sessions.ingest_seed_captures_org_id_idx");
    // Every later upstream step is already rolled back here (045000, 044040 and 044030, explicitly above), so no helper call.
    const laterNames = (await plan(SUPA_ET)).filter((s) => s.name.startsWith("upstream:") && s.name > CB).map((s) => s.name);
    expect((await names()).filter((n) => laterNames.includes(n))).toEqual([]);
    await rollback("20261105044020_claim_bootstrap_org_indexes.down.sql");
    expect(await names()).not.toContain(CB);
    expect(await relation("sessions.ingest_seed_captures_org_id_idx")).toBeNull();
    // 20261105044010 (erase takes the fence lock) is then the newest: 043910's down refuses (55000) while it is
    // applied, and its own down restores 043910's exact body, without the lock.
    expect(await eraseSrc()).toContain("ingest_lock_fences(v_org,v_ws,v_user)");
    expect((await failure(rollback("20261105043910_selfhost_session_selection_erase.down.sql")))?.code).toBe("55000");
    await rollback("20261105044010_selfhost_erase_lock_fences.down.sql");
    expect(await names()).not.toContain(SL);
    expect(await eraseSrc()).not.toContain("ingest_lock_fences");
    // Its ledger row is gone, so running it again refuses and changes nothing.
    expect((await failure(rollback("20261105044010_selfhost_erase_lock_fences.down.sql")))?.code).toBe("55000");
    // 20261105044000 (read API tokens) is then the latest: its own down drops the table.
    expect(await relation("sessions.read_api_tokens")).toBe("sessions.read_api_tokens");
    await rollback("20261105044000_read_api_tokens.down.sql");
    expect(await relation("sessions.read_api_tokens")).toBeNull();
    // 20261105043910 (self-host per-session erase) is then the newest: its down drops the function, and
    // 20261105043900's down refuses (55000) while it is applied.
    expect((await failure(rollback("20261105043900_session_consumer_function_grants.down.sql")))?.code).toBe("55000");
    await rollback("20261105043910_selfhost_session_selection_erase.down.sql");
    expect(await names()).not.toContain(SE);
    expect((await db.query<{ p: string | null }>("SELECT to_regprocedure('sessions.ingest_selfhost_set_session_uploads(jsonb)')::text AS p")).rows[0]!.p).toBeNull();
    // 20261105043900 (consumer function grants) is then the latest: earlier downs refuse (55000)
    // while it is applied; its own down revokes the twelve search_consumer_runtime
    // EXECUTEs and marks the validation pending.
    const writerGrants = async () =>
      (await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, aclexplode(p.proacl) a JOIN pg_roles r ON r.oid = a.grantee WHERE n.nspname = 'sessions' AND r.rolname = 'search_consumer_runtime' AND a.privilege_type = 'EXECUTE'")).rows[0]!.n;
    expect(await names()).toContain(CG);
    expect(await writerGrants()).toBe(12);
    expect((await failure(rollback("20261105043890_buffered_search_gate_retired_witness.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043880_ingest_bookkeeping_cleanup.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([GR, BK]));
    await rollback("20261105043900_session_consumer_function_grants.down.sql");
    expect(await names()).not.toContain(CG);
    expect(await writerGrants()).toBe(0);
    expect(await passed()).toBe(false);
    // 043890 (gate retired witness) is then the latest: 043880 refuses (55000) while it is applied, so it
    // rolls back next; its down restores 043860's exact gate (no retired witness), owner and ACL unchanged.
    const gateSrc = async () =>
      (await db.query<{ s: string | null }>("SELECT prosrc s FROM pg_proc WHERE oid=to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)')")).rows[0]?.s ?? null;
    const gateGrantees = async () =>
      (await db.query<{ g: string[] }>("SELECT array(SELECT r.rolname::text FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)') AND a.privilege_type='EXECUTE' AND a.grantee<>p.proowner ORDER BY 1) g")).rows[0]!.g;
    expect(await names()).toContain(GR);
    expect(await gateSrc()).toContain("ingest_bookkeeping_retired_claims");
    expect(await gateGrantees()).toEqual(["bulls96_read_api"]);
    expect((await failure(rollback("20261105043880_ingest_bookkeeping_cleanup.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(BK);
    await rollback("20261105043890_buffered_search_gate_retired_witness.down.sql");
    expect(await names()).not.toContain(GR);
    expect(await gateSrc()).not.toContain("ingest_bookkeeping_retired_claims");
    expect(await gateGrantees()).toEqual(["bulls96_read_api"]);
    // 043880 (ingest bookkeeping cleanup, Phase A) is then the latest: 043870 refuses (55000) while it is
    // applied, so it rolls back next; its down restores 042000's exact Babysitter register.
    expect(await names()).toContain(BK);
    expect((await failure(rollback("20261105043870_buffered_search_feed_cursor.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(X);
    await rollback("20261105043880_ingest_bookkeeping_cleanup.down.sql");
    expect(await names()).not.toContain(BK);
    expect(await relation("sessions.ingest_bookkeeping_retired_claims")).toBeNull();
    // 043870 (buffered search feed cursor) is then the latest: 043860 and 043850 refuse (55000) while it is applied;
    // its own down drops only the three feed functions, the cursor table and its ledger row (the validation stays as it was).
    const feedFns = async () =>
      (await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE oid IN (to_regprocedure('sessions.buffered_search_feed_page(jsonb)'), to_regprocedure('sessions.buffered_search_feed_stage(jsonb)'), to_regprocedure('sessions.buffered_search_feed_advance(jsonb)'))")).rows[0]!.n;
    expect(await names()).toContain(X);
    expect(await feedFns()).toBe(3);
    expect(await relation("sessions.buffered_search_feed_cursors")).toBe("sessions.buffered_search_feed_cursors");
    expect((await failure(rollback("20261105043860_buffered_search_gate.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043850_ingest_acquisition_owner.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([W, "upstream:20261105043850_ingest_acquisition_owner.sql"]));
    await rollback("20261105043870_buffered_search_feed_cursor.down.sql");
    expect(await names()).not.toContain(X);
    expect(await feedFns()).toBe(0);
    expect(await relation("sessions.buffered_search_feed_cursors")).toBeNull();
    // The down revokes the runtime role's schema USAGE and keeps the role itself.
    expect((await db.query<{ u: boolean | null }>("SELECT (SELECT has_schema_privilege(r.oid, 'sessions', 'USAGE') FROM pg_roles r WHERE r.rolname = 'buffered_search_feed_runtime') AS u")).rows[0]!.u).toBe(false);
    // Merged tree: the consumer-grants down above already marked validation
    // pending, and the feed down keeps it as it was.
    expect(await passed()).toBe(false);
    // 043860 (buffered search gate reader) is then the latest: 043800 and 043700 refuse (55000) while it is applied;
    // its own down drops only the one function and its ledger row (the validation stays as it was).
    const gateFn = async () =>
      (await db.query<{ r: string | null }>("SELECT to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)')::text r")).rows[0]?.r ?? null;
    expect(await names()).toContain(W);
    expect(await gateFn()).not.toBeNull();
    expect((await failure(rollback("20261105043800_lazy_ack_clock_no_key_update.down.sql")))?.code).toBe("55000");
    expect((await failure(rollback("20261105043700_owner_buffered_read.down.sql")))?.code).toBe("55000");
    expect(await names()).toEqual(expect.arrayContaining([O, Q]));
    await rollback("20261105043860_buffered_search_gate.down.sql");
    expect(await names()).not.toContain(W);
    expect(await gateFn()).toBeNull();
    // Merged tree: validation is still pending from the consumer-grants down above.
    expect(await passed()).toBe(false);
    // Later lanes roll back first and remove their own ledger rows; only then
    // 043850 (acquisition owner, dormant) is the latest: 043000 refuses while it is applied.
    const AQ = "upstream:20261105043850_ingest_acquisition_owner.sql";
    expect(await names()).toContain(AQ);
    expect((await failure(rollback("20261105043000_brief_titles.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(H);
    await rollback("20261105043850_ingest_acquisition_owner.down.sql");
    expect(await names()).not.toContain(AQ);
    expect(await relation("sessions.ingest_acquisition_owners")).toBeNull();
    // 043800 (staged-ACK clock NO KEY UPDATE) is the latest: 043700 and 043500 refuse (55000) while it is
    // applied; its own down restores the exact 043500 ACK body (clock FOR UPDATE) and marks the validation pending.
    const lazyAck = async () =>
      (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure")).rows[0]!.s;
    expect(await names()).toContain(Q);
    expect(await lazyAck()).toContain("WHERE singleton FOR NO KEY UPDATE;");
    expect((await failure(rollback("20261105043700_owner_buffered_read.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(O);
    expect((await failure(rollback("20261105043500_lazy_claim_clock_narrowing.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(N);
    await rollback("20261105043800_lazy_ack_clock_no_key_update.down.sql");
    expect(await names()).not.toContain(Q);
    expect(await passed()).toBe(false);
    expect(await lazyAck()).not.toContain("FOR NO KEY UPDATE");
    expect(await lazyAck()).toContain("WHERE singleton FOR UPDATE;");
    expect(await lazyAck()).toContain("WHERE singleton FOR KEY SHARE;");
    // 043700 (owner buffered read) is then the latest: 043500 and 043000 refuse (55000) while it is applied;
    // its own down removes its four functions and parity table.
    const ownerReadFns = async () =>
      (await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE proname IN ('owner_buffered_read_scope','owner_buffered_parity_expected','buffered_sessions_for','buffered_session_stages_for')")).rows[0]!.n;
    const parityTable = async () => relation("sessions.owner_buffered_parity");
    expect(await names()).toContain(O);
    expect(await ownerReadFns()).toBe(4);
    expect(await parityTable()).toBe("sessions.owner_buffered_parity");
    expect((await failure(rollback("20261105043500_lazy_claim_clock_narrowing.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(N);
    expect((await failure(rollback("20261105043000_brief_titles.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(H);
    await rollback("20261105043700_owner_buffered_read.down.sql");
    expect(await names()).not.toContain(O);
    expect(await ownerReadFns()).toBe(0);
    expect(await parityTable()).toBeNull();
    // 043500 (lazy claim clock narrowing, a non-1000 step between 043000 and 044000) is then the latest: 043000
    // refuses (55000) while it is applied; its own down restores the exact 055000 bodies and marks the
    // validation pending, and the reapply below restores PASS.
    const lazyRecords = async () =>
      (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_lazy_records(jsonb)'::regprocedure")).rows[0]!.s;
    expect(await names()).toContain(N);
    expect(await lazyRecords()).toContain("lazy-native-origin-v1");
    expect((await failure(rollback("20261105043000_brief_titles.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(H);
    await rollback("20261105043500_lazy_claim_clock_narrowing.down.sql");
    expect(await names()).not.toContain(N);
    expect(await passed()).toBe(false);
    expect(await lazyRecords()).not.toContain("lazy-native-origin-v1");
    expect(await lazyRecords()).toContain("WHERE singleton FOR UPDATE;");
    // Then 043000 (F2 brief titles) is the latest: 042000, 041000, 040000 and 039000 all refuse
    // (55000) while it is applied, so it rolls back first.
    expect(await names()).toContain(H);
    expect((await failure(rollback("20261105042000_babysitter_evidence_retention.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain("upstream:20261105042000_babysitter_evidence_retention.sql");
    expect((await failure(rollback("20261105041000_ingest_privacy_authority.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(Y);
    expect((await failure(rollback("20261105040000_ingest_lazy_source_tables.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(Z);
    expect((await failure(rollback("20261105039000_buffered_gc_single_candidate.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(G);
    await rollback("20261105043000_brief_titles.down.sql");
    expect(await names()).not.toContain(H);
    // Roll back the complete 041000..038000 suffix in descending order before earlier lanes.
    // 042000 (babysitter retention) is the latest lane: 041000 refuses until it rolls back.
    const BR = "upstream:20261105042000_babysitter_evidence_retention.sql";
    expect(await names()).toContain(BR);
    expect((await failure(rollback("20261105041000_ingest_privacy_authority.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(Y);
    await rollback("20261105042000_babysitter_evidence_retention.down.sql");
    expect(await names()).not.toContain(BR);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.babysitter_retention_objects')::text r")).rows[0]?.r).toBeNull();
    // 041000 is the latest lane: 040000 refuses while it is applied; its own down needs Neon authority (no row).
    expect((await failure(rollback("20261105040000_ingest_lazy_source_tables.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(Z);
    await db.query("INSERT INTO sessions.ingest_privacy_authority(state,control_epoch) VALUES('fenced',$1)", ["3".repeat(64)]);
    expect((await failure(rollback("20261105041000_ingest_privacy_authority.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(Y);
    await db.query("DELETE FROM sessions.ingest_privacy_authority");
    // 041000's own down refuses while any later upstream migration is ledgered.
    await db.query(
      "INSERT INTO sessions.__96bulls_migrations SELECT (jsonb_populate_record(NULL::sessions.__96bulls_migrations, to_jsonb(m) || jsonb_build_object('name', $2::text))).* FROM sessions.__96bulls_migrations m WHERE m.name = $1",
      [Y, "upstream:20261105041500_later_fixture.sql"],
    );
    expect((await failure(rollback("20261105041000_ingest_privacy_authority.down.sql")))?.code).toBe("55000");
    await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name = $1", ["upstream:20261105041500_later_fixture.sql"]);
    expect((await lazySrc()).match(/NOT IN\('ingest_frozen_source_records'/g)?.length).toBe(2);
    await rollback("20261105041000_ingest_privacy_authority.down.sql");
    expect(await names()).not.toContain(Y);
    // The 041000 down restores the EXACT 040000 lazy bodies (prefix exclusion), not 055000's.
    expect((await lazySrc()).split("NOT LIKE 'ingest\\_frozen\\_source\\_%'").length).toBe(3);
    expect(await lazySrc()).not.toContain("NOT IN('ingest_frozen_source_records'");
    expect(await relation("sessions.ingest_privacy_authority")).toBeNull();
    // 040000 is the latest lane: 039000 refuses while it is applied.
    expect((await failure(rollback("20261105039000_buffered_gc_single_candidate.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(G);
    await rollback("20261105040000_ingest_lazy_source_tables.down.sql");
    expect(await names()).not.toContain(Z);
    expect(await passed()).toBe(false);
    expect((await failure(rollback("20261105038000_claim_attempt_records_retirement.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(T);
    // 045000 (lane F4 digest narratives) was already rolled back newest-first at the
    // top of this test via rollbackLaterThan044020 (it was the latest lane); its table stays gone.
    expect(await names()).not.toContain(DG);
    expect(await relation("sessions.daily_digest_narratives")).toBeNull();
    // 039000 (buffered GC) is now the latest lane: it rolls back directly.
    expect(await names()).toContain(G);
    await rollback("20261105039000_buffered_gc_single_candidate.down.sql");
    expect(await names()).not.toContain(G);
    expect(await passed()).toBe(false);
    // can the next down preserve a gap-free applied prefix. 038000 is the latest lane:
    // 037000 and 036000 refuse while it is applied.
    expect(await names()).toEqual(expect.arrayContaining([F, T]));
    expect((await failure(rollback("20261105037000_session_directory_document_pool.down.sql")))?.code).toBe("55000");
    expect(await names()).toContain(F);
    expect((await failure(rollback("20261105036000_canonical_consumer_lease.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.canonical_feature_consumer_lease")).toBe("sessions.canonical_feature_consumer_lease");
    await rollback("20261105038000_claim_attempt_records_retirement.down.sql");
    expect(await names()).not.toContain(T);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_attribute WHERE attrelid='sessions.ingest_claim_attempts'::regclass AND attname LIKE 'records\\_%' AND NOT attisdropped")).rows[0]?.n).toBe(0);
    await rollback("20261105037000_session_directory_document_pool.down.sql");
    expect(await names()).not.toContain(F);
    await rollback("20261105036000_canonical_consumer_lease.down.sql");
    expect(await names()).not.toContain(E);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.canonical_feature_consumer_lease')::text r")).rows[0]?.r).toBeNull();
    // With 036000 gone, 035000 is still a later ledgered lane: 032000 keeps refusing until it rolls back.
    expect((await failure(rollback("20261105032000_session_large_read_routing_fixes.down.sql")))?.code).toBe("55000");
    expect(await relation("sessions.session_large_read_scope_errors")).toBe("sessions.session_large_read_scope_errors");
    await rollback("20261105035000_semantic_copy_state.down.sql");
    expect(await names()).not.toContain(S);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.search_feature_copy_state')::text r")).rows[0]?.r).toBeNull();
    // 034000 (P3b) is now the latest lane: 032000 refuses until it rolls back (strictly descending).
    expect((await failure(rollback("20261105032000_session_large_read_routing_fixes.down.sql")))?.code).toBe("55000");
    await rollback("20261105034000_session_directory_read_api.down.sql");
    expect(await names()).not.toContain(R);
    expect((await db.query<{ f: string | null }>("SELECT to_regprocedure('sessions.session_directory_activity_for(text,timestamptz,timestamptz)')::text f")).rows[0]?.f).toBeNull();
    // With 034000 gone, 033000 is still a later lane; 032000 must keep refusing.
    expect((await failure(rollback("20261105032000_session_large_read_routing_fixes.down.sql")))?.code).toBe("55000");
    await rollback("20261105033000_canonical_project_events.down.sql");
    expect(await names()).not.toContain(P);
    expect(await relation("sessions.canonical_project_events")).toBeNull();
    expect(await relation("sessions.canonical_project_event_scopes")).toBeNull();
    await rollback("20261105032000_session_large_read_routing_fixes.down.sql");
    expect(await names()).not.toContain(K);
    expect(await relation("sessions.session_large_read_scope_errors")).toBeNull();
    expect(await routeRpc()).toContain("unknown_count");
    await rollback("20261105031000_desktop_catalog_forget_reproject.down.sql");
    expect(await names()).not.toContain(D);
    expect(await relation("sessions.session_desktop_catalog_reproject")).toBeNull();
    expect(await relation("sessions.session_desktop_catalog_control")).toBeNull();
    expect(await catalogRpc()).not.toContain("forget_tracking");
    await rollback("20261105030000_canonical_feature_consumer_acks.down.sql");
    expect(await names()).not.toContain(C);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.canonical_feature_copy_state')::text r")).rows[0]?.r).toBeNull();
    await rollback("20261105026000_canonical_history_lineage.down.sql");
    expect(await names()).not.toContain(L);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.ingest_history_switch')::text r")).rows[0]?.r).toBeNull();
    await rollback("20261105025000_canonical_feature_journal.down.sql");
    expect(await names()).not.toContain(J);
    await rollback("20261105024000_session_brief_documents.down.sql");
    expect(await names()).not.toContain(B);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.session_brief_document_scopes')::text r")).rows[0]?.r).toBeNull();
    // The very next migrate reapplies 024000, 025000, 026000, 030000, 031000, 032000, 033000 (F10), 034000 (P3b),
    // 035000 through 039000, 040000 (lazy source tables) and 041000 (privacy authority); the post-validation
    // exact-ACL check finds no drift.
    expect(await failure(runMigrations(client(db), { profile: SUPA_A }))).toBeNull();
    // Up again: 041000's exact source-table list is back in both lazy functions, 043500's lazy claim
    // narrowing is back, 043700's four functions and parity table, 043800's staged-ACK NO KEY UPDATE,
    // 043860's gate reader (with 043890's retired witness), 043870's feed functions and cursor table and 044030's overlap fence.
    expect(await lazyRecords()).toContain("lazy-native-origin-v1");
    expect(await overlapFns()).toBe(6);
    expect(await fenceSrc()).toContain("ingest_generation_overlap_admits");
    expect(await gateFn()).not.toBeNull();
    expect(await gateSrc()).toContain("ingest_bookkeeping_retired_claims");
    expect(await gateGrantees()).toEqual(["bulls96_read_api"]);
    expect(await feedFns()).toBe(3);
    expect(await relation("sessions.buffered_search_feed_cursors")).toBe("sessions.buffered_search_feed_cursors");
    expect(await lazyAck()).toContain("WHERE singleton FOR NO KEY UPDATE;");
    expect(await ownerReadFns()).toBe(4);
    expect(await parityTable()).toBe("sessions.owner_buffered_parity");
    expect((await lazySrc()).match(/NOT IN\('ingest_frozen_source_records'/g)?.length).toBe(2);
    expect(await names()).toEqual(expect.arrayContaining([B, J, L, R, C, D, K, P, S, E, F, T, G, Z, Y, H, N, O, Q, W, X, BK, GR, CG, SE, SL, V]));
    expect(await relation("sessions.session_desktop_catalog_reproject")).toBe("sessions.session_desktop_catalog_reproject");
    expect(await catalogRpc()).toContain("forget_tracking_not_enabled");
    expect(await relation("sessions.session_large_read_scope_errors")).toBe("sessions.session_large_read_scope_errors");
    expect(await routeRpc()).not.toContain("unknown_count");
    expect(await passed()).toBe(true);
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.canonical_feature_consumer_lease')::text r")).rows[0]?.r).toBe("sessions.canonical_feature_consumer_lease");
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.session_brief_document_scopes')::text r")).rows[0]?.r).toBe("sessions.session_brief_document_scopes");
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.ingest_history_switch')::text r")).rows[0]?.r).toBe("sessions.ingest_history_switch");
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.canonical_feature_copy_state')::text r")).rows[0]?.r).toBe("sessions.canonical_feature_copy_state");
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.search_feature_copy_state')::text r")).rows[0]?.r).toBe("sessions.search_feature_copy_state");
    // And a no-op rerun stays clean.
    expect(await failure(runMigrations(client(db), { profile: SUPA_A }))).toBeNull();
  });

  test("migrate, operator bootstrap, migrate: the claimed LOGIN converges; hand promotion and extra memberships refuse", async () => {
    const profile: MigrationProfile = {
      ...SUPA_A,
      mode: "selfhosted",
      shim: false,
      bootstrap: true,
      verify: true,
      functionBoundary: "global-default-revoke",
    };
    const rolesFile = readFileSync(join(root, "supabase/bootstrap/session_consumer_roles.sql"), "utf8");
    const operatorFile = readFileSync(join(root, "supabase/bootstrap/session_consumer_role.sql"), "utf8");
    // File level, on a bare database: absent creates, bare passes, a
    // hand-promoted LOGIN without the claim marker refuses, and a role
    // granted to another login refuses before any grant migration runs.
    {
      const db = await fresh();
      await db.exec(rolesFile); // absent -> creates the bare role
      await db.exec(rolesFile); // bare -> passes
      const shape = async () =>
        (await db.query<{ login: boolean; setting: string | null }>(
          `SELECT rolcanlogin AS login, (SELECT setconfig FROM pg_db_role_setting WHERE setrole = pg_roles.oid AND setdatabase = 0) AS setting FROM pg_roles WHERE rolname = 'search_consumer_runtime'`,
        )).rows[0];
      expect(await shape()).toMatchObject({ login: false });
      await db.exec(`ALTER ROLE search_consumer_runtime LOGIN`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("28000");
      await db.exec(`ALTER ROLE search_consumer_runtime NOLOGIN`);
      await db.exec(rolesFile); // back to bare -> passes again
      await db.exec(`CREATE ROLE attacker NOLOGIN; GRANT search_consumer_runtime TO attacker`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("55000");
    }
    // Migrate level: a full migrate provisions the bare role, the operator
    // bootstrap claims it (LOGIN plus the timeout marker), and the next
    // migrate converges instead of 28000ing on the promotion. The operator
    // file GRANTs on the writer-owned tables, so the test stands in the
    // composition-built search tables and service tables (production builds
    // them via the writer bootstrap before the operator step; their shape
    // is proven by the installed()/e2e fixtures, not here).
    {
      const db = await supabaseLike();
      await db.exec(OWNER_FN_DEFAULT_REVOKE);
      expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
      await db.exec(`CREATE SCHEMA search_index;
        CREATE TABLE search_index.transcript_events (id serial PRIMARY KEY);
        CREATE TABLE search_index.projection_state (id serial PRIMARY KEY);
        CREATE TABLE sessions.search_consumer_pending (backend_id text, manifest_key text, PRIMARY KEY (backend_id, manifest_key));
        CREATE TABLE sessions.search_consumer_progress (backend_id text PRIMARY KEY);`);
      await db.exec(operatorFile);
      const claimed = await db.query<{ login: boolean }>(
        `SELECT rolcanlogin AS login FROM pg_roles WHERE rolname = 'search_consumer_runtime'`,
      );
      expect(claimed.rows[0]).toMatchObject({ login: true });
      expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
      const marker = await db.query<{ status: string }>(
        `SELECT status FROM sessions.__96bulls_role_validation WHERE name = 'session_directory_search_worker'`,
      );
      expect(marker.rows[0]).toMatchObject({ status: "pass" });
    }
  });

  test("consumer role settings are exact: extras refuse in bare and claimed states, preset credentials refuse promotion", async () => {
    const rolesFile = readFileSync(join(root, "supabase/bootstrap/session_consumer_roles.sql"), "utf8");
    const operatorFile = readFileSync(join(root, "supabase/bootstrap/session_consumer_role.sql"), "utf8");
    const dbName = async (db: PGlite) =>
      (await db.query<{ db: string }>(`SELECT current_database() AS db`)).rows[0].db;
    // Bare + an extra global setting refuses (it would inherit into the promotion).
    {
      const db = await fresh();
      await db.exec(rolesFile);
      await db.exec(`ALTER ROLE search_consumer_runtime SET search_path = attacker, pg_catalog`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("28000");
    }
    // Bare + a per-database extra refuses (rolconfig would still look exact).
    {
      const db = await fresh();
      await db.exec(rolesFile);
      const name = await dbName(db);
      await db.exec(`ALTER ROLE search_consumer_runtime IN DATABASE "${name}" SET search_path = attacker`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("28000");
    }
    // A hand-claimed exact shape (LOGIN + only the timeout marker) passes;
    // an extra global or per-database setting then refuses in the claimed state.
    {
      const db = await fresh();
      await db.exec(rolesFile);
      await db.exec(`ALTER ROLE search_consumer_runtime LOGIN`);
      await db.exec(`ALTER ROLE search_consumer_runtime SET statement_timeout = '60s'`);
      expect(await failure(db.exec(rolesFile))).toBeNull();
      await db.exec(`ALTER ROLE search_consumer_runtime SET search_path = attacker, pg_catalog`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("28000");
      await db.exec(`ALTER ROLE search_consumer_runtime RESET search_path`);
      expect(await failure(db.exec(rolesFile))).toBeNull();
      const name = await dbName(db);
      await db.exec(`ALTER ROLE search_consumer_runtime IN DATABASE "${name}" SET search_path = attacker`);
      expect((await failure(db.exec(rolesFile)))?.code).toBe("28000");
    }
    // A preset password refuses the operator promotion (it would turn the
    // claim into an already-credentialed account); same for VALID UNTIL.
    // PGlite runs as superuser, so pg_authid carries the true value here.
    {
      const db = await fresh();
      await db.exec(rolesFile);
      await db.exec(`ALTER ROLE search_consumer_runtime PASSWORD 'preset-credential'`);
      const err = await failure(db.exec(operatorFile));
      expect(err?.code).toBe("55000");
      expect(err?.message).toMatch(/pre-existing password/);
    }
    {
      const db = await fresh();
      await db.exec(rolesFile);
      await db.exec(`ALTER ROLE search_consumer_runtime VALID UNTIL '2027-01-01'`);
      const err = await failure(db.exec(operatorFile));
      expect(err?.code).toBe("55000");
      expect(err?.message).toMatch(/pre-existing validity/);
    }
  });

  test("grant rollback revokes with the role present, is a no-op with the role absent, and refuses without the ledger", async () => {
    const down = readFileSync(join(root, "supabase/rollbacks/20261105043900_session_consumer_function_grants.down.sql"), "utf8");
    // Newer than 043900, so the grants' down refuses until they are rolled back,
    // newest first: every upstream step after 044020 (rollbackLaterThan044020: 045000, 044050, 044040, 044030),
    // then 044020 (claim-bootstrap indexes), 044010 (erase lock), 044000 (tokens), 043910 (erase).
    // Newer than 043900, so its down refuses until they are rolled back, newest first: every upstream step after
    // 044020 (rollbackLaterThan044020: 044050, 044040, 044030), then 044020, 044010, 044000, 043910.
    const orgIndexesDown = readFileSync(join(root, "supabase/rollbacks/20261105044020_claim_bootstrap_org_indexes.down.sql"), "utf8");
    const lockDown = readFileSync(join(root, "supabase/rollbacks/20261105044010_selfhost_erase_lock_fences.down.sql"), "utf8");
    const readTokensDown = readFileSync(join(root, "supabase/rollbacks/20261105044000_read_api_tokens.down.sql"), "utf8");
    const newerDown = readFileSync(join(root, "supabase/rollbacks/20261105043910_selfhost_session_selection_erase.down.sql"), "utf8");
    const grantRow = `SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043900_session_consumer_function_grants.sql'`;
    const holds = async (db: PGlite) =>
      (await db.query<{ holds: boolean }>(
        `SELECT has_function_privilege('search_consumer_runtime', 'sessions.upsert_session_index(jsonb)', 'EXECUTE') AS holds`,
      )).rows[0].holds;
    // No ledger at all: the rollback refuses (fail closed — nothing to roll back).
    {
      const db = await fresh();
      const err = await failure(db.exec(down));
      expect(err?.code).toBe("55000");
      expect(err?.message).toMatch(/ledger\/marker required/);
    }
    // Present role after a full migrate: the twelve EXECUTEs are installed,
    // then revoked by the rollback and the ledger row is removed.
    {
      const db = await supabaseLike();
      await db.exec(ET_BOOT);
      await runMigrations(client(db), { profile: SUPA_ET });
      expect(await holds(db)).toBe(true);
      await rollbackLaterThan044020(db, SUPA_ET);
      expect(await failure(db.exec(orgIndexesDown))).toBeNull();
      await db.exec(lockDown);
      expect(await failure(db.exec(readTokensDown))).toBeNull();
      await db.exec(newerDown);
      expect(await failure(db.exec(down))).toBeNull();
      expect(await holds(db)).toBe(false);
      expect((await db.query<{ n: number }>(grantRow)).rows[0].n).toBe(0);
    }
    // Role dropped by hand after the grants were installed: the revoke block
    // is a no-op but the rollback still passes and removes the ledger row.
    {
      const db = await supabaseLike();
      await db.exec(ET_BOOT);
      await runMigrations(client(db), { profile: SUPA_ET });
      await db.exec(`DROP OWNED BY search_consumer_runtime`);
      await db.exec(`DROP ROLE search_consumer_runtime`);
      await rollbackLaterThan044020(db, SUPA_ET);
      expect(await failure(db.exec(orgIndexesDown))).toBeNull();
      await db.exec(lockDown);
      expect(await failure(db.exec(readTokensDown))).toBeNull();
      await db.exec(newerDown);
      expect(await failure(db.exec(down))).toBeNull();
      expect((await db.query<{ n: number }>(grantRow)).rows[0].n).toBe(0);
    }
  });

  test("Cloud SQL trust root: only Google's fixed cloudsqladmin SUPERUSER is excluded in cloudsql mode", async () => {
    expect(Object.isFrozen(CLOUDSQL_PLATFORM_SUPERUSERS)).toBe(true);
    expect([...CLOUDSQL_PLATFORM_SUPERUSERS]).toEqual(["cloudsqladmin"]);
    const db = await supabaseLike();
    await db.exec(
      `${OWNER_FN_DEFAULT_REVOKE}; CREATE ROLE cloudsqladmin SUPERUSER NOLOGIN`,
    );
    const cloudsql = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "cloudsql",
    );
    expect(cloudsql.satisfied).toBe(true);
    expect(cloudsql.excluded_platform_superusers).toEqual([
      expect.objectContaining({
        role: "cloudsqladmin",
        rolsuper: true,
        function_default: ["<built-in>"],
      }),
    ]);
    expect(cloudsql.sessions_create_roles.map((c) => c.role)).not.toContain(
      "cloudsqladmin",
    );
    const selfhosted = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "selfhosted",
    );
    expect(selfhosted.satisfied).toBe(false);
    expect(selfhosted.excluded_platform_superusers).toEqual([]);
    expect(selfhosted.sessions_create_roles).toContainEqual(
      expect.objectContaining({
        role: "cloudsqladmin",
        superuser: true,
        function_default_exact: false,
      }),
    );
  });

  test("RDS trust root: only AWS's fixed rdsadmin SUPERUSER is excluded, only in rds mode", async () => {
    expect(Object.isFrozen(RDS_PLATFORM_SUPERUSERS)).toBe(true);
    expect([...RDS_PLATFORM_SUPERUSERS]).toEqual(["rdsadmin"]);
    const db = await supabaseLike();
    await db.exec(
      `${OWNER_FN_DEFAULT_REVOKE}; CREATE ROLE rdsadmin SUPERUSER NOLOGIN; CREATE ROLE rds_lookalike SUPERUSER NOLOGIN`,
    );
    const rds = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "rds",
    );
    // A second superuser stays covered, even with an RDS-like name.
    expect(rds.satisfied).toBe(false);
    expect(rds.excluded_platform_superusers.map((e) => e.role)).toEqual([
      "rdsadmin",
    ]);
    expect(rds.sessions_create_roles).toContainEqual(
      expect.objectContaining({ role: "rds_lookalike", function_default_exact: false }),
    );
    await db.exec("DROP ROLE rds_lookalike");
    expect(
      (await functionBoundaryEvidence(client(db), "global-default-revoke", "rds"))
        .satisfied,
    ).toBe(true);
    for (const mode of ["selfhosted", "cloudsql", "supabase"] as const) {
      const other = await functionBoundaryEvidence(
        client(db),
        "global-default-revoke",
        mode,
      );
      expect(other.satisfied).toBe(false);
      expect(other.excluded_platform_superusers.map((e) => e.role)).not.toContain(
        "rdsadmin",
      );
    }
    // The fixed name excluded only while it is SUPERUSER.
    await db.exec("CREATE SCHEMA IF NOT EXISTS sessions; ALTER ROLE rdsadmin NOSUPERUSER; GRANT CREATE ON SCHEMA sessions TO rdsadmin");
    const demoted = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "rds",
    );
    expect(demoted.excluded_platform_superusers).toEqual([]);
    expect(demoted.satisfied).toBe(false);
  });

  test("Cloud SQL owner and migration-role bootstraps produce catalog-proven, membership-free state", async () => {
    const db = new PGlite({
      username: "cloudsqladmin",
      database: "postgres",
      initDbStartParams: ["--username=cloudsqladmin"],
      extensions: { btree_gin, pg_trgm },
    });
    open.push(db);
    await db.exec(`
      CREATE ROLE cloudsqlsuperuser NOLOGIN;
      CREATE ROLE migration_owner LOGIN CREATEROLE;
      GRANT cloudsqlsuperuser TO migration_owner;
      GRANT migration_owner TO cloudsqladmin;
      GRANT CONNECT, CREATE ON DATABASE postgres TO migration_owner;
      SET ROLE migration_owner;
      CREATE SCHEMA sessions AUTHORIZATION migration_owner;
      CREATE FUNCTION sessions.probe() RETURNS integer LANGUAGE sql AS 'SELECT 1';
    `);
    await db.exec(
      readFileSync(
        join(
          root,
          "supabase/bootstrap/cloudsql_owner_function_default_revoke.sql",
        ),
        "utf8",
      ),
    );
    const evidence = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "cloudsql",
    );
    expect(evidence.satisfied).toBe(true);
    expect(evidence.owner_function_default).toEqual(["<self>:EXECUTE"]);
    expect(evidence.excluded_platform_superusers).toEqual([
      expect.objectContaining({ role: "cloudsqladmin", rolsuper: true }),
    ]);
    await db.exec(
      readFileSync(
        join(root, "supabase/bootstrap/cloudsql_migration_roles.sql"),
        "utf8",
      ),
    );
    await db.exec(
      `SET ROLE migration_owner; ${readFileSync(
        join(root, "supabase/bootstrap/session_directory_search_worker.sql"),
        "utf8",
      )}`,
    );
    // N1: the consumer role is never trampolined. The migration owner
    // provisions it with session_consumer_roles.sql, keeping the unavoidable
    // ADMIN creator edge so the owner can promote it to LOGIN later.
    await db.exec(
      `SET ROLE migration_owner; ${readFileSync(
        join(root, "supabase/bootstrap/session_consumer_roles.sql"),
        "utf8",
      )}`,
    );
    // No superuser role bootstrap may fill a missing Cloud SQL prerequisite here.
    const targets = await db.query<{
      rolname: string;
      rolcanlogin: boolean;
      rolinherit: boolean;
      rolbypassrls: boolean;
      memberships: number;
    }>(
      `SELECT role.rolname, role.rolcanlogin, role.rolinherit, role.rolbypassrls,
              (SELECT count(*)::int FROM pg_auth_members membership
                WHERE membership.roleid = role.oid OR membership.member = role.oid) AS memberships
         FROM pg_roles role
        WHERE role.rolname IN ('anon', 'authenticated', 'service_role', 'session_directory_search_worker', 'session_token_verifier', 'bulls96_session_ingest', 'bulls96_session_ingest_seeder', 'bulls96_canonical_feature_producer', 'bulls96_read_api', 'bulls96_project_events', 'bulls96_digest_narrative', 'bulls96_privacy_eraser', 'bulls96_babysitter_retention', 'search_consumer_runtime', 'bulls96_acquisition_installer', 'buffered_search_feed_runtime')
        ORDER BY 1`,
    );
    expect(targets.rows).toHaveLength(16);
    const trampolined = targets.rows.filter(r => r.rolname !== "search_consumer_runtime");
    expect(trampolined).toHaveLength(15);
    expect(trampolined.every(r => !r.rolcanlogin && !r.rolinherit && !r.rolbypassrls && r.memberships === 0)).toBe(true);
    // The owner-created consumer carries exactly the creator edge: the owner
    // as member WITH ADMIN, neither INHERIT nor SET.
    const consumerEdge = await db.query(
      `SELECT m.rolname AS member, a.admin_option AS admin, a.inherit_option AS inherit, a.set_option AS set
         FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
        WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname = 'search_consumer_runtime')`,
    );
    expect(consumerEdge.rows).toEqual([{ member: "migration_owner", admin: true, inherit: false, set: false }]);
    const manifest = JSON.parse(readFileSync(join(root, "src/hoopdb/sessions-function-acl.json"), "utf8")).functions as Record<string, string[]>;
    const prerequisites = new Set(["anon", ...Object.values(manifest).flat().map(grant => grant.split(":")[0]!).filter(role => role !== "<owner>")]);
    expect(targets.rows.map(r => r.rolname).sort()).toEqual([...prerequisites].sort());
    expect(targets.rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          rolname: "service_role",
          rolbypassrls: false,
        }),
        expect.objectContaining({
          rolname: "session_directory_search_worker",
          rolcanlogin: false,
          rolinherit: false,
          memberships: 0,
        }),
        expect.objectContaining({
          rolname: "bulls96_read_api",
          rolcanlogin: false,
          rolinherit: false,
          rolbypassrls: false,
          memberships: 0,
        }),
      ]),
    );
    expect(
      await db.query(
        `SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_cloudsql_role_seed'`,
      ),
    ).toMatchObject({ rows: [] });
    // Claimed-LOGIN steady state: the operator promotes the consumer role to
    // LOGIN with exactly the 60s marker, and the consumer file re-run
    // converges. Promoting as the owner that created it is the N1 proof: a
    // trampoline copy would refuse these ALTERs with 42501. An extra setting
    // still refuses, and the trampoline re-run ignores the role.
    await db.exec("SET ROLE migration_owner");
    await db.exec(`ALTER ROLE search_consumer_runtime LOGIN`);
    await db.exec(`ALTER ROLE search_consumer_runtime SET statement_timeout = '60s'`);
    expect(await failure(db.exec(readFileSync(join(root, "supabase/bootstrap/session_consumer_roles.sql"), "utf8")))).toBeNull();
    await db.exec(`ALTER ROLE search_consumer_runtime SET search_path = attacker`);
    expect(await failure(db.exec(readFileSync(join(root, "supabase/bootstrap/cloudsql_migration_roles.sql"), "utf8")))).toBeNull();
    // The consumer file owns the claimed-shape refusal now: its 28000 shape error, not the trampoline 55000.
    expect((await failure(db.exec(readFileSync(join(root, "supabase/bootstrap/session_consumer_roles.sql"), "utf8"))))?.code).toBe("28000");
    // The added prerequisite shares the existing fail-closed, never-repair contract.
    await db.exec("RESET ROLE; ALTER ROLE bulls96_read_api INHERIT; SET ROLE migration_owner");
    expect((await failure(db.exec(readFileSync(join(root, "supabase/bootstrap/cloudsql_migration_roles.sql"), "utf8"))))?.code).toBe("55000");
    expect((await db.query<{ rolinherit: boolean }>("SELECT rolinherit FROM pg_roles WHERE rolname='bulls96_read_api'")).rows[0]?.rolinherit).toBe(true);
  });

  test("platform trust root: supabase_admin as a superuser with no safe default is excluded in supabase mode (listed with its attributes); the same catalog fails without the exclusion (selfhosted)", async () => {
    expect(platformList).toEqual(["supabase_admin"]);
    const db = await supabaseLike();
    await db.exec(
      `${OWNER_FN_DEFAULT_REVOKE}; CREATE ROLE supabase_admin SUPERUSER NOLOGIN`,
    );
    const ev = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "supabase",
    );
    expect(ev.satisfied).toBe(true);
    expect(ev.excluded_platform_superusers).toEqual([
      {
        role: "supabase_admin",
        rolsuper: true,
        rolinherit: true,
        rolcreaterole: false,
        rolcreatedb: false,
        rolcanlogin: false,
        rolreplication: false,
        rolbypassrls: false,
        function_default: ["<built-in>"],
      },
    ]);
    expect(ev.sessions_create_roles.map((c) => c.role)).not.toContain(
      "supabase_admin",
    );
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_A })),
    ).toBeNull();
    // Revert-sensitive: with no exclusion (selfhosted, or no mode) the same catalog fails
    // on supabase_admin, so the supabase pass above exists only because of the exclusion.
    for (const other of ["selfhosted", undefined] as const) {
      const full = await functionBoundaryEvidence(
        client(db),
        "global-default-revoke",
        other,
      );
      expect(full.satisfied).toBe(false);
      expect(full.excluded_platform_superusers).toEqual([]);
      expect(full.sessions_create_roles).toContainEqual(
        expect.objectContaining({
          role: "supabase_admin",
          superuser: true,
          function_default_exact: false,
        }),
      );
    }
    const selfError = await failure(
      runMigrations(client(db), {
        profile: { ...SUPA_A, mode: "selfhosted", shim: false },
      }),
    );
    expect(selfError?.code).toBe("function_boundary_unproven");
  });

  test("platform trust root: a non-platform superuser (ops_admin) with no safe default is still covered in supabase mode: refused before any write", async () => {
    const db = await supabaseLike();
    await db.exec(
      `${OWNER_FN_DEFAULT_REVOKE}; CREATE ROLE ops_admin SUPERUSER NOLOGIN`,
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: SUPA_A }));
    expect(error?.code).toBe("function_boundary_unproven");
    const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
    expect(ev.excluded_platform_superusers).toEqual([]);
    expect(ev.sessions_create_roles).toContainEqual(
      expect.objectContaining({
        role: "ops_admin",
        superuser: true,
        function_default_exact: false,
      }),
    );
    expect(await snapshot(db)).toEqual(before);
  });

  test("platform trust root: a role NAMED supabase_admin that is not a superuser, with CREATE on sessions and no safe default, is covered: refused", async () => {
    const db = await supabaseLike();
    await db.exec(OWNER_FN_DEFAULT_REVOKE);
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_A })),
    ).toBeNull();
    await db.exec(
      "CREATE ROLE supabase_admin NOSUPERUSER NOLOGIN; GRANT CREATE, USAGE ON SCHEMA sessions TO supabase_admin",
    );
    const before = await snapshot(db);
    const error = await failure(runMigrations(client(db), { profile: SUPA_A }));
    expect(error?.code).toBe("function_boundary_unproven");
    const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
    expect(ev.excluded_platform_superusers).toEqual([]);
    expect(ev.sessions_create_roles).toContainEqual(
      expect.objectContaining({
        role: "supabase_admin",
        superuser: false,
        function_default_exact: false,
      }),
    );
    expect(await snapshot(db)).toEqual(before);
  });

  test("platform trust root: the migration owner is always covered in supabase mode (no safe default: refused, even with supabase_admin excluded)", async () => {
    const db = await supabaseLike();
    await db.exec("CREATE ROLE supabase_admin SUPERUSER NOLOGIN");
    const ev = await functionBoundaryEvidence(
      client(db),
      "global-default-revoke",
      "supabase",
    );
    expect(ev.satisfied).toBe(false);
    expect(ev.owner_function_default_exact).toBe(false);
    expect(ev.excluded_platform_superusers.map((x) => x.role)).toEqual([
      "supabase_admin",
    ]);
    expect(ev.sessions_create_roles).toContainEqual(
      expect.objectContaining({
        role: "postgres",
        is_migration_owner: true,
        function_default_exact: false,
      }),
    );
    expect(
      (await failure(runMigrations(client(db), { profile: SUPA_A })))?.code,
    ).toBe("function_boundary_unproven");
  });

  test("platform trust root: no environment variable or role name widens the exclusion", async () => {
    const db = await supabaseLike();
    await db.exec(
      `${OWNER_FN_DEFAULT_REVOKE}; CREATE ROLE ops_admin SUPERUSER NOLOGIN`,
    );
    const hostile = {
      SUPABASE_PLATFORM_SUPERUSERS: "ops_admin",
      HOOPDB_SUPABASE_PLATFORM_SUPERUSERS: "ops_admin",
      HOOPDB_PLATFORM_SUPERUSERS: "ops_admin",
      HOOPDB_EXCLUDED_SUPERUSERS: "ops_admin",
      HOOPDB_FUNCTION_BOUNDARY_APPROVED: undefined,
    };
    const saved = Object.fromEntries(
      Object.keys(hostile).map((k) => [k, process.env[k]]),
    );
    try {
      for (const [k, v] of Object.entries(hostile))
        if (v === undefined) delete process.env[k];
        else process.env[k] = v;
      const profile = migrationProfile({
        ...process.env,
        HOOPDB_DB_MODE: "supabase",
        HOOPDB_RUNTIME_ROLE: "g6_rt",
        HOOPDB_RUNTIME_ROLE_LOGIN: "false",
        HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
      });
      const error = await failure(runMigrations(client(db), { profile }));
      expect(error?.code).toBe("function_boundary_unproven");
      const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
      expect(ev.excluded_platform_superusers).toEqual([]);
      expect(ev.sessions_create_roles).toContainEqual(
        expect.objectContaining({
          role: "ops_admin",
          function_default_exact: false,
        }),
      );
      // The list itself is frozen code.
      expect(Object.isFrozen(SUPABASE_PLATFORM_SUPERUSERS)).toBe(true);
      expect([...SUPABASE_PLATFORM_SUPERUSERS]).toEqual(["supabase_admin"]);
    } finally {
      for (const [k, v] of Object.entries(saved))
        if (v === undefined) delete process.env[k];
        else process.env[k] = v;
    }
  });

  test("event-trigger mode is accepted only with HOOPDB_DB_MODE=supabase; selfhosted, cloudsql and local refuse it", () => {
    const env = {
      HOOPDB_RUNTIME_ROLE: "g6_rt",
      HOOPDB_RUNTIME_ROLE_LOGIN: "false",
      HOOPDB_FUNCTION_BOUNDARY_MODE: "event-trigger",
    };
    expect(
      migrationProfile({ ...env, HOOPDB_DB_MODE: "supabase" }).functionBoundary,
    ).toBe("event-trigger");
    for (const mode of ["selfhosted", "cloudsql", "local"])
      expect(() =>
        migrationProfile({
          ...env,
          HOOPDB_DB_MODE: mode,
          HOOPDB_SUPABASE_SHIM: mode === "local" ? "true" : "false",
        }),
      ).toThrow(
        expect.objectContaining({ code: "function_boundary_mode_unsupported" }),
      );
  });

  test("event-trigger mode: a trigger of the right name but another definition never satisfies the gate", async () => {
    const db = await supabaseLike();
    await db.exec(`CREATE FUNCTION public.g6_revoke_sessions_public() RETURNS event_trigger LANGUAGE plpgsql AS $f$
      BEGIN EXECUTE 'REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC'; END $f$;
      CREATE EVENT TRIGGER ${SESSIONS_FUNCTION_EVENT_TRIGGER} ON ddl_command_end WHEN TAG IN ('CREATE FUNCTION')
        EXECUTE FUNCTION public.g6_revoke_sessions_public();`);
    const error = await failure(
      runMigrations(client(db), { profile: SUPA_ET }),
    );
    expect(error?.code).toBe("function_boundary_unproven");
    const ev = JSON.parse(error!.message!.match(/(\{.*\})/)![1]!);
    expect(ev.event_trigger).toMatchObject({
      present: true,
      tags_exact: false,
      function_is_reviewed: false,
      prosrc_exact: false,
      verified: false,
    });
    expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
  });

  test("release read-only plan preserves the populated ledger/marker and reuses checksum/ACL guards", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    await runMigrations(client(db), { profile: SUPA_ET });
    const before = await snapshot(db);
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const report = await inspectMigrations(client(db), SUPA_ET);
    await db.exec("ROLLBACK");
    expect(report.inventory.every(s => !s.pending)).toBe(true);
    expect(report.inventory.length).toBeGreaterThan(20);
    expect(report.ledgerSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(await snapshot(db)).toEqual(before);
    // The inspection cannot turn ledger drift into an apparently successful plan.
    await db.exec("UPDATE sessions.__96bulls_migrations SET sha256 = repeat('0',64) WHERE name = (SELECT min(name) FROM sessions.__96bulls_migrations)");
    await db.exec("BEGIN READ ONLY");
    const error = await failure(inspectMigrations(client(db), SUPA_ET));
    await db.exec("ROLLBACK");
    expect(error?.code).toBe("migration_changed");
  });

  test("release read-only plan goes red on 20261105044040 when the records GIN is absent and ingest_claim_attempts is populated; after the CONCURRENTLY build the apply adopts it and replaces both readers", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    const SF = "upstream:20261105044040_buffered_stages_session_first.sql";
    const GIN = ADOPTABLE_INDEXES[SF]!;
    expect(GIN.map((x) => x.name)).toEqual(["ingest_claim_attempts_records_gin_idx"]);
    // The previous release: everything before 044040 (no later upstream step: a ledger gap), validated.
    // Validated against that release's ACL manifest and catalog (previousReleaseModel), not the current ones.
    const { steps, later, restore } = await previousReleaseModel(await plan(SUPA_ET), SF);
    try {
      expect(await failure(runMigrations(client(db), { profile: SUPA_ET, steps }))).toBeNull();
    } finally {
      restore();
    }
    // Production shape: ingest_claim_attempts populated (FK triggers off for this one superuser insert), no GIN yet.
    await db.exec(`SET session_replication_role = replica;
      INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records)
        VALUES (gen_random_uuid(),'org-probe','ws-probe','u','o','b','d','[]','[{"source":"claude","session_id":"s"}]');
      SET session_replication_role = origin`);
    const readerSrc = async () =>
      (await db.query<{ s: string }>("SELECT string_agg(prosrc, '' ORDER BY proname) s FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname IN ('buffered_sessions_for', 'buffered_session_stages_for')")).rows[0]!.s;
    const readerAcl = async () =>
      (await db.query<{ a: string }>("SELECT string_agg(proname || ':' || prosecdef || ':' || provolatile::text || ':' || coalesce(proacl::text, ''), ',' ORDER BY proname) a FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname IN ('buffered_sessions_for', 'buffered_session_stages_for')")).rows[0]!.a;
    const srcBefore = await readerSrc(), aclBefore = await readerAcl();
    const before = await snapshot(db);
    expect((before.marker as Array<{ status: string }>).map((r) => r.status)).toEqual(["pass"]);
    // Plan mode (read-only): red, naming the index and this step's operator packet.
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const planned = await failure(inspectMigrations(client(db), SUPA_ET));
    await db.exec("ROLLBACK");
    expect(planned?.code).toBe("index_prebuild_required");
    expect(planned?.message).toContain(`index sessions.ingest_claim_attempts_records_gin_idx on sessions.ingest_claim_attempts (${SF}) is absent and table sessions.ingest_claim_attempts is not empty`);
    expect(planned?.message).toContain("buffered-stages session-first operator packet");
    expect(planned?.message).not.toContain("claim-bootstrap index packet");
    // Apply: the same refusal before anything is written; the readers keep 043700's bodies, the marker stays 'pass'.
    expect((await failure(runMigrations(client(db), { profile: SUPA_ET })))?.code).toBe("index_prebuild_required");
    expect(await snapshot(db)).toEqual(before);
    expect(await readerSrc()).toBe(srcBefore);
    // After the operator's CONCURRENTLY build: green with 044040 pending; the apply adopts the index (same oid)
    // and replaces both readers, ACL, SECURITY DEFINER and volatility unchanged.
    await db.exec(`CREATE INDEX CONCURRENTLY ingest_claim_attempts_records_gin_idx ON sessions.ingest_claim_attempts USING gin (records jsonb_path_ops)`);
    const oid = async () => (await db.query<{ o: number }>("SELECT to_regclass('sessions.ingest_claim_attempts_records_gin_idx')::oid::int o")).rows[0]!.o;
    const built = await oid();
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const report = await inspectMigrations(client(db), SUPA_ET);
    await db.exec("ROLLBACK");
    expect(report.inventory.filter((s) => s.pending).map((s) => s.name)).toEqual([SF, ...later]);
    expect(await failure(runMigrations(client(db), { profile: SUPA_ET, expectedLedgerSha256: report.ledgerSha256 }))).toBeNull();
    expect(await oid()).toBe(built);
    expect(await readerSrc()).toContain("count(*) OVER (PARTITION BY rc.stage_token, rc.ord)");
    expect(await readerAcl()).toBe(aclBefore);
    expect(((await snapshot(db)).marker as Array<{ status: string }>).map((r) => r.status)).toEqual(["pass"]);
  });

  describe("20261105044040 exact-body adoption: an out-of-band apply of the step's own reader bodies", () => {
    const SF = "upstream:20261105044040_buffered_stages_session_first.sql";
    const SF_SQL = readFileSync(join(root, "supabase/migrations/20261105044040_buffered_stages_session_first.sql"), "utf8");
    /** The standalone CREATE OR REPLACE statement for one reader, exactly as the file has it (packet A's extraction). */
    const statement = (fn: string, tag: string) => {
      const start = SF_SQL.indexOf(`CREATE OR REPLACE FUNCTION sessions.${fn}(`);
      const close = `$${tag}$;`;
      const end = SF_SQL.indexOf(close, start);
      expect(start).toBeGreaterThan(0);
      expect(end).toBeGreaterThan(start);
      return SF_SQL.slice(start, end + close.length);
    };
    const LIST = () => statement("buffered_sessions_for", "buffered_sessions");
    const STAGES = () => statement("buffered_session_stages_for", "buffered_stages");
    const SENTINEL = () => MIGRATION_SENTINELS[SF]!.sql;
    /**
     * The previous release (everything before 044040, no later upstream step), validated against that release's ACL
     * manifest and catalog (previousReleaseModel), on the supabase/ET shape. `full` and `current` let the regression
     * test inject a synthetic later slot.
     */
    const previousRelease = async (
      full?: PlanStep[],
      current?: { manifest: FunctionAclManifest; catalog: RuntimeAclCatalog },
    ) => {
      const db = await supabaseLike();
      await db.exec(ET_BOOT);
      const { steps, restore } = await previousReleaseModel(full ?? await plan(SUPA_ET), SF, current);
      try {
        expect(await failure(runMigrations(client(db), { profile: SUPA_ET, steps }))).toBeNull();
      } finally {
        restore();
      }
      return db;
    };
    const ledgered = async (db: PGlite) =>
      (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name = $1", [SF])).rows[0]!.n === 1;
    const sentinel = async (db: PGlite) => (await db.query<{ applied: boolean }>(SENTINEL())).rows[0]!.applied;

    test("the declared tags parse to exactly the file's two bodies", () => {
      for (const tag of ["buffered_sessions", "buffered_stages"]) {
        const body = migrateGuards.dollarQuotedBody(SF_SQL, tag);
        expect(body).not.toBeNull();
        expect(body!.startsWith("\n")).toBe(true);
        expect(body!.trimEnd().endsWith("END")).toBe(true);
      }
      expect(migrateGuards.dollarQuotedBody(SF_SQL, "no_such_tag")).toBeNull();
      expect(migrateGuards.dollarQuotedBody(`${SF_SQL}\n-- $buffered_stages$`, "buffered_stages")).toBeNull();
    });

    test("both bodies pre-applied byte-identical: the migrator adopts them, applies the step and ledgers it", async () => {
      const db = await previousRelease();
      await db.exec(LIST());
      await db.exec(STAGES());
      expect(await sentinel(db)).toBe(true);
      expect(await failure(runMigrations(client(db), { profile: SUPA_ET }))).toBeNull();
      expect(await ledgered(db)).toBe(true);
      expect(await sentinel(db)).toBe(true);
    });

    test("one body differs by a single byte: refused (unledgered_schema) before any write", async () => {
      const db = await previousRelease();
      await db.exec(LIST());
      const stages = STAGES();
      const tweaked = stages.replace("$buffered_stages$\n", "$buffered_stages$\n ");
      expect(tweaked).not.toBe(stages);
      await db.exec(tweaked);
      expect(await sentinel(db)).toBe(true);
      const error = await failure(runMigrations(client(db), { profile: SUPA_ET }));
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`(${SF})`);
      expect(await ledgered(db)).toBe(false);
    });

    test("both bodies byte-identical but an attribute differs (volatility): refused", async () => {
      const db = await previousRelease();
      await db.exec(LIST());
      await db.exec(STAGES());
      await db.exec("ALTER FUNCTION sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text) VOLATILE");
      const error = await failure(runMigrations(client(db), { profile: SUPA_ET }));
      expect(error?.code).toBe("unledgered_schema");
      expect(await ledgered(db)).toBe(false);
    });

    test("a later slot (synthetic step, no migration file added) cannot break the previous release: its relation and function are modelled out of the ACL catalog and manifest", async () => {
      // A future upstream slot creating a table (with its implied serial sequence) and a function, which a current
      // binary's catalog and manifest would list. Written under this test's own tmpdir, never under supabase/.
      const dir = mkdtempSync(join(tmpdir(), "prev-release-later-slot-"));
      const path = join(dir, "20261105099999_zz_future_probe.sql");
      writeFileSync(path, `CREATE TABLE sessions.zz_future_probe (id bigserial PRIMARY KEY, v text);
CREATE FUNCTION sessions.zz_future_probe_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1';
`);
      const probe = { name: "upstream:20261105099999_zz_future_probe.sql", path } as PlanStep;
      const real = await plan(SUPA_ET);
      const full = [...real, probe];
      const manifest = await migrateGuards.sessionsFunctionAclManifest();
      const catalog = migrateGuards.runtimeApplicationAclCatalog();
      const current = {
        manifest: { ...manifest, "sessions.zz_future_probe_fn()": ["<owner>:EXECUTE", "service_role:EXECUTE"] },
        catalog: {
          tables: { ...catalog.tables, "sessions.zz_future_probe": ["SELECT", "INSERT"] },
          sequences: [...catalog.sequences, "sessions.zz_future_probe_id_seq"],
          rlsTables: [...catalog.rlsTables, "sessions.zz_future_probe"],
          functions: [...catalog.functions, "sessions.zz_future_probe_fn()"],
        },
      };
      // Pure derivation: the synthetic names are excluded, and the rest is exactly the view derived from the real
      // manifest and catalog with the same later steps.
      const laterSteps = laterUpstream(full, SF);
      expect(laterSteps.map((s) => s.name)).toContain(probe.name);
      const derived = previousReleaseAcl(laterSteps, current.manifest, current.catalog);
      expect([...derived.laterRels]).toContain("sessions.zz_future_probe");
      expect([...derived.laterFns]).toContain("sessions.zz_future_probe_fn");
      expect(Object.keys(derived.previousManifest)).not.toContain("sessions.zz_future_probe_fn()");
      expect(Object.keys(derived.previousCatalog.tables)).not.toContain("sessions.zz_future_probe");
      expect(derived.previousCatalog.sequences).not.toContain("sessions.zz_future_probe_id_seq");
      expect(derived.previousCatalog.rlsTables).not.toContain("sessions.zz_future_probe");
      expect(derived.previousCatalog.functions).not.toContain("sessions.zz_future_probe_fn()");
      expect(derived).toEqual(previousReleaseAcl(laterSteps, manifest, catalog) as typeof derived);
      // Negative control: the same truncated migrate with the current (unmodelled) catalog and manifest fails on the
      // relation the truncated plan never created (42P01), which is the defect the model prevents.
      const db = await supabaseLike();
      await db.exec(ET_BOOT);
      const steps = full.filter((s) => s.name !== SF && !laterSteps.includes(s));
      expect(steps).not.toContain(probe);
      const unmodelledManifest = spyOn(migrateGuards, "sessionsFunctionAclManifest").mockResolvedValue(current.manifest);
      const unmodelledCatalog = spyOn(migrateGuards, "runtimeApplicationAclCatalog").mockReturnValue(current.catalog);
      try {
        const error = await failure(runMigrations(client(db), { profile: SUPA_ET, steps }));
        expect(error?.code).toBe("42P01");
        expect(error?.message).toContain("zz_future_probe");
      } finally {
        unmodelledManifest.mockRestore();
        unmodelledCatalog.mockRestore();
      }
      // Through the model: the 044040 previous-release setup succeeds (it asserts the migrate is green), never
      // creates the synthetic slot's objects, and leaves the real manifest and catalog unmocked afterwards.
      const modelled = await previousRelease(full, current);
      expect(await ledgered(modelled)).toBe(false);
      expect((await modelled.query<{ r: string | null }>("SELECT to_regclass('sessions.zz_future_probe')::text r")).rows[0]!.r).toBeNull();
      expect((await modelled.query<{ p: string | null }>("SELECT to_regprocedure('sessions.zz_future_probe_fn()')::text p")).rows[0]!.p).toBeNull();
      expect(migrateGuards.runtimeApplicationAclCatalog()).toEqual(catalog);
      expect(await migrateGuards.sessionsFunctionAclManifest()).toEqual(manifest);
    });

    test("only stages pre-applied (packet A): the sentinel does not fire and the migrator applies normally", async () => {
      const db = await previousRelease();
      await db.exec(STAGES());
      expect(await sentinel(db)).toBe(false);
      expect(await failure(runMigrations(client(db), { profile: SUPA_ET }))).toBeNull();
      expect(await ledgered(db)).toBe(true);
      expect(await sentinel(db)).toBe(true);
    });
  });

  test("release read-only plan goes red on 20261105044020 when a claim-bootstrap index is absent on a populated table; the apply refuses before the pending marker", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    const CB = "upstream:20261105044020_claim_bootstrap_org_indexes.sql";
    const LAZY = "ingest_lazy_native_slots_org_id_idx";
    // The previous release: everything but 044020 (and no later upstream step: that would be a ledger gap), validated (marker 'pass').
    // That release's binary shipped the function ACL manifest and runtime application ACL catalog of its day, which
    // could not yet list a function or relation a later step creates: validate against them (previousReleaseModel).
    // No later step: the current manifest and catalog, unmocked.
    const { steps, later, restore } = await previousReleaseModel(await plan(SUPA_ET), CB);
    try {
      expect(await failure(runMigrations(client(db), { profile: SUPA_ET, steps }))).toBeNull();
    } finally {
      restore();
    }
    // Production shape: seven prebuilt CONCURRENTLY, the lazy-slot one held, its table populated.
    for (const x of ADOPTABLE_INDEXES[CB]!) if (x.name !== LAZY) await db.exec(x.def);
    await db.query(
      `INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,
         source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES ($1,'{}','org-probe','ws-probe','o','r',$1,$1,$1)`,
      ["a".repeat(64)],
    );
    const before = await snapshot(db);
    expect((before.marker as Array<{ status: string }>).map((r) => r.status)).toEqual(["pass"]);
    // Plan mode (read-only): red, naming the index and the packet.
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const planned = await failure(inspectMigrations(client(db), SUPA_ET));
    await db.exec("ROLLBACK");
    expect(planned?.code).toBe("index_prebuild_required");
    expect(planned?.message).toContain(`index sessions.${LAZY} on sessions.ingest_lazy_native_slots (${CB}) is absent and table sessions.ingest_lazy_native_slots is not empty`);
    expect(planned?.message).toContain("claim-bootstrap index packet");
    // Apply: the same refusal, before anything is written; the marker stays 'pass'.
    const applied = await failure(runMigrations(client(db), { profile: SUPA_ET }));
    expect(applied?.code).toBe("index_prebuild_required");
    expect(await snapshot(db)).toEqual(before);
    // After the operator's CONCURRENTLY build: the plan is green with 044020 pending, the apply adopts it.
    await db.exec(`CREATE INDEX CONCURRENTLY ${LAZY} ON sessions.ingest_lazy_native_slots USING btree (org_id)`);
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const report = await inspectMigrations(client(db), SUPA_ET);
    await db.exec("ROLLBACK");
    expect(report.inventory.filter((s) => s.pending).map((s) => s.name)).toEqual([CB, ...later]);
    expect(await failure(runMigrations(client(db), { profile: SUPA_ET, expectedLedgerSha256: report.ledgerSha256 }))).toBeNull();
    expect(((await snapshot(db)).marker as Array<{ status: string }>).map((r) => r.status)).toEqual(["pass"]);
  });

  test("release read-only plan shows rename adoptions, nothing pending, and binds the ledger as stored", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    await runMigrations(client(db), { profile: SUPA_ET });
    for (const a of MIGRATION_RENAME_ALIASES)
      await db.query(
        "UPDATE sessions.__96bulls_migrations SET name = $1, sha256 = $2 WHERE name = $3",
        [a.ledger_name, a.ledger_sha, a.migration],
      );
    const stored = new Map(
      (await db.query<{ name: string; sha256: string }>("SELECT name, sha256 FROM sessions.__96bulls_migrations")).rows
        .map((r) => [r.name, r.sha256]),
    );
    const before = await snapshot(db);
    await db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const report = await inspectMigrations(client(db), SUPA_ET);
    await db.exec("ROLLBACK");
    expect(report.inventory.every((s) => !s.pending)).toBe(true);
    expect(report.migrationRenameAliases).toEqual([...MIGRATION_RENAME_ALIASES]);
    expect(report.ledgerSha256).toBe(migrationLedgerSha256(stored));
    expect(await snapshot(db)).toEqual(before);
    // The apply binding recomputes the same stored-ledger hash.
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET, expectedLedgerSha256: report.ledgerSha256 })),
    ).toBeNull();
    expect((await snapshot(db)).ledger).toEqual(before.ledger);
  });

  test("release inspection converges on an operator-claimed consumer role; extras still refuse", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    await runMigrations(client(db), { profile: SUPA_ET });
    await db.exec(`CREATE SCHEMA IF NOT EXISTS search_index;
      CREATE TABLE IF NOT EXISTS search_index.transcript_events (id serial PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS search_index.projection_state (id serial PRIMARY KEY)`);
    // The operator grant file also touches the runtime service tables, which
    // only the serve-time bootstrap creates: stub their shape here so the
    // promotion under test is the role claim, not service provisioning.
    await db.exec(`CREATE TABLE IF NOT EXISTS sessions.search_consumer_pending (id serial PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS sessions.search_consumer_progress (id serial PRIMARY KEY)`);
    // Promote exactly like the operator bootstrap: the release inspection
    // must accept the claimed steady state every later deploy sees (it
    // returns an inventory instead of throwing).
    await db.exec(readFileSync(join(root, "supabase/bootstrap/session_consumer_role.sql"), "utf8"));
    expect((await inspectMigrations(client(db), SUPA_ET)).inventory.every((s) => !s.pending)).toBe(true);
    await db.exec(`ALTER ROLE search_consumer_runtime SET search_path = attacker`);
    const error = await failure(inspectMigrations(client(db), SUPA_ET));
    expect(error?.code).toBe("bootstrap_prerequisite");
    await db.exec(`ALTER ROLE search_consumer_runtime RESET search_path`);
    expect((await inspectMigrations(client(db), SUPA_ET)).inventory.every((s) => !s.pending)).toBe(true);
  });

  test("event-trigger mode (supabase): the reviewed bootstrap passes a fresh full migrate; the trigger enforces the boundary afterwards", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET })),
    ).toBeNull();
    const ev = await functionBoundaryEvidence(
      client(db),
      "event-trigger",
      "supabase",
      "g6_rt",
    );
    expect(ev.satisfied).toBe(true);
    expect(ev.event_trigger.verified).toBe(true);
    expect(ev.owner_function_default_exact).toBe(false); // the trigger covers the owner
    // The exact manifest (plus the runtime application ACL) holds after the migrate.
    expect(await sessionsFunctionAcl(client(db))).toEqual(
      manifestWithRuntime("g6_rt"),
    );
    // A new function is owner-only; a grant outside the manifest aborts.
    await db.exec(
      "CREATE FUNCTION sessions.g6_new() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    );
    expect(
      (await sessionsFunctionAcl(client(db)))["sessions.g6_new()"],
    ).toEqual(["<owner>:EXECUTE"]);
    for (const stmt of [
      "GRANT EXECUTE ON FUNCTION sessions.g6_new() TO PUBLIC",
      "GRANT EXECUTE ON FUNCTION sessions.g6_new() TO anon",
      "GRANT EXECUTE ON FUNCTION sessions.g6_new() TO authenticated",
      "GRANT EXECUTE ON FUNCTION sessions.jwt_org_id() TO anon",
      "ALTER FUNCTION sessions.g6_new() SECURITY DEFINER",
      "ALTER FUNCTION sessions.g6_new() OWNER TO g6_rt",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT EXECUTE ON FUNCTIONS TO anon",
    ])
      await expect(db.exec(stmt)).rejects.toThrow(/sessions function boundary/);
    // CREATE OR REPLACE keeps a manifested function's exact ACL.
    await db.exec(
      "CREATE OR REPLACE FUNCTION sessions.g6_new() RETURNS int LANGUAGE sql AS 'SELECT 2'",
    );
    expect(await sessionsFunctionAcl(client(db))).toEqual({
      ...manifestWithRuntime("g6_rt"),
      "sessions.g6_new()": ["<owner>:EXECUTE"],
    });
  });

  for (const [name, drift] of [
    [
      "disabled",
      `ALTER EVENT TRIGGER ${"hoopdb_sessions_function_boundary"} DISABLE`,
    ],
    [
      "enabled ORIGIN only (skipped in replica mode)",
      "ALTER EVENT TRIGGER hoopdb_sessions_function_boundary ENABLE",
    ],
    [
      "function body replaced",
      "CREATE OR REPLACE FUNCTION hoopdb_boundary.sessions_function_boundary() RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $f$ BEGIN END $f$",
    ],
    [
      "function owner changed",
      "CREATE ROLE g6_other_owner; ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() OWNER TO g6_other_owner",
    ],
    [
      "a tag missing",
      "DROP EVENT TRIGGER hoopdb_sessions_function_boundary; CREATE EVENT TRIGGER hoopdb_sessions_function_boundary ON ddl_command_end WHEN TAG IN ('CREATE FUNCTION') EXECUTE FUNCTION hoopdb_boundary.sessions_function_boundary()",
    ],
    [
      "search_path unpinned",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() RESET search_path",
    ],
    [
      "made PUBLIC-executable",
      "GRANT EXECUTE ON FUNCTION hoopdb_boundary.sessions_function_boundary() TO PUBLIC",
    ],
    [
      "made SECURITY DEFINER",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() SECURITY DEFINER",
    ],
    [
      "a function-level setting added",
      "ALTER FUNCTION hoopdb_boundary.sessions_function_boundary() SET work_mem = '64MB'",
    ],
    [
      "schema hoopdb_boundary usable by PUBLIC",
      "GRANT USAGE ON SCHEMA hoopdb_boundary TO PUBLIC",
    ],
    [
      "schema hoopdb_boundary open for CREATE",
      "GRANT CREATE ON SCHEMA hoopdb_boundary TO PUBLIC",
    ],
  ] as const)
    test(`event-trigger mode: drift (${name}) fails closed before any write`, async () => {
      const db = await supabaseLike();
      await db.exec(ET_BOOT);
      await db.exec(drift);
      const error = await failure(
        runMigrations(client(db), { profile: SUPA_ET }),
      );
      // Schema privileges also reach the runtime role, whose allowlist may refuse first;
      // either way nothing is written and the trigger evidence is unverified.
      expect(["function_boundary_unproven", "runtime_role_unsafe"]).toContain(
        error?.code,
      );
      expect(
        (
          await functionBoundaryEvidence(
            client(db),
            "event-trigger",
            "supabase",
            "g6_rt",
          )
        ).event_trigger.verified,
      ).toBe(false);
      expect(await snapshot(db)).toEqual({ ledger: null, marker: null });
    });

  test("event-trigger mode: another CREATE-capable role without an exact default still fails the gate (the trigger covers only the owner)", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET })),
    ).toBeNull();
    await db.exec(
      "CREATE ROLE g6_can_create NOLOGIN; GRANT CREATE, USAGE ON SCHEMA sessions TO g6_can_create",
    );
    const error = await failure(
      runMigrations(client(db), { profile: SUPA_ET }),
    );
    expect(error?.code).toBe("function_boundary_unproven");
  });

  test("event-trigger mode: the bootstrap installs the trigger ENABLE ALWAYS", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    expect(
      (
        await db.query<{ e: string }>(
          "SELECT evtenabled::text AS e FROM pg_event_trigger WHERE evtname = 'hoopdb_sessions_function_boundary'",
        )
      ).rows[0]?.e,
    ).toBe("A");
    // A rerun over an ORIGIN-only trigger refuses (an operator decides).
    await db.exec(
      "ALTER EVENT TRIGGER hoopdb_sessions_function_boundary ENABLE",
    );
    await expect(db.exec(ET_BOOT)).rejects.toThrow(/another definition/);
  });

  test("event-trigger mode: CREATE TYPE ... AS RANGE in sessions is checked (its PUBLIC constructors abort it), so later GRANTs elsewhere keep working", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET })),
    ).toBeNull();
    // PGlite runs as the bootstrap superuser, which owns the constructors and can revoke
    // them: they end owner-only. On real PostgreSQL (pg-proof) the command is aborted.
    await db.exec("CREATE TYPE sessions.g6_range AS RANGE (subtype = int4)");
    const publicCtors = (
      await db.query<{ n: number }>(
        "SELECT count(*)::int AS n FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND proname LIKE 'g6_%range' AND (proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee = 0))",
      )
    ).rows[0]!.n;
    expect(publicCtors).toBe(0);
    await db.exec(
      "CREATE FUNCTION public.g6_later() RETURNS int LANGUAGE sql AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION public.g6_later() TO anon",
    );
  });

  test("run-time exact manifest: a missing manifest grant (allowed by the trigger as a subset) is refused by the next migrate, rolled back (function_acl_drift)", async () => {
    const db = await supabaseLike();
    await db.exec(ET_BOOT);
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET })),
    ).toBeNull();
    await db.exec(
      "REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM authenticated",
    );
    const error = await failure(
      runMigrations(client(db), { profile: SUPA_ET }),
    );
    expect(error?.code).toBe("function_acl_drift");
    expect(error?.message).toContain("sessions.jwt_org_id()");
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.jwt_org_id() TO authenticated",
    );
    expect(
      await failure(runMigrations(client(db), { profile: SUPA_ET })),
    ).toBeNull();
  });

  test("the trigger's SECURITY DEFINER list equals exactly the SECURITY DEFINER sessions functions a clean full migrate creates", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const created = (
      await db.query<{ f: string }>(
        "SELECT p.oid::regprocedure::text AS f FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace AND p.prosecdef",
      )
    ).rows
      .map((r) => r.f)
      .sort();
    const listed = ET_BOOT.split("v_secdef text[] := ARRAY[")[1]!
      .split("]::text[]")[0]!
      .match(/'[^']+'/g)!
      .map((x) => x.slice(1, -1))
      .sort();
    expect(listed).toEqual(created);
    // Every function the migrations create is in the embedded manifest (no signature the
    // trigger would treat as owner-only by accident).
    const all = (
      await db.query<{ f: string }>(
        "SELECT p.oid::regprocedure::text AS f FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace",
      )
    ).rows.map((r) => r.f);
    const embedded = JSON.parse(ET_BOOT.split("$manifest$")[1]!) as Record<
      string,
      string[]
    >;
    expect(all.filter((fn) => !(fn in embedded))).toEqual([]);
  });

  test("the frozen trigger constants match the bootstrap file (prosrc sha256 and tags)", () => {
    const body = ET_BOOT.split("$boundary$")[1]!;
    expect(createHash("sha256").update(body).digest("hex")).toBe(
      SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256,
    );
    for (const tag of SESSIONS_FUNCTION_EVENT_TRIGGER_TAGS)
      expect(ET_BOOT).toContain(`'${tag}'`);
    const embedded = JSON.parse(ET_BOOT.split("$manifest$")[1]!) as Record<
      string,
      string[]
    >;
    expect(embedded).toEqual(MANIFEST);
    const runtimeFns = ET_BOOT.split("v_runtime_fns text[] := ARRAY[")[1]!
      .split("]::text[]")[0]!
      .match(/'[^']+'/g)!
      .map((x) => x.slice(1, -1));
    expect(runtimeFns).toEqual([...RUNTIME_APPLICATION_FUNCTIONS]);
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

  // Run-time exact manifest, mode matrix: every non-local boundary mode refuses an ACL
  // outside the manifest after migrate (rolled back, marker left pending); local does not.
  for (const [name, mode, boundary, drift] of [
    [
      "selfhosted global-default-revoke: an extra grant to authenticated",
      "selfhosted",
      "global-default-revoke",
      "GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO authenticated",
    ],
    [
      "cloudsql global-default-revoke: an extra grant to authenticated",
      "cloudsql",
      "global-default-revoke",
      "GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO authenticated",
    ],
    [
      "supabase event-trigger: a missing manifest grant (the trigger blocks extra ones)",
      "supabase",
      "event-trigger",
      "REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM authenticated",
    ],
  ] as const)
    test(`run-time exact manifest (${name}): function_acl_drift, rolled back, marker pending`, async () => {
      const db = await fresh();
      if (mode === "supabase") {
        await db.exec(
          readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
        );
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/session_directory_search_worker.sql",
            ),
            "utf8",
          ),
        );
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/session_token_verifier.sql",
            ),
            "utf8",
          ),
        );
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/session_ingest_roles.sql",
            ),
            "utf8",
          ),
        );
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/session_read_api_roles.sql",
            ),
            "utf8",
          ),
        );
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/session_consumer_roles.sql",
            ),
            "utf8",
          ),
        );
        await db.exec(`CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}`);
        await db.exec(
          readFileSync(
            join(
              root,
              "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
            ),
            "utf8",
          )
            .split("\n")
            .filter((l) => !l.startsWith("\\"))
            .join("\n")
            .replaceAll(":'runtime_role'", "'g6_rt'"),
        );
      } else {
        // cloudsql runs no in-migrate role bootstrap: the Worker role is provisioned first.
        if (mode === "cloudsql")
          await db.exec(
            readFileSync(
              join(
                root,
                "supabase/bootstrap/session_directory_search_worker.sql",
              ),
              "utf8",
            ),
          );
        if (mode === "cloudsql")
          await db.exec(
            readFileSync(
              join(
                root,
                "supabase/bootstrap/session_token_verifier.sql",
              ),
              "utf8",
            ),
          );
          await db.exec(
            readFileSync(
              join(
                root,
                "supabase/bootstrap/session_ingest_roles.sql",
              ),
              "utf8",
            ),
          );
          await db.exec(
            readFileSync(
              join(
                root,
                "supabase/bootstrap/session_read_api_roles.sql",
              ),
              "utf8",
            ),
          );
          await db.exec(
            readFileSync(
              join(
                root,
                "supabase/bootstrap/session_consumer_roles.sql",
              ),
              "utf8",
            ),
          );
        await db.exec(
          `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
        );
      }
      const profile: MigrationProfile = {
        ...LOCAL,
        mode,
        shim: mode !== "supabase",
        bootstrap: mode === "selfhosted",
        runtimeRole: "g6_rt",
        runtimeLogin: false,
        functionBoundary: boundary,
      };
      expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
      await db.exec(drift);
      const ledgerBefore = (await snapshot(db)).ledger;
      const error = await failure(runMigrations(client(db), { profile }));
      expect(error?.code).toBe("function_acl_drift");
      const after = await snapshot(db);
      expect(after.ledger).toEqual(ledgerBefore);
      expect(
        (after.marker as Array<{ status: string }>).map((m) => m.status),
      ).toEqual(["pending"]);
    });

  test("read-only ACL preview: before migrate only missing; clean migrate both empty; an extra grant is extra; a missing runtime grant is missing only", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    // Before the first migrate (migrations pending): absent functions are added, not refused.
    const before = await sessionsFunctionAclPreview(client(db), "g6_rt", true);
    expect(before.function_acl_drift_extra).toEqual([]);
    expect(before.function_acl_missing_refused).toEqual([]);
    expect(before.function_acl_missing.length).toBeGreaterThan(0);
    // With nothing pending, an absent manifest function is refused.
    expect(
      (await sessionsFunctionAclPreview(client(db), "g6_rt", false))
        .function_acl_missing_refused.length,
    ).toBeGreaterThan(0);
    const profile: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
    expect(await sessionsFunctionAclPreview(client(db), "g6_rt")).toEqual({
      runtime_role: "g6_rt",
      function_acl_drift_extra: [],
      function_acl_missing: [],
      function_acl_missing_refused: [],
      function_acl_public_revoked: [],
    });
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO authenticated",
    );
    expect(
      (await sessionsFunctionAclPreview(client(db), "g6_rt"))
        .function_acl_drift_extra,
    ).toEqual(["sessions.repo_backfill_report(): authenticated:EXECUTE"]);
    await db.exec(
      "REVOKE EXECUTE ON FUNCTION sessions.repo_backfill_report() FROM authenticated; REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM g6_rt",
    );
    expect(await sessionsFunctionAclPreview(client(db), "g6_rt")).toEqual({
      runtime_role: "g6_rt",
      function_acl_drift_extra: [],
      function_acl_missing: ["sessions.jwt_org_id(): g6_rt:EXECUTE"],
      function_acl_missing_refused: [],
      function_acl_public_revoked: [],
    });
    // PUBLIC EXECUTE is what the migrator revokes, not refuses: reported separately.
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.jwt_org_id() TO g6_rt, PUBLIC",
    );
    const pub = await sessionsFunctionAclPreview(client(db), "g6_rt");
    expect(pub.function_acl_drift_extra).toEqual([]);
    expect(pub.function_acl_public_revoked).toEqual([
      "sessions.jwt_org_id(): PUBLIC:EXECUTE",
    ]);
    await db.exec(
      "REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM PUBLIC",
    );
    await db.exec(
      "REVOKE EXECUTE ON FUNCTION sessions.jwt_org_id() FROM g6_rt",
    );
    // Read-only: the preview changed nothing.
    expect(
      (await sessionsFunctionAclPreview(client(db), "g6_rt"))
        .function_acl_missing,
    ).toEqual(["sessions.jwt_org_id(): g6_rt:EXECUTE"]);
  });

  test("read-only ACL preview agrees with the migrator: a missing non-runtime manifest grant and a duplicate-grantor surplus are refused by both", async () => {
    const db = await fresh();
    await db.exec(
      `CREATE ROLE g6_rt NOLOGIN; ${NO_PUBLIC_TEMP}; ${OWNER_FN_DEFAULT_REVOKE}`,
    );
    const profile: MigrationProfile = {
      ...LOCAL,
      mode: "selfhosted",
      runtimeRole: "g6_rt",
      runtimeLogin: false,
      functionBoundary: "global-default-revoke",
    };
    expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
    // A missing service_role grant: nothing re-adds it.
    await db.exec(
      "REVOKE EXECUTE ON FUNCTION sessions.repo_backfill_report() FROM service_role",
    );
    const missing = await sessionsFunctionAclPreview(client(db), "g6_rt");
    expect(missing.function_acl_missing_refused).toEqual([
      "sessions.repo_backfill_report(): service_role:EXECUTE",
    ]);
    expect(missing.function_acl_missing).toEqual([]);
    expect((await failure(runMigrations(client(db), { profile })))?.code).toBe(
      "function_acl_drift",
    );
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO service_role",
    );
    expect(await failure(runMigrations(client(db), { profile }))).toBeNull();
    // The same grantee:privilege from a second grantor is a surplus for both.
    await db.exec(`CREATE ROLE g6_grantor NOLOGIN;
      GRANT USAGE ON SCHEMA sessions TO g6_grantor;
      GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO g6_grantor WITH GRANT OPTION;
      SET ROLE g6_grantor;
      GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO service_role;
      RESET ROLE`);
    const dup = await sessionsFunctionAclPreview(client(db), "g6_rt");
    expect(dup.function_acl_drift_extra).toEqual([
      "sessions.repo_backfill_report(): g6_grantor:EXECUTE*",
      "sessions.repo_backfill_report(): service_role:EXECUTE",
    ]);
    expect((await failure(runMigrations(client(db), { profile })))?.code).toBe(
      "function_acl_drift",
    );
  });

  test("run-time exact manifest: local mode is unchanged (no check)", async () => {
    const db = await fresh();
    expect(
      await failure(runMigrations(client(db), { profile: LOCAL })),
    ).toBeNull();
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.repo_backfill_report() TO authenticated",
    );
    expect(
      await failure(runMigrations(client(db), { profile: LOCAL })),
    ).toBeNull();
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
    expect(await sessionsFunctionAcl(client(db))).toEqual(
      manifestWithRuntime("g6_rt"),
    );
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
    expect(await sessionsFunctionAcl(client(db))).toEqual(
      manifestWithRuntime("g6_rt"),
    );
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

describe("session token verifier phase 2", () => {
  const VERIFY = "supabase/bootstrap/session_token_verifier_verify.sql";

  test("wired: a clean migrate passes the verifier phase 2; a table grant on auth_sessions (drift) is caught", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    // The phase-2 file itself passes on a clean migrate.
    await db.exec(readFileSync(join(root, VERIFY), "utf8"));
    // Drift: a table grant the approved shape refuses.
    await db.exec(
      "GRANT SELECT ON sessions.auth_sessions TO session_token_verifier",
    );
    await expect(
      db.exec(readFileSync(join(root, VERIFY), "utf8")),
    ).rejects.toThrow(/session token verifier is not in the approved shape/);
    // Wired: the next migrate runs phase 2 and fails closed as well.
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error).not.toBeNull();
  });

  test("the live gate and the migrator see the same verifier set: table-grant drift closes schemaVerdict", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    expect(await schemaVerdict(expected, transact(db))).toEqual({ ok: true });
    await db.exec(
      "GRANT SELECT ON sessions.auth_sessions TO session_token_verifier",
    );
    const closed = await schemaVerdict(expected, transact(db));
    expect(closed.ok).toBe(false);
  });

  test("PG17+: a direct or PUBLIC MAINTAIN grant is effective drift, caught by phase 2", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const v = Number(
      (await db.query<{ v: string }>("SELECT current_setting('server_version_num') AS v")).rows[0].v,
    );
    if (v < 170000) return; // MAINTAIN does not exist before PG17
    const verify = () => db.exec(readFileSync(join(root, VERIFY), "utf8"));
    // Direct grant to the role: fails closed.
    await db.exec(
      "GRANT MAINTAIN ON sessions.auth_sessions TO session_token_verifier",
    );
    await expect(verify()).rejects.toThrow(
      /session token verifier is not in the approved shape/,
    );
    await db.exec(
      "REVOKE MAINTAIN ON sessions.auth_sessions FROM session_token_verifier",
    );
    await verify(); // clean again
    // The same grant via PUBLIC is effective drift too.
    await db.exec("GRANT MAINTAIN ON sessions.auth_sessions TO PUBLIC");
    await expect(verify()).rejects.toThrow(
      /session token verifier is not in the approved shape/,
    );
  });
});

describe("S3 proof seeder phase 2",()=>{
 const verify="supabase/bootstrap/session_ingest_seeder_verify.sql";
 test("OID comparison survives visible sessions schema and effective column drift closes live gate",async()=>{
  const db=await fresh();await runMigrations(client(db),{profile:LOCAL});
  await db.exec("SET search_path=sessions,public");
  await db.exec(readFileSync(join(root,verify),"utf8"));
  const expected=await expectedSchema();
  expect(await schemaVerdict(expected,transact(db))).toEqual({ok:true});
  await db.exec("GRANT SELECT(id) ON sessions.ingest_seed_captures TO PUBLIC");
  expect((await schemaVerdict(expected,transact(db))).ok).toBe(false);
 });
 test("seeder failure cannot leave PASS after the other two verifiers succeeded",async()=>{
  const db=await fresh();await runMigrations(client(db),{profile:LOCAL});
  await db.exec("GRANT SELECT ON sessions.ingest_seed_captures TO bulls96_session_ingest_seeder");
  const error=await failure(runMigrations(client(db),{profile:LOCAL}));
  expect(error).not.toBeNull();
  expect((await db.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0]).toEqual({status:"pending"});
 });
});

describe("buffered ingest phase2",()=>{
 test("direct ingest-only column drift closes live gate while unrelated role verifiers remain valid",async()=>{
  const db=await fresh();await runMigrations(client(db),{profile:LOCAL});
  const expected=await expectedSchema();
  expect(await schemaVerdict(expected,transact(db))).toEqual({ok:true});
  await db.exec("GRANT SELECT(stage_key) ON sessions.ingest_stage_intents TO bulls96_session_ingest");
  for(const file of ["session_directory_search_worker_verify.sql","session_token_verifier_verify.sql","session_ingest_seeder_verify.sql"])
   await db.exec(readFileSync(join(root,"supabase/bootstrap",file),"utf8"));
  expect((await schemaVerdict(expected,transact(db))).ok).toBe(false);
 });
 test("ingest failure after all other verifiers leaves the generation marker pending",async()=>{
  const db=await fresh();
  const error=await failure(runMigrations(client(db),{profile:LOCAL,beforeVerify:async()=>{
   await db.exec("GRANT SELECT ON sessions.ingest_stage_intents TO bulls96_session_ingest");
  }}));
  expect(error).not.toBeNull();
  expect((await db.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0]).toEqual({status:"pending"});
 });
});

describe("release pre-write fences", () => {
  test("a changed planned ledger refuses before the first persistent write", async () => {
    const db = await fresh();
    let invoked = false;
    const error = await failure(runMigrations(client(db), {
      profile: LOCAL, steps: [], expectedLedgerSha256: "0".repeat(64),
      beforeWrite: async () => { invoked = true; },
    }));
    expect(error?.code).toBe("release_ledger_changed");
    expect(invoked).toBe(false);
    expect(await snapshot(db)).toEqual({ledger:null,marker:null});
  });
  test("a revoked live release gate refuses under the lock before schema/marker creation", async () => {
    const db = await fresh();
    let invoked = false;
    const error = await failure(runMigrations(client(db), {
      profile: LOCAL, steps: [], expectedLedgerSha256:migrationLedgerSha256(new Map()),
      beforeWrite: async () => { invoked = true; throw Object.assign(new Error("refused"),{code:"live_release_refused"}); },
    }));
    expect(error?.code).toBe("live_release_refused");
    expect(invoked).toBe(true);
    expect(await snapshot(db)).toEqual({ledger:null,marker:null});
  });
});
