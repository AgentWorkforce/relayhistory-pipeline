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
import { schemaVerdict, type Transact } from "../../shared/db.ts";
import {
  expectedSchema,
  migrate,
  migrationProfile,
  runMigrations,
  type MigrationClient,
  type MigrationProfile,
} from "../migrate.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
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
  const base = { HOOPDB_RUNTIME_ROLE: "hoop_runtime" };
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
    expect(migrationProfile({ ...base, HOOPDB_DB_MODE: "supabase" })).toEqual({
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
    await db.exec("CREATE ROLE g6_rt NOLOGIN");
    await runMigrations(client(db), {
      profile: { ...LOCAL, runtimeRole: "g6_rt" },
    });
    await runMigrations(client(db), {
      profile: { ...LOCAL, runtimeRole: "g6_rt" },
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
