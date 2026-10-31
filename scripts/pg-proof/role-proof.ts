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
import { writeFileSync } from "node:fs";
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

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const HOST = process.env.PGPROOF_HOST ?? "127.0.0.1";
const PORT = process.env.PGPROOF_PORT ?? "5432";
const SUPER = process.env.PGPROOF_SUPERUSER ?? "postgres";
const PSQL = process.env.PGPROOF_PSQL ?? "psql";
const OUT = process.env.PGPROOF_OUT ?? "pg-proof.json";
const W = "session_directory_search_worker";
const url = (user: string, db: string) =>
  `postgres://${user}@${HOST}:${PORT}/${db}`;
const BOOT = join(
  ROOT,
  "supabase/bootstrap/session_directory_search_worker.sql",
);
const RUNTIME_BOOT = join(ROOT, "supabase/bootstrap/hoopdb_runtime_role.sql");
const MARK_PASS = join(
  ROOT,
  "supabase/bootstrap/session_directory_search_worker_mark_pass.sql",
);
const SELF = {
  HOOPDB_DB_MODE: "selfhosted",
  HOOPDB_SUPABASE_SHIM: "true",
  HOOPDB_RUNTIME_ROLE: "g6_rt",
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
};

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
): { exit: number; code: string } {
  const clean = Object.fromEntries(
    Object.entries(process.env).filter(([k]) => !k.startsWith("HOOPDB_")),
  );
  const r = spawnSync("bun", ["src/hoopdb/migrate.ts"], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...clean, HOOPDB_URL: url("hoop", db), ...env },
  });
  const codes = [
    ...`${r.stdout}${r.stderr}`.matchAll(/"error_code":"([^"]+)"/g),
  ].map((m) => m[1]!);
  return { exit: r.status ?? -1, code: codes.at(-1) ?? "" };
}
async function fresh(db: string): Promise<void> {
  await sql("postgres", `DROP DATABASE IF EXISTS ${db}`);
  await sql("postgres", `CREATE DATABASE ${db} OWNER hoop`);
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
for (const r of ["g6_rt", "g6_rt2", "g6_bad", "g6_m", "g6_never"]) {
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
if (
  !psql("postgres", ["-f", join(ROOT, "supabase/tests/supabase_shim.sql")]).ok
)
  throw new Error("shim setup failed");
if (!psql("postgres", ["-f", BOOT]).ok)
  throw new Error("worker bootstrap failed");

await check(
  "runtime-role bootstrap creates a dedicated role and is idempotent",
  async () => {
    const a = psql("postgres", [
      "-v",
      "runtime_role=g6_rt",
      "-v",
      "runtime_login=true",
      "-f",
      RUNTIME_BOOT,
    ]);
    const b = psql("postgres", [
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
      "postgres",
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
    await fresh("g6_w");
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
    await fresh("g6_w");
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
await fresh("g6_rt_db");
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
  ["existing DML", "GRANT INSERT ON sessions.session_index TO g6_bad"],
  [
    "a column privilege",
    "GRANT SELECT (org_id) ON sessions.session_directory TO g6_bad",
  ],
  [
    "unapproved EXECUTE",
    "GRANT EXECUTE ON FUNCTION sessions.upsert_session_index(jsonb) TO g6_bad",
  ],
  [
    "EXECUTE through PUBLIC",
    "GRANT EXECUTE ON FUNCTION sessions.repo_visibility_ids(text, uuid) TO PUBLIC",
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
      psql("postgres", [
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
      await attempt(
        "g6_rt_db",
        "REVOKE EXECUTE ON FUNCTION sessions.repo_visibility_ids(text, uuid) FROM PUBLIC",
      );
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
  "the dedicated runtime role, connected as itself: SELECT only, no DML/DDL/EXECUTE, owns nothing, live gate ok",
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
    got["EXECUTE upsert_session_index"] = await attempt(
      "g6_rt_db",
      "SELECT sessions.upsert_session_index('{}'::jsonb)",
      "g6_rt",
    );
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
      role.rolbypassrls === false &&
      role.owned === 0 &&
      v.ok;
    return [ok, `${fmt(got)} ${fmt(role)} verdict=${fmt(v)}`];
  },
);

// (2) The full ordered sequence: one contiguous prefix, no unledgered footprint. These run
// in their own database with their own runtime role (g6_rt already holds approved grants
// in g6_rt_db, and a reference from another database is refused by design).
psql("postgres", [
  "-v",
  "runtime_role=g6_rt2",
  "-v",
  "runtime_login=true",
  "-f",
  RUNTIME_BOOT,
]);
const SEQ = { ...SELF, HOOPDB_RUNTIME_ROLE: "g6_rt2" };
await check(
  "helm (a): only session_directory present: refused before any write",
  async () => {
    await fresh("g6_seq");
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
    await fresh("g6_seq");
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
    await fresh("g6_seq");
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
    await fresh("g6_seq");
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
      ["-v", `generation=${"f".repeat(64)}`, "-f", MARK_PASS],
      "hoop",
    );
    const right = psql(
      "g6_rt_db",
      ["-v", `generation=${await generation()}`, "-f", MARK_PASS],
      "hoop",
    );
    const again = psql(
      "g6_rt_db",
      ["-v", `generation=${await generation()}`, "-f", MARK_PASS],
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
    await fresh("g6_crash");
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

await attempt("postgres", "DROP DATABASE IF EXISTS g6_rt_db");
await attempt("postgres", "DROP DATABASE IF EXISTS g6_mode");
await attempt("postgres", "DROP DATABASE IF EXISTS g6_w");

const failed = cases.filter((c) => !c.pass).length;
const results = {
  head: process.env.PGPROOF_HEAD ?? "unknown",
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
