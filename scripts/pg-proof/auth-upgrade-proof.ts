/** Real PG15/16/17 regression proof. Run only against a disposable trust-auth
 * cluster, before role-proof.ts. Creates/drops ONLY its own databases/roles.
 * Requires PGPROOF_AUTH_UPGRADE_DISPOSABLE=1; no production connection fallback.
 * Same PGPROOF_HOST/PORT/SUPERUSER/HEAD/OUT convention as role-proof.ts. */
import pg from "pg";
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { runMigrations, generation } from "../../src/hoopdb/migrate.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../src/hoopdb/session-bootstrap-chain.ts";
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
if (process.env.PGPROOF_AUTH_UPGRADE_DISPOSABLE !== "1")
  throw Error("disposable cluster opt-in required");
const root = fileURLToPath(new URL("../..", import.meta.url));
const read = (p: string) => readFileSync(join(root, p), "utf8");
const fixture = JSON.parse(read("scripts/pg-proof/auth-upgrade-base.json")) as {
  base: string;
  steps: { name: string; path: string; sha256: string }[];
};
const connect = async (database: string) => {
  const c = new pg.Client({
    host: process.env.PGPROOF_HOST ?? "127.0.0.1",
    port: Number(process.env.PGPROOF_PORT ?? 5432),
    user: process.env.PGPROOF_SUPERUSER ?? "postgres",
    database,
  });
  await c.connect();
  return c;
};
const admin = await connect("postgres");
const version = (await admin.query("SHOW server_version")).rows[0]
  .server_version as string;
const major = Number(version.split(".")[0]);
const roles = [
  "authenticator",
  "session_token_verifier",
  "session_directory_search_worker",
  "anon",
  "authenticated",
  "service_role",
];
const initialRoles = new Set<string>(
  (await admin.query("SELECT rolname FROM pg_roles")).rows.map(
    (r) => r.rolname,
  ),
);
for (const r of roles.slice(0, 3))
  if (initialRoles.has(r)) throw Error("refusing existing proof role " + r);
const prefix = "auth_upgrade_" + process.pid + "_";
const databases: string[] = [];
const cases: { name: string; pass: boolean; detail: unknown }[] = [];
const check = async (name: string, work: () => Promise<unknown>) => {
  try {
    const detail = await work();
    cases.push({ name, pass: true, detail });
    console.log("PASS " + name);
  } catch (e: any) {
    cases.push({
      name,
      pass: false,
      detail: { code: e.code, message: e.message },
    });
    console.error("FAIL " + name + ": " + e.message);
  }
};
const assert = (v: unknown, message: string) => {
  if (!v) throw Error(message);
};
const database = async (suffix: string) => {
  const name = prefix + suffix;
  await admin.query("CREATE DATABASE " + name);
  databases.push(name);
  return connect(name);
};
const closeDatabase = async (c: pg.Client) => {
  const name = (await c.query("SELECT current_database() db")).rows[0].db;
  await c.end();
  await admin.query("DROP DATABASE " + name);
  databases.splice(databases.indexOf(name), 1);
};
const migrationFiles = () =>
  readdirSync(join(root, "supabase/migrations"))
    .filter((f) => f.endsWith(".sql"))
    .sort();
const authFiles = () =>
  migrationFiles().filter((f) =>
    /_auth_(sessions|session_verify)\.sql$/.test(f),
  );
const state = async (c: pg.Client) =>
  (
    await c.query(
      "SELECT (SELECT jsonb_agg(to_jsonb(r) ORDER BY rolname) FROM pg_roles r WHERE rolname IN('authenticator','session_token_verifier')) roles,(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member,grantor) FROM pg_auth_members m WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname='session_token_verifier')) edges",
    )
  ).rows[0];
const guardSql = () => {
  const file = authFiles().find((f) => f.endsWith("_auth_session_verify.sql"))!;
  const migration = read("supabase/migrations/" + file).match(
    /DO \$role\$[\s\S]*?\$role\$;/,
  )?.[0];
  if (!migration) throw Error("migration role guard absent");
  return [
    ["bootstrap", read("supabase/bootstrap/session_token_verifier.sql")],
    ["migration", migration],
    ["phase2", read("supabase/bootstrap/session_token_verifier_verify.sql")],
  ] as const;
};
try {
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine();
  await check("fresh_T0_exact_target_setup", async () => {
    const c = await database("t0");
    try {
      const setup = read("scripts/rehearse-relayhistory-t0.sh").match(
        /cat "\$WORK\/bulls\/supabase\/tests\/supabase_shim\.sql"[\s\S]*?\| psql_in dst/,
      )?.[0];
      if (!setup) throw Error("actual T0 setup not found");
      const entries = [
        ...setup.matchAll(
          /"\$WORK\/bulls(?:\/([^"\n]+))?"(\/supabase\/migrations\/\*\.sql)?/g,
        ),
      ];
      const files = entries.flatMap((m) =>
        m[2] ? migrationFiles().map((f) => "supabase/migrations/" + f) : [m[1]],
      );
      assert(files.length > 2, "setup sequence empty");
      // Fidelity binding: the operational T0 harness must provision exactly
      // the single-sourced bootstrap chain (plus the test-only shim) before
      // the migration chain. If the harness drifts, this fails explicitly
      // instead of the grant migration failing closed downstream.
      const expectedPrefix = [
        "supabase/tests/supabase_shim.sql",
        ...SESSION_BOOTSTRAP_FILES,
      ];
      const prefix = files.filter((f) => !f.startsWith("supabase/migrations/"));
      assert(
        JSON.stringify(prefix) === JSON.stringify(expectedPrefix),
        "T0 harness drifted from session-bootstrap-chain.ts: " + JSON.stringify(prefix),
      );
      for (const f of files) await c.query(read(f));
      assert(
        (
          await c.query(
            "SELECT to_regclass('sessions.auth_token_verifier_policy') rel",
          )
        ).rows[0].rel,
        "auth verifier schema missing",
      );
      return {
        files: files.length,
        verifierBootstrapBeforeMigrations:
          files.indexOf("supabase/bootstrap/session_token_verifier.sql") <
          files.indexOf("supabase/migrations/" + migrationFiles()[0]),
      };
    } finally {
      await closeDatabase(c);
    }
  });
  await check(
    "populated_base_ledger_upgrade_preserves_old_rows_and_data",
    async () => {
      const c = await database("ledger");
      try {
        await c.query(read("supabase/tests/supabase_shim.sql"));
        await c.query(
          read("supabase/bootstrap/session_directory_search_worker.sql"),
        );
        await c.query(
          "CREATE SCHEMA IF NOT EXISTS sessions;CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())",
        );
        for (const step of fixture.steps) {
          const sql = read(step.path);
          assert(
            createHash("sha256").update(sql).digest("hex") === step.sha256,
            "immutable base blob changed: " + step.name,
          );
          await c.query(sql);
          await c.query(
            "INSERT INTO sessions.__96bulls_migrations(name,sha256)VALUES($1,$2)",
            [step.name, step.sha256],
          );
        }
        await c.query(
          "INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,title) VALUES('synthetic-org','synthetic-ws','codex','synthetic-old','m','u',now(),now(),'s3','synthetic','synthetic/manifest.json','00000000-0000-4000-8000-000000000001','preserved upgrade row')",
        );
        const oldLedger = (
          await c.query(
            "SELECT * FROM sessions.__96bulls_migrations ORDER BY name",
          )
        ).rows;
        const data = (
          await c.query("SELECT to_jsonb(i) row FROM sessions.session_index i")
        ).rows;
        const first = await runMigrations(c as any, {
          profile: {
            mode: "local",
            shim: true,
            bootstrap: true,
            verify: true,
            runtimeRole: "postgres",
          },
        });
        const current = (
          await c.query(
            "SELECT * FROM sessions.__96bulls_migrations WHERE name=ANY($1::text[]) ORDER BY name",
            [fixture.steps.map((s) => s.name)],
          )
        ).rows;
        assert(
          JSON.stringify(oldLedger) === JSON.stringify(current),
          "old ledger rows mutated",
        );
        assert(
          JSON.stringify(data) ===
            JSON.stringify(
              (
                await c.query(
                  "SELECT to_jsonb(i) row FROM sessions.session_index i",
                )
              ).rows,
            ),
          "old source data mutated",
        );
        const authNames = (
          await c.query(
            "SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE '%auth_session%' ORDER BY name",
          )
        ).rows.map((r) => r.name);
        assert(
          authNames.length === 2 &&
            authNames.every((n) => n.startsWith("upstream:2026110402")),
          "auth migrations did not append in reserved order",
        );
        const again = await runMigrations(c as any, {
          profile: {
            mode: "local",
            shim: true,
            bootstrap: true,
            verify: true,
            runtimeRole: "postgres",
          },
        });
        assert(again.applied === 0, "repeat migrate not idempotent");
        return {
          base: fixture.base,
          baseRows: oldLedger.length,
          appliedIncludingShim: first.applied,
          schemaApplied: authNames.length,
          rerunApplied: again.applied,
          authNames,
          generation: await generation(),
        };
      } finally {
        await closeDatabase(c);
      }
    },
  );
  const c = await database("members");
  try {
    await c.query(read("supabase/tests/supabase_shim.sql"));
    await c.query(
      read("supabase/bootstrap/session_directory_search_worker.sql"),
    );
    await c.query(read("supabase/bootstrap/session_token_verifier.sql"));
    for (const f of migrationFiles())
      await c.query(read("supabase/migrations/" + f));
    await c.query("CREATE ROLE authenticator NOLOGIN NOINHERIT");
    const membership = async (
      label: string,
      setup: string,
      expected: "ok" | "55000",
    ) => {
      await c.query("REVOKE session_token_verifier FROM authenticator");
      await c.query(setup);
      for (const [guard, sql] of guardSql())
        await check(label + "_" + guard, async () => {
          const before = await state(c);
          let code = "ok";
          try {
            await c.query(sql);
          } catch (e: any) {
            code = e.code;
          }
          assert(code === expected, "expected " + expected + ", got " + code);
          assert(
            JSON.stringify(before) === JSON.stringify(await state(c)),
            "guard mutated roles or edges",
          );
          return { code, roleStateUnchanged: true };
        });
    };
    if (major >= 16) {
      await membership(
        "reject_inherit_true",
        "GRANT session_token_verifier TO authenticator WITH INHERIT TRUE, SET TRUE, ADMIN FALSE",
        "55000",
      );
      await membership(
        "reject_set_false",
        "GRANT session_token_verifier TO authenticator WITH INHERIT FALSE, SET FALSE, ADMIN FALSE",
        "55000",
      );
      await membership(
        "reject_admin_true",
        "GRANT session_token_verifier TO authenticator WITH INHERIT FALSE, SET TRUE, ADMIN TRUE",
        "55000",
      );
      await membership(
        "accept_switch_only",
        "GRANT session_token_verifier TO authenticator WITH INHERIT FALSE, SET TRUE, ADMIN FALSE",
        "ok",
      );
    } else {
      await membership(
        "reject_member_inherit",
        "ALTER ROLE authenticator INHERIT;GRANT session_token_verifier TO authenticator",
        "55000",
      );
      await membership(
        "reject_admin_true",
        "ALTER ROLE authenticator NOINHERIT;GRANT session_token_verifier TO authenticator WITH ADMIN OPTION",
        "55000",
      );
      await membership(
        "accept_member_noinherit",
        "ALTER ROLE authenticator NOINHERIT;GRANT session_token_verifier TO authenticator",
        "ok",
      );
    }
    await check(
      "switch_edge_has_no_inherited_rpc_but_can_SET_ROLE",
      async () => {
        const effective = (
          await c.query(
            "SELECT has_schema_privilege('authenticator','sessions','USAGE') schema_usage,has_function_privilege('authenticator','sessions.auth_session_verify(text,timestamptz)','EXECUTE') rpc_execute",
          )
        ).rows[0];
        assert(
          !effective.schema_usage && !effective.rpc_execute,
          "switch edge inherits verifier privileges",
        );
        await c.query("SET SESSION AUTHORIZATION authenticator");
        try {
          await c.query("SET ROLE session_token_verifier");
          assert(
            (await c.query("SELECT current_user")).rows[0].current_user ===
              "session_token_verifier",
            "SET ROLE failed",
          );
        } finally {
          await c.query("RESET ROLE;RESET SESSION AUTHORIZATION");
        }
        return effective;
      },
    );
  } finally {
    await closeDatabase(c);
  }
} finally {
  for (const db of databases)
    await admin.query("DROP DATABASE " + db + " WITH(FORCE)");
  for (const r of roles)
    if (!initialRoles.has(r)) await admin.query("DROP ROLE IF EXISTS " + r);
  await admin.end();
}
const failed = cases.filter((c) => !c.pass).length;
const result = {
  head: process.env.PGPROOF_HEAD ?? "working-tree",
  pg_version: version,
  pg_major: major,
  passed: cases.length - failed,
  failed,
  cases,
};
writeFileSync(
  process.env.PGPROOF_OUT ?? "auth-upgrade-proof.json",
  JSON.stringify(result, null, 2) + "\n",
);
console.log(
  JSON.stringify({ passed: result.passed, failed, pg_version: version }),
);
process.exitCode = failed ? 1 : 0;
