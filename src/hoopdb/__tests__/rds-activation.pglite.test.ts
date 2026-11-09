/**
 * rds-activation/v1 on in-process PGlite: runs the real rdsSeparateOwnerPreflight
 * query (not a canned row) against real catalogs, with negative cases. Also covers the
 * security-relevant driver helpers that need no server: secret scrubbing, result-file
 * exclusivity, write loops, failure classes and the execute() exit-code mapping. The
 * last test proves the driver runs under Node's erasable-only type stripping.
 *
 * PGlite's bootstrap superuser is postgres, not rdsadmin, so the emulated-RDS PG16/17
 * proofs of the full activation transaction stay with the pg-proof harness.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { rdsSeparateOwnerPreflight, type MigrationClient } from "../migrate.ts";
import {
  commitRecheckStatus,
  EXIT,
  execute,
  isServerError,
  openResultChannel,
  parseInputs,
  parseRoleSpec,
  passwordEnv,
  roleSpecSources,
  scrub,
  writeAll,
  writeResult,
} from "../rds-activation.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((d) => d.close().catch(() => undefined)));
});
const client = (db: PGlite): MigrationClient => ({
  async query<R>(text: string, params?: unknown[]) {
    return (await db.query<R>(text, params as never)) as { rows: R[] };
  },
});

/** A plain hoop that owns the database and sessions, on an RDS-shaped catalog. */
async function separateLayout(mutate = "", asHoop = true): Promise<PGlite> {
  const db = new PGlite();
  open.push(db);
  const dbname = (await db.query<{ d: string }>("SELECT current_database() AS d")).rows[0]!.d;
  await db.exec(`CREATE ROLE rdsadmin SUPERUSER NOLOGIN;
    CREATE ROLE rds_superuser NOLOGIN;
    CREATE ROLE hoop LOGIN NOINHERIT;
    CREATE ROLE foreign_owner NOLOGIN;
    ALTER DATABASE ${dbname} OWNER TO hoop;
    CREATE SCHEMA sessions AUTHORIZATION hoop;
    ${mutate.replaceAll("$DB", dbname)}`);
  if (asHoop) await db.exec("SET SESSION AUTHORIZATION hoop");
  return db;
}
const preflight = async (db: PGlite) => {
  try {
    await rdsSeparateOwnerPreflight(client(db));
    return "ok";
  } catch (e) {
    return `${(e as { code?: string }).code}: ${(e as Error).message}`;
  }
};

describe("rdsSeparateOwnerPreflight on real catalogs (PGlite)", () => {
  test("passes for a plain, membership-free hoop that owns the database and sessions", async () => {
    expect(await preflight(await separateLayout())).toBe("ok");
  });
  const cases: [string, string, RegExp][] = [
    ["superuser owner", "ALTER ROLE hoop SUPERUSER", /owner is SUPERUSER/],
    ["owner is a member of rds_superuser", "GRANT rds_superuser TO hoop", /member of 1 role/],
    ["owner is a member of anything (SET-only edge)", "GRANT foreign_owner TO hoop WITH INHERIT FALSE, SET TRUE", /member of 1 role/],
    ["another role can SET ROLE to the owner", "GRANT hoop TO foreign_owner WITH INHERIT FALSE, SET TRUE", /has 1 member/],
    ["another role holds ADMIN over the owner", "GRANT hoop TO foreign_owner WITH ADMIN TRUE, INHERIT FALSE, SET FALSE", /has 1 member/],
    ["owner has CREATEROLE", "ALTER ROLE hoop CREATEROLE", /CREATEROLE/],
    ["owner has CREATEDB", "ALTER ROLE hoop CREATEDB", /CREATEDB/],
    ["owner has BYPASSRLS", "ALTER ROLE hoop BYPASSRLS", /BYPASSRLS/],
    ["foreign database owner", "ALTER DATABASE $DB OWNER TO foreign_owner", /does not own the current database/],
    ["foreign sessions owner", "ALTER SCHEMA sessions OWNER TO foreign_owner", /does not own schema sessions/],
    ["sessions missing", "DROP SCHEMA sessions", /does not own schema sessions/],
    ["executor left behind", "CREATE ROLE hoopdb_activation_executor NOLOGIN", /hoopdb_activation_executor still exists/],
    ["rdsadmin not a superuser", "ALTER ROLE rdsadmin NOSUPERUSER", /rdsadmin/],
    ["rds_superuser can log in", "ALTER ROLE rds_superuser LOGIN", /rds_superuser/],
    ["rds_superuser absent", "DROP ROLE rds_superuser", /rds_superuser/],
  ];
  for (const [name, sql, why] of cases)
    test(`refuses: ${name}`, async () => {
      const r = await preflight(await separateLayout(sql));
      expect(r).toMatch(/^rds_profile_unproven: /);
      expect(r).toMatch(why);
    });
  test("refuses a SET ROLE session (session_user differs from current_user)", async () => {
    // session_user stays the bootstrap superuser; current_user becomes hoop.
    const db = await separateLayout("", false);
    await db.exec("SET ROLE hoop");
    expect((await db.query<{ c: string }>("SELECT current_user AS c")).rows[0]!.c).toBe("hoop");
    expect(await preflight(db)).toMatch(/SET ROLE/);
  });
  test("search_path spoofing: empty shadow catalogs first on the path do not hide an rds_superuser membership", async () => {
    // hoop owns the database, so it can create a schema, shadow the catalogs and put the
    // shadow first on its own path (or as a database default). Every catalog reference
    // in the preflight is pg_catalog-qualified, so the real edge is still seen.
    const db = await separateLayout("GRANT rds_superuser TO hoop");
    await db.exec(`CREATE SCHEMA shadow;
      CREATE TABLE shadow.pg_auth_members AS SELECT * FROM pg_catalog.pg_auth_members WHERE false;
      CREATE TABLE shadow.pg_roles AS SELECT * FROM pg_catalog.pg_roles;
      SET search_path = shadow, pg_catalog`);
    expect((await db.query<{ n: number }>("SELECT count(*)::int AS n FROM pg_auth_members")).rows[0]!.n).toBe(0);
    expect(await preflight(db)).toMatch(/^rds_profile_unproven: .*member of 1 role/);
  });
  test("every catalog reference in the separate-layout preflight and the fragments is pg_catalog-qualified", () => {
    const cats = /(?<![\w.'])(pg_roles|pg_auth_members|pg_database|pg_namespace|pg_default_acl|pg_db_role_setting|pg_extension|pg_class|pg_proc|pg_shdepend|pg_authid)\b(?!')/;
    const migrate = readFileSync(join(root, "src/hoopdb/migrate.ts"), "utf8");
    const pre = migrate.slice(migrate.indexOf("export async function rdsSeparateOwnerPreflight"), migrate.indexOf("Refuses, before any write, when Supabase"));
    const sqlOf = (text: string) => text.split("\n").filter((l) => !/^\s*(--|\*|\/\/|\/\*)/.test(l));
    const offenders = sqlOf(pre).filter((l) => /\b(FROM|JOIN)\b/.test(l) && cats.test(l));
    for (const f of ["00_preconditions", "10_provisioner_function_defaults", "20_executor", "30_roles", "40_login", "50_owner_handoff", "60_cleanup", "70_validate"])
      offenders.push(...sqlOf(readFileSync(join(root, `supabase/bootstrap/rds_activation/${f}.sql`), "utf8")).filter((l) => cats.test(l)).map((l) => `${f}: ${l}`));
    const driver = readFileSync(join(root, "src/hoopdb/rds-activation.ts"), "utf8");
    offenders.push(...sqlOf(driver).filter((l) => /\b(FROM|JOIN)\s+pg_/.test(l) && cats.test(l)).map((l) => `driver: ${l}`));
    expect(offenders).toEqual([]);
  });
  test("fragments 00 and 70 refuse database-level role settings (ALTER DATABASE ... SET)", () => {
    for (const f of ["00_preconditions", "70_validate"])
      expect(readFileSync(join(root, `supabase/bootstrap/rds_activation/${f}.sql`), "utf8")).toContain("hoopdb has database-level role settings");
    const driver = readFileSync(join(root, "src/hoopdb/rds-activation.ts"), "utf8");
    expect(driver).toContain('options: "-c search_path=pg_catalog,pg_temp"');
    expect(driver).toContain('SET LOCAL search_path = pg_catalog, pg_temp');
  });
});

describe("driver helpers without a server", () => {
  test("scrub redacts every secret and any SCRAM verifier", () => {
    const secrets = { provisioner_password: "prov-secret-AAAA", role_passwords: { hoop: "hoop-secret-BBBB" },
      current_password: "cur-secret-CCCC", new_password: "new-secret-DDDD" };
    const out = scrub("auth failed prov-secret-AAAA hoop-secret-BBBB cur-secret-CCCC new-secret-DDDD SCRAM-SHA-256$4096:abc$def:ghi end", { secrets });
    for (const v of Object.values(secrets).flatMap((x) => (typeof x === "string" ? [x] : Object.values(x))))
      expect(out).not.toContain(v);
    expect(out).not.toContain("SCRAM-SHA-256$");
    expect(out).toContain("[redacted]");
    expect(out).toContain("[redacted-verifier]");
  });
  test("result channel: file created exclusively before any work; an existing file is refused; a closed fd is refused", () => {
    const dir = mkdtempSync(join(tmpdir(), "rds-activation-"));
    try {
      const file = join(dir, "r.json");
      writeResult({ file }, { a: 1 });
      expect(JSON.parse(readFileSync(file, "utf8"))).toEqual({ a: 1 });
      expect(() => openResultChannel({ file })).toThrow(/cannot be created exclusively/);
      expect(() => writeResult({ file }, { a: 2 })).toThrow();
      expect(JSON.parse(readFileSync(file, "utf8"))).toEqual({ a: 1 });
      expect(() => openResultChannel({ fd: 987 })).toThrow(/not an open descriptor/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test("writeAll loops over short writes and retries EAGAIN", () => {
    const data = Buffer.from("x".repeat(100));
    const got: Buffer[] = [];
    let calls = 0;
    writeAll(9, data, (_fd, buf, off, len) => {
      calls++;
      if (calls % 3 === 0) throw Object.assign(new Error("again"), { code: "EAGAIN" });
      const n = Math.min(7, len);
      got.push(buf.subarray(off, off + n));
      return n;
    });
    expect(Buffer.concat(got).equals(data)).toBe(true);
    expect(() => writeAll(9, data, () => { throw Object.assign(new Error("io"), { code: "EIO" }); })).toThrow(/io/);
    expect(() => writeAll(9, data, () => 0)).toThrow(/no progress/);
  });
  test("server errors vs transport failures; COMMIT re-check outcomes", () => {
    expect(isServerError({ severity: "ERROR", code: "40001" })).toBe(true);
    expect(isServerError(new Error("Connection terminated unexpectedly"))).toBe(false);
    expect(isServerError({ code: "ECONNRESET" })).toBe(false);
    expect(commitRecheckStatus("activated")).toBe("activated");
    expect(commitRecheckStatus("fresh")).toBe("rolled_back");
    expect(commitRecheckStatus("inconsistent")).toBe("internal_error");
    expect(commitRecheckStatus("unreachable")).toBe("internal_error");
  });
  test("execute() exit-code mapping: artifact 0; an unreachable server is internal_error (1), never refused", async () => {
    const spec = parseRoleSpec(roleSpecSources());
    const pw = (n: string) => `${n}-`.padEnd(40, "x");
    const env = {
      HOOPDB_ACTIVATION_HOST: "127.0.0.1", HOOPDB_ACTIVATION_PORT: "1", HOOPDB_ACTIVATION_TLS: "disable",
      HOOPDB_ACTIVATION_PROVISIONER: "rds_master_login", HOOPDB_ACTIVATION_PROVISIONER_PASSWORD: pw("provisioner"),
      HOOPDB_ACTIVATION_RESULT_FD: "3",
      ...Object.fromEntries(spec.filter((r) => r.login).map((r) => [passwordEnv(r.name), pw(r.name)])),
    };
    const art = await execute(parseInputs(["artifact"], env, () => "", spec));
    expect([art.status, art.exit_code]).toEqual(["artifact", EXIT.ok]);
    for (const cmd of ["create-database", "activate", "verify", "run"]) {
      const r = await execute(parseInputs([cmd], env, () => "", spec));
      expect([cmd, r.status, r.exit_code]).toEqual([cmd, "internal_error", EXIT.internal_error]);
      expect(JSON.stringify(r)).not.toContain(pw("hoop"));
    }
  });
});

describe("Node erasable-only type stripping (infra #67 guest)", () => {
  test("the driver runs under node --experimental-strip-types", () => {
    const node = spawnSync("node", ["--version"], { encoding: "utf8" });
    if (node.status !== 0) throw new Error("node is required for this check");
    const major = Number(/^v(\d+)/.exec(node.stdout)?.[1] ?? 0);
    expect(major).toBeGreaterThanOrEqual(22);
    const dir = mkdtempSync(join(tmpdir(), "rds-activation-node-"));
    try {
      const file = join(dir, "artifact.json");
      const r = spawnSync("node", ["--experimental-strip-types", "--no-warnings", join(root, "src/hoopdb/rds-activation.ts"),
        "artifact", "--result-file", file], { cwd: root, encoding: "utf8", env: { PATH: process.env.PATH ?? "" } });
      expect(r.stderr).not.toMatch(/ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX|SyntaxError|TypeScript/);
      expect(r.status).toBe(0);
      expect(JSON.parse(readFileSync(file, "utf8"))).toMatchObject({ contract: "rds-activation/v1", status: "artifact", exit_code: 0 });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
