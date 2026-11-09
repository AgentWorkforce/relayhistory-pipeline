/**
 * rds-activation/v1 (docs/rds-activation-executor.md): pure unit tests. They cover the
 * fragment form, the generated role spec and its coverage of the repository's role SQL,
 * the artifact identity, the SCRAM verifier, input handling (secrets only from env or
 * stdin), and the separate-database migrator preflight. The emulated-RDS PG16/17 proofs
 * (grantor graph, rollback at every fragment, no helper left) run separately against a
 * real server.
 */
import { describe, expect, test } from "bun:test";
import { createHash, createHmac, pbkdf2Sync } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import {
  artifactIdentity,
  classifyActivateFailure,
  CONTRACT,
  EXECUTOR,
  FRAGMENTS,
  InvalidInput,
  invalidInputResult,
  isPolicyRefusal,
  LOGIN_DEFERRED_BY_FROZEN_MIGRATION,
  parseInputs,
  parseRoleSpec,
  passwordEnv,
  rawResultTarget,
  roleSpecSources,
  writeResult,
  scramVerifier,
  type RoleSpec,
} from "../rds-activation.ts";
import { isRdsCreatorEdge } from "../migrate-guards.ts";
import { migrationProfile, rdsSeparateOwnerPreflight, type MigrationClient } from "../migrate.ts";

const root = join(import.meta.dir, "../../..");
const bootstrap = join(root, "supabase/bootstrap");
const read = (rel: string) => readFileSync(join(root, rel), "utf8");
const stripComments = (sql: string) => sql.split("\n").map((l) => l.replace(/--.*$/, "")).join("\n");
/** Text outside dollar-quoted bodies, comments removed. */
const outsideDollarQuotes = (sql: string) => stripComments(sql).replace(/\$([A-Za-z0-9_]*)\$[\s\S]*?\$\1\$/g, " ");
const SPEC = parseRoleSpec(roleSpecSources());
const LOGINS = SPEC.filter((r) => r.login).map((r) => r.name);

describe("fragments: composable, no transaction control of their own", () => {
  for (const f of FRAGMENTS) {
    test(f, () => {
      const sql = read(`supabase/bootstrap/rds_activation/${f}.sql`);
      const bare = stripComments(sql);
      // No BEGIN/COMMIT/ROLLBACK/SAVEPOINT/START TRANSACTION statement anywhere (top level or in plpgsql).
      expect(outsideDollarQuotes(sql)).not.toMatch(/\b(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|START\s+TRANSACTION|END)\b\s*;/i);
      expect(bare).not.toMatch(/\b(COMMIT|ROLLBACK|SAVEPOINT|START\s+TRANSACTION)\b/i);
      // No psql meta-commands or variables: the driver binds inputs with set_config.
      expect(bare).not.toMatch(/^\s*\\/m);
      expect(bare).not.toMatch(/:'[a-z_]+'/);
      // Never a password literal: only bound verifiers.
      expect(bare).not.toMatch(/PASSWORD\s+'/i);
      // Exactly one DO block per fragment.
      expect([...outsideDollarQuotes(sql).matchAll(/\bDO\b/g)].length).toBe(1);
    });
  }
  test("CREATE DATABASE appears in no fragment (it cannot run in a transaction block)", () => {
    for (const f of FRAGMENTS) expect(stripComments(read(`supabase/bootstrap/rds_activation/${f}.sql`))).not.toMatch(/CREATE\s+DATABASE/i);
  });
  test("the executor is created in 20 and dropped in 60, and 70 asserts it is gone", () => {
    expect(stripComments(read("supabase/bootstrap/rds_activation/20_executor.sql"))).toMatch(/CREATE ROLE hoopdb_activation_executor\s+NOLOGIN NOINHERIT CREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS NOSUPERUSER/);
    expect(stripComments(read("supabase/bootstrap/rds_activation/60_cleanup.sql"))).toContain("DROP ROLE hoopdb_activation_executor");
    expect(stripComments(read("supabase/bootstrap/rds_activation/70_validate.sql"))).toContain("executor hoopdb_activation_executor still exists");
    expect(EXECUTOR).toBe("hoopdb_activation_executor");
  });
  test("fragment files on disk are exactly the ordered FRAGMENTS list", () => {
    const onDisk = readdirSync(join(bootstrap, "rds_activation")).filter((f) => f.endsWith(".sql")).map((f) => f.replace(/\.sql$/, "")).sort();
    expect(onDisk).toEqual([...FRAGMENTS]);
  });
});

describe("role spec: generated from the repository's role SQL", () => {
  test("contains hoop and the runtime as LOGIN, and the full fixed set", () => {
    const names = SPEC.map((r) => r.name);
    expect(names).toContain("hoop");
    expect(names).toContain("sessions_pipeline_runtime");
    expect(SPEC.find((r) => r.name === "hoop")).toEqual({ name: "hoop", login: true, settings: [] });
    expect(SPEC.find((r) => r.name === "sessions_pipeline_runtime")).toEqual({ name: "sessions_pipeline_runtime", login: true, settings: [] });
    // Every name in the legacy trampoline inventory is covered.
    const inventory = /FOREACH\s+v_role\s+IN\s+ARRAY\s+ARRAY\[([\s\S]*?)\]/.exec(stripComments(read("supabase/bootstrap/rds_migration_roles.sql")))![1]!;
    for (const m of inventory.matchAll(/'([a-z_0-9]+)'::name/g)) expect(names).toContain(m[1]!);
    expect(names).toContain("search_consumer_runtime");
  });
  test("settings match the operator role files exactly (spot checks)", () => {
    const by = Object.fromEntries(SPEC.map((r) => [r.name, r]));
    expect(by.session_directory_search_worker).toEqual({ name: "session_directory_search_worker", login: false, settings: ["statement_timeout=2s"] });
    expect(by.bulls96_read_api).toEqual({ name: "bulls96_read_api", login: true, settings: ["default_transaction_read_only=on", "statement_timeout=2s"] });
    expect(by.bulls96_analysis_admit).toEqual({ name: "bulls96_analysis_admit", login: true, settings: ["default_transaction_read_only=off", "statement_timeout=2s"] });
    expect(by.search_consumer_runtime).toEqual({ name: "search_consumer_runtime", login: true, settings: ["statement_timeout=60s"] });
    expect(by.bulls96_session_ingest!.settings).toEqual(["idle_in_transaction_session_timeout=10s", "lock_timeout=2s", "statement_timeout=5s"]);
    for (const r of ["anon", "authenticated", "service_role", "session_token_verifier", "bulls96_session_ingest_seeder", "bulls96_canonical_feature_producer"])
      expect(by[r]).toEqual({ name: r, login: false, settings: [] });
  });
  test("coverage: every role a bootstrap file creates or alters by name is in the activation spec", () => {
    // Trampoline seeds belong to the legacy master-owner / Cloud SQL paths only.
    const notThisLayout = new Set(["bulls96_rds_role_seed", "bulls96_cloudsql_role_seed"]);
    const names = new Set(SPEC.map((r) => r.name));
    const missing: string[] = [];
    for (const f of readdirSync(bootstrap).filter((x) => x.endsWith(".sql"))) {
      for (const m of stripComments(readFileSync(join(bootstrap, f), "utf8")).matchAll(/\b(?:CREATE|ALTER)\s+ROLE\s+([a-z_][a-z0-9_]*)\b/gi)) {
        const n = m[1]!.toLowerCase();
        if (!names.has(n) && !notThisLayout.has(n)) missing.push(`${f}: ${n}`);
      }
    }
    expect(missing).toEqual([]);
  });
  test("coverage: every operator file that arms LOGIN or sets a role setting is a spec source", () => {
    const sources = new Set(roleSpecSources().map((p) => p.split("/").pop()!));
    // Not for this layout: the legacy runtime-role files (replaced by activation), the
    // LOGIN rollback file, the Supabase-only worker wrapper and the Cloud SQL trampoline
    // (its worker timeout is the same one session_directory_search_worker.sql sets).
    const excluded = new Set(["hoopdb_runtime_role.sql", "hoopdb_runtime_role_rds.sql", "hoopdb_runtime_role_supabase.sql",
      "buffered_search_feed_runtime_role_nologin.sql", "session_directory_search_worker_supabase.sql", "cloudsql_migration_roles.sql"]);
    const uncovered = readdirSync(bootstrap).filter((f) => f.endsWith(".sql") && !sources.has(f) && !excluded.has(f) &&
      /\bALTER\s+ROLE\s+[a-z_][a-z0-9_]*\s+(?:[A-Z ]*\bLOGIN\b|SET\b)/i.test(stripComments(readFileSync(join(bootstrap, f), "utf8"))));
    expect(uncovered).toEqual([]);
  });
  test("frozen-migration LOGIN deferral is justified by the migration text", () => {
    for (const [role, migration] of Object.entries(LOGIN_DEFERRED_BY_FROZEN_MIGRATION)) {
      const sql = read(migration);
      expect(sql).toContain(`rolname='${role}'`);
      expect(sql).toMatch(/IF r\.rolcanlogin OR/);
      expect(SPEC.find((r) => r.name === role)?.login).toBe(false);
      // Its operator file still arms LOGIN, which is why the deferral is needed.
      expect(stripComments(read("supabase/bootstrap/babysitter_retention_role.sql"))).toMatch(new RegExp(`ALTER ROLE ${role} LOGIN`));
    }
  });
  test("parser refuses conflicting settings, RESET and unsupported ALTER ROLE", () => {
    const src = (body: string) => () => parseRoleSpec(["x/a_role.sql"], () => body);
    expect(src("ALTER ROLE r1 SET statement_timeout='5s'; ALTER ROLE r1 SET statement_timeout='6s';")).toThrow(/conflicting/);
    expect(src("ALTER ROLE r1 RESET statement_timeout;")).toThrow(/RESET/);
    expect(src("ALTER ROLE r1 RENAME TO r2;")).toThrow(/unsupported/);
    expect(src("ALTER ROLE r1 NOLOGIN;")()).toContainEqual({ name: "r1", login: false, settings: [] });
    expect(src("-- ALTER ROLE r9 LOGIN;\nALTER ROLE r1 LOGIN NOINHERIT; ALTER ROLE r1 SET lock_timeout = '2s';")())
      .toContainEqual({ name: "r1", login: true, settings: ["lock_timeout=2s"] });
    expect(src("-- ALTER ROLE r9 LOGIN;")().map((r: RoleSpec) => r.name)).not.toContain("r9");
  });
});

describe("artifact identity", () => {
  test("covers fragments in order, the spec sources and the driver; deterministic", () => {
    const a = artifactIdentity();
    expect(a.contract).toBe(CONTRACT);
    expect(a.contract).toBe("rds-activation/v1");
    expect(a.artifact_files.map(([p]) => p)).toEqual([
      ...FRAGMENTS.map((f) => `supabase/bootstrap/rds_activation/${f}.sql`),
      ...roleSpecSources(),
      "src/hoopdb/rds-activation.ts",
    ]);
    expect(a.artifact_sha256).toBe(createHash("sha256").update(JSON.stringify(a.artifact_files)).digest("hex"));
    expect(artifactIdentity().artifact_sha256).toBe(a.artifact_sha256);
  });
  test("any byte change to a fragment, a spec source or the driver changes it", () => {
    const base = artifactIdentity();
    for (const target of [base.artifact_files[3]![0], roleSpecSources()[0]!, "src/hoopdb/rds-activation.ts"]) {
      const changed = artifactIdentity((rel) => {
        const b = readFileSync(join(root, rel));
        return rel === target ? Buffer.concat([b, Buffer.from(" ")]) : b;
      });
      expect(changed.artifact_sha256).not.toBe(base.artifact_sha256);
    }
  });
});

describe("SCRAM-SHA-256 verifier", () => {
  test("PostgreSQL stored format, recomputed independently", () => {
    const salt = Buffer.alloc(16, 7);
    const v = scramVerifier("correct-horse-battery-staple-0123456789", salt);
    expect(v).toMatch(/^SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$/);
    const salted = pbkdf2Sync("correct-horse-battery-staple-0123456789", salt, 4096, 32, "sha256");
    const stored = createHash("sha256").update(createHmac("sha256", salted).update("Client Key").digest()).digest("base64");
    const server = createHmac("sha256", salted).update("Server Key").digest("base64");
    expect(v).toBe(`SCRAM-SHA-256$4096:${salt.toString("base64")}$${stored}:${server}`);
    expect(v).not.toContain("correct-horse");
  });
  test("random salt per call; non-ASCII refused", () => {
    expect(scramVerifier("a".repeat(40))).not.toBe(scramVerifier("a".repeat(40)));
    expect(() => scramVerifier("pässword-with-umlaut-000000000000")).toThrow(/ASCII/);
    expect(() => scramVerifier("has space in it 0000000000000000000")).toThrow(/ASCII/);
  });
});

describe("inputs: secrets only from env or stdin; refused before any connection", () => {
  const pw = (n: string) => `${n}-`.padEnd(40, "x");
  const roleEnv = Object.fromEntries(LOGINS.map((r) => [passwordEnv(r), pw(r)]));
  const env = (extra: Record<string, string | undefined> = {}) => ({
    HOOPDB_ACTIVATION_HOST: "hoop-rds.example.internal",
    HOOPDB_ACTIVATION_PROVISIONER: "rds_master_login",
    HOOPDB_ACTIVATION_CA_FILE: "/etc/rds/global-bundle.pem",
    HOOPDB_ACTIVATION_RESULT_FD: "3",
    HOOPDB_ACTIVATION_PROVISIONER_PASSWORD: pw("provisioner"),
    ...roleEnv,
    ...extra,
  });
  const parse = (argv: string[], e = env(), stdin = "") => parseInputs(argv, e, () => stdin, SPEC);
  const refused = (argv: string[], e = env(), stdin = "") => {
    try {
      parse(argv, e, stdin);
      return "accepted";
    } catch (err) {
      expect(err).toBeInstanceOf(InvalidInput);
      return (err as Error).message;
    }
  };

  test("a complete env configuration parses for run, verify-full by default", () => {
    const c = parse(["run"]);
    expect(c).toMatchObject({ command: "run", tls: "verify-full", port: 5432, maintenanceDb: "postgres", result: { fd: 3 } });
    expect(Object.keys(c.secrets.role_passwords).sort()).toEqual([...LOGINS].sort());
  });
  test("stdin secrets parse and exclude the env", () => {
    const stdin = JSON.stringify({ provisioner_password: pw("provisioner"), role_passwords: Object.fromEntries(LOGINS.map((r) => [r, pw(r)])) });
    const bare = Object.fromEntries(Object.entries(env()).filter(([k]) => !k.includes("PASSWORD")));
    expect(parse(["run", "--secrets-stdin"], bare, stdin).secrets.role_passwords.hoop).toBe(pw("hoop"));
    expect(refused(["run", "--secrets-stdin"], env(), stdin)).toMatch(/ambiguous/);
    expect(refused(["run", "--secrets-stdin"], bare, "not json")).toMatch(/JSON object/);
    expect(refused(["run", "--secrets-stdin"], bare, JSON.stringify({ hoop_password: "x" }))).toMatch(/unexpected stdin key/);
    expect(refused(["run", "--secrets-stdin"], bare, JSON.stringify({ provisioner_password: pw("p"), role_passwords: { rdsadmin: pw("z") } })))
      .toMatch(/unexpected role/);
  });
  test("secrets or connection URLs on argv are refused", () => {
    for (const a of ["--password=x", "--hoop-password", "postgres://u:p@h/db", "--token=abc", "--verifier=x"])
      expect(refused(["run", a])).toMatch(/never accepted on the command line/);
  });
  test("missing, short, non-ASCII, duplicate or provisioner-equal passwords are refused", () => {
    expect(refused(["run"], env({ [passwordEnv("bulls96_read_api")]: undefined }))).toMatch(/passwords are required for: bulls96_read_api/);
    expect(refused(["run"], env({ [passwordEnv("hoop")]: "short" }))).toMatch(/at least 32/);
    expect(refused(["run"], env({ [passwordEnv("hoop")]: `${"é".repeat(40)}` }))).toMatch(/printable ASCII/);
    expect(refused(["run"], env({ [passwordEnv("hoop")]: pw("sessions_pipeline_runtime") }))).toMatch(/distinct/);
    expect(refused(["run"], env({ [passwordEnv("hoop")]: pw("provisioner") }))).toMatch(/provisioner password/);
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_PROVISIONER_PASSWORD: undefined }))).toMatch(/provisioner password is required/);
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_PASSWORD_RDSADMIN: pw("z") }))).toMatch(/outside the spec/);
  });
  test("verify needs only the hoop and runtime passwords; create-database needs none of the role passwords", () => {
    const only = { ...env(), ...Object.fromEntries(LOGINS.filter((r) => r !== "hoop" && r !== "sessions_pipeline_runtime").map((r) => [passwordEnv(r), undefined])) };
    expect(parse(["verify"], only).command).toBe("verify");
    const none = Object.fromEntries(Object.entries(env()).filter(([k]) => !k.startsWith("HOOPDB_ACTIVATION_PASSWORD_")));
    expect(parse(["create-database"], none).command).toBe("create-database");
    expect(refused(["verify"], { ...only, [passwordEnv("hoop")]: undefined })).toMatch(/hoop/);
  });
  test("TLS: disable only on loopback; verify-full needs a CA file and a host name", () => {
    expect(refused(["run", "--tls", "disable"])).toMatch(/loopback/);
    expect(parse(["run", "--tls", "disable"], env({ HOOPDB_ACTIVATION_HOST: "127.0.0.1" })).tls).toBe("disable");
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_CA_FILE: undefined }))).toMatch(/ca-file/);
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_HOST: "10.0.0.5" }))).toMatch(/host name/);
    expect(refused(["run", "--tls", "require"])).toMatch(/verify-full or disable/);
  });
  test("result channel: exactly one of file or fd; fd 3 or above", () => {
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_RESULT_FD: undefined }))).toMatch(/exactly one/);
    expect(refused(["run", "--result-file", "/tmp/r.json"])).toMatch(/exactly one/);
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_RESULT_FD: "1" }))).toMatch(/3 or above/);
    expect(parse(["run", "--result-file", "/tmp/r.json"], env({ HOOPDB_ACTIVATION_RESULT_FD: undefined })).result).toEqual({ file: "/tmp/r.json" });
  });
  test("flags and env must agree; unknown or repeated flags are refused", () => {
    expect(refused(["run", "--host", "other.example.internal"])).toMatch(/both set and differ/);
    expect(parse(["run", "--host", "hoop-rds.example.internal"]).host).toBe("hoop-rds.example.internal");
    expect(refused(["run", "--database", "x"])).toMatch(/unknown argument/);
    expect(refused(["run", "--port", "1", "--port", "2"])).toMatch(/twice/);
    expect(refused(["nope"])).toMatch(/command must be/);
  });
  test("the provisioner may not be a HoopDb, executor or platform role", () => {
    for (const p of ["hoop", "sessions_pipeline_runtime", "anon", EXECUTOR, "rdsadmin", "rds_superuser"])
      expect(refused(["run"], env({ HOOPDB_ACTIVATION_PROVISIONER: p }))).toMatch(/must not be/);
    expect(refused(["run"], env({ HOOPDB_ACTIVATION_PROVISIONER: "Bad-Name" }))).toMatch(/identifier/);
  });
  test("rotate-self: a LOGIN role of the spec, current and new password, new differs", () => {
    const e = { HOOPDB_ACTIVATION_HOST: "hoop-rds.example.internal", HOOPDB_ACTIVATION_CA_FILE: "/ca.pem", HOOPDB_ACTIVATION_RESULT_FD: "3",
      HOOPDB_ACTIVATION_CURRENT_PASSWORD: pw("old"), HOOPDB_ACTIVATION_NEW_PASSWORD: pw("new") };
    expect(parse(["rotate-self", "--role", "sessions_pipeline_runtime"], e).rotateRole).toBe("sessions_pipeline_runtime");
    expect(refused(["rotate-self", "--role", "anon"], e)).toMatch(/LOGIN role/);
    expect(refused(["rotate-self", "--role", "rdsadmin"], e)).toMatch(/LOGIN role/);
    expect(refused(["rotate-self"], e)).toMatch(/LOGIN role/);
    expect(refused(["rotate-self", "--role", "hoop"], { ...e, HOOPDB_ACTIVATION_NEW_PASSWORD: pw("old") })).toMatch(/differ/);
    expect(refused(["rotate-self", "--role", "hoop"], { ...e, HOOPDB_ACTIVATION_NEW_PASSWORD: "short" })).toMatch(/at least 32/);
    expect(refused(["run", "--role", "hoop"])).toMatch(/only for rotate-self/);
  });
});

describe("migrator: HOOPDB_RDS_LAYOUT=separate-database", () => {
  const base = { HOOPDB_DB_MODE: "rds", HOOPDB_RUNTIME_ROLE: "sessions_pipeline_runtime", HOOPDB_RUNTIME_ROLE_LOGIN: "true",
    HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke" };
  test("rds defaults to the master-owner layout; separate-database is explicit; other values and modes are refused", () => {
    expect(migrationProfile(base).rdsLayout).toBe("master-owner");
    expect(migrationProfile({ ...base, HOOPDB_RDS_LAYOUT: "separate-database" }).rdsLayout).toBe("separate-database");
    expect(() => migrationProfile({ ...base, HOOPDB_RDS_LAYOUT: "shared" })).toThrow(/master-owner or separate-database/);
    for (const mode of ["selfhosted", "cloudsql", "supabase", "local"])
      expect(() => migrationProfile({ ...base, HOOPDB_DB_MODE: mode, HOOPDB_SUPABASE_SHIM: "false", HOOPDB_RDS_LAYOUT: "separate-database" }))
        .toThrow(/only valid with HOOPDB_DB_MODE=rds/);
  });
  test("the master-owner creator edge is not accepted for the runtime role in the separate layout", () => {
    const edge = { rolname: "hoop", grantor: "rdsadmin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false };
    expect(isRdsCreatorEdge({ mode: "rds" }, 170004, edge, "hoop")).toBe(true);
    expect(isRdsCreatorEdge({ mode: "rds", rdsLayout: "master-owner" }, 170004, edge, "hoop")).toBe(true);
    expect(isRdsCreatorEdge({ mode: "rds", rdsLayout: "separate-database" }, 170004, edge, "hoop")).toBe(false);
  });

  const good = {
    server_version_num: 170004, same_session: true, owner_super: false, owner_createrole: false, owner_createdb: false,
    owner_replication: false, owner_bypassrls: false, owner_memberships: 0, owner_members: 0, rdsadmin_super: true,
    rds_superuser_shape: true, owns_database: true, owns_sessions: true, executor_present: false,
  };
  const fake = (row: Record<string, unknown> | undefined): MigrationClient => ({
    async query<R>() {
      return { rows: (row ? [row] : []) as R[] };
    },
  });
  const code = async (row: Record<string, unknown> | undefined) => {
    try {
      await rdsSeparateOwnerPreflight(fake(row));
      return "ok";
    } catch (e) {
      return `${(e as { code?: string }).code}: ${(e as Error).message}`;
    }
  };
  test("passes for a plain hoop that owns its database and sessions on an RDS-shaped server", async () => {
    expect(await code(good)).toBe("ok");
  });
  test("refuses (rds_profile_unproven) a superuser, an rds_superuser member, CREATEROLE, members, and every missing proof", async () => {
    const cases: [Record<string, unknown>, RegExp][] = [
      [{ owner_super: true }, /SUPERUSER/],
      [{ owner_memberships: 1 }, /member of 1 role/],
      [{ owner_members: 2 }, /2 member/],
      [{ owner_createrole: true }, /CREATEROLE/],
      [{ owner_createdb: true }, /CREATEDB/],
      [{ owner_replication: true }, /REPLICATION/],
      [{ owner_bypassrls: true }, /BYPASSRLS/],
      [{ same_session: false }, /SET ROLE/],
      [{ server_version_num: 150010 }, /16\+/],
      [{ rdsadmin_super: false }, /rdsadmin/],
      [{ rds_superuser_shape: false }, /rds_superuser/],
      [{ owns_database: false }, /own the current database/],
      [{ owns_sessions: false }, /schema sessions/],
      [{ executor_present: true }, /hoopdb_activation_executor still exists/],
    ];
    for (const [patch, why] of cases) {
      const r = await code({ ...good, ...patch });
      expect(r).toMatch(/^rds_profile_unproven: /);
      expect(r).toMatch(why);
      expect(r).toMatch(/nothing was written/);
    }
    expect(await code(undefined)).toMatch(/^rds_profile_unproven: .*owner role not found/);
  });
});

describe("role counts (v1)", () => {
  test("18 roles, 10 LOGIN: analysis admit worker login; babysitter retention still deferred", () => {
    expect(SPEC.length).toBe(18);
    expect(LOGINS.length).toBe(10);
    expect(LOGINS).toContain("bulls96_analysis_admit");
    expect(LOGINS).not.toContain("bulls96_babysitter_retention");
  });
});

describe("failure classes: a lost connection is never reported as a policy refusal", () => {
  const conn = Object.assign(new Error("Connection terminated unexpectedly"), {});
  const sock = Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" });
  const shutdown = Object.assign(new Error("terminating connection due to administrator command"), { code: "57P01" });
  const policy = Object.assign(new Error("rds-activation refused"), { code: "55000" });
  test("policy SQLSTATE classes only", () => {
    for (const c of ["0A000", "28P01", "42501", "55000", "55P03", "22023"]) expect(isPolicyRefusal({ code: c })).toBe(true);
    for (const e of [conn, sock, shutdown, { code: "08006" }, { code: "57014" }, { code: "XX000" }, undefined]) expect(isPolicyRefusal(e)).toBe(false);
  });
  test("before or in fragment 00: refused only for a policy refusal; a lost connection is internal_error", () => {
    for (const at of ["connect", "lock", "begin", "00_preconditions"]) {
      expect(classifyActivateFailure(at, policy)).toBe("refused");
      for (const e of [conn, sock, shutdown]) expect(classifyActivateFailure(at, e)).toBe("internal_error");
    }
  });
  test("inside the transaction: rolled_back; at COMMIT a lost connection is internal_error (outcome unknown)", () => {
    for (const at of ["10_provisioner_function_defaults", "30_roles", "70_validate", "40_login:injected-fault"]) {
      expect(classifyActivateFailure(at, policy)).toBe("rolled_back");
      expect(classifyActivateFailure(at, conn)).toBe("rolled_back");
    }
    expect(classifyActivateFailure("commit", conn)).toBe("internal_error");
    // Only a server error reply to COMMIT proves it did not commit.
    expect(classifyActivateFailure("commit", { code: "40001" })).toBe("internal_error");
    expect(classifyActivateFailure("commit", policy)).toBe("internal_error");
    expect(classifyActivateFailure("commit", { severity: "ERROR", code: "40001" })).toBe("rolled_back");
  });
});

describe("invalid input still writes the result when a usable target was given", () => {
  test("target recovery from raw argv/env", () => {
    expect(rawResultTarget(["run", "--result-file", "/x/r.json"], {})).toEqual({ file: "/x/r.json" });
    expect(rawResultTarget(["run", "--result-fd=4"], {})).toEqual({ fd: 4 });
    expect(rawResultTarget(["run"], { HOOPDB_ACTIVATION_RESULT_FD: "5" })).toEqual({ fd: 5 });
    expect(rawResultTarget(["run", "--result-fd", "1"], {})).toBeNull();
    expect(rawResultTarget(["run", "--result-fd", "4", "--result-file", "/x"], {})).toBeNull();
    expect(rawResultTarget(["run", "--result-fd", "4"], { HOOPDB_ACTIVATION_RESULT_FD: "6" })).toBeNull();
    expect(rawResultTarget(["run"], {})).toBeNull();
  });
  test("document shape; the message never carries a value", () => {
    const d = invalidInputResult(["run"], "x");
    expect(d).toMatchObject({ contract: "rds-activation/v1", command: "run", status: "invalid_input", exit_code: 2 });
    expect(invalidInputResult(["bogus"], "x").command).toBeNull();
    let msg = "";
    try {
      parseInputs(["run", "hunter2-not-a-flag", "--result-fd", "3"], {}, () => "", SPEC);
    } catch (e) {
      msg = (e as Error).message;
    }
    expect(msg).not.toContain("hunter2");
  });
  test("CLI end to end: exit 2 and a result file with invalid_input", () => {
    const dir = mkdtempSync(join(tmpdir(), "rds-activation-"));
    try {
      const file = join(dir, "result.json");
      const r = spawnSync("bun", [join(root, "src/hoopdb/rds-activation.ts"), "run", "--result-file", file, "--password=leak-me"], {
        encoding: "utf8", env: { PATH: process.env.PATH ?? "" },
      });
      expect(r.status).toBe(2);
      const doc = JSON.parse(readFileSync(file, "utf8"));
      expect(doc).toMatchObject({ contract: "rds-activation/v1", status: "invalid_input", exit_code: 2, command: "run" });
      expect(doc.steps[0].message).toMatch(/never accepted on the command line/);
      expect(JSON.stringify(doc) + r.stdout + r.stderr).not.toContain("leak-me");
      // An existing file is never overwritten; exit 2 stays the signal.
      const again = spawnSync("bun", [join(root, "src/hoopdb/rds-activation.ts"), "nope", "--result-file", file], {
        encoding: "utf8", env: { PATH: process.env.PATH ?? "" },
      });
      expect(again.status).toBe(2);
      expect(JSON.parse(readFileSync(file, "utf8")).command).toBe("run");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test("writeResult refuses to overwrite an existing file", () => {
    const dir = mkdtempSync(join(tmpdir(), "rds-activation-"));
    try {
      const file = join(dir, "r.json");
      writeResult({ file }, { a: 1 });
      expect(() => writeResult({ file }, { a: 2 })).toThrow();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
