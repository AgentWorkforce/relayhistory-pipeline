/**
 * rds-activation/v1: the named-executor activation for the separate-database layout on
 * Amazon RDS for PostgreSQL 16+ (infra #107: hoopdb / sessions / hoop /
 * sessions_pipeline_runtime). Interface: docs/rds-activation-executor.md. SQL:
 * supabase/bootstrap/rds_activation/*.sql (fragments without BEGIN/COMMIT).
 *
 *   artifact         print the artifact identity, no connection.
 *   create-database  CREATE DATABASE hoopdb if absent (autocommit: PostgreSQL refuses
 *                    CREATE DATABASE inside a transaction block), then verify it.
 *   activate         fragments 00..70 in ONE transaction, as the provisioner.
 *   verify           fragment 70 read-only as hoop, plus a runtime LOGIN probe.
 *   run              create-database, activate (skipped when the shared catalogs show
 *                    the activated state; the provisioner can no longer CONNECT to
 *                    hoopdb), verify.
 *   rotate-self      one LOGIN role changes its own password (the only rotation path:
 *                    after activation nobody but rdsadmin holds ADMIN on any HoopDb role).
 *
 * Secrets come only from the environment or stdin. Only SCRAM verifiers reach the
 * server, and neither secrets nor verifiers appear in output or in the result. The
 * JSON result goes only to --result-file or --result-fd. stdout is a human log.
 */
import { createHash, createHmac, pbkdf2Sync, randomBytes } from "node:crypto";
import { closeSync, openSync, readdirSync, readFileSync, writeSync } from "node:fs";
import { isIP } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

export const CONTRACT = "rds-activation/v1";
export const DATABASE = "hoopdb";
export const OWNER = "hoop";
export const RUNTIME = "sessions_pipeline_runtime";
export const EXECUTOR = "hoopdb_activation_executor";
export const FRAGMENTS = Object.freeze([
  "00_preconditions",
  "10_provisioner_function_defaults",
  "20_executor",
  "30_roles",
  "40_login",
  "50_owner_handoff",
  "60_cleanup",
  "70_validate",
] as const);
export type Fragment = (typeof FRAGMENTS)[number];

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const BOOTSTRAP = "supabase/bootstrap";
const FRAGMENT_DIR = `${BOOTSTRAP}/rds_activation`;
const DRIVER = "src/hoopdb/rds-activation.ts";
const fragmentPath = (f: Fragment) => `${FRAGMENT_DIR}/${f}.sql`;
const readRepo = (rel: string) => readFileSync(join(ROOT, rel));
const sha256 = (b: Buffer | string) => createHash("sha256").update(b).digest("hex");
/** Session-level advisory lock key: two executors never interleave. */
const LOCK_KEY = 0x0968_07a1;

export const EXIT = Object.freeze({
  ok: 0, internal_error: 1, invalid_input: 2, refused: 3, rolled_back: 4, verify_failed: 5,
});

// ---------------------------------------------------------------------------
// Role spec, generated from the repository's role SQL (never hand-maintained)
// ---------------------------------------------------------------------------
export interface RoleSpec { name: string; login: boolean; settings: string[] }

/**
 * The files the role spec is read from (repo-relative, sorted):
 *  - rds_migration_roles.sql: the fixed NOLOGIN inventory ('name'::name entries);
 *  - session_consumer_roles.sql: the bare consumer role (v_role := '...');
 *  - session_directory_search_worker.sql and every operator *_role.sql (except the
 *    legacy runtime-role files): each literal CREATE ROLE / ALTER ROLE statement,
 *    which yields LOGIN and SET k = v role settings.
 */
export function roleSpecSources(list: () => string[] = () => readdirSync(join(ROOT, BOOTSTRAP))): string[] {
  const files = list().filter((f) =>
    f === "rds_migration_roles.sql" || f === "session_consumer_roles.sql" ||
    f === "session_directory_search_worker.sql" ||
    (f.endsWith("_role.sql") && !f.startsWith("hoopdb_runtime_role")));
  return files.sort().map((f) => `${BOOTSTRAP}/${f}`);
}

const stripComments = (sql: string) => sql.split("\n").map((l) => l.replace(/--.*$/, "")).join("\n");

/**
 * LOGIN roles that a landed (frozen) migration requires to be NOLOGIN when it first
 * applies. Activation creates them NOLOGIN, with their settings. After activation nobody
 * holds ADMIN on them, so on this layout they stay NOLOGIN until a reviewed path exists.
 * A unit test pins each entry to the migration text that forces it.
 */
export const LOGIN_DEFERRED_BY_FROZEN_MIGRATION: Readonly<Record<string, string>> = Object.freeze({
  bulls96_babysitter_retention: "supabase/migrations/20261105042000_babysitter_evidence_retention.sql",
});

export function parseRoleSpec(sources: string[], read: (rel: string) => string = (p) => readRepo(p).toString("utf8")): RoleSpec[] {
  const roles = new Map<string, { login: boolean; settings: Map<string, string> }>();
  const touch = (name: string) => {
    if (!roles.has(name)) roles.set(name, { login: false, settings: new Map() });
    return roles.get(name)!;
  };
  for (const p of [OWNER, RUNTIME]) touch(p).login = true;
  for (const src of sources) {
    const sql = stripComments(read(src));
    if (src.endsWith("/rds_migration_roles.sql")) {
      const inventory = /FOREACH\s+v_role\s+IN\s+ARRAY\s+ARRAY\[([\s\S]*?)\]/i.exec(sql)?.[1];
      if (!inventory) throw new Error(`${src}: role inventory not found`);
      for (const m of inventory.matchAll(/'([a-z_][a-z0-9_]*)'::name/g)) touch(m[1]!);
      continue;
    }
    if (src.endsWith("/session_consumer_roles.sql")) {
      const name = /v_role\s+text\s*:=\s*'([a-z_][a-z0-9_]*)'/.exec(sql)?.[1];
      if (!name) throw new Error(`${src}: consumer role name not found`);
      touch(name);
      continue;
    }
    for (const m of sql.matchAll(/\b(CREATE|ALTER)\s+ROLE\s+([a-z_][a-z0-9_]*)\b([^;]*);/gi)) {
      const [, verb, name, rest] = m as unknown as [string, string, string, string];
      const role = touch(name);
      const set = /^\s*SET\s+([a-z_]+)\s*(?:=|TO)\s*'?([A-Za-z0-9]+)'?\s*$/i.exec(rest);
      if (set) {
        const [k, v] = [set[1]!.toLowerCase(), set[2]!];
        if (role.settings.has(k) && role.settings.get(k) !== v) throw new Error(`${src}: conflicting ${name} ${k}`);
        role.settings.set(k, v);
      } else if (/^\s*RESET\b/i.test(rest)) {
        throw new Error(`${src}: RESET of a role setting is not part of the activation spec`);
      } else if (/(^|[^A-Za-z])LOGIN\b/i.test(rest)) {
        role.login = true;
      } else if (verb.toUpperCase() === "ALTER" && !/^\s*(NOLOGIN|NOINHERIT|\s)*$/i.test(rest)) {
        throw new Error(`${src}: unsupported ALTER ROLE ${name}${rest}`);
      }
    }
  }
  return [...roles.entries()]
    .map(([name, r]) => ({
      name,
      login: r.login && !(name in LOGIN_DEFERRED_BY_FROZEN_MIGRATION),
      settings: [...r.settings].map(([k, v]) => `${k}=${v}`).sort(),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

// ---------------------------------------------------------------------------
// Artifact identity
// ---------------------------------------------------------------------------
export interface ArtifactIdentity {
  contract: typeof CONTRACT;
  artifact_sha256: string;
  artifact_files: [string, string][];
}
/**
 * sha256 over the ordered [path, sha256(bytes)] list: the fragments in execution order,
 * then the role-spec source files (sorted), then the driver.
 */
export function artifactIdentity(read: (rel: string) => Buffer = readRepo, sources: string[] = roleSpecSources()): ArtifactIdentity {
  const files: [string, string][] = [...FRAGMENTS.map(fragmentPath), ...sources, DRIVER].map((p) => [p, sha256(read(p))]);
  return { contract: CONTRACT, artifact_sha256: sha256(JSON.stringify(files)), artifact_files: files };
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 verifier (RFC 5802 / RFC 7677; PostgreSQL's stored format)
// ---------------------------------------------------------------------------
export function scramVerifier(password: string, salt: Buffer = randomBytes(16), iterations = 4096): string {
  // Printable ASCII only, so SASLprep is the identity and the server derives the same key.
  if (!/^[\x21-\x7e]+$/.test(password)) throw new Error("password must be printable ASCII without spaces");
  const salted = pbkdf2Sync(Buffer.from(password, "utf8"), salt, iterations, 32, "sha256");
  const clientKey = createHmac("sha256", salted).update("Client Key").digest();
  const storedKey = createHash("sha256").update(clientKey).digest();
  const serverKey = createHmac("sha256", salted).update("Server Key").digest();
  return `SCRAM-SHA-256$${iterations}:${salt.toString("base64")}$${storedKey.toString("base64")}:${serverKey.toString("base64")}`;
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------
export type Command = "artifact" | "create-database" | "activate" | "verify" | "run" | "rotate-self";
const COMMANDS: readonly Command[] = ["artifact", "create-database", "activate", "verify", "run", "rotate-self"];
export interface Secrets {
  provisioner_password?: string;
  /** LOGIN role name -> password. */
  role_passwords: Record<string, string>;
  current_password?: string;
  new_password?: string;
}
export interface Config {
  command: Command;
  host: string;
  port: number;
  provisioner: string;
  maintenanceDb: string;
  tls: "verify-full" | "disable";
  caFile?: string;
  rotateRole?: string;
  result: { file: string } | { fd: number };
  secrets: Secrets;
  spec: RoleSpec[];
}
export class InvalidInput extends Error {}

export const passwordEnv = (role: string) => `HOOPDB_ACTIVATION_PASSWORD_${role.toUpperCase()}`;
const PROVISIONER_PASSWORD_ENV = "HOOPDB_ACTIVATION_PROVISIONER_PASSWORD";
const CURRENT_PASSWORD_ENV = "HOOPDB_ACTIVATION_CURRENT_PASSWORD";
const NEW_PASSWORD_ENV = "HOOPDB_ACTIVATION_NEW_PASSWORD";
const OPTIONS: Record<string, { env: string; flag: boolean }> = {
  "--host": { env: "HOOPDB_ACTIVATION_HOST", flag: false },
  "--port": { env: "HOOPDB_ACTIVATION_PORT", flag: false },
  "--provisioner": { env: "HOOPDB_ACTIVATION_PROVISIONER", flag: false },
  "--maintenance-db": { env: "HOOPDB_ACTIVATION_MAINTENANCE_DB", flag: false },
  "--tls": { env: "HOOPDB_ACTIVATION_TLS", flag: false },
  "--ca-file": { env: "HOOPDB_ACTIVATION_CA_FILE", flag: false },
  "--role": { env: "HOOPDB_ACTIVATION_ROLE", flag: false },
  "--result-file": { env: "HOOPDB_ACTIVATION_RESULT_FILE", flag: false },
  "--result-fd": { env: "HOOPDB_ACTIVATION_RESULT_FD", flag: false },
  "--secrets-stdin": { env: "", flag: true },
};
const IDENT = /^[a-z_][a-z0-9_]{0,62}$/;
const LOOPBACK = new Set(["localhost", "127.0.0.1", "::1"]);
const needs = {
  provisionerName: (c: Command) => c !== "artifact" && c !== "rotate-self",
  provisionerPassword: (c: Command) => c === "create-database" || c === "activate" || c === "run",
  allRolePasswords: (c: Command) => c === "activate" || c === "run",
  verifyPasswords: (c: Command) => c === "verify",
};
const checkPassword = (label: string, v: string) => {
  if (v.length < 32) throw new InvalidInput(`${label} must be at least 32 characters`);
  if (!/^[\x21-\x7e]+$/.test(v)) throw new InvalidInput(`${label} must be printable ASCII without spaces`);
};

/** Resolves every input before any connection. Throws InvalidInput (exit 2). */
export function parseInputs(
  argv: string[],
  env: Record<string, string | undefined>,
  readStdin: () => string = () => readFileSync(0, "utf8"),
  spec: RoleSpec[] = parseRoleSpec(roleSpecSources()),
): Config {
  const [command, ...rest] = argv;
  if (!COMMANDS.includes(command as Command))
    throw new InvalidInput(`command must be one of ${COMMANDS.join(", ")}`);
  const flags: Record<string, string | true> = {};
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    if (a !== "--secrets-stdin" && /password|secret|token|verifier|postgres(ql)?:\/\//i.test(a))
      throw new InvalidInput("secrets and connection URLs are never accepted on the command line");
    const name = a.includes("=") ? a.slice(0, a.indexOf("=")) : a;
    const spec = OPTIONS[name];
    if (!spec) throw new InvalidInput(name.startsWith("--") ? `unknown argument ${name}` : "unexpected positional argument (value not shown)");
    if (name in flags) throw new InvalidInput(`${name} given twice`);
    if (spec.flag) {
      if (a.includes("=")) throw new InvalidInput(`${name} takes no value`);
      flags[name] = true;
    } else {
      const v = a.includes("=") ? a.slice(a.indexOf("=") + 1) : rest[++i];
      if (v === undefined || v === "") throw new InvalidInput(`${name} needs a value`);
      flags[name] = v;
    }
  }
  const opt = (name: string): string | undefined => {
    const f = flags[name];
    const e = env[OPTIONS[name]!.env] || undefined;
    if (typeof f === "string" && e !== undefined && f !== e)
      throw new InvalidInput(`${name} and ${OPTIONS[name]!.env} are both set and differ`);
    return typeof f === "string" ? f : e;
  };
  const c = command as Command;

  const resultFile = opt("--result-file");
  const resultFdText = opt("--result-fd");
  if ((resultFile === undefined) === (resultFdText === undefined))
    throw new InvalidInput("exactly one of --result-file or --result-fd is required");
  let result: Config["result"];
  if (resultFdText !== undefined) {
    if (!/^[0-9]+$/.test(resultFdText) || Number(resultFdText) < 3)
      throw new InvalidInput("--result-fd must be an open descriptor number of 3 or above");
    result = { fd: Number(resultFdText) };
  } else result = { file: resultFile! };

  const host = opt("--host") ?? "";
  if (c !== "artifact" && !host) throw new InvalidInput("--host / HOOPDB_ACTIVATION_HOST is required");
  const portText = opt("--port") ?? "5432";
  if (!/^[0-9]{1,5}$/.test(portText) || Number(portText) < 1 || Number(portText) > 65535)
    throw new InvalidInput("--port must be 1-65535");
  const ours = new Set(spec.map((r) => r.name));
  const provisioner = opt("--provisioner") ?? "";
  if (needs.provisionerName(c) && !IDENT.test(provisioner))
    throw new InvalidInput("--provisioner / HOOPDB_ACTIVATION_PROVISIONER must name the provisioning login (lower-case identifier)");
  if (needs.provisionerName(c) && (ours.has(provisioner) || [EXECUTOR, "rdsadmin", "rds_superuser"].includes(provisioner)))
    throw new InvalidInput("--provisioner must not be a HoopDb, executor or RDS platform role");
  const rotateRole = opt("--role");
  const loginRoles = spec.filter((r) => r.login).map((r) => r.name);
  if (c === "rotate-self" && (!rotateRole || !loginRoles.includes(rotateRole)))
    throw new InvalidInput(`rotate-self needs --role naming one LOGIN role of the spec: ${loginRoles.join(", ")}`);
  if (c !== "rotate-self" && rotateRole !== undefined) throw new InvalidInput("--role is only for rotate-self");
  const maintenanceDb = opt("--maintenance-db") ?? "postgres";
  if (!IDENT.test(maintenanceDb) || maintenanceDb === DATABASE)
    throw new InvalidInput("--maintenance-db must be an identifier other than hoopdb");
  const tls = (opt("--tls") ?? "verify-full") as Config["tls"];
  if (tls !== "verify-full" && tls !== "disable") throw new InvalidInput("--tls must be verify-full or disable");
  if (c !== "artifact" && tls === "disable" && !LOOPBACK.has(host))
    throw new InvalidInput("--tls disable is allowed only for a loopback host (emulation)");
  const caFile = opt("--ca-file");
  if (c !== "artifact" && tls === "verify-full" && !caFile)
    throw new InvalidInput("--ca-file / HOOPDB_ACTIVATION_CA_FILE is required with --tls verify-full");
  if (c !== "artifact" && tls === "verify-full" && isIP(host))
    throw new InvalidInput("--tls verify-full needs the endpoint host name, not an IP address");

  // Secrets: either the environment or one JSON object on stdin, never both.
  const secretEnvNames = [PROVISIONER_PASSWORD_ENV, CURRENT_PASSWORD_ENV, NEW_PASSWORD_ENV, ...loginRoles.map(passwordEnv)];
  const strayEnv = Object.keys(env).filter((k) => k.startsWith("HOOPDB_ACTIVATION_PASSWORD_") && !secretEnvNames.includes(k));
  if (strayEnv.length) throw new InvalidInput(`password variables for roles outside the spec: ${strayEnv.join(", ")}`);
  const envSet = secretEnvNames.filter((k) => env[k] !== undefined);
  let secrets: Secrets;
  if (flags["--secrets-stdin"]) {
    if (envSet.length) throw new InvalidInput("--secrets-stdin with secret environment variables set is ambiguous");
    let parsed: unknown;
    try {
      parsed = JSON.parse(readStdin());
    } catch {
      throw new InvalidInput("stdin must be one JSON object");
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new InvalidInput("stdin must be one JSON object");
    const o = parsed as Record<string, unknown>;
    for (const k of Object.keys(o))
      if (!["provisioner_password", "role_passwords", "current_password", "new_password"].includes(k))
        throw new InvalidInput(`unexpected stdin key: ${k}`);
    for (const k of ["provisioner_password", "current_password", "new_password"])
      if (o[k] !== undefined && typeof o[k] !== "string") throw new InvalidInput(`stdin ${k} must be a string`);
    const rp = o.role_passwords ?? {};
    if (typeof rp !== "object" || Array.isArray(rp) || rp === null) throw new InvalidInput("stdin role_passwords must be an object");
    for (const [k, v] of Object.entries(rp))
      if (!loginRoles.includes(k) || typeof v !== "string") throw new InvalidInput(`stdin role_passwords: unexpected role or non-string value: ${k}`);
    secrets = {
      ...(typeof o.provisioner_password === "string" ? { provisioner_password: o.provisioner_password } : {}),
      ...(typeof o.current_password === "string" ? { current_password: o.current_password } : {}),
      ...(typeof o.new_password === "string" ? { new_password: o.new_password } : {}),
      role_passwords: rp as Record<string, string>,
    };
  } else {
    secrets = {
      ...(env[PROVISIONER_PASSWORD_ENV] !== undefined ? { provisioner_password: env[PROVISIONER_PASSWORD_ENV] } : {}),
      ...(env[CURRENT_PASSWORD_ENV] !== undefined ? { current_password: env[CURRENT_PASSWORD_ENV] } : {}),
      ...(env[NEW_PASSWORD_ENV] !== undefined ? { new_password: env[NEW_PASSWORD_ENV] } : {}),
      role_passwords: Object.fromEntries(loginRoles.filter((r) => env[passwordEnv(r)] !== undefined).map((r) => [r, env[passwordEnv(r)]!])),
    };
  }
  if (needs.provisionerPassword(c) && !secrets.provisioner_password)
    throw new InvalidInput(`the provisioner password is required (${PROVISIONER_PASSWORD_ENV} or stdin)`);
  const required = needs.allRolePasswords(c) ? loginRoles : needs.verifyPasswords(c) ? [OWNER, RUNTIME] : [];
  const missing = required.filter((r) => !secrets.role_passwords[r]);
  if (missing.length) throw new InvalidInput(`passwords are required for: ${missing.join(", ")} (${missing.map(passwordEnv).join(", ")} or stdin role_passwords)`);
  if (c === "rotate-self") {
    if (!secrets.current_password || !secrets.new_password)
      throw new InvalidInput(`rotate-self needs the current and the new password (${CURRENT_PASSWORD_ENV}, ${NEW_PASSWORD_ENV} or stdin)`);
    checkPassword("the new password", secrets.new_password);
    if (secrets.new_password === secrets.current_password) throw new InvalidInput("the new password must differ from the current one");
  }
  const rolePws = Object.values(secrets.role_passwords);
  for (const [r, v] of Object.entries(secrets.role_passwords)) checkPassword(`the password of ${r}`, v);
  if (new Set(rolePws).size !== rolePws.length) throw new InvalidInput("every role password must be distinct");
  if (secrets.provisioner_password !== undefined && rolePws.includes(secrets.provisioner_password))
    throw new InvalidInput("role passwords must differ from the provisioner password");
  return {
    command: c, host, port: Number(portText), provisioner, maintenanceDb, tls,
    ...(caFile ? { caFile } : {}), ...(rotateRole ? { rotateRole } : {}), result, secrets, spec,
  };
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------
export interface Step { name: string; status: "ok" | "skipped" | "failed"; sqlstate: string | null; message: string | null }
export interface Result extends ArtifactIdentity {
  migration_generation: string | null;
  command: Command;
  status: string;
  exit_code: number;
  server_version_num: number | null;
  steps: Step[];
  database: { name: string; created: boolean; owner: string | null };
  graph: { edges: string[]; helper_present: boolean; roles: string[] } | null;
}
export interface ActivateOptions {
  /** Test seam (never exposed on the CLI): raise after this fragment, inside the transaction. */
  faultAfter?: Fragment;
}

type FailStatus = "refused" | "rolled_back" | "verify_failed" | "internal_error";

/**
 * A server-reported policy or precondition outcome (feature, privilege, object state,
 * lock busy, authentication or data refusal) is a refusal. Anything else (a lost
 * connection, a socket error, a timeout, an admin shutdown such as 57P01) carries no
 * policy verdict and is reported as internal_error, so an operator never reads a
 * dropped connection as a refusal.
 */
export function isPolicyRefusal(e: unknown): boolean {
  const code = (e as { code?: string })?.code;
  return typeof code === "string" && /^(0A|28|42|55|22)[0-9A-Z]{3}$/.test(code);
}

/**
 * Classifies an activate-transaction failure by where it happened and what failed.
 * Before or in fragment 00: policy refusal -> refused, otherwise internal_error. Inside
 * fragments 10..70: rolled_back (the server aborts the transaction, connection loss
 * included). At COMMIT, a non-policy failure (connection lost) leaves the outcome
 * unknown, so it is internal_error, and the operator must run verify.
 */
export function classifyActivateFailure(at: string, e: unknown): FailStatus {
  if (at === "connect" || at === "lock" || at === "begin" || at === "00_preconditions")
    return isPolicyRefusal(e) ? "refused" : "internal_error";
  if (at === "commit") return isPolicyRefusal(e) ? "rolled_back" : "internal_error";
  return "rolled_back";
}
class StepFailure extends Error {
  constructor(readonly status: FailStatus, readonly steps: Step[], readonly graph: Result["graph"] = null) {
    super(steps.at(-1)?.message ?? status);
  }
}

function scrub(text: string, cfg: Config): string {
  let out = text;
  const s = cfg.secrets;
  for (const v of [s.provisioner_password, s.current_password, s.new_password, ...Object.values(s.role_passwords)])
    if (v) out = out.split(v).join("[redacted]");
  return out.replace(/SCRAM-SHA-256\$[^\s'"]+/g, "[redacted-verifier]");
}

function connect(cfg: Config, user: string, database: string, password: string): pg.Client {
  return new pg.Client({
    host: cfg.host,
    port: cfg.port,
    user,
    database,
    password,
    application_name: "hoopdb-rds-activation",
    ssl: cfg.tls === "disable" ? false : { ca: readFileSync(cfg.caFile!, "utf8"), rejectUnauthorized: true, servername: cfg.host },
    connectionTimeoutMillis: 15_000,
  });
}

const failed = (cfg: Config, name: string, e: unknown): Step => {
  const code = (e as { code?: string })?.code;
  return {
    name,
    status: "failed",
    sqlstate: typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : null,
    message: scrub(String((e as Error)?.message ?? e), cfg),
  };
};
const ok = (name: string, message: string | null = null): Step => ({ name, status: "ok", sqlstate: null, message });

async function bindSettings(client: pg.Client, cfg: Config, phase: "activate" | "verify", verifiers: Record<string, string> = {}) {
  await client.query(
    `SELECT set_config('hoopdb_activation.phase', $1, true), set_config('hoopdb_activation.provisioner', $2, true),
            set_config('hoopdb_activation.role_spec', $3, true), set_config('hoopdb_activation.verifiers', $4, true)`,
    [phase, cfg.provisioner, JSON.stringify(cfg.spec), JSON.stringify(verifiers)],
  );
}

/** Step 1, autocommit in the maintenance database. Idempotent and verified. */
export async function createDatabase(cfg: Config): Promise<{ step: Step; created: boolean; owner: string | null; version: number }> {
  const name = "create-database";
  const client = connect(cfg, cfg.provisioner, cfg.maintenanceDb, cfg.secrets.provisioner_password!);
  const refuse = (sqlstate: string, message: string) => new StepFailure("refused", [{ name, status: "failed", sqlstate, message }]);
  try {
    await client.connect();
    const who = (await client.query("SELECT session_user::text AS s, current_user::text AS c, current_setting('server_version_num')::int AS v")).rows[0];
    if (who.s !== cfg.provisioner || who.c !== cfg.provisioner) throw refuse("42501", "connected identity is not the named provisioner");
    if (who.v < 160000) throw refuse("0A000", "rds-activation/v1 requires PostgreSQL 16 or later (PostgreSQL 15 is refused)");
    let created = false;
    const exists = async () => (await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [DATABASE])).rowCount === 1;
    if (!(await exists())) {
      try {
        // Not transactional by PostgreSQL design (it copies a template on disk), so this
        // is its own step. template0 guarantees a pristine copy (template1 may be modified).
        await client.query(`CREATE DATABASE ${DATABASE} TEMPLATE template0 ENCODING 'UTF8'`);
        created = true;
      } catch (e) {
        if ((e as { code?: string }).code !== "42P04") throw e; // a concurrent creator won; verify below
      }
    }
    const d = (await client.query(
      `SELECT pg_get_userbyid(datdba) AS owner, pg_encoding_to_char(encoding) AS enc, datistemplate, datallowconn
         FROM pg_database WHERE datname = $1`, [DATABASE])).rows[0];
    const problems = [
      ...(d?.enc === "UTF8" ? [] : [`encoding ${d?.enc}`]),
      ...(d?.datistemplate ? ["is a template"] : []),
      ...(d?.datallowconn ? [] : ["does not allow connections"]),
      ...([cfg.provisioner, OWNER].includes(d?.owner) ? [] : [`owned by ${d?.owner}, expected ${cfg.provisioner} (fresh) or ${OWNER} (activated)`]),
    ];
    if (problems.length) throw refuse("55000", `hoopdb exists but ${problems.join(", ")}`);
    return { step: ok(name, created ? "created" : "already present"), created, owner: d.owner, version: who.v };
  } catch (e) {
    if (e instanceof StepFailure) throw e;
    throw new StepFailure(isPolicyRefusal(e) ? "refused" : "internal_error", [failed(cfg, name, e)]);
  } finally {
    await client.end().catch(() => undefined);
  }
}

/** Shared catalogs only (pg_database, pg_roles): fresh, activated, or neither. */
export async function activationState(cfg: Config): Promise<"fresh" | "activated" | "inconsistent"> {
  const client = connect(cfg, cfg.provisioner, cfg.maintenanceDb, cfg.secrets.provisioner_password!);
  try {
    await client.connect();
    const r = (await client.query(
      `SELECT (SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = $1) AS owner,
              (SELECT count(*)::int FROM pg_roles WHERE rolname = ANY ($2)) AS present,
              EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $3) AS executor`,
      [DATABASE, cfg.spec.map((s) => s.name), EXECUTOR])).rows[0];
    if (r.owner === OWNER && r.present === cfg.spec.length && !r.executor) return "activated";
    // Still the provisioner's database: fragment 00 decides, and lists any pre-existing
    // role (or a leftover executor) in its refusal.
    if (r.owner === cfg.provisioner) return "fresh";
    return "inconsistent";
  } finally {
    await client.end().catch(() => undefined);
  }
}

/** Step 2: fragments 00..70 in one transaction. Any failure rolls everything back. */
export async function activate(cfg: Config, opts: ActivateOptions = {}): Promise<Step[]> {
  const verifiers = Object.fromEntries(cfg.spec.filter((r) => r.login).map((r) => [r.name, scramVerifier(cfg.secrets.role_passwords[r.name]!)]));
  const steps: Step[] = [];
  const client = connect(cfg, cfg.provisioner, DATABASE, cfg.secrets.provisioner_password!);
  let current = "connect";
  try {
    await client.connect();
    current = "lock";
    const lock = (await client.query("SELECT pg_try_advisory_lock($1) AS ok", [LOCK_KEY])).rows[0];
    if (!lock.ok) throw Object.assign(new Error("another rds-activation holds the lock"), { code: "55P03" });
    current = "begin";
    await client.query("BEGIN");
    await client.query("SET LOCAL lock_timeout = '10s'");
    await bindSettings(client, cfg, "activate", verifiers);
    for (const f of FRAGMENTS) {
      current = f;
      await client.query(readRepo(fragmentPath(f)).toString("utf8"));
      steps.push(ok(f));
      if (opts.faultAfter === f) {
        current = `${f}:injected-fault`;
        await client.query("DO $f$ BEGIN RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'rds-activation: injected fault'; END $f$");
      }
    }
    current = "commit";
    await client.query("COMMIT");
    return steps;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => undefined);
    steps.push(failed(cfg, current, e));
    throw new StepFailure(classifyActivateFailure(current, e), steps);
  } finally {
    await client.end().catch(() => undefined);
  }
}

/** Step 3: read-only. Fragment 70 as hoop, then a runtime LOGIN probe. */
export async function verify(cfg: Config): Promise<{ steps: Step[]; graph: Result["graph"] }> {
  const steps: Step[] = [];
  let graph: Result["graph"] = null;
  const names = cfg.spec.map((s) => s.name);
  const hoop = connect(cfg, OWNER, DATABASE, cfg.secrets.role_passwords[OWNER]!);
  try {
    await hoop.connect();
    await hoop.query("BEGIN READ ONLY");
    await bindSettings(hoop, cfg, "verify");
    const g = (await hoop.query(
      `SELECT coalesce((SELECT array_agg(x.e ORDER BY x.e) FROM (
                          SELECT format('%s<-%s/%s', pg_get_userbyid(roleid), pg_get_userbyid(member), pg_get_userbyid(grantor)) AS e
                            FROM pg_auth_members m
                           WHERE m.roleid IN (SELECT oid FROM pg_roles WHERE rolname = ANY ($1))
                              OR m.member IN (SELECT oid FROM pg_roles WHERE rolname = ANY ($1))
                              OR m.grantor IN (SELECT oid FROM pg_roles WHERE rolname = ANY ($1))) x), '{}') AS edges,
              EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2) AS helper,
              coalesce((SELECT array_agg(rolname::text ORDER BY rolname) FROM pg_roles WHERE rolname = ANY ($1)), '{}') AS roles`,
      [names, EXECUTOR])).rows[0];
    graph = { edges: g.edges, helper_present: g.helper, roles: g.roles };
    await hoop.query(readRepo(fragmentPath("70_validate")).toString("utf8"));
    await hoop.query("COMMIT");
    steps.push(ok("verify:hoop"));
  } catch (e) {
    await hoop.query("ROLLBACK").catch(() => undefined);
    steps.push(failed(cfg, "verify:hoop", e));
    throw new StepFailure("verify_failed", steps, graph);
  } finally {
    await hoop.end().catch(() => undefined);
  }
  const rt = connect(cfg, RUNTIME, DATABASE, cfg.secrets.role_passwords[RUNTIME]!);
  try {
    await rt.connect();
    const r = (await rt.query(
      `SELECT session_user::text AS s, current_user::text AS c,
              has_database_privilege(current_user, current_database(), 'CREATE') AS db_create,
              has_database_privilege(current_user, current_database(), 'TEMPORARY') AS db_temp,
              has_schema_privilege(current_user, 'sessions', 'CREATE') AS schema_create`)).rows[0];
    if (r.s !== RUNTIME || r.c !== RUNTIME || r.db_create || r.db_temp || r.schema_create)
      throw Object.assign(new Error(`runtime identity or privileges unexpected: ${JSON.stringify(r)}`), { code: "55000" });
    steps.push(ok("verify:runtime"));
  } catch (e) {
    steps.push(failed(cfg, "verify:runtime", e));
    throw new StepFailure("verify_failed", steps, graph);
  } finally {
    await rt.end().catch(() => undefined);
  }
  return { steps, graph };
}

/**
 * rotate-self: the role logs in with its current password and sets its own new SCRAM
 * verifier (ALTER ROLE CURRENT_USER, which PostgreSQL allows any role for its own
 * password). Then the new password must log in and the old one must be refused.
 */
export async function rotateSelf(cfg: Config): Promise<Step[]> {
  const role = cfg.rotateRole!;
  const name = `rotate-self:${role}`;
  const before = connect(cfg, role, DATABASE, cfg.secrets.current_password!);
  try {
    await before.connect();
    const who = (await before.query("SELECT session_user::text AS s, current_user::text AS c")).rows[0];
    if (who.s !== role || who.c !== role)
      throw new StepFailure("refused", [{ name, status: "failed", sqlstate: "42501", message: "connected identity is not the named role" }]);
    // Same handling as activate: bound as a parameter, used through EXECUTE (never in a
    // top-level statement text), then cleared, all in one transaction.
    await before.query("BEGIN");
    await before.query("SELECT set_config('hoopdb_activation.new_verifier', $1, true)", [scramVerifier(cfg.secrets.new_password!)]);
    await before.query(`DO $rotate$ BEGIN
      IF current_setting('hoopdb_activation.new_verifier') !~ '^SCRAM-SHA-256\\$4096:[A-Za-z0-9+/=]+\\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'rotate-self: malformed verifier';
      END IF;
      EXECUTE format('ALTER ROLE CURRENT_USER PASSWORD %L', current_setting('hoopdb_activation.new_verifier'));
      PERFORM set_config('hoopdb_activation.new_verifier', '', true);
    END $rotate$`);
    await before.query("COMMIT");
  } catch (e) {
    if (e instanceof StepFailure) throw e;
    throw new StepFailure(isPolicyRefusal(e) ? "refused" : "internal_error", [failed(cfg, name, e)]);
  } finally {
    await before.end().catch(() => undefined);
  }
  const steps = [ok(name, "password changed")];
  const after = connect(cfg, role, DATABASE, cfg.secrets.new_password!);
  try {
    await after.connect();
    await after.query("SELECT 1");
  } catch (e) {
    steps.push(failed(cfg, `${name}:new-login`, e));
    throw new StepFailure("verify_failed", steps);
  } finally {
    await after.end().catch(() => undefined);
  }
  const stale = connect(cfg, role, DATABASE, cfg.secrets.current_password!);
  try {
    await stale.connect();
    steps.push({ name: `${name}:old-login`, status: "failed", sqlstate: null, message: "the old password still logs in" });
    throw new StepFailure("verify_failed", steps);
  } catch (e) {
    if (e instanceof StepFailure) throw e;
    if ((e as { code?: string }).code !== "28P01") {
      steps.push(failed(cfg, `${name}:old-login`, e));
      throw new StepFailure("verify_failed", steps);
    }
  } finally {
    await stale.end().catch(() => undefined);
  }
  steps.push(ok(`${name}:verified`, "new password logs in, old password refused"));
  return steps;
}

async function migrationGeneration(): Promise<string | null> {
  try {
    return await (await import("./migrate.ts")).generation();
  } catch {
    return null;
  }
}

/** Runs one command and returns the result document. Never throws. */
export async function execute(cfg: Config, opts: ActivateOptions = {}): Promise<Result> {
  const result: Result = {
    ...artifactIdentity(),
    migration_generation: await migrationGeneration(),
    command: cfg.command,
    status: "internal_error",
    exit_code: EXIT.internal_error,
    server_version_num: null,
    steps: [],
    database: { name: DATABASE, created: false, owner: null },
    graph: null,
  };
  const done = (status: string, code: number) => Object.assign(result, { status, exit_code: code });
  try {
    if (cfg.command === "artifact") return done("artifact", EXIT.ok);
    if (cfg.command === "rotate-self") {
      result.steps.push(...(await rotateSelf(cfg)));
      return done("rotated", EXIT.ok);
    }
    if (cfg.command === "create-database" || cfg.command === "run") {
      const d = await createDatabase(cfg);
      result.steps.push(d.step);
      Object.assign(result.database, { created: d.created, owner: d.owner });
      result.server_version_num = d.version;
      if (cfg.command === "create-database") return done("database_ready", EXIT.ok);
    }
    let already = false;
    if (cfg.command === "activate" || cfg.command === "run") {
      const state = await activationState(cfg);
      if (state === "inconsistent") {
        result.steps.push({ name: "activation-state", status: "failed", sqlstate: "55000",
          message: "hoopdb ownership, HoopDb role existence or the executor match neither the fresh nor the activated state" });
        return done("refused", EXIT.refused);
      }
      if (state === "activated") {
        already = true;
        result.steps.push({ name: "activate", status: "skipped", sqlstate: null, message: "already activated (shared catalogs)" });
      } else {
        result.steps.push(...(await activate(cfg, opts)));
        result.database.owner = OWNER;
      }
      if (cfg.command === "activate") return done(already ? "already_activated" : "activated", EXIT.ok);
    }
    const v = await verify(cfg);
    result.steps.push(...v.steps);
    result.graph = v.graph;
    result.database.owner = OWNER;
    if (cfg.command === "verify") return done("verified", EXIT.ok);
    return done(already ? "already_activated" : "activated", EXIT.ok);
  } catch (e) {
    if (e instanceof StepFailure) {
      result.steps.push(...e.steps);
      if (e.graph) result.graph = e.graph;
      return done(e.status, EXIT[e.status]);
    }
    result.steps.push({ name: "internal", status: "failed", sqlstate: null, message: scrub(String((e as Error)?.message ?? e), cfg) });
    return done("internal_error", EXIT.internal_error);
  }
}

/**
 * For invalid input (exit 2): the result target, if the raw argv/env named a usable one
 * (--result-file/--result-fd, = or separate form, or their env variables). Never
 * throws. When the target itself is the invalid part, it returns null and only exit 2
 * remains.
 */
export function rawResultTarget(argv: string[], env: Record<string, string | undefined>): Config["result"] | null {
  const pick = (flag: string, envName: string): string | undefined => {
    const vals: string[] = [];
    argv.forEach((a, i) => {
      if (a === flag && argv[i + 1] !== undefined) vals.push(argv[i + 1]!);
      else if (a.startsWith(`${flag}=`)) vals.push(a.slice(flag.length + 1));
    });
    if (env[envName]) vals.push(env[envName]!);
    return new Set(vals).size === 1 ? vals[0] : undefined;
  };
  const file = pick("--result-file", "HOOPDB_ACTIVATION_RESULT_FILE");
  const fd = pick("--result-fd", "HOOPDB_ACTIVATION_RESULT_FD");
  if (file !== undefined && fd !== undefined) return null;
  if (fd !== undefined) return /^[0-9]+$/.test(fd) && Number(fd) >= 3 ? { fd: Number(fd) } : null;
  return file ? { file } : null;
}

/** The exit-2 result document. The message names flags or variables, never their values. */
export function invalidInputResult(argv: string[], message: string) {
  const command = COMMANDS.includes(argv[0] as Command) ? (argv[0] as Command) : null;
  return {
    contract: CONTRACT,
    command,
    status: "invalid_input",
    exit_code: EXIT.invalid_input,
    steps: [{ name: "inputs", status: "failed", sqlstate: null, message }],
  };
}

export function writeResult(target: Config["result"], result: object): void {
  const body = `${JSON.stringify(result, null, 2)}\n`;
  if ("fd" in target) {
    writeSync(target.fd, body);
    return;
  }
  const fd = openSync(target.file, "wx", 0o600);
  try {
    writeSync(fd, body);
  } finally {
    closeSync(fd);
  }
}

if (import.meta.main) {
  let cfg: Config;
  try {
    cfg = parseInputs(process.argv.slice(2), process.env);
  } catch (e) {
    const message = e instanceof InvalidInput ? e.message : "input could not be parsed";
    console.error(`rds-activation: invalid input: ${message}`);
    // Written whenever a usable result target was given; otherwise exit 2 alone.
    const target = rawResultTarget(process.argv.slice(2), process.env);
    if (target) {
      try {
        writeResult(target, invalidInputResult(process.argv.slice(2), message));
      } catch (w) {
        console.error(`rds-activation: could not write the result: ${(w as Error).message}`);
      }
    }
    process.exit(EXIT.invalid_input);
  }
  const result = await execute(cfg);
  for (const s of result.steps)
    console.log(`${s.status.toUpperCase().padEnd(7)} ${s.name}${s.sqlstate ? ` ${s.sqlstate}` : ""}${s.message ? ` ${s.message}` : ""}`);
  console.log(`rds-activation: ${result.status} (exit ${result.exit_code}) artifact ${result.artifact_sha256}`);
  try {
    writeResult(cfg.result, result);
  } catch (e) {
    console.error(`rds-activation: could not write the result: ${(e as Error).message}`);
    process.exit(EXIT.internal_error);
  }
  process.exit(result.exit_code);
}
