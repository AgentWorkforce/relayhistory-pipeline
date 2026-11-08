/**
 * RAM-only credential producer for the post-G4 owner-buffered parity operator.
 *
 * It obtains the existing Neon relayhistory_readonly connection and the
 * existing Supabase CLI temporary login, validates both against fixed target
 * metadata, and writes exactly one {neonUrl,supabaseUrl} JSON object to stdout.
 * Stdout must be a pipe or socket whose downstream fd 0 is the parity operator;
 * a terminal or regular file is refused before either credential is acquired.
 *
 * Usage (source-only until Root's post-G4 execution gates pass):
 *   bun run scripts/owner-buffered-parity-credential-producer.ts \
 *     --expect-source-sha <the reviewed merge SHA> | \
 *   bun run scripts/owner-buffered-parity-operator.ts bulk \
 *     --expect-source-sha <the same reviewed merge SHA> ...
 *
 * No credential is accepted through argv, environment, stdin or a file. The
 * authenticated CLIs return the two short-lived values in child stdout, which
 * remains in process memory and is never logged.
 */
import { execFileSync } from "node:child_process";
import { fstatSync } from "node:fs";
import {
  CredentialRefusal,
  MAX_STDIN_CREDENTIAL_BYTES,
  refuseCredentialEnv,
} from "../src/migration/stdin-credentials.ts";
import { parseCliLogin } from "../src/shared/supabase-cli-login.ts";

export const OWNER_PARITY_CREDENTIAL_TARGET = {
  git: "/usr/bin/git",
  neonctl: "/home/khaliqgant/.local/share/mise/installs/npm-neonctl/4.18.0/node_modules/.bin/neonctl",
  neonctlVersion: "4.18.0",
  neonProject: "divine-snow-84812652",
  neonBranch: "br-damp-mud-apbr6ifp",
  neonRole: "relayhistory_readonly",
  neonDatabase: "neondb",
  neonHost: "ep-floral-pond-apkvat38.c-7.us-east-1.aws.neon.tech",
  supabaseCli: "/home/khaliqgant/.local/bin/supabase",
  supabaseCliVersion: "2.122.0",
  supabaseProject: "pyamkrhoyszgzirqqffq",
  supabaseHost: "aws-0-us-east-1.pooler.supabase.com",
} as const;

export class OwnerParityCredentialProducerRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "OwnerParityCredentialProducerRefusal";
  }
}

const PRODUCER_CREDENTIAL_ENV = [
  "NEON_API_KEY",
  "NEON_DATABASE_URL",
  "SUPABASE_ACCESS_TOKEN",
  "SUPABASE_DB_PASSWORD",
  "SUPABASE_URL",
] as const;

interface OutputStat {
  isCharacterDevice(): boolean;
  isFile(): boolean;
  isFIFO(): boolean;
  isSocket(): boolean;
}

export interface ProducerCommand {
  executable: string;
  args: readonly string[];
  env: Record<string, string>;
}

export type ProducerRunner = (command: ProducerCommand) => string;

export interface ProducerArgs {
  expectedSourceSha: string;
}

function refuse(code: string): never {
  throw new OwnerParityCredentialProducerRefusal(code);
}

export function parseProducerArgs(argv: readonly string[]): ProducerArgs {
  if (argv.length !== 2 || argv[0] !== "--expect-source-sha" || !/^[0-9a-f]{40}$/u.test(argv[1] ?? ""))
    refuse("producer_usage");
  return { expectedSourceSha: argv[1]! };
}

/** Refuse any route that could print or persist the produced credential JSON. */
export function requireRamOnlyProducerOutput(stat: OutputStat = fstatSync(1)): void {
  if (stat.isCharacterDevice()) refuse("producer_stdout_is_a_terminal");
  if (stat.isFile()) refuse("producer_stdout_is_a_file");
  if (!stat.isFIFO() && !stat.isSocket()) refuse("producer_stdout_not_pipe_or_socket");
}

export function requireProducerEnvironment(env: Record<string, string | undefined> = process.env): void {
  refuseCredentialEnv(env);
  for (const key of PRODUCER_CREDENTIAL_ENV)
    if (env[key] !== undefined) throw new CredentialRefusal(`credential_env_refused.${key}`);
}

function decode(value: string, code: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return refuse(code);
  }
}

/** Validate the exact existing restricted Neon identity and normalize verify-full. */
export function pinnedNeonReadonlyUrl(raw: string): string {
  if (Buffer.byteLength(raw) > 64 * 1024) refuse("neon_reply_limit");
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    value = raw.trim();
  }
  if (typeof value !== "string" || !value || /[\x00-\x1f\x7f]/u.test(value))
    refuse("neon_reply_shape");
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return refuse("neon_url_parse");
  }
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    url.hash !== "" ||
    decode(url.username, "neon_url_parse") !== OWNER_PARITY_CREDENTIAL_TARGET.neonRole ||
    !url.password ||
    url.hostname !== OWNER_PARITY_CREDENTIAL_TARGET.neonHost ||
    (url.port !== "" && url.port !== "5432") ||
    decode(url.pathname, "neon_url_parse") !== `/${OWNER_PARITY_CREDENTIAL_TARGET.neonDatabase}`
  ) refuse("neon_target_pin");
  for (const [key, setting] of url.searchParams) {
    if (key === "sslmode" && (setting === "require" || setting === "verify-full")) continue;
    if (key === "channel_binding" && setting === "require") continue;
    refuse("neon_startup_parameter_refused");
  }
  const password = decode(url.password, "neon_url_parse");
  const pinned = new URL(`postgresql://${OWNER_PARITY_CREDENTIAL_TARGET.neonHost}:5432/${OWNER_PARITY_CREDENTIAL_TARGET.neonDatabase}`);
  pinned.username = OWNER_PARITY_CREDENTIAL_TARGET.neonRole;
  pinned.password = password;
  pinned.searchParams.set("sslmode", "verify-full");
  return pinned.toString();
}

/** Parse the reviewed CLI reply and rebuild the fixed session-pooler 5432 URL. */
export function pinnedSupabaseOwnerUrl(raw: string): string {
  let login: { password: string };
  try {
    login = parseCliLogin(raw, OWNER_PARITY_CREDENTIAL_TARGET.supabaseProject);
  } catch {
    return refuse("supabase_cli_reply_refused");
  }
  const pinned = new URL(`postgresql://${OWNER_PARITY_CREDENTIAL_TARGET.supabaseHost}:5432/postgres`);
  pinned.username = `cli_login_postgres.${OWNER_PARITY_CREDENTIAL_TARGET.supabaseProject}`;
  pinned.password = login.password;
  login.password = "";
  pinned.searchParams.set("sslmode", "verify-full");
  return pinned.toString();
}

function childEnvironment(env: Record<string, string | undefined>): Record<string, string> {
  return Object.fromEntries(
    ["PATH", "HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "DBUS_SESSION_BUS_ADDRESS"]
      .flatMap((key) => env[key] ? [[key, env[key]!]] : []),
  );
}

export function defaultProducerRunner(command: ProducerCommand): string {
  try {
    return execFileSync(command.executable, [...command.args], {
      cwd: process.cwd(),
      env: command.env,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 40_000,
      maxBuffer: 64 * 1024,
    });
  } catch {
    return refuse("credential_command_failed");
  }
}

/** Refuse a different or dirty checkout before either credential-producing command. */
export function requireProducerCheckout(
  expectedSourceSha: string,
  runner: ProducerRunner,
  env: Record<string, string>,
): void {
  if (!/^[0-9a-f]{40}$/u.test(expectedSourceSha)) refuse("producer_source_pin");
  const head = runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.git,
    args: ["rev-parse", "HEAD"],
    env,
  }).trim();
  const dirty = runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.git,
    args: ["status", "--porcelain"],
    env,
  }).trim();
  if (head !== expectedSourceSha || dirty) refuse("producer_checkout_pin_mismatch");
}

/** Acquire both values only after the output sink and environment are proven safe. */
export function ownerParityCredentialDocument(
  expectedSourceSha: string,
  runner: ProducerRunner = defaultProducerRunner,
  env: Record<string, string | undefined> = process.env,
  output: OutputStat = fstatSync(1),
): string {
  requireRamOnlyProducerOutput(output);
  requireProducerEnvironment(env);
  const childEnv = childEnvironment(env);
  requireProducerCheckout(expectedSourceSha, runner, childEnv);
  if (runner({ executable: OWNER_PARITY_CREDENTIAL_TARGET.neonctl, args: ["--version"], env: childEnv }).trim() !== OWNER_PARITY_CREDENTIAL_TARGET.neonctlVersion)
    refuse("neonctl_version");
  if (runner({ executable: OWNER_PARITY_CREDENTIAL_TARGET.supabaseCli, args: ["--version"], env: childEnv }).trim() !== OWNER_PARITY_CREDENTIAL_TARGET.supabaseCliVersion)
    refuse("supabase_cli_version");
  let neonRaw = runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.neonctl,
    args: [
      "connection-string",
      OWNER_PARITY_CREDENTIAL_TARGET.neonBranch,
      "--project-id", OWNER_PARITY_CREDENTIAL_TARGET.neonProject,
      "--role-name", OWNER_PARITY_CREDENTIAL_TARGET.neonRole,
      "--database-name", OWNER_PARITY_CREDENTIAL_TARGET.neonDatabase,
      "--output", "json",
      "--no-color",
    ],
    env: childEnv,
  });
  const neonUrl = pinnedNeonReadonlyUrl(neonRaw);
  neonRaw = "";
  let supabaseRaw = runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.supabaseCli,
    args: ["db", "dump", "--linked", "--project-ref", OWNER_PARITY_CREDENTIAL_TARGET.supabaseProject, "--dry-run"],
    env: childEnv,
  });
  const supabaseUrl = pinnedSupabaseOwnerUrl(supabaseRaw);
  supabaseRaw = "";
  const document = JSON.stringify({ neonUrl, supabaseUrl });
  if (Buffer.byteLength(document) > MAX_STDIN_CREDENTIAL_BYTES) refuse("credential_document_limit");
  return document;
}

export function producerErrorCode(error: unknown): string {
  const code = error instanceof OwnerParityCredentialProducerRefusal || error instanceof CredentialRefusal
    ? error.code : "credential_producer_failed";
  return /^[A-Za-z0-9_.-]{1,96}$/u.test(code) ? code : "credential_producer_failed";
}

if (import.meta.main) {
  try {
    const args = parseProducerArgs(process.argv.slice(2));
    process.stdout.write(ownerParityCredentialDocument(args.expectedSourceSha) + "\n");
  } catch (error) {
    process.stderr.write(JSON.stringify({ status: "refused", code: producerErrorCode(error) }) + "\n");
    process.exitCode = 1;
  }
}
