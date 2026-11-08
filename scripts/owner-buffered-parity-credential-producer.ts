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
 *   SUPABASE_PROJECT_REF=<reviewed-target> \
 *   bun run scripts/owner-buffered-parity-credential-producer.ts \
 *     --expect-source-sha <the reviewed merge SHA> | \
 *   bun run scripts/owner-buffered-parity-operator.ts bulk \
 *     --expect-source-sha <the same reviewed merge SHA> ...
 *
 * No credential is accepted through argv, environment, stdin or a file. The
 * authenticated CLIs return the two short-lived values in child stdout, which
 * remains in process memory and is never logged.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { fstatSync } from "node:fs";
import {
  CredentialRefusal,
  MAX_STDIN_CREDENTIAL_BYTES,
  refuseCredentialEnv,
} from "../src/migration/stdin-credentials.ts";
import { parseCliLogin } from "../src/shared/supabase-cli-login.ts";
import { requiredSupabaseProjectRef } from "../src/shared/deploy-target.ts";

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
  // Exact production project pin without publishing the project reference in
  // engine source. The raw value is explicit operator configuration.
  supabaseProjectSha256: "963aa096d6944d19073ec6e37b2dd5162ed1f014d1743c2d55b508f066f6977c",
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

export type ProducerRunner = (command: ProducerCommand, signal?: AbortSignal) => Promise<string>;

export interface SupervisedRunnerOptions {
  timeoutMs?: number;
  killGraceMs?: number;
  maxOutputBytes?: number;
  signal?: AbortSignal;
  /** Test/receipt hook; never receives command output. */
  onSpawn?: (pid: number) => void;
}

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

/** Validate explicit operator target configuration against the reviewed hash. */
export function pinnedSupabaseProjectRef(
  env: Record<string, string | undefined>,
  expectedSha256 = OWNER_PARITY_CREDENTIAL_TARGET.supabaseProjectSha256,
): string {
  let projectRef: string;
  try {
    projectRef = requiredSupabaseProjectRef(env);
  } catch (error) {
    const code = (error as { code?: unknown }).code;
    return refuse(code === "supabase_project_ref_invalid"
      ? "supabase_project_ref_invalid" : "supabase_project_ref_required");
  }
  if (!/^[0-9a-f]{64}$/u.test(expectedSha256) ||
      createHash("sha256").update(projectRef).digest("hex") !== expectedSha256)
    refuse("supabase_target_pin");
  return projectRef;
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
export function pinnedSupabaseOwnerUrl(raw: string, projectRef: string): string {
  let login: { password: string };
  try {
    login = parseCliLogin(raw, projectRef);
  } catch {
    return refuse("supabase_cli_reply_refused");
  }
  const pinned = new URL(`postgresql://${OWNER_PARITY_CREDENTIAL_TARGET.supabaseHost}:5432/postgres`);
  pinned.username = `cli_login_postgres.${projectRef}`;
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

/**
 * Run one CLI in its own process group. Timeout, output overflow and caller
 * cancellation terminate the whole group, escalate to SIGKILL, and stop
 * waiting after a second bounded grace even if a child mishandles SIGTERM.
 */
export function runSupervisedProducerCommand(
  command: ProducerCommand,
  options: SupervisedRunnerOptions = {},
): Promise<string> {
  if (process.platform === "win32")
    return Promise.reject(new OwnerParityCredentialProducerRefusal("credential_runner_platform"));
  const timeoutMs = options.timeoutMs ?? 40_000;
  const killGraceMs = options.killGraceMs ?? 250;
  const maxOutputBytes = options.maxOutputBytes ?? 64 * 1024;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || !Number.isInteger(killGraceMs) || killGraceMs < 1 ||
      !Number.isInteger(maxOutputBytes) || maxOutputBytes < 1)
    return Promise.reject(new OwnerParityCredentialProducerRefusal("credential_runner_bounds"));
  if (options.signal?.aborted)
    return Promise.reject(new OwnerParityCredentialProducerRefusal("credential_command_cancelled"));

  return new Promise((resolve, reject) => {
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command.executable, [...command.args], {
        cwd: process.cwd(),
        env: command.env,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch {
      reject(new OwnerParityCredentialProducerRefusal("credential_command_failed"));
      return;
    }
    if (!child.pid) {
      child.once("error", () => undefined);
      child.stdout?.destroy();
      child.stderr?.destroy();
      child.unref();
      reject(new OwnerParityCredentialProducerRefusal("credential_command_failed"));
      return;
    }
    const chunks: Buffer[] = [];
    let bytes = 0;
    let settled = false;
    let refusal: OwnerParityCredentialProducerRefusal | undefined;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let stopTimer: ReturnType<typeof setTimeout> | undefined;

    const killGroup = (signal: NodeJS.Signals) => {
      try {
        process.kill(-child.pid!, signal);
      } catch {
        try { child.kill(signal); } catch { /* already gone */ }
      }
    };
    const groupAlive = () => {
      try { process.kill(-child.pid!, 0); return true; } catch { return false; }
    };
    const cleanup = () => {
      clearTimeout(timeout);
      if (killTimer) clearTimeout(killTimer);
      if (stopTimer) clearTimeout(stopTimer);
      options.signal?.removeEventListener("abort", onAbort);
      child.stdout?.removeAllListeners();
      child.stderr?.removeAllListeners();
      child.stdout?.destroy();
      child.stderr?.destroy();
      child.unref();
      chunks.length = 0;
    };
    const finish = (error?: OwnerParityCredentialProducerRefusal, output?: string) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve(output ?? "");
    };
    const terminate = (code: string) => {
      if (refusal || settled) return;
      refusal = new OwnerParityCredentialProducerRefusal(code);
      chunks.length = 0;
      killGroup("SIGTERM");
      if (!groupAlive()) return finish(refusal);
      killTimer = setTimeout(() => killGroup("SIGKILL"), killGraceMs);
      stopTimer = setTimeout(() => finish(refusal), killGraceMs * 2);
    };
    const onAbort = () => terminate("credential_command_cancelled");
    const timeout = setTimeout(() => terminate("credential_command_timeout"), timeoutMs);
    options.signal?.addEventListener("abort", onAbort, { once: true });
    try { options.onSpawn?.(child.pid); } catch { terminate("credential_command_failed"); }
    if (options.signal?.aborted) onAbort();
    child.stdout?.on("data", (chunk: Buffer | string) => {
      if (refusal || settled) return;
      const value = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      bytes += value.length;
      if (bytes > maxOutputBytes) return terminate("credential_command_output_limit");
      chunks.push(value);
    });
    // Drain but never retain or expose child stderr; it can contain provider details.
    child.stderr?.on("data", () => undefined);
    child.once("error", () => terminate("credential_command_failed"));
    child.once("close", (code) => {
      // A group descendant can survive the leader's clean SIGTERM exit. Keep
      // the scheduled group SIGKILL and bounded stop timer on every refusal.
      if (refusal) return groupAlive() ? undefined : finish(refusal);
      if (code !== 0) return terminate("credential_command_failed");
      // A successful leader must not leave a background descendant holding
      // the owned process group after its output has been accepted.
      if (groupAlive()) return terminate("credential_command_lingering_process");
      finish(undefined, Buffer.concat(chunks, bytes).toString("utf8"));
    });
  });
}

export function defaultProducerRunner(command: ProducerCommand, signal?: AbortSignal): Promise<string> {
  return runSupervisedProducerCommand(command, { signal });
}

/** Refuse a different or dirty checkout before either credential-producing command. */
export async function requireProducerCheckout(
  expectedSourceSha: string,
  runner: ProducerRunner,
  env: Record<string, string>,
  signal?: AbortSignal,
): Promise<void> {
  if (!/^[0-9a-f]{40}$/u.test(expectedSourceSha)) refuse("producer_source_pin");
  const head = (await runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.git,
    args: ["rev-parse", "HEAD"],
    env,
  }, signal)).trim();
  const dirty = (await runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.git,
    args: ["status", "--porcelain"],
    env,
  }, signal)).trim();
  if (head !== expectedSourceSha || dirty) refuse("producer_checkout_pin_mismatch");
}

/** Acquire both values only after the output sink and environment are proven safe. */
export async function ownerParityCredentialDocument(
  expectedSourceSha: string,
  runner: ProducerRunner = defaultProducerRunner,
  env: Record<string, string | undefined> = process.env,
  output: OutputStat = fstatSync(1),
  signal?: AbortSignal,
  expectedSupabaseProjectSha256 = OWNER_PARITY_CREDENTIAL_TARGET.supabaseProjectSha256,
): Promise<string> {
  requireRamOnlyProducerOutput(output);
  requireProducerEnvironment(env);
  const supabaseProject = pinnedSupabaseProjectRef(env, expectedSupabaseProjectSha256);
  const childEnv = childEnvironment(env);
  await requireProducerCheckout(expectedSourceSha, runner, childEnv, signal);
  if ((await runner({ executable: OWNER_PARITY_CREDENTIAL_TARGET.neonctl, args: ["--version"], env: childEnv }, signal)).trim() !== OWNER_PARITY_CREDENTIAL_TARGET.neonctlVersion)
    refuse("neonctl_version");
  if ((await runner({ executable: OWNER_PARITY_CREDENTIAL_TARGET.supabaseCli, args: ["--version"], env: childEnv }, signal)).trim() !== OWNER_PARITY_CREDENTIAL_TARGET.supabaseCliVersion)
    refuse("supabase_cli_version");
  let neonRaw = await runner({
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
  }, signal);
  const neonUrl = pinnedNeonReadonlyUrl(neonRaw);
  neonRaw = "";
  let supabaseRaw = await runner({
    executable: OWNER_PARITY_CREDENTIAL_TARGET.supabaseCli,
    args: ["db", "dump", "--linked", "--project-ref", supabaseProject, "--dry-run"],
    env: childEnv,
  }, signal);
  const supabaseUrl = pinnedSupabaseOwnerUrl(supabaseRaw, supabaseProject);
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

async function writeProducerOutput(document: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onError = () => reject(new OwnerParityCredentialProducerRefusal("producer_output_closed"));
    process.stdout.once("error", onError);
    process.stdout.write(document + "\n", (error) => {
      process.stdout.removeListener("error", onError);
      if (error) reject(new OwnerParityCredentialProducerRefusal("producer_output_closed"));
      else resolve();
    });
  });
}

/** Bind process signals and the actual fd 1 stream to one cancellation signal. */
export function bindProcessCancellation(): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  // Keep handling repeated cooperative signals until the owned child group is
  // gone; a second signal must not restore the default exit and orphan it.
  process.on("SIGINT", cancel);
  process.on("SIGTERM", cancel);
  process.on("SIGHUP", cancel);
  // A FIFO peer close may not be observable until the final write. Every CLI
  // therefore retains its independent hard wall even with these early hooks.
  process.stdout.once("close", cancel);
  process.stdout.once("error", cancel);
  return {
    signal: controller.signal,
    dispose: () => {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
      process.removeListener("SIGHUP", cancel);
      process.stdout.removeListener("close", cancel);
      process.stdout.removeListener("error", cancel);
    },
  };
}

if (import.meta.main) {
  const cancellation = bindProcessCancellation();
  try {
    const args = parseProducerArgs(process.argv.slice(2));
    const document = await ownerParityCredentialDocument(
      args.expectedSourceSha, defaultProducerRunner, process.env, fstatSync(1), cancellation.signal,
    );
    if (cancellation.signal.aborted) refuse("credential_command_cancelled");
    await writeProducerOutput(document);
  } catch (error) {
    process.stderr.write(JSON.stringify({ status: "refused", code: producerErrorCode(error) }) + "\n");
    process.exitCode = 1;
  } finally {
    cancellation.dispose();
  }
}
