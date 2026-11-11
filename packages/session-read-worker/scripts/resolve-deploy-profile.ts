/** Preserve the live buffered-read profile unless the operator explicitly chooses on/off.
 * Only nonsecret, validated settings cross into GITHUB_ENV. No provider mutation.
 */
import { appendFileSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { CONTROL_HEAD_BINDING, parseCanaryWorkspaces, renderDeployConfig, RenderError, type RenderInputs } from "./render-deploy-config.ts";

type Binding = { name?: unknown; type?: unknown; text?: unknown; service?: unknown; entrypoint?: unknown; environment?: unknown; id?: unknown };
type Version = { id?: unknown; resources?: { bindings?: Binding[] } };
type Environment = Record<string, string | undefined>;

const VARIABLES = {
  OWNER_BUFFERED_WORKSPACES: "READ_API_OWNER_BUFFERED_WORKSPACES",
  OWNER_BUFFERED_S3_BUCKET: "INGEST_S3_BUCKET",
  OWNER_BUFFERED_S3_REGION: "INGEST_S3_REGION",
  DIRECTORY_WORKSPACES: "READ_API_DIRECTORY_WORKSPACES",
  OWNER_BUFFERED_STAGE_CONCURRENCY: "READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY",
  OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS",
} as const;
const FLAGS = {
  OWNER_BUFFERED: ["RELAYHISTORY_READ_OWNER_BUFFERED", ["true", "1"]],
  OWNER_BUFFERED_EVENTS: ["READ_API_OWNER_BUFFERED_EVENTS", ["on"]],
  OWNER_BUFFERED_PHASE_LOG: ["READ_API_OWNER_BUFFERED_PHASE_LOG", ["1"]],
  OWNER_BUFFERED_INDEX_VERIFY: ["READ_API_OWNER_BUFFERED_INDEX_VERIFY", ["on"]],
  BUFFERED_SEARCH_GATE: ["READ_API_BUFFERED_SEARCH_GATE", ["on"]],
} as const;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function activeVersion(d: unknown): string {
  const active = (d as { deployments?: Array<{ versions?: Array<{ version_id?: string; percentage?: number }> }> })?.deployments?.[0]?.versions;
  need(active?.length === 1 && active[0]?.percentage === 100 && UUID.test(active[0].version_id ?? ""), "Preservation requires exactly one live version at 100%; refusing an absent or split deployment");
  return active[0].version_id!;
}

function need(ok: unknown, message: string): asserts ok {
  if (!ok) throw new ProfileError(message);
}
class ProfileError extends Error {}

function directoryValue(value: string): string {
  if (value && value !== "*") parseCanaryWorkspaces(value);
  return value;
}

async function readLive(get: (path: string) => Promise<unknown>): Promise<{ id: string; version: Version }> {
  const id = activeVersion(await get("/deployments"));
  const version = await get(`/versions/${id}`) as Version;
  need(version.id === id, "Live version identity mismatch");
  return { id, version };
}

/** Omitted input/variable means preserve; only an explicit 'off' input clears the list. */
function requestedDirectory(env: Environment): string | undefined {
  const input = env.DIRECTORY_WORKSPACES_INPUT?.trim();
  if (input) return directoryValue(input === "off" ? "" : input);
  return env.DIRECTORY_WORKSPACES?.trim() ? directoryValue(env.DIRECTORY_WORKSPACES.trim()) : undefined;
}

function liveDirectory(version: Version): string {
  const matches = version.resources?.bindings?.filter(b => b.name === "READ_API_DIRECTORY_WORKSPACES");
  need(matches?.length === 1 && matches[0]?.type === "plain_text" && typeof matches[0].text === "string", "Live directory setting is missing or ambiguous; provide an explicit directory input");
  return directoryValue(matches[0].text);
}

/** Refuse collateral changes instead of guessing how to preserve an unrelated binding. */
export function unchangedOtherSettings(version: Version, template: string, env: Environment): void {
  const rendered = ts.parseConfigFileTextToJson("rendered.jsonc", renderDeployConfig(template, profileInputs(env)));
  need(!rendered.error, "Cannot compare rendered Reader settings");
  const config = rendered.config;
  const vars = { ...config.vars, READ_API_ORIGIN: env.ORIGIN ?? config.vars.READ_API_ORIGIN, READ_API_COMPLETE_SINCE: env.COMPLETE_SINCE ?? config.vars.READ_API_COMPLETE_SINCE };
  for (const [key, name] of [["BRIEF_TITLES_WORKSPACES", "READ_API_BRIEF_TITLES_WORKSPACES"], ["TIMING_ASSUME_NO_TURNS", "READ_API_TIMING_ASSUME_NO_TURNS"]] as const) {
    if (env[key]) vars[name] = env[key];
  }
  const managed = new Set<string>([...Object.values(VARIABLES), ...Object.values(FLAGS).map(([name]) => name)]);
  for (const b of version.resources?.bindings ?? []) {
    need(typeof b.name === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(b.name), "Invalid live binding name");
    const name = b.name;
    if (managed.has(name) || name === CONTROL_HEAD_BINDING.binding) continue;
    if (b.type === "secret_text") {
      need(!(name in vars), `Preserve refuses replacing secret binding ${name} with a plain variable`);
    } else if (b.type === "plain_text") {
      need(vars[name] === b.text, `Preserve would change or drop live setting ${name}; review the explicit deployment configuration`);
    } else if (b.type === "hyperdrive") {
      need(config.hyperdrive?.some((v: { binding?: string; id?: string }) => v.binding === name && v.id === b.id), `Preserve would change or drop live Hyperdrive binding ${name}`);
    } else if (b.type === "service") {
      need(config.services?.some((v: { binding?: string; service?: string; entrypoint?: string; environment?: string }) => v.binding === name && v.service === b.service && v.entrypoint === b.entrypoint && (v.environment ?? "production") === (b.environment ?? "production")), `Preserve would change or drop live service binding ${name}`);
    } else throw new ProfileError(`Preserve cannot compare live binding ${name}; explicit deployment review required`);
  }
}

export function preservedProfile(version: Version): Record<string, string> {
  const bindings = version.resources?.bindings;
  need(Array.isArray(bindings), "Live version has no binding inventory; refusing to guess a deployment profile");
  const names = bindings.map((b) => b.name);
  need(new Set(names).size === names.length, "Live version has duplicate bindings");
  const known = new Set<string>([...Object.values(VARIABLES), ...Object.values(FLAGS).map(([name]) => name)]);
  need(!names.some((name) => typeof name === "string" && (name.startsWith("READ_API_OWNER_BUFFERED_") || name.startsWith("READ_API_BUFFERED_SEARCH_")) && !known.has(name)), "Unknown live owner-buffered setting; update preservation before deploying");
  const text = (name: string): string => {
    const b = bindings.find((v) => v.name === name);
    if (!b) return "";
    need(b.type === "plain_text" && typeof b.text === "string" && b.text.length <= 8192 && !/[\u0000-\u001f\u007f]/.test(b.text), "Live feature binding is not safe plain text");
    return b.text;
  };
  const env: Record<string, string> = {};
  for (const [key, [name, enabled]] of Object.entries(FLAGS)) {
    const value = text(name);
    need(value === "" || (enabled as readonly string[]).includes(value), "Live feature flag has an unknown value; refusing to change its meaning");
    env[key] = value === "" ? "off" : "on";
  }
  for (const [key, name] of Object.entries(VARIABLES)) env[key] = text(name);
  for (const k of ["OWNER_BUFFERED_STAGE_CONCURRENCY", "OWNER_BUFFERED_STATEMENT_TIMEOUT_MS"]) env[k] ||= "off";
  directoryValue(env.DIRECTORY_WORKSPACES!);
  const control = bindings.find((b) => b.name === CONTROL_HEAD_BINDING.binding);
  if (env.OWNER_BUFFERED === "on" || env.BUFFERED_SEARCH_GATE === "on") {
    need(control?.type === "service" && control.service === CONTROL_HEAD_BINDING.service && control.entrypoint === CONTROL_HEAD_BINDING.entrypoint && (control.environment === undefined || control.environment === "production"), "Live control-head binding does not match the pinned privacy authority");
  } else need(!control, "Live control binding without an enabled reader; explicit deployment review required");
  if (env.OWNER_BUFFERED === "off") need(!env.OWNER_BUFFERED_S3_BUCKET && !env.OWNER_BUFFERED_S3_REGION, "Live bucket settings without owner-buffered reads; explicit deployment review required");
  return env;
}

export function profileInputs(env: Environment): RenderInputs {
  return {
    hyperdriveId: env.HYPERDRIVE_ID ?? "", ownerBuffered: env.OWNER_BUFFERED,
    ownerBufferedWorkspaces: env.OWNER_BUFFERED_WORKSPACES, directoryWorkspaces: env.DIRECTORY_WORKSPACES,
    s3Bucket: env.OWNER_BUFFERED_S3_BUCKET, s3Region: env.OWNER_BUFFERED_S3_REGION,
    ownerBufferedEvents: env.OWNER_BUFFERED_EVENTS, bufferedSearchGate: env.BUFFERED_SEARCH_GATE,
    ownerBufferedStageConcurrency: env.OWNER_BUFFERED_STAGE_CONCURRENCY,
    ownerBufferedPhaseLog: env.OWNER_BUFFERED_PHASE_LOG, ownerBufferedIndexVerify: env.OWNER_BUFFERED_INDEX_VERIFY,
    ownerBufferedStatementTimeoutMs: env.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS,
  };
}

/** Injected GET for offline tests; production URL/account/script are fixed. */
export async function resolveProfile(env: Environment, template: string, get: (path: string) => Promise<unknown>): Promise<Record<string, string>> {
  const mode = env.OWNER_BUFFERED?.trim() || "preserve";
  need(["preserve", "on", "off"].includes(mode), "owner_buffered must be preserve, on or off");
  let resolved: Record<string, string>;
  if (mode === "preserve") {
    // Child inputs would otherwise be silently ignored. Require explicit on/off to change them.
    for (const key of ["OWNER_BUFFERED_EVENTS", "OWNER_BUFFERED_STAGE_CONCURRENCY", "OWNER_BUFFERED_PHASE_LOG", "OWNER_BUFFERED_INDEX_VERIFY", "BUFFERED_SEARCH_GATE", "OWNER_BUFFERED_STATEMENT_TIMEOUT_MS"]) {
      need(!env[key] || env[key] === "off", "preserve cannot be combined with feature overrides; explicitly choose on/off");
    }
    need(!env.OWNER_BUFFERED_WORKSPACES, "preserve cannot change the workspace allowlist");
    const { id, version } = await readLive(get);
    resolved = { ...preservedProfile(version), PRESERVED_READER_VERSION: id };
    const requested = requestedDirectory(env);
    need(requested === undefined || requested === resolved.DIRECTORY_WORKSPACES, "Directory input/environment differs from live; choose explicit owner_buffered=on/off to change the directory list");
    unchangedOtherSettings(version, template, { ...env, ...resolved });
  } else {
    const directory = requestedDirectory(env);
    if (directory === undefined) {
      const { id, version } = await readLive(get);
      resolved = { OWNER_BUFFERED: mode, DIRECTORY_WORKSPACES: liveDirectory(version), PRESERVED_READER_VERSION: id };
    } else resolved = { OWNER_BUFFERED: mode, DIRECTORY_WORKSPACES: directory };
  }
  // Use the same validation as the real render: no weaker second interpretation of privacy flags.
  renderDeployConfig(template, profileInputs({ ...env, ...resolved }));
  return resolved;
}

export async function checkPreservedVersion(env: Environment, get: (path: string) => Promise<unknown>): Promise<void> {
  if (!env.PRESERVED_READER_VERSION) return;
  need(UUID.test(env.PRESERVED_READER_VERSION), "Invalid preserved version");
  need(activeVersion(await get("/deployments")) === env.PRESERVED_READER_VERSION, "Live Reader changed after preservation; rerun instead of overwriting its profile");
}

export function workflowEnvironment(resolved: Record<string, string>): string {
  const allowed = new Set([...Object.keys(VARIABLES), ...Object.keys(FLAGS), "PRESERVED_READER_VERSION"]);
  return Object.entries(resolved).map(([key, value]) => {
    need(allowed.has(key) && !/[\r\n\u0000]/.test(value), "Unsafe workflow environment value");
    return `${key}=${value}\n`;
  }).join("");
}

export function apiRoot(template: string, expectedAccount: string | undefined): string {
  const config = ts.parseConfigFileTextToJson("wrangler.jsonc", template);
  const { account_id: account, name } = config.config ?? {};
  need(!config.error && typeof account === "string" && /^[0-9a-f]{32}$/.test(account) && account === expectedAccount, "Worker template and deploy account differ");
  need(typeof name === "string" && /^[a-z0-9-]{1,63}$/.test(name), "Invalid Worker script name");
  return `https://api.cloudflare.com/client/v4/accounts/${account}/workers/scripts/${name}`;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const template = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    const root = apiRoot(template, process.env.CLOUDFLARE_ACCOUNT_ID);
    const get = async (path: string) => {
      const token = process.env.CLOUDFLARE_API_TOKEN;
      need(token, "Cloudflare token required for live configuration read");
      const response = await fetch(root + path, { headers: { Authorization: `Bearer ${token}` }, redirect: "error", signal: AbortSignal.timeout(10_000) });
      need(response.ok, "Cannot read live Reader configuration; refusing to deploy");
      const raw = await response.text();
      need(raw.length <= 2 * 1024 * 1024, "Live configuration response too large");
      const value = JSON.parse(raw);
      need(value.success === true && value.result, "Cloudflare refused live configuration read");
      return value.result;
    };
    if (process.argv[2] === "--check-live-version") {
      await checkPreservedVersion(process.env, get);
      console.log("Live Reader profile guard checked; no provider writes");
    } else {
      need(process.argv.length === 2, "Unsupported profile command");
      const resolved = await resolveProfile(process.env, template, get);
      need(process.env.GITHUB_ENV && process.env.GITHUB_OUTPUT, "GitHub output files required");
      appendFileSync(process.env.GITHUB_ENV, workflowEnvironment(resolved));
      appendFileSync(process.env.GITHUB_OUTPUT, `owner_buffered=${resolved.OWNER_BUFFERED}\n`);
      console.log(`Reader profile validated: owner-buffered ${resolved.OWNER_BUFFERED}; no provider writes`);
    }
  } catch (error) {
    const reason = error instanceof ProfileError || error instanceof RenderError ? error.message : "Live profile could not be read or parsed";
    console.error(`::error::Reader profile refused: ${reason}. No deploy.`);
    process.exitCode = 1;
  }
}
