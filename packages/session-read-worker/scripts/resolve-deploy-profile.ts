/** Resolve the Reader deploy profile before render. Precedence, highest first:
 *   1. explicit inputs (owner_buffered=on/off and its child inputs);
 *   2. the committed production profile, deploy/production-profile.json (DEPLOY_TARGET=production
 *      only; owner_buffered=profile, the default, or an empty child input under explicit on);
 *   3. the live Worker version (owner_buffered=preserve, and every setting the profile does not own).
 * Profile mode is guarded: it may fill a live setting that is unset (the restore case) but refuses
 * to change or drop one that is set, and never turns owner-buffered reads on from off.
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

/** The four settings deploy/production-profile.json owns, as resolver/render env keys. */
export type CommittedProfile = { OWNER_BUFFERED: string; OWNER_BUFFERED_WORKSPACES: string; OWNER_BUFFERED_EVENTS: string; OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: string };
const PROFILE_FILE_KEYS: Record<string, keyof CommittedProfile> = {
  owner_buffered: "OWNER_BUFFERED", owner_buffered_workspaces: "OWNER_BUFFERED_WORKSPACES",
  owner_buffered_events: "OWNER_BUFFERED_EVENTS", owner_buffered_statement_timeout_ms: "OWNER_BUFFERED_STATEMENT_TIMEOUT_MS",
};
const PROFILE_CHILDREN = ["OWNER_BUFFERED_WORKSPACES", "OWNER_BUFFERED_EVENTS", "OWNER_BUFFERED_STATEMENT_TIMEOUT_MS"] as const;
/** Not in the committed profile: profile mode preserves these from live exactly as preserve mode does. */
const LIVE_ONLY_CHILDREN = ["OWNER_BUFFERED_STAGE_CONCURRENCY", "OWNER_BUFFERED_PHASE_LOG", "OWNER_BUFFERED_INDEX_VERIFY", "BUFFERED_SEARCH_GATE"] as const;
const PROFILE_FILE = new URL("../deploy/production-profile.json", import.meta.url);

/** Strict: exactly the four keys; workspaces only "*" (never a workspace id in the repo). Values are re-validated by the render. */
export function parseCommittedProfile(raw: string): CommittedProfile {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new ProfileError("Committed production profile is not valid JSON"); }
  need(value !== null && typeof value === "object" && !Array.isArray(value), "Committed production profile must be a JSON object");
  const entries = Object.entries(value);
  need(entries.length === 4 && entries.every(([k, v]) => Object.hasOwn(PROFILE_FILE_KEYS, k) && typeof v === "string"), "Committed production profile must set exactly owner_buffered, owner_buffered_workspaces, owner_buffered_events and owner_buffered_statement_timeout_ms as strings");
  const p = Object.fromEntries(entries.map(([k, v]) => [PROFILE_FILE_KEYS[k], (v as string).trim()])) as CommittedProfile;
  need(["on", "off"].includes(p.OWNER_BUFFERED) && ["on", "off"].includes(p.OWNER_BUFFERED_EVENTS), "Committed production profile flags must be on or off");
  need(p.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS === "off" || /^[1-9][0-9]*$/.test(p.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS), "Committed production profile timeout must be off or an integer");
  need(p.OWNER_BUFFERED === "on" ? p.OWNER_BUFFERED_WORKSPACES === "*" : p.OWNER_BUFFERED_WORKSPACES === "" && p.OWNER_BUFFERED_EVENTS === "off" && p.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS === "off", 'Committed production profile workspaces must be "*" when on (no workspace ids in the repo) and every child off when off');
  return p;
}

function productionProfile(): CommittedProfile {
  let raw: string;
  try { raw = readFileSync(PROFILE_FILE, "utf8"); } catch { throw new ProfileError("Cannot read the committed production profile"); }
  return parseCommittedProfile(raw);
}

/** The guard for profile mode: fill an unset live setting (e.g. the lost statement timeout), never change or drop a set one. */
function committedOverLive(live: Record<string, string>, profile: CommittedProfile): Record<string, string> {
  need(!(profile.OWNER_BUFFERED === "on" && live.OWNER_BUFFERED !== "on"), "Live owner-buffered reads are off and a default dispatch does not turn them on; choose explicit owner_buffered=on (or preserve)");
  need(!(profile.OWNER_BUFFERED === "off" && live.OWNER_BUFFERED === "on"), "The committed production profile would drop live owner-buffered reads; choose explicit owner_buffered=off (or preserve)");
  const out: Record<string, string> = { OWNER_BUFFERED: profile.OWNER_BUFFERED };
  for (const key of PROFILE_CHILDREN) {
    const was = live[key] ?? "";
    need(was === "" || was === "off" || was === profile[key], `The committed production profile would change or drop the live ${key.toLowerCase()} setting; update deploy/production-profile.json or choose explicit owner_buffered=on/off`);
    out[key] = profile[key];
  }
  return out;
}

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

/** Injected GET (and profile) for offline tests; production URL/account/script are fixed. */
export async function resolveProfile(env: Environment, template: string, get: (path: string) => Promise<unknown>, committed?: CommittedProfile): Promise<Record<string, string>> {
  const requested = env.OWNER_BUFFERED?.trim() || "profile";
  need(["profile", "preserve", "on", "off"].includes(requested), "owner_buffered must be profile, preserve, on or off");
  // An empty or "profile" child input defers to the committed profile; below it is simply unset.
  const explicit: Environment = { ...env };
  for (const key of PROFILE_CHILDREN) if (!env[key]?.trim() || env[key]!.trim() === "profile") explicit[key] = "";
  // Only the production deploy target has a committed profile; elsewhere "profile" means preserve.
  const profile = env.DEPLOY_TARGET === "production" ? committed ?? productionProfile() : undefined;
  const mode = requested === "profile" && !profile ? "preserve" : requested;
  let resolved: Record<string, string>;
  if (mode === "preserve" || mode === "profile") {
    // Child inputs would otherwise be silently ignored. Require explicit on/off to change them.
    for (const key of LIVE_ONLY_CHILDREN) need(!explicit[key] || explicit[key] === "off", `${mode} cannot be combined with feature overrides; explicitly choose on/off`);
    for (const key of ["OWNER_BUFFERED_EVENTS", "OWNER_BUFFERED_STATEMENT_TIMEOUT_MS"]) {
      // In preserve, "off" was the old unset default; in profile mode it would override the committed value.
      need(!explicit[key] || (mode === "preserve" && explicit[key] === "off"), `${mode} cannot be combined with feature overrides; explicitly choose on/off`);
    }
    need(!explicit.OWNER_BUFFERED_WORKSPACES, `${mode} cannot change the workspace allowlist`);
    const { id, version } = await readLive(get);
    const live = preservedProfile(version);
    resolved = { ...live, ...(mode === "profile" ? committedOverLive(live, profile!) : {}), PRESERVED_READER_VERSION: id };
    const directory = requestedDirectory(env);
    need(directory === undefined || directory === resolved.DIRECTORY_WORKSPACES, "Directory input/environment differs from live; choose explicit owner_buffered=on/off to change the directory list");
    unchangedOtherSettings(version, template, { ...env, ...resolved });
  } else {
    // Explicit mode renders the inputs; on production an unset child falls back to the committed profile.
    const children: Record<string, string> = {};
    for (const key of PROFILE_CHILDREN) children[key] = explicit[key] || (mode === "on" && profile ? profile[key] : key === "OWNER_BUFFERED_WORKSPACES" ? "" : "off");
    const directory = requestedDirectory(env);
    if (directory === undefined) {
      const { id, version } = await readLive(get);
      resolved = { OWNER_BUFFERED: mode, ...children, DIRECTORY_WORKSPACES: liveDirectory(version), PRESERVED_READER_VERSION: id };
    } else resolved = { OWNER_BUFFERED: mode, ...children, DIRECTORY_WORKSPACES: directory };
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
