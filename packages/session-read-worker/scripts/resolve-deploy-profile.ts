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
import { TENANT_KEY_BASE } from "../../../stacks/aws/tenants.ts";
import { ANALYSIS_ADMIT_BINDING, CONTROL_HEAD_BINDING, parseCanaryWorkspaces, RAW_STORE_OTHER_READS_VAR, RAW_STORE_VARS, renderDeployConfig, RenderError, type RenderInputs } from "./render-deploy-config.ts";

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
  OWNER_BUFFERED_EXACT_SESSION: ["READ_API_OWNER_BUFFERED_EXACT_SESSION", ["on"]],
  OWNER_BUFFERED_STAGES_PAGED: ["READ_API_OWNER_BUFFERED_STAGES_PAGED", ["on"]],
  OWNER_BUFFERED: ["RELAYHISTORY_READ_OWNER_BUFFERED", ["true", "1"]],
  OWNER_BUFFERED_EVENTS: ["READ_API_OWNER_BUFFERED_EVENTS", ["on"]],
  OWNER_BUFFERED_PHASE_LOG: ["READ_API_OWNER_BUFFERED_PHASE_LOG", ["1"]],
  OWNER_BUFFERED_INDEX_VERIFY: ["READ_API_OWNER_BUFFERED_INDEX_VERIFY", ["on"]],
  BUFFERED_SEARCH_GATE: ["READ_API_BUFFERED_SEARCH_GATE", ["on"]],
} as const;
/**
 * Deploy-command-only flags (--var, never the render): an "on" input renders exactly "true"; off renders
 * nothing. Never preserved from live: a dispatch that would drop a live one is refused (unchangedOtherSettings).
 */
export const SUMMARY_FLAGS = {
  BRIEF_SUMMARY: "READ_API_BRIEF_SUMMARY_ENABLED",
  LIST_SHORT_SUMMARY: "READ_API_LIST_SHORT_SUMMARY_ENABLED",
  LIST_READINESS: "READ_API_LIST_READINESS_ENABLED",
  SUMMARY_FALLBACK: "READ_API_SUMMARY_FALLBACK_ENABLED",
  SUMMARY_SEARCH: "READ_API_SUMMARY_SEARCH_ENABLED",
  RELAYFACTS: "READ_API_RELAYFACTS_ENABLED",
  SESSION_SOURCES: "READ_API_SESSION_SOURCES_ENABLED",
  ANALYSIS: "READ_API_ANALYSIS_ENABLED",
} as const;
/**
 * The analysis input's values: off (the default; never drops a live analysis flag or ANALYSIS_ADMIT
 * binding, in every mode), on (renders READ_API_ANALYSIS_ENABLED=true), disable (the explicit
 * removal: renders no flag and lets the dispatch drop the live flag and, with
 * SESSION_READ_ANALYSIS_HYPERDRIVE_ID unset, the live binding; a set variable must name the live
 * binding's config, so disable never silently replaces it).
 */
const ANALYSIS_VALUES = ["", "off", "on", "disable"];

/**
 * The analysis guard, in every mode (profile, preserve, and explicit owner_buffered=on/off, which
 * otherwise renders its inputs without comparing live): unless analysis=disable, a live
 * READ_API_ANALYSIS_ENABLED=true needs analysis=on, and a live ANALYSIS_ADMIT binding needs
 * SESSION_READ_ANALYSIS_HYPERDRIVE_ID to name the same config. With analysis=disable, the variable
 * must be unset (drop the binding) or name the live binding's config (keep it), never another one.
 */
export function unchangedAnalysisSettings(version: Version, env: Environment): void {
  if (env.ANALYSIS === "disable") {
    const set = (env.ANALYSIS_HYPERDRIVE_ID ?? "").trim();
    const live = (version.resources?.bindings ?? []).find((b) => b.name === ANALYSIS_ADMIT_BINDING);
    need(!set || !live || (live.type === "hyperdrive" && live.id === set), `analysis=disable would replace live Hyperdrive binding ${ANALYSIS_ADMIT_BINDING}; unset SESSION_READ_ANALYSIS_HYPERDRIVE_ID to drop it, or keep the live config's id`);
    return;
  }
  for (const b of version.resources?.bindings ?? []) {
    if (b.name === SUMMARY_FLAGS.ANALYSIS) {
      need(b.type === "plain_text" && typeof b.text === "string", `Live ${SUMMARY_FLAGS.ANALYSIS} is not plain text; explicit deployment review required`);
      need(b.text !== "true" || env.ANALYSIS === "on", `This dispatch would change or drop live setting ${SUMMARY_FLAGS.ANALYSIS}; repeat analysis=on, or choose analysis=disable to remove it`);
    } else if (b.name === ANALYSIS_ADMIT_BINDING) {
      need(b.type === "hyperdrive" && typeof b.id === "string" && (env.ANALYSIS_HYPERDRIVE_ID ?? "").trim() === b.id, `This dispatch would change or drop live Hyperdrive binding ${ANALYSIS_ADMIT_BINDING}; keep SESSION_READ_ANALYSIS_HYPERDRIVE_ID, or choose analysis=disable to remove it`);
    }
  }
}

/** True when the dispatch renders the raw-store vars (render-deploy-config.ts rawStoreVars). */
function rendersRawStore(env: Environment): boolean {
  return env.SUMMARY_FALLBACK === "on" || env.ANALYSIS === "on";
}

/** The owner_buffered inputs that select explicit mode (resolveProfile), where the live-settings guard does not run. */
const EXPLICIT_MODES = ["on", "off"];

/** The raw store's credential secrets: expected live without a bucket (the render never writes them). */
const RAW_STORE_CREDENTIALS = ["PIPELINE_RAW_S3_ACCESS_KEY_ID", "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"];

/** The raw-store removal: analysis=disable without summary_fallback=on lets the dispatch drop the live raw-store vars. */
function removesRawStore(env: Environment): boolean {
  return env.ANALYSIS === "disable" && env.SUMMARY_FALLBACK !== "on";
}

/**
 * RAW_STORE_OTHER_READS, the render input for READ_API_RAW_STORE_OTHER_READS (index.ts
 * rawStoreOtherReadsEnabled), decided from the live version so that rendering or removing the
 * raw store for the summary fallback or analysis never changes what timing, turns, metadata,
 * owner-buffered turns, hosted canonical events, babysitter evidence or /v1/canonical/* answer:
 *   - the dispatch renders the raw store: first, in every mode, refused while any of RAW_STORE_VARS
 *     is live as a secret, and refused while no PIPELINE_RAW_S3_BUCKET is live but another live
 *     PIPELINE_RAW_* binding is (other than the two credential secrets and RAW_STORE_VARS);
 *   - the dispatch renders the raw store and the live Worker has the switch: keep it (only "off"
 *     is accepted; any other live value or a secret is refused);
 *   - it renders the raw store, the live Worker has PIPELINE_RAW_S3_BUCKET and no switch: add no
 *     switch (those reads were already served through the live raw store), and require all of
 *     RAW_STORE_VARS live as plain text, so the render changes none of them;
 *   - it renders the raw store and no raw store is live: "off" (this render introduces the raw
 *     store, and the switch keeps those reads off);
 *   - the raw-store removal (removesRawStore): refused while a live plain raw-store var exists
 *     without a live switch "off" (those reads are served through the pre-existing raw store and
 *     dropping it would silently turn them off), and refused while a live switch sits beside a
 *     secret bucket (dropping the switch would turn them on); otherwise "" (the switch is dropped
 *     with the vars);
 *   - otherwise "": nothing rendered. In profile and preserve mode a live switch and live plain
 *     raw-store vars are compared like any other live setting; in explicit owner_buffered=on/off
 *     mode (no such comparison) the two removal refusals above apply instead.
 * Returns "" or "off". Never turns those reads on; turning them on is a separate decision.
 * resolveProfile carries a non-empty value as RAW_STORE_OTHER_READS and ignores the key in its input.
 */
export function rawStoreOtherReads(version: Version, env: Environment): string {
  const bindings = version.resources?.bindings ?? [];
  const sw = bindings.find((b) => b.name === RAW_STORE_OTHER_READS_VAR);
  const bucket = bindings.find((b) => b.name === RAW_STORE_VARS[0]);
  if (sw) need(sw.type === "plain_text" && sw.text === "off", `Live ${RAW_STORE_OTHER_READS_VAR} is not plain text "off"; explicit deployment review required`);
  const livePlain = bindings.some((b) => (RAW_STORE_VARS as readonly unknown[]).includes(b.name) && b.type === "plain_text");
  if (removesRawStore(env)) {
    need(!livePlain || sw, `analysis=disable would drop the live raw-store vars that timing, turns and metadata read through (no live ${RAW_STORE_OTHER_READS_VAR}); keep summary_fallback=on or analysis=on, or remove them in a separate reviewed change`);
    need(!sw || !bucket || bucket.type === "plain_text", `analysis=disable would drop live ${RAW_STORE_OTHER_READS_VAR} while ${RAW_STORE_VARS[0]} stays a secret, turning timing, turns and metadata on; explicit deployment review required`);
    return "";
  }
  if (!rendersRawStore(env)) {
    // Explicit owner_buffered=on/off skips the live-settings guard (unchangedOtherSettings), which
    // in profile and preserve mode already refuses dropping any of these live plain vars. Here the
    // dispatch would drop them silently, so the two removal refusals apply. With none of them live
    // (today's redeploy) nothing is refused and nothing changes.
    if (EXPLICIT_MODES.includes(env.OWNER_BUFFERED?.trim() ?? "")) {
      need(!livePlain || sw, `This dispatch (summary_fallback and analysis both off) would drop the live raw-store vars that timing, turns and metadata read through (no live ${RAW_STORE_OTHER_READS_VAR}); keep summary_fallback=on or analysis=on, or remove them in a separate reviewed change`);
      need(!sw || !bucket || bucket.type === "plain_text", `This dispatch (summary_fallback and analysis both off) would drop live ${RAW_STORE_OTHER_READS_VAR} while ${RAW_STORE_VARS[0]} stays a secret, turning timing, turns and metadata on; keep summary_fallback=on or analysis=on, or remove them in a separate reviewed change`);
    }
    return "";
  }
  // From here the dispatch renders the raw store. In every mode, and before a live switch is
  // accepted: never shadow a secret with a rendered plain var (explicit mode skips the
  // live-settings guard that otherwise refuses it).
  for (const name of RAW_STORE_VARS) {
    need(bindings.find((b) => b.name === name)?.type !== "secret_text", `The render refuses replacing secret binding ${name} with a plain variable`);
  }
  // Fresh-add detection keys on PIPELINE_RAW_S3_BUCKET. Without it, any other live PIPELINE_RAW_*
  // setting (a provider, endpoint, SSE mode or a store set up by hand) would be combined with the
  // rendered values; only the two credential secrets are expected there.
  if (!bucket) {
    const other = bindings.find((b) => typeof b.name === "string" && b.name.startsWith("PIPELINE_RAW_") && !RAW_STORE_CREDENTIALS.includes(b.name) && !(RAW_STORE_VARS as readonly string[]).includes(b.name));
    const shown = /^[A-Z0-9_]{1,64}$/.test(String(other?.name)) ? String(other?.name) : "another PIPELINE_RAW_* binding";
    need(!other, `The live Worker has ${shown} without ${RAW_STORE_VARS[0]}; rendering the raw store would combine it with the rendered values; explicit deployment review required`);
  }
  if (sw) return "off";
  if (!bucket) return "off";
  // The values this dispatch renders (render-deploy-config.ts rawStoreVars); compared here in every
  // mode, since explicit owner_buffered=on/off skips the live-settings guard.
  const rendered = [(env.RAW_S3_BUCKET ?? "").trim(), (env.RAW_S3_REGION ?? "").trim(), TENANT_KEY_BASE, "true"];
  RAW_STORE_VARS.forEach((name, i) => {
    const live = bindings.find((b) => b.name === name);
    need(live?.type === "plain_text", `The live Worker has ${RAW_STORE_VARS[0]} without ${RAW_STORE_OTHER_READS_VAR} and without plain ${name}; rendering the raw store would change what timing, turns and metadata read; explicit deployment review required`);
    // An empty bucket or region is left to the render's own refusal, which names the variables.
    need(!rendered[i] || live.text === rendered[i], `This dispatch would change or drop live setting ${name}, which timing, turns and metadata read (no live ${RAW_STORE_OTHER_READS_VAR}); explicit deployment review required`);
  });
  return "";
}

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
const LIVE_ONLY_CHILDREN = ["OWNER_BUFFERED_EXACT_SESSION", "OWNER_BUFFERED_STAGES_PAGED", "OWNER_BUFFERED_STAGE_CONCURRENCY", "OWNER_BUFFERED_PHASE_LOG", "OWNER_BUFFERED_INDEX_VERIFY", "BUFFERED_SEARCH_GATE"] as const;
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
  for (const [key, name] of Object.entries(SUMMARY_FLAGS)) {
    if (env[key] === "on") vars[name] = "true";
  }
  const managed = new Set<string>([...Object.values(VARIABLES), ...Object.values(FLAGS).map(([name]) => name)]);
  // analysis=disable is the explicit removal of the flag and the binding (unchangedAnalysisSettings).
  if (env.ANALYSIS === "disable") for (const name of [SUMMARY_FLAGS.ANALYSIS, ANALYSIS_ADMIT_BINDING]) managed.add(name);
  // The raw-store vars render while summary_fallback or analysis is on and are compared like any
  // other live setting, so a dispatch never silently drops or changes them. analysis=disable with
  // summary_fallback not on is the explicit removal of the last reader that needs them.
  // The switch (READ_API_RAW_STORE_OTHER_READS) goes with them; rawStoreOtherReads refuses the
  // removal whenever dropping either would change what timing, turns or metadata answer.
  if (removesRawStore(env)) for (const name of [...RAW_STORE_VARS, RAW_STORE_OTHER_READS_VAR]) managed.add(name);
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
    ownerBufferedEvents: env.OWNER_BUFFERED_EVENTS, ownerBufferedExactSession: env.OWNER_BUFFERED_EXACT_SESSION, ownerBufferedStagesPaged: env.OWNER_BUFFERED_STAGES_PAGED, bufferedSearchGate: env.BUFFERED_SEARCH_GATE,
    ownerBufferedStageConcurrency: env.OWNER_BUFFERED_STAGE_CONCURRENCY,
    ownerBufferedPhaseLog: env.OWNER_BUFFERED_PHASE_LOG, ownerBufferedIndexVerify: env.OWNER_BUFFERED_INDEX_VERIFY,
    ownerBufferedStatementTimeoutMs: env.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS,
    analysisHyperdriveId: env.ANALYSIS_HYPERDRIVE_ID,
    summaryFallback: env.SUMMARY_FALLBACK, analysis: env.ANALYSIS,
    rawS3Bucket: env.RAW_S3_BUCKET, rawS3Region: env.RAW_S3_REGION,
    rawStoreOtherReads: env.RAW_STORE_OTHER_READS,
  };
}

/** Injected GET (and profile) for offline tests; production URL/account/script are fixed. */
export async function resolveProfile(input: Environment, template: string, get: (path: string) => Promise<unknown>, committed?: CommittedProfile): Promise<Record<string, string>> {
  // RAW_STORE_OTHER_READS is decided here from the live Worker (rawStoreOtherReads), never taken from the environment.
  const env: Environment = { ...input, RAW_STORE_OTHER_READS: undefined };
  const otherReadsKey = (value: string): Record<string, string> => (value ? { RAW_STORE_OTHER_READS: value } : {});
  const requested = env.OWNER_BUFFERED?.trim() || "profile";
  need(["profile", "preserve", "on", "off"].includes(requested), "owner_buffered must be profile, preserve, on or off");
  for (const key of Object.keys(SUMMARY_FLAGS)) {
    if (key === "ANALYSIS") need(ANALYSIS_VALUES.includes(env[key] ?? ""), "analysis must be off, on or disable");
    else need(["", "off", "on"].includes(env[key] ?? ""), `${key.toLowerCase()} must be off or on`);
  }
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
    resolved = { ...live, ...(mode === "profile" ? committedOverLive(live, profile!) : {}), ...otherReadsKey(rawStoreOtherReads(version, env)), PRESERVED_READER_VERSION: id };
    const directory = requestedDirectory(env);
    need(directory === undefined || directory === resolved.DIRECTORY_WORKSPACES, "Directory input/environment differs from live; choose explicit owner_buffered=on/off to change the directory list");
    unchangedOtherSettings(version, template, { ...env, ...resolved });
    unchangedAnalysisSettings(version, env);
  } else {
    // Explicit mode renders the inputs; on production an unset child falls back to the committed profile.
    const children: Record<string, string> = {};
    for (const key of PROFILE_CHILDREN) children[key] = explicit[key] || (mode === "on" && profile ? profile[key] : key === "OWNER_BUFFERED_WORKSPACES" ? "" : "off");
    const directory = requestedDirectory(env);
    if (directory !== undefined && env.ANALYSIS === "disable") {
      // Break-glass: explicit owner_buffered=on/off with a directory and analysis=disable contacts
      // no live metadata at all (it works with the live Worker split, absent or unreadable). It
      // cannot compare live, so SESSION_READ_ANALYSIS_HYPERDRIVE_ID, if set, renders as given.
      // Nor can it tell whether summary_fallback=on would introduce the raw store (and with it
      // timing, turns and metadata reads), so that combination is refused.
      need(!rendersRawStore(env), "summary_fallback=on needs the live raw-store check, which break-glass (explicit owner_buffered with a directory and analysis=disable) skips; dispatch without the directory input");
      resolved = { OWNER_BUFFERED: mode, ...children, DIRECTORY_WORKSPACES: directory };
    } else {
      // Otherwise explicit mode renders its inputs without the profile/preserve comparison, but
      // still reads live for the analysis guard, so an owner_buffered=on/off dispatch never
      // silently drops a live analysis flag or ANALYSIS_ADMIT binding. With a directory given,
      // the version read here is not pinned (no PRESERVED_READER_VERSION).
      const { id, version } = await readLive(get);
      unchangedAnalysisSettings(version, env);
      const otherReads = otherReadsKey(rawStoreOtherReads(version, env));
      if (directory === undefined) {
        resolved = { OWNER_BUFFERED: mode, ...children, DIRECTORY_WORKSPACES: liveDirectory(version), ...otherReads, PRESERVED_READER_VERSION: id };
      } else resolved = { OWNER_BUFFERED: mode, ...children, DIRECTORY_WORKSPACES: directory, ...otherReads };
    }
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
  const allowed = new Set([...Object.keys(VARIABLES), ...Object.keys(FLAGS), "RAW_STORE_OTHER_READS", "PRESERVED_READER_VERSION"]);
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
