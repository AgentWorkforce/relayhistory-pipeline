/**
 * Pure planning logic for the 96bulls-session-ingest CI deploy
 * (.github/workflows/deploy-session-ingest.yml, runbooks/sessions-prod-session-ingest.md).
 *
 * The workflow reads the live Worker (the one version at 100%) and renders a wrangler
 * config that reproduces it binding for binding. Only the feature knobs below may differ,
 * by this precedence: an explicit dispatch input, then the committed production profile
 * (deploy/session-ingest/production-profile.json), then live. Everything else (INGEST_EXPECT_GENERATION,
 * S3/KMS, limits, Hyperdrive, queues, Secrets Store bindings, Worker secrets) is copied from
 * live, and the guard refuses a render that drops or changes any of it.
 *
 * Nothing here talks to the network or holds a credential; cli.ts does the reads.
 * Error messages carry variable names and knob values (on/off/drain/buffered/canonical) only,
 * never a live value, id or secret.
 */
import { COMPATIBILITY_DATE, COMPATIBILITY_FLAGS, SESSION_INGEST_WORKER } from "../session-ingest.ts";

export class IngestDeployError extends Error {}
const need: (ok: unknown, why: string) => asserts ok = (ok, why) => {
  if (!ok) throw new IngestDeployError(why);
};

/** The knobs the committed profile owns. Nothing else may appear in it. */
export const PROFILE_KNOBS = [
  "INGEST_ACCEPTANCE_MODE",
  "INGEST_ASYNC_ADMISSION",
  "INGEST_CLAIM_LOCK_BUSY",
  "INGEST_CLAIM_TIMEOUT_BUSY",
] as const;
/** Every knob a dispatch may touch: the profile's, plus the input-only frozen-receipt flag. */
export const KNOBS = [...PROFILE_KNOBS, "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT"] as const;
export type ProfileKnob = (typeof PROFILE_KNOBS)[number];
export type Knob = (typeof KNOBS)[number];

/** Canonical values; the first is what an absent variable means at runtime (src/session-ingest/worker.ts). */
export const KNOB_VALUES: Record<Knob, readonly string[]> = {
  INGEST_ACCEPTANCE_MODE: ["canonical", "buffered"],
  INGEST_ASYNC_ADMISSION: ["off", "drain", "on"],
  INGEST_CLAIM_LOCK_BUSY: ["off", "on"],
  INGEST_CLAIM_TIMEOUT_BUSY: ["off", "on"],
  INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: ["off", "on"],
};
const absentMeans = (k: Knob) => KNOB_VALUES[k][0]!;
const isKnob = (name: string): name is Knob => (KNOBS as readonly string[]).includes(name);

export type CommittedProfile = Record<ProfileKnob, string>;

/** Strict: a JSON object with exactly the four profile knobs, each a canonical string, no duplicate keys. */
export function parseCommittedProfile(raw: string): CommittedProfile {
  need(typeof raw === "string" && raw.length > 0 && raw.length <= 4096, "production profile must be a small JSON file");
  need(!raw.startsWith("﻿"), "production profile must not start with a byte-order mark");
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new IngestDeployError("production profile is not valid JSON");
  }
  need(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed), "production profile must be a JSON object");
  const keys = Object.keys(parsed as object);
  for (const k of keys) need((PROFILE_KNOBS as readonly string[]).includes(k), `production profile has an unknown key (only ${PROFILE_KNOBS.join(", ")})`);
  for (const k of PROFILE_KNOBS) {
    need(keys.includes(k), `production profile must set ${k}`);
    // JSON.parse keeps the last of duplicate keys silently; refuse them instead.
    need((raw.match(new RegExp(`"${k}"\\s*:`, "g")) ?? []).length === 1, `production profile sets ${k} more than once`);
  }
  const profile = parsed as Record<string, unknown>;
  for (const k of PROFILE_KNOBS) {
    const v = profile[k];
    need(typeof v === "string" && KNOB_VALUES[k].includes(v), `production profile ${k} must be one of ${KNOB_VALUES[k].join(", ")}`);
  }
  return profile as CommittedProfile;
}

export interface DispatchInputs {
  INGEST_ASYNC_ADMISSION: "profile" | "off" | "drain" | "on";
  INGEST_CLAIM_LOCK_BUSY: "profile" | "off" | "on";
  INGEST_CLAIM_TIMEOUT_BUSY: "profile" | "off" | "on";
  /** "" = preserve live. */
  INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "" | "off" | "on";
  dryRun: boolean;
}

/** Workflow env names for the dispatch inputs (inputs reach the script only through env). */
export const INPUT_ENV = {
  INGEST_ASYNC_ADMISSION: "IN_INGEST_ASYNC_ADMISSION",
  INGEST_CLAIM_LOCK_BUSY: "IN_INGEST_CLAIM_LOCK_BUSY",
  INGEST_CLAIM_TIMEOUT_BUSY: "IN_INGEST_CLAIM_TIMEOUT_BUSY",
  INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "IN_INGEST_SYNC_ADMISSION_FROZEN_RECEIPT",
  dryRun: "IN_DRY_RUN",
} as const;

export function parseDispatchInputs(env: Record<string, string | undefined>): DispatchInputs {
  const choice = <T extends string>(name: string, allowed: readonly T[]): T => {
    const v = env[name];
    need(typeof v === "string" && (allowed as readonly string[]).includes(v), `input ${name} must be one of ${allowed.map((a) => a || "(empty)").join(", ")}`);
    return v as T;
  };
  return {
    INGEST_ASYNC_ADMISSION: choice(INPUT_ENV.INGEST_ASYNC_ADMISSION, ["profile", "off", "drain", "on"] as const),
    INGEST_CLAIM_LOCK_BUSY: choice(INPUT_ENV.INGEST_CLAIM_LOCK_BUSY, ["profile", "off", "on"] as const),
    INGEST_CLAIM_TIMEOUT_BUSY: choice(INPUT_ENV.INGEST_CLAIM_TIMEOUT_BUSY, ["profile", "off", "on"] as const),
    // Free-text in the dispatch form: exactly empty, "off" or "on" (no case folding, no whitespace).
    INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: choice(INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT, ["", "off", "on"] as const),
    dryRun: choice(INPUT_ENV.dryRun, ["true", "false"] as const) === "true",
  };
}

/** The defaults a dispatch without any input carries. */
export const DEFAULT_INPUTS: DispatchInputs = {
  INGEST_ASYNC_ADMISSION: "profile",
  INGEST_CLAIM_LOCK_BUSY: "profile",
  INGEST_CLAIM_TIMEOUT_BUSY: "profile",
  INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "",
  dryRun: true,
};

// ---------------------------------------------------------------- live Worker

export type LiveBinding =
  | { type: "plain_text"; name: string; text: string }
  | { type: "secret_text"; name: string }
  | { type: "hyperdrive"; name: string; id: string }
  | { type: "queue"; name: string; queue_name: string }
  | { type: "secrets_store_secret"; name: string; store_id: string; secret_name: string }
  | { type: "service"; name: string; service: string; environment?: string; entrypoint?: string };

const BINDING_KEYS: Record<LiveBinding["type"], { required: readonly string[]; optional: readonly string[] }> = {
  plain_text: { required: ["text"], optional: [] },
  secret_text: { required: [], optional: [] },
  hyperdrive: { required: ["id"], optional: [] },
  queue: { required: ["queue_name"], optional: [] },
  secrets_store_secret: { required: ["store_id", "secret_name"], optional: [] },
  service: { required: ["service"], optional: ["environment", "entrypoint"] },
};

export interface LiveVersion {
  id: string;
  bindings: LiveBinding[];
  compatibilityDate: string;
  compatibilityFlags: string[];
  /** Copied only when the live version has one. */
  limits?: { cpu_ms: number };
}

export interface LiveSettings {
  placementMode: "smart";
  observability?: Record<string, unknown>;
  logpush?: boolean;
  tailConsumers?: { service: string; environment?: string; namespace?: string }[];
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
// eslint-disable-next-line no-control-regex
const CONTROL = /[\u0000-\u001f\u007f]/;

/** GET .../workers/scripts/<name>/deployments result: exactly one deployment entry at the top
 * with exactly one version at 100%. A gradual split is refused (a deploy would end it). */
export function activeVersionId(result: unknown): string {
  const deployments = isObject(result) ? result.deployments : undefined;
  need(Array.isArray(deployments) && deployments.length > 0, "live Worker has no deployment");
  const latest = deployments[0];
  const versions = isObject(latest) ? latest.versions : undefined;
  need(Array.isArray(versions) && versions.length === 1, "live Worker is on a split deployment; refusing to deploy over it");
  const only = versions[0];
  need(isObject(only) && only.percentage === 100, "live Worker's active version is not at 100%");
  need(typeof only.version_id === "string" && UUID.test(only.version_id), "live Worker's active version id is not a version id");
  return only.version_id;
}

/** GET .../workers/scripts/<name>/versions/<id> result. Unknown binding types or fields are refused:
 * the render cannot promise to reproduce what it does not understand. */
export function parseLiveVersion(result: unknown, expectedId: string): LiveVersion {
  need(isObject(result) && result.id === expectedId, "live version read does not match the active version");
  const resources = result.resources;
  need(isObject(resources) && Array.isArray(resources.bindings), "live version has no bindings list");
  const runtime = resources.script_runtime;
  need(isObject(runtime), "live version has no runtime settings");
  const bindings: LiveBinding[] = [];
  const names = new Set<string>();
  for (const b of resources.bindings as unknown[]) {
    need(isObject(b) && typeof b.name === "string" && /^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(b.name), "live binding has an invalid name");
    need(!names.has(b.name), `live binding ${b.name} appears twice`);
    names.add(b.name);
    const type = b.type as LiveBinding["type"];
    const spec = typeof type === "string" && Object.hasOwn(BINDING_KEYS, type) ? BINDING_KEYS[type] : undefined;
    need(spec, `cannot reproduce live binding ${b.name} (type ${typeof b.type === "string" && /^[a-z0-9_]{1,40}$/.test(b.type) ? b.type : "unknown"})`);
    for (const k of Object.keys(b)) need(k === "name" || k === "type" || spec.required.includes(k) || spec.optional.includes(k), `cannot reproduce live binding ${b.name}: unexpected field ${/^[a-z_]{1,40}$/.test(k) ? k : "unknown"}`);
    for (const k of [...spec.required, ...spec.optional]) {
      if (!spec.required.includes(k) && b[k] === undefined) continue;
      need(typeof b[k] === "string", `live binding ${b.name} field ${k} is not a string`);
      need(!CONTROL.test(b[k] as string), `live binding ${b.name} field ${k} has control characters`);
      if (k !== "text") need((b[k] as string).length > 0, `live binding ${b.name} field ${k} is empty`);
    }
    bindings.push(b as unknown as LiveBinding);
  }
  const date = runtime.compatibility_date;
  const flags = runtime.compatibility_flags ?? [];
  need(typeof date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(date), "live version has no compatibility date");
  need(Array.isArray(flags) && flags.every((f) => typeof f === "string" && /^[a-z0-9_]{1,64}$/.test(f)), "live version compatibility flags are invalid");
  const version: LiveVersion = { id: expectedId, bindings, compatibilityDate: date, compatibilityFlags: [...(flags as string[])] };
  const limits = runtime.limits;
  if (limits !== undefined && limits !== null) {
    need(isObject(limits) && Object.keys(limits).every((k) => k === "cpu_ms"), "live version limits are not understood");
    if (limits.cpu_ms !== undefined) {
      need(Number.isInteger(limits.cpu_ms) && (limits.cpu_ms as number) > 0, "live version cpu_ms limit is invalid");
      version.limits = { cpu_ms: limits.cpu_ms as number };
    }
  }
  return version;
}

/** GET .../workers/scripts/<name>/settings result: the non-versioned settings a wrangler deploy
 * would otherwise reset (wrangler disables observability when the config omits it). */
export function parseLiveSettings(result: unknown): LiveSettings {
  need(isObject(result), "live Worker settings are missing");
  const placement = result.placement;
  const mode = isObject(placement) ? placement.mode : result.placement_mode;
  need(mode === "smart", "live Worker is not on smart placement (stacks/cloudflare/session-ingest.ts requires it)");
  const settings: LiveSettings = { placementMode: "smart" };
  if (result.observability !== undefined && result.observability !== null) {
    need(isObject(result.observability) && typeof result.observability.enabled === "boolean", "live observability settings are not understood");
    need(JSON.stringify(result.observability).length <= 4096, "live observability settings are too large");
    settings.observability = result.observability;
  }
  if (result.logpush !== undefined && result.logpush !== null) {
    need(typeof result.logpush === "boolean", "live logpush setting is not understood");
    settings.logpush = result.logpush;
  }
  if (result.tail_consumers !== undefined && result.tail_consumers !== null) {
    need(Array.isArray(result.tail_consumers), "live tail consumers are not understood");
    settings.tailConsumers = (result.tail_consumers as unknown[]).map((t) => {
      need(isObject(t) && typeof t.service === "string" && Object.keys(t).every((k) => ["service", "environment", "namespace"].includes(k)), "live tail consumer is not understood");
      return t as { service: string };
    });
  }
  return settings;
}

// ---------------------------------------------------------------- precedence

export type Source = "explicit" | "profile" | "live";

export interface KnobDecision {
  knob: Knob;
  source: Source;
  /** The live literal, undefined when the variable is absent. */
  liveRaw: string | undefined;
  /** The literal the render carries, undefined = absent. */
  renderedRaw: string | undefined;
  liveValue: string;
  renderedValue: string;
}

/** A live knob literal's runtime meaning, with the Worker's own parsing; anything it would not accept is refused. */
export function effectiveValue(knob: Knob, raw: string | undefined): string {
  if (raw === undefined) return absentMeans(knob);
  if (knob === "INGEST_ACCEPTANCE_MODE") {
    need(raw === "canonical" || raw === "buffered", `live ${knob} has a value this workflow does not understand`);
    return raw;
  }
  const v = raw.trim().toLowerCase();
  const value = v === "" ? "off" : v;
  need(KNOB_VALUES[knob].includes(value), `live ${knob} has a value this workflow does not understand`);
  return value;
}

/** Render literal for a target value: absent when it is what absence means (as the stack renders it). */
const literalFor = (knob: Knob, value: string): string | undefined => (value === absentMeans(knob) ? undefined : value);

export function resolveKnobs(liveVars: ReadonlyMap<string, string>, profile: CommittedProfile, inputs: DispatchInputs): KnobDecision[] {
  return KNOBS.map((knob): KnobDecision => {
    const liveRaw = liveVars.get(knob);
    const liveValue = effectiveValue(knob, liveRaw);
    const keep = (source: Source): KnobDecision => ({ knob, source, liveRaw, renderedRaw: liveRaw, liveValue, renderedValue: liveValue });
    const to = (source: Source, value: string): KnobDecision =>
      value === liveValue ? keep(source) : { knob, source, liveRaw, renderedRaw: literalFor(knob, value), liveValue, renderedValue: value };

    const input = knob === "INGEST_ACCEPTANCE_MODE" ? "profile" : inputs[knob];
    if (input !== "profile" && input !== "") return to("explicit", input);
    if (knob === "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT") return keep("live");
    const wanted = profile[knob as ProfileKnob];
    // Profile mode may fill a variable live does not set at all; it never overrides a value live sets.
    if (liveRaw === undefined) return to("profile", wanted);
    need(liveValue === wanted, `live ${knob} is ${liveValue} but the production profile says ${wanted}; refusing in profile mode (dispatch the input explicitly, or change the profile in a reviewed commit)`);
    return keep("profile");
  });
}

/** The cross-knob rules of stacks/cloudflare/session-ingest.ts and the Worker's config parser,
 * applied to the rendered variable set. */
export function assertConsistent(rendered: ReadonlyMap<string, string>, bindings: readonly LiveBinding[], decisions: readonly KnobDecision[]): void {
  const value = (k: Knob) => decisions.find((d) => d.knob === k)!.renderedValue;
  const live = (k: Knob) => decisions.find((d) => d.knob === k)!.liveValue;
  const buffered = value("INGEST_ACCEPTANCE_MODE") === "buffered";
  if (buffered) need(/^[a-f0-9]{64}$/.test(rendered.get("INGEST_EXPECT_GENERATION") ?? ""), "buffered acceptance needs INGEST_EXPECT_GENERATION on the live Worker");
  const asyncMode = value("INGEST_ASYNC_ADMISSION");
  if (asyncMode !== "off") {
    need(buffered, "INGEST_ASYNC_ADMISSION on/drain needs buffered acceptance");
    need(bindings.some((b) => b.name === "INGEST_ADMISSION" && b.type === "queue"), "INGEST_ADMISSION on/drain needs the INGEST_ADMISSION queue binding on the live Worker (this workflow never adds bindings)");
  }
  if (asyncMode === "on" && live("INGEST_ASYNC_ADMISSION") === "off")
    throw new IngestDeployError("INGEST_ASYNC_ADMISSION goes to drain before on (session-ingest.ts rollout rule)");
  if (value("INGEST_SYNC_ADMISSION_FROZEN_RECEIPT") === "on") {
    need(buffered, "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT=on needs buffered acceptance");
    need(rendered.get("INGEST_CUSTODY_SOURCE") === "frozen-supabase", "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT=on needs INGEST_CUSTODY_SOURCE=frozen-supabase on the live Worker");
  }
}

// ---------------------------------------------------------------- render

export interface WranglerConfig {
  name: string;
  main: string;
  tsconfig: string;
  compatibility_date: string;
  compatibility_flags: string[];
  workers_dev: false;
  preview_urls: false;
  placement: { mode: "smart" };
  send_metrics: false;
  keep_vars: false;
  vars: Record<string, string>;
  hyperdrive?: { binding: string; id: string }[];
  queues?: { producers: { binding: string; queue: string }[] };
  secrets_store_secrets?: { binding: string; store_id: string; secret_name: string }[];
  services?: { binding: string; service: string; environment?: string; entrypoint?: string }[];
  limits?: { cpu_ms: number };
  observability?: Record<string, unknown>;
  logpush?: boolean;
  tail_consumers?: { service: string; environment?: string; namespace?: string }[];
}

export interface Plan {
  liveVersionId: string;
  decisions: KnobDecision[];
  config: WranglerConfig;
}

export const liveVarMap = (bindings: readonly LiveBinding[]) =>
  new Map(bindings.filter((b): b is Extract<LiveBinding, { type: "plain_text" }> => b.type === "plain_text").map((b) => [b.name, b.text]));

/** paths: main and tsconfig relative to the directory the rendered config is written to. */
export function planDeploy(live: LiveVersion, settings: LiveSettings, profile: CommittedProfile, inputs: DispatchInputs, paths: { main: string; tsconfig: string }): Plan {
  need(live.compatibilityDate === COMPATIBILITY_DATE, "live compatibility date differs from stacks/cloudflare/session-ingest.ts; refusing to ship main's code on it");
  need(JSON.stringify(live.compatibilityFlags) === JSON.stringify(COMPATIBILITY_FLAGS), "live compatibility flags differ from stacks/cloudflare/session-ingest.ts");
  const liveVars = liveVarMap(live.bindings);
  for (const [name, text] of liveVars) need(!CONTROL.test(text), `live variable ${name} has control characters`);
  const decisions = resolveKnobs(liveVars, profile, inputs);

  const vars: Record<string, string> = {};
  for (const [name, text] of liveVars) {
    const d = isKnob(name) ? decisions.find((x) => x.knob === name)! : undefined;
    if (!d) vars[name] = text;
    else if (d.renderedRaw !== undefined) vars[name] = d.renderedRaw;
  }
  for (const d of decisions) if (d.liveRaw === undefined && d.renderedRaw !== undefined) vars[d.knob] = d.renderedRaw;
  assertConsistent(new Map(Object.entries(vars)), live.bindings, decisions);

  const of = <T extends LiveBinding["type"]>(t: T) => live.bindings.filter((b): b is Extract<LiveBinding, { type: T }> => b.type === t);
  const config: WranglerConfig = {
    name: SESSION_INGEST_WORKER,
    main: paths.main,
    tsconfig: paths.tsconfig,
    compatibility_date: live.compatibilityDate,
    compatibility_flags: [...live.compatibilityFlags],
    workers_dev: false,
    preview_urls: false,
    placement: { mode: settings.placementMode },
    send_metrics: false,
    keep_vars: false,
    vars,
  };
  const hyperdrive = of("hyperdrive").map((b) => ({ binding: b.name, id: b.id }));
  const producers = of("queue").map((b) => ({ binding: b.name, queue: b.queue_name }));
  const store = of("secrets_store_secret").map((b) => ({ binding: b.name, store_id: b.store_id, secret_name: b.secret_name }));
  const services = of("service").map((b) => ({ binding: b.name, service: b.service, ...(b.environment !== undefined && { environment: b.environment }), ...(b.entrypoint !== undefined && { entrypoint: b.entrypoint }) }));
  if (hyperdrive.length) config.hyperdrive = hyperdrive;
  if (producers.length) config.queues = { producers };
  if (store.length) config.secrets_store_secrets = store;
  if (services.length) config.services = services;
  if (live.limits) config.limits = { ...live.limits };
  if (settings.observability) config.observability = structuredClone(settings.observability);
  if (settings.logpush !== undefined) config.logpush = settings.logpush;
  if (settings.tailConsumers) config.tail_consumers = settings.tailConsumers.map((t) => ({ ...t }));
  const plan = { liveVersionId: live.id, decisions, config };
  guardRender(live, settings, plan);
  return plan;
}

// ---------------------------------------------------------------- guard

const CONFIG_KEYS = new Set([
  "name", "main", "tsconfig", "compatibility_date", "compatibility_flags", "workers_dev", "preview_urls", "placement", "send_metrics", "keep_vars",
  "vars", "hyperdrive", "queues", "secrets_store_secrets", "services", "limits", "observability", "logpush", "tail_consumers",
]);

/** The bindings a wrangler deploy of this config leaves on the new version: what the config
 * declares plus the live Worker secrets (wrangler always keeps secret_text on deploy). */
export function bindingsAfterDeploy(config: WranglerConfig, liveSecrets: readonly LiveBinding[]): LiveBinding[] {
  const out: LiveBinding[] = [];
  for (const [name, text] of Object.entries(config.vars ?? {})) out.push({ type: "plain_text", name, text });
  for (const h of config.hyperdrive ?? []) out.push({ type: "hyperdrive", name: h.binding, id: h.id });
  for (const q of config.queues?.producers ?? []) out.push({ type: "queue", name: q.binding, queue_name: q.queue });
  for (const s of config.secrets_store_secrets ?? []) out.push({ type: "secrets_store_secret", name: s.binding, store_id: s.store_id, secret_name: s.secret_name });
  for (const s of config.services ?? []) out.push({ type: "service", name: s.binding, service: s.service, ...(s.environment !== undefined && { environment: s.environment }), ...(s.entrypoint !== undefined && { entrypoint: s.entrypoint }) });
  out.push(...liveSecrets.filter((b) => b.type === "secret_text"));
  return out;
}

const canonicalBinding = (b: LiveBinding) => JSON.stringify(Object.fromEntries(Object.entries(b).sort(([a], [z]) => a.localeCompare(z))));

/** Independent of how the config was rendered: refuses anything that drops, adds or changes a live
 * binding or setting other than a knob change the precedence allowed. */
export function guardRender(live: LiveVersion, settings: LiveSettings, plan: Plan): void {
  const { config, decisions } = plan;
  for (const k of Object.keys(config)) need(CONFIG_KEYS.has(k), `rendered config has an unexpected key ${/^[a-z_]{1,40}$/.test(k) ? k : "unknown"}`);
  need(config.name === SESSION_INGEST_WORKER, "rendered config targets another Worker");
  need(config.workers_dev === false && config.preview_urls === false, "rendered config must keep workers.dev and preview URLs off");
  need(config.placement?.mode === "smart" && settings.placementMode === "smart", "rendered config must keep smart placement");
  need(config.keep_vars === false, "rendered config must declare every variable (keep_vars false)");
  need(config.compatibility_date === live.compatibilityDate && JSON.stringify(config.compatibility_flags) === JSON.stringify(live.compatibilityFlags), "rendered compatibility settings differ from live");
  need(JSON.stringify(config.limits) === JSON.stringify(live.limits), "rendered limits differ from live");
  need(JSON.stringify(config.observability) === JSON.stringify(settings.observability), "rendered observability differs from live");
  need(config.logpush === settings.logpush, "rendered logpush differs from live");
  need(JSON.stringify(config.tail_consumers) === JSON.stringify(settings.tailConsumers), "rendered tail consumers differ from live");
  const queues = config.queues as Record<string, unknown> | undefined;
  need(queues === undefined || Object.keys(queues).every((k) => k === "producers"), "rendered config must not declare queue consumers (they are Alchemy resources)");

  const after = bindingsAfterDeploy(config, live.bindings);
  const byName = new Map<string, LiveBinding>();
  for (const b of after) {
    need(!byName.has(b.name), `rendered binding ${b.name} is declared twice or shadows a live secret`);
    byName.set(b.name, b);
  }
  const liveByName = new Map(live.bindings.map((b) => [b.name, b]));
  for (const [name, b] of liveByName) {
    const r = byName.get(name);
    const d = isKnob(name) ? decisions.find((x) => x.knob === name) : undefined;
    if (d && b.type === "plain_text") {
      if (d.renderedRaw === undefined) need(r === undefined, `rendered ${name} should be absent`);
      else need(r?.type === "plain_text" && r.text === d.renderedRaw, `rendered ${name} does not match its decision`);
      if (d.renderedValue !== d.liveValue) need(d.source !== "live", `rendered ${name} changed without an input or the profile`);
      continue;
    }
    need(r !== undefined, `rendered config drops live binding ${name}`);
    need(canonicalBinding(r) === canonicalBinding(b), `rendered config changes live binding ${name} (${b.type})`);
  }
  for (const [name, r] of byName) {
    if (liveByName.has(name)) continue;
    const d = isKnob(name) ? decisions.find((x) => x.knob === name) : undefined;
    need(d && d.liveRaw === undefined && r.type === "plain_text" && r.text === d.renderedRaw, `rendered config adds binding ${name} that live does not have`);
  }
}

// ---------------------------------------------------------------- diff and gate

/** Sanitized: knob names and their on/off/drain/buffered/canonical values only. Non-knob bindings
 * cannot differ (guardRender), so this is the whole diff vs live. */
export function sanitizedDiff(decisions: readonly KnobDecision[]): string[] {
  return decisions.filter((d) => d.renderedValue !== d.liveValue).map((d) => `${d.knob}: ${d.liveValue} -> ${d.renderedValue} (${d.source})`);
}

/** A change the dispatch did not ask for explicitly fails closed (also in dry run). */
export function assertDiffAllowed(decisions: readonly KnobDecision[]): void {
  for (const d of decisions)
    if (d.renderedValue !== d.liveValue)
      need(d.source === "explicit", `the production profile would change ${d.knob} from ${d.liveValue} to ${d.renderedValue}; a profile-mode render must equal live, so dispatch that input explicitly if the change is intended`);
}

/** Values the job log must never show (wrangler prints every binding on deploy). Knob tokens and
 * short values (limits, "0") are left readable; GitHub masks the rest. The version id stays
 * readable on purpose: it is the rollback handle (runbook). */
export function valuesToMask(live: LiveVersion): string[] {
  const out = new Set<string>();
  const add = (v: string | undefined) => {
    if (v && v.length >= 6 && !CONTROL.test(v)) out.add(v);
  };
  for (const b of live.bindings) {
    if (b.type === "plain_text" && !isKnob(b.name)) add(b.text);
    if (b.type === "hyperdrive") add(b.id);
    if (b.type === "secrets_store_secret") add(b.store_id);
  }
  return [...out].sort();
}

export function renderedJson(config: WranglerConfig): string {
  return JSON.stringify(config, null, 2) + "\n";
}
