/**
 * SCRUB_ENGINE=wasm-bindgen: binds `ScrubEngine` to the reviewed relayscrubber
 * wasm-bindgen package (generated ESM glue + `.wasm`), pinned in
 * `packages/pipeline/scrub-bindgen-pin.json`. docs/scrub-engine.md, "wasm-bindgen path".
 *
 * Node / Bun load sequence (`initBindgenScrubEngine`), every step fail-closed with a
 * named `ScrubEngineUnavailable` reason:
 *   1. parse the pin; every placeholder, empty or malformed value is refused;
 *   2. read the wasm, glue and loader bytes and copy each into a private snapshot;
 *   3. SHA-256 every snapshot and compare it to the pin, before anything is evaluated
 *      or instantiated;
 *   4. evaluate the glue FROM THE VERIFIED SNAPSHOT (a `data:` URL of those bytes; the
 *      file is never read again, so there is no check-then-use gap);
 *   5. initialise it with the verified wasm snapshot as `module_or_path` (bytes, never a
 *      path or URL, so the glue never resolves or fetches anything);
 *   6. read `manifestJson()` and compare every field, and the crate-owned profile, to the pin.
 *
 * The loader (`index.js`) bytes are pinned and verified as package identity but never
 * evaluated: it imports `./generated/relayscrubber.js` relative to itself, which from a
 * `data:` URL would either fail or (with a file URL) re-read the glue from disk after
 * hashing. This adapter therefore re-implements the loader's transport/identity checks
 * against the verified glue directly. No secret recognition happens here.
 *
 * There is no fallback: not to the TypeScript engine, not to the raw-ABI engine
 * (wasm-engine.ts), and no passthrough. Every scrub call goes through the engine, even
 * when the output would equal the input. A glue error discards the input.
 *
 * The initialised glue module is held only in this file's private state and the
 * engine's private fields; nothing exports it.
 */
import { ScrubberError, taggedScrubberRefusal } from "./scrubber-error.js";
import { readerMaxRuleset, checkedReaderMaxRuleset } from "./reader-policy.js";
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import { SCRUB_RULESET_VERSION } from "../pipeline/raw-chunks.js";
import { preflightScrubRecords, preflightScrubText, refuseMalformedScrubInput } from "./bindgen-admission.js";
import {
  ScrubEngineUnavailable,
  type BindgenArtifactName,
  type BindgenManifestField,
  type ScrubEngine,
  type ScrubEngineFailure,
  type ScrubMarker,
  type ScrubRecordsResult,
} from "./engine.js";

// ---------------------------------------------------------------- pin

/** Candidate manifest schema and ABI accepted by this adapter; every identity field is also pinned. */
export const BINDGEN_MANIFEST_SCHEMA = 3;
export const BINDGEN_ABI_VERSION = 2;
export const BINDGEN_PACKAGE = "@agentworkforce/relayscrubber";
const PIN_SCHEMA = 1;

export const BINDGEN_MANIFEST_FIELDS: readonly BindgenManifestField[] = [
  "schema",
  "ruleset",
  "rulesetVersion",
  "rulesetSourceSha256",
  "crateVersion",
  "abiVersion",
  "policy",
  "redactionToken",
  "scrubProfile",
];
const ARTIFACTS: readonly BindgenArtifactName[] = ["wasm", "glue", "loader"];
const POLICIES = new Set(["secrets", "pii"]);

/** The relayscrubber schema-3 manifest. A pin may list further primitive fields, which must then match exactly too. */
export interface BindgenManifest {
  schema: number;
  ruleset: string;
  rulesetVersion: number;
  rulesetSourceSha256: string;
  crateVersion: string;
  abiVersion: number;
  policy: string;
  redactionToken: string;
  scrubProfile: string;
  [extra: string]: string | number | boolean | null;
}

export type BindgenArtifactPaths = Readonly<Record<BindgenArtifactName, string>>;

/** `packages/pipeline/scrub-bindgen-pin.json`, validated. */
export interface ScrubBindgenPin {
  package_version: string;
  wasm_sha256: string;
  glue_sha256: string;
  loader_sha256: string;
  paths: BindgenArtifactPaths;
  scrub_profile: string;
  manifest: Readonly<BindgenManifest>;
}

const HEX64 = /^[0-9a-f]{64}$/;
const ZERO64 = /^0{64}$/;
const PLACEHOLDER = /UNPINNED|PLACEHOLDER/i;

function refuse(reason: ScrubEngineFailure): never {
  throw new ScrubEngineUnavailable(reason);
}
const isRealString = (v: unknown): v is string => typeof v === "string" && v.trim() !== "" && !PLACEHOLDER.test(v);
const isDigest = (v: unknown): v is string => typeof v === "string" && HEX64.test(v) && !ZERO64.test(v);
const isPositiveInt = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) > 0;
const isPrimitive = (v: unknown) => v === null || ["string", "number", "boolean"].includes(typeof v);
/** Relative, inside the package directory, no drive/scheme, no backslashes. */
const isPackagePath = (v: unknown): v is string =>
  typeof v === "string" &&
  v !== "" &&
  !v.startsWith("/") &&
  !/[\\:\0]/.test(v) &&
  v.split("/").every((seg) => seg !== "" && seg !== "." && seg !== "..");

/** The crate owns the profile identifier; the instance and pin must agree verbatim. */
export function bindgenProfile(m: Pick<BindgenManifest, "scrubProfile">): string {
  return m.scrubProfile;
}

/** Validate the pin. Every placeholder or malformed value has its own named reason. */
export function parseScrubBindgenPin(raw: unknown, maximum = readerMaxRuleset()): ScrubBindgenPin {
  const p = raw as Record<string, unknown> | null;
  if (!p || typeof p !== "object" || Array.isArray(p) || p.schema !== PIN_SCHEMA || p.package !== BINDGEN_PACKAGE)
    refuse("bindgen_pin_invalid");
  const pin = p as Record<string, unknown>;
  if (!isRealString(pin.package_version)) refuse("bindgen_unpinned_package_version");
  for (const name of ARTIFACTS) if (!isDigest(pin[`${name}_sha256`])) refuse(`bindgen_unpinned_${name}_sha256`);
  const paths = pin.paths as Record<string, unknown> | null;
  if (!paths || typeof paths !== "object" || ARTIFACTS.some((name) => !isPackagePath(paths[name])))
    refuse("bindgen_pin_paths_invalid");
  if (!isRealString(pin.scrub_profile)) refuse("bindgen_unpinned_scrub_profile");
  const m = pin.manifest as Record<string, unknown> | null;
  if (!m || typeof m !== "object" || Array.isArray(m)) return refuse("bindgen_pin_invalid");
  if (m.schema !== BINDGEN_MANIFEST_SCHEMA) refuse("bindgen_unpinned_manifest_schema");
  if (m.ruleset !== "relayhistory-scrub") refuse("bindgen_unpinned_manifest_ruleset");
  if (!isPositiveInt(m.rulesetVersion)) refuse("bindgen_unpinned_manifest_rulesetVersion");
  if (!isDigest(m.rulesetSourceSha256)) refuse("bindgen_unpinned_manifest_rulesetSourceSha256");
  if (!isRealString(m.crateVersion)) refuse("bindgen_unpinned_manifest_crateVersion");
  if (m.abiVersion !== BINDGEN_ABI_VERSION) refuse("bindgen_unpinned_manifest_abiVersion");
  if (!isRealString(m.policy) || !POLICIES.has(m.policy)) refuse("bindgen_unpinned_manifest_policy");
  if (!isRealString(m.redactionToken)) refuse("bindgen_unpinned_manifest_redactionToken");
  if (!isRealString(m.scrubProfile)) refuse("bindgen_unpinned_manifest_scrubProfile");
  if (Object.values(m).some((v) => !isPrimitive(v))) refuse("bindgen_pin_invalid");
  // Writer floor and reader capability are separate: increasing the latter never changes TS stamps.
  if ((m.rulesetVersion as number) < SCRUB_RULESET_VERSION) refuse("bindgen_ruleset_below_floor");
  if ((m.rulesetVersion as number) > checkedReaderMaxRuleset(maximum)) refuse("bindgen_ruleset_above_reader_ceiling");
  const manifest = Object.freeze({ ...m }) as Readonly<BindgenManifest>;
  if (pin.scrub_profile !== bindgenProfile(manifest)) refuse("bindgen_pin_profile_inconsistent");
  return Object.freeze({
    package_version: pin.package_version as string,
    wasm_sha256: pin.wasm_sha256 as string,
    glue_sha256: pin.glue_sha256 as string,
    loader_sha256: pin.loader_sha256 as string,
    paths: Object.freeze({ wasm: paths!.wasm as string, glue: paths!.glue as string, loader: paths!.loader as string }),
    scrub_profile: pin.scrub_profile as string,
    manifest,
  });
}

/** Two pins are the same identity iff this string is equal (manifest keys sorted). */
function pinKey(pin: ScrubBindgenPin): string {
  const manifest = Object.keys(pin.manifest)
    .sort()
    .map((k) => [k, pin.manifest[k]]);
  return JSON.stringify([
    pin.package_version,
    pin.wasm_sha256,
    pin.glue_sha256,
    pin.loader_sha256,
    pin.paths.wasm,
    pin.paths.glue,
    pin.paths.loader,
    pin.scrub_profile,
    manifest,
  ]);
}

// ---------------------------------------------------------------- glue surface

/** The generated glue's exports this adapter uses (wasm-bindgen `--target web`). */
interface BindgenGlue {
  default(input: { module_or_path: unknown }): Promise<unknown>;
  initSync(input: { module: unknown }): unknown;
  manifestJson(): string;
  scrubText(text: string): string;
  scrubLong(text: string): string;
  /** Returns JSON text `{value, jsonText, redactionCount}`; `jsonText` is the Rust-serialised result. */
  scrubJson(jsonText: string): string;
  /** Returns JSON text `{text, recordCount, redactionCount}`; throws on invalid input with no partial output. */
  scrubRecords(bytes: Uint8Array): string;
}
const SCRUB_FUNCTIONS = ["manifestJson", "scrubText", "scrubLong", "scrubJson", "scrubRecords"] as const;

function bindGlue(raw: unknown, entry: "default" | "initSync"): BindgenGlue {
  const g = raw as Record<string, unknown> | null;
  if (!g || (typeof g !== "object" && typeof g !== "function")) return refuse("bindgen_export_missing");
  for (const name of [entry, ...SCRUB_FUNCTIONS]) if (typeof g[name] !== "function") refuse("bindgen_export_missing");
  return g as unknown as BindgenGlue;
}

/** Read and compare the manifest the initialised instance reports. Returns a frozen copy. */
function verifyManifest(glue: BindgenGlue, pin: ScrubBindgenPin): Readonly<BindgenManifest> {
  let m: Record<string, unknown>;
  try {
    const text = glue.manifestJson();
    if (typeof text !== "string") throw new Error("manifest");
    m = JSON.parse(text) as Record<string, unknown>;
  } catch {
    return refuse("bindgen_manifest_invalid");
  }
  if (!m || typeof m !== "object" || Array.isArray(m)) refuse("bindgen_manifest_invalid");
  for (const field of BINDGEN_MANIFEST_FIELDS)
    if (!Object.hasOwn(m, field) || m[field] !== pin.manifest[field]) refuse(`bindgen_manifest_mismatch_${field}`);
  if (m.schema !== BINDGEN_MANIFEST_SCHEMA) refuse("bindgen_manifest_mismatch_schema");
  const keys = new Set([...Object.keys(m), ...Object.keys(pin.manifest)]);
  for (const k of keys)
    if (!Object.hasOwn(m, k) || !Object.hasOwn(pin.manifest, k) || m[k] !== pin.manifest[k]) refuse("bindgen_manifest_mismatch_extra_field");
  const manifest = Object.freeze({ ...m }) as Readonly<BindgenManifest>;
  if (bindgenProfile(manifest) !== pin.scrub_profile) refuse("bindgen_profile_mismatch");
  return manifest;
}

// ---------------------------------------------------------------- engine

export type BindgenScrubEngine = ScrubEngine & {
  readonly kind: "wasm-bindgen";
  /** True after an unrecognized failure; the instance is never called again. */
  readonly failed: boolean;
  /** The manifest the instance reported (equal to the pin). */
  readonly manifest: Readonly<BindgenManifest>;
};

const encoder = new TextEncoder();

/** Build the engine around a verified, initialised glue module. Not exported: only the two init paths call it. */
function makeEngine(glue: BindgenGlue, manifest: Readonly<BindgenManifest>): BindgenScrubEngine {
  // The marker is what the instance itself reported (checked equal to the pin), never the pin alone.
  const profile = bindgenProfile(manifest);
  const marker: ScrubMarker = Object.freeze({ ruleset: manifest.ruleset, version: manifest.rulesetVersion, profile });
  let failed = false;

  const ensureUsable = () => {
    if (failed) throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
  };
  const poison = (): never => {
    failed = true;
    throw new ScrubberError("scrubber_engine_failure", "instance_poison");
  };
  /** Unknown exceptions are never inferred to be deliberate from their JS class. */
  const invoke = <R>(call: () => R): R => {
    ensureUsable();
    try { return call(); }
    catch (error) {
      const refusal = taggedScrubberRefusal(error);
      if (refusal) throw refusal;
      return poison();
    }
  };
  const text = (fn: (s: string) => string, value: string): string => {
    ensureUsable();
    preflightScrubText(value);
    const out = invoke(() => fn(value));
    if (typeof out !== "string") return poison();
    return out;
  };
  const parseEnvelope = (out: unknown): Record<string, unknown> => {
    if (typeof out !== "string") return poison();
    try {
      const v = JSON.parse(out) as unknown;
      if (v && typeof v === "object" && !Array.isArray(v)) return v as Record<string, unknown>;
    } catch { /* No returned or thrown artifact text reaches diagnostics. */ }
    return poison();
  };

  const engine: BindgenScrubEngine = {
    kind: "wasm-bindgen",
    rulesetVersion: manifest.rulesetVersion,
    scrubProfile: profile,
    marker,
    manifest,
    get failed() {
      return failed;
    },
    scrubText: (value: string) => text((s) => glue.scrubText(s), value),
    scrubLong: (value: string) => text((s) => glue.scrubLong(s), value),
    scrubJson: <T>(value: T): T => {
      ensureUsable();
      // `undefined` has no JSON text, so there is nothing the engine could scrub or the caller
      // could persist; it is the one value returned without an engine call (TS returns it too).
      // null, booleans and numbers DO go through the engine (no zero-change bypass).
      if (value === undefined) return value;
      let input: unknown;
      try {
        input = JSON.stringify(value);
      } catch {
        return refuseMalformedScrubInput();
      }
      preflightScrubText(input);
      const envelope = parseEnvelope(invoke(() => glue.scrubJson(input)));
      // Use the Rust-serialised jsonText, never `.value` (a JS view that may round large integers).
      if (typeof envelope.jsonText !== "string") return poison();
      try {
        return JSON.parse(envelope.jsonText) as T;
      } catch {
        return poison();
      }
    },
    scrubRecords: (source: Uint8Array): ScrubRecordsResult => {
      ensureUsable();
      const bytes = preflightScrubRecords(source);
      const out = invoke(() => glue.scrubRecords(bytes));
      const r = parseEnvelope(out);
      if (typeof r.text !== "string" || !Number.isSafeInteger(r.recordCount) || (r.recordCount as number) < 0)
        return poison();
      try { return { scrubbed: encoder.encode(r.text), records: r.recordCount as number, scrub: marker }; }
      catch { return poison(); }
    },
  };
  return Object.freeze(engine);
}

// ---------------------------------------------------------------- Node / Bun loader path

/** Read one artifact (`relativePath` is the pin's path inside the package directory). Injectable for tests. */
export type ReadBindgenArtifact = (name: BindgenArtifactName, relativePath: string) => Promise<Uint8Array>;
/** Evaluate the VERIFIED glue bytes and return its module namespace. Injectable for tests. */
export type ImportBindgenGlue = (verifiedGlue: Uint8Array) => Promise<unknown>;

/** Default package directory (self-host image), relative to this file; `SCRUB_BINDGEN_DIR` (absolute) overrides it. */
export const SCRUB_BINDGEN_DEFAULT_DIR = "../../../scrub-bindgen/";

/** Node / Bun reader: `<dir>/<pin path>`. The location is not trusted: every byte is hash-checked against the pin. */
export function bindgenFileReader(dir: string | undefined): ReadBindgenArtifact {
  if (dir !== undefined && dir !== "" && !dir.startsWith("/")) refuse("bindgen_dir_invalid");
  return async (_name, relativePath) => {
    const { readFile } = await import("node:fs/promises");
    const { resolve } = await import("node:path");
    const { fileURLToPath } = await import("node:url");
    // Resolved only here: a Worker bundle may have no usable import.meta.url.
    const base = dir ? dir : fileURLToPath(new URL(SCRUB_BINDGEN_DEFAULT_DIR, import.meta.url).href);
    const bytes = await readFile(resolve(base, relativePath));
    return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  };
}

function toBase64(bytes: Uint8Array): string {
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(bin);
}

/**
 * Evaluate the glue from the verified snapshot itself: the `data:` URL is built from
 * those exact bytes, so what runs is what was hashed (no re-read, no TOCTOU). The
 * glue's own `import.meta.url` is meaningless here, which is why init always gets the
 * wasm as bytes (`module_or_path`) and never resolves `relayscrubber_bg.wasm` or fetches.
 * The generated glue has no static imports; if a build ever adds one, this import fails
 * (bindgen_glue_import_failed) rather than resolving it from disk.
 */
export const importGlueFromVerifiedBytes: ImportBindgenGlue = (verifiedGlue) =>
  import(/* @vite-ignore */ `data:text/javascript;base64,${toBase64(verifiedGlue)}`);

/** A private copy: later writes to the caller's buffer cannot reach what is hashed and used. */
function snapshot(bytes: unknown, name: BindgenArtifactName): Uint8Array {
  if (!(bytes instanceof Uint8Array) || bytes.byteLength === 0) return refuse(`bindgen_artifact_missing_${name}`);
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

/** Read, snapshot and hash all three artifacts; refuse unless every digest equals the pin. */
async function readVerified(pin: ScrubBindgenPin, read: ReadBindgenArtifact): Promise<Record<BindgenArtifactName, Uint8Array>> {
  const out = {} as Record<BindgenArtifactName, Uint8Array>;
  for (const name of ARTIFACTS) {
    let raw: unknown;
    try {
      raw = await read(name, pin.paths[name]);
    } catch {
      refuse(`bindgen_artifact_missing_${name}`);
    }
    out[name] = snapshot(raw, name);
  }
  for (const name of ARTIFACTS)
    if ((await sha256Hex(out[name])) !== pin[`${name}_sha256`]) refuse(`bindgen_sha256_mismatch_${name}`);
  return out;
}

export interface BindgenInitOptions {
  readerMaxRuleset?: number;
  pin: unknown;
  readArtifact: ReadBindgenArtifact;
  importGlue?: ImportBindgenGlue;
}

/** The one Node-path initialisation of this process (sticky: a failure stays a failure). */
let nodeInit: { key: string; promise: Promise<BindgenScrubEngine> } | undefined;
/** Set once the Workers compiled path has started initialising. */
let compiledInit: { key: string; engine?: BindgenScrubEngine } | undefined;

async function initialise(pin: ScrubBindgenPin, options: BindgenInitOptions): Promise<BindgenScrubEngine> {
  const verified = await readVerified(pin, options.readArtifact);
  let rawGlue: unknown;
  try {
    rawGlue = await (options.importGlue ?? importGlueFromVerifiedBytes)(verified.glue);
  } catch {
    refuse("bindgen_glue_import_failed");
  }
  const glue = bindGlue(rawGlue, "default");
  try {
    await glue.default({ module_or_path: verified.wasm });
  } catch {
    refuse("bindgen_init_failed");
  }
  return makeEngine(glue, verifyManifest(glue, pin));
}

/**
 * Verify and initialise the pinned bindgen package (Node / Bun). Concurrent and repeated
 * calls with the same pin share ONE initialisation and see the same engine or the same
 * failure; a later caller's own bytes are still hashed against the pin. A call with a
 * different pin, or whose bytes do not verify, throws `bindgen_init_conflict`.
 */
export async function initBindgenScrubEngine(options: BindgenInitOptions): Promise<BindgenScrubEngine> {
  if (compiledInit) refuse("bindgen_path_conflict");
  const pin = parseScrubBindgenPin(options.pin, options.readerMaxRuleset);
  const key = pinKey(pin);
  if (!nodeInit) {
    // Assigned synchronously, before any await, so a concurrent caller always sees it.
    nodeInit = {
      key,
      promise: initialise(pin, options).catch((error: unknown) => {
        throw error instanceof ScrubEngineUnavailable ? error : new ScrubEngineUnavailable("bindgen_init_failed");
      }),
    };
    return nodeInit.promise;
  }
  if (nodeInit.key !== key) refuse("bindgen_init_conflict");
  const engine = await nodeInit.promise;
  if (engine.failed) throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
  try {
    await readVerified(pin, options.readArtifact);
  } catch {
    refuse("bindgen_init_conflict");
  }
  return engine;
}

// ---------------------------------------------------------------- Workers compiled-module path

export interface CompiledBindgenOptions {
  readerMaxRuleset?: number;
  /** The statically imported glue namespace (its bytes are pinned and verified at build time). */
  glue: unknown;
  /** A statically imported `WebAssembly.Module` of the pinned `.wasm` (byte pin verified at build time). */
  module: unknown;
  pin: unknown;
}

/**
 * Workers: runtime compilation from bytes is unavailable and a compiled Module is opaque
 * (its bytes cannot be hashed here). The build must verify the wasm and glue byte pins
 * before bundling; this verifies the full source identity (manifest + profile) at startup.
 * Mirrors `loadCompiledScrubber`: synchronous, refused once the Node path has started,
 * and only one compiled initialisation per isolate (a repeat with the same pin returns it).
 * NOT wired to any Worker entrypoint yet.
 */
export function createCompiledBindgenEngine(options: CompiledBindgenOptions): BindgenScrubEngine {
  if (nodeInit) refuse("bindgen_path_conflict");
  const pin = parseScrubBindgenPin(options.pin, options.readerMaxRuleset);
  const key = pinKey(pin);
  if (compiledInit) {
    if (compiledInit.key !== key || !compiledInit.engine) refuse("bindgen_init_conflict");
    if (compiledInit.engine.failed) throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
    return compiledInit.engine;
  }
  const Module = (globalThis as unknown as { WebAssembly?: { Module?: abstract new (...args: never[]) => unknown } }).WebAssembly?.Module;
  if (!Module || !(options.module instanceof Module)) refuse("bindgen_module_invalid");
  const glue = bindGlue(options.glue, "initSync");
  // Sticky from here: a failed compiled init is never retried in this isolate.
  compiledInit = { key };
  try {
    glue.initSync({ module: options.module });
  } catch {
    refuse("bindgen_init_failed");
  }
  const engine = makeEngine(glue, verifyManifest(glue, pin));
  compiledInit.engine = engine;
  return engine;
}

/** Tests only. */
export function resetBindgenScrubEngineForTests(): void {
  nodeInit = undefined;
  compiledInit = undefined;
}
