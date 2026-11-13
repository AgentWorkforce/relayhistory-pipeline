/**
 * The scrub engine seam. Every pipeline scrub call site goes through the
 * functions here; docs/scrub-engine.md has the design, the ABI contract and the
 * reachability matrix.
 *
 * - `SCRUB_ENGINE` unset, "" or "ts": the TypeScript engine, byte-identical to
 *   calling scrub.ts / raw-chunks.ts directly (this is today's behaviour).
 * - `SCRUB_ENGINE=wasm-bindgen`: the pinned relayscrubber wasm-bindgen package
 *   (bindgen-engine.ts, scrub-bindgen-pin.json), with the same fail-closed rules.
 *   This is the first-cutover path; the raw ABI v1 `wasm` path stays in place.
 * - `SCRUB_ENGINE=wasm`: the pinned relayscrubber artifact only. Until
 *   `initScrubEngine` has succeeded, and for good after the instance traps,
 *   every scrub call throws a typed, content-free error. There is no fallback to
 *   the TypeScript engine and no raw passthrough.
 * - Anything else: `ScrubEngineConfigError` at selection (startup fails).
 *
 * `SCRUB_ENGINE_REQUIRED` (default off): unset or "" imposes nothing (today's
 * behaviour). Set to exactly "ts", "wasm" or "wasm-bindgen", it refuses any
 * selection whose SCRUB_ENGINE kind differs (unset SCRUB_ENGINE is "ts") with
 * `ScrubEngineConfigError` reason `engine_selection_required`, and that refusal
 * is sticky for the process: every later selection and scrub call refuses too,
 * so a deployment that requires the wasm path never silently runs TypeScript.
 * It only refuses; it never selects or initialises anything.
 *
 * The selection is read once per process (isolate); a later, different
 * selection throws rather than switching engines.
 */
import { ScrubberError } from "./scrubber-error.js";
export { ScrubberError, SCRUBBER_CALL_REFUSAL_CODES, type ScrubberDisposition, type ScrubberErrorCode } from "./scrubber-error.js";
import { readerMaxRuleset } from "./reader-policy.js";
import { sealRawChunk as sealRawChunkTs, type SealRawChunk, type SealedChunk } from "../pipeline/raw-chunks.js";
import type { ObjectStore } from "../pipeline/object-layout.js";
import { bindEngineRawSealer, type BoundRawChunkSealer } from "../pipeline/engine-raw-chunks.js";
import pinJson from "../../../scrub-wasm-pin.json";
import {
  bindgenFileReader,
  initBindgenScrubEngine,
  resetBindgenScrubEngineForTests,
  type ImportBindgenGlue,
  type ReadBindgenArtifact,
} from "./bindgen-engine.js";
import {
  ScrubEngineConfigError,
  ScrubEngineUnavailable,
  tsScrubEngine,
  type ScrubEngine,
  type ScrubEngineFailure,
  type ScrubMarker,
  type ScrubRecordsResult,
} from "./engine.js";
import { installPredicateScrub } from "./predicate-hook.js";
import {
  createWasmScrubEngine,
  type InstantiateScrubWasm,
  type LoadScrubWasmArtifact,
} from "./wasm-engine.js";

export {
  ScrubEngineConfigError,
  ScrubEngineUnavailable,
  TS_SCRUB_PROFILE,
  tsScrubEngine,
  type ScrubEngine,
  type ScrubEngineFailure,
  type ScrubMarker,
  type ScrubRecordsResult,
} from "./engine.js";
export {
  BINDGEN_MANIFEST_FIELDS,
  BINDGEN_MANIFEST_SCHEMA,
  bindgenProfile,
  createCompiledBindgenEngine,
  parseScrubBindgenPin,
  type BindgenManifest,
  type BindgenScrubEngine,
  type CompiledBindgenOptions,
  type ImportBindgenGlue,
  type ReadBindgenArtifact,
  type ScrubBindgenPin,
} from "./bindgen-engine.js";
export {
  WasmScrubEngine,
  createWasmScrubEngine,
  parseScrubWasmPin,
  type InstantiateScrubWasm,
  type LoadScrubWasmArtifact,
  type ScrubWasmArtifact,
  type ScrubWasmPin,
} from "./wasm-engine.js";

export type ScrubEngineKind = "ts" | "wasm" | "wasm-bindgen";

/** The pin checked in at packages/pipeline/scrub-wasm-pin.json. */
export const SCRUB_WASM_PIN: unknown = pinJson;
/**
 * Where the checked-in wasm-bindgen pin (packages/pipeline/scrub-bindgen-pin.json) lives,
 * relative to this file. It is deliberately NOT a static import: the hosted ingest Worker
 * bundles this module, and Workers are excluded from the cutover, so the Worker bundle must
 * not carry the pin's ruleset-10 identity. Only the Node / Bun wasm-bindgen initialisation
 * reads it (`loadScrubBindgenPin`).
 */
export const SCRUB_BINDGEN_PIN_PATH = "../../../scrub-bindgen-pin.json";

let checkedInBindgenPin: Promise<unknown> | undefined;

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object") {
    for (const v of Object.values(value)) deepFreeze(v);
    Object.freeze(value);
  }
  return value;
}

/**
 * The checked-in wasm-bindgen pin, read from disk on first use (Node / Bun only) and
 * cached as one frozen object. A read or parse failure refuses with
 * `bindgen_pin_unreadable` and is not cached; the pin is validated by bindgen-engine.ts.
 */
export function loadScrubBindgenPin(): Promise<unknown> {
  checkedInBindgenPin ??= (async () => {
    try {
      const { readFile } = await import("node:fs/promises");
      const { fileURLToPath } = await import("node:url");
      // Resolved only here: a Worker bundle may have no usable import.meta.url.
      const text = await readFile(fileURLToPath(new URL(SCRUB_BINDGEN_PIN_PATH, import.meta.url).href), "utf8");
      return deepFreeze(JSON.parse(text) as unknown);
    } catch {
      checkedInBindgenPin = undefined;
      throw new ScrubEngineUnavailable("bindgen_pin_unreadable");
    }
  })();
  return checkedInBindgenPin;
}

/** Where the self-host image / Node runtime expects the pinned artifact. */
export const SCRUB_WASM_PATH = "../../../scrub-wasm/relayscrubber.wasm";
// Resolved only inside the Node/Bun loader: a Worker bundle may have no usable
// import.meta.url, and module evaluation must not depend on it (hosted Worker unchanged).

interface ScrubEngineEnv {
  SCRUB_ENGINE?: string | undefined;
  /**
   * Default off. Unset or "": no requirement. "ts" | "wasm" | "wasm-bindgen": selection
   * refuses (ScrubEngineConfigError, reason "engine_selection_required") unless the
   * SCRUB_ENGINE kind (unset = "ts") is exactly this one. Anything else is a config error.
   */
  SCRUB_ENGINE_REQUIRED?: string | undefined;
  READER_MAX_RULESET?: string | undefined;
  /** wasm-bindgen only: absolute directory of the installed package (default: next to the pipeline package). */
  SCRUB_BINDGEN_DIR?: string | undefined;
}

let selected: ScrubEngineKind | undefined;
let active: ScrubEngine | undefined;
let lastFailure: ScrubEngineFailure | undefined;
let pending: Promise<ScrubEngine> | undefined;
/** Sticky for the process: a refused SCRUB_ENGINE_REQUIRED selection is never followed by a TS fallback. */
let requiredRefused = false;
/** The SCRUB_ENGINE_REQUIRED kind a selection was made under (sticky once set). */
let requiredKind: ScrubEngineKind | undefined;
/** The active wasm-bindgen instance was verified against the checked-in pin (not a test pin). */
let activePinExact = false;

/** Parse SCRUB_ENGINE. Exact and case-sensitive; "" counts as unset. */
export function parseScrubEngineKind(value: string | undefined): ScrubEngineKind {
  if (value === undefined || value === "" || value === "ts") return "ts";
  if (value === "wasm") return "wasm";
  if (value === "wasm-bindgen") return "wasm-bindgen";
  throw new ScrubEngineConfigError(`SCRUB_ENGINE must be unset, "ts", "wasm" or "wasm-bindgen"`);
}

/** Parse SCRUB_ENGINE_REQUIRED. Exact and case-sensitive; unset or "" = no requirement. */
export function parseScrubEngineRequired(value: string | undefined): ScrubEngineKind | undefined {
  if (value === undefined || value === "") return undefined;
  if (value === "ts" || value === "wasm" || value === "wasm-bindgen") return value;
  throw new ScrubEngineConfigError(`SCRUB_ENGINE_REQUIRED must be unset, "", "ts", "wasm" or "wasm-bindgen"`);
}

function refuseRequiredSelection(): never {
  requiredRefused = true;
  lastFailure = "engine_selection_required";
  throw new ScrubEngineConfigError(
    "SCRUB_ENGINE does not select the engine SCRUB_ENGINE_REQUIRED requires (engine_selection_required)",
    "engine_selection_required",
  );
}

/**
 * Select the engine for this process from `env.SCRUB_ENGINE`. Idempotent for the
 * same value; a different value after the first selection throws. With
 * `env.SCRUB_ENGINE_REQUIRED` set, a selection (new or already made) of another
 * kind refuses, permanently for this process.
 */
export function selectScrubEngine(env: ScrubEngineEnv | undefined): ScrubEngineKind {
  if (requiredRefused) refuseRequiredSelection();
  const required = parseScrubEngineRequired(env?.SCRUB_ENGINE_REQUIRED);
  const kind = parseScrubEngineKind(env?.SCRUB_ENGINE);
  if (required !== undefined && (kind !== required || (selected !== undefined && selected !== required)))
    refuseRequiredSelection();
  if (required !== undefined) requiredKind = required;
  if (selected !== undefined && selected !== kind)
    throw new ScrubEngineConfigError(`SCRUB_ENGINE was already selected as "${selected}"`);
  if (selected === undefined) {
    selected = kind;
    if (kind === "ts") active = tsScrubEngine;
  }
  return kind;
}

/** Node / Bun default loader: the fixed file next to the package. */
export const loadScrubWasmFromFile: LoadScrubWasmArtifact = async () => {
  const { readFile } = await import("node:fs/promises");
  const { fileURLToPath } = await import("node:url");
  const bytes = await readFile(fileURLToPath(new URL(SCRUB_WASM_PATH, import.meta.url).href));
  return { bytes: new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength) };
};

export interface InitScrubEngineOptions {
  /** Defaults to `process.env` where it exists. */
  env?: ScrubEngineEnv;
  loadArtifact?: LoadScrubWasmArtifact;
  /** Tests only: a pin other than the checked-in one. */
  pin?: unknown;
  instantiate?: InstantiateScrubWasm;
  /** SCRUB_ENGINE=wasm-bindgen only. The raw-ABI options above are never consulted for it. */
  bindgen?: {
    /** Tests only: a pin other than the checked-in one. */
    pin?: unknown;
    readArtifact?: ReadBindgenArtifact;
    importGlue?: ImportBindgenGlue;
  };
}

function processEnv(): ScrubEngineEnv | undefined {
  return (globalThis as { process?: { env?: ScrubEngineEnv } }).process?.env;
}

/**
 * Select and, for wasm, verify and instantiate the pinned engine. Call before
 * serving. Throws ScrubEngineConfigError (bad SCRUB_ENGINE / SCRUB_ENGINE_REQUIRED,
 * or a selection SCRUB_ENGINE_REQUIRED refuses) or
 * ScrubEngineUnavailable (wasm init failed); after a failure every scrub call
 * throws until a later init succeeds. With "ts" this does no I/O.
 */
export async function initScrubEngine(options: InitScrubEngineOptions = {}): Promise<ScrubEngine> {
  const env = options.env ?? processEnv();
  readerMaxRuleset(env); // Validate before selecting even the unchanged TS engine.
  const kind = selectScrubEngine(env);
  if (kind === "ts") return tsScrubEngine;
  if (kind === "wasm-bindgen") return initBindgen(env, options.bindgen ?? {});
  if (active) return active;
  pending ??= (async () => {
    try {
      const engine = await createWasmScrubEngine({
        pin: "pin" in options ? options.pin : SCRUB_WASM_PIN,
        loadArtifact: options.loadArtifact ?? loadScrubWasmFromFile,
        instantiate: options.instantiate,
      });
      active = engine;
      lastFailure = undefined;
      return engine;
    } catch (error) {
      lastFailure = error instanceof ScrubEngineUnavailable ? error.reason : "instantiate_failed";
      throw error instanceof ScrubEngineUnavailable ? error : new ScrubEngineUnavailable("instantiate_failed");
    } finally {
      pending = undefined;
    }
  })();
  return pending;
}

/**
 * wasm-bindgen: bindgen-engine.ts owns coalescing and double-init refusal (same pin:
 * one shared initialisation; other pin or other bytes: bindgen_init_conflict), so
 * every call goes through it. A conflicting later call does not disturb a verified engine.
 */
async function initBindgen(env: ScrubEngineEnv | undefined, o: NonNullable<InitScrubEngineOptions["bindgen"]>): Promise<ScrubEngine> {
  try {
    const checkedIn = await loadScrubBindgenPin();
    const pin = "pin" in o ? o.pin : checkedIn;
    const engine = await initBindgenScrubEngine({
      readerMaxRuleset: readerMaxRuleset(env),
      pin,
      readArtifact: o.readArtifact ?? bindgenFileReader(env?.SCRUB_BINDGEN_DIR),
      importGlue: o.importGlue,
    });
    active = engine;
    // The bytes were hashed against `pin`; equal to the checked-in pin means the exact pinned package.
    activePinExact = pin === checkedIn || JSON.stringify(pin) === JSON.stringify(checkedIn);
    // A repeat init after an unknown failure refuses instead of resolving.
    if (engine.failed) throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
    lastFailure = undefined;
    return engine;
  } catch (error) {
    const unavailable = error instanceof ScrubEngineUnavailable || error instanceof ScrubberError ? error : new ScrubEngineUnavailable("bindgen_init_failed");
    if (!active) lastFailure = unavailable.reason;
    throw unavailable;
  }
}

/**
 * The engine scrub calls use. Synchronous: with nothing selected yet it selects
 * from `process.env` (unset there means TypeScript, today's behaviour); with
 * wasm selected and not initialised (or failed) it throws ScrubEngineUnavailable.
 */
export function activeScrubEngine(): ScrubEngine {
  if (requiredRefused) refuseRequiredSelection();
  if (selected === undefined) selectScrubEngine(processEnv());
  if (selected === "ts") return tsScrubEngine;
  const engine = active as (ScrubEngine & { failed?: boolean }) | undefined;
  if (!engine) throw new ScrubEngineUnavailable(lastFailure ?? "not_initialised");
  if (engine.failed) {
    if (engine.kind === "wasm-bindgen") throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
    throw new ScrubEngineUnavailable("engine_failed");
  }
  // A deployment that requires wasm-bindgen runs only the exact checked-in pin.
  if (engine.kind === "wasm-bindgen" && requiredKind === "wasm-bindgen" && !activePinExact)
    throw new ScrubEngineUnavailable("bindgen_pin_not_exact");
  return engine;
}

/** Readiness: true only when scrub calls can succeed. Never throws. */
export function scrubEngineReady(): boolean {
  try {
    activeScrubEngine();
    return true;
  } catch {
    return false;
  }
}

/** Tests only. */
export function resetScrubEngineForTests(): void {
  selected = undefined;
  active = undefined;
  lastFailure = undefined;
  pending = undefined;
  requiredRefused = false;
  requiredKind = undefined;
  activePinExact = false;
  resetBindgenScrubEngineForTests();
}

export const scrubText = (value: string): string => activeScrubEngine().scrubText(value);
export const scrubLong = (text: string): string => activeScrubEngine().scrubLong(text);
export const scrubJson = <T>(value: T): T => activeScrubEngine().scrubJson(value);
export const scrubRecords = (source: Uint8Array): ScrubRecordsResult => activeScrubEngine().scrubRecords(source);
/** scrub.ts `scrubNumber`, through the engine: scrubJson of a number is exactly scrubNumber. */
export const scrubNumber = (value: number): number | string => activeScrubEngine().scrubJson<number | string>(value);
/** The active engine's marker, for callers that stamp what they stored. */
export const activeScrubMarker = (): ScrubMarker => activeScrubEngine().marker;

// trace.ts isSafeIdentifier: TypeScript engine exactly as before; otherwise an id the
// engine cannot judge is not safe (undefined never equals the id).
installPredicateScrub((value) => {
  let engine: ScrubEngine;
  try {
    engine = activeScrubEngine();
  } catch {
    return undefined;
  }
  if (engine.kind === "ts") return engine.scrubText(value);
  try {
    return engine.scrubText(value);
  } catch {
    return undefined;
  }
});

/** What a log line carries instead of its text when the wasm engine cannot scrub it. */
export const SCRUB_UNAVAILABLE_LOG_TEXT = "[scrub-unavailable]";

/**
 * scrubText for log lines. TypeScript engine: exactly scrubText (errors
 * propagate as before). Otherwise a log line never fails and never carries
 * unscrubbed text: if the engine cannot scrub it, the text is replaced whole.
 */
export function scrubTextForLog(value: string): string {
  let engine: ScrubEngine;
  try {
    engine = activeScrubEngine();
  } catch {
    return SCRUB_UNAVAILABLE_LOG_TEXT;
  }
  if (engine.kind === "ts") return engine.scrubText(value);
  try {
    return engine.scrubText(value);
  } catch {
    return SCRUB_UNAVAILABLE_LOG_TEXT;
  }
}

/**
 * raw-chunks.ts `sealRawChunk`, which scrubs with the TypeScript engine
 * internally and stamps its constants. It cannot take an injected engine
 * without re-pinning raw-chunks.ts, so with wasm selected it is refused
 * (`seal_path_not_routed`) rather than run on the TypeScript engine. Under
 * SCRUB_ENGINE=wasm-bindgen the production constructors install
 * `routedRawChunkSealer()` instead; a call site that reaches this function then
 * was not wired and refuses (no TypeScript or raw fallback).
 */
export async function sealRawChunk(store: ObjectStore, input: SealRawChunk): Promise<SealedChunk> {
  const engine = activeScrubEngine();
  if (engine.kind !== "ts") throw new ScrubEngineUnavailable("seal_path_not_routed");
  return sealRawChunkTs(store, input);
}

/** One bound sealer per verified instance, so every call site shares its health state. */
const boundSealers = new WeakMap<ScrubEngine, BoundRawChunkSealer>();

/**
 * The raw-chunk sealer a production constructor installs as `rawChunkSealer`
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.9").
 *
 * - SCRUB_ENGINE unset, "" or "ts" (and the raw-ABI "wasm"): `undefined`, with no
 *   selection or any other side effect, so the constructor's object is exactly
 *   today's and the call site keeps using `sealRawChunk` above.
 * - SCRUB_ENGINE=wasm-bindgen: a sealer bound (`bindEngineRawSealer`) to the exact
 *   verified instance `initScrubEngine` installed. Throws, like `activeScrubEngine`,
 *   when that instance is not initialised, failed verification, is poisoned, or is
 *   not the checked-in pin under SCRUB_ENGINE_REQUIRED=wasm-bindgen. Call it after
 *   `initScrubEngine` has resolved.
 */
export function routedRawChunkSealer(): BoundRawChunkSealer | undefined {
  if (requiredRefused) refuseRequiredSelection();
  // Exact match only: any other value (unset, "", "ts", "wasm", or invalid) leaves the
  // constructor's object as it is today; invalid values still fail where they do today.
  const kind = selected ?? (processEnv()?.SCRUB_ENGINE === "wasm-bindgen" ? "wasm-bindgen" : undefined);
  if (kind !== "wasm-bindgen") return undefined;
  const engine = activeScrubEngine() as ScrubEngine & { readonly failed: boolean };
  // Only the instance initBindgen installed reaches here (activeScrubEngine never returns another).
  if (engine !== active || engine.kind !== "wasm-bindgen") throw new ScrubEngineUnavailable("not_initialised");
  let sealer = boundSealers.get(engine);
  if (!sealer) {
    sealer = bindEngineRawSealer(engine, { retained: "same-family-up-to-writer" });
    boundSealers.set(engine, sealer);
  }
  return sealer;
}

/** `{ rawChunkSealer }` under wasm-bindgen, `{}` otherwise: spread into a constructor's deps/target. */
export function routedRawChunkSealerDeps(): { rawChunkSealer?: BoundRawChunkSealer } {
  const rawChunkSealer = routedRawChunkSealer();
  return rawChunkSealer ? { rawChunkSealer } : {};
}

export type { BoundRawChunkSealer } from "../pipeline/engine-raw-chunks.js";
