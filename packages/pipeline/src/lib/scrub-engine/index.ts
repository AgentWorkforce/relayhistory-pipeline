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
 *   every scrub call throws `ScrubEngineUnavailable`. There is no fallback to
 *   the TypeScript engine and no raw passthrough.
 * - Anything else: `ScrubEngineConfigError` at selection (startup fails).
 *
 * The selection is read once per process (isolate); a later, different
 * selection throws rather than switching engines.
 */
import { readerMaxRuleset } from "./reader-policy.js";
import { sealRawChunk as sealRawChunkTs, type SealRawChunk, type SealedChunk } from "../pipeline/raw-chunks.js";
import type { ObjectStore } from "../pipeline/object-layout.js";
import pinJson from "../../../scrub-wasm-pin.json";
import bindgenPinJson from "../../../scrub-bindgen-pin.json";
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
/** The pin checked in at packages/pipeline/scrub-bindgen-pin.json. */
export const SCRUB_BINDGEN_PIN: unknown = bindgenPinJson;

/** Where the self-host image / Node runtime expects the pinned artifact. */
export const SCRUB_WASM_PATH = "../../../scrub-wasm/relayscrubber.wasm";
// Resolved only inside the Node/Bun loader: a Worker bundle may have no usable
// import.meta.url, and module evaluation must not depend on it (hosted Worker unchanged).

interface ScrubEngineEnv {
  SCRUB_ENGINE?: string | undefined;
  READER_MAX_RULESET?: string | undefined;
  /** wasm-bindgen only: absolute directory of the installed package (default: next to the pipeline package). */
  SCRUB_BINDGEN_DIR?: string | undefined;
}

let selected: ScrubEngineKind | undefined;
let active: ScrubEngine | undefined;
let lastFailure: ScrubEngineFailure | undefined;
let pending: Promise<ScrubEngine> | undefined;

/** Parse SCRUB_ENGINE. Exact and case-sensitive; "" counts as unset. */
export function parseScrubEngineKind(value: string | undefined): ScrubEngineKind {
  if (value === undefined || value === "" || value === "ts") return "ts";
  if (value === "wasm") return "wasm";
  if (value === "wasm-bindgen") return "wasm-bindgen";
  throw new ScrubEngineConfigError(`SCRUB_ENGINE must be unset, "ts", "wasm" or "wasm-bindgen"`);
}

/**
 * Select the engine for this process from `env.SCRUB_ENGINE`. Idempotent for the
 * same value; a different value after the first selection throws.
 */
export function selectScrubEngine(env: ScrubEngineEnv | undefined): ScrubEngineKind {
  const kind = parseScrubEngineKind(env?.SCRUB_ENGINE);
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
 * serving. Throws ScrubEngineConfigError (bad SCRUB_ENGINE) or
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
    const engine = await initBindgenScrubEngine({
      readerMaxRuleset: readerMaxRuleset(env),
      pin: "pin" in o ? o.pin : SCRUB_BINDGEN_PIN,
      readArtifact: o.readArtifact ?? bindgenFileReader(env?.SCRUB_BINDGEN_DIR),
      importGlue: o.importGlue,
    });
    active = engine;
    // A repeat init after the instance trapped reports the trap instead of resolving.
    if (engine.failed) throw new ScrubEngineUnavailable("engine_failed");
    lastFailure = undefined;
    return engine;
  } catch (error) {
    const unavailable = error instanceof ScrubEngineUnavailable ? error : new ScrubEngineUnavailable("bindgen_init_failed");
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
  if (selected === undefined) selectScrubEngine(processEnv());
  if (selected === "ts") return tsScrubEngine;
  const engine = active as (ScrubEngine & { failed?: boolean }) | undefined;
  if (!engine) throw new ScrubEngineUnavailable(lastFailure ?? "not_initialised");
  if (engine.failed) throw new ScrubEngineUnavailable("engine_failed");
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
 * (`seal_path_not_routed`) rather than run on the TypeScript engine.
 */
export async function sealRawChunk(store: ObjectStore, input: SealRawChunk): Promise<SealedChunk> {
  const engine = activeScrubEngine();
  if (engine.kind !== "ts") throw new ScrubEngineUnavailable("seal_path_not_routed");
  return sealRawChunkTs(store, input);
}
