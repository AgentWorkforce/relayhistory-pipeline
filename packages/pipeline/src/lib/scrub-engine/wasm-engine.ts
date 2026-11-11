/**
 * WasmScrubEngine: binds `ScrubEngine` to one pinned relayscrubber wasm artifact
 * through `wasm-abi.ts`. Initialisation verifies, in order: the pin is real (not
 * the placeholder), the artifact loads, its sha256 is the pinned one, it
 * instantiates, every ABI export exists, and the ABI version, ruleset version and
 * scrub profile the module reports equal the pin. Any failure throws
 * `ScrubEngineUnavailable`; there is no TypeScript fallback and no passthrough.
 *
 * The adapter does no scrubbing of its own: records go to the engine as raw
 * bytes, text/long/json as UTF-8 text, and the engine does all parsing,
 * duplicate-key handling, segmentation and secret-context logic.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import { SCRUB_RULESET, SCRUB_RULESET_VERSION } from "../pipeline/raw-chunks.js";
import { PipelineError } from "../pipeline/errors.js";
import {
  ScrubEngineUnavailable,
  type ScrubEngine,
  type ScrubMarker,
  type ScrubRecordsResult,
} from "./engine.js";
import {
  ABI_STATUS,
  AbiExportMissing,
  WASM_ABI_VERSION,
  bindScrubAbi,
  type AbiOperation,
  type ScrubAbi,
} from "./wasm-abi.js";

/** `packages/pipeline/scrub-wasm-pin.json`, validated. */
export interface ScrubWasmPin {
  crate: string;
  crate_version: string;
  ruleset_version: number;
  scrub_profile: string;
  wasm_sha256: string;
  abi_version: number;
}

/**
 * The artifact. `bytes` is always required (it is what the sha256 pin checks).
 * `module` is for runtimes that refuse to compile wasm from bytes (Cloudflare
 * Workers): a precompiled module from the same file, used to instantiate.
 */
export interface ScrubWasmArtifact {
  bytes: Uint8Array;
  /** A `WebAssembly.Module` (typed loosely: not every tsconfig here has the WebAssembly lib types). */
  module?: object;
}
export type LoadScrubWasmArtifact = () => Promise<ScrubWasmArtifact>;
/** Instantiate with no imports and return the instance exports. Injectable for tests. */
export type InstantiateScrubWasm = (artifact: ScrubWasmArtifact) => Promise<Record<string, unknown>>;

/** The slice of the WebAssembly global used here. */
interface WasmGlobal {
  /** From bytes: a module and an instance. From a module: the instance. */
  instantiate(source: object, imports: object): Promise<{ exports?: unknown; instance?: { exports: unknown } }>;
}

export const defaultInstantiate: InstantiateScrubWasm = async (artifact) => {
  const wasm = (globalThis as unknown as { WebAssembly: WasmGlobal }).WebAssembly;
  // No imports: the module gets no host capabilities.
  if (artifact.module) return (await wasm.instantiate(artifact.module, {})).exports as Record<string, unknown>;
  return (await wasm.instantiate(artifact.bytes, {})).instance?.exports as Record<string, unknown>;
};

const ZERO_SHA = /^0{64}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const PLACEHOLDER = /UNPINNED/i;

/** Validate the pin. The checked-in placeholder (all-zero sha, UNPINNED) is refused as `unpinned`. */
export function parseScrubWasmPin(raw: unknown): ScrubWasmPin {
  const p = raw as Partial<ScrubWasmPin> | null;
  if (!p || typeof p !== "object") throw new ScrubEngineUnavailable("pin_invalid");
  if (
    typeof p.wasm_sha256 !== "string" ||
    ZERO_SHA.test(p.wasm_sha256) ||
    typeof p.crate_version !== "string" ||
    PLACEHOLDER.test(p.crate_version) ||
    typeof p.scrub_profile !== "string" ||
    PLACEHOLDER.test(p.scrub_profile)
  )
    throw new ScrubEngineUnavailable("unpinned");
  if (
    p.crate !== "relayscrubber" ||
    !HEX64.test(p.wasm_sha256) ||
    !p.crate_version ||
    !p.scrub_profile ||
    !Number.isSafeInteger(p.ruleset_version) ||
    !Number.isSafeInteger(p.abi_version)
  )
    throw new ScrubEngineUnavailable("pin_invalid");
  if (p.abi_version !== WASM_ABI_VERSION) throw new ScrubEngineUnavailable("abi_mismatch");
  // Never a ruleset older than the one the TypeScript engine stamps today (readers' min-version logic is unchanged).
  if ((p.ruleset_version as number) < SCRUB_RULESET_VERSION) throw new ScrubEngineUnavailable("ruleset_mismatch");
  return {
    crate: p.crate,
    crate_version: p.crate_version,
    ruleset_version: p.ruleset_version as number,
    scrub_profile: p.scrub_profile,
    wasm_sha256: p.wasm_sha256,
    abi_version: p.abi_version as number,
  };
}

const encoder = new TextEncoder();
/** Fatal on invalid UTF-8; ignoreBOM keeps a leading U+FEFF as text, as the TypeScript engine does. */
const strictDecoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
/** A lone surrogate cannot cross a UTF-8 ABI unchanged. */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

export interface WasmScrubEngineOptions {
  pin: unknown;
  loadArtifact: LoadScrubWasmArtifact;
  instantiate?: InstantiateScrubWasm;
}

/** Verify and instantiate the pinned artifact. Throws ScrubEngineUnavailable; never returns a half-checked engine. */
export async function createWasmScrubEngine(options: WasmScrubEngineOptions): Promise<WasmScrubEngine> {
  const pin = parseScrubWasmPin(options.pin);
  let artifact: ScrubWasmArtifact;
  try {
    artifact = await options.loadArtifact();
  } catch {
    throw new ScrubEngineUnavailable("artifact_missing");
  }
  if (!artifact || !(artifact.bytes instanceof Uint8Array) || artifact.bytes.byteLength === 0)
    throw new ScrubEngineUnavailable("artifact_missing");
  if ((await sha256Hex(artifact.bytes)) !== pin.wasm_sha256) throw new ScrubEngineUnavailable("sha256_mismatch");
  let exports: Record<string, unknown>;
  try {
    exports = await (options.instantiate ?? defaultInstantiate)(artifact);
  } catch {
    throw new ScrubEngineUnavailable("instantiate_failed");
  }
  let abi: ScrubAbi;
  try {
    abi = bindScrubAbi(exports ?? {});
  } catch (error) {
    throw new ScrubEngineUnavailable(error instanceof AbiExportMissing ? "export_missing" : "instantiate_failed");
  }
  let abiVersion: number, rulesetVersion: number, profile: string;
  try {
    abiVersion = abi.abiVersion();
    rulesetVersion = abi.rulesetVersion();
    const p = abi.profile();
    if (p.status !== ABI_STATUS.OK || !p.bytes) throw new Error("profile");
    profile = strictDecoder.decode(p.bytes);
  } catch {
    throw new ScrubEngineUnavailable("instantiate_failed");
  }
  if (abiVersion !== pin.abi_version) throw new ScrubEngineUnavailable("abi_mismatch");
  if (rulesetVersion !== pin.ruleset_version) throw new ScrubEngineUnavailable("ruleset_mismatch");
  if (profile !== pin.scrub_profile) throw new ScrubEngineUnavailable("profile_mismatch");
  // The marker is what the instance itself reported (checked equal to the pin), never the pin alone.
  return new WasmScrubEngine(abi, { ruleset: SCRUB_RULESET, version: rulesetVersion, profile });
}

export class WasmScrubEngine implements ScrubEngine {
  readonly kind = "wasm" as const;
  readonly rulesetVersion: number;
  readonly scrubProfile: string;
  readonly marker: ScrubMarker;
  /** Set when the instance trapped: its memory may be inconsistent, so it is never called again. */
  #failed = false;
  readonly #abi: ScrubAbi;

  /** Use createWasmScrubEngine: the constructor does no verification. */
  constructor(abi: ScrubAbi, marker: ScrubMarker) {
    this.#abi = abi;
    this.marker = Object.freeze({ ...marker });
    this.rulesetVersion = marker.version;
    this.scrubProfile = marker.profile;
  }

  get failed(): boolean {
    return this.#failed;
  }

  #invoke(op: AbiOperation, input: Uint8Array) {
    if (this.#failed) throw new ScrubEngineUnavailable("engine_failed");
    try {
      return this.#abi.call(op, input);
    } catch {
      this.#failed = true;
      throw new ScrubEngineUnavailable("engine_failed");
    }
  }

  #decode(bytes: Uint8Array | undefined): string {
    if (!bytes) throw new ScrubEngineUnavailable("engine_refused");
    try {
      return strictDecoder.decode(bytes);
    } catch {
      throw new ScrubEngineUnavailable("engine_refused");
    }
  }

  #text(op: "scrub_text" | "scrub_long", value: string): string {
    if (LONE_SURROGATE.test(value))
      throw new PipelineError("scrub_input_not_well_formed", "scrub input is not well-formed UTF-16", false);
    const r = this.#invoke(op, encoder.encode(value));
    if (r.status !== ABI_STATUS.OK) throw new ScrubEngineUnavailable("engine_refused");
    return this.#decode(r.bytes);
  }

  scrubText(value: string): string {
    return this.#text("scrub_text", value);
  }

  scrubLong(text: string): string {
    return this.#text("scrub_long", text);
  }

  /** Defined over JSON values: the value crosses the ABI as JSON text and comes back parsed. */
  scrubJson<T>(value: T): T {
    // Nothing to scrub in these, and JSON.stringify(undefined) has no text.
    if (value === undefined || value === null || typeof value === "boolean") return value;
    const text = JSON.stringify(value);
    if (typeof text !== "string" || LONE_SURROGATE.test(text))
      throw new PipelineError("scrub_input_not_well_formed", "scrub input is not a JSON value", false);
    const r = this.#invoke("scrub_json", encoder.encode(text));
    if (r.status !== ABI_STATUS.OK) throw new ScrubEngineUnavailable("engine_refused");
    try {
      return JSON.parse(this.#decode(r.bytes)) as T;
    } catch (error) {
      if (error instanceof ScrubEngineUnavailable) throw error;
      throw new ScrubEngineUnavailable("engine_refused");
    }
  }

  scrubRecords(source: Uint8Array): ScrubRecordsResult {
    const r = this.#invoke("scrub_records", source);
    // The same refusal, code and retryability as the TypeScript engine.
    if (r.status === ABI_STATUS.INVALID_UTF8)
      throw new PipelineError("chunk_not_utf8", "raw chunk is not valid UTF-8", false);
    if (r.status !== ABI_STATUS.OK || !r.bytes || !Number.isSafeInteger(r.records) || (r.records as number) < 0)
      throw new ScrubEngineUnavailable("engine_refused");
    // Stored bytes must be text the readers can decode.
    this.#decode(r.bytes);
    return { scrubbed: r.bytes, records: r.records as number, scrub: this.marker };
  }
}
