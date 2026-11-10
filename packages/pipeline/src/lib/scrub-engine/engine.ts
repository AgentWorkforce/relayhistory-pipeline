/**
 * The scrub engine seam: what the pipeline needs from a scrubber, and the
 * TypeScript engine (`lib/scrub.ts` + `scrubRecords`/`scrubLong` in
 * `lib/pipeline/raw-chunks.ts`), which delegates straight to those functions so
 * its output is byte-identical to calling them directly. docs/scrub-engine.md.
 *
 * Neither scrub.ts nor raw-chunks.ts is modified (technical-foul.json pins them);
 * they stay the reference implementation and, after the wasm cutover, the frozen
 * oracle the parity harness compares against.
 */
import { scrubJson, scrubText } from "../scrub.js";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  scrubLong,
  scrubRecords,
} from "../pipeline/raw-chunks.js";
import { PipelineError } from "../pipeline/errors.js";

/**
 * What a sealed object's scrub marker may say. It is taken from the engine
 * instance that produced the stored bytes (its `marker`, or the `scrub` field of
 * the `scrubRecords` result), never from configuration or the pin file.
 */
export interface ScrubMarker {
  /** `raw.scrub.ruleset` (the rule family name). */
  readonly ruleset: string;
  /** `raw.scrub.version`. */
  readonly version: number;
  /** The engine's scrub profile identifier (see docs/scrub-engine.md). */
  readonly profile: string;
}

export interface ScrubRecordsResult {
  scrubbed: Uint8Array;
  records: number;
  /** The marker of the engine instance that produced `scrubbed`. */
  scrub: ScrubMarker;
}

/** Exactly what the pipeline calls; nothing else crosses the seam. */
export interface ScrubEngine {
  readonly kind: "ts" | "wasm" | "wasm-bindgen";
  /** Ruleset version this engine instance stamps (reported by the engine itself). */
  readonly rulesetVersion: number;
  /** Scrub profile this engine instance stamps (reported by the engine itself). */
  readonly scrubProfile: string;
  readonly marker: ScrubMarker;
  /** One string; truncated (announced in band) past `MAX_SCRUB_CHARS`. */
  scrubText(value: string): string;
  /** One string of any length, scrubbed whole in segments. */
  scrubLong(text: string): string;
  /** Every string value and key of a JSON value, and card numbers among its numbers. */
  scrubJson<T>(value: T): T;
  /** One raw source range, record by record (`raw-chunks.ts` `scrubRecords`). */
  scrubRecords(source: Uint8Array): ScrubRecordsResult;
}

/**
 * Why scrubbing is refused. Never carries input, output or artifact bytes.
 *
 * - unpinned: the pin file still holds placeholder values
 * - pin_invalid: the pin file is malformed
 * - artifact_missing: the loader failed or returned no bytes
 * - sha256_mismatch / abi_mismatch / ruleset_mismatch / profile_mismatch: the artifact is not the pinned one
 * - instantiate_failed: instantiation or a version/profile export threw
 * - export_missing: a required ABI export is absent or not callable
 * - not_initialised: SCRUB_ENGINE=wasm but `initScrubEngine` has not completed
 * - engine_failed: the instance trapped during a call; it is never used again
 * - engine_refused: the instance returned an unexpected status for one call
 * - seal_path_not_routed: `sealRawChunk` can only run the TypeScript engine (it lives in pinned raw-chunks.ts)
 * - bindgen_*: SCRUB_ENGINE=wasm-bindgen (bindgen-engine.ts); see `BindgenFailure`
 */
export type ScrubEngineFailure =
  | "unpinned"
  | "pin_invalid"
  | "artifact_missing"
  | "sha256_mismatch"
  | "abi_mismatch"
  | "ruleset_mismatch"
  | "profile_mismatch"
  | "instantiate_failed"
  | "export_missing"
  | "not_initialised"
  | "engine_failed"
  | "engine_refused"
  | "seal_path_not_routed"
  | BindgenFailure;

/** The three verified byte artifacts of the wasm-bindgen package. */
export type BindgenArtifactName = "wasm" | "glue" | "loader";
/** Every field of the relayscrubber schema-3 manifest. */
export type BindgenManifestField =
  | "schema"
  | "ruleset"
  | "rulesetVersion"
  | "rulesetSourceSha256"
  | "crateVersion"
  | "abiVersion"
  | "policy"
  | "redactionToken"
  | "scrubProfile";

/**
 * Why SCRUB_ENGINE=wasm-bindgen is refused (docs/scrub-engine.md, "wasm-bindgen path").
 *
 * - bindgen_pin_invalid: the pin file is not an object, or names another pin schema / package
 * - bindgen_pin_paths_invalid: an artifact path in the pin is absent, absolute or escapes the package directory
 * - bindgen_unpinned_<field>: that pin field is a placeholder, empty, or malformed (digests: not 64 lowercase hex)
 * - bindgen_pin_profile_inconsistent: the pinned profile is not the crate-owned profile in the pinned manifest
 * - bindgen_ruleset_below_floor: the pinned writer ruleset is below the supported writer floor
 * - bindgen_ruleset_above_reader_ceiling: the pinned writer version exceeds the explicit reader capability
 * - bindgen_dir_invalid: SCRUB_BINDGEN_DIR is set but not an absolute path
 * - bindgen_artifact_missing_<artifact>: reading it failed or returned no bytes
 * - bindgen_sha256_mismatch_<artifact>: its snapshot does not hash to the pin
 * - bindgen_glue_import_failed: evaluating the verified glue bytes failed
 * - bindgen_export_missing: the glue lacks a required function
 * - bindgen_init_failed: initialising the glue with the verified wasm failed
 * - bindgen_manifest_invalid: manifestJson() threw or is not a JSON object
 * - bindgen_manifest_mismatch_<field>: the instance's manifest differs from the pin in that field
 * - bindgen_manifest_mismatch_extra_field: the instance's manifest has a field the pin does not
 * - bindgen_profile_mismatch: the crate-owned profile in the instance's manifest is not the pinned one
 * - bindgen_init_conflict: a second initialisation with other pins or other bytes
 * - bindgen_path_conflict: the Node loader path and the Workers compiled-module path in one process
 * - bindgen_module_invalid: the compiled path was given something other than a WebAssembly.Module
 */
export type BindgenFailure =
  | "bindgen_pin_invalid"
  | "bindgen_pin_paths_invalid"
  | "bindgen_unpinned_package_version"
  | "bindgen_unpinned_scrub_profile"
  | `bindgen_unpinned_${BindgenArtifactName}_sha256`
  | `bindgen_unpinned_manifest_${BindgenManifestField}`
  | "bindgen_pin_profile_inconsistent"
  | "bindgen_ruleset_below_floor"
  | "bindgen_ruleset_above_reader_ceiling"
  | "bindgen_dir_invalid"
  | `bindgen_artifact_missing_${BindgenArtifactName}`
  | `bindgen_sha256_mismatch_${BindgenArtifactName}`
  | "bindgen_glue_import_failed"
  | "bindgen_export_missing"
  | "bindgen_init_failed"
  | "bindgen_manifest_invalid"
  | `bindgen_manifest_mismatch_${BindgenManifestField}`
  | "bindgen_manifest_mismatch_extra_field"
  | "bindgen_profile_mismatch"
  | "bindgen_init_conflict"
  | "bindgen_path_conflict"
  | "bindgen_module_invalid";

/** Scrubbing is refused: nothing is returned, nothing passes through unscrubbed. Retryable: no input is lost. */
export class ScrubEngineUnavailable extends PipelineError {
  constructor(readonly reason: ScrubEngineFailure) {
    super("scrub_engine_unavailable", `the scrub engine is unavailable (${reason})`, true);
    this.name = "ScrubEngineUnavailable";
  }
}

/** SCRUB_ENGINE holds a value other than unset, "", "ts", "wasm" or "wasm-bindgen", or a second selection disagrees. Startup fails. */
export class ScrubEngineConfigError extends PipelineError {
  constructor(message: string) {
    super("scrub_engine_invalid", message, false);
    this.name = "ScrubEngineConfigError";
  }
}

/** Profile the TypeScript engine reports. It is not written anywhere: sealRawChunk stamps ruleset + version only, as before. */
export const TS_SCRUB_PROFILE = "relayhistory-scrub/typescript";

const TS_MARKER: ScrubMarker = Object.freeze({
  ruleset: SCRUB_RULESET,
  version: SCRUB_RULESET_VERSION,
  profile: TS_SCRUB_PROFILE,
});

/** The default engine: the existing functions, called directly. */
export const tsScrubEngine: ScrubEngine = Object.freeze({
  kind: "ts" as const,
  rulesetVersion: SCRUB_RULESET_VERSION,
  scrubProfile: TS_SCRUB_PROFILE,
  marker: TS_MARKER,
  scrubText: (value: string) => scrubText(value),
  scrubLong: (text: string) => scrubLong(text),
  scrubJson: <T>(value: T): T => scrubJson(value),
  scrubRecords: (source: Uint8Array): ScrubRecordsResult => {
    const { scrubbed, records } = scrubRecords(source);
    return { scrubbed, records, scrub: TS_MARKER };
  },
});
