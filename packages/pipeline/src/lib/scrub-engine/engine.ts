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
  readonly kind: "ts" | "wasm";
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
  | "seal_path_not_routed";

/** Scrubbing is refused: nothing is returned, nothing passes through unscrubbed. Retryable: no input is lost. */
export class ScrubEngineUnavailable extends PipelineError {
  constructor(readonly reason: ScrubEngineFailure) {
    super("scrub_engine_unavailable", `the scrub engine is unavailable (${reason})`, true);
    this.name = "ScrubEngineUnavailable";
  }
}

/** SCRUB_ENGINE holds a value other than unset, "", "ts" or "wasm", or a second selection disagrees. Startup fails. */
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
