/** Reader capability is independent of the version a writer stamps. */
import type { GetObject } from "../../../../object-store/src/types.js";
import { PipelineError } from "../pipeline/errors.js";

export const DEFAULT_READER_MAX_RULESET = 8;
export const SUPPORTED_READER_MAX_RULESET = 10;

function invalidConfig(): never {
  throw new Error("READER_MAX_RULESET must be an integer from 8 through 10");
}

export function checkedReaderMaxRuleset(value: number): number {
  if (!Number.isSafeInteger(value) || value < DEFAULT_READER_MAX_RULESET || value > SUPPORTED_READER_MAX_RULESET) invalidConfig();
  return value;
}

/** Unset preserves legacy reads; an explicit exact decimal opts into the policy. */
export function parseReaderMaxRuleset(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !/^[1-9][0-9]*$/.test(value)) invalidConfig();
  return checkedReaderMaxRuleset(Number(value));
}

/** Workers pass their bindings explicitly. Node services default to their process environment. */
export function readerMaxRuleset(env?: { READER_MAX_RULESET?: unknown }): number | undefined {
  const source = env ?? (globalThis as { process?: { env?: { READER_MAX_RULESET?: string } } }).process?.env;
  return parseReaderMaxRuleset(source?.READER_MAX_RULESET);
}

export function assertReaderScrubMarker(marker: unknown, maximum?: number): void {
  if (maximum === undefined) return; // Caller retains its existing presence/quarantine checks.
  checkedReaderMaxRuleset(maximum);
  const m = marker as { ruleset?: unknown; version?: unknown } | null;
  if (!m || typeof m !== "object" || Array.isArray(m) || m.ruleset !== "relayhistory-scrub" || typeof m.version !== "number" ||
      !Number.isSafeInteger(m.version) || m.version < 1 || m.version > maximum)
    throw new PipelineError("scrub_ruleset_unsupported", "the stored scrub ruleset is not supported by this reader", false);
}

/** The manifest carries a minimum, so every declared per-chunk version must also fit. */
export function assertReaderChunkMetadata(metadata: Record<string, string>, marker: unknown, maximum?: number): void {
  if (maximum === undefined) return;
  assertReaderScrubMarker(marker, maximum);
  const minimum = (marker as { version: number }).version;
  const version = metadata["scrub-version"];
  const ruleset = metadata["scrub-ruleset"];
  // Historical objects without per-chunk metadata retain their existing read
  // contract. Newly supported versions must carry their own chunk marker.
  if (version !== undefined || ruleset !== undefined || minimum > DEFAULT_READER_MAX_RULESET) {
    const parsed = typeof version === "string" && /^[1-9][0-9]*$/.test(version) ? Number(version) : Number.NaN;
    assertReaderScrubMarker({ ruleset, version: parsed }, maximum);
    if (parsed < minimum)
      throw new PipelineError("scrub_ruleset_unsupported", "the stored chunk scrub ruleset is inconsistent", false);
  }
}

/** Bind metadata to the same GET as the body; cancel a refused body before consuming it. */
export async function readerChunkBody(object: GetObject, marker: unknown, maximum?: number): Promise<ReadableStream<Uint8Array>> {
  if (maximum === undefined) return object.body;
  try {
    assertReaderChunkMetadata(object.meta.metadata ?? {}, marker, maximum);
    return object.body;
  } catch (error) {
    await object.body.cancel().catch(() => {});
    throw error;
  }
}
