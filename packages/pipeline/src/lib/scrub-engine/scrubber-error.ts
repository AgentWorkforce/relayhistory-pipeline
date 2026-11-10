/** Content-free call refusals and sticky instance failures at the Rust boundary. */
import { PipelineError } from "../pipeline/errors.js";

export const SCRUBBER_CALL_REFUSAL_CODES = [
  "text_too_large", "record_too_large", "stream_too_large", "json_too_deep",
  "invalid_json", "invalid_utf8", "serialization_error",
] as const;
export type ScrubberCallRefusalCode = typeof SCRUBBER_CALL_REFUSAL_CODES[number];
export type ScrubberErrorCode = ScrubberCallRefusalCode
  | "scrubber_engine_failure" | "scrubber_instance_poisoned"
  | "scrub_input_not_well_formed" | "scrub_input_too_large"
  | "scrub_input_shared_buffer" | "chunk_not_utf8" | "scrub_admission_unavailable";
export type ScrubberDisposition = "call_refusal" | "instance_poison";

export class ScrubberError extends PipelineError {
  readonly reason: "engine_failed" | "engine_refused";
  constructor(code: ScrubberErrorCode, readonly disposition: ScrubberDisposition,
              readonly record?: number, retryable = disposition === "instance_poison") {
    super(code, `scrubber ${disposition} (${code})`, retryable);
    this.name = "ScrubberError";
    this.reason = disposition === "instance_poison" ? "engine_failed" : "engine_refused";
  }
}

/** Only the crate's explicit JSON string tag can declare a reusable refusal. */
export function taggedScrubberRefusal(error: unknown): ScrubberError | undefined {
  if (typeof error !== "string" || error.length > 1024) return;
  let tag: unknown;
  try { tag = JSON.parse(error); } catch { return; }
  if (!tag || typeof tag !== "object" || Array.isArray(tag)) return;
  const v = tag as Record<string, unknown>;
  if (v.disposition !== "call_refusal" || typeof v.code !== "string" ||
      !(SCRUBBER_CALL_REFUSAL_CODES as readonly string[]).includes(v.code) ||
      Object.keys(v).some(k => k !== "code" && k !== "disposition" && k !== "record") ||
      (Object.hasOwn(v, "record") && (!Number.isSafeInteger(v.record) || (v.record as number) < 1))) return;
  return new ScrubberError(v.code as ScrubberCallRefusalCode, "call_refusal", v.record as number | undefined);
}
