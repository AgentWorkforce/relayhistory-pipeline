/**
 * The runtime-free half of partial-canonical.ts (the hosted Lambda's partial-session canonical
 * fallback): its deps type and the refusal classifier. app.ts imports only this file, so the read
 * Worker bundle never loads the Lambda's S3/zlib wiring. See partial-canonical.ts for the contract.
 */
import { ObjectStoreError } from "../../packages/object-store/src/errors.ts";
import { PipelineError } from "../../packages/pipeline/src/lib/pipeline/errors.ts";
import { CanonicalReadError } from "./canonical.ts";
import type { CanonicalEventsDeps } from "./canonical-events.ts";
import type { SessionSources } from "./session-sources.ts";

/** Left of the budget below this, the object read is not started (a transient refusal, no IO). */
export const PARTIAL_CANONICAL_MIN_READ_MS = 250;

/** The fallback's deps on ReadApiDeps (app.ts canonicalEventsPartial). */
export interface PartialCanonicalDeps {
  /** canonicalEventPage's source; its `deadlineMs` is replaced per request by what is left of `deadlineMs` below. */
  events: CanonicalEventsDeps;
  workspaces: ReadonlySet<string> | "*";
  /** The live sources of a session id (session-sources.ts), for a read without `source`. */
  sources: SessionSources;
  /** The whole request's budget for this path (READ_API_PARTIAL_CANONICAL_DEADLINE_MS). */
  deadlineMs: number;
  /**
   * The authoritative operator erase fence for one (source, id) key (liveness.ts sqlOperatorErased:
   * sessions.ingest_operator_runtime_erased as the read role). Always wired with this path; a throw
   * refuses the read (fail closed). True = 404 `session_fenced`, no content.
   */
  operatorFence(key: { orgId: string; workspaceId: string; source: string; sessionId: string }): Promise<boolean>;
}

export type PartialCanonicalClass = "missing" | "forbidden" | "transient" | "oversize" | "invalid";

const FORBIDDEN_REASONS = new Set([
  "tenant_mismatch", "pointer_invalid", "manifest_mismatch", "prefix_mismatch",
  // readCanonicalScope codes behind canonical_projection_incomplete
  "manifest_key_mismatch", "manifest_unscrubbed", "canonical_session_mismatch", "scrub_ruleset_unsupported",
  "chunk_digest_mismatch", "event_owner_conflict",
]);
const OVERSIZE_REASONS = new Set(["pointer_bound", "chunk_bound", "scope_too_large", "chunk_too_large", "row_bound"]);
const MISSING_REASONS = new Set(["object_missing", "manifest_missing", "chunk_missing"]);
const TRANSIENT_REASONS = new Set(["deadline", "source_generation_changed", "chunk_unavailable", "not_configured"]);

/** One refusal's class plus its reason codes (snake_case only; logged as `reason_codes`, never sent). */
export function classifyPartialCanonical(error: unknown): { class: PartialCanonicalClass; codes: string[] } {
  const of = (codes: string[]): PartialCanonicalClass => {
    // Trust-boundary refusals win: a forbidden object is never reported as merely large or gone.
    if (codes.some((c) => FORBIDDEN_REASONS.has(c))) return "forbidden";
    if (codes.some((c) => OVERSIZE_REASONS.has(c))) return "oversize";
    if (codes.some((c) => MISSING_REASONS.has(c))) return "missing";
    if (codes.some((c) => TRANSIENT_REASONS.has(c))) return "transient";
    return "invalid";
  };
  if (error instanceof CanonicalReadError) {
    const codes = [error.reason, ...error.details];
    return { class: of(codes), codes };
  }
  if (error instanceof ObjectStoreError) {
    const code = `store_${error.code}`;
    switch (error.code) {
      case "not_found": return { class: "missing", codes: [code] };
      case "unauthorized": case "content_mismatch": case "invalid_key": return { class: "forbidden", codes: [code] };
      case "transient": return { class: "transient", codes: [code] };
      // objectStoreManifestReader: a manifest past its 1 MiB cap.
      case "invalid_argument": return error.backend === "manifest" ? { class: "oversize", codes: ["manifest_too_large"] } : { class: "invalid", codes: [code] };
      default: return { class: "invalid", codes: [code] };
    }
  }
  if (error instanceof PipelineError) {
    const codes = /^[a-z][a-z0-9_]{0,63}$/u.test(error.code) ? [error.code] : [];
    const cls = of(codes);
    return { class: cls === "invalid" && error.retryable ? "transient" : cls, codes };
  }
  // Abort, network and anything unrecognised: retryable, never content-bearing.
  return { class: "transient", codes: [] };
}
