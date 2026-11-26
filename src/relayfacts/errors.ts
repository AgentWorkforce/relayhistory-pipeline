/**
 * Errors the relayfacts tools print. A tool's own refusal (UsageError and its kin) carries a message written
 * here, never data; anything else (a database, provider, parse or system error) is reduced to a fixed code,
 * since its message can carry provider, database or session text.
 */
import { constants } from "node:os";

const errnoConstants = constants.errno;

export class UsageError extends Error {
  constructor(message: string) { super(message); this.name = "UsageError"; }
}

/** Canonical read refusals (session-read-api CanonicalReadError reasons) and the error classes the tools meet. */
export const KNOWN_CODES: ReadonlySet<string> = new Set([
  "analysis_cache_unavailable", "analysis_wakeup", "authorization", "canonical_projection_incomplete", "canonical_unavailable",
  "chunk_bound", "cursor_scope", "cursor", "deadline", "event_invalid", "legacy_evidence_invalid", "manifest_mismatch",
  "metadata_bound", "metadata_parse", "not_configured", "object_missing", "parameter", "pointer_bound", "pointer_invalid",
  "prefix_mismatch", "route", "row_bound", "source_generation_changed", "tenant_mismatch", "timing_turns_unknown", "too_large",
  "UsageError", "ScopeError", "CorpusError", "CanonicalReadError", "SessionSourceIncompleteError",
  "TypeError", "RangeError", "SyntaxError", "AbortError", "TimeoutError",
]);
const ERRNO: ReadonlySet<string> = new Set(Object.keys(errnoConstants));
/** PostgreSQL's SQLSTATE classes (Appendix A): only these become `pg_<class>`. */
const SQLSTATE_CLASSES: ReadonlySet<string> = new Set(("00 01 02 03 08 09 0A 0B 0F 0L 0P 0Z 20 21 22 23 24 25 26 27 28 2B 2D 2F 34 38 39 " +
  "3B 3D 3F 40 42 44 53 54 55 57 58 72 F0 HV P0 XX").split(" "));

/**
 * A fixed error code for a log line, from a closed set: a known code or class, a system errno name, or a
 * PostgreSQL SQLSTATE reduced to its two-character class (`pg_42`); anything else is `error`. Never a message or a
 * caller-supplied value.
 */
export function errorCode(error: unknown): string {
  const e = (error ?? {}) as { reason?: unknown; code?: unknown; name?: unknown; constructor?: { name?: string } };
  for (const v of [e.reason, e.code, e.name, e.constructor?.name]) {
    if (typeof v !== "string") continue;
    if (KNOWN_CODES.has(v) || ERRNO.has(v)) return v;
    if (/^[0-9A-Z]{5}$/u.test(v) && SQLSTATE_CLASSES.has(v.slice(0, 2))) return `pg_${v.slice(0, 2)}`;
  }
  return "error";
}

/** What a CLI prints for an error: the message of its own refusals, the code of anything else. */
export function printable(error: unknown, own: ReadonlyArray<abstract new (...args: never[]) => Error> = []): string {
  return error instanceof UsageError || own.some((c) => error instanceof c) ? (error as Error).message : errorCode(error);
}
