/**
 * One JSON line per event, ids only. Section 12 of the plan: telemetry carries
 * ids, hashes, codes and counts, never session content, prompts, summaries,
 * paths, object bodies or foreign error text. The allowlist is enforced here,
 * at the only place a service writes a log line.
 */
import { scrubText } from "@rhc/lib/scrub.ts";

export const ALLOWED_LOG_FIELDS = new Set([
  "service", "deployment", "deployment_id", "stage", "status", "outcome", "attempt",
  "trace_id", "span_id", "traceparent", "session_uuid", "session_uuid_source",
  "tenant_id", "org_id", "workspace_id", "machine_id", "harness", "session_id",
  "revision", "work_key", "error_code", "retryable", "duration_ms",
  "scrub_ruleset", "scrub_version", "redaction_count",
  "object_sha256", "bytes_raw", "bytes_stored", "chunk_count", "seq", "epoch", "offset",
  "model_id", "tokens_in", "tokens_out",
  "count", "inserted", "deleted", "manifests", "claimed", "acked", "retried", "exhausted", "listed", "enqueued", "skipped", "failed",
  "state", "rule", "kind", "port", "ok", "reason", "key_sha256", "manifest_key_sha256",
  "migration", "ledger_name", "ledger_sha", "accepted_as_alias_of",
  "route", "correlation_id",
  // Closed values only (src/session-ingest/db-error-class.ts): never a message or stack.
  "db_error_class", "db_error_code", "db_error_basis", "error_name",
  // Lock-wait alert line (session-ingest lockAlertFields): 55P03 / 57014 only. Read API buffered fence
  // logs (src/session-read-api/app.ts): the two-char SQLSTATE class only (BufferedFenced.sqlstateClass).
  "sqlstate",
  // Read API buffered search gate (src/session-read-api/app.ts gatedPage): bounded counts only,
  // see BOUNDED_COUNT_FIELDS.
  "buffered_checked", "buffered_dropped",
  // Self-host async admission consumer (src/session-ingest/selfhost-admission.ts): bounded counts only.
  "admission_backlog", "admission_oldest_age_ms", "admission_dead_letters", "admission_in_flight",
  // Search index failures (searchIndexErrorDetail): the adapter's bounded detail code only
  // (e.g. `http_429:bulk_items_812:bulk_bytes_4190000`), never backend text or a reason.
  "error_detail",
]);

/**
 * Count fields that are emitted only as non-negative integers clamped to a ceiling. Anything else
 * (a string, a fraction, NaN, a negative or non-number value) is dropped, so these fields can
 * never carry free text or an identifier.
 */
export const BOUNDED_COUNT_FIELDS: ReadonlyMap<string, number> = new Map([
  ["buffered_checked", 100_000],
  ["buffered_dropped", 100_000],
  ["admission_backlog", 10_000_000],
  ["admission_oldest_age_ms", 10 * 365 * 24 * 3600 * 1000],
  ["admission_dead_letters", 10_000_000],
  ["admission_in_flight", 1_000],
]);

function boundedCount(v: unknown, max: number): number | undefined {
  if (typeof v !== "number" || !Number.isInteger(v) || v < 0) return undefined;
  return Math.min(v, max);
}

/**
 * The one field rule shared by log lines (logLine/emit) and span attributes (telemetry.ts
 * spanAttrs): the value to emit for `key`, or undefined to drop it. A key off the allowlist is
 * dropped; a BOUNDED_COUNT_FIELDS key survives only as a clamped non-negative integer; a string
 * is scrubbed and truncated. Other values pass through (callers narrow further by type).
 */
export function sanitizeLogField(key: string, value: unknown): unknown {
  if (!ALLOWED_LOG_FIELDS.has(key) || value === undefined) return undefined;
  const max = BOUNDED_COUNT_FIELDS.get(key);
  if (max !== undefined) return boundedCount(value, max);
  // Strings are scrubbed again before emit as a backstop; ids never match a rule.
  return typeof value === "string" ? scrubText(value).slice(0, 256) : value;
}

export type Level = "debug" | "info" | "warn" | "error";
const service = process.env.OTEL_SERVICE_NAME ?? "96bulls";
const deployment = process.env.DEPLOYMENT ?? "selfhost";
const deploymentId = process.env.DEPLOYMENT_ID ?? "local";

function filter(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(fields)) {
    const safe = sanitizeLogField(k, v);
    if (safe !== undefined) out[k] = safe;
  }
  return out;
}

/** The JSON line `emit` writes, for a runtime without process streams (the session read Worker logs it to the console). */
export function logLine(level: Level, message: string, fields: Record<string, unknown>): string {
  return JSON.stringify({
    ts: new Date().toISOString(),
    level,
    service,
    deployment,
    deployment_id: deploymentId,
    message: scrubText(message).slice(0, 200),
    ...filter(fields),
  });
}

function emit(level: Level, message: string, fields: Record<string, unknown>) {
  const line = logLine(level, message, fields);
  if (level === "error" || level === "warn") process.stderr.write(line + "\n");
  else process.stdout.write(line + "\n");
}

export const log = {
  debug: (m: string, f: Record<string, unknown> = {}) => emit("debug", m, f),
  info: (m: string, f: Record<string, unknown> = {}) => emit("info", m, f),
  warn: (m: string, f: Record<string, unknown> = {}) => emit("warn", m, f),
  error: (m: string, f: Record<string, unknown> = {}) => emit("error", m, f),
};

/** The code and retryable flag of an error, never its message. */
export function errorFields(err: unknown): Record<string, unknown> {
  const e = err as { code?: unknown; retryable?: unknown; name?: unknown };
  return {
    error_code: typeof e?.code === "string" ? e.code : typeof e?.name === "string" ? e.name : "unknown",
    ...(typeof e?.retryable === "boolean" ? { retryable: e.retryable } : {}),
  };
}

/** A SearchIndexError detail code: identifier tokens joined by `:`, never free text (emitted truncated to 256). */
const SEARCH_ERROR_DETAIL = /^[a-z0-9_.:]+$/;

/**
 * `{ error_detail }` for a search index adapter error (SearchIndexError, matched by
 * name so either package copy qualifies) whose detail is a bounded code; `{}` for
 * anything else. Never another error's message or `detail` (a pg error's detail
 * quotes row values).
 */
export function searchIndexErrorDetail(err: unknown): { error_detail?: string } {
  const e = err as { name?: unknown; detail?: unknown };
  if (e?.name !== "SearchIndexError" || typeof e.detail !== "string" || !SEARCH_ERROR_DETAIL.test(e.detail)) return {};
  return { error_detail: e.detail.slice(0, 256) };
}
