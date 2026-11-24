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
  "migration", "ledger_sha", "accepted_as_alias_of",
  "route", "correlation_id",
  // Queue and upload latency of the session pipeline's lines (session-pipeline/consumer.ts); value rules below.
  "event_age_ms", "s3_event_age_ms", "queue_origin",
  // How a search pass's acceptance refusal looked (session-pipeline/contender-seats.ts); value rule below.
  "contender_seats",
  // How often a scope was requeued for the opt-in batch deadline (session-pipeline/consumer.ts); value rule below.
  "batch_deadline_deferrals",
]);

/** Allowlisted keys that survive only as a non-negative safe integer; any other value is dropped. */
export const NON_NEGATIVE_INTEGER_FIELDS: ReadonlySet<string> = new Set(["event_age_ms", "s3_event_age_ms", "batch_deadline_deferrals"]);

/** Allowlisted keys that survive only as one of a fixed set of strings; any other value is dropped. */
export const EXACT_VALUE_FIELDS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  ["queue_origin", new Set(["s3", "requeue", "sweep"])],
  ["contender_seats", new Set(["all_unseated", "seated_unseated", "both", "catalog_key"])],
]);

/** Whether an allowlisted key's value passes its value rule (keys without a rule: always). */
function valueAllowed(k: string, v: unknown): boolean {
  if (NON_NEGATIVE_INTEGER_FIELDS.has(k)) return typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
  const exact = EXACT_VALUE_FIELDS.get(k);
  if (exact) return typeof v === "string" && exact.has(v);
  return true;
}

export type Level = "debug" | "info" | "warn" | "error";
const service = process.env.OTEL_SERVICE_NAME ?? "96bulls";
const deployment = process.env.DEPLOYMENT ?? "selfhost";
const deploymentId = process.env.DEPLOYMENT_ID ?? "local";

function filter(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (!ALLOWED_LOG_FIELDS.has(k)) continue;
    if (v === undefined) continue;
    if (!valueAllowed(k, v)) continue;
    // Strings are scrubbed again before emit as a backstop; ids never match a rule.
    out[k] = typeof v === "string" ? scrubText(v).slice(0, 256) : v;
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
