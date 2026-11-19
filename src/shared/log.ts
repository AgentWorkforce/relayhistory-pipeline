/**
 * One JSON line per event, ids only. Section 12 of the plan: telemetry carries
 * ids, hashes, codes and counts, never session content, prompts, summaries,
 * paths, object bodies or foreign error text. The allowlist is enforced here,
 * at the only place a service writes a log line.
 */
import { scrubTextForLog as scrubText } from "@rhc/lib/scrub-engine/index.ts";

/** The analytics sink's operator counters (never analytics events themselves). */
const ANALYTICS_COUNTER_FIELDS = [
  "analytics_accepted", "analytics_dropped_queue_full", "analytics_dropped_invalid", "analytics_dropped_oversize",
  "analytics_dropped_rejected", "analytics_dropped_auth", "analytics_dropped_exhausted", "analytics_dropped_shutdown",
  "analytics_export_failures",
];

/** One async admission's wall time per phase (selfhost-admission.ts): deliverPrepared's timing_ms keys it may log. */
export const ADMISSION_PHASE_FIELDS: ReadonlyMap<string, string> = new Map([
  ["read", "admission_read_ms"], ["claim", "admission_claim_ms"], ["admission_gate", "admission_gate_ms"],
  ["objects", "admission_objects_ms"], ["commit", "admission_commit_ms"], ["finish", "admission_finish_ms"],
]);
const ADMISSION_PHASE_FIELDS_LIST = [...ADMISSION_PHASE_FIELDS.values()];

/** The ingest edge's seal pool (session-ingest/seal-pool.ts sealPoolLogFields): its size and counters only. */
export const SEAL_POOL_LOG_FIELDS = [
  "seal_pool_workers", "seal_pool_pooled", "seal_pool_fallback", "seal_pool_worker_errors", "seal_pool_respawned", "seal_pool_retired",
];

/**
 * Bob Costas's summary input (src/bob-costas/session-input.ts): how much of a capped session a
 * digest read, and the caps it logs at startup. Bounded counts only.
 */
const SUMMARY_INPUT_FIELDS = [
  "chunks_read", "chunks_total", "bytes_read", "names_dropped", "turns_dropped",
  "max_chunks", "max_input_bytes", "max_chunk_bytes",
];

/** The streaming reconcile read budgets serve() logs at startup (searchIndexStreamBudgetWarning). */
const STREAM_BUDGET_FIELDS = ["max_read_bytes", "max_read_events", "largest_streamed_scope_bytes", "largest_streamed_scope_events"];

/** The search pass's phase timers and write counters (session-pipeline/consumer.ts). */
const SEARCH_PHASE_FIELDS = ["source_read_ms", "index_scan_ms", "locked_apply_ms", "write_calls", "write_batches"];

export const ALLOWED_LOG_FIELDS = new Set([
  // The staged run summary line: the pass was answered from a cached size refusal (search-index.ts StagedRefusals).
  "cached_refusal",
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
  // The pg Client 'error' line's closed message shape, the first frame's function name and the
  // client phase (DB_ERROR_DETAIL_VALUES, FRAME_NAME_SHAPE, DB_CLIENT_PHASE_VALUES; anything else is
  // dropped; the mirrors are pinned in src/session-ingest/__tests__/db-error-class.test.ts).
  "db_error_detail", "error_frame", "db_client_phase",
  // ...and a 55000's closed refusal class (db-refusal-class.ts; DB_REFUSAL_VALUES, anything else dropped;
  // the mirror is pinned in src/session-ingest/__tests__/ingest-refusal-log.test.ts).
  "db_refusal",
  // The batch route's uploading client (src/session-ingest/client-agent.ts): closed values from the
  // User-Agent, never its text (CLIENT_KIND_VALUES, CLIENT_OS_VALUES, CLIENT_VERSION_SHAPE; mirrors pinned
  // in src/session-ingest/__tests__/ingest-refusal-log.test.ts).
  "client_kind", "client_version", "client_os",
  // Lock-wait alert line (session-ingest lockAlertFields): 55P03 / 57014, plus 40P01 for a buffer stage converted
  // to 429 (BufferLockBusy). The buffer lock retry line (deliver.ts BufferLockRetryFields): the failed retry's
  // SQLSTATE. Origin fence fallback (ledger-sql.ts): 42883. Read API buffered fence
  // logs (src/session-read-api/app.ts): the two-char SQLSTATE class only (BufferedFenced.sqlstateClass).
  "sqlstate",
  // Read API buffered search gate (src/session-read-api/app.ts gatedPage): bounded counts only,
  // see BOUNDED_COUNT_FIELDS.
  "buffered_checked", "buffered_dropped",
  // Self-host async admission consumer (src/session-ingest/selfhost-admission.ts): bounded counts only.
  "admission_backlog", "admission_oldest_age_ms", "admission_dead_letters", "admission_in_flight",
  // ...its upload-inbox promotion pass: bounded counts, and `upload_results` (`code:n,...`, built from fixed
  // result codes by uploadResultsField, never a key or id).
  "promoted", "waiting", "upload_due", "upload_origins", "upload_results",
  // ...a `ledger_store_*` retry's object-store HTTP status (LedgerUnavailableError.storeStatus): a bounded integer.
  "store_status",
  // ...and, with no HTTP status, its closed transport-failure class (STORE_CAUSE_VALUES; anything else dropped).
  "store_cause",
  // ...and one admission's wall time per phase (ADMISSION_PHASE_FIELDS): bounded counts only.
  ...ADMISSION_PHASE_FIELDS_LIST,
  // ...and which batch and which attempt it was (selfhost-admission.ts admissionLogTags): the first 8 hex of a
  // sha256 of the receipt directory and of the inbox key, so attempts of one batch group together. Never the key.
  "receipt_tag", "attempt_tag",
  // Search index failures (searchIndexErrorDetail): the adapter's bounded detail code only
  // (e.g. `http_429:bulk_items_812:bulk_bytes_4190000`), never backend text or a reason.
  "error_detail",
  // Canonical projection reason codes (src/session-read-api/canonical.ts canonicalReasonCodeFields):
  // snake_case codes joined by `,`, never ids, keys or values.
  "reason_codes",
  // Analytics export counters (src/shared/analytics-config.ts analyticsCounterFields): bounded counts only.
  ...ANALYTICS_COUNTER_FIELDS,
  // The seal pool's size and counters (SEAL_POOL_LOG_FIELDS): bounded counts only.
  ...SEAL_POOL_LOG_FIELDS,
  // The consumer's streaming reconcile startup line (search-index.ts searchIndexStreamStartup):
  // `streaming` is on / off only (STREAMING_VALUES), the budgets bounded counts (STREAM_BUDGET_FIELDS).
  "streaming",
  ...STREAM_BUDGET_FIELDS,
  ...SEARCH_PHASE_FIELDS,
  // The ingest origin claim fence fallback (src/session-ingest/ledger-sql.ts originScoped): epoch ms until which the
  // isolate uses the per-user functions; a bounded integer.
  "fallback_until_ms",
  ...SUMMARY_INPUT_FIELDS,
  // Summary regeneration (src/summary-regen/audit.ts): the session id is logged only as session_hash (sha256);
  // system and regeneration_reason are closed sets (ENUM_FIELDS); prior_* and failure_code are controlled codes.
  "session_hash", "system", "request_id", "prior_status", "prior_failure_code", "regeneration_reason", "failure_code",
  // The claims scheduler tick line (src/session-analysis/claims-scheduler.ts): bounded counts only.
  "selected", "skipped_budget", "skipped_control",
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
  ["promoted", 100_000],
  ["waiting", 10_000_000],
  ["upload_due", 10_000_000],
  ["upload_origins", 1_000_000],
  ["store_status", 599],
  ["fallback_until_ms", Number.MAX_SAFE_INTEGER],
  ["selected", 100_000],
  ["skipped_budget", 100_000],
  ["skipped_control", 100_000],
  ...ADMISSION_PHASE_FIELDS_LIST.map((f): [string, number] => [f, 3_600_000]),
  ...ANALYTICS_COUNTER_FIELDS.map((f): [string, number] => [f, Number.MAX_SAFE_INTEGER]),
  ...SEAL_POOL_LOG_FIELDS.map((f): [string, number] => [f, Number.MAX_SAFE_INTEGER]),
  ...STREAM_BUDGET_FIELDS.map((f): [string, number] => [f, Number.MAX_SAFE_INTEGER]),
  ...SEARCH_PHASE_FIELDS.map((f): [string, number] => [f, Number.MAX_SAFE_INTEGER]),
  ...SUMMARY_INPUT_FIELDS.map((f): [string, number] => [f, Number.MAX_SAFE_INTEGER]),
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
/**
 * Enumerated fields: emitted only when the value is one of a closed set (else dropped), so they can never
 * carry free text. `store_cause` mirrors object-store STORE_TRANSPORT_CAUSES (log.test.ts pins the two equal).
 */
export const STORE_CAUSE_VALUES: ReadonlySet<string> = new Set([
  "econnreset", "etimedout", "econnrefused", "epipe", "abort", "timeout", "dns", "tls", "socket", "other",
]);
/** `streaming` on the consumer's streaming reconcile startup line. */
export const STREAMING_VALUES: ReadonlySet<string> = new Set(["on", "off"]);
/** `db_refusal` mirrors session-ingest DB_REFUSALS (ingest-refusal-log.test.ts pins the two equal). */
export const DB_REFUSAL_VALUES: ReadonlySet<string> = new Set([
  "generation_not_ready", "policy_expired", "policy_unavailable", "org_admission_closed", "org_prior_authority_unbound",
  "user_privacy_authority", "ack_seal", "native_custody_identity_changed", "source_row_privacy_fenced",
  "custody_privacy_inconsistent", "user_seal_missing", "receipt_proof_missing", "publication_transition", "other",
]);
/** `db_error_detail` / `db_client_phase` mirror session-ingest db-error-class.ts DB_ERROR_DETAILS / DB_CLIENT_PHASES. */
export const DB_ERROR_DETAIL_VALUES: ReadonlySet<string> = new Set(["socket_closed", "stream_state", "read_property", "not_function", "other", "none"]);
export const DB_CLIENT_PHASE_VALUES: ReadonlySet<string> = new Set(["active", "ending"]);
/** `client_kind` / `client_os` mirror session-ingest client-agent.ts CLIENT_KINDS / CLIENT_OSES (pinned in ingest-refusal-log.test.ts). */
export const CLIENT_KIND_VALUES: ReadonlySet<string> = new Set(["desktop", "other"]);
export const CLIENT_OS_VALUES: ReadonlySet<string> = new Set(["macos", "linux", "windows", "other"]);
/** Summary regeneration reasons and systems (src/summary-regen/settings.ts REGEN_REASONS pins the same set). */
export const REGEN_REASON_VALUES: ReadonlySet<string> = new Set([
  "turns_gained", "idle_uncovered", "ended_uncovered", "retry_backoff", "config_changed", "operator_requeue", "version_backfill",
]);
export const REGEN_SYSTEM_VALUES: ReadonlySet<string> = new Set(["bob_costas", "brief"]);
const ENUM_FIELDS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  ["store_cause", STORE_CAUSE_VALUES], ["streaming", STREAMING_VALUES],
  ["db_refusal", DB_REFUSAL_VALUES], ["client_kind", CLIENT_KIND_VALUES], ["client_os", CLIENT_OS_VALUES],
  ["regeneration_reason", REGEN_REASON_VALUES], ["system", REGEN_SYSTEM_VALUES],
  ["db_error_detail", DB_ERROR_DETAIL_VALUES], ["db_client_phase", DB_CLIENT_PHASE_VALUES],
]);
/** Shaped fields: emitted only when the string matches its pattern AND the scrubber leaves it unchanged (else
 * dropped). `client_version` is a release version (client-agent.ts CLIENT_VERSION, pinned equal in
 * ingest-refusal-log.test.ts): never a token, hostname or hex id. */
export const CLIENT_VERSION_SHAPE = /^[0-9]{1,5}(?:\.[0-9]{1,5}){1,3}(?:-[0-9A-Za-z.]{1,16})?$/;
export const LOG_TAG_SHAPE = /^[0-9a-f]{8}$/;
/** `error_frame`: a function name only (db-error-class.ts FRAME_NAME, pinned equal); never a path, line or text. */
export const FRAME_NAME_SHAPE = /^(?=.{1,64}$)[A-Za-z_$][A-Za-z0-9_$]*(?:\.[A-Za-z_$][A-Za-z0-9_$]*){0,3}$/;
const SHAPED_FIELDS: ReadonlyMap<string, RegExp> = new Map([["client_version", CLIENT_VERSION_SHAPE], ["receipt_tag", LOG_TAG_SHAPE], ["attempt_tag", LOG_TAG_SHAPE], ["error_frame", FRAME_NAME_SHAPE]]);

export function sanitizeLogField(key: string, value: unknown): unknown {
  if (!ALLOWED_LOG_FIELDS.has(key) || value === undefined) return undefined;
  const allowed = ENUM_FIELDS.get(key);
  if (allowed !== undefined) return typeof value === "string" && allowed.has(value) ? value : undefined;
  const shape = SHAPED_FIELDS.get(key);
  if (shape !== undefined) return typeof value === "string" && shape.test(value) && scrubText(value) === value ? value : undefined;
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
