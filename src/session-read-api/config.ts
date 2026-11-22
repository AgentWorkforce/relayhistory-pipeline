/**
 * Runtime-neutral configuration parsing shared by the Lambda (lambda.ts) and
 * the Worker (packages/session-read-worker): no AWS or Node imports here.
 */

/** "epoch" → 0 (complete history); otherwise an ISO timestamp. Missing or junk refuses to start. */
export function parseCompleteSince(raw: string | undefined): number {
  if (raw === "epoch") return 0;
  const t = raw ? Date.parse(raw) : Number.NaN;
  if (Number.isNaN(t)) throw new Error('session read api: READ_API_COMPLETE_SINCE must be an ISO timestamp or "epoch"');
  return t;
}

/** READ_API_DIRECTORY_WORKSPACES: unset or blank = none; "*" = every workspace; else comma-separated ids. */
export function parseDirectoryWorkspaces(raw: string | undefined): ReadonlySet<string> | "*" {
  const value = raw?.trim() ?? "";
  if (value === "*") return "*";
  return new Set(value.split(",").map((v) => v.trim()).filter(Boolean));
}

/** The facts store an Output or digest backend reads: today's ClickHouse facts, or the self-host Postgres facts (migration 045040). */
export type FactsBackendKind = "clickhouse" | "postgres";
export const READ_API_OUTPUT_BACKEND = "READ_API_OUTPUT_BACKEND";
export const READ_API_DIGEST_BACKEND = "READ_API_DIGEST_BACKEND";
export const READ_API_COORDINATION_OVERLAPS_BACKEND = "READ_API_COORDINATION_OVERLAPS_BACKEND";
export const READ_API_COORDINATION_OVERLAPS_ENABLED = "READ_API_COORDINATION_OVERLAPS_ENABLED";
/** Exactly "true" (exactTrueFlag): GET /v1/coordination/advisories and its ack; anything else = 501 `unsupported_by_backend`, reason `advisories_disabled`. */
export const READ_API_COORDINATION_ADVISORIES_ENABLED = "READ_API_COORDINATION_ADVISORIES_ENABLED";
/**
 * Exactly "true" (exactTrueFlag): M3-7 advisory priority. The advisories GET reads 062190's priority-first
 * pending_for_v2 and its items, and the workspace view's alerts, carry `priority` and `notifyEligible` (the
 * advisories GET also `delivery`). Anything else: 062000's v1 only and the pre-M3-7 response, 062190 applied or not.
 */
export const READ_API_ADVISORY_PRIORITY_ENABLED = "READ_API_ADVISORY_PRIORITY_ENABLED";
/**
 * Exactly "true" (exactTrueFlag): the advisory instance (20261105062220). The advisories GET reads pending_for_v3,
 * answers contract "advisories/v3" and gives each item its `incarnation` (null on a legacy row); an ack POST with an
 * `incarnation` acks that instance through ack_v2. Anything else: the response bytes and the v1 ack exactly as before.
 * Instance mode is priority-first whatever READ_API_ADVISORY_PRIORITY_ENABLED says (v3 is v2's order; that switch
 * still decides the priority fields). Needs 062220 applied (no fallback: the GET answers 503 and logs
 * coordination_advisories_pending_for_v3_missing; turn it off before rolling 062220 back).
 */
export const READ_API_ADVISORY_INSTANCE_ENABLED = "READ_API_ADVISORY_INSTANCE_ENABLED";
/** Exactly "true" (exactTrueFlag): GET /v1/session-intel/workspace; anything else = 501 `unsupported_by_backend`, reason `session_intel_disabled`. */
export const READ_API_SESSION_INTEL_ENABLED = "READ_API_SESSION_INTEL_ENABLED";

/**
 * READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND: unset or "" = "clickhouse" (the default,
 * today's behaviour); exactly "clickhouse" or "postgres"; anything else refuses to start.
 * Never echoes the value.
 */
export function parseFactsBackend(
  env: Record<string, unknown>,
  name: typeof READ_API_OUTPUT_BACKEND | typeof READ_API_DIGEST_BACKEND | typeof READ_API_COORDINATION_OVERLAPS_BACKEND,
): FactsBackendKind {
  const raw = env[name];
  if (raw === undefined || raw === "" || raw === "clickhouse") return "clickhouse";
  if (raw === "postgres") return "postgres";
  throw new Error(`session read api: ${name} must be unset, "", "clickhouse" or "postgres"`);
}

/** Only the exact string "true" exposes GET /v1/coordination/overlaps. */
export function coordinationOverlapsEnabled(raw: unknown): boolean {
  return raw === "true";
}

/** Hosted Lambda entry: wire the brief-summary read (GET /v1/sessions/:id/summary). The service always wires it. */
export const READ_API_BRIEF_SUMMARY_ENABLED = "READ_API_BRIEF_SUMMARY_ENABLED";
/** Every entry: serve the relayfacts reads (GET /v1/relayfacts, /v1/relayfacts/:topic). */
export const READ_API_RELAYFACTS_ENABLED = "READ_API_RELAYFACTS_ENABLED";
/** GET /v1/sessions: serve the opt-in includeShortSummary=recorded list extension. */
export const READ_API_LIST_SHORT_SUMMARY_ENABLED = "READ_API_LIST_SHORT_SUMMARY_ENABLED";
/**
 * GET /v1/sessions: serve the opt-ins includeReadiness=true (eventsReady, eventsProjectedAt) and readyOnly=true
 * (search-projection-ready rows only, filtered in SQL) through migration 062200's readiness functions. Exactly
 * "true" (exactTrueFlag); anything else = either param is 501 param_includeReadiness / param_readyOnly. Default off.
 */
export const READ_API_LIST_READINESS_ENABLED = "READ_API_LIST_READINESS_ENABLED";
/** GET /v1/sessions/summaries/search: ClickHouse summary search (clickhouse/summary-search-query.ts). Default off. */
export const READ_API_SUMMARY_SEARCH_ENABLED = "READ_API_SUMMARY_SEARCH_ENABLED";
/**
 * The store GET /v1/sessions/summaries/search reads once READ_API_SUMMARY_SEARCH_ENABLED is on: today's
 * ClickHouse documents, or the Postgres session directory (postgres/summary-search.ts). Service entry only.
 */
export type SummarySearchBackendKind = "clickhouse" | "postgres";
export const READ_API_SUMMARY_SEARCH_BACKEND = "READ_API_SUMMARY_SEARCH_BACKEND";

/**
 * READ_API_SUMMARY_SEARCH_BACKEND: unset or "" = "clickhouse" (the default, today's behaviour); exactly
 * "clickhouse" or "postgres"; anything else refuses to start. Never echoes the value.
 */
export function parseSummarySearchBackend(env: Record<string, unknown>): SummarySearchBackendKind {
  const raw = env[READ_API_SUMMARY_SEARCH_BACKEND];
  if (raw === undefined || raw === "" || raw === "clickhouse") return "clickhouse";
  if (raw === "postgres") return "postgres";
  throw new Error(`session read api: ${READ_API_SUMMARY_SEARCH_BACKEND} must be unset, "", "clickhouse" or "postgres"`);
}
/** GET /v1/sessions/:sessionId/sources: the live sources of one session id (session-sources.ts). Default off. */
export const READ_API_SESSION_SOURCES_ENABLED = "READ_API_SESSION_SOURCES_ENABLED";
/**
 * Self-host SERVICE only, default off (ReadApiDeps.sessionPartialRetryable): the first page of a session-scoped read
 * (GET /v1/sessions/:sessionId/events, GET /v1/events?session=) of a partial session answers 503
 * `backend_unavailable`, reason `session_partial`, `Retry-After: 60`, instead of 501 `session_partial`. The hosted
 * Lambda refuses to start with it on; the Worker refuses every request (lambda.ts sessionPartialRetryable,
 * session-read-worker handle()).
 */
export const READ_API_SESSION_PARTIAL_RETRYABLE = "READ_API_SESSION_PARTIAL_RETRYABLE";

/**
 * READ_API_SESSION_PARTIAL_RETRYABLE, strict (as lambda.ts sessionQueryEnabled): unset, "" or "false" is off, exactly
 * "true" is on, anything else throws, naming the variable and never its value. The entry gate is the caller's.
 */
export function sessionPartialRetryableFlag(raw: string | undefined): boolean {
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw !== "true") throw new Error(`session read api: ${READ_API_SESSION_PARTIAL_RETRYABLE} must be unset, "", "true" or "false"`);
  return true;
}

/** READ_API_BRIEF_SUMMARY_ENABLED / READ_API_LIST_SHORT_SUMMARY_ENABLED / READ_API_LIST_READINESS_ENABLED: only the exact string "true" enables, as coordinationOverlapsEnabled. */
export function exactTrueFlag(raw: unknown): boolean {
  return raw === "true";
}

/** READ_API_OUTPUT_ENABLED / READ_API_DIGEST_ENABLED: only "1" (trimmed) enables, as on the read Worker. */
export function readApiRouteEnabled(raw: unknown): boolean {
  return typeof raw === "string" && raw.trim() === "1";
}
