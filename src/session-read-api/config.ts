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

/** The facts store an Output or digest backend reads: today's ClickHouse facts, or the self-host Postgres facts (migration 045040). */
export type FactsBackendKind = "clickhouse" | "postgres";
export const READ_API_OUTPUT_BACKEND = "READ_API_OUTPUT_BACKEND";
export const READ_API_DIGEST_BACKEND = "READ_API_DIGEST_BACKEND";
export const READ_API_COORDINATION_OVERLAPS_BACKEND = "READ_API_COORDINATION_OVERLAPS_BACKEND";
export const READ_API_COORDINATION_OVERLAPS_ENABLED = "READ_API_COORDINATION_OVERLAPS_ENABLED";
/** Wires `kind=long` on GET /v1/sessions/:sessionId/summary (migration 20261105060000). Off by default. */
export const READ_API_LONG_SUMMARY_ENABLED = "READ_API_LONG_SUMMARY_ENABLED";
/** Bob Costas projects the scrubbed long summary into HoopDb (migration 20261105060000). Off by default. */
export const SESSION_LONG_SUMMARY_PROJECTION_ENABLED = "SESSION_LONG_SUMMARY_PROJECTION_ENABLED";

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

/**
 * A default-off flag whose ONLY on-value is the exact string "true": unset, "", "1", "TRUE",
 * " true" and every non-string are off. Never echoes the value. Used by
 * READ_API_LONG_SUMMARY_ENABLED, SESSION_LONG_SUMMARY_PROJECTION_ENABLED,
 * READ_API_BRIEF_SUMMARY_ENABLED and READ_API_LIST_SHORT_SUMMARY_ENABLED.
 */
export function exactTrueFlag(raw: unknown): boolean {
  return raw === "true";
}

/** Only the exact string "true" exposes GET /v1/coordination/overlaps. */
export function coordinationOverlapsEnabled(raw: unknown): boolean {
  return raw === "true";
}

/** Hosted Lambda entry: wire the brief-summary read (GET /v1/sessions/:id/summary). The service always wires it. */
export const READ_API_BRIEF_SUMMARY_ENABLED = "READ_API_BRIEF_SUMMARY_ENABLED";
/** GET /v1/sessions: serve the opt-in includeShortSummary=recorded list extension. */
export const READ_API_LIST_SHORT_SUMMARY_ENABLED = "READ_API_LIST_SHORT_SUMMARY_ENABLED";

/** READ_API_OUTPUT_ENABLED / READ_API_DIGEST_ENABLED: only "1" (trimmed) enables, as on the read Worker. */
export function readApiRouteEnabled(raw: unknown): boolean {
  return typeof raw === "string" && raw.trim() === "1";
}
