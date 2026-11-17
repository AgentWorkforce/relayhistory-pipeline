export const EXPECTED_SUPABASE_HOST = "aws-0-us-east-1.pooler.supabase.com";
export const EXPECTED_SUPABASE_USER = "bulls96_read_api.pyamkrhoyszgzirqqffq";
export const EXPECTED_SUPABASE_SCHEME = "postgres";
export const EXPECTED_SUPABASE_PORT = 5432;
export const EXPECTED_SUPABASE_DATABASE = "postgres";
/**
 * The analysis admission principal's pooler user (ANALYSIS_ADMIT binding): the same Supabase
 * project as the read role, role bulls96_analysis_admit. Derived, so the project ref lives in
 * one place only.
 */
export const EXPECTED_ANALYSIS_ADMIT_USER = EXPECTED_SUPABASE_USER.replace(/^bulls96_read_api\./, "bulls96_analysis_admit.");

/** Which Worker binding's Hyperdrive config is being checked: SUPABASE (the read role) or ANALYSIS_ADMIT. */
export type HyperdriveRole = "read_api" | "analysis_admit";

export function parseHyperdriveRole(raw: string | undefined): HyperdriveRole {
  const value = (raw ?? "").trim() || "read_api";
  if (value !== "read_api" && value !== "analysis_admit") throw new Error('HYPERDRIVE_ROLE must be "read_api" or "analysis_admit"');
  return value;
}

type JsonRecord = Record<string, unknown>;

function record(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

export interface HyperdriveCheckReport {
  ok: boolean;
  checks: Record<string, boolean>;
}

/**
 * Validate only the safety properties the Worker relies on. The report contains
 * field names and booleans, never the live config's values or credentials.
 * Both roles need caching disabled (the admission's writes and its read-your-write
 * answers must never come from a cache), verify-full and the pinned origin; only
 * the origin user differs.
 */
export function checkHyperdriveConfig(body: unknown, expected: { id: string; caId: string; role?: HyperdriveRole }): HyperdriveCheckReport {
  const user = (expected.role ?? "read_api") === "analysis_admit" ? EXPECTED_ANALYSIS_ADMIT_USER : EXPECTED_SUPABASE_USER;
  const envelope = record(body);
  const result = record(envelope.result);
  const caching = record(result.caching);
  const mtls = record(result.mtls);
  const origin = record(result.origin);
  const checks = {
    "api.success": envelope.success === true,
    "result.id": result.id === expected.id,
    "caching.disabled": caching.disabled === true,
    "mtls.sslmode": mtls.sslmode === "verify-full",
    "mtls.ca_certificate_id": mtls.ca_certificate_id === expected.caId,
    "origin.user": origin.user === user,
    "origin.host": origin.host === EXPECTED_SUPABASE_HOST,
    "origin.scheme": origin.scheme === EXPECTED_SUPABASE_SCHEME,
    "origin.port": origin.port === EXPECTED_SUPABASE_PORT,
    "origin.database": origin.database === EXPECTED_SUPABASE_DATABASE,
  };
  return { ok: Object.values(checks).every(Boolean), checks };
}
