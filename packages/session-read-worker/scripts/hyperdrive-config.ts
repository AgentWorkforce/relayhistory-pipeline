export const EXPECTED_SUPABASE_HOST = "aws-0-us-east-1.pooler.supabase.com";
export const EXPECTED_SUPABASE_USER = "bulls96_read_api";

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
 */
export function checkHyperdriveConfig(body: unknown, expected: { id: string; caId: string }): HyperdriveCheckReport {
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
    "origin.user": origin.user === EXPECTED_SUPABASE_USER,
    "origin.host": origin.host === EXPECTED_SUPABASE_HOST,
  };
  return { ok: Object.values(checks).every(Boolean), checks };
}
