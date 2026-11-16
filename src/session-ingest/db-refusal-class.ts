/**
 * Closed classification of a ledger SQLSTATE 55000 (object_not_in_prerequisite_state) refusal, for the
 * route line's `db_refusal`. Diagnostic only: it never changes status, retry or generation selection.
 *
 * The class is derived from the Postgres error MESSAGE by matching the exact texts the SQL functions in
 * supabase/migrations raise with ERRCODE '55000' (the database lower-cased and bounded in RAM by
 * db-error-class.ts `dbErrorSnapshot`). Only the closed class leaves this module; the message itself is
 * never returned, logged or interpolated. A 55000 whose message matches none of the known prefixes is
 * `other`.
 *
 * The generation classes (GENERATION_REFUSALS) are the ones the buffered ledger's bounded generation
 * overlap retries at the other accepted generation (ledger-sql.ts `admitted`); any other class is a real
 * refusal that must not be masked by a later attempt's generation refusal.
 */
export const DB_REFUSALS = Object.freeze([
  "generation_not_ready",
  "policy_expired",
  "policy_unavailable",
  "org_admission_closed",
  "org_prior_authority_unbound",
  "user_privacy_authority",
  "ack_seal",
  "native_custody_identity_changed",
  "source_row_privacy_fenced",
  "custody_privacy_inconsistent",
  "user_seal_missing",
  "receipt_proof_missing",
  "publication_transition",
  "other",
] as const);
export type DbRefusal = (typeof DB_REFUSALS)[number];

/** The refusals that depend on which accepted generation the call was offered (retried at the other one). */
// Not policy_expired: ingest_lazy_begin checks the generation ("current custody policy unavailable") before
// expiry ("policy expired during operation"), so an expiry is raised only at the right generation: a real refusal.
export const GENERATION_REFUSALS: ReadonlySet<DbRefusal> = new Set<DbRefusal>(["generation_not_ready", "policy_unavailable"]);

/** Exact message prefixes (lower-cased) from supabase/migrations, each with its class. First match wins. */
export const DB_REFUSAL_PREFIXES: readonly (readonly [string, DbRefusal])[] = [
  ["session ingest: generation not ready", "generation_not_ready"],
  ["ingest seed: generation not ready", "generation_not_ready"],
  ["session ingest: policy expired during operation", "policy_expired"],
  ["session ingest: current custody policy unavailable", "policy_unavailable"],
  ["session ingest: retained org admission closed or conflicting", "org_admission_closed"],
  ["session ingest: buffered admission closed", "org_admission_closed"],
  ["session ingest: unbound prior org authority requires recovery", "org_prior_authority_unbound"],
  ["session ingest: existing user privacy authority changed", "user_privacy_authority"],
  ["session ingest: unsealed user authority cannot be adopted", "user_privacy_authority"],
  ["session ingest: prior user authority requires reviewed recovery", "user_privacy_authority"],
  ["session ingest: complete bounded cold privacy required", "user_privacy_authority"],
  ["session ingest: actual privacy seal required for ack", "ack_seal"],
  ["session ingest: real user seal required before any receipt", "ack_seal"],
  ["session ingest: native custody identity changed", "native_custody_identity_changed"],
  ["session ingest: source current row is already privacy fenced", "source_row_privacy_fenced"],
  ["session ingest: inconsistent current custody privacy", "custody_privacy_inconsistent"],
  ["session ingest: user privacy seal missing", "user_seal_missing"],
  ["session ingest: buffered receipt proof missing", "receipt_proof_missing"],
  ["session ingest: explicit publication transition required", "publication_transition"],
];

/** The class of a 55000 refusal from its lower-cased message (absent: `other`). Never throws. */
export function dbRefusalFromMessage(lowerMessage: string | undefined): DbRefusal {
  if (typeof lowerMessage !== "string") return "other";
  for (const [prefix, refusal] of DB_REFUSAL_PREFIXES) if (lowerMessage.startsWith(prefix)) return refusal;
  return "other";
}

export function isGenerationRefusal(refusal: DbRefusal | undefined): boolean {
  return refusal !== undefined && GENERATION_REFUSALS.has(refusal);
}
