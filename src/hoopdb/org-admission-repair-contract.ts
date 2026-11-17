/**
 * Dormant switch and fixed constants for the org admission repair operator
 * (src/hoopdb/ingest-org-admission-repair-operator.ts, .github/workflows/ingest-org-admission-repair.yml).
 *
 * While ORG_ADMISSION_REPAIR_ENABLED is false, `plan` and `admit` refuse with org_admission_repair_dormant
 * before any credential is read; `readback` (READ ONLY) is always allowed. Enabling is a separate reviewed
 * one-line change. Nothing about a specific repair lives here: the org list is a workflow input only (never
 * committed), and the policy, generation, scan and ledger proof are captured by a `plan` run and carried in
 * its artifact.
 */
export const ORG_ADMISSION_REPAIR_ENABLED: boolean = true;

/**
 * The only ingest_/upload_ tables whose rows for an org may be adopted by a repair admission. Exactly the
 * 040000 per-session change counter written by the ingest_seed_source_changed trigger on
 * sessions.session_index (one row per session, no target authority). Any other table with rows for the org
 * refuses (org_admission_repair_unbound_authority), so real foreign authority is never adopted.
 */
export const ORG_ADMISSION_REPAIR_ALLOWLIST: readonly string[] = Object.freeze(["ingest_seed_source_epochs"]);

/** At most this many orgs per dispatch (deduplicated). */
export const ORG_ADMISSION_REPAIR_MAX_ORGS = 50;
/** A plan older than this (by its run's created_at and by its DB capturedAt) cannot be admitted. */
export const ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export const ORG_ADMISSION_REPAIR_WORKFLOW = ".github/workflows/ingest-org-admission-repair.yml";
/** Plan artifact (retention 1 day) and its single file, downloaded by `admit` from the plan run. */
export const ORG_ADMISSION_REPAIR_PLAN_ARTIFACT = "ingest-org-admission-repair-plan";
export const ORG_ADMISSION_REPAIR_PLAN_FILE = "release-plan/ingest-org-admission-repair-plan.json";
/** Sanitized receipt (retention 30 days). */
export const ORG_ADMISSION_REPAIR_RECEIPT_FILE = "release-receipt/ingest-org-admission-repair.json";
