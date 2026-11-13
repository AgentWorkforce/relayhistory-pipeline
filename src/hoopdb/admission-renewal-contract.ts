/**
 * Dormant switch and fixed constants for the buffered-admission renewal operator
 * (src/hoopdb/ingest-admission-renewal-operator.ts, .github/workflows/ingest-admission-renewal.yml).
 *
 * While ADMISSION_RENEWAL_ENABLED is false, `plan` and `renew` refuse with admission_renewal_dormant
 * before any credential is read; `readback` (READ ONLY) is always allowed. Enabling is a separate
 * reviewed one-line PR. Nothing about a specific renewal lives here: the old expiry, row count, org
 * roster digest and descriptor are captured by a `plan` run and carried in its artifact, and the new
 * expiry is fixed by that plan (capturedAt + exactly ADMISSION_RENEWAL_HORIZON_MS), never an input.
 * The generation is likewise never an input: plan discovers the live PASS marker generation, and binds a
 * separate proof that the live migration ledger is a checksum-equal prefix of the dispatched SHA's steps.
 */
export const ADMISSION_RENEWAL_ENABLED: boolean = false;

/** renewTo = plan capturedAt + exactly 30 days (canonical UTC milliseconds). */
export const ADMISSION_RENEWAL_HORIZON_MS = 30 * 24 * 60 * 60 * 1000;
/** A plan older than this (by its run's created_at and by its DB capturedAt) cannot be renewed. */
export const ADMISSION_RENEWAL_PLAN_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export const ADMISSION_RENEWAL_WORKFLOW = ".github/workflows/ingest-admission-renewal.yml";
/** Plan artifact (retention 1 day) and its single file, downloaded by `renew` from the plan run. */
export const ADMISSION_RENEWAL_PLAN_ARTIFACT = "ingest-admission-renewal-plan";
export const ADMISSION_RENEWAL_PLAN_FILE = "release-plan/ingest-admission-renewal-plan.json";
/** Sanitized receipt (retention 30 days). */
export const ADMISSION_RENEWAL_RECEIPT_FILE = "release-receipt/ingest-admission-renewal.json";
