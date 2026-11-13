/**
 * INGEST_HTTP_ADMISSION_STOPPED_ORGS: the per-org stop for the Worker's HTTP upload admission (decision 0005).
 *
 * The Worker admits through the SQL ledger. Before an org's S3 fence-authority marker may be written
 * (ledger-s3.ts FenceAuthorityDoc, `workerHttpAdmission: "off"`), nothing may admit that org's uploads through
 * SQL any more, and that must hold BEFORE the org's revision and ownership backfills run: an SQL admission made
 * after them is absent from the S3 index and owner stubs. The desktop falls back to the Worker POST on any
 * direct-upload credential error, so redirecting uploads to the direct path is not enough on its own.
 *
 * A listed org's every HTTP admission entry (POST /v1/delivery/batches, /v1/turns/batches, /v1/delivery/recovery)
 * answers 503 `delivery_unavailable` with Retry-After, before the body is parsed and before any ledger call: a
 * retryable refusal, so the desktop keeps the batch and retries (later through the direct path). Controls
 * (selection, reset, consent, custody), reads and the onboarding routes are untouched. The admission queue
 * consumer is untouched too: a batch it holds was already ACKed to the desktop, so it must still be admitted.
 * The operator therefore waits for the INGEST_ADMISSION queue to drain before the backfills
 * (docs/selfhost-composition.md, "Stopping the Worker's HTTP admission for an org").
 *
 * Order: stop (this var, deployed) -> admission queue drained -> backfills (#579) -> marker.
 *
 * Value: a comma-separated list of org ids, each `[A-Za-z0-9._-]{1,128}` not starting with a dot (the
 * HISTORY_UPLOAD_DIRECT_ALLOWLIST segment rule). Unset or blank: none (byte-for-byte as before). Anything else
 * throws, which the Worker turns into 503 `not_configured` on every request: a stop that cannot be read is
 * never silently off. Errors and logs carry codes only, never an org id.
 */
import { IngestUnavailable } from "./deliver.ts";

export const HTTP_ADMISSION_STOPPED_ORGS_VAR = "INGEST_HTTP_ADMISSION_STOPPED_ORGS";
/** How long the desktop is asked to wait: the cutover is operator-paced, so retrying sooner only adds load. */
export const HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS = 300;

const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;

/** The stopped orgs, or undefined when none are listed. Throws on any malformed entry. */
export function httpAdmissionStoppedOrgs(raw: string | undefined): ReadonlySet<string> | undefined {
  if (raw === undefined || raw.trim() === "") return undefined;
  const orgs = new Set<string>();
  for (const entry of raw.split(",").map((e) => e.trim())) {
    // An empty entry ("a,,b", a trailing comma) is a typo in a safety list: refuse it rather than guess.
    if (!ORG.test(entry)) throw new Error(`session ingest: ${HTTP_ADMISSION_STOPPED_ORGS_VAR} must be a comma-separated list of org ids`);
    orgs.add(entry);
  }
  return orgs;
}

/** The refusal: a 503 `delivery_unavailable` (retryable), stage `http_admission`, with Retry-After. */
export class HttpAdmissionStopped extends IngestUnavailable {
  readonly retryAfterSeconds = HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS;
  constructor() {
    super("http_admission", "http_admission_stopped");
  }
}
