/**
 * Hosted Lambda fallback for PARTIAL exact-session replay (dormant, default off).
 *
 * GET /v1/sessions/:sessionId/events answers a session whose search state is partial
 * (`needs_full = 1`, page.ts partialSessions) with 501 `session_partial`, and Cloud then reads
 * the session from relayhistory. With READ_API_PARTIAL_CANONICAL_EVENTS=true on the hosted
 * Lambda, an allowlisted workspace's FIRST page of such a read (and the canonical cursor pages
 * that follow it) is served instead from the session's verified canonical objects, through the
 * existing canonical-events.ts canonicalEventPage over canonical.ts canonicalSnapshot, exactly as
 * the self-host service's service-canonical.ts reads them. Nothing else changes: the history
 * floor, a tombstone's empty page, the public response body and every query filter are those of
 * the route as without the flag (app.ts ReadApiDeps.canonicalEventsPartial).
 *
 *   READ_API_PARTIAL_CANONICAL_EVENTS       unset, "" or "false": off, nothing below is read and nothing is
 *                                            built (byte-identical). Exactly "true": on. Anything else fails
 *                                            startup, naming the variable, never its value. The hosted Lambda
 *                                            only: the self-host service refuses it (it has its own canonical
 *                                            path, service-canonical.ts), the read Worker never reads it.
 *   READ_API_PARTIAL_CANONICAL_WORKSPACES   parseDirectoryWorkspaces syntax ("*" or a comma list); unset or
 *                                            empty = no workspace (on but serving nothing: the canary step).
 *   READ_API_PARTIAL_CANONICAL_BUCKET       the canonical transcript bucket (an S3 bucket name). Required when on.
 *   READ_API_PARTIAL_CANONICAL_DEADLINE_MS  the whole request's budget for this path, measured from the request's
 *                                            start (pointer, source and partial-state SQL included), so the object
 *                                            read gets only what is left. Whole ms in 1000..3500; unset/empty 3000.
 *                                            Kept below Cloud's 4 s read timeout and far below the 10 s Lambda timeout.
 *   PIPELINE_ORG_FROM_PREFIX                must be exactly "true" when on: manifests carry no org, so only the
 *                                            org-derived key layout binds an object to the caller's org
 *                                            (service-canonical.ts "Org binding").
 *   PIPELINE_KEY_PREFIX                     the base of that layout; required when on.
 *   LOCKER_PROVIDER                         unset or "s3" (this path reads AWS S3 only); anything else refuses.
 *   MESSAGE_ORIGIN_LABELS_ENABLED           as on the Worker's hosted canonical events (writer labels).
 *
 * Authorization: an object is read only through a live pointer the tenant-bound read role returned
 * (session-timing-source.ts sqlCanonicalPointers, the same `sessions.session_index` read the Lambda's
 * liveness already does: no new SQL or grant). canonicalSnapshot rechecks tenant, provider, bucket,
 * content hash, revision, etag, user, machine and the org key prefix, re-reads the pointers after the
 * object IO and refuses a moved generation. Credentials are the execution role's (lambdaCredentials);
 * each request's store is built over a fetch carrying the route's abort signal. Bounds are the hosted
 * ones (16 MiB raw scope, 100000 records, 64 pointers, 1 MiB manifest), never the self-host knobs.
 *
 * Every refusal is classified (classifyPartialCanonical) so operators can tell them apart in the log
 * (`reason_codes`): missing, forbidden, transient, oversize, invalid. On a FIRST page every class
 * answers the exact 501 `session_partial` the route gives without the flag, so Cloud's relayhistory
 * fallback is unchanged. On a canonical cursor page: transient is a 503 (retry the same cursor),
 * missing or a moved generation is a 409 `cursor_backend_transition` (restart), the rest the 501.
 */
import { zstdDecompressSync } from "node:zlib";
import { MESSAGE_ORIGIN_LABELS_VAR, messageOriginLabelsEnabled } from "../../packages/pipeline/src/lib/pipeline/message-origin.ts";
import { readerMaxRuleset } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { createS3Store } from "../shared/vendor.ts";
import { isManifestRest, parseSessionKey, parseUploadGenerationPrefix } from "../../packages/object-store/src/index.ts";
import { CanonicalReadError, canonicalSnapshot, type CanonicalDeps, type CanonicalIdentity, type CanonicalPointer } from "./canonical.ts";
import { parseDirectoryWorkspaces } from "./config.ts";
import type { TenantRunner } from "./liveness.ts";
import { sqlCanonicalPointers } from "./session-timing-source.ts";
import { sqlSessionSources } from "./session-sources.ts";
import type { PartialCanonicalDeps } from "./partial-canonical-policy.ts";

export { classifyPartialCanonical, PARTIAL_CANONICAL_MIN_READ_MS, type PartialCanonicalClass, type PartialCanonicalDeps } from "./partial-canonical-policy.ts";

export const READ_API_PARTIAL_CANONICAL_EVENTS = "READ_API_PARTIAL_CANONICAL_EVENTS";
export const READ_API_PARTIAL_CANONICAL_WORKSPACES = "READ_API_PARTIAL_CANONICAL_WORKSPACES";
export const READ_API_PARTIAL_CANONICAL_BUCKET = "READ_API_PARTIAL_CANONICAL_BUCKET";
export const READ_API_PARTIAL_CANONICAL_DEADLINE_MS = "READ_API_PARTIAL_CANONICAL_DEADLINE_MS";

export const PARTIAL_CANONICAL_DEADLINE_MS_DEFAULT = 3000;
export const PARTIAL_CANONICAL_DEADLINE_MS_MIN = 1000, PARTIAL_CANONICAL_DEADLINE_MS_MAX = 3500;
/** The exact flag: unset, "" or "false" off; "true" on; anything else throws, naming the variable only. */
export function partialCanonicalFlag(raw: string | undefined): boolean {
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`session read api: ${READ_API_PARTIAL_CANONICAL_EVENTS} must be unset, "", "true" or "false"`);
}

/** READ_API_PARTIAL_CANONICAL_DEADLINE_MS (see the header); never clamped. */
export function partialCanonicalDeadlineMs(raw: string | undefined): number {
  if (raw === undefined || raw === "") return PARTIAL_CANONICAL_DEADLINE_MS_DEFAULT;
  const m = /^([1-9][0-9]{3})$/u.exec(raw);
  const value = m ? Number(m[1]) : NaN;
  if (!(value >= PARTIAL_CANONICAL_DEADLINE_MS_MIN && value <= PARTIAL_CANONICAL_DEADLINE_MS_MAX))
    throw new Error(`session read api: ${READ_API_PARTIAL_CANONICAL_DEADLINE_MS} must be a whole number of milliseconds in ${PARTIAL_CANONICAL_DEADLINE_MS_MIN}..${PARTIAL_CANONICAL_DEADLINE_MS_MAX}`);
  return value;
}

/** An S3 bucket name (3-63 of [a-z0-9.-], starting and ending alphanumeric). */
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
/** A key-prefix base: slash-separated segments of [A-Za-z0-9._-], none empty, "." or "..". */
const KEY_PREFIX = /^(?!.*(?:^|\/)\.{1,2}(?:\/|$))[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*$/u;

export interface PartialCanonicalConfig {
  bucket: string;
  region: string;
  keyPrefix: string;
  workspaces: ReadonlySet<string> | "*";
  deadlineMs: number;
  originLabels: boolean;
  readerMaxRuleset: number | undefined;
}

/**
 * The validated configuration, or undefined when the flag is off (nothing else is read then).
 * Throws at startup, naming the variable only, on the self-host service entry, a missing or
 * malformed bucket, region or prefix, the static key layout, or another provider.
 */
export function partialCanonicalConfig(env: NodeJS.ProcessEnv, entry: "lambda" | "service"): PartialCanonicalConfig | undefined {
  if (!partialCanonicalFlag(env[READ_API_PARTIAL_CANONICAL_EVENTS])) return undefined;
  const refuse = (what: string): never => { throw new Error(`session read api: ${READ_API_PARTIAL_CANONICAL_EVENTS}=true ${what}`); };
  if (entry !== "lambda") refuse("is served only by the hosted Lambda");
  const bucket = env[READ_API_PARTIAL_CANONICAL_BUCKET]?.trim() ?? "";
  if (!BUCKET.test(bucket)) refuse(`needs ${READ_API_PARTIAL_CANONICAL_BUCKET} set to an S3 bucket name`);
  const region = env.AWS_REGION?.trim() ?? "";
  if (!/^[a-z]{2}(?:-[a-z]+)+-[0-9]$/u.test(region)) refuse("needs AWS_REGION");
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true") refuse("needs PIPELINE_ORG_FROM_PREFIX=true (the org-prefix layout binds each object to its org)");
  const keyPrefix = env.PIPELINE_KEY_PREFIX ?? "";
  if (!KEY_PREFIX.test(keyPrefix)) refuse("needs PIPELINE_KEY_PREFIX");
  const provider = env.LOCKER_PROVIDER?.trim() ?? "";
  if (provider !== "" && provider !== "s3") refuse("reads AWS S3 only (LOCKER_PROVIDER unset or s3)");
  return {
    bucket, region, keyPrefix,
    workspaces: parseDirectoryWorkspaces(env[READ_API_PARTIAL_CANONICAL_WORKSPACES]),
    deadlineMs: partialCanonicalDeadlineMs(env[READ_API_PARTIAL_CANONICAL_DEADLINE_MS]),
    originLabels: messageOriginLabelsEnabled(env[MESSAGE_ORIGIN_LABELS_VAR]),
    readerMaxRuleset: readerMaxRuleset(env),
  };
}

/** zstd under the projection's output bound; any failure is the projection's `chunk_decompress_failed`. */
const decompress = (bytes: Uint8Array, max: number): Uint8Array => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max }));

export interface PartialCanonicalPorts {
  /** Test port: the S3 fetch (default global fetch). */
  fetch?: typeof fetch;
  /** Test port: the execution role's credentials (default lambdaCredentials()). */
  credentials?: () => Promise<{ accessKeyId: string; secretAccessKey: string; sessionToken?: string }>;
  /** Test port: the store builder (default createS3Store). */
  makeStore?: typeof createS3Store;
}

/** The fallback's deps for the Lambda, or undefined when off. See partialCanonicalConfig for refusals. */
export function partialCanonicalDeps(env: NodeJS.ProcessEnv, entry: "lambda" | "service", run: TenantRunner, ports: PartialCanonicalPorts = {}): PartialCanonicalDeps | undefined {
  const checked = partialCanonicalConfig(env, entry);
  if (!checked) return undefined;
  const config: PartialCanonicalConfig = checked;
  const fetcher = ports.fetch ?? ((input, init) => fetch(input, init));
  const credentials = ports.credentials ?? (async () => lambdaCredentials());
  const makeStore = ports.makeStore ?? createS3Store;
  const sql = sqlCanonicalPointers(run);
  /**
   * Key scope before any object IO. canonicalSnapshot checks the org prefix only on the manifest it
   * has already fetched, so a pointer row naming a key elsewhere would still be requested. Here every
   * pointer's manifest key must be a manifest under the caller's `<PIPELINE_KEY_PREFIX>/<org>` base
   * (directly, or under that base's upload-generation prefix), in the caller's workspace and harness,
   * in the v1 layout; otherwise the whole read is refused (prefix_mismatch, forbidden) with no GET. Keys
   * that pass sit inside the IAM object pattern "<PIPELINE_KEY_PREFIX>/ * /v1/ *" (spaces added here only).
   */
  const pointers = async (identity: CanonicalIdentity): Promise<CanonicalPointer[]> => {
    const rows = await sql.pointers(identity);
    const base = `${config.keyPrefix}/${identity.orgId}`;
    for (const p of Array.isArray(rows) ? rows : []) {
      const key = typeof p?.manifest_key === "string" ? p.manifest_key : "";
      const at = key.split("/").some((x) => x === "" || x === "." || x === "..") ? undefined : parseSessionKey(key);
      if (!at || !isManifestRest(at.rest) || at.workspaceId !== identity.workspaceId || at.harness !== identity.source ||
          !(at.prefix === base || parseUploadGenerationPrefix(at.prefix)?.base === base))
        throw new CanonicalReadError("prefix_mismatch");
    }
    return rows;
  };
  const prefix = { PIPELINE_KEY_PREFIX: config.keyPrefix, PIPELINE_ORG_FROM_PREFIX: "true" };
  function deps(signal: AbortSignal): CanonicalDeps {
    // Every object read of this request is aborted at the route's deadline.
    const bounded: typeof fetch = (input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal });
    const store = makeStore({ bucket: config.bucket, region: config.region, credentials, fetch: bounded });
    return {
      ...(config.readerMaxRuleset !== undefined ? { readerMaxRuleset: config.readerMaxRuleset } : {}),
      store, provider: "s3", bucket: config.bucket, prefix, pointers, decompress,
    };
  }
  return {
    events: {
      // The hosted bounds: no scope options, so canonicalSnapshot keeps CANONICAL_SCOPE_BYTES / _EVENTS.
      async source(identity: CanonicalIdentity, signal: AbortSignal) {
        const snapshot = await canonicalSnapshot(deps(signal), identity);
        return { rows: snapshot.rows, legacyCoverage: snapshot.legacyCoverage, generation: snapshot.generation, exists: snapshot.exists };
      },
      ...(config.originLabels ? { originLabels: true } : {}),
    },
    workspaces: config.workspaces,
    sources: sqlSessionSources(run),
    deadlineMs: config.deadlineMs,
  };
}
