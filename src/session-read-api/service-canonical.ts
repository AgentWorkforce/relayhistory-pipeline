/**
 * The self-host service's canonical S3 source (SERVICE=session-read-api only;
 * lambda.ts wires it for `entry: "service"`, never for the hosted Lambda). It
 * backs GET /v1/sessions/:id/timing, /turns, /metadata and (with
 * RELAYHISTORY_SELFHOST_READ_API) /catalog, the same way the Worker's
 * timingDeps/turnsDeps do (packages/session-read-worker/src/index.ts):
 * pointers through the read role inside the request's tenant-bound READ ONLY
 * transaction (session-timing-source.ts sqlCanonicalPointers), objects with
 * canonical.ts canonicalSnapshot.
 *
 * The store is the self-host Locker, configured by the names every other
 * self-host service already reads (src/shared/env.ts lockerEnv/vendorBindings):
 *
 *   LOCKER_BUCKET             unset or blank: nothing is wired, and the routes keep their 501
 *                             `*_not_configured` (exactly as before this file)
 *   LOCKER_PROVIDER           s3 (default; MinIO, AWS S3) | gcs (Workload Identity, no HMAC keys)
 *   LOCKER_ENDPOINT           s3: e.g. http://minio:9000 (path-style); gcs: the official API or a loopback emulator
 *   LOCKER_REGION             s3: default us-east-1
 *   LOCKER_ACCESS_KEY_ID / LOCKER_SECRET_ACCESS_KEY   s3 static keys (MinIO), from the secret store; unset:
 *                             EKS Pod Identity (AWS_CONTAINER_CREDENTIALS_FULL_URI + _AUTHORIZATION_TOKEN_FILE),
 *                             short-lived credentials refreshed before expiry
 *   LOCKER_GCS_TOKEN_URI      gcs only: the GKE metadata server (default) or a loopback stand-in
 *   PIPELINE_ORG_FROM_PREFIX  must be exactly "true" (startup refuses otherwise): see below
 *   PIPELINE_KEY_PREFIX       the base of the org-derived key layout the ingest writes
 *   READ_API_TIMING_ASSUME_NO_TURNS                   as on the Worker (exactly "true")
 *   READ_API_TURNS_ASSUME_NO_LEGACY                   exactly "true": this deployment never had a Neon source, so a
 *                             scope with no legacy export marker serves its canonical turns instead of 501 turns_unknown
 *   READ_API_CANONICAL_PIN_LAGGING_POINTER            exactly "true" (default off; read only here, so never on the
 *                             hosted Lambda): a manifest that advanced past its sessions.session_index pointer row
 *                             (async admission CAS'd revision N+k after the reconcile poll indexed N) is served
 *                             pinned to the indexed generation N (canonical.ts pinnedToPointer: strict chunk-prefix
 *                             hash-chain match, manifest revision > pointer revision >= 1) instead of 501
 *                             `*_canonical_manifest_mismatch`; the newer chunks appear after the next reconcile.
 *                             Unset: any manifest/pointer difference is refused, exactly as before.
 *   READ_API_CANONICAL_SCOPE_BYTES                    raw-byte budget of one canonical snapshot (sum over the
 *                             session's chunks of max(bytes, stored_raw_bytes)), a plain positive integer or an
 *                             integer with a MiB / GiB suffix; default 64 MiB for this self-host SERVICE (mirrors the search
 *                             indexer's maxScopeBytes), accepted 16 MiB..2 GiB; 16 MiB remains the hosted Worker/Lambda
 *                             budget, which this knob never touches. A session past
 *                             the budget is 501 `*_canonical_canonical_projection_incomplete` (reason `scope_too_large`). Read only here,
 *                             so never on the hosted Lambda; artifact and producer reads keep their own budgets.
 *   READ_API_CANONICAL_SCOPE_EVENTS                   record budget of the same snapshot, a plain positive integer;
 *                             default 100000, accepted 100000..4000000. For both: unset or empty is the default
 *                             (byte-identical); malformed, below the default or above the bound fails startup,
 *                             naming the variable (never clamped). The 500000 projected-row bound is unchanged.
 *
 * The store is built by src/shared/locker.ts buildLockerStore, the builder
 * every other service uses, so credentials, provider selection and egress rules
 * are the same: static keys or Pod Identity (the credentials token goes only to
 * the trusted agent hosts, loopback or https), GCS Workload Identity with the
 * configured LOCKER_ENDPOINT / LOCKER_GCS_TOKEN_URI (official API or loopback;
 * metadata server or loopback). One credentials provider and one GCS token
 * source serve the whole process (cached, refreshed before expiry); each
 * request gets its own store over a fetch that carries the route's abort
 * signal. LOCKER_BUCKET set but no usable store (no S3 credentials source, an
 * untrusted credentials URI, an unknown provider, GCS behind the S3 client, HMAC
 * keys on GCS): startup fails, naming the variable only. Pointer rows are the authorization boundary: an object is read
 * only through a live pointer the tenant-bound read role returned, and
 * canonicalSnapshot rechecks tenant, bucket, provider, digest and key prefix.
 *
 * Org binding: manifests carry no org, so only the org-derived key layout
 * (`<PIPELINE_KEY_PREFIX>/<org>/...`, checked by prefixAllowed) ties an object to
 * the caller's org. With a static layout prefixAllowed checks only the shared
 * base, and another org's pointer row naming this org's manifest would be
 * served. So, as src/session-pipeline/lambda.ts does, the org-derived layout is
 * required: PIPELINE_ORG_FROM_PREFIX must be exactly "true" whenever
 * LOCKER_BUCKET is set, and the reader always runs with it on.
 */
import { zstdDecompressSync } from "node:zlib";
import { cachedProvider } from "../../packages/object-store/src/core.ts";
import { CANONICAL_SCOPE_BYTES, CANONICAL_SCOPE_EVENTS, canonicalSnapshot, type CanonicalIdentity } from "./canonical.ts";
import type { TenantRunner } from "./liveness.ts";
import { sqlCanonicalPointers, type CanonicalRows } from "./session-timing-source.ts";
import { sqlSessionHarnesses } from "./session-turns-source.ts";
import type { TimingDeps, TurnsDeps } from "./app.ts";
import type { CatalogDeps } from "./selfhost.ts";

export interface ServiceCanonicalDeps {
  events: import("./canonical-events.ts").CanonicalEventsDeps;
  timing: TimingDeps;
  turns: TurnsDeps;
  catalog: CatalogDeps;
}

/** zstd under the projection's output bound; any failure is the projection's `chunk_decompress_failed` refusal. */
const decompress = (bytes: Uint8Array, max: number): Uint8Array => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max }));

const MiB = 1024 * 1024;
/** READ_API_CANONICAL_SCOPE_BYTES bounds: the canonical.ts default up to the `large` node source budget (node-read-profile.ts). */
export const SCOPE_BYTES_MIN = CANONICAL_SCOPE_BYTES, SCOPE_BYTES_MAX = 2 * 1024 * MiB;
/** READ_API_CANONICAL_SCOPE_EVENTS bounds: the canonical.ts default up to 4,000,000 records. */
/**
 * The self-host default byte budget when READ_API_CANONICAL_SCOPE_BYTES is unset or empty. Mirrors
 * SEARCH_INDEX_DEFAULT_LIMITS.maxScopeBytes (src/session-pipeline/search-index.ts), so a self-host install does
 * not 501 scope_too_large on sessions the indexer already handles. The Worker/Lambda default stays 16 MiB.
 */
export const SELFHOST_CANONICAL_SCOPE_BYTES = 64 * MiB;
export const SCOPE_EVENTS_MIN = CANONICAL_SCOPE_EVENTS, SCOPE_EVENTS_MAX = 4_000_000;

/**
 * The self-host canonical snapshot budget (see the header). scopeBytes is always
 * explicit (the self-host default); scopeEvents appears only when set, so an unset
 * one leaves canonicalSnapshot on its default. Throws at startup on a malformed or out-of-range value, naming the
 * variable and its range (not the value); never clamps.
 */
export function canonicalScopeBudget(env: NodeJS.ProcessEnv): { scopeBytes: number; scopeEvents?: number } {
  const parse = (name: string, raw: string | undefined, suffixes: boolean, min: number, max: number, range: string): number | undefined => {
    if (raw === undefined || raw === "") return undefined;
    const m = (suffixes ? /^([1-9][0-9]{0,12})(MiB|GiB)?$/u : /^([1-9][0-9]{0,12})()$/u).exec(raw);
    const value = m ? Number(m[1]) * (m[2] === "GiB" ? 1024 * MiB : m[2] === "MiB" ? MiB : 1) : NaN;
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new Error(`session read api: ${name} must be ${suffixes ? "a whole number of bytes (optional MiB or GiB suffix)" : "a whole number of records"} in ${range}`);
    return value;
  };
  const scopeBytes = parse("READ_API_CANONICAL_SCOPE_BYTES", env.READ_API_CANONICAL_SCOPE_BYTES, true, SCOPE_BYTES_MIN, SCOPE_BYTES_MAX, "16MiB..2GiB") ?? SELFHOST_CANONICAL_SCOPE_BYTES;
  const scopeEvents = parse("READ_API_CANONICAL_SCOPE_EVENTS", env.READ_API_CANONICAL_SCOPE_EVENTS, false, SCOPE_EVENTS_MIN, SCOPE_EVENTS_MAX, "100000..4000000");
  return { scopeBytes, ...(scopeEvents !== undefined ? { scopeEvents } : {}) };
}

/**
 * The canonical source for the service, or undefined when LOCKER_BUCKET is unset
 * (the routes then answer 501 `*_not_configured`, unchanged). Throws at startup
 * on a selection that names a bucket but cannot build a store.
 */
export async function serviceCanonicalDeps(
  env: NodeJS.ProcessEnv,
  run: TenantRunner,
  fetcher: typeof fetch = (input, init) => fetch(input, init),
): Promise<ServiceCanonicalDeps | undefined> {
  if (!env.LOCKER_BUCKET?.trim()) return undefined;
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true")
    throw new Error("session read api: LOCKER_BUCKET needs PIPELINE_ORG_FROM_PREFIX=true (the org-prefix layout binds each object to its org)");
  // Loaded only here, so the hosted Lambda (which never calls this) never evaluates the Locker client.
  const { buildLockerStore, credentialSource, gcsMetadataToken, lockerCredentials, trustedCredentialsUri } = await import("../shared/locker.ts");
  const { lockerProvider } = await import("../shared/locker-selection.ts");
  // Startup validation: provider selection and endpoint/token-URI trust (assertLockerSelection), and
  // for S3 a credentials source (static keys, or Pod Identity whose token goes only to a trusted host).
  buildLockerStore(env);
  const gcs = lockerProvider(env) === "gcs";
  if (!gcs && credentialSource(env) === "pod-identity" && !trustedCredentialsUri(env.AWS_CONTAINER_CREDENTIALS_FULL_URI!))
    throw new Error("session read api: AWS_CONTAINER_CREDENTIALS_FULL_URI is not a trusted credentials endpoint");
  // One process-wide credentials provider (or GCS token source), cached and refreshed before expiry.
  const credentials = gcs ? undefined : cachedProvider(lockerCredentials(env));
  const gcsToken = gcs ? cachedProvider(() => gcsMetadataToken(env)) : undefined;
  const prefix = { PIPELINE_KEY_PREFIX: env.PIPELINE_KEY_PREFIX || undefined, PIPELINE_ORG_FROM_PREFIX: "true" };
  const pointers = sqlCanonicalPointers(run);
  // Self-host only, default off: see READ_API_CANONICAL_PIN_LAGGING_POINTER in the header.
  const pinLaggingPointer = env.READ_API_CANONICAL_PIN_LAGGING_POINTER?.trim() === "true";
  // Self-host only, bytes default 64 MiB, events default unchanged: see READ_API_CANONICAL_SCOPE_BYTES / _EVENTS in the header.
  const scope = canonicalScopeBudget(env);
  async function source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows & { generation: string; exists: boolean }> {
    // Every object read of this request is aborted at the route's deadline.
    const bounded: typeof fetch = (input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal });
    const raw = buildLockerStore(env, { fetch: bounded, ...(credentials ? { credentials } : {}), ...(gcsToken ? { gcsToken } : {}) });
    const snapshot = await canonicalSnapshot(
      { store: raw.store, provider: raw.provider, bucket: raw.bucket, prefix, pointers: pointers.pointers, decompress, ...(pinLaggingPointer ? { pinLaggingPointer } : {}) },
      identity,
      scope,
    );
    return { rows: snapshot.rows, legacyCoverage: snapshot.legacyCoverage, generation: snapshot.generation, exists: snapshot.exists };
  }
  return {
    events: { source },
    timing: { state: pointers.state, source, assumeNoTurns: env.READ_API_TIMING_ASSUME_NO_TURNS?.trim() === "true" },
    turns: { harnesses: sqlSessionHarnesses(run), source, assumeNoLegacy: env.READ_API_TURNS_ASSUME_NO_LEGACY === "true" },
    catalog: { state: pointers.state, source },
  };
}
