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
 *   LOCKER_ACCESS_KEY_ID / LOCKER_SECRET_ACCESS_KEY   s3 only, from the secret store
 *   LOCKER_GCS_TOKEN_URI      gcs only: the GKE metadata server (default) or a loopback stand-in
 *   PIPELINE_ORG_FROM_PREFIX  must be exactly "true" (startup refuses otherwise): see below
 *   PIPELINE_KEY_PREFIX       the base of the org-derived key layout the ingest writes
 *   READ_API_TIMING_ASSUME_NO_TURNS                   as on the Worker (exactly "true")
 *
 * LOCKER_BUCKET set but no usable store (an S3 selection without keys, an
 * unknown provider, GCS behind the S3 client): startup fails, naming the
 * variable only. Pointer rows are the authorization boundary: an object is read
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
import { rawStoreFromEnv } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { assertLockerSelection, lockerProvider } from "../shared/locker-selection.ts";
import { canonicalSnapshot, CanonicalReadError, type CanonicalIdentity } from "./canonical.ts";
import type { TenantRunner } from "./liveness.ts";
import { sqlCanonicalPointers, type CanonicalRows } from "./session-timing-source.ts";
import { sqlSessionHarnesses } from "./session-turns-source.ts";
import type { TimingDeps, TurnsDeps } from "./app.ts";
import type { CatalogDeps } from "./selfhost.ts";

export interface ServiceCanonicalDeps {
  timing: TimingDeps;
  turns: TurnsDeps;
  catalog: CatalogDeps;
}

type RawEnv = Parameters<typeof rawStoreFromEnv>[0];

/** LOCKER_* onto the raw-store names, exactly as vendorBindings maps them for the other services. */
function lockerRawEnv(env: NodeJS.ProcessEnv): RawEnv {
  assertLockerSelection(env);
  const provider = lockerProvider(env);
  const bucket = env.LOCKER_BUCKET?.trim();
  return {
    PIPELINE_RAW_PROVIDER: provider,
    PIPELINE_RAW_S3_BUCKET: bucket,
    ...(provider === "s3"
      ? {
          PIPELINE_RAW_S3_ENDPOINT: env.LOCKER_ENDPOINT || undefined,
          PIPELINE_RAW_S3_REGION: env.LOCKER_REGION || "us-east-1",
          PIPELINE_RAW_S3_ACCESS_KEY_ID: env.LOCKER_ACCESS_KEY_ID || undefined,
          PIPELINE_RAW_S3_SECRET_ACCESS_KEY: env.LOCKER_SECRET_ACCESS_KEY || undefined,
        }
      : {
          // GCS: the configured API endpoint (e.g. a loopback emulator) and token source.
          // Unmapped, the adapter falls back to storage.googleapis.com and 169.254.169.254.
          PIPELINE_RAW_S3_ENDPOINT: env.LOCKER_ENDPOINT || undefined,
          PIPELINE_RAW_GCS_TOKEN_URI: env.LOCKER_GCS_TOKEN_URI?.trim() || undefined,
        }),
  } as RawEnv;
}

/** zstd under the projection's output bound; any failure is the projection's `chunk_decompress_failed` refusal. */
const decompress = (bytes: Uint8Array, max: number): Uint8Array => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max }));

/**
 * The canonical source for the service, or undefined when LOCKER_BUCKET is unset
 * (the routes then answer 501 `*_not_configured`, unchanged). Throws at startup
 * on a selection that names a bucket but cannot build a store.
 */
export function serviceCanonicalDeps(
  env: NodeJS.ProcessEnv,
  run: TenantRunner,
  fetcher: typeof fetch = (input, init) => fetch(input, init),
): ServiceCanonicalDeps | undefined {
  if (!env.LOCKER_BUCKET?.trim()) return undefined;
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true")
    throw new Error("session read api: LOCKER_BUCKET needs PIPELINE_ORG_FROM_PREFIX=true (the org-prefix layout binds each object to its org)");
  const rawEnv = lockerRawEnv(env);
  if (!rawStoreFromEnv(rawEnv, fetcher))
    throw new Error("session read api: LOCKER_BUCKET is set but LOCKER_* does not configure a readable store");
  const prefix = { PIPELINE_KEY_PREFIX: env.PIPELINE_KEY_PREFIX || undefined, PIPELINE_ORG_FROM_PREFIX: "true" };
  const pointers = sqlCanonicalPointers(run);
  async function source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows> {
    // Every object read of this request is aborted at the route's deadline.
    const bounded: typeof fetch = (input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal });
    const raw = rawStoreFromEnv(rawEnv, bounded);
    if (!raw) throw new CanonicalReadError("not_configured");
    const snapshot = await canonicalSnapshot(
      { store: raw.store, provider: raw.provider, bucket: raw.bucket, prefix, pointers: pointers.pointers, decompress },
      identity,
    );
    return { rows: snapshot.rows, legacyCoverage: snapshot.legacyCoverage };
  }
  return {
    timing: { state: pointers.state, source, assumeNoTurns: env.READ_API_TIMING_ASSUME_NO_TURNS?.trim() === "true" },
    turns: { harnesses: sqlSessionHarnesses(run), source },
    catalog: { state: pointers.state, source },
  };
}
