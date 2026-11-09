/**
 * Analysis results in object storage. relayhistory stored the result jsonb on its job
 * row; model text may not live in Postgres here, so the job row keeps only this object's
 * key, SHA-256 and size, and a reader verifies all three before it serves the result.
 *
 * Key layout: under the session's own manifest directory (the same anchor the #245
 * analysis graphs use), so the session's erasure and any later per-plan TTL over the
 * org/date-prefixed session keys cover the result too:
 *
 *   <manifest dir>/normalized/session-analysis-v2/<job id>/<sha256>.json
 *
 * Bucket default encryption (SSE-KMS) applies; nothing here sets a weaker one.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { boundedPut as sharedBoundedPut, PutTimeout } from "../migration/neon-s3-erasure/write-obligation.ts";
import type { SessionAnalysisResult } from "./engine/session-analysis-result.ts";
import type { StoredAnalysisResult } from "./processor.ts";

/** relayhistory's largest live envelope is 684,738 B; refuse anything far beyond it. */
export const ANALYSIS_RESULT_MAX_BYTES = 8 * 1024 * 1024;

export class AnalysisResultError extends Error {
  constructor(readonly code: string, readonly retryable = false) {
    super(code);
    this.name = "AnalysisResultError";
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/** The result directory of one session, from its anchor manifest key. */
export function analysisResultPrefix(anchorManifestKey: string): string {
  if (!anchorManifestKey.endsWith("/manifest.json"))
    throw new AnalysisResultError("analysis_anchor_invalid");
  return `${anchorManifestKey.slice(0, -"manifest.json".length)}normalized/session-analysis-v2/`;
}

export function analysisResultKey(prefix: string, jobId: string, digest: string): string {
  if (!UUID.test(jobId) || !/^[0-9a-f]{64}$/u.test(digest))
    throw new AnalysisResultError("analysis_result_key_invalid");
  return `${prefix}${jobId}/${digest}.json`;
}

/** The result's bytes and digest; the key is planned from these before any write. */
export function encodeAnalysisResult(result: unknown): { bytes: Uint8Array; sha256: string } {
  const bytes = new TextEncoder().encode(JSON.stringify(result));
  if (bytes.length > ANALYSIS_RESULT_MAX_BYTES) throw new AnalysisResultError("analysis_result_bound");
  return { bytes, sha256: sha256(bytes) };
}

/** Create-only put of planned bytes at a reserved key, then a byte-exact read-back. */
/**
 * Hard bound on one result PUT, well inside every lease that fences it (the worker's 600 s job
 * lease, the backfill's 600 s ticket lease) and the sweeper's grace (TICKET_GRACE_SECONDS): a
 * PUT still in flight at this deadline is aborted (S3: the HTTP request) and rejected, so no
 * write can outlive the lease it was started under by more than the commit of an
 * already-received body.
 */
export const ANALYSIS_PUT_TIMEOUT_MS = 120_000;

/** What a bounded put created: `versionId` is the exact version (versioned buckets). */
export interface AnalysisPutAck {
  created: boolean;
  versionId?: string;
}

/** The request's own evidence hooks (write-obligation v3): unknown stays dispatched; refused is terminal. */
export interface PutEvidenceHooks {
  onUnknown?: () => Promise<void>;
  onRefused?: () => Promise<void>;
}

/**
 * Create-only put through THE shared bounded PUT (write-obligation.ts boundedPut: an enforced
 * wall deadline, late results contained). A definite provider refusal (precondition_failed /
 * already_exists) calls onRefused; a timeout, abort, error or lost response calls onUnknown and
 * leaves the invocation dispatched. The deadline surfaces as analysis_put_timeout.
 */
export async function boundedPut(
  store: ObjectStore,
  key: string,
  bytes: Uint8Array,
  digest: string,
  timeoutMs = ANALYSIS_PUT_TIMEOUT_MS,
  hooks: PutEvidenceHooks = {},
): Promise<AnalysisPutAck> {
  try {
    const ack = await sharedBoundedPut(
      (signal) => store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: digest, contentType: "application/json", signal }),
      {
        onUnknown: hooks.onUnknown ?? (async () => {}),
        ...(hooks.onRefused ? { onRefused: hooks.onRefused } : {}),
        isRefusal: (e) => isObjectStoreError(e, "precondition_failed") || isObjectStoreError(e, "already_exists"),
      },
      timeoutMs,
    );
    if (ack.size !== bytes.length || ack.contentSha256 !== digest) throw new AnalysisResultError("analysis_write_ack_invalid", true);
    // Identical bytes already current (create-only): the provider refused this request; no write.
    if (!ack.created && hooks.onRefused) await hooks.onRefused();
    // Its OWN ack: the exact version it created. An unversioned store (S3 suspended versioning,
    // the fs store) names its single version "null", exactly as the strict eraser lists it.
    return { created: ack.created, ...(ack.created ? { versionId: ack.version.version ?? "null" } : {}) };
  } catch (error) {
    if (error instanceof PutTimeout) throw new AnalysisResultError("analysis_put_timeout", true);
    throw error;
  }
}

export async function putAnalysisResult(
  store: ObjectStore,
  key: string,
  bytes: Uint8Array,
  digest: string,
  timeoutMs = ANALYSIS_PUT_TIMEOUT_MS,
  hooks: PutEvidenceHooks = {},
): Promise<{ versionId?: string; ackedVersionId?: string }> {
  const ack = await boundedPut(store, key, bytes, digest, timeoutMs, hooks);
  const got = await store.getObject(objectKey(key));
  const read = await readAll(got.body);
  if (read.length !== bytes.length || sha256(read) !== digest)
    throw new AnalysisResultError("analysis_write_verification_failed", true);
  // The version the publication owns: the one this PUT created (its OWN ack, the only request
  // evidence), else the current one it found identical, as the verified read reports it.
  const versionId = ack.versionId ?? got.meta.version.version ?? "null";
  return { ...(versionId ? { versionId } : {}), ...(ack.versionId ? { ackedVersionId: ack.versionId } : {}) };
}

/** Create-only write, then a read-back that must match byte for byte. */
export async function writeAnalysisResult(
  store: ObjectStore,
  prefix: string,
  jobId: string,
  result: SessionAnalysisResult,
): Promise<StoredAnalysisResult> {
  const bytes = new TextEncoder().encode(JSON.stringify(result));
  if (bytes.length > ANALYSIS_RESULT_MAX_BYTES) throw new AnalysisResultError("analysis_result_bound");
  const digest = sha256(bytes);
  const key = analysisResultKey(prefix, jobId, digest);
  const ack = await store.put(objectKey(key), bytes, {
    ifNoneMatch: true,
    contentSha256: digest,
    contentType: "application/json",
  });
  if (ack.size !== bytes.length || ack.contentSha256 !== digest)
    throw new AnalysisResultError("analysis_write_ack_invalid", true);
  const read = await readAll(await store.get(objectKey(key)));
  if (read.length !== bytes.length || sha256(read) !== digest)
    throw new AnalysisResultError("analysis_write_verification_failed", true);
  return { key, sha256: digest, bytes: bytes.length };
}

/** Read a result the job row points at; refuses a missing, resized or altered object. */
export async function readAnalysisResult(
  store: ObjectStore,
  stored: StoredAnalysisResult,
): Promise<SessionAnalysisResult> {
  if (stored.bytes > ANALYSIS_RESULT_MAX_BYTES) throw new AnalysisResultError("analysis_result_bound");
  let read: Uint8Array;
  try {
    read = await readAll(await store.get(objectKey(stored.key)));
  } catch (error) {
    if ((error as { code?: string }).code === "not_found")
      throw new AnalysisResultError("analysis_result_missing");
    throw error;
  }
  if (read.length !== stored.bytes || sha256(read) !== stored.sha256)
    throw new AnalysisResultError("analysis_result_mismatch");
  return JSON.parse(new TextDecoder().decode(read)) as SessionAnalysisResult;
}
