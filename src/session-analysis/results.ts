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
import { objectKey, readAll, type ObjectStore } from "../../packages/object-store/src/index.ts";
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

/** Create-only put with a hard deadline (aborted at `timeoutMs`). */
export async function boundedPut(
  store: ObjectStore,
  key: string,
  bytes: Uint8Array,
  digest: string,
  timeoutMs = ANALYSIS_PUT_TIMEOUT_MS,
): Promise<AnalysisPutAck> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const ack = await Promise.race([
      store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: digest, contentType: "application/json", signal: controller.signal }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new AnalysisResultError("analysis_put_timeout", true));
          controller.abort();
        }, timeoutMs);
      }),
    ]);
    if (ack.size !== bytes.length || ack.contentSha256 !== digest) throw new AnalysisResultError("analysis_write_ack_invalid", true);
    return { created: ack.created, ...(ack.created && ack.version.version ? { versionId: ack.version.version } : {}) };
  } finally {
    clearTimeout(timer);
  }
}

export async function putAnalysisResult(
  store: ObjectStore,
  key: string,
  bytes: Uint8Array,
  digest: string,
  timeoutMs = ANALYSIS_PUT_TIMEOUT_MS,
): Promise<void> {
  await boundedPut(store, key, bytes, digest, timeoutMs);
  const read = await readAll(await store.get(objectKey(key)));
  if (read.length !== bytes.length || sha256(read) !== digest)
    throw new AnalysisResultError("analysis_write_verification_failed", true);
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
