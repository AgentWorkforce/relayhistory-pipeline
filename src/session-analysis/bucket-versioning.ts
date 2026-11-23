/**
 * The session-analysis runner's startup versioning check (runner.ts). The orphan-result sweeper erases
 * every version of a ticketed result key by exact version id (runtime-deps.ts, eraser.ts), so a bucket
 * that is not versioned (never enabled, or Suspended) is refused before anything listens.
 *
 * One signed, read-only GetBucketVersioning (`GET /<bucket>?versioning`, path-style on a custom endpoint,
 * virtual-hosted on AWS S3, exactly the store's and eraser's addressing). It is used instead of a
 * create/list-versions probe because:
 *   - it is read-only: a probe would write an object and then need DeleteObjectVersion to remove it,
 *     leaving a delete marker or a version behind if the pod dies in between;
 *   - it tells Enabled from Suspended: on a Suspended bucket a new put still answers a version id
 *     ("null"), and older versions still list, so a probe has to special-case a value that S3-compatible
 *     stores spell differently;
 *   - AWS S3 and MinIO (the bundled store) both implement it with the same document.
 * A store without the API answers an error status, which refuses as unverified (the operator learns the
 * store cannot prove the property the sweeper needs, rather than the sweeper failing later).
 *
 * Verdicts: "Enabled" passes. A readable document whose Status is absent or not Enabled is the refusal
 * `bucket_not_versioned`. A 4xx, a 501 or an unreadable document is the refusal `bucket_versioning_unverified`.
 * Any other 5xx or a network failure throws a plain coded error (exit 1: transient, the pod restarts and retries).
 * Errors carry codes only, never the bucket, endpoint or a credential.
 */
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import { signRequest, type AwsCredentials } from "../../packages/object-store/src/sigv4.ts";

export interface BucketVersioningCheck {
  bucket: string;
  region: string;
  /** Bare origin of an S3-compatible store (path-style); unset is AWS S3 in `region`. */
  endpoint?: string;
  credentials: AwsCredentials;
  fetch?: typeof fetch;
  timeoutMs?: number;
}

export type BucketVersioningVerdict = "Enabled" | "not_versioned" | "unverified";

const MAX_BYTES = 64 * 1024;

export function bucketVersioningUrl(cfg: Pick<BucketVersioningCheck, "bucket" | "region" | "endpoint">): URL {
  const url = cfg.endpoint
    ? new URL(`${cfg.endpoint.replace(/\/+$/, "")}/${cfg.bucket}`)
    : new URL(`https://${cfg.bucket}.s3.${cfg.region}.amazonaws.com/`);
  url.searchParams.set("versioning", "");
  return url;
}

/** The Status of a GetBucketVersioning document, "" when absent (never enabled), or undefined when unreadable. */
export function versioningStatus(xml: string): string | undefined {
  const doc = /^\s*(?:<\?xml[^?]*\?>\s*)?<VersioningConfiguration(?:\s+xmlns=(?:"[^"]*"|'[^']*'))?\s*(?:\/>\s*$|>([\s\S]*)<\/VersioningConfiguration>\s*$)/.exec(xml);
  if (!doc) return undefined;
  const body = doc[1] ?? "";
  const statuses = [...body.matchAll(/<Status\s*>\s*([^<]*?)\s*<\/Status\s*>/g)].map((m) => m[1]!);
  if (statuses.length > 1) return undefined;
  return statuses[0] ?? "";
}

const coded = (code: string) => Object.assign(new Error(code), { code });

export async function readBucketVersioning(cfg: BucketVersioningCheck): Promise<BucketVersioningVerdict> {
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const url = bucketVersioningUrl(cfg);
  const headers = new Headers();
  await signRequest({ method: "GET", url, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: cfg.region, credentials: cfg.credentials });
  let res: Response;
  try {
    res = await doFetch(url, { method: "GET", headers, redirect: "error", signal: AbortSignal.timeout(cfg.timeoutMs ?? 10_000) });
  } catch {
    throw coded("bucket_versioning_unavailable");
  }
  // 501 is the store saying it has no such API: unverified (a refusal), not a transient failure.
  if (res.status >= 500 && res.status !== 501) throw coded("bucket_versioning_unavailable");
  if (!res.ok) return "unverified";
  const text = await boundedText(res);
  const status = text === undefined ? undefined : versioningStatus(text);
  if (status === undefined) return "unverified";
  return status === "Enabled" ? "Enabled" : "not_versioned";
}

async function boundedText(res: Response): Promise<string | undefined> {
  const reader = res.body?.getReader();
  if (!reader) return undefined;
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const r = await reader.read();
      if (r.done) break;
      size += r.value.byteLength;
      if (size > MAX_BYTES) return undefined;
      chunks.push(r.value);
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let at = 0;
  for (const c of chunks) { bytes.set(c, at); at += c.byteLength; }
  try { return new TextDecoder("utf-8", { fatal: true }).decode(bytes); } catch { return undefined; }
}
