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
 * Verdicts: "Enabled" passes. A strictly parsed document whose Status is absent or Suspended is the refusal
 * `bucket_not_versioned`. A 4xx, a 501, a body over 64 KiB, or a document that is not exactly the known
 * shape (see versioningStatus) is the refusal `bucket_versioning_unverified`. A 3xx is the refusal
 * `bucket_versioning_redirect` (the endpoint or region is wrong; redirects are never followed). Any other
 * 5xx, a network failure, or a timeout while reading the body throws a plain coded error
 * `bucket_versioning_unavailable` (exit 1: transient, the pod restarts and retries). Errors carry codes only,
 * never the bucket, endpoint or a credential.
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

export type BucketVersioningVerdict = "Enabled" | "not_versioned" | "unverified" | "redirect";

const MAX_BYTES = 64 * 1024;

export function bucketVersioningUrl(cfg: Pick<BucketVersioningCheck, "bucket" | "region" | "endpoint">): URL {
  const url = cfg.endpoint
    ? new URL(`${cfg.endpoint.replace(/\/+$/, "")}/${cfg.bucket}`)
    : new URL(`https://${cfg.bucket}.s3.${cfg.region}.amazonaws.com/`);
  url.searchParams.set("versioning", "");
  return url;
}

/**
 * The Status of a GetBucketVersioning document, parsed strictly: "Enabled" or "Suspended", "" when absent
 * (never enabled), or undefined when the document cannot prove the bucket's versioning, namely:
 *   - anything but one VersioningConfiguration root (optional XML declaration and xmlns), comments ignored;
 *   - an element other than Status, MfaDelete, ExcludeFolders, ExcludedPrefixes and its Prefix (any other
 *     element, attribute, CDATA or processing instruction);
 *   - a repeated Status, MfaDelete or ExcludeFolders, or a value outside Enabled/Suspended, Enabled/Disabled,
 *     true/false;
 *   - MinIO's exclusions in effect: ExcludeFolders true, or any non-empty Prefix under ExcludedPrefixes (the
 *     excluded keys are written unversioned, so the sweeper's exact-version erase would not hold for them).
 * MfaDelete is accepted and not evaluated: with MFA delete on, the eraser's version deletes fail loudly.
 */
export function versioningStatus(xml: string): string | undefined {
  if (/<!--(?![\s\S]*?-->)/.test(xml)) return undefined;
  const text = xml.replace(/<!--[\s\S]*?-->/g, "");
  const doc = /^\s*(?:<\?xml\s[^?<>]*\?>\s*)?<VersioningConfiguration(?:\s+xmlns=(?:"[^"<>]*"|'[^'<>]*'))?\s*(?:\/>\s*$|>([\s\S]*)<\/VersioningConfiguration\s*>\s*$)/.exec(text);
  if (!doc) return undefined;
  const body = doc[1] ?? "";
  const seen = new Map<string, string>();
  const child = /\s*(?:<(Status|MfaDelete|ExcludeFolders)>([^<]*)<\/\1>|<ExcludedPrefixes\s*\/>|<ExcludedPrefixes>((?:\s*<Prefix>[^<]*<\/Prefix>|\s*<Prefix\s*\/>)*)\s*<\/ExcludedPrefixes>)/y;
  let at = 0;
  while (at < body.length) {
    if (/^\s*$/.test(body.slice(at))) break;
    child.lastIndex = at;
    const m = child.exec(body);
    if (!m) return undefined;
    at = child.lastIndex;
    if (m[1]) {
      if (seen.has(m[1])) return undefined;
      seen.set(m[1], m[2]!.trim());
    } else if (m[3] !== undefined) {
      for (const p of m[3].matchAll(/<Prefix>([^<]*)<\/Prefix>/g)) if (p[1]!.trim() !== "") return undefined;
    }
  }
  const status = seen.get("Status") ?? "";
  if (status !== "" && status !== "Enabled" && status !== "Suspended") return undefined;
  const mfa = seen.get("MfaDelete");
  if (mfa !== undefined && mfa !== "Enabled" && mfa !== "Disabled") return undefined;
  const folders = seen.get("ExcludeFolders");
  if (folders !== undefined && folders !== "false") return undefined;
  return status;
}

const coded = (code: string) => Object.assign(new Error(code), { code });

export async function readBucketVersioning(cfg: BucketVersioningCheck): Promise<BucketVersioningVerdict> {
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const url = bucketVersioningUrl(cfg);
  const headers = new Headers();
  await signRequest({ method: "GET", url, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: cfg.region, credentials: cfg.credentials });
  let res: Response;
  try {
    // Never followed: a redirect means the endpoint or region is wrong, and the signed request must not move.
    res = await doFetch(url, { method: "GET", headers, redirect: "manual", signal: AbortSignal.timeout(cfg.timeoutMs ?? 10_000) });
  } catch {
    throw coded("bucket_versioning_unavailable");
  }
  const discard = async () => { await res.body?.cancel().catch(() => {}); };
  if (res.type === "opaqueredirect" || res.redirected || (res.status >= 300 && res.status < 400)) { await discard(); return "redirect"; }
  // 501 is the store saying it has no such API: unverified (a refusal), not a transient failure.
  if (res.status >= 500 && res.status !== 501) { await discard(); throw coded("bucket_versioning_unavailable"); }
  if (!res.ok) { await discard(); return "unverified"; }
  let text: string | undefined;
  // A stall past the deadline while reading the body (the signal's TimeoutError) is transient, by code.
  try { text = await boundedText(res); } catch { throw coded("bucket_versioning_unavailable"); }
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
