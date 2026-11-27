/**
 * Test-only MinIO fixture for the 062320 hold harness.
 *
 * SESSION_INGEST_TEST_S3_ENDPOINT (http, loopback only), SESSION_INGEST_TEST_S3_ACCESS_KEY and
 * SESSION_INGEST_TEST_S3_SECRET_KEY name a disposable MinIO whose throwaway root credentials exist only in the test
 * process's environment (run.ts generates them per run). Unset, suites skip, except under
 * SESSION_INGEST_TEST_S3_REQUIRED=1, where that is a failure.
 *
 * scratchBucket gives each case its own bucket and an ObjectStore on it through the repository's own S3 client
 * (createS3Store), optionally pointed at a put-gate.ts proxy so one PUT can be paused and delivered late. MinIO
 * honours `If-None-Match: *` on PUT (412 when the key exists), the create-only write ledger-s3.ts casPut makes.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createS3Store, type ObjectStore } from "../../../shared/vendor.ts";
import { signRequest, type AwsCredentials } from "../../../../packages/object-store/src/sigv4.ts";
import { encodeKeyPath } from "../../../../packages/object-store/src/crypto.ts";

/** The MinIO pin of versions.lock.json (`locker`), the image the local stack and pg-proof run. */
export const MINIO_IMAGE = (() => {
  const lock = JSON.parse(readFileSync(new URL("../../../../versions.lock.json", import.meta.url), "utf8"));
  const { ref, digest } = lock.images?.locker ?? {};
  if (typeof ref !== "string" || !/^sha256:[0-9a-f]{64}$/.test(digest)) throw Error("versions_lock_locker_pin_missing");
  return `${ref}@${digest}`;
})();
export const REGION = "us-east-1";

export interface S3Fixture { endpoint: string; credentials: AwsCredentials }

export function fixtureS3(env: NodeJS.ProcessEnv = process.env): S3Fixture | undefined {
  const endpoint = env.SESSION_INGEST_TEST_S3_ENDPOINT || undefined;
  const accessKeyId = env.SESSION_INGEST_TEST_S3_ACCESS_KEY || undefined;
  const secretAccessKey = env.SESSION_INGEST_TEST_S3_SECRET_KEY || undefined;
  if (!(endpoint && accessKeyId && secretAccessKey)) {
    if (env.SESSION_INGEST_TEST_S3_REQUIRED === "1") throw Error("SESSION_INGEST_TEST_S3_* required (SESSION_INGEST_TEST_S3_REQUIRED=1)");
    return undefined;
  }
  const u = new URL(endpoint);
  if (u.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username || u.password ||
      u.pathname !== "/" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
  return { endpoint: u.origin, credentials: { accessKeyId, secretAccessKey } };
}

/** One signed request straight to the store (or a gate in front of it), outside any client under test. */
export async function rawS3(endpoint: string, credentials: AwsCredentials, method: string, path: string,
  body = "", headers: Record<string, string> = {}): Promise<Response> {
  const url = new URL(endpoint + path);
  const h = new Headers(headers);
  await signRequest({ method, url, headers: h, payloadSha256: createHash("sha256").update(body).digest("hex"), region: REGION, credentials });
  return fetch(url, { method, headers: h, ...(body ? { body } : {}), signal: AbortSignal.timeout(10_000) });
}

export interface ScratchBucket {
  bucket: string;
  /** an ObjectStore on this bucket via `endpoint` (default: the fixture; pass a PutGate's url to interpose) */
  store(endpoint?: string, fetchImpl?: typeof fetch): ObjectStore;
  /** a signed request on this bucket, `key` path-encoded as the S3 client encodes it */
  raw(method: string, key: string, body?: string, headers?: Record<string, string>, endpoint?: string): Promise<Response>;
  drop(): Promise<void>;
}

export async function scratchBucket(fx: S3Fixture, opts: { prefix?: string; versioned?: boolean } = {}): Promise<ScratchBucket> {
  const bucket = `${opts.prefix ?? "hold"}-${crypto.randomUUID().replaceAll("-", "").slice(0, 20)}`;
  if (!/^[a-z0-9][a-z0-9-]{2,62}$/.test(bucket)) throw Error("scratch_bucket_invalid");
  const made = await rawS3(fx.endpoint, fx.credentials, "PUT", `/${bucket}/`);
  if (!made.ok) throw Error(`scratch_bucket_create ${made.status} ${await made.text()}`);
  if (opts.versioned) {
    const v = await rawS3(fx.endpoint, fx.credentials, "PUT", `/${bucket}/?versioning`,
      `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`, { "content-type": "application/xml" });
    if (!v.ok) throw Error(`scratch_bucket_versioning ${v.status} ${await v.text()}`);
  }
  const raw: ScratchBucket["raw"] = (method, key, body = "", headers = {}, endpoint = fx.endpoint) =>
    rawS3(endpoint, fx.credentials, method, `/${bucket}/${encodeKeyPath(key)}`, body, headers);
  return {
    bucket,
    store: (endpoint = fx.endpoint, fetchImpl) => createS3Store({ bucket, region: REGION, endpoint, credentials: fx.credentials, ...(fetchImpl ? { fetch: fetchImpl } : {}) }),
    raw,
    async drop() {
      // Disposable container: best effort. Delete every version and marker, then the bucket.
      const listed = await rawS3(fx.endpoint, fx.credentials, "GET", `/${bucket}/?versions`);
      const xml = listed.ok ? await listed.text() : "";
      for (const m of xml.matchAll(/<(?:Version|DeleteMarker)>[\s\S]*?<Key>([^<]*)<\/Key>[\s\S]*?<VersionId>([^<]*)<\/VersionId>/g)) {
        const key = m[1]!.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'");
        await rawS3(fx.endpoint, fx.credentials, "DELETE", `/${bucket}/${encodeKeyPath(key)}?versionId=${encodeURIComponent(m[2]!)}`).catch(() => {});
      }
      await rawS3(fx.endpoint, fx.credentials, "DELETE", `/${bucket}/`).catch(() => {});
    },
  };
}
