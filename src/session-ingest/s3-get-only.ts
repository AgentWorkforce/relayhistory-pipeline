/**
 * A GET-ONLY S3 reader for the read-only sweeper-pass verifier (sweeper-pass-verify.ts): one SigV4-signed GetObject,
 * nothing else. It is not an ObjectStore and is not built from one: no BaseStore or S3 transport is constructed, so
 * no put, delete, list, head or presign exists anywhere in the object it returns (a frozen { getObject }).
 *
 * It accepts only the fixed key alphabet of the sweeper's pass objects (no encoding needed, nothing that could
 * reshape the URL), sends `response-content-encoding=identity` as the S3 transport does (the stored bytes, never a
 * decoded view), and maps the response the way the transport does: 404 `not_found`, 401/403 `unauthorized`, any
 * other failure `transient` (network) or `backend`, with no key, URL or response body in the error.
 */
import { ObjectStoreError, type ObjectKey } from "@rhc-object-store";
// The object-store package's own signer (not re-exported from its index; imported directly so this reader needs
// no store instance).
import { signRequest } from "../../packages/object-store/src/sigv4.ts";
import type { RearmReceiptReadStore } from "./rearm-sweeper-receipt.ts";

const BACKEND = "s3";
const EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const REGION = /^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/;
/** The pass objects' keys: `<keyRoot>/.ledger/rearm-sweeper/passes/<scope>/<startedAtMs|latest>.json`. */
const PASS_KEY = /^[a-z0-9][a-z0-9-]{0,62}\/\.ledger\/rearm-sweeper\/passes\/[A-Za-z0-9._-]{1,128}\/(?:latest|[1-9][0-9]{0,15})\.json$/;

export interface S3GetOnlyConfig {
  bucket: string;
  region: string;
  credentials: { accessKeyId: string; secretAccessKey: string; sessionToken?: string };
  /** Tests only: the HTTP client (default the global fetch). */
  fetch?: typeof fetch;
}

const fail = (code: "not_found" | "unauthorized" | "transient" | "backend" | "invalid_argument", status?: number) =>
  new ObjectStoreError(code, BACKEND, code, status === undefined ? {} : { status });

export function s3GetOnly(config: S3GetOnlyConfig): RearmReceiptReadStore {
  if (!BUCKET.test(config.bucket) || config.bucket.includes("..") || !REGION.test(config.region)) throw fail("invalid_argument");
  const { bucket, region } = config;
  const credentials = Object.freeze({ ...config.credentials });
  const http = config.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  async function getObject(key: ObjectKey) {
    const k = String(key);
    if (!PASS_KEY.test(k) || k.split("/").some((seg) => seg === "." || seg === "..")) throw fail("invalid_argument");
    const url = new URL(`https://${bucket}.s3.${region}.amazonaws.com/${k}`);
    url.searchParams.set("response-content-encoding", "identity");
    const headers = new Headers();
    await signRequest({ method: "GET", url, headers, payloadSha256: EMPTY_SHA256, region, credentials });
    let res: Response;
    try { res = await http(url, { method: "GET", headers, redirect: "error" }); }
    catch { throw fail("transient"); }
    if (!res.ok || !res.body) {
      await res.body?.cancel().catch(() => {});
      throw fail(res.status === 404 ? "not_found" : res.status === 401 || res.status === 403 ? "unauthorized" : "backend", res.status);
    }
    const versionId = res.headers.get("x-amz-version-id"), eTag = res.headers.get("etag");
    return {
      meta: {
        key,
        size: Number(res.headers.get("content-length") ?? 0),
        lastModified: new Date(res.headers.get("last-modified") ?? 0),
        version: { ...(eTag ? { eTag } : {}), ...(versionId && versionId !== "null" ? { version: versionId } : {}) },
        metadata: {},
      },
      body: res.body,
    };
  }
  return Object.freeze({ getObject });
}
