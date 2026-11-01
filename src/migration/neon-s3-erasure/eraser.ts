/**
 * Permanent erasure for the sessions-prod transcript bucket, with only the
 * eraser key (96BullSessions #100: ListBucket/ListBucketVersions under
 * `96bulls/`, DeleteObject/DeleteObjectVersion, no GetObject, no key use).
 *
 * Every version and delete marker of a key (or under a prefix) is deleted by
 * version id, so nothing waits for lifecycle: a plain DELETE on a versioned
 * bucket would only add a delete marker and leave the bytes for a day.
 */
import { encodeKeyPath, sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { signRequest, type AwsCredentials } from "../../../packages/object-store/src/sigv4.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";

export interface Eraser {
  /** Every version of exactly this key. Returns versions removed. */
  eraseObject(key: string): Promise<number>;
  /** Every version of every key under `prefix` (which ends in `/`), manifests first. */
  erasePrefix(prefix: string): Promise<number>;
}

export interface S3EraserConfig {
  bucket: string;
  region: string;
  /** Path-style override (MinIO). */
  endpoint?: string;
  credentials: AwsCredentials;
  fetch?: typeof fetch;
}

interface Version {
  key: string;
  versionId: string;
}

const coded = (code: string, message: string) => Object.assign(new Error(message), { code });

export function createS3Eraser(cfg: S3EraserConfig): Eraser {
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const url = (key = "") => {
    if (cfg.endpoint) {
      const u = new URL(cfg.endpoint);
      u.pathname = `${u.pathname.replace(/\/+$/, "")}/${cfg.bucket}/${key ? encodeKeyPath(key) : ""}`;
      return u;
    }
    return new URL(`https://${cfg.bucket}.s3.${cfg.region}.amazonaws.com/${key ? encodeKeyPath(key) : ""}`);
  };
  const request = async (method: string, u: URL) => {
    const headers = new Headers();
    await signRequest({ method, url: u, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: cfg.region, credentials: cfg.credentials });
    const res = await doFetch(u, { method, headers });
    // Status only: S3 error bodies can echo keys.
    if (!res.ok && !(method === "DELETE" && res.status === 404)) throw coded(`s3_${res.status}`, `S3 ${method} failed`);
    return res;
  };

  async function versions(prefix: string): Promise<Version[]> {
    const out: Version[] = [];
    let keyMarker: string | undefined;
    let versionMarker: string | undefined;
    for (;;) {
      const u = url();
      u.searchParams.set("versions", "");
      u.searchParams.set("prefix", prefix);
      u.searchParams.set("max-keys", "1000");
      if (keyMarker !== undefined) u.searchParams.set("key-marker", keyMarker);
      if (versionMarker !== undefined) u.searchParams.set("version-id-marker", versionMarker);
      const xml = await (await request("GET", u)).text();
      for (const tag of ["Version", "DeleteMarker"])
        for (const el of xmlElements(xml, tag)) {
          const key = xmlText(el, "Key");
          const versionId = xmlText(el, "VersionId");
          // xmlText already decodes entities; decoding again would mangle a literal `&amp;`.
          if (key !== undefined && versionId !== undefined) out.push({ key, versionId });
        }
      if (xmlText(xml, "IsTruncated") !== "true") return out;
      keyMarker = xmlText(xml, "NextKeyMarker");
      versionMarker = xmlText(xml, "NextVersionIdMarker");
      if (keyMarker === undefined) return out;
    }
  }

  async function remove(list: Version[]): Promise<number> {
    if (list.some((v) => v.key.split("/").some((seg) => seg === "." || seg === "..")))
      throw coded("erasure_key_shape", "a key with a dot segment would be normalized away");
    // Manifests first, so no reader finds a manifest whose chunks are gone.
    list.sort((a, b) => Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")));
    for (const v of list) {
      const u = url(v.key);
      u.searchParams.set("versionId", v.versionId);
      await request("DELETE", u);
    }
    return list.length;
  }

  return {
    eraseObject: async (key) => remove((await versions(key)).filter((v) => v.key === key)),
    erasePrefix: async (prefix) => {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      return remove(await versions(prefix));
    },
  };
}

/** The same contract over a plain (unversioned) store, for tests and local runs. */
export function storeEraser(store: ObjectStore): Eraser {
  return {
    async eraseObject(key) {
      const had = await store.head(objectKey(key));
      await store.delete(objectKey(key));
      return had ? 1 : 0;
    },
    async erasePrefix(prefix) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => i.key));
        cursor = page.nextCursor;
      } while (cursor);
      keys.sort((a, b) => Number(b.endsWith("/manifest.json")) - Number(a.endsWith("/manifest.json")));
      for (const k of keys) await store.delete(objectKey(k));
      return keys.length;
    },
  };
}
