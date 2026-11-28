/**
 * Offline ListObjectVersions / DeleteObject(versionId) provider for the strict paged eraser:
 * keys in C order, versions newest first, `key-marker` (keys strictly after) and
 * `version-id-marker` (versions of the marker key after that version, then later keys) as S3
 * documents them, and 400 for a version marker that no longer exists. Fault hooks let a test
 * corrupt a later page, repeat markers, inject foreign keys, or lose a DELETE acknowledgement.
 */
import { createS3Eraser, createS3VersionReader, type S3EraserConfig } from "../neon-s3-erasure/eraser.ts";

export interface FakeVersion { id: string; at: number; marker?: boolean; etag?: string; body?: Uint8Array }

const NO_LIFECYCLE = { status: 404, body: "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>" };

export class FakeS3Versions {
  readonly keys = new Map<string, FakeVersion[]>();
  pageSize = 1000;
  lists = 0;
  deletes: string[] = [];
  #n = 0;
  /** (listCall, xml) => xml : rewrite a page (1-based list call). */
  rewrite?: (call: number, xml: string) => string;
  /**
   * Called for every DELETE: "lose" applies it but answers 500; "transport" applies it, then fails the request
   * (a timeout or connection reset after the provider acted); "transport-before" fails it without applying;
   * "credential-after" applies it, then throws a short_lived_*-coded error tagged notSent (a lying fetch).
   */
  onDelete?: (key: string, id: string) => "lose" | "transport" | "transport-before" | "credential-after" | void;
  /** GET ?lifecycle answer (default: S3's NoSuchLifecycleConfiguration 404). */
  lifecycle: { status: number; body: string } = NO_LIFECYCLE;
  /** GET ?versioning answer (default Enabled). */
  versioning: { status: number; body: string } = { status: 200, body: `<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>` };
  /** Object GETs by version id. */
  gets: string[] = [];

  put(key: string, at = Date.now(), etag?: string, body?: Uint8Array): string {
    const id = `v${String(++this.#n).padStart(8, "0")}`;
    this.keys.set(key, [{ id, at, ...(etag ? { etag } : {}), ...(body ? { body } : {}) }, ...(this.keys.get(key) ?? [])]);
    return id;
  }
  /** A delete marker as the new current version of `key`. */
  mark(key: string, at = Date.now()): string {
    const id = `v${String(++this.#n).padStart(8, "0")}`;
    this.keys.set(key, [{ id, at, marker: true }, ...(this.keys.get(key) ?? [])]);
    return id;
  }
  count(prefix = ""): number {
    let n = 0;
    for (const [k, vs] of this.keys) if (k.startsWith(prefix)) n += vs.length;
    return n;
  }
  remove(key: string, id: string): void {
    const vs = (this.keys.get(key) ?? []).filter((v) => v.id !== id);
    if (vs.length) this.keys.set(key, vs); else this.keys.delete(key);
  }

  #list(u: URL): Response {
    const prefix = u.searchParams.get("prefix") ?? "", km = u.searchParams.get("key-marker"), vm = u.searchParams.get("version-id-marker");
    const sorted = [...this.keys.keys()].filter((k) => k.startsWith(prefix)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const flat: { key: string; v: FakeVersion; latest: boolean }[] = [];
    for (const k of sorted) this.keys.get(k)!.forEach((v, i) => flat.push({ key: k, v, latest: i === 0 }));
    let start = 0;
    if (km !== null && vm !== null) {
      const at = flat.findIndex((e) => e.key === km && e.v.id === vm);
      if (at < 0) return new Response("<Error><Code>InvalidArgument</Code></Error>", { status: 400 });
      start = at + 1;
    } else if (km !== null) {
      start = flat.findIndex((e) => e.key > km);
      if (start < 0) start = flat.length;
    }
    const page = flat.slice(start, start + this.pageSize);
    const truncated = start + this.pageSize < flat.length;
    const last = page[page.length - 1];
    const entries = page.map((e) => {
      const tag = e.v.marker ? "DeleteMarker" : "Version";
      return `<${tag}><Key>${e.key}</Key><VersionId>${e.v.id}</VersionId><IsLatest>${e.latest}</IsLatest><LastModified>${new Date(e.v.at).toISOString()}</LastModified>${e.v.etag && !e.v.marker ? `<ETag>&quot;${e.v.etag}&quot;</ETag>` : ""}</${tag}>`;
    }).join("");
    let xml = `<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>b</Name><Prefix>${prefix}</Prefix><IsTruncated>${truncated}</IsTruncated>`
      + (truncated && last ? `<NextKeyMarker>${last.key}</NextKeyMarker><NextVersionIdMarker>${last.v.id}</NextVersionIdMarker>` : "") + `${entries}</ListVersionsResult>`;
    this.lists++;
    if (this.rewrite) xml = this.rewrite(this.lists, xml);
    return new Response(xml);
  }

  fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const u = new URL(String(input));
    if ((init?.method ?? "GET") === "GET") {
      if (u.searchParams.has("lifecycle")) return new Response(this.lifecycle.body, { status: this.lifecycle.status });
      if (u.searchParams.has("versioning")) return new Response(this.versioning.body, { status: this.versioning.status });
      const key = decodeURIComponent(u.pathname.replace(/^\/b\/?/, ""));
      if (key && !u.searchParams.has("versions")) {
        const id = u.searchParams.get("versionId") ?? "";
        this.gets.push(`${key}@${id}`);
        const v = (this.keys.get(key) ?? []).find((x) => x.id === id);
        if (!v || v.marker || !v.body) return new Response(null, { status: v?.marker ? 405 : 404 });
        return new Response(v.body.slice(), { status: 200, headers: { "x-amz-version-id": id } });
      }
    }
    if (init?.method === "DELETE") {
      const key = decodeURIComponent(u.pathname.replace(/^\/b\//, "")), id = u.searchParams.get("versionId")!;
      this.deletes.push(`${key}@${id}`);
      const fault = this.onDelete?.(key, id);
      if (fault === "transport-before") throw new TypeError("fetch failed");
      this.remove(key, id);
      if (fault === "transport") throw new TypeError("fetch failed");
      if (fault === "credential-after") throw Object.assign(new Error("credential refused"), { code: "short_lived_expired.eraser", notSent: true });
      return new Response(null, { status: fault === "lose" ? 500 : 204 });
    }
    return this.#list(u);
  }) as typeof fetch;

  reader() {
    return createS3VersionReader({ bucket: "b", region: "us-east-1", endpoint: "http://fake-s3.local", credentials: { accessKeyId: "a", secretAccessKey: "s" }, fetch: this.fetch });
  }

  eraser(opts: { requestDeadlineMs?: number; signer?: S3EraserConfig["signer"] } = {}) {
    return createS3Eraser({ bucket: "b", region: "us-east-1", endpoint: "http://fake-s3.local", credentials: { accessKeyId: "a", secretAccessKey: "s" }, fetch: this.fetch, ...opts });
  }
}
