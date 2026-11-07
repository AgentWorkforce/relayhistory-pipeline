/**
 * Offline ListObjectVersions / DeleteObject(versionId) provider for the strict paged eraser:
 * keys in C order, versions newest first, `key-marker` (keys strictly after) and
 * `version-id-marker` (versions of the marker key after that version, then later keys) as S3
 * documents them, and 400 for a version marker that no longer exists. Fault hooks let a test
 * corrupt a later page, repeat markers, inject foreign keys, or lose a DELETE acknowledgement.
 */
import { createS3Eraser } from "../neon-s3-erasure/eraser.ts";

export interface FakeVersion { id: string; at: number; marker?: boolean }

export class FakeS3Versions {
  readonly keys = new Map<string, FakeVersion[]>();
  pageSize = 1000;
  lists = 0;
  deletes: string[] = [];
  #n = 0;
  /** (listCall, xml) => xml : rewrite a page (1-based list call). */
  rewrite?: (call: number, xml: string) => string;
  /** Called for every DELETE; return "lose" to apply it but answer 500. */
  onDelete?: (key: string, id: string) => "lose" | void;

  put(key: string, at = Date.now()): string {
    const id = `v${String(++this.#n).padStart(8, "0")}`;
    this.keys.set(key, [{ id, at }, ...(this.keys.get(key) ?? [])]);
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
      return `<${tag}><Key>${e.key}</Key><VersionId>${e.v.id}</VersionId><IsLatest>${e.latest}</IsLatest><LastModified>${new Date(e.v.at).toISOString()}</LastModified></${tag}>`;
    }).join("");
    let xml = `<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>b</Name><Prefix>${prefix}</Prefix><IsTruncated>${truncated}</IsTruncated>`
      + (truncated && last ? `<NextKeyMarker>${last.key}</NextKeyMarker><NextVersionIdMarker>${last.v.id}</NextVersionIdMarker>` : "") + `${entries}</ListVersionsResult>`;
    this.lists++;
    if (this.rewrite) xml = this.rewrite(this.lists, xml);
    return new Response(xml);
  }

  fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const u = new URL(String(input));
    if (init?.method === "DELETE") {
      const key = decodeURIComponent(u.pathname.replace(/^\/b\//, "")), id = u.searchParams.get("versionId")!;
      this.deletes.push(`${key}@${id}`);
      const fault = this.onDelete?.(key, id);
      this.remove(key, id);
      return new Response(null, { status: fault === "lose" ? 500 : 204 });
    }
    return this.#list(u);
  }) as typeof fetch;

  eraser() {
    return createS3Eraser({ bucket: "b", region: "us-east-1", endpoint: "http://fake-s3.local", credentials: { accessKeyId: "a", secretAccessKey: "s" }, fetch: this.fetch });
  }
}
