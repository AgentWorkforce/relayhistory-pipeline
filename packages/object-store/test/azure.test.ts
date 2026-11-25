import { describe, expect, it } from "vitest";
import { azureSharedKeyAuthorization, createAzureBlobStore } from "../src/azure.js";
import { fromBase64, hmacSha256, toBase64 } from "../src/crypto.js";
import { objectKey } from "../src/key.js";

const ACCOUNT = "fxacct";
const KEY = toBase64(new Uint8Array(64).map((_, i) => (i * 37 + 11) & 0xff));

/** The Shared Key string-to-sign exactly as AzureTransport#sharedKeySignature built it before the export. */
async function previousSignature(method: string, url: URL, headers: Headers, contentLength: number, accountKey: string): Promise<string> {
  const h = (name: string) => headers.get(name) ?? "";
  const msHeaders: [string, string][] = [];
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower.startsWith("x-ms-")) msHeaders.push([lower, value.trim()]);
  });
  msHeaders.sort(([a], [b]) => (a < b ? -1 : 1));
  const params = [...url.searchParams.entries()].map(([k, v]) => [k.toLowerCase(), v] as const).sort(([a], [b]) => (a < b ? -1 : 1));
  const resource = `/${ACCOUNT}${url.pathname}` + params.map(([k, v]) => `\n${k}:${v}`).join("");
  const stringToSign = [method, h("content-encoding"), h("content-language"), contentLength > 0 ? String(contentLength) : "", h("content-md5"),
    h("content-type"), "", h("if-modified-since"), h("if-match"), h("if-none-match"), h("if-unmodified-since"), h("range"),
    msHeaders.map(([k, v]) => `${k}:${v}`).join("\n"), resource].join("\n");
  return toBase64(await hmacSha256(fromBase64(accountKey), stringToSign));
}

describe("azure Shared Key signer (exported for the strict eraser)", () => {
  it("is byte-identical to the adapter's previous signature", async () => {
    const date = "Mon, 23 Nov 2026 10:00:00 GMT";
    const cases: [string, string, Record<string, string>, number][] = [
      ["PUT", "http://127.0.0.1:10000/fxacct/fx-container/a/b.json", { "x-ms-blob-type": "BlockBlob", "content-type": "application/json", "if-none-match": "*", "x-ms-meta-trace_id": "t" }, 17],
      ["GET", "https://fxacct.blob.core.windows.net/fx-container?restype=container&comp=list&prefix=a%2F&include=deleted%2Csnapshots%2Cversions&maxresults=1000&marker=eyJ2Ijox", {}, 0],
      ["DELETE", "https://fxacct.blob.core.windows.net/fx-container/a/b.json?versionid=2026-11-23T10%3A00%3A00.1234567Z&deletetype=permanent", { "if-match": '"0x1"' }, 0],
      ["GET", "https://fxacct.blob.core.windows.net/fx-container/a/b.json", { "x-ms-range": "bytes=0-9" }, 0],
    ];
    for (const [method, raw, extra, length] of cases) {
      const url = new URL(raw);
      const headers = new Headers({ ...extra, "x-ms-version": "2021-12-02", "x-ms-date": date });
      expect(await azureSharedKeyAuthorization(ACCOUNT, KEY, method, url, headers, length))
        .toBe(`SharedKey ${ACCOUNT}:${await previousSignature(method, url, headers, length, KEY)}`);
    }
  });
});

describe("azure adapter version ids", () => {
  const fakeFetch = (versionId: string | null) => {
    const seen: { method: string; signal: boolean }[] = [];
    const fetcher = (async (_input: RequestInfo | URL, init?: RequestInit) => {
      seen.push({ method: init?.method ?? "GET", signal: !!init?.signal });
      const headers = new Headers({ etag: '"0x1D"', "content-length": "2", "last-modified": "Mon, 23 Nov 2026 10:00:00 GMT" });
      if (versionId) headers.set("x-ms-version-id", versionId);
      if (init?.method === "PUT") return new Response(null, { status: 201, headers });
      return new Response(init?.method === "HEAD" ? null : "ok", { status: 200, headers });
    }) as typeof fetch;
    return { fetcher, seen };
  };

  it("by default the version stays the ETag alone even when x-ms-version-id is sent (existing stores unchanged)", async () => {
    const { fetcher } = fakeFetch("2026-11-23T10:00:00.1234567Z");
    const store = createAzureBlobStore({ account: ACCOUNT, container: "fx-container", endpoint: "http://127.0.0.1:1/fxacct", auth: { kind: "sharedKey", accountKey: KEY }, fetch: fetcher });
    expect((await store.put(objectKey("a/b.json"), new TextEncoder().encode("ok"))).version).toEqual({ eTag: '"0x1D"' });
    expect((await store.head(objectKey("a/b.json")))?.version).toEqual({ eTag: '"0x1D"' });
    const got = await store.getObject(objectKey("a/b.json"));
    await got.body.cancel();
    expect(got.meta.version).toEqual({ eTag: '"0x1D"' });
  });

  it("versionIds: put, head and getObject surface x-ms-version-id as the version (a versioning account)", async () => {
    const v = "2026-11-23T10:00:00.1234567Z";
    const { fetcher, seen } = fakeFetch(v);
    const store = createAzureBlobStore({ account: ACCOUNT, container: "fx-container", endpoint: "http://127.0.0.1:1/fxacct", auth: { kind: "sharedKey", accountKey: KEY }, fetch: fetcher, versionIds: true });
    const signal = new AbortController().signal;
    const ack = await store.put(objectKey("a/b.json"), new TextEncoder().encode("ok"), { signal });
    expect(ack.version).toEqual({ eTag: '"0x1D"', version: v });
    expect(seen[0]).toEqual({ method: "PUT", signal: true });
    expect((await store.head(objectKey("a/b.json")))?.version).toEqual({ eTag: '"0x1D"', version: v });
    const got = await store.getObject(objectKey("a/b.json"));
    await got.body.cancel();
    expect(got.meta.version).toEqual({ eTag: '"0x1D"', version: v });
  });

  it("versionIds without the header (versioning off): the version is the ETag alone", async () => {
    const { fetcher } = fakeFetch(null);
    const store = createAzureBlobStore({ account: ACCOUNT, container: "fx-container", endpoint: "http://127.0.0.1:1/fxacct", auth: { kind: "sharedKey", accountKey: KEY }, fetch: fetcher, versionIds: true });
    expect((await store.put(objectKey("a/b.json"), new TextEncoder().encode("ok"))).version).toEqual({ eTag: '"0x1D"' });
    expect((await store.head(objectKey("a/b.json")))?.version).toEqual({ eTag: '"0x1D"' });
  });
});
