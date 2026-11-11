import { describe, expect, it } from "vitest";
import { presignUrl, signRequest } from "../src/sigv4.js";
import {
  isObjectKey,
  machinePrefix,
  manifestKey,
  normalizedKey,
  objectKey,
  parseRawChunkName,
  parseSessionKey,
  rawChunkKey,
  sha256HexForTest,
} from "./helpers.js";

describe("SigV4", () => {
  // AWS's published presigned-URL example (S3 docs, "Authenticating Requests:
  // Using Query Parameters").
  it("matches the AWS presigned GET test vector", async () => {
    const url = await presignUrl({
      method: "GET",
      url: new URL("https://examplebucket.s3.amazonaws.com/test.txt"),
      region: "us-east-1",
      credentials: {
        accessKeyId: "AKIAIOSFODNN7EXAMPLE",
        secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      },
      expiresSeconds: 86400,
      now: new Date("2013-05-24T00:00:00Z"),
    });
    expect(url.searchParams.get("X-Amz-Signature")).toBe(
      "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
    );
  });

  // Independently computed (Python hmac/hashlib): service "es", path double-encoded.
  it("signs a non-S3 service with its scope and a double-encoded path", async () => {
    const headers = new Headers();
    await signRequest({
      method: "POST",
      url: new URL("https://search.example.com/my%2Findex/_search?refresh=true"),
      headers,
      payloadSha256: "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862",
      region: "us-east-1",
      service: "es",
      credentials: {
        accessKeyId: "AKIDEXAMPLE",
        secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
      },
      now: new Date("2015-08-30T12:36:00Z"),
    });
    expect(headers.get("authorization")).toBe(
      "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/es/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0f6e7ef329ff450191ea48bd01cfaf45682547ada824f0c09480a0bdbf517132",
    );
  });

  // AWS's published "GET Object" header-auth example (range request).
  it("matches the AWS GET object header-signing test vector", async () => {
    const headers = new Headers({ range: "bytes=0-9" });
    await signRequest({
      method: "GET",
      url: new URL("https://examplebucket.s3.amazonaws.com/test.txt"),
      headers,
      payloadSha256:
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      region: "us-east-1",
      credentials: {
        accessKeyId: "AKIAIOSFODNN7EXAMPLE",
        secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      },
      now: new Date("2013-05-24T00:00:00Z"),
    });
    // The AWS example also signs `range`; we sign host + x-amz-* only, so
    // assert the parts that are independent of the header set.
    const auth = headers.get("authorization")!;
    expect(auth).toMatch(
      /^AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE\/20130524\/us-east-1\/s3\/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$/,
    );
    expect(headers.get("x-amz-date")).toBe("20130524T000000Z");
  });
});

describe("keys and layout", () => {
  const loc = {
    prefix: "tenant-a/",
    workspaceId: "ws_1",
    machineId: "mach-9",
    harness: "claude",
    startDate: "2026-01-05",
    sessionId: "0192f0c4-7f00-7000-8000-000000000001",
  };

  it("builds the same keys as the Rust layout", async () => {
    expect(manifestKey(loc)).toBe(
      "tenant-a/v1/ws=ws_1/m=mach-9/h=claude/d=2026/01/05/s=0192f0c4-7f00-7000-8000-000000000001/manifest.json",
    );
    const sha = await sha256HexForTest("chunk");
    expect(rawChunkKey(loc, 42, 1024, sha, "jsonl")).toMatch(
      new RegExp(`/raw/0000000042-1024-${sha.slice(0, 8)}\\.jsonl\\.zst$`),
    );
    expect(machinePrefix(loc)).toBe("tenant-a/v1/ws=ws_1/m=mach-9");
    expect(normalizedKey(loc, 2, "events.jsonl.zst")).toMatch(
      /\/normalized\/v2\/events\.jsonl\.zst$/,
    );
    expect(
      manifestKey({ ...loc, startDate: new Date("2026-01-05T23:59:59-05:00") }),
    ).toContain("/d=2026/01/06/");
  });

  it("parses chunk names and session keys back", async () => {
    const sha = await sha256HexForTest("x");
    const k = rawChunkKey(
      { ...loc, prefix: undefined },
      10,
      77,
      sha,
      "sqlite-rows",
    );
    const name = k.slice(k.lastIndexOf("/") + 1);
    expect(parseRawChunkName(name)).toEqual({
      seq: 10,
      startOffset: 77,
      sha256_8: sha.slice(0, 8),
      format: "sqlite-rows",
    });
    expect(parseRawChunkName("manifest.json")).toBeUndefined();
    expect(parseSessionKey(manifestKey(loc))).toMatchObject({
      prefix: "tenant-a",
      workspaceId: "ws_1",
      startDate: "2026-01-05",
      rest: "manifest.json",
    });
  });

  it("rejects ambiguous keys and components", () => {
    for (const bad of [
      "",
      "/a",
      "a/",
      "a//b",
      "a/./b",
      "a/../b",
      ".rh/x",
      "a b",
      "a\\b",
      "a%20b",
      "é",
    ]) {
      expect(isObjectKey(bad), bad).toBe(false);
      expect(() => objectKey(bad)).toThrow();
    }
    for (const bad of ["", "a/b", "a=b", "..", ".hidden", "a b"]) {
      expect(() => manifestKey({ ...loc, sessionId: bad }), bad).toThrow();
    }
  });
});

describe("list cursors", () => {
  it("passes an S3 continuation token through exactly once decoded", async () => {
    const { createS3Store } = await import("../src/s3.js");
    const seen: string[] = [];
    const fetchImpl = (async (url: URL | string) => {
      const u = new URL(String(url));
      seen.push(u.searchParams.get("continuation-token") ?? "");
      const truncated = seen.length === 1;
      const xml = `<ListBucketResult><IsTruncated>${truncated}</IsTruncated>${
        truncated
          ? "<NextContinuationToken>tok&amp;amp;x</NextContinuationToken>"
          : ""
      }</ListBucketResult>`;
      return new Response(xml, { status: 200 });
    }) as typeof fetch;
    const s = createS3Store({
      bucket: "b",
      region: "us-east-1",
      endpoint: "http://localhost:1",
      credentials: { accessKeyId: "k", secretAccessKey: "s" },
      fetch: fetchImpl,
    });
    const first = await s.list(objectKey("p"));
    expect(first.nextCursor).toBe("tok&amp;x");
    await s.list(objectKey("p"), first.nextCursor);
    expect(seen[1]).toBe("tok&amp;x");
  });
});

describe("S3 server-side encryption", () => {
  it("requests it on every put when configured, and signs the header", async () => {
    const { createS3Store } = await import("../src/s3.js");
    const seen: Headers[] = [];
    const fetchImpl = (async (_url: URL | string, init?: RequestInit) => {
      seen.push(new Headers(init?.headers));
      return new Response(null, { status: 200, headers: { etag: '"e"' } });
    }) as typeof fetch;
    const config = {
      bucket: "b",
      region: "us-east-1",
      endpoint: "http://localhost:1",
      credentials: { accessKeyId: "k", secretAccessKey: "s" },
      fetch: fetchImpl,
    };
    await createS3Store({ ...config, serverSideEncryption: "AES256" }).put(
      objectKey("a/b.json"),
      new Uint8Array([1]),
    );
    await createS3Store(config).put(objectKey("a/c.json"), new Uint8Array([1]));
    expect(seen[0]!.get("x-amz-server-side-encryption")).toBe("AES256");
    expect(seen[0]!.get("authorization")).toContain(
      "x-amz-server-side-encryption",
    );
    expect(seen[1]!.has("x-amz-server-side-encryption")).toBe(false);
  });
});

describe("S3 head/getObject: server-side encryption in ObjectMeta", () => {
  it("reports the SSE headers when present and omits the fields when absent", async () => {
    const { createS3Store } = await import("../src/s3.js");
    let sse = true;
    const fetchImpl = (async () => {
      const headers: Record<string, string> = { etag: '"e"', "content-length": "1" };
      if (sse) {
        headers["x-amz-server-side-encryption"] = "aws:kms";
        headers["x-amz-server-side-encryption-aws-kms-key-id"] = "arn:aws:kms:us-east-1:000000000000:key/example";
      }
      return new Response(new Uint8Array([1]), { status: 200, headers });
    }) as typeof fetch;
    const s = createS3Store({ bucket: "b", region: "us-east-1", endpoint: "http://localhost:1", credentials: { accessKeyId: "k", secretAccessKey: "s" }, fetch: fetchImpl });
    const head = await s.head(objectKey("a/b.json"));
    const got = await s.getObject(objectKey("a/b.json"));
    await got.body.cancel();
    for (const meta of [head!, got.meta]) {
      expect(meta.serverSideEncryption).toBe("aws:kms");
      expect(meta.sseKmsKeyId).toBe("arn:aws:kms:us-east-1:000000000000:key/example");
    }
    sse = false;
    const plainHead = await s.head(objectKey("a/b.json"));
    const plainGet = await s.getObject(objectKey("a/b.json"));
    await plainGet.body.cancel();
    for (const meta of [plainHead!, plainGet.meta]) {
      expect("serverSideEncryption" in meta).toBe(false);
      expect("sseKmsKeyId" in meta).toBe(false);
    }
  });
});

describe("S3 put: abort signal and the created version id", () => {
  it("passes the caller's signal to the request and returns x-amz-version-id", async () => {
    const { createS3Store } = await import("../src/s3.js");
    const signals: (AbortSignal | undefined)[] = [];
    const fetchImpl = (async (_url: URL | string, init?: RequestInit) => {
      signals.push(init?.signal ?? undefined);
      return new Response(null, { status: 200, headers: { etag: '"e"', "x-amz-version-id": "v-123" } });
    }) as typeof fetch;
    const s = createS3Store({ bucket: "b", region: "us-east-1", endpoint: "http://localhost:1", credentials: { accessKeyId: "k", secretAccessKey: "s" }, fetch: fetchImpl });
    const controller = new AbortController();
    const ack = await s.put(objectKey("a/v.json"), new Uint8Array([1]), { ifNoneMatch: true, signal: controller.signal });
    expect(signals[0]).toBe(controller.signal);
    expect(ack.version.version).toBe("v-123");
    controller.abort();
    await expect(s.put(objectKey("a/w.json"), new Uint8Array([1]), { signal: controller.signal })).rejects.toThrow("put aborted");
    expect(signals).toHaveLength(1);
  });
});
