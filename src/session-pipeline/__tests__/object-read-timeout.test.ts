/**
 * SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS: the env grammar, the bucket client it configures, and
 * how a timed-out read reaches the stages: the manifest reader rethrows the transient error (not a
 * "missing" null), and a raw chunk read becomes the retryable chunk_unavailable that the search
 * pass rethrows (search-index.ts readChunkRecords and its caller).
 */
import { describe, expect, test } from "bun:test";
import {
  OBJECT_READ_TIMEOUT_MAX_MS,
  OBJECT_READ_TIMEOUT_MIN_MS,
  lambdaEnv,
  objectReadTimeoutFromEnv,
  pipelineStore,
} from "../lambda.ts";
import { readChunkRecords } from "../search-index.ts";
import { isObjectStoreError, objectKey, objectStoreManifestReader } from "../../shared/vendor.ts";

const base = {
  AWS_REGION: "us-east-1",
  PIPELINE_RAW_S3_BUCKET: "b",
  PIPELINE_KEY_PREFIX: "/96bulls/",
  PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn",
  SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};

const credentials = { accessKeyId: "k", secretAccessKey: "s" };

/** Headers at once; the body sends one chunk and then stalls for ever. */
const stallBody = (async () =>
  new Response(
    new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([1, 2, 3]));
      },
    }),
    { status: 200, headers: { etag: '"e"' } },
  )) as unknown as typeof fetch;

/** Never answers, ignores the signal. */
const stallHeaders = (() => new Promise<Response>(() => {})) as unknown as typeof fetch;

describe("SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS", () => {
  test("unset or empty: the env is exactly as without it", () => {
    const plain = lambdaEnv(base);
    expect(Object.keys(plain)).not.toContain("objectReadTimeoutMs");
    expect(lambdaEnv({ ...base, SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS: "" })).toEqual(plain);
  });

  test("an integer of ms within the bounds sets it, with or without the search stage", () => {
    expect(OBJECT_READ_TIMEOUT_MIN_MS).toBe(1_000);
    expect(OBJECT_READ_TIMEOUT_MAX_MS).toBe(299_000);
    for (const search of ["0", "1"])
      for (const v of ["1000", "20000", "299000"])
        expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: search, SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS: v })).toEqual({
          ...lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: search }),
          objectReadTimeoutMs: Number(v),
        });
  });

  test("refuses negative, NaN, fractional, too small, too large and padded values, naming the variable but not the value", () => {
    for (const v of ["-1", "0", "999", "NaN", "abc", "1.5", "1e4", "299001", "300000", "9999999999", " 20000", "+20000", "020000"]) {
      let message = "";
      try {
        lambdaEnv({ ...base, SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS: v });
      } catch (e) {
        message = (e as Error).message;
      }
      expect([v, message]).toEqual([v, "session pipeline: SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS must be an integer from 1000 to 299000"]);
    }
    expect(objectReadTimeoutFromEnv({})).toBeUndefined();
  });
});

describe("pipelineStore", () => {
  test("unset: no signal on any request", async () => {
    const inits: (RequestInit | undefined)[] = [];
    const fetchImpl = (async (_u: URL | string, init?: RequestInit) => {
      inits.push(init);
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;
    const store = pipelineStore({ bucket: "b", region: "us-east-1" }, { credentials, fetch: fetchImpl });
    expect(await store.head(objectKey("p/a.json"))).toBeUndefined();
    expect(inits[0] && "signal" in inits[0]).toBe(false);
  });

  test("set: a stalled HEAD fails transient instead of hanging", async () => {
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallHeaders });
    const error = await store.head(objectKey("p/a.json")).catch((e: unknown) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
  });

  test("set: a manifest read whose body stalls rejects transient, it is not read as missing", async () => {
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallBody });
    const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "b" });
    const error = await reader.read("p/a.json").catch((e: unknown) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
  });

  test("set: a raw chunk read whose body stalls is the retryable chunk_unavailable", async () => {
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallBody });
    const entry = { bytes: 6, stored_bytes: 6, stored_raw_bytes: 6, stored_sha256: "0".repeat(64) } as unknown as Parameters<typeof readChunkRecords>[0];
    const error = (await readChunkRecords(entry, objectKey("p/c.zst"), store, (b) => b).catch((e: unknown) => e)) as { code?: string; retryable?: boolean };
    expect([error.code, error.retryable]).toEqual(["chunk_unavailable", true]);
  });
});
