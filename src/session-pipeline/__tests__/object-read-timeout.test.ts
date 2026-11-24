/**
 * SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS: the env grammar, the bucket client it configures, and
 * how a timed-out read reaches the stages: the manifest reader rethrows the transient error (not a
 * "missing" null), and a raw chunk read becomes the retryable chunk_unavailable
 * (canonical-projection.ts streamManifestRecords) that the search pass rethrows.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { MS_KNOB_MAX, MS_KNOB_MIN, lambdaEnv, objectReadTimeoutFromEnv, pipelineStore } from "../lambda.ts";
import { readManifestRecords } from "../../session-read-api/canonical-projection.ts";
import { isObjectStoreError, objectKey, objectStoreManifestReader } from "../../shared/vendor.ts";
import { SCOPE, decompress, exportStreamScope, generate, prefixFor, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";

const base = {
  AWS_REGION: "us-east-1",
  PIPELINE_RAW_S3_BUCKET: "b",
  PIPELINE_KEY_PREFIX: "/96bulls/",
  PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn",
  SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};
const VAR = "SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS";
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

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0)) await c();
});

describe(VAR, () => {
  test("unset or empty: the env is exactly as without it", () => {
    const plain = lambdaEnv(base);
    expect(Object.keys(plain)).not.toContain("objectReadTimeoutMs");
    const empty = lambdaEnv({ ...base, [VAR]: "" });
    expect(Object.keys(empty)).toEqual(Object.keys(plain));
    expect(JSON.parse(JSON.stringify(empty))).toEqual(JSON.parse(JSON.stringify(plain)));
    expect(objectReadTimeoutFromEnv({})).toBeUndefined();
    expect(objectReadTimeoutFromEnv({ [VAR]: "" })).toBeUndefined();
  });

  test("an integer of ms within the bounds sets it, with or without the search stage, as the last key", () => {
    expect([MS_KNOB_MIN, MS_KNOB_MAX]).toEqual([1_000, 299_000]);
    for (const search of ["0", "1"])
      for (const v of ["1000", "20000", "299000"]) {
        const off = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: search });
        const on = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: search, [VAR]: v });
        expect(on.objectReadTimeoutMs).toBe(Number(v));
        expect(Object.keys(on)).toEqual([...Object.keys(off), "objectReadTimeoutMs"]);
        expect(JSON.parse(JSON.stringify({ ...on, objectReadTimeoutMs: undefined, adapterEnv: undefined }))).toEqual(
          JSON.parse(JSON.stringify({ ...off, adapterEnv: undefined })),
        );
      }
  });

  test("refuses negative, NaN, fractional, too small, too large and padded values, naming the variable but not the value", () => {
    for (const v of ["-1", "0", "999", "NaN", "abc", "1.5", "1e4", "299001", "300000", "9999999999", " 20000", "+20000", "020000"]) {
      let message = "";
      try {
        lambdaEnv({ ...base, [VAR]: v });
      } catch (e) {
        message = (e as Error).message;
      }
      expect([v, message]).toEqual([v, `session pipeline: ${VAR} must be an integer from 1000 to 299000`]);
    }
  });
});

describe("pipelineStore", () => {
  test("unset: no signal on any request, and the fetch's own body stream comes back", async () => {
    const inits: (RequestInit | undefined)[] = [];
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([7]));
        c.close();
      },
    });
    const fetchImpl = (async (_u: URL | string, init?: RequestInit) => {
      inits.push(init);
      return init?.method === "GET" ? new Response(body, { status: 200, headers: { etag: '"e"' } }) : new Response(null, { status: 404 });
    }) as unknown as typeof fetch;
    const store = pipelineStore({ bucket: "b", region: "us-east-1" }, { credentials, fetch: fetchImpl });
    expect(await store.head(objectKey("p/a.json"))).toBeUndefined();
    expect((await store.getObject(objectKey("p/a.json"))).body).toBe(body);
    expect(inits).toHaveLength(2);
    for (const init of inits) expect(init && "signal" in init).toBe(false);
  });

  test("set: a stalled HEAD fails transient instead of hanging", async () => {
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallHeaders });
    const error = await store.head(objectKey("p/a.json")).catch((e: unknown) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
    expect((error as { transportCause?: string }).transportCause).toBe("timeout");
  });

  test("set: a manifest read whose body stalls rejects transient, it is not read as missing", async () => {
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallBody });
    const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "b" });
    const error = await reader.read("p/a.json").catch((e: unknown) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
  });

  test("set: a raw chunk read whose body stalls is the retryable chunk_unavailable", async () => {
    const x: ExportedScope = await exportStreamScope(generate(1));
    cleanup.push(() => x.dispose());
    const { manifest } = (await x.reader.read(x.keys[0]!))!;
    const store = pipelineStore({ bucket: "b", region: "us-east-1", objectReadTimeoutMs: 50 }, { credentials, fetch: stallBody });
    const error = (await readManifestRecords(manifest, store, prefixFor(SCOPE.orgId), decompress).catch((e: unknown) => e)) as {
      code?: string;
      retryable?: boolean;
    };
    expect([error.code, error.retryable]).toEqual(["chunk_unavailable", true]);
  });
});
