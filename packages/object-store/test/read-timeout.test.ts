import { afterEach, describe, expect, it, vi } from "vitest";
import { readAll, send } from "../src/core.js";
import { isObjectStoreError } from "../src/errors.js";
import { objectKey } from "../src/key.js";
import { createS3Store } from "../src/s3.js";

const base = {
  bucket: "b",
  region: "us-east-1",
  endpoint: "http://localhost:1",
  credentials: { accessKeyId: "k", secretAccessKey: "s" },
};

type Seen = { method: string; init: RequestInit | undefined };

/** A fake fetch that records each call and answers with `answer`. */
function recording(
  answer: (init: RequestInit | undefined) => Promise<Response>,
) {
  const seen: Seen[] = [];
  const fetchImpl = (async (_url: URL | string, init?: RequestInit) => {
    seen.push({ method: init?.method ?? "GET", init });
    return answer(init);
  }) as typeof fetch;
  return { seen, fetchImpl };
}

/** Never answers and ignores the abort signal: only the race in `send` can end the call. */
const stallHeadersIgnoringSignal = () => new Promise<Response>(() => {});

/** Never answers until aborted, then rejects as a real fetch does. */
const stallHeadersHonouringSignal = (init: RequestInit | undefined) =>
  new Promise<Response>((_, reject) => {
    init?.signal?.addEventListener("abort", () =>
      reject(new DOMException("aborted", "AbortError")),
    );
  });

/** Headers at once; the body sends one chunk and then stalls for ever. */
const stallBody = async () =>
  new Response(
    new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([1, 2, 3]));
      },
    }),
    { status: 200, headers: { etag: '"e"', "content-length": "6" } },
  );

const transientTimeout = (error: unknown) =>
  isObjectStoreError(error, "transient") &&
  error.retryable &&
  /request timed out after 50 ms/.test(error.message);

afterEach(() => {
  vi.useRealTimers();
});

describe("object read deadline: unset", () => {
  it("send passes init through untouched and returns the fetch's own response", async () => {
    const response = new Response("x");
    const init = { method: "GET" };
    const { seen, fetchImpl } = recording(async () => response);
    const got = await send("s3", "k", "http://localhost:1/k", init, fetchImpl);
    expect(got).toBe(response);
    expect(seen[0]!.init).toBe(init);
  });

  it("an S3 store without readTimeoutMs sends no signal on GET, HEAD or PUT and returns the body stream as is", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([7]));
        c.close();
      },
    });
    const { seen, fetchImpl } = recording(async (init) =>
      init?.method === "GET"
        ? new Response(body, { status: 200, headers: { etag: '"e"' } })
        : new Response(null, { status: 200, headers: { etag: '"e"' } }),
    );
    const store = createS3Store({ ...base, fetch: fetchImpl });
    const got = await store.getObject(objectKey("a/b.json"));
    expect(got.body).toBe(body);
    await store.head(objectKey("a/b.json"));
    await store.put(objectKey("a/b.json"), new Uint8Array([1]));
    for (const s of seen) expect(s.init && "signal" in s.init).toBe(false);
  });
});

describe("object read deadline: set", () => {
  it("HEAD stalled on headers (fetch ignores the signal) fails transient", async () => {
    const { seen, fetchImpl } = recording(stallHeadersIgnoringSignal);
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    const error = await store.head(objectKey("a/b.json")).catch((e) => e);
    expect(transientTimeout(error)).toBe(true);
    expect(seen[0]!.init?.signal?.aborted).toBe(true);
  });

  it("GET stalled on headers (fetch honours the signal) fails transient", async () => {
    const { fetchImpl } = recording(stallHeadersHonouringSignal);
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    const error = await store.getObject(objectKey("a/b.json")).catch((e) => e);
    expect(transientTimeout(error)).toBe(true);
  });

  it("GET whose body stalls after the headers fails the body read transient", async () => {
    const { fetchImpl } = recording(stallBody);
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    const object = await store.getObject(objectKey("a/b.json"));
    const error = await readAll(object.body).catch((e) => e);
    expect(transientTimeout(error)).toBe(true);
    const viaGet = await readAll(await store.get(objectKey("a/b.json"))).catch(
      (e) => e,
    );
    expect(transientTimeout(viaGet)).toBe(true);
  });

  it("a GET that finishes in time returns the same bytes, headers and status, and stops its timer", async () => {
    vi.useFakeTimers();
    const { fetchImpl } = recording(
      async () =>
        new Response(new Uint8Array([4, 5, 6]), {
          status: 206,
          headers: { etag: '"e"', "content-range": "bytes 0-2/9" },
        }),
    );
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    const object = await store.getObject(objectKey("a/b.json"), {
      start: 0,
      end: 3,
    });
    expect(object.meta.size).toBe(9);
    expect(object.meta.version.eTag).toBe('"e"');
    expect(vi.getTimerCount()).toBe(1);
    expect([...(await readAll(object.body))]).toEqual([4, 5, 6]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a cancelled body and a bodiless HEAD stop the timer", async () => {
    vi.useFakeTimers();
    const { fetchImpl } = recording(async (init) =>
      init?.method === "HEAD"
        ? new Response(null, { status: 404 })
        : stallBody(),
    );
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    expect(await store.head(objectKey("a/b.json"))).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    const object = await store.getObject(objectKey("a/b.json"));
    expect(vi.getTimerCount()).toBe(1);
    await object.body.cancel();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a network failure before the deadline is still 'request failed', transient", async () => {
    const { fetchImpl } = recording(async () => {
      throw new TypeError("connection reset");
    });
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    const error = await store.head(objectKey("a/b.json")).catch((e) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
    expect(error.message).toMatch(/request failed/);
  });

  it("a network failure stops the timer at once", async () => {
    vi.useFakeTimers();
    const { fetchImpl } = recording(async () => {
      throw new TypeError("connection reset");
    });
    const error = await send(
      "s3",
      "k",
      "http://localhost:1/k",
      { method: "GET" },
      fetchImpl,
      50,
    ).catch((e) => e);
    expect(isObjectStoreError(error, "transient")).toBe(true);
    expect(error.message).toMatch(/request failed/);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([204, 205, 304])(
    "a %i response is returned as it came and stops the timer",
    async (status) => {
      vi.useFakeTimers();
      // A fetch could hand back a body with a null-body status; the Response
      // constructor refuses to build one, so the status is set afterwards.
      const response = new Response("x", { status: 200 });
      Object.defineProperty(response, "status", { value: status });
      const { fetchImpl } = recording(async () => response);
      const got = await send(
        "s3",
        "k",
        "http://localhost:1/k",
        { method: "GET" },
        fetchImpl,
        50,
      );
      expect(got).toBe(response);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("PUT, list and delete carry no signal and are not cut off", async () => {
    const { seen, fetchImpl } = recording(async (init) =>
      init?.method === "GET"
        ? new Response(
            "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            { status: 200 },
          )
        : new Response(null, { status: 200, headers: { etag: '"e"' } }),
    );
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    await store.put(objectKey("a/b.json"), new Uint8Array([1]));
    await store.list(objectKey("a"));
    await store.delete(objectKey("a/b.json"));
    expect(seen.length).toBeGreaterThanOrEqual(3);
    for (const s of seen) expect(s.init && "signal" in s.init).toBe(false);
  });

  it("a timed-out call carries transportCause 'timeout'; a network failure keeps its own class", async () => {
    const stalled = recording(stallHeadersIgnoringSignal);
    const timedOut = await createS3Store({
      ...base,
      fetch: stalled.fetchImpl,
      readTimeoutMs: 50,
    })
      .head(objectKey("a/b.json"))
      .catch((e) => e);
    expect(transientTimeout(timedOut)).toBe(true);
    expect(timedOut.transportCause).toBe("timeout");
    const reset = Object.assign(new TypeError("fetch failed"), {
      code: "ECONNRESET",
    });
    const failing = recording(async () => {
      throw reset;
    });
    const failed = await createS3Store({
      ...base,
      fetch: failing.fetchImpl,
      readTimeoutMs: 50,
    })
      .head(objectKey("a/b.json"))
      .catch((e) => e);
    expect(failed.message).toMatch(/request failed/);
    expect(failed.transportCause).toBe("econnreset");
    expect(failed.cause).toBe(reset);
  });

  it("send with a deadline still honours the caller's own signal (request failed, not timed out)", async () => {
    const caller = new AbortController();
    const { seen, fetchImpl } = recording(stallHeadersHonouringSignal);
    const pending = send(
      "s3",
      "k",
      "http://localhost:1/k",
      { method: "GET", signal: caller.signal },
      fetchImpl,
      60_000,
    ).catch((e) => e);
    await new Promise((r) => setTimeout(r, 5));
    caller.abort();
    const error = await pending;
    expect(isObjectStoreError(error, "transient")).toBe(true);
    expect(error.message).toMatch(/request failed/);
    expect(error.transportCause).toBe("abort");
    expect(seen[0]!.init?.signal).not.toBe(caller.signal);
    expect(seen[0]!.init?.signal?.aborted).toBe(true);
  });

  it("a put's caller signal still reaches the fetch with readTimeoutMs set, and only that signal", async () => {
    const caller = new AbortController();
    const { seen, fetchImpl } = recording(
      async () => new Response(null, { status: 200, headers: { etag: '"e"' } }),
    );
    const store = createS3Store({
      ...base,
      fetch: fetchImpl,
      readTimeoutMs: 50,
    });
    await store.put(objectKey("a/b.json"), new Uint8Array([1]), {
      signal: caller.signal,
    });
    expect(seen).toHaveLength(1);
    expect(seen[0]!.init?.signal).toBe(caller.signal);
  });

  it("refuses a readTimeoutMs that is not a positive integer", () => {
    for (const bad of [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      2 ** 53,
    ])
      expect(
        () => createS3Store({ ...base, readTimeoutMs: bad }),
        String(bad),
      ).toThrow(/readTimeoutMs must be a positive integer/);
  });
});
