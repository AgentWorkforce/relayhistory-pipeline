/**
 * SEARCH_INDEX_STREAMING_CH in the Lambda: parsed strictly, refused without its
 * dependencies, and per invocation only a deadline and a one-scope slot. Unset, the
 * runtime is exactly as before.
 */
import { describe, expect, test } from "bun:test";
import { handler, invocationDeps, lambdaEnv } from "../lambda.ts";
import type { ConsumerDeps } from "../consumer.ts";
import { SEARCH_INDEX_STREAM_CH_BUDGET, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS, type SearchIndexDeps } from "../search-index.ts";

const base = {
  AWS_REGION: "us-east-1",
  PIPELINE_RAW_S3_BUCKET: "b",
  PIPELINE_KEY_PREFIX: "/96bulls/",
  PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn",
  SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};

describe("SEARCH_INDEX_STREAMING_CH", () => {
  test("unset, empty or 0: the env is exactly as without it", () => {
    const plain = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" });
    expect(Object.keys(plain)).not.toContain("searchStreamCh");
    for (const value of ["", "0"]) expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: value })).toEqual(plain);
  });

  test("1 with SEARCH_INDEX_ENABLED=1 arms it", () => {
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1" })).toEqual({ ...lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" }), searchStreamCh: true });
  });

  test("refuses any other value, and 1 without SEARCH_INDEX_ENABLED=1", () => {
    for (const value of ["true", "2", "on", " 1"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: value })).toThrow("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
    for (const enabled of [undefined, "0", "true"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: enabled, SEARCH_INDEX_STREAMING_CH: "1" })).toThrow("session pipeline: SEARCH_INDEX_STREAMING_CH=1 needs SEARCH_INDEX_ENABLED=1");
  });

  test("armed, init refuses a secret without the clickhouse block; nothing in the batch is processed", async () => {
    const saved = { ...process.env };
    const realFetch = globalThis.fetch;
    let calls = 0;
    try {
      Object.assign(process.env, base, { SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1", AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test" });
      // The only call init makes before the check: GetSecretValue, answered here (no network).
      globalThis.fetch = (async () => {
        calls++;
        return new Response(JSON.stringify({ SecretString: JSON.stringify({ hoopdb_url: "postgresql://u:p@127.0.0.1:1/db" }) }), { status: 200 });
      }) as unknown as typeof fetch;
      const out = await handler({ Records: [{ messageId: "m1", body: "{}" }, { messageId: "m2", body: "{}" }] }, { getRemainingTimeInMillis: () => 300_000 });
      expect(out).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }, { itemIdentifier: "m2" }] });
      expect(calls).toBe(1);
    } finally {
      globalThis.fetch = realFetch;
      for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
      Object.assign(process.env, saved);
    }
  });
});

describe("invocationDeps", () => {
  const search = { ch: {} } as unknown as SearchIndexDeps;
  const deps = (s?: SearchIndexDeps): ConsumerDeps => ({ pointer: {} as ConsumerDeps["pointer"], ...(s ? { search: s } : {}) });

  test("unarmed: the runtime deps themselves, untouched", () => {
    const d = deps(search);
    expect(invocationDeps(d, { getRemainingTimeInMillis: () => 1 })).toBe(d);
    const none = deps();
    expect(invocationDeps(none)).toBe(none);
  });

  test("armed: a fresh slot per invocation and the deadline at remaining time less the margin", () => {
    const d = deps({ ...search, stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET } });
    const one = invocationDeps(d, { getRemainingTimeInMillis: () => 300_000 }, () => 1_000);
    expect(one.search).toMatchObject({ streamSlot: { used: false }, streamDeadline: { at: 1_000 + 300_000 - SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS } });
    const two = invocationDeps(d, { getRemainingTimeInMillis: () => 300_000 }, () => 1_000);
    expect(two.search!.streamSlot).not.toBe(one.search!.streamSlot);
    expect(d.search).not.toHaveProperty("streamSlot");
    // No context: the slot still holds, no deadline.
    const bare = invocationDeps(d);
    expect(bare.search).toMatchObject({ streamSlot: { used: false } });
    expect(bare.search).not.toHaveProperty("streamDeadline");
  });

  test("the budgets: four passes over at most 256 MiB / 250,000 records", () => {
    expect(SEARCH_INDEX_STREAM_CH_BUDGET).toEqual({ maxReadBytes: 1024 * 1024 * 1024, maxReadEvents: 1_000_000 });
    expect(SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS).toBe(60_000);
  });
});
