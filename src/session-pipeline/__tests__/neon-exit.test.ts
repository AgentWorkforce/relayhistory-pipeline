/**
 * SESSIONS_NEON_EXIT on the session pipeline (src/shared/neon-exit.ts,
 * docs/neon-exit-edge-map.md): the analysis cache and the semantic copier read
 * the legacy readonly source (analysis_source_readonly_url), which has no
 * non-Neon replacement. With the profile on, either flag refuses startup, and
 * with both off the source pool is never constructed even when the secret
 * carries the URL. pg.Pool is wrapped to record every construction and connect.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { analysisDeps, handler, lambdaEnv, runtimeFromSecret, semanticCopyDeps } from "../lambda.ts";
import { processBatch } from "../consumer.ts";
import { semanticCopyMain } from "../../search-features/semantic-copy-main.ts";

const POISON = "postgres://poison:poison@neon.invalid:5432/poison";
const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q" };
const ch = { url: "https://ch.invalid", user: "u", password: "p", database: "d" };
const secret = JSON.stringify({ hoopdb_url: "postgres://u:p@127.0.0.1:1/db", clickhouse: ch, analysis_source_readonly_url: POISON });

const RealPool = pg.Pool;
const realFetch = globalThis.fetch;
let pools: string[] = [], connects: string[] = [], fetches: string[] = [];
const savedEnv = { ...process.env };

beforeEach(() => {
  pools = []; connects = []; fetches = [];
  // Every module reads pg.Pool at call time from the shared default export.
  pg.Pool = class extends RealPool {
    constructor(config?: pg.PoolConfig) { super(config); pools.push(String(config?.connectionString ?? "")); }
    override connect(...args: unknown[]): never { connects.push("connect"); void args; throw new Error("pool connect refused in tests"); }
    override query(...args: unknown[]): never { connects.push("query"); void args; throw new Error("pool query refused in tests"); }
  } as unknown as typeof pg.Pool;
  globalThis.fetch = Object.assign(async (input: unknown) => { fetches.push(String(input instanceof Request ? input.url : input)); throw new Error("poisoned fetch"); }, { preconnect: () => {} }) as typeof fetch;
});
afterEach(() => {
  pg.Pool = RealPool;
  globalThis.fetch = realFetch;
  for (const k of Object.keys(process.env)) if (!(k in savedEnv)) delete process.env[k];
  Object.assign(process.env, savedEnv);
});
const neonPools = () => pools.filter((url) => url.includes("neon.invalid"));
const message = (work: () => unknown) => { try { work(); return ""; } catch (e) { return (e as Error).message; } };

describe("SESSIONS_NEON_EXIT=true refuses the readonly-source flags at startup", () => {
  test("lambdaEnv names the variable and the flag; no host check is an escape", () => {
    expect(message(() => lambdaEnv({ ...base, SESSIONS_NEON_EXIT: "true", SESSION_ANALYSIS_CACHE_ENABLED: "1" }))).toBe("96bulls: SESSIONS_NEON_EXIT=true refuses the session pipeline: SESSION_ANALYSIS_CACHE_ENABLED=1");
    expect(message(() => lambdaEnv({ ...base, SESSIONS_NEON_EXIT: "true", SESSION_SEMANTIC_COPY_ENABLED: "1", SESSION_SEARCH_FEATURE_SOURCE_ID: "legacy-main" }))).toBe("96bulls: SESSIONS_NEON_EXIT=true refuses the session pipeline: SESSION_SEMANTIC_COPY_ENABLED=1");
    expect(message(() => lambdaEnv({ ...base, SESSIONS_NEON_EXIT: "yes" }))).toContain("SESSIONS_NEON_EXIT must be");
    expect(lambdaEnv({ ...base, SESSIONS_NEON_EXIT: "true" })).toMatchObject({ neonExit: true, analysisCacheEnabled: false, semanticCopyEnabled: false });
  });

  test("handler: each invocation kind refuses before any secret lookup or pool, and a retry refuses again", async () => {
    Object.assign(process.env, base, { SESSIONS_NEON_EXIT: "true", SESSION_SEMANTIC_COPY_ENABLED: "1", SESSION_ANALYSIS_CACHE_ENABLED: "1" });
    // Error, then the retried invocation: a failed init is not cached, it refuses again.
    expect(await handler({ semanticCopy: true })).toEqual({ ok: false });
    expect(await handler({ semanticCopy: true })).toEqual({ ok: false });
    expect(await handler({ analysisCache: true })).toEqual({ ok: false });
    await expect(handler({ analysisCacheReconcile: true })).rejects.toThrow("analysis cache reconcile failed");
    // SQS: nothing processed, every message goes back to the queue for redelivery.
    expect(await handler({ Records: [{ messageId: "m1", body: "{}" }, { messageId: "m2", body: "{}" }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }, { itemIdentifier: "m2" }] });
    expect(fetches).toEqual([]);
    expect(pools).toEqual([]);
    expect(connects).toEqual([]);
  });

  test("the standalone semantic copier refuses before its factory runs", async () => {
    let calls = 0;
    const factory = () => { calls++; throw new Error("factory must not run"); };
    await expect(semanticCopyMain({ SESSIONS_NEON_EXIT: "true", SESSION_SEMANTIC_COPY_ENABLED: "1" }, factory)).rejects.toThrow("96bulls: SESSIONS_NEON_EXIT=true refuses the semantic copier: SESSION_SEMANTIC_COPY_ENABLED=1");
    // The real default factory is not reached either: no pool from the poisoned secret.
    await expect(semanticCopyMain({ SESSIONS_NEON_EXIT: "true", SESSION_SEMANTIC_COPY_ENABLED: "1", SESSION_SEARCH_FEATURE_SOURCE_ID: "legacy-main", SESSION_PIPELINE_SECRET_JSON: secret })).rejects.toThrow("SESSIONS_NEON_EXIT");
    expect(await semanticCopyMain({ SESSIONS_NEON_EXIT: "true" }, factory)).toEqual({ enabled: false });
    expect(calls).toBe(0);
    expect(pools).toEqual([]);
  });
});

describe("SESSIONS_NEON_EXIT=true with both flags off never builds the readonly source", () => {
  test("a poisoned analysis_source_readonly_url is dropped; SQS success and error paths run; no source pool or fetch", async () => {
    const env = lambdaEnv({ ...base, SESSIONS_NEON_EXIT: "true", SEARCH_INDEX_ENABLED: "1" });
    const rt = runtimeFromSecret(env, secret, "fixture-generation");
    expect(rt.analysisSourceUrl).toBeUndefined();
    expect(rt.semanticSecret).toBeUndefined();
    expect(message(() => analysisDeps(rt))).toBe("analysis cache runtime unavailable");
    expect(message(() => semanticCopyDeps(rt))).toBe("semantic copier unavailable");
    // Even a runtime whose flag were forced on past lambdaEnv has no URL to build from.
    expect(message(() => analysisDeps({ ...rt, env: { ...env, analysisCacheEnabled: true } }))).toMatch(/analysis_source_readonly_url is missing/);
    // The shared runtime still serves SQS: a test event is acked (success), an unreadable body is failed for redelivery (error/retry).
    expect(await processBatch([{ messageId: "ok", body: JSON.stringify({ Event: "s3:TestEvent" }) }, { messageId: "bad", body: "not json" }], rt.deps)).toEqual({ batchItemFailures: [{ itemIdentifier: "bad" }] });
    // The scheduled branches are disabled before init.
    Object.assign(process.env, base, { SESSIONS_NEON_EXIT: "true" });
    expect(await handler({ analysisCache: true })).toEqual({ ok: true });
    expect(await handler({ analysisCacheReconcile: true })).toEqual({ ok: true });
    expect(await handler({ semanticCopy: true })).toEqual({ ok: true });
    expect(neonPools()).toEqual([]);
    expect(pools).toEqual(["postgres://u:p@127.0.0.1:1/db"]);
    expect(connects).toEqual([]);
    expect(fetches).toEqual([]);
  });
});

describe("the default (SESSIONS_NEON_EXIT unset or false) is unchanged", () => {
  test("the flags still build the readonly source from the secret (constructed, never connected here)", async () => {
    for (const flag of [undefined, "false"]) {
      pools = [];
      const env = lambdaEnv({ ...base, SESSIONS_NEON_EXIT: flag, SEARCH_INDEX_ENABLED: "1", SESSION_ANALYSIS_CACHE_ENABLED: "1", SESSION_SEMANTIC_COPY_ENABLED: "1", SESSION_SEARCH_FEATURE_SOURCE_ID: "legacy-main" });
      expect(env.neonExit).toBeUndefined();
      const rt = runtimeFromSecret(env, secret, "fixture-generation");
      expect(rt.analysisSourceUrl).toBe(POISON);
      analysisDeps(rt);
      semanticCopyDeps(rt);
      expect(neonPools()).toHaveLength(2);
      let calls = 0;
      expect(await semanticCopyMain({ SESSIONS_NEON_EXIT: flag, SESSION_SEMANTIC_COPY_ENABLED: "1" }, () => { calls++; return { run: async () => ({ published: 0 }) as never, close: async () => {} }; })).toMatchObject({ enabled: true });
      expect(calls).toBe(1);
    }
    expect(connects).toEqual([]);
    expect(fetches).toEqual([]);
  });
});
