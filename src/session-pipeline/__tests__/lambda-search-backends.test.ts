/**
 * The shipped Lambda artifact entries refuse SEARCH_INDEX_BACKEND=opensearch.
 * An entry built on a composition allows every name the composition's registry
 * holds, so the ABI's exported registry (src/lambda-artifact/composition.ts)
 * must stay postgres-only; opensearch is reachable only through the explicitly
 * named createOpenSearchCapableSearchIndexRegistry, which the ABI does not export.
 */
import { describe, expect, test } from "bun:test";
import { createOpenSearchCapableSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import * as pipelineEntry from "../../lambda-artifact/pipeline-entry.ts";
import * as readApiEntry from "../../lambda-artifact/read-api-entry.ts";
import { composeSearchService, type SearchComposition } from "../../shared/search-composition.ts";
import { invocationDeps } from "../lambda.ts";

const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q" };
const opensearch = { SEARCH_INDEX_BACKEND: "opensearch", SEARCH_INDEX_OPENSEARCH_URL: "http://127.0.0.1:9200" };
// Never parsed: the backend check runs first.
const secret = JSON.stringify({ supabase_url: "postgres://u:p@127.0.0.1:1/db" });

/** The compositions a shipped entry is built with: none (the default handler),
 * the in-repo postgres composition, and one assembled from the ABI's registry export. */
const shipped: [string, SearchComposition | undefined][] = [
  ["no composition (default handler)", undefined],
  ["createPostgresSearchComposition", pipelineEntry.createPostgresSearchComposition({ backendId: "postgres", fromMs: 0 })],
  ["ABI createSearchIndexRegistry", { registry: pipelineEntry.createSearchIndexRegistry(), state: () => { throw new Error("unused"); } }],
];

describe("shipped Lambda entries refuse SEARCH_INDEX_BACKEND=opensearch", () => {
  test("the ABI exports a postgres-only registry and no opensearch-capable one", () => {
    expect(pipelineEntry.createSearchIndexRegistry().registered()).toEqual(["postgres"]);
    expect(readApiEntry.createSearchIndexRegistry().registered()).toEqual(["postgres"]);
    expect("createOpenSearchCapableSearchIndexRegistry" in pipelineEntry).toBe(false);
    expect("createOpenSearchCapableSearchIndexRegistry" in readApiEntry).toBe(false);
  });

  for (const [name, composition] of shipped) {
    test(`session pipeline lambdaEnv: ${name}`, () => {
      expect(() => pipelineEntry.lambdaEnv({ ...base, ...opensearch }, composition)).toThrow(/not allowed for this entry point/);
      expect(pipelineEntry.lambdaEnv({ ...base }, composition).searchBackend).toBe("clickhouse");
    });

    test(`session read api env: ${name}`, async () => {
      await expect(readApiEntry.readAppFromSecret(secret, { ...opensearch }, composition)).rejects.toThrow(/not allowed for this entry point/);
    });
  }

  test("control: an explicitly opensearch-capable composition is what widens the allow list", async () => {
    const capable: SearchComposition = { registry: createOpenSearchCapableSearchIndexRegistry(), state: () => { throw new Error("composition reached"); } };
    expect(pipelineEntry.lambdaEnv({ ...base, ...opensearch }, capable).searchBackend).toBe("opensearch");
    // Past backend selection the read api reaches its reader-role check against the unreachable database.
    const error = await readApiEntry.readAppFromSecret(secret, { ...opensearch, READ_API_COMPLETE_SINCE: "epoch" }, capable).then(() => null, (e: Error) => e);
    expect(error?.message).not.toContain("not allowed");
  });
});

describe("SEARCH_INDEX_STREAMING_CH on the hosted Lambda (default off)", () => {
  const MiB = 1024 * 1024;
  const synthetic = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "https://ch.invalid", user: "u", password: "p", database: "d" } });
  const on = { SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1" };
  const refusal = (env: Record<string, string>, composition?: SearchComposition) => {
    try { pipelineEntry.lambdaEnv({ ...base, ...env }, composition); } catch (error) { return (error as Error).message; }
    return "";
  };

  test("unset, empty or 0 arms nothing; the self-host SEARCH_INDEX_STREAMING_ENABLED is ignored", () => {
    for (const flag of [{}, { SEARCH_INDEX_STREAMING_CH: "" }, { SEARCH_INDEX_STREAMING_CH: "0" }, { SEARCH_INDEX_STREAMING_ENABLED: "true" }, { SEARCH_INDEX_STREAMING_ENABLED: "garbage" }])
      expect(pipelineEntry.lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", ...flag }).searchStreamCh).toBeUndefined();
    // Off: the knobs are never read, even malformed, even without SEARCH_INDEX_ENABLED.
    expect(pipelineEntry.lambdaEnv({ ...base, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "x", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "1" }).searchStreamCh).toBeUndefined();
  });

  test("armed: the parsed budgets and margin; dependencies refused at startup with a clear error", () => {
    expect(pipelineEntry.lambdaEnv({ ...base, ...on }).searchStreamCh).toEqual({ maxReadBytes: 1024 * MiB, maxReadEvents: 1_000_000, deadlineMarginMs: 60_000 });
    expect(pipelineEntry.lambdaEnv({ ...base, ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "512MiB", SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "1500000", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "90000" }).searchStreamCh)
      .toEqual({ maxReadBytes: 512 * MiB, maxReadEvents: 1_500_000, deadlineMarginMs: 90_000 });
    expect(refusal({ SEARCH_INDEX_STREAMING_CH: "1" })).toBe("session pipeline: SEARCH_INDEX_STREAMING_CH=1 needs SEARCH_INDEX_ENABLED=1");
    expect(refusal({ ...on, SEARCH_INDEX_BACKEND: "postgres" }, shipped[1]![1])).toBe("session pipeline: SEARCH_INDEX_STREAMING_CH=1 needs the clickhouse search backend");
    expect(refusal({ ...on, SEARCH_INDEX_OUTPUT_FACTS: "1" })).toBe("session pipeline: SEARCH_INDEX_STREAMING_CH=1 cannot be combined with SEARCH_INDEX_OUTPUT_FACTS=1");
    expect(refusal({ ...on, SEARCH_INDEX_STREAMING_CH: "true" })).toBe("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
    expect(refusal({ ...on, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "5000" })).toStartWith("session pipeline: SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS must be");
  });

  test("the new names forwarded to a portable adapter through adapterEnv change nothing", () => {
    const composition = shipped[1]![1]!;
    const knobs = { SEARCH_INDEX_STREAMING_CH: "0", SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "2GiB", SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "2000000", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "60000" };
    const plain = pipelineEntry.lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_BACKEND: "postgres" }, composition);
    const forwarded = pipelineEntry.lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_BACKEND: "postgres", ...knobs }, composition);
    expect(forwarded.adapterEnv).toMatchObject(knobs);
    const { adapterEnv: a, ...restPlain } = plain;
    const { adapterEnv: b, ...restForwarded } = forwarded;
    expect(restForwarded).toEqual(restPlain);
    void a; void b;
    const sql = { query: async () => ({ rows: [] }) } as never;
    const bind = (env: typeof plain) => composeSearchService(composition, { env: { ...env.adapterEnv, SEARCH_INDEX_BACKEND: env.searchBackend }, sql }, "writer");
    expect(bind(forwarded).index.capabilities()).toEqual(bind(plain).index.capabilities());
    expect(Object.keys(bind(forwarded).state).sort()).toEqual(Object.keys(bind(plain).state).sort());
  });

  test("flags unset: the search deps carry no stream and every invocation keeps the shared deps; armed: a per-invocation deadline", () => {
    const off = pipelineEntry.runtimeFromSecret(pipelineEntry.lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" }), synthetic, "fixture-generation");
    expect(off.deps.search).toBeDefined();
    expect("stream" in off.deps.search!).toBe(false);
    expect("streamDeadline" in off.deps.search!).toBe(false);
    const context = { getRemainingTimeInMillis: () => 900_000 };
    expect(invocationDeps(off, context)).toBe(off.deps);

    const armed = pipelineEntry.runtimeFromSecret(pipelineEntry.lambdaEnv({ ...base, ...on, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "45000" }), synthetic, "fixture-generation");
    expect(armed.deps.search!.stream).toEqual({ maxReadBytes: 1024 * MiB, maxReadEvents: 1_000_000 });
    expect("streamDeadline" in armed.deps.search!).toBe(false); // never on the shared deps
    const per = invocationDeps(armed, context, () => 1_000_000);
    expect(per.search!.streamDeadline).toEqual({ at: 1_000_000 + 900_000 - 45_000 });
    expect(per.search!.stream).toBe(armed.deps.search!.stream);
    expect("streamDeadline" in armed.deps.search!).toBe(false);
    // No usable context (a direct invoke or a test harness): no deadline.
    expect(invocationDeps(armed)).toBe(armed.deps);
    expect(invocationDeps(armed, {} as never)).toBe(armed.deps);
    expect(invocationDeps(armed, { getRemainingTimeInMillis: () => Number.NaN })).toBe(armed.deps);
  });
});
