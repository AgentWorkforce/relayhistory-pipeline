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
import type { SearchComposition } from "../../shared/search-composition.ts";

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
