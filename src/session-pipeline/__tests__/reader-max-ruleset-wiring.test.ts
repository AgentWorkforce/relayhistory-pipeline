/**
 * Reader-first rollout: every sealed-object reader the Lambda and the large-read Job build
 * receives READER_MAX_RULESET, and unset keeps today's legacy reads (no field, no check).
 * No database, socket or AWS call is made: pools are lazy and closed unopened.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import { lambdaEnv, runtimeFromSecret } from "../lambda.ts";
import { largeReadRuntime } from "../large-read-main.ts";
import { produceCanonicalReplay } from "../replay-publication.ts";
import { clonedReplayDatabase, closeReplayTemplate, createFsStore, exportReplayFixture, PGLITE_TEST_TIMEOUT, REPLAY_FIXTURE_IDENTITY as identity, replayDatabase, warmReplayTemplate } from "./replay-fixture.ts";

const base = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
};
// Synthetic, in-memory only: never written to disk, never sent anywhere.
const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

test("lambdaEnv: unset carries no field; a valid value is parsed; a malformed value refuses init", () => {
  expect("readerMaxRuleset" in lambdaEnv({ ...base })).toBe(false);
  expect(lambdaEnv({ ...base, READER_MAX_RULESET: "8" }).readerMaxRuleset).toBe(8);
  expect(lambdaEnv({ ...base, READER_MAX_RULESET: "10" }).readerMaxRuleset).toBe(10);
  for (const bad of ["", "7", "11", "08", "9.0", "nine"])
    expect(() => lambdaEnv({ ...base, READER_MAX_RULESET: bad })).toThrow("READER_MAX_RULESET must be an integer from 8 through 10");
});

for (const [label, value, expected] of [["unset", undefined, undefined], ["set", "9", 9]] as const) {
  test(`runtimeFromSecret (${label}): search and replay readers receive the parsed maximum`, async () => {
    const env = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_REPLAY_PAGES_ENABLED: "1", ...(value === undefined ? {} : { READER_MAX_RULESET: value }) });
    const rt = runtimeFromSecret(env, secret, "synthetic-generation");
    try {
      expect(rt.deps.search?.readerMaxRuleset).toBe(expected);
      expect(rt.deps.replay?.readerMaxRuleset).toBe(expected);
    } finally {
      await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
    }
  });

  test(`largeReadRuntime (${label}): search and replay readers receive the parsed maximum`, async () => {
    const runtime = largeReadRuntime({
      SESSION_PIPELINE_SECRET_JSON: secret, AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls",
      PIPELINE_ORG_FROM_PREFIX: "true", AWS_ACCESS_KEY_ID: "synthetic", AWS_SECRET_ACCESS_KEY: "synthetic",
      ...(value === undefined ? {} : { READER_MAX_RULESET: value }),
    });
    try {
      expect(runtime.deps.search.readerMaxRuleset).toBe(expected);
      expect(runtime.deps.replay.readerMaxRuleset).toBe(expected);
    } finally {
      await runtime.close();
    }
  });
}

test("largeReadRuntime: a malformed maximum refuses before a pool opens", () => {
  expect(() => largeReadRuntime({
    SESSION_PIPELINE_SECRET_JSON: secret, AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true", AWS_ACCESS_KEY_ID: "synthetic", AWS_SECRET_ACCESS_KEY: "synthetic", READER_MAX_RULESET: "7",
  })).toThrow("READER_MAX_RULESET must be an integer from 8 through 10");
});

const roots: string[] = [], dbs: Awaited<ReturnType<typeof replayDatabase>>[] = [];
afterEach(async () => { for (const x of dbs.splice(0)) await x.pg.close(); for (const r of roots.splice(0)) await rm(r, { recursive: true, force: true }); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** Every raw chunk GET reports per-chunk scrub metadata v10; the manifest itself is unchanged. */
function chunksStampedV10(target: ObjectStore, seen: { raw: number }): ObjectStore {
  return new Proxy(target, {
    get(t, key) {
      if (key === "getObject")
        return async (k: unknown, range?: unknown) => {
          const o = await t.getObject(k as never, range as never);
          if (!String(k).includes("/raw/")) return o;
          seen.raw++;
          return { ...o, meta: { ...o.meta, metadata: { ...o.meta.metadata, "scrub-ruleset": "relayhistory-scrub", "scrub-version": "10" } } };
        };
      const value = Reflect.get(t, key);
      return typeof value === "function" ? value.bind(t) : value;
    },
  });
}

for (const [label, maximum, published] of [["unset", undefined, true], ["10", 10, true], ["9", 9, false]] as const) {
  test(`produceCanonicalReplay forwards readerMaxRuleset (${label}) to the canonical raw read`, async () => {
    const x = await clonedReplayDatabase(); dbs.push(x);
    const root = await mkdtemp(join(tmpdir(), "reader-max-")); roots.push(root);
    const fs = createFsStore({ root });
    await exportReplayFixture(fs, x.db, 3);
    const seen = { raw: 0 };
    const deps = { db: x.db, store: chunksStampedV10(fs, seen), provider: "s3", bucket: "replay-fixture", prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" }, ...(maximum === undefined ? {} : { readerMaxRuleset: maximum }) };
    const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, harness: identity.source, sessionId: identity.sessionId };
    if (published) expect((await produceCanonicalReplay(scope, deps)).published).toBe(true);
    else {
      const error = await produceCanonicalReplay(scope, deps).then(() => undefined, (e: unknown) => e) as { name?: string; details?: readonly string[] } | undefined;
      expect(error?.name).toBe("CanonicalReadError");
      expect(error?.details).toContain("scrub_ruleset_unsupported");
    }
    expect(seen.raw).toBeGreaterThan(0);
  }, PGLITE_TEST_TIMEOUT);
}
