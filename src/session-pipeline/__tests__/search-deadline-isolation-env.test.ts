// SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED in the hosted Lambda: grammar, prerequisites and wiring. Synthetic values only.
import { describe, expect, test } from "bun:test";
import { lambdaEnv, runtimeFromSecret, searchIndexDeadlineIsolationFromEnv } from "../lambda.ts";

describe("SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", SEARCH_INDEX_ENABLED: "1",
  };
  const ready = { ...base, SEARCH_INDEX_STREAMING_CH: "1", SESSION_LARGE_READ_HANDOFF_ENABLED: "1" };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

  test("unset, empty or false: env and deps exactly as without it; true arms deps.deadlineIsolation", async () => {
    const { adapterEnv: _a, ...plainEnv } = lambdaEnv(ready);
    for (const [value, armed] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const { adapterEnv, ...env } = lambdaEnv({ ...ready, ...(value === undefined ? {} : { SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED: value }) });
      expect(env).toEqual(armed ? { ...plainEnv, searchDeadlineIsolation: true } : plainEnv);
      const rt = runtimeFromSecret({ ...env, adapterEnv }, secret, "synthetic-generation");
      try {
        expect(rt.deps.deadlineIsolation !== undefined).toBe(armed);
        expect(rt.deps.large).toBeDefined();
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
  });

  test("only true and false; true needs the search stage, ClickHouse streaming and the large-read handoff", () => {
    for (const bad of ["1", "TRUE", "on", " true"])
      expect(() => searchIndexDeadlineIsolationFromEnv({ SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED: bad })).toThrow("SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED must be true or false");
    const need = "SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true needs SEARCH_INDEX_ENABLED=1, SEARCH_INDEX_STREAMING_CH=1 and SESSION_LARGE_READ_HANDOFF_ENABLED=1";
    const on = { SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED: "true" };
    const { SEARCH_INDEX_STREAMING_CH: _s, ...noStream } = ready;
    const { SESSION_LARGE_READ_HANDOFF_ENABLED: _l, ...noLarge } = ready;
    expect(() => lambdaEnv({ ...noStream, ...on })).toThrow(need);
    expect(() => lambdaEnv({ ...noLarge, ...on })).toThrow(need);
    expect(() => lambdaEnv({ ...ready, SEARCH_INDEX_ENABLED: "0", ...on })).toThrow();
  });
});
