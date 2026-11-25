/**
 * SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: the SQS delay of a scope_busy requeue only. Unset keeps 60 s and
 * renders no LambdaEnv field; every other requeue (stream deferred, pointer changed, buffered stage pending)
 * keeps 60 s whatever the setting. Pure, no database. Synthetic values only.
 */
import { describe, expect, test } from "bun:test";
import { BUSY_REQUEUE_DELAY_SECONDS, busyRequeueDelayFromEnv, lambdaEnv, requeueDelaySeconds } from "../lambda.ts";

const base = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
};

describe("busyRequeueDelayFromEnv", () => {
  test("unset or empty is the default (60 s)", () => {
    expect(BUSY_REQUEUE_DELAY_SECONDS).toBe(60);
    expect(busyRequeueDelayFromEnv({})).toBe(60);
    expect(busyRequeueDelayFromEnv({ SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: "" })).toBe(60);
  });
  test("an integer 1-900 is taken as is", () => {
    for (const [v, n] of [["1", 1], ["10", 10], ["60", 60], ["900", 900]] as const)
      expect(busyRequeueDelayFromEnv({ SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: v })).toBe(n);
  });
  test("anything else refuses init", () => {
    for (const bad of ["0", "901", "1000", "-5", "10.5", "1e2", " 10", "10 ", "010", "ten", "0x10"])
      expect(() => busyRequeueDelayFromEnv({ SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: bad })).toThrow("must be an integer 1-900");
  });
});

describe("lambdaEnv and requeueDelaySeconds", () => {
  test("unset: no field, and every requeue is 60 s", () => {
    const env = lambdaEnv(base);
    expect("busyRequeueDelaySeconds" in env).toBe(false);
    expect(requeueDelaySeconds(env, "scope_busy")).toBe(60);
    expect(requeueDelaySeconds(env)).toBe(60);
  });
  test("set to the default: still no field", () => {
    expect("busyRequeueDelaySeconds" in lambdaEnv({ ...base, SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: "60" })).toBe(false);
  });
  test("set: scope_busy takes it; an untagged requeue keeps 60 s", () => {
    const env = lambdaEnv({ ...base, SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: "10" });
    expect(env.busyRequeueDelaySeconds).toBe(10);
    expect(requeueDelaySeconds(env, "scope_busy")).toBe(10);
    expect(requeueDelaySeconds(env)).toBe(60);
  });
  test("a malformed value refuses lambdaEnv", () => {
    expect(() => lambdaEnv({ ...base, SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS: "0" })).toThrow("must be an integer 1-900");
  });
});
