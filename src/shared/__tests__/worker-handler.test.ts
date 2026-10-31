import { describe, expect, test } from "bun:test";
import { instrumented, traced } from "../worker.ts";
import type { StageMessage } from "../vendor.ts";

const message = { v: 1, stage: "normalized-ready", ctx: { traceId: "t", orgId: "o", workspaceId: "w" }, workKey: "k", payload: {} } as unknown as StageMessage;
const job = { id: "1", leaseId: "l" };

describe("worker handler wrappers", () => {
  // normalized-ready applies its count through the job, so a wrapper that drops it
  // quarantines every record in production while tests of the poller stay green.
  test("both wrappers hand the job capability to the handler", async () => {
    let seen: unknown;
    const handler = instrumented(traced("normalized-ready", async (_m, _r, _a, j) => void (seen = j)), { start: () => {}, end: () => {} });
    await handler(message, { log: { info() {} } } as never, 1, job);
    expect(seen).toEqual(job);
  });
  test("the in-flight tracking ends when the handler throws", async () => {
    const calls: string[] = [];
    const handler = instrumented(async () => { throw new Error("x"); }, { start: () => calls.push("start"), end: () => calls.push("end") });
    await expect(handler(message, {} as never, 1, job)).rejects.toThrow("x");
    expect(calls).toEqual(["start", "end"]);
  });
});
