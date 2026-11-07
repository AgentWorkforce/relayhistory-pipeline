/** Lane F1 P0-1: the pipeline Lambda serves `{"sessionAnalysis": true}`, off before any init. */
import { afterEach, expect, test } from "bun:test";
import { handler } from "../../session-pipeline/lambda.ts";

afterEach(() => {
  delete process.env.SESSION_ANALYSIS_ENABLED;
});

test("a sessionAnalysis invocation with the flag off returns before reading any secret", async () => {
  delete process.env.SESSION_ANALYSIS_ENABLED;
  // No AWS env at all: init would throw, so ok:true proves nothing was initialized.
  expect(await handler({ sessionAnalysis: true })).toEqual({ ok: true });
});

