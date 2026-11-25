/**
 * SESSION_PIPELINE_RETRY_VISIBILITY_S: with it set, a message whose pointer stage failed with
 * pointer_move_pending gets one ChangeMessageVisibility (the configured seconds) and is still a batch
 * item failure; other failures keep the full timeout; a rejected or hung call logs a warn and the
 * message still fails. Unset: processBatch behaves and logs exactly as before. Pure fakes, synthetic data.
 */
import { describe, expect, test } from "bun:test";
import { PipelineError } from "@rhc/lib/pipeline/errors.ts";
import { processBatch, RETRY_VISIBILITY_TIMEOUT_MS, type ConsumerDeps, type QueueRecord } from "../consumer.ts";
import { lambdaEnv, queueRecords, retryVisibilityFromEnv, sqsChangeVisibility } from "../lambda.ts";
import type { PointerDeps } from "../pointer.ts";

const HANDLE = "receipt-handle-synthetic-SECRET-7f3a";
const keyOf = (session: string) => `96bulls/org-synthetic/v1/ws=ws-synthetic/m=m-synthetic/h=claude/d=2026/11/06/s=${session}/manifest.json`;
const s3Body = (session: string) => JSON.stringify({
  Records: [{
    eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: "2026-11-06T00:00:00.000Z",
    s3: { bucket: { name: "bucket-synthetic" }, object: { key: keyOf(session), size: 10, eTag: "etag-synthetic", sequencer: "0A" } },
  }],
});

/** Pointer deps whose org lookup throws the error chosen per session id. */
function pointerThrowing(codeFor: (session: string) => string): PointerDeps {
  return {
    reader: { bucket: undefined },
    orgs: { orgFor: async (loc: { sessionId: string }) => { throw new PipelineError(codeFor(loc.sessionId), "synthetic failure", true); } },
  } as unknown as PointerDeps;
}

async function capture<T>(run: () => Promise<T>) {
  const lines: string[] = [];
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
    ((chunk: unknown, ...rest: unknown[]) => {
      for (const l of String(chunk).split("\n")) if (l.startsWith("{")) lines.push(l);
      return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
    }) as typeof process.stdout.write;
  process.stdout.write = tap(out, process.stdout);
  process.stderr.write = tap(err, process.stderr);
  try {
    return { result: await run(), lines };
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}

/** Log lines without the volatile fields, for parity checks. */
const stable = (lines: string[]) => lines.map((l) => { const { ts: _t, duration_ms: _d, ...rest } = JSON.parse(l); return rest; });

const records = (...sessions: string[]): QueueRecord[] =>
  sessions.map((s, i) => ({ messageId: `msg-${s}`, body: s3Body(s), receiptHandle: `${HANDLE}-${i}` }));

const codeFor = (s: string) => (s.startsWith("pmp") ? "pointer_move_pending" : "db_unavailable");

describe("retry visibility off (default)", () => {
  test("no change call, same failures and the same log lines as without receipt handles", async () => {
    const base = { pointer: pointerThrowing(codeFor) } as ConsumerDeps;
    const withHandles = await capture(() => processBatch(records("pmp-1", "other-1"), base));
    const without = await capture(() => processBatch(records("pmp-1", "other-1").map(({ receiptHandle: _r, ...r }) => r), base));
    expect(withHandles.result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }, { itemIdentifier: "msg-other-1" }] });
    expect(withHandles.result).toEqual(without.result);
    expect(stable(withHandles.lines)).toEqual(stable(without.lines));
    expect(withHandles.lines.some((l) => l.includes("retry visibility"))).toBe(false);
  });

  test("lambdaEnv and queueRecords are unchanged when the env is unset, empty or 0", () => {
    const env = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "p", SESSION_PIPELINE_SECRET_ARN: "s", SESSION_PIPELINE_QUEUE_URL: "q", PIPELINE_ORG_FROM_PREFIX: "true" };
    const plain = lambdaEnv(env);
    expect(plain.retryVisibilityS).toBeUndefined();
    for (const v of ["", "0"]) expect(lambdaEnv({ ...env, SESSION_PIPELINE_RETRY_VISIBILITY_S: v })).toEqual(plain);
    expect(lambdaEnv({ ...env, SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" })).toEqual({ ...plain, retryVisibilityS: 60 });
    const sqs = [{ messageId: "m", body: "{}", receiptHandle: HANDLE, attributes: { SentTimestamp: "1" } }];
    expect(queueRecords(sqs)).toEqual([{ messageId: "m", body: "{}", sentTimestamp: "1" }]);
    expect(queueRecords(sqs, true)).toEqual([{ messageId: "m", body: "{}", sentTimestamp: "1", receiptHandle: HANDLE }]);
  });

  test("the env grammar: integers 1..43200, anything else fails init naming the variable", () => {
    expect(retryVisibilityFromEnv({})).toBe(0);
    expect(retryVisibilityFromEnv({ SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" })).toBe(60);
    for (const v of ["-1", "1.5", "60s", "43201", "true", " 60"])
      expect(() => retryVisibilityFromEnv({ SESSION_PIPELINE_RETRY_VISIBILITY_S: v })).toThrow("SESSION_PIPELINE_RETRY_VISIBILITY_S must be");
  });
});

describe("retry visibility on", () => {
  test("a pointer_move_pending failure gets one change call with the configured seconds and still fails", async () => {
    const calls: Array<[string, number]> = [];
    const deps: ConsumerDeps = { pointer: pointerThrowing(codeFor), retryVisibility: { seconds: 60, change: async (h, s) => { calls.push([h, s]); } } };
    const { result, lines } = await capture(() => processBatch(records("pmp-1"), deps));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(calls).toEqual([[`${HANDLE}-0`, 60]]);
    expect(lines.join("\n")).not.toContain(HANDLE);
  });

  test("a non-pmp failure gets no change call and still fails", async () => {
    const calls: string[] = [];
    const deps: ConsumerDeps = { pointer: pointerThrowing(codeFor), retryVisibility: { seconds: 60, change: async (h) => { calls.push(h); } } };
    const { result } = await capture(() => processBatch(records("pmp-1", "other-1"), deps));
    expect(result.batchItemFailures.map((f) => f.itemIdentifier).sort()).toEqual(["msg-other-1", "msg-pmp-1"]);
    expect(calls).toEqual([`${HANDLE}-0`]);
  });

  test("a message that also fed a non-pmp failure keeps the full timeout", async () => {
    const calls: string[] = [];
    const body = JSON.stringify({ Records: [...JSON.parse(s3Body("pmp-1")).Records, ...JSON.parse(s3Body("other-1")).Records] });
    const deps: ConsumerDeps = { pointer: pointerThrowing(codeFor), retryVisibility: { seconds: 60, change: async (h) => { calls.push(h); } } };
    const { result } = await capture(() => processBatch([{ messageId: "msg-both", body, receiptHandle: HANDLE }], deps));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-both" }] });
    expect(calls).toEqual([]);
  });

  test("a rejected change call logs a warn with a reason code and the message still fails", async () => {
    const deps: ConsumerDeps = {
      pointer: pointerThrowing(codeFor),
      retryVisibility: { seconds: 60, change: async (h) => { throw Object.assign(new Error(`denied for ${h}`), { code: "AccessDenied" }); } },
    };
    const { result, lines } = await capture(() => processBatch(records("pmp-1"), deps));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    const warn = lines.map((l) => JSON.parse(l)).find((l) => l.message === "retry visibility not shortened");
    expect(warn).toMatchObject({ level: "warn", reason: "change_visibility_failed", error_code: "AccessDenied" });
    expect(lines.join("\n")).not.toContain(HANDLE);
  });

  test("a hung change call is abandoned after the bound, logs a warn and the message still fails", async () => {
    const deps: ConsumerDeps = { pointer: pointerThrowing(codeFor), retryVisibility: { seconds: 60, change: () => new Promise<void>(() => {}) } };
    const started = Date.now();
    const { result, lines } = await capture(() => processBatch(records("pmp-1"), deps));
    expect(Date.now() - started).toBeLessThan(RETRY_VISIBILITY_TIMEOUT_MS + 1_500);
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    const warn = lines.map((l) => JSON.parse(l)).find((l) => l.message === "retry visibility not shortened");
    expect(warn).toMatchObject({ level: "warn", reason: "change_visibility_timeout" });
    expect(lines.join("\n")).not.toContain(HANDLE);
  });

  test("sqsChangeVisibility sends one bounded ChangeMessageVisibility to the pipeline queue", async () => {
    const sent: unknown[] = [];
    const change = sqsChangeVisibility({ region: "us-east-1", queueUrl: "queue-url-synthetic" }, (async (args: unknown) => { sent.push(args); return {}; }) as never, () => ({}) as never);
    await change(HANDLE, 60);
    expect(sent).toEqual([{
      service: "sqs", target: "AmazonSQS.ChangeMessageVisibility", region: "us-east-1",
      body: { QueueUrl: "queue-url-synthetic", ReceiptHandle: HANDLE, VisibilityTimeout: 60 },
      credentials: {}, timeoutMs: RETRY_VISIBILITY_TIMEOUT_MS,
    }]);
  });
});
