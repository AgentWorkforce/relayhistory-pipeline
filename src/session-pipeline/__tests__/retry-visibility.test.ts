/**
 * SESSION_PIPELINE_RETRY_VISIBILITY_S (forward port of the backport lineage's ae153e62 and 35cf5404): with it set, a
 * message whose pointer stage failed only with pointer_move_pending gets one ChangeMessageVisibility (the configured
 * seconds) and is still a batch item failure; a message that also fed any other failure keeps the full timeout; a
 * rejected or hung call logs a warn and the message still fails; near the invocation deadline no call is made.
 * Unset: processBatch, the records and every log line are exactly as before. Pure fakes, synthetic data only.
 */
import { describe, expect, test } from "bun:test";
import { PipelineError } from "@rhc/lib/pipeline/errors.ts";
import { processBatch, FAST_RETRY_CODES, RETRY_VISIBILITY_MARGIN_MS, RETRY_VISIBILITY_TIMEOUT_MS, type ConsumerDeps, type QueueRecord } from "../consumer.ts";
import { invocationDeps, lambdaEnv, queueRecords, retryVisibilityFromEnv, runtimeFromSecret, sqsChangeVisibility, type Runtime } from "../lambda.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import type { PointerDeps } from "../pointer.ts";
import { captureLogs, realHandler } from "./search-index-skip-flags-harness.ts";

const HANDLE = "receipt-handle-synthetic-SECRET-7f3a";
const keyOf = (session: string) => `96bulls/org-synthetic/v1/ws=ws-synthetic/m=m-synthetic/h=claude/d=2026/11/06/s=${session}/manifest.json`;
const s3Record = (session: string) => ({
  eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: "2026-11-06T00:00:00.000Z",
  s3: { bucket: { name: "bucket-synthetic" }, object: { key: keyOf(session), size: 10, eTag: "etag-synthetic", sequencer: "0A" } },
});
const s3Body = (...sessions: string[]) => JSON.stringify({ Records: sessions.map(s3Record) });

/**
 * Pointer deps by session id: "pmp-*" fails with pointer_move_pending, "err-*" with another pointer error, "srch-*"
 * is indexed (its search pass then fails: the search deps below have no binding), anything else is ignored (acked).
 */
const pointer = {
  reader: { provider: "s3", bucket: undefined },
  orgs: {
    orgFor: async (loc: { sessionId: string }) => {
      if (loc.sessionId.startsWith("pmp")) throw new PipelineError("pointer_move_pending", "synthetic", true);
      if (loc.sessionId.startsWith("err")) throw new PipelineError("db_unavailable", "synthetic", true);
      return loc.sessionId.startsWith("srch") ? "org-synthetic" : null;
    },
  },
  allowPrefix: () => true,
  lock: async (scope: { sessionId: string }) => ({ status: "indexed", scope, manifestKey: keyOf(scope.sessionId), revision: 1 }),
} as unknown as PointerDeps;
/** Search deps with no service binding: every pass throws ("search index failed"), a non-pmp failure. */
const brokenSearch = { transact: async () => { throw new Error("unused"); } } as unknown as SearchIndexDeps;

const records = (...bodies: string[][]): QueueRecord[] =>
  bodies.map((sessions, i) => ({ messageId: `msg-${sessions.join("+")}`, body: s3Body(...sessions), receiptHandle: `${HANDLE}-${i}` }));
const stable = (lines: Array<Record<string, unknown>>) => lines.map(({ ts: _t, duration_ms: _d, ...rest }) => rest);
const ids = (r: { batchItemFailures: { itemIdentifier: string }[] }) => r.batchItemFailures.map((f) => f.itemIdentifier).sort();

describe("retry visibility off (default)", () => {
  test("no change call, the same failures and the same log lines as without receipt handles", async () => {
    const base: ConsumerDeps = { pointer, search: brokenSearch };
    const batch = records(["pmp-1"], ["err-1"], ["srch-1"], ["ok-1"]);
    const withHandles = await captureLogs(() => processBatch(batch, base));
    const without = await captureLogs(() => processBatch(batch.map(({ receiptHandle: _r, ...r }) => r), base));
    expect(ids(withHandles.result)).toEqual(["msg-err-1", "msg-pmp-1", "msg-srch-1"]);
    expect(withHandles.result).toEqual(without.result);
    expect(stable(withHandles.lines)).toEqual(stable(without.lines));
    expect(withHandles.lines.some((l) => String(l.message).includes("retry visibility"))).toBe(false);
  });

  test("lambdaEnv, the runtime deps and queueRecords are unchanged when the env is unset, empty or 0", async () => {
    const env = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "p", SESSION_PIPELINE_SECRET_ARN: "s", SESSION_PIPELINE_QUEUE_URL: "q", PIPELINE_ORG_FROM_PREFIX: "true" };
    const plain = lambdaEnv(env);
    expect(plain.retryVisibilityS).toBeUndefined();
    for (const v of ["", "0"]) expect(lambdaEnv({ ...env, SESSION_PIPELINE_RETRY_VISIBILITY_S: v })).toEqual(plain);
    expect(lambdaEnv({ ...env, SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" })).toEqual({ ...plain, retryVisibilityS: 60 });
    const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture" });
    for (const [e, armed] of [[plain, false], [{ ...plain, retryVisibilityS: 60 }, true]] as const) {
      const rt = runtimeFromSecret(e, secret, "synthetic-generation");
      try {
        expect("retryVisibility" in rt.deps).toBe(armed);
        if (armed) expect(rt.deps.retryVisibility!.seconds).toBe(60);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    const sqs = [{ messageId: "m", body: "{}", receiptHandle: HANDLE, attributes: { SentTimestamp: "1" } }];
    expect(queueRecords(sqs)).toEqual([{ messageId: "m", body: "{}" }]);
    expect(queueRecords(sqs, true)).toEqual([{ messageId: "m", body: "{}", receiptHandle: HANDLE }]);
  });

  test("the backport's grammar: integers 1..43200, anything else fails init naming the variable", () => {
    expect(retryVisibilityFromEnv({})).toBe(0);
    expect(retryVisibilityFromEnv({ SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" })).toBe(60);
    expect(retryVisibilityFromEnv({ SESSION_PIPELINE_RETRY_VISIBILITY_S: "43200" })).toBe(43_200);
    for (const v of ["-1", "1.5", "60s", "43201", "true", " 60", "060000"])
      expect(() => retryVisibilityFromEnv({ SESSION_PIPELINE_RETRY_VISIBILITY_S: v })).toThrow("SESSION_PIPELINE_RETRY_VISIBILITY_S must be");
    expect([...FAST_RETRY_CODES]).toEqual(["pointer_move_pending"]);
  });

  test("through the real handler: the batch response and the records the consumer sees are as before", async () => {
    const send = realHandler({ pointer });
    const { result, lines } = await captureLogs(() => send(records(["pmp-1"], ["ok-1"]), { getRemainingTimeInMillis: () => 300_000 }));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(lines.some((l) => String(l.message).includes("retry visibility"))).toBe(false);
  });
});

describe("retry visibility on: attribution", () => {
  const armed = (calls: Array<[string, number]>, extra: Partial<ConsumerDeps> = {}): ConsumerDeps => ({
    pointer, search: brokenSearch, retryVisibility: { seconds: 60, change: async (h, s) => { calls.push([h, s]); } }, ...extra,
  });

  test("pmp only: one change call with the configured seconds; still a failure; the handle is never logged", async () => {
    const calls: Array<[string, number]> = [];
    const { result, lines } = await captureLogs(() => processBatch(records(["pmp-1"]), armed(calls)));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(calls).toEqual([[`${HANDLE}-0`, 60]]);
    expect(JSON.stringify(lines)).not.toContain(HANDLE);
  });

  test("per message: pmp gets the call, another pointer error, a search failure and a success get none", async () => {
    const calls: Array<[string, number]> = [];
    const { result } = await captureLogs(() => processBatch(records(["pmp-1"], ["err-1"], ["srch-1"], ["ok-1"]), armed(calls)));
    expect(ids(result)).toEqual(["msg-err-1", "msg-pmp-1", "msg-srch-1"]);
    expect(calls).toEqual([[`${HANDLE}-0`, 60]]);
  });

  test("a message that also fed another pointer failure keeps the full timeout", async () => {
    const calls: Array<[string, number]> = [];
    const { result } = await captureLogs(() => processBatch(records(["pmp-2", "err-2"]), armed(calls)));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-2+err-2" }] });
    expect(calls).toEqual([]);
  });

  test("a message that also fed a failed search pass keeps the full timeout (the scope's other message kept it running)", async () => {
    const calls: Array<[string, number]> = [];
    // The search pass of srch-5 runs because one of its messages has not failed; it fails both.
    const { result } = await captureLogs(() => processBatch(records(["pmp-5", "srch-5"], ["srch-5"]), armed(calls)));
    expect(ids(result)).toEqual(["msg-pmp-5+srch-5", "msg-srch-5"]);
    expect(calls).toEqual([]);
  });

  test("a scope whose every message already failed runs no search pass: a pmp message that only shares it gets the call", async () => {
    const calls: Array<[string, number]> = [];
    const { result, lines } = await captureLogs(() => processBatch(records(["pmp-6", "srch-6"]), armed(calls)));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-6+srch-6" }] });
    expect(lines.some((l) => l.message === "search index failed")).toBe(false);
    expect(calls).toEqual([[`${HANDLE}-0`, 60]]);
  });

  test("a message that also fed an invalid record keeps the full timeout; one that also fed a success gets the call", async () => {
    const calls: Array<[string, number]> = [];
    const batch: QueueRecord[] = [
      { messageId: "msg-bad", body: "not json", receiptHandle: `${HANDLE}-bad` },
      { messageId: "msg-pmp+ok", body: s3Body("pmp-3", "ok-3"), receiptHandle: `${HANDLE}-mixed-ok` },
    ];
    const { result } = await captureLogs(() => processBatch(batch, armed(calls)));
    expect(ids(result)).toEqual(["msg-bad", "msg-pmp+ok"]);
    expect(calls).toEqual([[`${HANDLE}-mixed-ok`, 60]]);
  });

  test("a large-read route failure is another failure: no call", async () => {
    const calls: Array<[string, number]> = [];
    const large = { route: async () => { throw Object.assign(new Error("synthetic"), { code: "08006" }); } };
    const { result } = await captureLogs(() => processBatch(records(["pmp-4", "srch-4"], ["srch-4"]), armed(calls, { large })));
    expect(ids(result)).toEqual(["msg-pmp-4+srch-4", "msg-srch-4"]);
    expect(calls).toEqual([]);
  });

  test("a rejected call logs a warn with a reason code and the message still fails; a hung call is abandoned at the bound", async () => {
    const rejected = await captureLogs(() => processBatch(records(["pmp-1"]), {
      pointer, retryVisibility: { seconds: 60, change: async (h) => { throw Object.assign(new Error(`denied for ${h}`), { code: "AccessDenied" }); } },
    }));
    expect(rejected.result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(rejected.lines.find((l) => l.message === "retry visibility not shortened")).toMatchObject({ level: "warn", reason: "change_visibility_failed", error_code: "AccessDenied" });
    expect(JSON.stringify(rejected.lines)).not.toContain(HANDLE);
    const started = Date.now();
    const hung = await captureLogs(() => processBatch(records(["pmp-1"]), { pointer, retryVisibility: { seconds: 60, change: () => new Promise<void>(() => {}) } }));
    expect(Date.now() - started).toBeLessThan(RETRY_VISIBILITY_TIMEOUT_MS + 1_500);
    expect(hung.result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(hung.lines.find((l) => l.message === "retry visibility not shortened")).toMatchObject({ level: "warn", reason: "change_visibility_timeout" });
  });

  test("the search-jobs follow-on runs before the calls and never changes which messages fail", async () => {
    const order: string[] = [];
    const deps: ConsumerDeps = {
      pointer,
      searchJobs: { drain: async () => { order.push("drain"); } },
      retryVisibility: { seconds: 60, change: async () => { order.push("change"); } },
    };
    const { result } = await captureLogs(() => processBatch(records(["pmp-1"], ["ok-1"]), deps));
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }] });
    expect(order).toEqual(["drain", "change"]);
  });

  test("sqsChangeVisibility sends one bounded ChangeMessageVisibility to the pipeline queue", async () => {
    const sent: unknown[] = [];
    const change = sqsChangeVisibility({ region: "us-east-1", queueUrl: "queue-url-synthetic" }, (async (args: unknown) => { sent.push(args); return {}; }) as never, () => ({}) as never);
    await change(HANDLE, 60, RETRY_VISIBILITY_TIMEOUT_MS);
    await change(HANDLE, 60, 1_500);
    await change(HANDLE, 60, 9_000);
    const call = (timeoutMs: number) => ({
      service: "sqs", target: "AmazonSQS.ChangeMessageVisibility", region: "us-east-1",
      body: { QueueUrl: "queue-url-synthetic", ReceiptHandle: HANDLE, VisibilityTimeout: 60 },
      credentials: {}, timeoutMs,
    });
    expect(sent).toEqual([call(RETRY_VISIBILITY_TIMEOUT_MS), call(1_500), call(RETRY_VISIBILITY_TIMEOUT_MS)]);
  });
});

/**
 * Through the exported Lambda handler (createPipelineLambdaHandler with loadRuntime: no secret, pool or S3):
 * "pmp-*" fails with pointer_move_pending, "ok-*" is acknowledged, and the SQS transport takes 350 ms per call.
 */
describe("the real handler near the invocation deadline", () => {
  const SQS_MS = 350;
  const sqsRecords = (...sessions: string[]) => sessions.map((s, i) => ({ messageId: `msg-${s}`, body: s3Body(s), receiptHandle: `${HANDLE}-${i}` }));
  const streamingSearch = new Proxy({ stream: { maxReadBytes: 1, maxReadEvents: 1 } } as unknown as SearchIndexDeps, {
    get: (t, p) => { if (p in t) return (t as unknown as Record<string | symbol, unknown>)[p]; throw new Error(`search touched: ${String(p)}`); },
  });
  const STREAM_ENV = { searchStreamCh: { maxReadBytes: 1, maxReadEvents: 1, deadlineMarginMs: 60_000 } };

  async function run(opts: { streaming: boolean; remaining?: number }) {
    const calls: Array<{ handle: string; seconds: number; timeoutMs: number }> = [];
    const send = realHandler({
      pointer,
      ...(opts.streaming ? { search: streamingSearch } : {}),
      retryVisibility: {
        seconds: 60,
        change: async (handle, seconds, timeoutMs) => { calls.push({ handle, seconds, timeoutMs }); await new Promise((r) => setTimeout(r, SQS_MS)); },
      },
    }, opts.streaming ? STREAM_ENV : {});
    const context = opts.remaining === undefined ? undefined : { getRemainingTimeInMillis: () => opts.remaining! };
    const started = Date.now();
    const { result, lines } = await captureLogs(() => send(sqsRecords("pmp-1", "pmp-2", "ok-1"), context));
    return { result, lines, calls, elapsed: Date.now() - started };
  }

  const FAILED = { batchItemFailures: [{ itemIdentifier: "msg-pmp-1" }, { itemIdentifier: "msg-pmp-2" }] };

  for (const streaming of [false, true]) {
    const path = streaming ? "streaming" : "non-streaming";
    test(`${path}: 100 ms remaining makes no call and returns the same failures at once, the success acknowledged`, async () => {
      const { result, lines, calls, elapsed } = await run({ streaming, remaining: 100 });
      expect(result).toEqual(FAILED);
      expect(calls).toEqual([]);
      expect(elapsed).toBeLessThan(100);
      const warns = lines.filter((l) => l.message === "retry visibility not shortened");
      expect(warns).toEqual([expect.objectContaining({ level: "warn", reason: "retry_visibility_deadline", count: 2 })]);
      expect(JSON.stringify(lines)).not.toContain(HANDLE);
    });

    test(`${path}: just under the call bound plus margin still makes no call`, async () => {
      const { result, calls } = await run({ streaming, remaining: RETRY_VISIBILITY_TIMEOUT_MS + RETRY_VISIBILITY_MARGIN_MS - 1 });
      expect(result).toEqual(FAILED);
      expect(calls).toEqual([]);
    });

    test(`${path}: exactly the bound plus margin makes both calls, each bounded by remaining less the margin`, async () => {
      const { result, calls } = await run({ streaming, remaining: RETRY_VISIBILITY_TIMEOUT_MS + RETRY_VISIBILITY_MARGIN_MS });
      expect(result).toEqual(FAILED);
      expect(calls.map((c) => c.timeoutMs)).toEqual([RETRY_VISIBILITY_TIMEOUT_MS, RETRY_VISIBILITY_TIMEOUT_MS]);
    });

    test(`${path}: an ample budget makes both calls, concurrently and bounded, and the same failures`, async () => {
      const { result, lines, calls, elapsed } = await run({ streaming, remaining: 300_000 });
      expect(result).toEqual(FAILED);
      expect(calls.map((c) => c.handle).sort()).toEqual([`${HANDLE}-0`, `${HANDLE}-1`]);
      for (const c of calls) expect(c).toMatchObject({ seconds: 60, timeoutMs: RETRY_VISIBILITY_TIMEOUT_MS });
      expect(elapsed).toBeLessThan(2 * SQS_MS);
      expect(JSON.stringify(lines)).not.toContain(HANDLE);
    });
  }

  test("no context: the fixed bound (calls made, each bounded by the cap)", async () => {
    const { result, calls } = await run({ streaming: false });
    expect(result).toEqual(FAILED);
    expect(calls.map((c) => c.timeoutMs)).toEqual([RETRY_VISIBILITY_TIMEOUT_MS, RETRY_VISIBILITY_TIMEOUT_MS]);
  });

  test("invocationDeps: remaining time reaches retryVisibility with or without streaming; neither armed is deps itself", () => {
    const rv = { seconds: 60, change: async () => {} };
    const ctx = { getRemainingTimeInMillis: () => 4_321 };
    const rt = (deps: ConsumerDeps, env: Partial<Runtime["env"]> = {}) => ({ env, deps }) as unknown as Runtime;
    expect(invocationDeps(rt({ pointer, retryVisibility: rv }), ctx).retryVisibility!.remainingMs!()).toBe(4_321);
    const streamed = invocationDeps(rt({ pointer, search: streamingSearch, retryVisibility: rv }, STREAM_ENV), ctx);
    expect(streamed.retryVisibility!.remainingMs!()).toBe(4_321);
    expect(streamed.search!.streamSlot).toEqual({ used: false });
    const bare = { pointer, retryVisibility: rv };
    expect(invocationDeps(rt(bare))).toBe(bare);
    const plain = { pointer };
    expect(invocationDeps(rt(plain), ctx)).toBe(plain);
  });
});
