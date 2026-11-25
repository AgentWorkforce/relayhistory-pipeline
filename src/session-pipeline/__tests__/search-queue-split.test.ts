/**
 * SESSION_PIPELINE_SEARCH_QUEUE_URL (the search-queue split), driven through the exported Lambda
 * handler wherever the runtime decides something: which queue a record came from (full ARN), what
 * startup refuses, handoff attribution, the handoff's time bound, the search-only path, search-queue
 * records left over after a rollback, and parity with 6d88b638 while unset. The two stages are spied;
 * synthetic data and documentation-placeholder account ids only.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { PipelineError } from "@rhc/lib/pipeline/errors.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { SEARCH_HANDOFF_MARGIN_MS, SEARCH_HANDOFF_MIN_MS, SEARCH_HANDOFF_TIMEOUT_MS, processBatch, type ConsumerDeps, type QueueRecord } from "../consumer.ts";
import {
  BUSY_REQUEUE_DELAY_SECONDS,
  handler,
  lambdaEnv,
  queueRecords,
  queueSourceArns,
  searchQueueUrlFromEnv,
  setRuntimeForTests,
  sqsQueueArn,
  sqsSearchHandoff,
  type LambdaEnv,
} from "../lambda.ts";
import * as pointerModule from "../pointer.ts";
import * as searchModule from "../search-index.ts";
import type { PointerDeps, SessionScope } from "../pointer.ts";
import type { SearchIndexDeps, SearchIndexResult } from "../search-index.ts";
import { requeueBody } from "../s3-events.ts";

const REGION = "us-west-2";
const ACCOUNT = "111122223333";
const POINTER_URL = `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/events-synthetic`;
const SEARCH_URL = `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/search-synthetic`;
const POINTER_ARN = `arn:aws:sqs:${REGION}:${ACCOUNT}:events-synthetic`;
const SEARCH_ARN = `arn:aws:sqs:${REGION}:${ACCOUNT}:search-synthetic`;
const AT = "2026-11-06T00:00:00.000Z";
const keyOf = (session: string, machine = "m-synthetic") =>
  `96bulls/org-synthetic/v1/ws=ws-synthetic/m=${machine}/h=claude/d=2026/11/06/s=${session}/manifest.json`;
const s3Body = (key: string, sequencer = "0A") =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket-synthetic" }, object: { key, eTag: "e", sequencer } } }] });
/** The scope a synthetic key names (the pointer stage is spied, so this stands in for its outcome). */
const scopeOfKey = (key: string): SessionScope => ({ orgId: "org-synthetic", workspaceId: "ws-synthetic", harness: "claude", sessionId: /\/s=([^/]+)\//.exec(key)![1]! });

const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
let lockCalls = 0;
/** Only what resolveScope reads, plus a lock that counts: the search-only path must never take it. */
const pointer = {
  reader: { bucket: undefined },
  orgs: prefixOrgResolver,
  allowPrefix: (prefix: string | undefined, org: string) => prefixAllowed(bindings, prefix, org),
  lock: async () => {
    lockCalls++;
    throw new Error("the pointer lock was taken");
  },
} as unknown as PointerDeps;

const done: SearchIndexResult = { manifests: 1, rows: 2, inserted: 2, deleted: 0, complete: true, reasons: [] };
function searchOutcome(scope: SessionScope): SearchIndexResult {
  if (scope.sessionId.startsWith("busy")) return { ...done, manifests: 0, rows: 0, inserted: 0, complete: false, busy: true, reasons: ["scope_busy"] };
  if (scope.sessionId.startsWith("defer")) return { ...done, manifests: 0, rows: 0, inserted: 0, complete: false, deferred: true, reasons: [] };
  if (scope.sessionId.startsWith("boom")) throw new PipelineError("clickhouse_unavailable", "synthetic failure", true);
  return done;
}

let pointerSpy: ReturnType<typeof spyOn<typeof pointerModule, "indexPointer">>;
let searchSpy: ReturnType<typeof spyOn<typeof searchModule, "indexSearchScope">>;
beforeEach(() => {
  lockCalls = 0;
  pointerSpy = spyOn(pointerModule, "indexPointer").mockImplementation(async (landed) => {
    if (!landed.key.endsWith("/manifest.json")) return { status: "ignored", reason: "not_a_session_key" };
    const scope = scopeOfKey(landed.key);
    if (scope.sessionId.startsWith("pmp")) throw new PipelineError("pointer_move_pending", "synthetic", true);
    return { status: "indexed", scope, manifestKey: landed.key, revision: 1 };
  });
  searchSpy = spyOn(searchModule, "indexSearchScope").mockImplementation(async (scope) => searchOutcome(scope));
});
afterEach(() => {
  pointerSpy.mockRestore();
  searchSpy.mockRestore();
  setRuntimeForTests(undefined);
});

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
    return { result: await run(), lines: lines.map((l) => JSON.parse(l) as Record<string, unknown>) };
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}
const stable = (lines: Record<string, unknown>[]) => lines.map(({ ts: _t, duration_ms: _d, service: _s, deployment: _p, deployment_id: _i, ...rest }) => rest);
const failures = (r: unknown) => (r as { batchItemFailures: { itemIdentifier: string }[] }).batchItemFailures.map((f) => f.itemIdentifier).sort();

/** Consumer deps with the split armed (or not), recording every send and requeue. */
function setup(opts: { split?: boolean; send?: (key: string, timeoutMs: number) => Promise<void>; search?: Partial<SearchIndexDeps> } = {}) {
  const sent: string[] = [];
  const timeouts: number[] = [];
  const searchRequeued: string[] = [];
  const pointerRequeued: string[] = [];
  const deps: ConsumerDeps = {
    pointer,
    search: { ...opts.search } as SearchIndexDeps,
    requeue: async (key) => void pointerRequeued.push(key),
    now: () => Date.parse(AT) + 5_000,
    ...(opts.split === false
      ? {}
      : {
          searchHandoff: {
            send: async (key, _oldest, timeoutMs) => {
              timeouts.push(timeoutMs);
              await opts.send?.(key, timeoutMs);
              sent.push(key);
            },
            requeue: async (key) => void searchRequeued.push(key),
          },
        }),
  };
  return { deps, sent, timeouts, searchRequeued, pointerRequeued };
}
const armedEnv: Partial<LambdaEnv> = { region: REGION, queueUrl: POINTER_URL, searchQueueUrl: SEARCH_URL };
const unarmedEnv: Partial<LambdaEnv> = { region: REGION, queueUrl: POINTER_URL };
const rec = (messageId: string, body: string, eventSourceARN = POINTER_ARN) => ({ messageId, body, eventSource: "aws:sqs", eventSourceARN, attributes: { SentTimestamp: String(Date.parse(AT)) } });
const ctx = (ms: number) => ({ getRemainingTimeInMillis: () => ms });

describe("which queue a record came from: the full ARN", () => {
  test("sqsQueueArn derives partition, region, account and name, and refuses a URL outside the function's region or partition", () => {
    expect(sqsQueueArn(SEARCH_URL, REGION)).toBe(SEARCH_ARN);
    expect(sqsQueueArn(`https://sqs.cn-north-1.amazonaws.com.cn/${ACCOUNT}/q`, "cn-north-1")).toBe(`arn:aws-cn:sqs:cn-north-1:${ACCOUNT}:q`);
    expect(sqsQueueArn(`https://sqs.us-gov-west-1.amazonaws.com/${ACCOUNT}/q`, "us-gov-west-1")).toBe(`arn:aws-us-gov:sqs:us-gov-west-1:${ACCOUNT}:q`);
    for (const [url, region] of [
      [SEARCH_URL, "eu-west-1"], // another region than the function's
      [`https://sqs.${REGION}.amazonaws.com.cn/${ACCOUNT}/q`, REGION], // another partition's endpoint
      [`https://sqs.cn-north-1.amazonaws.com/${ACCOUNT}/q`, "cn-north-1"],
      [`http://sqs.${REGION}.amazonaws.com/${ACCOUNT}/q`, REGION],
      [`https://sqs.${REGION}.example.invalid/${ACCOUNT}/q`, REGION],
      [`https://sqs.${REGION}.amazonaws.com/owner/q`, REGION],
      [`https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/q.fifo`, REGION],
      [`https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/q/extra`, REGION],
      [`https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/q?x=1`, REGION],
      ["https://sqs", REGION],
    ] as const)
      expect([url, region, sqsQueueArn(url, region)]).toEqual([url, region, undefined]);
  });

  test("handler: only the exact search ARN runs search-only; another region, partition, account or name is not the search queue", async () => {
    const { deps, sent } = setup();
    setRuntimeForTests(deps, armedEnv);
    const lookalikes = {
      region: `arn:aws:sqs:eu-west-1:${ACCOUNT}:search-synthetic`,
      partition: `arn:aws-cn:sqs:${REGION}:${ACCOUNT}:search-synthetic`,
      account: `arn:aws:sqs:${REGION}:444455556666:search-synthetic`,
      name: `arn:aws:sqs:${REGION}:${ACCOUNT}:x-search-synthetic`,
    };
    const event = {
      Records: [
        rec("exact", requeueBody(keyOf("s-exact")), SEARCH_ARN),
        ...Object.entries(lookalikes).map(([why, arn]) => rec(why, requeueBody(keyOf(`s-${why}`)), arn)),
        rec("pointer", s3Body(keyOf("s-pointer"))),
      ],
    };
    const { result, lines } = await capture(() => handler(event, ctx(300_000)));
    expect(result).toEqual({ batchItemFailures: [] });
    // Every look-alike took the pointer path (pointer stage, then handoff); only the exact ARN skipped it.
    expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual(["region", "partition", "account", "name", "pointer"].map((s) => keyOf(`s-${s}`)));
    expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-exact"]);
    expect(sent.sort()).toEqual(["account", "name", "partition", "pointer", "region"].map((s) => keyOf(`s-${s}`)));
    const indexed = Object.fromEntries(lines.filter((l) => l.message === "pointer indexed").map((l) => [l.session_id, l.queue]));
    expect(indexed).toEqual({ "s-region": "other", "s-partition": "other", "s-account": "other", "s-name": "other", "s-pointer": "pointer" });
    expect(lines.find((l) => l.message === "search index reconciled")).toMatchObject({ session_id: "s-exact", queue: "search" });
    expect(lockCalls).toBe(0);
  });

  test("queueRecords: the queue from the ARN whenever the pointer ARN is known; the ARN itself is not passed on", () => {
    const sqs = [
      { messageId: "a", body: "{}", eventSourceARN: SEARCH_ARN },
      { messageId: "b", body: "{}", eventSourceARN: POINTER_ARN },
      { messageId: "c", body: "{}" },
    ];
    expect(queueRecords(sqs)).toEqual(sqs.map(({ messageId, body }) => ({ messageId, body, sentTimestamp: undefined })));
    expect(queueRecords(sqs, queueSourceArns(armedEnv)).map((r) => r.queue)).toEqual(["search", "pointer", undefined]);
    // Unarmed (or rolled back): the search queue is not known, so its records are "other", never "search".
    expect(queueRecords(sqs, queueSourceArns(unarmedEnv)).map((r) => r.queue)).toEqual(["other", "pointer", undefined]);
    expect(JSON.stringify(queueRecords(sqs, queueSourceArns(armedEnv)))).not.toContain("arn:");
    // A search URL equal to the pointer URL is never a search source.
    expect(queueSourceArns({ ...armedEnv, searchQueueUrl: POINTER_URL })).toEqual({ pointer: POINTER_ARN });
  });
});

describe("startup: the split is refused before anything is consumed", () => {
  const base = {
    AWS_REGION: REGION,
    PIPELINE_RAW_S3_BUCKET: "bucket-synthetic",
    PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "secret-synthetic",
    SESSION_PIPELINE_QUEUE_URL: POINTER_URL,
    SEARCH_INDEX_ENABLED: "1",
  };
  const refused: [string, Record<string, string>, string][] = [
    ["equal to the pointer URL", { SESSION_PIPELINE_SEARCH_QUEUE_URL: POINTER_URL }, "must name a different queue than SESSION_PIPELINE_QUEUE_URL"],
    ["in another region", { SESSION_PIPELINE_SEARCH_QUEUE_URL: `https://sqs.eu-west-1.amazonaws.com/${ACCOUNT}/search-synthetic` }, "must be an SQS queue URL in this function's region and partition"],
    ["on another partition's endpoint", { SESSION_PIPELINE_SEARCH_QUEUE_URL: `https://sqs.${REGION}.amazonaws.com.cn/${ACCOUNT}/search-synthetic` }, "must be an SQS queue URL in this function's region and partition"],
    ["in another account", { SESSION_PIPELINE_SEARCH_QUEUE_URL: `https://sqs.${REGION}.amazonaws.com/444455556666/search-synthetic` }, "must be in the account of SESSION_PIPELINE_QUEUE_URL"],
    ["not a queue URL", { SESSION_PIPELINE_SEARCH_QUEUE_URL: "search-synthetic" }, "must be an SQS queue URL in this function's region and partition"],
    ["with an unparseable pointer URL", { SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL, SESSION_PIPELINE_QUEUE_URL: "https://sqs" }, "needs SESSION_PIPELINE_QUEUE_URL to be an SQS queue URL in this function's region and partition"],
    ["without SEARCH_INDEX_ENABLED=1", { SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL, SEARCH_INDEX_ENABLED: "0" }, "needs SEARCH_INDEX_ENABLED=1"],
  ];

  test("lambdaEnv: each refusal names the variable, never the value", () => {
    for (const [why, env, message] of refused) {
      let got = "";
      try {
        lambdaEnv({ ...base, ...env });
      } catch (e) {
        got = (e as Error).message;
      }
      expect([why, got]).toEqual([why, `session pipeline: SESSION_PIPELINE_SEARCH_QUEUE_URL ${message}`]);
      expect(got).not.toContain(ACCOUNT);
    }
    expect(lambdaEnv({ ...base, SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL }).searchQueueUrl).toBe(SEARCH_URL);
    expect(searchQueueUrlFromEnv({ ...base, SESSION_PIPELINE_SEARCH_QUEUE_URL: "" })).toBeUndefined();
  });

  test("handler: init fails, every message is returned, and no AWS call is made", async () => {
    const saved = { ...process.env };
    const realFetch = globalThis.fetch;
    let calls = 0;
    globalThis.fetch = (async () => {
      calls++;
      throw new Error("no network in this test");
    }) as unknown as typeof fetch;
    try {
      for (const [why, env] of refused) {
        Object.assign(process.env, base, { AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test" }, env);
        const { result, lines } = await capture(() => handler({ Records: [rec("m1", s3Body(keyOf("s-1"))), rec("m2", requeueBody(keyOf("s-2")), SEARCH_ARN)] }, ctx(300_000)));
        expect([why, failures(result)]).toEqual([why, ["m1", "m2"]]);
        expect(lines.map((l) => [l.message, l.queue, l.count])).toEqual([["session pipeline init failed", "unattributed", 2]]);
        expect(JSON.stringify(lines)).not.toContain(ACCOUNT);
      }
      expect(calls).toBe(0);
      expect(pointerSpy).not.toHaveBeenCalled();
      expect(searchSpy).not.toHaveBeenCalled();
    } finally {
      globalThis.fetch = realFetch;
      for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
      Object.assign(process.env, saved);
    }
  });
});

describe("handler, armed: the pointer queue hands off", () => {
  test("one handoff per scope after the pointer stage; a failed send fails exactly that scope's messages", async () => {
    const { deps, sent } = setup({ send: async (k) => (k === keyOf("s-2") ? Promise.reject(new Error("synthetic send failure")) : undefined) });
    setRuntimeForTests(deps, armedEnv);
    const event = { Records: [rec("m1", s3Body(keyOf("s-1"))), rec("m2", s3Body(keyOf("s-2"))), rec("m3", s3Body(keyOf("s-2", "m-other"))), rec("m4", s3Body(keyOf("s-3"))), rec("m5", s3Body(keyOf("pmp-1")))] };
    const { result, lines } = await capture(() => handler(event, ctx(300_000)));
    expect(failures(result)).toEqual(["m2", "m3", "m5"]);
    expect(sent.sort()).toEqual([keyOf("s-1"), keyOf("s-3")]);
    expect(searchSpy).not.toHaveBeenCalled();
    const handoff = lines.filter((l) => l.reason === "search_handoff").map((l) => [l.level, l.session_id, l.queue]);
    expect(handoff.sort()).toEqual([["error", "s-2", "pointer"], ["info", "s-1", "pointer"], ["info", "s-3", "pointer"]]);
    expect(lines.find((l) => l.message === "pointer index failed")).toMatchObject({ queue: "pointer", error_code: "pointer_move_pending" });
    expect(JSON.stringify(lines)).not.toContain(SEARCH_URL);
  });

  test("near the deadline no send starts: the unsent scopes' messages come back at once, the rest of the batch is acknowledged", async () => {
    const { deps, sent } = setup();
    setRuntimeForTests(deps, armedEnv);
    const event = { Records: [rec("m1", s3Body(keyOf("s-1"))), rec("m2", s3Body(keyOf("s-2"))), rec("bad", "not json"), rec("t", JSON.stringify({ Event: "s3:TestEvent" }))] };
    for (const remaining of [100, SEARCH_HANDOFF_MARGIN_MS + SEARCH_HANDOFF_MIN_MS - 1]) {
      sent.length = 0;
      const started = performance.now();
      const { result, lines } = await capture(() => handler(event, ctx(remaining)));
      expect(performance.now() - started).toBeLessThan(100);
      expect(failures(result)).toEqual(["bad", "m1", "m2"]);
      expect(sent).toEqual([]);
      expect(lines.find((l) => l.reason === "handoff_deadline")).toMatchObject({ level: "warn", count: 2, queue: "pointer", stage: "search-index" });
    }
  });

  test("the sends run together, each bounded by min(5 s, remaining - margin); a hung send returns before the invocation ends", async () => {
    const slow = setup({ send: () => new Promise((r) => setTimeout(r, 200)) });
    setRuntimeForTests(slow.deps, armedEnv);
    const started = performance.now();
    const many = { Records: ["s-1", "s-2", "s-3", "s-4"].map((s, n) => rec(`m${n}`, s3Body(keyOf(s)))) };
    expect(failures((await capture(() => handler(many, ctx(300_000)))).result)).toEqual([]);
    expect(performance.now() - started).toBeLessThan(600);
    expect(slow.timeouts).toEqual([SEARCH_HANDOFF_TIMEOUT_MS, SEARCH_HANDOFF_TIMEOUT_MS, SEARCH_HANDOFF_TIMEOUT_MS, SEARCH_HANDOFF_TIMEOUT_MS]);

    // The real SendMessage dep, over a transport that hangs until its request timeout: with 3.5 s left
    // the call is cut at 1.5 s and the batch response (the hung scope failed) comes back well before 3.5 s.
    const timeouts: number[] = [];
    const call = (async (req: { timeoutMs?: number; body: { MessageBody: string } }) => {
      timeouts.push(req.timeoutMs!);
      if (req.body.MessageBody.includes("s=s-hung/")) {
        await new Promise((r) => setTimeout(r, req.timeoutMs));
        throw new DOMException("The operation timed out.", "TimeoutError");
      }
    }) as never;
    const real = setup();
    real.deps.searchHandoff = sqsSearchHandoff({ region: REGION, searchQueueUrl: SEARCH_URL }, call, () => ({}) as never);
    setRuntimeForTests(real.deps, armedEnv);
    const t0 = performance.now();
    const { result, lines } = await capture(() => handler({ Records: [rec("ok", s3Body(keyOf("s-ok"))), rec("hung", s3Body(keyOf("s-hung")))] }, ctx(3_500)));
    const took = performance.now() - t0;
    expect(failures(result)).toEqual(["hung"]);
    expect(timeouts).toEqual([1_500, 1_500]);
    expect(took).toBeGreaterThanOrEqual(1_400);
    expect(took).toBeLessThan(3_500 - SEARCH_HANDOFF_MARGIN_MS + 300);
    expect(lines.find((l) => l.message === "search index handoff failed")).toMatchObject({ session_id: "s-hung", error_code: "TimeoutError", queue: "pointer" });
  });

  test("sqsSearchHandoff: the requeue body to the search queue, no delay for a handoff, the busy delay for a requeue", async () => {
    const calls: { body: Record<string, unknown>; timeoutMs?: number }[] = [];
    const h = sqsSearchHandoff({ region: REGION, searchQueueUrl: SEARCH_URL }, (async (req: { body: Record<string, unknown>; timeoutMs?: number }) => void calls.push(req)) as never, () => ({}) as never);
    await h.send(keyOf("s-1"), AT, 1_234);
    await h.requeue(keyOf("s-1"));
    expect(calls.map((c) => [c.body, c.timeoutMs])).toEqual([
      [{ QueueUrl: SEARCH_URL, MessageBody: requeueBody(keyOf("s-1"), AT), DelaySeconds: 0 }, 1_234],
      [{ QueueUrl: SEARCH_URL, MessageBody: requeueBody(keyOf("s-1")), DelaySeconds: BUSY_REQUEUE_DELAY_SECONDS }, undefined],
    ]);
  });
});

describe("handler, armed: the search queue runs search only", () => {
  test("no pointer stage, no lock, no pointer requeue; busy and deferred requeue to the search queue; failures are its own", async () => {
    const { deps, sent, searchRequeued, pointerRequeued } = setup();
    setRuntimeForTests(deps, armedEnv);
    const event = {
      Records: [
        rec("q1", requeueBody(keyOf("s-1"), AT), SEARCH_ARN),
        rec("q2", requeueBody(keyOf("s-1", "m-other"), AT), SEARCH_ARN),
        rec("q3", requeueBody(keyOf("busy-1")), SEARCH_ARN),
        rec("q4", requeueBody(keyOf("defer-1")), SEARCH_ARN),
        rec("q5", requeueBody(keyOf("boom-1")), SEARCH_ARN),
        rec("q6", "not json", SEARCH_ARN),
        rec("q7", requeueBody("96bulls/not-a-session"), SEARCH_ARN),
      ],
    };
    const { result, lines } = await capture(() => handler(event, ctx(300_000)));
    expect(failures(result)).toEqual(["q5", "q6"]);
    expect(pointerSpy).not.toHaveBeenCalled();
    expect(lockCalls).toBe(0);
    expect(sent).toEqual([]);
    expect(pointerRequeued).toEqual([]);
    expect(searchRequeued).toEqual([keyOf("busy-1"), keyOf("defer-1")]);
    expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-1", "busy-1", "defer-1", "boom-1"]);
    // Every line about these messages says queue "search"; the failure lines are distinguishable from the pointer queue's.
    expect(lines.filter((l) => l.stage !== "init").every((l) => l.queue === "search")).toBe(true);
    expect(lines.filter((l) => l.level === "error" || l.level === "warn").map((l) => [l.message, l.queue])).toEqual([
      ["object event unreadable", "search"],
      ["search index failed", "search"],
    ]);
    expect(lines.find((l) => l.message === "search index reconciled")).toMatchObject({ session_id: "s-1", queue_origin: "requeue", s3_event_age_ms: 5_000 });
  });

  test("past the stream deadline a search-queue pass requeues to the search queue", async () => {
    const late = setup({ search: { stream: {} as SearchIndexDeps["stream"], streamDeadline: { at: 0 } } });
    const { result } = await capture(() => processBatch([{ messageId: "q", body: requeueBody(keyOf("s-2")), queue: "search" }], late.deps));
    expect(result.batchItemFailures).toEqual([]);
    expect(late.searchRequeued).toEqual([keyOf("s-2")]);
    expect(late.pointerRequeued).toEqual([]);
  });

  test("armed without the search stage, a search mark is not served search-only (nothing is silently acknowledged)", async () => {
    const { deps } = setup();
    delete deps.search;
    await capture(() => processBatch([{ messageId: "q", body: requeueBody(keyOf("s-1")), queue: "search" }], deps));
    expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual([keyOf("s-1")]);
  });
});

describe("handler, rolled back: search-queue records left over", () => {
  test("with the variable unset they are recognised as another queue, take the ordinary path, requeue only by SendMessage, and log queue other", async () => {
    const { deps, pointerRequeued } = setup({ split: false });
    setRuntimeForTests(deps, unarmedEnv);
    const event = {
      Records: [
        { ...rec("left1", requeueBody(keyOf("s-1"), AT), SEARCH_ARN), receiptHandle: "handle-synthetic-1" },
        { ...rec("left2", requeueBody(keyOf("busy-1")), SEARCH_ARN), receiptHandle: "handle-synthetic-2" },
        { ...rec("left3", requeueBody(keyOf("boom-1")), SEARCH_ARN), receiptHandle: "handle-synthetic-3" },
        rec("fresh", s3Body(keyOf("s-2"))),
      ],
    };
    const { result, lines } = await capture(() => handler(event, ctx(300_000)));
    expect(failures(result)).toEqual(["left3"]);
    // The ordinary path: the idempotent pointer stage, then search inline; the busy pass is a new pointer-queue message.
    expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual([keyOf("s-1"), keyOf("busy-1"), keyOf("boom-1"), keyOf("s-2")]);
    expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-1", "busy-1", "boom-1", "s-2"]);
    expect(pointerRequeued).toEqual([keyOf("busy-1")]);
    // Source tracking does not depend on the split flag: leftovers say "other", the fresh pointer event says nothing (as before).
    const bySession = (m: string) => Object.fromEntries(lines.filter((l) => l.message === m).map((l) => [l.session_id, l.queue ?? null]));
    expect(bySession("pointer indexed")).toEqual({ "s-1": "other", "busy-1": "other", "boom-1": "other", "s-2": null });
    expect(bySession("search index failed")).toEqual({ "boom-1": "other" });
    // No receipt handle reaches the consumer, so nothing can act on one against the pointer queue.
    expect(JSON.stringify(lines)).not.toContain("handle-synthetic");
    expect(JSON.stringify(queueRecords(event.Records, queueSourceArns(unarmedEnv)))).not.toContain("handle-synthetic");
  });
});

describe("flags off: parity with 6d88b638", () => {
  /** The batch the golden below was recorded from, on 6d88b638 (processBatch(queueRecords(records), deps)). */
  const records = () => [
    rec("m1", s3Body(keyOf("s-1"))),
    rec("m2", s3Body(keyOf("s-1", "m-other"))),
    rec("m3", s3Body(keyOf("s-1"), "0B")),
    rec("m4", s3Body(keyOf("busy-1"))),
    rec("m5", s3Body(keyOf("boom-1"))),
    rec("m6", s3Body(keyOf("pmp-1"))),
    rec("m7", "not json"),
    rec("m8", JSON.stringify({ Event: "s3:TestEvent" })),
    rec("m9", requeueBody("96bulls/not-a-session")),
  ];
  const golden = [
    { level: "warn", message: "object event unreadable", stage: "object-landed", reason: "invalid_body" },
    { level: "info", message: "pointer indexed", stage: "session-indexed", outcome: "indexed", org_id: "org-synthetic", workspace_id: "ws-synthetic", harness: "claude", session_id: "s-1", revision: 1, event_age_ms: 5000, s3_event_age_ms: 5000, queue_origin: "s3" },
    { level: "info", message: "pointer indexed", stage: "session-indexed", outcome: "indexed", org_id: "org-synthetic", workspace_id: "ws-synthetic", harness: "claude", session_id: "s-1", revision: 1, event_age_ms: 5000, s3_event_age_ms: 5000, queue_origin: "s3" },
    { level: "info", message: "pointer indexed", stage: "session-indexed", outcome: "indexed", org_id: "org-synthetic", workspace_id: "ws-synthetic", harness: "claude", session_id: "busy-1", revision: 1, event_age_ms: 5000, s3_event_age_ms: 5000, queue_origin: "s3" },
    { level: "info", message: "pointer indexed", stage: "session-indexed", outcome: "indexed", org_id: "org-synthetic", workspace_id: "ws-synthetic", harness: "claude", session_id: "boom-1", revision: 1, event_age_ms: 5000, s3_event_age_ms: 5000, queue_origin: "s3" },
    { level: "error", message: "pointer index failed", stage: "session-indexed", error_code: "pointer_move_pending", retryable: true },
    { level: "info", message: "object event ignored", stage: "object-landed", reason: "not_a_session_key" },
    { level: "info", message: "search index reconciled", stage: "search-index", org_id: "org-synthetic", workspace_id: "ws-synthetic", harness: "claude", session_id: "s-1", manifests: 1, count: 2, inserted: 2, deleted: 0, ok: true, event_age_ms: 5000, s3_event_age_ms: 5000, queue_origin: "s3" },
    { level: "info", message: "search index busy, requeued", stage: "search-index", org_id: "org-synthetic", session_id: "busy-1", reason: "scope_busy" },
    { level: "error", message: "search index failed", stage: "search-index", org_id: "org-synthetic", session_id: "boom-1", error_code: "clickhouse_unavailable", retryable: true },
  ];
  const goldenFailures = ["m5", "m6", "m7"];

  test("handler with the variable unset: same failures, same lines, same requeues as 6d88b638; no handoff, no queue field", async () => {
    const { deps, pointerRequeued } = setup({ split: false });
    setRuntimeForTests(deps, unarmedEnv);
    const { result, lines } = await capture(() => handler({ Records: records() }, ctx(300_000)));
    expect(failures(result)).toEqual(goldenFailures);
    expect(stable(lines)).toEqual(golden);
    expect(pointerRequeued).toEqual([keyOf("busy-1")]);
    expect(lines.some((l) => "queue" in l)).toBe(false);
  });

  test("processBatch without any queue mark (the 6d88b638 queueRecords shape) gives the same golden", async () => {
    const { deps } = setup({ split: false });
    const { result, lines } = await capture(() => processBatch(queueRecords(records()), deps));
    expect(failures(result)).toEqual(goldenFailures);
    expect(stable(lines)).toEqual(golden);
  });

  test("lambdaEnv is unchanged when the variable is unset or empty", () => {
    const env = { AWS_REGION: REGION, PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "p", SESSION_PIPELINE_SECRET_ARN: "s", SESSION_PIPELINE_QUEUE_URL: "q", PIPELINE_ORG_FROM_PREFIX: "true", SEARCH_INDEX_ENABLED: "1" };
    const plain = lambdaEnv(env);
    expect("searchQueueUrl" in plain).toBe(false);
    expect(lambdaEnv({ ...env, SESSION_PIPELINE_SEARCH_QUEUE_URL: "" })).toEqual(plain);
    // An unparseable pointer URL is still accepted while the split is off (as on 6d88b638); records then carry no queue.
    expect(queueSourceArns(plain)).toEqual({});
  });
});

describe("mixed batches (one function, two mappings)", () => {
  test("pointer records hand off, search records search; each failure is attributed to its own messages", async () => {
    const { deps, sent, searchRequeued } = setup({ send: async (k) => (k === keyOf("s-bad") ? Promise.reject(new Error("synthetic")) : undefined) });
    const ptr = (id: string, key: string): QueueRecord => ({ messageId: id, body: s3Body(key), queue: "pointer" });
    const srch = (id: string, key: string): QueueRecord => ({ messageId: id, body: requeueBody(key, AT), queue: "search" });
    const { result } = await capture(() =>
      processBatch([ptr("p1", keyOf("s-1")), ptr("p2", keyOf("s-bad")), ptr("p3", keyOf("pmp-1")), srch("q1", keyOf("s-1")), srch("q2", keyOf("boom-1")), srch("q3", keyOf("busy-1"))], deps),
    );
    expect(failures(result)).toEqual(["p2", "p3", "q2"]);
    expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual([keyOf("s-1"), keyOf("s-bad"), keyOf("pmp-1")]);
    expect(sent).toEqual([keyOf("s-1")]);
    expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-1", "boom-1", "busy-1"]);
    expect(searchRequeued).toEqual([keyOf("busy-1")]);
  });
});
