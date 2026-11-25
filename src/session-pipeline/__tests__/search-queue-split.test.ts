/**
 * SESSION_PIPELINE_SEARCH_QUEUE_URL (the search-queue split): armed, a pointer-queue batch runs the
 * pointer stage and hands each scope's search pass to the search queue; a search-queue record runs
 * only the search stage, and its requeues go to the search queue. Unset: processBatch behaves and
 * logs exactly as before. Pure fakes (the two stages are spied), synthetic data only.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { PipelineError } from "@rhc/lib/pipeline/errors.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { processBatch, type ConsumerDeps, type QueueRecord } from "../consumer.ts";
import { handler, isFromQueue, lambdaEnv, queueRecords, searchQueueUrlFromEnv, setRuntimeForTests, sqsSearchHandoff, BUSY_REQUEUE_DELAY_SECONDS } from "../lambda.ts";
import * as pointerModule from "../pointer.ts";
import * as searchModule from "../search-index.ts";
import type { PointerDeps, SessionScope } from "../pointer.ts";
import type { SearchIndexDeps, SearchIndexResult } from "../search-index.ts";
import { requeueBody } from "../s3-events.ts";

const SEARCH_URL = "https://sqs.example.invalid/owner-synthetic/search-synthetic";
const SEARCH_ARN = "arn:aws:sqs:region-synthetic:owner-synthetic:search-synthetic";
const POINTER_ARN = "arn:aws:sqs:region-synthetic:owner-synthetic:events-synthetic";
const AT = "2026-11-06T00:00:00.000Z";
const keyOf = (session: string, machine = "m-synthetic") =>
  `96bulls/org-synthetic/v1/ws=ws-synthetic/m=${machine}/h=claude/d=2026/11/06/s=${session}/manifest.json`;
const s3Body = (key: string, sequencer = "0A") =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket-synthetic" }, object: { key, eTag: "e", sequencer } } }] });

const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
/** Only what resolveScope reads: the pointer stage itself is spied. */
const pointer = { reader: { bucket: undefined }, orgs: prefixOrgResolver, allowPrefix: (prefix: string | undefined, org: string) => prefixAllowed(bindings, prefix, org) } as unknown as PointerDeps;

const done: SearchIndexResult = { manifests: 1, rows: 2, inserted: 2, deleted: 0, complete: true, reasons: [] };
/** The search outcome chosen by session id prefix. */
function searchOutcome(scope: SessionScope): SearchIndexResult {
  if (scope.sessionId.startsWith("busy")) return { ...done, manifests: 0, rows: 0, inserted: 0, complete: false, busy: true, reasons: ["scope_busy"] };
  if (scope.sessionId.startsWith("defer")) return { ...done, manifests: 0, rows: 0, inserted: 0, complete: false, deferred: true, reasons: [] };
  if (scope.sessionId.startsWith("boom")) throw new PipelineError("clickhouse_unavailable", "synthetic failure", true);
  return done;
}

let pointerSpy: ReturnType<typeof spyOn<typeof pointerModule, "indexPointer">>;
let searchSpy: ReturnType<typeof spyOn<typeof searchModule, "indexSearchScope">>;
beforeEach(() => {
  pointerSpy = spyOn(pointerModule, "indexPointer").mockImplementation(async (landed, deps) => {
    const resolved = await pointerModule.resolveScope(landed, deps);
    if ("status" in resolved) return resolved;
    if (resolved.scope.sessionId.startsWith("pmp")) throw new PipelineError("pointer_move_pending", "synthetic", true);
    return { status: "indexed", scope: resolved.scope, manifestKey: landed.key, revision: 1 };
  });
  searchSpy = spyOn(searchModule, "indexSearchScope").mockImplementation(async (scope) => searchOutcome(scope));
});
afterEach(() => {
  pointerSpy.mockRestore();
  searchSpy.mockRestore();
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
const stable = (lines: Record<string, unknown>[]) => lines.map(({ ts: _t, duration_ms: _d, ...rest }) => rest);
const failures = (r: { batchItemFailures: { itemIdentifier: string }[] }) => r.batchItemFailures.map((f) => f.itemIdentifier).sort();

/** Deps with the split armed (or not), recording every send. */
function setup(opts: { split?: boolean; sendFails?: (key: string) => boolean; search?: Partial<SearchIndexDeps> } = {}) {
  const sent: string[] = [];
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
            send: async (key) => {
              if (opts.sendFails?.(key)) throw new Error("synthetic send failure");
              sent.push(key);
            },
            requeue: async (key) => void searchRequeued.push(key),
          },
        }),
  };
  return { deps, sent, searchRequeued, pointerRequeued };
}
const ptr = (id: string, key: string, sequencer?: string): QueueRecord => ({ messageId: id, body: s3Body(key, sequencer) });
const srch = (id: string, key: string): QueueRecord => ({ messageId: id, body: requeueBody(key, AT), fromSearchQueue: true });

describe("split off (default)", () => {
  test("the search-queue mark is ignored: same failures, same log lines, search inline, no handoff", async () => {
    const { deps, pointerRequeued } = setup({ split: false });
    const batch = [ptr("m1", keyOf("s-1")), ptr("m2", keyOf("busy-1"))];
    const plain = await capture(() => processBatch(batch, deps));
    const marked = await capture(() => processBatch(batch.map((r) => ({ ...r, fromSearchQueue: true as const })), deps));
    expect(marked.result).toEqual(plain.result);
    expect(stable(marked.lines)).toEqual(stable(plain.lines));
    expect(plain.result.batchItemFailures).toEqual([]);
    expect(pointerSpy).toHaveBeenCalledTimes(4);
    expect(searchSpy).toHaveBeenCalledTimes(4);
    expect(pointerRequeued).toEqual([keyOf("busy-1"), keyOf("busy-1")]);
    expect(plain.lines.map((l) => l.message)).not.toContain("search index handed off");
    expect(plain.lines.some((l) => l.reason === "search_handoff")).toBe(false);
  });

  test("lambdaEnv and queueRecords are unchanged when the variable is unset or empty", () => {
    const env = { AWS_REGION: "r", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "p", SESSION_PIPELINE_SECRET_ARN: "s", SESSION_PIPELINE_QUEUE_URL: "q", PIPELINE_ORG_FROM_PREFIX: "true", SEARCH_INDEX_ENABLED: "1" };
    const plain = lambdaEnv(env);
    expect("searchQueueUrl" in plain).toBe(false);
    expect(lambdaEnv({ ...env, SESSION_PIPELINE_SEARCH_QUEUE_URL: "" })).toEqual(plain);
    expect(lambdaEnv({ ...env, SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL })).toEqual({ ...plain, searchQueueUrl: SEARCH_URL });
    const sqs = [{ messageId: "m", body: "{}", eventSourceARN: SEARCH_ARN, attributes: { SentTimestamp: "1" } }];
    expect(queueRecords(sqs)).toEqual([{ messageId: "m", body: "{}", sentTimestamp: "1" }]);
    expect(queueRecords(sqs, false, undefined)).toEqual(queueRecords(sqs));
  });
});

describe("split on: the pointer queue", () => {
  test("one handoff per scope, after the pointer stage; no search pass runs here", async () => {
    const { deps, sent, pointerRequeued } = setup();
    const a = keyOf("s-1"), a2 = keyOf("s-1", "m-other"), b = keyOf("s-2");
    const { result, lines } = await capture(() => processBatch([ptr("m1", a), ptr("m2", a2), ptr("m3", b), ptr("m4", a, "0B")], deps));
    expect(result.batchItemFailures).toEqual([]);
    expect(pointerSpy).toHaveBeenCalledTimes(3);
    expect(searchSpy).not.toHaveBeenCalled();
    // Two scopes (s-1 has two keys), each sent once with the first key the pointer stage gave it.
    expect(sent).toEqual([a, b]);
    expect(pointerRequeued).toEqual([]);
    const handed = lines.filter((l) => l.reason === "search_handoff");
    expect(handed.map((l) => [l.level, l.stage, l.org_id, l.session_id])).toEqual([
      ["info", "search-index", "org-synthetic", "s-1"],
      ["info", "search-index", "org-synthetic", "s-2"],
    ]);
  });

  test("a failed send fails exactly that scope's messages", async () => {
    const { deps, sent } = setup({ sendFails: (k) => k === keyOf("s-2") });
    const { result, lines } = await capture(() => processBatch([ptr("m1", keyOf("s-1")), ptr("m2", keyOf("s-2")), ptr("m3", keyOf("s-2", "m-other")), ptr("m4", keyOf("s-3"))], deps));
    expect(failures(result)).toEqual(["m2", "m3"]);
    expect(sent).toEqual([keyOf("s-1"), keyOf("s-3")]);
    const failed = lines.find((l) => l.level === "error" && l.reason === "search_handoff")!;
    expect(failed).toMatchObject({ stage: "search-index", org_id: "org-synthetic", session_id: "s-2" });
    expect(JSON.stringify(failed)).not.toContain(SEARCH_URL);
  });

  test("a pointer failure is not handed off; the fast retry still fires for pointer_move_pending only", async () => {
    const { deps, sent } = setup();
    const changed: [string, number][] = [];
    deps.retryVisibility = { seconds: 60, change: async (h, s) => void changed.push([h, s]) };
    const batch: QueueRecord[] = [
      { ...ptr("m1", keyOf("pmp-1")), receiptHandle: "h1" },
      { ...ptr("m2", keyOf("s-1")), receiptHandle: "h2" },
      { ...srch("m3", keyOf("boom-1")), receiptHandle: "h3" },
    ];
    const { result } = await capture(() => processBatch(batch, deps));
    expect(failures(result)).toEqual(["m1", "m3"]);
    expect(sent).toEqual([keyOf("s-1")]);
    // Only the pointer_move_pending message gets the short visibility; the search failure keeps the full timeout.
    expect(changed).toEqual([["h1", 60]]);
  });
});

describe("split on: the search queue", () => {
  test("runs the search stage for the key's scope, never the pointer stage", async () => {
    const { deps, sent } = setup();
    const { result, lines } = await capture(() => processBatch([srch("m1", keyOf("s-1")), srch("m2", keyOf("s-1", "m-other")), srch("m3", keyOf("s-2"))], deps));
    expect(result.batchItemFailures).toEqual([]);
    expect(pointerSpy).not.toHaveBeenCalled();
    expect(sent).toEqual([]);
    expect(searchSpy.mock.calls.map((c) => c[0])).toEqual([
      { orgId: "org-synthetic", workspaceId: "ws-synthetic", harness: "claude", sessionId: "s-1" },
      { orgId: "org-synthetic", workspaceId: "ws-synthetic", harness: "claude", sessionId: "s-2" },
    ]);
    const reconciled = lines.filter((l) => l.stage === "search-index" && l.ok === true);
    expect(reconciled.map((l) => [l.session_id, l.queue_origin])).toEqual([["s-1", "requeue"], ["s-2", "requeue"]]);
  });

  test("busy, deferred and past-the-deadline passes requeue to the search queue, not the pointer queue", async () => {
    const { deps, searchRequeued, pointerRequeued } = setup();
    const { result, lines } = await capture(() => processBatch([srch("m1", keyOf("busy-1")), srch("m2", keyOf("defer-1")), srch("m3", keyOf("s-1"))], deps));
    expect(result.batchItemFailures).toEqual([]);
    expect(searchRequeued).toEqual([keyOf("busy-1"), keyOf("defer-1")]);
    expect(pointerRequeued).toEqual([]);
    expect(lines.filter((l) => l.reason === "scope_busy" || l.reason === "stream_deferred").map((l) => l.session_id)).toEqual(["busy-1", "defer-1"]);

    const late = setup({ search: { stream: {} as SearchIndexDeps["stream"], streamDeadline: { at: 0 } } });
    const past = await capture(() => processBatch([srch("m4", keyOf("s-2"))], late.deps));
    expect(past.result.batchItemFailures).toEqual([]);
    expect(late.searchRequeued).toEqual([keyOf("s-2")]);
    expect(late.pointerRequeued).toEqual([]);
    expect(past.lines.some((l) => l.reason === "stream_batch_deadline")).toBe(true);
  });

  test("a failed search requeue fails that message; an unreadable body fails; an ignored key succeeds", async () => {
    const { deps } = setup();
    deps.searchHandoff!.requeue = async () => {
      throw new Error("synthetic");
    };
    const { result } = await capture(() =>
      processBatch([srch("m1", keyOf("busy-1")), { messageId: "m2", body: "not json", fromSearchQueue: true }, srch("m3", "96bulls/not-a-session"), srch("m4", keyOf("s-1"))], deps),
    );
    expect(failures(result)).toEqual(["m1", "m2"]);
    expect(pointerSpy).not.toHaveBeenCalled();
  });
});

describe("split on: a mixed batch", () => {
  test("pointer records hand off, search records search; each failure is attributed to its own messages", async () => {
    const { deps, sent, searchRequeued } = setup({ sendFails: (k) => k === keyOf("s-bad") });
    const { result } = await capture(() =>
      processBatch(
        [ptr("p1", keyOf("s-1")), ptr("p2", keyOf("s-bad")), ptr("p3", keyOf("pmp-1")), srch("q1", keyOf("s-1")), srch("q2", keyOf("boom-1")), srch("q3", keyOf("busy-1"))],
        deps,
      ),
    );
    expect(failures(result)).toEqual(["p2", "p3", "q2"]);
    expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual([keyOf("s-1"), keyOf("s-bad"), keyOf("pmp-1")]);
    expect(sent).toEqual([keyOf("s-1")]);
    expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-1", "boom-1", "busy-1"]);
    expect(searchRequeued).toEqual([keyOf("busy-1")]);
  });
});

describe("lambda wiring", () => {
  test("the env grammar: an https SQS queue URL with SEARCH_INDEX_ENABLED=1; errors name the variable, never the value", () => {
    expect(searchQueueUrlFromEnv({})).toBeUndefined();
    expect(searchQueueUrlFromEnv({ SESSION_PIPELINE_SEARCH_QUEUE_URL: "" })).toBeUndefined();
    expect(searchQueueUrlFromEnv({ SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL, SEARCH_INDEX_ENABLED: "1" })).toBe(SEARCH_URL);
    for (const bad of ["http://sqs.example.invalid/owner/q", "https://sqs.example.invalid/q", "https://sqs.example.invalid/owner/q/extra", "search-synthetic", "https://sqs.example.invalid/owner/q?x=1"]) {
      let message = "";
      try {
        searchQueueUrlFromEnv({ SESSION_PIPELINE_SEARCH_QUEUE_URL: bad, SEARCH_INDEX_ENABLED: "1" });
      } catch (e) {
        message = (e as Error).message;
      }
      expect(message).toBe("session pipeline: SESSION_PIPELINE_SEARCH_QUEUE_URL must be an https SQS queue URL");
    }
    expect(() => searchQueueUrlFromEnv({ SESSION_PIPELINE_SEARCH_QUEUE_URL: SEARCH_URL })).toThrow(/SESSION_PIPELINE_SEARCH_QUEUE_URL needs SEARCH_INDEX_ENABLED=1/);
  });

  test("search-queue records are recognised by eventSourceARN: same owner and exact queue name", () => {
    expect(isFromQueue(SEARCH_ARN, SEARCH_URL)).toBe(true);
    expect(isFromQueue(POINTER_ARN, SEARCH_URL)).toBe(false);
    expect(isFromQueue("arn:aws:sqs:region-synthetic:owner-other:search-synthetic", SEARCH_URL)).toBe(false);
    expect(isFromQueue("arn:aws:sqs:region-synthetic:owner-synthetic:x-search-synthetic", SEARCH_URL)).toBe(false);
    expect(isFromQueue(undefined, SEARCH_URL)).toBe(false);
    const sqs = [
      { messageId: "a", body: "{}", eventSourceARN: SEARCH_ARN },
      { messageId: "b", body: "{}", eventSourceARN: POINTER_ARN },
    ];
    expect(queueRecords(sqs, false, SEARCH_URL)).toEqual([
      { messageId: "a", body: "{}", sentTimestamp: undefined, fromSearchQueue: true },
      { messageId: "b", body: "{}", sentTimestamp: undefined },
    ]);
  });

  test("the handoff and the search requeue reuse the requeue SendMessage, to the search queue", async () => {
    const calls: { body: Record<string, unknown> }[] = [];
    const h = sqsSearchHandoff({ region: "region-synthetic", searchQueueUrl: SEARCH_URL }, (async (req: { body: Record<string, unknown> }) => void calls.push(req)) as never, () => ({}) as never);
    await h.send(keyOf("s-1"), AT);
    await h.requeue(keyOf("s-1"));
    expect(calls.map((c) => c.body)).toEqual([
      { QueueUrl: SEARCH_URL, MessageBody: requeueBody(keyOf("s-1"), AT), DelaySeconds: 0 },
      { QueueUrl: SEARCH_URL, MessageBody: requeueBody(keyOf("s-1")), DelaySeconds: BUSY_REQUEUE_DELAY_SECONDS },
    ]);
  });

  test("the handler marks search-queue records only while the split is armed", async () => {
    const { deps, sent } = setup();
    setRuntimeForTests(deps, { searchQueueUrl: SEARCH_URL });
    try {
      const event = { Records: [{ messageId: "a", body: requeueBody(keyOf("s-1")), eventSourceARN: SEARCH_ARN }, { messageId: "b", body: s3Body(keyOf("s-2")), eventSourceARN: POINTER_ARN }] };
      expect(await capture(() => handler(event)).then((c) => c.result)).toEqual({ batchItemFailures: [] });
      expect(pointerSpy.mock.calls.map((c) => c[0].key)).toEqual([keyOf("s-2")]);
      expect(searchSpy.mock.calls.map((c) => c[0].sessionId)).toEqual(["s-1"]);
      expect(sent).toEqual([keyOf("s-2")]);
    } finally {
      setRuntimeForTests(undefined);
    }
  });
});
