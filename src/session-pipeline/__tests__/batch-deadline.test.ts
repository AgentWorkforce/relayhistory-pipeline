/**
 * SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS: the search loop's batch deadline (consumer.ts) and its
 * deferral counter, the env grammar and the per-invocation deadline (lambda.ts), and the
 * counter's place in the requeue body (s3-events.ts). The pointer and search stages are
 * stubbed (spyOn) so the clock and each pass's outcome are exact; the log lines are the real
 * logger's, through logLine and its allowlist.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import * as pointerModule from "../pointer.ts";
import * as searchModule from "../search-index.ts";
import type { PointerDeps, PointerOutcome, SessionScope } from "../pointer.ts";
import type { SearchIndexDeps, SearchIndexResult } from "../search-index.ts";
import { BATCH_DEADLINE_MAX_DEFERRALS, processBatch, type ConsumerDeps, type QueueRecord } from "../consumer.ts";
import {
  SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MAX_MS,
  SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MIN_MS,
  invocationDeps,
  lambdaEnv,
  sqsRequeue,
} from "../lambda.ts";
import { BATCH_DEADLINE_DEFERRALS_FIELD, parseQueueBody, requeueBody } from "../s3-events.ts";
import type { awsJsonCall } from "../aws-json.ts";

const AT = "2026-11-01T10:00:00.000Z";
const key = (s: string) => `96bulls/org_1/ws_1/claude/${s}/manifest.json`;
const scopeOf = (k: string): SessionScope => ({ orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: k.split("/")[4]! });
const put = (...keys: string[]) =>
  JSON.stringify({ Records: keys.map((k, n) => ({ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "b" }, object: { key: k, eTag: "e", sequencer: `0${n}` } } })) });
const msg = (messageId: string, body: string): QueueRecord => ({ messageId, body });
const complete: SearchIndexResult = { manifests: 1, rows: 1, inserted: 1, deleted: 0, complete: true, reasons: [] };

type Line = Record<string, unknown>;
/** The real logger's JSON lines while `work` runs. */
async function captured<T>(work: () => Promise<T>): Promise<{ out: T; lines: Line[] }> {
  const lines: Line[] = [];
  const o = process.stdout.write.bind(process.stdout);
  const e = process.stderr.write.bind(process.stderr);
  const grab = (chunk: unknown) => {
    for (const l of String(chunk).split("\n")) if (l.startsWith("{")) lines.push(JSON.parse(l) as Line);
    return true;
  };
  process.stdout.write = grab as typeof process.stdout.write;
  process.stderr.write = grab as typeof process.stderr.write;
  try {
    return { out: await work(), lines };
  } finally {
    process.stdout.write = o;
    process.stderr.write = e;
  }
}

let pointerFails: Set<string>;
let clock: number;
/** Each search pass takes this long on the clock. */
let passMs: number;
let passes: string[];
let pointerSpy: ReturnType<typeof spyOn>;
let searchSpy: ReturnType<typeof spyOn>;

beforeEach(() => {
  pointerFails = new Set();
  clock = 0;
  passMs = 60;
  passes = [];
  pointerSpy = spyOn(pointerModule, "indexPointer").mockImplementation((async (landed: { key: string }): Promise<PointerOutcome> => {
    if (pointerFails.has(landed.key)) throw Object.assign(new Error("pointer failed"), { code: "P" });
    return { status: "indexed", scope: scopeOf(landed.key), manifestKey: landed.key, revision: 1 };
  }) as never);
  searchSpy = spyOn(searchModule, "indexSearchScope").mockImplementation((async (scope: SessionScope): Promise<SearchIndexResult> => {
    passes.push(scope.sessionId);
    clock += passMs;
    return complete;
  }) as never);
});
afterEach(() => {
  pointerSpy.mockRestore();
  searchSpy.mockRestore();
});

type Requeued = { key: string; time?: string; deferrals?: number };
function deps(o: { deadlineAt?: number; requeue?: ConsumerDeps["requeue"] | null; search?: Partial<SearchIndexDeps> } = {}) {
  const requeued: Requeued[] = [];
  const d: ConsumerDeps = {
    pointer: {} as PointerDeps,
    search: (o.search ?? {}) as SearchIndexDeps,
    ...(o.requeue === null
      ? {}
      : {
          requeue:
            o.requeue ??
            (async (k, time, deferrals) => void requeued.push({ key: k, ...(time === undefined ? {} : { time }), ...(deferrals === undefined ? {} : { deferrals }) })),
        }),
    ...(o.deadlineAt === undefined ? {} : { batchDeadline: { at: o.deadlineAt, now: () => clock } }),
  };
  return { d, requeued };
}
const failures = (r: { batchItemFailures: { itemIdentifier: string }[] }) => r.batchItemFailures.map((f) => f.itemIdentifier).sort();
const deadlineLines = (lines: Line[]) => lines.filter((l) => typeof l.reason === "string" && (l.reason as string).startsWith("batch_deadline"));

describe("batch deadline in the search loop", () => {
  const batch = () => [msg("m1", put(key("a"))), msg("m2", put(key("b"))), msg("m3", put(key("c")))];

  test("not reached: the same passes, failures and no requeue as without it", async () => {
    const base = deps();
    const r0 = await processBatch(batch(), base.d);
    const order0 = [...passes];
    passes = [];
    clock = 0;
    const armed = deps({ deadlineAt: 10_000 });
    const r1 = await processBatch(batch(), armed.d);
    expect(passes).toEqual(order0);
    expect(passes).toEqual(["a", "b", "c"]);
    expect(r1).toEqual(r0);
    expect(armed.requeued).toEqual([]);
  });

  test("reached before scope 3: earlier scopes indexed (the running pass is not interrupted), later ones requeued once each with the carried time and count 1, none failed", async () => {
    const extra = [msg("m4", put(key("d")))];
    const { d, requeued } = deps({ deadlineAt: 100 });
    const { out, lines } = await captured(() => processBatch([...batch(), ...extra], d));
    // a starts at 0, b at 60 (crosses 100 while running and finishes), c and d are not started.
    expect(passes).toEqual(["a", "b"]);
    expect(lines.filter((l) => l.message === "search index reconciled").map((l) => l.session_id)).toEqual(["a", "b"]);
    expect(requeued).toEqual([
      { key: key("c"), time: AT, deferrals: 1 },
      { key: key("d"), time: AT, deferrals: 1 },
    ]);
    expect(failures(out)).toEqual([]);
    const dl = deadlineLines(lines);
    expect(dl.map((l) => [l.level, l.message, l.session_id, l.reason, l.batch_deadline_deferrals])).toEqual([
      ["info", "search index past the batch deadline, requeued", "c", "batch_deadline", 1],
      ["info", "search index past the batch deadline, requeued", "d", "batch_deadline", 1],
    ]);
  });

  test("requeue throws: the unstarted scopes' message ids are failures; finished scopes are not", async () => {
    const { d } = deps({ deadlineAt: 100, requeue: async () => { throw Object.assign(new Error("send failed"), { code: "Q" }); } });
    const { out, lines } = await captured(() => processBatch(batch(), d));
    expect(passes).toEqual(["a", "b"]);
    expect(failures(out)).toEqual(["m3"]);
    expect(lines.filter((l) => l.message === "search index failed").map((l) => [l.session_id, l.error_code])).toEqual([["c", "Q"]]);
  });

  test("no requeue dep: the unstarted scopes' message ids are failures", async () => {
    const { d } = deps({ deadlineAt: 100, requeue: null });
    const { out } = await captured(() => processBatch(batch(), d));
    expect(failures(out)).toEqual(["m3"]);
  });

  test("a scope whose messages all failed in the pointer stage is still skipped: not searched, not requeued", async () => {
    // m1 carries a (fails in the pointer stage) and b (indexes): b's scope has only m1, which failed.
    pointerFails.add(key("a"));
    const { d, requeued } = deps({ deadlineAt: 0 });
    const { out } = await captured(() => processBatch([msg("m1", put(key("a"), key("b"))), msg("m2", put(key("c")))], d));
    expect(passes).toEqual([]);
    expect(requeued).toEqual([{ key: key("c"), time: AT, deferrals: 1 }]);
    expect(failures(out)).toEqual(["m1"]);
  });

  test("the counter increments across deferrals, then a scope at the cap runs first and is indexed", async () => {
    expect(BATCH_DEADLINE_MAX_DEFERRALS).toBe(3);
    let body = put(key("a"));
    for (let n = 1; n <= BATCH_DEADLINE_MAX_DEFERRALS; n++) {
      clock = 0;
      const { d, requeued } = deps({ deadlineAt: 0 });
      await captured(() => processBatch([msg(`m${n}`, body)], d));
      expect(requeued).toEqual([{ key: key("a"), time: AT, deferrals: n }]);
      body = requeueBody(requeued[0]!.key, requeued[0]!.time, requeued[0]!.deferrals);
      expect(JSON.parse(body).reconcile[BATCH_DEADLINE_DEFERRALS_FIELD]).toBe(n);
    }
    // Now at the cap: listed last, it runs first; the deadline is crossed during its pass.
    clock = 0;
    const { d, requeued } = deps({ deadlineAt: 30 });
    const { out } = await captured(() => processBatch([msg("x1", put(key("b"))), msg("x2", put(key("c"))), msg("x3", body)], d));
    expect(passes).toEqual(["a"]);
    expect(requeued).toEqual([
      { key: key("b"), time: AT, deferrals: 1 },
      { key: key("c"), time: AT, deferrals: 1 },
    ]);
    expect(failures(out)).toEqual([]);
  });

  test("a scope at the cap that still cannot start is failed with an error line, not requeued; the other scopes of the batch are requeued as before", async () => {
    const capped = requeueBody(key("a"), AT, BATCH_DEADLINE_MAX_DEFERRALS);
    const { d, requeued } = deps({ deadlineAt: 0 });
    const { out, lines } = await captured(() => processBatch([msg("m1", put(key("b"))), msg("m2", capped), msg("m3", requeueBody(key("c"), AT, 1))], d));
    expect(passes).toEqual([]);
    expect(failures(out)).toEqual(["m2"]);
    expect(requeued).toEqual([
      { key: key("b"), time: AT, deferrals: 1 },
      { key: key("c"), time: AT, deferrals: 2 },
    ]);
    const dl = deadlineLines(lines);
    expect(dl.map((l) => [l.level, l.session_id, l.reason, l.batch_deadline_deferrals])).toEqual([
      ["error", "a", "batch_deadline_deferrals_exhausted", 3],
      ["info", "b", "batch_deadline", 1],
      ["info", "c", "batch_deadline", 2],
    ]);
  });

  test("a mixed batch with the deadline not reached: the capped scope first, then the others in arrival order, all indexed", async () => {
    const { d, requeued } = deps({ deadlineAt: 10_000 });
    const out = await processBatch([msg("m1", put(key("b"))), msg("m2", requeueBody(key("a"), AT, 5)), msg("m3", put(key("c")))], d);
    expect(passes).toEqual(["a", "b", "c"]);
    expect(requeued).toEqual([]);
    expect(failures(out)).toEqual([]);
  });

  test("a scope's counter is the largest over its messages", async () => {
    const { d, requeued } = deps({ deadlineAt: 0 });
    await captured(() => processBatch([msg("m1", requeueBody(key("a"), AT, 1)), msg("m2", requeueBody(key("a"), AT, 2)), msg("m3", put(key("a")))], d));
    expect(requeued).toEqual([{ key: key("a"), time: AT, deferrals: 3 }]);
  });

  test("a malformed or out-of-range counter is read as 0 (or, when a large safe integer, as at the cap); never a throw", async () => {
    for (const bad of [-1, 1.5, "2", null, true, [2], { n: 2 }, Number.MAX_SAFE_INTEGER + 2]) {
      clock = 0;
      const body = JSON.stringify({ reconcile: { key: key("a"), requeued: true, oldest_event_time: AT, [BATCH_DEADLINE_DEFERRALS_FIELD]: bad } });
      const { d, requeued } = deps({ deadlineAt: 0 });
      const { out } = await captured(() => processBatch([msg("m1", body)], d));
      expect([bad, requeued]).toEqual([bad, [{ key: key("a"), time: AT, deferrals: 1 }]]);
      expect(failures(out)).toEqual([]);
    }
    const { d, requeued } = deps({ deadlineAt: 0 });
    const huge = JSON.stringify({ reconcile: { key: key("a"), requeued: true, [BATCH_DEADLINE_DEFERRALS_FIELD]: 1e9 } });
    const { out } = await captured(() => processBatch([msg("m1", huge)], d));
    expect(requeued).toEqual([]);
    expect(failures(out)).toEqual(["m1"]);
  });

  test("the counter never reaches the pointer stage", async () => {
    const { d } = deps({ deadlineAt: 10_000 });
    await processBatch([msg("m1", requeueBody(key("a"), AT, 2))], d);
    expect(pointerSpy.mock.calls[0]![0]).toEqual({ provider: "s3", key: key("a") });
  });

  test("off (no batchDeadline): no counter written, no reordering, and a busy requeue's body is the base body", async () => {
    const busy = true;
    searchSpy.mockImplementation((async (scope: SessionScope): Promise<SearchIndexResult> => {
      passes.push(scope.sessionId);
      clock += 1_000_000;
      return busy ? { ...complete, busy: true } : complete;
    }) as never);
    const { d, requeued } = deps();
    await captured(() => processBatch([msg("m1", put(key("b"))), msg("m2", requeueBody(key("a"), AT, 5)), msg("m3", put(key("c")))], d));
    expect(passes).toEqual(["b", "a", "c"]);
    expect(requeued).toEqual([{ key: key("b"), time: AT }, { key: key("a"), time: AT }, { key: key("c"), time: AT }]);
    expect(requeueBody(key("b"), AT)).toBe(JSON.stringify({ reconcile: { key: key("b"), requeued: true, oldest_event_time: AT } }));
  });

  test("streaming armed with its deadline passed first: the stream path and its reason are unchanged, no counter", async () => {
    const { d, requeued } = deps({
      deadlineAt: 10_000,
      search: { stream: { maxReadBytes: 1, maxReadEvents: 1 }, streamDeadline: { at: 0, now: () => clock } },
    });
    const { lines } = await captured(() => processBatch([msg("m1", put(key("a")))], d));
    expect(passes).toEqual([]);
    expect(requeued).toEqual([{ key: key("a"), time: AT }]);
    expect(lines.filter((l) => l.reason !== undefined).map((l) => l.reason)).toEqual(["stream_batch_deadline"]);
  });

  test("streaming armed with the batch deadline earlier: the batch deadline applies", async () => {
    const { d, requeued } = deps({
      deadlineAt: 0,
      search: { stream: { maxReadBytes: 1, maxReadEvents: 1 }, streamDeadline: { at: 10_000, now: () => clock } },
    });
    await captured(() => processBatch([msg("m1", put(key("a")))], d));
    expect(requeued).toEqual([{ key: key("a"), time: AT, deferrals: 1 }]);
  });
});

describe("the counter in the requeue body", () => {
  test("only the batch-deadline requeue writes it; a body without it is byte-identical to before", () => {
    expect(requeueBody("k")).toBe('{"reconcile":{"key":"k","requeued":true}}');
    expect(requeueBody("k", AT)).toBe(`{"reconcile":{"key":"k","requeued":true,"oldest_event_time":"${AT}"}}`);
    expect(requeueBody("k", AT, 2)).toBe(`{"reconcile":{"key":"k","requeued":true,"oldest_event_time":"${AT}","batch_deadline_deferrals":2}}`);
  });

  test("the parser reads the same key, etag, version, time and origin with or without it (an unknown field to a parser that picks named fields)", () => {
    const strip = (p: ReturnType<typeof parseQueueBody>) =>
      p.kind === "events" ? { ...p, events: p.events.map(({ batchDeadlineDeferrals: _d, ...e }) => e) } : p;
    for (const [withIt, without] of [
      [requeueBody("k", AT, 2), requeueBody("k", AT)],
      [JSON.stringify({ reconcile: { key: "k", etag: "e", version: "v", batch_deadline_deferrals: 1 } }), JSON.stringify({ reconcile: { key: "k", etag: "e", version: "v" } })],
    ] as const)
      expect(strip(parseQueueBody(withIt))).toEqual(parseQueueBody(without));
    expect(parseQueueBody(requeueBody("k", AT, 2))).toMatchObject({ kind: "events", events: [{ batchDeadlineDeferrals: 2 }] });
  });

  test("sqsRequeue sends the counter only when given one", async () => {
    const bodies: string[] = [];
    const call = (async (req: { body: { MessageBody: string } }) => void bodies.push(req.body.MessageBody)) as unknown as typeof awsJsonCall;
    const send = sqsRequeue({ region: "us-east-1", queueUrl: "https://sqs" }, call, () => ({ accessKeyId: "a", secretAccessKey: "b" }) as never);
    await send("k", AT);
    await send("k", AT, 3);
    expect(bodies).toEqual([requeueBody("k", AT), requeueBody("k", AT, 3)]);
  });
});

describe("SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS", () => {
  const base = {
    AWS_REGION: "us-east-1",
    PIPELINE_RAW_S3_BUCKET: "b",
    PIPELINE_KEY_PREFIX: "/96bulls/",
    PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "arn",
    SESSION_PIPELINE_QUEUE_URL: "https://sqs",
    SEARCH_INDEX_ENABLED: "1",
  };

  test("unset or empty: the env is exactly as without it", () => {
    const plain = lambdaEnv(base);
    expect(Object.keys(plain)).not.toContain("searchBatchDeadlineMarginMs");
    expect(lambdaEnv({ ...base, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS: "" })).toEqual(plain);
  });

  test("an integer of ms within the bounds sets it", () => {
    expect(SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MIN_MS).toBe(1_000);
    expect(SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MAX_MS).toBe(299_000);
    for (const v of ["1000", "90000", "299000"])
      expect(lambdaEnv({ ...base, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS: v })).toEqual({ ...lambdaEnv(base), searchBatchDeadlineMarginMs: Number(v) });
  });

  test("refuses negative, NaN, fractional, too small, too large and padded values, naming the variable but not the value", () => {
    for (const v of ["-1", "0", "999", "NaN", "abc", "1.5", "90000.0", "1e5", "299001", "300000", "9999999999", " 90000", "90000 ", "+90000", "090000", "0x100"]) {
      let message = "";
      try {
        lambdaEnv({ ...base, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS: v });
      } catch (e) {
        message = (e as Error).message;
      }
      expect([v, message]).toEqual([v, "session pipeline: SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS must be an integer from 1000 to 299000"]);
    }
  });

  test("refuses a valid value without SEARCH_INDEX_ENABLED=1", () => {
    for (const enabled of [undefined, "0"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: enabled, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS: "90000" })).toThrow(
        "session pipeline: SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS needs SEARCH_INDEX_ENABLED=1",
      );
  });
});

describe("invocationDeps with a batch deadline margin", () => {
  const search = {} as SearchIndexDeps;
  const d: ConsumerDeps = { pointer: {} as PointerDeps, search };
  const ctx = (ms: number) => {
    const c = { calls: 0, getRemainingTimeInMillis: () => (c.calls++, ms) };
    return c;
  };

  test("unset with a context present: the deps themselves, the context not read, no deadline, and a slow batch never requeues", async () => {
    const c = ctx(1);
    const out = invocationDeps(d, c, () => 0);
    expect(out).toBe(d);
    expect(c.calls).toBe(0);
    expect(out).not.toHaveProperty("batchDeadline");
    const requeued: string[] = [];
    passMs = 1_000_000;
    await captured(() => processBatch([msg("m1", put(key("a"))), msg("m2", put(key("b")))], { ...out, requeue: async (k) => void requeued.push(k) }));
    expect(passes).toEqual(["a", "b"]);
    expect(requeued).toEqual([]);
  });

  test("set: the deadline at now + remaining - margin; the search deps untouched when streaming is off", () => {
    const out = invocationDeps(d, ctx(300_000), () => 1_000, 90_000);
    expect(out.batchDeadline).toEqual({ at: 1_000 + 300_000 - 90_000 });
    expect(out.search).toBe(search);
    expect(d).not.toHaveProperty("batchDeadline");
  });

  test("set without a context, or without the search stage: no deadline", () => {
    expect(invocationDeps(d, undefined, () => 0, 90_000)).not.toHaveProperty("batchDeadline");
    const none: ConsumerDeps = { pointer: {} as PointerDeps };
    expect(invocationDeps(none, ctx(300_000), () => 0, 90_000)).toBe(none);
  });

  test("set with streaming armed: both deadlines, the stream deps as without the margin", () => {
    const armed: ConsumerDeps = { ...d, search: { ...search, stream: { maxReadBytes: 1, maxReadEvents: 1 } } };
    const without = invocationDeps(armed, ctx(300_000), () => 1_000);
    const withIt = invocationDeps(armed, ctx(300_000), () => 1_000, 90_000);
    expect(withIt.search).toEqual(without.search);
    expect(withIt.batchDeadline).toEqual({ at: 1_000 + 300_000 - 90_000 });
    expect(without).not.toHaveProperty("batchDeadline");
  });
});
