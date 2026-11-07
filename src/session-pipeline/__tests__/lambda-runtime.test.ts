import { expect, test } from "bun:test";
import { dispatchRegisteredLambda, type PipelineLambdaEvent } from "../lambda-runtime.ts";
import { parseQueueBody } from "../s3-events.ts";
import { settle, scheduleItem, type PipelineRuntimeAdapter, type WorkItem } from "@rhc/runtime/contracts.ts";
import { createPipelineLambdaHandler } from "../lambda.ts";

/** Test-only platform driver. Provider conformance remains separate evidence. */
const runtime = (items: readonly WorkItem[], schedule = false): PipelineRuntimeAdapter => ({
  kind: "lambda",
  capabilities: () => ({ kind: "lambda", trigger: "push", maxBatch: 10, perItemDelay: false, partialBatchFailure: true, scheduler: "platform", scopeSerialization: "advisory-lock", deadLetter: "platform-dlq" }),
  bind(work) { return { async handler() {
    const result = await settle(work, items, { runtime: "lambda" });
    if (schedule) { if ([...result.values()].some(v => v.status !== "done")) throw new Error("fixture retry"); return; }
    return { batchItemFailures: [...result].filter(([, value]) => value.status !== "done").map(([id]) => ({ itemIdentifier: id })) };
  } }; },
});

test("registered runtime dispatch keeps S3 identity/deletion and partial failures", async () => {
  const items: WorkItem[] = [
    { type: "manifest-landed", id: "a", key: "prefix/space + ☃/manifest.json", bucket: "bucket", etag: "etag", version: "version", sequencer: "001", attempt: 2 },
    { type: "manifest-landed", id: "b", key: "prefix/erased/manifest.json", bucket: "bucket", deleted: true, sequencer: "002", attempt: 1 },
  ];
  const event = { Records: items.map(item => ({ messageId: item.id, body: "platform input" })) };
  let seen: PipelineLambdaEvent | undefined;
  const result = await dispatchRegisteredLambda(runtime(items), event, async input => { seen = input; return { batchItemFailures: [{ itemIdentifier: "b" }] }; });
  expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "b" }] });
  const first = parseQueueBody(seen!.Records![0]!.body);
  expect(first).toMatchObject({ kind: "events", events: [{ key: items[0]!.key, bucket: "bucket", etag: "etag", version: "version", sequencer: "001" }] });
  expect(parseQueueBody(seen!.Records![1]!.body)).toMatchObject({ kind: "events", events: [{ key: items[1]!.key, deleted: true, sequencer: "002" }] });
});

test("schedule continuation fields survive and failed schedules request platform retry", async () => {
  const event = { reconcile: true, cursor: "opaque", since: "2026-11-01T00:00:00Z", link: 3 };
  const adapter = runtime([scheduleItem("reconcile", 1)], true);
  let seen: PipelineLambdaEvent | undefined;
  expect(await dispatchRegisteredLambda(adapter, event, async input => { seen = input; return { ok: true }; })).toEqual({ ok: true });
  expect(seen).toMatchObject(event);
  await expect(dispatchRegisteredLambda(adapter, event, async () => ({ ok: false }))).rejects.toThrow();
  await expect(dispatchRegisteredLambda(adapter, { ...event, Records: [] }, async () => ({ ok: true }))).rejects.toThrow("ambiguous");
});

test("an incompatible binding or malformed ack cannot acknowledge input", async () => {
  const event = { Records: [{ messageId: "a", body: "input" }] };
  for (const binding of [{ handler: async () => undefined }, { handler: async () => ({ batchItemFailures: [{ itemIdentifier: "other" }] }) }, {}]) {
    await expect(dispatchRegisteredLambda({ ...runtime([]), bind: () => binding }, event, async () => ({ ok: true }))).rejects.toThrow();
  }
});

const schedules = [
  ["reconcile", "reconcile"], ["brief", "brief"], ["session-analysis", "sessionAnalysis"],
  ["analysis-cache", "analysisCache"], ["analysis-cache-reconcile", "analysisCacheReconcile"],
  ["semantic-copy", "semanticCopy"],
] as const;

const invalidSchedules: unknown[] = ["unknown", "", " ", "RECONCILE", "__proto__", null, false, 0, [], {}, ["brief"]];

test("explicit invalid schedules refuse before binding or invoking, including queue envelopes", async () => {
  for (const schedule of invalidSchedules) for (const Records of [undefined, [{ messageId: "a", body: "input" }]]) {
    let bound = 0, invoked = 0;
    const base = runtime([]);
    const adapter = { ...base, bind(work) { bound++; return base.bind(work); } } satisfies PipelineRuntimeAdapter;
    const event = { schedule, ...(Records ? { Records } : {}) } as PipelineLambdaEvent;
    await expect(dispatchRegisteredLambda(adapter, event, async () => { invoked++; return { ok: false }; })).rejects.toThrow("schedule");
    expect({ bound, invoked }).toEqual({ bound: 0, invoked: 0 });
  }
});

test("absent or undefined schedules retain the no-selector legacy fallback", async () => {
  for (const event of [{}, { schedule: undefined }]) {
    let bound = 0, invoked = 0;
    const base = runtime([]);
    const adapter = { ...base, bind(work) { bound++; return base.bind(work); } } satisfies PipelineRuntimeAdapter;
    expect(await dispatchRegisteredLambda(adapter, event, async input => {
      invoked++;
      expect(input).toBe(event);
      return { ok: false };
    })).toEqual({ ok: false });
    expect({ bound, invoked }).toEqual({ bound: 0, invoked: 1 });
  }
});

test("the public composed handler rejects invalid schedules without loading and preserves queue failures", async () => {
  const previous = process.env.PIPELINE_RUNTIME;
  process.env.PIPELINE_RUNTIME = "lambda";
  let bound = 0, loaded = 0;
  const base = runtime([]);
  const adapter = { ...base, bind(work) { bound++; return base.bind(work); } } satisfies PipelineRuntimeAdapter;
  const handler = createPipelineLambdaHandler({
    runtime: { create: () => adapter } as never,
    loadRuntime: async () => { loaded++; throw new Error("must not load a service"); },
  });
  try {
    for (const schedule of invalidSchedules) {
      await expect(handler({ schedule } as PipelineLambdaEvent)).rejects.toThrow("Lambda runtime schedule failed");
      expect(await handler({ schedule, Records: [{ messageId: "a", body: "input" }] } as PipelineLambdaEvent)).toEqual({ batchItemFailures: [{ itemIdentifier: "a" }] });
      expect({ bound, loaded }).toEqual({ bound: 0, loaded: 0 });
    }
    for (const event of [{}, { schedule: undefined }]) expect(await handler(event)).toEqual({ ok: false });
    expect({ bound, loaded }).toEqual({ bound: 0, loaded: 0 });
  } finally {
    if (previous === undefined) delete process.env.PIPELINE_RUNTIME;
    else process.env.PIPELINE_RUNTIME = previous;
  }
});

test("conflicting named schedules and legacy flags refuse before binding or invoking", async () => {
  for (const [job] of schedules) for (const [other, flag] of schedules) {
    if (job === other) continue;
    let bound = 0, invoked = 0;
    const base = runtime([scheduleItem(job, 1)], true);
    const adapter = { ...base, bind(work) { bound++; return base.bind(work); } } satisfies PipelineRuntimeAdapter;
    await expect(dispatchRegisteredLambda(adapter, { schedule: job, [flag]: true }, async () => { invoked++; return { ok: true }; })).rejects.toThrow("ambiguous");
    expect({ bound, invoked }).toEqual({ bound: 0, invoked: 0 });
  }
});

test("all schedule forms forward exactly one matching flag and continuation data", async () => {
  for (const [job, flag] of schedules) for (const form of ["named", "flag", "matching"] as const) {
    const continuation = { cursor: "opaque", since: "2026-11-01T00:00:00Z", link: 3, analysisCursor: { fixture: true } };
    const event = { ...continuation, ...(form !== "flag" ? { schedule: job } : {}), ...(form !== "named" ? { [flag]: true } : {}) };
    let invoked = 0;
    expect(await dispatchRegisteredLambda(runtime([scheduleItem(job, 1)], true), event, async input => {
      invoked++;
      expect(input).toMatchObject(continuation);
      expect(input.schedule).toBeUndefined();
      expect(schedules.filter(([, key]) => input[key] === true).map(([name]) => name)).toEqual([job]);
      return { ok: true };
    })).toEqual({ ok: true });
    expect(invoked).toBe(1);
  }
});

test("the public composed handler refuses a wrong-job envelope before runtime loading", async () => {
  const previous = process.env.PIPELINE_RUNTIME;
  process.env.PIPELINE_RUNTIME = "lambda";
  let bound = 0, loaded = 0;
  const base = runtime([scheduleItem("brief", 1)], true);
  const adapter = { ...base, bind(work) { bound++; return base.bind(work); } } satisfies PipelineRuntimeAdapter;
  const handler = createPipelineLambdaHandler({
    runtime: { create: () => adapter } as never,
    loadRuntime: async () => { loaded++; throw new Error("must not load a service"); },
  });
  try {
    await expect(handler({ schedule: "brief", reconcile: true })).rejects.toThrow("Lambda runtime schedule failed");
    expect({ bound, loaded }).toEqual({ bound: 0, loaded: 0 });
  } finally {
    if (previous === undefined) delete process.env.PIPELINE_RUNTIME;
    else process.env.PIPELINE_RUNTIME = previous;
  }
});
