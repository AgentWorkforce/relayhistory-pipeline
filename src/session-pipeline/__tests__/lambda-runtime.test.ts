import { expect, test } from "bun:test";
import { dispatchRegisteredLambda, type PipelineLambdaEvent } from "../lambda-runtime.ts";
import { parseQueueBody } from "../s3-events.ts";
import { settle, scheduleItem, type PipelineRuntimeAdapter, type WorkItem } from "@rhc/runtime/contracts.ts";

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
