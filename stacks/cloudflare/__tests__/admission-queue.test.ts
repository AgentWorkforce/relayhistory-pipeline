/**
 * S3-first async admission queues (declared only with sessionIngest.asyncAdmission): the buffered
 * Queue provider delegates them to the official lifecycle without ever pausing them, keeps its
 * ownership refusals, and leaves the fanout queues paused-only; the retained operator's network
 * scope grows by exactly the admission queue/DLQ/consumer calls, and only for such a capture.
 */
import { test, expect } from "bun:test";
import * as Effect from "effect/Effect";
import { guardOperatorFetch } from "../../../scripts/private-ingest-operator.ts";
import { pausedQueueService } from "../buffered-queue-provider.ts";
import { ADMISSION_DLQ, ADMISSION_QUEUE, FANOUT_DLQ, FANOUT_QUEUE, SESSION_INGEST_WORKER } from "../session-ingest.ts";

const account = "a".repeat(32), qid = "b".repeat(32);

function admissionFixture(mode = "ok") {
  const order: string[] = [];
  let created = false;
  const value = (name: string) => ({ accountId: account, queueId: qid, queueName: name });
  const base = {
    version: 0,
    diff: (i: any) => Effect.sync(() => { order.push("base-diff:" + i.id); return undefined; }),
    read: (i: any) => Effect.succeed(created ? value(i.id === "session-ingest-admission" ? ADMISSION_QUEUE : ADMISSION_DLQ) : undefined),
    reconcile: (i: any) => Effect.sync(() => { order.push("create"); created = true; return value(i.news.name); }),
    delete: () => Effect.void,
  };
  const api = {
    list: () => Effect.sync(() => (mode === "unowned" ? [{ queueId: "f".repeat(32), queueName: ADMISSION_QUEUE, consumersTotalCount: 0, producersTotalCount: 0 }] : [])),
    get: () => Effect.sync(() => { order.push("read"); return { queueId: qid, queueName: ADMISSION_QUEUE, settings: {}, consumersTotalCount: mode === "busy" ? 1 : 0, producersTotalCount: 0 }; }),
    pause: () => Effect.sync(() => { order.push("pause"); }),
  };
  return { order, service: pausedQueueService(base, api as never, account) as any };
}
const admissionRequest = { id: "session-ingest-admission", news: { name: ADMISSION_QUEUE } };

test("admission queue: official lifecycle, never paused, ownership refusals kept", async () => {
  const x = admissionFixture();
  const row = await Effect.runPromise(x.service.reconcile(admissionRequest));
  expect(row).toEqual({ accountId: account, queueId: qid, queueName: ADMISSION_QUEUE });
  expect(x.order).toEqual(["create", "read"]);
  await Effect.runPromise(x.service.diff(admissionRequest));
  expect(x.order).toContain("base-diff:session-ingest-admission");
  expect(await Effect.runPromise(x.service.read({ id: "session-ingest-admission" }))).toEqual(row);
  expect(x.order).not.toContain("pause");
  for (const mode of ["unowned", "busy"]) {
    const y = admissionFixture(mode);
    await expect(Effect.runPromise(y.service.reconcile(admissionRequest))).rejects.toThrow();
    expect(y.order).not.toContain("pause");
    if (mode === "unowned") expect(y.order).toEqual([]);
  }
});

test("admission queue: any pause prop, a wrong name or a fanout name refuses; fanout stays paused-only; delete forbidden", async () => {
  const x = admissionFixture();
  for (const news of [{ name: ADMISSION_QUEUE, deliveryPaused: true }, { name: ADMISSION_QUEUE, deliveryPaused: false }, { name: FANOUT_QUEUE }])
    await expect(Effect.runPromise(x.service.reconcile({ ...admissionRequest, news }))).rejects.toThrow("captured_admission_props");
  await expect(Effect.runPromise(x.service.reconcile({ id: "session-ingest-admission-dlq", news: { name: ADMISSION_QUEUE } }))).rejects.toThrow("captured_admission_props");
  await expect(Effect.runPromise(x.service.diff({ ...admissionRequest, news: { name: ADMISSION_QUEUE, deliveryPaused: true } }))).rejects.toThrow("captured_admission_props");
  // An unpaused fanout declaration is refused exactly as before.
  await expect(Effect.runPromise(x.service.reconcile({ id: "session-ingest-fanout", news: { name: FANOUT_QUEUE } }))).rejects.toThrow("captured_paused_props");
  await expect(Effect.runPromise(x.service.reconcile({ id: "session-ingest-fanout", news: { name: ADMISSION_QUEUE, deliveryPaused: true } }))).rejects.toThrow("captured_paused_props");
  await expect(Effect.runPromise(x.service.delete({ id: "session-ingest-admission" }))).rejects.toThrow("queue_delete_forbidden");
});

test("operator: admission calls only with a buffered asyncAdmission capture; never paused; consumer pinned to its DLQ", async () => {
  const secrets = { accountId: account, apiToken: "synthetic-api-token", stateUrl: "https://alchemy-state-store-96bulls.fixture.workers.dev", stateToken: "synthetic-state-token" };
  const aid = "c".repeat(32), fid = qid;
  const calls: string[] = [];
  const base: typeof fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push((init?.method ?? "GET") + " " + new URL(String(input)).pathname.split("/").slice(5).join("/"));
    return Response.json({ success: true, result: [{ queue_id: fid, queue_name: FANOUT_QUEUE }, { queue_id: aid, queue_name: ADMISSION_QUEUE }] });
  }) as typeof fetch;
  const root = "https://api.cloudflare.com/client/v4/accounts/" + account + "/queues";
  const post = (name: string) => ({ method: "POST", body: JSON.stringify({ queue_name: name }) });
  const consumer = (dlq: string) => ({ method: "POST", body: JSON.stringify({ script_name: SESSION_INGEST_WORKER, dead_letter_queue: dlq }) });

  // Without asyncAdmission: exactly today's scope.
  const plain = guardOperatorFetch(secrets, "deploy", base, "buffered");
  for (const name of [ADMISSION_QUEUE, ADMISSION_DLQ]) await expect(plain(root, post(name))).rejects.toThrow("queue_scope");
  await plain(root);
  await expect(plain(root + "/" + aid + "/consumers", consumer(ADMISSION_DLQ))).rejects.toThrow("unplanned_cloudflare_write");
  expect(() => guardOperatorFetch(secrets, "deploy", base, "canonical", true)).toThrow("admission_scope");

  const withAdmission = guardOperatorFetch(secrets, "deploy", base, "buffered", true);
  await withAdmission(root, post(ADMISSION_QUEUE));
  await withAdmission(root, post(ADMISSION_DLQ));
  await withAdmission(root);
  await withAdmission(root + "/" + aid + "/consumers", consumer(ADMISSION_DLQ));
  await expect(withAdmission(root + "/" + aid + "/consumers", consumer(FANOUT_DLQ))).rejects.toThrow("consumer_scope");
  await expect(withAdmission(root + "/" + fid + "/consumers", consumer(ADMISSION_DLQ))).rejects.toThrow("consumer_scope");
  await withAdmission(root + "/" + fid + "/consumers", consumer(FANOUT_DLQ));
  // An admission queue is never PATCHed (paused or not); the fanout pause stays true-only.
  for (const settings of [{ delivery_paused: true }, { delivery_paused: false }])
    await expect(withAdmission(root + "/" + aid, { method: "PATCH", body: JSON.stringify({ settings }) })).rejects.toThrow("queue_pause_scope");
  await expect(withAdmission(root + "/" + fid, { method: "PATCH", body: JSON.stringify({ settings: { delivery_paused: false } }) })).rejects.toThrow("queue_pause_scope");
  await withAdmission(root + "/" + fid, { method: "PATCH", body: JSON.stringify({ settings: { delivery_paused: true } }) });
  await expect(withAdmission(root, post("some-other-queue"))).rejects.toThrow("queue_scope");
  expect(calls.filter((c) => c.startsWith("PATCH"))).toEqual(["PATCH queues/" + fid]);
});
