import { test, expect, spyOn } from "bun:test";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { Stage } from "alchemy";
import * as Layer from "effect/Layer";
import { stockCloudflareState } from "../alchemy.cloudflare.run.ts";
import {
  assertSessionIngestInputs,
  sessionIngestVars,
} from "../session-ingest.ts";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Redacted from "effect/Redacted";
import {
  Credentials,
  apiTokenCredentials,
} from "@distilled.cloud/cloudflare/Credentials";
import * as queues from "@distilled.cloud/cloudflare/queues";
import { guardOperatorFetch } from "../../../scripts/private-ingest-operator.ts";
import {
  pausedQueueService,
  liveBufferedQueueApi,
} from "../buffered-queue-provider.ts";
import { FANOUT_QUEUE } from "../session-ingest.ts";
const account = "a".repeat(32),
  qid = "b".repeat(32);
const request = {
  id: "session-ingest-fanout",
  news: { name: FANOUT_QUEUE, deliveryPaused: true },
};
function fixture(mode = "ok") {
  let paused = false,
    created = false;
  const order: string[] = [];
  const value = () => ({
    accountId: account,
    queueId: qid,
    queueName: FANOUT_QUEUE,
    deliveryPaused: paused,
  });
  const observed = () => ({
    queueId: qid,
    queueName: FANOUT_QUEUE,
    settings: { deliveryPaused: mode === "ignored" ? false : paused },
    consumersTotalCount: mode === "busy" ? 1 : 0,
    producersTotalCount: 0,
  });
  const base = {
    version: 0,
    diff: () => Effect.succeed(undefined),
    read: () => Effect.succeed(created ? value() : undefined),
    reconcile: () =>
      Effect.sync(() => {
        order.push("create");
        created = true;
        return value();
      }),
    delete: () => Effect.void,
  };
  const api = {
    list: () => Effect.sync(() => (mode === "unowned" ? [observed()] : [])),
    get: () =>
      Effect.sync(() => {
        order.push(paused ? "read-paused" : "read-unpaused");
        return mode === "foreign"
          ? { ...observed(), queueId: "f".repeat(32) }
          : observed();
      }),
    pause: () =>
      Effect.sync(() => {
        order.push("pause");
        if (mode === "error") throw Error("pause unavailable");
        paused = true;
      }),
  };
  return {
    base,
    api,
    order,
    service: pausedQueueService(base, api, account),
    setPaused: (v: boolean) => {
      created = true;
      paused = v;
    },
  };
}
test("pause+readback completes before dependent consumer can start", async () => {
  const x = fixture();
  const row = await Effect.runPromise(
    x.service.reconcile(request).pipe(
      Effect.map((v) => {
        x.order.push("consumer");
        return v;
      }),
    ),
  );
  expect(x.order).toEqual([
    "create",
    "read-unpaused",
    "pause",
    "read-paused",
    "consumer",
  ]);
  expect(row.deliveryPaused).toBe(true);
});
test.each(["ignored", "busy", "error", "foreign", "unowned"])(
  "%s cannot expose queue output or start consumer",
  async (mode) => {
    const x = fixture(mode);
    let consumer = false;
    await expect(
      Effect.runPromise(
        x.service.reconcile(request).pipe(Effect.map(() => (consumer = true))),
      ),
    ).rejects.toThrow();
    expect(consumer).toBe(false);
    if (mode === "unowned") expect(x.order).toEqual([]);
  },
);
test("unpaused drift stays visible to diff and refuses an implicit resume", async () => {
  const x = fixture();
  x.setPaused(false);
  const read = await Effect.runPromise(x.service.read({ id: request.id }));
  expect(read.deliveryPaused).toBe(false);
  expect(
    await Effect.runPromise(x.service.diff({ ...request, output: read })),
  ).toEqual({ action: "update" });
  await expect(
    Effect.runPromise(
      x.service.reconcile({
        ...request,
        news: { ...request.news, deliveryPaused: false },
      }),
    ),
  ).rejects.toThrow("captured_paused_props");
});
test("real pinned SDK serializes create→PATCH pausedtrue→GET and observes pause before output", async () => {
  const order: string[] = [],
    rows = new Map<string, any>();
  let paused = false;
  const fetcher: typeof fetch = async (input, init) => {
    const u = new URL(String(input)),
      method = init?.method ?? "GET";
    if (
      u.origin !== "https://api.cloudflare.com" ||
      !u.pathname.startsWith("/client/v4/accounts/" + account + "/queues")
    )
      throw Error("synthetic transport forbids unknown URL");
    order.push(method + " " + u.pathname);
    const result = () => ({
      queue_id: qid,
      queue_name: FANOUT_QUEUE,
      settings: { delivery_paused: paused },
      consumers_total_count: 0,
      producers_total_count: 0,
    });
    if (method === "GET" && u.pathname.endsWith("/queues"))
      return Response.json({
        success: true,
        result: [...rows.values()],
        result_info: {
          page: 1,
          per_page: 100,
          count: rows.size,
          total_count: rows.size,
        },
      });
    if (method === "POST" && u.pathname.endsWith("/queues")) {
      expect(await new Response(init?.body).json()).toEqual({
        queue_name: FANOUT_QUEUE,
      });
      rows.set(qid, result());
      return Response.json({ success: true, result: result() });
    }
    if (method === "PATCH" && u.pathname.endsWith("/" + qid)) {
      expect(await new Response(init?.body).json()).toEqual({
        settings: { delivery_paused: true },
      });
      paused = true;
      return Response.json({ success: true, result: result() });
    }
    if (method === "GET" && u.pathname.endsWith("/" + qid))
      return Response.json({ success: true, result: result() });
    throw Error("synthetic transport forbids unexpected write");
  };
  const base = {
    version: 0,
    reconcile: () =>
      queues.createQueue({ accountId: account, queueName: FANOUT_QUEUE }).pipe(
        Effect.map((v) => ({
          accountId: account,
          queueId: v.queueId,
          queueName: v.queueName,
        })),
      ),
  };
  const service = pausedQueueService(base, liveBufferedQueueApi, account);
  const row = await Effect.runPromise(
    service.reconcile(request).pipe(
      Effect.provideService(
        Credentials,
        Effect.succeed(
          apiTokenCredentials({
            apiToken: Redacted.make("synthetic-token-not-authorized"),
          }),
        ),
      ),
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetcher),
    ),
  );
  expect(row.deliveryPaused).toBe(true);
  expect(order.map((s) => s.split(" ")[0])).toEqual([
    "GET",
    "POST",
    "GET",
    "PATCH",
    "GET",
  ]);
});

test("stalled pause is bounded and never releases a consumer", async () => {
  const x = fixture();
  let consumer = false;
  const service = pausedQueueService(
    x.base,
    { ...x.api, pause: () => Effect.never },
    account,
    5,
  );
  await expect(
    Effect.runPromise(
      service.reconcile(request).pipe(Effect.map(() => (consumer = true))),
    ),
  ).rejects.toThrow();
  expect(consumer).toBe(false);
});
test("stock buffered declaration refuses before constructing convenience state helpers", async () => {
  let calls = 0;
  const watched = spyOn(Cloudflare, "state").mockImplementation(() => {
    calls++;
    return Layer.die(Error("must not build convenience state"));
  });
  try {
    const state = stockCloudflareState(() => ({
      inputs: { sessionIngest: { acceptanceMode: "buffered" } } as any,
      inputsText: "synthetic",
    }));
    await expect(
      Effect.runPromise(
        Layer.build(state).pipe(
          Effect.provideService(Stage, "cf-sessions-prod"),
          Effect.scoped,
        ) as any,
      ),
    ).rejects.toThrow("stock_buffered_refused");
    expect(calls).toBe(0);
  } finally {
    watched.mockRestore();
  }
});
test("buffered mode captures inline0/off and rejects accidental canonical settings", () => {
  const input = {
    accountId: account,
    hyperdriveId: "c".repeat(32),
    secretsStoreId: "d".repeat(32),
    kmsKeyArn:
      "arn:aws:kms:us-east-1:941433935742:key/12345678-1234-1234-1234-123456789abc",
    acceptanceMode: "buffered" as const,
    expectedGeneration: "e".repeat(64),
    inlineMaxSessions: 0,
    replicaMode: "off" as const,
  };
  const refuse = (why: string): never => {
    throw Error(why);
  };
  assertSessionIngestInputs(input, refuse);
  expect(sessionIngestVars(input).INGEST_ACCEPTANCE_MODE).toBe("buffered");
  expect(sessionIngestVars(input).INGEST_EXPECT_GENERATION).toBe("e".repeat(64));
  for (const value of [
    { ...input, expectedGeneration: undefined },
    { ...input, expectedGeneration: "e".repeat(63) },
    { ...input, expectedGeneration: "E".repeat(64) },
    { ...input, expectedGeneration: 42 },
    { ...input, inlineMaxSessions: 1 },
    { ...input, replicaMode: "queue" },
    { ...input, acceptanceMode: "canonical" },
  ])
    expect(() => assertSessionIngestInputs(value, refuse)).toThrow();
  // Bounded generation overlap: absent by default; one other explicit hex64 value renders the accepted set.
  expect(sessionIngestVars(input).INGEST_ACCEPT_GENERATIONS).toBeUndefined();
  const overlap = { ...input, overlapGeneration: "f".repeat(64) };
  assertSessionIngestInputs(overlap, refuse);
  expect(sessionIngestVars(overlap).INGEST_ACCEPT_GENERATIONS).toBe("e".repeat(64) + "," + "f".repeat(64));
  for (const value of [
    { ...input, overlapGeneration: "*" },
    { ...input, overlapGeneration: "e".repeat(64) },
    { ...input, overlapGeneration: "F".repeat(64) },
    { ...input, overlapGeneration: "f".repeat(64) + "," + "a".repeat(64) },
    { ...input, overlapGeneration: ["f".repeat(64)] },
    { ...input, acceptanceMode: "canonical", expectedGeneration: undefined, inlineMaxSessions: 1, overlapGeneration: "f".repeat(64) },
  ]) {
    expect(() => assertSessionIngestInputs(value, refuse)).toThrow();
    expect(() => sessionIngestVars(value as never)).toThrow();
  }
});

test("operator pause capability requires captured buffered mode, observed exact queue ID and true-only settings", async () => {
  const calls: string[] = [];
  const secrets = {
    accountId: account,
    apiToken: "synthetic-api-token",
    stateUrl: "https://alchemy-state-store-96bulls.fixture.workers.dev",
    stateToken: "synthetic-state-token",
  };
  const base: typeof fetch = async (input, init) => {
    const u = new URL(String(input));
    calls.push(init?.method ?? "GET");
    return Response.json({
      success: true,
      result: u.pathname.endsWith("/queues")
        ? [{ queue_id: qid, queue_name: FANOUT_QUEUE }]
        : {
            queue_id: qid,
            queue_name: FANOUT_QUEUE,
            settings: { delivery_paused: true },
          },
    });
  };
  const root =
    "https://api.cloudflare.com/client/v4/accounts/" + account + "/queues";
  const buffered = guardOperatorFetch(secrets, "deploy", base, "buffered");
  await buffered(root);
  await buffered(root + "/" + qid, {
    method: "PATCH",
    body: JSON.stringify({ settings: { delivery_paused: true } }),
  });
  expect(calls).toEqual(["GET", "PATCH"]);
  const before = calls.length;
  for (const settings of [
    { delivery_paused: false },
    { delivery_paused: true, message_retention_period: 30 },
  ])
    await expect(
      buffered(root + "/" + qid, {
        method: "PATCH",
        body: JSON.stringify({ settings }),
      }),
    ).rejects.toThrow("queue_pause_scope");
  const canonical = guardOperatorFetch(secrets, "deploy", base, "canonical");
  await canonical(root);
  await expect(
    canonical(root + "/" + qid, {
      method: "PATCH",
      body: JSON.stringify({ settings: { delivery_paused: true } }),
    }),
  ).rejects.toThrow("queue_pause_scope");
  expect(calls.length).toBe(before + 1);
});

test("buffered declaration refuses missing expected generation even before vars are rendered", () => {
  const input = {
    accountId: account, hyperdriveId: "c".repeat(32), secretsStoreId: "d".repeat(32),
    kmsKeyArn: "arn:aws:kms:us-east-1:941433935742:key/12345678-1234-1234-1234-123456789abc",
    acceptanceMode: "buffered" as const, inlineMaxSessions: 0, replicaMode: "off" as const,
  };
  expect(() => sessionIngestVars(input)).toThrow("expectedGeneration");
  expect(() => assertSessionIngestInputs(input, (why) => { throw Error(why); })).toThrow("expectedGeneration");
});
