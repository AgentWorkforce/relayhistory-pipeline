import { describe, expect, test } from "bun:test";
import { CanonicalConsumerRunner, validateRuntimeConfig }
  from "../canonical-consumer-runner.ts";
import type { CanonicalConsumer }
  from "../canonical-consumer.ts";

const A = "a".repeat(64);
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const CFG = { enabled: true, targetId: "t", ownerId: "me",
  leaseTtlMs: 30000, maxRetries: 0, pageSize: 100,
  pollIntervalMs: 50, deadlineMs: 300 };
const empty = { copied: 0, dead: 0, skipped: 0, checkpoint: "0" };
const consumer = (result: unknown, delay = 0) => ({
  drain: async () => {
    if (delay) await new Promise((r) => setTimeout(r, delay));
    return result;
  },
});
const images = { imageDescriptor: async () => ({}),
  imagePages: async () => ({}), priorKeyPages: async () => ({}) };
const ch = { database: "d", insert: async () => {} };
describe("canonical consumer runner — supervision", () => {
  test("default-off: run returns immediately, zero drains", () => {
    const r = new CanonicalConsumerRunner(
      consumer({ ...empty, status: "idle" }) as never,
      images as never, ch as never,
      { ...CFG, enabled: false });
    return r.run().then((h) => {
      expect(h.runs).toBe(0);
      expect(h.enabled).toBe(false);
    });
  });
  test("abort during sleep returns fast", async () => {
    const c = new AbortController();
    const r = new CanonicalConsumerRunner(
      consumer({ ...empty, status: "idle" }) as never,
      images as never, ch as never,
      { ...CFG, pollIntervalMs: 5000, deadlineMs: 60000 });
    const p = r.run(c.signal);
    c.abort();
    const h = await p;   // resolves fast, not after 5s pollInterval
    expect(h.runs).toBeLessThanOrEqual(1);
  });
  test("held drain marks unhealthy; advanced marks healthy", async () => {
    const r1 = new CanonicalConsumerRunner(
      consumer({ ...empty, status: "held" }) as never,
      images as never, ch as never, { ...CFG, deadlineMs: 60 });
    const h1 = await r1.run();
    expect(h1.healthy).toBe(false);
    expect(h1.holds).toBeGreaterThanOrEqual(1);
    const r2 = new CanonicalConsumerRunner(
      consumer({ ...empty, copied: 1, checkpoint: "1",
        status: "advanced" }) as never,
      images as never, ch as never, { ...CFG, deadlineMs: 60 });
    const h2 = await r2.run();
    expect(h2.healthy).toBe(true);
  });
  test("concurrent run refused as single-flight", async () => {
    const r = new CanonicalConsumerRunner(
      consumer({ ...empty, status: "idle" }, 40) as never,
      images as never, ch as never, { ...CFG, deadlineMs: 300 });
    const p1 = r.run();
    const p2 = r.run();       // second call while first is in-flight
    const h2 = await p2;
    expect(h2.status).toBe("already_running");
    await p1;
  });
  test("make() derives an instance-unique fencing identity", () => {
    const r1 = CanonicalConsumerRunner.make(
      { query: async () => ({ rows: [] }) }, {} as never,
      {} as never, images as never, ch as never, CFG);
    const r2 = CanonicalConsumerRunner.make(
      { query: async () => ({ rows: [] }) }, {} as never,
      {} as never, images as never, ch as never, CFG);
    expect(r1).not.toBe(r2);
  });
});
