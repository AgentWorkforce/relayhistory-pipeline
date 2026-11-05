import { describe, expect, test } from "bun:test";
import { CanonicalConsumer, validateConfig } from "../canonical-consumer.ts";

const A = "a".repeat(64);
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const OTHER = { orgId: "other-org", workspaceId: "other-ws" };
const CFG = { targetId: "t", ownerId: "me", leaseTtlMs: 30000,
  maxRetries: 2, pageSize: 100 };
const fakeDb = () => {
  const lease: Record<string, unknown>[] = [];
  const dead: Record<string, unknown>[] = [];
  const ckpt: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("consumer_lease") && text.includes("INSERT")) {
      const owned = lease.find((l) => l.partition === params[0]);
      if (!owned || owned.owner === params[1]) {
        if (!owned) lease.push({ partition: params[0], owner: params[1],
          heartbeat_at: new Date() });
        return { rows: [{ n: 1 } as T] };
      }
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_dead_letter") && text.includes("INSERT")) {
      if (!dead.some((d) => d.revision === params[0]))
        dead.push({ revision: params[0], reason: params[2] });
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_checkpoint") && text.includes("SELECT")) {
      const c = ckpt.find((r) => r.partition === params[0]);
      return { rows: (c ? [{ rev: c.rev }] : []) as T[] };
    }
    if (text.includes("consumer_checkpoint") && text.includes("INSERT")) {
      const c = ckpt.find((r) => r.partition === params[0]);
      if (!c || Number(c.rev) < Number(params[1])) {
        if (!c) ckpt.push({ partition: params[0], rev: params[1] });
        else c.rev = params[1];
      }
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_lease") && text.includes("SELECT")) {
      const l = lease.find((r) => r.partition === params[0] &&
        r.owner === params[1]);
      return { rows: (l ? [{ n: 1 }] : []) as T[] };
    }
    if (text.includes("consumer_lease") && text.includes("UPDATE")) {
      const l = lease.find((r) => r.partition === params[0] &&
        r.owner === params[1]);
      return { rows: (l ? [{ n: 1 }] : []) as T[] };
    }
    return { rows: [] as T[] };
  };
  return { lease, dead, ckpt, query };
};
const entry = (rev: string, scope = CANARY) => ({
  kind: "publish" as const, revision: rev, imageKey: "i",
  schemaGeneration: A, sourceHash: A,
  scope: { ...scope, harness: "h", sessionId: "s", generation: A },
  sourceAuthority: { authorityId: "u", epoch: "1", fingerprint: A },
  pointerSet: { authorityId: "u", hash: A, count: "1" },
});
const journal = (entries: unknown[]) => ({
  entries: async () => ({ entries: entries as never[], nextRevision: "0",
    hasMore: false }),
});
const images = { imageDescriptor: async () => ({}),
  imagePages: async () => ({}), priorKeyPages: async () => ({}) };
const ch = { database: "d", insert: async () => {} };
const acks = {} as never;
describe("canonical consumer — scope fence + config + fail-atomic", () => {
  test("config validated before DB access", () => {
    expect(() => validateConfig(CFG)).not.toThrow();
    expect(() => validateConfig({ ...CFG, targetId: "" }))
      .toThrow("config_bad_target");
  });
  test("out-of-scope entries scope-skip with ZERO image/CH access", async () => {
    const db = fakeDb();
    let chTouched = 0;
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10", OTHER)]) as never, acks, CFG);
    const out = await c.drain({ ...images,
      imagePages: async () => { chTouched++; return {}; } } as never,
      ch as never);
    expect(chTouched).toBe(0);
    expect(out.skipped).toBe(1);
    expect(out.checkpoint).toBe("10");   // skipped advances past it
  });
  test("dead letter only for poison, immutable on repeat", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    await c.deadLetter(entry("10"), "row_invalid");
    await c.deadLetter(entry("10"), "row_invalid");   // repeat: immutable
    await c.deadLetter(entry("11"), "lease_lost");     // not poison
    expect(db.dead.length).toBe(1);
  });
  test("fail-atomic: a stopped entry leaves checkpoint at its position", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10"), entry("20")]) as never, acks,
      { ...CFG, maxRetries: 0 });
    const out = await c.drain(images as never, ch as never);
    expect(out.copied).toBe(0);
    expect(out.checkpoint).toBe("0");
  });
});
