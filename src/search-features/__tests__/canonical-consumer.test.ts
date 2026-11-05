import { describe, expect, test } from "bun:test";
import { CanonicalConsumer, validateConfig } from "../canonical-consumer.ts";

const A = "a".repeat(64);
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const CFG = { allowlist: [CANARY], targetId: "t", ownerId: "me",
  partition: "0", leaseTtlMs: 30000, maxRetries: 2, pageSize: 100 };
const fakeDb = () => {
  const lease: Record<string, unknown>[] = [];
  const dead: Record<string, unknown>[] = [];
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
      dead.push({ revision: params[0], reason: params[2] });
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
  return { lease, dead, query };
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
describe("canonical consumer — config + lease + drain ordering", () => {
  test("config validated before any DB access", () => {
    expect(() => validateConfig(CFG)).not.toThrow();
    expect(() => validateConfig({ ...CFG, allowlist: [] }))
      .toThrow("config_not_canary_scope");
    expect(() => validateConfig({ ...CFG, targetId: "" }))
      .toThrow("config_bad_target");
    expect(() => validateConfig({ ...CFG, leaseTtlMs: 999999999 }))
      .toThrow("config_bad_leaseTtlMs");
  });
  test("lease: one owner; same owner renews; other cannot take fresh", async () => {
    const db = fakeDb();
    const a = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    expect(await a.acquireLease()).toBe(true);
    expect(await a.acquireLease()).toBe(true);   // same owner renews
    const b = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, ownerId: "b" });
    expect(await b.acquireLease()).toBe(false);
  });
  test("dead-letter only for poison codes, never lease/infra", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    await c.deadLetter(entry("10"), "row_invalid");
    await c.deadLetter(entry("11"), "lease_lost");   // not poison
    expect(db.dead.length).toBe(1);
    expect(db.dead[0]!.reason).toBe("row_invalid");
  });
});

describe("canonical consumer — drain fail-atomic ordering", () => {
  test("out-of-order page refuses the whole drain", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10"), entry("5")]) as never, acks, CFG);
    const out = await c.drain(images as never, ch as never);
    expect(out.checkpoint).toBe("0");   // nothing advanced past the disorder
  });
  test("dead entry stops the drain — later revisions never copy", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10"), entry("20")]) as never, acks,
      { ...CFG, maxRetries: 0 });
    const out = await c.drain(images as never, ch as never);
    expect(out.copied).toBe(0);
    expect(out.checkpoint).toBe("0");
  });
});
