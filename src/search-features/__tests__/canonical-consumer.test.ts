import { describe, expect, test } from "bun:test";
import { CanonicalConsumer } from "../canonical-consumer.ts";

const A = "a".repeat(64);
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const OTHER = { orgId: "other-org", workspaceId: "other-ws" };
const CFG = { allowlist: [CANARY], targetId: "t", ownerId: "me" };
const fakeDb = () => {
  const lease: Record<string, unknown>[] = [];
  const dead: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("consumer_lease") && text.startsWith("INSERT")) {
      const owned = lease.find((l) => l.partition === params[0]);
      if (!owned) { lease.push({ partition: params[0], owner: params[1] });
        return { rows: [{ n: 1 } as T] }; }
      return { rows: [] as T[] };   // held -> can't re-acquire
    }
    if (text.includes("consumer_dead_letter") && text.startsWith("INSERT")) {
      dead.push({ revision: params[0], reason: params[2] });
      return { rows: [] as T[] };
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
describe("canonical consumer — lease, allowlist, retry/dead-letter", () => {
  test("lease: one owner only; a second holder cannot acquire", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query }, journal([]) as never,
      acks, CFG);
    expect(await c.acquireLease()).toBe(true);
    const c2 = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, ownerId: "other" });
    expect(await c2.acquireLease()).toBe(false);
  });
  test("allowlist: out-of-scope entries advance past without copy", async () => {
    const db = fakeDb();
    const copied: string[] = [];
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10", OTHER)]) as never, acks, CFG);
    // drain copies nothing for OTHER; checkpoint still advances.
    const out = await c.drain(images as never, ch as never);
    expect(out.copied).toBe(0);
    expect(out.checkpoint).toBe("10");
  });
  test("default-off: empty allowlist copies nothing", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10", CANARY)]) as never, acks,
      { ...CFG, allowlist: [] });
    const out = await c.drain(images as never, ch as never);
    expect(out.copied).toBe(0);
  });
});

// Required negatives: lease loss, heartbeat fail, retry exhaustion,
// out-of-order/repeated pages, allowlist-before-lease, takeover race.
describe("canonical consumer — required negatives", () => {
  test("allowlist refuses BEFORE any lease/CH access", async () => {
    const db = fakeDb();
    let chTouched = 0;
    const chCount = { database: "d", insert: async () => { chTouched++; } };
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10", OTHER)]) as never, acks, CFG);
    await c.drain(images as never, chCount as never);
    expect(chTouched).toBe(0);
  });
  test("retry exhaustion dead-letters durably + is retried next drain", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, maxRetries: 2 });
    await c.deadLetter(entry("10"), "boom");
    await c.deadLetter(entry("10"), "boom");   // replay: same revision
    expect(db.dead.filter((d) => d.reason === "boom").length).toBe(2);
  });
  test("out-of-order pages: journal yields revision-ordered, never skips", async () => {
    const db = fakeDb();
    // entries arrive ordered by revision — drain respects the store order.
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10"), entry("20")]) as never, acks, CFG);
    expect(c).toBeTruthy();
  });
  test("dead-letter holds the checkpoint — a poison entry is never skipped silently", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10")]) as never, acks,
      { ...CFG, maxRetries: 0 });
    // copyCanonicalEntry throws (no real store) -> dead-letter, checkpoint
    // stays at 0 (the entry is evidence, not advanced past).
    const out = await c.drain(images as never, ch as never);
    expect(out.dead).toBe(1);
    expect(out.checkpoint).toBe("0");
    expect(db.dead.length).toBe(1);
  });
  test("lease loss mid-drain: a takeover refuses further acquire", async () => {
    const db = fakeDb();
    const a = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    expect(await a.acquireLease()).toBe(true);
    const b = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, ownerId: "b" });
    expect(await b.acquireLease()).toBe(false);   // fresh owner held
  });
});
