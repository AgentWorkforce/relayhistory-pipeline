import { describe, expect, test } from "bun:test";
import { CanonicalConsumer, validateConfig } from "../canonical-consumer.ts";

const A = "a".repeat(64);
const CANARY = { orgId: "10000000-0000-4000-8000-000000000001",
  workspaceId: "20000000-0000-4000-8000-000000000002" };
const OTHER = { orgId: "other-org", workspaceId: "other-ws" };
const CFG = { enabled: true, scope: CANARY, partitionScheme: "legacy-prefix-v1" as const, targetId: "t", ownerId: "me", leaseTtlMs: 60000,
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
          heartbeat_at: new Date(), ttl_ms: params[2] });
        return { rows: [{ n: 1 } as T] };
      }
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_dead_letter") && text.includes("INSERT")) {
      // (entry_revision, partition, image_key, reason, attempts, dead_at)
      if (!dead.some((d) => d.revision === params[0] &&
        d.partition === params[1]))
        dead.push({ revision: params[0], partition: params[1],
          reason: params[3] });
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_checkpoint") && text.includes("SELECT")) {
      const c = ckpt.find((r) => r.partition === params[0]);
      return { rows: (c ? [{ rev: c.rev }] : []) as T[] };
    }
    if (text.includes("consumer_checkpoint") && text.includes("INSERT")) {
      // seedCheckpoint: VALUES($1,0) ON CONFLICT DO NOTHING
      if (!ckpt.some((r) => r.partition === params[0]))
        ckpt.push({ partition: params[0], rev: "0" });
      return { rows: [] as T[] };
    }
    if (text.includes("consumer_checkpoint") && text.includes("UPDATE")) {
      // advanceCheckpoint: UPDATE ... AND rev < $2 AND held fresh lease
      const c = ckpt.find((r) => r.partition === params[0]);
      const l = lease.find((r) => r.partition === params[0] &&
        r.owner === params[2]);
      if (c && Number(c.rev) < Number(params[1]) && l) {
        c.rev = params[1];
        return { rows: [{ n: 1 } as T] };
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

describe("canonical consumer — enabled gate + target/partition split", () => {
  test("disabled: zero lease/journal/CH access", async () => {
    const db = fakeDb();
    let jTouched = 0, chTouched = 0;
    const j = { entries: async () => { jTouched++;
      return { entries: [], nextRevision: "0", hasMore: false }; } };
    const c = new CanonicalConsumer({ query: db.query }, j as never, acks,
      { ...CFG, enabled: false });
    const out = await c.drain(images as never,
      { database: "d", insert: async () => { chTouched++; } } as never);
    expect(db.lease.length).toBe(0);
    expect(jTouched).toBe(0);
    expect(out.copied).toBe(0);
    expect(chTouched).toBe(0);
  });
  test("different targets get disjoint lease+checkpoint partitions", async () => {
    const db = fakeDb();
    const a = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, targetId: "target-A" });
    const b = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, targetId: "target-B" });
    expect(await a.acquireLease()).toBe(true);
    expect(await b.acquireLease()).toBe(true);   // different partition: both hold
    expect(db.lease.length).toBe(2);
    expect(db.lease[0]!.partition).not.toBe(db.lease[1]!.partition);
  });
  test("strict order: a non-increasing page holds the whole drain", async () => {
    const db = fakeDb();
    // revision 10 then a repeat — the store yields ordered pages, but a
    // non-increasing row is a violation and stops everything.
    const c = new CanonicalConsumer({ query: db.query },
      journal([entry("10", OTHER), entry("10", OTHER)]) as never, acks, CFG);
    const out = await c.drain(images as never, ch as never);
    expect(out.checkpoint).toBe("10");   // first accepted, repeat held
  });
});

describe("canonical consumer — poison map + renewal", () => {
  test("every stable producer poison code persists verbatim", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    const codes = ["reference_invalid", "tombstone_invalid",
      "payload_invalid", "vector_invalid", "payload_bound",
      "batch_invalid", "duplicate", "row_invalid", "corrupt_chain"];
    codes.forEach((code, i) =>
      void c.deadLetter(entry(String(1000 + i)), code));
    await Promise.all(codes.map((code, i) =>
      c.deadLetter(entry(String(1000 + i)), code)));
    expect(db.dead.length).toBe(9);
    // a hold/unknown code is NEVER persisted
    await c.deadLetter(entry("99"), "source_changed");
    await c.deadLetter(entry("98"), "something_random");
    expect(db.dead.length).toBe(9);
  });
  test("heartbeat renews a held lease; a stale owner cannot renew", async () => {
    const db = fakeDb();
    const a = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, CFG);
    expect(await a.acquireLease()).toBe(true);
    expect(await a.heartbeat()).toBe(true);    // held -> renew succeeds
    const b = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, ownerId: "b" });
    expect(await b.heartbeat()).toBe(false);   // not the owner -> cannot renew
  });
  test("disabled: every public method is zero-access", async () => {
    const db = fakeDb();
    const c = new CanonicalConsumer({ query: db.query },
      journal([]) as never, acks, { ...CFG, enabled: false });
    expect(await c.acquireLease()).toBe(false);
    expect(await c.heartbeat()).toBe(false);
    await c.deadLetter(entry("1"), "row_invalid");
    expect(db.lease.length + db.dead.length).toBe(0);
  });
});


describe("canonical consumer — explicit tenant and partition continuity", () => {
  test("enabled configuration refuses missing/wildcard tenants and scheme before access", () => {
    let queries = 0;
    const db = { query: async () => { queries++; return { rows: [] }; } };
    for (const cfg of [
      { ...CFG, scope: undefined },
      { ...CFG, scope: { ...CANARY, orgId: "*" } },
      { ...CFG, scope: { ...CANARY, workspaceId: "" } },
      { ...CFG, scope: { ...CANARY, workspaceId: " ws " } },
      { ...CFG, partitionScheme: undefined },
    ]) expect(() => new CanonicalConsumer(db, journal([]) as never, acks, cfg))
      .toThrow("config_bad_");
    expect(queries).toBe(0);
  });
  test("disabled without scope/scheme stays zero-access across every public operation", async () => {
    let touched = 0;
    const db = { query: async () => { touched++; return { rows: [] }; } };
    const c = new CanonicalConsumer(db, { entries: async () => {
      touched++; throw new Error("unexpected_journal");
    } } as never, acks, { ...CFG, enabled: false,
      scope: undefined, partitionScheme: undefined });
    expect(await c.acquireLease()).toBe(false);
    expect(await c.heartbeat()).toBe(false);
    await c.deadLetter(entry("1"), "row_invalid");
    expect((await c.drain(images as never, ch as never)).status).toBe("idle");
    expect(touched).toBe(0);
  });
  test("legacy scheme resumes the exact existing partition checkpoint", async () => {
    const db = fakeDb();
    const partition = "10000000-20000000-e3b98a4da31a127d4bde6e43033f66ba";
    db.ckpt.push({ partition, rev: "42" });
    const c = new CanonicalConsumer(db, journal([]) as never, acks, CFG);
    const out = await c.drain(images as never, ch as never);
    expect(out.checkpoint).toBe("42");
    expect(db.ckpt).toEqual([{ partition, rev: "42" }]);
    expect(db.lease[0]!.partition).toBe(partition);
    await c.deadLetter(entry("43"), "row_invalid");
    expect(db.dead[0]!.partition).toBe(partition);
  });
  test("full-scope scheme separates tenants even with identical UUID prefixes", async () => {
    const db = fakeDb();
    for (const scope of [CANARY,
      { ...CANARY, workspaceId: "20000000-0000-4000-8000-000000000003" },
      { ...CANARY, orgId: "10000000-0000-4000-8000-000000000003" }]) {
      const c = new CanonicalConsumer(db, journal([]) as never, acks,
        { ...CFG, partitionScheme: "scope-v1", scope });
      expect(await c.acquireLease()).toBe(true);
    }
    expect(db.lease).toHaveLength(3);
    expect(new Set(db.lease.map((row) => row.partition)).size).toBe(3);
    for (const row of db.lease)
      expect(row.partition).toMatch(/^[a-z0-9-]{4,64}$/);
  });
  test("organization and workspace each enforce a zero-image scope fence", async () => {
    for (const scope of [{ ...CANARY, orgId: "other-org" },
      { ...CANARY, workspaceId: "other-workspace" }]) {
      let imageReads = 0, inserts = 0;
      const c = new CanonicalConsumer(fakeDb(), journal([entry("1", scope)]) as never,
        acks, { ...CFG, partitionScheme: "scope-v1" });
      const out = await c.drain({ imageDescriptor: async () => {
        imageReads++; throw new Error("unexpected_image");
      } } as never, { database: "d", insert: async () => { inserts++; } });
      expect(out.skipped).toBe(1);
      expect(imageReads).toBe(0);
      expect(inserts).toBe(0);
    }
  });
  test("caller mutation cannot change a constructed consumer's tenant/target", async () => {
    const db = fakeDb();
    const cfg = { ...CFG, scope: { ...CANARY } };
    const c = new CanonicalConsumer(db, journal([]) as never, acks, cfg);
    cfg.scope.orgId = "other-org";
    cfg.targetId = "other-target";
    expect(c.cfg.scope).toEqual(CANARY);
    expect(c.cfg.targetId).toBe("t");
    await c.acquireLease();
    expect(db.lease[0]!.partition)
      .toBe("10000000-20000000-e3b98a4da31a127d4bde6e43033f66ba");
  });
});


test("full-scope partitions bind target and exclude a competing owner", async () => {
  const db = fakeDb();
  const a = new CanonicalConsumer(db, journal([]) as never, acks,
    { ...CFG, partitionScheme: "scope-v1" });
  const same = new CanonicalConsumer(db, journal([]) as never, acks,
    { ...CFG, partitionScheme: "scope-v1", ownerId: "competing-owner" });
  const other = new CanonicalConsumer(db, journal([]) as never, acks,
    { ...CFG, partitionScheme: "scope-v1", targetId: "other-target" });
  expect(await a.acquireLease()).toBe(true);
  expect(await same.acquireLease()).toBe(false);
  expect(await other.acquireLease()).toBe(true);
  expect(db.lease).toHaveLength(2);
});
