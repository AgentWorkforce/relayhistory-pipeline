import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { createInboxSnapshot, type SnapshotValue } from "../inbox-snapshot.ts";
import { scratchStore } from "./support.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
const prefix = "synthetic-inbox/v1";
const budget = () => ({
  maxOperations: 10000,
  maxBytes: 64 * 1024 * 1024,
  maxCacheBytes: 8 * 1024 * 1024,
  deadline: Date.now() + 30000,
});
const sha = (x: Uint8Array) => createHash("sha256").update(x).digest("hex");
test("paged snapshots survive cold reopen and preserve old committed roots", async () => {
  const f = scratchStore();
  try {
    const pairs = Array.from(
      { length: 1000 },
      (_, n) =>
        [`native/${n}`, { owner: `user-${n % 4}`, revision: n }] as [
          string,
          SnapshotValue,
        ],
    );
    const root = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      pairs,
    );
    expect(root).not.toBeNull();
    const cold = createInboxSnapshot(f.store, prefix, budget());
    for (const n of [0, 17, 127, 500, 999])
      expect(await cold.lookup(root, `native/${n}`)).toEqual({
        owner: `user-${n % 4}`,
        revision: n,
      });
    expect(await cold.lookup(root, "native/absent")).toBeUndefined();
    const next = await cold.apply(root, [
      ["native/17", { owner: "new", revision: 2000 }],
      ["native/18", undefined],
    ]);
    expect(await cold.lookup(next, "native/17")).toEqual({
      owner: "new",
      revision: 2000,
    });
    expect(await cold.lookup(next, "native/18")).toBeUndefined();
    expect(await cold.lookup(root, "native/17")).toEqual({
      owner: "user-1",
      revision: 17,
    });
    expect(await cold.lookup(root, "native/18")).toEqual({
      owner: "user-2",
      revision: 18,
    });
  } finally {
    f.done();
  }
});
test("equivalent unordered bulk input produces deterministic immutable pages", async () => {
  const f = scratchStore();
  try {
    const values = Array.from(
      { length: 100 },
      (_, n) => [String(n), { z: 1, a: n }] as [string, SnapshotValue],
    );
    const a = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      values,
    );
    const b = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      [...values].reverse(),
    );
    expect(a).toBe(b);
  } finally {
    f.done();
  }
});
test("missing or corrupted pages never become proof of key absence", async () => {
  const f = scratchStore();
  try {
    const root = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      [["a", { owner: "a" }]],
    );
    const broken = new Proxy(f.store, {
      get(t, p) {
        if (p === "getObject")
          return async () => {
            throw Error("missing page");
          };
        const v = Reflect.get(t, p);
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
    await expect(
      createInboxSnapshot(broken, prefix, budget()).lookup(root, "a"),
    ).rejects.toThrow();
    await f.store.put(
      objectKey(`${prefix}/snapshots/sha256/${root}.json`),
      new TextEncoder().encode("{}"),
    );
    await expect(
      createInboxSnapshot(f.store, prefix, budget()).lookup(root, "a"),
    ).rejects.toThrow("page_digest");
  } finally {
    f.done();
  }
});
test("a hash-valid unknown page format refuses", async () => {
  const f = scratchStore();
  try {
    const bytes = new TextEncoder().encode(
      JSON.stringify({ v: 99, kind: "leaf", entries: [] }),
    );
    const hash = sha(bytes);
    await f.store.put(
      objectKey(`${prefix}/snapshots/sha256/${hash}.json`),
      bytes,
    );
    await expect(
      createInboxSnapshot(f.store, prefix, budget()).lookup(hash, "a"),
    ).rejects.toThrow("page_version");
  } finally {
    f.done();
  }
});
test("work budgets and deadlines stop before unbounded reads", async () => {
  const f = scratchStore();
  try {
    const root = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      Array.from(
        { length: 100 },
        (_, i) => [String(i), i] as [string, SnapshotValue],
      ),
    );
    await expect(
      createInboxSnapshot(f.store, prefix, {
        ...budget(),
        maxOperations: 1,
      }).lookup(root, "1"),
    ).rejects.toThrow("budget");
    await expect(
      createInboxSnapshot(f.store, prefix, {
        ...budget(),
        deadline: Date.now() - 1,
      }).lookup(root, "1"),
    ).rejects.toThrow("deadline");
  } finally {
    f.done();
  }
});

test("invalid numeric budgets cannot disable the work bounds", () => {
  const f = scratchStore();
  try {
    for (const field of [
      "maxOperations",
      "maxBytes",
      "maxCacheBytes",
      "deadline",
    ] as const) {
      for (const n of [NaN, Infinity, -1, 0.5])
        expect(() =>
          createInboxSnapshot(f.store, prefix, { ...budget(), [field]: n }),
        ).toThrow("budget_config");
    }
  } finally {
    f.done();
  }
});
test("caller mutation cannot change a cached snapshot or its committed identity", async () => {
  const f = scratchStore();
  try {
    const value = { owner: "original", tags: ["first"] };
    const snap = createInboxSnapshot(f.store, prefix, budget());
    const pending = snap.apply(null, [["a", value]]);
    value.owner = "mutated";
    value.tags.push("later");
    const root = await pending;
    const found = (await snap.lookup(root, "a")) as typeof value;
    expect(found).toEqual({ owner: "original", tags: ["first"] });
    found.tags.push("changed return");
    expect(await snap.lookup(root, "a")).toEqual({
      owner: "original",
      tags: ["first"],
    });
    expect(
      await createInboxSnapshot(f.store, prefix, budget()).lookup(root, "a"),
    ).toEqual({ owner: "original", tags: ["first"] });
  } finally {
    f.done();
  }
});
test("an idempotent write verifies stored bytes instead of trusting metadata or a hot cache", async () => {
  const f = scratchStore();
  try {
    const snap = createInboxSnapshot(f.store, prefix, budget());
    const root = await snap.apply(null, [["a", 1]]);
    const lying = new Proxy(f.store, {
      get(t, p) {
        const v = Reflect.get(t, p);
        if (p === "put") return async () => ({ created: false });
        if (p === "getObject")
          return async (key: any) => {
            const r = await t.getObject(key);
            return {
              ...r,
              body: new ReadableStream({
                start(c) {
                  c.enqueue(new TextEncoder().encode("bad"));
                  c.close();
                },
              }),
              meta: { ...r.meta, size: 3 },
            };
          };
        return typeof v === "function" ? v.bind(t) : v;
      },
    }) as ObjectStore;
    await expect(
      createInboxSnapshot(lying, prefix, budget()).apply(null, [["a", 1]]),
    ).rejects.toThrow("page_digest");
    expect(root).not.toBeNull();
  } finally {
    f.done();
  }
});
test("transport errors are sanitized", async () => {
  const f = scratchStore();
  try {
    const broken = new Proxy(f.store, {
      get(t, p) {
        if (p === "getObject")
          return async () => {
            throw Error("sensitive remote body");
          };
        const v = Reflect.get(t, p);
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
    await expect(
      createInboxSnapshot(broken, prefix, budget()).lookup("a".repeat(64), "x"),
    ).rejects.toThrow("inbox_snapshot_storage_unavailable");
  } finally {
    f.done();
  }
});

test("stalled storage and body reads refuse at the deadline", async () => {
  const f = scratchStore();
  try {
    const root = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      [["a", 1]],
    );
    for (const method of ["getObject", "body", "put"]) {
      const blocked = new Proxy(f.store, {
        get(t, p) {
          const v = Reflect.get(t, p);
          if (p === "getObject" && method === "getObject")
            return () => new Promise(() => {});
          if (p === "put" && method === "put")
            return () => new Promise(() => {});
          if (p === "getObject" && method === "body")
            return async (k: any) => {
              const r = await t.getObject(k);
              return {
                ...r,
                body: new ReadableStream({
                  pull() {
                    return new Promise(() => {});
                  },
                  cancel() {
                    return new Promise(() => {});
                  },
                }),
              };
            };
          return typeof v === "function" ? v.bind(t) : v;
        },
      });
      const api = createInboxSnapshot(blocked, prefix, {
        ...budget(),
        deadline: Date.now() + 30,
      });
      const start = Date.now();
      await expect(
        method === "put" ? api.apply(null, [["a", 1]]) : api.lookup(root, "a"),
      ).rejects.toThrow("deadline");
      expect(Date.now() - start).toBeLessThan(1000);
    }
  } finally {
    f.done();
  }
});
test("expired or empty budgets refuse before evaluating input getters", async () => {
  const f = scratchStore();
  try {
    let invoked = 0;
    const value = {
      get secret() {
        invoked++;
        return "no";
      },
    };
    for (const b of [
      { ...budget(), deadline: Date.now() - 1 },
      { ...budget(), maxBytes: 0 },
      { ...budget(), maxOperations: 0 },
    ])
      await expect(
        createInboxSnapshot(f.store, prefix, b).apply(null, [["a", value]]),
      ).rejects.toThrow();
    expect(invoked).toBe(0);
    await expect(
      createInboxSnapshot(f.store, prefix, budget()).apply(null, [
        ["a", value],
      ]),
    ).rejects.toThrow("value_type");
    expect(invoked).toBe(0);
  } finally {
    f.done();
  }
});
test("oversized metadata and excessive change counts refuse before storage", async () => {
  const f = scratchStore();
  try {
    let puts = 0;
    const watched = new Proxy(f.store, {
      get(t, p) {
        const v = Reflect.get(t, p);
        if (p === "put")
          return (...a: any[]) => {
            puts++;
            return (t.put as any)(...a);
          };
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
    await expect(
      createInboxSnapshot(watched, prefix, budget()).apply(null, [
        ["a", "x".repeat(1000000)],
      ]),
    ).rejects.toThrow("value_bound");
    await expect(
      createInboxSnapshot(watched, prefix, budget()).apply(
        null,
        Array.from(
          { length: 2049 },
          (_, i) => [String(i), i] as [string, SnapshotValue],
        ),
      ),
    ).rejects.toThrow("input_count");
    expect(puts).toBe(0);
  } finally {
    f.done();
  }
});
test("one snapshot instance refuses overlapping operations instead of overspending shared budgets", async () => {
  const f = scratchStore();
  try {
    const root = await createInboxSnapshot(f.store, prefix, budget()).apply(
      null,
      [["a", 1]],
    );
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const blocked = new Proxy(f.store, {
      get(t, p) {
        const v = Reflect.get(t, p);
        if (p === "getObject")
          return async (k: any) => {
            await gate;
            return t.getObject(k);
          };
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
    const api = createInboxSnapshot(blocked, prefix, budget());
    const first = api.lookup(root, "a");
    try {
      await expect(api.lookup(root, "a")).rejects.toThrow("concurrent_use");
    } finally {
      release();
    }
    expect(await first).toBe(1);
  } finally {
    f.done();
  }
});

test("maximum value depth roundtrips through page wrappers and unrelated updates", async () => {
  const f = scratchStore();
  try {
    let value: SnapshotValue = null;
    for (let i = 0; i < 24; i++) value = [value];
    const api = createInboxSnapshot(f.store, prefix, budget());
    const root = await api.apply(null, [["a", value]]);
    expect(
      await createInboxSnapshot(f.store, prefix, budget()).lookup(root, "a"),
    ).toEqual(value);
    const next = await api.apply(root, [["b", 1]]);
    expect(await api.lookup(next, "a")).toEqual(value);
    await expect(api.apply(null, [["too-deep", [value]]])).rejects.toThrow(
      "value_depth",
    );
  } finally {
    f.done();
  }
});
