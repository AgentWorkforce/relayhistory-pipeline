/**
 * SEARCH_INDEX_PREREAD_ENABLED=true on the hosted legacy ClickHouse path: canonical source and
 * index reads finish before the apply transaction. Every race is injected after that pre-read and
 * before the transaction starts. Synthetic, secret-free fixtures only.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { exportStreamScope, generate, SCOPE } from "../../session-read-api/__tests__/stream-fixture.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

async function rig(seed = 31) {
  const source = await exportStreamScope(generate(seed), 1200);
  cleanup.push(source.dispose);
  const r = await hostedRig(source, cleanup);
  expect(await r.tick()).toEqual({ batchItemFailures: [] });
  return { source, r };
}

function between(
  base: SearchIndexDeps,
  changes: ReadonlyMap<number, () => Promise<void>>,
): { deps: SearchIndexDeps; transactions(): number } {
  let transactions = 0;
  return {
    deps: {
      ...base,
      preRead: true,
      transact: async (work) => {
        transactions += 1;
        await changes.get(transactions)?.();
        return base.transact(work);
      },
    },
    transactions: () => transactions,
  };
}

async function bumpEpoch(r: Awaited<ReturnType<typeof hostedRig>>): Promise<void> {
  await r.db.pg.exec(`UPDATE sessions.session_directory
    SET event_projection_epoch=event_projection_epoch+1
    WHERE org_id='org_stream' AND workspace_id='ws_stream' AND harness='claude'`);
}

test("skip-current stays first: a current scope pays no source read or index call", async () => {
  const { r } = await rig();
  const reads = r.reads.count, calls = r.fake.calls.length;
  const result = await indexSearchScope(SCOPE, r.search({ preRead: true, skipCurrent: true }));
  expect(result).toMatchObject({ complete: true, skipped: true, reasons: ["already_current"] });
  expect(r.reads.count).toBe(reads);
  expect(r.fake.calls).toHaveLength(calls);
});

test("skip-current off: a busy scope returns before any source or ClickHouse read", async () => {
  const { r } = await rig();
  const reads = r.reads.count, calls = r.fake.calls.length;
  const held = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
    r.transact((tx) => work({
      query: (sql: string, params?: unknown[]) =>
        sql.includes("pg_try_advisory_xact_lock") ? Promise.resolve({ rows: [{ ok: false }] }) as never : tx.query(sql, params),
    }));
  expect(await indexSearchScope(SCOPE, r.search({ preRead: true, skipCurrent: false, transact: held })))
    .toMatchObject({ busy: true, reasons: ["scope_busy"] });
  expect(r.reads.count).toBe(reads);
  expect(r.fake.calls).toHaveLength(calls);
});

test("a missing ClickHouse binding takes the base validation path", async () => {
  const { r } = await rig();
  const reads = r.reads.count, calls = r.fake.calls.length;
  await expect(indexSearchScope(SCOPE, { ...r.search({ preRead: true }), ch: undefined }))
    .rejects.toThrow("search requires exactly one service binding");
  expect(r.reads.count).toBe(reads);
  expect(r.fake.calls).toHaveLength(calls);
});

test("a directory epoch bump after source and index reads retries, then applies", async () => {
  const { r } = await rig();
  const reads = r.reads.count, calls = r.fake.calls.length;
  const raced = between(r.search(), new Map([[2, async () => {
    expect(r.reads.count).toBeGreaterThan(reads);
    const preLock = r.fake.calls.slice(calls);
    expect(preLock.some((call) => call.ch === "select")).toBe(true);
    expect(preLock.some((call) => call.ch === "insert" || call.ch === "command")).toBe(false);
    await bumpEpoch(r);
  }]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true });
  expect(raced.transactions()).toBe(4);
});

test("a pointer move after the pre-read changes the stamp and the retry reads the moved key", async () => {
  const { source, r } = await rig(33);
  const oldKey = source.keys[0]!;
  const movedKey = source.keys[1]!;
  const raced = between(r.search(), new Map([[2, async () => {
    await r.db.pg.query("DELETE FROM sessions.session_index WHERE manifest_key=$1", [movedKey]);
    await r.db.pg.query("UPDATE sessions.session_index SET manifest_key=$1 WHERE manifest_key=$2", [movedKey, oldKey]);
  }]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true });
  expect(raced.transactions()).toBe(4);
  expect((await r.db.pg.query<{ manifest_key: string }>("SELECT manifest_key FROM sessions.session_index WHERE manifest_key=$1", [movedKey])).rows).toHaveLength(1);
});

test("an erase after the pre-read retries to the untouched locked erase branch", async () => {
  const { r } = await rig();
  const raced = between(r.search(), new Map([[2, async () => {
    await r.db.pg.exec("UPDATE sessions.session_index SET deleted_at=now() WHERE org_id='org_stream' AND workspace_id='ws_stream' AND harness='claude'");
  }]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true, reasons: ["erased"] });
  expect(raced.transactions()).toBe(4);
  expect(r.fake.dump()).toEqual([]);
  expect(r.fake.calls.some((call) => call.ch === "command" && String(call.query).includes("DELETE FROM"))).toBe(true);
});

test("an incomplete S3 pre-read changed before the lock retries and never deletes existing rows", async () => {
  const { source, r } = await rig(33);
  const before = r.fake.dump();
  const base = r.search();
  const missing = source.keys[0]!;
  const reader = { ...base.reader, read: (key: string) => key === missing ? Promise.resolve(null) : base.reader.read(key) };
  const raced = between({ ...base, reader }, new Map([[2, () => bumpEpoch(r)]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: ["manifest_missing"] });
  // An incomplete capture is never applied from outside the locks: capture, then the locked base path.
  expect(raced.transactions()).toBe(2);
  expect(r.fake.dump()).toEqual(before);
});

async function projectionReady(r: Awaited<ReturnType<typeof hostedRig>>): Promise<boolean[]> {
  const { rows } = await r.db.pg.query<{ ready: boolean }>(`SELECT event_projection_ready AS ready FROM sessions.session_directory
    WHERE org_id='org_stream' AND workspace_id='ws_stream' AND harness='claude'`);
  return rows.map((x) => x.ready);
}

for (const stream of [false, true]) {
  const label = stream ? "streaming on" : "streaming off";

  test(`${label}: a transiently incomplete pre-read never overrides readiness a newer pass committed meanwhile`, async () => {
    // Pipeline review of 3b9cf638: A sees manifest_missing outside the locks; before A's apply, B reads
    // the same pointers successfully and commits event_projection_ready=true; the stamp is unchanged.
    const { source, r } = await rig(33);
    const base = r.search(stream ? { stream: UNLIMITED } : {});
    const flaky = source.keys[0]!;
    let failOnce = true;
    const reader = { ...base.reader, read: (key: string) => {
      if (key === flaky && failOnce) { failOnce = false; return Promise.resolve(null); }
      return base.reader.read(key);
    } };
    let newer: Awaited<ReturnType<typeof indexSearchScope>> | undefined;
    const a = between({ ...base, reader }, new Map([[2, async () => {
      expect(failOnce).toBe(false); // A's pre-read already hit the transient miss
      newer = await indexSearchScope(SCOPE, r.search(stream ? { stream: UNLIMITED } : {}));
    }]]));
    const result = await indexSearchScope(SCOPE, a.deps);
    expect(newer).toMatchObject({ complete: true });
    expect(result).toMatchObject({ complete: true });
    expect((await projectionReady(r)).every(Boolean)).toBe(true);
  });

  test(`${label}: a transiently truncated pre-read scan never overrides readiness a newer pass committed meanwhile`, async () => {
    // Pipeline review of 1f1e3a39: A captures an index scan with truncated=true; before A's apply, B
    // finishes a complete pass at the same pointer stamp; A must not invalidate B's readiness.
    const { r } = await rig(33);
    const base = r.search(stream ? { stream: UNLIMITED } : {});
    let padOnce = true;
    const ch = { ...base.ch!, select: async <T,>(query: string, options?: { params?: Record<string, unknown> }) => {
      const rows = await base.ch!.select<T>(query, options);
      const limit = Number(options?.params?.limit);
      if (padOnce && Number.isFinite(limit) && rows.length > 0 && rows.length < limit) {
        padOnce = false;
        while (rows.length < limit) rows.push(rows[0]!);
      }
      return rows;
    } };
    let newer: Awaited<ReturnType<typeof indexSearchScope>> | undefined;
    const a = between({ ...base, ch: ch as never }, new Map([[2, async () => {
      expect(padOnce).toBe(false); // A's pre-read scan was truncated
      newer = await indexSearchScope(SCOPE, r.search(stream ? { stream: UNLIMITED } : {}));
    }]]));
    const result = await indexSearchScope(SCOPE, a.deps);
    expect(newer).toMatchObject({ complete: true });
    expect(result).toMatchObject({ complete: true });
    expect((await projectionReady(r)).every(Boolean)).toBe(true);
  });

  test(`${label}: a persistently truncated index scan still marks the scope incomplete under the locks`, async () => {
    const { r } = await rig(33);
    const before = r.fake.dump();
    const a = between(r.search({ ...(stream ? { stream: UNLIMITED } : {}), limits: { maxRows: 2 } }), new Map());
    const result = await indexSearchScope(SCOPE, a.deps);
    expect(result).toMatchObject({ complete: false, deleted: 0 });
    expect(result.reasons).toContain("clickhouse_truncated");
    expect(a.transactions()).toBe(2);
    expect((await projectionReady(r)).some((x) => x === false)).toBe(true);
    expect(r.fake.dump()).toEqual(before);
  });

  test(`${label}: a persistently missing manifest still marks the scope incomplete under the locks`, async () => {
    const { source, r } = await rig(33);
    const base = r.search(stream ? { stream: UNLIMITED } : {});
    const missing = source.keys[0]!;
    const reader = { ...base.reader, read: (key: string) => key === missing ? Promise.resolve(null) : base.reader.read(key) };
    const before = r.fake.dump();
    const a = between({ ...base, reader }, new Map());
    expect(await indexSearchScope(SCOPE, a.deps)).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: ["manifest_missing"] });
    expect(a.transactions()).toBe(2);
    expect((await projectionReady(r)).some((x) => x === false)).toBe(true);
    expect(r.fake.dump()).toEqual(before);
  });
}

test("two same-stamp stale scans apply idempotently: no resurrected row and no lost current row", async () => {
  const { r } = await rig(33);
  const wanted = r.fake.dump();
  const stale = r.fake.rows[0]!;
  stale.ts = new Date(Date.parse(`${stale.ts.replace(" ", "T")}Z`) - 1000).toISOString().replace("T", " ").replace("Z", "");
  let arrived = 0;
  let release!: () => void;
  const both = new Promise<void>((resolve) => { release = resolve; });
  const waitAtApply = async () => { arrived += 1; if (arrived === 2) release(); await both; };
  const one = between(r.search(), new Map([[2, waitAtApply]]));
  const two = between(r.search(), new Map([[2, waitAtApply]]));
  const [a, b] = await Promise.all([indexSearchScope(SCOPE, one.deps), indexSearchScope(SCOPE, two.deps)]);
  expect(a).toMatchObject({ complete: true, inserted: 1, deleted: 1 });
  expect(b).toMatchObject({ complete: true, inserted: 1, deleted: 1 });
  expect(r.fake.dump()).toEqual(wanted);
});

// SEARCH_INDEX_STREAMING_CH=1 (the hosted default): the pre-read no longer bypasses streaming.
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };

test("streaming on: a whole-read scope reads source and index before the apply transaction, never inside it", async () => {
  const { r } = await rig(33);
  r.fake.rows.splice(0, 3);
  const wanted = (await rig(33)).r.fake.dump();
  const reads = r.reads.count, calls = r.fake.calls.length;
  let readsAtApply = -1, callsAtApply = -1;
  const raced = between(r.search({ stream: UNLIMITED }), new Map([[2, async () => {
    readsAtApply = r.reads.count;
    callsAtApply = r.fake.calls.length;
    const preLock = r.fake.calls.slice(calls);
    expect(preLock.some((call) => call.ch === "select")).toBe(true);
    expect(preLock.some((call) => call.ch === "insert" || call.ch === "command")).toBe(false);
  }]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true, inserted: 3, deleted: 0 });
  expect(raced.transactions()).toBe(2);
  expect(readsAtApply).toBeGreaterThan(reads);
  // Inside the apply transaction: no object read and no index select, only the writes.
  expect(r.reads.count).toBe(readsAtApply);
  expect(r.fake.calls.slice(callsAtApply).some((call) => call.ch === "select")).toBe(false);
  expect(r.fake.dump()).toEqual(wanted);
});

test("streaming on: a directory epoch bump after the pre-read retries, then applies the newer source", async () => {
  const { r } = await rig();
  const raced = between(r.search({ stream: UNLIMITED }), new Map([[2, () => bumpEpoch(r)]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true });
  expect(raced.transactions()).toBe(4);
});

// The too-large streamed parity (rows, directory, catalog, state, page walk) is in
// search-index-stream-ch.test.ts ("streaming on, pre-read on").

test("streaming on: a scope_too_large scope whose stamp changes after the pre-read retries and never applies the stale read", async () => {
  const { r } = await rig(33);
  const wanted = r.fake.dump();
  r.fake.rows.splice(0, 2);
  const raced = between(r.search({ limits: TINY, stream: UNLIMITED }), new Map([[2, () => bumpEpoch(r)]]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true, inserted: 2 });
  expect(raced.transactions()).toBe(4);
  expect(r.fake.dump()).toEqual(wanted);
});

test("streaming on: two concurrent same-stamp passes apply idempotently: no resurrected row and no lost current row", async () => {
  const { r } = await rig(33);
  const wanted = r.fake.dump();
  const stale = r.fake.rows[0]!;
  stale.ts = new Date(Date.parse(`${stale.ts.replace(" ", "T")}Z`) - 1000).toISOString().replace("T", " ").replace("Z", "");
  let arrived = 0;
  let release!: () => void;
  const both = new Promise<void>((resolve) => { release = resolve; });
  const waitAtApply = async () => { arrived += 1; if (arrived === 2) release(); await both; };
  const one = between(r.search({ stream: UNLIMITED }), new Map([[2, waitAtApply]]));
  const two = between(r.search({ stream: UNLIMITED }), new Map([[2, waitAtApply]]));
  await Promise.all([indexSearchScope(SCOPE, one.deps), indexSearchScope(SCOPE, two.deps)]);
  expect(r.fake.dump()).toEqual(wanted);
});

test("streaming on: an older pass racing a newer pass that commits first never overwrites the newer result", async () => {
  const { r } = await rig(33);
  const wanted = r.fake.dump();
  // The older pass A pre-reads first; the rows its read would restore are gone.
  r.fake.rows.splice(0, 3);
  let newer: Awaited<ReturnType<typeof indexSearchScope>> | undefined;
  const older = between(r.search({ stream: UNLIMITED }), new Map([[2, async () => {
    // Before A's apply transaction: a newer revision commits (epoch bump) and a whole
    // newer pass B runs to completion and commits its result.
    await bumpEpoch(r);
    newer = await indexSearchScope(SCOPE, r.search({ stream: UNLIMITED }));
  }]]));
  const a = await indexSearchScope(SCOPE, older.deps);
  expect(newer).toMatchObject({ complete: true, inserted: 3 });
  // A's apply saw the changed stamp, refused, re-read and found nothing left to change.
  expect(older.transactions()).toBe(4);
  expect(a).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
  expect(r.fake.dump()).toEqual(wanted);
});

test("two stamp changes exhaust the retry and fall back to the unmodified locked read", async () => {
  const { r } = await rig();
  let readsAfterSecondPreRead = 0;
  const raced = between(r.search(), new Map([
    [2, () => bumpEpoch(r)],
    [4, async () => { readsAfterSecondPreRead = r.reads.count; await bumpEpoch(r); }],
  ]));
  expect(await indexSearchScope(SCOPE, raced.deps)).toMatchObject({ complete: true });
  expect(raced.transactions()).toBe(5);
  expect(r.reads.count).toBeGreaterThan(readsAfterSecondPreRead);
});
