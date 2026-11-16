/**
 * Issue 610 L3 D1 (C1): the streamed reconcile on the hosted ClickHouse binding
 * (SEARCH_INDEX_STREAMING_CH=1). An in-memory transcript_events table with FINAL and
 * keyset semantics stands in for ClickHouse; the directory writes go through the
 * writeEvents/writeCatalog seams. Synthetic, secret-free data only.
 */
import { afterEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { clickhouseScopePage, indexSearchScope, legacyProjectionIo, type SearchIndexDeps } from "../search-index.ts";
import { STREAM_DEADLINE, STREAM_READ_EVENTS_BUDGET, type StreamDeadline } from "../../session-read-api/canonical-stream-projection.ts";
import { SCOPE, decompress, exportStreamScope, generate, prefixFor, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0)) await c();
});

type Selected = { keyset: boolean; after: boolean; limit: number; returned: number };

/** transcript_events in memory: FINAL, the whole-scope scan, keyset pages, inserts and deletes. */
function fakeClickHouse() {
  const rows: IndexRow[] = [];
  const tsMs = (r: IndexRow) => Date.parse(`${r.ts.replace(" ", "T")}Z`);
  const identity = (r: IndexRow) => [r.org_id, r.workspace_id, tsMs(r), r.session_id, r.source, r.machine_id, r.kind, r.event_id].join("|");
  const final = () => {
    const latest = new Map<string, IndexRow>();
    for (const r of rows) {
      const held = latest.get(identity(r));
      if (!held || r.version >= held.version) latest.set(identity(r), r);
    }
    return [...latest.values()];
  };
  const key = (r: IndexRow): [number, string, string, string, string] => [tsMs(r), r.session_id, r.machine_id, r.kind, r.event_id];
  const cmp = (a: readonly (number | string)[], b: readonly (number | string)[]) => {
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return a[i]! < b[i]! ? -1 : 1;
    return 0;
  };
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const selects: Selected[] = [];
  const state: { session_id: string; needs_full: number; complete?: number }[] = [];
  const other: string[] = [];
  const hooks = { onSelect: (_s: Selected) => {}, onInsert: (_n: number) => {} };
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      if (!query.startsWith("DELETE FROM")) throw new Error(`unexpected command ${query.slice(0, 30)}`);
      if (query.includes("{org:String}")) {
        const p = options!.params!;
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && (r.session_id === p.session || r.session_id === p.canonical)) rows.splice(i, 1);
        }
        return;
      }
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) =>
        [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!),
      );
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples)
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms))
            rows.splice(i, 1);
        }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_state") || query.includes("FROM system.columns")) return [] as T[];
      if (!query.includes(`FROM ${EVENTS_TABLE} FINAL`)) throw new Error(`unexpected select ${query.slice(0, 40)}`);
      const p = options!.params!;
      const keyset = query.includes("ORDER BY ts, session_id, machine_id, kind, event_id");
      const after = query.includes("{after_ts:Int64}");
      // The later page's redundant sort-key bound, applied as ClickHouse would (it never changes the rows).
      const tsBound = query.includes("AND ts >= fromUnixTimestamp64Milli({after_ts:Int64}, 'UTC')");
      let found = final().filter((r) => r.org_id === p.org && (r.session_id === p.session || r.session_id === p.canonical) && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from));
      if (keyset) found.sort((a, b) => cmp(key(a), key(b)));
      if (tsBound) found = found.filter((r) => tsMs(r) >= Number(p.after_ts));
      if (after) {
        const last = [Number(p.after_ts), String(p.after_session), String(p.after_machine), String(p.after_kind), String(p.after_event)];
        found = found.filter((r) => cmp(key(r), last) > 0);
      }
      found = found.slice(0, Number(p.limit));
      const s = { keyset, after, limit: Number(p.limit), returned: found.length };
      selects.push(s);
      hooks.onSelect(s);
      return found.map((r) => ({ workspace_id: r.workspace_id, session_id: r.session_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      if (table === SESSION_STATE_TABLE) {
        state.push(...(batch as typeof state));
        return;
      }
      if (table !== EVENTS_TABLE) {
        other.push(table);
        return;
      }
      rows.push(...(batch as IndexRow[]));
      hooks.onInsert(batch.length);
    },
  };
  const dump = () => final().map((r) => JSON.stringify([r.session_id, r.machine_id, r.kind, r.event_id, tsMs(r), r.fingerprint])).sort();
  return { ch, rows, final, dump, selects, state, other, hooks };
}

/** The Supabase transaction as the indexer sees it: live pointers of the scope and a call log. */
function fakeSupabase(x: ExportedScope) {
  const calls: string[] = [];
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      calls.push(text.trim().replace(/\s+/g, " "));
      if (text.includes("pg_catalog.pg_attribute") && text.includes("recorded_catalog_title")) return { rows: [{ supported: false }] as R[] };
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: x.keys.map((manifest_key) => ({ manifest_key, canonical_id: x.canonicalId })) as R[] };
      if (text.includes("AS canonical_id FROM sessions.session_index")) return { rows: [{ canonical_id: x.canonicalId }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, calls, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

function backend(x: ExportedScope, extra: Partial<SearchIndexDeps>) {
  const fake = fakeClickHouse();
  const sb = fakeSupabase(x);
  const directory: unknown[] = [];
  const catalog: unknown[] = [];
  const deps: SearchIndexDeps = {
    transact: sb.transact, reader: x.reader, store: x.store, ch: fake.ch, prefixFor, decompress, now: () => NOW,
    writeEvents: async (_tx, _key, input) => { directory.push(input); return { written: true } as never; },
    writeCatalog: (async (_tx: unknown, _key: unknown, value: unknown) => { catalog.push(value); }) as never,
    ...extra,
  };
  return { ...fake, sb, deps, directory, catalog };
}

async function scope(lines: Line[]) {
  const x = await exportStreamScope(lines, 1200);
  cleanup.push(x.dispose);
  return x;
}

/** A clock that reads "before the deadline" until `cross()` is called. */
function switchClock(): { deadline: StreamDeadline; cross(): void; reads(): number } {
  let late = false, reads = 0;
  return { deadline: { at: 1, now: () => { reads++; return late ? 1 : 0; } }, cross: () => { late = true; }, reads: () => reads };
}

test("ClickHouse binding, streaming on: a too-large scope gets the whole read's rows, in keyset pages of one insert batch", async () => {
  const x = await scope(generate(31));
  const whole = backend(x, {});
  const streamed = backend(x, { limits: { ...TINY, insertBatch: 3 }, stream: UNLIMITED });
  const w = await indexSearchScope(SCOPE, whole.deps);
  const s = await indexSearchScope(SCOPE, streamed.deps);
  expect(w).toMatchObject({ complete: true, reasons: [] });
  expect(w.rows).toBeGreaterThan(10);
  expect(s).toMatchObject({ complete: true, reasons: [], rows: w.rows, inserted: w.inserted, deleted: 0 });
  expect(s.stream).toMatchObject({ passes: 4 });
  expect(streamed.dump()).toEqual(whole.dump());
  expect(streamed.directory).toEqual(whole.directory);
  expect(streamed.catalog).toEqual(whole.catalog);
  expect(streamed.state.map((r) => r.needs_full)).toEqual(whole.state.map((r) => r.needs_full));
  // The streamed pass never ran the single LIMIT maxRows+1 scan; the whole read never paged.
  expect(streamed.selects.every((q) => q.keyset && q.limit <= 3)).toBe(true);
  expect(whole.selects.some((q) => q.keyset)).toBe(false);
  // Up to date: the scan walks every row in pages of 3 and nothing is regenerated.
  streamed.selects.length = 0;
  const again = await indexSearchScope(SCOPE, streamed.deps);
  expect(again).toMatchObject({ complete: true, inserted: 0, deleted: 0, rows: w.rows });
  expect(again.stream).toMatchObject({ passes: 3 });
  expect(streamed.selects.reduce((n, q) => n + q.returned, 0)).toBe(streamed.final().length);
  expect(streamed.selects.length).toBe(Math.floor(streamed.final().length / 3) + 1);
  expect(streamed.selects.filter((q) => !q.after).length).toBe(1);
}, 60_000);

test("ClickHouse binding, streaming on: a retracted row is deleted and a missing row is regenerated", async () => {
  const x = await scope(generate(32));
  const b = backend(x, { limits: { ...TINY, insertBatch: 4 }, stream: UNLIMITED });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  const before = b.dump();
  const [first, second] = b.final();
  b.rows.push({ ...first!, event_id: `${first!.event_id}-retracted`, fingerprint: "stale", version: NOW.getTime() - 1 });
  b.rows.splice(b.rows.indexOf(second!), 1);
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: true, deleted: 1, inserted: 1 });
  expect(b.dump()).toEqual(before);
}, 60_000);

test("keyset scan: every row exactly once across page boundaries, ties on ts included, and the row cap is exact", async () => {
  const fake = fakeClickHouse();
  const base = { org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, source: SCOPE.harness, fingerprint: "f", version: 1 } as unknown as IndexRow;
  const ts = (ms: number) => new Date(ms).toISOString().replace("T", " ").replace("Z", "");
  // Ten rows sharing three timestamps, differing only in later key columns; one out of window; one other scope.
  for (let i = 0; i < 10; i++)
    fake.rows.push({ ...base, ts: ts(1_000 + (i % 3)), session_id: i % 2 ? "canonical" : SCOPE.sessionId, machine_id: `m${i % 2}`, kind: i % 4 ? "message" : "tool", event_id: `e${i}` } as IndexRow);
  fake.rows.push({ ...base, ts: ts(10), session_id: SCOPE.sessionId, machine_id: "m0", kind: "message", event_id: "old" } as IndexRow);
  fake.rows.push({ ...base, ts: ts(1_000), workspace_id: "other", session_id: SCOPE.sessionId, machine_id: "m0", kind: "message", event_id: "x" } as IndexRow);
  const io = legacyProjectionIo(fake.ch);
  const ids = (rows: { eventId: string }[]) => rows.map((r) => r.eventId).sort();
  const all = Array.from({ length: 10 }, (_, i) => `e${i}`).sort();
  for (const pageSize of [1, 2, 3, 4, 9, 10, 11, 5_000]) {
    let between = 0;
    const got = await io.scanPages(SCOPE, "canonical", 1_000, pageSize, 100, async () => { between++; });
    expect({ pageSize, truncated: got.truncated, ids: ids(got.rows) }).toEqual({ pageSize, truncated: false, ids: all });
    expect(between).toBe(Math.floor(10 / pageSize)); // one call before each page after the first
  }
  // At the cap: complete. One past it: truncated, and it stops reading.
  expect((await io.scanPages(SCOPE, "canonical", 1_000, 3, 10, async () => {})).truncated).toBe(false);
  fake.selects.length = 0;
  const over = await io.scanPages(SCOPE, "canonical", 1_000, 3, 9, async () => {});
  expect(over.truncated).toBe(true);
  expect(over.rows.length).toBe(10);
  expect(fake.selects.map((q) => q.limit)).toEqual([3, 3, 3, 1]);
  await expect(io.scanPages(SCOPE, "canonical", 0, 0, 5, async () => {})).rejects.toThrow("bounds invalid");
});

test("keyset page SQL is exact: the first page, and a later page whose ts bound lets the sort key prune", async () => {
  const seen: { sql: string; params: Record<string, unknown> }[] = [];
  const ch = {
    database: "test",
    async select(query: string, options: { params: Record<string, unknown> }) {
      seen.push({ sql: query.replace(/\s+/g, " ").trim(), params: options.params });
      return [];
    },
  } as unknown as ClickHouseClient;
  await clickhouseScopePage(ch, SCOPE, "canonical", 1_000, 3);
  await clickhouseScopePage(ch, SCOPE, "canonical", 1_000, 3, { tsMs: 1_002, sessionId: "s_last", machineId: "m_last", kind: "k_last", eventId: "e_last" });
  const head =
    "SELECT workspace_id, session_id, machine_id, source, kind, event_id, toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint" +
    ` FROM ${EVENTS_TABLE} FINAL` +
    " WHERE org_id = {org:String} AND (session_id = {session:String} OR session_id = {canonical:String})" +
    " AND workspace_id = {workspace:String} AND source = {source:String}" +
    " AND toUnixTimestamp64Milli(ts) >= {from:Int64}";
  const tail = " ORDER BY ts, session_id, machine_id, kind, event_id LIMIT {limit:UInt32}";
  expect(seen.map((q) => q.sql)).toEqual([
    head + tail,
    head +
      " AND ts >= fromUnixTimestamp64Milli({after_ts:Int64}, 'UTC')" +
      " AND (ts, session_id, machine_id, kind, event_id) > (fromUnixTimestamp64Milli({after_ts:Int64}, 'UTC'), {after_session:String}, {after_machine:String}, {after_kind:String}, {after_event:String})" +
      tail,
  ]);
  const scope = { org: SCOPE.orgId, session: SCOPE.sessionId, canonical: "canonical", workspace: SCOPE.workspaceId, source: SCOPE.harness, from: 1_000, limit: 3 };
  expect(seen.map((q) => q.params)).toEqual([
    scope,
    { ...scope, after_ts: 1_002, after_session: "s_last", after_machine: "m_last", after_kind: "k_last", after_event: "e_last" },
  ]);
});

test("ClickHouse binding, streaming on: index rows past the events budget leave the scope partial and delete nothing", async () => {
  const x = await scope(generate(33));
  const b = backend(x, { limits: { ...TINY, insertBatch: 5_000 }, stream: { maxReadBytes: 1 << 30, maxReadEvents: 20_000 } });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  const [first] = b.final();
  // Stale rows the next pass would delete, more of them than the events budget.
  for (let i = 0; i <= 20_000; i++) b.rows.push({ ...first!, event_id: `stale-${i}`, fingerprint: "stale" });
  const held = b.final().length;
  b.state.length = 0;
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_READ_EVENTS_BUDGET] });
  expect(b.final().length).toBe(held);
  expect(b.state.length).toBeGreaterThan(0);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

test("deadline between chunks of the read passes: partial with stream_deadline, nothing written, no throw", async () => {
  const x = await scope(generate(39));
  const clock = switchClock();
  const b = backend(x, { limits: TINY, stream: UNLIMITED, streamDeadline: clock.deadline });
  // Crosses after the first chunk check.
  const now = clock.deadline.now!;
  clock.deadline.now = () => { const v = now(); if (clock.reads() === 1) clock.cross(); return v; };
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_DEADLINE] });
  expect(s.stream!.chunks).toBe(1);
  expect(b.final()).toEqual([]);
  expect(b.selects).toEqual([]);
  expect(b.directory).toEqual([]);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

/** A scope indexed once, then one row lost (pass 4 has work) and one stale row added (a delete is due). */
async function damaged(seed: number, insertBatch: number, deadline: StreamDeadline) {
  const x = await scope(generate(seed));
  const b = backend(x, { limits: { ...TINY, insertBatch }, stream: UNLIMITED });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  const good = b.dump();
  const all = b.final();
  const stale = { ...all[0]!, event_id: `${all[0]!.event_id}-retracted`, fingerprint: "stale" };
  b.rows.push(stale);
  const lost = all.slice(1, 4);
  for (const r of lost) b.rows.splice(b.rows.indexOf(r), 1);
  b.state.length = 0;
  b.selects.length = 0;
  b.directory.length = 0;
  return { b, good, stale, lost, deps: { ...b.deps, streamDeadline: deadline } };
}

test("deadline between index scan pages: partial with stream_deadline before any write", async () => {
  const clock = switchClock();
  const { b, stale, deps } = await damaged(35, 2, clock.deadline);
  b.hooks.onSelect = () => { if (b.selects.length === 2) clock.cross(); };
  const s = await indexSearchScope(SCOPE, deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_DEADLINE] });
  expect(b.selects.length).toBe(2); // the third page is never read
  expect(b.final()).toContainEqual(stale);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

test("deadline before pass 4: the scan completes, nothing is regenerated or deleted", async () => {
  const clock = switchClock();
  const { b, stale, deps } = await damaged(42, 5_000, clock.deadline);
  b.hooks.onSelect = (q) => { if (q.returned < q.limit) clock.cross(); }; // the last page
  const s = await indexSearchScope(SCOPE, deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_DEADLINE] });
  expect(s.stream).toMatchObject({ passes: 3 });
  expect(b.final()).toContainEqual(stale);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

test("deadline during pass 4: the rows already written stay, nothing is deleted", async () => {
  const clock = switchClock();
  const { b, stale, lost, deps } = await damaged(37, 1, clock.deadline);
  b.hooks.onInsert = () => clock.cross(); // after the first regenerated batch
  const s = await indexSearchScope(SCOPE, deps);
  expect(s).toMatchObject({ complete: false, partial: true, deleted: 0, reasons: ["scope_too_large", STREAM_DEADLINE] });
  expect(s.inserted).toBeGreaterThanOrEqual(1);
  expect(s.inserted).toBeLessThan(lost.length);
  expect(b.final()).toContainEqual(stale);
  expect(b.final().length).toBe(b.dump().length);
  expect(lost.filter((r) => b.dump().some((d) => d.includes(JSON.stringify(r.event_id)))).length).toBe(s.inserted);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

test("deadline after pass 4, before the deletes: every regenerated row stays, the stale row is not deleted", async () => {
  const clock = switchClock();
  const { b, stale, lost, deps } = await damaged(45, 5_000, clock.deadline);
  b.hooks.onInsert = () => clock.cross(); // the one and only batch
  const s = await indexSearchScope(SCOPE, deps);
  expect(s).toMatchObject({ complete: false, partial: true, deleted: 0, inserted: lost.length, reasons: ["scope_too_large", STREAM_DEADLINE] });
  expect(b.final()).toContainEqual(stale);
  expect(b.directory).toEqual([]);
  expect(b.state.every((r) => r.needs_full === 1)).toBe(true);
}, 60_000);

test("a deadline that never passes changes nothing", async () => {
  const x = await scope(generate(44));
  const plain = backend(x, { limits: TINY, stream: UNLIMITED });
  const timed = backend(x, { limits: TINY, stream: UNLIMITED, streamDeadline: { at: 2, now: () => 1 } });
  const a = await indexSearchScope(SCOPE, plain.deps);
  const c = await indexSearchScope(SCOPE, timed.deps);
  expect(c).toEqual(a);
  expect(timed.dump()).toEqual(plain.dump());
}, 60_000);

test("gate matrix on the ClickHouse binding: streams only with stream set and no output facts; flags unset never streams", async () => {
  const x = await scope(generate(40));
  // Hosted with the flags unset: no stream deps, so a too-large scope is partial exactly as before.
  const off = backend(x, { limits: TINY });
  const r = await indexSearchScope(SCOPE, off.deps);
  expect(r).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large"] });
  expect(r.stream).toBeUndefined();
  expect(off.selects.some((q) => q.keyset)).toBe(false);
  expect(off.final()).toEqual([]);
  // Output facts on: never streamed, even with stream set.
  const facts = backend(x, { limits: TINY, stream: UNLIMITED, outputFacts: true });
  const f = await indexSearchScope(SCOPE, facts.deps);
  expect(f).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large"] });
  expect(f.stream).toBeUndefined();
  expect(facts.selects.some((q) => q.keyset)).toBe(false);
  // Stream set, facts off: streamed.
  const on = backend(x, { limits: TINY, stream: UNLIMITED });
  expect((await indexSearchScope(SCOPE, on.deps)).stream).toMatchObject({ passes: 4 });
}, 60_000);
