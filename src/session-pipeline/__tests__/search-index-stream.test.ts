/**
 * Issue 610 phase 2: SEARCH_INDEX_STREAMING_ENABLED reconciles a scope past the
 * whole-read bound through the streaming reader. The registered Postgres search
 * adapter runs on an in-process PGlite; the directory writes go through the
 * writeEvents/writeCatalog seams. Synthetic, secret-free data only.
 */
import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { positionOf } from "@rhc/search-index/contracts.ts";
import type { ManifestReader } from "@rhc/lib/pipeline/adapter.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import type { SearchProjectionState } from "../search-adapter.ts";
import {
  STREAM_READ_CEILING, assertStagedSearchIndex, indexSearchScope, searchIndexStreamBudgetWarning, searchIndexStreamFlag, searchIndexStreamFloor, searchIndexStreamFromEnv,
  searchIndexStreamStartup, type SearchIndexDeps,
} from "../search-index.ts";
import { STREAM_DEADLINE, STREAM_KEYED_RECORDS_CAP, STREAM_READ_BYTES_BUDGET, STREAM_READ_EVENTS_BUDGET, STREAM_SOURCE_CHANGED } from "../../session-read-api/canonical-stream-projection.ts";
import {
  SCOPE, T0, activity, decompress, exportStreamScope, generate, prefixFor, type ExportedScope, type Line,
} from "../../session-read-api/__tests__/stream-fixture.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const tenantOf = () => ({ orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId });
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0)) await c();
});

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

const pgliteTenants = (db: PGlite): TenantSqlRunner => (orgId, workspaceId, work) =>
  db.transaction(async (tx) => {
    await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
    return work(tx);
  });

/** One search backend and the directory writes it was handed. */
async function backend(x: ExportedScope, extra: Partial<SearchIndexDeps>) {
  const db = new PGlite({ extensions: { pg_trgm } });
  cleanup.push(() => db.close());
  await db.exec("CREATE EXTENSION pg_trgm");
  const recorded: Array<{ complete: boolean; rows: number }> = [];
  const state: SearchProjectionState = {
    async fromMs() { return 0; },
    async record(_tx, _scope, value) { recorded.push({ complete: value.complete, rows: (value as { rows?: number }).rows ?? -1 }); },
    async partials() { return { sessions: [], overflow: false }; },
  };
  const adapter = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: db }, "writer", pgliteTenants(db));
  await adapter.index.ensureSchema();
  const sb = fakeSupabase(x);
  const directory: unknown[] = [];
  const catalog: unknown[] = [];
  const deps: SearchIndexDeps = {
    transact: sb.transact, reader: x.reader, store: x.store, adapter, prefixFor, decompress, now: () => NOW,
    writeEvents: async (_tx, _key, input) => { directory.push(input); return { written: true } as never; },
    writeCatalog: (async (_tx: unknown, _key: unknown, value: unknown) => { catalog.push(value); }) as never,
    ...extra,
  };
  const tenant = { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId };
  const scan = async () =>
    (await adapter.index.scanFingerprints({ ...tenant, source: SCOPE.harness, sessionIds: [SCOPE.sessionId, x.canonicalId] }, { fromMs: 0, limit: 100_000 })).rows
      .map((r) => JSON.stringify([r.sessionId, r.machineId, r.kind, r.eventId, r.tsMs, r.fingerprint]))
      .sort();
  const docs = async () => (await adapter.index.query({ tenant, order: "asc", limit: 1000 })).hits;
  return { db, adapter, sb, deps, recorded, directory, catalog, scan, docs };
}

async function scope(lines: Line[]) {
  const x = await exportStreamScope(lines, 1200);
  cleanup.push(x.dispose);
  return x;
}

test("streaming on: a too-large scope gets the whole read's rows, directory events and catalog", async () => {
  const x = await scope(generate(31));
  const whole = await backend(x, {});
  const streamed = await backend(x, { limits: TINY, stream: UNLIMITED });
  const w = await indexSearchScope(SCOPE, whole.deps);
  const s = await indexSearchScope(SCOPE, streamed.deps);
  expect(w).toMatchObject({ complete: true, reasons: [] });
  expect(w.rows).toBeGreaterThan(10);
  expect(s).toMatchObject({ complete: true, reasons: [], rows: w.rows, inserted: w.inserted, deleted: 0 });
  expect(s.stream).toMatchObject({ passes: 4 });
  expect(await streamed.scan()).toEqual(await whole.scan());
  expect(streamed.directory).toEqual(whole.directory);
  expect(streamed.catalog).toEqual(whole.catalog);
  expect(streamed.recorded).toEqual(whole.recorded);
  // Up to date: nothing to regenerate, so no fourth pass.
  const again = await indexSearchScope(SCOPE, streamed.deps);
  expect(again).toMatchObject({ complete: true, inserted: 0, deleted: 0, rows: w.rows });
  expect(again.stream).toMatchObject({ passes: 3 });
}, 60_000);

test("streaming on: MESSAGE_ORIGIN_LABELS_ENABLED labels a streamed row as the whole read does", async () => {
  const m = "machine:0";
  const at = (i: number) => T0 + i * 1000;
  const x = await scope([
    ...generate(33),
    activity(m, `capacity:${m}`, "session_event", "label-human", at(1), { role: "user", text: "typed by a person", message_id: "lh", origin_kind: "human" }),
    activity(m, `capacity:${m}`, "session_event", "label-agent", at(3), { role: "assistant", text: "an answer", message_id: "la", subagent_session_id: "sub-1" }),
  ]);
  const whole = await backend(x, { originLabels: true });
  const streamed = await backend(x, { limits: TINY, stream: UNLIMITED, originLabels: true });
  const unlabelled = await backend(x, { limits: TINY, stream: UNLIMITED });
  expect(await indexSearchScope(SCOPE, whole.deps)).toMatchObject({ complete: true, reasons: [] });
  expect(await indexSearchScope(SCOPE, streamed.deps)).toMatchObject({ complete: true, reasons: [] });
  expect(await indexSearchScope(SCOPE, unlabelled.deps)).toMatchObject({ complete: true, reasons: [] });
  const labels = async (b: Awaited<ReturnType<typeof backend>>) =>
    Object.fromEntries(
      (await b.docs()).flatMap((h) => {
        const id = /:(label-[a-z]+)$/.exec(h.eventId)?.[1];
        return id ? [[id, [h.actorName, h.subagentId]]] : [];
      }),
    );
  expect(await labels(whole)).toEqual({ "label-human": ["human", ""], "label-agent": ["agent", "sub-1"] });
  expect(await labels(streamed)).toEqual(await labels(whole));
  expect(await streamed.scan()).toEqual(await whole.scan());
  // Labels are part of the fingerprint: without them the streamed rows differ.
  expect(await unlabelled.scan()).not.toEqual(await whole.scan());
}, 60_000);

test("streaming on: a retracted row is deleted and a missing row is regenerated", async () => {
  const x = await scope(generate(32));
  const b = await backend(x, { limits: TINY, stream: UNLIMITED });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  const before = await b.scan();
  const [first, second] = await b.docs();
  // A stale row of a regenerable kind that the source no longer holds, and one row lost.
  await b.adapter.index.upsert([{ ...first!, eventId: `${first!.eventId}-retracted`, fingerprint: "stale", version: NOW.getTime() - 1, taskRefId: "", deliveryRecordId: "" }]);
  expect(await b.adapter.index.deleteEvents(tenantOf(), [positionOf(second!)])).toEqual({ deleted: 1 });
  expect((await b.scan()).length).toBe(before.length);
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: true, deleted: 1 });
  expect(await b.scan()).toEqual(before);
}, 60_000);

test("streaming on: a budget the scope cannot fit leaves it partial with the budget code, writing nothing", async () => {
  const x = await scope(generate(33));
  const b = await backend(x, { limits: TINY, stream: { maxReadBytes: 1024, maxReadEvents: 1 << 24 } });
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_READ_BYTES_BUDGET] });
  expect(s.stream).toMatchObject({ passes: 0, chunks: 0 });
  expect(await b.scan()).toEqual([]);
  expect(b.directory).toEqual([]);
  expect(b.recorded).toEqual([expect.objectContaining({ complete: false })]);
  expect(b.sb.calls.some((c) => c.includes("event_projection_ready"))).toBe(true); // directory invalidated
}, 60_000);

test("streaming off: a too-large scope stays partial exactly as before", async () => {
  const x = await scope(generate(34));
  const b = await backend(x, { limits: TINY });
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toEqual({ manifests: x.keys.length, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large"] });
  expect(await b.scan()).toEqual([]);
}, 60_000);

/** The reader without chunk raw digests, so a changed chunk body reaches the record digests. */
function withoutRawDigests(reader: ManifestReader): ManifestReader {
  return {
    ...reader,
    async read(key: string) {
      const got = await reader.read(key);
      if (!got) return got;
      const raw = { ...got.manifest.raw, chunks: got.manifest.raw.chunks.map(({ raw_sha256: _d, ...c }) => c) };
      return { ...got, manifest: { ...got.manifest, raw } } as typeof got;
    },
  } as ManifestReader;
}

test("streaming on: a source that changes during regeneration keeps the verified rows it wrote, deletes nothing and owes a pass", async () => {
  const lines = [...generate(35), activity("machine:0", "capacity:marker", "session_event", "marked", T0 + 3000, { role: "user", text: "MARKER-A" }, "1", 999)];
  const x = await scope(lines);
  let calls = 0;
  let after = Number.POSITIVE_INFINITY;
  const changing = (stored: Uint8Array, max: number) => {
    const raw = decompress(stored, max);
    calls += 1;
    if (calls <= after) return raw;
    const text = new TextDecoder().decode(raw);
    return text.includes("MARKER-A") ? new TextEncoder().encode(text.replace("MARKER-A", "MARKER-B")) : raw;
  };
  // One row per write: the rows pass 4 regenerates before it reaches the change are written.
  const b = await backend(x, { limits: { ...TINY, insertBatch: 1 }, stream: UNLIMITED, decompress: changing, reader: withoutRawDigests(x.reader) });
  // The whole-read sizing reads no chunk; passes 1 to 3 read every chunk once, pass 4 sees the change.
  const chunks = await Promise.all(x.keys.map(async (k) => (await x.reader.read(k))!.manifest.raw.chunks.length));
  after = 3 * chunks.reduce((a, n) => a + n, 0);
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(calls).toBeGreaterThan(after); // pass 4 ran
  expect(s).toMatchObject({ complete: false, deleted: 0, reasons: [STREAM_SOURCE_CHANGED] });
  expect(s.inserted).toBeGreaterThan(0);
  expect((await b.scan()).length).toBe(s.inserted); // only verified rows, each one pass 3 wanted
  expect(b.directory).toEqual([]);
  expect(b.recorded).toEqual([expect.objectContaining({ complete: false })]);
  expect(b.sb.calls.some((c) => c.includes("event_projection_ready"))).toBe(true); // directory invalidated
  // A rerun over a settled source repairs the scope: it matches a whole read.
  after = Number.POSITIVE_INFINITY;
  calls = Number.NEGATIVE_INFINITY;
  const again = await indexSearchScope(SCOPE, b.deps);
  expect(again).toMatchObject({ complete: true, reasons: [] });
  const whole = await backend(x, {});
  expect(await indexSearchScope(SCOPE, whole.deps)).toMatchObject({ complete: true });
  expect(await b.scan()).toEqual(await whole.scan());
}, 60_000);

test("streaming on: the stream keeps its transaction busy between the reads of every pass", async () => {
  const x = await scope(generate(31));
  const pings = () => b.sb.calls.filter((c) => c === "SELECT 1 AS stream_keepalive").length;
  const seen: number[] = [];
  const counting = (stored: Uint8Array, max: number) => {
    seen.push(pings());
    return decompress(stored, max);
  };
  const b = await backend(x, { limits: TINY, stream: UNLIMITED, decompress: counting, streamKeepaliveMs: 0 });
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: true, stream: { passes: 4 } });
  const chunks = (await Promise.all(x.keys.map(async (k) => (await x.reader.read(k))!.manifest.raw.chunks.length))).reduce((a, n) => a + n, 0);
  expect(seen.length).toBe(4 * chunks);
  // Every chunk read after the first follows a ping: no pass, and no gap between passes, is quiet.
  for (let i = 1; i < seen.length; i++) expect({ i, more: seen[i]! > seen[i - 1]! }).toEqual({ i, more: true });
  // Throttled: with a long interval only the first record pings, plus the two forced around the index scan.
  const quiet = await backend(x, { limits: TINY, stream: UNLIMITED, streamKeepaliveMs: 1e12 });
  expect(await indexSearchScope(SCOPE, quiet.deps)).toMatchObject({ complete: true });
  expect(quiet.sb.calls.filter((c) => c === "SELECT 1 AS stream_keepalive").length).toBe(3);
}, 60_000);

const PING = "SELECT 1 AS stream_keepalive";
/** Logs the backend's scans and deletes into the transaction's call log, to see what runs between pings. */
function logBackend(b: Awaited<ReturnType<typeof backend>>) {
  const index = b.adapter.index;
  const scan = index.scanFingerprints.bind(index);
  const remove = index.deleteEvents.bind(index);
  index.scanFingerprints = (async (...args: Parameters<typeof scan>) => { b.sb.calls.push("SCAN"); return scan(...args); }) as typeof scan;
  index.deleteEvents = (async (...args: Parameters<typeof remove>) => { b.sb.calls.push("DELETE"); return remove(...args); }) as typeof remove;
}

test("streaming on: the transaction is pinged around the index scan and between delete batches", async () => {
  const x = await scope(generate(32));
  const b = await backend(x, { limits: { ...TINY, insertBatch: 1 }, stream: UNLIMITED, streamKeepaliveMs: 1e12 });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  const before = await b.scan();
  const [first] = await b.docs();
  const stale = [1, 2, 3].map((n) => ({ ...first!, eventId: `${first!.eventId}-retracted-${n}`, fingerprint: "stale", version: NOW.getTime() - 1, taskRefId: "", deliveryRecordId: "" }));
  await b.adapter.index.upsert(stale);
  logBackend(b);
  b.sb.calls.length = 0;
  // A long interval: the pings around the scan are forced, the throttled ones between deletes are not due.
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true, deleted: 3 });
  const log = b.sb.calls.filter((c) => c === PING || c === "SCAN" || c === "DELETE");
  expect(log).toEqual([PING, PING, "SCAN", PING, "DELETE", "DELETE", "DELETE"]);
  expect(await b.scan()).toEqual(before);
  // Due every time: each delete batch follows a ping.
  await b.adapter.index.upsert(stale);
  b.sb.calls.length = 0;
  expect(await indexSearchScope(SCOPE, { ...b.deps, streamKeepaliveMs: 0 })).toMatchObject({ complete: true, deleted: 3 });
  const tail = b.sb.calls.filter((c) => c === PING || c === "SCAN" || c === "DELETE");
  expect(tail.slice(tail.indexOf("SCAN") - 1)).toEqual([PING, "SCAN", PING, PING, "DELETE", PING, "DELETE", PING, "DELETE"]);
  expect(await b.scan()).toEqual(before);
}, 60_000);

test("streaming on: a store error while regenerating surfaces as itself, not as a read code", async () => {
  const x = await scope(generate(31));
  const b = await backend(x, { limits: { ...TINY, insertBatch: 1 }, stream: UNLIMITED });
  const down = new Error("index store unavailable");
  b.adapter.index.upsert = async () => { throw down; };
  await expect(indexSearchScope(SCOPE, b.deps)).rejects.toBe(down);
  expect(b.recorded).toEqual([]);
}, 60_000);

test("streaming on: a failed keepalive ends the pass with its error, not a read code", async () => {
  const x = await scope(generate(37));
  const b = await backend(x, { limits: TINY, stream: UNLIMITED });
  const lost = new Error("terminating connection due to idle-in-transaction timeout");
  const deps: SearchIndexDeps = {
    ...b.deps,
    transact: (work) => b.sb.transact((tx) => work({
      query: (text: string, params?: unknown[]) => (text.includes("stream_keepalive") ? Promise.reject(lost) : tx.query(text, params)),
    } as SqlExecutor)),
  };
  await expect(indexSearchScope(SCOPE, deps)).rejects.toBe(lost);
  expect(await b.scan()).toEqual([]);
  expect(b.recorded).toEqual([]);
}, 60_000);

test("SEARCH_INDEX_STREAMING_ENABLED is on by default, off with output facts, and the read budgets are validated", () => {
  const MiB = 1024 * 1024;
  const limits = { maxScopeBytes: 64 * MiB, maxScopeEvents: 100_000 };
  const CEILING = { maxReadBytes: 2048 * MiB, maxReadEvents: 2_000_000 };
  expect(STREAM_READ_CEILING).toEqual(CEILING);
  // Unset or "": the code default, on, at the ceiling budgets.
  expect(searchIndexStreamFlag({}, false)).toBe("default_on");
  expect(searchIndexStreamFromEnv({}, limits, false)).toEqual(CEILING);
  expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "" }, limits, false)).toEqual(CEILING);
  // Unset or "" with output facts: off, no failure, and the budgets are not read.
  for (const env of [{}, { SEARCH_INDEX_STREAMING_ENABLED: "" }]) {
    expect(searchIndexStreamFlag(env, true)).toBe("default_off_output_facts");
    expect(searchIndexStreamFromEnv({ ...env, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "nonsense" }, limits, true)).toBeUndefined();
  }
  // Explicit "false": off, with or without output facts, and the budgets are not read.
  for (const facts of [false, true]) {
    expect(searchIndexStreamFlag({ SEARCH_INDEX_STREAMING_ENABLED: "false" }, facts)).toBe("explicit_off");
    expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "false", SEARCH_INDEX_STREAM_MAX_READ_BYTES: "nonsense" }, limits, facts)).toBeUndefined();
  }
  // Strict grammar: anything but "true" / "false" / "" fails, "1" and "0" included.
  for (const flag of ["1", "0", "TRUE", "False", "yes", " true", "false "])
    expect(() => searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: flag }, limits, false)).toThrow("SEARCH_INDEX_STREAMING_ENABLED must be true or false");
  // Explicit "true" with output facts still fails startup, exactly as before the default flip.
  expect(() => searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "true" }, limits, true)).toThrow(
    "pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true",
  );
  const on = { SEARCH_INDEX_STREAMING_ENABLED: "true" };
  expect(searchIndexStreamFlag(on, false)).toBe("explicit_on");
  expect(searchIndexStreamFromEnv(on, limits, false)).toEqual(CEILING);
  // The floor (four whole reads, capped at the ceiling) is accepted; the default stays the ceiling.
  expect(searchIndexStreamFloor(limits)).toEqual({ maxReadBytes: 256 * MiB, maxReadEvents: 400_000 });
  expect(searchIndexStreamFloor({ maxScopeBytes: 1024 * MiB, maxScopeEvents: 1_000_000 })).toEqual(CEILING);
  expect(searchIndexStreamFromEnv({ ...on, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "256MiB", SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "400000" }, limits, false)).toEqual({ maxReadBytes: 256 * MiB, maxReadEvents: 400_000 });
  expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAM_MAX_READ_BYTES: "512MiB" }, limits, false)).toEqual({ maxReadBytes: 512 * MiB, maxReadEvents: 2_000_000 });
  expect(searchIndexStreamFromEnv({ ...on, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "2GiB", SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "2000000" }, limits, false)).toEqual(CEILING);
  for (const [name, value] of [
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "255MiB"], // below the floor
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "3GiB"], // above the cap
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "1.5GiB"],
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "-1"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "399999"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "2000001"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "1MiB"],
  ] as const) {
    for (const env of [on, {}]) {
      let message = "";
      try {
        searchIndexStreamFromEnv({ ...env, [name]: value }, limits, false);
      } catch (error) {
        message = (error as Error).message;
      }
      expect(message).toStartWith(`pipeline consumer: ${name} must be`);
      expect(message).not.toContain(value); // never echoes the value
    }
  }
});

test("startup: unset with output facts logs why streaming is off; budgets at their floors warn; the defaults do not", () => {
  const MiB = 1024 * 1024;
  const limits = { maxScopeBytes: 64 * MiB, maxScopeEvents: 100_000 };
  const start = (env: Record<string, string>, facts = false, l: Partial<typeof limits> = limits) => searchIndexStreamStartup(env, l, facts);
  expect(start({}, true)).toEqual({
    stream: undefined,
    notice: { level: "info", message: expect.stringContaining("RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true"), fields: { streaming: "off", reason: "output_facts" } },
  });
  expect(start({ SEARCH_INDEX_STREAMING_ENABLED: "false" }, true)).toEqual({ stream: undefined, notice: undefined });
  expect(start({ SEARCH_INDEX_STREAMING_ENABLED: "false" })).toEqual({ stream: undefined, notice: undefined });
  expect(() => start({ SEARCH_INDEX_STREAMING_ENABLED: "true" }, true)).toThrow("RELAYHISTORY_SELFHOST_OUTPUT_FACTS");
  // The defaults (the ceiling) stream scopes past the bound: no warning, at any accepted whole-read bound.
  expect(start({})).toEqual({ stream: STREAM_READ_CEILING, notice: undefined });
  expect(start({ SEARCH_INDEX_STREAMING_ENABLED: "true" })).toEqual({ stream: STREAM_READ_CEILING, notice: undefined });
  expect(start({}, false, { maxScopeBytes: 256 * MiB, maxScopeEvents: 1_000_000 }).notice).toBeUndefined();
  // Both budgets lowered to their floors: no scope past the bound fits, so startup warns.
  const floor = { SEARCH_INDEX_STREAM_MAX_READ_BYTES: "256MiB", SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "400000" };
  expect(start(floor).notice).toEqual({
    level: "warn",
    message: expect.stringContaining("no scope past the whole-read bound fits"),
    fields: { max_read_bytes: 256 * MiB, max_read_events: 400_000, largest_streamed_scope_bytes: 64 * MiB, largest_streamed_scope_events: 100_000 },
  });
  // One budget that fits the smallest scope past its bound lets those scopes stream: no warning.
  expect(start({ ...floor, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "257MiB" }).notice).toBeUndefined();
  expect(start({ ...floor, SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "400004" }).notice).toBeUndefined();
  // Above the floor but below four times (bound + 1): still no scope fits, still a warning.
  expect(start({ ...floor, SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "400001" }).notice?.level).toBe("warn");
  expect(searchIndexStreamBudgetWarning(undefined, limits)).toBeUndefined();
});

test("budget warning boundary: a scope streams only strictly past the bound, so a budget needs four times (bound + 1)", () => {
  // canonical-projection.ts hands a scope to the streaming reader only when bytes > maxScopeBytes
  // or events > maxScopeEvents; canonical-stream-projection.ts refuses size * 4 > budget. The
  // smallest streamable scope is bound + 1, so 4 * bound .. 4 * bound + 3 fits none of them.
  const bound = { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000 };
  const floor = searchIndexStreamFloor(bound);
  expect(floor).toEqual({ maxReadBytes: 4 * bound.maxScopeBytes, maxReadEvents: 4 * bound.maxScopeEvents });
  const warn = (maxReadBytes: number, maxReadEvents: number) => searchIndexStreamBudgetWarning({ maxReadBytes, maxReadEvents }, bound);
  for (const [axis, b] of [["bytes", bound.maxScopeBytes], ["events", bound.maxScopeEvents]] as const) {
    const at = (budget: number) => (axis === "bytes" ? warn(budget, floor.maxReadEvents) : warn(floor.maxReadBytes, budget));
    expect(at(4 * b)).toBeDefined(); // exactly four whole reads (the accepted floor): no scope fits
    expect(at(4 * b + 3)).toBeDefined(); // one short of four times (bound + 1)
    expect(at(4 * b + 4)).toBeUndefined(); // four times (bound + 1): the smallest scope past the bound fits
    expect(at(4 * b + 5)).toBeUndefined(); // above it
  }
  // The floor is still accepted (values valid before stay valid) and warns at startup.
  const env = { SEARCH_INDEX_STREAM_MAX_READ_BYTES: String(floor.maxReadBytes), SEARCH_INDEX_STREAM_MAX_READ_EVENTS: String(floor.maxReadEvents) };
  expect(searchIndexStreamFromEnv(env, bound, false)).toEqual(floor);
  expect(searchIndexStreamStartup(env, bound, false).notice?.level).toBe("warn");
  // Four times (bound + 1) on the events axis is accepted and quiet.
  const events = { ...env, SEARCH_INDEX_STREAM_MAX_READ_EVENTS: String(4 * (bound.maxScopeEvents + 1)) };
  expect(searchIndexStreamStartup(events, bound, false).notice).toBeUndefined();
});

test("streaming at the default budgets: a scope past the whole-read bound is streamed, not left partial", async () => {
  const x = await scope(generate(31));
  const stream = searchIndexStreamFromEnv({}, TINY, false);
  expect(stream).toEqual(STREAM_READ_CEILING);
  const b = await backend(x, { limits: TINY, stream });
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s).toMatchObject({ complete: true, reasons: [], deleted: 0 });
  expect(s.stream).toMatchObject({ passes: 4 });
  expect(s.rows).toBeGreaterThan(10);
  // Off by default with output facts: the same scope stays partial (scope_too_large).
  expect(searchIndexStreamFromEnv({}, TINY, true)).toBeUndefined();
  const off = await backend(x, { limits: TINY });
  expect(await indexSearchScope(SCOPE, off.deps)).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large"] });
}, 60_000);

test("gate matrix, adapter binding: output facts never stream, even with stream set", async () => {
  const x = await scope(generate(42));
  // The ClickHouse facts on the adapter binding are refused before any read, as before.
  const ch = await backend(x, { limits: TINY, stream: UNLIMITED, outputFacts: true });
  await expect(indexSearchScope(SCOPE, ch.deps)).rejects.toThrow("output facts require the ClickHouse search binding");
  // The Postgres facts store: the scope is marked partial exactly as before, not streamed.
  const states: boolean[] = [];
  const store = {
    previousState: async () => ({ range: null, version: 0 }),
    recordState: async (_s: unknown, state: { complete: boolean }) => { states.push(state.complete); return true; },
    reconcile: async () => { throw new Error("not on a partial scope"); },
    erase: async () => {},
    schemaReady: async () => ({ ready: true, present: true }),
  };
  const pg = await backend(x, { limits: TINY, stream: UNLIMITED, outputFactsStore: () => store });
  const result = await indexSearchScope(SCOPE, pg.deps);
  expect(result).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large"] });
  expect(result.stream).toBeUndefined();
  expect(states).toEqual([false]);
  expect(await pg.scan()).toEqual([]);
}, 60_000);

// SEARCH_INDEX_STAGED_RECONCILE_ENABLED (Phase 3 slice 1): keyset-paged index scans and per-pass budgets.

/** Counts the adapter's paged and bounded scans. */
function countScans(b: Awaited<ReturnType<typeof backend>>) {
  const index = b.adapter.index;
  const counts = { pages: 0, bounded: 0 };
  const page = index.scanFingerprintsPage!.bind(index);
  const bounded = index.scanFingerprints.bind(index);
  index.scanFingerprintsPage = (async (...args: Parameters<typeof page>) => { counts.pages += 1; return page(...args); }) as typeof page;
  index.scanFingerprints = (async (...args: Parameters<typeof bounded>) => { counts.bounded += 1; return bounded(...args); }) as typeof bounded;
  return counts;
}

/** Index rows of a regenerable kind the source does not hold: a complete pass deletes them. */
async function addStale(b: Awaited<ReturnType<typeof backend>>, n: number) {
  const [first] = await b.docs();
  await b.adapter.index.upsert(Array.from({ length: n }, (_, i) => ({
    ...first!, eventId: `${first!.eventId}-stale-${i}`, fingerprint: "stale", version: NOW.getTime() - 1, taskRefId: "", deliveryRecordId: "",
  })));
}

const STAGED = { keyedRecordsCap: 10_000 };

test("staged on: a streamed scope with more index rows than SEARCH_INDEX_MAX_ROWS is scanned in pages and ends complete", async () => {
  const x = await scope(generate(51));
  const whole = await backend(x, {});
  expect(await indexSearchScope(SCOPE, whole.deps)).toMatchObject({ complete: true, reasons: [] });
  const limits = { ...TINY, maxRows: 3, insertBatch: 4 };
  const on = await backend(x, { limits, stream: UNLIMITED, staged: STAGED });
  const off = await backend(x, { limits, stream: UNLIMITED });
  for (const b of [on, off]) {
    expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true }); // empty index: nothing to truncate
    await addStale(b, 5);
  }
  const rows = (await whole.scan()).length;
  expect(rows + 5).toBeGreaterThan(limits.maxRows);
  // Off: the bounded scan stops past SEARCH_INDEX_MAX_ROWS, nothing is deleted.
  const offScans = countScans(off);
  expect(await indexSearchScope(SCOPE, off.deps)).toMatchObject({ complete: false, deleted: 0, reasons: ["search_index_truncated"] });
  expect(offScans).toEqual({ pages: 0, bounded: 1 });
  // On: every index row in pages of the insert batch; the stale rows are deleted and the pass is complete.
  const onScans = countScans(on);
  const s = await indexSearchScope(SCOPE, on.deps);
  expect(s).toMatchObject({ complete: true, deleted: 5, inserted: 0, reasons: [] });
  expect(s.stream).toMatchObject({ passes: 3 });
  expect(onScans).toEqual({ pages: Math.ceil((rows + 5) / 4), bounded: 0 });
  expect(await on.scan()).toEqual(await whole.scan());
  expect(on.recorded.at(-1)).toEqual({ complete: true, rows });
  expect(on.directory.length).toBeGreaterThan(0);
}, 60_000);

test("staged on: a whole read with more index rows than SEARCH_INDEX_MAX_ROWS is scanned in pages and ends complete", async () => {
  const x = await scope(generate(52));
  const limits = { maxRows: 3, insertBatch: 4 };
  const on = await backend(x, { limits, stream: UNLIMITED, staged: STAGED });
  const off = await backend(x, { limits, stream: UNLIMITED });
  for (const b of [on, off]) {
    expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
    await addStale(b, 2);
  }
  expect(await indexSearchScope(SCOPE, off.deps)).toMatchObject({ complete: false, deleted: 0, reasons: ["search_index_truncated"] });
  const scans = countScans(on);
  const s = await indexSearchScope(SCOPE, on.deps);
  expect(s).toMatchObject({ complete: true, deleted: 2, reasons: [] });
  expect(s.stream).toBeUndefined();
  expect(scans.bounded).toBe(0);
  expect(scans.pages).toBeGreaterThan(1);
}, 60_000);

test("staged on: the read budgets bound each pass, so a scope past a quarter of a budget still streams", async () => {
  const x = await scope(generate(53));
  const probe = await backend(x, { limits: TINY, stream: UNLIMITED, staged: STAGED });
  const read = (await indexSearchScope(SCOPE, probe.deps)).stream!;
  expect(read.passes).toBe(4);
  // Pass 1 to 3 read the same chunks and records; pass 4 reads them again.
  const perPass = { bytes: read.bytes / 4, events: read.events / 4 };
  expect(Number.isInteger(perPass.bytes) && Number.isInteger(perPass.events)).toBe(true);
  // Twice one pass: fits each pass, not four of them.
  for (const [budget, code] of [
    [{ maxReadBytes: 2 * perPass.bytes, maxReadEvents: 1 << 24 }, STREAM_READ_BYTES_BUDGET],
    [{ maxReadBytes: 1 << 30, maxReadEvents: 2 * perPass.events }, STREAM_READ_EVENTS_BUDGET],
  ] as const) {
    const off = await backend(x, { limits: TINY, stream: budget });
    expect(await indexSearchScope(SCOPE, off.deps)).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", code] });
    const on = await backend(x, { limits: TINY, stream: budget, staged: STAGED });
    const s = await indexSearchScope(SCOPE, on.deps);
    expect(s).toMatchObject({ complete: true, reasons: [] });
    // Summed over the passes it read well past the budget; no single pass did.
    expect(s.stream!.bytes > budget.maxReadBytes || s.stream!.events > budget.maxReadEvents).toBe(true);
    expect(await on.scan()).toEqual(await probe.scan());
  }
  // Under one pass is still refused, per pass.
  const tight = await backend(x, { limits: TINY, stream: { maxReadBytes: 1 << 30, maxReadEvents: perPass.events - 1 }, staged: STAGED });
  expect(await indexSearchScope(SCOPE, tight.deps)).toMatchObject({ complete: false, partial: true, inserted: 0, reasons: ["scope_too_large", STREAM_READ_EVENTS_BUDGET] });
  expect(await tight.scan()).toEqual([]);
}, 60_000);

test("staged on: past the keyed-records cap the scope stays partial with stream_keyed_records_cap, nothing written or deleted", async () => {
  const x = await scope(generate(54));
  const probe = await backend(x, { limits: TINY, stream: UNLIMITED, staged: STAGED });
  const read = (await indexSearchScope(SCOPE, probe.deps)).stream!;
  const records = read.events / read.passes;
  // Records of one pass past the cap.
  const over = await backend(x, { limits: TINY, stream: UNLIMITED, staged: { keyedRecordsCap: records - 1 } });
  const s = await indexSearchScope(SCOPE, over.deps);
  expect(s).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", STREAM_KEYED_RECORDS_CAP] });
  expect(await over.scan()).toEqual([]);
  expect(over.recorded).toEqual([{ complete: false, rows: 0 }]);
  // At the cap the records fit; index rows past it stop the scan before any delete.
  const at = await backend(x, { limits: { ...TINY, maxRows: 3 }, stream: UNLIMITED, staged: { keyedRecordsCap: records } });
  expect(await indexSearchScope(SCOPE, at.deps)).toMatchObject({ complete: true });
  const before = await at.scan();
  expect(before.length).toBeLessThanOrEqual(records);
  await addStale(at, records + 1 - before.length);
  const capped = await indexSearchScope(SCOPE, at.deps);
  expect(capped).toMatchObject({ complete: false, partial: true, deleted: 0, reasons: ["scope_too_large", STREAM_KEYED_RECORDS_CAP] });
  expect((await at.scan()).length).toBe(records + 1);
  expect(at.recorded.at(-1)).toEqual({ complete: false, rows: 0 });
}, 60_000);

test("staged on: an index without paged scans is refused, not scanned partially", async () => {
  const x = await scope(generate(55));
  const b = await backend(x, { limits: TINY, stream: UNLIMITED, staged: STAGED });
  delete (b.adapter.index as { scanFingerprintsPage?: unknown }).scanFingerprintsPage;
  await expect(indexSearchScope(SCOPE, b.deps)).rejects.toThrow("search paged scan unsupported by this index");
  expect(await b.scan()).toEqual([]);
}, 60_000);

/** Counts upsert and deleteEvents calls made from now on. */
function countWrites(b: Awaited<ReturnType<typeof backend>>) {
  const index = b.adapter.index;
  const writes = { upserts: 0, deletes: 0 };
  const upsert = index.upsert.bind(index);
  const remove = index.deleteEvents.bind(index);
  index.upsert = (async (...args: Parameters<typeof upsert>) => { writes.upserts += 1; return upsert(...args); }) as typeof upsert;
  index.deleteEvents = (async (...args: Parameters<typeof remove>) => { writes.deletes += 1; return remove(...args); }) as typeof remove;
  return writes;
}

test("staged on: SEARCH_INDEX_MAX_ROWS above the cap is the scan bound, so the walk reads past the cap and ends complete", async () => {
  const x = await scope(generate(56));
  const b = await backend(x, { limits: { maxRows: 1_000, insertBatch: 2 }, stream: UNLIMITED, staged: { keyedRecordsCap: 3 } });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  await addStale(b, 2);
  const rows = (await b.scan()).length;
  expect(rows).toBeGreaterThan(3 + 1); // past the cap (and past cap + 1, the cap-only bound)
  const scans = countScans(b);
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true, deleted: 2, reasons: [] });
  expect(scans).toEqual({ pages: Math.ceil(rows / 2), bounded: 0 });
}, 60_000);

test("staged on: a whole read whose index scan passes the cap is incomplete with stream_keyed_records_cap alone, nothing deleted", async () => {
  const x = await scope(generate(31));
  const b = await backend(x, { limits: { maxRows: 2, insertBatch: 2 }, stream: UNLIMITED, staged: { keyedRecordsCap: 3 } });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true }); // empty index
  await addStale(b, 1);
  const before = await b.scan();
  expect(before.length).toBeGreaterThan(3);
  const writes = countWrites(b);
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(s.reasons).toEqual([STREAM_KEYED_RECORDS_CAP]);
  expect(s).toMatchObject({ complete: false, deleted: 0 });
  expect(s.partial).toBeUndefined();
  expect(s.stream).toBeUndefined();
  expect(writes.deletes).toBe(0);
  expect(await b.scan()).toEqual(before);
  expect(b.recorded.at(-1)).toMatchObject({ complete: false });
}, 60_000);

test("staged on: a malformed scan page is refused before any write or delete, on both paths", async () => {
  type Page = NonNullable<Awaited<ReturnType<typeof backend>>["adapter"]["index"]["scanFingerprintsPage"]>;
  const corrupt: Array<[string, (real: Page) => Page]> = [
    ["next is not the last row", (real) => async (sc, o) => { const p = await real(sc, o); return p.next ? { ...p, next: positionOf(p.rows[0]!) } : p; }],
    ["rows out of order", (real) => async (sc, o) => { const p = await real(sc, o); return { ...p, rows: [...p.rows].reverse() }; }],
    ["a row repeated across pages", (real) => {
      let last: Awaited<ReturnType<Page>>["rows"][number] | undefined;
      return async (sc, o) => {
        const p = await real(sc, o);
        const rows = last ? [last, ...p.rows.slice(0, -1)] : p.rows;
        last = p.rows[p.rows.length - 1];
        return { rows, ...(p.next ? { next: positionOf(rows[rows.length - 1]!) } : {}) };
      };
    }],
    ["an empty page with next", () => async () => ({ rows: [], next: { tsMs: 0, sessionId: "s", source: "claude", machineId: "m", kind: "k", eventId: "e" } })],
    ["a page larger than the page size", (real) => async (sc, o) => real(sc, { ...o, limit: o.limit + 1 })],
  ];
  const x = await scope(generate(33));
  for (const [name, wrap] of corrupt)
    for (const limits of [{ maxRows: 1_000, insertBatch: 2 }, { ...TINY, maxRows: 1_000, insertBatch: 2 }]) {
      const b = await backend(x, { limits, stream: UNLIMITED, staged: STAGED });
      expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
      await addStale(b, 3);
      const before = await b.scan();
      const index = b.adapter.index;
      index.scanFingerprintsPage = wrap(index.scanFingerprintsPage!.bind(index));
      const writes = countWrites(b);
      const recorded = b.recorded.length;
      await expect(indexSearchScope(SCOPE, b.deps), name).rejects.toThrow(/search scan (result|order) invalid/);
      expect({ name, ...writes }).toEqual({ name, upserts: 0, deletes: 0 });
      expect(b.recorded.length).toBe(recorded);
      expect(await b.scan()).toEqual(before);
    }
}, 120_000);

test("staged on: the consumer refuses at startup an index without paged scans; off never checks", () => {
  const without = {};
  const withPages = { scanFingerprintsPage: async () => ({ rows: [] }) };
  expect(() => assertStagedSearchIndex(STAGED, without)).toThrow(
    "pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires a search index with paged scans",
  );
  expect(() => assertStagedSearchIndex(STAGED, withPages)).not.toThrow();
  expect(() => assertStagedSearchIndex(undefined, without)).not.toThrow();
});

test("staged on: the whole-read page walk keeps the transaction busy and stops at the deadline between pages", async () => {
  const x = await scope(generate(31));
  const b = await backend(x, { limits: { maxRows: 1_000, insertBatch: 2 }, stream: UNLIMITED, staged: STAGED });
  expect(await indexSearchScope(SCOPE, b.deps)).toMatchObject({ complete: true });
  await addStale(b, 1);
  const rows = (await b.scan()).length;
  // Due every time: one ping between each pair of pages, none before the first.
  b.sb.calls.length = 0;
  const scans = countScans(b);
  expect(await indexSearchScope(SCOPE, { ...b.deps, streamKeepaliveMs: 0 })).toMatchObject({ complete: true, deleted: 1 });
  expect(scans.pages).toBe(Math.ceil(rows / 2));
  expect(b.sb.calls.filter((c) => c === PING).length).toBe(scans.pages - 1);
  // Past the deadline: stopped after the first page, nothing written or deleted, the scope owes a pass.
  await addStale(b, 1);
  const before = await b.scan();
  const writes = countWrites(b);
  const s = await indexSearchScope(SCOPE, { ...b.deps, streamDeadline: { at: 0 } });
  expect(s).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: [STREAM_DEADLINE] });
  expect(writes).toEqual({ upserts: 0, deletes: 0 });
  expect(await b.scan()).toEqual(before);
  expect(b.recorded.at(-1)).toEqual({ complete: false, rows: 0 });
}, 60_000);
