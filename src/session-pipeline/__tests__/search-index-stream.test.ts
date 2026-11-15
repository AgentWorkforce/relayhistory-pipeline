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
import { indexSearchScope, searchIndexStreamFromEnv, type SearchIndexDeps } from "../search-index.ts";
import { STREAM_READ_BYTES_BUDGET, STREAM_SOURCE_CHANGED } from "../../session-read-api/canonical-stream-projection.ts";
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

test("streaming on: a source that changes before regeneration writes no rows, deletes nothing and owes a pass", async () => {
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
  const b = await backend(x, { limits: TINY, stream: UNLIMITED, decompress: changing, reader: withoutRawDigests(x.reader) });
  // The whole-read sizing reads no chunk; passes 1 to 3 read every chunk once, pass 4 sees the change.
  const chunks = await Promise.all(x.keys.map(async (k) => (await x.reader.read(k))!.manifest.raw.chunks.length));
  after = 3 * chunks.reduce((a, n) => a + n, 0);
  const s = await indexSearchScope(SCOPE, b.deps);
  expect(calls).toBeGreaterThan(after); // pass 4 ran
  expect(s).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: [STREAM_SOURCE_CHANGED] });
  expect(await b.scan()).toEqual([]);
  expect(b.directory).toEqual([]);
  expect(b.recorded).toEqual([expect.objectContaining({ complete: false })]);
}, 60_000);

test("SEARCH_INDEX_STREAMING_ENABLED and the read budgets are validated", () => {
  const MiB = 1024 * 1024;
  const limits = { maxScopeBytes: 64 * MiB, maxScopeEvents: 100_000 };
  expect(searchIndexStreamFromEnv({}, limits, false)).toBeUndefined();
  expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "" }, limits, false)).toBeUndefined();
  expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "false" }, limits, false)).toBeUndefined();
  // Off ignores the budgets entirely.
  expect(searchIndexStreamFromEnv({ SEARCH_INDEX_STREAM_MAX_READ_BYTES: "nonsense" }, limits, true)).toBeUndefined();
  for (const flag of ["1", "TRUE", "yes", " true"])
    expect(() => searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: flag }, limits, false)).toThrow("SEARCH_INDEX_STREAMING_ENABLED must be true or false");
  expect(() => searchIndexStreamFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "true" }, limits, true)).toThrow("RELAYHISTORY_SELFHOST_OUTPUT_FACTS");
  const on = { SEARCH_INDEX_STREAMING_ENABLED: "true" };
  expect(searchIndexStreamFromEnv(on, limits, false)).toEqual({ maxReadBytes: 256 * MiB, maxReadEvents: 400_000 });
  // Defaults: four whole reads, capped at 2 GiB / 2,000,000.
  expect(searchIndexStreamFromEnv(on, { maxScopeBytes: 1024 * MiB, maxScopeEvents: 1_000_000 }, false)).toEqual({ maxReadBytes: 2048 * MiB, maxReadEvents: 2_000_000 });
  expect(searchIndexStreamFromEnv({ ...on, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "2GiB", SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "2000000" }, limits, false)).toEqual({ maxReadBytes: 2048 * MiB, maxReadEvents: 2_000_000 });
  expect(searchIndexStreamFromEnv({ ...on, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "512MiB" }, limits, false)!.maxReadBytes).toBe(512 * MiB);
  for (const [name, value] of [
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "255MiB"], // below the default
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "3GiB"], // above the cap
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "1.5GiB"],
    ["SEARCH_INDEX_STREAM_MAX_READ_BYTES", "-1"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "399999"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "2000001"],
    ["SEARCH_INDEX_STREAM_MAX_READ_EVENTS", "1MiB"],
  ] as const) {
    let message = "";
    try {
      searchIndexStreamFromEnv({ ...on, [name]: value }, limits, false);
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toStartWith(`pipeline consumer: ${name} must be`);
    expect(message).not.toContain(value); // never echoes the value
  }
});
