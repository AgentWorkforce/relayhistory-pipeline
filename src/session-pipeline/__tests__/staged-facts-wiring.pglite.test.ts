/**
 * Staged output facts through the consumer's wiring (Phase 3 slice 2c phase B): with
 * SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true and RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true, a scope the
 * whole read refuses as too large runs as a staged run whose facts are staged by its hooks
 * (output-facts-store.ts, migration 20261105062140) and end equal to the whole read's facts. Also: the
 * facts pass bounded by bytes as well as inputs; a chunk that cannot be staged abandons the run by name
 * and writes nothing; with a facts store and no staged run a too-large scope stays incomplete as
 * before; facts turned on mid-run (a kill at every phase boundary, then the hooks attached) send the
 * run round once and converge, never emptying the facts; startup with the staged flag off is
 * byte-identical to before; and the Azure-shaped startup chain (LOCKER_PROVIDER=azure, facts and
 * staged on) in-process, with no Azure account.
 *
 * The staging and facts functions refuse any caller but search_consumer_runtime: the test transaction
 * takes that role around their calls only. Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { pipelineQueueDeps, prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { buildLockerStore } from "../../shared/locker.ts";
import { workerAdapterEntry, workerBindings } from "../../shared/worker.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { consumerServiceConfig } from "../consumer-config.ts";
import { CONSUMER_WORKER, outputFactsDeps } from "../consumer-service.ts";
import { droppedFanout } from "../pointer.ts";
import type { SearchProjectionState, SearchServiceBinding } from "../search-adapter.ts";
import {
  indexSearchScope, searchIndexStagedOn, searchIndexStagedStartup, searchIndexStreamStartup, STAGED_FACTS_CHUNK, STAGED_FACTS_RECORD_BYTES_CAP, STAGED_FACTS_WIDE_TEXT_WEIGHT, STAGED_REFUSAL_MAX_ENTRIES, STAGED_REFUSAL_TTL_MS, StagedRefusals,
  type SearchIndexDeps, type SearchIndexResult, type StagedReconcile,
} from "../search-index.ts";
import {
  MAX_FACTS_CHUNK_INPUTS, MAX_STAGE_CALL_BYTES, OUTPUT_FACTS_CHUNK_UNSTAGEABLE, STAGED_OUTPUT_FACTS_FUNCTIONS, assertStagedOutputFactsSchema,
  postgresOutputFactsStore, stagedFactsSendLimit, unstageable,
} from "../output-facts-store.ts";
import { OUTPUT_FACTS_MAX_ITEMS_PER_INPUT } from "../output-facts.ts";
import type { StagedBoundary } from "../staged-reconcile.ts";
import { SCOPE, USER, decompress, exportStreamScope, generate, prefixFor, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { legacyMachineId } from "../../shared/legacy-export-machine.ts";
import { FACTS_CHUNK_MAX_BYTES, streamCanonicalScope } from "../../session-read-api/canonical-stream-projection.ts";
import { readCanonicalScope } from "../../session-read-api/canonical-projection.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const STAGED_LIMITS = { ...TINY, insertBatch: 4 };
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const RUNTIME = "search_consumer_runtime";
const FACTS = outputFactsDeps({ outputFacts: true });

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

class Kill extends Error {}

/** serve()'s staged deps with the flag on (output facts on too), test batch sizes and the boundary seam. */
function stagedOn(run: StagedReconcile["run"] = {}): StagedReconcile {
  const started = searchIndexStagedStartup({ SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "true" }, UNLIMITED, true);
  expect(started).toBeDefined();
  return { ...started!.staged, run: { stageBatch: 7, writeKeys: 5, factsChunk: 7, ...run } };
}

const ROLE_CALLS = ["sessions.search_reconcile_", "sessions.staged_facts_", "sessions.output_facts_"];

async function rig(x: ExportedScope) {
  const hoop = await clonedReplayDatabase();
  cleanup.push(() => hoop.pg.close());
  await hoop.pg.exec(`GRANT USAGE ON SCHEMA sessions TO ${RUNTIME}`); // as session_consumer_role.sql does
  const searchDb = new PGlite({ extensions: { pg_trgm } });
  cleanup.push(() => searchDb.close());
  await searchDb.exec("CREATE EXTENSION pg_trgm");
  const tenant: TenantSqlRunner = (orgId, workspaceId, work) =>
    searchDb.transaction(async (tx) => {
      await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      return work(tx);
    });
  const recorded: Array<{ complete: boolean }> = [];
  const state: SearchProjectionState = {
    fromMs: async () => 0,
    record: async (_tx, _scope, value) => {
      recorded.push({ complete: value.complete });
    },
    partials: async () => ({ sessions: [], overflow: false }),
  };
  const composed = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: searchDb }, "writer", tenant);
  await composed.index.ensureSchema();
  const adapter: SearchServiceBinding = { index: composed.index, state: composed.state };
  const transact = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
    hoop.pg.transaction((tx) =>
      work({
        query: async (sql: string, params?: unknown[]) => {
          if (!ROLE_CALLS.some((p) => sql.includes(p))) return tx.query(sql, params) as never;
          await tx.exec(`SET LOCAL ROLE ${RUNTIME}`);
          const result = await tx.query(sql, params); // a failure aborts the transaction: no RESET then
          await tx.exec("RESET ROLE");
          return result as never;
        },
      }),
    ) as Promise<T>;
  const consumer = (search: Partial<SearchIndexDeps>): ConsumerDeps => ({
    pointer: {
      db: { ...hoop.db, pool: undefined, end: async () => {} } as never,
      lock: (async (_s: unknown, work: () => Promise<unknown>) => work()) as never,
      reader: x.reader,
      exists: async (k: string) => (await x.store.head(objectKey(k as never))) !== undefined,
      orgs: prefixOrgResolver,
      allowPrefix: (p: string, o: string) => prefixAllowed(bindings, p, o),
      fanout: droppedFanout,
    } as never,
    search: { transact, reader: x.reader, store: x.store, adapter, prefixFor, now: () => NOW, ...search },
    requeue: async () => {},
  });
  const messages = x.keys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  const tick = async (search: Partial<SearchIndexDeps>) => processBatch(messages, consumer(search));
  const pass = (search: Partial<SearchIndexDeps>): Promise<SearchIndexResult> => indexSearchScope(SCOPE, { ...consumer(search).search } as SearchIndexDeps);
  const count = async (table: string) => (await hoop.pg.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${table}`)).rows[0]!.n;
  /** The scope's facts and state rows, without the columns that carry the pass's version and clock. */
  const facts = async () => {
    const rows = async (table: string, order: string, drop: string[]) =>
      (await hoop.pg.query<{ j: Record<string, unknown> }>(`SELECT to_jsonb(t) j FROM sessions.${table} t ORDER BY ${order}`)).rows
        .map((r) => JSON.stringify(Object.fromEntries(Object.entries(r.j).filter(([k]) => !drop.includes(k)))));
    return {
      events: await rows("session_event_facts", "row_id", ["source_version"]),
      facts: await rows("session_output_facts", "row_id", ["source_version"]),
      state: await rows("output_facts_session_state", "session_id", ["source_version", "synced_at"]),
    };
  };
  const expire = () => hoop.pg.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
  return { hoop, tick, pass, transact, count, facts, expire, recorded };
}

/** One streamed read's facts pass, chunked at `chunk` inputs and `bytes` UTF-8 bytes. */
async function factsChunksOf(x: ExportedScope, chunk: number, bytes: number) {
  const admitted = (await readCanonicalScope(SCOPE, x.keys, { reader: x.reader, store: x.store, prefixFor, decompress } as never,
    { ...TINY, maxRows: 1 << 24, insertBatch: 5000 }, NOW.getTime(), x.canonicalId)).admitted!;
  const read = await streamCanonicalScope(SCOPE, admitted, { store: x.store, prefixFor, decompress } as never, UNLIMITED, NOW.getTime(), 0);
  expect(read.complete).toBe(true);
  const chunks: Array<{ inputs: string[]; sizes: number[]; bytes: number }> = [];
  await read.facts(chunk, async (e) => {
    const inputs = [...e.projectionRows.map((p) => JSON.stringify(p)), ...e.records.map((r) => JSON.stringify(r))];
    const sizes = [...e.projectionRows.map((p) => Buffer.byteLength(JSON.stringify(p.row), "utf8")), ...e.records.map((r) => Buffer.byteLength(JSON.stringify(r), "utf8"))];
    chunks.push({ inputs, sizes, bytes: sizes.reduce((n, v) => n + v, 0) });
  }, bytes);
  return chunks;
}

/** Every split of the facts pass holds the same inputs, each chunk within both bounds or a single larger input. */
async function checkFactsChunks(x: ExportedScope) {
  const whole = await factsChunksOf(x, 1_000_000, Number.MAX_SAFE_INTEGER);
  expect(whole.length).toBe(1);
  const total = whole[0]!.inputs.slice().sort();
  const largest = Math.max(...whole[0]!.sizes);
  for (const [chunk, bytes] of [[7, Number.MAX_SAFE_INTEGER], [1_000_000, 4096], [50, 1500], [1_000_000, largest], [1_000_000, largest - 1], [1, 1]] as const) {
    const chunks = await factsChunksOf(x, chunk, bytes);
    expect(chunks.flatMap((c) => c.inputs).sort(), `${chunk}/${bytes}`).toEqual(total);
    for (const c of chunks) {
      expect(c.inputs.length).toBeLessThanOrEqual(chunk);
      // Never over the byte bound, unless one input alone is larger: then it is a chunk of its own.
      expect(c.bytes <= bytes || c.inputs.length === 1, `${chunk}/${bytes}: ${c.bytes} bytes in ${c.inputs.length}`).toBe(true);
    }
    if (bytes < Number.MAX_SAFE_INTEGER) expect(chunks.length).toBeGreaterThan(1);
  }
  return total.length;
}

describe("staged output facts through the consumer wiring", () => {
  let x: ExportedScope;
  let ref: Awaited<ReturnType<Awaited<ReturnType<typeof rig>>["facts"]>>;
  beforeAll(async () => {
    x = await exportStreamScope(generate(31), 1200);
    cleanup.length = 0; // the exported scope outlives the per-test cleanup
    // The reference: the whole read with unlimited limits and the Postgres facts store (no staged run).
    const r = await rig(x);
    await r.tick(FACTS);
    expect((await r.pass(FACTS)).outputFacts).toMatchObject({ complete: true });
    ref = await r.facts();
    expect(ref.events.length).toBeGreaterThan(10);
    expect(ref.state.every((s) => JSON.parse(s).complete === true)).toBe(true);
  }, 180_000);
  afterAll(() => x?.dispose());

  /** A rig whose pointer stage ran and whose facts hold the whole read's (the earlier pass of the scope). */
  const withFacts = async () => {
    const r = await rig(x);
    await r.tick(FACTS);
    expect(await r.facts()).toEqual(ref);
    return r;
  };
  /** A rig whose pointer stage ran, with no facts yet. */
  const bare = async () => {
    const r = await rig(x);
    await r.tick({});
    expect((await r.facts()).events).toEqual([]);
    return r;
  };

  test("flag and facts on: a too-large scope runs staged, its facts equal the whole read's, no stage is left", async () => {
    const r = await bare();
    const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(out).toMatchObject({ complete: true, reasons: [] });
    expect(out.stream?.passes).toBeGreaterThanOrEqual(4); // passes 1-3 and the facts pass
    expect(out.outputFacts).toMatchObject({ complete: true, deleted: 0 });
    expect(out.outputFacts!.inserted).toBe(ref.events.length + ref.facts.length);
    expect(await r.facts()).toEqual(ref);
    expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1"), await r.count("staged_facts_v1")]).toEqual([0, 0, 0]);
    // Up to date: the next staged pass writes and deletes no facts.
    const again = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(again.outputFacts).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
    expect(await r.facts()).toEqual(ref);
  }, 180_000);

  test("facts that changed since the last pass: the staged run deletes what the source no longer has", async () => {
    const r = await withFacts();
    // A fact row the source does not carry (as if written by an earlier source): a complete staged pass removes it.
    await r.hoop.pg.exec(`INSERT INTO sessions.session_output_facts
      SELECT org_id, workspace_id, fact_kind, ts, scope_source, source, session_id, 'gone', origin, 'gone', added, removed, reverted, retention_days, repeat('f', 64), fingerprint, source_version
        FROM sessions.session_output_facts LIMIT 1`);
    const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(out.outputFacts).toMatchObject({ complete: true, deleted: 1 });
    expect(await r.facts()).toEqual(ref);
  }, 180_000);

  test("a refused directory write (directory_event_sources_changed): the staged facts insert what was read and delete nothing, as the whole-set path does; no restart, the run finishes", async () => {
    const refuse = { writeEvents: async () => ({ written: false }) } as unknown as Partial<SearchIndexDeps>;
    // A fact row the source does not carry: a pass that may delete removes it; this one must not.
    const gone = `INSERT INTO sessions.session_output_facts
      SELECT org_id, workspace_id, fact_kind, ts, scope_source, source, session_id, 'gone', origin, 'gone', added, removed, reverted, retention_days, repeat('f', 64), fingerprint, source_version
        FROM sessions.session_output_facts LIMIT 1`;
    // The whole-set path, same refusal (unlimited limits, no staged run).
    const w = await withFacts();
    await w.hoop.pg.exec(gone);
    const whole = await w.pass({ ...FACTS, ...refuse });
    expect(whole).toMatchObject({ complete: false });
    expect(whole.reasons).toContain("directory_event_sources_changed");
    expect(whole.outputFacts).toMatchObject({ complete: false, deleted: 0 });
    // The staged run.
    const r = await withFacts();
    await r.hoop.pg.exec(gone);
    const out = await r.pass({ ...FACTS, ...refuse, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(out).toMatchObject({ complete: false });
    expect(out.reasons).toContain("directory_event_sources_changed");
    expect(out.reasons).not.toContain("output_facts_stage_incomplete");
    expect(out.outputFacts).toMatchObject({ complete: false, deleted: 0 });
    expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1"), await r.count("staged_facts_v1")]).toEqual([0, 0, 0]);
    expect(await r.facts()).toEqual(await w.facts());
    expect((await r.facts()).facts.some((f) => JSON.parse(f).fact_key === "gone")).toBe(true);
  }, 180_000);

  test("a chunk that cannot be staged: the run abandons as output_facts_chunk_unstageable and writes no facts; with room again it converges", async () => {
    const r = await withFacts();
    stagedFactsSendLimit.bytes = 64; // every chunk is over it
    try {
      const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
      expect(out).toMatchObject({ complete: false, reasons: [OUTPUT_FACTS_CHUNK_UNSTAGEABLE] });
      expect(out.outputFacts).toBeUndefined();
    } finally {
      stagedFactsSendLimit.bytes = Number.MAX_SAFE_INTEGER;
    }
    stagedFactsSendLimit.bytes = Number.MAX_SAFE_INTEGER;
    // The facts rows are untouched; their state now owes a pass (recorded at the claim), as on the whole
    // read's partial path.
    const after = await r.facts();
    expect([after.events, after.facts]).toEqual([ref.events, ref.facts]);
    expect(after.state.length).toBeGreaterThan(0);
    expect(after.state.every((row) => JSON.parse(row).complete === false)).toBe(true);
    // Abandoned (not left leased): the run row stays, back at want, with no facts stage.
    expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1"), await r.count("staged_facts_v1")]).toEqual([1, 0, 0]);
    const again = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(again).toMatchObject({ complete: true });
    expect(await r.facts()).toEqual(ref);
  }, 180_000);

  const stateRows = (r: Awaited<ReturnType<typeof rig>>) =>
    r.hoop.pg.query<{ session_id: string; complete: boolean; source_version: string }>(
      "SELECT session_id, complete, source_version::text FROM sessions.output_facts_session_state ORDER BY session_id").then((q) => q.rows);

  test("the session grows (a new directory epoch) and the staged run abandons: every facts state row owes a pass at the new epoch; the next run converges", async () => {
    const r = await withFacts();
    expect((await stateRows(r)).every((s) => s.complete)).toBe(true);
    await r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1");
    const epoch = (await r.hoop.pg.query<{ e: string }>("SELECT max(event_projection_epoch)::text e FROM sessions.session_directory")).rows[0]!.e;
    stagedFactsSendLimit.bytes = 64;
    try {
      const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
      expect(out.reasons).toEqual([OUTPUT_FACTS_CHUNK_UNSTAGEABLE]);
    } finally {
      stagedFactsSendLimit.bytes = Number.MAX_SAFE_INTEGER;
    }
    const owing = await stateRows(r);
    expect(owing.length).toBeGreaterThan(0);
    expect(owing.every((s) => !s.complete && s.source_version === epoch)).toBe(true);
    expect(await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() })).toMatchObject({ complete: true });
    expect((await stateRows(r)).every((s) => s.complete)).toBe(true);
  }, 180_000);

  test("a run killed right after its claim leaves the facts state owing (recorded in the claim's transaction)", async () => {
    const r = await withFacts();
    const out = await r.pass({
      ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED,
      staged: stagedOn({ releaseOnError: false, boundary: (p) => { if (p === "claimed") throw new Kill(p); } }),
    });
    expect(out.reasons).toEqual(["staged_run_interrupted"]);
    expect((await stateRows(r)).every((s) => !s.complete)).toBe(true);
  }, 180_000);

  test("a facts store and no staged run: a too-large scope stays incomplete (scope_too_large, partial), the streamed reconcile is never reached", async () => {
    const r = await withFacts();
    const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED });
    expect(out).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large"] });
    expect(out.stream).toBeUndefined();
    expect(await r.count("search_reconcile_runs_v1")).toBe(0);
    // The facts are left as they were; their state now owes a pass.
    const after = await r.facts();
    expect([after.events, after.facts]).toEqual([ref.events, ref.facts]);
    expect(after.state.every((s) => JSON.parse(s).complete === false)).toBe(true);
  }, 180_000);

  test("the ClickHouse facts flag with a staged run is refused (the staged hooks carry the Postgres store only)", async () => {
    const r = await bare();
    await expect(r.pass({ adapter: undefined, outputFacts: true, ch: {} as never, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() })).rejects.toThrow(/Postgres store's facts only/);
  });

  // Facts off for the first attempt (no hooks), killed at a boundary; the next attempts have the hooks. A run
  // resumed past the want phase has no whole stage: onFinish sends it round once (output_facts_stage_incomplete,
  // facts untouched); the attempt after that stages from its first chunk and reconciles. Never emptied.
  // (No write:page: the index already holds every row, so pass 4 writes no page here.)
  const BOUNDARIES: StagedBoundary[] = ["claimed", "want:batch", "want:done", "have:batch", "have:done", "write:done", "final:locked", "final:removed"];
  for (const point of BOUNDARIES)
    test(`facts off, killed at ${point}, then on: the run converges to the whole read's facts and never empties them`, async () => {
      const r = await withFacts();
      let killed = false;
      const kill = (p: StagedBoundary) => {
        if (p === point && !killed) {
          killed = true;
          throw new Kill(p);
        }
      };
      const first = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ boundary: kill, releaseOnError: false }) });
      expect(killed).toBe(true);
      expect(first.reasons).toEqual(["staged_run_interrupted"]);
      expect(await r.facts()).toMatchObject({ events: ref.events, facts: ref.facts });
      await r.expire();
      const reasons: string[][] = [];
      for (let i = 0; i < 3; i++) {
        const out = await r.pass({ ...FACTS, limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
        reasons.push(out.reasons);
        const now = await r.facts();
        expect([now.events, now.facts]).toEqual([ref.events, ref.facts]); // unchanged or correct: here the same rows
        if (out.complete) break;
      }
      // Resumed in the want phase (before its facts pass): whole at once. Past it: one restart first.
      const resumedInWant = point === "claimed" || point === "want:batch";
      expect(reasons).toEqual(resumedInWant ? [[]] : [["output_facts_stage_incomplete"], []]);
      expect(await r.facts()).toEqual(ref);
      expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1")]).toEqual([0, 0]);
    }, 180_000);

  test("facts pass bounded by UTF-8 bytes as well as inputs: catalog rows counted, every chunk within both bounds or a single input, the same inputs in total", async () => {
    await checkFactsChunks(x);
    expect(FACTS_CHUNK_MAX_BYTES).toBe(16 * 1024 * 1024);
  }, 180_000);
});

/** The staged run summary lines a block of work logs (as staged-search-wiring.pglite.test.ts captures them). */
async function summaries<T>(work: () => Promise<T>): Promise<{ value: T; lines: Array<Record<string, unknown>> }> {
  const raw: string[] = [];
  const stdout = process.stdout.write, stderr = process.stderr.write;
  const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
    ((chunk: unknown, ...rest: unknown[]) => {
      raw.push(String(chunk));
      return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
    }) as typeof process.stdout.write;
  process.stdout.write = tap(stdout, process.stdout);
  process.stderr.write = tap(stderr, process.stderr);
  try {
    const value = await work();
    const lines = raw.flatMap((l) => l.split("\n")).filter((l) => l.startsWith("{")).map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((l) => l.message === "search index staged run");
    return { value, lines };
  } finally {
    process.stdout.write = stdout;
    process.stderr.write = stderr;
  }
}

describe("G2 fast-fail: a cached size refusal skips the claim and the source read (StagedRefusals)", () => {
  let x: ExportedScope;
  beforeAll(async () => {
    x = await exportStreamScope(generate(31), 1200);
    cleanup.length = 0;
  }, 180_000);
  afterAll(() => x?.dispose());
  afterEach(() => {
    stagedFactsSendLimit.bytes = MAX_STAGE_CALL_BYTES;
  });

  /** A rig with the scope's facts reconciled whole, and a store that counts its object reads. */
  const setup = async () => {
    const r = await rig(x);
    await r.tick(FACTS);
    let reads = 0;
    const counting = new Proxy(x.store, {
      get(target, prop, receiver) {
        const v = Reflect.get(target, prop, receiver);
        return typeof v === "function" && String(prop).startsWith("get") ? (...a: unknown[]) => { reads += 1; return (v as (...b: unknown[]) => unknown).apply(target, a); } : v;
      },
    });
    const attempts = async () => (await r.hoop.pg.query<{ a: number }>("SELECT attempts a FROM sessions.search_reconcile_runs_v1")).rows.map((q) => q.a);
    const states = async () => (await r.hoop.pg.query<{ complete: boolean }>("SELECT complete FROM sessions.output_facts_session_state")).rows.map((q) => q.complete);
    const pass = async (staged: StagedReconcile, extra: Partial<SearchIndexDeps> = {}) => {
      reads = 0;
      const out = await r.pass({ ...FACTS, store: counting as never, limits: STAGED_LIMITS, stream: UNLIMITED, staged, ...extra });
      return { out, reads };
    };
    return { r, pass, attempts, states };
  };
  /** One pass abandoned by a size refusal (every chunk over the send limit). */
  const refusedPass = async (t: Awaited<ReturnType<typeof setup>>, staged: StagedReconcile, extra: Partial<SearchIndexDeps> = {}) => {
    stagedFactsSendLimit.bytes = 64;
    try {
      return await t.pass(staged, extra);
    } finally {
      stagedFactsSendLimit.bytes = MAX_STAGE_CALL_BYTES;
    }
  };

  test("the next pass of the same stamp: no claim, no source read, the abandon's result and state, one summary line marked cached_refusal", async () => {
    const t = await setup();
    const staged = stagedOn();
    const first = await refusedPass(t, staged);
    expect(first.out).toMatchObject({ complete: false, reasons: [OUTPUT_FACTS_CHUNK_UNSTAGEABLE] });
    expect(staged.refusals!.size).toBe(1);
    const claimed = await t.attempts();
    // With room again, the cached refusal still answers: nothing is claimed or read.
    const { value: hit, lines } = await summaries(() => t.pass(staged));
    expect(hit.out).toEqual({ ...first.out, ...(first.out.stream ? { stream: undefined } : {}) } as never);
    expect(hit.out.reasons).toEqual([OUTPUT_FACTS_CHUNK_UNSTAGEABLE]);
    expect(hit.out.partial).toBeUndefined();
    expect(hit.out.stream).toBeUndefined();
    expect(await t.attempts()).toEqual(claimed);
    expect(hit.reads).toBeLessThan(first.reads);
    // The search state and the facts state stay owing.
    expect(t.r.recorded.at(-1)?.complete).toBe(false);
    expect((await t.states()).every((c) => !c)).toBe(true);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ outcome: "incomplete", reason: OUTPUT_FACTS_CHUNK_UNSTAGEABLE, cached_refusal: true });
    expect(lines[0]!.attempt).toBeUndefined();
  }, 180_000);

  test("a new stamp claims and runs again; a finished run clears the entry", async () => {
    const t = await setup();
    const staged = stagedOn();
    await refusedPass(t, staged);
    const before = await t.attempts();
    await t.r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1");
    const again = await t.pass(staged);
    expect(again.out).toMatchObject({ complete: true });
    expect(again.out.stream).toBeDefined();
    expect(staged.refusals!.size).toBe(0);
    expect(before.length).toBe(1);
    expect(await t.attempts()).toEqual([]); // finished: the run row is gone
    expect((await t.states()).every((c) => c)).toBe(true);
  }, 180_000);

  test("a fresh instance (a restart) does one full pass, then caches again", async () => {
    const t = await setup();
    await refusedPass(t, stagedOn());
    const fresh = stagedOn();
    const full = await refusedPass(t, fresh);
    expect(full.out.reasons).toEqual([OUTPUT_FACTS_CHUNK_UNSTAGEABLE]);
    expect(full.out.stream).toBeDefined(); // it read
    expect(fresh.refusals!.size).toBe(1);
    const n = await t.attempts();
    expect((await t.pass(fresh)).out.stream).toBeUndefined();
    expect(await t.attempts()).toEqual(n);
  }, 180_000);

  test("expiry: past 24 hours the next pass claims and runs again", async () => {
    const t = await setup();
    const staged = stagedOn();
    await refusedPass(t, staged);
    const day = new Date(NOW.getTime() + STAGED_REFUSAL_TTL_MS - 1);
    expect((await t.pass(staged, { now: () => day })).out.stream).toBeUndefined();
    const later = new Date(NOW.getTime() + STAGED_REFUSAL_TTL_MS);
    const run = await t.pass(staged, { now: () => later });
    expect(run.out).toMatchObject({ complete: true });
    expect(run.out.stream).toBeDefined();
    expect(staged.refusals!.size).toBe(0);
  }, 180_000);

  test("no entry for a stream budget, the attempts cap, a lost lease, a restart, or a thrown error", async () => {
    const t = await setup();
    const cases: Array<[string, StagedReconcile, Partial<SearchIndexDeps>]> = [
      ["budget", stagedOn(), { stream: { maxReadBytes: 1, maxReadEvents: 1 } }],
      ["attempts cap", stagedOn({ attemptsCap: { cap: 0, retryEvery: 1000 } }), {}],
      ["lost lease", stagedOn({ boundary: async (p) => { if (p === "want:done") await t.r.hoop.pg.exec("DELETE FROM sessions.search_reconcile_runs_v1"); } }), {}],
      ["restart", stagedOn({ boundary: async (p) => { if (p === "write:done") await t.r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1"); } }), {}],
      ["thrown", stagedOn({ releaseOnError: false, boundary: (p) => { if (p === "want:done") throw new Kill(p); } }), {}],
    ];
    const seenReasons: string[] = [];
    for (const [name, staged, extra] of cases) {
      const out = (await t.pass(staged, extra)).out;
      seenReasons.push(`${name}:${out.reasons.join(",")}`);
      expect(staged.refusals!.size, name).toBe(0);
      await t.r.hoop.pg.exec("DELETE FROM sessions.search_reconcile_runs_v1");
    }
    expect(seenReasons).toEqual([
      "budget:scope_too_large,stream_read_bytes_budget",
      "attempts cap:staged_run_failed",
      "lost lease:staged_lease_lost",
      "restart:stream_source_changed",
      "thrown:staged_run_interrupted",
    ]);
  }, 180_000);

  test("StagedRefusals: a new stamp replaces the entry, expiry drops it, the bound evicts the oldest", () => {
    const c = new StagedRefusals(2, 1000);
    c.set("a", "s1", "r", 0);
    expect(c.hit("a", "s1", 999)).toBe("r");
    expect(c.hit("a", "s2", 999)).toBeNull(); // another stamp (epoch or manifest fingerprint): dropped
    expect(c.size).toBe(0);
    c.set("a", "s1", "r", 0);
    expect(c.hit("a", "s1", 1000)).toBeNull(); // expired
    c.set("a", "s1", "r", 0);
    c.set("b", "s1", "r", 1);
    c.set("c", "s1", "r", 2); // over the bound: "a" goes
    expect([c.size, c.hit("a", "s1", 3), c.hit("b", "s1", 3), c.hit("c", "s1", 3)]).toEqual([2, null, "r", "r"]);
    c.set("b", "s1", "r", 4); // re-set moves it to the newest
    c.set("d", "s1", "r", 5);
    expect([c.hit("c", "s1", 6), c.hit("b", "s1", 6)]).toEqual([null, "r"]);
    c.clear("b");
    expect(c.hit("b", "s1", 6)).toBeNull();
    expect([STAGED_REFUSAL_TTL_MS, STAGED_REFUSAL_MAX_ENTRIES]).toEqual([24 * 60 * 60 * 1000, 10_000]);
  });
});

describe("the facts pass's record bytes bound (STAGED_FACTS_RECORD_BYTES_CAP, wide text weighted)", () => {
  // Scopes with 30 legacy turns on the exporter-owned legacy machine (legacy-provenance.ts admits them only there):
  // ASCII turns, turns each holding one character above U+00FF, and half of each.
  type Shape = "ascii" | "wide" | "mixed";
  const scopes = new Map<Shape, { x: ExportedScope; counted: number; weighted: number }>();
  beforeAll(async () => {
    const machine = legacyMachineId(SCOPE.workspaceId, USER);
    const payload = { org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, source: SCOPE.harness, session_id: SCOPE.sessionId };
    for (const shape of ["ascii", "wide", "mixed"] as const) {
      const legacy: Line[] = [{ machine, userId: USER, record: { kind: "legacy_scope", origin_id: "legacy:scope", record_id: "legacy:scope:coverage", revision: "1", operation: "upsert",
        received_at: "2026-11-01T00:00:00.000000Z", payload: { ...payload, version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] } } }];
      for (let i = 0; i < 30; i++) {
        const wide = shape === "wide" || (shape === "mixed" && i % 2 === 0);
        legacy.push({ machine, userId: USER, record: { kind: "legacy_conversation_turn", origin_id: "legacy:conversation_turns", record_id: `legacy:conversation_turns:t-${i}`, revision: "1",
          operation: "upsert", received_at: "2026-11-01T00:00:00.000000Z",
          payload: { ...payload, id: `t-${i}`, turn_index: i, role: i % 2 ? "assistant" : "user", session_owner: USER, actor_name: "a", actor_role: "owner",
            ts: new Date(Date.parse("2026-11-01T09:00:00Z") + i * 1000).toISOString(), content: (wide ? "\u2014 " : "") + `legacy turn ${i} `.repeat(20) } } });
      }
      const x = await exportStreamScope([...generate(31), ...legacy], 1200);
      cleanup.length = 0;
      // What the facts pass stages as record items: each record's JSON text; wide ones weigh STAGED_FACTS_WIDE_TEXT_WEIGHT.
      let counted = 0;
      let weighted = 0;
      for (const l of legacy) {
        const text = JSON.stringify(l.record);
        const bytes = Buffer.byteLength(text, "utf8");
        counted += bytes;
        weighted += /[^\u0000-\u00ff]/.test(text) ? bytes * STAGED_FACTS_WIDE_TEXT_WEIGHT : bytes;
      }
      scopes.set(shape, { x, counted, weighted: Math.ceil(weighted) });
    }
  }, 300_000);
  afterAll(() => {
    for (const s of scopes.values()) s.x.dispose();
  });
  const withCap = (cap: number): StagedReconcile => ({ ...stagedOn(), factsRecordBytesCap: cap });
  const run = async (shape: Shape, cap: number, facts = true) => {
    const r = await rig(scopes.get(shape)!.x);
    await r.tick({});
    const out = await r.pass({ ...(facts ? FACTS : {}), limits: STAGED_LIMITS, stream: UNLIMITED, staged: withCap(cap) });
    return { r, out };
  };
  const reconciled = async ({ r, out }: Awaited<ReturnType<typeof run>>) => {
    expect(out).toMatchObject({ complete: true });
    expect(out.outputFacts).toMatchObject({ complete: true });
    expect((await r.facts()).facts.some((row) => JSON.parse(row).fact_kind === "legacy_turn_message")).toBe(true);
  };
  const refused = async ({ r, out }: Awaited<ReturnType<typeof run>>) => {
    expect(out).toMatchObject({ complete: false, reasons: [OUTPUT_FACTS_CHUNK_UNSTAGEABLE] });
    expect(out.outputFacts).toBeUndefined();
    expect((await r.facts()).events).toEqual([]);
    // Abandoned by name, not retried in the pass: the run row stays, its facts stage is gone.
    expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1"), await r.count("staged_facts_v1")]).toEqual([1, 0, 0]);
  };

  for (const shape of ["ascii", "wide", "mixed"] as const) {
    test(`${shape}: exactly at the weighted bound the run reconciles (legacy facts included)`, async () => {
      await reconciled(await run(shape, scopes.get(shape)!.weighted));
    }, 180_000);
    test(`${shape}: one byte under it the run abandons as output_facts_chunk_unstageable, nothing reconciled`, async () => {
      await refused(await run(shape, scopes.get(shape)!.weighted - 1));
    }, 180_000);
  }

  test("the same bytes that pass as ASCII are refused when each record holds one wide character", async () => {
    const ascii = scopes.get("ascii")!;
    const wide = scopes.get("wide")!;
    const cap = wide.counted; // at least the ASCII records' bytes (each wide record is two bytes longer)
    expect(ascii.counted).toBeLessThanOrEqual(cap);
    expect(wide.weighted).toBeGreaterThan(cap);
    await reconciled(await run("ascii", cap));
    await refused(await run("wide", cap));
  }, 180_000);

  test("wide text: the facts pass counts its chunks in UTF-8 bytes (three per em dash), every chunk within the bound", async () => {
    const wide = scopes.get("wide")!.x;
    expect(await checkFactsChunks(wide)).toBeGreaterThan(30);
    // A wide record's size is its UTF-8 bytes, more than its character count (the bound a character count
    // would apply is looser by two bytes per em dash).
    const one = (await factsChunksOf(wide, 1_000_000, Number.MAX_SAFE_INTEGER))[0]!;
    const wideRecords = one.inputs.map((text, i) => ({ text, bytes: one.sizes[i]! })).filter((v) => v.text.includes("\u2014"));
    expect(wideRecords.length).toBe(30);
    for (const v of wideRecords) {
      const chars = JSON.stringify(JSON.parse(v.text)).length;
      expect(v.bytes).toBe(chars + 2);
    }
  }, 180_000);

  test("a mixed run sums both weights", () => {
    const { counted, weighted } = scopes.get("mixed")!;
    expect(weighted).toBeGreaterThan(counted);
    expect(weighted).toBeLessThan(counted * STAGED_FACTS_WIDE_TEXT_WEIGHT);
  });

  test("facts off (no facts store): the bound does not apply", async () => {
    const { out } = await run("wide", 1, false);
    expect(out).toMatchObject({ complete: true });
    expect(out.outputFacts).toBeUndefined();
  }, 180_000);

  test("the production bound, the weight and their model keep a run at both bounds under 2.8 GB", () => {
    expect(STAGED_FACTS_RECORD_BYTES_CAP).toBe(512 * 1024 * 1024);
    expect(STAGED_FACTS_WIDE_TEXT_WEIGHT).toBe(2.5);
    const atCap = 1.61e9; // 500000 keyed records, short ASCII turns aside
    expect(atCap + 2.09 * STAGED_FACTS_RECORD_BYTES_CAP).toBeLessThanOrEqual(2.8e9); // ASCII, about 2.73 GB
    expect(atCap + 4.6 * (STAGED_FACTS_RECORD_BYTES_CAP / STAGED_FACTS_WIDE_TEXT_WEIGHT)).toBeLessThanOrEqual(2.8e9); // wide, about 2.60 GB
  });
});

describe("startup with output facts and the staged reconcile", () => {
  const FACTS_ENV = { RELAYHISTORY_SELFHOST_OUTPUT_FACTS: "true" };
  test("staged flag off: every streaming answer and notice is today's, byte for byte", () => {
    for (const staged of [{}, { SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "" }, { SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "false" }]) {
      const env = { ...FACTS_ENV, ...staged };
      expect(searchIndexStagedOn(env)).toBe(false);
      expect(searchIndexStreamStartup(env, {}, true)).toEqual({
        stream: undefined,
        notice: {
          level: "info",
          message: "pipeline consumer streaming reconcile off: RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true builds facts from a whole read",
          fields: { streaming: "off", reason: "output_facts" },
        },
      });
      expect(() => searchIndexStreamStartup({ ...env, SEARCH_INDEX_STREAMING_ENABLED: "true" }, {}, true)).toThrow(
        new Error("pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true"),
      );
      expect(searchIndexStagedStartup(env, undefined, true)).toBeUndefined();
    }
  });
  test("staged flag on: output facts no longer hold streaming off; the staged run starts; invalid values still fail", () => {
    const env = { ...FACTS_ENV, SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "true" };
    expect(searchIndexStagedOn(env)).toBe(true);
    const { stream, notice } = searchIndexStreamStartup(env, {}, true);
    expect(stream).toBeDefined();
    expect(notice).toBeUndefined();
    expect(searchIndexStreamStartup({ ...env, SEARCH_INDEX_STREAMING_ENABLED: "true" }, {}, true).stream).toBeDefined();
    expect(searchIndexStagedStartup(env, stream, true)?.staged).toBeDefined();
    // Explicit streaming off still refuses the staged flag, facts or not.
    const off = searchIndexStreamStartup({ ...env, SEARCH_INDEX_STREAMING_ENABLED: "false" }, {}, true);
    expect(() => searchIndexStagedStartup(env, off.stream, true)).toThrow(/requires the streaming reconcile on/);
    for (const bad of ["TRUE", "1", " true"]) {
      expect(searchIndexStagedOn({ SEARCH_INDEX_STAGED_RECONCILE_ENABLED: bad })).toBe(false);
      expect(() => searchIndexStagedStartup({ SEARCH_INDEX_STAGED_RECONCILE_ENABLED: bad }, stream, true)).toThrow(/must be true or false/);
    }
  });
  test("a facts chunk always fits one staged call: the run's chunk is clamped to MAX_FACTS_CHUNK_INPUTS", () => {
    expect(STAGED_FACTS_CHUNK).toBeLessThanOrEqual(MAX_FACTS_CHUNK_INPUTS);
    expect(MAX_FACTS_CHUNK_INPUTS * OUTPUT_FACTS_MAX_ITEMS_PER_INPUT).toBeLessThanOrEqual(100_000);
  });
  test("unstageable: the database's size refusals and the local ones are named and not retryable; anything else is itself", () => {
    for (const e of [Object.assign(new Error("x"), { code: "54000" }), Object.assign(new Error("x"), { code: "53200" }), new RangeError("Invalid string length"), new Error("output_facts_chunk_too_large")])
      expect(unstageable(e)).toMatchObject({ code: OUTPUT_FACTS_CHUNK_UNSTAGEABLE, retryable: false });
    for (const e of [Object.assign(new Error("x"), { code: "RH062" }), Object.assign(new Error("x"), { code: "08006" }), new Error("other")]) expect(unstageable(e)).toBeNull();
    // The detail names the bound with counts and codes only.
    expect(unstageable(Object.assign(new Error("secret content"), { code: "54000" }), { seq: 3, items: 120 })?.message).toBe(
      "staged output facts: item size bound: chunk 3, 120 items, database 54000");
    expect(unstageable(new Error("output_facts_chunk_too_large"), { seq: 0, items: 7 })?.message).toMatch(/^staged output facts: item size bound: chunk 0, 7 items, send limit \d+ bytes$/);
  });
  test("startup refuses a database without the 062140 writers, naming the migration", async () => {
    const missing = { query: async () => ({ rows: [{ fn: STAGED_OUTPUT_FACTS_FUNCTIONS[0] }] }) } as unknown as SqlExecutor;
    await expect(assertStagedOutputFactsSchema(missing)).rejects.toThrow(/20261105062140_staged_output_facts\.sql/);
    const present = { query: async () => ({ rows: [] }) } as unknown as SqlExecutor;
    await assertStagedOutputFactsSchema(present);
  });
});

describe("Azure-shaped self-host consumer with output facts and the staged reconcile (in-process, no Azure account)", () => {
  const KEYS = [
    "LOCKER_PROVIDER", "LOCKER_FS_ROOT", "LOCKER_BUCKET", "LOCKER_ENDPOINT", "LOCKER_ACCESS_KEY_ID", "LOCKER_SECRET_ACCESS_KEY",
    "LOCKER_AZURE_ACCOUNT", "LOCKER_AZURE_ACCOUNT_KEY", "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER", "RELAYHISTORY_SELFHOST_INGEST",
    "RELAYHISTORY_SELFHOST_READ_API", "PIPELINE_ORG_FROM_PREFIX", "PIPELINE_ORG_ID", "PIPELINE_KEY_PREFIX", "TENANT_ORG_ID",
    "AWS_LAMBDA_FUNCTION_NAME", "SEARCH_INDEX_BACKEND", "RELAYHISTORY_PIPELINE_CONSUMER_SECRET", "SESSION_POSTGRES_TLS_PROFILE",
    "SEARCH_INDEX_POSTGRES_BACKEND_ID", "RELAYHISTORY_SELFHOST_OUTPUT_FACTS", "SEARCH_INDEX_STAGED_RECONCILE_ENABLED", "SEARCH_INDEX_STREAMING_ENABLED", "READ_API_COMPLETE_SINCE",
  ] as const;
  const saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  });
  test("LOCKER_PROVIDER=azure, RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true, SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true: serve()'s startup chain to the search deps", () => {
    const env: Record<string, string> = {
      LOCKER_PROVIDER: "azure", LOCKER_BUCKET: "sessions", LOCKER_AZURE_ACCOUNT: "devstoreaccount1",
      LOCKER_AZURE_ACCOUNT_KEY: Buffer.from("synthetic-azure-account-key").toString("base64"),
      RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true", PIPELINE_ORG_FROM_PREFIX: "true",
      RELAYHISTORY_SELFHOST_OUTPUT_FACTS: "true", SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "true",
      RELAYHISTORY_PIPELINE_CONSUMER_SECRET: JSON.stringify({ hoopdb_url: "postgres://consumer@db.invalid:5432/hoop" }),
      SESSION_POSTGRES_TLS_PROFILE: "custom-ca", SEARCH_INDEX_BACKEND: "postgres", SEARCH_INDEX_POSTGRES_BACKEND_ID: "pg-1",
      PIPELINE_KEY_PREFIX: "96bulls", READ_API_COMPLETE_SINCE: "epoch",
    };
    for (const k of KEYS) delete process.env[k];
    Object.assign(process.env, env);
    // serve(): the config, then streaming and the staged run, before any connection.
    const config = consumerServiceConfig(process.env);
    expect(config).toMatchObject({ enabled: true, outputFacts: true });
    if (!config.enabled) throw new Error("unreachable");
    const { stream } = searchIndexStreamStartup(process.env, {}, config.outputFacts);
    const staged = searchIndexStagedStartup(process.env, stream, config.outputFacts);
    expect(stream).toBeDefined();
    expect(staged?.staged).toBeDefined();
    expect(outputFactsDeps(config).outputFactsStore).toBe(postgresOutputFactsStore);
    // runWorker past the vendored bindings with the consumer's own Azure Locker store.
    validateAdapterEnv(process.env, workerAdapterEntry(CONSUMER_WORKER));
    const wenv = workerBindings(CONSUMER_WORKER);
    expect(() => pipelineQueueDeps(wenv as Parameters<typeof pipelineQueueDeps>[0], {
      queue: { backend: "postgres", send: async () => {}, sendBatch: async () => {} } as never,
      deadLetters: { quarantine: async () => {} } as never, sink: { emit: async () => {} } as never, handlers: {},
    })).not.toThrow();
    const raw = buildLockerStore(process.env);
    expect([raw.provider, raw.bucket, raw.store.backend]).toEqual(["azure", "sessions", "azure"]);
  });
});
