/**
 * SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true through the consumer's wiring (Phase 3 slice 2a part 2): the
 * consumer's event path (processBatch, the pointer stage then indexSearchScope) on a migrated PGlite with
 * migration 062110, the registered Postgres search adapter in its own database, and the search deps serve()
 * builds (staged from searchIndexStagedStartup). A scope the whole read refuses as too large runs as a staged
 * run; this proves it ends equal to the whole read with unlimited limits (index rows, directory row, state),
 * resumes on the next tick after a kill at every phase boundary, restarts after a source change, is erased with
 * its scope, answers a second consumer without the queue's busy path, and keeps the slice 1 guarantees (paged
 * scans, per-pass budgets, the keyed-records cap, a malformed scan page refused before any write).
 *
 * The staging functions refuse any caller but search_consumer_runtime: the test transaction takes that role
 * around their calls only (the role's full grants are proven by staged-reconcile.pglite.test.ts and the
 * consumer-service writer-role suite). Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { positionOf, type SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout } from "../pointer.ts";
import type { SearchProjectionState, SearchServiceBinding } from "../search-adapter.ts";
import { indexSearchScope, searchIndexStagedStartup, type SearchIndexDeps, type SearchIndexResult, type StagedReconcile } from "../search-index.ts";
import { STAGED_RECONCILE_FUNCTIONS, StagedRunRestart, assertStagedReconcileSchema, type StagedBoundary } from "../staged-reconcile.ts";
import { SCOPE, decompress, exportStreamScope, generate, prefixFor, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { readCanonicalScope } from "../../session-read-api/canonical-projection.ts";
import { outputFactsFromScope } from "../output-facts.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const RUNTIME = "search_consumer_runtime";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

class Kill extends Error {}

/** serve()'s staged deps for the flag on, with the test-only batch sizes and boundary seam. */
function stagedOn(run: StagedReconcile["run"] = {}): StagedReconcile {
  const started = searchIndexStagedStartup({ SEARCH_INDEX_STAGED_RECONCILE_ENABLED: "true" }, UNLIMITED, false);
  expect(started).toBeDefined();
  return { ...started!.staged, run: { stageBatch: 7, writeKeys: 5, ...run } };
}

/** One consumer: a migrated HoopDb clone and a search database with the registered Postgres adapter. */
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
  const recorded: Array<{ complete: boolean; rows: number }> = [];
  const state: SearchProjectionState = {
    fromMs: async () => 0,
    record: async (_tx, _scope, value) => {
      recorded.push({ complete: value.complete, rows: (value as { rows?: number }).rows ?? -1 });
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
          if (!sql.includes("sessions.search_reconcile_")) return tx.query(sql, params) as never;
          await tx.exec(`SET LOCAL ROLE ${RUNTIME}`);
          const result = await tx.query(sql, params); // a failure aborts the transaction: no RESET then
          await tx.exec("RESET ROLE");
          return result as never;
        },
      }),
    ) as Promise<T>;
  const requeued: string[] = [];
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
    requeue: async (key: string) => {
      requeued.push(key);
    },
  });
  const messages = x.keys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  /** One consumer pass over the scope's manifests (what a tick sends). */
  const tick = async (search: Partial<SearchIndexDeps>) => processBatch(messages, consumer(search));
  /** The search pass alone, for its result (the pointer stage already ran). */
  const pass = (search: Partial<SearchIndexDeps>): Promise<SearchIndexResult> => indexSearchScope(SCOPE, { ...consumer(search).search } as SearchIndexDeps);
  const tenantOf = { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId };
  const scan = async () =>
    (await adapter.index.scanFingerprints({ ...tenantOf, source: SCOPE.harness, sessionIds: [SCOPE.sessionId, x.canonicalId] }, { fromMs: 0, limit: 100_000 })).rows
      .map((r) => JSON.stringify([r.sessionId, r.machineId, r.kind, r.eventId, r.tsMs, r.fingerprint]))
      .sort();
  /** The directory row of the scope, without its clock columns. */
  const directory = async () =>
    (await hoop.pg.query<{ d: Record<string, unknown> }>(
      "SELECT to_jsonb(d) AS d FROM sessions.session_directory d WHERE org_id=$1 AND workspace_id=$2 AND harness=$3", [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness])).rows
      .map((r) => Object.fromEntries(Object.entries(r.d).filter(([k]) => !/(_at|_epoch)$/.test(k))));
  const runs = async () => (await hoop.pg.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_runs_v1")).rows[0]!.n;
  const staged = async () => (await hoop.pg.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_stage_v1")).rows[0]!.n;
  const expire = () => hoop.pg.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
  /** Index rows of a regenerable kind the source does not hold: a complete pass deletes them. */
  const addStale = async (n: number) => {
    const docs = (await adapter.index.query({ tenant: tenantOf, order: "asc", limit: 1 })).hits;
    const first = docs[0]!;
    await adapter.index.upsert(Array.from({ length: n }, (_, i) => ({
      ...first, eventId: `${first.eventId}-stale-${i}`, fingerprint: "stale", version: NOW.getTime() - 1, taskRefId: "", deliveryRecordId: "",
    })) as never);
  };
  /** Drop `n` of the scope's index rows: a pass must regenerate them (pass 4 writes). */
  const dropSome = async (n: number) => {
    const hits = (await adapter.index.query({ tenant: tenantOf, order: "asc", limit: n })).hits;
    await adapter.index.deleteEvents(tenantOf, hits.map((h) => ({ tsMs: h.tsMs, sessionId: h.sessionId, source: h.source, machineId: h.machineId, kind: h.kind, eventId: h.eventId })));
  };
  return { hoop, adapter, tick, pass, transact, scan, directory, runs, staged, expire, addStale, dropSome, recorded, requeued };
}

/** The whole read with unlimited limits: the reference every staged run must end equal to. */
async function reference(x: ExportedScope) {
  const r = await rig(x);
  await r.tick({});
  expect((await r.pass({})).complete).toBe(true);
  return { scan: await r.scan(), directory: await r.directory() };
}

/** A staged consumer whose index first held the whole read's rows plus `stale` rows a pass must delete. */
async function seeded(x: ExportedScope, stale = 5) {
  const r = await rig(x);
  await r.tick({});
  await r.dropSome(DROPPED);
  await r.addStale(stale);
  return r;
}
const STAGED_LIMITS = { ...TINY, insertBatch: 4 };
/** Index rows `seeded` drops (pass 4 regenerates them) before adding the stale rows a pass deletes. */
const DROPPED = 12;

describe("staged reconcile through the consumer wiring", () => {
  let x: ExportedScope;
  let ref: Awaited<ReturnType<typeof reference>>;
  beforeAll(async () => {
    x = await exportStreamScope(generate(31), 1200);
    cleanup.length = 0; // the exported scope outlives the per-test cleanup
    ref = await reference(x);
  }, 120_000);
  afterAll(() => x?.dispose());

  test("flag on: a too-large scope runs staged, ends equal to the whole read, and leaves no run behind", async () => {
    const r = await seeded(x);
    const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(out).toMatchObject({ complete: true, inserted: DROPPED, deleted: 5, reasons: [] });
    expect(out.stream?.passes).toBeGreaterThanOrEqual(3);
    expect(await r.scan()).toEqual(ref.scan);
    expect(await r.directory()).toEqual(ref.directory);
    // The claim recorded the scope owing a pass before any work; the final step recorded it complete.
    expect(r.recorded.at(-2)).toEqual({ complete: false, rows: 0 });
    expect(r.recorded.at(-1)?.complete).toBe(true);
    expect([await r.runs(), await r.staged()]).toEqual([0, 0]);
    // Up to date: the next pass writes and deletes nothing.
    expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() })).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
  }, 120_000);

  test("flag on: a scope the whole read completes never touches the staged run", async () => {
    const r = await seeded(x);
    const out = await r.pass({ limits: { insertBatch: 4 }, stream: UNLIMITED, staged: stagedOn() });
    expect(out).toMatchObject({ complete: true, deleted: 5 });
    expect(out.stream).toBeUndefined();
    expect(await r.runs()).toBe(0);
    expect(await r.scan()).toEqual(ref.scan);
  }, 120_000);

  const BOUNDARIES: StagedBoundary[] = ["claimed", "want:batch", "want:done", "have:batch", "have:done", "write:page", "write:done", "final:locked", "final:removed", "final:committed", "finished"];
  for (const point of BOUNDARIES)
    test(`kill at ${point}: the tick resumes the run after the lease expires and ends equal to the whole read`, async () => {
      const r = await seeded(x);
      let killed = false;
      const kill = (p: StagedBoundary) => {
        if (p === point && !killed) {
          killed = true;
          throw new Kill(p);
        }
      };
      // A crash: the lease is kept (releaseOnError off), so only its expiry frees the run.
      const first = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ boundary: kill, releaseOnError: false }) });
      expect(killed).toBe(true);
      if (point === "finished") {
        // Past the final commit: the run is done; the interruption still leaves the scope owing, and
        // the tick finds nothing to write.
        expect(first.reasons).toEqual(["staged_run_interrupted"]);
        expect(r.recorded.at(-1)?.complete).toBe(false);
        expect(await r.runs()).toBe(0);
      } else {
        // Interrupted after the claim: no throw (no queue retry), the scope stays owing a pass for the tick.
        expect(first).toMatchObject({ complete: false, partial: true, reasons: ["staged_run_interrupted"] });
        expect(r.recorded.at(-1)?.complete).toBe(false);
        // A second consumer before the lease expires: answered without the queue's busy path.
        const second = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
        expect(second).toMatchObject({ complete: false, reasons: ["staged_run_leased"] });
        expect(second.busy).toBeUndefined();
        await r.expire();
      }
      // The next tick: the consumer's event path again.
      const before = r.requeued.length;
      const response = await r.tick({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
      expect(response.batchItemFailures).toEqual([]);
      expect(r.requeued.length).toBe(before);
      expect(await r.scan()).toEqual(ref.scan);
      expect(await r.directory()).toEqual(ref.directory);
      expect(r.recorded.at(-1)?.complete).toBe(true);
      expect([await r.runs(), await r.staged()]).toEqual([0, 0]);
    }, 120_000);

  test("a source change mid-run: the final step abandons the run and the next tick converges", async () => {
    const r = await seeded(x);
    const out = await r.pass({
      limits: STAGED_LIMITS, stream: UNLIMITED,
      staged: stagedOn({
        boundary: async (p) => {
          // Between transactions: a pointer change moves the directory epoch.
          if (p === "write:done") await r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1");
        },
      }),
    });
    expect(out).toMatchObject({ complete: false, reasons: ["stream_source_changed"] });
    expect(await r.staged()).toBe(0);
    expect(r.recorded.at(-1)?.complete).toBe(false); // restart leaves the scope owing
    const again = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    expect(again).toMatchObject({ complete: true });
    expect(await r.scan()).toEqual(ref.scan);
    expect(await r.runs()).toBe(0);
  }, 120_000);

  test("erase during a run: the scope's erase drops the run and its stage rows; the interrupted holder stops", async () => {
    const r = await seeded(x);
    const out = await r.pass({
      limits: STAGED_LIMITS, stream: UNLIMITED,
      staged: stagedOn({
        boundary: async (p) => {
          if (p !== "have:done") return;
          expect(await r.staged()).toBeGreaterThan(0);
          // The session's last pointer goes; the erase pass (flag on) drops the staged run.
          await r.hoop.pg.exec(`UPDATE sessions.session_index SET deleted_at = now() WHERE org_id='${SCOPE.orgId}'`);
          expect(await r.pass({ staged: stagedOn() })).toMatchObject({ complete: true, reasons: ["erased"] });
        },
      }),
    });
    expect(out).toMatchObject({ complete: false, reasons: ["staged_lease_lost"] });
    expect([await r.runs(), await r.staged()]).toEqual([0, 0]);
    // Nothing written back after the erase, and the lost lease writes no state: the erase pass's stays.
    expect(await r.scan()).toEqual([]);
    expect(r.recorded.at(-1)?.complete).toBe(true);
  }, 120_000);

  test("T1: a whole read completing between two write pages: the next page abandons, no stale row comes back, the scope ends owing", async () => {
    const r = await seeded(x);
    let pages = 0;
    const out = await r.pass({
      limits: STAGED_LIMITS, stream: UNLIMITED,
      staged: stagedOn({
        boundary: async (p) => {
          if (p !== "write:page" || pages++ > 0) return;
          // The source moves (epoch) and a whole read of the new source reconciles the scope and records it current.
          await r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1");
          expect(await r.pass({ staged: stagedOn() })).toMatchObject({ complete: true });
        },
      }),
    });
    expect(pages).toBe(1); // the second page never wrote
    expect(out).toMatchObject({ complete: false, reasons: ["stream_source_changed"] });
    expect(await r.scan()).toEqual(ref.scan);
    expect(r.recorded.at(-1)?.complete).toBe(false);
    expect(await r.staged()).toBe(0);
  }, 120_000);

  test("T3: an interrupted run is logged (class and code, no content) and releases its lease: the next pass resumes at once", async () => {
    const r = await seeded(x);
    const index = r.adapter.index as SearchIndexAdapter;
    const page = index.scanFingerprintsPage!.bind(index);
    let once = true;
    index.scanFingerprintsPage = (async (...a: Parameters<typeof page>) => {
      const got = await page(...a);
      if (!once) return got;
      once = false;
      return { ...got, rows: [...got.rows].reverse() };
    }) as typeof page;
    const lines: string[] = [];
    const stdout = process.stdout.write, stderr = process.stderr.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        lines.push(String(chunk));
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    let out: SearchIndexResult;
    try {
      out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    expect(out).toMatchObject({ complete: false, partial: true, reasons: ["staged_run_interrupted"] });
    const logged = lines.flatMap((l) => l.split("\n")).filter((l) => l.includes("search index staged run interrupted")).map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(logged.length).toBe(1);
    expect(logged[0]).toMatchObject({ level: "warn", stage: "search-index", org_id: SCOPE.orgId, error_name: "Error" });
    expect(typeof logged[0]!.error_code).toBe("string");
    expect(JSON.stringify(logged[0])).not.toContain("search scan order invalid"); // no message text
    expect(r.recorded.at(-1)?.complete).toBe(false);
    // Released: no expiry needed; the next pass takes the run and finishes it.
    expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() })).toMatchObject({ complete: true });
    expect(await r.scan()).toEqual(ref.scan);
  }, 120_000);

  test("T4: a source read longer than the lease keeps the run: the heartbeat renews it, a second consumer is leased out", async () => {
    const r = await seeded(x);
    const slow = { ...x.store, getObject: async (...a: Parameters<typeof x.store.getObject>) => {
      await new Promise((resolve) => setTimeout(resolve, 110)); // about 10 s for passes 1-3 here
      return x.store.getObject(...a);
    } } as typeof x.store;
    // A 3 s lease renewed every 1 s; the second consumer comes at 6 s (two leases later), mid-read.
    const first = r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, store: slow, staged: stagedOn({ leaseMs: 3000 }) });
    await new Promise((resolve) => setTimeout(resolve, 6000));
    const second = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ leaseMs: 3000 }) });
    expect(second).toMatchObject({ complete: false, reasons: ["staged_run_leased"] });
    expect(await first).toMatchObject({ complete: true });
    expect(await r.scan()).toEqual(ref.scan);
  }, 120_000);

  test("N3: a transient lease renewal failure is logged and retried; the lease stays renewed", async () => {
    const r = await seeded(x);
    const slow = { ...x.store, getObject: async (...a: Parameters<typeof x.store.getObject>) => {
      await new Promise((resolve) => setTimeout(resolve, 110));
      return x.store.getObject(...a);
    } } as typeof x.store;
    // The first renewal (an advance in the want phase, before any batch) fails once with a transient error.
    let failed = 0;
    const base = r.transact;
    const flaky = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      base((tx) => work({
        query: (sql: string, params?: unknown[]) => {
          if (failed === 0 && sql.includes("search_reconcile_advance_v1") && params?.[2] === "want") {
            failed += 1;
            return Promise.reject(Object.assign(new Error("transient"), { code: "08006" }));
          }
          return tx.query(sql, params);
        },
      }));
    const lines: string[] = [];
    const stderr = process.stderr.write, stdout = process.stdout.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        lines.push(String(chunk));
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    try {
      const first = r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, store: slow, transact: flaky, staged: stagedOn({ leaseMs: 3000 }) });
      await new Promise((resolve) => setTimeout(resolve, 6000));
      // Had the renewal stopped at the failure, the lease would have expired at about 3 s.
      expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ leaseMs: 3000 }) })).toMatchObject({ reasons: ["staged_run_leased"] });
      expect(await first).toMatchObject({ complete: true });
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    expect(failed).toBe(1);
    const warned = lines.flatMap((l) => l.split("\n")).filter((l) => l.includes("search index staged lease renewal failed")).map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(warned.length).toBe(1);
    expect(warned[0]).toMatchObject({ stage: "search-index", error_code: "08006" });
    expect(JSON.stringify(warned[0])).not.toContain("transient");
  }, 120_000);

  test("N2: the taker finishes, then the old holder hits RH062: staged_lease_lost writes nothing, the state stays current", async () => {
    const r = await seeded(x);
    let took = false;
    const out = await r.pass({
      limits: STAGED_LIMITS, stream: UNLIMITED,
      staged: stagedOn({
        boundary: async (p) => {
          if (p !== "want:batch" || took) return;
          took = true;
          await r.expire();
          expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() })).toMatchObject({ complete: true });
        },
      }),
    });
    expect(out).toMatchObject({ complete: false, reasons: ["staged_lease_lost"] });
    expect(r.recorded.at(-1)?.complete).toBe(true);
    expect(await r.scan()).toEqual(ref.scan);
  }, 120_000);

  for (const [name, error, reason] of [
    ["StagedRunRestart", () => new StagedRunRestart("output_facts_stage_incomplete"), "output_facts_stage_incomplete"],
    ["RH062", () => Object.assign(new Error("stale"), { code: "RH062" }), "staged_lease_lost"],
  ] as const)
    test(`N1: a hook error on the delivery made by the facts pass's final emit (${name}) reaches the run as itself`, async () => {
      // Count the chunks of a clean pass first.
      const count = async () => {
        const r = await seeded(x);
        let n = 0;
        await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ factsChunk: 16, hooks: { async onBatch(_t: SqlExecutor, _r: unknown, b: { phase: string }) { if (b.phase === "facts") n += 1; } } as never }) });
        return n;
      };
      const n = await count();
      expect(n).toBeGreaterThanOrEqual(3);
      const r = await seeded(x);
      // The module delivers one chunk behind: the chunk before the last is delivered from the final emit.
      const hooks = { async onBatch(_t: SqlExecutor, _r: unknown, b: { phase: string; chunk?: { seq: number } }) {
        if (b.phase === "facts" && b.chunk!.seq === n - 2) throw error();
      } };
      const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ factsChunk: 16, hooks: hooks as never }) });
      expect(out.reasons).toEqual([reason]);
      if (name === "StagedRunRestart") {
        const run = (await r.hoop.pg.query<{ phase: string; last_error: string }>("SELECT phase, last_error FROM sessions.search_reconcile_runs_v1")).rows;
        expect(run).toEqual([{ phase: "want", last_error: "output_facts_stage_incomplete" }]); // abandoned
      }
    }, 120_000);

  /** A rig whose scan pages are corrupt until `heal()`, counting source reads and index writes. */
  async function failing() {
    const r = await seeded(x);
    const index = r.adapter.index as SearchIndexAdapter;
    const page = index.scanFingerprintsPage!.bind(index);
    let corrupt = true;
    index.scanFingerprintsPage = (async (...a: Parameters<typeof page>) => {
      const got = await page(...a);
      return corrupt ? { ...got, rows: [...got.rows].reverse() } : got;
    }) as typeof page;
    const counts = { reads: 0, upserts: 0, deletes: 0 };
    const upsert = index.upsert.bind(index);
    const remove = index.deleteEvents.bind(index);
    index.upsert = (async (...a: Parameters<typeof upsert>) => { counts.upserts += 1; return upsert(...a); }) as typeof upsert;
    index.deleteEvents = (async (...a: Parameters<typeof remove>) => { counts.deletes += 1; return remove(...a); }) as typeof remove;
    const store = { ...x.store, getObject: (...a: Parameters<typeof x.store.getObject>) => { counts.reads += 1; return x.store.getObject(...a); } } as typeof x.store;
    const cap = { cap: 2, retryEvery: 3 };
    const pass = () => r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, store, staged: stagedOn({ attemptsCap: cap }) });
    const tick = async () => {
      const before = { ...counts };
      const out = await pass();
      return { out, reads: counts.reads - before.reads, writes: counts.upserts - before.upserts + counts.deletes - before.deletes };
    };
    return { r, tick, heal: () => { corrupt = false; } };
  }
  /** The summary lines a block of work logs. */
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

  test("attempts cap: past it a claim fails before any read and writes nothing; every Nth claim retries; a healed scope finishes", async () => {
    const { r, tick, heal } = await failing();
    const seen: Array<{ reason: string; reads: number; writes: number }> = [];
    for (let attempt = 1; attempt <= 6; attempt++) {
      const t = await tick();
      seen.push({ reason: t.out.reasons[0]!, reads: t.reads > 0 ? 1 : 0, writes: t.writes });
      expect(r.recorded.at(-1)?.complete).toBe(false);
    }
    // cap 2, retry every 3: claims 1-2 run (2 resumes in the have phase, which reads no source), 3-4 fail,
    // 5 retries (the failure abandoned the run, so it starts over and reads), 6 fails.
    expect(seen).toEqual([
      { reason: "staged_run_interrupted", reads: 1, writes: 0 },
      { reason: "staged_run_interrupted", reads: 0, writes: 0 },
      { reason: "staged_run_failed", reads: 0, writes: 0 },
      { reason: "staged_run_failed", reads: 0, writes: 0 },
      { reason: "staged_run_interrupted", reads: 1, writes: 0 },
      { reason: "staged_run_failed", reads: 0, writes: 0 },
    ]);
    expect((await r.hoop.pg.query<{ last_error: string; attempts: number }>("SELECT last_error, attempts FROM sessions.search_reconcile_runs_v1")).rows)
      .toEqual([{ last_error: "staged_run_failed", attempts: 6 }]);
    heal();
    // Claim 7 fails (no read), claim 8 is the next slow retry: it runs and finishes; the row goes.
    const { value, lines } = await summaries(async () => [await tick(), await tick()]);
    expect(value.map((t) => [t.out.reasons[0] ?? "done", t.reads > 0])).toEqual([["staged_run_failed", false], ["done", true]]);
    expect(value[1]!.out.complete).toBe(true);
    expect(lines.map((l) => [l.level, l.outcome, l.reason ?? null, l.attempt])).toEqual([["warn", "failed", "staged_run_failed", 7], ["info", "done", null, 8]]);
    for (const l of lines) {
      expect(l).toMatchObject({ stage: "search-index", org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, harness: SCOPE.harness, session_id: SCOPE.sessionId });
      expect(typeof l.duration_ms).toBe("number");
    }
    expect(await r.runs()).toBe(0);
    expect(await r.scan()).toEqual(ref.scan);
  }, 300_000);

  test("attempts cap: a source change resets a failed run (fresh claim, attempt 1)", async () => {
    const { r, tick, heal } = await failing();
    for (let i = 0; i < 3; i++) await tick();
    expect((await r.hoop.pg.query<{ attempts: number }>("SELECT attempts FROM sessions.search_reconcile_runs_v1")).rows).toEqual([{ attempts: 3 }]);
    heal();
    await r.hoop.pg.exec("UPDATE sessions.session_directory SET event_projection_epoch = event_projection_epoch + 1");
    const { value, lines } = await summaries(() => tick());
    expect(value.reads).toBeGreaterThan(0);
    expect(value.out).toMatchObject({ complete: true });
    expect(lines.map((l) => l.attempt)).toEqual([1]);
    expect(await r.runs()).toBe(0);
  }, 300_000);

  test("operator SQL: reset lets the next tick run at once; cleanup removes old idle runs and never one under a live lease", async () => {
    const { r, tick, heal } = await failing();
    for (let i = 0; i < 3; i++) await tick();
    // List failed runs (runbook query).
    expect((await r.hoop.pg.query<{ session_id: string }>("SELECT session_id FROM sessions.search_reconcile_runs_v1 WHERE last_error = 'staged_run_failed'")).rows.length).toBe(1);
    heal();
    // Reset (runbook, owner SQL).
    await r.hoop.pg.query("DELETE FROM sessions.search_reconcile_runs_v1 WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4", [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId]);
    const after = await tick();
    expect(after.reads).toBeGreaterThan(0);
    expect(after.out).toMatchObject({ complete: true });
    // Cleanup (runbook): an idle old run goes, a run under a live lease stays.
    const CLEANUP = "DELETE FROM sessions.search_reconcile_runs_v1 WHERE (lease_nonce IS NULL OR lease_expires_at < clock_timestamp()) AND updated_at < clock_timestamp() - interval '7 days'";
    let killed = false;
    expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn({ releaseOnError: false, boundary: (p) => { if (p === "want:batch" && !killed) { killed = true; throw new Kill(p); } } }) }))
      .toMatchObject({ reasons: ["staged_run_interrupted"] });
    await r.hoop.pg.exec("UPDATE sessions.search_reconcile_runs_v1 SET updated_at = clock_timestamp() - interval '30 days'");
    await r.hoop.pg.exec(CLEANUP);
    expect(await r.runs()).toBe(1); // live lease: kept
    await r.expire();
    await r.hoop.pg.exec("UPDATE sessions.search_reconcile_runs_v1 SET updated_at = clock_timestamp() - interval '30 days'");
    await r.hoop.pg.exec(CLEANUP);
    expect([await r.runs(), await r.staged()]).toEqual([0, 0]);
  }, 300_000);

  test("A1: a deadline spent inside the facts pass: partial with scope_too_large and stream_deadline, onFinish never runs", async () => {
    const r = await seeded(x);
    const deadline = { at: Number.MAX_SAFE_INTEGER };
    let finished = 0;
    const hooks = {
      async onBatch(_tx: SqlExecutor, _run: unknown, batch: { phase: string; chunk?: { seq: number } }) {
        if (batch.phase === "facts" && batch.chunk!.seq === 0) deadline.at = 0; // spent from here on
      },
      async onFinish() {
        finished += 1;
      },
    };
    const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, streamDeadline: deadline, staged: stagedOn({ factsChunk: 4, hooks: hooks as never }) });
    expect(out).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", "stream_deadline"] });
    expect(finished).toBe(0);
    expect(r.recorded.at(-1)?.complete).toBe(false);
  }, 120_000);

  test("slice 1 kept: the index is walked in keyset pages, never one bounded read", async () => {
    const r = await seeded(x);
    const index = r.adapter.index as SearchIndexAdapter;
    const counts = { pages: 0, bounded: 0 };
    const page = index.scanFingerprintsPage!.bind(index);
    const bounded = index.scanFingerprints.bind(index);
    index.scanFingerprintsPage = (async (...a: Parameters<typeof page>) => { counts.pages += 1; return page(...a); }) as typeof page;
    index.scanFingerprints = (async (...a: Parameters<typeof bounded>) => { counts.bounded += 1; return bounded(...a); }) as typeof bounded;
    expect(await r.pass({ limits: { ...STAGED_LIMITS, maxRows: 3 }, stream: UNLIMITED, staged: stagedOn() })).toMatchObject({ complete: true, deleted: 5 });
    expect(counts.bounded).toBe(0);
    expect(counts.pages).toBe(Math.ceil((ref.scan.length - DROPPED + 5) / 4));
  }, 120_000);

  test("slice 1 kept: the read budgets bound each pass, so a scope past a quarter of a budget still runs", async () => {
    const r = await seeded(x);
    // One pass reads about 29 KB here: summed over the passes this budget refuses, per pass it does not.
    expect(await r.pass({ limits: STAGED_LIMITS, stream: { maxReadBytes: 40_000, maxReadEvents: 1 << 24 }, staged: stagedOn() })).toMatchObject({ complete: true });
    expect(await r.scan()).toEqual(ref.scan);
  }, 120_000);

  test("slice 1 kept: past the keyed-records cap the scope stays partial, nothing written or deleted", async () => {
    const r = await seeded(x);
    const before = await r.scan();
    const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: { ...stagedOn(), keyedRecordsCap: 2 } });
    expect(out).toMatchObject({ complete: false, partial: true, inserted: 0, deleted: 0, reasons: ["scope_too_large", "stream_keyed_records_cap"] });
    expect(await r.scan()).toEqual(before);
    expect(r.recorded.at(-1)?.complete).toBe(false);
  }, 120_000);

  test("slice 1 kept: a malformed scan page is refused before any write or delete", async () => {
    type Page = NonNullable<SearchIndexAdapter["scanFingerprintsPage"]>;
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
    for (const [name, wrap] of corrupt) {
      const r = await seeded(x);
      const before = await r.scan();
      const index = r.adapter.index as SearchIndexAdapter;
      index.scanFingerprintsPage = wrap(index.scanFingerprintsPage!.bind(index));
      const writes = { upserts: 0, deletes: 0 };
      const upsert = index.upsert.bind(index);
      const remove = index.deleteEvents.bind(index);
      index.upsert = (async (...a: Parameters<typeof upsert>) => { writes.upserts += 1; return upsert(...a); }) as typeof upsert;
      index.deleteEvents = (async (...a: Parameters<typeof remove>) => { writes.deletes += 1; return remove(...a); }) as typeof remove;
      const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn() });
      expect({ name, reasons: out.reasons, complete: out.complete }).toEqual({ name, reasons: ["staged_run_interrupted"], complete: false });
      expect({ name, ...writes }).toEqual({ name, upserts: 0, deletes: 0 });
      expect(await r.scan()).toEqual(before);
      expect(r.recorded.at(-1)?.complete).toBe(false);
    }
  }, 300_000);

  test("facts pass (output-facts hooks): killed mid-pass and resumed, the chunks give the whole read's facts exactly", async () => {
    const r = await seeded(x);
    type Chunk = { seq: number; last: boolean };
    type Events = { projectionRows: Array<{ table: string; row: Record<string, unknown> }>; records: RelayhistoryStreamRecord[] };
    let staged: { chunks: Chunk[]; events: Events[]; sealed: number } = { chunks: [], events: [], sealed: 0 };
    const finished: Array<typeof staged> = [];
    let killAt: number | undefined = 1;
    const hooks = {
      async onBatch(_tx: SqlExecutor, _run: unknown, batch: { phase: "facts"; events: Events; chunk: Chunk } | { phase: "write" }) {
        if (batch.phase !== "facts") return;
        if (batch.chunk.seq === 0) staged = { chunks: [], events: [], sealed: 0 };
        staged.chunks.push(batch.chunk);
        staged.events.push(batch.events);
        if (batch.chunk.seq === killAt) {
          killAt = undefined;
          throw new Kill("facts");
        }
      },
      async onWantDone() {
        staged.sealed += 1;
      },
      async onFinish(_tx: SqlExecutor, _run: unknown, pass: { complete: boolean; sourceComplete: boolean }) {
        expect(pass).toMatchObject({ complete: true, sourceComplete: true });
        finished.push(staged);
      },
    };
    const run = { factsChunk: 16, hooks };
    expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn(run) })).toMatchObject({ complete: false, reasons: ["staged_run_interrupted"] });
    await r.expire();
    expect(await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged: stagedOn(run) })).toMatchObject({ complete: true });
    expect(finished.length).toBe(1);
    const done = finished[0]!;
    expect(done.chunks.map((c) => c.seq)).toEqual([...done.chunks.keys()]);
    expect(done.chunks.map((c) => c.last)).toEqual(done.chunks.map((_, i) => i === done.chunks.length - 1));
    expect(done.sealed).toBe(1);
    expect(done.chunks.length).toBeGreaterThan(2);
    // The whole read of the same source, with unlimited limits, and the facts each input builds.
    const whole = await readCanonicalScope(SCOPE, x.keys, { reader: x.reader, store: x.store, prefixFor, decompress } as never,
      { maxScopeBytes: 1 << 30, maxScopeEvents: 1 << 24, maxRows: 1 << 24, insertBatch: 5000 }, NOW.getTime(), x.canonicalId);
    expect(whole.complete).toBe(true);
    const fScope = { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, harness: SCOPE.harness, sessionIds: [SCOPE.sessionId, x.canonicalId], canonicalId: x.canonicalId };
    const stagedInput = {
      projectionRows: done.events.flatMap((e) => e.projectionRows),
      records: done.events.flatMap((e) => e.records),
      complete: true,
    };
    expect(stagedInput.projectionRows.length).toBe(whole.projectionRows.length);
    expect(stagedInput.records.length).toBe(whole.records.length);
    const sorted = (rows: Array<{ row_id: string }>) => [...rows].sort((a, b) => (a.row_id < b.row_id ? -1 : 1));
    const a = outputFactsFromScope(fScope, stagedInput as never, 7);
    const b = outputFactsFromScope(fScope, { projectionRows: whole.projectionRows, records: whole.records, complete: true } as never, 7);
    expect(a.events.length).toBeGreaterThan(0);
    expect({ ...a, events: sorted(a.events), facts: sorted(a.facts) }).toEqual({ ...b, events: sorted(b.events), facts: sorted(b.facts) });
  }, 120_000);

  test("flag on with the 062110 functions missing: startup refuses, naming the migration", async () => {
    const r = await rig(x);
    await expect(assertStagedReconcileSchema(r.hoop.db as never)).resolves.toBeUndefined();
    await r.hoop.pg.exec(`DROP FUNCTION ${STAGED_RECONCILE_FUNCTIONS[2]}`);
    await expect(assertStagedReconcileSchema(r.hoop.db as never)).rejects.toThrow("20261105062110_search_reconcile_staging.sql");
  }, 120_000);
});
