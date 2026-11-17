/**
 * SEARCH_INDEX_STAGED_RECONCILE_ENABLED unset must leave the search pass byte-identical to
 * the base commit (Phase 3 slice 1). The consumer's event path (processBatch) runs the
 * search-index-stream / consumer-large fixtures through a recording search transaction
 * (SQL text and parameters), a recording adapter (every index and projection-state call
 * with its arguments) and the consumer's log records, and compares them to a golden
 * captured from the base commit, before the staged reconcile existed.
 *
 * Regenerate (only on purpose, from the base commit):
 *   UPDATE_STAGED_OFF_GOLDEN=1 bun test ./src/session-pipeline/__tests__/search-index-staged-off-golden.test.ts
 *
 * Synthetic, secret-free data only.
 */
import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout } from "../pointer.ts";
import type { SearchProjectionState, SearchServiceBinding } from "../search-adapter.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { exportStreamScope, generate, prefixFor } from "../../session-read-api/__tests__/stream-fixture.ts";
import { clonedReplayDatabase, closeReplayTemplate } from "./replay-fixture.ts";

const GOLDEN = join(import.meta.dir, "fixtures", "search-index-staged-off.golden.json");
const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});

type Entry = Record<string, unknown>;

/** JSON-stable form of a call argument: Dates as ISO strings, functions dropped. */
const plain = (value: unknown): unknown => JSON.parse(JSON.stringify(value ?? null));

/** Every index method call (name and arguments), in order; the real adapter answers. */
function recordingIndex(index: SearchIndexAdapter, out: Entry[]): SearchIndexAdapter {
  const methods = ["ensureSchema", "upsert", "deleteEvents", "eraseScope", "scanFingerprints", "query", "health"] as const;
  const wrapped: Record<string, unknown> = { id: index.id, backend: index.backend, capabilities: () => index.capabilities() };
  for (const name of methods)
    wrapped[name] = async (...args: unknown[]) => {
      out.push({ adapter: name, args: plain(args.filter((a) => a !== undefined)) });
      return (index[name] as (...a: unknown[]) => Promise<unknown>)(...args);
    };
  // Any other method the adapter grows (a later slice's paged scan) is recorded too: a flag-off
  // pass that calls one shows up as a golden mismatch.
  for (const name of Object.keys(index) as Array<keyof SearchIndexAdapter>)
    if (!(name in wrapped) && typeof index[name] === "function")
      wrapped[name] = async (...args: unknown[]) => {
        out.push({ adapter: name, args: plain(args.filter((a) => a !== undefined)) });
        return (index[name] as (...a: unknown[]) => Promise<unknown>)(...args);
      };
  return wrapped as unknown as SearchIndexAdapter;
}

interface Scenario {
  name: string;
  /** The search deps of each consumer run, in order (one processBatch per entry). */
  runs: Array<Partial<SearchIndexDeps>>;
  seed: number;
}

const SCENARIOS: Scenario[] = [
  { name: "whole read: complete, then up to date", seed: 41, runs: [{}, {}] },
  { name: "whole read: index rows past SEARCH_INDEX_MAX_ROWS are search_index_truncated", seed: 42, runs: [{}, { limits: { maxRows: 3 } }] },
  { name: "streamed: complete, then up to date", seed: 43, runs: [{ limits: TINY, stream: UNLIMITED }, { limits: TINY, stream: UNLIMITED }] },
  {
    name: "streamed: index rows past SEARCH_INDEX_MAX_ROWS are search_index_truncated",
    seed: 44,
    runs: [{ limits: TINY, stream: UNLIMITED }, { limits: { ...TINY, maxRows: 3 }, stream: UNLIMITED }],
  },
  { name: "streamed: four times the scope past the read budget is refused", seed: 45, runs: [{ limits: TINY, stream: { maxReadBytes: 1024, maxReadEvents: 1 << 24 } }] },
  { name: "streaming off: too large stays partial", seed: 46, runs: [{ limits: TINY }] },
];

async function record(s: Scenario): Promise<Entry[]> {
  const out: Entry[] = [];
  const x = await clonedReplayDatabase();
  cleanup.push(() => x.pg.close());
  const exported = await exportStreamScope(generate(s.seed), 1200);
  cleanup.push(exported.dispose);
  const searchDb = new PGlite({ extensions: { pg_trgm } });
  cleanup.push(() => searchDb.close());
  await searchDb.exec("CREATE EXTENSION pg_trgm");
  const tenant: TenantSqlRunner = (orgId, workspaceId, work) =>
    searchDb.transaction(async (tx) => {
      await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      return work(tx);
    });
  const state: SearchProjectionState = {
    fromMs: async () => 0,
    record: async (_tx, scope, value) => {
      out.push({ state: "record", scope: plain(scope), value: plain(value) });
    },
    partials: async () => ({ sessions: [], overflow: false }),
  };
  const composed = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: searchDb }, "writer", tenant);
  await composed.index.ensureSchema();
  const adapter: SearchServiceBinding = { index: recordingIndex(composed.index, out), state: composed.state };
  const base: SearchIndexDeps = {
    transact: <T>(work: (tx: never) => Promise<T>) =>
      x.pg.transaction((tx) =>
        work({
          query: (sql: string, params?: unknown[]) => {
            out.push({ sql: sql.trim().replace(/\s+/g, " "), params: plain(params ?? []) });
            return tx.query(sql, params);
          },
        } as never),
      ) as Promise<T>,
    reader: exported.reader,
    store: exported.store,
    adapter,
    prefixFor,
    now: () => NOW,
    // Only the forced keepalives (before and after the scan, the first record): never time-dependent.
    streamKeepaliveMs: 1e12,
  };
  for (const [i, extra] of s.runs.entries()) {
    out.push({ run: i });
    const deps: ConsumerDeps = {
      pointer: {
        db: { ...x.db, pool: undefined, end: async () => {} } as never,
        lock: (async (_s: unknown, work: () => Promise<unknown>) => work()) as never,
        reader: exported.reader,
        exists: async (k: string) => (await exported.store.head(objectKey(k as never))) !== undefined,
        orgs: prefixOrgResolver,
        allowPrefix: (p: string, o: string) => prefixAllowed(bindings, p, o),
        fanout: droppedFanout,
      } as never,
      search: { ...base, ...extra },
    };
    const lines: Entry[] = [];
    const stdout = process.stdout.write, stderr = process.stderr.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        for (const line of String(chunk).split("\n"))
          if (line.startsWith("{"))
            try {
              // The search stage's records; the pointer stage's carry a random traceparent.
              const { ts: _ts, duration_ms: _d, ...entry } = JSON.parse(line) as Entry;
              if (entry.stage === "search-index") lines.push(entry);
            } catch {}
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    try {
      const messages = exported.keys.map((key, n) => ({ messageId: `golden-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
      out.push({ result: plain(await processBatch(messages, deps)) });
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    for (const line of lines) out.push({ log: line });
  }
  return out;
}

test("staged reconcile unset: SQL, parameters, adapter calls and log records equal the base commit's golden", async () => {
  const got: Record<string, Entry[]> = {};
  try {
    for (const s of SCENARIOS) got[s.name] = await record(s);
  } finally {
    await closeReplayTemplate();
  }
  if (process.env.UPDATE_STAGED_OFF_GOLDEN === "1") writeFileSync(GOLDEN, JSON.stringify(got, null, 1) + "\n");
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry[]>;
  expect(Object.keys(got)).toEqual(Object.keys(golden));
  for (const s of SCENARIOS) expect(got[s.name]).toEqual(golden[s.name]!);
  // The golden exercises what the staged reconcile changes when on: both truncations, a
  // streamed pass and the four-times refusal.
  const reasons = JSON.stringify(golden);
  for (const code of ["search_index_truncated", "stream_read_bytes_budget", "scope_too_large"]) expect(reasons).toContain(code);
}, 300_000);
