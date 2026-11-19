/**
 * The hosted search path for tests: the consumer's event path (processBatch: the pointer stage, then
 * indexSearchScope) on a migrated PGlite (pointers, the session directory, its triggers), the legacy
 * ClickHouse binding against an in-memory transcript_events / search_ingest_session_state (FINAL and the
 * whole-scope scan), and an object store that counts reads. Every ClickHouse call and every SQL statement of
 * the search transaction is recorded, for goldens and for "nothing was read" checks. Synthetic data only.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout } from "../pointer.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { prefixFor, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { clonedReplayDatabase } from "./replay-fixture.ts";

export const HOSTED_NOW = new Date("2026-11-01T12:00:00.000Z");
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };

/** JSON-stable form of a value: Dates as ISO strings, functions dropped. */
export const plain = (value: unknown): unknown => JSON.parse(JSON.stringify(value ?? null));
const squash = (sql: string) => sql.trim().replace(/\s+/g, " ");

/** transcript_events and the session state in memory, every call recorded. */
export function fakeClickHouse() {
  const rows: IndexRow[] = [];
  const state: Array<Record<string, unknown>> = [];
  const other: Array<{ table: string; rows: unknown[] }> = [];
  const calls: Array<Record<string, unknown>> = [];
  /** What the facts state answers for `complete` (undefined: no row). */
  const facts: { complete?: number } = {};
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
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      calls.push({ ch: "command", query: squash(query), params: plain(options?.params ?? {}) });
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
      calls.push({ ch: "select", query: squash(query), params: plain(options?.params ?? {}) });
      if (query.includes("search_ingest_state") || query.includes("FROM system.columns")) return [] as T[];
      // The output facts state of one session id (skip-current with output facts on).
      if (query.includes("output_facts_session_state")) return (facts.complete === undefined ? [] : [{ complete: facts.complete }]) as T[];
      if (!query.includes(`FROM ${EVENTS_TABLE} FINAL`)) throw new Error(`unexpected select ${query.slice(0, 40)}`);
      const p = options!.params!;
      const found = final()
        .filter((r) => r.org_id === p.org && (r.session_id === p.session || r.session_id === p.canonical) && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from))
        .slice(0, Number(p.limit));
      return found.map((r) => ({ workspace_id: r.workspace_id, session_id: r.session_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      calls.push({ ch: "insert", table, rows: plain(batch) });
      if (table === SESSION_STATE_TABLE) state.push(...(batch as Array<Record<string, unknown>>));
      else if (table === EVENTS_TABLE) rows.push(...(batch as IndexRow[]));
      else other.push({ table, rows: batch as unknown[] });
    },
  };
  const dump = () => final().map((r) => JSON.stringify([r.session_id, r.machine_id, r.kind, r.event_id, tsMs(r), r.fingerprint])).sort();
  return { ch, rows, state, other, calls, dump, facts };
}

/** One hosted consumer over `x`: a migrated HoopDb clone, the fake ClickHouse, a read-counting store. */
export async function hostedRig(x: ExportedScope, cleanup: Array<() => Promise<unknown>>) {
  const db = await clonedReplayDatabase();
  cleanup.push(() => db.pg.close());
  const fake = fakeClickHouse();
  const sql: Array<Record<string, unknown>> = [];
  const reads = { count: 0 };
  const store: ObjectStore = {
    ...x.store,
    getObject: (...a: Parameters<ObjectStore["getObject"]>) => {
      reads.count += 1;
      return x.store.getObject(...a);
    },
  } as ObjectStore;
  const transact = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
    db.pg.transaction((tx) =>
      work({
        query: (text: string, params?: unknown[]) => {
          sql.push({ sql: squash(text), params: plain(params ?? []) });
          return tx.query(text, params) as never;
        },
      }),
    ) as Promise<T>;
  const search = (extra: Partial<SearchIndexDeps> = {}): SearchIndexDeps => ({
    transact, reader: x.reader, store, ch: fake.ch, prefixFor, now: () => HOSTED_NOW,
    // Only the forced keepalives: never time-dependent.
    streamKeepaliveMs: 1e12,
    ...extra,
  });
  const requeued: string[] = [];
  const consumer = (extra: Partial<SearchIndexDeps> = {}): ConsumerDeps => ({
    pointer: {
      db: { ...db.db, pool: undefined, end: async () => {} } as never,
      lock: (async (_s: unknown, work: () => Promise<unknown>) => work()) as never,
      reader: x.reader,
      exists: async (k: string) => (await x.store.head(objectKey(k as never))) !== undefined,
      orgs: prefixOrgResolver,
      allowPrefix: (p: string, o: string) => prefixAllowed(bindings, p, o),
      fanout: droppedFanout,
    } as never,
    search: search(extra),
    requeue: async (key: string) => {
      requeued.push(key);
    },
  });
  const messages = x.keys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  /** One consumer pass over the scope's manifests (what a tick or a redelivered event sends). */
  const tick = (extra: Partial<SearchIndexDeps> = {}) => processBatch(messages, consumer(extra));
  return { db, fake, sql, reads, store, transact, search, tick, requeued, messages };
}
