/**
 * The hosted search path for tests: the consumer's event path (processBatch: the pointer stage, then
 * indexSearchScope) on a migrated PGlite (every Supabase and HoopDb migration: pointers, the session
 * directory, its triggers), the ClickHouse binding against an in-memory transcript_events /
 * search_ingest_session_state (FINAL and the whole-scope scan), and an object store that counts reads.
 * Every ClickHouse call and every SQL statement of the search transaction is recorded, for goldens and for
 * "nothing was read" checks. Synthetic data only.
 */
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  prefixAllowed,
  prefixOrgResolver,
  rawChunkKey,
  type ChunkEntry,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type RelayhistoryStreamRecord,
} from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { memoryLock } from "./support.ts";

export const HOSTED_NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
export const SCOPE: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
export const prefixFor = (org: string) => `96bulls/${org}`;
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const repo = join(import.meta.dir, "../../..");
const readRepo = (p: string) => readFileSync(join(repo, p), "utf8");

/** JSON-stable form of a value: Dates as ISO strings, functions dropped. */
export const plain = (value: unknown): unknown => JSON.parse(JSON.stringify(value ?? null));
const squash = (sql: string) => sql.trim().replace(/\s+/g, " ");

/** A PGlite with every Supabase and HoopDb migration applied (as pointer-race.test.ts builds it). */
export async function migratedPglite(): Promise<PGlite> {
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readRepo("supabase/tests/supabase_shim.sql"));
  await pg.exec(readRepo("supabase/bootstrap/session_directory_search_worker.sql"));
  await pg.exec(readRepo("supabase/bootstrap/session_token_verifier.sql"));
  await pg.exec(readRepo("supabase/bootstrap/session_ingest_roles.sql"));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readRepo(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort())
    await pg.exec(readRepo(`src/hoopdb/migrations/${f}`));
  return pg;
}

/** One synthetic session on the fs store: one manifest (one zstd chunk) per machine, all of SCOPE. */
export interface ExportedScope {
  root: string;
  store: ObjectStore;
  keys: string[];
  dispose(): Promise<void>;
}

/** `machines` manifests of SCOPE with `events` session events each (deterministic from `seed`). */
export async function exportScope(seed: number, machines = 2, events = 3): Promise<ExportedScope> {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  const root = await mkdtemp(join(base, "96bulls-search-hosted-"));
  const store = createFsStore({ root });
  const keys: string[] = [];
  for (let m = 0; m < machines; m++) {
    const machine = `srv_${String.fromCharCode(97 + m)}${seed}`;
    const location = { prefix: prefixFor(SCOPE.orgId), workspaceId: SCOPE.workspaceId, machineId: machine, harness: SCOPE.harness, sessionId: SCOPE.sessionId, startDate: "2026-11-01" };
    const records: RelayhistoryStreamRecord[] = Array.from({ length: events }, (_, i) => ({
      kind: "session_event",
      origin_id: "desktop-origin",
      record_id: `r${seed}-${i}`,
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: { ts_ms: Date.parse(AT) + seed * 1000 + i, text: `synthetic event ${seed}.${i}`, role: i % 2 ? "user" : "assistant" },
    }));
    const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = {
      seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength,
      sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength,
      raw_sha256: sha, compression: "zstd", records: records.length,
    };
    await store.put(rawChunkKey(location, chunk.seq, 0, sha, "ndjson"), stored, {});
    const manifest = {
      schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: AT, updated_at: AT,
      session: { workspace_id: SCOPE.workspaceId, machine_id: machine, harness: SCOPE.harness, session_id: SCOPE.sessionId, started_at: AT },
      writer: { delivery_instance_id: machine, plugin_version: "test" },
      raw: {
        owner: machine, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1,
        raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0,
        epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: machine }],
        next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
      },
      extensions: { "96bulls": { user_id: "user_1" } },
    } as unknown as ManifestV1;
    const key = manifestKey(location);
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
    keys.push(key);
  }
  return { root, store, keys, dispose: () => rm(root, { recursive: true, force: true }) };
}

/** transcript_events and the session state in memory, every call recorded. */
export function fakeClickHouse() {
  const rows: IndexRow[] = [];
  const state: Array<Record<string, unknown>> = [];
  const other: Array<{ table: string; rows: unknown[] }> = [];
  const calls: Array<Record<string, unknown>> = [];
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
      if (query.includes("search_ingest_state")) return [] as T[];
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
  return { ch, rows, state, other, calls, dump };
}

/** One hosted consumer over `x`: a migrated PGlite, the fake ClickHouse, a read-counting store. */
export async function hostedRig(x: ExportedScope, cleanup: Array<() => Promise<unknown>>) {
  const pg = await migratedPglite();
  cleanup.push(() => pg.close());
  const fake = fakeClickHouse();
  const sql: Array<Record<string, unknown>> = [];
  const reads = { count: 0 };
  // The fs store is a class (private fields): forward every member, counting the two body reads.
  const store = new Proxy(x.store, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target) as unknown;
      if (typeof value !== "function") return value;
      if (prop === "get" || prop === "getObject")
        return (...a: unknown[]) => {
          reads.count += 1;
          return (value as (...b: unknown[]) => unknown).apply(target, a);
        };
      return (value as (...b: unknown[]) => unknown).bind(target);
    },
  }) as ObjectStore;
  const reader: ManifestReader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  const transact = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
    pg.transaction((tx) =>
      work({
        query: (text: string, params?: unknown[]) => {
          sql.push({ sql: squash(text), params: plain(params ?? []) });
          return tx.query(text, params) as never;
        },
      }),
    ) as Promise<T>;
  const search = (extra: Partial<SearchIndexDeps> = {}): SearchIndexDeps => ({
    transact, reader, store, ch: fake.ch, prefixFor, now: () => HOSTED_NOW,
    ...extra,
  });
  const db = {
    pool: undefined,
    end: async () => {},
    async query(text: string, params?: unknown[]) {
      const r = await pg.query(text, params as unknown[]);
      return { rows: r.rows };
    },
  } as unknown as HoopDb;
  const requeued: string[] = [];
  const consumer = (extra: Partial<SearchIndexDeps> = {}): ConsumerDeps => ({
    pointer: {
      db,
      lock: memoryLock(),
      reader,
      exists: async (k: string) => (await x.store.head(objectKey(k as never))) !== undefined,
      orgs: prefixOrgResolver,
      allowPrefix: (p: string | undefined, o: string) => prefixAllowed(bindings, p, o),
      fanout: droppedFanout,
      now: () => HOSTED_NOW,
    },
    search: search(extra),
    requeue: async (key: string) => {
      requeued.push(key);
    },
  });
  const messages = x.keys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  /** One consumer pass over the scope's manifests (what a tick or a redelivered event sends). */
  const tick = (extra: Partial<SearchIndexDeps> = {}) => processBatch(messages, consumer(extra));
  return { pg, fake, sql, reads, store, reader, transact, search, tick, requeued, messages };
}
