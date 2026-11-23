/**
 * SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED (search-index.ts acceptance stamp): with the flag
 * on, a pass that ends incomplete for relayhistory_acceptance_missing alone, having read every
 * live pointer as listed, stamps a digest of the pointer set (plus window, limits and salt) into
 * search_ingest_state; a later pass whose digest equals a stamp younger than
 * SKIP_STATE_MAX_AGE_MS returns that outcome before any manifest read and writes nothing. Any
 * other outcome clears the stamp. With the flag off nothing of this runs: no stamp read or write,
 * no extra statement. Pointer rows come from the real pointer stage into PGlite with every
 * Supabase and HoopDb migration.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
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
  type SessionLocation,
} from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { EVENTS_TABLE, INGEST_STATE_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import {
  ACCEPTANCE_MISSING,
  ACCEPTANCE_SKIP_VERSION,
  ACCEPTANCE_STAMP_PREFIX,
  SKIPPED_BEFORE_READ,
  SKIP_STATE_MAX_AGE_MS,
  acceptanceDigest,
  acceptanceStampName,
  indexSearchScope,
  livePointerFacts,
  type SearchIndexDeps,
  type SearchIndexResult,
} from "../search-index.ts";
import { ALLOWED_LOG_FIELDS, logLine } from "../../shared/log.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { lambdaEnv } from "../lambda.ts";
import { memoryLock } from "./support.ts";

const repo = join(import.meta.dir, "../../..");
const readF = (p: string) => readFileSync(join(repo, p), "utf8");
const AT = "2026-11-01T10:00:00.000Z";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string): SessionLocation => ({
  prefix: "96bulls/org_1", workspaceId: scope.workspaceId, machineId, harness: scope.harness, sessionId: scope.sessionId, startDate: "2026-11-01",
});
const SKIPPED: SearchIndexResult = {
  manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, skippedBeforeRead: true,
  reasons: [ACCEPTANCE_MISSING, SKIPPED_BEFORE_READ],
};
const chTime = (ms: number) => new Date(ms).toISOString().replace("T", " ").replace("Z", "");

let root: string;
let store: ObjectStore;
let reader: ManifestReader;
let pg: PGlite;
beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-accskip-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readF("supabase/tests/supabase_shim.sql"));
  await pg.exec(readF("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readF(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort()) await pg.exec(readF(`src/hoopdb/migrations/${f}`));
}, 120_000);
afterEach(async () => {
  await pg.close();
  await rm(root, { recursive: true, force: true });
});

/**
 * One manifest of `machine` whose records all come from `origin`. With `shared`, one record id
 * ("shared") is also written by another manifest's origin and no record carries accepted_seq,
 * so folding both manifests together refuses with relayhistory_acceptance_missing.
 */
async function writeManifest(machine: string, revision = 1, opts: { origin?: string; shared?: boolean; unscrubbed?: boolean; records?: number } = {}) {
  const location = loc(machine);
  const origin = opts.origin ?? `o_${machine}`;
  const ids = Array.from({ length: opts.records ?? 4 }, (_, i) => `${machine}-r${i}`);
  if (opts.shared ?? true) ids.push("shared");
  const lines = ids.map((id, i) =>
    JSON.stringify({ kind: "session_event", origin_id: origin, record_id: id, revision: String(revision), operation: "upsert", received_at: AT, payload: { ts_ms: Date.parse(AT) + i, text: `t-${machine}-${i}`, role: "assistant" } }),
  );
  const source = new TextEncoder().encode(`${lines.join("\n")}\n`);
  const stored = new Uint8Array(zstdCompressSync(source));
  const sha = await sha256Hex(source);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: lines.length,
  };
  await store.put(rawChunkKey(location, 0, 0, sha, "ndjson"), stored, {});
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision, created_at: AT, updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: scope.sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "t" },
    raw: {
      owner: machine, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: chunk.bytes,
      stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: machine }], next_offset: source.byteLength, chunks: [chunk],
      ...(opts.unscrubbed ? {} : { scrub: { ruleset: "relayhistory-scrub", version: 1 } }),
    },
    extensions: { "96bulls": { user_id: "user_1" } },
  } as unknown as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return { key, bytes: source.byteLength };
}

const s3Body = (key: string, name: string, seq: string) =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: name, eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: seq } } }] });
const pgDb = (): HoopDb =>
  ({ pool: undefined, end: async () => {}, async query(text: string, params?: unknown[]) { return { rows: (await pg.query(text, params as unknown[])).rows }; } }) as unknown as HoopDb;
const pointerDeps = (): ConsumerDeps["pointer"] => ({
  db: pgDb(), lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver,
  allowPrefix: (p, o) => prefixAllowed({ PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" }, p, o), fanout: droppedFanout,
});
let seq = 0;
async function pointer(key: string, name = "ObjectCreated:Put") {
  seq++;
  const out = await processBatch([{ messageId: `p${seq}`, body: s3Body(key, name, String(seq).padStart(4, "0")) }], { pointer: pointerDeps() });
  expect(out.batchItemFailures).toEqual([]);
}

type StateRow = { name: string; value: string; updated_at: string };
/** ClickHouse in memory: search_ingest_state answers readState and the stamp read; every statement is logged. */
function fakeClickHouse() {
  const ingest: StateRow[] = [];
  const statements: string[] = [];
  const writes: string[] = [];
  const ctl: { window: string | null; stampReadThrows: boolean; stampWriteThrows: boolean; atMs?: unknown; eventRows: unknown[] } = {
    window: null, stampReadThrows: false, stampWriteThrows: false, eventRows: [],
  };
  const ch: ClickHouseClient = {
    database: "test",
    async command(query) {
      statements.push(`command ${query.trim().replace(/\s+/g, " ")}`);
      writes.push(`command ${query.trim().slice(0, 40)}`);
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      const q = query.trim().replace(/\s+/g, " ");
      statements.push(`select ${q}`);
      if (q.includes(`FROM ${INGEST_STATE_TABLE}`)) {
        const name = String(options!.params!.name);
        if (q.includes("argMax")) {
          // readState: window_start is the only name a pass reads this way.
          return (name === "window_start" && ctl.window !== null ? [{ value: ctl.window, n: 1 }] : []) as T[];
        }
        if (ctl.stampReadThrows) throw new Error("clickhouse unavailable");
        const latest = ingest.filter((r) => r.name === name).sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1))[0];
        const atMs = "atMs" in ctl ? ctl.atMs : latest ? Date.parse(`${latest.updated_at.replace(" ", "T")}Z`) : undefined;
        return (latest ? [{ value: latest.value, at_ms: atMs }] : []) as T[];
      }
      // The whole read's scan of the events table (the streamed path pages it the same way).
      if (q.includes(`FROM ${EVENTS_TABLE}`)) return ctl.eventRows as T[];
      return [] as T[];
    },
    async insert(table, rows) {
      statements.push(`insert ${table}`);
      writes.push(`insert ${table}`);
      if (table === INGEST_STATE_TABLE) {
        if (ctl.stampWriteThrows) throw new Error("Authorization: Bearer sk-live-123");
        ingest.push(...(rows as StateRow[]));
      } else if (table !== EVENTS_TABLE && table !== SESSION_STATE_TABLE) throw new Error(`unexpected insert into ${table}`);
    },
  };
  return { ch, ingest, statements, writes, ctl };
}

function searchDeps(ch: ClickHouseClient, extra: Partial<SearchIndexDeps> = {}) {
  const reads: string[] = [];
  const sql: string[] = [];
  const transact = async <T>(work: (tx: SqlExecutor) => Promise<T>) =>
    pg.transaction(async (t) =>
      work({
        async query<R>(text: string, params?: unknown[]) {
          sql.push(text.trim().replace(/\s+/g, " "));
          return { rows: (await t.query(text, params as unknown[])).rows as R[] };
        },
      }),
    );
  const deps: SearchIndexDeps = {
    transact: transact as SearchIndexDeps["transact"],
    reader: { ...reader, read: async (k) => { reads.push(k); return reader.read(k); } },
    store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW,
    writeCatalog: async () => ({ written: true }),
    ...extra,
  };
  return { deps, reads, sql };
}
const ON = { skipAcceptanceMissing: true as const };

/** Two live scrubbed manifests from two origins sharing one record id: the fold refuses. */
async function acceptanceScope() {
  const a = await writeManifest("srv_aa");
  const b = await writeManifest("srv_bb");
  await pointer(a.key);
  await pointer(b.key);
  return { a, b };
}
const stampsOf = (rows: StateRow[]) => rows.filter((r) => r.name.startsWith(ACCEPTANCE_STAMP_PREFIX));
const live = async () => (await pg.query<{ manifest_key: string }>("SELECT 1")).rows && livePointerFactsNow();
async function livePointerFactsNow() {
  return pg.transaction(async (t) => livePointerFacts({ query: async (text: string, params?: unknown[]) => ({ rows: (await t.query(text, params as unknown[])).rows }) } as unknown as SqlExecutor, scope));
}
/** The stamp the implementation would write for the current pointer rows under the default window and limits. */
async function currentDigest(windowMarker = "default:14d", limits = { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000 }, stream?: { maxReadBytes: number; maxReadEvents: number }) {
  return acceptanceDigest(scope, await live(), windowMarker, limits, stream);
}
const stampRow = (value: unknown, ms: number): StateRow => ({
  name: acceptanceStampName(scope), value: typeof value === "string" ? value : JSON.stringify(value), updated_at: chTime(ms),
});

/** Runs `work` with stdout and stderr captured: the JSON log lines and the raw text. */
async function captured<T>(work: () => Promise<T>): Promise<{ result: T; lines: Record<string, unknown>[]; raw: string }> {
  const lines: Record<string, unknown>[] = [];
  const raw: string[] = [];
  const out = process.stdout.write.bind(process.stdout);
  const err = process.stderr.write.bind(process.stderr);
  const grab = (chunk: unknown) => {
    raw.push(String(chunk));
    for (const line of String(chunk).split("\n")) if (line.startsWith("{")) lines.push(JSON.parse(line));
    return true;
  };
  process.stdout.write = grab as typeof process.stdout.write;
  process.stderr.write = grab as typeof process.stderr.write;
  try {
    return { result: await work(), lines, raw: raw.join("") };
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}
const etagOf = async (machine: string) =>
  (await pg.query<{ manifest_etag: string }>(`SELECT manifest_etag FROM sessions.session_index WHERE session_id = 'sess_1' AND machine_id = $1`, [machine])).rows[0]!.manifest_etag;

const LISTING = "SELECT manifest_key FROM sessions.session_index WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND deleted_at IS NULL ORDER BY machine_id";
const LOCK = "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok";

describe("flag off", () => {
  test("every pass reads every manifest; statements are exactly the pre-flag ones; no stamp read or write", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch);
    const results: SearchIndexResult[] = [];
    for (let i = 0; i < 2; i++) results.push(await indexSearchScope(scope, s.deps));
    expect(results[0]).toEqual({ manifests: 2, rows: results[0]!.rows, inserted: results[0]!.inserted, deleted: 0, complete: false, reasons: [ACCEPTANCE_MISSING], contenderSeats: "all_unseated" });
    expect(results[0]!.rows).toBeGreaterThan(0);
    expect(results[1]).toEqual({ ...results[0]!, inserted: results[0]!.inserted });
    expect(s.reads).toHaveLength(4);
    expect(s.sql).toEqual([LOCK, LISTING, LOCK, LISTING]);
    expect(ch.ingest).toEqual([]);
    expect(ch.statements.filter((q) => q.includes(INGEST_STATE_TABLE))).toEqual([
      `select SELECT argMax(value, updated_at) AS value, count() AS n FROM ${INGEST_STATE_TABLE} WHERE name = {name:String} HAVING n > 0`,
      `select SELECT argMax(value, updated_at) AS value, count() AS n FROM ${INGEST_STATE_TABLE} WHERE name = {name:String} HAVING n > 0`,
    ]);
    expect(ch.statements.some((q) => q.includes("acceptance") || q.includes("ORDER BY updated_at DESC"))).toBe(false);
  }, 60_000);

  test("a matching fresh stamp present in ClickHouse is never read: the pass reads in full", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() }, NOW.getTime() - 1_000));
    const s = searchDeps(ch.ch);
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(2);
    expect(ch.ingest).toHaveLength(1);
  }, 60_000);

  test("erase with the flag off: the delete alone, no stamp read or write", async () => {
    const { a, b } = await acceptanceScope();
    for (const k of [a.key, b.key]) {
      await store.delete(objectKey(k));
      await pointer(k, "ObjectRemoved:DeleteMarkerCreated");
    }
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch);
    expect(await indexSearchScope(scope, s.deps)).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] });
    expect(ch.statements.map((q) => q.split(" ")[0] + " " + q.split(" ")[1])).toEqual(["command DELETE"]);
  }, 60_000);
});

describe("flag on: stamp and skip", () => {
  test("first pass: the same result as flag off, plus one stamp of the pointer digest; repeat: skipped, no read, no write", async () => {
    await acceptanceScope();
    const off = fakeClickHouse();
    const offResult = await indexSearchScope(scope, searchDeps(off.ch).deps);
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    const first = await indexSearchScope(scope, s.deps);
    expect(first).toEqual(offResult);
    expect(s.reads).toHaveLength(2);
    expect(stampsOf(ch.ingest)).toEqual([stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() }, NOW.getTime())]);
    // Flag on writes what flag off writes, plus the stamp.
    expect(ch.writes).toEqual([...off.writes, `insert ${INGEST_STATE_TABLE}`]);
    const writes = ch.writes.length;
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    expect(s.reads).toHaveLength(2);
    expect(ch.writes.length).toBe(writes);
    expect(ch.statements.filter((q) => q.includes(`FROM ${EVENTS_TABLE}`))).toHaveLength(1);
  }, 60_000);

  test("the skip decides before any S3 access: with the manifests gone it still skips (pointer rows are the only input)", async () => {
    const { a, b } = await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    await store.delete(objectKey(a.key));
    await store.delete(objectKey(b.key));
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("flag on statements: the facts listing replaces the key listing; one stamp read after the window read", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    expect(s.sql).toHaveLength(2);
    expect(s.sql[1]).toContain("SELECT manifest_key, machine_id, revision::text AS revision");
    expect(s.sql[1]).toContain("WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND deleted_at IS NULL ORDER BY machine_id");
    const ingestStatements = ch.statements.filter((q) => q.includes(INGEST_STATE_TABLE));
    expect(ingestStatements).toHaveLength(3);
    expect(ingestStatements[0]).toContain("argMax");
    expect(ingestStatements[1]).toContain("ORDER BY updated_at DESC LIMIT 1");
    expect(ingestStatements[2]).toBe(`insert ${INGEST_STATE_TABLE}`);
  }, 60_000);

  test("a pointer added: digest changes, the pass reads in full", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    const c = await writeManifest("srv_cc", 1, { shared: false });
    await pointer(c.key);
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 3, complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(result.skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(5);
    expect(stampsOf(ch.ingest)).toHaveLength(2);
    expect(JSON.parse(stampsOf(ch.ingest)[1]!.value).digest).toBe(await currentDigest());
  }, 60_000);

  test("a pointer removed: the pass reads, completes and clears the stamp; no later skip", async () => {
    const { b } = await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    await store.delete(objectKey(b.key));
    await pointer(b.key, "ObjectRemoved:DeleteMarkerCreated");
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ manifests: 1, complete: true, reasons: [] });
    // Same clock as the stamp: one millisecond above it, so the two never tie.
    expect(stampsOf(ch.ingest).at(-1)).toEqual({ name: acceptanceStampName(scope), value: "", updated_at: chTime(NOW.getTime() + 1) });
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ manifests: 1, complete: true });
    expect(s.reads).toHaveLength(4);
    // Nothing more to clear: no further stamp row.
    expect(stampsOf(ch.ingest)).toHaveLength(2);
  }, 60_000);

  test("a revision raised: the pass reads in full and re-stamps; the next repeat skips", async () => {
    const { a } = await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    const before = JSON.parse(stampsOf(ch.ingest)[0]!.value).digest;
    await writeManifest("srv_aa", 2);
    await pointer(a.key);
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(s.reads).toHaveLength(4);
    const after = JSON.parse(stampsOf(ch.ingest).at(-1)!.value).digest;
    expect(after).not.toBe(before);
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
  }, 60_000);

  test("an etag change in the pointer row: no skip, and a read that differs from the pointer row writes no stamp (clears)", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    await pg.query(`UPDATE sessions.session_index SET manifest_etag = '"other"' WHERE session_id = 'sess_1' AND machine_id = 'srv_aa'`);
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(s.reads).toHaveLength(4);
    expect(stampsOf(ch.ingest).at(-1)!.value).toBe("");
    // Still no stamp to skip on.
    await indexSearchScope(scope, s.deps);
    expect(s.reads).toHaveLength(6);
  }, 60_000);

  test("a content sha256 or revision in the pointer row that the manifest read does not show: no stamp", async () => {
    await acceptanceScope();
    for (const update of [
      `UPDATE sessions.session_index SET content_sha256 = decode(repeat('ab', 32), 'hex') WHERE session_id = 'sess_1' AND machine_id = 'srv_bb'`,
      `UPDATE sessions.session_index SET revision = 9 WHERE session_id = 'sess_1' AND machine_id = 'srv_bb'`,
      `UPDATE sessions.session_index SET content_sha256 = NULL WHERE session_id = 'sess_1' AND machine_id = 'srv_bb'`,
    ]) {
      await pg.query(update);
      const ch = fakeClickHouse();
      const s = searchDeps(ch.ch, ON);
      expect(await indexSearchScope(scope, s.deps)).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
      expect(stampsOf(ch.ingest)).toEqual([]);
    }
  }, 60_000);

  test("only the revision in the pointer row differs from the manifest read (content sha256 and etag equal): no stamp, no skip", async () => {
    await acceptanceScope();
    const [before] = (await live()).filter((p) => p.machine_id === "srv_bb");
    await pg.query(`UPDATE sessions.session_index SET revision = 9 WHERE session_id = 'sess_1' AND machine_id = 'srv_bb'`);
    const [after] = (await live()).filter((p) => p.machine_id === "srv_bb");
    expect({ ...after!, revision: before!.revision }).toEqual(before!);
    expect(after!.revision).toBe("9");
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(stampsOf(ch.ingest)).toEqual([]);
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(4);
  }, 60_000);

  test("a tombstone from a slower clock still sorts above the stamp it clears; no later skip", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const fastMs = NOW.getTime() + 60_000;
    // A pass on a clock one minute ahead stamps the pointer set.
    await indexSearchScope(scope, searchDeps(ch.ch, { ...ON, now: () => new Date(fastMs) }).deps);
    const stamped = await currentDigest();
    expect(stampsOf(ch.ingest)).toEqual([stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: stamped }, fastMs)]);
    // A pass on the slower clock sees a pointer row that disagrees with the read and clears.
    const etag = await etagOf("srv_aa");
    await pg.query(`UPDATE sessions.session_index SET manifest_etag = '"other"' WHERE session_id = 'sess_1' AND machine_id = 'srv_aa'`);
    expect(await indexSearchScope(scope, searchDeps(ch.ch, ON).deps)).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(stampsOf(ch.ingest).at(-1)).toEqual({ name: acceptanceStampName(scope), value: "", updated_at: chTime(fastMs + 1) });
    // The pointer set is the stamped one again: the newest row is the tombstone, so no skip.
    await pg.query(`UPDATE sessions.session_index SET manifest_etag = $1 WHERE session_id = 'sess_1' AND machine_id = 'srv_aa'`, [etag]);
    expect(await currentDigest()).toBe(stamped);
    for (let i = 0; i < 2; i++) {
      const s = searchDeps(ch.ch, ON);
      const result = await indexSearchScope(scope, s.deps);
      expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING] });
      expect(result.skippedBeforeRead).toBeUndefined();
      expect(s.reads).toHaveLength(2);
    }
  }, 60_000);

  test("the tombstone is written at the pass's own time when that is later than the stamp", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 60_000));
    await indexSearchScope(scope, searchDeps(ch.ch, { ...ON, limits: { maxScopeBytes: 10 } }).deps);
    expect(stampsOf(ch.ingest).at(-1)).toEqual({ name: acceptanceStampName(scope), value: "", updated_at: chTime(NOW.getTime()) });
  }, 60_000);

  test("the window moves (stored window_start set or changed): no skip", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    ch.ctl.window = "2026-10-01T00:00:00.000Z";
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(4);
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    ch.ctl.window = "2026-09-01T00:00:00.000Z";
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(6);
  }, 60_000);

  test("the window the pass uses is windowStart's: the stored value, else now less 14 days", async () => {
    await acceptanceScope();
    const fromOf = async (extra: Partial<SearchIndexDeps>, window: string | null) => {
      const ch = fakeClickHouse();
      ch.ctl.window = window;
      await indexSearchScope(scope, searchDeps(ch.ch, extra).deps);
      return ch.statements.filter((q) => q.includes(`FROM ${EVENTS_TABLE}`));
    };
    for (const window of [null, "2026-10-20T00:00:00.000Z"]) {
      const offScan = await fromOf({}, window);
      const onScan = await fromOf(ON, window);
      expect(onScan).toEqual(offScan);
    }
  }, 60_000);

  test("limits or stream budgets changed: no skip", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    await indexSearchScope(scope, searchDeps(ch.ch, ON).deps);
    const raised = searchDeps(ch.ch, { ...ON, limits: { maxScopeEvents: 99_999 } });
    expect((await indexSearchScope(scope, raised.deps)).skippedBeforeRead).toBeUndefined();
    expect(raised.reads).toHaveLength(2);
  }, 60_000);
});

describe("flag on: stamps that never skip", () => {
  const cases: [string, (digest: string) => StateRow][] = [
    ["stale (at the age bound)", (d) => stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: d }, NOW.getTime() - SKIP_STATE_MAX_AGE_MS)],
    ["far in the future", (d) => stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: d }, NOW.getTime() + SKIP_STATE_MAX_AGE_MS + 1_000)],
    ["another reason", (d) => stampRow({ v: 1, reason: "manifest_missing", digest: d }, NOW.getTime() - 1_000)],
    ["another format version", (d) => stampRow({ v: 2, reason: ACCEPTANCE_MISSING, digest: d }, NOW.getTime() - 1_000)],
    ["another digest", () => stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 1_000)],
    ["no digest", () => stampRow({ v: 1, reason: ACCEPTANCE_MISSING }, NOW.getTime() - 1_000)],
    ["malformed JSON", () => stampRow("{not json", NOW.getTime() - 1_000)],
    ["a JSON scalar", (d) => stampRow(JSON.stringify(d), NOW.getTime() - 1_000)],
    ["null", () => stampRow("null", NOW.getTime() - 1_000)],
  ];
  for (const [label, row] of cases)
    test(`${label}: the pass reads in full`, async () => {
      await acceptanceScope();
      const ch = fakeClickHouse();
      ch.ingest.push(row(await currentDigest()));
      const s = searchDeps(ch.ch, ON);
      const result = await indexSearchScope(scope, s.deps);
      expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING] });
      expect(result.skippedBeforeRead).toBeUndefined();
      expect(s.reads).toHaveLength(2);
      // The pass re-stamps with its own digest.
      expect(JSON.parse(stampsOf(ch.ingest).at(-1)!.value)).toEqual({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() });
    }, 60_000);

  test("just under the age bound skips; a non-numeric or missing time does not", async () => {
    await acceptanceScope();
    const fresh = fakeClickHouse();
    fresh.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() }, NOW.getTime() - SKIP_STATE_MAX_AGE_MS + 1_000));
    expect(await indexSearchScope(scope, searchDeps(fresh.ch, ON).deps)).toEqual(SKIPPED);
    for (const atMs of ["not-a-number", undefined, null, {}]) {
      const ch = fakeClickHouse();
      ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() }, NOW.getTime() - 1_000));
      ch.ctl.atMs = atMs;
      const s = searchDeps(ch.ch, ON);
      expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
      expect(s.reads).toHaveLength(2);
    }
  }, 60_000);

  test("the latest row decides: a newer tombstone over a matching stamp does not skip, an older one does", async () => {
    await acceptanceScope();
    const d = await currentDigest();
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: d }, NOW.getTime() - 60_000), stampRow("", NOW.getTime() - 30_000));
    const s = searchDeps(ch.ch, ON);
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    const older = fakeClickHouse();
    older.ingest.push(stampRow("", NOW.getTime() - 60_000), stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: d }, NOW.getTime() - 30_000));
    expect(await indexSearchScope(scope, searchDeps(older.ch, ON).deps)).toEqual(SKIPPED);
  }, 60_000);

  test("a stale stamp of an older pointer set, written later than the current one, never skips (digest mismatch)", async () => {
    const { a } = await acceptanceScope();
    const old = await currentDigest();
    await writeManifest("srv_aa", 2);
    await pointer(a.key);
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: old }, NOW.getTime() - 1_000));
    const s = searchDeps(ch.ch, ON);
    expect((await indexSearchScope(scope, s.deps)).skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("the digest carries the salt, the scope, the window marker, the limits, the stream budgets and the sorted pointer facts", async () => {
    await acceptanceScope();
    const facts = await live();
    const limits = { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000 };
    const rows = facts.map((p) => JSON.stringify([p.manifest_key, p.machine_id, p.revision, p.content_sha256, p.manifest_etag, p.scrubbed])).sort();
    const expected = createHash("sha256")
      .update(JSON.stringify([ACCEPTANCE_SKIP_VERSION, [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId], "default:14d", [limits.maxScopeBytes, limits.maxScopeEvents], null, rows]))
      .digest("hex");
    expect(ACCEPTANCE_SKIP_VERSION).toBe("acceptance-skip-v1");
    expect(acceptanceDigest(scope, facts, "default:14d", limits, undefined)).toBe(expected);
    expect(acceptanceDigest(scope, [...facts].reverse(), "default:14d", limits, undefined)).toBe(expected);
    expect(acceptanceDigest(scope, facts, "default:14d", limits, { maxReadBytes: 1, maxReadEvents: 1 })).not.toBe(expected);
    expect(acceptanceDigest({ ...scope, sessionId: "sess_2" }, facts, "default:14d", limits, undefined)).not.toBe(expected);
    for (const field of ["manifest_key", "machine_id", "revision", "content_sha256", "manifest_etag"] as const)
      expect(acceptanceDigest(scope, facts.map((p, i) => (i === 0 ? { ...p, [field]: `${p[field]}x` } : p)), "default:14d", limits, undefined)).not.toBe(expected);
    expect(acceptanceDigest(scope, facts.map((p, i) => (i === 0 ? { ...p, scrubbed: !p.scrubbed } : p)), "default:14d", limits, undefined)).not.toBe(expected);
    expect(facts.every((p) => /^[0-9a-f]{64}$/.test(p.content_sha256 ?? "") && p.manifest_etag !== null && p.scrubbed)).toBe(true);
  }, 60_000);

  test("stamp names are per scope, prefixed, and collide with no vendored state name", () => {
    const name = acceptanceStampName(scope);
    expect(name).toMatch(/^acceptance_skip\/[0-9a-f]{64}$/);
    expect(acceptanceStampName({ ...scope, harness: "codex" })).not.toBe(name);
    const vendored = readF("src/session-pipeline/vendor/search-index/reconcile.ts");
    for (const m of vendored.matchAll(/STATE_[A-Z_]+ = "([^"]+)"/g)) expect(m[1]!.startsWith(ACCEPTANCE_STAMP_PREFIX)).toBe(false);
  });
});

describe("flag on: other outcomes", () => {
  test("mixed reasons (acceptance plus an unscrubbed manifest) write no stamp and clear one", async () => {
    await acceptanceScope();
    const q = await writeManifest("srv_qq", 1, { unscrubbed: true, shared: false });
    await pointer(q.key);
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 1_000));
    const s = searchDeps(ch.ch, ON);
    const result = await indexSearchScope(scope, s.deps);
    expect(result.reasons).toEqual(["manifest_unscrubbed", ACCEPTANCE_MISSING]);
    expect(stampsOf(ch.ingest).at(-1)!.value).toBe("");
    expect(stampsOf(ch.ingest)).toHaveLength(2);
  }, 60_000);

  test("acceptance-missing first plus clickhouse_truncated: no stamp is written and no skip follows", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const row = { workspace_id: scope.workspaceId, machine_id: "srv_zz", source: scope.harness, kind: "assistant", ts_ms: Date.parse(AT), fingerprint: "f" };
    ch.ctl.eventRows = [{ ...row, event_id: "z1" }, { ...row, event_id: "z2" }];
    const s = searchDeps(ch.ch, { ...ON, limits: { maxRows: 1 } });
    for (let i = 0; i < 2; i++) {
      const result = await indexSearchScope(scope, s.deps);
      expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING, "clickhouse_truncated"] });
      expect(result.skippedBeforeRead).toBeUndefined();
    }
    expect(s.reads).toHaveLength(4);
    expect(stampsOf(ch.ingest)).toEqual([]);
  }, 60_000);

  test("a complete pass with no stamp present writes nothing to the state table (growth bound)", async () => {
    const a = await writeManifest("srv_aa", 1, { shared: false });
    await pointer(a.key);
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ complete: true, reasons: [] });
    expect(ch.ingest).toEqual([]);
  }, 60_000);

  test("a size refusal clears a stamp (limits are in the digest, so the stamp cannot match)", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    await indexSearchScope(scope, searchDeps(ch.ch, ON).deps);
    const s = searchDeps(ch.ch, { ...ON, limits: { maxScopeBytes: 10 } });
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ partial: true, reasons: ["scope_too_large"] });
    expect(stampsOf(ch.ingest).at(-1)!.value).toBe("");
  }, 60_000);

  test("erase clears the stamp; every row of the scope is deleted as without the flag", async () => {
    const { a, b } = await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    for (const k of [a.key, b.key]) {
      await store.delete(objectKey(k));
      await pointer(k, "ObjectRemoved:DeleteMarkerCreated");
    }
    expect(await indexSearchScope(scope, s.deps)).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] });
    expect(ch.statements.some((q) => q.startsWith(`command DELETE FROM ${EVENTS_TABLE}`))).toBe(true);
    expect(stampsOf(ch.ingest).at(-1)!.value).toBe("");
  }, 60_000);

  test("erase with no stamp present: the delete and one stamp read, no tombstone", async () => {
    const { a, b } = await acceptanceScope();
    for (const k of [a.key, b.key]) {
      await store.delete(objectKey(k));
      await pointer(k, "ObjectRemoved:DeleteMarkerCreated");
    }
    const ch = fakeClickHouse();
    expect(await indexSearchScope(scope, searchDeps(ch.ch, ON).deps)).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] });
    expect(ch.ingest).toEqual([]);
    expect(ch.writes).toHaveLength(1);
    expect(ch.writes[0]!.startsWith("command DELETE")).toBe(true);
    expect(ch.statements.filter((q) => q.includes(INGEST_STATE_TABLE))).toHaveLength(1);
  }, 60_000);

  test("stamp read fails: the pass reads in full (fail open), then stamps its outcome", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: await currentDigest() }, NOW.getTime() - 1_000));
    ch.ctl.stampReadThrows = true;
    const s = searchDeps(ch.ch, ON);
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 2, complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(result.skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("stamp read fails on a complete pass: the stamp is cleared (it could have been there)", async () => {
    const a = await writeManifest("srv_aa", 1, { shared: false });
    await pointer(a.key);
    const ch = fakeClickHouse();
    ch.ctl.stampReadThrows = true;
    expect(await indexSearchScope(scope, searchDeps(ch.ch, ON).deps)).toMatchObject({ complete: true });
    expect(stampsOf(ch.ingest)).toEqual([{ name: acceptanceStampName(scope), value: "", updated_at: chTime(NOW.getTime()) }]);
  }, 60_000);

  test("stamp write fails: the result is unchanged and one warn line through the real logger carries codes only", async () => {
    await acceptanceScope();
    const offResult = await indexSearchScope(scope, searchDeps(fakeClickHouse().ch).deps);
    const ch = fakeClickHouse();
    ch.ctl.stampWriteThrows = true;
    const lines: Record<string, unknown>[] = [];
    const raw: string[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    const grab = (chunk: unknown) => {
      raw.push(String(chunk));
      for (const line of String(chunk).split("\n")) if (line.startsWith("{")) lines.push(JSON.parse(line));
      return true;
    };
    process.stdout.write = grab as typeof process.stdout.write;
    process.stderr.write = grab as typeof process.stderr.write;
    let result;
    try {
      result = await indexSearchScope(scope, searchDeps(ch.ch, ON).deps);
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    expect(result).toEqual(offResult);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ level: "warn", message: "search index acceptance stamp not written", stage: "search-index", org_id: "org_1", session_id: "sess_1", reason: "acceptance_stamp_write_failed" });
    for (const k of Object.keys(lines[0]!)) expect(["ts", "level", "message", ...ALLOWED_LOG_FIELDS]).toContain(k);
    expect(raw.join("")).not.toContain("sk-live");
  }, 60_000);

  test("ClickHouse refuses every insert into the state table: flag-off results, nothing thrown, one warning per pass, every pass reads in full", async () => {
    await acceptanceScope();
    const warnings = (lines: Record<string, unknown>[]) => lines.filter((l) => l.message === "search index acceptance stamp not written");
    // Stamp writes refused: each pass is the flag-off pass and the next one reads again.
    const offResult = await indexSearchScope(scope, searchDeps(fakeClickHouse().ch).deps);
    const ch = fakeClickHouse();
    ch.ctl.stampWriteThrows = true;
    const s = searchDeps(ch.ch, ON);
    for (let i = 1; i <= 2; i++) {
      const pass = await captured(() => indexSearchScope(scope, s.deps));
      expect(pass.result).toEqual(offResult);
      expect(pass.lines).toHaveLength(1);
      expect(warnings(pass.lines).map((l) => [l.level, l.reason])).toEqual([["warn", "acceptance_stamp_write_failed"]]);
      expect(pass.raw).not.toContain("sk-live");
      expect(s.reads).toHaveLength(2 * i);
    }
    expect(stampsOf(ch.ingest)).toEqual([]);
    // Tombstone writes refused: a size refusal over a stamp that does not match.
    const limited = { limits: { maxScopeBytes: 10 } };
    const offPartial = await indexSearchScope(scope, searchDeps(fakeClickHouse().ch, limited).deps);
    const clear = fakeClickHouse();
    clear.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 1_000));
    clear.ctl.stampWriteThrows = true;
    const c = searchDeps(clear.ch, { ...ON, ...limited });
    for (let i = 1; i <= 2; i++) {
      const pass = await captured(() => indexSearchScope(scope, c.deps));
      expect(pass.result).toEqual(offPartial);
      expect(pass.lines).toHaveLength(1);
      expect(warnings(pass.lines).map((l) => [l.level, l.reason])).toEqual([["warn", "acceptance_stamp_clear_failed"]]);
      expect(pass.raw).not.toContain("sk-live");
      expect(c.reads).toHaveLength(2 * i);
    }
    expect(stampsOf(clear.ingest)).toHaveLength(1);
  }, 60_000);

  test("ClickHouse refuses the tombstone on erase: the erase result stands, one warning", async () => {
    const { a, b } = await acceptanceScope();
    for (const k of [a.key, b.key]) {
      await store.delete(objectKey(k));
      await pointer(k, "ObjectRemoved:DeleteMarkerCreated");
    }
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 1_000));
    ch.ctl.stampWriteThrows = true;
    const pass = await captured(() => indexSearchScope(scope, searchDeps(ch.ch, ON).deps));
    expect(pass.result).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] });
    expect(pass.lines.map((l) => [l.level, l.reason])).toEqual([["warn", "acceptance_stamp_clear_failed"]]);
  }, 60_000);

  test("the stamp log line keeps every field through logLine's allowlist", () => {
    const fields = { stage: "search-index", org_id: "o", session_id: "s", reason: "acceptance_stamp_clear_failed", retryable: false };
    for (const k of Object.keys(fields)) expect(ALLOWED_LOG_FIELDS.has(k)).toBe(true);
    expect(JSON.parse(logLine("warn", "search index acceptance stamp not written", fields))).toMatchObject(fields);
  });
});

describe("flag on: streamed path", () => {
  const streamed = (extra: Partial<SearchIndexDeps> = {}) => ({
    ...ON, limits: { maxScopeBytes: 100 }, stream: { maxReadBytes: 64 * 1024 * 1024, maxReadEvents: 1_000_000 }, ...extra,
  });

  test("a streamed acceptance stop stamps; the repeat skips without taking the stream slot", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    const slot = { used: false };
    const s = searchDeps(ch.ch, streamed({ streamSlot: slot }));
    const first = await indexSearchScope(scope, s.deps);
    expect(first).toEqual({ manifests: 2, rows: 0, inserted: 0, deleted: 0, complete: false, streamed: true, reasons: [ACCEPTANCE_MISSING], contenderSeats: "all_unseated" });
    expect(stampsOf(ch.ingest)).toHaveLength(1);
    expect(JSON.parse(stampsOf(ch.ingest)[0]!.value).digest).toBe(
      await currentDigest("default:14d", { maxScopeBytes: 100, maxScopeEvents: 100_000 }, { maxReadBytes: 64 * 1024 * 1024, maxReadEvents: 1_000_000 }),
    );
    const fresh = { used: false };
    const again = searchDeps(ch.ch, streamed({ streamSlot: fresh }));
    const writes = ch.writes.length;
    expect(await indexSearchScope(scope, again.deps)).toEqual(SKIPPED);
    expect(again.reads).toEqual([]);
    expect(fresh.used).toBe(false);
    expect(ch.writes.length).toBe(writes);
  }, 60_000);

  test("deferred passes neither stamp nor clear", async () => {
    await acceptanceScope();
    const ch = fakeClickHouse();
    ch.ingest.push(stampRow({ v: 1, reason: ACCEPTANCE_MISSING, digest: "0".repeat(64) }, NOW.getTime() - 1_000));
    const s = searchDeps(ch.ch, streamed({ streamSlot: { used: true } }));
    expect(await indexSearchScope(scope, s.deps)).toMatchObject({ deferred: true });
    expect(stampsOf(ch.ingest)).toHaveLength(1);
  }, 60_000);
});

describe("consumer", () => {
  test("a skipped pass is acknowledged as the acceptance outcome: no failure, no requeue, one reconciled line", async () => {
    const { b } = await acceptanceScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, ON);
    await indexSearchScope(scope, s.deps);
    const requeued: string[] = [];
    const lines: Record<string, unknown>[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    const grab = (chunk: unknown) => {
      for (const line of String(chunk).split("\n")) if (line.startsWith("{")) lines.push(JSON.parse(line));
      return true;
    };
    process.stdout.write = grab as typeof process.stdout.write;
    process.stderr.write = grab as typeof process.stderr.write;
    let response;
    try {
      response = await processBatch([{ messageId: "m1", body: JSON.stringify({ reconcile: { key: b.key } }) }], {
        pointer: pointerDeps(), search: s.deps, requeue: async (k) => void requeued.push(k),
      });
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    expect(response).toEqual({ batchItemFailures: [] });
    expect(requeued).toEqual([]);
    expect(s.reads).toHaveLength(2);
    const search = lines.filter((l) => l.stage === "search-index");
    expect(search.map((l) => [l.level, l.message, l.reason])).toEqual([["info", "search index reconciled", `${ACCEPTANCE_MISSING},${SKIPPED_BEFORE_READ}`]]);
    expect(search[0]).toMatchObject({ manifests: 0, count: 0, inserted: 0, deleted: 0, ok: false });
  }, 60_000);
});

const env = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "/96bulls/", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};

describe("SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED", () => {
  test("unset, empty or false: the env is exactly as without it", () => {
    const plain = lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1" });
    expect(Object.keys(plain)).not.toContain("searchSkipAcceptanceMissing");
    for (const value of ["", "false"]) expect(lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: value })).toEqual(plain);
  });

  test("exactly true with SEARCH_INDEX_ENABLED=1 arms it, independent of the over-bound flag", () => {
    expect(lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" })).toEqual({ ...lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1" }), searchSkipAcceptanceMissing: true });
    expect(lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" })).not.toHaveProperty("searchSkipOverBound");
  });

  test("refuses any other value, and true without SEARCH_INDEX_ENABLED=1", () => {
    for (const value of ["1", "0", "TRUE", "True", " true", "true ", "yes", "on"])
      expect(() => lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: value })).toThrow("session pipeline: SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED must be true or false");
    for (const enabled of [undefined, "0", "true"])
      expect(() => lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: enabled, SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" })).toThrow("session pipeline: SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  });

  test("init wires it into the search deps only when armed (source check)", () => {
    const source = readFileSync(join(import.meta.dir, "../lambda.ts"), "utf8");
    const search = source.slice(source.indexOf("deps.search = {"), source.indexOf("};", source.indexOf("deps.search = {")));
    expect(search).toContain("...(env.searchSkipAcceptanceMissing ? { skipAcceptanceMissing: true as const } : {}),");
  });
});
