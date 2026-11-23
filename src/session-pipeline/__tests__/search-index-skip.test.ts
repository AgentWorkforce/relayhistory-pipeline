/**
 * SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED (search-index.ts overBoundBeforeRead): with the flag on,
 * a scope whose live scrubbed pointers' stored raw_bytes are over the whole-read bound, which
 * the streamed path could not take, and whose latest index state already records the refusal
 * at the highest live pointer revision, returns before any manifest read and writes nothing.
 * Anything else runs the normal pass. The pointer rows are written by the real pointer stage
 * into PGlite with every Supabase and HoopDb migration, so raw_bytes, scrubbed and revision
 * are what production SQL stores.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
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
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { SKIPPED_BEFORE_READ, SKIP_STATE_MAX_AGE_MS, indexSearchScope, type SearchIndexDeps, type SearchIndexResult } from "../search-index.ts";
import { STREAM_PASSES } from "../search-index-stream.ts";
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
/** Each manifest holds about 2000 raw bytes; two are over this bound, one is not. */
const BOUND = 3_000;
const SKIPPED: SearchIndexResult = {
  manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, skippedBeforeRead: true,
  reasons: ["scope_too_large", SKIPPED_BEFORE_READ],
};

let root: string;
let store: ObjectStore;
let reader: ManifestReader;
let pg: PGlite;
beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-skip-"));
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

/** One manifest of `records` session events (about 2000 raw bytes for 8) for `machine`. */
async function writeManifest(machine: string, revision = 1, opts: { records?: number; unscrubbed?: boolean } = {}) {
  const location = loc(machine);
  const lines = Array.from({ length: opts.records ?? 8 }, (_, i) =>
    JSON.stringify({ kind: "session_event", origin_id: "o", record_id: `${machine}-r${i}`, revision: String(revision), operation: "upsert", received_at: AT, payload: { ts_ms: Date.parse(AT) + i, text: "x".repeat(40), role: "assistant" } }),
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
/** The pointer stage only (no search deps): the session_index row as production SQL writes it. */
async function pointer(key: string, name = "ObjectCreated:Put") {
  seq++;
  const out = await processBatch([{ messageId: `p${seq}`, body: s3Body(key, name, String(seq).padStart(4, "0")) }], { pointer: pointerDeps() });
  expect(out.batchItemFailures).toEqual([]);
}

/** ClickHouse in memory: the state table answers the skip's read; every write is counted. */
function fakeClickHouse() {
  const state: { org_id: string; session_id: string; revision: number; synced_at: string; scanned_rows: number; needs_full: number }[] = [];
  const writes: string[] = [];
  const stateReads: string[] = [];
  const ctl: { stateThrows: boolean; syncedMs?: unknown } = { stateThrows: false };
  const ch: ClickHouseClient = {
    database: "test",
    async command(query) {
      writes.push(`command ${query.trim().slice(0, 40)}`);
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes(`FROM ${SESSION_STATE_TABLE}`)) {
        stateReads.push(query);
        if (ctl.stateThrows) throw new Error("clickhouse unavailable");
        const p = options!.params!;
        const latest = state
          .filter((s) => s.org_id === p.org && s.session_id === p.session)
          .sort((a, b) => (a.synced_at < b.synced_at ? 1 : -1))[0];
        // As the client's JSON output (output_format_json_quote_64bit_integers=0): numbers.
        const syncedMs = "syncedMs" in ctl ? ctl.syncedMs : latest ? Date.parse(`${latest.synced_at.replace(" ", "T")}Z`) : undefined;
        return (latest ? [{ revision: Number(latest.revision), needs_full: latest.needs_full, scanned_rows: latest.scanned_rows, synced_ms: syncedMs }] : []) as T[];
      }
      return [] as T[];
    },
    async insert(table, rows) {
      writes.push(`insert ${table}`);
      if (table === SESSION_STATE_TABLE) state.push(...(rows as typeof state));
      else if (table !== EVENTS_TABLE) throw new Error(`unexpected insert into ${table}`);
    },
  };
  return { ch, state, writes, stateReads, ctl };
}

/** The search stage over PGlite, with every manifest read and every Postgres statement counted. */
function searchDeps(ch: ClickHouseClient, extra: Partial<SearchIndexDeps> = {}, afterListing?: (t: { query: (text: string) => Promise<unknown> }) => Promise<void>) {
  const reads: string[] = [];
  const sql: string[] = [];
  const transact = async <T>(work: (tx: SqlExecutor) => Promise<T>) =>
    pg.transaction(async (t) =>
      work({
        async query<R>(text: string, params?: unknown[]) {
          sql.push(text.trim().replace(/\s+/g, " "));
          const rows = (await t.query(text, params as unknown[])).rows as R[];
          // Test seam: change the scope's rows right after livePointerKeys listed them.
          if (afterListing && text.includes("SELECT manifest_key FROM sessions.session_index")) await afterListing(t);
          return { rows };
        },
      }),
    );
  const deps: SearchIndexDeps = {
    transact: transact as SearchIndexDeps["transact"],
    reader: { ...reader, read: async (k) => { reads.push(k); return reader.read(k); } },
    store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW,
    writeCatalog: async () => ({ written: true }),
    limits: { maxScopeBytes: BOUND },
    ...extra,
  };
  return { deps, reads, sql };
}

/** Two live scrubbed manifests, together over BOUND, each under it; pointers indexed. */
async function overBoundScope() {
  const a = await writeManifest("srv_aa");
  const b = await writeManifest("srv_bb");
  expect(a.bytes).toBeLessThan(BOUND);
  expect(a.bytes + b.bytes).toBeGreaterThan(BOUND);
  await pointer(a.key);
  await pointer(b.key);
  return { a, b, total: a.bytes + b.bytes };
}

const REFUSED = { manifests: 2, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large"] };

describe("flag off", () => {
  test("a refused scope reads every manifest each pass; no pointer-facts query, no state read", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch);
    for (let i = 0; i < 2; i++) {
      const before = s.reads.length;
      expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
      expect(s.reads.length - before).toBe(2);
    }
    expect(ch.stateReads).toEqual([]);
    expect(ch.state).toHaveLength(2);
    expect(s.sql.some((q) => q.includes("sum(raw_bytes)"))).toBe(false);
  }, 60_000);
});

describe("flag on", () => {
  test("first pass over the bound: full read and refusal as before (no state row yet)", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    expect(s.reads).toHaveLength(2);
    expect(ch.stateReads).toHaveLength(1);
    expect(ch.state).toEqual([expect.objectContaining({ revision: 1, needs_full: 1, scanned_rows: 0 })]);
  }, 60_000);

  test("repeat at the same revision: no manifest read, no ClickHouse write, the skipped result", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    await indexSearchScope(scope, s.deps);
    const reads = s.reads.length;
    const writes = ch.writes.length;
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    expect(s.reads.length).toBe(reads);
    expect(ch.writes.length).toBe(writes);
    expect(ch.state).toHaveLength(1);
  }, 60_000);

  test("a new pointer revision takes the normal pass, then the next repeat skips", async () => {
    const { a } = await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    await indexSearchScope(scope, s.deps);
    await writeManifest("srv_aa", 2);
    await pointer(a.key);
    const reads = s.reads.length;
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    expect(s.reads.length - reads).toBe(2);
    expect(ch.state.at(-1)).toMatchObject({ revision: 2, needs_full: 1 });
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
  }, 60_000);

  test("a pointer deleted so the set fits takes the normal pass (and completes)", async () => {
    const { b } = await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    await indexSearchScope(scope, s.deps);
    await store.delete(objectKey(b.key));
    await pointer(b.key, "ObjectRemoved:DeleteMarkerCreated");
    const reads = s.reads.length;
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 1, complete: true, reasons: [] });
    expect(s.reads.length - reads).toBe(1);
  }, 60_000);

  test("limits raised above the size take the normal pass (and complete)", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    await indexSearchScope(scope, searchDeps(ch.ch, { skipOverBound: true }).deps);
    const raised = searchDeps(ch.ch, { skipOverBound: true, limits: { maxScopeBytes: 64 * 1024 } });
    const result = await indexSearchScope(scope, raised.deps);
    expect(result).toMatchObject({ manifests: 2, complete: true, reasons: [] });
    expect(raised.reads).toHaveLength(2);
  }, 60_000);

  test("unscrubbed pointers are not counted: scrubbed bytes under the bound never skip", async () => {
    const a = await writeManifest("srv_aa");
    const q = await writeManifest("srv_qq", 1, { unscrubbed: true, records: 40 });
    await pointer(a.key);
    await pointer(q.key);
    expect(a.bytes + q.bytes).toBeGreaterThan(BOUND);
    const ch = fakeClickHouse();
    // A state row that would match, so only the scrubbed filter keeps this from skipping.
    ch.state.push({ org_id: scope.orgId, session_id: scope.sessionId, revision: 1, synced_at: "2026-11-01 11:00:00.000", scanned_rows: 0, needs_full: 1 });
    const s = searchDeps(ch.ch, { skipOverBound: true });
    const result = await indexSearchScope(scope, s.deps);
    expect(result.skippedBeforeRead).toBeUndefined();
    expect(s.reads).toHaveLength(2);
    expect(ch.stateReads).toEqual([]);
  }, 60_000);

  test("state read throws: the normal pass runs", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    await indexSearchScope(scope, s.deps);
    ch.ctl.stateThrows = true;
    const reads = s.reads.length;
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    expect(s.reads.length - reads).toBe(2);
  }, 60_000);

  test("state row not a refusal at this revision (needs_full 0, rows scanned, other revision): the normal pass runs", async () => {
    await overBoundScope();
    for (const row of [{ revision: 1, needs_full: 0, scanned_rows: 0 }, { revision: 1, needs_full: 1, scanned_rows: 5 }, { revision: 7, needs_full: 1, scanned_rows: 0 }]) {
      const ch = fakeClickHouse();
      ch.state.push({ org_id: scope.orgId, session_id: scope.sessionId, synced_at: "2026-11-01 11:00:00.000", ...row });
      const s = searchDeps(ch.ch, { skipOverBound: true });
      expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
      expect(s.reads).toHaveLength(2);
    }
  }, 60_000);
});

const at = (ms: number) => new Date(ms).toISOString().replace("T", " ").replace("Z", "");
const refusalRow = (synced_at: string, revision = 1) => ({ org_id: scope.orgId, session_id: scope.sessionId, revision, synced_at, scanned_rows: 0, needs_full: 1 });

describe("flag on: state age bound", () => {
  test("a row just under the bound skips; one older runs the normal pass, whose refusal makes the next pass skip", async () => {
    await overBoundScope();
    const fresh = fakeClickHouse();
    fresh.state.push(refusalRow(at(NOW.getTime() - SKIP_STATE_MAX_AGE_MS + 1_000)));
    expect(await indexSearchScope(scope, searchDeps(fresh.ch, { skipOverBound: true }).deps)).toEqual(SKIPPED);

    const ch = fakeClickHouse();
    ch.state.push(refusalRow(at(NOW.getTime() - SKIP_STATE_MAX_AGE_MS - 1_000)));
    const s = searchDeps(ch.ch, { skipOverBound: true });
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    expect(s.reads).toHaveLength(2);
    expect(ch.state).toHaveLength(2);
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("a row far in the future does not skip", async () => {
    await overBoundScope();
    const ch = fakeClickHouse();
    ch.state.push(refusalRow(at(NOW.getTime() + SKIP_STATE_MAX_AGE_MS + 1_000)));
    const s = searchDeps(ch.ch, { skipOverBound: true });
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("a non-numeric or missing synced_ms does not skip", async () => {
    await overBoundScope();
    for (const syncedMs of ["not-a-number", undefined, null, {}]) {
      const ch = fakeClickHouse();
      ch.state.push(refusalRow("2026-11-01 11:00:00.000"));
      ch.ctl.syncedMs = syncedMs;
      const s = searchDeps(ch.ch, { skipOverBound: true });
      expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
      expect(s.reads).toHaveLength(2);
    }
  }, 60_000);
});

describe("flag on: boundaries", () => {
  test("a scrubbed sum exactly at maxScopeBytes does not skip (the bound is strict)", async () => {
    const { total } = await overBoundScope();
    const ch = fakeClickHouse();
    ch.state.push(refusalRow("2026-11-01 11:00:00.000"));
    const s = searchDeps(ch.ch, { skipOverBound: true, limits: { maxScopeBytes: total } });
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toMatchObject({ manifests: 2, complete: true, reasons: [] });
    expect(s.reads).toHaveLength(2);
  }, 60_000);

  test("an unscrubbed live pointer holding the highest revision never skips", async () => {
    await overBoundScope();
    const q = await writeManifest("srv_qq", 2, { unscrubbed: true, records: 1 });
    await pointer(q.key);
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
    const refused = { manifests: 2, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["manifest_unscrubbed", "scope_too_large"] };
    expect(await indexSearchScope(scope, s.deps)).toEqual(refused);
    // The refusal records the highest readable revision (1), not the pointer's highest (2).
    expect(ch.state.at(-1)).toMatchObject({ revision: 1, needs_full: 1, scanned_rows: 0 });
    expect(await indexSearchScope(scope, s.deps)).toEqual(refused);
    expect(s.reads).toHaveLength(6);
  }, 60_000);

  test("live rows changed between the listing and the facts query: the normal pass runs", async () => {
    await overBoundScope();
    const c = await writeManifest("srv_cc");
    await pointer(c.key);
    const ch = fakeClickHouse();
    ch.state.push(refusalRow("2026-11-01 11:00:00.000"));
    // Without the change the pass would skip (the two remaining pointers alone are over the bound).
    const s = searchDeps(ch.ch, { skipOverBound: true }, async (t) => {
      await t.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 'sess_1' AND machine_id = 'srv_cc'");
    });
    const result = await indexSearchScope(scope, s.deps);
    expect(result).toEqual({ ...REFUSED, manifests: 3 });
    expect(s.reads).toHaveLength(3);
  }, 60_000);
});

describe("flag on with stream deps", () => {
  test("a scope the streamed path could take is never skipped", async () => {
    const { total } = await overBoundScope();
    const ch = fakeClickHouse();
    // The recorded refusal is in place; only the stream guard keeps this pass from skipping.
    ch.state.push({ org_id: scope.orgId, session_id: scope.sessionId, revision: 1, synced_at: "2026-11-01 11:00:00.000", scanned_rows: 0, needs_full: 1 });
    const s = searchDeps(ch.ch, { skipOverBound: true, stream: { maxReadBytes: total * STREAM_PASSES, maxReadEvents: 1_000_000 }, streamSlot: { used: false } });
    const result = await indexSearchScope(scope, s.deps);
    expect(result.skippedBeforeRead).toBeUndefined();
    expect(result.streamed).toBe(true);
    expect(s.reads).toHaveLength(2);
    expect(ch.stateReads).toEqual([]);
  }, 60_000);

  test("a scope beyond the stream budgets is refused once, then skipped", async () => {
    const { total } = await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true, stream: { maxReadBytes: total * STREAM_PASSES - 1, maxReadEvents: 1_000_000 }, streamSlot: { used: false } });
    expect(await indexSearchScope(scope, s.deps)).toEqual(REFUSED);
    const reads = s.reads.length;
    const writes = ch.writes.length;
    expect(await indexSearchScope(scope, s.deps)).toEqual(SKIPPED);
    expect(s.reads.length).toBe(reads);
    expect(ch.writes.length).toBe(writes);
  }, 60_000);
});

describe("consumer", () => {
  test("a skipped pass is handled as the size refusal: no failure, no requeue, logs through the real log function", async () => {
    const { b } = await overBoundScope();
    const ch = fakeClickHouse();
    const s = searchDeps(ch.ch, { skipOverBound: true });
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
    const reads = s.reads.length;
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
    expect(s.reads.length).toBe(reads);
    const search = lines.filter((l) => l.stage === "search-index");
    expect(search.map((l) => [l.level, l.message, l.reason])).toEqual([
      ["warn", "search index partial", "scope_too_large"],
      ["info", "search index reconciled", `scope_too_large,${SKIPPED_BEFORE_READ}`],
    ]);
    expect(search[1]).toMatchObject({ manifests: 0, count: 0, inserted: 0, deleted: 0, ok: false });
  }, 60_000);
});

const env = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "/96bulls/", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};

describe("SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED", () => {
  test("unset, empty or false: the env is exactly as without it", () => {
    const plain = lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1" });
    expect(Object.keys(plain)).not.toContain("searchSkipOverBound");
    for (const value of ["", "false"]) expect(lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: value })).toEqual(plain);
  });

  test("exactly true with SEARCH_INDEX_ENABLED=1 arms it", () => {
    expect(lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true" })).toEqual({ ...lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1" }), searchSkipOverBound: true });
  });

  test("refuses any other value, and true without SEARCH_INDEX_ENABLED=1", () => {
    for (const value of ["1", "0", "TRUE", "True", " true", "true ", "yes", "on"])
      expect(() => lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: value })).toThrow("session pipeline: SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED must be true or false");
    for (const enabled of [undefined, "0", "true"])
      expect(() => lambdaEnv({ ...env, SEARCH_INDEX_ENABLED: enabled, SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true" })).toThrow("session pipeline: SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  });

  test("init wires it into the search deps only when armed (source check, as for the stream flag)", () => {
    const source = readFileSync(join(import.meta.dir, "../lambda.ts"), "utf8");
    const search = source.slice(source.indexOf("deps.search = {"), source.indexOf("};", source.indexOf("deps.search = {")));
    expect(search).toContain("...(env.searchSkipOverBound ? { skipOverBound: true as const } : {}),");
  });
});
