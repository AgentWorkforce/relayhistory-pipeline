import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
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
  type SessionLocation,
} from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE } from "../vendor/search-index/schema.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { reconcileRecent } from "../lambda.ts";

const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const PREFIX = "96bulls/org_1";
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string): SessionLocation => ({
  prefix: PREFIX,
  workspaceId: scope.workspaceId,
  machineId,
  harness: scope.harness,
  sessionId: scope.sessionId,
  startDate: "2026-11-01",
});
const event = (record_id: string, revision: string, ts_ms: number, text: string, overrides: Partial<RelayhistoryStreamRecord> = {}): RelayhistoryStreamRecord => ({
  kind: "session_event",
  origin_id: "desktop-origin",
  record_id,
  revision,
  operation: "upsert",
  received_at: AT,
  payload: { ts_ms, text, role: "assistant" },
  ...overrides,
});

let root: string;
let store: ObjectStore;
let reader: ManifestReader;

beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-session-pipeline-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

/** Write one manifest (one chunk) for `machine` and return its key. */
async function writeSession(machine: string, records: RelayhistoryStreamRecord[], revision = 1, opts: { scrubbed?: boolean } = {}) {
  const location = loc(machine);
  const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
  const stored = new Uint8Array(zstdCompressSync(source));
  const sha = await sha256Hex(source);
  const chunk: ChunkEntry = {
    seq: 0,
    epoch: 0,
    start_offset: 0,
    bytes: source.byteLength,
    stored_bytes: stored.byteLength,
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored),
    stored_raw_bytes: source.byteLength,
    raw_sha256: sha,
    compression: "zstd",
    records: records.length,
  };
  await store.put(rawChunkKey(location, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest: ManifestV1 = {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision,
    created_at: AT,
    updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: scope.sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine,
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: chunk.bytes,
      stored_bytes: chunk.stored_bytes,
      content_sha256: await computeContentSha256([chunk]),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: machine }],
      next_offset: source.byteLength,
      chunks: [chunk],
      ...(opts.scrubbed === false ? {} : { scrub: { ruleset: "relayhistory-scrub", version: 1 } }),
    },
    extensions: { "96bulls": { user_id: "user_1" } },
  } as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}

/** An in-memory transcript_events table speaking the four statements the indexer sends. */
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
  let failInsert = false;
  const ch: ClickHouseClient = {
    database: "test",
    async command(query) {
      if (!query.startsWith("DELETE FROM")) throw new Error(`unexpected command ${query.slice(0, 30)}`);
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) =>
        [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!),
      );
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples) {
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && r.event_id === eventId && tsMs(r) === Number(ms))
            rows.splice(i, 1);
        }
      }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_state")) return [] as T[];
      const p = options!.params!;
      return final()
        .filter((r) => r.org_id === p.org && r.session_id === p.session && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from))
        .slice(0, Number(p.limit))
        .map((r) => ({ workspace_id: r.workspace_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      if (failInsert) throw Object.assign(new Error("boom"), { code: "241" });
      expect(table).toBe(EVENTS_TABLE);
      rows.push(...(batch as IndexRow[]));
    },
  };
  return { ch, rows, final, fail: (on: boolean) => (failInsert = on) };
}

/** Supabase as the indexer sees it: the live pointer keys of the scope, plus a call log. */
function fakeSupabase(live: () => string[]) {
  const calls: string[] = [];
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      calls.push(text.trim().replace(/\s+/g, " "));
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key })) as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, calls, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

function searchDeps(ch: ClickHouseClient, transact: SearchIndexDeps["transact"]): SearchIndexDeps {
  return { transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW };
}

describe("search index from S3", () => {
  test("two manifests carrying the same delivery records index each event once, by its Neon identity", async () => {
    const records = [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "first"), event("r2", "1", Date.parse("2026-11-01T10:00:02Z"), "second")];
    const exporter = await writeSession("delivery_aa", records);
    const live = await writeSession("srv_bb", [...records, event("r3", "1", Date.parse("2026-11-01T10:00:03Z"), "third")]);
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => [exporter, live]);

    const first = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(first).toMatchObject({ manifests: 2, rows: 3, inserted: 3, deleted: 0, complete: true });
    const machines = new Set(ch.final().map((r) => r.machine_id));
    expect(machines.size).toBe(1);
    expect([...machines][0]).toMatch(/^delivery:[0-9a-f]{32}$/);
    expect(ch.final().map((r) => r.content).sort()).toEqual(["first", "second", "third"]);
    expect(ch.final().every((r) => r.org_id === "org_1" && r.user_id === "user_1" && r.fingerprint.startsWith("s3"))).toBe(true);
    // The pointer listing ran under the scope's advisory lock.
    expect(sb.calls.some((c) => c.startsWith("SELECT pg_advisory_xact_lock"))).toBe(true);

    // Replay: nothing changes.
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ inserted: 0, deleted: 0, complete: true });
  });

  test("a retracted record, an edit and an erasure propagate; an incomplete read never deletes", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(ch.final()).toHaveLength(2);

    // Revision 2: r1 edited, r2 deleted at the source.
    keys = [
      await writeSession(
        "srv_bb",
        [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two"), event("r1", "2", t(1), "one, edited"), event("r2", "2", t(2), "two", { operation: "delete", payload: null })],
        2,
      ),
    ];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ inserted: 1, deleted: 1, complete: true });
    expect(ch.final().map((r) => r.content)).toEqual(["one, edited"]);

    // A live pointer whose manifest is gone (its delete is in flight): no deletes.
    const ghost = manifestKey(loc("srv_gone"));
    keys = [...keys, ghost];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: false, deleted: 0, reasons: ["manifest_missing"] });
    expect(ch.final()).toHaveLength(1);

    // Erasure: no live pointer left, every row of the scope goes.
    keys = [];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ deleted: 1, complete: true });
    expect(ch.final()).toHaveLength(0);
  });

  test("unscrubbed raw is never read or indexed; events outside the window are skipped", async () => {
    const quarantined = await writeSession("srv_q", [event("q1", "1", Date.parse("2026-11-01T10:00:01Z"), "secret-ish")], 1, { scrubbed: false });
    const old = await writeSession("delivery_old", [event("o1", "1", Date.parse("2026-09-01T10:00:00Z"), "too old")]);
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => [quarantined, old]);
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(result).toMatchObject({ inserted: 0, reasons: ["manifest_unscrubbed"] });
    expect(ch.final()).toHaveLength(0);
  });
});

describe("SQS consumer", () => {
  const s3Put = (key: string, sequencer: string) =>
    JSON.stringify({
      Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key: encodeURIComponent(key).replace(/%2F/g, "/"), eTag: "e", sequencer } } }],
    });

  function consumer(ch: ClickHouseClient, live: () => string[]) {
    const sb = fakeSupabase(live);
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const deps: ConsumerDeps = {
      pointer: { db, reader, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: searchDeps(ch, sb.transact),
    };
    return { deps, sb };
  }

  test("a manifest put upserts the pointer, then indexes its session; duplicates in a batch collapse", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    const { deps, sb } = consumer(ch.ch, () => [key]);
    const out = await processBatch(
      [
        { messageId: "m1", body: s3Put(key, "01") },
        { messageId: "m2", body: s3Put(key, "02") },
        { messageId: "m3", body: JSON.stringify({ Event: "s3:TestEvent" }) },
        { messageId: "m4", body: s3Put(`${PREFIX}/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_1/raw/0000000000-0-aaaaaaaa.ndjson.zst`, "03") },
      ],
      deps,
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(sb.calls.filter((c) => c.includes("upsert_session_index"))).toHaveLength(1);
    expect(ch.final().map((r) => r.content)).toEqual(["hello"]);
  });

  test("a delete is checked against S3: an old version deleted keeps the pointer, a gone manifest tombstones it", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const s3Delete = (k: string) =>
      JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectRemoved:Delete", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key: k, versionId: "old", sequencer: "05" } } }] });
    let { deps, sb } = consumer(fakeClickHouse().ch, () => [key]);
    expect((await processBatch([{ messageId: "d1", body: s3Delete(key) }], deps)).batchItemFailures).toEqual([]);
    expect(sb.calls.some((c) => c.includes("upsert_session_index"))).toBe(true);
    expect(sb.calls.some((c) => c.includes("SET deleted_at"))).toBe(false);

    const gone = manifestKey(loc("srv_gone"));
    ({ deps, sb } = consumer(fakeClickHouse().ch, () => []));
    expect((await processBatch([{ messageId: "d2", body: s3Delete(gone) }], deps)).batchItemFailures).toEqual([]);
    expect(sb.calls.some((c) => c.includes("SET deleted_at"))).toBe(true);
    expect(sb.calls.some((c) => c.includes("upsert_session_index"))).toBe(false);
  });

  test("a key outside the deployment prefix is ignored; a failed index fails only its messages", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    const { deps } = consumer(ch.ch, () => [key]);
    ch.fail(true);
    const out = await processBatch(
      [
        { messageId: "ok", body: s3Put("other/org_1/v1/ws=ws_1/m=x/h=claude/d=2026/11/01/s=s/manifest.json", "01") },
        { messageId: "bad", body: s3Put(key, "02") },
        { messageId: "junk", body: "{" },
      ],
      deps,
    );
    expect(out.batchItemFailures.map((f) => f.itemIdentifier).sort()).toEqual(["bad", "junk"]);
  });
});

describe("reconcile", () => {
  test("enqueues only recent manifests whose pointer is missing or stale", async () => {
    const indexed = await writeSession("srv_ok", [event("r1", "1", 1, "x")]);
    const missing = await writeSession("srv_new", [event("r2", "1", 1, "y")]);
    const listed = await store.list(objectKey(PREFIX));
    const etagOf = (k: string) => listed.items.find((i) => String(i.key) === k)!.version.eTag!.replace(/^"|"$/g, "");
    const sent: string[] = [];
    const db: SqlExecutor = {
      async query<R>() {
        return { rows: [{ manifest_key: indexed, manifest_etag: etagOf(indexed), deleted: false }] as R[] };
      },
    };
    const summary = await reconcileRecent(
      { store, db, keyPrefix: "96bulls", lookbackMinutes: 60, send: async (b) => void sent.push(b) },
      new Date(Date.now() + 60_000),
    );
    expect(summary).toMatchObject({ listed: 2, recent: 2, enqueued: 1, complete: true });
    expect(JSON.parse(sent[0]!).reconcile.key).toBe(missing);
  });
});
