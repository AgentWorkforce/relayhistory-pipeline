/**
 * Golden transcript of the S3 -> ClickHouse search stage with SEARCH_INDEX_STREAMING_CH
 * unset (the deployed behaviour). Fixed inputs run through the SQS consumer and
 * indexSearchScope; every ClickHouse statement (text, params, settings, rows), every
 * Postgres statement (text, params), every log line (minus its clock fields) and every
 * result is recorded, and the whole transcript must equal the committed snapshot
 * (__snapshots__/search-index-golden.test.ts.snap).
 *
 * The snapshot was written before the streamed reconcile was ported and must not
 * change with it: with the flag off the stage is byte-for-byte what it was.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
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
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { memoryLock } from "./support.ts";

const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const PREFIX = "96bulls/org_1";
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string): SessionLocation => ({
  prefix: PREFIX, workspaceId: scope.workspaceId, machineId, harness: scope.harness, sessionId: scope.sessionId, startDate: "2026-11-01",
});
const t = (s: number) => Date.parse(`2026-11-01T10:00:${String(s).padStart(2, "0")}Z`);
const rec = (kind: string, record_id: string, revision: string, payload: Record<string, unknown> | null, o: Partial<RelayhistoryStreamRecord> = {}): RelayhistoryStreamRecord => ({
  kind, origin_id: "desktop-origin", record_id, revision, operation: "upsert", received_at: AT, payload, ...o,
});
const say = (id: string, rev: string, s: number, text: string, o: Partial<RelayhistoryStreamRecord> = {}) =>
  rec("session_event", id, rev, { ts_ms: t(s), text, role: "assistant" }, o);

let root: string;
let store: ObjectStore;
let reader: ManifestReader;

/** Write one manifest (one chunk per entry of `chunks`) for `machine`; `declare` overrides the chunk sizes it declares. */
async function writeSession(
  machine: string,
  chunks: RelayhistoryStreamRecord[][],
  revision = 1,
  opts: { declare?: { records?: number; bytes?: number }; extensions?: Record<string, unknown> } = {},
) {
  const location = loc(machine);
  const entries: ChunkEntry[] = [];
  let offset = 0;
  for (const [seq, records] of chunks.entries()) {
    const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = {
      seq, epoch: 0, start_offset: offset, bytes: source.byteLength, stored_bytes: stored.byteLength,
      sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: opts.declare?.bytes ?? source.byteLength,
      raw_sha256: sha, compression: "zstd", records: opts.declare?.records ?? records.length,
    };
    offset += source.byteLength;
    entries.push(chunk);
    await store.put(rawChunkKey(location, chunk.seq, chunk.start_offset, sha, "ndjson"), stored, {});
  }
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: TRACE, revision, created_at: AT, updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: scope.sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: entries.length,
      raw_bytes: offset, stored_bytes: entries.reduce((n, c) => n + c.stored_bytes, 0), content_sha256: await computeContentSha256(entries),
      epoch: 0, epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: machine }], next_offset: offset,
      chunks: entries, scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1" }, ...(opts.extensions ?? {}) },
  } as unknown as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}

type Entry = Record<string, unknown>;
const norm = (q: string) => q.trim().replace(/\s+/g, " ");

/** transcript_events + session state in memory; every call is appended to `log`. */
function recordingClickHouse(log: Entry[]) {
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
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: Record<string, unknown>) {
      log.push({ ch: "command", query: norm(query), options: options ?? null });
      const p = (options?.params ?? {}) as Record<string, unknown>;
      if (query.includes("{org:String}")) {
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && r.session_id === p.session) rows.splice(i, 1);
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
    async select<T>(query: string, options?: Record<string, unknown>) {
      log.push({ ch: "select", query: norm(query), options: options ?? null });
      if (!query.includes(EVENTS_TABLE)) return [] as T[];
      const p = (options?.params ?? {}) as Record<string, unknown>;
      return final()
        .filter((r) => r.org_id === p.org && r.session_id === p.session && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from))
        .slice(0, Number(p.limit))
        .map((r) => ({ workspace_id: r.workspace_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch, options?: Record<string, unknown>) {
      log.push({ ch: "insert", table, rows: batch, options: options ?? null });
      if (table === SESSION_STATE_TABLE) return;
      rows.push(...(batch as IndexRow[]));
    },
  };
  return { ch, rows, final };
}

/** Supabase as the stage sees it; every statement and its params go to `log`. */
function recordingSupabase(log: Entry[], live: () => string[], busy: () => boolean = () => false) {
  const tx: SqlExecutor = {
    async query<R>(text: string, params?: unknown[]) {
      log.push({ pg: norm(text), params: params ?? null });
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: !busy() }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key })) as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

/** Log lines written while `work` runs, without their clock fields. */
async function captureLogs(log: Entry[], work: () => Promise<unknown>) {
  const out = process.stdout.write.bind(process.stdout);
  const err = process.stderr.write.bind(process.stderr);
  const grab = (chunk: unknown) => {
    for (const line of String(chunk).split("\n")) {
      if (!line.startsWith("{")) continue;
      // Clock fields, and the packages/pipeline work_key's per-run suffix, are not behaviour.
      const { ts: _ts, duration_ms: _d, ...rest } = JSON.parse(line) as Entry;
      if (typeof rest.work_key === "string") rest.work_key = rest.work_key.replace(/@[^@]*$/, "@<run>");
      log.push({ log: rest });
    }
    return true;
  };
  process.stdout.write = grab as typeof process.stdout.write;
  process.stderr.write = grab as typeof process.stderr.write;
  try {
    return await work();
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}

/**
 * The transcript with the pointer stage's per-run values replaced: the fs store's ETag
 * (inode and mtime), the v7 session uuid (clock and random) and the trace's span id. None
 * of them is search-stage behaviour; everything else is compared as recorded.
 */
function stable(log: Entry[]): unknown {
  const ids = new Map<string, string>();
  const text = JSON.stringify(log)
    .replace(/\b[0-9a-f]{4,8}-[0-9a-f]{10,13}-([0-9a-f]{1,8})\b/g, (_m, size: string) => `<etag:${size}>`)
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}\b/g, (m) => {
      if (!ids.has(m)) ids.set(m, `<uuid${ids.size + 1}>`);
      return ids.get(m)!;
    })
    .replace(/\b00-([0-9a-f]{32})-[0-9a-f]{16}-01\b/g, "00-$1-<span>-01");
  return JSON.parse(text);
}

const s3Put = (key: string, sequencer: string) =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer } } }] });

function consumer(log: Entry[], ch: ClickHouseClient, live: () => string[], busy?: () => boolean, extra: Partial<SearchIndexDeps> = {}) {
  const sb = recordingSupabase(log, live, busy);
  const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
  const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
  const search: SearchIndexDeps = { transact: sb.transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW, ...extra };
  const deps: ConsumerDeps = {
    pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
    search,
    requeue: async (k) => void log.push({ requeue: k }),
  };
  return { deps, search };
}

const saved = { flag: process.env.SEARCH_INDEX_STREAMING_CH };
beforeAll(() => { delete process.env.SEARCH_INDEX_STREAMING_CH; });
afterAll(() => { if (saved.flag !== undefined) process.env.SEARCH_INDEX_STREAMING_CH = saved.flag; });
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-golden-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});
afterEach(async () => { await rm(root, { recursive: true, force: true }); });

describe("search stage golden transcript (SEARCH_INDEX_STREAMING_CH unset)", () => {
  test("index, replay, edit and retraction, foreign kinds, tool settlement, catalog, erasure", async () => {
    const log: Entry[] = [];
    const ch = recordingClickHouse(log);
    const catalog = rec("session", "session-1", "1", { project_key: "github.com/acme/widgets", project_key_method: "git_remote", repo_url: "git@github.com:acme/widgets.git", git_branch: "main", agent_name: "codex", models_json: ["gpt-5"] });
    const parent = rec("session_event", "p1", "1", { ts_ms: t(5), text: "calling", role: "assistant", tool_use_id: "tu1" });
    const tool = rec("tool_call", "c1", "1", { name: "Read", target: "a.ts", tool_use_id: "tu1", is_error: "false" });
    const edit = rec("file_edit", "e1", "1", { file_path: "a.ts", tool_use_id: "tu1" });
    const exporter = await writeSession("delivery_aa", [[say("r1", "1", 1, "one"), say("r2", "1", 2, "two")]]);
    let live = await writeSession("srv_bb", [[say("r1", "1", 1, "one"), catalog], [say("r2", "1", 2, "two"), parent, tool, edit, say("r3", "1", 3, "three")]]);
    let keys = [exporter, live];
    const { deps, search } = consumer(log, ch.ch, () => keys);
    const step = async (name: string, work: () => Promise<unknown>) => {
      log.push({ step: name });
      log.push({ result: (await captureLogs(log, work)) as Entry });
    };
    await step("index", () => processBatch([{ messageId: "m1", body: s3Put(live, "01") }, { messageId: "m2", body: s3Put(exporter, "02") }], deps));
    await step("replay", () => processBatch([{ messageId: "m3", body: s3Put(live, "03") }], deps));
    // A row of a kind S3 never carries, and a stale activity row S3 no longer has.
    const base = ch.final()[0]!;
    ch.rows.push({ ...base, kind: "tool_result", event_id: "tail:tr1", fingerprint: "neon-x" }, { ...base, event_id: "tail:stale", fingerprint: "neon-y" });
    live = await writeSession("srv_bb", [[say("r1", "1", 1, "one"), catalog], [say("r2", "1", 2, "two"), parent, tool, edit, say("r3", "1", 3, "three")], [say("r1", "2", 1, "one, edited"), say("r3", "2", 3, "three", { operation: "delete", payload: null })]], 2);
    keys = [exporter, live];
    await step("edit+retract", () => processBatch([{ messageId: "m4", body: s3Put(live, "04") }], deps));
    await step("direct", () => indexSearchScope(scope, search));
    keys = [exporter, live, manifestKey(loc("srv_missing"))];
    await step("incomplete", () => indexSearchScope(scope, search));
    keys = [];
    await step("erased", () => indexSearchScope(scope, search));
    log.push({ final: ch.final() });
    expect(stable(log)).toMatchSnapshot();
  });

  test("scopes past the whole-read bound are marked partial (declared events, declared bytes, small limits)", async () => {
    const log: Entry[] = [];
    const ch = recordingClickHouse(log);
    const step = async (name: string, work: () => Promise<unknown>) => {
      log.push({ step: name });
      log.push({ result: (await captureLogs(log, work)) as Entry });
    };
    let keys = [await writeSession("srv_bb", [[say("r1", "1", 1, "one")]], 1, { declare: { records: 100_001 } })];
    const a = consumer(log, ch.ch, () => keys);
    await step("events>100000", () => processBatch([{ messageId: "m1", body: s3Put(keys[0]!, "01") }], a.deps));
    keys = [await writeSession("srv_bb", [[say("r1", "1", 1, "one")]], 2, { declare: { bytes: 64 * 1024 * 1024 + 1 } })];
    await step("bytes>64MiB", () => processBatch([{ messageId: "m2", body: s3Put(keys[0]!, "02") }], a.deps));
    keys = [await writeSession("srv_bb", [[say("r1", "1", 1, "one"), say("r2", "1", 2, "two")], [say("r3", "1", 3, "three")]], 3)];
    const b = consumer(log, ch.ch, () => keys, undefined, { limits: { maxScopeEvents: 2 } });
    await step("events>limit", () => indexSearchScope(scope, b.search));
    await step("within limit", () => indexSearchScope(scope, a.search));
    log.push({ final: ch.final() });
    expect(stable(log)).toMatchSnapshot();
  });

  test("a busy scope is requeued; two scopes in one batch each get a pass", async () => {
    const log: Entry[] = [];
    const ch = recordingClickHouse(log);
    let busy = true;
    const key = await writeSession("srv_bb", [[say("r1", "1", 1, "one")]]);
    const { deps } = consumer(log, ch.ch, () => [key], () => busy);
    log.push({ result: (await captureLogs(log, () => processBatch([{ messageId: "m1", body: s3Put(key, "01") }], deps))) as Entry });
    busy = false;
    log.push({ result: (await captureLogs(log, () => processBatch([{ messageId: "m2", body: s3Put(key, "02") }, { messageId: "m3", body: JSON.stringify({ reconcile: { key } }) }, { messageId: "m4", body: "not json" }], deps))) as Entry });
    log.push({ final: ch.final() });
    expect(stable(log)).toMatchSnapshot();
  });
});
