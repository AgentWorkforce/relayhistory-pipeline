import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
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
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import * as pointerModule from "../pointer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { memoryLock } from "./support.ts";
import { RECONCILE_DEADLINE_MS, reconcileRecent } from "../lambda.ts";
import { requeueBody } from "../s3-events.ts";

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
async function writeSession(
  machine: string,
  records: RelayhistoryStreamRecord[],
  revision = 1,
  opts: { scrubbed?: boolean; extensions?: Record<string, unknown> } = {},
) {
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
    extensions: {
      "96bulls": { user_id: "user_1" },
      ...(opts.extensions ?? {}),
    },
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
  const commands: string[] = [];
  const state: { session_id: string; needs_full: number }[] = [];
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      commands.push(query);
      if (!query.startsWith("DELETE FROM")) throw new Error(`unexpected command ${query.slice(0, 30)}`);
      if (query.includes("{org:String}")) {
        const p = options!.params!;
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && r.session_id === p.session) rows.splice(i, 1);
        }
        return;
      }
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) =>
        [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!),
      );
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples) {
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms))
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
      if (table === SESSION_STATE_TABLE) {
        state.push(...(batch as { session_id: string; needs_full: number }[]));
        return;
      }
      if (failInsert) throw Object.assign(new Error("boom"), { code: "241" });
      expect(table).toBe(EVENTS_TABLE);
      rows.push(...(batch as IndexRow[]));
    },
  };
  return { ch, rows, final, commands, state, fail: (on: boolean) => (failInsert = on) };
}

/** Supabase as the indexer sees it: the live pointer keys of the scope, plus a call log. */
function fakeSupabase(live: () => string[], busy: () => boolean = () => false) {
  const calls: string[] = [];
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      calls.push(text.trim().replace(/\s+/g, " "));
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: !busy() }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key })) as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, calls, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

function searchDeps(ch: ClickHouseClient, transact: SearchIndexDeps["transact"], limits?: SearchIndexDeps["limits"]): SearchIndexDeps {
  return { transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW, ...(limits ? { limits } : {}) };
}

describe("search index from S3", () => {
  test("writes project/repository catalog only after a complete canonical scope projection", async () => {
    const sessionRecord: RelayhistoryStreamRecord = {
      kind: "session",
      origin_id: "desktop-origin",
      record_id: "session-1",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        project_key: "github.com/acme/widgets",
        project_key_method: "git_remote",
        repo_url: "git@github.com:acme/widgets.git",
        git_branch: "main",
        agent_name: "codex",
        models_json: ["gpt-5"],
      },
    };
    let keys = [
      await writeSession("srv_bb", [sessionRecord], 1, {
        extensions: {
          "96bulls": {
            user_id: "user_1",
            canonical_session_id: "sess:1",
          },
        },
      }),
    ];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    const writes: Array<{
      key: Parameters<NonNullable<SearchIndexDeps["writeCatalog"]>>[1];
      catalog: Parameters<NonNullable<SearchIndexDeps["writeCatalog"]>>[2];
    }> = [];
    const deps: SearchIndexDeps = {
      ...searchDeps(ch.ch, sb.transact),
      writeCatalog: async (_db, key, catalog) => {
        writes.push({ key, catalog });
        return { written: true };
      },
    };
    expect(await indexSearchScope(scope, deps)).toMatchObject({ complete: true });
    expect(writes).toHaveLength(1);
    expect(writes[0]).toMatchObject({
      key: { sessionId: "sess:1" },
      catalog: {
        projectKey: "github.com/acme/widgets",
        projectId: "acme/widgets",
        gitRepo: "git@github.com:acme/widgets.git",
        gitBranch: "main",
        agentNames: ["codex"],
      },
    });

    keys = [...keys, manifestKey(loc("srv_missing"))];
    expect(await indexSearchScope(scope, deps)).toMatchObject({ complete: false });
    expect(writes).toHaveLength(1);
  });

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
    expect(sb.calls.some((c) => c.startsWith("SELECT pg_try_advisory_xact_lock"))).toBe(true);

    // Replay: nothing changes.
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ inserted: 0, deleted: 0, complete: true });
  });

  test("rows of kinds S3 never carries survive a complete pass; erasure still removes them", async () => {
    const ch = fakeClickHouse();
    let keys = [await writeSession("srv_bb", [event("r1", "1", Date.parse(AT), "one")])];
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // A row written by another ingest path (Neon tail) in a kind the exported stream never carries.
    const base = ch.final()[0]!;
    ch.rows.push({ ...base, kind: "tool_result", event_id: "tail:tr1", fingerprint: "neon-x" });
    // A stale activity-kind row S3 no longer has is still removed.
    ch.rows.push({ ...base, event_id: "tail:stale", fingerprint: "neon-y" });
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, deleted: 1 });
    expect(ch.final().some((r) => r.kind === "tool_result" && r.event_id === "tail:tr1")).toBe(true);
    expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(false);
    keys = [];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final()).toHaveLength(0);
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
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final()).toHaveLength(0);
  });

  test("an erasure removes every row of the scope however large it is, without listing (review M2)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two"), event("r3", "1", t(3), "three")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // An old row outside the index window, as the Neon backfill may have left.
    ch.rows.push({ ...ch.final()[0]!, event_id: "old", ts: "2026-08-01 00:00:00.000" });
    // Another session's rows are untouched.
    ch.rows.push({ ...ch.final()[0]!, session_id: "other" });
    keys = [];
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact, { maxRows: 1 }));
    expect(result).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final().map((r) => r.session_id)).toEqual(["other"]);
  });

  test("an unscrubbed manifest is an incomplete read: rows from an earlier revision stay (review M1)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2, { scrubbed: false })];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: false, deleted: 0, reasons: ["manifest_unscrubbed"] });
    expect(ch.final()).toHaveLength(1);
  });

  test("a manifest whose body names another session is not read and deletes nothing (review L1)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // Overwrite the manifest at srv_bb's key with a body naming machine srv_other.
    const key = keys[0]!;
    const body = JSON.parse(new TextDecoder().decode(await new Response((await store.getObject(objectKey(key))).body).arrayBuffer()));
    body.session.machine_id = "srv_other";
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(body)), { contentType: "application/json" });
    const result = await indexSearchScope(scope, { ...searchDeps(ch.ch, sb.transact), reader: { ...reader, read: async (k) => ({ manifest: JSON.parse(new TextDecoder().decode(await new Response((await store.getObject(objectKey(k))).body).arrayBuffer())) }) } });
    expect(result).toMatchObject({ complete: false, deleted: 0, inserted: 0, reasons: ["manifest_key_mismatch"] });
    expect(ch.final()).toHaveLength(1);
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

describe("search index: a bad read never deletes (review #2 L-a)", () => {
  const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
  /** Index r1 + r2, then rewrite the session so that r2 is retracted; `spoil` makes that read bad. */
  async function retractedAfter(spoil: (key: string) => Promise<void>, limits?: SearchIndexDeps["limits"]) {
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(ch.final()).toHaveLength(2);
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2)];
    await spoil(keys[0]!);
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact, limits));
    return { result, ch };
  }
  const chunkKeyOf = async (key: string) => {
    const listed = await store.list(objectKey(key.replace(/\/manifest\.json$/, "/raw")));
    return listed.items.map((i) => i.key)[listed.items.length - 1]!;
  };

  test("control: a good read deletes the retracted row", async () => {
    const { result, ch } = await retractedAfter(async () => {});
    expect(result).toMatchObject({ complete: true, deleted: 1 });
    expect(ch.final()).toHaveLength(1);
    expect(ch.state.at(-1)).toMatchObject({ needs_full: 0 });
  });

  test("an unreadable manifest (a chunk with the wrong digest)", async () => {
    const { result, ch } = await retractedAfter(async (key) => {
      await store.put(await chunkKeyOf(key), new Uint8Array(zstdCompressSync(new TextEncoder().encode("tampered\n"))), {});
    });
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["chunk_digest_mismatch"] });
    expect(ch.final()).toHaveLength(2);
    expect(ch.state.at(-1)).toMatchObject({ needs_full: 1 });
  });

  test("a projection error (a convergence_event line that is not this session's)", async () => {
    const { result, ch } = await retractedAfter(async () => {
      await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("x", "1", t(3), "x", { kind: "convergence_event", origin_id: "machine:elsewhere", payload: { source: "claude", session_id: "other", machine_id: "elsewhere", user_id: "u", event_id: "x", ts: AT } })], 2);
    });
    expect(result).toMatchObject({ complete: false, deleted: 0 });
    expect(result.reasons).toContain("relayhistory_event_identity");
    expect(ch.final()).toHaveLength(2);
  });

  test("a fold error (two copies of one revision that differ)", async () => {
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2), await writeSession("delivery_aa", [event("r1", "1", t(1), "one, but different")])];
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["relayhistory_revision_conflict"] });
    expect(ch.final().map((r) => r.event_id)).toContain(ch.final().find((r) => r.content === "two")!.event_id);
  });

  test("a truncated ClickHouse read of a live session", async () => {
    const { result, ch } = await retractedAfter(async () => {}, { maxRows: 1 });
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_truncated"] });
    expect(ch.final()).toHaveLength(2);
  });

  test("a session over the size bounds is marked partial: nothing read, written or deleted (new requirement)", async () => {
    for (const limits of [{ maxScopeBytes: 64 }, { maxScopeEvents: 0 }]) {
      const { result, ch } = await retractedAfter(async () => {}, limits);
      expect(result).toMatchObject({ partial: true, complete: false, inserted: 0, deleted: 0, reasons: ["scope_too_large"] });
      expect(ch.final()).toHaveLength(2);
      expect(ch.state.at(-1)).toMatchObject({ session_id: "sess_1", needs_full: 1 });
    }
  });
});

describe("search index: a busy session is requeued, not waited on (review #2 N2)", () => {
  test("a pass that finds the session's search lock held does nothing and re-enqueues its manifest", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    let busy = true;
    const sb = fakeSupabase(() => [key], () => busy);
    const requeued: string[] = [];
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const deps: ConsumerDeps = {
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: searchDeps(ch.ch, sb.transact),
      requeue: async (k) => void requeued.push(k),
    };
    const put = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: "01" } } }] });
    expect((await processBatch([{ messageId: "m", body: put }], deps)).batchItemFailures).toEqual([]);
    expect(requeued).toEqual([key]);
    expect(ch.final()).toHaveLength(0);
    busy = false;
    await processBatch([{ messageId: "m2", body: put }], deps);
    expect(ch.final()).toHaveLength(1);
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
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
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
      async query<R>(text: string) {
        if (text.includes("indexed_at >=")) return { rows: [] as R[] };
        return { rows: [{ manifest_key: indexed, manifest_etag: etagOf(indexed), deleted: false }] as R[] };
      },
    };
    const summary = await reconcileRecent({ store, db, keyPrefix: "96bulls", since: new Date(Date.now() - 60 * 60_000), send: async (b) => void sent.push(b) });
    expect(summary).toMatchObject({ listed: 2, recent: 2, enqueued: 1, complete: true });
    expect(JSON.parse(sent[0]!).reconcile.key).toBe(missing);
  });

  test("a live pointer whose manifest is gone is enqueued, so a lost or raced delete is repaired (review H1)", async () => {
    const live = await writeSession("srv_ok", [event("r1", "1", 1, "x")]);
    const gone = manifestKey(loc("srv_gone"));
    const sent: string[] = [];
    const db: SqlExecutor = {
      async query<R>(text: string) {
        if (text.includes("indexed_at >=")) return { rows: [{ manifest_key: live }, { manifest_key: gone }] as R[] };
        return { rows: [{ manifest_key: live, manifest_etag: "whatever", deleted: false }] as R[] };
      },
    };
    const summary = await reconcileRecent({ store, db, keyPrefix: "96bulls", since: new Date(0), send: async (b) => void sent.push(b) });
    expect(summary).toMatchObject({ pointersChecked: 2, pointersGone: 1 });
    expect(sent.map((b) => JSON.parse(b).reconcile.key)).toContain(gone);
  });

  test("stops at its deadline, under the function timeout, and returns the cursor for the next link (review M5)", async () => {
    for (const m of ["a", "b", "c"]) await writeSession(`srv_${m}`, [event("r", "1", 1, m)]);
    let clock = 0;
    const pages: (string | undefined)[] = [];
    const paged = {
      head: store.head.bind(store),
      async list(prefix: Parameters<ObjectStore["list"]>[0], cursor?: string) {
        pages.push(cursor);
        clock += 200_000; // each page "takes" 200 s
        const all = await store.list(prefix);
        const at = cursor ? Number(cursor) : 0;
        return { items: all.items.slice(at, at + 1), ...(at + 1 < all.items.length ? { nextCursor: String(at + 1) } : {}) };
      },
    };
    const db: SqlExecutor = { async query<R>() { return { rows: [] as R[] }; } };
    const first = await reconcileRecent({ store: paged, db, keyPrefix: "96bulls", since: new Date(0), send: async () => {}, now: () => clock });
    expect(RECONCILE_DEADLINE_MS).toBeLessThan(300_000);
    expect(first.complete).toBe(false);
    expect(first.nextCursor).toBe("2");
    const second = await reconcileRecent({ store: paged, db, keyPrefix: "96bulls", since: new Date(0), cursor: first.nextCursor, send: async () => {}, now: () => clock });
    expect(pages).toEqual([undefined, "1", "2", "3", "4", "5"].slice(0, pages.length));
    expect(second.pages).toBeGreaterThan(0);
  });
});

describe("latency fields (s3_event_age_ms, event_age_ms, queue_origin) on the pointer and search pass lines", () => {
  type Line = Record<string, unknown>;
  const T0 = Date.parse(AT);
  const LAG = ["event_age_ms", "queue_origin", "s3_event_age_ms"];
  // Each line's keys before the fields existed (reason is absent on a complete pass).
  const RECONCILED_KEYS = ["count", "deleted", "deployment", "deployment_id", "duration_ms", "harness", "inserted", "level", "manifests", "message", "ok", "org_id", "service", "session_id", "stage", "ts", "workspace_id"];
  const POINTER_KEYS = ["deployment", "deployment_id", "duration_ms", "harness", "level", "message", "org_id", "outcome", "revision", "service", "session_id", "stage", "ts", "workspace_id"];
  const keysOf = (l: Line | undefined) => Object.keys(l!).sort();
  const lagOf = (l: Line | undefined) => Object.fromEntries(LAG.filter((k) => k in l!).map((k) => [k, l![k]]));

  /** The real logger's JSON lines (log.info / log.warn through logLine and its allowlist) while `work` runs. */
  async function captured(work: () => Promise<unknown>): Promise<Line[]> {
    const lines: Line[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    const grab = (chunk: unknown) => {
      for (const l of String(chunk).split("\n")) if (l.startsWith("{")) lines.push(JSON.parse(l) as Line);
      return true;
    };
    process.stdout.write = grab as typeof process.stdout.write;
    process.stderr.write = grab as typeof process.stderr.write;
    try {
      await work();
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    return lines;
  }
  const reconciled = (lines: Line[]) => lines.filter((l) => l.message === "search index reconciled");
  const pointers = (lines: Line[]) => lines.filter((l) => l.message === "pointer indexed");

  // eventTime null: the record has none.
  const put = (key: string, sequencer: string, eventTime: string | null = AT, eventName = "ObjectCreated:Put") =>
    JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName, ...(eventTime === null ? {} : { eventTime }), s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer } } }] });
  const sweep = (key: string) => JSON.stringify({ reconcile: { key, etag: "e", version: "v1" } });
  /** An SQS record as the Lambda hands it on: SentTimestamp an epoch-ms decimal string. */
  const msg = (messageId: string, body: string, sentMs?: number | string) =>
    sentMs === undefined ? { messageId, body } : { messageId, body, sentTimestamp: typeof sentMs === "number" ? String(sentMs) : sentMs };

  function consumer(live: () => string[], now: () => number, busy: () => boolean = () => false, limits?: SearchIndexDeps["limits"]) {
    const ch = fakeClickHouse();
    const sb = fakeSupabase(live, busy);
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const requeued: { key: string; time?: string }[] = [];
    const deps: ConsumerDeps = {
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: searchDeps(ch.ch, sb.transact, limits),
      requeue: async (key, time) => void requeued.push(time === undefined ? { key } : { key, time }),
      now,
    };
    return { deps, requeued, ch };
  }
  const one = () => writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);

  test("one S3 put: both ages at the injected clock and queue_origin s3, on the pointer and the reconciled line; otherwise the same field sets", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    const lines = await captured(() => processBatch([msg("m1", put(key, "01"), T0 + 2_000)], deps));
    const [line, ...rest] = reconciled(lines);
    expect(rest).toEqual([]);
    expect(line).toMatchObject({ ok: true, s3_event_age_ms: 5_000, event_age_ms: 3_000, queue_origin: "s3" });
    expect(keysOf(line)).toEqual([...RECONCILED_KEYS, ...LAG].sort());
    const [pointer, ...more] = pointers(lines);
    expect(more).toEqual([]);
    expect(lagOf(pointer)).toEqual({ s3_event_age_ms: 5_000, event_age_ms: 3_000, queue_origin: "s3" });
    expect(keysOf(pointer)).toEqual([...POINTER_KEYS, ...LAG].sort());
  });

  test("several messages: each age is the oldest of its own clock, per key on the pointer line and per scope on the reconciled line", async () => {
    const a = await one();
    const b = await writeSession("delivery_aa", [event("r2", "1", Date.parse("2026-11-01T10:00:02Z"), "two")]);
    const { deps } = consumer(() => [a, b], () => T0 + 60_000);
    const lines = await captured(() =>
      processBatch(
        [
          // The oldest upload is m3 (key b); the oldest enqueue is m1 (key a): the two ages need not come from one message.
          msg("m1", put(a, "01", "2026-11-01T10:00:30.000Z"), T0 + 30_000),
          // The later sequencer wins the key, but the earlier message is still covered.
          msg("m2", put(a, "02", "2026-11-01T10:00:50.000Z"), T0 + 55_000),
          msg("m3", put(b, "03", "2026-11-01T09:59:58.000Z"), T0 + 45_000),
        ],
        deps,
      ),
    );
    const [line, ...rest] = reconciled(lines);
    expect(rest).toEqual([]);
    expect(line).toMatchObject({ ok: true, manifests: 2 });
    expect(lagOf(line)).toEqual({ s3_event_age_ms: 62_000, event_age_ms: 30_000, queue_origin: "s3" });
    expect(pointers(lines).map(lagOf)).toEqual([
      { s3_event_age_ms: 30_000, event_age_ms: 30_000, queue_origin: "s3" },
      { s3_event_age_ms: 62_000, event_age_ms: 15_000, queue_origin: "s3" },
    ]);
  });

  test("s3_event_age_ms is omitted when an upload time is unknown: a sweep body, a requeue body without a time, a put without or with a bad eventTime, or any of them in the scope", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    const batches: string[][] = [
      [sweep(key)],
      [requeueBody(key)],
      [JSON.stringify({ reconcile: { key } })],
      [put(key, "01", null)],
      [put(key, "02", "not a time")],
      [put(key, "03"), JSON.stringify({ reconcile: { key } })],
    ];
    for (const bodies of batches) {
      const lines = await captured(() => processBatch(bodies.map((body, i) => msg(`m${i}`, body, T0 + 1_000)), deps));
      const [line, ...rest] = reconciled(lines);
      expect(rest).toEqual([]);
      expect(keysOf(line)).toEqual([...RECONCILED_KEYS, "event_age_ms", "queue_origin"].sort());
      expect(line!.event_age_ms).toBe(4_000);
      expect("s3_event_age_ms" in pointers(lines)[0]!).toBe(false);
    }
  });

  test("event_age_ms is omitted when a covered message has no SentTimestamp that is a positive integer; s3_event_age_ms is unaffected", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    for (const bad of [undefined, "", "abc", "0", "-1000", "1.5", "1e12", " 1000", "9".repeat(17)]) {
      const lines = await captured(() => processBatch([msg("m1", put(key, "01"), bad)], deps));
      for (const l of [...reconciled(lines), ...pointers(lines)]) expect(lagOf(l)).toEqual({ s3_event_age_ms: 5_000, queue_origin: "s3" });
    }
    // Garbage that is not a string (a hand-built record): omitted too.
    for (const bad of [Number.NaN, -1, 1.5, null, { at: 1 }]) {
      const [line] = reconciled(await captured(() => processBatch([{ messageId: "m1", body: put(key, "02"), sentTimestamp: bad }], deps)));
      expect("event_age_ms" in line!).toBe(false);
    }
    // One good and one missing in a scope: omitted, never the good one alone.
    const lines = await captured(() => processBatch([msg("m1", put(key, "03"), T0 + 1_000), msg("m2", put(key, "04"))], deps));
    expect("event_age_ms" in reconciled(lines)[0]!).toBe(false);
    expect("event_age_ms" in pointers(lines)[0]!).toBe(false);
  });

  test("queue_origin: s3, requeue and sweep, and the mixed rule (s3 only when every message is s3, else requeue if any is, else sweep)", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    const cases: [string[], string][] = [
      [[put(key, "01")], "s3"],
      [[requeueBody(key)], "requeue"],
      [[requeueBody(key, AT)], "requeue"],
      // A requeue written with a time before the marker existed.
      [[JSON.stringify({ reconcile: { key, oldest_event_time: AT } })], "requeue"],
      [[sweep(key)], "sweep"],
      // The body every build before the marker requeued: indistinguishable from the sweep's.
      [[JSON.stringify({ reconcile: { key } })], "sweep"],
      [[put(key, "02"), put(key, "03")], "s3"],
      [[put(key, "04"), sweep(key)], "sweep"],
      [[put(key, "05"), requeueBody(key, AT)], "requeue"],
      [[sweep(key), requeueBody(key)], "requeue"],
      [[put(key, "06"), sweep(key), requeueBody(key)], "requeue"],
    ];
    for (const [bodies, origin] of cases) {
      const lines = await captured(() => processBatch(bodies.map((body, i) => msg(`m${i}`, body, T0)), deps));
      expect([origin, reconciled(lines).map((l) => l.queue_origin)]).toEqual([origin, [origin]]);
      expect([origin, pointers(lines).map((l) => l.queue_origin)]).toEqual([origin, [origin]]);
    }
  });

  test("the partial warn carries the same fields as its reconciled line", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 7_000, undefined, { maxScopeEvents: 0 });
    const lines = await captured(() => processBatch([msg("m1", put(key, "01"), T0 + 1_000)], deps));
    const partial = lines.filter((l) => l.message === "search index partial");
    expect(partial).toHaveLength(1);
    expect(partial[0]).toMatchObject({ level: "warn", reason: "scope_too_large" });
    expect(lagOf(partial[0])).toEqual({ s3_event_age_ms: 7_000, event_age_ms: 6_000, queue_origin: "s3" });
    expect(lagOf(reconciled(lines)[0])).toEqual(lagOf(partial[0]));
  });

  test("a delete carries no upload: a delete-only scope logs no s3 age, and a delete beside a put does not change the put's", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 9_000);
    const only = reconciled(await captured(() => processBatch([msg("d", put(key, "05", "2026-11-01T09:00:00.000Z", "ObjectRemoved:Delete"), T0)], deps)))[0]!;
    expect(lagOf(only)).toEqual({ event_age_ms: 9_000, queue_origin: "s3" });
    const both = reconciled(
      await captured(() =>
        processBatch([msg("p", put(key, "06", "2026-11-01T10:00:01.000Z"), T0 + 2_000), msg("d", put(key, "07", "2026-11-01T09:00:00.000Z", "ObjectRemoved:DeleteMarkerCreated"), T0 + 4_000)], deps),
      ),
    )[0]!;
    expect(lagOf(both)).toEqual({ s3_event_age_ms: 8_000, event_age_ms: 7_000, queue_origin: "s3" });
  });

  test("carried across a busy requeue: the body holds the oldest time; the later pass reports the full s3 age, its own queue wait, origin requeue", async () => {
    const key = await one();
    let busy = true;
    let clock = T0 + 1_000;
    const { deps, requeued } = consumer(() => [key], () => clock, () => busy);
    const first = await captured(() => processBatch([msg("m1", put(key, "01"), T0 + 500)], deps));
    expect(reconciled(first)).toEqual([]);
    expect(requeued).toEqual([{ key, time: AT }]);
    // The requeued line itself is unchanged.
    const busyLine = first.find((l) => l.reason === "scope_busy")!;
    for (const k of LAG) expect(k in busyLine).toBe(false);
    busy = false;
    clock = T0 + 31_000;
    const [line] = reconciled(await captured(() => processBatch([msg("m2", requeueBody(key, requeued[0]!.time), T0 + 21_000)], deps)));
    expect(lagOf(line)).toEqual({ s3_event_age_ms: 31_000, event_age_ms: 10_000, queue_origin: "requeue" });
    // A newer put beside the carried body does not hide the older carried time.
    const [both] = reconciled(await captured(() => processBatch([msg("m3", requeueBody(key, AT), T0 + 29_000), msg("m4", put(key, "02", "2026-11-01T10:00:20.000Z"), T0 + 25_000)], deps)));
    expect(lagOf(both)).toEqual({ s3_event_age_ms: 31_000, event_age_ms: 6_000, queue_origin: "requeue" });
  });

  test("the carried upload time never reaches the pointer stage: indexPointer gets the event without it", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    const spy = spyOn(pointerModule, "indexPointer");
    try {
      const [line] = reconciled(await captured(() => processBatch([msg("m1", requeueBody(key, AT), T0 + 1_000)], deps)));
      expect(lagOf(line)).toEqual({ s3_event_age_ms: 5_000, event_age_ms: 4_000, queue_origin: "requeue" });
      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy.mock.calls[0]![0]).toEqual({ provider: "s3", key });
      expect("oldestEventTime" in spy.mock.calls[0]![0]).toBe(false);
    } finally {
      spy.mockRestore();
    }
  });

  test("a scope with an unknown upload time requeues the key and the marker only", async () => {
    const key = await one();
    const { deps, requeued } = consumer(() => [key], () => T0, () => true);
    await captured(() => processBatch([msg("m1", put(key, "01")), msg("m2", JSON.stringify({ reconcile: { key } }))], deps));
    expect(requeued).toEqual([{ key }]);
    expect(requeueBody(requeued[0]!.key, requeued[0]!.time)).toBe(JSON.stringify({ reconcile: { key, requeued: true } }));
  });

  test("clamped at 0 when an event or enqueue time is ahead of the clock (skew); a non-finite clock omits both ages", async () => {
    const key = await one();
    const ahead = consumer(() => [key], () => T0 - 2_500);
    const [line] = reconciled(await captured(() => processBatch([msg("m1", put(key, "01"), T0 + 1_000)], ahead.deps)));
    expect(lagOf(line)).toEqual({ s3_event_age_ms: 0, event_age_ms: 0, queue_origin: "s3" });
    const broken = consumer(() => [key], () => Number.NaN);
    const [none] = reconciled(await captured(() => processBatch([msg("m2", put(key, "02"), T0)], broken.deps)));
    expect(keysOf(none)).toEqual([...RECONCILED_KEYS, "queue_origin"].sort());
  });

  test("the fields carry no ids, keys or times: only integers and the three origin words, and nothing else of the message is logged", async () => {
    const key = await one();
    const { deps } = consumer(() => [key], () => T0 + 5_000);
    const messageId = "059f36b3-02ba-4e7d-a2c1-8f1e1c0d9a77";
    const lines = await captured(() =>
      processBatch([{ messageId, body: put(key, "01"), sentTimestamp: String(T0 + 1_000) }, msg("m2", requeueBody(key, AT), T0 + 2_000), msg("m3", sweep(key), T0 + 3_000)], deps),
    );
    const tagged = lines.filter((l) => LAG.some((k) => k in l));
    expect(tagged.map((l) => l.message).sort()).toEqual(["pointer indexed", "search index reconciled"]);
    for (const l of tagged) {
      for (const k of ["event_age_ms", "s3_event_age_ms"]) if (k in l) expect(Number.isSafeInteger(l[k]) && (l[k] as number) >= 0).toBe(true);
      expect(["s3", "requeue", "sweep"]).toContain(l.queue_origin as string);
    }
    const text = JSON.stringify(lines);
    for (const secret of [messageId, key, AT, String(T0 + 1_000), "oldest_event_time", "requeued"]) expect(text).not.toContain(secret);
  });
});
