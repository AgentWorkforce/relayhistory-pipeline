/**
 * The streamed ClickHouse reconcile (SEARCH_INDEX_STREAMING_CH=1; search-index.ts
 * streamedReconcile over search-index-stream.ts): a scope the whole read refuses for its
 * size alone is reconciled in passes and ends exactly where the whole read ends for the
 * same S3 truth, or stops partial, never deleting, and stays owing a full pass.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
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
import { SEARCH_INDEX_STREAM_CH_BUDGET, indexSearchScope, projectedToNeonRow, readChunkRecords, type SearchIndexDeps } from "../search-index.ts";
import { STREAM_PASSES, streamScope } from "../search-index-stream.ts";
import { transaction } from "../lambda.ts";
import { chunkKeys } from "../../shared/vendor.ts";
import { EventEmitter } from "node:events";
import { zstdDecompressSync } from "node:zlib";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { memoryLock } from "./support.ts";

const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string, sessionId = scope.sessionId): SessionLocation => ({
  prefix: "96bulls/org_1", workspaceId: scope.workspaceId, machineId, harness: scope.harness, sessionId, startDate: "2026-11-01",
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
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-stream-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});
afterEach(async () => { await rm(root, { recursive: true, force: true }); });

async function writeSession(
  machine: string,
  chunks: RelayhistoryStreamRecord[][],
  revision = 1,
  sessionId = scope.sessionId,
  opts: { canonical?: string; noRawDigest?: boolean } = {},
) {
  const location = loc(machine, sessionId);
  const entries: ChunkEntry[] = [];
  let offset = 0;
  for (const [seq, records] of chunks.entries()) {
    const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = {
      seq, epoch: 0, start_offset: offset, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
      stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, ...(opts.noRawDigest ? {} : { raw_sha256: sha }), compression: "zstd", records: records.length,
    };
    offset += source.byteLength;
    entries.push(chunk);
    await store.put(rawChunkKey(location, chunk.seq, chunk.start_offset, sha, "ndjson"), stored, {});
  }
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: TRACE, revision, created_at: AT, updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: entries.length, raw_bytes: offset,
      stored_bytes: entries.reduce((n, c) => n + c.stored_bytes, 0), content_sha256: await computeContentSha256(entries), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: machine }], next_offset: offset, chunks: entries,
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1", ...(opts.canonical ? { canonical_session_id: opts.canonical } : {}) } },
  } as unknown as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}

function fakeClickHouse(onInsert?: () => void) {
  const rows: IndexRow[] = [];
  const state: { session_id: string; needs_full: number; scanned_rows: number }[] = [];
  const statements: string[] = [];
  const tsMs = (r: IndexRow) => Date.parse(`${r.ts.replace(" ", "T")}Z`);
  const identity = (r: IndexRow) => [r.org_id, r.workspace_id, tsMs(r), r.session_id, r.source, r.machine_id, r.kind, r.event_id].join("|");
  const final = () => {
    const latest = new Map<string, IndexRow>();
    for (const r of rows) {
      const held = latest.get(identity(r));
      if (!held || r.version >= held.version) latest.set(identity(r), r);
    }
    return [...latest.values()].sort((a, b) => identity(a).localeCompare(identity(b)));
  };
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const norm = (q: string) => q.trim().replace(/\s+/g, " ").replace(/IN \[.*\]$/, "IN [...]");
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      statements.push(`command ${norm(query)}`);
      const p = options?.params ?? {};
      if (query.includes("{org:String}")) {
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && r.session_id === p.session) rows.splice(i, 1);
        }
        return;
      }
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) => [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!));
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples)
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms)) rows.splice(i, 1);
        }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      statements.push(`select ${norm(query)}`);
      if (!query.includes(EVENTS_TABLE)) return [] as T[];
      const p = options!.params!;
      // ClickHouse orders String by bytes (UTF-8), not by UTF-16 code units as JS `<` does.
      const cmp = (a: unknown[], b: unknown[]) => {
        for (let i = 0; i < a.length; i++) {
          if (a[i] === b[i]) continue;
          if (typeof a[i] === "string") return Buffer.compare(Buffer.from(a[i] as string), Buffer.from(b[i] as string));
          return (a[i] as number) < (b[i] as number) ? -1 : 1;
        }
        return 0;
      };
      const tuple = (r: IndexRow) => [tsMs(r), r.machine_id, r.kind, r.event_id];
      let found = final().filter((r) => r.org_id === p.org && r.session_id === p.session && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from));
      // The keyset page: ORDER BY (ts, machine_id, kind, event_id), after the last row's tuple.
      if (query.includes("ORDER BY ts, machine_id, kind, event_id")) {
        found = found.sort((a, b) => cmp(tuple(a), tuple(b)));
        if (p.after_ts !== undefined) found = found.filter((r) => cmp(tuple(r), [Number(p.after_ts), p.after_machine, p.after_kind, p.after_event]) > 0);
      }
      return found
        .slice(0, Number(p.limit))
        .map((r) => ({ workspace_id: r.workspace_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      statements.push(`insert ${table}`);
      if (table === SESSION_STATE_TABLE) {
        state.push(...(batch as typeof state));
        return;
      }
      rows.push(...(batch as IndexRow[]));
      onInsert?.();
    },
  };
  return { ch, rows, final, state, statements };
}

function fakeSupabase(live: () => string[]) {
  const calls: string[] = [];
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      calls.push(text.trim().replace(/\s+/g, " "));
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL")) return { rows: live().map((manifest_key) => ({ manifest_key })) as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, calls, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

type Catalogs = Parameters<NonNullable<SearchIndexDeps["writeCatalog"]>>[2][];
function deps(ch: ClickHouseClient, live: () => string[], extra: Partial<SearchIndexDeps> = {}) {
  const sb = fakeSupabase(live);
  const catalogs: Catalogs = [];
  const d: SearchIndexDeps = {
    transact: sb.transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW,
    writeCatalog: async (_tx, _key, catalog) => { catalogs.push(catalog); return { written: true }; },
    ...extra,
  };
  return { d, sb, catalogs };
}

/** Whole-read bound 1 event: every fixture here is "too large" for it and streams when armed. */
const tiny = { maxScopeEvents: 1 };
const armed = { limits: tiny, stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET } };

/** A scope that exercises the fold and the projection across manifests and chunks. */
async function richScope() {
  const direct = (id: string, s: number, content: string) =>
    rec("convergence_event", id, "1", {
      source: "claude", session_id: scope.sessionId, machine_id: "srv_bb", user_id: "user_1", event_id: id, kind: "assistant", type: "assistant",
      ts: new Date(t(s)).toISOString(), content, tags: [], files_touched: [], task_ref: {},
    }, { origin_id: "machine:srv_bb" });
  const catalog = rec("session", "session-1", "1", { project_key: "github.com/acme/widgets", project_key_method: "git_remote", repo_url: "git@github.com:acme/widgets.git", git_branch: "main", agent_name: "codex", models_json: ["gpt-5"] }, { accepted_seq: "3" });
  const catalogLater = rec("session", "session-1", "1", { project_key: "github.com/acme/gadgets", git_branch: "dev", agent_name: "claude" }, { origin_id: "other-origin", accepted_seq: "9" });
  const exporter = await writeSession("delivery_aa", [
    [say("r1", "1", 1, "one"), say("r2", "1", 2, "two", { received_at: "2026-11-01T09:00:00.000Z" })],
    [rec("session_event", "p1", "1", { ts_ms: t(5), text: "calling", role: "assistant", tool_use_id: "tu1" }), catalog],
  ]);
  const live = await writeSession("srv_bb", [
    [say("r1", "1", 1, "one"), say("r2", "1", 2, "two"), direct("d1", 7, "direct one")],
    [rec("tool_call", "c1", "1", { name: "Read", target: "a.ts", tool_use_id: "tu1", is_error: "false" }), rec("file_edit", "e1", "1", { file_path: "a.ts", tool_use_id: "tu1" })],
    [rec("tool_call", "c2", "1", { name: "Grep", tool_use_id: "tu-none" }), say("r3", "1", 3, "three"), say("r3", "2", 3, "three, edited"), say("r4", "1", 4, "gone")],
    [say("r4", "2", 4, "gone", { operation: "delete", payload: null }), rec("session_event", "p0", "1", { ts_ms: t(4), text: "earlier", role: "assistant", tool_use_id: "tu1" }), catalogLater, direct("d2", 8, "direct two")],
  ]);
  return [exporter, live];
}

describe("streamed reconcile", () => {
  test("ends exactly where the whole read ends: same rows, catalog and a complete state (needs_full=0)", async () => {
    const keys = await richScope();
    const whole = fakeClickHouse();
    const w = deps(whole.ch, () => keys);
    const wr = await indexSearchScope(scope, w.d);
    expect(wr).toMatchObject({ complete: true, reasons: [] });

    const streamed = fakeClickHouse();
    const s = deps(streamed.ch, () => keys, armed);
    // Unarmed, the tiny bound refuses the scope as before.
    const refused = await indexSearchScope(scope, deps(fakeClickHouse().ch, () => keys, { limits: tiny }).d);
    expect(refused).toMatchObject({ partial: true, reasons: ["scope_too_large"] });

    const sr = await indexSearchScope(scope, s.d);
    expect(sr).toEqual({ ...wr, streamed: true });
    expect(streamed.final()).toEqual(whole.final());
    expect(streamed.final().length).toBeGreaterThan(5);
    expect(s.catalogs).toEqual(w.catalogs);
    // A pre-mark owing a pass (1 ms earlier), then the whole read's own state row.
    expect(streamed.state.map((r) => r.needs_full)).toEqual([1, 0]);
    expect(streamed.state.slice(1)).toEqual(whole.state);
    expect((streamed.state[0] as unknown as { synced_at: string }).synced_at < (streamed.state[1] as unknown as { synced_at: string }).synced_at).toBe(true);
    // Tool times settled across chunks and manifests: c1 and e1 take p0's earlier time.
    const byEvent = new Map(streamed.final().map((r) => [r.delivery_record_id, r.ts]));
    expect(byEvent.get("c1")).toBe(byEvent.get("p0"));
    expect(byEvent.get("e1")).toBe(byEvent.get("p0"));
  });

  test("redelivery is idempotent: nothing inserted or deleted, rows unchanged", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    const s = deps(ch.ch, () => keys, armed);
    await indexSearchScope(scope, s.d);
    const before = ch.final();
    expect(await indexSearchScope(scope, s.d)).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
    expect(ch.final()).toEqual(before);
    expect(ch.state.map((r) => r.needs_full)).toEqual([1, 0, 1, 0]);
  });

  test("never diff-deletes kinds S3 cannot regenerate; a stale activity row is deleted", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    const s = deps(ch.ch, () => keys, armed);
    await indexSearchScope(scope, s.d);
    const base = ch.final()[0]!;
    ch.rows.push({ ...base, kind: "tool_result", event_id: "tail:tr1", fingerprint: "neon-x" }, { ...base, kind: "session_event", event_id: "tail:stale", fingerprint: "neon-y" });
    expect(await indexSearchScope(scope, s.d)).toMatchObject({ complete: true, deleted: 1 });
    expect(ch.final().some((r) => r.event_id === "tail:tr1")).toBe(true);
    expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(false);
  });

  test("above the streamed limit it refuses exactly as the whole read does", async () => {
    const keys = await richScope();
    let events = 0; // the fixture's records, as the manifests declare them
    for (const key of keys) events += (await reader.read(key))!.manifest.raw.chunks.reduce((n, c) => n + (c.records ?? 0), 0);
    const tight = { limits: tiny, stream: { maxReadBytes: SEARCH_INDEX_STREAM_CH_BUDGET.maxReadBytes, maxReadEvents: events * STREAM_PASSES - 1 } };
    const a = fakeClickHouse();
    const b = fakeClickHouse();
    const off = await indexSearchScope(scope, deps(a.ch, () => keys, { limits: tiny }).d);
    const on = await indexSearchScope(scope, deps(b.ch, () => keys, tight).d);
    expect(on).toEqual(off);
    expect(on).toMatchObject({ partial: true, reasons: ["scope_too_large"], inserted: 0 });
    expect(b.statements).toEqual(a.statements);
    expect(b.state).toEqual(a.state);
    expect(b.state.map((r) => r.needs_full)).toEqual([1]);
    // At exactly STREAM_PASSES reads it streams.
    const fits = { limits: tiny, stream: { ...tight.stream, maxReadEvents: events * STREAM_PASSES } };
    expect(await indexSearchScope(scope, deps(fakeClickHouse().ch, () => keys, fits).d)).toMatchObject({ complete: true });
  });

  test("a deadline cut at every check point stops partial, deletes nothing, keeps needs_full=1, and the next attempt repairs", async () => {
    const keys = await richScope();
    const truth = fakeClickHouse();
    await indexSearchScope(scope, deps(truth.ch, () => keys).d);
    // A damaged index: two rows lost, one changed, one stale activity row, one foreign-kind row.
    const damaged = () => {
      const rows = truth.final().filter((r) => r.delivery_record_id !== "r1" && r.delivery_record_id !== "c1");
      const base = rows[0]!;
      return [
        ...rows.map((r) => (r.delivery_record_id === "r2" ? { ...r, fingerprint: "s3old", version: 1 } : r)),
        { ...base, event_id: "tail:stale", fingerprint: "neon-y" },
        { ...base, kind: "tool_result", event_id: "tail:tr1", fingerprint: "neon-x" },
      ];
    };
    // Count the checks of an uncut pass: before each chunk of passes 1-4, before the scan, before pass 4, before the deletes.
    let calls = 0;
    const probe = fakeClickHouse();
    probe.rows.push(...damaged());
    expect(await indexSearchScope(scope, deps(probe.ch, () => keys, { ...armed, streamDeadline: { at: Number.POSITIVE_INFINITY, now: () => calls++ } }).d)).toMatchObject({ complete: true, deleted: 1 });
    expect(calls).toBeGreaterThan(20);
    const repaired = probe.final();
    // `calls` counts the start check too: the cut points are the calls - 1 checks after it.
    for (let cut = 0; cut < calls - 1; cut++) {
      const ch = fakeClickHouse();
      ch.rows.push(...damaged());
      // The first reading is the start check (STREAM_MIN_START_MS must be left); the k-th check
      // after it reads START + (k - 1) * STEP, so the cut lands on check number `cut`. STEP is
      // larger than STREAM_TAIL_RESERVE_MS, so the tail's reserve check cuts exactly where the
      // plain deadline check would.
      let n = 0;
      const START = 120_000;
      const STEP = 100_000;
      const result = await indexSearchScope(scope, deps(ch.ch, () => keys, { ...armed, streamDeadline: { at: START + cut * STEP, now: () => (n++ === 0 ? 0 : START + (n - 2) * STEP) } }).d);
      expect(result).toMatchObject({ complete: false, partial: true, deleted: 0, reasons: ["scope_too_large", "stream_deadline"] });
      expect(ch.statements.some((q) => q.startsWith("command DELETE"))).toBe(false);
      expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(true);
      expect(ch.state.at(-1)!.needs_full).toBe(1);
      const retry = await indexSearchScope(scope, deps(ch.ch, () => keys, armed).d);
      expect(retry).toMatchObject({ complete: true, deleted: 1 });
      expect(ch.state.at(-1)!.needs_full).toBe(0);
      expect(ch.final()).toEqual(repaired);
    }
    expect(repaired.some((r) => r.event_id === "tail:tr1")).toBe(true);
  });

  test("a keepalive failure aborts the pass: nothing deleted, needs_full=1, the message fails for redelivery", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    await indexSearchScope(scope, deps(ch.ch, () => keys, armed).d);
    ch.rows.push({ ...ch.final()[0]!, event_id: "tail:stale", fingerprint: "neon-y" });
    const s = deps(ch.ch, () => keys, { ...armed, streamKeepaliveMs: 0 });
    let locks = 0;
    const failing: SearchIndexDeps = { ...s.d, transact: async (work) => work({ query: async (text: string, params?: unknown[]) => {
      if (text.includes("pg_try_advisory_xact_lock") && ++locks > 1) throw Object.assign(new Error("terminated"), { code: "57P01" });
      return s.sb.tx.query(text, params);
    } }) };
    await expect(indexSearchScope(scope, failing)).rejects.toMatchObject({ code: "57P01" });
    expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(true);
    expect(ch.state.at(-1)!.needs_full).toBe(1);
  });

  test("a keepalive that does not hold the lock (ok not true) aborts the pass: nothing deleted, needs_full=1", async () => {
    for (const answer of [{ ok: false }, { ok: null }, { ok: "t" }, undefined]) {
      const keys = await richScope();
      const ch = fakeClickHouse();
      await indexSearchScope(scope, deps(ch.ch, () => keys, armed).d);
      ch.rows.push({ ...ch.final()[0]!, event_id: "tail:stale", fingerprint: "neon-y" });
      const s = deps(ch.ch, () => keys, { ...armed, streamKeepaliveMs: 0 });
      let locks = 0;
      const odd: SearchIndexDeps = { ...s.d, transact: async (work) => work({ query: async (text: string, params?: unknown[]) => {
        if (text.includes("pg_try_advisory_xact_lock") && ++locks > 1) return { rows: answer ? [answer] : [] } as never;
        return s.sb.tx.query(text, params);
      } }) };
      await expect(indexSearchScope(scope, odd)).rejects.toMatchObject({ code: "stream_keepalive_failed", retryable: true });
      expect(ch.statements.filter((q) => q.startsWith("command DELETE"))).toHaveLength(0);
      expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(true);
      expect(ch.state.at(-1)!.needs_full).toBe(1);
    }
  });

  test("armed, a scope refused for more than its size is not streamed: exactly the unarmed result", async () => {
    const keys = [...(await richScope()), manifestKey(loc("srv_missing"))];
    const a = fakeClickHouse();
    const b = fakeClickHouse();
    const off = await indexSearchScope(scope, deps(a.ch, () => keys, { limits: tiny }).d);
    const on = await indexSearchScope(scope, deps(b.ch, () => keys, armed).d);
    expect(off).toMatchObject({ partial: true, reasons: ["manifest_missing", "scope_too_large"] });
    expect(on).toEqual(off);
    expect(b.statements).toEqual(a.statements);
    expect(b.state).toEqual(a.state);
  });

  test("a retryable read error fails the message as the whole read does; the session is left owing a pass", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    const s = deps(ch.ch, () => keys, armed);
    const missing = { ...store, get: async () => { throw Object.assign(new Error("gone"), { code: "not_found" }); } } as ObjectStore;
    await expect(indexSearchScope(scope, { ...s.d, store: missing })).rejects.toMatchObject({ code: "chunk_missing", retryable: true });
    // Nothing written but the state row: the session owes a full pass.
    expect(ch.rows).toEqual([]);
    expect(ch.state.map((r) => r.needs_full)).toEqual([1, 1]);
  });

  test("keeps the transaction busy with the statement it already sends; no new Postgres statement, the keyset page the only new ClickHouse one", async () => {
    const keys = await richScope();
    const whole = fakeClickHouse();
    const w = deps(whole.ch, () => keys);
    await indexSearchScope(scope, w.d);
    const ch = fakeClickHouse();
    const s = deps(ch.ch, () => keys, { ...armed, streamKeepaliveMs: 0 });
    await indexSearchScope(scope, s.d);
    expect(s.sb.calls.filter((c) => c.startsWith("SELECT pg_try_advisory_xact_lock")).length).toBeGreaterThan(1);
    expect(new Set(s.sb.calls)).toEqual(new Set(w.sb.calls));
    const added = [...new Set(ch.statements)].filter((q) => !whole.statements.includes(q));
    const dropped = [...new Set(whole.statements)].filter((q) => !ch.statements.includes(q));
    expect(added).toHaveLength(1);
    expect(added[0]).toStartWith("select SELECT workspace_id, machine_id, source, kind, event_id, toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint FROM transcript_events FINAL");
    expect(added[0]).toContain("ORDER BY ts, machine_id, kind, event_id LIMIT {limit:UInt32}");
    // The whole read's single LIMIT scan is not sent by the streamed pass.
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).not.toContain("ORDER BY");
  });

  test("one streamed scope per invocation: a second is requeued (stream_deferred) and streams on the next", async () => {
    const one = await richScope();
    const two = await writeSession("srv_cc", [[say("x1", "1", 1, "x"), say("x2", "1", 2, "y")]], 1, "sess_2");
    const ch = fakeClickHouse();
    const live = () => [...one, two];
    const sb = fakeSupabase(() => []);
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const requeued: string[] = [];
    const scoped = (sessionId: string) => () => live().filter((k) => k.includes(`/s=${sessionId}/`));
    const search = (slot: { used: boolean }): SearchIndexDeps => {
      const base = deps(ch.ch, () => [], { ...armed, streamSlot: slot }).d;
      return { ...base, transact: async (work) => work({ query: async (text: string, params?: unknown[]) => {
        if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
          return { rows: scoped(String(params?.[3]))().map((manifest_key) => ({ manifest_key })) } as never;
        return sb.tx.query(text, params);
      } }) };
    };
    const consumer = (slot: { used: boolean }): ConsumerDeps => ({
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: search(slot),
      requeue: async (k) => void requeued.push(k),
    });
    const put = (key: string, seq: string) => ({ messageId: seq, body: JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: seq } } }] }) });
    const first = await processBatch([put(one[1]!, "01"), put(two, "02")], consumer({ used: false }));
    expect(first.batchItemFailures).toEqual([]);
    expect(requeued).toEqual([two]);
    expect(ch.final().some((r) => r.session_id === "sess_2")).toBe(false);
    expect(ch.state.map((r) => [r.session_id, r.needs_full])).toEqual([["sess_1", 1], ["sess_1", 0]]);
    const second = await processBatch([{ messageId: "03", body: JSON.stringify({ reconcile: { key: two } }) }], consumer({ used: false }));
    expect(second.batchItemFailures).toEqual([]);
    expect(requeued).toEqual([two]);
    expect(ch.final().filter((r) => r.session_id === "sess_2")).toHaveLength(2);
    expect(ch.state.at(-1)).toMatchObject({ session_id: "sess_2", needs_full: 0 });
  });

  test("the existing identities span many keyset pages: complete, needs_full=0, correct deletes, foreign kinds kept", async () => {
    const keys = await richScope();
    const truth = fakeClickHouse();
    await indexSearchScope(scope, deps(truth.ch, () => keys).d);
    const ch = fakeClickHouse();
    const base = truth.final()[0]!;
    const stale = Array.from({ length: 7 }, (_, i) => ({ ...base, event_id: `tail:stale${i}`, fingerprint: "neon-y" }));
    const foreign = Array.from({ length: 5 }, (_, i) => ({ ...base, kind: "tool_result", event_id: `tail:tr${i}`, fingerprint: "neon-x" }));
    ch.rows.push(...truth.final().filter((r) => r.delivery_record_id !== "r2"), ...stale, ...foreign);
    // Pages of 2 identities: more than ten pages; the whole read's cap (maxRows) stays at its default.
    const pages = deps(ch.ch, () => keys, { ...armed, limits: { ...tiny, insertBatch: 2 } });
    expect(await indexSearchScope(scope, pages.d)).toMatchObject({ complete: true, inserted: 1, deleted: 7, reasons: [] });
    expect(ch.statements.filter((q) => q.includes("ORDER BY ts, machine_id")).length).toBeGreaterThan(10);
    expect(ch.state.at(-1)!.needs_full).toBe(0);
    expect(ch.final().filter((r) => r.kind !== "tool_result")).toEqual(truth.final());
    expect(ch.final().filter((r) => r.kind === "tool_result")).toHaveLength(5);
  });

  test("more existing identities than the whole read's cap: the streamed pass still completes", async () => {
    const keys = await richScope();
    const truth = fakeClickHouse();
    await indexSearchScope(scope, deps(truth.ch, () => keys).d);
    const seed = () => {
      const ch = fakeClickHouse();
      const base = truth.final()[0]!;
      ch.rows.push(...truth.final(), ...Array.from({ length: 6 }, (_, i) => ({ ...base, kind: "tool_result", event_id: `tail:tr${i}`, fingerprint: "neon-x" })), { ...base, event_id: "tail:stale", fingerprint: "neon-y" });
      return ch;
    };
    const cap = { maxRows: 5 };
    // The whole read with this cap truncates: nothing deleted, needs_full stays 1.
    const whole = seed();
    expect(await indexSearchScope(scope, deps(whole.ch, () => keys, { limits: cap }).d)).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_truncated"] });
    expect(whole.state.at(-1)!.needs_full).toBe(1);
    // Streamed under the same cap: complete, the stale row deleted, the foreign kinds kept.
    const ch = seed();
    expect(await indexSearchScope(scope, deps(ch.ch, () => keys, { ...armed, limits: { ...tiny, ...cap } }).d)).toMatchObject({ complete: true, deleted: 1, reasons: [] });
    expect(ch.state.at(-1)!.needs_full).toBe(0);
    expect(ch.final().filter((r) => r.kind !== "tool_result")).toEqual(truth.final());
    expect(ch.final().filter((r) => r.kind === "tool_result")).toHaveLength(6);
  });

  test("past the events budget the keyset scan stops partial: nothing deleted, needs_full=1", async () => {
    const keys = await richScope();
    let events = 0;
    for (const key of keys) events += (await reader.read(key))!.manifest.raw.chunks.reduce((n, c) => n + (c.records ?? 0), 0);
    const ch = fakeClickHouse();
    await indexSearchScope(scope, deps(ch.ch, () => keys, armed).d);
    const base = ch.final()[0]!;
    // More index identities than the events budget allows.
    const budget = { maxReadBytes: SEARCH_INDEX_STREAM_CH_BUDGET.maxReadBytes, maxReadEvents: events * STREAM_PASSES };
    ch.rows.push(...Array.from({ length: budget.maxReadEvents }, (_, i) => ({ ...base, event_id: `tail:stale${i}`, fingerprint: "neon-y" })));
    const out = await indexSearchScope(scope, deps(ch.ch, () => keys, { limits: tiny, stream: budget }).d);
    expect(out).toMatchObject({ complete: false, partial: true, deleted: 0, reasons: ["scope_too_large", "stream_read_events_budget"] });
    expect(ch.statements.some((q) => q.startsWith("command DELETE"))).toBe(false);
    expect(ch.state.at(-1)!.needs_full).toBe(1);
  });

  /** A pass whose stale rows and whose result say whether it deleted. */
  const staleSeeded = async (keys: string[], stale = 1) => {
    const truth = fakeClickHouse();
    await indexSearchScope(scope, deps(truth.ch, () => keys).d);
    const ch = fakeClickHouse();
    const base = truth.final()[0]!;
    ch.rows.push(...truth.final(), ...Array.from({ length: stale }, (_, i) => ({ ...base, event_id: `tail:stale${i}`, fingerprint: "neon-y" })));
    return { ch, truth };
  };
  const noDelete = (ch: ReturnType<typeof fakeClickHouse>) => {
    expect(ch.statements.some((q) => q.startsWith("command DELETE"))).toBe(false);
    expect(ch.final().some((r) => r.event_id.startsWith("tail:stale"))).toBe(true);
    expect(ch.state.at(-1)!.needs_full).toBe(1);
  };

  test("an incomplete pass deletes nothing: two manifests naming different canonical sessions", async () => {
    const a = await writeSession("srv_aa", [[say("r1", "1", 1, "one")]], 1, scope.sessionId, { canonical: "sess:1" });
    const b = await writeSession("srv_bb", [[say("r2", "1", 2, "two")]], 1, scope.sessionId, { canonical: "sess/1" });
    const { ch } = await staleSeeded([a, b]);
    const s = deps(ch.ch, () => [a, b], armed);
    expect(await indexSearchScope(scope, s.d)).toMatchObject({ complete: false, deleted: 0, reasons: ["canonical_session_mismatch"], streamed: true });
    noDelete(ch);
    expect(s.catalogs).toEqual([]);
  });

  test("an equal revision with different content stops the pass: nothing inserted or deleted", async () => {
    const a = await writeSession("srv_aa", [[say("r1", "1", 1, "one")]]);
    const b = await writeSession("srv_bb", [[say("r1", "1", 1, "not one"), say("r2", "1", 2, "two")]]);
    const ch = fakeClickHouse();
    ch.rows.push({ ...(await (async () => { const t = fakeClickHouse(); await indexSearchScope(scope, deps(t.ch, () => [b]).d); return t.final()[0]!; })()), event_id: "tail:stale0", fingerprint: "neon-y" });
    const out = await indexSearchScope(scope, deps(ch.ch, () => [a, b], armed).d);
    expect(out).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: ["relayhistory_revision_conflict"] });
    expect(ch.statements.filter((q) => q === `insert ${EVENTS_TABLE}`)).toEqual([]);
    noDelete(ch);
  });

  /**
   * Pass 4 must regenerate exactly what pass 3 recorded. Through indexSearchScope every chunk is
   * digest-verified (stored and raw sha256, required for scrubbed raw), so a source that changes
   * between passes cannot be produced there; these drive the reader with a readChunk that
   * answers differently on pass 4, the case the guards exist for.
   */
  async function readerOn(change: (records: RelayhistoryStreamRecord[]) => RelayhistoryStreamRecord[]) {
    const keys = await richScope();
    const manifests = await Promise.all(keys.map(async (k) => (await reader.read(k))!.manifest));
    const decompress = (stored: Uint8Array, max: number) => new Uint8Array(zstdDecompressSync(stored, { maxOutputLength: max }));
    const chunks = manifests.reduce((n, m) => n + m.raw.chunks.length, 0);
    let reads = 0;
    const read = await streamScope(
      {
        scope, manifests, userIds: manifests.map(() => "user_1"),
        chunks: (m) => chunkKeys(m, { prefix: "96bulls/org_1" }),
        readChunk: async (entry, key) => {
          const records = await readChunkRecords(entry, key as never, store, decompress);
          return ++reads > 3 * chunks ? change(records) : records;
        },
        beforeChunk: async () => {}, budget: { ...SEARCH_INDEX_STREAM_CH_BUDGET }, version: 1, fromMs: 0,
      },
      projectedToNeonRow,
    );
    return read;
  }

  test("pass 4 finding a row missing writes fewer rows than asked (the reconcile then records stream_short_write)", async () => {
    const read = await readerOn((records) => records.filter((r) => r.record_id !== "r3"));
    const all = new Set(read.identities.keys());
    const written: IndexRow[] = [];
    expect(await read.regenerate(all, 100, async (rows) => void written.push(...rows))).toBe(all.size - 1);
    expect(written.some((r) => r.delivery_record_id === "r3")).toBe(false);
  });

  test("pass 4 regenerating a row unlike pass 3's stops the read (stream_source_changed)", async () => {
    const read = await readerOn((records) => records.map((r) => (r.record_id === "r1" && r.payload ? { ...r, payload: { ...r.payload, text: "changed" } } : r)));
    const all = new Set(read.identities.keys());
    await expect(read.regenerate(all, 100, async () => {})).rejects.toMatchObject({ code: "stream_source_changed", partial: false });
  });

  test("two surviving events with one index key stop the streamed read (stream_duplicate_event)", async () => {
    const keys = await richScope();
    const manifests = await Promise.all(keys.map(async (k) => (await reader.read(k))!.manifest));
    const decompress = (stored: Uint8Array, max: number) => new Uint8Array(zstdDecompressSync(stored, { maxOutputLength: max }));
    const read = streamScope(
      {
        scope, manifests, userIds: manifests.map(() => "user_1"),
        chunks: (m) => chunkKeys(m, { prefix: "96bulls/org_1" }),
        readChunk: (entry, key) => readChunkRecords(entry, key as never, store, decompress),
        beforeChunk: async () => {}, budget: { ...SEARCH_INDEX_STREAM_CH_BUDGET }, version: 1, fromMs: 0,
      },
      // Every activity row gets one event id: two rows of one key.
      (row, sc, uid) => ({ ...projectedToNeonRow(row, sc, uid), event_id: "same" }),
    );
    await expect(read).rejects.toMatchObject({ code: "stream_duplicate_event", partial: false });
  });

  test("malformed keyset pages (over the limit, out of order, repeating) are not trusted: nothing deleted", async () => {
    const keys = await richScope();
    for (const breakPage of [
      (rows: unknown[]) => [...rows, ...rows], // more rows than asked for
      (rows: unknown[]) => [...rows].reverse(), // out of order
      (rows: unknown[], after: boolean) => (after ? [...rows.slice(0, 1), ...rows] : rows), // the first row again
    ]) {
      const { ch } = await staleSeeded(keys, 3);
      let pages = 0;
      const odd = { ...ch.ch, select: async <T,>(q: string, o?: { params?: Record<string, unknown> }) => {
        const rows = await ch.ch.select<T>(q, o as never);
        if (!q.includes("ORDER BY ts, machine_id")) return rows;
        pages++;
        return (pages === 2 ? breakPage(rows as unknown[], true) : rows) as T[];
      } } as ClickHouseClient;
      const out = await indexSearchScope(scope, deps(odd, () => keys, { ...armed, limits: { ...tiny, insertBatch: 3 } }).d);
      expect(out).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_page_invalid"] });
      noDelete(ch);
    }
  });

  test("deletes run in batches, each after a deadline check and an unthrottled lock check; a cut between batches owes the rest", async () => {
    const keys = await richScope();
    const { ch, truth } = await staleSeeded(keys, 1_200);
    let clock = 0;
    let deleteBatches = 0;
    const counting = { ...ch.ch, command: async (q: string, o?: unknown) => {
      await ch.ch.command(q, o as never);
      if (q.startsWith("DELETE") && ++deleteBatches === 1) clock = 10_000_000; // past the deadline after the first batch
    } } as ClickHouseClient;
    const s = deps(counting, () => keys, { ...armed, streamDeadline: { at: 1_000_000, now: () => clock } });
    const out = await indexSearchScope(scope, s.d);
    expect(out).toMatchObject({ complete: false, partial: true, deleted: 500, reasons: ["scope_too_large", "stream_deadline"] });
    expect(ch.state.at(-1)!.needs_full).toBe(1);
    // Every delete batch was preceded by a lock check (one batch ran).
    const locks = s.sb.calls.filter((c) => c.startsWith("SELECT pg_try_advisory_xact_lock")).length;
    expect(locks).toBeGreaterThanOrEqual(4); // the pass's lock, before and after the scan, before the batch
    // The retry deletes the rest and makes the session current.
    expect(await indexSearchScope(scope, deps(ch.ch, () => keys, armed).d)).toMatchObject({ complete: true, deleted: 700 });
    expect(ch.final()).toEqual(truth.final());
    expect(ch.state.at(-1)!.needs_full).toBe(0);
  });

  test("a scope is not started streaming with less than STREAM_MIN_START_MS left: deferred, nothing written", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    const out = await indexSearchScope(scope, deps(ch.ch, () => keys, { ...armed, streamSlot: { used: false }, streamDeadline: { at: 119_999, now: () => 0 } }).d);
    expect(out).toMatchObject({ deferred: true, reasons: ["stream_deferred"], inserted: 0 });
    expect(ch.rows).toEqual([]);
    expect(ch.state).toEqual([]);
  });

  /** One batch through the consumer: sessions sess_1 (streams) and the small sess_2, sess_3 (whole reads). */
  async function batchFixture() {
    const rich = await richScope();
    const small = [
      await writeSession("srv_cc", [[say("x1", "1", 1, "x")]], 1, "sess_2"),
      await writeSession("srv_dd", [[say("y1", "1", 1, "y")]], 1, "sess_3"),
    ];
    const all = [...rich, ...small];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => []);
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const requeued: string[] = [];
    const transact: SearchIndexDeps["transact"] = async (work) => work({ query: async (text: string, params?: unknown[]) => {
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: all.filter((k) => k.includes(`/s=${String(params?.[3])}/`)).map((manifest_key) => ({ manifest_key })) } as never;
      return sb.tx.query(text, params);
    } });
    const consumer = (search: Partial<SearchIndexDeps>): ConsumerDeps => ({
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      // Whole-read bound 3 events: the small sessions read whole, the rich one streams.
      search: { ...deps(ch.ch, () => [], { limits: { maxScopeEvents: 3 }, stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET }, ...search }).d, transact },
      requeue: async (k) => void requeued.push(k),
    });
    const put = (key: string, seq: string) => ({ messageId: `m${seq}`, body: JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: seq } } }] }) });
    return { rich, small, ch, requeued, consumer, put };
  }

  test("a batch whose streamed scope spends its deadline requeues the rest: inside the deadline, nothing failed", async () => {
    const { rich, small, ch, requeued, consumer, put } = await batchFixture();
    let clock = 0;
    const cutting = { ...store, get: async (key: Parameters<ObjectStore["get"]>[0]) => {
      if (String(key).includes("/s=sess_1/")) clock = 200_000; // the stream spends its time
      return store.get(key);
    } } as ObjectStore;
    const logs: string[] = [];
    const out1 = process.stdout.write.bind(process.stdout);
    const err1 = process.stderr.write.bind(process.stderr);
    const grab = ((chunk: unknown) => { logs.push(String(chunk)); return true; }) as typeof process.stdout.write;
    process.stdout.write = grab;
    process.stderr.write = grab;
    let out;
    try {
      out = await processBatch([put(rich[1]!, "01"), put(small[0]!, "02"), put(small[1]!, "03")], consumer({ store: cutting, streamSlot: { used: false }, streamDeadline: { at: 200_000, now: () => clock } }));
    } finally {
      process.stdout.write = out1;
      process.stderr.write = err1;
    }
    expect(out.batchItemFailures).toEqual([]);
    expect(requeued).toEqual(small);
    expect(ch.final().some((r) => r.session_id !== "sess_1")).toBe(false);
    expect(ch.state.map((r) => [r.session_id, r.needs_full])).toEqual([["sess_1", 1], ["sess_1", 1]]);
    expect(logs.filter((l) => l.includes('"reason":"stream_batch_deadline"'))).toHaveLength(2);
    // The partial line names the stream's own reasons, not only scope_too_large.
    expect(logs.some((l) => l.includes('"search index partial"') && l.includes('"reason":"scope_too_large,stream_deadline"'))).toBe(true);
  });

  test("whole reads before the stream run; a stream with too little time left is deferred, not started", async () => {
    const { rich, small, ch, requeued, consumer, put } = await batchFixture();
    const out = await processBatch([put(small[0]!, "01"), put(small[1]!, "02"), put(rich[1]!, "03")], consumer({ streamSlot: { used: false }, streamDeadline: { at: 100_000, now: () => 0 } }));
    expect(out.batchItemFailures).toEqual([]);
    expect(requeued).toEqual([rich[1]]);
    expect(ch.state.map((r) => [r.session_id, r.needs_full])).toEqual([["sess_2", 0], ["sess_3", 0]]);
  });

  test("a connection error emitted on the checked-out client mid-pass fails the message, never an uncaught exception", async () => {
    const keys = await richScope();
    const ch = fakeClickHouse();
    ch.rows.push({ ...(await staleSeeded(keys)).truth.final()[0]!, event_id: "tail:stale0", fingerprint: "neon-y" });
    const sb = fakeSupabase(() => keys);
    const client = Object.assign(new EventEmitter(), {
      released: [] as unknown[],
      async query(text: string, params?: unknown[]) {
        return sb.tx.query(text, params);
      },
      release(error?: unknown) {
        this.released.push(error);
      },
    });
    const db = { pool: { connect: async () => client } } as unknown as HoopDb;
    let emitted = false;
    let inFlight = 0;
    const query = client.query.bind(client);
    client.query = async (text: string, params?: unknown[]) => {
      inFlight++;
      try {
        return await query(text, params);
      } finally {
        inFlight--;
      }
    };
    const cutting = { ...store, get: async (key: Parameters<ObjectStore["get"]>[0]) => {
      if (!emitted) {
        emitted = true;
        // What pg does when the server ends the session between statements: an asynchronous
        // 'error' on the idle client, outside any query (none is in flight while S3 is read).
        setImmediate(() => {
          expect(inFlight).toBe(0);
          client.emit("error", Object.assign(new Error("Connection terminated unexpectedly"), { code: "ECONNRESET" }));
        });
        await new Promise((r) => setImmediate(r));
        await new Promise((r) => setImmediate(r));
      }
      return store.get(key);
    } } as ObjectStore;
    const uncaught: unknown[] = [];
    const onUncaught = (e: unknown) => void uncaught.push(e);
    process.on("uncaughtException", onUncaught);
    const queries: string[] = [];
    const s = deps(ch.ch, () => keys, { ...armed, store: cutting, streamKeepaliveMs: 60_000 });
    try {
      await expect(transaction(db, (tx) => indexSearchScope(scope, { ...s.d, transact: async (w) => w({ query: async (t: string, p?: unknown[]) => { queries.push(t); return tx.query(t, p); } }) }), true)).rejects.toMatchObject({ code: "ECONNRESET" });
    } finally {
      process.off("uncaughtException", onUncaught);
    }
    expect(emitted).toBe(true);
    expect(uncaught).toEqual([]);
    // The next statement after the emit (the unthrottled lock check after the scan) is the one that rejected.
    expect(queries.at(-1)).toContain("pg_try_advisory_xact_lock");
    // The broken client is destroyed (released with its error), not returned to the pool.
    expect(client.released).toHaveLength(1);
    expect(client.released[0]).toMatchObject({ code: "ECONNRESET" });
    expect(client.listenerCount("error")).toBe(0);
    noDelete(ch);
    // Unguarded (as the flag-off path always was), the same emit has no listener and throws.
    const bare = Object.assign(new EventEmitter(), { query: client.query, release() {} });
    await transaction({ pool: { connect: async () => bare } } as unknown as HoopDb, async () => {
      expect(() => bare.emit("error", new Error("x"))).toThrow("x");
    });
  });

  test("keyset pages: a repeated key, an over-limit page in valid order, and UTF-8 byte order (not UTF-16)", async () => {
    const keys = await richScope();
    // The previous page's last key again at the head of the next page: equal is not after.
    let last: unknown;
    {
      const { ch } = await staleSeeded(keys, 3);
      let pages = 0;
      const odd = { ...ch.ch, select: async <T,>(q: string, o?: { params?: Record<string, unknown> }) => {
        const rows = (await ch.ch.select<T>(q, o as never)) as unknown[];
        if (!q.includes("ORDER BY ts, machine_id")) return rows as T[];
        pages++;
        const out = pages === 2 ? [last, ...rows.slice(0, rows.length - 1)] : rows;
        last = rows[rows.length - 1];
        return out as T[];
      } } as ClickHouseClient;
      const out = await indexSearchScope(scope, deps(odd, () => keys, { ...armed, limits: { ...tiny, insertBatch: 3 } }).d);
      expect(out).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_page_invalid"] });
      noDelete(ch);
    }
    // More rows than asked for, every one in order and after the last: only the limit check sees it.
    {
      const { ch: ch2 } = await staleSeeded(keys, 3);
      let pages = 0;
      const extended = { ...ch2.ch, select: async <T,>(q: string, o?: { params?: Record<string, unknown> }) => {
        if (!q.includes("ORDER BY ts, machine_id")) return ch2.ch.select<T>(q, o as never);
        pages++;
        const p = (o as { params: Record<string, unknown> }).params;
        // Page 2 answers with one row more than its LIMIT, in order.
        return ch2.ch.select<T>(q, { ...(o as object), params: { ...p, limit: pages === 2 ? Number(p.limit) + 1 : p.limit } } as never);
      } } as ClickHouseClient;
      const result = await indexSearchScope(scope, deps(extended, () => keys, { ...armed, limits: { ...tiny, insertBatch: 3 } }).d);
      expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_page_invalid"] });
      noDelete(ch2);
    }
    // UTF-8 byte order: U+FFFD sorts before an astral character in ClickHouse, after it in UTF-16.
    {
      const truth = fakeClickHouse();
      await indexSearchScope(scope, deps(truth.ch, () => keys).d);
      const base = truth.final()[0]!;
      const ch = fakeClickHouse();
      ch.rows.push(...truth.final(), { ...base, kind: "tool_result", event_id: "x�", fingerprint: "neon-a" }, { ...base, kind: "tool_result", event_id: "x\u{1F600}", fingerprint: "neon-b" }, { ...base, event_id: "tail:stale0", fingerprint: "neon-y" });
      // One row per page: the two ids land on consecutive pages and are compared across them.
      const out = await indexSearchScope(scope, deps(ch.ch, () => keys, { ...armed, limits: { ...tiny, insertBatch: 1 } }).d);
      expect(out).toMatchObject({ complete: true, deleted: 1, reasons: [] });
      expect(ch.final().filter((r) => r.kind === "tool_result")).toHaveLength(2);
    }
  });

  test("the tail is not started inside STREAM_TAIL_RESERVE_MS of the deadline: no delete, the pass is owed", async () => {
    const keys = await richScope();
    const { ch } = await staleSeeded(keys, 2);
    let clock = 0;
    // Past the start check, the clock reads 20 s before the deadline: inside the 30 s reserve.
    const s = deps(ch.ch, () => keys, { ...armed, streamDeadline: { at: 1_000_000, now: () => clock } });
    const scan = { ...ch.ch, select: async <T,>(q: string, o?: unknown) => {
      if (q.includes("ORDER BY ts, machine_id")) clock = 980_000;
      return ch.ch.select<T>(q, o as never);
    } } as ClickHouseClient;
    const out = await indexSearchScope(scope, { ...s.d, ch: scan });
    expect(out).toMatchObject({ complete: false, partial: true, deleted: 0, reasons: ["scope_too_large", "stream_deadline"] });
    noDelete(ch);
    expect(s.catalogs).toEqual([]);
  });

  test("armed and past the deadline, no scope of the batch is started: whole reads are requeued too", async () => {
    const { small, ch, requeued, consumer, put } = await batchFixture();
    const out = await processBatch([put(small[0]!, "01"), put(small[1]!, "02")], consumer({ streamSlot: { used: false }, streamDeadline: { at: 100, now: () => 200 } }));
    expect(out.batchItemFailures).toEqual([]);
    expect(requeued).toEqual(small);
    expect(ch.state).toEqual([]);
    expect(ch.rows).toEqual([]);
    // Unarmed (no stream), the same batch runs as before.
    const again = await processBatch([put(small[0]!, "03")], { ...consumer({}), search: { ...consumer({}).search!, stream: undefined, streamDeadline: { at: 100, now: () => 200 } } });
    expect(again.batchItemFailures).toEqual([]);
    expect(ch.state.map((r) => r.session_id)).toEqual(["sess_2"]);
  });
});
