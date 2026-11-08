import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectStoreManifestReader, type ObjectStore } from "../../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { layoutComponent } from "../../shared/layout-component.ts";
import { groupRecords, writeGroups, NO_SESSION_HARNESS, NO_SESSION_ID, type Group } from "../../session-ingest/writer.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage, readBufferedStage } from "../../session-ingest/staging.ts";
import { record, scratchStore } from "../../session-ingest/__tests__/support.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { processBatch } from "../consumer.ts";
import { lambdaEnv } from "../lambda.ts";
import type { SessionScope } from "../pointer.ts";
import {
  BUFFERED_FEED_DEFAULT_SINCE,
  bufferedFeedConfig,
  feedBufferedStageByKey,
  projectBufferedStage,
  runBufferedFeed,
  sqlBufferedFeedSource,
  type BufferedFeedDeps,
  type BufferedFeedSource,
  type StageCandidate,
  type StageState,
} from "../buffered-search-feed.ts";

const NOW = Date.parse("2026-11-07T12:00:00.000Z");
const RECEIVED = new Date("2026-11-07T11:30:00.000Z");
const TS = NOW - 3_600_000;
const ORG = "org-a", WS = "ws-a", USER = "user-1", ORIGIN = "origin-1";
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));
const sha = (v: string) => createHash("sha256").update(v).digest("hex");
const cleanups: (() => void)[] = [];
afterEach(() => { for (const done of cleanups.splice(0)) done(); });
function newStore(): ObjectStore { const s = scratchStore(); cleanups.push(s.done); return s.store; }

/** One batch: a session with a colon id (layout-encoded), a second session, and a no-session record. */
function batchRecords(tag = "") {
  const r = (over: Parameters<typeof record>[0]) => ({ ...record({ origin_id: ORIGIN, ...over }), digest: sha(JSON.stringify(over)) });
  return [
    r({ record_id: `h1${tag}`, kind: "history", session_id: "pi:abc/1", payload: { prompt: "find the flaky test", ts_ms: TS } }),
    r({ record_id: `a1${tag}`, kind: "session_event", session_id: "pi:abc/1", payload: { text: "running the suite", role: "assistant", ts_ms: TS + 1, tool_use_id: "tu1" } }),
    r({ record_id: `t1${tag}`, kind: "tool_call", session_id: "pi:abc/1", payload: { name: "Bash", target: "bun test", tool_use_id: "tu1" } }),
    r({ record_id: `f1${tag}`, kind: "file_edit", session_id: "pi:abc/1", payload: { file_path: "src/a.ts", ts_ms: TS + 2 } }),
    r({ record_id: `b1${tag}`, kind: "session_event", session_id: "sess-2", payload: { text: "second session", role: "user", ts_ms: TS + 3 } }),
    r({ record_id: `n1${tag}`, kind: "session_event", session_id: null, payload: { text: "no session", role: "assistant", ts_ms: TS + 4 } }),
  ];
}

/** An in-memory transcript_events speaking the statements both indexers send. */
function fakeClickHouse() {
  const rows: IndexRow[] = [];
  const tsMs = (r: IndexRow) => Date.parse(`${r.ts.replace(" ", "T")}Z`);
  const identity = (r: IndexRow) => [r.org_id, r.workspace_id, tsMs(r), r.session_id, r.source, r.machine_id, r.kind, r.event_id].join("|");
  const final = () => {
    const latest = new Map<string, IndexRow>();
    for (const r of rows) { const held = latest.get(identity(r)); if (!held || r.version >= held.version) latest.set(identity(r), r); }
    return [...latest.values()];
  };
  let failNextInsert = 0;
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      if (!query.trimStart().startsWith("DELETE FROM")) throw new Error("unexpected command");
      if (query.includes("{org:String}")) {
        const p = options!.params!;
        for (let i = rows.length - 1; i >= 0; i--) { const r = rows[i]!; if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && (r.session_id === p.session || r.session_id === p.canonical)) rows.splice(i, 1); }
        return;
      }
      const bufferedOnly = query.includes("startsWith(fingerprint, 'bf')");
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) => [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!));
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples)
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms) && (!bufferedOnly || r.fingerprint.startsWith("bf")))
            rows.splice(i, 1);
        }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_state")) return [] as T[];
      const p = options!.params!;
      return final()
        .filter((r) => r.org_id === p.org && (r.session_id === p.session || r.session_id === p.canonical) && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from))
        .slice(0, Number(p.limit))
        .map((r) => ({ workspace_id: r.workspace_id, session_id: r.session_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      if (table === SESSION_STATE_TABLE) return;
      expect(table).toBe(EVENTS_TABLE);
      if (failNextInsert > 0) { failNextInsert -= 1; throw Object.assign(new Error("boom"), { code: "241" }); }
      rows.push(...(batch as IndexRow[]));
    },
  };
  return { ch, rows, final, failInsert: (n: number) => (failNextInsert = n) };
}

/** Supabase as indexSearchScope sees it: one live pointer per scope. */
function fakeSupabase(live: () => string[], canonicalId: () => string) {
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key, canonical_id: canonicalId(), event_epoch: "0" })) as R[] };
      if (text.includes("AS canonical_id FROM sessions.session_index")) return { rows: [{ canonical_id: canonicalId() }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx);
}

function canonicalDeps(store: ObjectStore, ch: ClickHouseClient, keys: string[], canonical: string): SearchIndexDeps {
  return {
    transact: fakeSupabase(() => keys, () => canonical), store, ch,
    reader: objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" }),
    prefixFor: (org) => `96bulls/${org}`, now: () => new Date(NOW),
    writeEvents: async () => ({ written: true }) as never, writeCatalog: async () => undefined as never,
  };
}
const scopeOf = (g: Group): SessionScope => ({ orgId: ORG, workspaceId: layoutComponent(WS), harness: layoutComponent(g.source), sessionId: layoutComponent(g.sessionId) });

/** Stage the batch exactly as deliver.ts does in buffered mode; ordinals = record indexes. */
async function stageBatch(store: ObjectStore, records = batchRecords(), claim = randomUUID()) {
  const grouped = groupRecords(records, RECEIVED);
  const prepared = prepareBufferedStage("96bulls",
    { claim_token: claim, batch_digest: sha(claim), received_at: RECEIVED.toISOString(), org_id: ORG, workspace_id: WS, origin_id: ORIGIN, user_id: USER },
    grouped.groups, grouped.index);
  await putBufferedStage(store, prepared);
  return { grouped, prepared, decoded: await readBufferedStage(store, prepared.spec, zstd) };
}

/** A BufferedFeedSource over in-memory stages with SQL's keyset + monotonic semantics. */
function memorySource(stages: { candidate: StageCandidate; key?: string; state: () => StageState }[]) {
  const cursor = { after: null as null | { createdAt: string; stageToken: string }, indexed: 0, skipped: 0 };
  const calls: string[] = [];
  const key = (s: { createdAt: string; stageToken: string }) => `${s.createdAt}|${s.stageToken}`;
  const source: BufferedFeedSource = {
    async page(_c, _s, limit) {
      calls.push("page");
      const after = cursor.after;
      return { after, stages: stages.map((s) => s.candidate).filter((c) => !after || key(c) > key(after)).sort((a, b) => key(a).localeCompare(key(b))).slice(0, limit) };
    },
    async stage(ref) {
      calls.push("stage");
      const hit = stages.find((s) => ("stageToken" in ref ? s.candidate.stageToken === ref.stageToken : (s.key ?? s.state().stage?.key) === ref.stageKey));
      return hit ? hit.state() : { found: false, status: "gone", reason: "stage_absent", retained: [] };
    },
    async advance(_c, to, counts) {
      calls.push("advance");
      if (cursor.after && key(to) <= key(cursor.after)) return false;
      cursor.after = to; cursor.indexed += counts.indexed; cursor.skipped += counts.skipped;
      return true;
    },
  };
  return { source, cursor, calls };
}

function liveState(prepared: Awaited<ReturnType<typeof stageBatch>>["prepared"], token: string, createdAt: string, retained?: number[]): StageState {
  return {
    found: true, status: "live", reason: null, retained: retained ?? prepared.spec.groupOrdinals.flat().sort((a, b) => a - b),
    tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "batch-1",
    stage: { ...prepared.spec, stageToken: token, createdAt },
  };
}
const feedDeps = (source: BufferedFeedSource, store: ObjectStore, ch: ClickHouseClient): BufferedFeedDeps =>
  ({ source, store, ch, keyRoot: "96bulls", now: () => NOW, sleep: async () => {} });
const shape = (r: IndexRow) => { const { version: _v, fingerprint: _f, ...rest } = r; return rest; };
const byIdentity = (a: IndexRow, b: IndexRow) => `${a.session_id}|${a.kind}|${a.event_id}`.localeCompare(`${b.session_id}|${b.kind}|${b.event_id}`);

describe("buffered-stage search feed", () => {
  test("row shape equals indexSearchScope's for the same lines written as live manifests", async () => {
    const store = newStore();
    const { grouped, decoded } = await stageBatch(store);
    // Canonical: the same groups written to session manifests (the pre-cutover inline path), then indexed.
    const written = await writeGroups({ store, keyRoot: "96bulls", now: () => RECEIVED }, { orgId: ORG, workspaceId: WS, userId: USER, originId: ORIGIN, batchDigest: sha("canonical") }, grouped.groups, RECEIVED);
    const canonical = fakeClickHouse();
    for (const [i, g] of grouped.groups.entries()) {
      const result = await indexSearchScope(scopeOf(g), canonicalDeps(store, canonical.ch, [written.manifests[i]!.key], g.sessionId));
      expect(result).toMatchObject({ complete: true });
    }
    const projected = projectBufferedStage(decoded, new Set(decoded.spec.groupOrdinals.flat()), "96bulls");
    expect(projected.reasons).toEqual([]);
    const ours = projected.rows.map((r) => r.row).sort(byIdentity), theirs = canonical.final().sort(byIdentity);
    expect(ours.length).toBe(6);
    expect(ours.map(shape)).toEqual(theirs.map(shape));
    // Same fingerprint body; only the provenance prefix differs.
    expect(ours.map((r) => r.fingerprint.slice(2))).toEqual(theirs.map((r) => r.fingerprint.slice(2)));
    expect(ours.every((r) => r.fingerprint.startsWith("bf"))).toBe(true);
    expect(theirs.every((r) => r.fingerprint.startsWith("s3"))).toBe(true);
    // Deterministic version: the stage's received_at.
    expect(new Set(ours.map((r) => r.version))).toEqual(new Set([RECEIVED.getTime()]));
    // The layout-encoded session keeps its RelayHistory id; the no-session record is in _none/_origin.
    expect(new Set(ours.map((r) => r.session_id))).toEqual(new Set(["pi:abc/1", "sess-2", NO_SESSION_ID]));
    expect(ours.find((r) => r.session_id === NO_SESSION_ID)!.source).toBe(NO_SESSION_HARNESS);
  });

  test("keep-up is idempotent: replays fold to the same rows", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const { source } = memorySource([{ candidate: { stageToken: "00000000-0000-4000-8000-000000000001", createdAt: "2026-11-07T11:30:00.000000Z", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "batch-1" },
      state: () => liveState(prepared, "00000000-0000-4000-8000-000000000001", "2026-11-07T11:30:00.000000Z") }]);
    const deps = feedDeps(source, store, ch.ch);
    expect(await feedBufferedStageByKey(prepared.spec.key, deps)).toEqual({ outcome: "indexed", rows: 6, deleted: 0 });
    const first = ch.final().sort(byIdentity);
    expect(await feedBufferedStageByKey(prepared.spec.key, deps)).toEqual({ outcome: "indexed", rows: 6, deleted: 0 });
    expect(ch.final().sort(byIdentity)).toEqual(first);
    expect(ch.final().length).toBe(6);
  });

  test("only retained ordinals are indexed; a recheck deletes buffered rows retention dropped since, never canonical ones", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared, grouped } = await stageBatch(store);
    const sess2 = grouped.index[grouped.groups.findIndex((g) => g.sessionId === "sess-2")]!;
    const all = prepared.spec.groupOrdinals.flat().sort((a, b) => a - b);
    // Retention already dropped sess-2: never indexed.
    let retained = all.filter((o) => !sess2.includes(o));
    let calls = 0, recheck = retained;
    const state = () => liveState(prepared, "00000000-0000-4000-8000-000000000002", "2026-11-07T11:30:00.000000Z", ++calls === 1 ? retained : recheck);
    const { source } = memorySource([{ candidate: { stageToken: "00000000-0000-4000-8000-000000000002", createdAt: "2026-11-07T11:30:00.000000Z", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "batch-1" }, key: prepared.spec.key, state }]);
    await feedBufferedStageByKey(prepared.spec.key, feedDeps(source, store, ch.ch));
    expect(ch.final().map((r) => r.session_id)).not.toContain("sess-2");
    expect(ch.final().length).toBe(5);
    // A deletion lands between the check and the write: the recheck removes that session's buffered rows.
    const noSession = grouped.index[grouped.groups.findIndex((g) => g.sessionId === NO_SESSION_ID)]!;
    calls = 0; retained = all; recheck = all.filter((o) => !noSession.includes(o) && !sess2.includes(o));
    // A canonical row with the same identity as a buffered one is never touched by that delete.
    const target = ch.final().find((r) => r.session_id === NO_SESSION_ID)!;
    ch.rows.push({ ...target, fingerprint: "s3canonical", version: NOW });
    const out = await feedBufferedStageByKey(prepared.spec.key, feedDeps(source, store, ch.ch));
    expect(out).toMatchObject({ outcome: "indexed", rows: 6, deleted: 2 });
    expect(ch.rows.filter((r) => r.session_id === "sess-2" || (r.session_id === NO_SESSION_ID && r.fingerprint.startsWith("bf")))).toEqual([]);
    expect(ch.rows.filter((r) => r.fingerprint === "s3canonical").length).toBe(1);
  });

  test("redacted/GC'd stages are skipped and passed; pending/deferred keep-up requeues; deferred blocks the backfill cursor", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const t = (n: number) => `00000000-0000-4000-8000-00000000000${n}`;
    const cand = (n: number): StageCandidate => ({ stageToken: t(n), createdAt: `2026-11-06T00:00:0${n}.000000Z`, tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: `b${n}` });
    const redacted: StageState = { found: true, status: "gone", reason: "stage_redacted", retained: [] };
    let deferred = true;
    const mem = memorySource([
      { candidate: cand(1), state: () => redacted },
      { candidate: cand(2), state: () => liveState(prepared, t(2), cand(2).createdAt) },
      { candidate: cand(3), state: () => (deferred ? { found: true, status: "deferred", reason: "gc_in_flight", retained: [] } : liveState(prepared, t(3), cand(3).createdAt)) },
      { candidate: cand(4), state: () => liveState(prepared, t(4), cand(4).createdAt) },
    ]);
    const cfg = { pageSize: 10, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "backfill-test", budgetMs: 60_000 };
    const first = await runBufferedFeed(feedDeps(mem.source, store, ch.ch), cfg);
    expect(first).toMatchObject({ indexed: 1, skipped: 1, blocked: "gc_in_flight", caughtUp: false, reasons: { stage_redacted: 1 } });
    // The cursor stops before the undecided stage.
    expect(mem.cursor.after?.stageToken).toBe(t(2));
    deferred = false;
    const second = await runBufferedFeed(feedDeps(mem.source, store, ch.ch), cfg);
    expect(second).toMatchObject({ indexed: 2, skipped: 0, caughtUp: true });
    expect(mem.cursor).toMatchObject({ after: { stageToken: t(4) }, indexed: 3, skipped: 1 });
    // Keep-up: a stage still preparing is put back on the queue, a redacted one is dropped.
    const pending = memorySource([{ candidate: cand(5), state: () => ({ ...liveState(prepared, t(5), cand(5).createdAt), status: "pending", reason: "stage_preparing" }) }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(pending.source, store, ch.ch))).toEqual({ outcome: "requeue", reason: "stage_preparing" });
    const gone = memorySource([{ candidate: cand(6), state: () => ({ ...liveState(prepared, t(6), cand(6).createdAt), status: "gone", reason: "stage_redacted" }) }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(gone.source, store, ch.ch))).toEqual({ outcome: "skipped", reason: "stage_redacted" });
  });

  test("backfill resumes from its cursor after a failed write: no stage lost, no row duplicated", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const stages = [];
    for (let n = 1; n <= 5; n++) {
      const { prepared } = await stageBatch(store, batchRecords(`-${n}`));
      const candidate: StageCandidate = { stageToken: `00000000-0000-4000-8000-00000000000${n}`, createdAt: `2026-11-06T00:00:0${n}.000000Z`, tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: `b${n}` };
      stages.push({ candidate, state: () => liveState(prepared, candidate.stageToken, candidate.createdAt) });
    }
    const mem = memorySource(stages);
    const cfg = { pageSize: 2, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "backfill-test", budgetMs: 60_000 };
    ch.failInsert(0);
    // Page 2's insert fails: the run throws after page 1 advanced.
    let inserts = 0;
    const original = ch.ch.insert.bind(ch.ch);
    ch.ch.insert = async (table, rows, o) => { if (table === EVENTS_TABLE && ++inserts === 2) throw new Error("ch down"); return original(table, rows, o); };
    await expect(runBufferedFeed(feedDeps(mem.source, store, ch.ch), cfg)).rejects.toThrow("ch down");
    expect(mem.cursor.after?.stageToken).toBe(stages[1]!.candidate.stageToken);
    expect(ch.final().length).toBe(12);
    const resumed = await runBufferedFeed(feedDeps(mem.source, store, ch.ch), cfg);
    expect(resumed).toMatchObject({ indexed: 3, caughtUp: true, pages: 2 });
    expect(ch.final().length).toBe(30);
    // A full re-run from scratch (a new backfill) only rewrites identical rows.
    const again = memorySource(stages);
    await runBufferedFeed(feedDeps(again.source, store, ch.ch), cfg);
    expect(ch.final().length).toBe(30);
  });

  test("a stage whose bytes or header do not match the SQL tenant is skipped, never indexed", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const t = "00000000-0000-4000-8000-000000000009";
    const foreign = { ...liveState(prepared, t, "2026-11-06T00:00:00.000000Z"), tenant: { orgId: ORG, workspaceId: WS, userId: "someone-else" } };
    const mem = memorySource([{ candidate: { stageToken: t, createdAt: "2026-11-06T00:00:00.000000Z", tenant: foreign.tenant, originId: ORIGIN, batchId: "b" }, state: () => foreign }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(mem.source, store, ch.ch))).toEqual({ outcome: "skipped", reason: "stage_tenant_mismatch" });
    const tampered = { ...liveState(prepared, t, "2026-11-06T00:00:00.000000Z"), stage: { ...prepared.spec, sha256: "0".repeat(64), stageToken: t, createdAt: "x" } };
    const mem2 = memorySource([{ candidate: { stageToken: t, createdAt: "2026-11-06T00:00:00.000000Z", tenant: tampered.tenant!, originId: ORIGIN, batchId: "b" }, state: () => tampered }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(mem2.source, store, ch.ch))).toEqual({ outcome: "skipped", reason: "buffered_sha_mismatch" });
    expect(ch.rows).toEqual([]);
  });

  test("a canonical pass keeps buffered-only rows of its session and supersedes the ones it carries", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared, grouped } = await stageBatch(store);
    const t = "00000000-0000-4000-8000-000000000003";
    const mem = memorySource([{ candidate: { stageToken: t, createdAt: "2026-11-06T00:00:00.000000Z", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state: () => liveState(prepared, t, "x") }]);
    await feedBufferedStageByKey(prepared.spec.key, feedDeps(mem.source, store, ch.ch));
    // An older manifest of session pi:abc/1 that carries only its history record (pre-cutover content).
    const g = grouped.groups.find((x) => x.sessionId === "pi:abc/1")!;
    const older: Group = { ...g, lines: [g.lines[0]!], meta: [g.meta![0]!] };
    const written = await writeGroups({ store, keyRoot: "96bulls", now: () => RECEIVED }, { orgId: ORG, workspaceId: WS, userId: USER, originId: ORIGIN, batchDigest: sha("old") }, [older], RECEIVED);
    const result = await indexSearchScope(scopeOf(g), canonicalDeps(store, ch.ch, [written.manifests[0]!.key], g.sessionId));
    expect(result).toMatchObject({ complete: true, deleted: 0, inserted: 1 });
    const session = ch.final().filter((r) => r.session_id === "pi:abc/1");
    expect(session.length).toBe(4);
    expect(session.filter((r) => r.fingerprint.startsWith("s3")).map((r) => r.kind)).toEqual(["history"]);
    expect(session.filter((r) => r.fingerprint.startsWith("bf")).length).toBe(3);
    // Erasure of the session (no live pointer) still removes everything, buffered rows included.
    await indexSearchScope(scopeOf(g), canonicalDeps(store, ch.ch, [], g.sessionId));
    expect(ch.final().filter((r) => r.session_id === "pi:abc/1")).toEqual([]);
  });

  test("consumer: a buffered-stage event feeds the stage when armed and requeues while pending; unarmed it never reaches the feed", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const body = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", s3: { object: { key: encodeURIComponent(prepared.spec.key) } } }] });
    let status: StageState["status"] = "pending";
    const t = "00000000-0000-4000-8000-000000000004";
    const mem = memorySource([{ candidate: { stageToken: t, createdAt: "x", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state: () => ({ ...liveState(prepared, t, "x"), status, reason: status === "pending" ? "stage_preparing" : null }) }]);
    const requeued: string[] = [];
    const pointerCalls: string[] = [];
    const pointer = { db: { query: async () => { pointerCalls.push("q"); return { rows: [] }; } } } as never;
    const deps = { pointer, bufferedFeed: feedDeps(mem.source, store, ch.ch), requeue: async (k: string) => { requeued.push(k); } };
    expect(await processBatch([{ messageId: "m1", body }], deps)).toEqual({ batchItemFailures: [] });
    expect(requeued).toEqual([prepared.spec.key]);
    status = "live";
    expect(await processBatch([{ messageId: "m2", body }], deps)).toEqual({ batchItemFailures: [] });
    expect(ch.final().length).toBe(6);
    expect(pointerCalls).toEqual([]);
    // Unarmed: the feed source is never consulted.
    mem.calls.length = 0;
    await processBatch([{ messageId: "m3", body }], { pointer: { db: { query: async () => ({ rows: [] }) }, lock: async (_s: unknown, w: () => unknown) => w(), reader: { read: async () => null }, exists: async () => false, orgs: async () => null, allowPrefix: () => false, fanout: { send: async () => {} } } as never });
    expect(mem.calls).toEqual([]);
  });

  test("keep-up leaves a fenced-authority stage to the cursor instead of requeueing it forever", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const t = "00000000-0000-4000-8000-000000000007";
    const fenced = memorySource([{ candidate: { stageToken: t, createdAt: "x", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state: () => ({ ...liveState(prepared, t, "x"), status: "deferred", reason: "privacy_fenced" }) }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(fenced.source, store, ch.ch))).toEqual({ outcome: "skipped", reason: "privacy_fenced" });
    const leased = memorySource([{ candidate: { stageToken: t, createdAt: "x", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state: () => ({ ...liveState(prepared, t, "x"), status: "deferred", reason: "gc_in_flight" }) }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(leased.source, store, ch.ch))).toEqual({ outcome: "requeue", reason: "gc_in_flight" });
    expect(ch.rows).toEqual([]);
  });

  test("a page writes as it goes past its byte bound; the cursor still moves once per page", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const stages = [];
    for (let n = 1; n <= 3; n++) {
      const { prepared } = await stageBatch(store, batchRecords(`-${n}`));
      const candidate: StageCandidate = { stageToken: `00000000-0000-4000-8000-00000000001${n}`, createdAt: `2026-11-06T00:00:0${n}.000000Z`, tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: `b${n}` };
      stages.push({ candidate, state: () => liveState(prepared, candidate.stageToken, candidate.createdAt) });
    }
    const mem = memorySource(stages);
    let inserts = 0;
    const original = ch.ch.insert.bind(ch.ch);
    ch.ch.insert = async (table, rows, o) => { if (table === EVENTS_TABLE) inserts += 1; return original(table, rows, o); };
    const summary = await runBufferedFeed({ ...feedDeps(mem.source, store, ch.ch), flushBytes: 1 }, { pageSize: 10, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "c", budgetMs: 60_000 });
    expect(summary).toMatchObject({ indexed: 3, rows: 18, caughtUp: true });
    expect(inserts).toBe(3);
    expect(mem.calls.filter((c) => c === "advance").length).toBe(1);
  });

  test("lambda env: a malformed feed setting disarms only the feed, never the shared runtime", () => {
    const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs" };
    expect(lambdaEnv(base).bufferedSearchFeed).toMatchObject({ enabled: false });
    expect(lambdaEnv({ ...base, INGEST_BUFFERED_SEARCH_FEED: "on" }).bufferedSearchFeed).toMatchObject({ enabled: true });
    expect(lambdaEnv({ ...base, INGEST_BUFFERED_SEARCH_FEED: "1" }).bufferedSearchFeed).toBeUndefined();
    expect(lambdaEnv({ ...base, INGEST_BUFFERED_SEARCH_FEED: "on", INGEST_BUFFERED_SEARCH_FEED_RATE: "999" }).bufferedSearchFeed).toBeUndefined();
  });

  test("config: default off; only the literal 'on' arms it; bounds refuse", () => {
    expect(bufferedFeedConfig({})).toMatchObject({ enabled: false, pageSize: 50, stagesPerSecond: 5, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "backfill-20261105" });
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: "on" }).enabled).toBe(true);
    for (const v of ["1", "true", "ON", "yes"]) expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: v })).toThrow("must be off or on");
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_PAGE: "500" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_RATE: "0" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SINCE: "yesterday" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_CURSOR: "../x" })).toThrow();
  });

  test("SQL source refuses malformed RPC results and classifies transient SQL errors", async () => {
    const answer = (r: unknown): SqlExecutor => ({ query: async <R>() => ({ rows: [{ r }] as R[] }) });
    await expect(sqlBufferedFeedSource(answer({ stages: [{ stageToken: "nope" }], after: null })).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_row" });
    await expect(sqlBufferedFeedSource(answer({ found: true, status: "live", reason: null, retained: [1], stage: { stageToken: randomUUID() } })).stage({ stageToken: randomUUID() })).rejects.toMatchObject({ code: "sql_stage_spec" });
    await expect(sqlBufferedFeedSource(answer({ found: true, status: "weird", reason: null, retained: [] })).stage({ stageToken: randomUUID() })).rejects.toMatchObject({ code: "sql_stage" });
    const failing = (code: string): SqlExecutor => ({ query: async () => { throw Object.assign(new Error("x"), { code }); } });
    await expect(sqlBufferedFeedSource(failing("40001")).stage({ stageToken: randomUUID() })).rejects.toMatchObject({ code: "sql_40001", retryable: true });
    await expect(sqlBufferedFeedSource(failing("42501")).stage({ stageToken: randomUUID() })).rejects.toMatchObject({ code: "sql_42501", retryable: false });
  });
});
