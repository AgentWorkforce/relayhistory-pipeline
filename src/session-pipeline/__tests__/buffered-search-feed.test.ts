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
import { createPipelineLambdaHandler, lambdaEnv, runtimeFromSecret } from "../lambda.ts";
import { SUPABASE_PROD_CA_2021 } from "../supabase-ca.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE } from "../buffered-feed-role.ts";
import { BUFFERED_SEARCH_GATE_CONTRACT } from "../../session-read-api/buffered-search-gate.ts";
import type { SessionScope } from "../pointer.ts";
import {
  assertOriginColumns,
  BUFFERED_FEED_DEFAULT_SINCE,
  bufferedFeedConfig,
  bufferedFeedGateRefusal,
  deleteBufferedRows,
  feedBufferedStageByKey,
  parseBufferedFingerprint,
  projectBufferedStage,
  runBufferedFeed,
  sqlBufferedFeedSource,
  BUFFERED_VERSION_CEILING,
  bufferedFeedVersionRefusal,
  canonicalPresent,
  clickhouseTieLedger,
  createdAtSeconds,
  encodeBufferedVersion,
  type BufferedTieLedger,
  type TieLedgerEntry,
  type BufferedFeedDeps,
  type BufferedFeedSource,
  type StageCandidate,
  type StageState,
} from "../buffered-search-feed.ts";

const NOW = Date.parse("2026-11-07T12:00:00.000Z");
/** Synthetic B1 ceiling for tests only, independent of the pinned production BUFFERED_VERSION_CEILING. */
const TEST_CEILING = 1_000_000_000_000;
const SECOND = { INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: "on" } as const;
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

/** system.columns rows for the origin columns as the feeder's DDL creates them (plain DEFAULT). */
type ColumnRow = { name: string; type: string; default_kind: string; default_expression: string };
const ORIGIN_COLUMN_ROWS: readonly ColumnRow[] = [
  { name: "origin", type: "LowCardinality(String)", default_kind: "DEFAULT", default_expression: "'canonical'" },
  { name: "origin_claim_token", type: "String", default_kind: "DEFAULT", default_expression: "''" },
];

/** An in-memory transcript_events speaking the statements both indexers send. */
function fakeClickHouse(opts: { originColumns?: false | readonly ColumnRow[] } = {}) {
  const rows: (IndexRow & { origin?: string; origin_claim_token?: string })[] = [];
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
      // Canonical deleteRows: 8-member identity tuples. deleteBufferedRows: the identity plus the
      // exact provenance (origin_claim_token, fingerprint, version) and origin = 'buffered'.
      const bound = query.includes("origin_claim_token, fingerprint, version)") && query.includes("origin = 'buffered'");
      expect(query).not.toContain("startsWith(");
      const token = /'((?:[^'\\]|\\.)*)'|(\d+)/g;
      const tuples = [...query.slice(query.indexOf("IN [")).matchAll(/\(((?:'(?:[^'\\]|\\.)*'|\d+)(?:,(?:'(?:[^'\\]|\\.)*'|\d+))*)\)/g)]
        .map((m) => [...m[1]!.matchAll(token)].map((t) => t[1] ?? t[2]!));
      for (const t of tuples) {
        expect(t.length).toBe(bound ? 11 : 8);
        const [org, session, ws, machine, source, kind, eventId, ms, claim, fingerprint, version] = t;
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms) &&
              (!bound || (r.origin === "buffered" && r.origin_claim_token === claim && r.fingerprint === fingerprint && r.version === Number(version))))
            rows.splice(i, 1);
        }
      }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_state")) return [] as T[];
      // The output-facts schema probe (erasure path): no fact tables exist in this fake.
      if (query.includes("system.columns") && query.includes("{tables:Array(String)}")) return [] as T[];
      if (query.includes("system.columns")) {
        // The check must read the default kind, not only type and expression.
        expect(query).toContain("default_kind");
        return (opts.originColumns === false ? [] : [...(opts.originColumns ?? ORIGIN_COLUMN_ROWS)]) as T[];
      }
      if (query.includes("origin <> 'buffered'")) {
        // canonicalPresent: a raw read (every physical row, no fold) of non-buffered rows by identity.
        expect(query).not.toContain("FINAL");
        const token = /'((?:[^'\\]|\\.)*)'|(\d+)/g;
        const wanted = new Set([...query.slice(query.indexOf("IN [")).matchAll(/\(((?:'(?:[^'\\]|\\.)*'|\d+)(?:,(?:'(?:[^'\\]|\\.)*'|\d+))*)\)/g)]
          .map((m) => [...m[1]!.matchAll(token)].map((t) => t[1] ?? t[2]!).join("|")));
        return rows.filter((r) => (r.origin ?? "canonical") !== "buffered")
          .map((r) => ({ org_id: r.org_id, session_id: r.session_id, workspace_id: r.workspace_id, machine_id: r.machine_id, source: r.source, kind: r.kind, e: sha(r.event_id), t: String(tsMs(r)) }))
          .filter((r) => wanted.has([r.org_id, r.session_id, r.workspace_id, r.machine_id, r.source, r.kind, r.e, r.t].join("|"))) as T[];
      }
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

const claims = new Map<string, string>();
/** Stage the batch exactly as deliver.ts does in buffered mode; ordinals = record indexes. */
async function stageBatch(store: ObjectStore, records = batchRecords(), claim = randomUUID()) {
  const grouped = groupRecords(records, RECEIVED);
  const prepared = prepareBufferedStage("96bulls",
    { claim_token: claim, batch_digest: sha(claim), received_at: RECEIVED.toISOString(), org_id: ORG, workspace_id: WS, origin_id: ORIGIN, user_id: USER },
    grouped.groups, grouped.index);
  await putBufferedStage(store, prepared);
  claims.set(prepared.spec.key, claim);
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
    tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "batch-1", claimToken: claims.get(prepared.spec.key)!,
    stage: { ...prepared.spec, stageToken: token, createdAt },
  };
}
const feedDeps = (source: BufferedFeedSource, store: ObjectStore, ch: ClickHouseClient): BufferedFeedDeps =>
  ({ source, store, ch, keyRoot: "96bulls", now: () => NOW, sleep: async () => {} });
const shape = (r: IndexRow & { origin?: string; origin_claim_token?: string }) => { const { version: _v, fingerprint: _f, origin: _o, origin_claim_token: _t, ...rest } = r; return rest; };
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
    const stageToken = randomUUID();
    const projected = projectBufferedStage(decoded, new Set(decoded.spec.groupOrdinals.flat()), "96bulls", stageToken);
    expect(projected.reasons).toEqual([]);
    const ours = projected.rows.map((r) => r.row).sort(byIdentity), theirs = canonical.final().sort(byIdentity);
    expect(ours.length).toBe(6);
    expect(ours.map(shape)).toEqual(theirs.map(shape));
    // Same fingerprint body; only the provenance prefix differs.
    // The s3 body, then the stage binding the later-erasure lane reads (buffered-erasure-delete.ts).
    expect(ours.map((r) => r.fingerprint.slice(2).split(":")[0])).toEqual(theirs.map((r) => r.fingerprint.slice(2)));
    expect(ours.every((r) => parseBufferedFingerprint(r.fingerprint)?.stageToken === stageToken)).toBe(true);
    expect(ours.every((r) => r.fingerprint.startsWith("bf"))).toBe(true);
    expect(theirs.every((r) => r.fingerprint.startsWith("s3"))).toBe(true);
    // The read gate's marker: origin and the stage's ORIGINAL claim token; canonical rows name neither.
    expect(ours.every((r) => r.origin === "buffered" && r.origin_claim_token === decoded.header.claim_token)).toBe(true);
    expect(theirs.every((r) => !("origin" in r) && !("origin_claim_token" in r))).toBe(true);
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
    ch.rows.push({ ...target, fingerprint: "s3canonical", version: NOW, origin: "canonical", origin_claim_token: "" });
    const out = await feedBufferedStageByKey(prepared.spec.key, feedDeps(source, store, ch.ch));
    expect(out).toMatchObject({ outcome: "indexed", rows: 6, deleted: 2 });
    expect(ch.rows.filter((r) => r.session_id === "sess-2" || (r.session_id === NO_SESSION_ID && r.fingerprint.startsWith("bf")))).toEqual([]);
    expect(ch.rows.filter((r) => r.fingerprint === "s3canonical").length).toBe(1);
  });

  test("two-claim interleaving: a stale claim's cleanup never removes a newer claim's row that already advanced the cursor", async () => {
    const store = newStore(), ch = fakeClickHouse();
    // Two claims of the SAME records (same event identities, same version, same fingerprint body):
    // older claim A and newer claim C differ only in their claim token.
    const a = await stageBatch(store, batchRecords(), "00000000-0000-4000-8000-00000000000a");
    const c = await stageBatch(store, batchRecords(), "00000000-0000-4000-8000-00000000000c");
    const tokenA = "00000000-0000-4000-8000-0000000000a1", tokenC = "00000000-0000-4000-8000-0000000000c1";
    const tenant = { orgId: ORG, workspaceId: WS, userId: USER };
    const cSource = memorySource([{ candidate: { stageToken: tokenC, createdAt: "2026-11-07T11:31:00.000000Z", tenant, originId: ORIGIN, batchId: "batch-1" }, key: c.prepared.spec.key,
      state: () => liveState(c.prepared, tokenC, "2026-11-07T11:31:00.000000Z") }]);
    let aCalls = 0;
    let cSummary: Awaited<ReturnType<typeof runBufferedFeed>> | null = null;
    const aSource = memorySource([{ candidate: { stageToken: tokenA, createdAt: "2026-11-07T11:30:00.000000Z", tenant, originId: ORIGIN, batchId: "batch-1" }, key: a.prepared.spec.key,
      state: () => {
        aCalls += 1;
        if (aCalls === 1) return liveState(a.prepared, tokenA, "2026-11-07T11:30:00.000000Z");
        // A has inserted. Before A's recheck runs, C becomes the batch's active stage and runs to
        // completion: insert, recheck, cursor advance.
        return { ...liveState(a.prepared, tokenA, "2026-11-07T11:30:00.000000Z", []), status: "gone", reason: "stage_not_active" } as StageState;
      } }]);
    // Drive the interleaving from inside A's recheck call (A's insert has already landed).
    const recheck = aSource.source.stage.bind(aSource.source);
    aSource.source.stage = async (ref) => {
      if (aCalls === 1 && "stageToken" in ref) {
        expect(ch.rows.filter((r) => r.origin_claim_token === claims.get(a.prepared.spec.key)).length).toBe(6);
        cSummary = await runBufferedFeed(feedDeps(cSource.source, store, ch.ch), { pageSize: 10, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "tail", budgetMs: 60_000 });
      }
      return recheck(ref);
    };
    const out = await feedBufferedStageByKey(a.prepared.spec.key, feedDeps(aSource.source, store, ch.ch));
    // C ran fully inside A's window and advanced past its stage.
    expect(cSummary).toMatchObject({ indexed: 1, rows: 6, deleted: 0 });
    expect(cSource.cursor.after).toEqual({ createdAt: "2026-11-07T11:31:00.000000Z", stageToken: tokenC });
    // A rechecked stale and cleaned up its own six rows ...
    expect(out).toEqual({ outcome: "indexed", rows: 6, deleted: 6 });
    expect(ch.rows.filter((r) => r.origin_claim_token === claims.get(a.prepared.spec.key))).toEqual([]);
    // ... and every one of C's rows survives, physically and in the folded view the reader sees.
    const cRows = ch.rows.filter((r) => r.origin_claim_token === claims.get(c.prepared.spec.key));
    expect(cRows.length).toBe(6);
    expect(ch.final().length).toBe(6);
    expect(ch.final().every((r) => (r as { origin_claim_token?: string }).origin_claim_token === claims.get(c.prepared.spec.key))).toBe(true);
  });

  test("buffered cleanup refuses a row without a bound buffered provenance and never issues a prefix delete", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { decoded } = await stageBatch(store);
    const [row] = projectBufferedStage(decoded, new Set(decoded.spec.groupOrdinals.flat()), "96bulls", randomUUID()).rows.map((r) => r.row);
    const sent: string[] = [];
    const spy: ClickHouseClient = { ...ch.ch, async command(q, o) { sent.push(q); return ch.ch.command(q, o); } };
    for (const bad of [{ ...row!, origin_claim_token: "" }, { ...row!, fingerprint: "bf" }, { ...row!, fingerprint: `s3${row!.fingerprint.slice(2)}` }, { ...row!, version: 0 }, { ...row!, origin: "canonical" }])
      await expect(deleteBufferedRows(spy, [bad as never])).rejects.toMatchObject({ code: "delete_provenance_unbound" });
    expect(sent).toEqual([]);
    await deleteBufferedRows(spy, [row!]);
    expect(sent.length).toBe(1);
    expect(sent[0]).toContain("origin_claim_token, fingerprint, version)");
    expect(sent[0]).toContain(`'${row!.origin_claim_token}','${row!.fingerprint}',${row!.version})`);
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
    const gate = { BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT };
    // The S version encoding must be armed: without INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION=on it is refused.
    expect(lambdaEnv({ ...base, ...gate, INGEST_BUFFERED_SEARCH_FEED: "on" }).bufferedSearchFeed).toBeUndefined();
    // With it on, the pinned production BUFFERED_VERSION_CEILING accepts.
    expect(lambdaEnv({ ...base, ...gate, INGEST_BUFFERED_SEARCH_FEED: "on", ...SECOND }).bufferedSearchFeed).toMatchObject({ enabled: true, secondVersion: true });
    expect(bufferedFeedConfig({ ...base, ...gate, INGEST_BUFFERED_SEARCH_FEED: "on", ...SECOND }, TEST_CEILING)).toMatchObject({ enabled: true, secondVersion: true });
    // The old read-flag declaration alone no longer arms it; only the contract id does.
    expect(lambdaEnv({ ...base, READ_API_BUFFERED_SEARCH_GATE: "on", INGEST_BUFFERED_SEARCH_FEED: "on" }).bufferedSearchFeed).toBeUndefined();
    expect(lambdaEnv({ ...base, BUFFERED_SEARCH_FEED_GATE_DECLARED: "buffered-search-gate/v0", INGEST_BUFFERED_SEARCH_FEED: "on" }).bufferedSearchFeed).toBeUndefined();
    // Without the read gate declared the feed is refused, and the shared runtime still builds.
    expect(lambdaEnv({ ...base, INGEST_BUFFERED_SEARCH_FEED: "on" }).bufferedSearchFeed).toBeUndefined();
    expect(lambdaEnv({ ...base, ...gate, INGEST_BUFFERED_SEARCH_FEED: "1" }).bufferedSearchFeed).toBeUndefined();
    expect(lambdaEnv({ ...base, ...gate, INGEST_BUFFERED_SEARCH_FEED: "on", INGEST_BUFFERED_SEARCH_FEED_RATE: "999" }).bufferedSearchFeed).toBeUndefined();
  });

  test("lambda runtime: the feed arms only with its own valid login fields; absent or malformed leaves it off", () => {
    const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs",
      SEARCH_INDEX_ENABLED: "1", INGEST_BUFFERED_SEARCH_FEED: "on", BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT, ...SECOND };
    const clickhouse = { url: "https://ch.synthetic.invalid", user: "u", password: "p", database: "d" };
    const feedUrl = `postgresql://${BUFFERED_SEARCH_FEED_RUNTIME_ROLE}:pw-test-only@db.synthetic.invalid:5432/history`;
    // The config is built here with a synthetic ceiling, independent of the production pin.
    const feed = (extra: Record<string, unknown>, env: Record<string, string> = base) =>
      runtimeFromSecret({ ...lambdaEnv(env), bufferedSearchFeed: (() => { try { return bufferedFeedConfig(env, TEST_CEILING); } catch { return undefined; } })() },
        JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse, ...extra }), "fixture-generation").deps.bufferedFeed;
    const armed = (extra: Record<string, unknown>, env: Record<string, string> = base) => feed(extra, env) !== undefined;
    // Flag on, gate declared, ClickHouse present, but no feeder login: the feed stays off (never the shared pool).
    expect(armed({})).toBe(false);
    // Malformed or partial login fields disarm it too.
    expect(armed({ buffered_search_feed_database_url: feedUrl })).toBe(false);
    expect(armed({ buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 })).toBe(false);
    expect(armed({ buffered_search_feed_database_url: feedUrl + "?sslmode=disable", buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 })).toBe(false);
    expect(armed({ buffered_search_feed_database_url: feedUrl.replace(`${BUFFERED_SEARCH_FEED_RUNTIME_ROLE}:`, "service_role:"), buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 })).toBe(false);
    expect(armed({ buffered_search_feed_database_url: feedUrl, buffered_search_feed_database_ca_pem: "not a pem" })).toBe(false);
    // Valid fields arm it (pools connect lazily; the login check runs before the first feed RPC).
    expect(armed({ buffered_search_feed_database_url: feedUrl, buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 })).toBe(true);
    // Armed means the S encoding: the refusal ledger is wired.
    expect(feed({ buffered_search_feed_database_url: feedUrl, buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 })?.secondVersion?.ledger).toBeDefined();
    // The production path (lambdaEnv, pinned BUFFERED_VERSION_CEILING): built only with every flag and valid login fields.
    const prod = (env: Record<string, string>) => runtimeFromSecret(lambdaEnv(env), JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse, buffered_search_feed_database_url: feedUrl, buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 }), "fixture-generation").deps.bufferedFeed;
    expect(prod(base)?.secondVersion?.ledger).toBeDefined();
    const { INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: _sv, ...legacy } = base;
    expect(prod(legacy)).toBeUndefined();
    // Flag off: valid fields still build nothing.
    const { INGEST_BUFFERED_SEARCH_FEED: _, ...off } = base;
    expect(armed({ buffered_search_feed_database_url: feedUrl, buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 }, off)).toBe(false);
  });

  test("refuses to write without the origin columns; a claim token that disagrees with SQL is skipped", async () => {
    const store = newStore();
    const { prepared } = await stageBatch(store);
    const t = "00000000-0000-4000-8000-000000000008";
    const entry = (state: () => StageState) => [{ candidate: { stageToken: t, createdAt: "x", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state }];
    const bare = fakeClickHouse({ originColumns: false });
    await expect(feedBufferedStageByKey(prepared.spec.key, feedDeps(memorySource(entry(() => liveState(prepared, t, "x"))).source, store, bare.ch))).rejects.toMatchObject({ code: "origin_columns_missing" });
    expect(bare.rows).toEqual([]);
    const ch = fakeClickHouse();
    const forged = memorySource(entry(() => ({ ...liveState(prepared, t, "x"), claimToken: randomUUID() })));
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(forged.source, store, ch.ch))).toEqual({ outcome: "skipped", reason: "stage_claim_mismatch" });
    expect(ch.rows).toEqual([]);
  });

  test("origin columns: type, default kind and default expression must all match; MATERIALIZED or ALIAS refuses", async () => {
    await expect(assertOriginColumns(fakeClickHouse().ch)).resolves.toBeUndefined();
    const variants: [string, Partial<ColumnRow>][] = [
      ["MATERIALIZED", { default_kind: "MATERIALIZED" }],
      ["ALIAS", { default_kind: "ALIAS" }],
      ["EPHEMERAL", { default_kind: "EPHEMERAL" }],
      ["no default", { default_kind: "", default_expression: "" }],
      ["type", { type: "String" }],
      ["expression", { default_expression: "'buffered'" }],
    ];
    for (const column of ORIGIN_COLUMN_ROWS)
      for (const [label, over] of variants) {
        if (label === "type" && column.name === "origin_claim_token") continue; // String is already its type
        const rows = ORIGIN_COLUMN_ROWS.map((r) => (r.name === column.name ? { ...r, ...over } : r));
        await expect(assertOriginColumns(fakeClickHouse({ originColumns: rows }).ch), `${column.name} ${label}`).rejects.toMatchObject({ code: "origin_columns_missing" });
      }
    // Only one of the two columns present.
    await expect(assertOriginColumns(fakeClickHouse({ originColumns: ORIGIN_COLUMN_ROWS.slice(0, 1) }).ch)).rejects.toMatchObject({ code: "origin_columns_missing" });
    // Through the feeder: a MATERIALIZED origin column refuses before any row is written.
    const store = newStore();
    const { prepared } = await stageBatch(store);
    const t = "00000000-0000-4000-8000-000000000009";
    const materialized = fakeClickHouse({ originColumns: ORIGIN_COLUMN_ROWS.map((r) => (r.name === "origin" ? { ...r, default_kind: "MATERIALIZED" } : r)) });
    const source = memorySource([{ candidate: { stageToken: t, createdAt: "x", tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: "b" }, state: () => liveState(prepared, t, "x") }]).source;
    await expect(feedBufferedStageByKey(prepared.spec.key, feedDeps(source, store, materialized.ch))).rejects.toMatchObject({ code: "origin_columns_missing" });
    expect(materialized.rows).toEqual([]);
  });

  test("config: default off; only the literal 'on' arms it; bounds refuse", () => {
    expect(bufferedFeedConfig({})).toMatchObject({ enabled: false, pageSize: 50, stagesPerSecond: 5, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "backfill-20261105" });
    const declared = { BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT };
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: "on", ...declared, ...SECOND }, TEST_CEILING).enabled).toBe(true);
    // The declaration alone never arms it: the feed's own flag stays the switch.
    expect(bufferedFeedConfig(declared).enabled).toBe(false);
    for (const v of ["1", "true", "ON", "yes"]) expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: v, ...declared })).toThrow(expect.objectContaining({ code: "config_flag_invalid" }));
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_PAGE: "500" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_RATE: "0" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SINCE: "yesterday" })).toThrow();
    expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_CURSOR: "../x" })).toThrow();
  });

  test("ordering guard: refuses unless the read gate's exact contract id is declared", async () => {
    expect(BUFFERED_SEARCH_GATE_CONTRACT).toBe("buffered-search-gate/v1");
    // Undeclared (unset or empty), even with the old read flag set.
    for (const env of [{}, { BUFFERED_SEARCH_FEED_GATE_DECLARED: "" }, { READ_API_BUFFERED_SEARCH_GATE: "on" }]) {
      expect(bufferedFeedGateRefusal(env)).toBe("gate_not_declared");
      expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: "on", ...env })).toThrow(expect.objectContaining({ code: "gate_not_declared" }));
    }
    // Declared, but not this contract: stale, other version, case or whitespace variants.
    for (const v of ["on", "true", "buffered-search-gate/v0", "buffered-search-gate/v2", "BUFFERED-SEARCH-GATE/V1", ` ${BUFFERED_SEARCH_GATE_CONTRACT}`, `${BUFFERED_SEARCH_GATE_CONTRACT}\n`]) {
      expect(bufferedFeedGateRefusal({ BUFFERED_SEARCH_FEED_GATE_DECLARED: v })).toBe("gate_contract_mismatch");
      expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: "on", BUFFERED_SEARCH_FEED_GATE_DECLARED: v })).toThrow(expect.objectContaining({ code: "gate_contract_mismatch" }));
    }
    // Both set: armed.
    expect(bufferedFeedGateRefusal({ BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT })).toBeNull();
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED: "on", BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT, ...SECOND }, TEST_CEILING).enabled).toBe(true);

    // The scheduled run: refused before any runtime load (no secret, pool or object access).
    const keys = ["INGEST_BUFFERED_SEARCH_FEED", "BUFFERED_SEARCH_FEED_GATE_DECLARED", "INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION"] as const;
    const saved = keys.map((k) => process.env[k]);
    let loaded = 0;
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => { loaded++; throw new Error("synthetic runtime unavailable"); } });
    try {
      process.env.INGEST_BUFFERED_SEARCH_FEED = "on";
      delete process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED;
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: false });
      process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED = "buffered-search-gate/v0";
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: false });
      expect(loaded).toBe(0);
      // Declared, but the version encoding not armed (version_encoding_not_armed): still refused before
      // any runtime load, whatever the ceiling.
      process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED = BUFFERED_SEARCH_GATE_CONTRACT;
      delete process.env.INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION;
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: false });
      process.env.INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION = "off";
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: false });
      expect(loaded).toBe(0);
      // Both flags on with the pinned ceiling: the guards pass and only then is the runtime loaded (here it
      // fails synthetically, so the run is still not ok).
      process.env.INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION = "on";
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: false });
      expect(loaded).toBe(1);
      // The feed's own flag off: nothing at all, whatever is declared.
      process.env.INGEST_BUFFERED_SEARCH_FEED = "off";
      expect(await handler({ bufferedSearchFeed: true })).toEqual({ ok: true });
      expect(loaded).toBe(1);
    } finally {
      keys.forEach((k, i) => { if (saved[i] === undefined) delete process.env[k]; else process.env[k] = saved[i]; });
    }
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

  /** Stages at fixed offsets (ms) before NOW; `listed` decides which the page RPC returns (acknowledged). */
  async function settleStages(store: ObjectStore, ages: number[]) {
    const out = [];
    for (let n = 0; n < ages.length; n++) {
      const { prepared } = await stageBatch(store, batchRecords(`-settle-${n}`));
      const candidate: StageCandidate = { stageToken: `00000000-0000-4000-8000-00000000002${n}`, createdAt: new Date(NOW - ages[n]!).toISOString().replace("Z", "000Z"), tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: `s${n}` };
      out.push({ candidate, state: () => liveState(prepared, candidate.stageToken, candidate.createdAt) });
    }
    return out;
  }
  const clockDeps = (source: BufferedFeedSource, store: ObjectStore, ch: ClickHouseClient, clock: { t: number }): BufferedFeedDeps => ({ ...feedDeps(source, store, ch), now: () => clock.t });
  const settleCfg = { pageSize: 10, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "settle-test", budgetMs: 60_000 };

  test("late ack: a stage acknowledged after a younger stage was listed is still indexed on the next run", async () => {
    const store = newStore();
    // 0: settled; 1: created 4 min ago, acknowledged only after the first run; 2: created 2 min ago, acknowledged.
    const [old, late, young] = await settleStages(store, [600_000, 240_000, 120_000]);
    const run = async (settleMs?: number) => {
      const ch = fakeClickHouse(), clock = { t: NOW };
      const listed = [old!, young!];
      const mem = memorySource(listed);
      const first = await runBufferedFeed(clockDeps(mem.source, store, ch.ch, clock), settleMs === undefined ? settleCfg : { ...settleCfg, settleMs });
      const afterFirst = mem.cursor.after?.stageToken;
      listed.push(late!);
      clock.t = NOW + 600_000;
      const second = await runBufferedFeed(clockDeps(mem.source, store, ch.ch, clock), settleMs === undefined ? settleCfg : { ...settleCfg, settleMs });
      const stagesWritten = new Set(ch.rows.map((r) => (r as { origin_claim_token?: string }).origin_claim_token));
      return { first, second, mem, afterFirst, stagesWritten };
    };
    const claimOf = (s: typeof old) => claims.get(s!.state().stage!.key)!;
    // Default holdback (300 s): the first run stops before the young stage without checking it.
    const held = await run();
    expect(held.first).toMatchObject({ indexed: 1, stages: 1, caughtUp: true });
    expect(held.afterFirst).toBe(old!.candidate.stageToken);
    expect(held.second).toMatchObject({ indexed: 2, stages: 2, caughtUp: true });
    expect(held.mem.cursor).toMatchObject({ after: { stageToken: young!.candidate.stageToken }, indexed: 3, skipped: 0 });
    expect(held.stagesWritten).toEqual(new Set([claimOf(old), claimOf(late), claimOf(young)]));
    // Control, the defect: without a holdback the cursor passes the young stage first and the late one is never listed.
    const unguarded = await run(0);
    expect(unguarded.first).toMatchObject({ indexed: 2 });
    expect(unguarded.afterFirst).toBe(young!.candidate.stageToken);
    expect(unguarded.second).toMatchObject({ indexed: 0, caughtUp: true });
    expect(unguarded.stagesWritten.has(claimOf(late))).toBe(false);
  });

  test("settle holdback: stopping and resuming neither duplicates nor alters rows", async () => {
    const store = newStore();
    const stages = await settleStages(store, [900_000, 700_000, 200_000, 100_000]);
    const canonicalOrder = (rows: IndexRow[]) => rows.map((r) => JSON.stringify(r)).sort();
    // Reference: one run long after every stage settled.
    const once = fakeClickHouse();
    await runBufferedFeed(clockDeps(memorySource(stages).source, store, once.ch, { t: NOW + 3_600_000 }), settleCfg);
    // Held: the first run stops at stage 2, later runs resume as the clock moves; then one more run past the end.
    const held = fakeClickHouse(), clock = { t: NOW };
    const mem = memorySource(stages);
    let inserts = 0;
    const original = held.ch.insert.bind(held.ch);
    held.ch.insert = async (table, rows, o) => { if (table === EVENTS_TABLE) inserts += rows.length; return original(table, rows, o); };
    const summaries = [];
    for (const t of [NOW, NOW + 150_000, NOW + 3_600_000, NOW + 7_200_000]) { clock.t = t; summaries.push(await runBufferedFeed(clockDeps(mem.source, store, held.ch, clock), settleCfg)); }
    expect(summaries.map((s) => s.indexed)).toEqual([2, 1, 1, 0]);
    expect(summaries.every((s) => s.caughtUp)).toBe(true);
    // Every stage written exactly once, and byte-identical (fingerprint and version included) to the one-shot run.
    expect(inserts).toBe(once.rows.length);
    expect(canonicalOrder(held.rows)).toEqual(canonicalOrder(once.rows));
    expect(new Set(held.rows.map((r) => `${r.fingerprint}|${r.version}`))).toEqual(new Set(once.rows.map((r) => `${r.fingerprint}|${r.version}`)));
    expect(mem.cursor).toMatchObject({ after: { stageToken: stages[3]!.candidate.stageToken }, indexed: 4, skipped: 0 });
    // A createdAt the clock cannot place is held, never processed.
    const odd = memorySource([{ ...stages[0]!, candidate: { ...stages[0]!.candidate, createdAt: "not-a-time" } }]);
    expect(await runBufferedFeed(clockDeps(odd.source, store, fakeClickHouse().ch, { t: NOW + 3_600_000 }), settleCfg)).toMatchObject({ stages: 0, indexed: 0, caughtUp: true });
    expect(odd.cursor.after).toBeNull();
  });

  test("config: the settle holdback defaults to 300 s and refuses values outside 180-3600 s", () => {
    expect(bufferedFeedConfig({}).settleMs).toBe(300_000);
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SETTLE_S: "" }).settleMs).toBe(300_000);
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SETTLE_S: "180" }).settleMs).toBe(180_000);
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SETTLE_S: "3600" }).settleMs).toBe(3_600_000);
    for (const v of ["0", "179", "3601", "1000000", "-300", "300.5", " 300", "5m", "3e2"])
      expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SETTLE_S: v })).toThrow(expect.objectContaining({ code: "config_settle_s_invalid" }));
  });
});

describe("buffered feed version (S): created_at seconds with same-second refusal", () => {
  const tenant = { orgId: ORG, workspaceId: WS, userId: USER };
  const tok = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
  const cfg = { pageSize: 10, stagesPerSecond: 50, since: BUFFERED_FEED_DEFAULT_SINCE, cursor: "backfill-test", budgetMs: 60_000 };
  /** The in-memory refusal ledger; `failWrite`/`corrupt` exercise the durable-record path. */
  function memoryLedger(opts: { failWrite?: boolean; corrupt?: boolean } = {}) {
    const entries = new Map<string, TieLedgerEntry>();
    const writes: string[] = [];
    const ledger: BufferedTieLedger = {
      async read(t) { return entries.get(t) ?? null; },
      async write(t, e) {
        if (opts.failWrite) throw new Error("ch down");
        writes.push(t);
        entries.set(t, opts.corrupt ? { ...e, siblings: e.siblings + 1 } : e);
      },
      async outstanding() { return [...entries.values()].filter((e) => !e.state.startsWith("resolved_")).length; },
    };
    return { ledger, entries, writes };
  }
  const sDeps = (source: BufferedFeedSource, store: ObjectStore, ch: ClickHouseClient, ledger: BufferedTieLedger): BufferedFeedDeps =>
    ({ ...feedDeps(source, store, ch), secondVersion: { ledger, versionCeiling: TEST_CEILING } });
  const live = (prepared: Awaited<ReturnType<typeof stageBatch>>["prepared"], n: number, createdAt: string, sameSecond?: { siblings: number; pending: number }): StageState =>
    ({ ...liveState(prepared, tok(n), createdAt), ...(sameSecond ? { sameSecond } : {}) });
  const cand = (n: number, createdAt: string, over: Partial<StageCandidate> = {}): StageCandidate =>
    ({ stageToken: tok(n), createdAt, tenant, originId: ORIGIN, batchId: "batch-1", ...over });

  test("encoder: exact grammar, real calendar instants only, floor seconds, B1 band, pinned production ceiling", () => {
    expect(createdAtSeconds("2026-11-07T11:30:00.000000Z")).toBe(Date.UTC(2026, 10, 7, 11, 30, 0) / 1000);
    expect(createdAtSeconds("2026-11-07T11:30:00.999999Z")).toBe(Date.UTC(2026, 10, 7, 11, 30, 0) / 1000);
    expect(createdAtSeconds("2024-02-29T23:59:59.000001Z")).toBe(Date.UTC(2024, 1, 29, 23, 59, 59) / 1000);
    for (const bad of ["2026-11-07T11:30:00Z", "2026-11-07T11:30:00.000Z", "2026-11-07T11:30:00.0000000Z", "2026-11-07T11:30:00.000000+00:00",
      "2026-11-07 11:30:00.000000Z", "2026-11-07T11:30:00.000000z", " 2026-11-07T11:30:00.000000Z", "2026-11-07T11:30:00.000000Z\n",
      "2025-02-29T00:00:00.000000Z", "2026-02-30T00:00:00.000000Z", "2026-13-01T00:00:00.000000Z", "2026-11-07T24:00:00.000000Z",
      "2026-11-07T11:60:00.000000Z", "2026-12-31T23:59:60.000000Z", "+02026-11-07T11:30:00.000000Z", "x", ""])
      expect(createdAtSeconds(bad)).toBeNull();
    expect(encodeBufferedVersion("2026-11-07T11:30:00.123456Z", TEST_CEILING)).toBe(Date.UTC(2026, 10, 7, 11, 30, 0) / 1000);
    // Seconds sit far below any raw-ms canonical version of the same era.
    expect(encodeBufferedVersion("2026-11-07T11:30:00.123456Z", TEST_CEILING)).toBeLessThan(Date.UTC(2020, 0, 1));
    expect(() => encodeBufferedVersion("2026-11-07T11:30:00.000Z", TEST_CEILING)).toThrow(expect.objectContaining({ code: "stage_created_at_invalid" }));
    expect(() => encodeBufferedVersion("1970-01-01T00:00:00.000000Z", TEST_CEILING)).toThrow(expect.objectContaining({ code: "buffered_version_out_of_band" }));
    expect(() => encodeBufferedVersion("2026-11-07T11:30:00.000000Z", 1_000)).toThrow(expect.objectContaining({ code: "buffered_version_out_of_band" }));
    // The production ceiling is pinned to the operator read's exact canonical min(version) (2026-11-10 16:20Z).
    expect(BUFFERED_VERSION_CEILING).toBe(1793454538262);
    expect(Number.isSafeInteger(BUFFERED_VERSION_CEILING)).toBe(true);
    expect(BUFFERED_VERSION_CEILING!).toBeGreaterThan(1e12);
    expect(encodeBufferedVersion("2026-11-07T11:30:00.000000Z")).toBe(Date.UTC(2026, 10, 7, 11, 30, 0) / 1000);
    const current = encodeBufferedVersion("2026-11-10T16:20:00.000000Z", 1793454538262);
    expect(current).toBeGreaterThan(0);
    expect(current).toBeLessThan(1793454538262);
    // An explicit `undefined` argument takes the parameter default, which is now the pin, so it does not refuse:
    // `buffered_version_ceiling_unpinned` is unreachable through these APIs while the constant is pinned.
    expect(encodeBufferedVersion("2026-11-07T11:30:00.000000Z", undefined)).toBe(Date.UTC(2026, 10, 7, 11, 30, 0) / 1000);
  });

  test("config and guard: the second-version flag is literal off/on; on with the legacy encoding refuses, pinned ceiling accepts", () => {
    const declared = { BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT, INGEST_BUFFERED_SEARCH_FEED: "on" };
    expect(bufferedFeedConfig({}).secondVersion).toBe(false);
    expect(bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: "on" }).secondVersion).toBe(true);
    for (const v of ["1", "true", "ON", ""]) expect(() => bufferedFeedConfig({ INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: v })).toThrow(expect.objectContaining({ code: "config_second_version_invalid" }));
    expect(bufferedFeedVersionRefusal({})).toBe("version_encoding_not_armed");
    expect(bufferedFeedVersionRefusal({ INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: "off" }, TEST_CEILING)).toBe("version_encoding_not_armed");
    expect(bufferedFeedVersionRefusal(SECOND)).toBeNull();
    expect(bufferedFeedVersionRefusal(SECOND, undefined)).toBeNull(); // explicit undefined takes the pinned default
    expect(bufferedFeedVersionRefusal({ INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: "off" })).toBe("version_encoding_not_armed");
    expect(bufferedFeedVersionRefusal(SECOND, TEST_CEILING)).toBeNull();
    expect(() => bufferedFeedConfig(declared, TEST_CEILING)).toThrow(expect.objectContaining({ code: "version_encoding_not_armed" }));
    expect(() => bufferedFeedConfig(declared)).toThrow(expect.objectContaining({ code: "version_encoding_not_armed" }));
    expect(bufferedFeedConfig({ ...declared, ...SECOND })).toMatchObject({ enabled: true, secondVersion: true });
    expect(bufferedFeedConfig({ ...declared, ...SECOND }, TEST_CEILING)).toMatchObject({ enabled: true, secondVersion: true });
  });

  test("strict SQL source: page grammar and keyset, stage grammar, sameSecond present and well formed", async () => {
    const answer = (r: unknown): SqlExecutor => ({ query: async <R>() => ({ rows: [{ r }] as R[] }) });
    const strict = (r: unknown) => sqlBufferedFeedSource(answer(r), { secondVersion: true });
    const row = (n: number, createdAt: string) => ({ stageToken: tok(n), createdAt, tenant, originId: ORIGIN, batchId: "b" });
    const a = "2026-11-07T11:30:00.000001Z", b = "2026-11-07T11:30:00.000002Z";
    expect((await strict({ stages: [row(1, a), row(2, b)], after: null }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).stages.length).toBe(2);
    // Same createdAt: the token breaks the tie, strictly.
    expect((await strict({ stages: [row(1, a), row(2, a)], after: null }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).stages.length).toBe(2);
    for (const bad of [[row(2, a), row(1, a)], [row(1, a), row(1, a)], [row(1, b), row(2, a)]])
      await expect(strict({ stages: bad, after: null }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_order" });
    // The first row must be after the cursor.
    await expect(strict({ stages: [row(1, a)], after: { createdAt: a, stageToken: tok(1) } }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_order" });
    await expect(strict({ stages: [row(1, a)], after: { createdAt: b, stageToken: tok(0) } }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_order" });
    expect((await strict({ stages: [row(2, a)], after: { createdAt: a, stageToken: tok(1) } }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).stages.length).toBe(1);
    await expect(strict({ stages: [], after: { createdAt: "2026-11-07T11:30:00.000Z", stageToken: tok(1) } }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_after" });
    await expect(strict({ stages: [], after: { createdAt: a, stageToken: "nope" } }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_after" });
    await expect(strict({ stages: [row(1, "2026-11-07T11:30:00.000000+00:00")], after: null }).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).rejects.toMatchObject({ code: "sql_page_row" });
    // The legacy source keeps its old (loose) checks: an equal-key page still passes, byte-identical.
    expect((await sqlBufferedFeedSource(answer({ stages: [row(1, a), row(1, a)], after: null })).page("c", BUFFERED_FEED_DEFAULT_SINCE, 5)).stages.length).toBe(2);

    const t = tok(7);
    const stageResult = (over: Record<string, unknown> = {}) => ({ found: true, status: "live", reason: null, retained: [0], tenant, originId: ORIGIN, batchId: "b", claimToken: tok(9),
      stage: { stageToken: t, key: "k", sha256: "0".repeat(64), bytes: 10, groupOrdinals: [[0]], createdAt: a }, sameSecond: { siblings: 0, pending: 0 }, ...over });
    expect((await strict(stageResult()).stage({ stageToken: t })).sameSecond).toEqual({ siblings: 0, pending: 0 });
    expect((await strict(stageResult({ sameSecond: { siblings: 3, pending: 1 } })).stage({ stageToken: t })).sameSecond).toEqual({ siblings: 3, pending: 1 });
    const { sameSecond: _, ...without } = stageResult();
    await expect(strict(without).stage({ stageToken: t })).rejects.toMatchObject({ code: "sql_same_second_unavailable" });
    for (const ss of [null, {}, { siblings: "1", pending: 0 }, { siblings: 0, pending: -1 }, { siblings: 0, pending: 1 }, { siblings: 1.5, pending: 0 }])
      await expect(strict(stageResult({ sameSecond: ss })).stage({ stageToken: t })).rejects.toMatchObject({ code: "sql_same_second_malformed" });
    await expect(strict(stageResult({ stage: { ...stageResult().stage, createdAt: "2026-11-07T11:30:00Z" } })).stage({ stageToken: t })).rejects.toMatchObject({ code: "sql_stage_spec" });
    // The legacy source neither needs nor surfaces it (flag off: unchanged).
    expect((await sqlBufferedFeedSource(answer(without)).stage({ stageToken: t })).sameSecond).toBeUndefined();
    expect("sameSecond" in (await sqlBufferedFeedSource(answer(stageResult())).stage({ stageToken: t }))).toBe(false);
  });

  test("flag off is unchanged: received_at ms version, sameSecond ignored, no canonical probe, no ledger", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const at = "2026-11-07T11:30:00.000000Z";
    const mem = memorySource([{ candidate: cand(1, at), state: () => live(prepared, 1, at, { siblings: 5, pending: 2 }) }]);
    const selects: string[] = [];
    const spy: ClickHouseClient = { ...ch.ch, async select(q, o) { selects.push(q); return ch.ch.select(q, o); } };
    const summary = await runBufferedFeed(feedDeps(mem.source, store, spy), cfg);
    expect(summary).toEqual({ pages: 1, stages: 1, indexed: 1, skipped: 0, rows: 6, deleted: 0, caughtUp: true, reasons: {} });
    expect(new Set(ch.rows.map((r) => r.version))).toEqual(new Set([RECEIVED.getTime()]));
    expect(selects.some((q) => q.includes("origin <> 'buffered'") || q.includes("buffered-tie"))).toBe(false);
    expect(await feedBufferedStageByKey(prepared.spec.key, feedDeps(mem.source, store, ch.ch))).toEqual({ outcome: "indexed", rows: 6, deleted: 0 });
  });

  test("flag on: version is the stage created_at second; replays fold to the same rows (keep-up and backfill)", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const at = "2026-11-07T11:31:07.654321Z", second = Date.UTC(2026, 10, 7, 11, 31, 7) / 1000;
    const { ledger, writes } = memoryLedger();
    const mem = memorySource([{ candidate: cand(1, at), state: () => live(prepared, 1, at, { siblings: 0, pending: 0 }) }]);
    expect(await feedBufferedStageByKey(prepared.spec.key, sDeps(mem.source, store, ch.ch, ledger))).toEqual({ outcome: "indexed", rows: 6, deleted: 0, canonicalPresent: 0 });
    expect(new Set(ch.rows.map((r) => r.version))).toEqual(new Set([second]));
    const first = ch.final().sort(byIdentity);
    const summary = await runBufferedFeed(sDeps(mem.source, store, ch.ch, ledger), cfg);
    expect(summary).toMatchObject({ indexed: 1, rows: 6, refused: 0, refusalsOutstanding: 0, canonicalPresent: 0, caughtUp: true });
    expect(await feedBufferedStageByKey(prepared.spec.key, sDeps(mem.source, store, ch.ch, ledger))).toMatchObject({ outcome: "indexed", rows: 6 });
    expect(ch.final().sort(byIdentity)).toEqual(first);
    expect(writes).toEqual([]);
    // The exact-version cleanup still matches what was written (the version is a pure function of created_at).
    expect(await deleteBufferedRows(ch.ch, ch.final() as never)).toBe(6);
    expect(ch.rows).toEqual([]);
  });

  test("same_second_tie: backfill writes no rows, records the refusal, reads it back, then advances; the run is not coverage", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const one = await stageBatch(store, batchRecords("-1")), two = await stageBatch(store, batchRecords("-2"));
    const tieAt = "2026-11-07T11:30:00.000000Z", okAt = "2026-11-07T11:30:05.000000Z";
    const m = memoryLedger();
    const mem = memorySource([
      { candidate: cand(1, tieAt), state: () => live(one.prepared, 1, tieAt, { siblings: 1, pending: 0 }) },
      { candidate: cand(2, okAt), state: () => live(two.prepared, 2, okAt, { siblings: 0, pending: 0 }) },
    ]);
    const summary = await runBufferedFeed(sDeps(mem.source, store, ch.ch, m.ledger), cfg);
    expect(summary).toMatchObject({ indexed: 1, skipped: 1, refused: 1, refusalsOutstanding: 1, caughtUp: true, reasons: { same_second_tie: 1 } });
    expect(ch.rows.every((r) => r.origin_claim_token === claims.get(two.prepared.spec.key))).toBe(true);
    expect(ch.rows.length).toBe(6);
    expect(m.entries.get(tok(1))).toEqual({ siblings: 1, firstSeen: new Date(NOW).toISOString(), revisits: 0, state: "refused" });
    expect(mem.cursor).toMatchObject({ after: { stageToken: tok(2) }, skipped: 1, indexed: 1 });
    // Replay (a fresh cursor): the entry is kept as first recorded, not rewritten.
    const again = memorySource([{ candidate: cand(1, tieAt), state: () => live(one.prepared, 1, tieAt, { siblings: 2, pending: 0 }) }]);
    expect(await runBufferedFeed(sDeps(again.source, store, ch.ch, m.ledger), cfg)).toMatchObject({ refused: 1, refusalsOutstanding: 1 });
    expect(m.writes).toEqual([tok(1)]);
    expect(m.entries.get(tok(1))!.siblings).toBe(1);
    // Keep-up never writes the ledger: it skips (logged by the consumer) and never indexes.
    const k = memoryLedger();
    expect(await feedBufferedStageByKey(one.prepared.spec.key, sDeps(again.source, store, ch.ch, k.ledger))).toEqual({ outcome: "skipped", reason: "same_second_tie", siblings: 2 });
    expect(k.writes).toEqual([]);
  });

  test("same_second_tie: a failed or mismatched ledger write throws and the cursor stays", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared } = await stageBatch(store);
    const at = "2026-11-07T11:30:00.000000Z";
    for (const [opts, code] of [[{ failWrite: true }, "ch down"], [{ corrupt: true }, "tie_ledger_readback"]] as const) {
      const mem = memorySource([{ candidate: cand(1, at), state: () => live(prepared, 1, at, { siblings: 1, pending: 0 }) }]);
      const run = runBufferedFeed(sDeps(mem.source, store, ch.ch, memoryLedger(opts).ledger), cfg);
      if (code === "ch down") await expect(run).rejects.toThrow("ch down");
      else await expect(run).rejects.toMatchObject({ code });
      expect(mem.cursor.after).toBeNull();
      expect(mem.calls).not.toContain("advance");
    }
    expect(ch.rows).toEqual([]);
  });

  test("same_second_pending: backfill blocks before the stage; keep-up requeues", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const one = await stageBatch(store, batchRecords("-1")), two = await stageBatch(store, batchRecords("-2"));
    const a = "2026-11-07T11:29:00.000000Z", b = "2026-11-07T11:30:00.000000Z";
    const m = memoryLedger();
    const mem = memorySource([
      { candidate: cand(1, a), state: () => live(one.prepared, 1, a, { siblings: 0, pending: 0 }) },
      { candidate: cand(2, b), state: () => live(two.prepared, 2, b, { siblings: 1, pending: 1 }) },
    ]);
    const summary = await runBufferedFeed(sDeps(mem.source, store, ch.ch, m.ledger), cfg);
    expect(summary).toMatchObject({ indexed: 1, blocked: "same_second_pending", caughtUp: false, refused: 0 });
    expect(mem.cursor.after?.stageToken).toBe(tok(1));
    expect(m.writes).toEqual([]);
    expect(await feedBufferedStageByKey(two.prepared.spec.key, sDeps(mem.source, store, ch.ch, m.ledger))).toEqual({ outcome: "requeue", reason: "same_second_pending", ageMs: NOW - Date.parse(b) });
  });

  test("hardening: sameSecond missing, stage/page mismatch, and a count that disagrees with the page all throw before any write", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const one = await stageBatch(store, batchRecords("-1")), two = await stageBatch(store, batchRecords("-2"));
    const a = "2026-11-07T11:30:00.100000Z", b = "2026-11-07T11:30:00.900000Z";
    const run = (stages: Parameters<typeof memorySource>[0]) => runBufferedFeed(sDeps(memorySource(stages).source, store, ch.ch, memoryLedger().ledger), cfg);
    await expect(run([{ candidate: cand(1, a), state: () => live(one.prepared, 1, a) }])).rejects.toMatchObject({ code: "sql_same_second_unavailable" });
    await expect(feedBufferedStageByKey(one.prepared.spec.key, sDeps(memorySource([{ candidate: cand(1, a), state: () => live(one.prepared, 1, a) }]).source, store, ch.ch, memoryLedger().ledger)))
      .rejects.toMatchObject({ code: "sql_same_second_unavailable" });
    // (d2) Every bound field of the stage must be the page candidate's.
    for (const over of [{ createdAt: b }, { originId: "origin-2" }, { batchId: "batch-2" }, { tenant: { ...tenant, userId: "user-2" } }, { tenant: { ...tenant, orgId: "org-b" } }])
      await expect(run([{ candidate: cand(1, a, over), state: () => live(one.prepared, 1, a, { siblings: 0, pending: 0 }) }])).rejects.toMatchObject({ code: "sql_stage_page_mismatch" });
    await expect(run([{ candidate: cand(1, a), state: () => ({ ...live(one.prepared, 1, a, { siblings: 0, pending: 0 }), stage: { ...live(one.prepared, 1, a).stage!, stageToken: tok(3) } }) }]))
      .rejects.toMatchObject({ code: "sql_stage_page_mismatch" });
    // Two page neighbours of one (org, workspace, origin, user) in one second, yet PG says no sibling.
    await expect(run([
      { candidate: cand(1, a), state: () => live(one.prepared, 1, a, { siblings: 0, pending: 0 }) },
      { candidate: cand(2, b), state: () => live(two.prepared, 2, b, { siblings: 0, pending: 0 }) },
    ])).rejects.toMatchObject({ code: "same_second_count_disagrees" });
    // A different user in that second is not a sibling.
    expect(await run([
      { candidate: cand(1, a), state: () => live(one.prepared, 1, a, { siblings: 0, pending: 0 }) },
      { candidate: cand(2, b, { tenant: { ...tenant, userId: "user-2" } }), state: () => ({ ...live(two.prepared, 2, b, { siblings: 0, pending: 0 }), tenant: { ...tenant, userId: "user-2" } }) },
    ])).toMatchObject({ stages: 2 });
    expect(ch.rows.filter((r) => r.origin_claim_token === claims.get(one.prepared.spec.key)).length).toBe(6);
  });

  test("guard (c): an identity with a canonical row in any part is not written and is counted canonical_present", async () => {
    const store = newStore(), ch = fakeClickHouse();
    const { prepared, decoded } = await stageBatch(store);
    const at = "2026-11-07T11:30:00.000000Z";
    const rows = projectBufferedStage(decoded, new Set(decoded.spec.groupOrdinals.flat()), "96bulls", randomUUID()).rows.map((r) => r.row);
    // One canonical row (no origin column value = canonical default) and one buffered row of another identity.
    const { origin: _o, origin_claim_token: _t, ...canonical } = rows[0]! as typeof rows[0] & { origin?: string; origin_claim_token?: string };
    ch.rows.push({ ...canonical, fingerprint: "s3x", version: NOW }, { ...rows[1]! });
    expect([...await canonicalPresent(ch.ch, rows)]).toEqual([0]);
    expect((await canonicalPresent(ch.ch, rows, 1)).size).toBe(1);
    ch.rows.splice(1, 1);
    const m = memoryLedger();
    const mem = memorySource([{ candidate: cand(1, at), state: () => live(prepared, 1, at, { siblings: 0, pending: 0 }) }]);
    const summary = await runBufferedFeed(sDeps(mem.source, store, ch.ch, m.ledger), cfg);
    expect(summary).toMatchObject({ indexed: 1, rows: 5, canonicalPresent: 1, reasons: { canonical_present: 1 } });
    expect(ch.rows.filter((r) => r.fingerprint.startsWith("bf")).length).toBe(5);
    expect(await feedBufferedStageByKey(prepared.spec.key, sDeps(mem.source, store, ch.ch, m.ledger))).toEqual({ outcome: "indexed", rows: 5, deleted: 0, canonicalPresent: 1 });
  });

  test("ClickHouse ledger: one search_ingest_state name per stage, JSON counts only, latest wins, resolved entries are not outstanding", async () => {
    const state: { name: string; value: string; updated_at: string }[] = [];
    const ch: ClickHouseClient = {
      database: "test",
      async command() { throw new Error("unexpected command"); },
      async insert(table, batch) { expect(table).toBe("search_ingest_state"); state.push(...(batch as typeof state)); },
      async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
        const latest = new Map<string, (typeof state)[number]>();
        for (const r of state) { const held = latest.get(r.name); if (!held || r.updated_at >= held.updated_at) latest.set(r.name, r); }
        if (query.includes("{name:String}")) { const hit = latest.get(String(options!.params!.name)); return (hit ? [{ value: hit.value, n: 1 }] : []) as T[]; }
        expect(options!.params!.prefix).toBe("buffered-tie:");
        return [{ n: String([...latest.values()].filter((r) => r.name.startsWith("buffered-tie:") && !JSON.parse(r.value).state.startsWith("resolved_")).length) }] as T[];
      },
    };
    let clock = NOW;
    const ledger = clickhouseTieLedger(ch, () => clock);
    expect(await ledger.read(tok(1))).toBeNull();
    const entry = { siblings: 2, firstSeen: new Date(NOW).toISOString(), revisits: 0, state: "refused" };
    await ledger.write(tok(1), entry);
    expect(state.map((r) => r.name)).toEqual([`buffered-tie:${tok(1)}`]);
    expect(JSON.parse(state[0]!.value)).toEqual(entry);
    expect(await ledger.read(tok(1))).toEqual(entry);
    await ledger.write(tok(2), entry);
    expect(await ledger.outstanding()).toBe(2);
    clock += 1_000;
    await ledger.write(tok(2), { ...entry, state: "resolved_indexed" });
    expect(await ledger.outstanding()).toBe(1);
    state.push({ name: `buffered-tie:${tok(3)}`, value: "{\"siblings\":\"x\"}", updated_at: "2026-11-07 12:00:00.000" });
    await expect(ledger.read(tok(3))).rejects.toMatchObject({ code: "tie_ledger_malformed" });
  });
});
