/**
 * Version order between canonical and buffered-origin rows of the SAME identity, against a real
 * ClickHouse with the shared `transcript_events` schema (skipped unless
 * READ_API_TEST_CLICKHOUSE_URL is set; CI only).
 *
 * The schema: ReplacingMergeTree(version) ORDER BY (org_id, workspace_id, ts, session_id, source,
 * machine_id, kind, event_id); `origin` / `origin_claim_token` are NOT in the key. Canonical writers
 * stamp `version` = their wall clock at read time (search-index.ts indexSearchScope,
 * vendor reconcile.ts reconcileSession); the buffered feeder stamps `version` =
 * Date.parse(stage.header.received_at) (buffered-search-feed.ts projectBufferedStage). Nothing
 * orders those two clocks, so a buffered row can carry the higher version or tie.
 *
 * Every assertion tagged `DEFECT (version-order)` documents the CURRENT, unwanted behaviour: a
 * buffered write physically replacing the canonical row of the same identity once a merge (or
 * OPTIMIZE ... FINAL) runs, after which no `origin = 'canonical'` filter can bring it back. The
 * chosen fix must flip exactly those assertions (and keep the `SAFE` ones). Assertions tagged
 * `RECOVERY` document today's only repair path (a later canonical pass over the scope).
 *
 * ReplacingMergeTree rules exercised: the highest `version` survives a merge regardless of insert
 * order; on an equal `version` the LAST inserted row survives (FINAL and OPTIMIZE FINAL alike).
 * Background merges run the same algorithm as OPTIMIZE FINAL, so the post-OPTIMIZE state is where
 * an unmerged table converges on its own.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectStoreManifestReader, type ObjectStore } from "../../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { layoutComponent } from "../../shared/layout-component.ts";
import { groupRecords, writeGroups, type Group } from "../../session-ingest/writer.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage, readBufferedStage } from "../../session-ingest/staging.ts";
import { record, scratchStore } from "../../session-ingest/__tests__/support.ts";
import { CH_URL, freshClickHouse, memoryLiveness, row } from "../../session-read-api/__tests__/support.ts";
import { readEventPage } from "../../session-read-api/page.ts";
import { EVENTS_TABLE } from "../vendor/search-index/schema.ts";
import { deleteRows, type PendingDelete } from "../vendor/search-index/reconcile.ts";
import { tsMillis, type IndexRow } from "../vendor/search-index/rows.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import type { SessionScope } from "../pointer.ts";
import {
  assertOriginColumns,
  deleteBufferedRows,
  feedBufferedStageByKey,
  projectBufferedStage,
  type BufferedFeedDeps,
  type BufferedFeedSource,
  type StageState,
} from "../buffered-search-feed.ts";

const suite = CH_URL ? describe : describe.skip;
const CLAIM = "11111111-2222-4333-8444-555555555555";
/** The feeder's DDL (docs/proposals/buffered-search-feed.clickhouse.draft.sql), as buffered-origin.clickhouse.test.ts. */
const ORIGIN_DDL = [
  "ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin LowCardinality(String) DEFAULT 'canonical'",
  "ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin_claim_token String DEFAULT ''",
];

type Ch = Awaited<ReturnType<typeof freshClickHouse>>;
type Physical = { origin: string; version: number; fingerprint: string };

/** Physical rows of one event (no FINAL): what a merge has kept. Lightweight-deleted rows are masked. */
async function physical(ch: Ch, eventId: string): Promise<Physical[]> {
  return ch.select<Physical>(
    `SELECT origin, toUInt64(version) AS version, fingerprint FROM ${EVENTS_TABLE} WHERE event_id = {id:String} ORDER BY version, origin`,
    { params: { id: eventId } },
  );
}
/** The collapsed row (FINAL): what `FINAL` readers (indexSearchScope's scan, reconcile) see. */
async function final(ch: Ch, eventId: string): Promise<Physical[]> {
  return ch.select<Physical>(
    `SELECT origin, toUInt64(version) AS version, fingerprint FROM ${EVENTS_TABLE} FINAL WHERE event_id = {id:String}`,
    { params: { id: eventId } },
  );
}
/** A plain `origin = 'canonical'` filter over physical rows: the "filter canonical" mitigation. */
async function canonicalFilter(ch: Ch, eventId: string): Promise<number> {
  const [r] = await ch.select<{ n: number | string }>(
    `SELECT count() AS n FROM ${EVENTS_TABLE} WHERE event_id = {id:String} AND origin = 'canonical'`,
    { params: { id: eventId } },
  );
  return Number(r!.n);
}
/** The read API with the gate NOT wired (the Lambda / gate off): canonical-only on the NEWEST version. */
async function servedCanonical(ch: Ch, sessionId: string): Promise<string[]> {
  const page = await readEventPage({ ch, liveness: memoryLiveness().liveness }, { orgId: "org-a", workspaceId: "ws-a", order: "asc", limit: 50, sessionId });
  return page.events.map((e) => e.eventId).sort();
}

const TS = "2026-10-01 10:00:00.000";
const canonicalRow = (id: string, version: number) => ({ ...row({ id, ts: TS, session: `s-${id}`, version }), fingerprint: `s3${id}` });
const bufferedRow = (id: string, version: number) => ({ ...row({ id, ts: TS, session: `s-${id}`, version }), fingerprint: `bf${id}`, origin: "buffered", origin_claim_token: CLAIM });

/**
 * Independently clocked writers, one batch. t0 = true receipt; the stage receiver stamps
 * received_at = t0 + receiverSkew; the canonical pass reads at t0 + LAG (publication + index
 * latency) on a host with hostSkew. Buffered wins iff receiverSkew - hostSkew > LAG (tie at =).
 */
const T0 = Date.parse("2026-11-07T11:30:00.000Z");
const LAG = 5_000;
const receiverVersion = (receiverSkew: number) => T0 + receiverSkew;
const hostVersion = (hostSkew: number) => T0 + LAG + hostSkew;

/** [eventId, insert order, expected survivor after FINAL / merge]. */
interface Case { id: string; inserts: (ReturnType<typeof canonicalRow> | ReturnType<typeof bufferedRow>)[]; survivor: "canonical" | "buffered"; defect: boolean }
const CASES: Case[] = [
  // (a) canonical v2, then buffered v1: canonical has the higher version. SAFE.
  { id: "a", inserts: [canonicalRow("a", 2), bufferedRow("a", 1)], survivor: "canonical", defect: false },
  // (a') same versions, buffered inserted first. SAFE (order does not matter, version does).
  { id: "a-rev", inserts: [bufferedRow("a-rev", 1), canonicalRow("a-rev", 2)], survivor: "canonical", defect: false },
  // (b) canonical v1, then buffered v2. DEFECT.
  { id: "b", inserts: [canonicalRow("b", 1), bufferedRow("b", 2)], survivor: "buffered", defect: true },
  // (b') buffered v2 inserted BEFORE canonical v1 (e.g. a canonical pass whose wall clock lags). DEFECT.
  { id: "b-rev", inserts: [bufferedRow("b-rev", 2), canonicalRow("b-rev", 1)], survivor: "buffered", defect: true },
  // Tie, canonical first: the last inserted (buffered) survives. DEFECT.
  { id: "tie-cb", inserts: [canonicalRow("tie-cb", 5), bufferedRow("tie-cb", 5)], survivor: "buffered", defect: true },
  // Tie, buffered first: the last inserted (canonical) survives. SAFE, but only by insert order.
  { id: "tie-bc", inserts: [bufferedRow("tie-bc", 5), canonicalRow("tie-bc", 5)], survivor: "canonical", defect: false },
  // Clock skew, receiver AHEAD of the reconcile host by more than LAG; feeder replay after canonical. DEFECT.
  { id: "skew-ahead", inserts: [canonicalRow("skew-ahead", hostVersion(0)), bufferedRow("skew-ahead", receiverVersion(LAG + 1_000))], survivor: "buffered", defect: true },
  // Reconcile host BEHIND by more than LAG (same effect from the other clock). DEFECT.
  { id: "host-behind", inserts: [canonicalRow("host-behind", hostVersion(-(LAG + 1_000))), bufferedRow("host-behind", receiverVersion(0))], survivor: "buffered", defect: true },
  // Skew exactly LAG: equal millisecond, replay after canonical -> tie, last inserted wins. DEFECT.
  { id: "skew-tie", inserts: [canonicalRow("skew-tie", hostVersion(0)), bufferedRow("skew-tie", receiverVersion(LAG))], survivor: "buffered", defect: true },
  // Receiver BEHIND / host ahead: canonical later by more than LAG. SAFE, by wall clock only.
  { id: "skew-behind", inserts: [canonicalRow("skew-behind", hostVersion(1_000)), bufferedRow("skew-behind", receiverVersion(-1_000))], survivor: "canonical", defect: false },
];

suite("transcript_events version order: canonical vs buffered (real ClickHouse)", () => {
  let ch: Ch;
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const ddl of ORIGIN_DDL) await ch.command(ddl);
    // Hold merges so the pre-merge state is observable; the OPTIMIZE test restarts them.
    await ch.command(`SYSTEM STOP MERGES ${EVENTS_TABLE}`);
    // One INSERT per row: separate parts, so versions coexist until a merge.
    for (const c of CASES) for (const r of c.inserts) await ch.insert(EVENTS_TABLE, [r]);
  });
  afterAll(async () => { await ch?.drop(); });

  test("the real system.columns definition passes the feeder's origin column check (default_kind DEFAULT)", async () => {
    await expect(assertOriginColumns(ch)).resolves.toBeUndefined();
  });

  test("before any merge: both physical rows exist; FINAL and the read API pick by version, ties by insert order", async () => {
    for (const c of CASES) {
      const rows = await physical(ch, c.id);
      expect(rows.map((r) => r.origin).sort(), c.id).toEqual(["buffered", "canonical"]);
      expect(await canonicalFilter(ch, c.id), c.id).toBe(1);
      const [kept] = await final(ch, c.id);
      expect(kept!.origin, c.id).toBe(c.survivor);
      // Gate not wired: canonical-only on the NEWEST version. A buffered newest version hides the
      // event even though its canonical row is still physically present.
      // DEFECT (version-order) for c.defect: the canonical event is not served.
      // (Ties are not asserted here: `ORDER BY version DESC LIMIT 1 BY` picks either row on a tie.)
      if (!c.id.includes("tie")) expect(await servedCanonical(ch, `s-${c.id}`), c.id).toEqual(c.survivor === "canonical" ? [c.id] : []);
    }
  });

  test("(c) after OPTIMIZE TABLE ... FINAL: the loser is physically gone; origin='canonical' cannot restore it", async () => {
    await ch.command(`SYSTEM START MERGES ${EVENTS_TABLE}`);
    await ch.command(`OPTIMIZE TABLE ${EVENTS_TABLE} FINAL`);
    for (const c of CASES) {
      const rows = await physical(ch, c.id);
      expect(rows.length, c.id).toBe(1);
      expect(rows[0]!.origin, c.id).toBe(c.survivor);
      if (c.defect) {
        // DEFECT (version-order): the canonical row of this identity no longer exists anywhere;
        // filtering origin = 'canonical' (or stopping the feeder) returns nothing.
        expect(await canonicalFilter(ch, c.id), c.id).toBe(0);
        expect(await servedCanonical(ch, `s-${c.id}`), c.id).toEqual([]);
      } else {
        // SAFE: canonical survives (by version, or for tie-bc by insert order only).
        expect(await canonicalFilter(ch, c.id), c.id).toBe(1);
        expect(await servedCanonical(ch, `s-${c.id}`), c.id).toEqual([c.id]);
      }
    }
  });
});

// ------------------------------------------------------------- (d) the real feeder path

const NOW = Date.parse("2026-11-07T12:00:00.000Z");
const EV_TS = NOW - 3_600_000;
const ORG = "org-a", WS = "ws-a", USER = "user-1", ORIGIN = "origin-1";
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));
const sha = (v: string) => createHash("sha256").update(v).digest("hex");

/** One session of the same shape buffered-search-feed.test.ts uses; `tag` makes identities distinct per case. */
function batchRecords(tag: string) {
  const r = (over: Parameters<typeof record>[0]) => ({ ...record({ origin_id: ORIGIN, ...over }), digest: sha(JSON.stringify(over)) });
  return [
    r({ record_id: `h1${tag}`, kind: "history", session_id: `sess${tag}`, payload: { prompt: "find the flaky test", ts_ms: EV_TS } }),
    r({ record_id: `a1${tag}`, kind: "session_event", session_id: `sess${tag}`, payload: { text: "running the suite", role: "assistant", ts_ms: EV_TS + 1 } }),
  ];
}

/** Stage records exactly as deliver.ts does in buffered mode, received at `receivedAt`. */
async function stage(store: ObjectStore, records: ReturnType<typeof batchRecords>, receivedAt: Date) {
  const claim = randomUUID();
  const grouped = groupRecords(records, receivedAt);
  const prepared = prepareBufferedStage("96bulls",
    { claim_token: claim, batch_digest: sha(claim), received_at: receivedAt.toISOString(), org_id: ORG, workspace_id: WS, origin_id: ORIGIN, user_id: USER },
    grouped.groups, grouped.index);
  await putBufferedStage(store, prepared);
  // The stage token the feed RPC reports; buffered fingerprints are bound to it (bufferedFingerprint, #383).
  return { claim, token: randomUUID(), grouped, prepared, decoded: await readBufferedStage(store, prepared.spec, zstd) };
}

/** A live stage as the 043870 stage() RPC would report it (everything retained). */
function liveSource(s: Awaited<ReturnType<typeof stage>>, retained?: () => number[]): BufferedFeedSource {
  const token = s.token;
  const state = (): StageState => ({
    found: true, status: "live", reason: null, retained: retained ? retained() : s.prepared.spec.groupOrdinals.flat().sort((a, b) => a - b),
    tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: ORIGIN, batchId: `batch-${token}`, claimToken: s.claim,
    stage: { ...s.prepared.spec, stageToken: token, createdAt: s.decoded.header.received_at },
  });
  return { page: async () => ({ after: null, stages: [] }), stage: async () => state(), advance: async () => true };
}

/** indexSearchScope against the real ClickHouse, Supabase faked to one live pointer per scope. */
function canonicalDeps(store: ObjectStore, ch: Ch, keys: string[], canonical: string, at: number): SearchIndexDeps {
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: keys.map((manifest_key) => ({ manifest_key, canonical_id: canonical, event_epoch: "0" })) as R[] };
      if (text.includes("AS canonical_id FROM sessions.session_index")) return { rows: [{ canonical_id: canonical }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return {
    transact: async (work) => work(tx), store, ch,
    reader: objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" }),
    prefixFor: (org) => `96bulls/${org}`, now: () => new Date(at),
    writeEvents: async () => ({ written: true }) as never, writeCatalog: async () => undefined as never,
  };
}
const scopeOf = (g: Group): SessionScope => ({ orgId: ORG, workspaceId: layoutComponent(WS), harness: layoutComponent(g.source), sessionId: layoutComponent(g.sessionId) });

/** Publish the batch canonically (session manifests) and run the canonical indexer at wall clock `at`. */
async function canonicalPass(store: ObjectStore, ch: Ch, s: Awaited<ReturnType<typeof stage>>, at: number) {
  const written = await writeGroups({ store, keyRoot: "96bulls", now: () => new Date(at) }, { orgId: ORG, workspaceId: WS, userId: USER, originId: ORIGIN, batchDigest: sha(`canonical-${s.claim}`) }, s.grouped.groups, new Date(at));
  const keys = written.manifests.map((m) => m.key);
  await reindex(store, ch, s, keys, at);
  return keys;
}
/** Only the canonical indexer pass (no S3 write) over already-published manifests, at wall clock `at`. */
async function reindex(store: ObjectStore, ch: Ch, s: Awaited<ReturnType<typeof stage>>, keys: string[], at: number) {
  for (const [i, g] of s.grouped.groups.entries())
    expect(await indexSearchScope(scopeOf(g), canonicalDeps(store, ch, [keys[i]!], g.sessionId, at))).toMatchObject({ complete: true });
}

const feedDeps = (source: BufferedFeedSource, store: ObjectStore, ch: Ch): BufferedFeedDeps =>
  ({ source, store, ch, keyRoot: "96bulls", now: () => NOW, sleep: async () => {} });

suite("(d) feeder replay after canonical, through feedBufferedStageByKey (real ClickHouse)", () => {
  let ch: Ch;
  const cleanups: (() => void)[] = [];
  const newStore = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const ddl of ORIGIN_DDL) await ch.command(ddl);
  });
  afterEach(() => { for (const done of cleanups.splice(0)) done(); });
  afterAll(async () => { await ch?.drop(); });

  const eventIds = (s: Awaited<ReturnType<typeof stage>>) => projectBufferedStage(s.decoded, new Set(s.prepared.spec.groupOrdinals.flat()), "96bulls", s.token).rows.map((r) => r.row.event_id);
  const settle = async () => { await ch.command(`OPTIMIZE TABLE ${EVENTS_TABLE} FINAL`); };

  test("d1: replay of the SAME stage after its canonical pass: canonical survives (received_at < canonical read time)", async () => {
    const store = newStore();
    const s = await stage(store, batchRecords("-d1"), new Date(NOW - 30 * 60_000));
    await canonicalPass(store, ch, s, NOW);
    expect(await feedBufferedStageByKey(s.prepared.spec.key, feedDeps(liveSource(s), store, ch))).toMatchObject({ outcome: "indexed", deleted: 0 });
    await settle();
    // SAFE, but only because the canonical indexer's wall clock (NOW) exceeds received_at; the
    // 043870 RPC would normally also report this stage gone (batch_not_buffered) once published.
    for (const id of eventIds(s)) {
      expect((await physical(ch, id)).map((r) => r.origin), id).toEqual(["canonical"]);
      expect(await canonicalFilter(ch, id), id).toBe(1);
    }
  });

  test("d2: a LATER-received stage carrying the same records after the canonical pass replaces canonical; a later canonical pass restores it", async () => {
    const store = newStore();
    const first = await stage(store, batchRecords("-d2"), new Date(NOW - 30 * 60_000));
    await canonicalPass(store, ch, first, NOW);
    // The same records re-uploaded in another batch (client retry / re-export), received after the canonical read.
    const later = await stage(store, batchRecords("-d2"), new Date(NOW + 60_000));
    expect(await feedBufferedStageByKey(later.prepared.spec.key, feedDeps(liveSource(later), store, ch))).toMatchObject({ outcome: "indexed", deleted: 0 });
    await settle();
    for (const id of eventIds(later)) {
      // DEFECT (version-order): the canonical row is physically gone; only the buffered copy remains.
      expect((await physical(ch, id)).map((r) => r.origin), id).toEqual(["buffered"]);
      expect(await canonicalFilter(ch, id), id).toBe(0);
    }
    // RECOVERY: the next canonical pass over the scope sees a `bf` fingerprint != its `s3` one and
    // rewrites the row with its newer wall-clock version. The only repair path today.
    await canonicalPass(store, ch, first, NOW + 120_000);
    await settle();
    for (const id of eventIds(first)) expect((await physical(ch, id)).map((r) => r.origin), id).toEqual(["canonical"]);
  });

  test("d3: after d2's replacement, the feeder's own exact-provenance cleanup leaves the identity with NO row at all", async () => {
    const store = newStore();
    const first = await stage(store, batchRecords("-d3"), new Date(NOW - 30 * 60_000));
    await canonicalPass(store, ch, first, NOW);
    const later = await stage(store, batchRecords("-d3"), new Date(NOW + 60_000));
    expect(await feedBufferedStageByKey(later.prepared.spec.key, feedDeps(liveSource(later), store, ch))).toMatchObject({ outcome: "indexed" });
    await settle();
    // Retention later drops the later stage's records (e.g. GC / deletion): the recheck or the
    // erasure lane deletes exactly the buffered rows it wrote (origin + claim + fingerprint + version).
    const rows = projectBufferedStage(later.decoded, new Set(later.prepared.spec.groupOrdinals.flat()), "96bulls", later.token).rows.map((r) => r.row);
    await deleteBufferedRows(ch, rows);
    for (const id of eventIds(later)) {
      // DEFECT (version-order): the canonical event that S3 still holds is absent from the index
      // until a canonical pass runs over the scope again.
      expect(await physical(ch, id), id).toEqual([]);
      expect(await canonicalFilter(ch, id), id).toBe(0);
    }
  });
});

// ------------------------------------------- legacy raw-ms canonical vs candidate buffered encodings
//
// Every canonical row in production carries a raw positive epoch-ms `version` (runBackfill: now()
// before each Neon page read; reconcileSession: now() once per session pass), and nothing
// validates the version domain. A buffered encoding is only safe if EVERY encoded buffered value is
// strictly below EVERY legacy canonical value without rewriting old rows, while staying monotonic
// in received_at among buffered rows. Two candidates are exercised factually; neither is chosen here.

/** A deliberately low legacy canonical value (2025-01-01); any real legacy row is at or above its write time. */
const LEGACY_LOW = Date.parse("2025-01-01T00:00:00.000Z");
/** A legacy canonical row written at the canonical read of a recent batch. */
const LEGACY_NOW = Date.parse("2026-11-07T12:00:00.000Z");
/** Candidate S: buffered version = received_at in whole seconds (~1.76e9 vs legacy ~1.7e12+). */
const encSeconds = (receivedMs: number) => Math.floor(receivedMs / 1_000);
/** Candidate O: buffered version = received_at ms minus E = 2026-01-01T00:00:00Z = 1_767_225_600_000. */
const BUFFERED_EPOCH_MS = Date.parse("2026-01-01T00:00:00.000Z");
const encOffset = (receivedMs: number) => {
  const v = receivedMs - BUFFERED_EPOCH_MS;
  // UInt64 has no negatives: a receiver clock before E would wrap to a huge value. The encoder must refuse.
  if (!Number.isSafeInteger(v) || v <= 0) throw new Error("buffered_version_before_epoch");
  return v;
};
/**
 * A high-bit canonical band (the owner's alternative). 2^52 keeps the test in JS-safe integers;
 * a real 2^63 band behaves the same in ClickHouse but needs bigint/string transport.
 */
const CANONICAL_BAND = 2 ** 52;

const legacyCanonical = (id: string, version: number) => ({ ...row({ id, ts: TS, session: `s-${id}`, version }), fingerprint: `s3${id}` });
const encodedBuffered = (id: string, version: number, tag: string) =>
  ({ ...row({ id, ts: TS, session: `s-${id}`, version }), fingerprint: `bf${id}-${tag}`, origin: "buffered", origin_claim_token: CLAIM });

interface EncCase { id: string; inserts: ReturnType<typeof legacyCanonical>[]; survivor: string; note: string }
function encodingCases(name: "sec" | "off", enc: (ms: number) => number): EncCase[] {
  const id = (s: string) => `${name}-${s}`;
  const R = Date.parse("2026-11-07T12:01:00.000Z"); // received AFTER the LEGACY_NOW canonical read
  const FAR = Date.parse("2040-01-01T00:00:00.000Z"); // far-future receiver vs the lowest legacy row
  // Same encoded value: same second for S (400 ms apart), same ms for O.
  const tieA = R, tieB = name === "sec" ? R + 400 : R;
  return [
    // Legacy canonical vs encoded buffered: canonical must win in both insert orders.
    { id: id("now-cb"), inserts: [legacyCanonical(id("now-cb"), LEGACY_NOW), encodedBuffered(id("now-cb"), enc(R), "r")], survivor: `s3${id("now-cb")}`, note: "received after canonical read; raw ms would lose (case b)" },
    { id: id("now-bc"), inserts: [encodedBuffered(id("now-bc"), enc(R), "r"), legacyCanonical(id("now-bc"), LEGACY_NOW)], survivor: `s3${id("now-bc")}`, note: "reverse insert order" },
    { id: id("far-cb"), inserts: [legacyCanonical(id("far-cb"), LEGACY_LOW), encodedBuffered(id("far-cb"), enc(FAR), "far")], survivor: `s3${id("far-cb")}`, note: "lowest legacy vs 2040 receiver" },
    { id: id("far-bc"), inserts: [encodedBuffered(id("far-bc"), enc(FAR), "far"), legacyCanonical(id("far-bc"), LEGACY_LOW)], survivor: `s3${id("far-bc")}`, note: "reverse insert order" },
    // Buffered vs buffered: the later-received stage wins in both insert orders.
    { id: id("bb-el"), inserts: [encodedBuffered(id("bb-el"), enc(R), "early"), encodedBuffered(id("bb-el"), enc(R + 2_000), "late")], survivor: `bf${id("bb-el")}-late`, note: "later stage wins" },
    { id: id("bb-le"), inserts: [encodedBuffered(id("bb-le"), enc(R + 2_000), "late"), encodedBuffered(id("bb-le"), enc(R), "early")], survivor: `bf${id("bb-le")}-late`, note: "later stage wins regardless of insert order" },
    // Equal encoded version: the LAST INSERTED buffered row wins (both orders), not the later-received one.
    { id: id("tie-ab"), inserts: [encodedBuffered(id("tie-ab"), enc(tieA), "a"), encodedBuffered(id("tie-ab"), enc(tieB), "b")], survivor: `bf${id("tie-ab")}-b`, note: "tie: last insert wins" },
    { id: id("tie-ba"), inserts: [encodedBuffered(id("tie-ba"), enc(tieB), "b"), encodedBuffered(id("tie-ba"), enc(tieA), "a")], survivor: `bf${id("tie-ba")}-a`, note: "tie: last insert wins (for S: the EARLIER-received stage, 400 ms older, wins)" },
  ];
}
const ENC_CASES: EncCase[] = [
  ...encodingCases("sec", encSeconds),
  ...encodingCases("off", encOffset),
  // High-bit canonical band ALONE: a legacy raw-ms canonical row that has not been rewritten
  // still loses to an unchanged raw-ms buffered row. DEFECT (version-order): the band does not
  // protect legacy rows; only rewritten canonical rows (band-new) win.
  { id: "band-legacy", inserts: [legacyCanonical("band-legacy", LEGACY_NOW), encodedBuffered("band-legacy", LEGACY_NOW + 60_000, "raw")], survivor: "bfband-legacy-raw", note: "legacy canonical not rewritten" },
  { id: "band-new", inserts: [legacyCanonical("band-new", CANONICAL_BAND + LEGACY_NOW), encodedBuffered("band-new", LEGACY_NOW + 60_000, "raw")], survivor: "s3band-new", note: "canonical rewritten into the band" },
];

describe("candidate buffered encodings: arithmetic bounds (pure, runs without ClickHouse)", () => {
  test("arithmetic bounds of the candidates hold for the values used", () => {
    // S: seconds stay ~1000x below any ms value after 1970-01-12; they reach LEGACY_LOW only in year ~57,000.
    expect(encSeconds(Date.parse("2100-01-01T00:00:00.000Z"))).toBeLessThan(LEGACY_LOW);
    // O: crosses LEGACY_LOW when received_ms >= E + LEGACY_LOW (= 2081-01-01..; earlier if legacy rows are older).
    expect(encOffset(Date.parse("2080-01-01T00:00:00.000Z"))).toBeLessThan(LEGACY_LOW);
    expect(encOffset(Date.parse("2082-01-01T00:00:00.000Z"))).toBeGreaterThan(LEGACY_LOW);
    expect(() => encOffset(BUFFERED_EPOCH_MS - 1)).toThrow("buffered_version_before_epoch");
  });
});

suite("legacy raw-ms canonical vs candidate buffered encodings (real ClickHouse)", () => {
  let ch: Ch;
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const ddl of ORIGIN_DDL) await ch.command(ddl);
    await ch.command(`SYSTEM STOP MERGES ${EVENTS_TABLE}`);
    for (const c of ENC_CASES) for (const r of c.inserts) await ch.insert(EVENTS_TABLE, [r]);
  });
  afterAll(async () => { await ch?.drop(); });

  test("before any merge, FINAL keeps the expected survivor", async () => {
    for (const c of ENC_CASES) {
      expect((await physical(ch, c.id)).length, c.id).toBe(2);
      const [kept] = await final(ch, c.id);
      expect(kept!.fingerprint, `${c.id}: ${c.note}`).toBe(c.survivor);
    }
  });

  test("after OPTIMIZE TABLE ... FINAL, exactly the expected survivor remains", async () => {
    await ch.command(`SYSTEM START MERGES ${EVENTS_TABLE}`);
    await ch.command(`OPTIMIZE TABLE ${EVENTS_TABLE} FINAL`);
    for (const c of ENC_CASES) {
      const rows = await physical(ch, c.id);
      expect(rows.map((r) => r.fingerprint), `${c.id}: ${c.note}`).toEqual([c.survivor]);
      // Every legacy-vs-encoded case keeps canonical visible; band-legacy is the documented DEFECT.
      if (c.survivor.startsWith("s3")) expect(await canonicalFilter(ch, c.id), c.id).toBe(1);
    }
  });
});

// ------------------------------------------------------------- reconcile deleteRows vs buffered copies

/** reconcile.ts deleteRows keys: identity + ts, NO origin predicate. */
const pendingDeletes = (rows: readonly IndexRow[]): PendingDelete[] => rows.map((r) => ({
  orgId: r.org_id, sessionId: r.session_id,
  identity: { workspaceId: r.workspace_id, machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id, tsMs: tsMillis(`${r.ts.replace(" ", "T")}Z`), fingerprint: r.fingerprint },
}));

suite("reconcile deleteRows on an identity with canonical AND buffered copies (real ClickHouse)", () => {
  let ch: Ch;
  const cleanups: (() => void)[] = [];
  const newStore = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const ddl of ORIGIN_DDL) await ch.command(ddl);
  });
  afterEach(() => { for (const done of cleanups.splice(0)) done(); });
  afterAll(async () => { await ch?.drop(); });
  const origins = async (id: string) => (await physical(ch, id)).map((r) => r.origin).sort();

  /** Canonical pass at NOW, then the feeder for the same (still live) stage: both copies physically present. */
  async function bothCopies(tag: string) {
    const store = newStore();
    const s = await stage(store, batchRecords(tag), new Date(NOW - 30 * 60_000));
    await ch.command(`SYSTEM STOP MERGES ${EVENTS_TABLE}`);
    const keys = await canonicalPass(store, ch, s, NOW);
    const source = liveSource(s);
    expect(await feedBufferedStageByKey(s.prepared.spec.key, feedDeps(source, store, ch))).toMatchObject({ outcome: "indexed", deleted: 0 });
    const rows = projectBufferedStage(s.decoded, new Set(s.prepared.spec.groupOrdinals.flat()), "96bulls", s.token).rows.map((r) => r.row);
    for (const r of rows) expect(await origins(r.event_id), r.event_id).toEqual(["buffered", "canonical"]);
    // Lightweight DELETE is a mutation; restart merges so it can complete (lightweight_deletes_sync = 2).
    await ch.command(`SYSTEM START MERGES ${EVENTS_TABLE}`);
    return { store, s, keys, source, rows };
  }

  test("deleteRows removes both copies; feeder cleanup is a no-op; a later canonical pass restores canonical only", async () => {
    const { store, s, keys, rows } = await bothCopies("-rd1");
    await deleteRows(ch, pendingDeletes(rows));
    // Reconcile's delete has no origin predicate: every provenance copy of the identity is gone.
    for (const r of rows) expect(await physical(ch, r.event_id), r.event_id).toEqual([]);
    // (b) The feeder's exact-provenance cleanup finds nothing and does not error. Its return value
    // is the number of keys issued, not rows removed.
    await expect(deleteBufferedRows(ch, rows)).resolves.toBe(rows.length);
    for (const r of rows) expect(await physical(ch, r.event_id), r.event_id).toEqual([]);
    // (a) The next canonical pass re-inserts the canonical copy only.
    await reindex(store, ch, s, keys, NOW + 120_000);
    await ch.command(`OPTIMIZE TABLE ${EVENTS_TABLE} FINAL`);
    for (const r of rows) expect(await origins(r.event_id), r.event_id).toEqual(["canonical"]);
    // (b) And a later feeder cleanup of the old buffered rows leaves the restored canonical row alone.
    await deleteBufferedRows(ch, rows);
    for (const r of rows) expect(await origins(r.event_id), r.event_id).toEqual(["canonical"]);
  });

  test("(c) replaying a still-live stage after the reconcile delete re-inserts ONLY the buffered copy", async () => {
    const { store, s, source, rows } = await bothCopies("-rd2");
    await deleteRows(ch, pendingDeletes(rows));
    for (const r of rows) expect(await physical(ch, r.event_id), r.event_id).toEqual([]);
    expect(await feedBufferedStageByKey(s.prepared.spec.key, feedDeps(source, store, ch))).toMatchObject({ outcome: "indexed", deleted: 0 });
    // Factual: the buffered copy is back, the canonical copy is not, until a canonical pass covers
    // the scope. If reconcile deleted because Neon/S3 no longer hold the event (erasure), the
    // replay resurrects it in the index for as long as the stage stays live.
    for (const r of rows) expect(await origins(r.event_id), r.event_id).toEqual(["buffered"]);
  });
});
