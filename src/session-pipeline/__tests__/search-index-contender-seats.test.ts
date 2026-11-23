/**
 * `contender_seats` (contender-seats.ts): how a read pass's relayhistory_acceptance_missing
 * refusal looked, on the whole read and on the streamed read, and on the consumer's lines.
 * Only a read pass with the reason carries it; a skipped-before-read pass and every other
 * pass do not.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
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
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import {
  ACCEPTANCE_MISSING,
  SEARCH_INDEX_STREAM_CH_BUDGET,
  acceptanceDigest,
  type PointerFacts,
  type SearchIndexDeps,
  type SearchIndexResult,
} from "../search-index.ts";
import { indexSearchScope } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { memoryLock } from "./support.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string): SessionLocation => ({
  prefix: "96bulls/org_1", workspaceId: scope.workspaceId, machineId, harness: scope.harness, sessionId: scope.sessionId, startDate: "2026-11-01",
});
const t = (s: number) => Date.parse(`2026-11-01T10:00:${String(s).padStart(2, "0")}Z`);
const rec = (kind: string, record_id: string, revision: string, payload: Record<string, unknown> | null, o: Partial<RelayhistoryStreamRecord> = {}): RelayhistoryStreamRecord => ({
  kind, origin_id: "desktop-origin", record_id, revision, operation: "upsert", received_at: AT, payload, ...o,
});
const say = (id: string, s: number, o: Partial<RelayhistoryStreamRecord> = {}) =>
  rec("session_event", id, "1", { ts_ms: t(s), text: `text ${s}`, role: "assistant" }, o);
const sessionRow = (id: string, o: Partial<RelayhistoryStreamRecord> = {}) =>
  rec("session", id, "1", { project_key: "github.com/acme/widgets", git_branch: "main", agent_name: "codex" }, o);
const direct = (id: string, s: number, eventId: string, o: Partial<RelayhistoryStreamRecord> = {}) =>
  rec("convergence_event", id, "1", {
    source: "claude", session_id: scope.sessionId, machine_id: "srv_aa", user_id: "user_1", event_id: eventId, kind: "assistant", type: "assistant",
    ts: new Date(t(s)).toISOString(), content: `direct ${s}`, tags: [], files_touched: [], task_ref: {},
  }, o);
/** The whole read's reasons: the per-manifest fallback refuses a single-manifest group too. */
const WHOLE = [ACCEPTANCE_MISSING, "manifest_fold_failed"];
/** Same record id from a second origin. */
const other = { origin_id: "second-origin" };

let root: string;
let store: ObjectStore;
let reader: ManifestReader;
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-seats-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});
afterEach(async () => { await rm(root, { recursive: true, force: true }); });

async function writeSession(machine: string, chunks: RelayhistoryStreamRecord[][]) {
  const location = loc(machine);
  const entries: ChunkEntry[] = [];
  let offset = 0;
  for (const [seq, records] of chunks.entries()) {
    const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = {
      seq, epoch: 0, start_offset: offset, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
      stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: records.length,
    };
    offset += source.byteLength;
    entries.push(chunk);
    await store.put(rawChunkKey(location, chunk.seq, chunk.start_offset, sha, "ndjson"), stored, {});
  }
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1, created_at: AT, updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: scope.sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: entries.length, raw_bytes: offset,
      stored_bytes: entries.reduce((n, c) => n + c.stored_bytes, 0), content_sha256: await computeContentSha256(entries), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: machine }], next_offset: offset, chunks: entries,
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1" } },
  } as unknown as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}

/** ClickHouse that holds nothing; `stamp` answers the acceptance stamp read. */
function fakeClickHouse(stamp?: { value: string; at_ms: number }) {
  const statements: string[] = [];
  const ch: ClickHouseClient = {
    database: "test",
    async command(query) { statements.push(`command ${query.trim().slice(0, 40)}`); },
    async select<T>(query: string) {
      statements.push(`select ${query.trim().replace(/\s+/g, " ")}`);
      if (stamp && query.includes(INGEST_STATE_TABLE) && query.includes("ORDER BY updated_at DESC")) return [stamp] as T[];
      return [] as T[];
    },
    async insert(table) { statements.push(`insert ${table}`); },
  };
  return { ch, statements };
}

/** The pointer listing: keys alone, or with `facts` the flag-on listing. */
function fakeSupabase(live: () => string[], facts?: PointerFacts[]) {
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: (facts ?? live().map((manifest_key) => ({ manifest_key }))) as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

function deps(ch: ClickHouseClient, live: () => string[], extra: Partial<SearchIndexDeps> = {}): SearchIndexDeps {
  return {
    transact: fakeSupabase(live).transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW,
    writeCatalog: async () => ({ written: true }), ...extra,
  };
}
/** Whole-read bound 1 event: every fixture streams when armed. */
const tiny = { maxScopeEvents: 1 };
const armed = { limits: tiny, stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET } };

const whole = async (keys: string[]) => indexSearchScope(scope, deps(fakeClickHouse().ch, () => keys));
const streamed = async (keys: string[]) => indexSearchScope(scope, deps(fakeClickHouse().ch, () => keys, armed));
const both = async (keys: string[]) => [await whole(keys), await streamed(keys)] as const;

/** One unseated record id group from two origins. */
const unseatedPair = (id: string, s: number) => [say(id, s), say(id, s, other)];
/** One record id group from two origins, one of them seated. */
const mixedPair = (id: string, s: number) => [say(id, s, { accepted_seq: "4" }), say(id, s, other)];

describe("record id groups", () => {
  test("all_unseated, seated_unseated and both, on the whole and the streamed read", async () => {
    for (const [records, word] of [
      [[...unseatedPair("dup-a", 1), say("solo", 2)], "all_unseated"],
      [[...mixedPair("dup-b", 1), say("solo", 2)], "seated_unseated"],
      [[...unseatedPair("dup-a", 1), ...mixedPair("dup-b", 3)], "both"],
    ] as const) {
      const key = await writeSession(`srv_${word}`, [[...records]]);
      const [w, s] = await both([key]);
      expect([word, w]).toEqual([word, { manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, reasons: WHOLE, contenderSeats: word }]);
      expect([word, s]).toEqual([word, { manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, streamed: true, reasons: [ACCEPTANCE_MISSING], contenderSeats: word }]);
    }
  });

  test("a group spread over manifests and chunks, merged seats and fully seated groups", async () => {
    // seated, seated, unseated in one group: a seated member, so seated_unseated. A fully
    // seated group and a group of one are not refused and do not count.
    const a = await writeSession("srv_aa", [[say("g", 1, { accepted_seq: "1" }), say("ok", 2, { accepted_seq: "2" }), say("one", 3)]]);
    const b = await writeSession("srv_bb", [[say("g", 1, { origin_id: "third-origin", accepted_seq: "5" })], [say("ok", 2, { ...other, accepted_seq: "3" })]]);
    const c = await writeSession("srv_cc", [[say("g", 1, other)]]);
    for (const r of await both([a, b, c])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "seated_unseated"]);
    // A fully seated group next to an unseated one: only the unseated group counts.
    const f = await writeSession("srv_ff", [[say("ok2", 1, { accepted_seq: "1" }), say("ok2", 1, { ...other, accepted_seq: "2" }), ...unseatedPair("u2", 2)]]);
    for (const r of await both([f])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "all_unseated"]);
    // Two copies of one (origin, record, revision): the seat the fold merges is seated, so the
    // group has a seated member even though one copy has none.
    const d = await writeSession("srv_dd", [[say("m", 1), ...unseatedPair("u", 2)], [say("m", 1, { accepted_seq: "7" }), say("m", 1, other)]]);
    for (const r of await both([d])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "both"]);
    // A newer revision replaces the older seat: the old seated revision does not count.
    const e = await writeSession("srv_ee", [[say("n", 1, { accepted_seq: "1" }), rec("session_event", "n", "2", { ts_ms: t(1), text: "edited", role: "assistant" }), say("n", 1, other)]]);
    for (const r of await both([e])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "all_unseated"]);
  });

  test("tombstones contend", async () => {
    const key = await writeSession("srv_aa", [[say("x", 1, { accepted_seq: "2" }), rec("session_event", "x", "1", null, { ...other, operation: "delete" }), say("y", 3)]]);
    for (const r of await both([key])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "seated_unseated"]);
  });

  test("convergence events and legacy kinds never contend", async () => {
    // Two unseated direct lines with one record id: the fold does not refuse them (the second
    // origin's direct line fails its own identity check later, in the projection).
    const fine = await writeSession("srv_aa", [[direct("d1", 1, "ev-1"), direct("d1", 1, "ev-2", other), say("solo", 2)]]);
    for (const r of await both([fine])) {
      expect(r.reasons).toEqual(["relayhistory_event_identity"]);
      expect("contenderSeats" in r).toBe(false);
    }
    // Next to a refused seated group they still do not count (else the word would be both).
    const mixed = await writeSession("srv_bb", [[direct("d2", 1, "ev-3"), direct("d2", 1, "ev-4", other), ...mixedPair("dup", 2)]]);
    for (const r of await both([fine, mixed])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "seated_unseated"]);
    // Two unseated legacy_* records with one record id from two origins: not refused (as on main).
    const legacy = await writeSession("srv_cc", [[rec("legacy_scope", "lg", "1", { note: "n" }), rec("legacy_scope", "lg", "1", { note: "n" }, other), say("solo", 2)]]);
    const [lw, ls] = await both([legacy]);
    expect(lw).toMatchObject({ complete: true, reasons: [] });
    expect(ls).toEqual({ ...lw, streamed: true });
    for (const r of [lw, ls]) expect("contenderSeats" in r).toBe(false);
    // Next to a refused seated group they do not count either (else the word would be both).
    const legacyMixed = await writeSession("srv_dd", [[rec("legacy_session_link", "lg2", "1", { note: "n" }), rec("legacy_session_link", "lg2", "1", { note: "n" }, other), ...mixedPair("dup2", 2)]]);
    for (const r of await both([legacyMixed])) expect([r.reasons[0], r.contenderSeats]).toEqual([ACCEPTANCE_MISSING, "seated_unseated"]);
  });

  test("a legacy record has its own seat: it never hides a delivery record of the same (origin, record id)", async () => {
    // A newer legacy revision under the desktop origin's "x": one seat per (origin, record)
    // would replace the desktop delivery record and leave "x" a group of one, unrefused.
    const key = await writeSession("srv_aa", [[...unseatedPair("x", 1), rec("legacy_scope", "x", "2", { note: "n" }), say("solo", 2)]]);
    const [w, s] = await both([key]);
    expect(w).toEqual({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, reasons: WHOLE, contenderSeats: "all_unseated" });
    expect(s).toEqual({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, streamed: true, reasons: [ACCEPTANCE_MISSING], contenderSeats: "all_unseated" });
    // Two legacy kinds with one (origin, record) and one revision but different content:
    // one seat each, so no revision conflict, on either read.
    const kinds = await writeSession("srv_bb", [[rec("legacy_scope", "k", "1", { note: "a" }), rec("legacy_session_outcome", "k", "1", { note: "b" }), say("solo", 2)]]);
    const [kw, ks] = await both([kinds]);
    expect(kw).toMatchObject({ complete: true, reasons: [] });
    expect(ks).toEqual({ ...kw, streamed: true });
  });
});

describe("catalog_key", () => {
  test("only the catalog-key fold refused: catalog_key on the whole and the streamed read", async () => {
    // Two session rows with different record ids share one catalog key; one is unseated.
    const key = await writeSession("srv_aa", [[sessionRow("s-1", { accepted_seq: "3" }), say("solo", 1)], [sessionRow("s-2")]]);
    const [w, s] = await both([key]);
    expect(w).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING], contenderSeats: "catalog_key" });
    expect(s).toEqual({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, streamed: true, reasons: [ACCEPTANCE_MISSING], contenderSeats: "catalog_key" });
  });

  test("a record id refusal wins even when the catalog-key fold refused too", async () => {
    // The record id group spans two manifests, so the whole read's per-manifest fallback
    // folds each and srv_aa's projection then refuses on the catalog key as well.
    const a = await writeSession("srv_aa", [[sessionRow("s-1", { accepted_seq: "3" }), sessionRow("s-2"), say("dup", 1)]]);
    const b = await writeSession("srv_bb", [[say("dup", 1, other)]]);
    const [w, s] = await both([a, b]);
    expect(w).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING], contenderSeats: "all_unseated" });
    expect(s).toMatchObject({ streamed: true, reasons: [ACCEPTANCE_MISSING], contenderSeats: "all_unseated" });
  });
});

describe("only a read pass with the reason carries it", () => {
  test("the reason next to another reason still carries the word", async () => {
    const key = await writeSession("srv_aa", [[...unseatedPair("dup", 1)]]);
    const missing = manifestKey(loc("srv_zz"));
    const r = await whole([key, missing]);
    expect(r.reasons).toContain(ACCEPTANCE_MISSING);
    expect(r.reasons.length).toBeGreaterThan(1);
    expect(r.contenderSeats).toBe("all_unseated");
  });

  test("passes without the reason have no key: complete, partial, and other stream stops", async () => {
    const key = await writeSession("srv_aa", [[say("a", 1), say("b", 2), sessionRow("s-1")]]);
    const results: SearchIndexResult[] = [
      await whole([key]),
      await streamed([key]),
      // Unarmed, the tiny bound refuses the scope before any fold.
      await indexSearchScope(scope, deps(fakeClickHouse().ch, () => [key], { limits: tiny })),
    ];
    expect(results.map((r) => r.reasons)).toEqual([[], [], ["scope_too_large"]]);
    for (const r of results) expect("contenderSeats" in r).toBe(false);
    // A revision conflict refuses with another code: no key.
    const conflict = await writeSession("srv_bb", [[say("c", 1), rec("session_event", "c", "1", { ts_ms: t(1), text: "different", role: "assistant" })]]);
    for (const r of await both([conflict])) {
      expect(r.reasons[0]).toBe("relayhistory_revision_conflict");
      expect(r.reasons).not.toContain(ACCEPTANCE_MISSING);
      expect("contenderSeats" in r).toBe(false);
    }
  });

  test("a skipped-before-read pass has no key", async () => {
    const key = await writeSession("srv_aa", [[...unseatedPair("dup", 1)]]);
    const facts: PointerFacts[] = [{ manifest_key: key, machine_id: "srv_aa", revision: "1", content_sha256: "ab".repeat(32), manifest_etag: "e", scrubbed: true }];
    const limits = { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000 };
    const digest = acceptanceDigest(scope, facts, "default:14d", limits, undefined);
    const { ch, statements } = fakeClickHouse({ value: JSON.stringify({ v: 1, reason: ACCEPTANCE_MISSING, digest }), at_ms: NOW.getTime() - 1_000 });
    const reads: string[] = [];
    const r = await indexSearchScope(scope, {
      ...deps(ch, () => [key], { skipAcceptanceMissing: true }),
      transact: fakeSupabase(() => [key], facts).transact,
      reader: { ...reader, read: async (k) => { reads.push(k); return reader.read(k); } },
    });
    expect(r.skippedBeforeRead).toBe(true);
    expect(r.reasons).toEqual([ACCEPTANCE_MISSING, "skipped_before_read"]);
    expect("contenderSeats" in r).toBe(false);
    expect(reads).toEqual([]);
    expect(statements.some((q) => q.includes("ORDER BY updated_at DESC"))).toBe(true);
  });
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

describe("consumer lines", () => {
  const consumer = (search: SearchIndexDeps): ConsumerDeps => {
    const db = { ...fakeSupabase(() => []).tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    return {
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (p, o) => prefixAllowed(bindings, p, o), fanout: droppedFanout },
      search,
      requeue: async () => {},
    };
  };
  const reconcile = (key: string) => [{ messageId: "m1", body: JSON.stringify({ reconcile: { key } }) }];
  const searchLines = (lines: Record<string, unknown>[]) => lines.filter((l) => l.stage === "search-index");

  test("search index reconciled carries the word only on a refused read pass; no ids", async () => {
    for (const [records, word, extra] of [
      [[...unseatedPair("dup-zz9", 1), say("solo", 2)], "all_unseated", {}],
      [mixedPair("dup-zz9", 1), "seated_unseated", armed],
      [[sessionRow("cat-zz9", { accepted_seq: "1" }), sessionRow("cat-yy8")], "catalog_key", {}],
      [[sessionRow("cat-zz9", { accepted_seq: "1" }), sessionRow("cat-yy8")], "catalog_key", armed],
    ] as const) {
      await rm(root, { recursive: true, force: true });
      const key = await writeSession("srv_aa", [[...records]]);
      const { result, lines, raw } = await captured(() => processBatch(reconcile(key), consumer(deps(fakeClickHouse().ch, () => [key], extra))));
      expect(result.batchItemFailures).toEqual([]);
      const out = searchLines(lines);
      expect([word, out.map((l) => [l.message, l.contender_seats])]).toEqual([word, [[expect.any(String), word]]]);
      expect(out[0]).toMatchObject({ message: "search index reconciled", ok: false, contender_seats: word });
      expect(String(out[0]!.reason).split(",")[0]).toBe(ACCEPTANCE_MISSING);
      for (const id of ["dup-zz9", "cat-zz9", "cat-yy8", "second-origin", "desktop-origin"]) expect([word, id, raw.includes(id)]).toEqual([word, id, false]);
    }
  });

  test("no key on a complete pass, a partial pass or a skipped-before-read pass", async () => {
    const key = await writeSession("srv_aa", [[say("a", 1), say("b", 2)]]);
    const plain = await captured(() => processBatch(reconcile(key), consumer(deps(fakeClickHouse().ch, () => [key]))));
    const partial = await captured(() => processBatch(reconcile(key), consumer(deps(fakeClickHouse().ch, () => [key], { limits: tiny }))));
    const facts: PointerFacts[] = [{ manifest_key: key, machine_id: "srv_aa", revision: "1", content_sha256: "ab".repeat(32), manifest_etag: "e", scrubbed: true }];
    const digest = acceptanceDigest(scope, facts, "default:14d", { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000 }, undefined);
    const stamped = fakeClickHouse({ value: JSON.stringify({ v: 1, reason: ACCEPTANCE_MISSING, digest }), at_ms: NOW.getTime() - 1_000 });
    const skipped = await captured(() => processBatch(reconcile(key), consumer({ ...deps(stamped.ch, () => [key], { skipAcceptanceMissing: true }), transact: fakeSupabase(() => [key], facts).transact })));
    const all = [plain, partial, skipped].map((c) => searchLines(c.lines));
    expect(all.map((ls) => ls.map((l) => l.reason ?? null))).toEqual([
      [null],
      ["scope_too_large", "scope_too_large"],
      [`${ACCEPTANCE_MISSING},skipped_before_read`],
    ]);
    for (const ls of all) for (const l of ls) expect("contender_seats" in l).toBe(false);
    for (const c of [plain, partial, skipped]) expect(c.raw.includes("contender_seats")).toBe(false);
  });
});
