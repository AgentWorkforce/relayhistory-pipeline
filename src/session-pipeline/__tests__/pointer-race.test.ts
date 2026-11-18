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
import { computeContentSha256, manifestKey, objectKey, objectStoreManifestReader, prefixAllowed, prefixOrgResolver, rawChunkKey, type ChunkEntry, type ManifestV1, type ObjectStore, type ManifestReader } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, indexPointer } from "../pointer.ts";
import { memoryLock } from "./support.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

// Every Supabase and HoopDb migration in PGlite (the reviewer's repro on #121), so
// the revision fence and the directory projection are the real SQL.
const repo = join(import.meta.dir, "../../..");
const readF = (p: string) => readFileSync(join(repo, p), "utf8");
const AT = "2026-11-01T10:00:00.000Z";
let root: string; let store: ObjectStore; let reader: ManifestReader; let pg: PGlite;
beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp"); await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-pointer-race-")); store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readF("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await pg.exec(readF(f));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readF(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort()) await pg.exec(readF(`src/hoopdb/migrations/${f}`));
}, 120_000);
afterEach(async () => { await pg.close(); await rm(root, { recursive: true, force: true }); });

const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
/** The same session after the exporter moved its start date (review #2 N1). */
const locB = { ...loc, startDate: "2026-10-31" };
/** The same session moved to a LATER start date. */
const locLater = { ...loc, startDate: "2026-11-03" };
/** A move further than the probe reaches (MOVE_PROBE_DAYS). */
const locFar = { ...loc, startDate: "2026-08-01" };
async function writeManifest(revision: number, at: typeof loc = loc) {
  const loc = at;
  const source = new TextEncoder().encode(JSON.stringify({ kind: "session_event", origin_id: "o", record_id: `r${revision}`, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: 1, text: "x" } }) + "\n");
  const stored = new Uint8Array(zstdCompressSync(source)); const sha = await sha256Hex(source);
  const chunk: ChunkEntry = { seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: 1 };
  await store.put(rawChunkKey(loc, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest = { schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision, created_at: AT, updated_at: AT,
    session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: `${loc.startDate}T10:00:00.000Z` },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "t" },
    raw: { owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0, epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: "srv_bb" }], next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 } },
    extensions: { "96bulls": { user_id: "user_1" } } } as unknown as ManifestV1;
  const key = manifestKey(loc);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}
const body = (key: string, name: string, seq: string) => JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: name, eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: seq } } }] });

const lock = memoryLock();
function deps(gate?: () => Promise<void>, afterRead?: () => Promise<void>): ConsumerDeps {
  const db = { pool: undefined, end: async () => {}, async query(text: string, params?: unknown[]) {
    if (gate && text.includes("upsert_session_index")) await gate();
    const r = await pg.query(text, params as unknown[]); return { rows: r.rows };
  } } as unknown as HoopDb;
  const b = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
  // `afterRead` pauses once, right after this Lambda's first manifest read.
  let paused = false;
  const gatedReader: ManifestReader = afterRead
    ? { ...reader, read: async (k) => { const r = await reader.read(k); if (!paused) { paused = true; await afterRead(); } return r; } }
    : reader;
  return { pointer: { db, lock, reader: gatedReader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (p, o) => prefixAllowed(b, p, o), fanout: droppedFanout } };
}
const row = async () => (await pg.query<{ revision: string; deleted_at: string | null; manifest_key: string }>("SELECT revision::text, deleted_at, manifest_key FROM sessions.session_index WHERE session_id='sess_1'")).rows[0];
const failures = async (id: string, b: string, d = deps()) => (await processBatch([{ messageId: id, body: b }], d)).batchItemFailures.map((f) => f.itemIdentifier);

test("sequential: put rev1, put rev2, erase -> tombstoned", async () => {
  const key = await writeManifest(1);
  expect((await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps())).batchItemFailures).toEqual([]);
  expect((await row())!.deleted_at).toBeNull();
  await store.delete(objectKey(key));
  await processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  expect((await row())!.deleted_at).not.toBeNull();
}, 60_000);

test("a put in flight when the manifest is erased cannot bring the pointer back (review H1, real SQL)", async () => {
  let key = await writeManifest(1);
  await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps());
  key = await writeManifest(2); // the live writer's revision 2
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let entered!: () => void;
  const atUpsert = new Promise<void>((r) => (entered = r));
  // Lambda A: the rev-2 put, paused just before its upsert (after its S3 read).
  const a = processBatch([{ messageId: "p2", body: body(key, "ObjectCreated:Put", "02") }], deps(async () => { entered(); await held; }));
  await atUpsert;
  // The erasure job deletes the manifest; Lambda B handles the delete. It waits for A's session lock.
  await store.delete(objectKey(key));
  const b = processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  // Let B run as far as it can while A is paused: to its tombstone without the
  // session lock (the bug), or until it blocks on A's lock (the fix).
  await Promise.race([b, new Promise((r) => setTimeout(r, 2_000))]);
  release();
  expect((await a).batchItemFailures).toEqual([]);
  expect((await b).batchItemFailures).toEqual([]);
  const final = await row();
  expect(final!.deleted_at).not.toBeNull();
  const directory = (await pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows;
  expect(directory.every((d) => d.deleted_at !== null)).toBe(true);
}, 60_000);

test("the S3 read itself is under the lock: a delete cannot slip between a put's read and its upsert (review #2 L-b)", async () => {
  let key = await writeManifest(1);
  await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps());
  key = await writeManifest(2);
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let entered!: () => void;
  const afterRead = new Promise<void>((r) => (entered = r));
  // Lambda A pauses right after reading the rev-2 manifest, before anything else.
  const a = processBatch([{ messageId: "p2", body: body(key, "ObjectCreated:Put", "02") }], deps(undefined, async () => { entered(); await held; }));
  await afterRead;
  await store.delete(objectKey(key));
  const b = processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  await Promise.race([b, new Promise((r) => setTimeout(r, 2_000))]);
  release();
  await a;
  await b;
  expect((await row())!.deleted_at).not.toBeNull();
}, 60_000);

describe("an exporter date move (review #2 N1, real SQL)", () => {
  async function atRevision3() {
    const a = await writeManifest(1);
    await processBatch([{ messageId: "1", body: body(a, "ObjectCreated:Put", "01") }], deps());
    await writeManifest(2);
    await processBatch([{ messageId: "2", body: body(a, "ObjectCreated:Put", "02") }], deps());
    await writeManifest(3);
    await processBatch([{ messageId: "3", body: body(a, "ObjectCreated:Put", "03") }], deps());
    return a;
  }

  const directoryLive = async () =>
    (await pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows.some((d) => d.deleted_at === null);

  test("new key at rev 1 while the old one is live, then the old prefix erased: the session ends live at the new key", async () => {
    const a = await atRevision3();
    const b = await writeManifest(1, locB); // exporter: (prior at the new key ?? 0) + 1
    // Both keys live: the move is in progress, so the put retries (and never lands on the old row).
    expect(await failures("4", body(b, "ObjectCreated:Put", "04"))).toEqual(["4"]);
    expect((await row())!.manifest_key).toBe(a);
    await store.delete(objectKey(a));
    // The old key's delete finds the session under its new date and follows it.
    expect(await failures("5", body(a, "ObjectRemoved:DeleteMarkerCreated", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b, revision: "1" });
    expect(await directoryLive()).toBe(true);
    // The retried put, and any reconcile re-enqueue, change nothing.
    expect(await failures("6", body(b, "ObjectCreated:Put", "06"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await directoryLive()).toBe(true);
  }, 60_000);

  test("the old key's delete handled before the new key's put: the session ends live at the new key", async () => {
    const a = await atRevision3();
    const b = await writeManifest(1, locB);
    await store.delete(objectKey(a));
    expect(await failures("4", body(a, "ObjectRemoved:DeleteMarkerCreated", "04"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await failures("5", body(b, "ObjectCreated:Put", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await directoryLive()).toBe(true);
  }, 60_000);

  // First-seen across a move (the analysis cutover proof, 20261105062120). The new key restarts at
  // revision 1, as the exporter writes it, so the revision fence keeps it out and the move branch runs.
  const created = async () => (await pg.query<{ c: string }>("SELECT created_at::text c FROM sessions.session_index WHERE session_id = 'sess_1'")).rows[0]!.c;
  const landedPut = (key: string) => ({ provider: "s3" as const, bucket: "bucket", key, etag: "e" });

  test("a move to a LATER start date is 'moved' and keeps the earlier created_at", async () => {
    const a = await atRevision3();
    const before = await created();
    const later = await writeManifest(1, locLater);
    await store.delete(objectKey(a));
    const out = await indexPointer(landedPut(later), deps().pointer);
    expect(out).toMatchObject({ status: "moved", manifestKey: later });
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: later, revision: "1" });
    expect(await created()).toBe(before);
  }, 60_000);

  test("a move to an EARLIER start date lowers created_at to the new start (least never raises)", async () => {
    const a = await atRevision3();
    const before = await created();
    const earlier = await writeManifest(1, locB);
    await store.delete(objectKey(a));
    expect(await indexPointer(landedPut(earlier), deps().pointer)).toMatchObject({ status: "moved", manifestKey: earlier });
    expect(Date.parse(await created())).toBeLessThan(Date.parse(before));
    expect(await created()).toBe((await pg.query<{ c: string }>(`SELECT '${locB.startDate}T10:00:00Z'::timestamptz::text c`)).rows[0]!.c);
  }, 60_000);

  test("a move through the PUBLICATION transaction (the production path) is 'moved', revision 1, created_at unchanged", async () => {
    const a = await atRevision3();
    const before = await created();
    const later = await writeManifest(1, locLater);
    await store.delete(objectKey(a));
    let transactions = 0;
    const statements: string[][] = []; // per publication transaction, in order
    // The publication authority's schema-generation witness (the role-validation pass the migrator records).
    await pg.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + "a".repeat(64) + "','pass')");
    const base = deps().pointer;
    const published = {
      ...base,
      publication: {
        schemaGeneration: "a".repeat(64),
        transact: <T,>(work: (tx: { query: (t: string, p?: unknown[]) => Promise<{ rows: any[] }> }) => Promise<T>) =>
          pg.transaction(async (tx) => {
            transactions += 1;
            const mine: string[] = [];
            statements.push(mine);
            return work({ query: async (t: string, p?: unknown[]) => (mine.push(t), { rows: (await tx.query(t, p as unknown[])).rows }) });
          }),
      },
    } as typeof base;
    expect(await indexPointer(landedPut(later), published)).toMatchObject({ status: "moved", manifestKey: later });
    expect(transactions).toBeGreaterThan(0);
    // The old-key DELETE runs in exactly one publication transaction, once, after that transaction's
    // authority lock and before at least one later statement of the same transaction (the re-key write;
    // its function name depends on the fence flag, so it is not matched by name). The row checks below
    // show the write landed.
    const isDelete = (t: string) => /DELETE FROM sessions\.session_index\b[\s\S]*RETURNING created_at/.test(t);
    const moveTx = statements.filter((st) => st.some(isDelete));
    expect(moveTx).toHaveLength(1);
    const tx = moveTx[0]!;
    const lock = tx.findIndex((t) => t.includes("session_publication_lock"));
    const del = tx.findIndex(isDelete);
    expect(lock).toBeGreaterThanOrEqual(0);
    expect(lock).toBeLessThan(del);
    expect(del).toBeLessThan(tx.length - 1);
    expect(tx.filter(isDelete)).toHaveLength(1);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: later, revision: "1" });
    expect(await created()).toBe(before);
    expect((await pg.query("SELECT 1 FROM sessions.session_index WHERE session_id = 'sess_1'")).rows).toHaveLength(1);
  }, 60_000);

  test("a skipped move (the new manifest vanished before the re-key) throws retryably and leaves the old row", async () => {
    const a = await atRevision3();
    const before = await created();
    const later = await writeManifest(1, locLater);
    await store.delete(objectKey(a));
    // The new key's manifest reads as gone once the move is detected (the re-key's own read).
    let moving = false;
    const base = deps().pointer;
    const db = { ...base.db, async query(text: string, params?: unknown[]) {
      if (text.includes("deleted_at IS NOT NULL AS deleted FROM sessions.session_index")) moving = true;
      return base.db.query(text, params);
    } } as HoopDb;
    const vanishing: ManifestReader = { ...reader, read: async (k) => (moving && k === later ? null : reader.read(k)) };
    const err = await indexPointer(landedPut(later), { ...base, db, reader: vanishing }).catch((e: unknown) => e as { code?: string; retryable?: boolean });
    expect(err).toMatchObject({ code: "pointer_move_skipped", retryable: true });
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: a, revision: "3" });
    expect(await created()).toBe(before);
  }, 60_000);

  // A pooled one-connection HoopDb over this PGlite (the move runs in a real BEGIN/COMMIT), and a
  // publication-transaction HoopDb (the production path). `hook` may answer a statement instead of PGlite.
  type Hook = (text: string, params: unknown[] | undefined, run: (t: string, p?: unknown[]) => Promise<{ rows: any[] }>) => Promise<{ rows: any[] } | undefined>;
  const viaPool = (hook?: Hook) => {
    const run = async (t: string, p?: unknown[]) => ({ rows: (await pg.query(t, p as unknown[])).rows });
    const client = { query: async (t: string, p?: unknown[]) => (await hook?.(t, p, run)) ?? run(t, p), release() {} };
    const base = deps().pointer;
    return { ...base, db: { ...base.db, query: (t: string, p?: unknown[]) => client.query(t, p), pool: { connect: async () => client } } as unknown as HoopDb };
  };
  const viaPublication = (hook?: Hook) => {
    const base = deps().pointer;
    return {
      ...base,
      publication: {
        schemaGeneration: "a".repeat(64),
        transact: <T,>(work: (tx: { query: (t: string, p?: unknown[]) => Promise<{ rows: any[] }> }) => Promise<T>) =>
          pg.transaction(async (tx) => {
            const run = async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows });
            return work({ query: async (t: string, p?: unknown[]) => (await hook?.(t, p, run)) ?? run(t, p) });
          }),
      },
    } as typeof base;
  };
  const witness = () => pg.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + "a".repeat(64) + "','pass') ON CONFLICT DO NOTHING");
  // The move's write (the first write after its old-key DELETE) runs for real, then reports applied=false:
  // the row is present, so this is not a guard.
  const notApplied = (): Hook => {
    let armed = false, once = true;
    return async (t, p, run) => {
      if (/DELETE FROM sessions\.session_index\b[\s\S]*RETURNING created_at/.test(t)) armed = true;
      else if (armed && once && /upsert_session_index|ingest_operator_runtime_upsert/.test(t)) { once = false; await run(t, p); return { rows: [{ applied: false, revision: "3" }] }; }
      return undefined;
    };
  };

  for (const [path, make] of [["pooled", viaPool], ["publication", viaPublication]] as const)
    test(`${path}: a move whose write reports applied=false throws retryably and rolls the delete back`, async () => {
      await witness();
      const a = await atRevision3();
      const before = await created();
      const later = await writeManifest(1, locLater);
      await store.delete(objectKey(a));
      const err = await indexPointer(landedPut(later), make(notApplied())).catch((e: unknown) => e as { code?: string; retryable?: boolean });
      expect(err).toMatchObject({ code: "pointer_move_not_applied", retryable: true });
      expect(await row()).toMatchObject({ deleted_at: null, manifest_key: a, revision: "3" });
      expect(await created()).toBe(before);
      expect(await indexPointer(landedPut(later), make())).toMatchObject({ status: "moved", manifestKey: later });
    }, 60_000);

  // An erased session: an erasure fence, and its row a tombstone at the old key K1. A put for K2 must
  // end without error (no poison message) and leave the tombstone, with the fence flag off and on.
  async function fencedTombstone() {
    await witness();
    const a = await atRevision3();
    const r = (await pg.query<{ org_id: string; workspace_id: string; harness: string; session_id: string }>(
      "SELECT org_id, workspace_id, harness, session_id FROM sessions.session_index WHERE session_id = 'sess_1'")).rows[0]!;
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 'sess_1'");
    await pg.query("INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id) VALUES ($1, $2, $3, $4)",
      [r.org_id, r.workspace_id, r.harness, r.session_id]);
    const later = await writeManifest(1, locLater);
    await store.delete(objectKey(a));
    return { a, later };
  }
  // The fence-flag path calls the runtime upsert, which only its role may call.
  const asRuntime: Hook = async (t, p, run) => {
    if (!t.includes("ingest_operator_runtime_upsert")) return undefined;
    await run("SET ROLE search_consumer_runtime");
    try { return await run(t, p); } finally { await run("RESET ROLE"); }
  };

  for (const flag of ["off", "on"] as const)
    for (const [path, make] of [["pooled", viaPool], ["publication", viaPublication]] as const)
      test(`fence flag ${flag}, ${path}: a fenced move is a terminal ignored outcome and the tombstone stays`, async () => {
        const previous = process.env.SESSION_ERASE_FENCE_ENFORCED;
        delete process.env.SESSION_ERASE_FENCE_ENFORCED;
        try {
          const { a, later } = await fencedTombstone(); // set up with the flag off (plain writes)
          if (flag === "on") {
            process.env.SESSION_ERASE_FENCE_ENFORCED = "1";
            // The runtime role's operator step (schema usage; the function's EXECUTE).
            await pg.exec("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime; GRANT EXECUTE ON FUNCTION sessions.ingest_operator_runtime_upsert(jsonb,text,text) TO search_consumer_runtime");
          }
          const tombstone = async () => (await pg.query<{ manifest_key: string; dead: boolean }>(
            "SELECT manifest_key, deleted_at IS NOT NULL AS dead FROM sessions.session_index WHERE session_id = 'sess_1'")).rows;
          const hook = flag === "on" ? asRuntime : undefined;
          expect(await indexPointer(landedPut(later), make(hook))).toEqual({
            status: "ignored", reason: flag === "on" ? "operator_erased" : "move_guarded",
          });
          expect(await tombstone()).toEqual([{ manifest_key: a, dead: true }]);
          // Redelivery: the same terminal outcome, still no error, still the tombstone.
          expect(await indexPointer(landedPut(later), make(hook))).toMatchObject({ status: "ignored" });
          expect(await tombstone()).toEqual([{ manifest_key: a, dead: true }]);
        } finally {
          if (previous === undefined) delete process.env.SESSION_ERASE_FENCE_ENFORCED; else process.env.SESSION_ERASE_FENCE_ENFORCED = previous;
        }
      }, 60_000);

  test("fence flag off: through the consumer, the fenced move's message completes (no batch item failure)", async () => {
    const { a, later } = await fencedTombstone();
    const d = deps();
    const pooled = viaPool();
    expect((await processBatch([{ messageId: "f", body: body(later, "ObjectCreated:Put", "09") }], { ...d, pointer: pooled })).batchItemFailures).toEqual([]);
    expect(await row()).toMatchObject({ manifest_key: a });
    expect((await row())!.deleted_at).not.toBeNull();
  }, 60_000);

  test("a move is atomic: a failure after the delete leaves the old row and its created_at; the retry moves it", async () => {
    const a = await atRevision3();
    const before = await created();
    const later = await writeManifest(1, locLater);
    await store.delete(objectKey(a));
    // A pooled HoopDb whose one connection is this PGlite: the move runs in a real transaction.
    let failNext = true;
    const client = {
      async query(text: string, params?: unknown[]) {
        if (failNext && text.includes("upsert_session_index")) { failNext = false; throw new Error("injected failure after the delete"); }
        return pg.query(text, params as unknown[]);
      },
      release() {},
    };
    const base = deps().pointer;
    const pooled = { ...base, db: { ...base.db, pool: { connect: async () => client } } as unknown as HoopDb };
    await expect(indexPointer(landedPut(later), pooled)).rejects.toThrow("injected failure after the delete");
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: a, revision: "3" });
    expect(await created()).toBe(before);
    expect(await indexPointer(landedPut(later), pooled)).toMatchObject({ status: "moved", manifestKey: later });
    expect(await row()).toMatchObject({ manifest_key: later, revision: "1" });
    expect(await created()).toBe(before);
  }, 60_000);

  test("deleting the old key never tombstones a row that already names the new key", async () => {
    const a = await atRevision3();
    const far = await writeManifest(4, locFar); // carried revision, beyond the probe's reach
    expect(await failures("4", body(far, "ObjectCreated:Put", "04"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: far, revision: "4" });
    await store.delete(objectKey(a));
    // The old key's delete arrives late: the tombstone is fenced on the key the row names.
    expect(await failures("5", body(a, "ObjectRemoved:DeleteMarkerCreated", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: far });
    expect(await directoryLive()).toBe(true);
  }, 60_000);
});
