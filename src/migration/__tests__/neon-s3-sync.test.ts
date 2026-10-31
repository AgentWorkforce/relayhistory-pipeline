/**
 * The continuous Neon to S3 export, end to end against the pipeline's own
 * code: objects it writes are read back through `parseManifest`, AlleyOop's
 * `chunksAreServerSealed`, the RelayHistory transform's `readRelayhistoryExport`
 * and the projection, and the reader's SQL runs against a Neon-shaped PGlite.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import {
  deleteParkedSession,
  objectKey,
  parkedManifestKey,
  parseManifest,
  parseSessionKey,
  readAll,
  readRelayhistoryExport,
  relayhistoryExportProvenance,
  type ObjectStore,
} from "../../shared/vendor.ts";
import { chunksAreServerSealed } from "../../alleyoop/manifests.ts";
import { buildSessionStream, type DeliveryRecord, type Queryable } from "../relayhistory-export.ts";
import { deleteExported, exportUnit, orgPrefix, type ExportTarget } from "../neon-s3-sync/export-session.ts";
import { boundStrings, MAX_STRING_TOKEN, neonUnitSource, readOnly, sessionUnits, type SessionUnit, type StreamLine, type UnitSource } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { Syncer, DEFAULT_SYNC_CONFIG } from "../neon-s3-sync/sync.ts";
import { Throttle, parseWindows } from "../neon-s3-sync/throttle.ts";

const dirs: string[] = [];
afterAll(async () => {
  await Promise.all(dirs.map((d) => rm(d, { recursive: true, force: true })));
});
async function tempDir() {
  const d = await mkdtemp(join(tmpdir(), "neon-s3-sync-"));
  dirs.push(d);
  return d;
}

/** An fs store that counts writes and deletes. */
async function countingStore() {
  const inner = createFsStore({ root: await tempDir() });
  const counts = { puts: 0, deletes: 0 };
  const store: ObjectStore = {
    backend: inner.backend,
    head: (k) => inner.head(k),
    get: (k, r) => inner.get(k, r),
    getObject: (k, r) => inner.getObject(k, r),
    list: (p, c) => inner.list(p, c),
    presignGet: (k, t) => inner.presignGet(k, t),
    put: (k, b, o) => {
      counts.puts += 1;
      return inner.put(k, b, o);
    },
    delete: (k) => {
      counts.deletes += 1;
      return inner.delete(k);
    },
  };
  return { store, counts };
}

async function allKeys(store: ObjectStore, prefix = "96bulls"): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.replace(/\/+$/, "")), cursor);
    keys.push(...page.items.map((i) => i.key));
    cursor = page.nextCursor;
  } while (cursor);
  return keys.sort();
}

async function readText(store: ObjectStore, key: string) {
  return new TextDecoder().decode(await readAll(await store.get(objectKey(key))));
}

const T0 = Date.parse("2026-10-20T10:00:00Z");
const unit = (orgId: string, over: Partial<SessionUnit> = {}): SessionUnit => ({
  orgId,
  workspaceId: "11111111-1111-4111-8111-111111111111",
  source: "claude",
  sessionId: "sess:with:colons-1",
  firstMs: T0,
  lastMs: T0 + 3_600_000,
  machineIds: [],
  totals: { event_count: "3", models: ["m1"] },
  ...over,
});

function rec(kind: string, origin: string, id: string, payload: Record<string, unknown>, revision = "1", seq?: string): DeliveryRecord {
  return {
    kind,
    origin_id: origin,
    record_id: id,
    revision,
    operation: "upsert",
    received_at: "2026-10-20 10:00:00+00",
    payload,
    ...(seq ? { accepted_seq: seq } : {}),
  };
}

/** A UnitSource over fixed records, one machine per origin, as the Neon reader would emit. */
function fixedSource(records: DeliveryRecord[], fenced = false): UnitSource {
  return {
    fenced: async () => fenced,
    async *lines() {
      for (const r of records) {
        const line: StreamLine = {
          machine: `delivery:${r.origin_id.replace(/[^a-z0-9]/g, "")}`,
          bytes: buildSessionStream([r]).bytes,
          feed: { origin: r.origin_id, revision: Number(r.revision) },
          userId: "user-1",
        };
        yield line;
      }
    },
  };
}

const baseRecords = () => [
  rec("session", "origin-a", "s-1", { session_id: "sess:with:colons-1", title: "t", first_activity_ms: T0 }, "1", "10"),
  rec("session_event", "origin-a", "e-1", { ts_ms: T0 + 1000, role: "user", text: "hello SECRET_MARKER_ALPHA" }, "2", "11"),
  rec("tool_call", "origin-a", "t-1", { ts_ms: T0 + 2000, name: "Bash", tool_use_id: "tu1" }, "3", "12"),
  rec("session_event", "origin-b", "e-2", { ts_ms: T0 + 3000, role: "assistant", text: "from another machine" }, "1", "13"),
];

const target = (store: ObjectStore, over: Partial<ExportTarget> = {}): ExportTarget => ({
  store,
  keyRoot: "96bulls",
  grantId: "neon-s3-sync",
  now: () => new Date("2026-10-31T16:00:00Z"),
  ...over,
});

describe("format: the pipeline's own validators accept what the exporter writes", () => {
  test("manifest validates against its key, chunks are server-sealed, the transform reads and projects them", async () => {
    const { store } = await countingStore();
    const records = baseRecords();
    const r = await exportUnit(target(store, { maxChunkBytes: 300 }), unit("org-a"), fixedSource(records));
    expect(r.manifests.length).toBe(2);
    let total = 0;
    for (const m of r.manifests) {
      expect(m.manifestKey.startsWith("96bulls/org-a/v1/ws=11111111-1111-4111-8111-111111111111/")).toBe(true);
      const text = await readText(store, m.manifestKey);
      const manifest = await parseManifest(text, m.manifestKey);
      const loc = parseSessionKey(m.manifestKey)!;
      expect(loc.prefix).toBe("96bulls/org-a");
      expect(loc.sessionId).toBe("sess_with_colons-1");
      expect(loc.startDate).toBe("2026-10-20");
      // AlleyOop's commit check, with the deployment prefix of the manifest's key.
      expect(await chunksAreServerSealed({ store } as never, manifest, loc.prefix)).toBe(true);
      // Without the prefix (the old bug) the chunks are not found.
      expect(await chunksAreServerSealed({ store } as never, manifest, undefined)).toBe(false);
      expect(relayhistoryExportProvenance(manifest)).toEqual({ grantId: "neon-s3-sync", sourceUserId: "user-1" });
      const read = await readRelayhistoryExport(manifest, store, (b) => new Uint8Array(zstdDecompressSync(b)), {
        prefix: loc.prefix,
      });
      total += read.length;
      const projection = projectRelayhistorySession(read, {
        workspaceId: loc.workspaceId,
        source: loc.harness,
        sessionId: loc.sessionId,
        machineId: loc.machineId,
        userId: "user-1",
      });
      expect(projection).toBeDefined();
    }
    expect(total).toBe(records.length);
  });

  test("multiple chunks: dense seq, contiguous offsets, hash chain, resume marks", async () => {
    const { store } = await countingStore();
    const r = await exportUnit(target(store, { maxChunkBytes: 200 }), unit("org-a"), fixedSource(baseRecords().slice(0, 3)));
    const manifest = await parseManifest(await readText(store, r.manifests[0]!.manifestKey), r.manifests[0]!.manifestKey);
    expect(manifest.raw.chunk_count).toBeGreaterThan(1);
    expect(manifest.raw.chunks.every((c) => (c as { source_origin?: string }).source_origin === "origin-a")).toBe(true);
    expect((manifest.extensions!["client.relayhistory_export"] as { origin_revisions: unknown }).origin_revisions).toEqual({
      "origin-a": 3,
    });
  });

  test("the stored chunks are scrubbed: a secret in the source never reaches storage", async () => {
    const { store } = await countingStore();
    const secret = "AKIAABCDEFGHIJKLMNOP";
    const records = [rec("session_event", "origin-a", "e-1", { ts_ms: T0, text: `key ${secret}` })];
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(records));
    for (const key of await allKeys(store)) {
      const bytes = await readAll(await store.get(objectKey(key)));
      const text = key.endsWith(".zst") ? new TextDecoder().decode(zstdDecompressSync(bytes)) : new TextDecoder().decode(bytes);
      expect(text.includes(secret)).toBe(false);
    }
    expect(r.manifests[0]!.written).toBe(true);
  });
});

describe("strings over the scrub bound", () => {
  test("only the oversize string is cut, with a note; every other byte is Neon's text; the sealer then accepts it", async () => {
    const huge = "x".repeat(300_000) + '"\\u00e9';
    const json = `{"big": 12345678901234567890, "out": ${JSON.stringify(huge)}, "k": "v"}`;
    const r = boundStrings(json);
    expect(r.truncated).toBe(1);
    expect(r.text.startsWith('{"big": 12345678901234567890, "out": "xxx')).toBe(true);
    expect(r.text.endsWith(', "k": "v"}')).toBe(true);
    const out = (JSON.parse(r.text) as { out: string }).out;
    expect(out).toContain("[neon-s3-sync: truncated");
    expect(JSON.stringify(out).length).toBeLessThanOrEqual(MAX_STRING_TOKEN);
    expect(boundStrings('{"a": "b"}')).toEqual({ text: '{"a": "b"}', truncated: 0 });
    // escapes: a token long only because of escapes is bounded by its literal length
    const escaped = `{"e": ${JSON.stringify("\u0001".repeat(60_000))}}`;
    expect(escaped.length).toBeGreaterThan(MAX_STRING_TOKEN);
    expect(boundStrings(escaped).truncated).toBe(1);
  });
});

describe("idempotency", () => {
  test("re-exporting an unchanged session writes and deletes nothing", async () => {
    const { store, counts } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const before = { ...counts };
    const keys = await allKeys(store);
    const again = await exportUnit({ ...t, now: () => new Date("2026-11-01T00:00:00Z") }, unit("org-a"), fixedSource(baseRecords()));
    expect(counts).toEqual(before);
    expect(await allKeys(store)).toEqual(keys);
    expect(again.manifests.every((m) => !m.written && m.chunksSealed === 0 && m.chunksReused > 0)).toBe(true);
  });

  test("a changed session is rewritten: new revision, created_at kept, unchanged chunks reused, orphans deleted", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    const first = await exportUnit(t, unit("org-a"), fixedSource(baseRecords().slice(0, 3)));
    const key = first.manifests[0]!.manifestKey;
    const v1 = await parseManifest(await readText(store, key), key);
    const grown = [...baseRecords().slice(0, 3), rec("file_edit", "origin-a", "f-1", { ts_ms: T0 + 5000, file_path: "x" }, "4")];
    const later = { ...t, now: () => new Date("2026-11-01T00:00:00Z") };
    const second = await exportUnit(later, unit("org-a"), fixedSource(grown));
    const v2 = await parseManifest(await readText(store, key), key);
    expect(v2.revision).toBe(v1.revision + 1);
    expect(v2.created_at).toBe(v1.created_at);
    expect(second.manifests[0]!.chunksReused).toBeGreaterThan(0);
    const raw = (await allKeys(store)).filter((k) => k.includes("/raw/"));
    expect(raw.length).toBe(v2.raw.chunk_count);
  });
});

describe("delete propagation", () => {
  test("a record deleted in Neon is gone from every stored object after the next export", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const without = baseRecords().filter((r) => r.record_id !== "e-1");
    await exportUnit(t, unit("org-a"), fixedSource(without));
    for (const key of await allKeys(store)) {
      const bytes = await readAll(await store.get(objectKey(key)));
      const text = key.endsWith(".zst") ? new TextDecoder().decode(zstdDecompressSync(bytes)) : new TextDecoder().decode(bytes);
      expect(text.includes("SECRET_MARKER_ALPHA")).toBe(false);
    }
  });

  test("a session hard-deleted from Neon (no units left) loses every object, including pipeline outputs", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const db = fakeDb([unit("org-a")]);
    const syncer = new Syncer(db, state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => fixedSource(baseRecords()));
    state.seeRevisions(1, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "5" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 5, kind: "recent", events: 3 })).toBe(true);
    const keys = state.exported("org-a", "sess:with:colons-1")!.keys;
    expect(keys.length).toBe(2);
    // a pipeline output next to the session
    await store.put(objectKey(keys[0]!.replace("manifest.json", "normalized/v2/relayhistory-old.ndjson")), new Uint8Array([1]));
    // Neon hard-deleted the session: its rollups are gone and the revision bumped.
    db.units = [];
    state.seeRevisions(2, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "6" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 6, kind: "changed", events: 0 })).toBe(true);
    expect(await allKeys(store)).toEqual([]);
    expect(state.exported("org-a", "sess:with:colons-1")!.keys).toEqual([]);
    state.close();
  });

  test("a group gone from both sweeps is queued for deletion first", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    state.seeRevisions(1, [{ org_id: "o", session_id: "gone", revision: "1" }, { org_id: "o", session_id: "kept", revision: "1" }]);
    state.seeActivity(1, [{ org_id: "o", session_id: "kept", last_ms: String(Date.now()), events: "1" }]);
    state.markExported("o", "gone", 1, ["96bulls/o/v1/x/manifest.json"]);
    state.seeRevisions(2, [{ org_id: "o", session_id: "kept", revision: "1" }]);
    state.seeActivity(2, [{ org_id: "o", session_id: "kept", last_ms: String(Date.now()), events: "1" }]);
    expect(state.dropUnseen(2, 2)).toBe(1);
    const work = state.nextWork(Date.now(), 14 * 86_400_000, 10);
    expect(work[0]).toMatchObject({ session_id: "gone", kind: "delete" });
    state.close();
  });

  test("a fenced session (user deletion tombstone) exports nothing", async () => {
    const { store, counts } = await countingStore();
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(baseRecords(), true));
    expect(r).toMatchObject({ fenced: true, manifests: [] });
    expect(counts.puts).toBe(0);
  });
});

describe("prefixes and tenant isolation", () => {
  test("the org prefix is one safe path segment under the key root", () => {
    expect(orgPrefix("96bulls", "4cb2aaaa-0000-4000-8000-000000000000")).toBe("96bulls/4cb2aaaa-0000-4000-8000-000000000000");
    for (const bad of ["", "a/b", "..", ".x", "a b"]) expect(() => orgPrefix("96bulls", bad)).toThrow();
    expect(() => orgPrefix("96Bulls/x", "o")).toThrow();
  });

  test("two orgs with identical workspace and session ids never share or delete each other's objects", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const a = await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const b = await exportUnit(t, unit("org-b"), fixedSource(baseRecords()));
    const keysA = await allKeys(store, "96bulls/org-a/");
    const keysB = await allKeys(store, "96bulls/org-b/");
    expect(keysA.length).toBeGreaterThan(0);
    expect(keysB.length).toBe(keysA.length);
    expect((await allKeys(store)).length).toBe(keysA.length + keysB.length);
    // An org-b key is refused when deleting for org-a.
    await expect(deleteExported(t, "org-a", b.manifests[0]!.manifestKey)).rejects.toThrow();
    for (const m of a.manifests) await deleteExported(t, "org-a", m.manifestKey);
    expect(await allKeys(store, "96bulls/org-a/")).toEqual([]);
    expect(await allKeys(store, "96bulls/org-b/")).toEqual(keysB);
  });
});

describe("parked-session erasure under a deployment prefix (prefix bug fix)", () => {
  test("deleteParkedSession removes parked chunks under 96bulls/<org>/", async () => {
    const { store } = await countingStore();
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(baseRecords().slice(0, 1)));
    const key = r.manifests[0]!.manifestKey;
    const loc = parseSessionKey(key)!;
    const text = await readText(store, key);
    await store.put(objectKey(parkedManifestKey(key, loc.machineId)), new TextEncoder().encode(text));
    await store.delete(objectKey(key));
    const raw = (await allKeys(store)).filter((k) => k.includes("/raw/"));
    expect(raw.length).toBe(1);
    expect(await deleteParkedSession(store, key, loc.machineId)).toEqual({ manifest: true, chunks: 1 });
    expect(await allKeys(store)).toEqual([]);
  });
});

describe("throttle", () => {
  test("inside a slow window the rate is halved", async () => {
    let now = Date.parse("2026-10-31T14:00:00Z");
    const slept: number[] = [];
    const t = new Throttle({
      rowsPerSec: 100,
      bytesPerSec: 1e9,
      slowWindows: parseWindows("2026-10-31T15:00Z/2026-11-01T00:00Z"),
      slowFactor: 0.5,
      now: () => now,
      sleep: async (ms) => {
        slept.push(ms);
        now += ms;
      },
    });
    expect(t.rates().rowsPerSec).toBe(100);
    await t.take(1100, 0); // 11 s of rows at 100/s, 1 s burst allowed
    expect(slept.at(-1)).toBe(10_000);
    now = Date.parse("2026-10-31T16:00:00Z");
    await t.take(0, 0);
    expect(t.rates()).toMatchObject({ rowsPerSec: 50, slow: true });
    await t.take(550, 0);
    expect(slept.at(-1)).toBe(10_000);
    expect(() => parseWindows("nope")).toThrow();
  });
});

describe("work order", () => {
  test("changed (past debounce) before recent before history, newest first", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const now = Date.parse("2026-10-31T16:00:00Z");
    const day = 86_400_000;
    state.seeRevisions(1, ["old", "new", "mid", "chg"].map((s) => ({ org_id: "o", session_id: s, revision: "2" })));
    state.seeActivity(1, [
      { org_id: "o", session_id: "old", last_ms: String(now - 100 * day), events: "1" },
      { org_id: "o", session_id: "new", last_ms: String(now - day), events: "1" },
      { org_id: "o", session_id: "mid", last_ms: String(now - 5 * day), events: "1" },
      { org_id: "o", session_id: "chg", last_ms: String(now - 50 * day), events: "1" },
    ]);
    state.markExported("o", "chg", 1, ["k"], now - 2 * 3_600_000);
    expect(state.nextWork(now, 14 * day, 10).map((w) => `${w.kind}:${w.session_id}`)).toEqual([
      "changed:chg",
      "recent:new",
      "recent:mid",
      "history:old",
    ]);
    // Just exported: the change waits for its debounce.
    state.markExported("o", "chg", 1, ["k"], now);
    expect(state.nextWork(now, 14 * day, 10).map((w) => w.session_id)).not.toContain("chg");
    state.close();
  });
});

// ---------------------------------------------------------------- Neon-shaped SQL

const NEON_SCHEMA = `
CREATE SCHEMA sessions;
CREATE TABLE sessions.delivery_records (org_id text, workspace_id text, origin_id text, record_id text, revision_id text,
  revision bigint, digest text, kind text, source text, session_id text, operation text, payload jsonb, user_id text,
  received_at timestamptz, PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE INDEX delivery_records_lookup ON sessions.delivery_records (org_id, workspace_id, kind, source, session_id, origin_id, record_id);
CREATE TABLE sessions.delivery_projection_acceptance (org_id text, workspace_id text, origin_id text, record_id text, accepted_seq bigint, historical boolean,
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE TABLE sessions.delivery_catalog_acceptance (org_id text, workspace_id text, origin_id text, record_id text, accepted_seq bigint,
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, machine_id text, user_id text, source text, session_id text,
  event_id text, kind text, ts timestamptz, content text, ingested_at timestamptz, delivery_record_id text,
  PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id));
CREATE TABLE sessions.session_rollups (org_id text, session_id text, workspace_id text, source text, project_id text,
  first_ts timestamptz, last_ts timestamptz, event_count bigint, machine_ids text[], models text[],
  cost_usd_micros bigint, input_tokens bigint, output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint);
CREATE TABLE sessions.session_revisions (org_id text, session_id text, revision bigint, updated_at timestamptz, PRIMARY KEY (org_id, session_id));
CREATE TABLE sessions.deleted_upload_sessions (org_id text, workspace_id text, user_id text, origin_id text, source text, session_id text);
CREATE TABLE sessions.session_resets (org_id text, workspace_id text, user_id text, request_id text, deleted bigint);
`;

describe("Neon reader SQL (Neon-shaped PGlite)", () => {
  test("pages records, settles a contended record by the ledger, skips deletes, adds undelivered events, honours the fence", async () => {
    const pg = new PGlite();
    await pg.exec(NEON_SCHEMA);
    const ws = "w1";
    const ins = (origin: string, id: string, kind: string, op: string, payload: unknown, rev = 1) =>
      pg.query(
        `INSERT INTO sessions.delivery_records VALUES ('o1', $1, $2, $3, 'r', $4, 'd', $5, 'claude', 's1', $6, $7, 'u1', '2026-10-20T10:00:00Z')`,
        [ws, origin, id, rev, kind, op, payload === null ? null : typeof payload === "string" ? payload : JSON.stringify(payload)],
      );
    await ins("oa", "s-1", "session", "upsert", { title: "t" });
    for (let i = 0; i < 7; i++) await ins("oa", `e-${i}`, "session_event", "upsert", `{"ts_ms": ${1000 + i}, "big": 12345678901234567890}`);
    await ins("oa", "gone", "session_event", "delete", null, 2);
    // contended: both origins hold c-1; ledger says ob accepted last
    await ins("oa", "c-1", "session_event", "upsert", { v: "oa" });
    await ins("ob", "c-1", "session_event", "upsert", { v: "ob" });
    await pg.query(`INSERT INTO sessions.delivery_projection_acceptance VALUES ('o1', $1, 'oa', 'c-1', 5, false), ('o1', $1, 'ob', 'c-1', 9, false)`, [ws]);
    // contended where the last accepted write is a tombstone: nobody exports it
    await ins("oa", "c-2", "session_event", "upsert", { v: "x" });
    await ins("ob", "c-2", "session_event", "delete", null);
    await pg.query(`INSERT INTO sessions.delivery_projection_acceptance VALUES ('o1', $1, 'oa', 'c-2', 5, false), ('o1', $1, 'ob', 'c-2', 9, false)`, [ws]);
    // an undelivered server-side event
    await pg.query(
      `INSERT INTO sessions.convergence_events VALUES ('o1', $1, 'relay-events:chatgpt', 'u1', 'claude', 's1', 'ev-1', 'message', '2026-10-20T10:00:00Z', 'hi', '2026-10-20T10:00:01Z', NULL)`,
      [ws],
    );
    await pg.query(
      `INSERT INTO sessions.session_rollups VALUES ('o1', 's1', $1, 'claude', NULL, '2026-10-20T10:00:00Z', '2026-10-20T11:00:00Z', 10,
        ARRAY['delivery:x', 'relay-events:chatgpt'], ARRAY['m'], 1, 2, 3, 4, 5, 6)`,
      [ws],
    );
    // a second rollup row (another project) with a different machine list
    await pg.query(
      `INSERT INTO sessions.session_rollups VALUES ('o1', 's1', $1, 'claude', 'p2', '2026-10-19T09:00:00Z', '2026-10-20T12:00:00Z', 5,
        ARRAY['delivery:x'], ARRAY['m', 'n'], 1, 2, 3, 4, 5, 6)`,
      [ws],
    );
    const db = readOnly(pg as unknown as Queryable);
    const units = await sessionUnits(db, "o1", "s1");
    expect(units).toHaveLength(1);
    expect(units[0]).toMatchObject({
      workspaceId: ws,
      source: "claude",
      machineIds: ["delivery:x", "relay-events:chatgpt"],
      firstMs: Date.parse("2026-10-19T09:00:00Z"),
      lastMs: Date.parse("2026-10-20T12:00:00Z"),
    });
    expect(units[0]!.totals).toMatchObject({ event_count: "15", models: ["m", "n"] });
    let pages = 0;
    // The paged path (forced with a tiny fast-path limit) and the one-statement path give identical lines.
    const source = neonUnitSource(db, async () => void (pages += 1), 3, 2);
    const lines: StreamLine[] = [];
    for await (const l of source.lines(units[0]!)) lines.push(l);
    const fast: StreamLine[] = [];
    for await (const l of neonUnitSource(db, async () => {}).lines(units[0]!)) fast.push(l);
    expect(fast.map((l) => [l.machine, new TextDecoder().decode(l.bytes)])).toEqual(
      lines.map((l) => [l.machine, new TextDecoder().decode(l.bytes)]),
    );
    const parsed = lines.map((l) => JSON.parse(new TextDecoder().decode(l.bytes)) as { record_id: string; origin_id: string; kind: string });
    const ids = parsed.map((p) => `${p.kind}:${p.origin_id}:${p.record_id}`);
    expect(ids).toContain("session:oa:s-1");
    expect(ids.filter((i) => i.startsWith("session_event:oa:e-"))).toHaveLength(7);
    expect(ids).toContain("session_event:ob:c-1");
    expect(ids).not.toContain("session_event:oa:c-1");
    expect(ids.some((i) => i.endsWith(":c-2"))).toBe(false);
    expect(ids.some((i) => i.endsWith(":gone"))).toBe(false);
    expect(ids).toContain("convergence_event:machine:relay-events:chatgpt:ev-1");
    // the delivery machine is Postgres's md5 of [workspace, origin], as Neon's projection names it
    const md5 = (await pg.query<{ m: string }>(`SELECT 'delivery:' || md5(jsonb_build_array('w1', 'oa')::text) AS m`)).rows[0]!.m;
    expect(lines.find((l) => l.feed?.origin === "oa")!.machine).toBe(md5);
    // a bigint payload is written as Neon's own text, not rounded
    expect(new TextDecoder().decode(lines[1]!.bytes)).toContain("12345678901234567890");
    expect(pages).toBeGreaterThan(3);
    expect(await source.fenced(units[0]!)).toBe(false);
    // a tool output longer than the scrub bound is stored (cut, with its note), not refused
    await ins("oa", "huge", "tool_call", "upsert", { output: "y".repeat(400_000) });
    const { store } = await countingStore();
    const exported = await exportUnit(target(store), units[0]!, neonUnitSource(db, async () => {}));
    expect(exported.manifests.reduce((n, m) => n + m.truncatedStrings, 0)).toBe(1);
    for (const m of exported.manifests) await parseManifest(await readText(store, m.manifestKey), m.manifestKey);
    await pg.query(`INSERT INTO sessions.deleted_upload_sessions VALUES ('o1', $1, 'u1', 'oa', 'claude', 's1')`, [ws]);
    // the fence is cached for two minutes; a fresh source reads it again
    expect(await neonUnitSource(db, async () => {}).fenced(units[0]!)).toBe(true);
    // a reset in the workspace falls back to #40's per-session query (no session record of that user here)
    await pg.query(`DELETE FROM sessions.deleted_upload_sessions`);
    await pg.query(`INSERT INTO sessions.session_resets VALUES ('o1', $1, 'u1', 'r1', 1)`, [ws]);
    expect(await neonUnitSource(db, async () => {}).fenced(units[0]!)).toBe(true);
    await pg.close();
  });
});

// ---------------------------------------------------------------- helpers

function fakeDb(units: SessionUnit[]): Queryable & { units: SessionUnit[] } {
  const self = {
    units,
    async query<T>(sql: string) {
      if (sql.includes("FROM sessions.session_rollups") && sql.includes("LIMIT 10000")) {
        return {
          rows: self.units.map((u) => ({
            workspace_id: u.workspaceId,
            source: u.source,
            first_ms: String(u.firstMs),
            last_ms: String(u.lastMs),
            machine_ids: u.machineIds,
            models: [],
            event_count: "3",
            input_tokens: "0",
            output_tokens: "0",
            reasoning_tokens: "0",
            cache_read_tokens: "0",
            cache_create_tokens: "0",
            cost_usd_micros: "0",
          })) as T[],
        };
      }
      if (sql.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      throw new Error("unexpected query");
    },
  };
  return self;
}

function fastThrottle() {
  return new Throttle({ rowsPerSec: 1e9, bytesPerSec: 1e12, slowWindows: [], slowFactor: 1, sleep: async () => {} });
}

// ---------------------------------------------------------------- real S3 API (MinIO), when configured

const minio = process.env.S3X_TEST_MINIO_ENDPOINT;
describe.skipIf(!minio)("against a real S3 API (MinIO, S3X_TEST_MINIO_*)", () => {
  test("create-only chunks, CAS manifests, idempotent re-export, erasure through the eraser store", async () => {
    const { createS3Store } = await import("../../shared/vendor.ts");
    const { splitStore } = await import("../neon-s3-sync/export-session.ts");
    const s3 = () =>
      createS3Store({
        bucket: process.env.S3X_TEST_MINIO_BUCKET ?? "s3x-test",
        region: "us-east-1",
        endpoint: minio!,
        credentials: {
          accessKeyId: process.env.S3X_TEST_MINIO_ACCESS_KEY!,
          secretAccessKey: process.env.S3X_TEST_MINIO_SECRET_KEY!,
        },
      });
    let deletes = 0;
    const eraser = s3();
    const store = splitStore(s3(), { ...eraser, delete: (k) => ((deletes += 1), eraser.delete(k)) });
    const org = `org-${Date.now().toString(36)}`;
    const t = target(store, { maxChunkBytes: 200 });
    const first = await exportUnit(t, unit(org), fixedSource(baseRecords()));
    for (const m of first.manifests) {
      const manifest = await parseManifest(await readText(store, m.manifestKey), m.manifestKey);
      expect(await chunksAreServerSealed({ store } as never, manifest, `96bulls/${org}`)).toBe(true);
    }
    const again = await exportUnit(t, unit(org), fixedSource(baseRecords()));
    expect(again.manifests.every((m) => !m.written && m.chunksSealed === 0)).toBe(true);
    await exportUnit(t, unit(org), fixedSource(baseRecords().filter((r) => r.record_id !== "e-1")));
    expect(deletes).toBeGreaterThan(0);
    for (const m of first.manifests) await deleteExported(t, org, m.manifestKey);
    expect(await allKeys(store, `96bulls/${org}`)).toEqual([]);
  });
});
