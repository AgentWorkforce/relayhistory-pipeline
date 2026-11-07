import { afterAll, beforeAll, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  objectKey,
  parseManifest,
  parseSessionKey,
  readAll,
  type ObjectStore,
} from "../../../packages/object-store/src/index.ts";
import {
  RELAYHISTORY_LEGACY_MACHINE_ID,
  legacyDiscoveryPage,
  legacyGroupScopes,
  legacyUnit,
  legacyUnitSource,
  type LegacyScope,
} from "../neon-s3-sync/legacy-source.ts";
import { exportUnit } from "../neon-s3-sync/export-session.ts";
import { groupExistsInNeon } from "../neon-s3-sync/neon-reader.ts";
import { Checkpoint, newCounters, runOrg, verifyExported, NEON_SCOPE_SQL } from "../legacy-export/runner.ts";
import { assertSelectOnly, readSecrets } from "../legacy-export/main.ts";
import {
  canonicalSnapshot,
  CanonicalReadError,
  type CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import type { Queryable } from "../relayhistory-export.ts";

const pg = new PGlite(),
  db = pg as unknown as Queryable;
const turnId = "11111111-1111-4111-8111-111111111111";
let root: string;
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const scope: LegacyScope = { orgId: "org_1", workspaceId: "ws_1", source: "claude", sessionId: "sess:1" };

beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), "canonical-legacy-"));
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_rollups(org_id text,session_id text,workspace_id text,source text,first_ts timestamptz,last_ts timestamptz,machine_ids text[],models text[],event_count bigint,input_tokens bigint,output_tokens bigint,reasoning_tokens bigint,cache_read_tokens bigint,cache_create_tokens bigint,cost_usd_micros bigint);
 CREATE TABLE sessions.session_catalog(org_id text,session_id text,workspace_id text,source text,first_activity_at timestamptz,last_activity_at timestamptz,received_at timestamptz);
 CREATE TABLE sessions.convergence_events(org_id text,workspace_id text,source text,session_id text,user_id text NOT NULL DEFAULT 'u_event');
 CREATE TABLE sessions.deleted_upload_sessions(org_id text,workspace_id text,source text,session_id text);
 CREATE TABLE sessions.conversation_turns(id uuid PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,session_owner text,turn_index int,role text,content text,actor_name text,actor_role text,metadata jsonb,ts timestamptz,UNIQUE(org_id,workspace_id,source,session_id,turn_index));
 CREATE TABLE sessions.session_links(id bigint PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,link_kind text,link_ref text,link_url text,metadata jsonb,confidence_basis_points int,link_ts timestamptz,updated_at timestamptz,created_at timestamptz);
 CREATE TABLE sessions.session_outcomes(org_id text,workspace_id text,source text,session_id text,commit_sha text,user_id text,reverted boolean,shipped_at timestamptz,ingested_at timestamptz,created_at timestamptz);`);
  await pg.query(
    `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','owner','sess:1','owner',3,'assistant','preserved legacy transcript','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00.123456Z')`,
    [turnId],
  );
});
afterAll(async () => {
  await pg.close();
  await rm(root, { recursive: true, force: true });
});

async function exportScope(s: LegacyScope, store: ObjectStore, grantId = "grant_1") {
  const unit = (await legacyUnit(db, s))!;
  return { unit, result: await exportUnit({ store, keyRoot: "96bulls", grantId }, unit, legacyUnitSource(db, async () => {}, 1)) };
}
async function pointerFor(store: ObjectStore, key: string): Promise<CanonicalPointer> {
  const manifest = await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(key)))), key);
  const ext = manifest.extensions?.["96bulls"] as { user_id: string; canonical_machine_id: string };
  return {
    org_id: "org_1",
    workspace_id: manifest.session.workspace_id,
    harness: manifest.session.harness,
    session_id: "sess:1",
    machine_id: ext.canonical_machine_id,
    user_id: ext.user_id,
    storage_provider: "s3",
    bucket: "bucket",
    manifest_key: key,
    content_sha256: manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: manifest.revision,
    deleted_at: null,
  };
}
const snapshot = (store: ObjectStore, pointers: CanonicalPointer[], s = scope) =>
  canonicalSnapshot(
    {
      store,
      provider: "s3",
      bucket: "bucket",
      prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" },
      pointers: async () => pointers,
      decompress,
    },
    s,
  );

test("turn-only discovery is per table and per org, and resolves the attributed source", async () => {
  const page = await legacyDiscoveryPage(db, "conversation_turns", "org_1", null);
  expect(page).toHaveLength(1);
  expect(page[0]).toMatchObject({ org_id: "org_1", workspace_id: "ws_1", stored_source: "owner", session_id: "sess:1", rows: "1" });
  expect(await legacyDiscoveryPage(db, "conversation_turns", "org_1", page[0]!)).toEqual([]);
  expect(await legacyDiscoveryPage(db, "conversation_turns", "other-org", null)).toEqual([]);
  expect((await legacyGroupScopes(db, page[0]!)).scopes).toEqual([scope]);
});

test("exports the legacy row and explicit empty-table coverage into its own legacy-machine manifest", async () => {
  const store = createFsStore({ root });
  const { result } = await exportScope(scope, store);
  expect(result.lines).toBe(2);
  expect(result.manifests).toHaveLength(1);
  const key = result.manifests[0]!.manifestKey;
  expect(parseSessionKey(key)?.machineId).toBe(RELAYHISTORY_LEGACY_MACHINE_ID);
  const snap = await snapshot(store, [await pointerFor(store, key)]);
  expect(snap.legacyCoverage).toEqual({ version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] });
  expect(snap.rows.filter((r) => r.table === "conversation_turns")).toHaveLength(1);
  expect(snap.rows.find((r) => r.table === "conversation_turns")).toMatchObject({
    row: {
      id: turnId,
      legacy_stored_source: "owner",
      content: "preserved legacy transcript",
      turn_index: 3,
      metadata: { nativeCli: "claude" },
      ts: "2026-11-01T10:00:00.123456+00:00",
    },
  });
});

test("erasure guard ignores a legacy-only group unless legacy recognition is switched on", async () => {
  expect(await groupExistsInNeon(db, "org_1", "sess:1")).toBe(false);
  expect(await groupExistsInNeon(db, "org_1", "sess:1", true)).toBe(true);
  expect(await groupExistsInNeon(db, "other-org", "sess:1", true)).toBe(false);
});

test("gate 1: an unattributable turn is counted as unbound and never stops discovery of other orgs", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.conversation_turns VALUES('44444444-4444-4444-8444-444444444444','org_a','ws','alice','lonely','alice',0,'user','hello','User','owner','{}','2026-11-01T10:00:00Z'),
 ('55555555-5555-4555-8555-555555555555','org_b','ws','claude','fine','bob',0,'user','hi','User','owner','{}','2026-11-01T10:00:00Z');`);
    const checkpoint = new Checkpoint(join(root, "g1.sqlite"));
    const counters = newCounters();
    const store = createFsStore({ root: join(root, "g1") });
    const deps = { db, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
    await runOrg(deps, "org_a", { export: true }, counters);
    expect(counters.unboundTurns).toBe(1);
    expect(counters.scopes).toBe(0);
    expect(counters.failed).toBe(0);
    const b = newCounters();
    await runOrg(deps, "org_b", { export: true }, b);
    expect(b).toMatchObject({ scopes: 1, exported: 1, failed: 0 });
    checkpoint.close();
  } finally {
    await pg.exec("DELETE FROM sessions.conversation_turns WHERE org_id IN('org_a','org_b')");
  }
});

test("gate 2: an ordinary unit keeps its date and key; the legacy manifest uses the ordinary start date on its own machine", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.session_rollups VALUES('org_1','sess:1','ws_1','claude','2026-11-03T00:00:00Z','2026-11-04T00:00:00Z','{m1}','{}',1,0,0,0,0,0,0)`);
    const unit = (await legacyUnit(db, scope))!;
    expect(new Date(unit.firstMs).toISOString()).toBe("2026-11-03T00:00:00.000Z");
    const { result } = await exportScope(scope, createFsStore({ root: join(root, "g2") }));
    const loc = parseSessionKey(result.manifests[0]!.manifestKey)!;
    expect(loc.machineId).toBe(RELAYHISTORY_LEGACY_MACHINE_ID);
    expect(result.manifests[0]!.manifestKey).toContain("/d=2026/11/03/");
  } finally {
    await pg.exec("DELETE FROM sessions.session_rollups");
  }
});

test("gate 3: a rerun skips unchanged scopes by fingerprint, and every discovery statement is one table in one org", async () => {
  const seen: string[] = [];
  const spy: Queryable = {
    query: async <T,>(sql: string, params?: unknown[]) => {
      seen.push(sql);
      return db.query<T>(sql, params);
    },
  };
  const checkpoint = new Checkpoint(join(root, "g3.sqlite"));
  const store = createFsStore({ root: join(root, "g3") });
  const deps = { db: spy, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
  const first = newCounters();
  await runOrg(deps, "org_1", { export: true }, first);
  expect(first).toMatchObject({ exported: 1, manifestsWritten: 1 });
  const second = newCounters();
  await runOrg(deps, "org_1", { export: true }, second);
  // Cursors are done: nothing is rediscovered or re-read.
  expect(second).toMatchObject({ groups: 0, exported: 0 });
  const refreshed = newCounters();
  await runOrg(deps, "org_1", { export: true, refresh: true }, refreshed);
  expect(refreshed).toMatchObject({ skippedCheckpoint: 1, exported: 0 });
  for (const sql of seen.filter((s) => s.includes("GROUP BY") && s.includes("count(*)::text AS rows"))) {
    expect(sql).toContain("WHERE org_id=$1");
    expect(sql).not.toContain("UNION");
  }
  // Changed rows change the fingerprint: exported again, unchanged objects reused.
  await pg.query(`UPDATE sessions.conversation_turns SET content='changed' WHERE id=$1`, [turnId]);
  const changed = newCounters();
  await runOrg(deps, "org_1", { export: true, refresh: true }, changed);
  expect(changed).toMatchObject({ exported: 1, manifestsWritten: 1 });
  await pg.query(`UPDATE sessions.conversation_turns SET content='preserved legacy transcript' WHERE id=$1`, [turnId]);
  checkpoint.close();
});

test("gate 4: legacy records are trusted only from the one exporter-owned legacy manifest", async () => {
  const store = createFsStore({ root: join(root, "g4") });
  const { result } = await exportScope(scope, store);
  const real = await pointerFor(store, result.manifests[0]!.manifestKey);
  // A second exporter-owned legacy manifest for the same scope (another start date): two legacy parts.
  const shifted = { ...(await legacyUnit(db, scope))!, firstMs: Date.parse("2026-10-01T00:00:00Z") };
  const copy = await exportUnit({ store, keyRoot: "96bulls", grantId: "grant_1" }, shifted, legacyUnitSource(db, async () => {}));
  const dup = await pointerFor(store, copy.manifests[0]!.manifestKey);
  expect(dup.manifest_key).not.toBe(real.manifest_key);
  await expect(snapshot(store, [real, dup])).rejects.toBeInstanceOf(CanonicalReadError);
  // Legacy-kinded records inside a manifest on an ordinary machine are refused.
  const forged = await exportUnit(
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    (await legacyUnit(db, scope))!,
    {
      fenced: async () => false,
      async *lines(unit) {
        for await (const line of legacyUnitSource(db, async () => {}).lines(unit)) yield { ...line, machine: "srv_forged" };
      },
    },
  );
  await expect(snapshot(store, [await pointerFor(store, forged.manifests[0]!.manifestKey)])).rejects.toBeInstanceOf(
    CanonicalReadError,
  );
  // The genuine one alone is accepted.
  expect((await snapshot(store, [real])).legacyCoverage).not.toBeNull();
});

test("gate 8: a legacy-only manifest carries the scope's real user, never the placeholder machine", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const store = createFsStore({ root: join(root, "g8") });
    const { result } = await exportScope(scope, store);
    const p = await pointerFor(store, result.manifests[0]!.manifestKey);
    expect(p.user_id).toBe("user_1");
    expect(p.machine_id).toBe(RELAYHISTORY_LEGACY_MACHINE_ID);
    // The delivery user of the scope's own events wins over an outcome user.
    await pg.exec(`INSERT INTO sessions.convergence_events VALUES('org_1','ws_1','claude','sess:1','u_delivery')`);
    const again = await exportScope(scope, createFsStore({ root: join(root, "g8b") }));
    expect((await pointerFor(createFsStore({ root: join(root, "g8b") }), again.result.manifests[0]!.manifestKey)).user_id).toBe(
      "u_delivery",
    );
  } finally {
    await pg.exec("DELETE FROM sessions.session_outcomes;DELETE FROM sessions.convergence_events");
  }
});

test("nonempty links and outcomes preserve exact source IDs, timestamps and metadata through the snapshot", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.session_links VALUES(9007199254740993,'org_1','ws_1','claude','sess:1','commit','','url','{"kept":true}',5000,'2026-11-01T10:00:00.123456Z','2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');
 INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const store = createFsStore({ root: join(root, "links") });
    const { result } = await exportScope(scope, store);
    const snap = await snapshot(store, [await pointerFor(store, result.manifests[0]!.manifestKey)]);
    expect(snap.rows.find((r) => r.table === "session_links")).toMatchObject({
      row: { id: "9007199254740993", link_ref: "", metadata: { kept: true }, link_ts: "2026-11-01T10:00:00.123456+00:00" },
    });
    expect(snap.rows.find((r) => r.table === "session_outcomes")).toMatchObject({
      row: { commit_sha: "abcdef", user_id: "user_1", reverted: false, shipped_at: null },
    });
    expect(snap.legacyCoverage).not.toBeNull();
  } finally {
    await pg.exec("DELETE FROM sessions.session_links;DELETE FROM sessions.session_outcomes");
  }
});

test("a fenced scope is not exported", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.deleted_upload_sessions VALUES('org_1','ws_1','claude','sess:1')`);
    const checkpoint = new Checkpoint(join(root, "fence.sqlite"));
    const counters = newCounters();
    await runOrg(
      { db, target: { store: createFsStore({ root: join(root, "fence") }), keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} },
      "org_1",
      { export: true },
      counters,
    );
    expect(counters).toMatchObject({ fenced: 1, exported: 0 });
    checkpoint.close();
  } finally {
    await pg.exec("DELETE FROM sessions.deleted_upload_sessions");
  }
});

test("verifier: equal on a fresh export, mismatched after Neon changes, including scrubbed content", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    await pg.query(`UPDATE sessions.conversation_turns SET content='key sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA end' WHERE id=$1`, [turnId]);
    const checkpoint = new Checkpoint(join(root, "verify.sqlite"));
    const store = createFsStore({ root: join(root, "verify") });
    await runOrg({ db, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} }, "org_1", { export: true }, newCounters());
    const ok = await verifyExported(db, store, checkpoint, decompress);
    expect(ok).toMatchObject({ ok: true, scopes: 1 });
    await pg.query(`UPDATE sessions.conversation_turns SET content='drifted' WHERE id=$1`, [turnId]);
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','fedcba','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const bad = await verifyExported(db, store, checkpoint, decompress);
    expect(bad.ok).toBe(false);
    expect(bad.mismatched[0]!.problems).toEqual(
      expect.arrayContaining(["session_outcomes:count neon=2 s3=1", "session_outcomes:keys", "conversation_turns:content"]),
    );
    checkpoint.close();
  } finally {
    await pg.query(`UPDATE sessions.conversation_turns SET content='preserved legacy transcript' WHERE id=$1`, [turnId]);
    await pg.exec("DELETE FROM sessions.session_outcomes");
  }
});

test("the verifier's Neon SQL is read-only and parameterized", () => {
  expect(NEON_SCOPE_SQL).not.toMatch(/\b(INSERT|UPDATE|DELETE|TRUNCATE|CREATE|ALTER|DROP)\b/u);
  expect(NEON_SCOPE_SQL).toContain("$1");
});

test("secrets come only from stdin lines, and a writable Neon role is refused", async () => {
  const s = await readSecrets("postgresql://ro@host/db\nAKIA\nsecret\n");
  expect(s).toEqual({ neonUrl: "postgresql://ro@host/db", s3: { accessKeyId: "AKIA", secretAccessKey: "secret" } });
  await expect(readSecrets("not-a-url\n")).rejects.toThrow("line 1");
  // PGlite's current user owns the tables: it can write, so it is refused.
  await expect(assertSelectOnly(db)).rejects.toThrow("SELECT-only");
});

test("a re-export keeps the scope's first key even if its oldest row moves earlier, and an interrupt does not advance the cursor", async () => {
  const id = "66666666-6666-4666-8666-666666666666";
  const checkpoint = new Checkpoint(join(root, "pin.sqlite"));
  const store = createFsStore({ root: join(root, "pin") });
  const deps = { db, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
  try {
    await runOrg(deps, "org_1", { export: true }, newCounters());
    const first = checkpoint.exported("org_1")[0]!.manifest_key;
    await pg.query(
      `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','claude','sess:1','owner',0,'user','older','U','owner','{}','2026-10-01T00:00:00Z')`,
      [id],
    );
    const again = newCounters();
    await runOrg(deps, "org_1", { export: true, refresh: true }, again);
    expect(again.exported).toBe(1);
    expect(checkpoint.exported("org_1")[0]!.manifest_key).toBe(first);
    const stopping = new Checkpoint(join(root, "stop.sqlite"));
    await expect(
      runOrg(
        { ...deps, checkpoint: stopping, hook: async () => { throw Object.assign(new Error("stopping"), { code: "interrupted" }); } },
        "org_1",
        { export: true },
        newCounters(),
      ),
    ).rejects.toThrow("stopping");
    expect(stopping.cursor("conversation_turns", "org_1")).toEqual({ after: null, done: false });
    stopping.close();
  } finally {
    checkpoint.close();
    await pg.query("DELETE FROM sessions.conversation_turns WHERE id=$1", [id]);
  }
});
