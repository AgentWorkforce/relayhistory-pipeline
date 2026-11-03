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
  readAll,
} from "../../../packages/object-store/src/index.ts";
import {
  withLegacySource,
  legacyActivityPage,
  includeLegacyUnits,
} from "../neon-s3-sync/legacy-source.ts";
import { Syncer, DEFAULT_SYNC_CONFIG } from "../neon-s3-sync/sync.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { Throttle } from "../neon-s3-sync/throttle.ts";
import { canonicalLegacy } from "../../session-read-api/canonical-legacy.ts";
import { groupExistsInNeon } from "../neon-s3-sync/neon-reader.ts";
import { exportUnit } from "../neon-s3-sync/export-session.ts";
import {
  canonicalSnapshot,
  type CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import type { Queryable } from "../relayhistory-export.ts";
const pg = new PGlite(),
  db = pg as unknown as Queryable;
const turnId = "11111111-1111-4111-8111-111111111111";
let root: string;
beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), "canonical-legacy-"));
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_rollups(org_id text,session_id text);CREATE TABLE sessions.session_catalog(org_id text,session_id text);
 CREATE TABLE sessions.conversation_turns(id uuid PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,session_owner text,turn_index int,role text,content text,actor_name text,actor_role text,metadata jsonb,ts timestamptz);
 CREATE TABLE sessions.session_links(id bigint PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,link_kind text,link_ref text,link_url text,metadata jsonb,confidence_basis_points int,link_ts timestamptz,updated_at timestamptz,created_at timestamptz);
 CREATE TABLE sessions.session_outcomes(org_id text,workspace_id text,source text,session_id text,commit_sha text,user_id text,reverted boolean,shipped_at timestamptz,ingested_at timestamptz,created_at timestamptz);`);
  await pg.query(
    `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','claude','sess:1','owner',3,'assistant','preserved legacy transcript','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00.123456Z')`,
    [turnId],
  );
});
afterAll(async () => {
  await pg.close();
  await rm(root, { recursive: true, force: true });
});
test("turn-only discovery and unit scope do not depend on catalog or rollups", async () => {
  const rows = await legacyActivityPage(db, null, 1);
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    org_id: "org_1",
    workspace_id: "ws_1",
    source: "claude",
    session_id: "sess:1",
  });
  expect(await legacyActivityPage(db, rows[0]!, 1)).toEqual([]);
  const units = await includeLegacyUnits(db, "org_1", "sess:1", []);
  expect(units).toHaveLength(1);
  expect(units[0]).toMatchObject({
    workspaceId: "ws_1",
    source: "claude",
    sessionId: "sess:1",
  });
  expect(await includeLegacyUnits(db, "other-org", "sess:1", [])).toEqual([]);
});
test("exports and reads actual legacy row plus explicit empty-table coverage through canonical objects", async () => {
  const unit = (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!;
  const store = createFsStore({ root });
  const source = withLegacySource(
    db,
    { fenced: async () => false, async *lines() {} },
    async () => {},
    1,
  );
  const result = await exportUnit(
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    unit,
    source,
  );
  expect(result.lines).toBe(2);
  expect(result.manifests).toHaveLength(1);
  const key = result.manifests[0]!.manifestKey;
  const manifest = await parseManifest(
    new TextDecoder().decode(await readAll(await store.get(objectKey(key)))),
    key,
  );
  const pointer: CanonicalPointer = {
    org_id: "org_1",
    workspace_id: "ws_1",
    harness: "claude",
    session_id: "sess:1",
    machine_id: "legacy:records",
    user_id: "unknown",
    storage_provider: "s3",
    bucket: "bucket",
    manifest_key: key,
    content_sha256: manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: manifest.revision,
    deleted_at: null,
  };
  const snapshot = await canonicalSnapshot(
    {
      store,
      provider: "s3",
      bucket: "bucket",
      prefix: {
        PIPELINE_KEY_PREFIX: "96bulls",
        PIPELINE_ORG_FROM_PREFIX: "true",
      },
      pointers: async () => [pointer],
      decompress: (b) => new Uint8Array(zstdDecompressSync(b)),
    },
    {
      orgId: "org_1",
      workspaceId: "ws_1",
      source: "claude",
      sessionId: "sess:1",
    },
  );
  expect(snapshot.legacyCoverage).toEqual({
    version: 1,
    tables: ["conversation_turns", "session_links", "session_outcomes"],
  });
  expect(snapshot.rows).toHaveLength(1);
  expect(snapshot.rows[0]).toMatchObject({
    table: "conversation_turns",
    row: {
      id: turnId,
      content: "preserved legacy transcript",
      turn_index: 3,
      metadata: { nativeCli: "claude" },
      ts: "2026-11-01T10:00:00.123456+00:00",
    },
  });
});

test("erasure guard retains a legacy-only group even with no catalog or rollup", async () => {
  expect(await groupExistsInNeon(db, "org_1", "sess:1")).toBe(true);
  expect(await groupExistsInNeon(db, "other-org", "sess:1")).toBe(false);
});

test("nonempty links and outcomes preserve exact source IDs, timestamp and metadata", async () => {
  await pg.exec(`INSERT INTO sessions.session_links VALUES(9007199254740993,'org_1','ws_1','claude','sess:1','commit','ref','url','{"kept":true}',5000,'2026-11-01T10:00:00.123456Z','2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');
 INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
  const unit = (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!;
  const source = withLegacySource(
    db,
    { fenced: async () => false, async *lines() {} },
    async () => {},
    1,
  );
  const records = [];
  for await (const line of source.lines(unit))
    records.push(JSON.parse(new TextDecoder().decode(line.bytes)));
  const result = canonicalLegacy(records, {
    orgId: "org_1",
    workspaceId: "ws_1",
    source: "claude",
    sessionId: "sess:1",
  });
  expect(result.rows.find((r) => r.table === "session_links")).toMatchObject({
    row: {
      id: "9007199254740993",
      metadata: { kept: true },
      link_ts: "2026-11-01T10:00:00.123456+00:00",
    },
  });
  expect(result.rows.find((r) => r.table === "session_outcomes")).toMatchObject(
    {
      row: {
        commit_sha: "abcdef",
        user_id: "user_1",
        reverted: false,
        shipped_at: null,
      },
    },
  );
  expect(result.coverage).not.toBeNull();
});
test("opt-in sync discovers and exports a legacy-only group; default source stays inert", async () => {
  const fake: Queryable = {
    async query<T>(sql: string, params?: unknown[]) {
      if (sql.includes("pg_locks"))
        return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      if (
        sql.includes("FROM sessions.session_rollups") ||
        sql.includes("FROM sessions.session_catalog")
      )
        return { rows: [] as T[] };
      return db.query<T>(sql, params);
    },
  };
  const state = new SyncState(join(root, "sync.sqlite"));
  const store = createFsStore({ root: join(root, "sync-objects") });
  const throttle = new Throttle({
    rowsPerSec: 1e9,
    bytesPerSec: 1e12,
    slowWindows: [],
    slowFactor: 1,
  });
  const base = () => ({ fenced: async () => false, async *lines() {} });
  const config = { ...DEFAULT_SYNC_CONFIG, includeLegacySources: true };
  const syncer = new Syncer(
    fake,
    state,
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    throttle,
    () => {},
    config,
    () => false,
    base,
  );
  await syncer.sweepActivity();
  const work = state.nextWork(Date.now(), config.recentMs, 10)[0]!;
  expect(work).toBeTruthy();
  expect(await syncer.processGroup(work)).toBe(true);
  expect(state.exported("org_1", "sess:1")?.keys.length).toBe(1);
  const dormant = new Syncer(
    fake,
    state,
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    throttle,
    () => {},
    DEFAULT_SYNC_CONFIG,
    () => false,
    base,
  );
  const lines = [];
  for await (const line of dormant.source.lines(
    (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!,
  ))
    lines.push(line);
  expect(lines).toEqual([]);
  state.close();
});
