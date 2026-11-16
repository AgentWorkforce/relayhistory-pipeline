import { afterAll as __modeAfterAll, beforeEach as __modeBeforeEach } from "bun:test";
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
  legacyMachineId,
  legacyDiscoveryPage,
  legacyGroupScopes,
  legacyScopeUser,
  legacyUnit,
  legacyUnitSource,
  type LegacyScope,
} from "../neon-s3-sync/legacy-source.ts";
import { exportUnit } from "../neon-s3-sync/export-session.ts";
import { groupExistsInNeon } from "../neon-s3-sync/neon-reader.ts";
import { Checkpoint, newCounters, runOrg, verifyExported, withLegacyPrivacyFence, NEON_SCOPE_SQL } from "../legacy-export/runner.ts";
import {
  assertDirectUrl,
  assertPrivacySelectOnly,
  assertSelectOnly,
  readSecrets,
} from "../legacy-export/main.ts";
import {
  canonicalSnapshot,
  CanonicalReadError,
  type CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { neonAuthority } from "../neon-s3-erasure/authority.ts";

// These suites exercise the obligation model (S3X_OBLIGATIONS on); the default-off path has its own suite.
// The suite's OWN mode, pinned per module, re-applied before every test and restored after the file (never leaked).
const OBLIGATIONS_MODE: "on" | "off" = "on";
const __modeBefore = process.env.S3X_OBLIGATIONS;
process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE;
__modeBeforeEach(() => void (process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE));
__modeAfterAll(() => { if (__modeBefore === undefined) delete process.env.S3X_OBLIGATIONS; else process.env.S3X_OBLIGATIONS = __modeBefore; });
const pg = new PGlite(),
  db = pg as unknown as Queryable;
const privacyDb: Queryable = {
  async query<T>(sql: string, params?: unknown[]) {
    if (
      /^(BEGIN|COMMIT|ROLLBACK|SET LOCAL)/u.test(sql) ||
      sql.includes("pg_advisory_xact_lock_shared")
    )
      return { rows: [] as T[] };
    return db.query<T>(sql, params);
  },
};
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
 CREATE TABLE sessions.conversation_turn_fences(org_id text,workspace_id text,user_id text,source text,session_id text);
 CREATE TABLE sessions.session_erasure_receipts(org_id text,workspace_id text,source text,session_id text);
 CREATE TABLE sessions.upload_deletions(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text);
 CREATE TABLE sessions.ingest_frozen_source_deletions(org_id text,workspace_id text,user_id text,source text,session_id text);
 CREATE TABLE sessions.upload_resets(org_id text,workspace_id text,user_id text,request_id text);
 CREATE TABLE sessions.turn_session_fences(org_id text,workspace_id text,source text,session_id text);
 CREATE TABLE sessions.operator_user_erasures(request_id text,org_id text,workspace_id text,user_id text,state text);
 CREATE TABLE sessions.conversation_turns(id uuid PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,session_owner text,turn_index int,role text,content text,actor_name text,actor_role text,metadata jsonb,ts timestamptz,UNIQUE(org_id,workspace_id,source,session_id,turn_index));
 CREATE TABLE sessions.session_links(id bigint PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,link_kind text,link_ref text,link_url text,metadata jsonb,confidence_basis_points int,link_ts timestamptz,updated_at timestamptz,created_at timestamptz);
 CREATE TABLE sessions.session_outcomes(org_id text,workspace_id text,source text,session_id text,commit_sha text,user_id text,reverted boolean,shipped_at timestamptz,ingested_at timestamptz,created_at timestamptz);`);
  await pg.exec(`CREATE TABLE sessions.legacy_export_scope_authority(
    org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
    legacy_user_id text NOT NULL,legacy_machine_id text NOT NULL,
    PRIMARY KEY(org_id,workspace_id,source,session_id));
  CREATE FUNCTION sessions.ingest_legacy_export_scope_authority_register(p jsonb) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $$
  DECLARE v jsonb;
  BEGIN
    INSERT INTO sessions.legacy_export_scope_authority VALUES(
      p->>'org_id',CASE WHEN p->>'workspace_id'='' THEN 'default' ELSE p->>'workspace_id' END,
      p->>'source',p->>'session_id',p->>'legacy_user_id',p->>'legacy_machine_id') ON CONFLICT DO NOTHING;
    SELECT to_jsonb(a) INTO v FROM sessions.legacy_export_scope_authority a
      WHERE a.org_id=p->>'org_id'
        AND a.workspace_id=CASE WHEN p->>'workspace_id'='' THEN 'default' ELSE p->>'workspace_id' END
        AND a.source=p->>'source' AND a.session_id=p->>'session_id';
    RETURN v;
  END $$;`);
  await pg.query(
    `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','owner','sess:1','person',3,'assistant','preserved legacy transcript','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00.123456Z')`,
    [turnId],
  );
  await pg.exec(`INSERT INTO sessions.convergence_events VALUES('org_1','ws_1','claude','sess:1','person')`);
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

async function restoreBaseOwner() {
  await pg.exec(`DELETE FROM sessions.convergence_events WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1';
    INSERT INTO sessions.convergence_events VALUES('org_1','ws_1','claude','sess:1','person')`);
}

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
  expect(parseSessionKey(key)?.machineId).toBe(legacyMachineId("ws_1", "person"));
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
 ('55555555-5555-4555-8555-555555555555','org_b','ws','claude','fine','bob',0,'user','hi','User','owner','{}','2026-11-01T10:00:00Z');
 INSERT INTO sessions.session_outcomes VALUES('org_b','ws','claude','fine','bob-owner','bob',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const checkpoint = new Checkpoint(join(root, "g1.sqlite"));
    const counters = newCounters();
    const store = createFsStore({ root: join(root, "g1") });
    const deps = { db, privacyDb, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
    await runOrg(deps, "org_a", { export: true }, counters);
    expect(counters.unboundTurns).toBe(1);
    expect(counters.scopes).toBe(0);
    expect(counters.failed).toBe(0);
    const b = newCounters();
    await runOrg(deps, "org_b", { export: true }, b);
    expect(b).toMatchObject({ scopes: 1, exported: 1, failed: 0 });
    checkpoint.close();
  } finally {
    await pg.exec("DELETE FROM sessions.conversation_turns WHERE org_id IN('org_a','org_b');DELETE FROM sessions.session_outcomes WHERE org_id='org_b'");
  }
});

test("gate 2: an ordinary unit keeps its date and key; the legacy manifest uses the ordinary start date on its own machine", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.session_rollups VALUES('org_1','sess:1','ws_1','claude','2026-11-03T00:00:00Z','2026-11-04T00:00:00Z','{m1}','{}',1,0,0,0,0,0,0)`);
    const unit = (await legacyUnit(db, scope))!;
    expect(new Date(unit.firstMs).toISOString()).toBe("2026-11-03T00:00:00.000Z");
    const { result } = await exportScope(scope, createFsStore({ root: join(root, "g2") }));
    const loc = parseSessionKey(result.manifests[0]!.manifestKey)!;
    expect(loc.machineId).toBe(legacyMachineId("ws_1", "person"));
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
  const deps = { db: spy, privacyDb, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
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
  const duplicate = await snapshot(store, [real, dup]);
  expect(duplicate.legacyCoverage).toBeNull();
  expect(
    duplicate.rows.filter((r) => r.table === "conversation_turns"),
  ).toEqual([]);
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
  const forgedSnapshot = await snapshot(store, [
    await pointerFor(store, forged.manifests[0]!.manifestKey),
  ]);
  expect(forgedSnapshot.legacyCoverage).toBeNull();
  expect(forgedSnapshot.rows).toEqual([]);
  // The genuine one alone is accepted.
  expect((await snapshot(store, [real])).legacyCoverage).not.toBeNull();
});

test("gate 8: a legacy-only manifest carries the scope's real user, never the placeholder machine", async () => {
  try {
    await pg.exec(`DELETE FROM sessions.convergence_events WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
    await pg.exec(`DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const store = createFsStore({ root: join(root, "g8") });
    const { result } = await exportScope(scope, store);
    const p = await pointerFor(store, result.manifests[0]!.manifestKey);
    expect(p.user_id).toBe("user_1");
    expect(p.machine_id).toBe(legacyMachineId("ws_1", "user_1"));
    // The delivery user of the scope's own events wins over an outcome user.
    await pg.exec(`INSERT INTO sessions.convergence_events VALUES('org_1','ws_1','claude','sess:1','u_delivery')`);
    const again = await exportScope(scope, createFsStore({ root: join(root, "g8b") }));
    expect((await pointerFor(createFsStore({ root: join(root, "g8b") }), again.result.manifests[0]!.manifestKey)).user_id).toBe(
      "u_delivery",
    );
  } finally {
    await pg.exec("DELETE FROM sessions.convergence_events;DELETE FROM sessions.session_outcomes");
    await restoreBaseOwner();
  }
});

test("nonempty links and outcomes preserve exact source IDs, timestamps and metadata through the snapshot", async () => {
  try {
    await pg.exec(`DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
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
    await restoreBaseOwner();
  }
});

test("frozen Neon delivery and conversation-turn fences both prevent export", async () => {
  for (const table of ["deleted_upload_sessions", "conversation_turn_fences"] as const) {
    const checkpoint = new Checkpoint(join(root, `fence-${table}.sqlite`));
    try {
      if (table === "deleted_upload_sessions")
        await pg.exec(`INSERT INTO sessions.deleted_upload_sessions VALUES('org_1','ws_1','claude','sess:1')`);
      else
        await pg.exec(`INSERT INTO sessions.conversation_turn_fences VALUES('org_1','ws_1','person','claude','sess:1')`);
      const counters = newCounters();
      await runOrg(
        { db, privacyDb, target: { store: createFsStore({ root: join(root, `fence-${table}`) }), keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} },
        "org_1",
        { export: true },
        counters,
      );
      expect(counters).toMatchObject({ fenced: 1, exported: 0 });
    } finally {
      checkpoint.close();
      await pg.exec("DELETE FROM sessions.deleted_upload_sessions;DELETE FROM sessions.conversation_turn_fences");
    }
  }
});

test("live session, upload deletion/reset, turn-session and completed-user erase ledgers fence legacy export", async () => {
  for (const erase of ["session", "deletion", "reset", "turn", "user"] as const) {
    const checkpoint = new Checkpoint(join(root, `live-fence-${erase}.sqlite`));
    try {
      if (erase === "session")
        await pg.exec(
          `INSERT INTO sessions.session_erasure_receipts VALUES('org_1','ws_1','claude','sess:1')`,
        );
      else if (erase === "deletion")
        await pg.exec(
          `INSERT INTO sessions.upload_deletions VALUES('org_1','ws_1','person','origin','claude','sess:1')`,
        );
      else if (erase === "reset")
        await pg.exec(
          `INSERT INTO sessions.upload_resets VALUES('org_1','ws_1','person','reset-1')`,
        );
      else if (erase === "turn")
        await pg.exec(
          `INSERT INTO sessions.turn_session_fences VALUES('org_1','ws_1','claude','sess:1')`,
        );
      else
        await pg.exec(
          `INSERT INTO sessions.operator_user_erasures VALUES('erase-1','org_1','ws_1','person','bytes_verified')`,
        );
      const counters = newCounters();
      await runOrg(
        {
          db,
          privacyDb,
          target: {
            store: createFsStore({ root: join(root, `live-fence-${erase}`) }),
            keyRoot: "96bulls",
            grantId: "g",
          },
          checkpoint,
          hook: async () => {},
          log: () => {},
        },
        "org_1",
        { export: true },
        counters,
      );
      expect(counters).toMatchObject({ fenced: 1, exported: 0 });
    } finally {
      checkpoint.close();
      await pg.exec(
        "DELETE FROM sessions.session_erasure_receipts;DELETE FROM sessions.upload_deletions;DELETE FROM sessions.upload_resets;DELETE FROM sessions.turn_session_fences;DELETE FROM sessions.operator_user_erasures",
      );
    }
  }
});

test("session, upload deletion and reset races are caught after the object write", async () => {
  for (const erase of ["session", "deletion", "reset"] as const) {
    await expect(
      withLegacyPrivacyFence(privacyDb, scope, "person", async () => {
        if (erase === "session")
          await pg.exec(`INSERT INTO sessions.session_erasure_receipts VALUES('org_1','ws_1','claude','sess:1')`);
        else if (erase === "deletion")
          await pg.exec(`INSERT INTO sessions.upload_deletions VALUES('org_1','ws_1','person','origin','claude','sess:1')`);
        else
          await pg.exec(`INSERT INTO sessions.upload_resets VALUES('org_1','ws_1','person','reset-race')`);
        return { manifests: [{ manifestKey: `written/${erase}/manifest.json` }] };
      }),
    ).rejects.toMatchObject({
      code: "legacy_privacy_race",
      privacyValue: { manifests: [{ manifestKey: `written/${erase}/manifest.json` }] },
    });
    await pg.exec("DELETE FROM sessions.session_erasure_receipts;DELETE FROM sessions.upload_deletions;DELETE FROM sessions.upload_resets");
  }
});

test("a post-PUT privacy race records the written manifest and aborts the run", async () => {
  const checkpoint = new Checkpoint(join(root, "privacy-race.sqlite"));
  let fenceReads = 0;
  const raceDb: Queryable = {
    async query<T>(sql: string, params?: unknown[]) {
      if (
        /^(BEGIN|COMMIT|ROLLBACK|SET LOCAL)/u.test(sql) ||
        sql.includes("pg_advisory_xact_lock_shared")
      )
        return { rows: [] as T[] };
      // Registration is committed in its own transaction, then the write
      // transaction has a precheck. Inject on that transaction's postcheck.
      if (sql.includes("AS fenced") && ++fenceReads === 4)
        await pg.exec(`INSERT INTO sessions.upload_resets VALUES('org_1','ws_1','person','post-put')`);
      return db.query<T>(sql, params);
    },
  };
  try {
    await expect(
      runOrg(
        {
          db,
          privacyDb: raceDb,
          target: {
            store: createFsStore({ root: join(root, "privacy-race") }),
            keyRoot: "96bulls",
            grantId: "g",
          },
          checkpoint,
          hook: async () => {},
          log: () => {},
        },
        "org_1",
        { export: true },
        newCounters(),
      ),
    ).rejects.toMatchObject({ code: "legacy_privacy_race" });
    expect(checkpoint.scope(JSON.stringify(["org_1", "ws_1", "claude", "sess:1"]))).toMatchObject({
      status: "failed:legacy_privacy_race",
      user_id: "person",
      manifest_key: expect.stringContaining("/manifest.json"),
    });
  } finally {
    checkpoint.close();
    await pg.exec("DELETE FROM sessions.upload_resets");
  }
});

test("verifier: equal on a fresh export, mismatched after Neon changes, including scrubbed content", async () => {
  try {
    await pg.exec(`DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    await pg.query(`UPDATE sessions.conversation_turns SET content='key sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA end' WHERE id=$1`, [turnId]);
    const checkpoint = new Checkpoint(join(root, "verify.sqlite"));
    const store = createFsStore({ root: join(root, "verify") });
    await runOrg({ db, privacyDb, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} }, "org_1", { export: true }, newCounters());
    const ok = await verifyExported(
      db,
      store,
      checkpoint,
      decompress,
      undefined,
      privacyDb,
    );
    expect(ok).toMatchObject({ ok: true, scopes: 1 });
    await pg.query(`UPDATE sessions.conversation_turns SET content='drifted' WHERE id=$1`, [turnId]);
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','fedcba','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
    const bad = await verifyExported(
      db,
      store,
      checkpoint,
      decompress,
      undefined,
      privacyDb,
    );
    expect(bad.ok).toBe(false);
    expect(bad.mismatched[0]!.problems).toEqual(
      expect.arrayContaining(["session_outcomes:count neon=2 s3=1", "session_outcomes:keys", "conversation_turns:content"]),
    );
    checkpoint.close();
  } finally {
    await pg.query(`UPDATE sessions.conversation_turns SET content='preserved legacy transcript' WHERE id=$1`, [turnId]);
    await pg.exec("DELETE FROM sessions.session_outcomes");
    await restoreBaseOwner();
  }
});

test("the verifier's Neon SQL is read-only and parameterized", () => {
  expect(NEON_SCOPE_SQL).not.toMatch(/\b(INSERT|UPDATE|DELETE|TRUNCATE|CREATE|ALTER|DROP)\b/u);
  expect(NEON_SCOPE_SQL).toContain("$1");
});

test("secrets come only from stdin lines, and a writable Neon role is refused", async () => {
  const s = await readSecrets(
    "postgresql://ro@host/db\npostgresql://privacy@host/db\nAKIA\nsecret\n",
  );
  expect(s).toEqual({
    neonUrl: "postgresql://ro@host/db",
    privacyUrl: "postgresql://privacy@host/db",
    s3: { accessKeyId: "AKIA", secretAccessKey: "secret" },
  });
  await expect(readSecrets("not-a-url\n")).rejects.toThrow("line 1");
  // PGlite's current user owns the tables: it can write, so it is refused.
  await expect(assertSelectOnly(db)).rejects.toThrow("SELECT-only");
  expect(() =>
    assertDirectUrl("postgresql://u@direct.example:5432/db", "Neon"),
  ).not.toThrow();
  expect(() =>
    assertDirectUrl("postgresql://u@direct.example:6543/db", "Neon"),
  ).toThrow("unpooled");
  expect(() =>
    assertDirectUrl("postgresql://u@x-pooler.example:5432/db", "privacy"),
  ).toThrow("unpooled");
  expect(() =>
    assertDirectUrl("postgresql://u@abc.pooler.supabase.com:5432/db", "privacy"),
  ).toThrow("unpooled");
});

test("privacy preflight rejects active RLS even for a SELECT-only role", async () => {
  await pg.exec(`CREATE ROLE legacy_privacy_reader;
    GRANT USAGE ON SCHEMA sessions TO legacy_privacy_reader;
    GRANT SELECT ON sessions.session_erasure_receipts,sessions.upload_deletions,sessions.ingest_frozen_source_deletions,sessions.upload_resets,sessions.turn_session_fences,sessions.operator_user_erasures TO legacy_privacy_reader;
    GRANT EXECUTE ON FUNCTION sessions.ingest_legacy_export_scope_authority_register(jsonb) TO legacy_privacy_reader;
    ALTER TABLE sessions.session_erasure_receipts ENABLE ROW LEVEL SECURITY;
    SET ROLE legacy_privacy_reader`);
  try {
    await expect(assertPrivacySelectOnly(db)).rejects.toMatchObject({
      code: "privacy_role_not_read_only",
    });
  } finally {
    await pg.exec(`RESET ROLE;
      ALTER TABLE sessions.session_erasure_receipts DISABLE ROW LEVEL SECURITY;
      REVOKE ALL PRIVILEGES ON sessions.session_erasure_receipts,sessions.upload_deletions,sessions.ingest_frozen_source_deletions,sessions.upload_resets,sessions.turn_session_fences,sessions.operator_user_erasures FROM legacy_privacy_reader;
      REVOKE EXECUTE ON FUNCTION sessions.ingest_legacy_export_scope_authority_register(jsonb) FROM legacy_privacy_reader;
      REVOKE USAGE ON SCHEMA sessions FROM legacy_privacy_reader;
      DROP ROLE legacy_privacy_reader`);
  }
});

test("turns-only scope uses an unambiguous session_owner verified elsewhere in the tenant", async () => {
  await pg.exec(`INSERT INTO sessions.convergence_events VALUES('org_1','ws_1','claude','verified-owner','person');
    DELETE FROM sessions.convergence_events WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
  await pg.exec(`DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
  try {
    expect(await legacyScopeUser(db, scope)).toBe("person");
  } finally {
    await pg.exec("DELETE FROM sessions.convergence_events WHERE session_id='verified-owner'");
    await restoreBaseOwner();
  }
});

test("turns-only free text without authenticated tenant evidence is not an erasure owner", async () => {
  await pg.exec(`DELETE FROM sessions.convergence_events;
    DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1'`);
  try {
    expect(await legacyScopeUser(db, scope)).toBeNull();
  } finally {
    await restoreBaseOwner();
  }
});

test("the Neon hosted eraser derives that same turns-only owner for a fenced scope", async () => {
  await pg.exec(`DELETE FROM sessions.convergence_events WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1';
    DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND workspace_id='ws_1' AND source='claude' AND session_id='sess:1';
    INSERT INTO sessions.deleted_upload_sessions VALUES('org_1','ws_1','claude','sess:1')`);
  try {
    expect(await neonAuthority(db).legacyFencedPage!(null)).toEqual([{
      org_id: "org_1", workspace_id: "ws_1", source: "claude", session_id: "sess:1",
      legacy_user_id: "person", legacy_machine_id: legacyMachineId("ws_1", "person"),
    }]);
  } finally {
    await pg.exec("DELETE FROM sessions.deleted_upload_sessions");
    await restoreBaseOwner();
  }
});

test("windowed cursors do not suppress a later full run, max-scopes progresses, and failed scopes fail verify", async () => {
  const checkpoint = new Checkpoint(join(root, "progress.sqlite"));
  const store = createFsStore({ root: join(root, "progress") });
  const deps = {
    db,
    privacyDb,
    target: { store, keyRoot: "96bulls", grantId: "g" },
    checkpoint,
    hook: async () => {},
    log: () => {},
  };
  const second = "77777777-7777-4777-8777-777777777777";
  try {
    await pg.query(
      `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','claude','sess:2','person2',0,'user','second','U','owner','{}','2026-11-02T00:00:00Z')`,
      [second],
    );
    await pg.exec(`INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:2','second-owner','person2',false,NULL,'2026-11-02T00:00:00Z','2026-11-02T00:00:00Z')`);
    const one = newCounters();
    await runOrg(
      deps,
      "org_1",
      {
        export: true,
        maxScopes: 1,
        window: {
          sinceMs: Date.parse("2026-11-01T00:00:00Z"),
          untilMs: Date.parse("2026-11-03T00:00:00Z"),
        },
      },
      one,
    );
    expect(one.exported).toBe(1);
    const two = newCounters();
    await runOrg(deps, "org_1", { export: true, maxScopes: 1 }, two);
    expect(two.exported).toBe(1);
    expect(checkpoint.exported("org_1")).toHaveLength(2);
    checkpoint.setScope(
      JSON.stringify(["org_1", "ws_1", "claude", "failed"]),
      "org_1",
      "failed:test",
      null,
      null,
    );
    checkpoint.setScope(
      JSON.stringify(["org_1", "ws_1", "claude", "retained"]),
      "org_1",
      "fenced",
      null,
      "96bulls/org_1/retained/manifest.json",
      "person",
    );
    const report = await verifyExported(
      db,
      store,
      checkpoint,
      decompress,
      "org_1",
      privacyDb,
    );
    expect(report.ok).toBe(false);
    expect(report.unexplained).toContain("failed:test:1");
    expect(report.unexplained).toContain(
      'retained_manifest:fenced:["org_1","ws_1","claude","retained"]',
    );
  } finally {
    checkpoint.close();
    await pg.query("DELETE FROM sessions.conversation_turns WHERE id=$1", [
      second,
    ]);
    await pg.exec(`DELETE FROM sessions.session_outcomes WHERE org_id='org_1' AND session_id='sess:2'`);
  }
});

test("a re-export keeps the scope's first key even if its oldest row moves earlier, and an interrupt does not advance the cursor", async () => {
  const id = "66666666-6666-4666-8666-666666666666";
  const checkpoint = new Checkpoint(join(root, "pin.sqlite"));
  const store = createFsStore({ root: join(root, "pin") });
  const deps = { db, privacyDb, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
  try {
    await runOrg(deps, "org_1", { export: true }, newCounters());
    const first = checkpoint.exported("org_1")[0]!.manifest_key;
    await pg.query(
      `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','claude','sess:1','person',0,'user','older','U','owner','{}','2026-10-01T00:00:00Z')`,
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
