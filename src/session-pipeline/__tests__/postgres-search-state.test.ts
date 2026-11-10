import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { postgresSearchState } from "../postgres-search-state.ts";
import type { TenantRunner } from "../../session-read-api/liveness.ts";

test("portable completeness binds current metadata epoch, missing state, tenant, floor and bounded partial checks", async () => {
  const db = new PGlite();
  const tenant = { orgId: "org-a", workspaceId: "ws-a" };
  const scope = { ...tenant, harness: "codex", sessionId: "s1" };
  try {
    await db.exec(`CREATE SCHEMA sessions;
      CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,canonical_session_id text,deleted_at timestamptz);
      CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,event_projection_epoch bigint,event_projection_ready boolean);
      CREATE ROLE portable_state_reader;
      INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','s1',NULL,NULL),('org-b','ws-a','codex','s1',NULL,NULL);
      INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s1',3,true),('org-b','ws-a','codex','s1',3,true);`);
    const reader: TenantRunner = (org, workspace, work) => db.transaction(async tx => {
      await tx.exec("SET LOCAL ROLE portable_state_reader");
      await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [org, workspace]);
      return work(tx);
    });
    const config = { reader, backendId: "postgres-v1", fromMs: 0 };
    const state = postgresSearchState(config);
    await state.ensureSchema(db);
    await state.ensureSchema(db);
    // Synthetic isolated roles only. Production grants remain a separate review.
    await db.exec("GRANT USAGE ON SCHEMA sessions,search_index TO portable_state_reader; GRANT SELECT ON ALL TABLES IN SCHEMA sessions,search_index TO portable_state_reader");
    const check = (sessionId?: string, limit = 1000) => state.partials(tenant, { sessionId, limit });
    const record = (complete: boolean, actual = scope) => db.transaction(tx => state.record(tx, actual, { revision: 1, rows: 1, complete, at: new Date(), canonicalSessionId: actual.sessionId }));
    expect(await check()).toEqual({ sessions: ["s1"], overflow: false }); // first pass has no index/state rows
    await record(true);
    expect(await check()).toEqual({ sessions: [], overflow: false });
    await db.exec("UPDATE sessions.session_directory SET event_projection_epoch=4,event_projection_ready=false WHERE org_id='org-a'");
    expect((await check()).sessions).toEqual(["s1"]);
    await db.exec("UPDATE sessions.session_directory SET event_projection_ready=true WHERE org_id='org-a'");
    expect((await check()).sessions).toEqual(["s1"]); // current readiness cannot clear an older backend epoch
    await record(false);
    expect((await check()).sessions).toEqual(["s1"]);
    await record(true);
    expect((await check()).sessions).toEqual([]);
    // Directory epochs are local to a canonical identity. A pointer remap can
    // land on another ready directory with the same epoch and must invalidate
    // the previous canonical projection rather than inheriting completeness.
    await db.exec("INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s-remapped',4,true); UPDATE sessions.session_index SET canonical_session_id='s-remapped' WHERE org_id='org-a' AND session_id='s1'");
    expect((await check()).sessions).toEqual(["s-remapped"]);
    expect((await check("s-remapped")).sessions).toEqual(["s-remapped"]);
    await db.exec("UPDATE sessions.session_index SET canonical_session_id=NULL WHERE org_id='org-a' AND session_id='s1'; DELETE FROM sessions.session_directory WHERE session_id='s-remapped'");
    expect((await check()).sessions).toEqual([]);
    expect((await state.partials({ ...tenant, orgId: "org-b" }, { limit: 1000 })).sessions).toEqual(["s1"]);
    await record(true, { ...scope, orgId: "org-b" });
    const visible = await reader(tenant.orgId, tenant.workspaceId, tx => tx.query<{ org_id: string }>("SELECT org_id FROM search_index.projection_state"));
    expect(visible.rows.map(row => row.org_id)).toEqual(["org-a"]);
    expect((await postgresSearchState({ reader, backendId: "postgres-v1", fromMs: 1 }).partials(tenant, { limit: 1000 })).sessions).toEqual(["s1"]);
    await db.exec("INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','s2',NULL,NULL),('org-a','ws-a','codex','s3',NULL,NULL)");
    expect(await check(undefined, 1)).toMatchObject({ overflow: true });
    expect((await check("s1")).sessions).toEqual([]);
    expect((await check("s2")).sessions).toEqual(["s2"]); // missing directory also refuses
    config.fromMs = 99;
    expect(await state.fromMs(new Date())).toBe(0); // capture reviewed config rather than a mutable caller object
    await db.exec("CREATE POLICY unexpected ON search_index.projection_state USING(true)");
    await expect(state.ensureSchema(db)).rejects.toThrow("tenant policy mismatch");
  } finally { await db.close(); }
}, 30_000);

test("ensureSchema grants the read API role only the projection columns its completeness check reads", async () => {
  const db = new PGlite();
  const tenant = { orgId: "org-a", workspaceId: "ws-a" };
  try {
    await db.exec(`CREATE SCHEMA sessions;
      CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,canonical_session_id text,deleted_at timestamptz);
      CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,event_projection_epoch bigint,event_projection_ready boolean);
      ALTER TABLE sessions.session_index ENABLE ROW LEVEL SECURITY;
      ALTER TABLE sessions.session_directory ENABLE ROW LEVEL SECURITY;
      CREATE ROLE bulls96_read_api;
      GRANT USAGE ON SCHEMA sessions TO bulls96_read_api;
      GRANT SELECT ON sessions.session_index, sessions.session_directory TO bulls96_read_api;
      CREATE POLICY t ON sessions.session_index FOR SELECT TO bulls96_read_api USING (org_id = current_setting('bulls96_read.org_id', true));
      CREATE POLICY t ON sessions.session_directory FOR SELECT TO bulls96_read_api USING (org_id = current_setting('bulls96_read.org_id', true));
      INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','s1',NULL,NULL);
      INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s1',3,true);`);
    const reader: TenantRunner = (org, workspace, work) => db.transaction(async tx => {
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [org, workspace]);
      return work(tx);
    });
    const state = postgresSearchState({ reader, backendId: "postgres-v1", fromMs: 0 });
    await state.ensureSchema(db);
    await state.ensureSchema(db); // idempotent
    // Before the fix this read failed with "permission denied for schema search_index".
    expect(await state.partials(tenant, { limit: 10 })).toEqual({ sessions: ["s1"], overflow: false });
    const { rows } = await db.query<{ col: string }>(
      `SELECT a.attname AS col FROM pg_attribute a
        WHERE a.attrelid='search_index.projection_state'::regclass AND a.attnum>0 AND NOT a.attisdropped
          AND has_column_privilege('bulls96_read_api', a.attrelid, a.attnum, 'SELECT') ORDER BY 1`);
    expect(rows.map(r => r.col)).toEqual(["backend_id", "canonical_session_id", "complete", "coverage_from_ms", "org_id", "session_id", "source", "source_epoch", "workspace_id"]);
    const writes = await db.query<{ w: boolean }>(`SELECT has_table_privilege('bulls96_read_api','search_index.projection_state','INSERT, UPDATE, DELETE, TRUNCATE') AS w`);
    expect(writes.rows[0]!.w).toBe(false);
  } finally {
    await db.close();
  }
});
