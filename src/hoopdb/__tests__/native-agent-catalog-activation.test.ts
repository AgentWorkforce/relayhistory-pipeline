import { afterAll, beforeAll, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { nativeAgentCatalogActivation } from "../native-agent-catalog-activation.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import { directoryCatalogFromProjection, writeLegacyDirectoryCatalog } from "../session-directory-catalog.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";

const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const generation = "a".repeat(64);
const opts = { mode: "selfhosted" as const, generation };
const transact = async <T>(work: (tx: SqlExecutor) => Promise<T>) => {
  await db.exec("BEGIN");
  try { const result = await work(db as never); await db.exec("COMMIT"); return result; }
  catch (e) { await db.exec("ROLLBACK"); throw e; }
};
beforeAll(async () => {
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read(`supabase/migrations/${f}`));
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text)");
  await db.query("INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker',$1,'pass')", [generation]);
}, 120_000);
afterAll(async () => { await db.close(); });

async function seed(id: string, workspace = "ws-a", complete = true) {
  await db.query(`INSERT INTO sessions.session_index
    (org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
      storage_provider,bucket,manifest_key,raw_format,trace_id,revision)
    VALUES('org-a',$1,'codex',$2,'m1','u1',now(),now(),'s3','bucket',$2,'jsonl',
      '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19',1)`, [workspace, id]);
  const key = { orgId: "org-a", workspaceId: workspace, harness: "codex", sessionId: id };
  await projectSessionDirectory(db as never, key);
  if (complete) {
    await db.query(`UPDATE sessions.session_directory SET event_projection_version=1,event_projection_ready=true,
      event_projected_at=now(),event_projection_sources='{"synthetic":1}'::jsonb
      WHERE workspace_id=$1 AND session_id=$2`, [workspace, id]);
    const catalog = directoryCatalogFromProjection([{ table: "session_catalog_agents", row: { agent_name: "reviewer" } }]);
    await writeLegacyDirectoryCatalog(db as never, key, catalog);
  }
}

test("native audit covers every tenant, accepts the legacy writer, and plan writes nothing", async () => {
  await seed("current");
  const plan = await nativeAgentCatalogActivation(transact, opts);
  expect(plan).toMatchObject({ scope: "all_native_tenants", rows: 1, eligible: true, activated: false });
  expect(plan.counts.liveRows).toBe(1);
  expect((await db.query<{ completed_at: unknown }>("SELECT completed_at FROM sessions.session_directory_catalog_rollout")).rows[0]!.completed_at).toBeNull();
  // A legacy install does not need Desktop relationship/model-catalog enablement.
  expect((await db.query<{ forget_tracking: boolean }>("SELECT forget_tracking FROM sessions.session_desktop_catalog_control")).rows[0]!.forget_tracking).toBe(false);
  await seed("old", "other-workspace", false);
  const incomplete = await nativeAgentCatalogActivation(transact, opts);
  expect(incomplete.eligible).toBe(false);
  expect(incomplete.counts.unprojected).toBe(1);
  await expect(nativeAgentCatalogActivation(transact, { ...opts, applyFingerprint: incomplete.fingerprint })).rejects.toThrow("projection_incomplete");
});

test("a stale reviewed plan cannot activate; changing an epoch is detected", async () => {
  const plan = await nativeAgentCatalogActivation(transact, opts);
  await db.exec("UPDATE sessions.session_directory SET event_projection_epoch=event_projection_epoch+1 WHERE session_id='current'");
  await expect(nativeAgentCatalogActivation(transact, { ...opts, applyFingerprint: plan.fingerprint })).rejects.toThrow("plan_changed");
});

test("missing companion agents cannot be hidden by a complete timestamp", async () => {
  await db.exec("DELETE FROM sessions.session_directory_agents WHERE session_id='current'");
  const plan = await nativeAgentCatalogActivation(transact, opts);
  expect(plan.counts.missingAgents).toBe(1);
  expect(plan.eligible).toBe(false);
});

test("RLS runtime role cannot present a tenant-filtered audit as global completeness", async () => {
  await db.exec("SET ROLE bulls96_read_api");
  try { await expect(nativeAgentCatalogActivation(transact, opts)).rejects.toThrow("owner_visibility_required"); }
  finally { await db.exec("RESET ROLE"); }
});

test("pending or wrong schema generations refuse, with no completion write", async () => {
  await expect(nativeAgentCatalogActivation(transact, { ...opts, generation: "b".repeat(64) })).rejects.toThrow("schema_validation_required");
  await db.exec("UPDATE sessions.__96bulls_role_validation SET status='pending'");
  await expect(nativeAgentCatalogActivation(transact, opts)).rejects.toThrow("schema_validation_required");
  await db.exec("UPDATE sessions.__96bulls_role_validation SET status='pass'");
  expect((await db.query<{ completed_at: unknown }>("SELECT completed_at FROM sessions.session_directory_catalog_rollout")).rows[0]!.completed_at).toBeNull();
});

test("complete-source repair permits activation and the real tenant-bound agent RPC", async () => {
  for (const [id, workspace] of [["current", "ws-a"], ["old", "other-workspace"]]) {
    await db.query(`UPDATE sessions.session_directory SET event_projection_version=1,event_projection_ready=true,
      event_projected_at=now(),event_projection_sources='{"synthetic":1}'::jsonb WHERE session_id=$1`, [id]);
    await writeLegacyDirectoryCatalog(db as never,
      { orgId: "org-a", workspaceId: workspace!, harness: "codex", sessionId: id! },
      directoryCatalogFromProjection([{ table: "session_catalog_agents", row: { agent_name: "reviewer" } }]));
  }
  const before = (await db.query("SELECT session_id,event_projection_epoch::text FROM sessions.session_directory ORDER BY session_id")).rows;
  const plan = await nativeAgentCatalogActivation(transact, opts);
  expect(plan.eligible).toBe(true);
  expect((await nativeAgentCatalogActivation(transact, { ...opts, applyFingerprint: plan.fingerprint })).activated).toBe(true);
  expect((await db.query("SELECT session_id,event_projection_epoch::text FROM sessions.session_directory ORDER BY session_id")).rows).toEqual(before);
  await db.exec("SET ROLE bulls96_read_api");
  try {
    await db.exec("BEGIN READ ONLY");
    await db.query(TENANT_SETTINGS_SQL, ["org-a", "ws-a"]);
    const listed = await db.query<{ session_id: string }>("SELECT session_id FROM sessions.list_session_directory_for('ws-a',p_agent=>'Reviewer')");
    expect(listed.rows.map((r) => r.session_id)).toEqual(["current"]);
    await expect(db.query("SELECT * FROM sessions.list_session_directory_for('other-workspace',p_agent=>'Reviewer')")).rejects.toMatchObject({ code: "42501" });
  } finally { await db.exec("ROLLBACK; RESET ROLE"); }
});

test("operator CLI refuses non-native targets and unsafe TLS without exposing the URL", () => {
  const cli = join(root, "scripts/native-agent-catalog-activation.ts");
  for (const env of [
    { HOOPDB_DB_MODE: "supabase" },
    { HOOPDB_DB_MODE: "selfhosted", HOOPDB_URL: "postgresql://operator:synthetic-password@db.test/hoopdb?sslmode=require" },
    { HOOPDB_DB_MODE: "selfhosted", HOOPDB_URL: "postgresql://operator:synthetic-password@db.test/hoopdb?sslmode=verify-full&sslmode=disable" },
    { HOOPDB_DB_MODE: "selfhosted", HOOPDB_URL: "postgresql://operator:synthetic-password@db.test/hoopdb?sslmode=verify-full", NODE_TLS_REJECT_UNAUTHORIZED: "0" },
  ]) {
    const child = Bun.spawnSync([process.execPath, cli, "--plan"], { env });
    expect(child.exitCode).toBe(64);
    expect(child.stderr.toString()).not.toContain("synthetic-password");
    expect(child.stdout.toString()).toBe("");
  }
});
