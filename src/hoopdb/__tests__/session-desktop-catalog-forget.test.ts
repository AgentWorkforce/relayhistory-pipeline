/**
 * Migration 20261105031000 (desktop catalog forget tracking, writer epoch, bounded
 * reprojection) on PGlite with the actual SQL: dormant by default, owner-only switch, forgets
 * and restores re-queue exactly the affected live parents, the read RPC refuses while any is
 * pending and on >500 parents/children, the desktop writer refuses a stale epoch, the repair
 * path resumes across pages, and the rollback restores the 20261105022000 RPC.
 */
import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  directoryCatalogFromProjection,
  writeDirectoryCatalog,
  writeLegacyDirectoryCatalog,
  type DirectoryCatalog,
} from "../session-directory-catalog.ts";
import { reprojectPendingDesktopCatalogParents } from "../session-desktop-catalog-reproject.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../.."),
  read = (p: string) => readFileSync(join(root, p), "utf8"),
  opened: PGlite[] = [];
const MIGRATION = "supabase/migrations/20261105031000_desktop_catalog_forget_reproject.sql";
const ROLLBACK = "supabase/rollbacks/20261105031000_desktop_catalog_forget_reproject.down.sql";
afterEach(async () => {
  for (const db of opened.splice(0)) await db.close();
});
async function database(tracking = true) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  opened.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read("supabase/migrations/" + file));
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://issuer.test','session-directory-search')",
  );
  if (tracking) await setTracking(db, true);
  return db;
}
const setTracking = async (db: PGlite, on: boolean) =>
  (await db.query<{ v: boolean }>("SELECT sessions.set_session_desktop_catalog_forget_tracking($1) v", [on])).rows[0]!.v;
async function seed(db: PGlite, session: string, workspace = "ws-a", org = "org-a") {
  await db.query(
    `INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,event_projection_version,event_projection_ready,event_projection_epoch,event_projected_at,catalog_read_version,catalog_read_epoch,catalog_read_projected_at,catalog_projected_at,catalog_models_complete,catalog_relationships_complete,catalog_models,event_models,event_projection_sources)
 VALUES($1,$2,'codex',$3,now(),now(),1,true,1,now(),1,1,now(),now(),true,true,ARRAY['m'],ARRAY['m'],'{"fixture":1}'::jsonb)`,
    [org, workspace, session],
  );
}
async function edge(db: PGlite, parent: string, child: string, workspace = "ws-a", uid = parent + "->" + child) {
  await db.query(
    "INSERT INTO sessions.session_directory_relationships(org_id,workspace_id,harness,parent_session_id,relationship_uid,child_session_id) VALUES('org-a',$1,'codex',$2,$3,$4)",
    [workspace, parent, uid, child],
  );
}
async function catalog(db: PGlite, session: string, workspace = "ws-a") {
  const iat = Math.floor(Date.now() / 1000);
  await db.query("SELECT set_config('request.jwt.claims',$1,false)", [
    JSON.stringify({
      role: "session_directory_search_worker",
      iss: "https://issuer.test",
      aud: "session-directory-search",
      iat,
      exp: iat + 60,
      app_metadata: { org_id: "org-a", workspace_ids: [workspace] },
    }),
  ]);
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    return (
      await db.query<{ body: any }>("SELECT sessions.session_directory_desktop_catalog($1,'codex',$2) body", [workspace, session])
    ).rows[0]!.body;
  } finally {
    await db.exec("RESET ROLE");
  }
}
const queue = async (db: PGlite) =>
  (
    await db.query<{ k: string }>(
      "SELECT workspace_id||'/'||session_id||':'||state k FROM sessions.session_desktop_catalog_reproject ORDER BY workspace_id,session_id",
    )
  ).rows.map((r) => r.k);
const epoch = async (db: PGlite, session: string, workspace = "ws-a") =>
  (
    await db.query<{ e: string }>(
      "SELECT event_projection_epoch::text e FROM sessions.session_directory WHERE workspace_id=$1 AND session_id=$2",
      [workspace, session],
    )
  ).rows[0]!.e;
const key = (sessionId: string, workspaceId = "ws-a") => ({ orgId: "org-a", workspaceId, harness: "codex", sessionId });
const source = (...children: string[]): DirectoryCatalog =>
  directoryCatalogFromProjection([
    { table: "session_catalog", row: { models: ["m"] } },
    ...children.map((child) => ({
      table: "session_relationships",
      row: { relationship_uid: "r-" + child, child_session_id: child, child_model: null, child_agent_name: null, child_agent_type: null },
    })),
  ]);
const project = async (db: PGlite, session: string, ...children: string[]) =>
  writeDirectoryCatalog(db as never, key(session), source(...children), { expectedEpoch: await epoch(db, session) });

test("dormant on apply: control off, no derived state, every trigger a no-op, RPC and repair page refuse", async () => {
  const db = await database(false);
  expect((await db.query("SELECT singleton,forget_tracking FROM sessions.session_desktop_catalog_control")).rows).toEqual([
    { singleton: true, forget_tracking: false },
  ]);
  for (const s of ["p", "c", "q"]) await seed(db, s);
  await edge(db, "p", "c");
  expect((await db.query("SELECT bool_or(catalog_forgotten_child_edges) f FROM sessions.session_directory")).rows).toEqual([{ f: false }]);
  // Forget, restore, purge and a desktop write all leave the queue empty while tracking is off.
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='c'");
  await project(db, "p", "c");
  await db.exec("UPDATE sessions.session_directory SET deleted_at=NULL WHERE session_id='c'");
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='c'");
  await db.exec("DELETE FROM sessions.session_directory WHERE session_id='c'");
  expect(await queue(db)).toEqual([]);
  await expect(catalog(db, "q")).rejects.toMatchObject({ code: "0A000", message: "desktop catalog: forget_tracking_not_enabled" });
  await expect(db.query("SELECT * FROM sessions.session_desktop_catalog_reproject_page(NULL,NULL,NULL,NULL,10)")).rejects.toMatchObject({
    code: "55000",
  });
});

test("the switch is owner-only, validated, idempotent, and enabling invalidates readiness and empties the queue", async () => {
  const db = await database(false);
  const fns = [
    "sessions.set_session_desktop_catalog_forget_tracking(boolean)",
    "sessions.session_desktop_catalog_mark_parents()",
    "sessions.session_desktop_catalog_requeue_watchers()",
    "sessions.session_desktop_catalog_projected()",
    "sessions.session_desktop_catalog_reproject_page(text,text,text,text,integer)",
  ];
  for (const role of ["session_directory_search_worker", "service_role", "authenticated", "anon"])
    for (const fn of fns)
      expect(
        (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1,$2,'EXECUTE') ok", [role, fn])).rows[0]!.ok,
        role + " " + fn,
      ).toBe(false);
  for (const table of ["sessions.session_desktop_catalog_control", "sessions.session_desktop_catalog_reproject"])
    for (const role of ["session_directory_search_worker", "service_role", "authenticated", "anon"])
      expect(
        (await db.query<{ ok: boolean }>("SELECT has_table_privilege($1,$2,'SELECT,INSERT,UPDATE,DELETE') ok", [role, table])).rows[0]!.ok,
      ).toBe(false);
  // The read RPC keeps exactly its 20261105022000 grant.
  expect(
    (await db.query<{ ok: boolean }>(
      "SELECT has_function_privilege('session_directory_search_worker','sessions.session_directory_desktop_catalog(text,text,text)','EXECUTE') ok",
    )).rows[0]!.ok,
  ).toBe(true);
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    await expect(db.query("SELECT sessions.set_session_desktop_catalog_forget_tracking(true)")).rejects.toMatchObject({ code: "42501" });
  } finally {
    await db.exec("RESET ROLE");
  }
  await expect(db.query("SELECT sessions.set_session_desktop_catalog_forget_tracking(NULL)")).rejects.toMatchObject({ code: "22023" });
  await seed(db, "ready");
  await db.exec("INSERT INTO sessions.session_desktop_catalog_reproject(org_id,workspace_id,harness,session_id,state) VALUES('org-a','ws-a','codex','stale','pending')");
  expect(await setTracking(db, true)).toBe(true);
  expect((await db.query("SELECT catalog_read_version FROM sessions.session_directory WHERE session_id='ready'")).rows).toEqual([
    { catalog_read_version: 0 },
  ]);
  expect(await queue(db)).toEqual([]);
  await expect(catalog(db, "ready")).rejects.toMatchObject({ message: "desktop catalog: relationship_metadata_not_ready" });
  expect(await setTracking(db, true)).toBe(true);
  expect(await setTracking(db, false)).toBe(false);
  await expect(catalog(db, "ready")).rejects.toMatchObject({ message: "desktop catalog: forget_tracking_not_enabled" });
});

test("a forgotten child marks exactly its live parents pending (tombstone and delete); the workspace refuses until they are reprojected", async () => {
  const db = await database();
  for (const s of ["p1", "p2", "p3", "c", "d", "q", "self"]) await seed(db, s);
  await seed(db, "p1", "ws-b");
  await seed(db, "c", "ws-b");
  await edge(db, "p1", "c");
  await edge(db, "p2", "c");
  await edge(db, "p2", "d");
  await edge(db, "q", "d");
  await edge(db, "self", "self");
  await edge(db, "p1", "c", "ws-b");
  // A tombstoned parent's stale edge (written behind the triggers) is not queued.
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='p3'");
  await edge(db, "p3", "c");
  expect((await catalog(db, "q")).exists).toBe(true);
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE workspace_id='ws-a' AND session_id='c'");
  expect(await queue(db)).toEqual(["ws-a/p1:pending", "ws-a/p2:pending"]);
  // 20261101050000 still removes every edge naming the child; the other workspace is untouched.
  expect((await db.query("SELECT workspace_id,parent_session_id FROM sessions.session_directory_relationships WHERE child_session_id='c'")).rows).toEqual([
    { workspace_id: "ws-b", parent_session_id: "p1" },
  ]);
  await expect(catalog(db, "q")).rejects.toMatchObject({ code: "0A000", message: "desktop catalog: relationship_metadata_not_ready" });
  expect((await catalog(db, "c", "ws-b")).exists).toBe(true);
  // Hard delete marks too; a self edge never queues the forgotten session itself.
  await db.exec("DELETE FROM sessions.session_directory WHERE session_id IN ('d','self')");
  expect(await queue(db)).toEqual(["ws-a/p1:pending", "ws-a/p2:pending", "ws-a/q:pending"]);
  // A forgotten parent leaves the queue with its own tombstone.
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='q'");
  expect(await queue(db)).toEqual(["ws-a/p1:pending", "ws-a/p2:pending"]);
  // Reprojection (the parent's source still names the forgotten child) settles each parent.
  expect(await project(db, "p1", "c")).toEqual({ written: true });
  expect(await project(db, "p2", "c", "d")).toEqual({ written: true });
  expect(await queue(db)).toEqual(["ws-a/p1:watch", "ws-a/p2:watch"]);
  expect((await db.query("SELECT count(*)::int n FROM sessions.session_directory_relationships WHERE workspace_id='ws-a'")).rows).toEqual([{ n: 1 }]);
  const p2 = await catalog(db, "p2");
  expect(p2.children.map((e: any) => e.childSessionId)).toEqual(["d"]);
});

test("one statement forgetting a parent and its child queues neither (only live parents are marked)", async () => {
  const db = await database();
  for (const s of ["p", "c", "o"]) await seed(db, s);
  await edge(db, "p", "c");
  await edge(db, "o", "c");
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id IN ('p','c')");
  expect(await queue(db)).toEqual(["ws-a/o:pending"]);
});

test("tombstone, parent reprojection, then restore: the parent is re-queued and the edge returns (#182 adversarial Medium)", async () => {
  const db = await database();
  for (const s of ["p", "c", "other"]) await seed(db, s);
  expect(await project(db, "p", "c")).toEqual({ written: true });
  expect((await catalog(db, "c")).parents.map((e: any) => e.parentSessionId)).toEqual(["p"]);
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='c'");
  expect(await queue(db)).toEqual(["ws-a/p:pending"]);
  await expect(catalog(db, "other")).rejects.toMatchObject({ code: "0A000" });
  // Repair while the child is still a tombstone: the edge is dropped, the parent watches.
  expect(await project(db, "p", "c")).toEqual({ written: true });
  expect((await db.query("SELECT catalog_forgotten_child_edges f FROM sessions.session_directory WHERE session_id='p'")).rows).toEqual([{ f: true }]);
  expect(await queue(db)).toEqual(["ws-a/p:watch"]);
  expect((await catalog(db, "p")).children).toEqual([]);
  // The child comes back: the watching parent is pending again and the workspace refuses.
  await db.exec("UPDATE sessions.session_directory SET deleted_at=NULL WHERE session_id='c'");
  expect(await queue(db)).toEqual(["ws-a/p:pending"]);
  // (The restored child's event projection is re-run too; mark it ready to isolate this gate.)
  await db.exec("UPDATE sessions.session_directory SET event_projection_version=1,event_projected_at=now(),event_projection_sources='{\"fixture\":1}'::jsonb,event_projection_ready=true WHERE session_id='c'");
  expect(await project(db, "c")).toEqual({ written: true });
  await expect(catalog(db, "c")).rejects.toMatchObject({ code: "0A000", message: "desktop catalog: relationship_metadata_not_ready" });
  // Before 20261105031000 this read exists:true, parents:[] as ready. Now the repair restores the edge.
  expect(await project(db, "p", "c")).toEqual({ written: true });
  expect(await queue(db)).toEqual([]);
  expect((await catalog(db, "c")).parents.map((e: any) => e.parentSessionId)).toEqual(["p"]);
  expect((await catalog(db, "p")).children.map((e: any) => e.childSessionId)).toEqual(["c"]);
});

test("purging a tombstone re-queues watchers; a restore elsewhere in the workspace touches only watch rows of its harness", async () => {
  const db = await database();
  for (const s of ["p", "c", "x"]) await seed(db, s);
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id IN ('c','x')");
  expect(await project(db, "p", "c")).toEqual({ written: true });
  expect(await queue(db)).toEqual(["ws-a/p:watch"]);
  await db.exec("DELETE FROM sessions.session_directory WHERE session_id='c'");
  expect(await queue(db)).toEqual(["ws-a/p:pending"]);
  // Reprojected with the child row gone, the edge is kept (an absent child is not a tombstone).
  expect(await project(db, "p", "c")).toEqual({ written: true });
  expect(await queue(db)).toEqual([]);
  await db.exec("UPDATE sessions.session_directory SET deleted_at=NULL WHERE session_id='x'");
  expect(await queue(db)).toEqual([]);
});

test("writer epoch: a stale writer never overwrites a newer projection; a malformed epoch throws", async () => {
  const db = await database();
  await seed(db, "p");
  await seed(db, "c");
  expect(await writeDirectoryCatalog(db as never, key("p"), source("c"), { expectedEpoch: "1" })).toEqual({ written: true });
  await db.exec("UPDATE sessions.session_directory SET event_projection_epoch=2 WHERE session_id='p'");
  const before = (await db.query("SELECT catalog_read_projected_at,catalog_read_epoch FROM sessions.session_directory WHERE session_id='p'")).rows;
  expect(await writeDirectoryCatalog(db as never, key("p"), source(), { expectedEpoch: "1" })).toEqual({ written: false, staleEpoch: true });
  expect((await db.query("SELECT catalog_read_projected_at,catalog_read_epoch FROM sessions.session_directory WHERE session_id='p'")).rows).toEqual(before);
  expect((await db.query("SELECT child_session_id FROM sessions.session_directory_relationships")).rows).toEqual([{ child_session_id: "c" }]);
  expect(await writeDirectoryCatalog(db as never, key("p"), source(), { expectedEpoch: "2" })).toEqual({ written: true });
  expect((await db.query("SELECT count(*)::int n FROM sessions.session_directory_relationships")).rows).toEqual([{ n: 0 }]);
  // Missing live row: written false, not stale.
  expect(await writeDirectoryCatalog(db as never, key("missing"), source(), { expectedEpoch: "1" })).toEqual({ written: false });
  for (const bad of ["", "01", "-1", "1.0", "x", "99999999999999999999"])
    await expect(writeDirectoryCatalog(db as never, key("p"), source(), { expectedEpoch: bad })).rejects.toThrow("directory_catalog_epoch_invalid");
  await expect(writeDirectoryCatalog(db as never, key("p"), source(), undefined as never)).rejects.toThrow("directory_catalog_epoch_invalid");
});

test(">500 incoming parents or outgoing children refuse explicitly; exactly 500 is served untruncated", async () => {
  const db = await database();
  await seed(db, "s");
  for (let i = 1; i <= 501; i++) await seed(db, "parent-" + i);
  await db.exec(
    "INSERT INTO sessions.session_directory_relationships(org_id,workspace_id,harness,parent_session_id,relationship_uid,child_session_id) SELECT 'org-a','ws-a','codex','parent-'||i,'u','s' FROM generate_series(1,500) i",
  );
  const ok = await catalog(db, "s");
  expect([ok.parents.length, ok.truncated]).toEqual([500, false]);
  await edge(db, "parent-501", "s");
  await expect(catalog(db, "s")).rejects.toMatchObject({ code: "0A000", message: "desktop catalog: relationship_fanout_exceeded" });
  await db.exec("DELETE FROM sessions.session_directory_relationships");
  await db.exec(
    "INSERT INTO sessions.session_directory_relationships(org_id,workspace_id,harness,parent_session_id,relationship_uid,child_session_id) SELECT 'org-a','ws-a','codex','s','u'||i,'kid-'||i FROM generate_series(1,501) i",
  );
  await expect(catalog(db, "s")).rejects.toMatchObject({ code: "0A000", message: "desktop catalog: relationship_fanout_exceeded" });
});

test("reprojection path: owner-only page, validated cursor, resumes across pages, failures stay pending", async () => {
  const db = await database();
  const parents = ["a1", "a2", "a3", "a4", "a5"];
  for (const s of [...parents, "kid"]) await seed(db, s);
  await seed(db, "b1", "ws-b");
  await seed(db, "kid", "ws-b");
  for (const p of parents) await edge(db, p, "kid");
  await edge(db, "b1", "kid", "ws-b");
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='kid'");
  expect(await queue(db)).toEqual([...parents.map((p) => "ws-a/" + p + ":pending"), "ws-b/b1:pending"]);
  for (const bad of ["NULL,NULL,NULL,NULL,0", "NULL,NULL,NULL,NULL,501", "'org-a',NULL,NULL,NULL,10"])
    await expect(db.query(`SELECT * FROM sessions.session_desktop_catalog_reproject_page(${bad})`)).rejects.toMatchObject({ code: "22023" });
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    await expect(db.query("SELECT * FROM sessions.session_desktop_catalog_reproject_page(NULL,NULL,NULL,NULL,10)")).rejects.toMatchObject({
      code: "42501",
    });
  } finally {
    await db.exec("RESET ROLE");
  }
  const seen: string[] = [];
  const reproject = async (p: { workspaceId: string; sessionId: string; epoch: string }) => {
    seen.push(p.workspaceId + "/" + p.sessionId);
    if (p.sessionId === "a2") throw new Error("source unavailable");
    const r = await writeDirectoryCatalog(db as never, key(p.sessionId, p.workspaceId), source("kid"), { expectedEpoch: p.epoch });
    expect(r).toEqual({ written: true });
  };
  // One page of two, then resume from the returned cursor.
  const first = await reprojectPendingDesktopCatalogParents(db as never, reproject, { pageSize: 2, maxPages: 1 });
  expect(first).toMatchObject({ pages: 1, processed: 1, failed: 1, done: false, cursor: { workspaceId: "ws-a", sessionId: "a2" } });
  const errors: string[] = [];
  const rest = await reprojectPendingDesktopCatalogParents(db as never, reproject, {
    cursor: first.cursor,
    pageSize: 2,
    onError: (p) => errors.push(p.sessionId),
  });
  expect(rest).toMatchObject({ processed: 4, failed: 0, done: true, cursor: null });
  expect(seen).toEqual(["ws-a/a1", "ws-a/a2", "ws-a/a3", "ws-a/a4", "ws-a/a5", "ws-b/b1"]);
  expect(await queue(db)).toEqual(["ws-a/a1:watch", "ws-a/a2:pending", "ws-a/a3:watch", "ws-a/a4:watch", "ws-a/a5:watch", "ws-b/b1:watch"]);
  // A rerun from the start retries only the failed parent.
  seen.length = 0;
  await expect(reprojectPendingDesktopCatalogParents(db as never, reproject)).resolves.toMatchObject({ failed: 1, done: true });
  expect(seen).toEqual(["ws-a/a2"]);
  await expect(reprojectPendingDesktopCatalogParents(db as never, reproject, { pageSize: 501 })).rejects.toThrow("page_size_invalid");
  // A stale epoch is not written: the reprojector's { written: false } counts as a failure.
  await db.exec("UPDATE sessions.session_directory SET event_projection_epoch=event_projection_epoch+1 WHERE session_id='a2'");
  const stale = await reprojectPendingDesktopCatalogParents(db as never, async (p) =>
    writeDirectoryCatalog(db as never, key(p.sessionId, p.workspaceId), source("kid"), { expectedEpoch: String(Number(p.epoch) - 1) }),
  );
  expect(stale).toMatchObject({ processed: 0, failed: 1, done: true });
  expect(await queue(db)).toContain("ws-a/a2:pending");
});

test("#182 contextual finding 4: models that are not an array or hold non-strings are never complete", () => {
  for (const [models, reason] of [
    [null, "models_not_array"],
    [undefined, "models_not_array"],
    ['["a"]', "models_not_array"],
    [{ 0: "a" }, "models_not_array"],
    [["a", 1], "models_non_string"],
    [["a", null], "models_non_string"],
  ] as const) {
    const c = directoryCatalogFromProjection([{ table: "session_catalog", row: { models } }]);
    expect([c.modelsComplete, c.incompleteReasons], JSON.stringify(models)).toEqual([false, [reason]]);
  }
  const ok = directoryCatalogFromProjection([{ table: "session_catalog", row: { models: ["a", ""] } }]);
  expect([ok.modelsComplete, ok.incompleteReasons, ok.models]).toEqual([true, undefined, ["a"]]);
  expect(directoryCatalogFromProjection([]).incompleteReasons).toBeUndefined();
  const edges = directoryCatalogFromProjection([
    { table: "session_relationships", row: { relationship_uid: "", child_session_id: "x" } },
    { table: "session_relationships", row: { relationship_uid: "r", child_session_id: "x" } },
  ]);
  expect([edges.relationshipsComplete, edges.incompleteReasons]).toEqual([false, ["relationship_metadata_missing", "relationship_uid_invalid"]]);
});

test("flag off: the legacy writer touches neither the new column nor the queue, even with tracking on", async () => {
  const db = await database();
  await seed(db, "p");
  await seed(db, "c");
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='c'");
  expect(await writeLegacyDirectoryCatalog(db as never, key("p"), source("c"))).toEqual({ written: true });
  expect((await db.query("SELECT catalog_forgotten_child_edges f FROM sessions.session_directory WHERE session_id='p'")).rows).toEqual([{ f: false }]);
  expect(await queue(db)).toEqual([]);
});

test("rollback restores the 20261105022000 RPC verbatim with its ACL, drops only 20261105031000 objects, and reapplies", async () => {
  const body = (sql: string) => sql.match(/FUNCTION sessions\.session_directory_desktop_catalog\([\s\S]*?\$catalog\$;/)![0];
  expect(body(read(ROLLBACK))).toBe(body(read("supabase/migrations/20261105022000_session_desktop_catalog.sql")));
  const db = await database();
  await seed(db, "s");
  const prosrc = async () =>
    (await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.session_directory_desktop_catalog(text,text,text)'::regprocedure")).rows[0]!.s;
  expect(await prosrc()).toContain("forget_tracking_not_enabled");
  await db.exec(read(ROLLBACK));
  expect(await prosrc()).not.toContain("forget_tracking");
  expect(await prosrc()).toContain("count(*)>500");
  const left = await db.query<{ n: number }>(
    `SELECT (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname LIKE '%desktop_catalog%' AND p.proname<>'session_directory_desktop_catalog')
      +(SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'session_directory_catalog_%')
      +(SELECT count(*) FROM pg_class WHERE relname LIKE 'session_desktop_catalog%')
      +(SELECT count(*) FROM pg_attribute WHERE attrelid='sessions.session_directory'::regclass AND attname='catalog_forgotten_child_edges' AND NOT attisdropped) AS n`,
  );
  expect(left.rows[0]!.n).toBe(0);
  expect((await catalog(db, "s")).exists).toBe(true);
  expect(
    (await db.query<{ ok: boolean }>(
      "SELECT has_function_privilege('session_directory_search_worker','sessions.session_directory_desktop_catalog(text,text,text)','EXECUTE') ok",
    )).rows[0]!.ok,
  ).toBe(true);
  await db.exec(read(MIGRATION));
  expect(await prosrc()).toContain("forget_tracking_not_enabled");
  await expect(catalog(db, "s")).rejects.toMatchObject({ message: "desktop catalog: forget_tracking_not_enabled" });
});

test("trigger names order the parent marking before 20261101050000 deletes the edges", async () => {
  const db = await database();
  const names = (
    await db.query<{ n: string }>(
      "SELECT tgname n FROM pg_trigger WHERE tgrelid='sessions.session_directory'::regclass AND NOT tgisinternal AND tgname IN('session_directory_catalog_mark_parents_on_tombstone','session_directory_forget_catalog_on_tombstone','session_directory_catalog_mark_parents_on_delete','session_directory_forget_catalog_on_delete') ORDER BY tgname COLLATE \"C\"",
    )
  ).rows.map((r) => r.n);
  expect(names).toEqual([
    "session_directory_catalog_mark_parents_on_delete",
    "session_directory_catalog_mark_parents_on_tombstone",
    "session_directory_forget_catalog_on_delete",
    "session_directory_forget_catalog_on_tombstone",
  ]);
});
