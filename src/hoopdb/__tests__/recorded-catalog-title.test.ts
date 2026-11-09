/** Real 044100 storage/invalidation and runtime writer/projector contracts. */
import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { database, hoopDb, KEY, pointer, read, root } from "../../session-brief/__tests__/harness.ts";
import { recordBrief, type BriefText } from "../../session-brief/store.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import { directoryCatalogFromProjection, writeDirectoryCatalog, writeLegacyDirectoryCatalog } from "../session-directory-catalog.ts";

const opened: PGlite[] = [];
afterEach(async () => { for (const db of opened.splice(0)) await db.close(); });
const values = Object.values(KEY);
async function setup() {
  const db = await database(); opened.push(db);
  await pointer(db, { title: null });
  await db.query(`UPDATE sessions.session_directory SET event_projection_ready=true,
    event_projection_version=1,event_projected_at=now(),event_projection_sources='{"m-1":1}'::jsonb`);
  return db;
}
async function before100() {
  const db=new PGlite({extensions:{btree_gin,pg_trgm}}); opened.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for(const file of SESSION_BOOTSTRAP_FILES) await db.exec(read(file));
  for(const file of readdirSync(join(root,"supabase/migrations")).sort())
    if(file<"20261105044100_session_brief_summary_read.sql") await db.exec(read("supabase/migrations/"+file));
  for(const file of readdirSync(join(root,"src/hoopdb/migrations")).filter(f=>f.endsWith(".sql")).sort())
    await db.exec(read("src/hoopdb/migrations/"+file));
  return db;
}
const input = (title: unknown) => directoryCatalogFromProjection([{ table: "session_catalog", row: { title, models: [], project_key: "/Users/example", project_key_method: "path" } }]);
async function epoch(db: PGlite) {
  return (await db.query<{ epoch: string }>("SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4", values)).rows[0]!.epoch;
}
async function state(db: PGlite) {
  return (await db.query<{ title: string | null; recorded_catalog_title: string | null; search_text: string; project_id: string | null }>("SELECT title,recorded_catalog_title,search_text,project_id FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4", values)).rows[0]!;
}
const text: BriefText = { title: "Generated model title", description: "Generated description.", state: "completed", scrubRuleset: "relayhistory-scrub", scrubVersion: 1, fingerprint: "f".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "fixture-model", messageCount: 3, shortenedMessages: 0, inputTokens: 10, outputTokens: 5, durationMs: 20, generatedAt: new Date("2026-11-01T00:00:00Z") };

for (const [name, writer] of [["current", writeDirectoryCatalog], ["legacy", writeLegacyDirectoryCatalog]] as const) {
  test(`${name}: complete recorded title fills an untitled list row without creating a project ID`, async () => {
    const db = await setup(); const opts = { expectedEpoch: await epoch(db) };
    expect(await writer(db as never, KEY, input("  Recorded session title  "), opts)).toMatchObject({ written: true });
    expect(await state(db)).toMatchObject({ title: "Recorded session title", recorded_catalog_title: "Recorded session title", project_id: null });
    expect((await state(db)).search_text).toContain("Recorded session title");
    await projectSessionDirectory(hoopDb(db), KEY);
    expect((await state(db)).title).toBe("Recorded session title");
    // Replays do not erase recorded metadata or replace a genuine model title.
    expect(await recordBrief(hoopDb(db), KEY, { "m-1": 1 }, { status: "ready", text })).toEqual({ written: true });
    await writer(db as never, KEY, input(undefined), opts);
    expect((await state(db)).recorded_catalog_title).toBeNull();
    expect((await state(db)).title).toBe(text.title);
    await writer(db as never, KEY, input("Changed recorded title"), opts);
    expect((await state(db)).recorded_catalog_title).toBe("Changed recorded title");
    expect((await state(db)).title).toBe(text.title);
    await projectSessionDirectory(hoopDb(db), KEY);
    expect((await state(db)).title).toBe(text.title);
    // Quarantine must not preserve the old generated title through coalesce(d.title,...).
    await recordBrief(hoopDb(db), KEY, { "m-1": 1 }, { status: "quarantined", error: "fixture" });
    expect((await state(db)).title).toBe("Changed recorded title");
    expect((await state(db)).search_text).toContain("Changed recorded title");
    expect((await state(db)).search_text).not.toContain(text.title);
  });

  test(`${name}: source epoch, tenant and erased-row fences admit no recorded title`, async () => {
    const db = await setup(), opts = { expectedEpoch: await epoch(db) };
    expect(await writer(db as never, { ...KEY, workspaceId: "foreign" }, input("Foreign title"), opts)).toMatchObject({ written: false });
    expect(await writer(db as never, KEY, input("Stale title"), { expectedEpoch: String(Number(opts.expectedEpoch) + 1) })).toMatchObject({ written: false });
    await db.query("UPDATE sessions.session_directory SET event_projection_ready=false");
    expect(await writer(db as never, KEY, input("Incomplete title"), opts)).toMatchObject({ written: false });
    await db.query("UPDATE sessions.session_directory SET deleted_at=now()");
    expect(await writer(db as never, KEY, input("Erased title"), opts)).toMatchObject({ written: false });
    expect((await state(db)).recorded_catalog_title).toBeNull();
  });
}

test("pointer title outranks recorded metadata, and source invalidation/tombstones remove the fallback", async () => {
  const db = await setup();
  await writeDirectoryCatalog(db as never, KEY, input("Recorded title"), { expectedEpoch: await epoch(db) });
  await db.query("UPDATE sessions.session_index SET title='Pointer title' WHERE session_id=$1", [KEY.sessionId]);
  await projectSessionDirectory(hoopDb(db), KEY);
  expect((await state(db)).title).toBe("Pointer title");
  await db.query("UPDATE sessions.session_directory SET event_projection_ready=false");
  expect((await state(db)).recorded_catalog_title).toBeNull();
  expect((await state(db)).search_text).toBe("");
  await db.query("UPDATE sessions.session_directory SET deleted_at=now()");
  expect((await state(db)).recorded_catalog_title).toBeNull();
});

test("invalid recorded titles never reach the column, while legitimate redacted metadata is preserved", async () => {
  const db = await setup(), opts = { expectedEpoch: await epoch(db) };
  for (const title of [null, 12, {}, "", "   ", "x".repeat(91), "🙂".repeat(46), "line\nbreak", "edge\n", "bad\u0000title", "bad\u007ftitle"]) {
    expect(input(title).recordedTitle).toBeNull();
    await writeDirectoryCatalog(db as never, KEY, input(title), opts);
    expect((await state(db)).recorded_catalog_title).toBeNull();
  }
  await writeDirectoryCatalog(db as never, KEY, input("[REDACTED]"), opts);
  expect((await state(db)).recorded_catalog_title).toBe("[REDACTED]");
  // Source validation is re-applied at the write boundary, even for a manually built DTO.
  await writeDirectoryCatalog(db as never, KEY, { ...input(null), recordedTitle: "x".repeat(91) }, opts);
  expect((await state(db)).recorded_catalog_title).toBeNull();
  expect((await state(db)).title).toBeNull();
});

test("recorded writes require the schema and legacy epoch contract instead of silently losing metadata", async () => {
  const db = await before100();
  await pointer(db, { title: null });
  await db.query(`UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=now(),event_projection_sources='{"m-1":1}'::jsonb`);
  await expect(writeDirectoryCatalog(db as never, KEY, input("Recorded title"), { expectedEpoch: await epoch(db) })).rejects.toMatchObject({ code: "42703" });
  await expect(writeLegacyDirectoryCatalog(db as never, KEY, input("Recorded title"))).rejects.toThrow("directory_recorded_title_not_migrated");
  const current=await setup();
  await expect(writeLegacyDirectoryCatalog(current as never, KEY, input("Recorded title"))).rejects.toThrow("directory_catalog_epoch_invalid");
});


test("recorded replay refreshes recorded fallback immediately, but a real pointer title still wins", async () => {
  const db = await setup(), opts = { expectedEpoch: await epoch(db) };
  await writeDirectoryCatalog(db as never, KEY, input("Recorded first"), opts);
  await writeDirectoryCatalog(db as never, KEY, input("Recorded next"), opts);
  expect((await state(db)).title).toBe("Recorded next");
  expect((await state(db)).search_text).not.toContain("Recorded first");
  await db.query("UPDATE sessions.session_index SET title='Actual pointer title' WHERE session_id=$1", [KEY.sessionId]);
  await db.query(`UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=now(),event_projection_sources='{"m-1":1}'::jsonb`);
  await writeDirectoryCatalog(db as never, KEY, input("Recorded next"), { expectedEpoch: await epoch(db) });
  expect((await state(db)).title).toBe("Actual pointer title");
  expect((await state(db)).recorded_catalog_title).toBe("Recorded next");
});

test("physical-to-canonical alias is explicitly bound, and another physical source cannot supply its title", async () => {
  const db = await setup();
  await db.query("UPDATE sessions.session_index SET canonical_session_id='logical-session' WHERE session_id=$1", [KEY.sessionId]);
  await db.query("DELETE FROM sessions.session_directory WHERE session_id=$1", [KEY.sessionId]);
  await projectSessionDirectory(hoopDb(db), KEY);
  const logical = { ...KEY, sessionId: "logical-session" };
  await db.query(`UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=now(),event_projection_sources='{"m-1":1}'::jsonb`);
  const e = (await db.query<{epoch:string}>("SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory")).rows[0]!.epoch;
  expect(await writeDirectoryCatalog(db as never, logical, input("Alias recorded"), {expectedEpoch:e})).toEqual({written:false});
  expect(await writeDirectoryCatalog(db as never, logical, input("Alias recorded"), {expectedEpoch:e,sourceSessionId:KEY.sessionId})).toEqual({written:true});
  expect((await db.query<{title:string}>("SELECT title FROM sessions.session_directory WHERE session_id='logical-session'")).rows[0]!.title).toBe("Alias recorded");
  expect(await writeDirectoryCatalog(db as never, logical, input("Foreign recorded"), {expectedEpoch:e,sourceSessionId:"foreign-physical"})).toEqual({written:false});
});


for (const priority of ["model", "pointer"] as const) {
  test(`${priority} title equal to recorded text is conservatively invalidated, then recomputed by authority`, async () => {
    const db = await setup(), opts = { expectedEpoch: await epoch(db) };
    await writeDirectoryCatalog(db as never, KEY, input("Same recorded title"), opts);
    if (priority === "model") {
      await recordBrief(hoopDb(db), KEY, {"m-1":1}, {status:"ready",text:{...text,title:"Same recorded title"}});
      await writeDirectoryCatalog(db as never, KEY, input("Different recorded title"), opts);
      expect((await state(db)).title).toBe("Same recorded title");
      await writeDirectoryCatalog(db as never, KEY, input("Same recorded title"), opts);
    } else {
      // The pointer title change invalidates the projection, so publish a fresh complete catalog.
      await db.query("UPDATE sessions.session_index SET title='Same recorded title' WHERE session_id=$1", [KEY.sessionId]);
      await db.query(`UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=now(),event_projection_sources='{"m-1":1}'::jsonb`);
      await writeDirectoryCatalog(db as never, KEY, input("Same recorded title"), {expectedEpoch:await epoch(db)});
    }
    // Even an epoch transition incorrectly left ready may not carry a recorded title forward.
    await db.query("UPDATE sessions.session_directory SET event_projection_epoch=event_projection_epoch+1,event_projection_ready=true");
    expect((await state(db)).recorded_catalog_title).toBeNull();
    expect((await state(db)).title).toBeNull();
    expect((await state(db)).search_text).toBe("");
    // No equality inference in normal publication: real authority deterministically restores its value.
    await projectSessionDirectory(hoopDb(db), KEY);
    expect((await state(db)).title).toBe("Same recorded title");
    if (priority === "model") {
      await recordBrief(hoopDb(db), KEY, {"m-1":1}, {status:"quarantined",error:"fixture"});
      expect((await state(db)).title).toBeNull();
    }
  });
}


for(const [name,writer] of [["current",writeDirectoryCatalog],["legacy",writeLegacyDirectoryCatalog]]as const){
 test(`${name}: same-epoch complete replay removes absent/invalid recorded metadata without retaining stale title`,async()=>{
  const db=await setup(),opts={expectedEpoch:await epoch(db)};
  for(const removed of [undefined,"x".repeat(91),"bad\ntitle"]){
   await writer(db as never,KEY,input("Former recorded title"),opts);
   expect((await state(db)).title).toBe("Former recorded title");
   await writer(db as never,KEY,input(removed),opts);
   expect((await state(db)).recorded_catalog_title).toBeNull();
   expect((await state(db)).title).toBeNull();
   expect((await state(db)).search_text).not.toContain("Former recorded title");
   expect(await epoch(db)).toBe(opts.expectedEpoch);
  }
 });
}

test("schema metadata denial/malformed result refuses before catalog writes",async()=>{
 let writes=0;
 const deniedCalls:string[]=[];
 const denied={query:async(sql:string)=>{deniedCalls.push(sql);throw Object.assign(new Error("fixture denial"),{code:"42501"});}};
 await expect(writeDirectoryCatalog(denied as never,KEY,input("Recorded"),{expectedEpoch:"0"})).rejects.toMatchObject({code:"42501"});
 expect(deniedCalls).toHaveLength(1);
 expect(deniedCalls[0]).toMatch(/^SELECT /);
 const malformed={query:async(sql:string)=>{if(!sql.startsWith("SELECT EXISTS"))writes++;return{rows:[]};}};
 await expect(writeDirectoryCatalog(malformed as never,KEY,input("Recorded"),{expectedEpoch:"0"})).rejects.toThrow("directory_catalog_schema_unavailable");
 expect(writes).toBe(0);
});


test("real 044100 strips default EXECUTE grants and keeps the invalidation function owner-only",async()=>{
 const db=await before100();
 await db.exec("CREATE ROLE unrelated_title_caller NOLOGIN; ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO unrelated_title_caller");
 await db.exec(read("supabase/migrations/20261105044100_session_brief_summary_read.sql"));
 for(const fn of ["sessions.session_directory_recorded_title_invalidate()","sessions.session_brief_summary_for(text,text,text)"]){
  expect((await db.query<{allowed:boolean}>("SELECT has_function_privilege('unrelated_title_caller',$1,'EXECUTE') AS allowed",[fn])).rows[0]!.allowed).toBe(false);
 }
 expect((await db.query<{secdef:boolean}>("SELECT prosecdef AS secdef FROM pg_proc WHERE oid='sessions.session_directory_recorded_title_invalidate()'::regprocedure")).rows).toEqual([{secdef:false}]);
});
