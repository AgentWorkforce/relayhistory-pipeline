import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { applySupabaseSchema, REPO_ROOT } from "./pglite-schema.ts";
setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map(d => d.close())); });
const key = ["org-a", "ws-a", "claude", "s-1"];
const trace = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
async function database() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(db);
  await applySupabaseSchema(db, { until: null });
  await db.exec("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime,bulls96_read_api");
  for (const f of readdirSync(join(REPO_ROOT, "src/hoopdb/migrations")).sort())
    await db.exec(readFileSync(join(REPO_ROOT, "src/hoopdb/migrations", f), "utf8"));
  const rpc = async (fn: string, params: unknown[]) => {
    await db.exec("SET ROLE bulls96_session_ingest");
    try { return (await db.query<{ r: any }>(`SELECT sessions.${fn}(${params.map((_, i) => `$${i + 1}`).join(",")}) r`, params)).rows[0]?.r; }
    finally { await db.exec("RESET ROLE"); }
  };
  const pointer = async (source = key[2], ws = key[1], session = key[3]) => db.query(`INSERT INTO sessions.session_index(
    org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id)
    VALUES($1,$2,$3,$4,'machine','user',now(),now(),'s3','bucket','96bulls/org-a/v1/ws=ws-a/m=machine/h=claude/d=2026/11/11/s=s-1/manifest.json',decode(repeat('ab',32),'hex'),'jsonl',$5)`, [key[0], ws, source, session, trace]);
  const begin = () => rpc("ingest_operator_runtime_begin", [...key, crypto.randomUUID(), "user", "operator", "bucket", "96bulls", "a".repeat(64)]);
  return { db, rpc, pointer, begin };
}
const code = (p: Promise<unknown>) => p.then(() => "ok", (e: { code: string }) => e.code);

describe("operator runtime SQL", () => {
  test("an open write survives the fence; only its confirmed finish permits byte erasure", async () => {
    const d = await database(); await d.pointer();
    const writer = crypto.randomUUID();
    expect(await d.rpc("ingest_operator_runtime_write_begin", [JSON.stringify({ orgId: key[0], workspaceId: key[1], writerId: writer, sessions: [[key[2], key[3]]], physicalSessions: [[key[2], key[3]]] })])).toBe(true);
    await d.db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,title) VALUES($1,$2,$3,$4,now(),now(),'private title')", key);
    expect(await d.begin()).toMatchObject({ status: "in_progress", unreadable: true, uploadsFenced: true });
    const lease = crypto.randomUUID();
    expect(await d.rpc("ingest_operator_runtime_next", [lease])).toMatchObject({ sessionIds: [key[3]] });
    expect(await d.rpc("ingest_operator_runtime_authorize", [...key, lease])).toBe(false);
    expect(await code(d.rpc("ingest_operator_runtime_purge", [...key, lease]))).toBe("RH014");
    expect((await d.db.query("SELECT title FROM sessions.session_directory")).rows).toEqual([{ title: "private title" }]);
    await d.rpc("ingest_operator_runtime_write_finish", [key[0], key[1], writer]);
    expect(await d.rpc("ingest_operator_runtime_authorize", [...key, lease])).toBe(true);
    expect(await d.rpc("ingest_operator_runtime_write_begin", [JSON.stringify({ orgId: key[0], workspaceId: key[1], writerId: crypto.randomUUID(), sessions: [[key[2], key[3]]], physicalSessions: [[key[2], key[3]]] })])).toBe(false);
    await d.rpc("ingest_operator_runtime_purge", [...key, lease]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_index")).rows).toEqual([]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_directory")).rows).toEqual([]);
    // Stored byte addresses and text are gone; independent protocol steps still prevent completion.
    expect(await d.rpc("ingest_operator_erase_receipt", key)).toMatchObject({ status: "in_progress", bytesErased: false });
  });

  test("physical metadata purge cannot be resurrected; another workspace remains intact", async () => {
    const d = await database(); await d.pointer(); await d.pointer(key[2], "other-workspace");
    await d.db.query("INSERT INTO sessions.session_index_ext(org_id,workspace_id,harness,session_id,machine_id,session_uuid,tenant_id,first_trace_id,last_trace_id) VALUES($1,$2,$3,$4,'machine',$5,'tenant',$6,$6)", [...key, crypto.randomUUID(), trace]);
    await d.begin(); const lease = crypto.randomUUID(); await d.rpc("ingest_operator_runtime_next", [lease]);
    await d.rpc("ingest_operator_runtime_purge", [...key, lease]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_index_ext")).rows).toEqual([]);
    await d.pointer();
    await d.db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,title) VALUES($1,$2,$3,$4,now(),now(),'resurrection')", key);
    expect((await d.db.query("SELECT workspace_id FROM sessions.session_index")).rows).toEqual([{ workspace_id: "other-workspace" }]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_directory")).rows).toEqual([]);
    const r = await d.rpc("ingest_operator_erase_receipt", key);
    expect(r.artifacts.pointerRows).toMatchObject({ state: "done", count: 2 });
  });

  test("a source collision refuses before any fence or tombstone trigger", async () => {
    const d = await database(); await d.pointer(); await d.pointer("other-source");
    expect(await code(d.begin())).toBe("RH013");
    expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_fences")).rows).toEqual([]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_index WHERE deleted_at IS NULL")).rows).toHaveLength(2);
  });

  test("another bucket or an unrecognized manifest layout refuses before fencing", async () => {
    const d = await database(); await d.pointer();
    await d.db.exec("UPDATE sessions.session_index SET bucket='other-bucket'");
    expect(await code(d.begin())).toBe("RH013");
    await d.db.exec("UPDATE sessions.session_index SET bucket='bucket',manifest_key='foreign/prefix/manifest.json'");
    expect(await code(d.begin())).toBe("RH013");
    expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_fences")).rows).toEqual([]);
  });

  test("the enforced indexer installs canonical identity atomically and refuses a new alias after erase", async () => {
    const d = await database(); await d.pointer();
    const row = (await d.db.query<{ row: Record<string, unknown> }>("SELECT to_jsonb(s) row FROM sessions.session_index s")).rows[0]!.row;
    const upsert = async (session: string) => {
      const value = { ...row, session_id: session, manifest_key: String(row.manifest_key).replace("s=s-1/", `s=${session}/`) };
      await d.db.exec("SET ROLE search_consumer_runtime");
      try { return (await d.db.query<{ fenced: boolean }>("SELECT * FROM sessions.ingest_operator_runtime_upsert($1::jsonb,$2,'machine')", [JSON.stringify([value]), key[3]])).rows[0]!; }
      finally { await d.db.exec("RESET ROLE"); }
    };
    expect((await upsert("alias-before")).fenced).toBe(false);
    expect((await d.db.query("SELECT canonical_session_id FROM sessions.session_index WHERE session_id='alias-before'")).rows)
      .toEqual([{ canonical_session_id: key[3] }]);
    await d.begin();
    expect((await d.rpc("ingest_operator_runtime_next", [crypto.randomUUID()])).sessionIds).toEqual(["alias-before", key[3]]);
    expect((await upsert("alias-after")).fenced).toBe(true);
    expect((await d.db.query("SELECT 1 FROM sessions.session_index WHERE session_id='alias-after'")).rows).toEqual([]);
  });

  test("lease expiry or an incorrect lease cannot delete metadata", async () => {
    const d = await database(); await d.pointer(); await d.begin();
    const lease = crypto.randomUUID(); await d.rpc("ingest_operator_runtime_next", [lease]);
    expect(await d.rpc("ingest_operator_runtime_authorize", [...key, crypto.randomUUID()])).toBe(false);
    expect(await code(d.rpc("ingest_operator_runtime_step", [...key, crypto.randomUUID(), "transcriptObjects", "done", 10, null]))).toBe("RH014");
    await d.db.exec("UPDATE sessions.session_erasure_runs SET lease_until=clock_timestamp()-interval '1 second'");
    expect(await code(d.rpc("ingest_operator_runtime_purge", [...key, lease]))).toBe("RH014");
    expect((await d.db.query("SELECT 1 FROM sessions.session_index")).rows).toHaveLength(1);
    expect(await d.rpc("ingest_operator_runtime_next", [crypto.randomUUID()])).toMatchObject({ sessionIds: [key[3]] });
  });

  test("only ingest can call the entry points; the login has no direct runtime-table privileges", async () => {
    const d = await database();
    await d.db.exec("SET ROLE anon");
    expect(await code(d.db.query("SELECT sessions.ingest_operator_runtime_next($1)", [crypto.randomUUID()]))).toBe("42501");
    await d.db.exec("RESET ROLE;SET ROLE bulls96_session_ingest");
    expect(await code(d.db.query("SELECT 1 FROM sessions.session_erasure_writes"))).toBe("42501");
    await d.db.exec("RESET ROLE");
  });

  test("mixed digest caches refuse before fencing and preserve all workspace narratives", async () => {
    const d=await database();await d.pointer();
    for(const ws of ["ws-a","other-workspace"])await d.db.query(`INSERT INTO sessions.daily_digest_narratives(org_id,workspace_id,day,tz,scope_key,stats_hash,narrative_text,narrative_generated_at)
      VALUES('org-a',$1,'2026-11-11','UTC',$2,repeat('a',64),'private narrative',now())`,[ws,`workspace:["${ws}",null]`]);
    expect(await code(d.begin())).toBe("RH013");
    expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_fences")).rows).toEqual([]);
    expect((await d.db.query("SELECT 1 FROM sessions.daily_digest_narratives")).rows).toHaveLength(2);
  });

  test("facts use owning source, and the ingest login cannot bypass runtime admission or leases", async () => {
    const d=await database();await d.pointer();
    const fact=()=>d.db.exec(`INSERT INTO sessions.session_output_facts(org_id,workspace_id,fact_kind,ts,scope_source,source,session_id,fact_key,origin,row_key,row_id,fingerprint,source_version)
      VALUES('org-a','ws-a','message',now(),'claude','different-event-source','s-1','fact','native','row',repeat('b',64),'hash',1)`);
    await fact();
    expect(await code(d.rpc("ingest_operator_erase_session",[...key,crypto.randomUUID(),"user","operator"]))).toBe("42501");
    await d.begin();const lease=crypto.randomUUID();await d.rpc("ingest_operator_runtime_next",[lease]);
    expect(await code(d.rpc("ingest_operator_erase_record_step",[...key,"transcriptObjects","done",0,null]))).toBe("42501");
    await d.rpc("ingest_operator_runtime_purge",[...key,lease]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_output_facts")).rows).toEqual([]);await fact();
    expect((await d.db.query("SELECT 1 FROM sessions.session_output_facts")).rows).toEqual([]);
    expect(await d.rpc("ingest_operator_erase_receipt",key)).toMatchObject({status:"in_progress",bytesErased:false});
  });

  test("a new physical alias naming an erased canonical id cannot register object writes", async () => {
    const d=await database();await d.pointer();await d.begin();
    expect(await d.rpc("ingest_operator_runtime_write_begin",[JSON.stringify({orgId:key[0],workspaceId:key[1],writerId:crypto.randomUUID(),sessions:[[key[2],key[3]]],physicalSessions:[[key[2],"new-physical-alias"]]})])).toBe(false);
    expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_writes")).rows).toEqual([]);
    expect(await code(d.rpc("ingest_operator_runtime_write_begin",[JSON.stringify({orgId:key[0],workspaceId:key[1],writerId:crypto.randomUUID(),sessions:[[key[2],"new-alias"]]})]))).toBe("22023");
  });

  test("an enabled independent digest writer refuses before fencing even without cached rows",async()=>{
    const d=await database();await d.pointer();await d.db.exec("ALTER ROLE bulls96_digest_narrative LOGIN");
    expect(await code(d.begin())).toBe("RH013");
    expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_receipts")).rows).toEqual([]);
    expect((await d.db.query("SELECT 1 FROM sessions.session_index WHERE deleted_at IS NULL")).rows).toHaveLength(1);
  });

  test("another organization is preserved, and colliding alias layout refuses before any fence", async () => {
    const d=await database();await d.pointer();
    await d.db.exec(`INSERT INTO sessions.session_index SELECT(jsonb_populate_record(NULL::sessions.session_index,to_jsonb(s)||'{"org_id":"other-org"}'::jsonb)).* FROM sessions.session_index s`);
    await d.begin();const lease=crypto.randomUUID();await d.rpc("ingest_operator_runtime_next",[lease]);await d.rpc("ingest_operator_runtime_purge",[...key,lease]);
    expect((await d.db.query("SELECT org_id FROM sessions.session_index")).rows).toEqual([{org_id:"other-org"}]);
    const other=await database();await other.pointer();
    await other.db.exec(`UPDATE sessions.session_index SET session_id='a_b',canonical_session_id='a:b';
      INSERT INTO sessions.session_index SELECT(jsonb_populate_record(NULL::sessions.session_index,to_jsonb(s)||'{"machine_id":"other-machine","canonical_session_id":"a/b"}'::jsonb)).* FROM sessions.session_index s`);
    expect(await code(other.rpc("ingest_operator_runtime_begin",[key[0],key[1],key[2],"a:b",crypto.randomUUID(),"user","operator","bucket","96bulls","a".repeat(64)]))).toBe("RH013");
    expect((await other.db.query("SELECT 1 FROM sessions.session_erasure_fences")).rows).toEqual([]);
  });

  test("read fence is bound to the authenticated tenant and survives physical pointer deletion", async () => {
    const d = await database(); await d.pointer(); await d.begin();
    const lease = crypto.randomUUID(); await d.rpc("ingest_operator_runtime_next", [lease]);
    await d.rpc("ingest_operator_runtime_purge", [...key, lease]);
    await d.db.exec("SET ROLE bulls96_read_api;BEGIN");
    await d.db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", key.slice(0, 2));
    expect((await d.db.query("SELECT sessions.ingest_operator_runtime_erased($1,$2,$3,$4) erased", key)).rows).toEqual([{ erased: true }]);
    expect(await code(d.db.query("SELECT sessions.ingest_operator_runtime_erased('other-org',$1,$2,$3)", key.slice(1)))).toBe("42501");
    await d.db.exec("ROLLBACK;RESET ROLE");
  });
});

test("ambiguous and crashed writes remain blockers until tested owner-only stopped-writer recovery",async()=>{
 const d=await database();await d.pointer();
 const writer=crypto.randomUUID(),neighbor=crypto.randomUUID();
 const register=(id:string,sessions:string[])=>d.rpc("ingest_operator_runtime_write_begin",[JSON.stringify({orgId:key[0],workspaceId:key[1],writerId:id,
  sessions:sessions.map(s=>[key[2],s]),physicalSessions:sessions.map(s=>[key[2],s])})]);
 await register(writer,[key[3],"same-batch-other-session"]);await register(neighbor,["unrelated-session"]);
 await d.rpc("ingest_operator_runtime_write_failed",[key[0],key[1],writer]);
 expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_writes WHERE writer_id=$1 AND failed_at IS NOT NULL",[writer])).rows).toHaveLength(2);
 await d.begin();const lease=crypto.randomUUID();await d.rpc("ingest_operator_runtime_next",[lease]);
 expect(await d.rpc("ingest_operator_runtime_authorize",[...key,lease])).toBe(false);
 await d.db.exec("UPDATE sessions.session_erasure_writes SET started_at='2000-01-01',failed_at='2000-01-01' WHERE failed_at IS NOT NULL");
 expect(await d.rpc("ingest_operator_runtime_authorize",[...key,lease])).toBe(false);
 const recovery=readFileSync(join(REPO_ROOT,"supabase/operations/operator_session_erase_recover_writer.sql"),"utf8");
 const recover=(ws:string,stopped:boolean)=>d.db.query(recovery,[key[0],ws,writer,stopped]);
 expect((await recover(key[1],false)).rows).toEqual([{recovered:0,audit_events:0}]);
 expect((await recover("other-workspace",true)).rows).toEqual([{recovered:0,audit_events:0}]);
 await d.db.exec("SET ROLE bulls96_session_ingest");
 expect(await code(recover(key[1],true))).toBe("42501");await d.db.exec("RESET ROLE");
 // Fixture owner now attests the entire writer is stopped and its writes reconciled.
 expect((await recover(key[1],true)).rows).toEqual([{recovered:2,audit_events:1}]);
 expect(await d.rpc("ingest_operator_runtime_authorize",[...key,lease])).toBe(true);
 expect((await recover(key[1],true)).rows).toEqual([{recovered:0,audit_events:0}]);
 expect((await d.db.query("SELECT state,error_code FROM sessions.session_erasure_events WHERE state='writer_recovered'")).rows)
  .toEqual([{state:"writer_recovered",error_code:"writer_stopped_verified"}]);
 expect((await d.db.query("SELECT writer_id FROM sessions.session_erasure_writes")).rows).toEqual([{writer_id:neighbor}]);
 const success=crypto.randomUUID();await register(success,["successful-session"]);
 await d.rpc("ingest_operator_runtime_write_finish",[key[0],key[1],success]);
 await d.rpc("ingest_operator_runtime_write_failed",[key[0],key[1],success]);
 expect((await d.db.query("SELECT 1 FROM sessions.session_erasure_writes WHERE writer_id=$1",[success])).rows).toEqual([]);
});
