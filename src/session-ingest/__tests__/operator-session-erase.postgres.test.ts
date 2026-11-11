/** Force both transaction orders against an owned disposable PostgreSQL database. */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { readFileSync,readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
if(url){const u=new URL(url);if(!["127.0.0.1","localhost","[::1]"].includes(u.hostname)||u.username!=="postgres"||u.password||u.pathname!=="/postgres"||u.search||u.hash)throw Error("owned_loopback_fixture_required");}
const root=join(import.meta.dir,"../../..");
const read=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
describe.skipIf(!url)("operator fence serializes all pointer statements",()=>{
 test("INSERT and UPDATE drain before erase or see its committed fence, including an old repeatable-read snapshot",async()=>{
  const admin=new pg.Client({connectionString:url});await admin.connect();
  const name=`erase_pointer_${crypto.randomUUID().replaceAll("-","")}`;await admin.query(`CREATE DATABASE ${name}`);
  const target=new URL(url!);target.pathname=`/${name}`;
  const pool=new pg.Pool({connectionString:target.href,max:5});const held:pg.PoolClient[]=[];
  try{
   await pool.query(read("supabase/tests/supabase_shim.sql"));await pool.query("CREATE EXTENSION btree_gin;CREATE EXTENSION pg_trgm");
   for(const f of SESSION_BOOTSTRAP_FILES)await pool.query(read(f));
   for(const f of readdirSync(join(root,"supabase/migrations")).sort())await pool.query(read(`supabase/migrations/${f}`));
   const writer=await pool.connect(),eraser=await pool.connect();held.push(writer,eraser);
   const pid=async(c:pg.PoolClient)=>(await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
   const wp=await pid(writer),ep=await pid(eraser);
   const blocked=async(waiter:number,blocker:number)=>{for(let i=0;i<500;i++){if((await pool.query("SELECT pg_blocking_pids($1) p",[waiter])).rows[0].p.includes(blocker))return;await Bun.sleep(10);}throw Error("expected_lock_not_observed");};
   const insert=(sid:string)=>writer.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id)
    VALUES('org','ws','claude',$1,'machine','user',now(),now(),'s3','bucket',$1,decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18') RETURNING session_id`,[sid]);
   const erase=(sid:string)=>eraser.query("SELECT sessions.ingest_operator_erase_session('org','ws','claude',$1,$2,'user','operator') r",[sid,crypto.randomUUID()]);
   // Writer already owns a row lock; the erase waits on the BEFORE STATEMENT shared gate.
   await writer.query("BEGIN");await insert("writer-first");await eraser.query("BEGIN");
   const pending=erase("writer-first");await blocked(ep,wp);await writer.query("COMMIT");
   expect((await pending).rows[0].r).toMatchObject({unreadable:true,artifacts:{pointerRows:{state:"done"}}});await eraser.query("COMMIT");
   expect((await pool.query("SELECT 1 FROM sessions.session_index WHERE deleted_at IS NULL")).rows).toEqual([]);
   // The writer statement starts before the erase commits; its guard must see the post-wait fence.
   await eraser.query("BEGIN");await erase("erase-first");
   await writer.query("BEGIN");const later=insert("erase-first");await blocked(wp,ep);await eraser.query("COMMIT");
   expect((await later).rows).toEqual([]);await writer.query("COMMIT");
   // A direct UPDATE also takes the gate before its row locks and cannot revive a tombstone.
   await insert("update-first");await writer.query("BEGIN");await writer.query("UPDATE sessions.session_index SET user_id='late-user' WHERE session_id='update-first'");
   await eraser.query("BEGIN");const updating=erase("update-first");await blocked(ep,wp);await writer.query("COMMIT");await updating;await eraser.query("COMMIT");
   expect((await writer.query("UPDATE sessions.session_index SET deleted_at=NULL WHERE session_id='update-first' RETURNING session_id")).rows).toEqual([]);
   // The actual projector can start with a stale statement snapshot while erase holds the gate.
   await insert("directory-race");
   const directoryKey={orgId:"org",workspaceId:"ws",harness:"claude",sessionId:"directory-race"};
   await projectSessionDirectory(writer,directoryKey);
   await eraser.query("BEGIN");await erase("directory-race");
   const projecting=projectSessionDirectory(writer,directoryKey);
   await blocked(wp,ep);await eraser.query("COMMIT");await projecting;
   expect((await pool.query("SELECT 1 FROM sessions.session_directory WHERE session_id='directory-race' AND deleted_at IS NULL")).rows).toEqual([]);
   expect((await writer.query("UPDATE sessions.session_directory SET deleted_at=NULL,title='late private text' WHERE session_id='directory-race' RETURNING session_id")).rows).toEqual([]);
   // Higher isolation cannot keep a stale snapshot and insert behind a committed erase.
   await writer.query("BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT count(*) FROM sessions.session_erasure_fences");
   await eraser.query("BEGIN");await erase("old-snapshot");
   const stale=insert("old-snapshot");const outcome=stale.then(()=>"unsafe",e=>(e as {code:string}).code);
   await blocked(wp,ep);await eraser.query("COMMIT");expect(await outcome).toBe("40001");await writer.query("ROLLBACK");
   // The eraser must also reject an old snapshot before it can miss newly committed aliases.
   await eraser.query("BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT 1");
   await expect(erase("stale-eraser")).rejects.toMatchObject({code:"40001"});await eraser.query("ROLLBACK");
   // Actual publication retirement, through both selection and reset, in both transaction orders.
   // Without the owner-row statement gate, control-first holds USER while erase holds GLOBAL:
   // the retirement trigger then requests GLOBAL and PostgreSQL reports 40P01.
   for (const kind of ["selection","reset"] as const) for (const first of ["control","erase"] as const) {
    const sid=`${kind}-${first}`,user=sid,generation=crypto.randomUUID();
    await insert(sid);await writer.query("UPDATE sessions.session_index SET user_id=$1 WHERE session_id=$2",[user,sid]);
    await projectSessionDirectory(writer,{orgId:"org",workspaceId:"ws",harness:"claude",sessionId:sid});
    await writer.query("SELECT sessions.ingest_lock_fences('org','ws',$1)",[user]);
    await writer.query("INSERT INTO sessions.ingest_session_ownership VALUES('org','ws',$1,'origin','claude',$2)",[user,sid]);
    await writer.query(`INSERT INTO sessions.ingest_publication_heads
     (org_id,workspace_id,user_id,origin_id,source,session_id,physical_workspace,physical_source,physical_session,machine_id,
      generation,fence_epoch,reset_epoch,pointer_revision,current_key,current_raw_sha256,state)
     VALUES('org','ws',$1,'origin','claude',$2,'ws','claude',$2,'machine',$3,0,0,1,$2,repeat('ab',32),'live')`,[user,sid,generation]);
    await writer.query(`INSERT INTO sessions.ingest_publication_generations
     (generation,prefix,org_id,workspace_id,user_id,origin_id,source,session_id,fence_epoch,reset_epoch,state,target_bucket,target_region,key_root)
     VALUES($1,$2,'org','ws',$3,'origin','claude',$2,0,0,'live','bucket','us-east-1','96bulls')`,[generation,sid,user]);
    const control=()=>kind==="selection"
     ?writer.query("SELECT sessions.ingest_apply_selection('org','ws',$1,'origin',jsonb_build_array(jsonb_build_array('claude',$2::text)),false)",[user,sid])
     :writer.query("SELECT sessions.ingest_apply_reset('org','ws',$1,$2)",[user,crypto.randomUUID()]);
    if(first==="control"){
     await writer.query("BEGIN");
     if(kind==="selection"){
      // The direct DELETE guard also locks USER even when it skips a live managed row.
      await writer.query("DELETE FROM sessions.session_index WHERE session_id=$1",[sid]);
     }else await writer.query("SELECT sessions.ingest_lock_fences('org','ws',$1)",[user]);
     await eraser.query("BEGIN");const pending=erase(sid);await blocked(ep,wp);
     await control();await writer.query("COMMIT");await pending;await eraser.query("COMMIT");
    }else{
     await eraser.query("BEGIN");await erase(sid);await writer.query("BEGIN");
     const pending=control();await blocked(wp,ep);await eraser.query("COMMIT");await pending;await writer.query("COMMIT");
    }
    expect((await pool.query("SELECT state FROM sessions.ingest_publication_heads WHERE generation=$1",[generation])).rows).toEqual([{state:"retired"}]);
    expect((await pool.query("SELECT state FROM sessions.ingest_publication_generations WHERE generation=$1",[generation])).rows).toEqual([{state:"retired"}]);
    expect((await pool.query("SELECT 1 FROM sessions.session_index WHERE session_id=$1 AND deleted_at IS NULL",[sid])).rows).toEqual([]);
    expect((await pool.query("SELECT 1 FROM sessions.session_directory WHERE session_id=$1 AND deleted_at IS NULL",[sid])).rows).toEqual([]);
   }
   expect((await pool.query("SELECT 1 FROM sessions.session_index WHERE deleted_at IS NULL")).rows).toEqual([]);
  }finally{for(const c of held){await c.query("ROLLBACK").catch(()=>{});c.release();}await pool.end();await admin.query(`DROP DATABASE ${name}`);await admin.end();}
 },120_000);
});
