/** The feed RPCs (supabase/migrations/20261105043870_buffered_search_feed_cursor.sql) on every migration in PGlite. */
import {orderedFixtureLedger,initializeFixtureSeatClock} from "../../session-ingest/__tests__/ordered-support.ts";
import {afterEach,expect,setDefaultTimeout,test} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {createHash,randomUUID} from "node:crypto";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {sqlLedger} from "../../session-ingest/ledger-sql.ts";
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
import {sqlBufferedFeedSource} from "../buffered-search-feed.ts";
import {BUFFERED_SEARCH_FEED_RUNTIME_ROLE as FEED_ROLE} from "../buffered-feed-role.ts";
setDefaultTimeout(60000);
const root=join(import.meta.dir,"../../.."),open:PGlite[]=[];
afterEach(async()=>{for(const db of open.splice(0))await db.close()});
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
const generation="a".repeat(64);
const tenant={orgId:"org-a",workspaceId:"ws-a",userId:"user-a"},origin="origin-a";
const SINCE="2026-11-05T05:33:45.000Z";

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(db);
 const read=(path:string)=>readFileSync(join(root,path),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 // Single-sourced bootstrap chain: the grant migration FAILs closed without the
 // bare consumer grantee, so provision it exactly like migrate's bootstrap step.
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const name of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())
  await db.exec(read("supabase/migrations/"+name));
 await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now());INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','"+generation+"','pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status=excluded.status");
 const as=async(role:string,text:string,params:unknown[]=[])=>{
  await db.exec("BEGIN;SET LOCAL ROLE "+role);
  try{const r=await db.query(text,params as never);await db.exec("COMMIT");return r}
  catch(e){await db.exec("ROLLBACK");throw e}
 };
 await db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[tenant.orgId,generation]);
 await initializeFixtureSeatClock(db as never,generation);
 const ledger=orderedFixtureLedger(sqlLedger(((s:string,p:unknown[])=>as("bulls96_session_ingest",s,p)) as never,{acceptanceMode:"buffered",expectedGeneration:generation}),db as never);
 let serial=0;
 /** buffer-gc.pglite.test.ts's owner fixture: ordinal 0 in session "gone", 1 in "live", 2 no session. */
 async function stage(opts:{ack?:boolean;createdAt?:string}={}){
  await db.query("SELECT sessions.ingest_lock_fences($1,$2,$3)",[tenant.orgId,tenant.workspaceId,tenant.userId]);
  const batch="batch-"+(++serial),claim=randomUUID(),token=randomUUID(),digest=hash(batch),sha=hash(token);
  const key="96bulls/org-a/buffered-batches/v1/ws=ws-a/m=srv_"+"f".repeat(32)+"/b="+hash(digest).slice(0,32)+"-"+sha.slice(0,16)+"-"+hash(claim).slice(0,32)+".staged.ndjson.zst";
  await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,$2,$3,$4,$5,$6,'rhr_'||repeat('a',64),'pending',NULL,'buffered')",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,digest]);
  const records=[{ord:0,source:"claude",session_id:"gone",record_id:"g",revision_id:"g:1",revision:1,digest:hash("g")},
   {ord:1,source:"claude",session_id:"live",record_id:"l",revision_id:"l:1",revision:1,digest:hash("l")},
   {ord:2,source:"claude",session_id:null,record_id:"n",revision_id:"n:1",revision:1,digest:hash("n")}];
  await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10)",[claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,JSON.stringify([{source:"claude",session_id:"gone",epoch:0,reset_epoch:0},{source:"claude",session_id:"live",epoch:0,reset_epoch:0}]),JSON.stringify(records),generation]);
  await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,10,$11,0,'preparing',clock_timestamp()+interval '5 minutes',$12)",[token,claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,key,sha,JSON.stringify([[0],[1],[2]]),opts.createdAt??"2026-11-06T00:00:00Z"]);
  if(opts.ack!==false)await ack({batch,token});
  return {batch,token,key};
 }
 async function ack({batch,token}:{batch:string;token:string}){
  await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,$2,$3,$4,$5,$6,$7)",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,hash(batch),token]);
  await db.query("UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged' WHERE stage_token=$1",[token]);
  await db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at=clock_timestamp(),buffer_receipt_kind='staged' WHERE batch_id=$1",[batch]);
 }
 const disable=()=>ledger.setSessionUploads({tenant,originId:origin,sessions:[["claude","gone"]],enabled:false});
 /** Run sql as a genuine login of `role` (SET SESSION AUTHORIZATION: session_user = role, role GUC 'none'),
  *  or, with `via`, as the login `via` that then runs SET LOCAL ROLE `role`. PGlite does not restore
  *  session_user on RESET or ROLLBACK, so the superuser is reinstated explicitly. */
 const login=async(role:string,text:string,params:unknown[]=[],via?:string)=>{
  await db.exec("SET SESSION AUTHORIZATION "+(via??role));
  try{
   await db.exec("BEGIN");
   try{if(via!==undefined)await db.exec("SET LOCAL ROLE "+role);const r=await db.query(text,params as never);await db.exec("COMMIT");return r}
   catch(e){await db.exec("ROLLBACK");throw e}
  }finally{await db.exec("SET SESSION AUTHORIZATION postgres")}
 };
 const feeder={query:(text:string,params?:unknown[])=>login(FEED_ROLE,text,params) as never};
 return {db,as,login,stage,ack,disable,source:sqlBufferedFeedSource(feeder)};
}

test("page lists only acknowledged, live, active stages after the cursor, in keyset order; advance is monotonic",async()=>{
 const f=await fixture();
 const before=await f.stage({createdAt:"2026-11-05T05:00:00Z"});
 const a=await f.stage({createdAt:"2026-11-06T00:00:00Z"});
 const b=await f.stage({createdAt:"2026-11-06T00:00:01Z"});
 await f.stage({createdAt:"2026-11-06T00:00:02Z",ack:false});
 const redacted=await f.stage({createdAt:"2026-11-06T00:00:03Z"});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted' WHERE stage_token=$1",[redacted.token]);
 const c=await f.stage({createdAt:"2026-11-06T00:00:04Z"});
 const p1=await f.source.page("backfill-test",SINCE,2);
 expect(p1.after).toBeNull();
 expect(p1.stages.map(s=>s.stageToken)).toEqual([a.token,b.token]);
 expect(p1.stages[0]).toMatchObject({tenant,originId:origin,batchId:a.batch,createdAt:"2026-11-06T00:00:00.000000Z"});
 expect(p1.stages.map(s=>s.stageToken)).not.toContain(before.token);
 expect(await f.source.advance("backfill-test",p1.stages[1]!,{indexed:2,skipped:0})).toBe(true);
 // Never backwards.
 expect(await f.source.advance("backfill-test",p1.stages[0]!,{indexed:0,skipped:0})).toBe(false);
 const p2=await f.source.page("backfill-test",SINCE,10);
 expect(p2.after).toEqual({createdAt:"2026-11-06T00:00:01.000000Z",stageToken:b.token});
 expect(p2.stages.map(s=>s.stageToken)).toEqual([c.token]);
 // A cursor is bound to its lower bound.
 await expect(f.source.page("backfill-test","2026-11-01T00:00:00.000Z",10)).rejects.toMatchObject({code:"sql_22023"});
 expect((await f.db.query("SELECT stages_indexed::int AS n FROM sessions.buffered_search_feed_cursors WHERE name='backfill-test'")).rows[0]).toEqual({n:2});
});

test("page orders by created_at, not ack time: a stage acknowledged behind the cursor is never listed (why the feed holds back)",async()=>{
 const f=await fixture();
 const late=await f.stage({createdAt:"2026-11-06T00:00:00Z",ack:false});
 const young=await f.stage({createdAt:"2026-11-06T00:00:01Z"});
 const p1=await f.source.page("late-ack-test",SINCE,10);
 expect(p1.stages.map(s=>s.stageToken)).toEqual([young.token]);
 expect(await f.source.advance("late-ack-test",p1.stages[0]!,{indexed:1,skipped:0})).toBe(true);
 await f.ack(late);
 expect((await f.source.stage({stageToken:late.token})).status).toBe("live");
 expect((await f.source.page("late-ack-test",SINCE,10)).stages).toEqual([]);
 // A cursor that had not passed it yet lists it.
 expect((await f.source.page("late-ack-fresh",SINCE,10)).stages.map(s=>s.stageToken)).toEqual([late.token,young.token]);
});

test("stage: retention comes from the GC authority; redacted, retired, preparing, leased and fenced fail closed",async()=>{
 const f=await fixture();
 const s=await f.stage();
 expect(await f.source.stage({stageToken:s.token})).toMatchObject({found:true,status:"live",reason:null,retained:[0,1,2],tenant,originId:origin,batchId:s.batch,stage:{key:s.key,groupOrdinals:[[0],[1],[2]]}});
 expect((await f.source.stage({stageKey:s.key})).stage?.stageToken).toBe(s.token);
 // A deleted session: exactly the ordinals GC would erase are no longer retained.
 await f.disable();
 expect(await f.source.stage({stageToken:s.token})).toMatchObject({status:"live",retained:[1,2]});
 // GC in flight defers; fenced privacy authority defers.
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_token=gen_random_uuid(),gc_lease_version=row_version,gc_lease_until=clock_timestamp()+interval '5 minutes' WHERE stage_token=$1",[s.token]);
 expect(await f.source.stage({stageToken:s.token})).toMatchObject({status:"deferred",reason:"gc_in_flight"});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_token=NULL,gc_lease_version=NULL,gc_lease_until=NULL WHERE stage_token=$1",[s.token]);
 await f.db.query("INSERT INTO sessions.ingest_privacy_authority(state,control_epoch) VALUES('fenced',repeat('3',64))");
 expect(await f.source.stage({stageToken:s.token})).toMatchObject({status:"deferred",reason:"privacy_fenced"});
 await f.db.query("DELETE FROM sessions.ingest_privacy_authority");
 // Redacted: never indexed, nothing retained.
 await f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted' WHERE stage_token=$1",[s.token]);
 expect(await f.source.stage({stageToken:s.token})).toMatchObject({status:"gone",reason:"stage_redacted",retained:[]});
 // Preparing (PUT before receipt): pending until it expires.
 const p=await f.stage({ack:false});
 expect(await f.source.stage({stageToken:p.token})).toMatchObject({status:"pending",reason:"stage_preparing",retained:[]});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[p.token]);
 expect(await f.source.stage({stageToken:p.token})).toMatchObject({status:"gone",reason:"stage_expired"});
 // Not the batch's active object (a retired parent of a GC replacement).
 const r=await f.stage();
 await f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='retired' WHERE stage_token=$1",[r.token]);
 expect(await f.source.stage({stageToken:r.token})).toMatchObject({status:"gone",reason:"stage_retired",retained:[]});
 expect(await f.source.stage({stageToken:randomUUID()})).toMatchObject({found:false,status:"gone",reason:"stage_absent"});
 await expect(f.source.stage({stageKey:"96bulls/org-a/live-batches/v1/x"})).rejects.toMatchObject({code:"sql_22023"});
});

test("only the genuine buffered_search_feed_runtime login may call the feed RPCs; the cursor table is not readable",async()=>{
 const f=await fixture();
 const page=["SELECT sessions.buffered_search_feed_page($1::jsonb) AS r",[JSON.stringify({cursor:"x",since:SINCE,limit:1})]] as const;
 const stage=["SELECT sessions.buffered_search_feed_stage($1::jsonb) AS r",[JSON.stringify({stageToken:randomUUID()})]] as const;
 const advance=["SELECT sessions.buffered_search_feed_advance($1::jsonb) AS r",[JSON.stringify({cursor:"x",to:{createdAt:"2026-11-06T00:00:00.000000Z",stageToken:randomUUID()},indexed:0,skipped:0})]] as const;
 // The genuine login: session_user is the feed role, no SET ROLE in effect.
 expect((await f.login(FEED_ROLE,"SELECT session_user::text AS s,current_user::text AS c,current_setting('role') AS r")).rows).toEqual([{s:FEED_ROLE,c:FEED_ROLE,r:"none"}]);
 expect((await f.login(FEED_ROLE,...page)).rows).toHaveLength(1);
 const code=(p:Promise<unknown>)=>p.then(()=>"accepted",(e:any)=>e?.code);
 for(const [sql,params] of [page,stage,advance]){
  // service_role (the previous grantee), non-member logins and the ingest/read roles: no EXECUTE, or not the login.
  for(const role of ["service_role","authenticated","anon","bulls96_read_api","bulls96_session_ingest"])
   expect([role,await code(f.login(role,sql,[...params]))]).toEqual([role,"42501"]);
  // A superuser session that SETs ROLE to the feed role: session_user is not the feed login.
  expect(["postgres via SET ROLE",await code(f.login(FEED_ROLE,sql,[...params],"postgres"))]).toEqual(["postgres via SET ROLE","42501"]);
  // The feed login running a (no-op) SET ROLE to its own name is not the genuine-login shape either.
  expect(["self SET ROLE",await code(f.login(FEED_ROLE,sql,[...params],FEED_ROLE))]).toEqual(["self SET ROLE","42501"]);
 }
 // A LOGIN granted the feed role (the membership the bootstrap and verifier refuse) that SETs ROLE into it.
 await f.db.exec("CREATE ROLE feed_probe_member LOGIN NOINHERIT;GRANT "+FEED_ROLE+" TO feed_probe_member");
 expect((await f.login(FEED_ROLE,"SELECT session_user::text AS s,current_user::text AS c",[],"feed_probe_member")).rows).toEqual([{s:"feed_probe_member",c:FEED_ROLE}]);
 for(const [sql,params] of [page,stage,advance]){
  const e=await f.login(FEED_ROLE,sql,[...params],"feed_probe_member").then(()=>null,(e:any)=>e);
  expect([e?.code,e?.message]).toEqual(["42501","buffered search feed: caller is not the buffered_search_feed_runtime login"]);
 }
 // The feed role reads neither the cursor table nor the ledger directly.
 for(const role of [FEED_ROLE,"service_role"]){
  await expect(f.login(role,"SELECT * FROM sessions.buffered_search_feed_cursors")).rejects.toMatchObject({code:"42501"});
  await expect(f.login(role,"SELECT * FROM sessions.ingest_stage_intents")).rejects.toMatchObject({code:"42501"});
 }
});
