/**
 * 20261105044090 claim-sessions reader v2 (PR 2, default off): sessions.buffered_sessions_for and
 * sessions.buffered_session_stages_for read sessions.ingest_claim_sessions (044080) while the policy gate
 * (read_enabled AND write_enabled AND backfill_complete_at) is on, and must return exactly 044040's rows,
 * order and cursor. 044040's two readers, unchanged, are loaded as ref044090.list / ref044090.stages (a
 * separate schema, so the sessions function manifest stays exact) and compared row for row.
 * Seeds through the real buffered acceptance path (POST /v1/turns/batches and /v1/delivery/batches as
 * bulls96_session_ingest), as owner-buffered-read.pglite.test.ts; reads as bulls96_read_api.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliveryAccount} from "../vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../vendor/delivery/delivery-contracts.ts";
import {TURN_KIND,TURN_ORIGIN_ID,turnRecordId} from "../turns.ts";
import {TENANT,scratchStore,record,batch} from "./support.ts";
import {backfillClaimSessions} from "../claim-sessions-backfill.ts";
import acl from "../../hoopdb/sessions-function-acl.json" with {type:"json"};
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
setDefaultTimeout(300000);

const root=join(import.meta.dir,"../../.."),generation="4".repeat(64),epoch="1".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const psql=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter((l)=>!l.startsWith("\\")).join("\n");
const READER="supabase/migrations/20261105044090_claim_sessions_reader.sql";
const SESSION_FIRST="supabase/migrations/20261105044040_buffered_stages_session_first.sql";
const CLAIM_SESSIONS="supabase/migrations/20261105044080_ingest_claim_sessions.sql";
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});
type Tenant={orgId:string;workspaceId:string;userId:string};
const S=(n:number)=>`7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a${String(n).padStart(2,"0")}`;
const HP=[epoch,null,null,null,null,false,baseline];
const LIST="SELECT source,session_id,first_ts,last_ts,last_ts::text AS cursor_ts,stage_count,bytes::text AS bytes,user_id,admission_epoch,admission_version FROM ";
const STAGES="SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at,admission_epoch,admission_version FROM ";
const LIST_FN="sessions.buffered_sessions_for",STAGES_FN="sessions.buffered_session_stages_for";
const pickFn=(sql:string,name:"sessions"|"stages")=>{
 const re=name==="sessions"?/CREATE OR REPLACE FUNCTION sessions\.buffered_sessions_for\([\s\S]*?END\n\$buffered_sessions\$;/:/CREATE OR REPLACE FUNCTION sessions\.buffered_session_stages_for\([\s\S]*?END\n\$buffered_stages\$;/;
 const m=sql.match(re);if(!m)throw Error("reader body missing: "+name);return m[0];
};

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const {createIngestApp}=await import("../app.ts");
 let who:Tenant=TENANT;
 const app=createIngestApp({identify:()=>({...who,scopes:["rth:sync"]}),deliver:deps,log:()=>{},custodySource:"frozen-supabase"});
 let serial=0;
 /** One accepted buffered turn batch (one session) as tenant t. */
 async function upload(session:string,turns=2,source="claude",start=0,t:Tenant=TENANT){
  const batchId="b-"+(++serial);who=t;
  const records=Array.from({length:turns},(_,j)=>{const i=start+j;
   const record_id=turnRecordId(t.workspaceId,source,session,i),revision=1_793_000_000_000_000+serial*100+i;
   return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,record_id,revision_id:`turnrev:${record_id.slice(5)}:${revision}`,revision,kind:TURN_KIND as any,source,session_id:session,operation:"upsert" as const,
    payload:{id:`${String(serial*100+i).padStart(8,"0")}-aaaa-4bbb-8ccc-dddddddddddd`,org_id:t.orgId,workspace_id:t.workspaceId,source,session_id:session,turn_index:i,session_owner:"user-1",role:"user",content:`turn ${i}`,actor_name:"Ada",actor_role:"owner",metadata_json:"{}",ts:"2026-11-05T12:00:00.123Z",tagged:true}};
  });
  const b={schema_version:1 as const,origin_id:TURN_ORIGIN_ID,batch_id:batchId,job_id:"relayhistory-turns",generation:1,destination_id:"relayhistory",instance_id:"relayhistory-api",account_id:await deliveryAccount(t),mapping_version:DELIVERY_MAPPING_VERSION,records};
  const r=await app.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:b})});
  who=TENANT;
  if(r.status!==200)throw new Error("upload "+r.status+" "+await r.text());
  return batchId;
 }
 /** One accepted buffered delivery batch holding several sessions (one commit: their last_ts tie). */
 async function deliver(groups:Array<[string,string,string[]]>,t:Tenant=TENANT){
  const batchId="b-"+(++serial);who=t;
  const records=groups.flatMap(([source,session_id,ids])=>ids.map((id)=>record({record_id:id,revision:serial,revision_id:`${id}:r${serial}`,source,session_id})));
  const b=await batch(records,{batch_id:batchId,account_id:await deliveryAccount(t)});
  const r=await app.request("/v1/delivery/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:b})});
  who=TENANT;
  if(r.status!==200)throw new Error("deliver "+r.status+" "+await r.text());
  return batchId;
 }
 const asReader=async(sql:string,params:unknown[]=[],bind:{org?:string;ws?:string;user?:string|null}={})=>{
  await db.exec("BEGIN READ ONLY");
  try{
   await db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",[bind.org??TENANT.orgId,bind.ws??TENANT.workspaceId,bind.user===undefined?TENANT.userId:(bind.user??"")]);
   await db.exec("SET LOCAL ROLE bulls96_read_api");
   const r=await db.query(sql,params as never);await db.exec("COMMIT");return r.rows as any[];
  }catch(e){await db.exec("ROLLBACK");throw e}
 };
 /** Test-only raw owner G2 proof with no head (as owner-buffered-read.pglite.test.ts's prove(NONE)). */
 const prove=async(o:{org?:string;user?:string}={})=>{
  const key=[o.org??TENANT.orgId,TENANT.workspaceId,o.user??TENANT.userId];
  await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3",key);
  await db.query(`INSERT INTO sessions.owner_buffered_parity(org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation)
   SELECT $1,$2,$3,$4,NULL,NULL,NULL,NULL,true,$5,e.deletion_set_digest,e.reset_set_digest,e.target_lineage_digest,e.target_generation
     FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,NULL::bigint,NULL,NULL::text) AS e`,[...key,epoch,baseline]);
 };
 const list=(fn:string,limit:number,cursor:Array<string|null>=[null,null,null],bind={})=>asReader(LIST+fn+"($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",[TENANT.workspaceId,...cursor,limit,...HP],bind);
 const stages=(fn:string,source:string,session:string,bind={})=>asReader(STAGES+fn+"($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",[TENANT.workspaceId,source,session,...HP],bind);
 const sql={query:(s:string,p?:unknown[])=>db.query(s,p as never) as never};
 const policy=async()=>(await db.query<{w:boolean;r:boolean;c:boolean}>("SELECT write_enabled w,read_enabled r,backfill_complete_at IS NOT NULL c FROM sessions.ingest_claim_sessions_policy")).rows[0]!;
 const writer=(on:boolean)=>db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=$1",[on]);
 const backfill=()=>backfillClaimSessions({sql,batchSize:3,statementTimeoutMs:30000,dryRun:false});
 const reader=(on:boolean)=>db.query("UPDATE sessions.ingest_claim_sessions_policy SET read_enabled=$1",[on]);
 /** The operator sequence from the packet: writer on, backfill to completion, reader on. */
 const enableAll=async()=>{await writer(true);const r=await backfill();expect(r.completionMarked).toBe(true);await reader(true);expect(await policy()).toEqual({w:true,r:true,c:true})};
 // 044040's readers, verbatim, as the reference.
 const ref=readFileSync(join(root,SESSION_FIRST),"utf8");
 const refList=pickFn(ref,"sessions").replace("CREATE OR REPLACE FUNCTION sessions.buffered_sessions_for(","CREATE FUNCTION ref044090.list(");
 const refStages=pickFn(ref,"stages").replace("CREATE OR REPLACE FUNCTION sessions.buffered_session_stages_for(","CREATE FUNCTION ref044090.stages(");
 await db.exec("CREATE SCHEMA ref044090;GRANT USAGE ON SCHEMA ref044090 TO bulls96_read_api;"+refList+refStages+"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref044090 FROM PUBLIC;GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ref044090 TO bulls96_read_api");
 return {db,upload,deliver,asReader,prove,list,stages,policy,writer,backfill,reader,enableAll};
}
type F=Awaited<ReturnType<typeof fixture>>;
const code=async(p:Promise<unknown>)=>{const e=await p.then(()=>null,(e)=>e) as any;return e?.code as string|undefined};

const binds:Array<[string,{org?:string;user?:string}]>=[["user-1",{}],["user-2",{user:"user-2"}],["org-b",{org:"org-b"}]];
const keys:Array<[string,string]>=[];for(const src of ["claude","codex"])for(const n of [...Array(16).keys(),90])keys.push([src,S(n)]);
/** Walk the whole list with page size `size`, recording each page (first page and every continued page). */
async function pages(f:F,fn:string,size:number,bind={}){
 const out:any[][]=[];let c:Array<string|null>=[null,null,null];
 for(let i=0;i<200;i++){const r=await f.list(fn,size,c,bind);if(!r.length)break;out.push(r);const l=r.at(-1);c=[l.cursor_ts,l.session_id,l.source]}
 return out;
}
/** Every binding: the whole list (limit 100), pages of 1, 2, 3 and 5, and the detail of every key; returns user-1's counts. */
async function same(f:F,label:string,stats:{lists:number;pages:number;details:number}){
 const out:Record<string,[number,number]>={};
 for(const [who,bind] of binds){
  const [a,b]=await Promise.all([f.list(LIST_FN,100,undefined,bind),f.list("ref044090.list",100,undefined,bind)]);
  expect([label,who,a]).toEqual([label,who,b]);stats.lists++;
  for(const size of [1,2,3,5]){
   const [pa,pb]=[await pages(f,LIST_FN,size,bind),await pages(f,"ref044090.list",size,bind)];
   expect([label,who,size,pa]).toEqual([label,who,size,pb]);
   expect(pa.flat()).toEqual(b);stats.pages+=pa.length;
  }
  let n=0;
  for(const [src,session] of keys){
   const [x,y]=await Promise.all([f.stages(STAGES_FN,src,session,bind),f.stages("ref044090.stages",src,session,bind)]);
   expect([label,who,src,session,x]).toEqual([label,who,src,session,y]);n+=x.length;stats.details++;
  }
  out[who]=[a.length,n];
 }
 return out;
}

test("(1) gate off: both readers are 044040's, with the writer off, and with the writer on and the backfill complete",async()=>{
 const f=await fixture();
 await f.deliver([["claude",S(1),["r1-a"]],["claude",S(2),["r2-a"]],["codex",S(1),["c1-a"]]]);
 await f.upload(S(3));await f.upload(S(4),3);
 await f.prove();
 const stats={lists:0,pages:0,details:0};
 expect(await f.policy()).toEqual({w:false,r:false,c:false});
 expect((await same(f,"writer off",stats))["user-1"]).toEqual([5,5]);
 await f.writer(true);await f.backfill();
 expect(await f.policy()).toEqual({w:true,r:false,c:true});
 // The side table is complete, but read_enabled is off: still 044040's query (no side-table read can change it).
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions WHERE session_id=$1",[S(1)]);
 expect((await same(f,"writer on, reader off, side rows missing",stats))["user-1"]).toEqual([5,5]);
 expect(stats.details).toBeGreaterThan(50);
});

test("(2)(3) gate on: list rows, order and cursor on the first and every continued page, and every detail, equal 044040's",async()=>{
 const f=await fixture();
 const U2={...TENANT,userId:"user-2"},OB={...TENANT,orgId:"org-b"};
 const stats={lists:0,pages:0,details:0};const trail:Array<[string,number,number]>=[];
 const check=async(label:string)=>{const o=await same(f,label,stats);trail.push([label,...o["user-1"]!]);return o};
 // History accepted before the writer exists (backfill-only side rows): one hidden by the reset below, then
 // several sessions per batch (their last_ts tie), a superseded record, turn batches.
 await f.upload(S(90));
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,reset_at) VALUES($1,$2,$3,'r-1',now()-interval '1 hour')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await f.deliver([["claude",S(1),["r1-a","r1-b"]],["claude",S(2),["r2-a","r2-b","r2-c"]],["codex",S(1),["c1-a"]]]);
 await f.deliver([["claude",S(1),["r1-c"]],["claude",S(3),["r3-a","r3-b"]]]);
 await f.deliver([["claude",S(2),["r2-d"]],["claude",S(4),["r4-a"]],["codex",S(3),["c3-a","c3-b"]]]);
 await f.deliver([["claude",S(1),["r1-a"]],["claude",S(5),["r5-a","r5-b"]]]);
 await f.deliver([["claude",U2.userId,["u2-a"]],["claude",S(7),["u2-b"]]],U2);
 await f.deliver([["claude",S(1),["ob-a"]]],OB);
 await f.enableAll();
 // Accepted with the writer on (writer-inserted rows, accepted_at from the commit refresh): more sessions
 // than a page, one batch of six tied sessions, and a session that already had rows.
 for(const n of [6,7,8])await f.upload(S(n),1);
 await f.deliver([["claude",S(9),["t9"]],["claude",S(10),["t10"]],["codex",S(10),["t10c"]],["claude",S(11),["t11"]],["codex",S(12),["t12"]],["claude",S(13),["t13"]]]);
 await f.upload(S(2),2,"claude",0);await f.upload(S(14),2,"codex");
 await f.deliver([["claude",S(1),["u2-c"]]],U2);
 await f.prove();await f.prove({user:"user-2"});await f.prove({org:"org-b"});
 // Every side row carries its batch's committed_at (backfilled or refreshed), none NULL.
 expect((await f.db.query("SELECT count(*)::int n,count(*) FILTER (WHERE cs.accepted_at IS DISTINCT FROM b.committed_at)::int bad FROM sessions.ingest_claim_sessions cs JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id)")).rows[0]).toMatchObject({bad:0});
 const base=await check("base");
 expect(base).toEqual({"user-1":[17,21],"user-2":[3,2],"org-b":[1,1]});
 // A deletion (any origin) of one session, re-proven.
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(4)]);
 await check("deletion, stale proof");await f.prove();
 await check("deletion");
 // A frozen source deletion (rule 5(a)) of a recent session: hidden without a new proof.
 await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(11)]);
 await check("frozen deletion");
 // A fenced attempt: the newest claim's admission version moves on.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_control_version=5 WHERE batch_id=(SELECT max(batch_id COLLATE \"C\") FROM sessions.ingest_claim_attempts WHERE user_id=$1 AND records::text LIKE '%'||$2||'%')",[TENANT.userId,S(14)]);
 await check("fenced attempt");
 // A live publication head on the newest tied sessions and on an old one: excluded from the list, walk goes on.
 const pub=(source:string,session:string,state:string,key:string|null)=>f.db.query("INSERT INTO sessions.ingest_publication_heads(org_id,workspace_id,user_id,origin_id,source,session_id,physical_workspace,physical_source,physical_session,machine_id,generation,fence_epoch,reset_epoch,pointer_revision,current_key,state) VALUES($1,$2,$3,$4,$5,$6,$2,$5,$6,'m-1',gen_random_uuid(),1,0,1,$7,$8)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,source,session,key,state]);
 await pub("claude",S(9),"live","k/9");await pub("codex",S(10),"live","k/10");await pub("claude",S(13),"building",null);await pub("claude",S(3),"live","k/3");
 await check("publication heads");
 // A lazy-ACK decision at another admission version hides that claim's records.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions d SET source_control_version=5 FROM sessions.ingest_claim_attempts a WHERE a.claim_token=d.claim_token AND a.batch_id='b-4'");
 await check("lazy ack");
 // A missed commit refresh (NULL accepted_at under a committed batch) on old and recent rows: the pre-pass evaluates them.
 await f.db.query("UPDATE sessions.ingest_claim_sessions SET accepted_at=NULL WHERE batch_id IN('b-3','b-12') AND user_id=$1",[TENANT.userId]);
 await check("missed refresh");
 // Duplicate record ordinal (recn) on a claim.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=records||jsonb_build_array(records->0) WHERE batch_id='b-5'");
 await check("duplicate record ordinal");
 // The reset moves past b-3's acceptance: everything accepted at or before it hides.
 const acc=((await f.db.query("SELECT committed_at::text t FROM sessions.ingest_batches WHERE batch_id='b-3'")).rows[0] as any).t;
 await f.db.query("UPDATE sessions.upload_resets SET reset_at=$1::timestamptz WHERE request_id='r-1'",[acc]);
 await check("reset, stale proof");await f.prove();
 await check("reset");
 expect(trail.map((t)=>t[0])).toEqual(["base","deletion, stale proof","deletion","frozen deletion","fenced attempt","publication heads","lazy ack","missed refresh","duplicate record ordinal","reset, stale proof","reset"]);
 // Every state but two changed what user-1 sees: a missed refresh must change nothing, and the reset already
 // applied under the stale proof (the re-proof keeps it). A stale proof after a deletion closes the gate.
 expect(trail).toEqual([["base",17,21],["deletion, stale proof",0,0],["deletion",16,20],["frozen deletion",15,19],["fenced attempt",14,18],["publication heads",11,18],["lazy ack",10,16],["missed refresh",10,16],["duplicate record ordinal",10,15],["reset, stale proof",8,10],["reset",8,10]]);
 expect(stats).toEqual({lists:33,pages:372,details:1122}); // 11 states x 3 bindings; pages of 1,2,3,5; 34 keys
});

test("(2) gate on: a first page reads a bounded number of side rows, not the whole history",async()=>{
 const f=await fixture();await f.enableAll();
 for(let n=0;n<30;n++)await f.upload(S(n),1);
 await f.prove();
 const [a,b]=await Promise.all([f.list(LIST_FN,2),f.list("ref044090.list",2)]);
 expect(a).toEqual(b);expect(a.map((r)=>r.session_id)).toEqual([S(29),S(28)]);
 // The first page needs only the first chunk (4*limit = 8 newest side rows): with the side rows of the 20
 // oldest sessions gone, page one (and the page after it) is unchanged; only deep pages lose those sessions.
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions WHERE session_id = ANY($1)",[Array.from({length:20},(_,n)=>S(n))]);
 expect(await f.list(LIST_FN,2)).toEqual(b);
 expect(await f.list(LIST_FN,2,[b[1].cursor_ts,b[1].session_id,b[1].source])).toEqual(await f.list("ref044090.list",2,[b[1].cursor_ts,b[1].session_id,b[1].source]));
 expect((await f.list(LIST_FN,100)).length).toBe(10);
});

test("(4) commit refresh: accepted_at becomes the batch's committed_at at commit; off while the writer is off; errors and lock timeouts swallowed, lock_timeout restored",async()=>{
 const f=await fixture();
 const T={orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:TENANT.userId},o="origin-r";let n=0;
 const pending=async()=>{const b="rb-"+(++n),token=crypto.randomUUID();
  await f.db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,$2,$3,$4,$5,repeat('d',64),'rhr_'||repeat('a',64),'pending',NULL,'canonical')",[T.orgId,T.workspaceId,o,b,T.userId]);
  await f.db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch) VALUES($1,$2,$3,$4,$5,$6,repeat('d',64),'[]',$7,0)",[token,T.orgId,T.workspaceId,T.userId,o,b,JSON.stringify([{ord:0,source:"claude",session_id:"s-"+n},{ord:1,source:"codex",session_id:"s-"+n}])]);
  return b};
 const commit=(b:string,at="2026-11-05 10:00:00+00")=>f.db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at=$2::timestamptz WHERE origin_id=$3 AND batch_id=$1",[b,at,o]);
 const acc=async(b:string)=>(await f.db.query<{a:string|null}>("SELECT accepted_at::text a FROM sessions.ingest_claim_sessions WHERE batch_id=$1 ORDER BY source",[b])).rows.map((r)=>r.a);
 // The trigger exists as designed, owner only, not SECURITY DEFINER.
 const trg=(await f.db.query<{d:string}>("SELECT pg_get_triggerdef(oid) d FROM pg_trigger WHERE tgname='ingest_batch_claim_sessions_refresh'")).rows.map((r)=>r.d);
 expect(trg).toEqual(["CREATE TRIGGER ingest_batch_claim_sessions_refresh AFTER UPDATE OF state, committed_at ON sessions.ingest_batches FOR EACH ROW WHEN (((new.state = 'committed'::text) AND ((old.state IS DISTINCT FROM 'committed'::text) OR (old.committed_at IS DISTINCT FROM new.committed_at)))) EXECUTE FUNCTION sessions.ingest_batch_claim_sessions_refresh()"]);
 expect((await f.db.query("SELECT p.prosecdef,p.provolatile,p.proconfig,(SELECT count(*)::int FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee<>p.proowner) others FROM pg_proc p WHERE p.oid='sessions.ingest_batch_claim_sessions_refresh()'::regprocedure")).rows).toEqual([{prosecdef:false,provolatile:"v",proconfig:["search_path=pg_catalog, pg_temp"],others:0}]);
 // Through the real buffered path as bulls96_session_ingest (writer on): every side row equals its batch's committed_at.
 await f.writer(true);
 await f.upload(S(1));await f.deliver([["claude",S(2),["x"]],["codex",S(2),["y"]]]);
 expect((await f.db.query("SELECT count(*)::int n,count(*) FILTER (WHERE cs.accepted_at IS NOT DISTINCT FROM b.committed_at AND b.committed_at IS NOT NULL)::int ok FROM sessions.ingest_claim_sessions cs JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id)")).rows).toEqual([{n:3,ok:3}]);
 // Direct owner claims on a pending batch: the claim is written before the commit (NULL), the commit sets both rows.
 const b1=await pending();
 expect(await acc(b1)).toEqual([null,null]);
 await commit(b1);
 expect(await acc(b1)).toEqual(["2026-11-05 10:00:00+00","2026-11-05 10:00:00+00"]);
 // A later unrelated batch update (publication_state) leaves it alone; another batch's rows are untouched.
 const b2=await pending();
 await f.db.query("UPDATE sessions.ingest_batches SET publication_state='buffered' WHERE batch_id=$1",[b1]);
 expect(await acc(b1)).toEqual(["2026-11-05 10:00:00+00","2026-11-05 10:00:00+00"]);
 expect(await acc(b2)).toEqual([null,null]);
 // Writer off: the commit refreshes nothing (rows of b2 stay NULL).
 await f.writer(false);
 await commit(b2);
 expect(await acc(b2)).toEqual([null,null]);
 // An error inside the refresh (WHEN OTHERS) does not break the commit: the row stays NULL.
 await f.writer(true);
 const b3=await pending();
 await f.db.exec("CREATE FUNCTION pg_temp.boom() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'boom';END$$;CREATE TRIGGER zz_boom BEFORE UPDATE ON sessions.ingest_claim_sessions FOR EACH ROW EXECUTE FUNCTION pg_temp.boom()");
 await commit(b3);
 expect((await f.db.query("SELECT state FROM sessions.ingest_batches WHERE batch_id=$1",[b3])).rows).toEqual([{state:"committed"}]);
 expect(await acc(b3)).toEqual([null,null]);
 await f.db.exec("DROP TRIGGER zz_boom ON sessions.ingest_claim_sessions");
 // A lock timeout (55P03) inside the refresh is swallowed too, and the caller's lock_timeout survives both the
 // swallowed error and a successful refresh (the refresh's 100ms is transaction-local and restored).
 const b4=await pending(),b5=await pending();
 await f.db.exec("CREATE FUNCTION pg_temp.locked() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF current_setting('lock_timeout')<>'100ms' THEN RAISE EXCEPTION 'lock_timeout not bounded: %',current_setting('lock_timeout');END IF;RAISE EXCEPTION USING ERRCODE='55P03',MESSAGE='lock timeout';END$$;CREATE TRIGGER zz_locked BEFORE UPDATE ON sessions.ingest_claim_sessions FOR EACH ROW EXECUTE FUNCTION pg_temp.locked()");
 await f.db.exec("BEGIN;SET LOCAL lock_timeout='7s'");
 await commit(b4);
 expect((await f.db.query("SELECT current_setting('lock_timeout') t")).rows).toEqual([{t:"7s"}]);
 await f.db.exec("COMMIT;DROP TRIGGER zz_locked ON sessions.ingest_claim_sessions");
 expect((await f.db.query("SELECT state FROM sessions.ingest_batches WHERE batch_id=$1",[b4])).rows).toEqual([{state:"committed"}]);
 expect(await acc(b4)).toEqual([null,null]);
 await f.db.exec("BEGIN;SET LOCAL lock_timeout='7s'");
 await commit(b5);
 expect((await f.db.query("SELECT current_setting('lock_timeout') t")).rows).toEqual([{t:"7s"}]);
 await f.db.exec("COMMIT");
 expect(await acc(b5)).toEqual(["2026-11-05 10:00:00+00","2026-11-05 10:00:00+00"]);
 expect((await f.db.query("SELECT current_setting('lock_timeout') t")).rows).toEqual([{t:"0"}]);
 // A non-owner committer that cannot read the owner-only policy (a role granted UPDATE on ingest_batches): the
 // commit succeeds and refreshes nothing, instead of raising 42501 outside the EXCEPTION block.
 const b6=await pending();
 await f.db.exec("CREATE ROLE claim_refresh_committer BYPASSRLS;GRANT USAGE ON SCHEMA sessions TO claim_refresh_committer;GRANT SELECT,UPDATE ON sessions.ingest_batches TO claim_refresh_committer");
 await f.db.exec("BEGIN;SET LOCAL ROLE claim_refresh_committer");
 await commit(b6);
 await f.db.exec("COMMIT");
 expect((await f.db.query("SELECT state FROM sessions.ingest_batches WHERE batch_id=$1",[b6])).rows).toEqual([{state:"committed"}]);
 expect(await acc(b6)).toEqual([null,null]);
 // Shape: the policy is read before the subtransaction, and only the side-table write is inside it, taking its
 // rows FOR UPDATE SKIP LOCKED (never waits on a row lock held by the backfill or another writer).
 const src=(await f.db.query<{s:string}>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_batch_claim_sessions_refresh()'::regprocedure")).rows[0]!.s;
 const policyAt=src.indexOf("FROM sessions.ingest_claim_sessions_policy"),blockAt=src.indexOf("\n BEGIN\n");
 expect(policyAt).toBeGreaterThan(0);expect(blockAt).toBeGreaterThan(policyAt);
 expect(src.slice(blockAt)).not.toContain("ingest_claim_sessions_policy");
 expect(src.slice(blockAt)).toContain("FOR UPDATE OF s SKIP LOCKED");
 expect(src.slice(blockAt)).toContain("set_config('lock_timeout','100ms',true)");
});

test("(5) the CHECK refuses read_enabled without the writer and completion; any write_enabled change clears read_enabled",async()=>{
 const f=await fixture();
 expect(await f.policy()).toEqual({w:false,r:false,c:false});
 expect(await code(f.reader(true))).toBe("23514");
 await f.writer(true);
 expect(await code(f.reader(true))).toBe("23514");
 await f.backfill();
 await f.reader(true);
 expect(await f.policy()).toEqual({w:true,r:true,c:true});
 // Clearing completion directly with the reader on is refused too.
 expect(await code(f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_complete_at=NULL"))).toBe("23514");
 // Same writer value: no transition, nothing cleared; the backfill re-run is a no-op.
 await f.writer(true);
 expect((await f.backfill()).alreadyComplete).toBe(true);
 expect(await f.policy()).toEqual({w:true,r:true,c:true});
 // Writer off: the guard clears completion and the reader together (otherwise the CHECK would refuse it).
 await f.writer(false);
 expect(await f.policy()).toEqual({w:false,r:false,c:false});
 // Writer back on: the reader stays off until a new completed backfill and an explicit reader switch.
 await f.writer(true);await f.backfill();
 expect(await f.policy()).toEqual({w:true,r:false,c:true});
 // Even one UPDATE setting both is cleared (the guard runs before the CHECK).
 await f.reader(true);
 await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=false,read_enabled=true");
 expect(await f.policy()).toEqual({w:false,r:false,c:false});
 // The column and constraint as designed; the read role sees nothing of the policy.
 expect((await f.db.query("SELECT pg_get_constraintdef(oid) d FROM pg_constraint WHERE conname='ingest_claim_sessions_policy_read_check'")).rows).toEqual([{d:"CHECK (((NOT read_enabled) OR (write_enabled AND (backfill_complete_at IS NOT NULL))))"}]);
 await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_read_api");
 expect(await code(f.db.query("SELECT read_enabled FROM sessions.ingest_claim_sessions_policy"))).toBe("42501");
 await f.db.exec("ROLLBACK");
});

test("(6) gate on: deleted side rows never raise (the session is then missing from v2, the documented divergence)",async()=>{
 const f=await fixture();
 await f.deliver([["claude",S(1),["a"]],["claude",S(2),["b"]]]);await f.upload(S(3));
 await f.enableAll();await f.prove();
 expect((await f.list(LIST_FN,10)).map((r)=>r.session_id)).toEqual([S(3),S(1),S(2)]);
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions WHERE session_id=$1",[S(1)]);
 expect((await f.list(LIST_FN,10)).map((r)=>r.session_id)).toEqual([S(3),S(2)]);
 expect(await f.stages(STAGES_FN,"claude",S(1))).toEqual([]);
 expect((await f.list("ref044090.list",10)).map((r)=>r.session_id)).toEqual([S(3),S(1),S(2)]);
 // Every side row gone: empty, no error, on the first page and under a cursor.
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions");
 expect(await f.list(LIST_FN,10)).toEqual([]);
 expect(await f.list(LIST_FN,1,["2026-11-05 00:00:00+00",S(1),"claude"])).toEqual([]);
 expect(await f.stages(STAGES_FN,"claude",S(3))).toEqual([]);
 // Gate off again (writer toggled): 044040's rows come back.
 await f.writer(false);
 expect((await f.list(LIST_FN,10)).map((r)=>r.session_id)).toEqual([S(3),S(1),S(2)]);
});

test("static: gate-off bodies are 044040's verbatim, v2 keep blocks differ only in stg's head, no locks or writes, attributes and ACL kept",async()=>{
 const sql=readFileSync(join(root,READER),"utf8"),old=readFileSync(join(root,SESSION_FIRST),"utf8");
 for(const name of ["sessions","stages"] as const){
  const fn=pickFn(sql,name),ref=pickFn(old,name);
  // Remove the 044090 declarations and the gate + v2 branch: what is left is 044040's function, byte for byte.
  const gate=fn.indexOf("  -- 044090: the claim-sessions reader gate"),off=fn.indexOf("  -- Gate off: 044040's query, unchanged.\n");
  expect(gate).toBeGreaterThan(0);expect(off).toBeGreaterThan(gate);
  const rest=(fn.slice(0,gate)+fn.slice(off+"  -- Gate off: 044040's query, unchanged.\n".length))
   .replace(/  -- 044090 \(claim-sessions reader v2[^\n]*\n(?:  [rev]_\w+ [^\n]*\n)+/,"")
   .replace("  v_v2 boolean; -- 044090: the claim-sessions reader gate\n","");
  expect(rest).toBe(ref);
  const code=fn.split("\n").filter((l)=>!l.trim().startsWith("--")).join("\n");
  expect(code).not.toMatch(/\bFOR\s+(UPDATE|SHARE|NO\s+KEY\s+UPDATE|KEY\s+SHARE)\b|pg_(try_)?advisory|\bLOCK\s+TABLE\b|\b(INSERT|UPDATE|DELETE)\s|\bGRANT\b|\bREVOKE\b|CREATE\s+TEMP/i);
 }
 // Keep blocks: 044090 has four (v2 and gate-off, per reader). Gate-off ones equal 044040's; v2 ones differ only in stg's head.
 const blocks=(s:string)=>[...s.matchAll(/-- keep-predicate:begin[\s\S]*?-- keep-predicate:end/g)].map((m)=>m[0]);
 const [v2List,offList,v2Stages,offStages]=blocks(sql) as [string,string,string,string];
 const [oldList,oldStages]=blocks(old) as [string,string];
 expect(blocks(sql).length).toBe(4);
 expect(offList).toBe(oldList);expect(offStages).toBe(oldStages);
 const noComments=(b:string)=>b.split("\n").filter((l)=>!l.trim().startsWith("--"));
 const diff=(a:string[],b:string[])=>{let i=0;while(i<a.length&&a[i]===b[i])i++;let j=0;while(j<a.length-i&&j<b.length-i&&a[a.length-1-j]===b[b.length-1-j])j++;return {at:i,from:a.slice(i,a.length-j),to:b.slice(i,b.length-j)}};
 const dl=diff(noComments(oldList),noComments(v2List));
 expect(dl.from).toEqual(["      FROM sessions.ingest_buffer_outbox AS o","      JOIN sessions.ingest_stage_intents AS s ON s.stage_token = o.active_stage_token","      JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = s.claim_token","      JOIN sessions.ingest_batches AS b","        ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id","     CROSS JOIN ctl","     WHERE o.org_id = v_org AND o.workspace_id = v_ws AND o.user_id = v_user AND o.state = 'buffered'"]);
 expect(dl.to).toEqual(["      FROM sessions.ingest_claim_attempts AS a","      JOIN sessions.ingest_stage_intents AS s ON a.claim_token = s.claim_token","      JOIN sessions.ingest_buffer_outbox AS o ON s.stage_token = o.active_stage_token","      JOIN sessions.ingest_batches AS b","        ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id","     CROSS JOIN ctl",
  "     WHERE a.claim_token IN (SELECT cs.claim_token","                               FROM sessions.ingest_claim_sessions AS cs","                               JOIN unnest(v_new_src, v_new_ses) AS cq(source, session_id)","                                 ON cs.source = cq.source AND cs.session_id = cq.session_id","                              WHERE cs.org_id = v_org AND cs.workspace_id = v_ws AND cs.user_id = v_user)","       AND o.org_id = v_org AND o.workspace_id = v_ws AND o.user_id = v_user AND o.state = 'buffered'"]);
 const ds=diff(noComments(oldStages),noComments(v2Stages));
 expect(ds.from).toEqual(["      FROM sessions.ingest_claim_attempts AS a","     WHERE jsonb_typeof(a.records) = 'array'","       AND a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session))"].slice(0,1).concat(ds.from.slice(1)));
 expect(ds.to.slice(0,2)).toEqual(["      FROM sessions.ingest_claim_sessions AS cs","      JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = cs.claim_token"]);
 expect(ds.to.slice(-4)).toEqual(["     WHERE cs.org_id = v_org AND cs.workspace_id = v_ws AND cs.user_id = v_user","       AND cs.source = v_source AND cs.session_id = v_session","       AND jsonb_typeof(a.records) = 'array'","       AND coalesce(a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session)), false)"]);
 // The guard is 044080's plus exactly one assignment.
 const guard=(s:string)=>s.match(/FUNCTION sessions\.ingest_claim_sessions_policy_guard\(\)[\s\S]*?END \$claim_sessions_policy\$;/)![0];
 expect(guard(sql).replace("NEW.read_enabled:=false;","")).toBe(guard(readFileSync(join(root,CLAIM_SESSIONS),"utf8")));
 // Installed: same attributes, and the ACLs equal the manifest.
 const f=await fixture();
 const rows=(await f.db.query("SELECT p.oid::regprocedure::text AS fn,p.prosecdef,p.provolatile,p.proconfig,array_agg(CASE WHEN a.grantee=p.proowner THEN '<owner>' ELSE r.rolname END||':'||a.privilege_type ORDER BY 1) AS acl FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.pronamespace='sessions'::regnamespace AND p.proname IN('buffered_sessions_for','buffered_session_stages_for','ingest_claim_sessions_policy_guard','ingest_batch_claim_sessions_refresh') GROUP BY p.oid,p.proowner ORDER BY 1")).rows as any[];
 expect(rows.map((r)=>[r.fn.replace(/\(.*$/,""),r.prosecdef,r.provolatile,r.proconfig])).toEqual([
  ["sessions.buffered_session_stages_for",true,"s",["search_path=pg_catalog, sessions, pg_temp"]],
  ["sessions.buffered_sessions_for",true,"s",["search_path=pg_catalog, sessions, pg_temp"]],
  ["sessions.ingest_batch_claim_sessions_refresh",false,"v",["search_path=pg_catalog, pg_temp"]],
  ["sessions.ingest_claim_sessions_policy_guard",false,"v",["search_path=pg_catalog, pg_temp"]]]);
 const manifest=(acl as any).functions??acl;
 const norm=(s:string)=>s.replace(/timestamptz/g,"timestamp with time zone");
 for(const r of rows){
  const entry=Object.entries(manifest).find(([k])=>norm(k)===norm(r.fn));
  expect([r.fn,!!entry]).toEqual([r.fn,true]);
 }
});
