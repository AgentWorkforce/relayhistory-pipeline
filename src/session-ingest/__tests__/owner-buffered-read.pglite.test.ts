/**
 * 20261105043700 owner buffered read RPCs. Seeds through the real buffered acceptance path
 * (POST /v1/turns/batches, frozen-supabase custody, lazy-current policy: real claim attempts,
 * stage intents, ACK decisions and native slots), then reads as bulls96_read_api with the
 * transaction-local bulls96_read.* binding the read API sets.
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
import {TENANT,scratchStore} from "./support.ts";
setDefaultTimeout(60000);

const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
/** psql meta-commands (\set) are not SQL. */
const psql=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter((l)=>!l.startsWith("\\")).join("\n");
const MIGRATION="supabase/migrations/20261105043700_owner_buffered_read.sql";
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles","session_read_api_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 // The read role's approved shape (schema USAGE, the session_index column grant, RLS), as the operator applies it.
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
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:deps,log:()=>{},custodySource:"frozen-supabase"});
 let serial=0;
 /** One accepted buffered turn batch for (source, session); returns its batch id. */
 async function upload(session:string,turns=2,source="claude",start=0){
  const batchId="b-"+(++serial);
  const records=Array.from({length:turns},(_,j)=>{const i=start+j;
   const record_id=turnRecordId(TENANT.workspaceId,source,session,i),revision=1_793_000_000_000_000+serial*100+i;
   return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,record_id,revision_id:`turnrev:${record_id.slice(5)}:${revision}`,revision,kind:TURN_KIND as any,source,session_id:session,operation:"upsert" as const,
    payload:{id:`${String(serial*100+i).padStart(8,"0")}-aaaa-4bbb-8ccc-dddddddddddd`,org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,source,session_id:session,turn_index:i,session_owner:"user-1",role:"user",content:`turn ${i}`,actor_name:"Ada",actor_role:"owner",metadata_json:"{}",ts:"2026-11-05T12:00:00.123Z",tagged:true}};
  });
  const b={schema_version:1 as const,origin_id:TURN_ORIGIN_ID,batch_id:batchId,job_id:"relayhistory-turns",generation:1,destination_id:"relayhistory",instance_id:"relayhistory-api",account_id:await deliveryAccount(TENANT),mapping_version:DELIVERY_MAPPING_VERSION,records};
  const r=await app.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:b})});
  if(r.status!==200)throw new Error("upload "+r.status+" "+await r.text());
  return batchId;
 }
 /** Read as bulls96_read_api, bound like the read API binds a verified identity. */
 const asReader=async(sql:string,params:unknown[]=[],bind:{org?:string;ws?:string;user?:string|null}={},role="bulls96_read_api")=>{
  await db.exec("BEGIN READ ONLY");
  try{
   await db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",[bind.org??TENANT.orgId,bind.ws??TENANT.workspaceId,bind.user===undefined?TENANT.userId:(bind.user??"")]);
   await db.exec("SET LOCAL ROLE "+role);
   const r=await db.query(sql,params as never);await db.exec("COMMIT");return r.rows as any[];
  }catch(e){await db.exec("ROLLBACK");throw e}
 };
 const list=(limit:number|null=null,cursor:[string|null,string|null,string|null]=[null,null,null],bind={})=>asReader("SELECT source,session_id,first_ts,last_ts,stage_count,bytes::text AS bytes,user_id FROM sessions.buffered_sessions_for($1,$2,$3,$4,$5)",[TENANT.workspaceId,cursor[0],cursor[1],cursor[2],limit],bind);
 const stages=(session:string,source="claude",bind={})=>asReader("SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at FROM sessions.buffered_session_stages_for($1,$2,$3)",[TENANT.workspaceId,source,session],bind);
 return {db,owner,upload,asReader,list,stages,ledger};
}
const S=(n:number)=>`7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a${String(n).padStart(2,"0")}`;

test("(a) the owner sees their buffered session with stage_count, bytes and acceptance times",async()=>{
 const f=await fixture();
 await f.upload(S(1));await f.upload(S(1),3,"claude",2);
 const rows=await f.list();
 const ledger=(await f.db.query("SELECT sum(i.stage_bytes)::text AS bytes,min(b.committed_at) AS first,max(b.committed_at) AS last FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token JOIN sessions.ingest_batches b ON b.org_id=o.org_id AND b.workspace_id=o.workspace_id AND b.origin_id=o.origin_id AND b.batch_id=o.batch_id")).rows[0] as any;
 expect(rows).toEqual([{source:"claude",session_id:S(1),first_ts:ledger.first,last_ts:ledger.last,stage_count:2,bytes:ledger.bytes,user_id:TENANT.userId}]);
 const st=await f.stages(S(1));
 expect(st.map((s)=>[s.batch_id,s.group_indexes,s.kept_ordinals])).toEqual([["b-1",[0],[0,1]],["b-2",[0],[0,1,2]]]);
 expect(st.map((s)=>[s.stage_key.includes("/buffered-batches/v1/ws=ws-a/"),/^[0-9a-f]{64}$/.test(s.stage_sha256),s.reset_epoch])).toEqual([[true,true,"0"],[true,true,"0"]]);
 // A later revision of the same turn supersedes the older one: the GC would drop it, so does the read.
 await f.upload(S(1),1,"claude",0);
 expect((await f.stages(S(1))).map((s)=>[s.batch_id,s.kept_ordinals])).toEqual([["b-1",[1]],["b-2",[0,1,2]],["b-3",[0]]]);
});
