/**
 * Isolated fixture for 047010 equivalence. Synthetic data and PGlite only.
 * 20261105044090 claim-sessions reader v2 (PR 2, default off): sessions.buffered_sessions_for and
 * sessions.buffered_session_stages_for read sessions.ingest_claim_sessions (044080) while the policy gate
 * (read_enabled AND write_enabled AND backfill_complete_at) is on, and must return exactly 044040's rows,
 * order and cursor. The frozen 044040 list and 044090 detail are loaded as ref044090.list / ref044090.stages (a
 * separate schema, so the sessions function manifest stays exact) and compared row for row.
 * Seeds through the real buffered acceptance path (POST /v1/turns/batches and /v1/delivery/batches as
 * bulls96_session_ingest), as owner-buffered-read.pglite.test.ts; reads as bulls96_read_api.
 */
import {expect,afterEach,setDefaultTimeout} from "bun:test";
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
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
setDefaultTimeout(300000);

const root=join(import.meta.dir,"../../.."),generation="4".repeat(64),epoch="1".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const psql=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter((l)=>!l.startsWith("\\")).join("\n");
const READER="supabase/migrations/20261105044090_claim_sessions_reader.sql";
const SESSION_FIRST="supabase/migrations/20261105044040_buffered_stages_session_first.sql";
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});
type Tenant={orgId:string;workspaceId:string;userId:string};
const HP=[epoch,null,null,null,null,false,baseline];
const LIST="SELECT source,session_id,first_ts,last_ts,last_ts::text AS cursor_ts,stage_count,bytes::text AS bytes,user_id,admission_epoch,admission_version FROM ";
const STAGES="SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at,admission_epoch,admission_version FROM ";
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
 // Frozen 044040 list and 044090 detail, verbatim, as independent references.
 const ref=readFileSync(join(root,SESSION_FIRST),"utf8");
 const refList=pickFn(ref,"sessions").replace("CREATE OR REPLACE FUNCTION sessions.buffered_sessions_for(","CREATE FUNCTION ref044090.list(");
 const refStages=pickFn(readFileSync(join(root,READER),"utf8"),"stages").replace("CREATE OR REPLACE FUNCTION sessions.buffered_session_stages_for(","CREATE FUNCTION ref044090.stages(");
 await db.exec("CREATE SCHEMA ref044090;GRANT USAGE ON SCHEMA ref044090 TO bulls96_read_api;"+refList+refStages+"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref044090 FROM PUBLIC;GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ref044090 TO bulls96_read_api");
 return {db,upload,deliver,asReader,prove,list,stages,policy,writer,backfill,reader,enableAll};
}
export { fixture };
